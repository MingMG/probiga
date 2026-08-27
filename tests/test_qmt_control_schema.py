from __future__ import annotations

import copy
import re

import pytest

from integrations.qmt import _control_schema as control_schema
from integrations.qmt import audit, catalog
from integrations.qmt._control_schema import FrozenIndex, FrozenTable
from tools import setup_guojin_qmt_catalog as catalog_refresh


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _Connection:
    def __init__(
        self,
        contracts: dict[str, FrozenTable],
        *,
        seed_rows=None,
        collations: dict[str, str] | None = None,
        engines: dict[str, str] | None = None,
        present_tables: set[str] | None = None,
        evidence_table_present: bool = False,
    ):
        self.contracts = contracts
        self.seed_rows = list(seed_rows or [])
        self.collations = {
            name: (collations or {}).get(name, contract.collation)
            for name, contract in contracts.items()
        }
        self.engines = {
            name: (engines or {}).get(name, contract.engine)
            for name, contract in contracts.items()
        }
        self.present_tables = set(
            contracts if present_tables is None else present_tables
        )
        self.evidence_table_present = evidence_table_present
        self.statements: list[str] = []

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        upper = sql.upper()
        if "INFORMATION_SCHEMA.TABLES" in upper:
            requested = {
                str(value)
                for key, value in (params or {}).items()
                if key == "table_name" or key.startswith("table_")
            }
            if not requested:
                requested = set(self.contracts)
            if requested == {control_schema.EVIDENCE_TABLE}:
                return _Rows(
                    [{"TABLE_NAME": control_schema.EVIDENCE_TABLE}]
                    if self.evidence_table_present else []
                )
            return _Rows([
                {
                    "TABLE_NAME": table_name,
                    "ENGINE": self.engines[table_name],
                    "TABLE_COLLATION": self.collations[table_name],
                }
                for table_name, contract in self.contracts.items()
                if table_name in requested and table_name in self.present_tables
            ])
        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            requested = {
                str(value)
                for key, value in (params or {}).items()
                if key.startswith("table_")
            } or set(self.contracts)
            rows = []
            for table_name, contract in self.contracts.items():
                if table_name not in requested or table_name not in self.present_tables:
                    continue
                for ordinal, (column_name, column) in enumerate(contract.columns, 1):
                    rows.append({
                        "TABLE_NAME": table_name,
                        "COLUMN_NAME": column_name,
                        "ORDINAL_POSITION": ordinal,
                        "COLUMN_TYPE": column.column_type,
                        "IS_NULLABLE": "YES" if column.nullable else "NO",
                        "COLUMN_DEFAULT": column.default,
                        "EXTRA": column.extra,
                        "CHARACTER_SET_NAME": "utf8mb4" if column.character else None,
                        "COLLATION_NAME": (
                            self.collations[table_name]
                            if column.character else None
                        ),
                    })
            return _Rows(rows)
        if "INFORMATION_SCHEMA.STATISTICS" in upper:
            requested = {
                str(value)
                for key, value in (params or {}).items()
                if key.startswith("table_")
            } or set(self.contracts)
            rows = []
            for table_name, contract in self.contracts.items():
                if table_name not in requested or table_name not in self.present_tables:
                    continue
                for index_name, index in contract.indexes.items():
                    for sequence, column_name in enumerate(index.columns, 1):
                        rows.append({
                            "TABLE_NAME": table_name,
                            "INDEX_NAME": index_name,
                            "NON_UNIQUE": 0 if index.unique else 1,
                            "SEQ_IN_INDEX": sequence,
                            "COLUMN_NAME": column_name,
                            "SUB_PART": None,
                            "INDEX_TYPE": index.index_type,
                        })
            return _Rows(rows)
        if "FROM QMT_API_REGISTRY" in upper:
            return _Rows(self.seed_rows)
        if upper.startswith("CREATE TABLE"):
            match = re.search(
                r"CREATE TABLE IF NOT EXISTS `?([A-Za-z0-9_]+)`?",
                sql,
                re.IGNORECASE,
            )
            if match and match.group(1) in self.contracts:
                self.present_tables.add(match.group(1))
            return _Rows([])
        if upper.startswith("ALTER TABLE"):
            match = re.match(r"ALTER TABLE `([^`]+)`", sql, re.I)
            assert match is not None
            self.collations[match.group(1)] = control_schema.EXPECTED_COLLATION
            return _Rows([])
        if upper.startswith("UPDATE QMT_API_REGISTRY"):
            return _Rows([])
        if upper.startswith("INSERT INTO QMT_API_REGISTRY"):
            return _Rows([])
        raise AssertionError(f"unexpected SQL in QMT contract test: {sql}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class _Engine:
    def __init__(self, connection: _Connection):
        self.connection = connection

    def connect(self):
        return self.connection

    def begin(self):
        return self.connection


def _seed_rows():
    return [dict(row) for row in catalog._seed_payload()]


def _stub_recovery_evidence(monkeypatch, *, fingerprints=None):
    calls = {"ensure": 0, "fingerprints": [], "persisted": []}
    queued = list(fingerprints or [])

    def ensure(_connection):
        calls["ensure"] += 1

    def fingerprint(_connection, table_name, *, order_by, columns):
        calls["fingerprints"].append((table_name, tuple(order_by), tuple(columns)))
        if queued:
            return queued.pop(0)
        return {"row_count": 3, "content_sha256": "a" * 64}

    def persist(_connection, records):
        batch = list(records)
        calls["persisted"].append(batch)
        return {
            "evidence_row_count": len(batch),
            "evidence_verified": True,
        }

    monkeypatch.setattr(control_schema, "ensure_evidence_table", ensure)
    monkeypatch.setattr(
        control_schema,
        "load_pending_physical_rewrite_plan",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        control_schema,
        "table_content_fingerprint",
        fingerprint,
    )
    monkeypatch.setattr(control_schema, "persist_and_verify_evidence", persist)
    return calls


@pytest.mark.parametrize(
    ("contracts", "planner"),
    [
        (catalog.CATALOG_TABLE_CONTRACTS, catalog.plan_catalog_schema_recovery),
        (audit.AUDIT_TABLE_CONTRACTS, audit.plan_audit_schema_recovery),
    ],
)
@pytest.mark.parametrize(
    ("physical_state", "expected_state", "migration_required"),
    [
        ("missing", "MISSING", True),
        ("target", "TARGET", False),
        ("legacy", "EXACT_GENERAL_CI", True),
    ],
)
def test_qmt_recovery_planners_are_read_only_content_hashed_and_deterministic(
    contracts,
    planner,
    physical_state,
    expected_state,
    migration_required,
    monkeypatch,
):
    calls = _stub_recovery_evidence(monkeypatch)

    def connection():
        return _Connection(
            contracts,
            present_tables=set() if physical_state == "missing" else None,
            collations=(
                {name: control_schema.LEGACY_COLLATION for name in contracts}
                if physical_state == "legacy" else None
            ),
        )

    first_connection = connection()
    first = planner(_Engine(first_connection))
    second_connection = connection()
    second = planner(_Engine(second_connection))

    assert first == second
    assert first["ready_for_privileged_apply"] is True
    assert first["read_only"] is True
    assert first["runtime_ddl_required"] is False
    assert first["migration_required"] is migration_required
    assert re.fullmatch(r"[0-9a-f]{64}", first["plan_sha256"])
    assert first["table_count"] == len(contracts)
    assert set(first["tables"]) == set(contracts)
    assert {
        detail["state"] for detail in first["tables"].values()
    } == {expected_state}
    assert first["state_counts"][expected_state] == len(contracts)
    expected_fingerprint = (
        None
        if physical_state == "missing"
        else {"row_count": 3, "content_sha256": "a" * 64}
    )
    assert all(
        detail["content_fingerprint"] == expected_fingerprint
        for detail in first["tables"].values()
    )
    assert all(
        detail["pending_plan"] is False
        for detail in first["tables"].values()
    )
    assert all(
        (
            re.fullmatch(
                r"[0-9a-f]{64}", str(detail["storage_plan_sha256"])
            ) is not None
        )
        is (physical_state == "legacy")
        for detail in first["tables"].values()
    )
    assert all(
        statement.upper().startswith("SELECT ")
        for statement in (
            *first_connection.statements,
            *second_connection.statements,
        )
    )
    assert calls["ensure"] == 0
    assert calls["persisted"] == []


@pytest.mark.parametrize(
    ("collation", "expected_action"),
    [
        (
            control_schema.EXPECTED_COLLATION,
            "FINALIZE_PENDING_VERIFICATION",
        ),
        (
            control_schema.LEGACY_COLLATION,
            "RESUME_COLLATION_NORMALIZATION",
        ),
    ],
)
def test_qmt_recovery_planner_accepts_verified_pending_plan(
    collation,
    expected_action,
    monkeypatch,
):
    original = catalog.CATALOG_TABLE_CONTRACTS["qmt_api_registry"]
    contract = FrozenTable(
        ddl=original.ddl,
        columns=original.columns,
        indexes={
            **original.indexes,
            "PRIMARY": FrozenIndex(("provider", "capability_key"), True),
        },
        engine=original.engine,
        collation=original.collation,
    )
    contracts = {"qmt_api_registry": contract}
    connection = _Connection(
        contracts,
        collations={"qmt_api_registry": collation},
        evidence_table_present=True,
    )
    calls = _stub_recovery_evidence(monkeypatch)
    before = {"row_count": 3, "content_sha256": "a" * 64}
    payload = control_schema._storage_plan_payload(
        table_name="qmt_api_registry",
        contract=contract,
        context="QMT pending planner",
        before_fingerprint=before,
    )
    pending_hash = control_schema.plan_sha256(
        recovery_version=control_schema.LEGACY_STORAGE_RECOVERY_VERSION,
        payload=payload,
    )
    record = control_schema.make_evidence_record(
        recovery_version=control_schema.LEGACY_STORAGE_RECOVERY_VERSION,
        source_table="qmt_api_registry",
        source_row_id=0,
        action=control_schema.PLAN_ACTION,
        business_key={"table": "qmt_api_registry"},
        source_row=payload,
        plan_payload=payload,
        plan_hash=pending_hash,
    )
    pending = {
        "record": record,
        "business_key": {"table": "qmt_api_registry"},
        "plan_sha256": pending_hash,
        "source_row": payload,
        "plan_payload": payload,
    }
    monkeypatch.setattr(
        control_schema,
        "load_pending_physical_rewrite_plan",
        lambda *_args, **_kwargs: pending,
    )
    trigger_requirements = []
    monkeypatch.setattr(
        control_schema,
        "validate_recovery_evidence_schema",
        lambda *_args, **kwargs: trigger_requirements.append(
            kwargs["require_triggers"]
        ) or {"read_only": True},
    )

    result = control_schema.plan_frozen_table_recovery(
        _Engine(connection),
        contracts,
        context="QMT pending planner",
    )

    detail = result["tables"]["qmt_api_registry"]
    assert result["ready_for_privileged_apply"] is True
    assert result["pending_table_names"] == ["qmt_api_registry"]
    assert result["pending_plan_count"] == 1
    assert result["migration_required"] is True
    assert detail["action"] == expected_action
    assert detail["pending_plan"] is True
    assert detail["pending_plan_sha256"] == pending_hash
    assert detail["storage_plan_sha256"] == pending_hash
    assert detail["content_fingerprint"] == before
    assert detail["order_by"] == ["provider", "capability_key"]
    assert trigger_requirements == [False, True]
    assert calls["ensure"] == 0
    assert calls["persisted"] == []
    assert all(
        statement.upper().startswith("SELECT ")
        for statement in connection.statements
    )


def test_qmt_recovery_planner_rejects_drift_and_pending_content_change(
    monkeypatch,
):
    contract = catalog.CATALOG_TABLE_CONTRACTS["qmt_api_registry"]
    contracts = {"qmt_api_registry": contract}
    drifted = _Connection(
        contracts,
        collations={"qmt_api_registry": control_schema.LEGACY_COLLATION},
        engines={"qmt_api_registry": "MyISAM"},
    )

    with pytest.raises(RuntimeError, match="unsupported physical drift"):
        control_schema.plan_frozen_table_recovery(
            _Engine(drifted),
            contracts,
            context="QMT planner drift",
        )
    assert all(
        statement.upper().startswith("SELECT ")
        for statement in drifted.statements
    )

    pending_connection = _Connection(
        contracts,
        evidence_table_present=True,
    )
    before = {"row_count": 3, "content_sha256": "a" * 64}
    payload = control_schema._storage_plan_payload(
        table_name="qmt_api_registry",
        contract=contract,
        context="QMT changed pending planner",
        before_fingerprint=before,
    )
    pending_hash = control_schema.plan_sha256(
        recovery_version=control_schema.LEGACY_STORAGE_RECOVERY_VERSION,
        payload=payload,
    )
    record = control_schema.make_evidence_record(
        recovery_version=control_schema.LEGACY_STORAGE_RECOVERY_VERSION,
        source_table="qmt_api_registry",
        source_row_id=0,
        action=control_schema.PLAN_ACTION,
        business_key={"table": "qmt_api_registry"},
        source_row=payload,
        plan_payload=payload,
        plan_hash=pending_hash,
    )
    monkeypatch.setattr(
        control_schema,
        "load_pending_physical_rewrite_plan",
        lambda *_args, **_kwargs: {
            "record": record,
            "business_key": {"table": "qmt_api_registry"},
            "plan_sha256": pending_hash,
            "source_row": payload,
            "plan_payload": payload,
        },
    )
    monkeypatch.setattr(
        control_schema,
        "validate_recovery_evidence_schema",
        lambda *_args, **_kwargs: {"read_only": True},
    )
    monkeypatch.setattr(
        control_schema,
        "table_content_fingerprint",
        lambda *_args, **_kwargs: {
            "row_count": 3,
            "content_sha256": "b" * 64,
        },
    )

    with pytest.raises(RuntimeError, match="source content changed"):
        control_schema.plan_frozen_table_recovery(
            _Engine(pending_connection),
            contracts,
            context="QMT changed pending planner",
        )
    assert all(
        statement.upper().startswith("SELECT ")
        for statement in pending_connection.statements
    )


@pytest.mark.parametrize(
    ("contracts", "validator"),
    [
        (catalog.CATALOG_TABLE_CONTRACTS, catalog.validate_catalog_schema),
        (audit.AUDIT_TABLE_CONTRACTS, audit.validate_audit_schema),
    ],
)
def test_qmt_runtime_schema_validators_are_select_only(contracts, validator):
    connection = _Connection(contracts)
    result = validator(_Engine(connection))

    assert result["physical_contract_verified"] is True
    assert result["runtime_ddl_required"] is False
    assert len(result["contract_hash"]) == 64
    assert connection.statements
    assert all(statement.upper().startswith("SELECT ") for statement in connection.statements)


@pytest.mark.parametrize(
    ("contracts", "validator"),
    [
        (catalog.CATALOG_TABLE_CONTRACTS, catalog.validate_catalog_schema),
        (audit.AUDIT_TABLE_CONTRACTS, audit.validate_audit_schema),
    ],
)
def test_qmt_runtime_schema_validators_fail_when_table_is_missing(contracts, validator):
    snapshot = dict(contracts)
    snapshot.pop(next(iter(snapshot)))

    with pytest.raises(RuntimeError, match="physical table inventory differs"):
        validator(_Engine(_Connection(snapshot)))


def test_catalog_runtime_validator_rejects_column_type_and_index_drift():
    contracts = copy.deepcopy(catalog.CATALOG_TABLE_CONTRACTS)
    registry = contracts["qmt_api_registry"]
    columns = list(registry.columns)
    name, original = columns[1]
    columns[1] = (name, type(original)("varchar(64)", False, character=True))
    contracts["qmt_api_registry"] = type(registry)(
        ddl=registry.ddl,
        columns=tuple(columns),
        indexes=registry.indexes,
        engine=registry.engine,
        collation=registry.collation,
    )
    engine = _Engine(_Connection(contracts))

    with pytest.raises(RuntimeError, match="physical column differs"):
        catalog.validate_catalog_schema(engine)

    contracts = copy.deepcopy(catalog.CATALOG_TABLE_CONTRACTS)
    registry = contracts["qmt_api_registry"]
    indexes = dict(registry.indexes)
    indexes["idx_unapproved"] = type(next(iter(indexes.values())))(
        ("provider",), False
    )
    contracts["qmt_api_registry"] = type(registry)(
        ddl=registry.ddl,
        columns=registry.columns,
        indexes=indexes,
        engine=registry.engine,
        collation=registry.collation,
    )
    with pytest.raises(RuntimeError, match="physical index inventory differs"):
        catalog.validate_catalog_schema(_Engine(_Connection(contracts)))


def test_catalog_seed_validator_binds_active_identity_and_is_select_only():
    connection = _Connection(
        catalog.CATALOG_TABLE_CONTRACTS,
        seed_rows=_seed_rows(),
    )
    result = catalog.validate_catalog_registry_seed(_Engine(connection))

    assert result["seed_identity_verified"] is True
    assert result["active_registry_rows"] == len(catalog.api_definitions())
    assert len(result["seed_contract_hash"]) == 64
    assert all(statement.upper().startswith("SELECT ") for statement in connection.statements)

    tampered = _seed_rows()
    tampered[0]["target_table"] = "forged_target"
    with pytest.raises(RuntimeError, match="seed payload differs"):
        catalog.validate_catalog_registry_seed(
            _Engine(_Connection(catalog.CATALOG_TABLE_CONTRACTS, seed_rows=tampered))
        )


def test_catalog_seed_validator_allows_only_inactive_historical_extras():
    historical = {
        **_seed_rows()[0],
        "capability_key": "native:retired_api:-",
        "api_name": "retired_api",
        "enabled": 0,
    }
    rows = _seed_rows() + [historical]
    result = catalog.validate_catalog_registry_seed(
        _Engine(_Connection(catalog.CATALOG_TABLE_CONTRACTS, seed_rows=rows))
    )
    assert result["inactive_registry_rows"] == 1

    historical["enabled"] = 1
    with pytest.raises(RuntimeError, match="active seed identity differs"):
        catalog.validate_catalog_registry_seed(
            _Engine(_Connection(catalog.CATALOG_TABLE_CONTRACTS, seed_rows=rows))
        )


def test_privileged_catalog_and_audit_migrations_are_the_only_ddl_paths():
    catalog_connection = _Connection(catalog.CATALOG_TABLE_CONTRACTS)
    catalog.privileged_migrate_catalog_schema(_Engine(catalog_connection))
    audit_connection = _Connection(audit.AUDIT_TABLE_CONTRACTS)
    audit.privileged_migrate_audit_schema(_Engine(audit_connection))

    assert sum(sql.upper().startswith("CREATE TABLE") for sql in catalog_connection.statements) == 2
    assert sum(sql.upper().startswith("CREATE TABLE") for sql in audit_connection.statements) == 5
    assert not any(
        sql.upper().startswith("ALTER TABLE")
        for sql in (*catalog_connection.statements, *audit_connection.statements)
    )
    assert all("COLLATE=UTF8MB4_UNICODE_CI" in sql.upper() for sql in catalog.CATALOG_TABLE_DDLS.values())
    assert all("COLLATE=UTF8MB4_UNICODE_CI" in sql.upper() for sql in audit.AUDIT_TABLE_DDLS)


@pytest.mark.parametrize(
    "contracts",
    [catalog.CATALOG_TABLE_CONTRACTS, audit.AUDIT_TABLE_CONTRACTS],
)
def test_privileged_qmt_migration_normalizes_only_exact_legacy_collation(
    contracts,
    monkeypatch,
):
    connection = _Connection(
        contracts,
        collations={name: control_schema.LEGACY_COLLATION for name in contracts},
    )
    calls = _stub_recovery_evidence(monkeypatch)

    result = control_schema.privileged_migrate_frozen_tables(
        _Engine(connection),
        contracts,
        context="QMT legacy control schema",
    )

    assert result["physical_contract_verified"] is True
    assert result["normalized_legacy_table_count"] == len(contracts)
    assert result["normalized_legacy_table_names"] == sorted(contracts)
    assert calls["ensure"] == 1
    assert len(calls["persisted"]) == 2
    assert {record["action"] for record in calls["persisted"][0]} == {
        control_schema.PLAN_ACTION
    }
    assert {record["action"] for record in calls["persisted"][1]} == {
        control_schema.VERIFIED_ACTION
    }
    assert len(calls["persisted"][0]) == len(contracts)
    assert len(calls["persisted"][1]) == len(contracts)
    for table_name, contract in contracts.items():
        expected_columns = tuple(name for name, _column in contract.columns)
        expected_order = contract.indexes["PRIMARY"].columns
        assert calls["fingerprints"].count(
            (table_name, expected_order, expected_columns)
        ) == 2
        assert connection.collations[table_name] == control_schema.EXPECTED_COLLATION
    assert sum(
        sql.upper().startswith("ALTER TABLE") for sql in connection.statements
    ) == len(contracts)


def test_qmt_fingerprint_order_uses_primary_or_all_contract_columns():
    original = catalog.CATALOG_TABLE_CONTRACTS["qmt_api_registry"]
    composite_primary = FrozenTable(
        ddl=original.ddl,
        columns=original.columns,
        indexes={
            **original.indexes,
            "PRIMARY": FrozenIndex(("provider", "capability_key"), True),
        },
        engine=original.engine,
        collation=original.collation,
    )
    columns = tuple(name for name, _column in original.columns)

    assert control_schema._fingerprint_shape(composite_primary) == (
        columns,
        ("provider", "capability_key"),
    )

    without_primary = FrozenTable(
        ddl=original.ddl,
        columns=original.columns,
        indexes={
            name: index
            for name, index in original.indexes.items()
            if name != "PRIMARY"
        },
        engine=original.engine,
        collation=original.collation,
    )
    assert control_schema._fingerprint_shape(without_primary) == (
        columns,
        columns,
    )


def test_pending_qmt_plan_verification_uses_manifest_primary_order(monkeypatch):
    observed = []
    expected = {"row_count": 4, "content_sha256": "c" * 64}

    def fingerprint(_connection, table_name, *, order_by, columns):
        observed.append((table_name, tuple(order_by), tuple(columns)))
        return expected

    monkeypatch.setattr(
        control_schema,
        "table_content_fingerprint",
        fingerprint,
    )
    result = control_schema._verify_storage_plan_content(
        object(),
        table_name="qmt_api_registry",
        payload={
            "before_fingerprint": expected,
            "order_by": ["provider", "capability_key"],
            "fingerprint_columns": [
                "provider",
                "capability_key",
                "api_name",
            ],
        },
        context="QMT pending control schema",
    )

    assert result == expected
    assert observed == [(
        "qmt_api_registry",
        ("provider", "capability_key"),
        ("provider", "capability_key", "api_name"),
    )]


def test_privileged_qmt_migration_resumes_durable_plan_without_id_order(
    monkeypatch,
):
    original = catalog.CATALOG_TABLE_CONTRACTS["qmt_api_registry"]
    contract = FrozenTable(
        ddl=original.ddl,
        columns=original.columns,
        indexes={
            **original.indexes,
            "PRIMARY": FrozenIndex(("provider", "capability_key"), True),
        },
        engine=original.engine,
        collation=original.collation,
    )
    contracts = {"qmt_api_registry": contract}
    connection = _Connection(
        contracts,
        evidence_table_present=True,
    )
    calls = _stub_recovery_evidence(monkeypatch)
    before = {"row_count": 3, "content_sha256": "a" * 64}
    payload = control_schema._storage_plan_payload(
        table_name="qmt_api_registry",
        contract=contract,
        context="QMT resumable control schema",
        before_fingerprint=before,
    )
    plan_hash = control_schema.plan_sha256(
        recovery_version=control_schema.LEGACY_STORAGE_RECOVERY_VERSION,
        payload=payload,
    )
    record = control_schema.make_evidence_record(
        recovery_version=control_schema.LEGACY_STORAGE_RECOVERY_VERSION,
        source_table="qmt_api_registry",
        source_row_id=0,
        action=control_schema.PLAN_ACTION,
        business_key={"table": "qmt_api_registry"},
        source_row=payload,
        plan_payload=payload,
        plan_hash=plan_hash,
    )
    pending = {
        "record": record,
        "business_key": {"table": "qmt_api_registry"},
        "plan_sha256": plan_hash,
        "source_row": payload,
        "plan_payload": payload,
    }
    monkeypatch.setattr(
        control_schema,
        "load_pending_physical_rewrite_plan",
        lambda *_args, **_kwargs: pending,
    )

    result = control_schema.privileged_migrate_frozen_tables(
        _Engine(connection),
        contracts,
        context="QMT resumable control schema",
    )

    assert result["normalized_legacy_table_count"] == 0
    assert result["resumed_legacy_table_names"] == ["qmt_api_registry"]
    assert not any(
        sql.upper().startswith("ALTER TABLE") for sql in connection.statements
    )
    columns = tuple(name for name, _column in contract.columns)
    assert calls["fingerprints"] == [
        ("qmt_api_registry", ("provider", "capability_key"), columns),
        ("qmt_api_registry", ("provider", "capability_key"), columns),
    ]
    assert calls["persisted"][0] == []
    assert calls["persisted"][1][0]["action"] == control_schema.VERIFIED_ACTION


def test_privileged_qmt_migration_rejects_non_collation_drift_before_ddl(
    monkeypatch,
):
    contracts = {
        "qmt_api_registry": catalog.CATALOG_TABLE_CONTRACTS["qmt_api_registry"]
    }
    connection = _Connection(
        contracts,
        collations={"qmt_api_registry": control_schema.LEGACY_COLLATION},
        engines={"qmt_api_registry": "MyISAM"},
    )
    monkeypatch.setattr(
        control_schema,
        "ensure_evidence_table",
        lambda _connection: pytest.fail("drift must fail before evidence DDL"),
    )

    with pytest.raises(RuntimeError, match="unsupported physical drift"):
        control_schema.privileged_migrate_frozen_tables(
            _Engine(connection),
            contracts,
            context="QMT drifted control schema",
        )

    assert not any(
        sql.upper().startswith(("CREATE TABLE", "ALTER TABLE"))
        for sql in connection.statements
    )


def test_privileged_qmt_migration_fails_when_content_fingerprint_changes(
    monkeypatch,
):
    contracts = {
        "qmt_api_registry": catalog.CATALOG_TABLE_CONTRACTS["qmt_api_registry"]
    }
    connection = _Connection(
        contracts,
        collations={"qmt_api_registry": control_schema.LEGACY_COLLATION},
    )
    calls = _stub_recovery_evidence(
        monkeypatch,
        fingerprints=[
            {"row_count": 3, "content_sha256": "a" * 64},
            {"row_count": 3, "content_sha256": "b" * 64},
        ],
    )

    with pytest.raises(RuntimeError, match="content fingerprint changed"):
        control_schema.privileged_migrate_frozen_tables(
            _Engine(connection),
            contracts,
            context="QMT legacy control schema",
        )

    assert len(calls["persisted"]) == 1
    assert calls["persisted"][0][0]["action"] == control_schema.PLAN_ACTION


def test_privileged_catalog_seed_is_explicit_and_revalidates_identity():
    connection = _Connection(
        catalog.CATALOG_TABLE_CONTRACTS,
        seed_rows=_seed_rows(),
    )
    result = catalog.privileged_seed_catalog_registry(_Engine(connection))

    assert result["privileged_seed"] is True
    assert result["seed_identity_verified"] is True
    assert result["seeded_registry_rows"] == len(catalog.api_definitions())
    assert any(sql.upper().startswith("UPDATE QMT_API_REGISTRY") for sql in connection.statements)
    assert any(sql.upper().startswith("INSERT INTO QMT_API_REGISTRY") for sql in connection.statements)


def test_runtime_catalog_refresh_validates_before_any_probe_or_dml(monkeypatch, capsys):
    calls = []
    engine = object()
    monkeypatch.setattr(catalog_refresh, "create_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(catalog_refresh, "get_mysql_url", lambda **kwargs: "mysql://unused")
    monkeypatch.setattr(
        catalog_refresh,
        "validate_catalog_schema",
        lambda value: calls.append(("schema", value)) or {"physical_contract_verified": True},
    )
    monkeypatch.setattr(
        catalog_refresh,
        "validate_catalog_registry_seed",
        lambda value: calls.append(("seed", value)) or {"active_registry_rows": 3},
    )
    monkeypatch.setattr(
        catalog_refresh,
        "capabilities",
        lambda **kwargs: calls.append(("capabilities", engine)) or {},
    )
    monkeypatch.setattr(
        catalog_refresh,
        "core_probe",
        lambda **kwargs: calls.append(("core_probe", engine)) or {"status": "ok"},
    )
    monkeypatch.setattr(
        catalog_refresh,
        "save_capabilities",
        lambda value, *_args: calls.append(("save", value)) or 2,
    )
    monkeypatch.setattr(
        catalog_refresh,
        "complete_capability_ledger",
        lambda value: calls.append(("complete", value)) or 1,
    )

    assert catalog_refresh.main() == 0
    assert [name for name, _ in calls] == [
        "schema", "seed", "capabilities", "core_probe", "save", "complete"
    ]
    assert '"registry_rows": 3' in capsys.readouterr().out


def test_runtime_catalog_refresh_fails_before_probe_when_seed_is_invalid(monkeypatch):
    engine = object()
    probe_called = False
    monkeypatch.setattr(catalog_refresh, "create_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(catalog_refresh, "get_mysql_url", lambda **kwargs: "mysql://unused")
    monkeypatch.setattr(catalog_refresh, "validate_catalog_schema", lambda value: {})

    def reject_seed(_engine):
        raise RuntimeError("seed drift")

    def probe(**_kwargs):
        nonlocal probe_called
        probe_called = True
        return {}

    monkeypatch.setattr(catalog_refresh, "validate_catalog_registry_seed", reject_seed)
    monkeypatch.setattr(catalog_refresh, "capabilities", probe)

    with pytest.raises(RuntimeError, match="seed drift"):
        catalog_refresh.main()
    assert probe_called is False
