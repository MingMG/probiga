from __future__ import annotations

import copy

import pytest

from integrations.qmt import audit, catalog
from integrations.qmt._control_schema import FrozenTable
from tools import setup_guojin_qmt_catalog as catalog_refresh


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _Connection:
    def __init__(self, contracts: dict[str, FrozenTable], *, seed_rows=None):
        self.contracts = contracts
        self.seed_rows = list(seed_rows or [])
        self.statements: list[str] = []

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        upper = sql.upper()
        if "INFORMATION_SCHEMA.TABLES" in upper:
            return _Rows([
                {
                    "TABLE_NAME": table_name,
                    "ENGINE": contract.engine,
                    "TABLE_COLLATION": contract.collation,
                }
                for table_name, contract in self.contracts.items()
            ])
        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            rows = []
            for table_name, contract in self.contracts.items():
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
                        "COLLATION_NAME": contract.collation if column.character else None,
                    })
            return _Rows(rows)
        if "INFORMATION_SCHEMA.STATISTICS" in upper:
            rows = []
            for table_name, contract in self.contracts.items():
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
    assert all("COLLATE=UTF8MB4_UNICODE_CI" in sql.upper() for sql in catalog.CATALOG_TABLE_DDLS.values())
    assert all("COLLATE=UTF8MB4_UNICODE_CI" in sql.upper() for sql in audit.AUDIT_TABLE_DDLS)


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
