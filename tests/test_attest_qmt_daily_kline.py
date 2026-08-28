import hashlib
import inspect
import json
import os
import re

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DatabaseError

from tools import attest_qmt_daily_kline as attester
from tools.attest_qmt_daily_kline import (
    ATTESTATION_PROTOCOL_VERSION,
    values_match,
)


def _bar(**overrides):
    row = {
        "open": 10.0,
        "close": 10.5,
        "high": 10.8,
        "low": 9.9,
        "volume": 123456.0,
        "amount": 1300000.0,
        "pre_close": 10.0,
        "pre_close_origin": "NATIVE_QMT",
    }
    row.update(overrides)
    return row


def test_v2_protocol_name_is_frozen():
    assert ATTESTATION_PROTOCOL_VERSION == "QMT_DAILY_UNADJUSTED_PRECLOSE_V2"


def test_attestation_source_window_requires_daily_k_type():
    source = inspect.getsource(attester.attest_range)

    assert "AND period='1d' AND k_type=1 AND adjust_type=0" in source


def test_attester_projects_reviewed_no_row_pairs_and_binds_manifest():
    source = inspect.getsource(attester.attest_range)
    proof_source = inspect.getsource(
        attester._build_attestation_no_row_contract
    )

    assert "_build_attestation_no_row_contract(" in source
    assert "build_no_row_exception_contract(" in proof_source
    assert "project_catalog_daily_codes(" in source
    assert "DELETE FROM `{expected_temp}`" in source
    assert "no_row_exception_contract=no_row_exception_contract" in source


def test_historical_unavailable_lookup_binds_provider_params():
    source = inspect.getsource(attester.attest_range)

    assert "LEFT JOIN `{source_temp}` AS history" in source
    assert "LEFT JOIN `{target_temp}` AS target" in source
    assert re.search(
        r"history\.provider=:provider.*?\"\"\"\), params\)"
        r"\.mappings\(\)\.all\(\)",
        source,
        re.DOTALL,
    )


def test_attester_no_row_proof_rejects_history_row_from_other_provider():
    class _Result:
        def mappings(self):
            return self

        def one(self):
            # This represents a row owned by any provider other than the
            # selected attestation provider.  The aggregate must still see it.
            return {"target_rows": 0, "history_rows": 1}

    class _Connection:
        def __init__(self):
            self.sql = ""
            self.params = None

        def execute(self, statement, params):
            self.sql = str(statement)
            self.params = dict(params)
            return _Result()

    connection = _Connection()
    with pytest.raises(RuntimeError, match="already has daily rows"):
        attester._build_attestation_no_row_contract(
            connection,
            target_table="`probiga`.`sm_stock_kline`",
            source_table=(
                "`probiga_qmt_history`.`qmt_local_stock_kline`"
            ),
            catalog=object(),
            calendar=object(),
            start_date="2026-03-06",
            end_date="2026-08-27",
            exact_lifecycle_no_row_codes=("002231",),
            not_yet_listed_no_row_codes=(),
        )

    assert "provider" not in connection.sql.lower()
    assert connection.params == {
        "no_row_stock_code": "002231",
        "start_date": "2026-03-06",
        "end_date": "2026-08-27",
    }


@pytest.mark.parametrize(
    "statement",
    (
        "CREATE TEMPORARY TABLE tmp_contract (id BIGINT)",
        "DROP TEMPORARY TABLE IF EXISTS tmp_contract",
        "SELECT 1",
        "UPDATE qmt_kline_attestation_run SET status='FAILED'",
    ),
)
def test_schema_prepared_sql_guard_allows_only_session_local_ddl(statement):
    attester.assert_schema_prepared_statement_is_session_local(statement)


@pytest.mark.parametrize(
    "statement",
    (
        "CREATE TABLE permanent_table (id BIGINT)",
        "DROP TABLE permanent_table",
        "ALTER TABLE permanent_table ADD COLUMN value INT",
        "ALTER TABLE tmp_contract ADD PRIMARY KEY (id)",
        "CREATE TRIGGER unsafe BEFORE INSERT ON t FOR EACH ROW SET @x=1",
        "DROP TRIGGER unsafe",
    ),
)
def test_schema_prepared_sql_guard_rejects_all_non_temporary_ddl(statement):
    with pytest.raises(RuntimeError, match="CREATE/DROP TEMPORARY"):
        attester.assert_schema_prepared_statement_is_session_local(statement)


def test_schema_prepared_attester_contains_no_alter_ddl():
    source = inspect.getsource(attester.attest_range).upper()
    assert "ALTER TABLE" not in source


def test_attester_validates_qualified_history_before_run_ledger_write(
    monkeypatch,
):
    class FakeEngine:
        def __init__(self, url):
            self.url = make_url(url)
            self.business_writes = []

        def begin(self):
            self.business_writes.append("begin")
            raise AssertionError("business transaction started before source validation")

    target_engine = FakeEngine(
        "mysql+pymysql://runtime@127.0.0.1:13306/probiga"
    )
    history_engine = FakeEngine(
        "mysql+pymysql://runtime@localhost:13306/"
        "probiga_qmt_history"
    )
    monkeypatch.setattr(
        attester,
        "validate_attestation_schema",
        lambda *_args, **_kwargs: {"status": "ok"},
    )
    monkeypatch.setattr(
        attester,
        "get_local_history_engine",
        lambda: history_engine,
    )

    def blocked_source_schema(engine, *, database=None):
        assert engine is target_engine
        assert database == "probiga_qmt_history"
        raise RuntimeError("provenance schema missing")

    monkeypatch.setattr(
        attester,
        "validate_local_history_provenance_schema",
        blocked_source_schema,
    )

    with pytest.raises(RuntimeError, match="provenance schema missing"):
        attester.attest_range(
            target_engine,
            start_date="2026-03-02",
            end_date="2026-08-21",
            apply=True,
            schema_prepared=True,
        )

    assert target_engine.business_writes == []


def test_table_names_accepts_explicit_protected_local_history_engine(
    monkeypatch,
):
    class FakeEngine:
        def __init__(self, url):
            self.url = make_url(url)

    target_engine = FakeEngine("mysql+pymysql:///probiga")
    history_engine = FakeEngine("mysql+pymysql:///probiga_qmt_history")
    events = []
    monkeypatch.setattr(
        attester,
        "get_local_history_engine",
        lambda: (_ for _ in ()).throw(
            AssertionError("configured history engine must not be opened")
        ),
    )
    monkeypatch.setattr(
        attester,
        "validate_local_history_provenance_schema",
        lambda engine, *, database: events.append((engine, database)),
    )

    target_table, source_table = attester._table_names(
        target_engine,
        local_history_engine=history_engine,
    )

    assert target_table == "`probiga`.`sm_stock_kline`"
    assert source_table == "`probiga_qmt_history`.`qmt_local_stock_kline`"
    assert events == [(target_engine, "probiga_qmt_history")]


def test_main_windows_option_file_route_disposes_both_engines(
    monkeypatch,
    capsys,
):
    from tools import backfill_guojin_qmt_local_history as backfill_tool

    class FakeEngine:
        def __init__(self, name):
            self.name = name
            self.disposed = False

        def dispose(self):
            self.disposed = True

    primary = FakeEngine("primary")
    history = FakeEngine("history")
    monkeypatch.setattr(attester, "load_project_env", lambda: None)
    monkeypatch.setattr(
        attester,
        "create_batch_engine",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("configured production engine must not be opened")
        ),
    )
    monkeypatch.setattr(
        backfill_tool,
        "_windows_local_engines",
        lambda: (primary, history),
    )
    calls = []

    def fake_attest(engine, **kwargs):
        calls.append((engine, kwargs))
        return {"status": "COMPLETED"}

    monkeypatch.setattr(attester, "attest_range", fake_attest)
    monkeypatch.setattr(
        attester.sys,
        "argv",
        [
            "attest_qmt_daily_kline.py",
            "--start-date",
            "2026-03-16",
            "--end-date",
            "2026-03-27",
            "--provider",
            "gj_big_qmt_inner",
            "--apply",
            "--json",
            "--windows-local-option-file",
        ],
    )

    assert attester.main() == 0
    assert calls[0][0] is primary
    assert calls[0][1]["local_history_engine"] is history
    assert calls[0][1]["apply"] is True
    assert primary.disposed is True
    assert history.disposed is True
    assert json.loads(capsys.readouterr().out)["status"] == "COMPLETED"


def test_main_forwards_exact_reviewed_no_row_code_categories(
    monkeypatch,
    capsys,
):
    from tools import backfill_guojin_qmt_local_history as backfill_tool

    class FakeEngine:
        def dispose(self):
            pass

    primary = FakeEngine()
    history = FakeEngine()
    monkeypatch.setattr(attester, "load_project_env", lambda: None)
    monkeypatch.setattr(
        backfill_tool, "_windows_local_engines", lambda: (primary, history),
    )
    calls = []
    monkeypatch.setattr(
        attester,
        "attest_range",
        lambda engine, **kwargs: calls.append((engine, kwargs))
        or {"status": "COMPLETED"},
    )
    monkeypatch.setattr(attester.sys, "argv", [
        "attest_qmt_daily_kline.py",
        "--start-date", "2026-03-06",
        "--end-date", "2026-08-27",
        "--windows-local-option-file",
        "--exact-lifecycle-no-row-codes", "002231,603056",
        "--not-yet-listed-no-row-codes", "301688,301689,301697,301699",
        "--historical-unavailable-pair-count", "1392",
        "--apply", "--json",
    ])

    assert attester.main() == 0
    assert calls[0][1]["exact_lifecycle_no_row_codes"] == (
        "002231", "603056",
    )
    assert calls[0][1]["not_yet_listed_no_row_codes"] == (
        "301688", "301689", "301697", "301699",
    )
    assert calls[0][1]["historical_unavailable_pair_count"] == 1392
    assert json.loads(capsys.readouterr().out)["status"] == "COMPLETED"


def test_native_pre_close_can_replace_legacy_target_value():
    target = _bar(pre_close=9.5, pre_close_origin="UNVERIFIED_LEGACY")
    source = _bar(pre_close=10.0)
    assert values_match(target, source) is True


def test_missing_or_derived_pre_close_never_matches():
    target = _bar()
    assert values_match(
        target,
        _bar(pre_close=None, pre_close_origin="MISSING_NATIVE_QMT"),
    ) is False
    assert values_match(
        target,
        _bar(pre_close=10.0, pre_close_origin="UNVERIFIED_LEGACY"),
    ) is False


def test_market_value_mismatch_never_receives_attestation():
    assert values_match(_bar(close=11.0), _bar(close=10.5)) is False


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("open", None),
        ("close", 0),
        ("high", -1),
        ("low", 0),
        ("volume", -1),
        ("amount", -1),
    ],
)
@pytest.mark.parametrize("side", ["target", "source"])
def test_null_zero_or_negative_market_values_never_match(
    field,
    value,
    side,
):
    target = _bar()
    source = _bar()
    (target if side == "target" else source)[field] = value
    assert values_match(target, source) is False


def test_attestation_runtime_is_ddl_free_but_exports_broker_trigger_contracts():
    source = inspect.getsource(attester.ensure_attestation_tables).upper()
    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "CREATE TRIGGER" not in source
    assert "DROP TRIGGER" not in source
    assert "INFORMATION_SCHEMA.TRIGGERS" not in source
    assert set(attester.ATTESTATION_TRIGGER_STATEMENTS) == set(
        attester._ATTESTATION_TRIGGER_CONTRACTS
    )
    assert len(attester.ATTESTATION_TRIGGER_STATEMENTS) == 6
    completed_update = attester.ATTESTATION_TRIGGER_STATEMENTS[
        "trg_qmt_kline_attestation_run_completed_bu"
    ]
    completed_delete = attester.ATTESTATION_TRIGGER_STATEMENTS[
        "trg_qmt_kline_attestation_run_completed_bd"
    ]
    assert "OLD.status = BINARY 'COMPLETED'" in completed_update
    assert "BEFORE UPDATE ON qmt_kline_attestation_run" in completed_update
    assert "OLD.status = BINARY 'COMPLETED'" in completed_delete
    assert "BEFORE DELETE ON qmt_kline_attestation_run" in completed_delete


class _SchemaResult:
    def __init__(self, rows):
        self._rows = [dict(row) for row in rows]

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _FrozenSchemaConnection:
    def __init__(
        self,
        *,
        nullable_attested_open=False,
        extra_trigger=False,
        update_trigger_body="",
        completed_runs=(),
        table_engine="InnoDB",
        table_collation=attester.QMT_ATTESTATION_COLLATION,
        column_collation=attester.QMT_ATTESTATION_COLLATION,
        omit_index="",
        migration_hash=None,
        legacy_marker_hash="",
    ):
        self.nullable_attested_open = nullable_attested_open
        self.extra_trigger = extra_trigger
        self.update_trigger_body = update_trigger_body
        self.completed_runs = list(completed_runs)
        self.table_engine = table_engine
        self.table_collation = table_collation
        self.column_collation = column_collation
        self.omit_index = omit_index
        self.migration_hash = (
            attester.TOLERANCE_MEDIUMTEXT_MIGRATION_HASH
            if migration_hash is None
            else migration_hash
        )
        self.legacy_marker_hash = legacy_marker_hash

    def execute(self, statement, params=None):
        sql = str(statement)
        if (
            "SELECT migration_hash FROM "
            "qmt_kline_attestation_schema_migration" in sql
        ):
            marker_hash = (
                self.legacy_marker_hash
                if (params or {}).get("migration_key")
                == attester.LEGACY_MANIFEST_GRANDFATHER_MIGRATION_KEY
                else self.migration_hash
            )
            return _SchemaResult(
                []
                if not marker_hash
                else [{"migration_hash": marker_hash}]
            )
        if "information_schema.TABLES" in sql:
            return _SchemaResult(
                [
                    {
                        "table_name": table_name,
                        "engine": self.table_engine,
                        "table_collation": self.table_collation,
                    }
                    for table_name in attester.ATTESTATION_TABLE_NAMES
                ]
            )
        if "information_schema.COLUMNS" in sql:
            rows = []
            for table_name, columns in (
                attester._ATTESTATION_COLUMN_CONTRACTS.items()
            ):
                for ordinal, contract in enumerate(columns, 1):
                    nullable = contract[5]
                    if (
                        self.nullable_attested_open
                        and table_name == "qmt_kline_attestation_row"
                        and contract[0] == "attested_open"
                    ):
                        nullable = "YES"
                    rows.append(
                        {
                            "table_name": table_name,
                            "column_name": contract[0],
                            "ordinal_position": ordinal,
                            "data_type": contract[1],
                            "character_maximum_length": contract[2],
                            "numeric_precision": contract[3],
                            "numeric_scale": contract[4],
                            "is_nullable": nullable,
                            "column_default": contract[6],
                            "extra": contract[7],
                            "character_set_name": (
                                "utf8mb4"
                                if contract[1]
                                in {"char", "varchar", "text", "mediumtext"}
                                else None
                            ),
                            "collation_name": (
                                self.column_collation
                                if contract[1]
                                in {"char", "varchar", "text", "mediumtext"}
                                else None
                            ),
                        }
                    )
            return _SchemaResult(rows)
        if "information_schema.STATISTICS" in sql:
            return _SchemaResult(
                [
                    {
                        "table_name": table_name,
                        "index_name": index_name,
                        "non_unique": non_unique,
                        "seq_in_index": sequence,
                        "column_name": column_name,
                        "sub_part": None,
                        "index_type": "BTREE",
                    }
                    for table_name, indexes in (
                        attester._ATTESTATION_INDEX_CONTRACTS.items()
                    )
                    for index_name, (non_unique, columns) in indexes.items()
                    if index_name != self.omit_index
                    for sequence, column_name in enumerate(columns, 1)
                ]
            )
        if "information_schema.TRIGGERS" in sql:
            rows = []
            for trigger_name, contract in (
                attester._ATTESTATION_TRIGGER_CONTRACTS.items()
            ):
                timing, event, table_name, body = contract
                if (
                    self.update_trigger_body
                    and event == "UPDATE"
                ):
                    body = self.update_trigger_body
                rows.append(
                    {
                        "trigger_name": trigger_name,
                        "action_timing": timing,
                        "event_manipulation": event,
                        "event_object_table": table_name,
                        "action_orientation": "ROW",
                        "action_statement": body,
                    }
                )
            if self.extra_trigger:
                rows.append(
                    {
                        "trigger_name": "trg_qmt_unexpected",
                        "action_timing": "BEFORE",
                        "event_manipulation": "INSERT",
                        "event_object_table": "qmt_kline_attestation_row",
                        "action_orientation": "ROW",
                        "action_statement": (
                            "BEGIN SIGNAL SQLSTATE '45000'; END"
                        ),
                    }
                )
            return _SchemaResult(rows)
        if (
            "FROM qmt_kline_attestation_run" in sql
            and "WHERE status='COMPLETED'" in sql
        ):
            return _SchemaResult(self.completed_runs)
        raise AssertionError(sql)


def test_frozen_schema_validator_accepts_only_frozen_collation():
    detail = attester.validate_attestation_schema(
        _FrozenSchemaConnection()
    )
    assert detail["protocol_version"] == ATTESTATION_PROTOCOL_VERSION
    assert detail["table_count"] == 4
    assert detail["trigger_count"] == 0
    assert detail["database_triggers_required"] is False
    assert detail["errors"] == []


def test_legacy_collation_is_accepted_only_by_private_fenced_migration_gate():
    connection = _FrozenSchemaConnection(
        table_collation=attester.QMT_ATTESTATION_LEGACY_COLLATION,
        column_collation=attester.QMT_ATTESTATION_LEGACY_COLLATION,
    )

    with pytest.raises(attester.QmtAttestationSchemaError):
        attester.validate_attestation_schema(connection)
    detail = attester._validate_attestation_schema_connection(
        connection,
        allow_legacy_collation=True,
    )

    assert set(detail["table_collations"].values()) == {
        attester.QMT_ATTESTATION_LEGACY_COLLATION
    }


class _CollationMigrationConnection:
    def __init__(self):
        self.collations = {
            table: attester.QMT_ATTESTATION_LEGACY_COLLATION
            for table in attester.ATTESTATION_TABLE_NAMES
        }
        self.markers = {}
        self.statements = []

    def execute(self, statement, params=None):
        sql = str(statement)
        params = dict(params or {})
        self.statements.append(sql)
        if sql.startswith("SELECT migration_hash"):
            marker = self.markers.get(params["migration_key"])
            return _SchemaResult([] if marker is None else [{"migration_hash": marker}])
        if sql.startswith("INSERT INTO qmt_kline_attestation_schema_migration"):
            self.markers[params["migration_key"]] = params["migration_hash"]
            return _SchemaResult([])
        if sql.startswith("ALTER TABLE `"):
            table_name = sql.split("`", 2)[1]
            self.collations[table_name] = attester.QMT_ATTESTATION_COLLATION
            return _SchemaResult([])
        raise AssertionError(sql)


class _CollationMigrationEngine:
    def __init__(self):
        self.connection = _CollationMigrationConnection()

    def begin(self):
        connection = self.connection

        class _Context:
            def __enter__(self):
                return connection

            def __exit__(self, *_args):
                return False

        return _Context()

    def connect(self):
        return self.begin()


def test_fenced_collation_migration_is_resumable_and_never_uses_triggers(
    monkeypatch,
):
    engine = _CollationMigrationEngine()

    def validate(
        connection,
        *,
        allow_legacy_collation=False,
        require_current_manifests=True,
        **_kwargs,
    ):
        assert require_current_manifests is False
        if not allow_legacy_collation:
            assert set(connection.collations.values()) == {
                attester.QMT_ATTESTATION_COLLATION
            }
        return {"table_collations": dict(connection.collations)}

    monkeypatch.setattr(
        attester, "_validate_attestation_schema_connection", validate
    )
    proof_version = {"value": 1}
    monkeypatch.setattr(
        attester,
        "_attestation_table_row_proof",
        lambda _connection, table_name, **_kwargs: {
            "row_count": proof_version["value"],
            "row_sha256": hashlib.sha256(
                f"{table_name}:{proof_version['value']}".encode()
            ).hexdigest(),
        },
    )

    with pytest.raises(ValueError, match="writers_fenced=True"):
        attester.migrate_legacy_attestation_collation(
            engine, writers_fenced=False
        )
    first = attester.migrate_legacy_attestation_collation(
        engine, writers_fenced=True
    )
    # Attestation ledgers are append-only and may grow after the one-time DDL.
    # The durable migration marker therefore binds the DDL contract, while the
    # transient before/after row proof guards the conversion itself.
    proof_version["value"] = 2
    second = attester.migrate_legacy_attestation_collation(
        engine, writers_fenced=True
    )

    assert [item["action"] for item in first["operations"]] == [
        "converted"
    ] * 4
    assert [item["action"] for item in second["operations"]] == [
        "already_target"
    ] * 4
    assert len(engine.connection.markers) == 8
    assert sum(sql.startswith("ALTER TABLE `") for sql in engine.connection.statements) == 4
    assert not any("TRIGGER" in sql.upper() for sql in engine.connection.statements)


def test_fenced_collation_migration_never_blesses_failed_row_proof(
    monkeypatch,
):
    engine = _CollationMigrationEngine()

    def validate(
        connection,
        *,
        allow_legacy_collation=False,
        require_current_manifests=True,
        **_kwargs,
    ):
        assert require_current_manifests is False
        if not allow_legacy_collation:
            assert set(connection.collations.values()) == {
                attester.QMT_ATTESTATION_COLLATION
            }
        return {"table_collations": dict(connection.collations)}

    monkeypatch.setattr(
        attester, "_validate_attestation_schema_connection", validate
    )
    first_table = attester.ATTESTATION_TABLE_NAMES[0]
    proof_calls = {first_table: 0}

    def drifting_proof(_connection, table_name, **_kwargs):
        proof_calls[table_name] = proof_calls.get(table_name, 0) + 1
        version = 1 if proof_calls[table_name] <= 2 else 2
        return {
            "row_count": version,
            "row_sha256": hashlib.sha256(
                f"{table_name}:{version}".encode()
            ).hexdigest(),
        }

    monkeypatch.setattr(
        attester,
        "_attestation_table_row_proof",
        drifting_proof,
    )

    with pytest.raises(
        attester.QmtAttestationSchemaError,
        match="row proof changed during conversion",
    ):
        attester.migrate_legacy_attestation_collation(
            engine,
            writers_fenced=True,
        )

    pending_key, complete_key, _contract_hash = (
        attester._collation_marker_contract(first_table)
    )
    assert engine.connection.collations[first_table] == (
        attester.QMT_ATTESTATION_COLLATION
    )
    assert pending_key in engine.connection.markers
    assert complete_key not in engine.connection.markers
    with pytest.raises(
        attester.QmtAttestationSchemaError,
        match="pending row proof differs",
    ):
        attester.migrate_legacy_attestation_collation(
            engine,
            writers_fenced=True,
        )


def test_fenced_collation_migration_finalizes_matching_interrupted_ddl(
    monkeypatch,
):
    engine = _CollationMigrationEngine()

    def validate(
        connection,
        *,
        allow_legacy_collation=False,
        require_current_manifests=True,
        **_kwargs,
    ):
        assert require_current_manifests is False
        if not allow_legacy_collation:
            assert set(connection.collations.values()) == {
                attester.QMT_ATTESTATION_COLLATION
            }
        return {"table_collations": dict(connection.collations)}

    monkeypatch.setattr(
        attester, "_validate_attestation_schema_connection", validate
    )

    def stable_proof(_connection, table_name, **_kwargs):
        return {
            "row_count": 1,
            "row_sha256": hashlib.sha256(table_name.encode()).hexdigest(),
        }

    monkeypatch.setattr(
        attester,
        "_attestation_table_row_proof",
        stable_proof,
    )
    first_table = attester.ATTESTATION_TABLE_NAMES[0]
    pending_key, complete_key, contract_hash = (
        attester._collation_marker_contract(first_table)
    )
    proof = stable_proof(engine.connection, first_table)
    engine.connection.markers[pending_key] = attester._collation_pending_hash(
        first_table,
        contract_hash,
        proof,
    )
    engine.connection.collations[first_table] = (
        attester.QMT_ATTESTATION_COLLATION
    )

    result = attester.migrate_legacy_attestation_collation(
        engine,
        writers_fenced=True,
    )

    assert result["operations"][0]["action"] == "finalized_after_interrupt"
    assert engine.connection.markers[complete_key] == (
        attester._collation_complete_hash(
            first_table,
            contract_hash,
            engine.connection.markers[pending_key],
        )
    )
    assert set(engine.connection.collations.values()) == {
        attester.QMT_ATTESTATION_COLLATION
    }


@pytest.mark.parametrize(
    ("connection", "expected_error"),
    [
        (
            _FrozenSchemaConnection(nullable_attested_open=True),
            "qmt_kline_attestation_row column contract differs",
        ),
        (
            _FrozenSchemaConnection(table_engine="MyISAM"),
            "engine is not InnoDB",
        ),
        (
            _FrozenSchemaConnection(table_collation="latin1_swedish_ci"),
            "table collation differs",
        ),
        (
            _FrozenSchemaConnection(table_collation="utf8mb4_0900_ai_ci"),
            "table collation differs",
        ),
        (
            _FrozenSchemaConnection(column_collation="utf8mb4_general_ci"),
            "character set/collation differs",
        ),
        (
            _FrozenSchemaConnection(
                omit_index="idx_qmt_kline_attestation_row_run"
            ),
            "qmt_kline_attestation_row index contract differs",
        ),
    ],
)
def test_frozen_schema_validator_fails_closed_on_drift(
    connection,
    expected_error,
):
    with pytest.raises(attester.QmtAttestationSchemaError) as captured:
        attester.validate_attestation_schema(connection)
    assert any(
        expected_error in error
        for error in captured.value.detail["errors"]
    )


@pytest.mark.parametrize(
    "connection",
    (
        _FrozenSchemaConnection(extra_trigger=True),
        _FrozenSchemaConnection(update_trigger_body="BEGIN SET @unsafe=1; END"),
    ),
)
def test_schema_validator_does_not_require_or_inventory_existing_triggers(
    connection,
):
    detail = attester.validate_attestation_schema(connection)

    assert detail["trigger_count"] == 0
    assert detail["trigger_names"] == []
    assert detail["database_triggers_required"] is False


def test_frozen_schema_validator_requires_hashed_mediumtext_migration():
    for migration_hash in ("", "f" * 64):
        with pytest.raises(attester.QmtAttestationSchemaError) as captured:
            attester.validate_attestation_schema(
                _FrozenSchemaConnection(migration_hash=migration_hash)
            )
        assert any(
            "migration marker/hash differs" in error
            for error in captured.value.detail["errors"]
        )


class _MigrationConnection:
    def __init__(self, *, data_type="text", marker_hash=""):
        self.data_type = data_type
        self.marker_hash = marker_hash
        self.statements = []

    def execute(self, statement, params=None):
        sql = str(statement)
        params = dict(params or {})
        self.statements.append((sql, params))
        if "SELECT migration_hash" in sql:
            return _SchemaResult(
                []
                if not self.marker_hash
                else [{"migration_hash": self.marker_hash}]
            )
        if "information_schema.COLUMNS" in sql:
            return _SchemaResult(
                [{
                    "data_type": self.data_type,
                    "character_maximum_length": (
                        65535 if self.data_type == "text" else 16777215
                    ),
                    "is_nullable": "NO",
                    "character_set_name": "utf8mb4",
                    "collation_name": attester.QMT_ATTESTATION_COLLATION,
                    "column_default": None,
                    "extra": "",
                }]
            )
        if "ALTER TABLE qmt_kline_attestation_run" in sql:
            self.data_type = "mediumtext"
            return _SchemaResult([])
        if "INSERT INTO qmt_kline_attestation_schema_migration" in sql:
            self.marker_hash = params["migration_hash"]
            return _SchemaResult([])
        raise AssertionError(sql)


def test_exact_legacy_text_schema_is_upgraded_once_and_hash_registered():
    connection = _MigrationConnection()

    attester._privileged_migrate_tolerance_json_mediumtext(connection)
    attester._privileged_migrate_tolerance_json_mediumtext(connection)

    assert connection.data_type == "mediumtext"
    assert connection.marker_hash == (
        attester.TOLERANCE_MEDIUMTEXT_MIGRATION_HASH
    )
    assert sum(
        "ALTER TABLE qmt_kline_attestation_run" in sql
        for sql, _params in connection.statements
    ) == 1


def test_fresh_mediumtext_schema_registers_without_alter():
    connection = _MigrationConnection(data_type="mediumtext")

    attester._privileged_migrate_tolerance_json_mediumtext(connection)

    assert connection.marker_hash == (
        attester.TOLERANCE_MEDIUMTEXT_MIGRATION_HASH
    )
    assert not any(
        "ALTER TABLE" in sql for sql, _params in connection.statements
    )


@pytest.mark.parametrize(
    ("data_type", "marker_hash"),
    [("longtext", ""), ("text", "0" * 64)],
)
def test_mediumtext_migration_fails_closed_on_drift(
    data_type,
    marker_hash,
):
    connection = _MigrationConnection(
        data_type=data_type,
        marker_hash=marker_hash,
    )

    with pytest.raises(attester.QmtAttestationSchemaError):
        attester._privileged_migrate_tolerance_json_mediumtext(connection)

    assert not any(
        "ALTER TABLE" in sql for sql, _params in connection.statements
    )


def test_v2_attested_market_values_are_frozen_not_null():
    source = "\n".join(attester.attestation_table_ddl_statements()).upper()
    for column in (
        "ATTESTED_OPEN",
        "ATTESTED_CLOSE",
        "ATTESTED_HIGH",
        "ATTESTED_LOW",
        "ATTESTED_VOLUME",
        "ATTESTED_AMOUNT",
    ):
        assert re.search(
            rf"{column}\s+DECIMAL\(\d+,\d+\)\s+NOT NULL",
            source,
        )


def test_apply_is_all_or_nothing_and_exactly_read_back():
    source = inspect.getsource(attester.attest_range)
    normalized = " ".join(source.split())
    assert "if apply and universe_complete:" in source
    assert "elif universe_complete:" in source
    assert "status = \"COMPLETED\" if apply" in source
    for assignment in (
        "t.`open`=q.qmt_open",
        "t.`close`=q.qmt_close",
        "t.`high`=q.qmt_high",
        "t.`low`=q.qmt_low",
        "t.volume=q.qmt_volume",
        "t.amount=q.qmt_amount",
    ):
        assert assignment in source
    assert "exact_readback_rows" in source
    assert "exact_readback_rows != matched_rows" in source
    assert "source_only_rows == 0" in normalized


def test_full_window_attestation_bounds_database_statements_to_ten_sessions():
    source = inspect.getsource(attester.attest_range)

    assert attester.ATTESTATION_SESSION_CHUNK_SIZE == 10
    assert "load_trade_calendar_receipt" in source
    assert "calendar_temp" in source
    assert "catalog_sessions" in source
    assert source.count("ATTESTATION_SESSION_CHUNK_SIZE") >= 2
    assert source.count("q.trade_date BETWEEN :chunk_start_date") >= 3
    assert "t.trade_date BETWEEN :chunk_start_date" in source
    assert "exact_readback_rows = already_attested_rows" in source
    assert "AND NOT COALESCE(q.provenance_already, 0)" in source


def test_attestation_uses_exact_a_share_predicate_for_all_market_sets():
    source = inspect.getsource(attester.attest_range)

    assert 'a_share_stock_code_sql("stock_code")' in source
    assert 'a_share_stock_code_sql("raw.stock_code")' in source
    assert source.count("AND {unqualified_a_share}") == 2
    assert "AND {raw_a_share}" in source
    assert "^(0|3|4|6|8|9)" not in source


def test_daily_universe_manifest_hash_and_parser_are_frozen():
    codes = ["600000", "000001", "600000"]
    contract = attester.expected_stock_set_contract("2026-08-21", codes)
    assert contract == {
        "stock_count": 2,
        "stock_set_hash": attester.canonical_digest(
            {
                "schema": attester.EXPECTED_STOCK_SET_SCHEMA,
                "trade_date": "2026-08-21",
                "stock_codes": ["000001", "600000"],
            }
        ),
    }
    tolerance_json = attester.build_qmt_v2_manifest(
        {"2026-08-21": contract}
    )
    assert attester.validated_universe_manifest(
        tolerance_json,
        start_date="2026-08-21",
        end_date="2026-08-21",
    ) == {"2026-08-21": contract}

    tampered = {
        **tolerance_json,
        "daily_universe": {
            "2026-08-21": {**contract, "unexpected": True}
        },
    }
    with pytest.raises(ValueError, match="entry fields differ"):
        attester.validated_universe_manifest(
            tampered,
            start_date="2026-08-21",
            end_date="2026-08-21",
        )


def test_completed_run_requires_parseable_universe_manifest():
    connection = _FrozenSchemaConnection(
        completed_runs=[
            {
                "run_id": "bad-completed-run",
                "start_date": "2026-08-21",
                "end_date": "2026-08-21",
                "tolerance_json": {
                    "attestation_protocol": ATTESTATION_PROTOCOL_VERSION,
                },
            }
        ]
    )
    with pytest.raises(attester.QmtAttestationSchemaError) as captured:
        attester.validate_attestation_schema(connection)
    assert any(
        "completed run universe manifest invalid" in error
        for error in captured.value.detail["errors"]
    )


def _legacy_completed_row(**changes):
    row = {
        "run_id": "legacy-run-1",
        "provider": attester.PROVIDER_ID,
        "start_date": "2026-07-01",
        "end_date": "2026-07-24",
        "target_rows": 93519,
        "qmt_rows": 94000,
        "matched_rows": 93519,
        "missing_qmt_rows": 0,
        "mismatched_rows": 0,
        "already_attested_rows": 0,
        "updated_rows": 93519,
        "tolerance_json": attester.LEGACY_TOLERANCE_JSON,
    }
    row.update(changes)
    return row


def test_exact_legacy_completed_run_is_only_relaxed_until_marker_binds_it():
    row = _legacy_completed_row()
    plan = attester.legacy_completed_run_binding_plan([row])
    legacy_expectation = {
        "expected_legacy_run_count": 1,
        "expected_legacy_plan_hash": plan["plan_hash"],
    }
    relaxed = attester.validate_attestation_schema(
        _FrozenSchemaConnection(completed_runs=[row]),
        require_current_manifests=False,
        **legacy_expectation,
    )
    assert relaxed["legacy_ineligible_run_count"] == 1
    assert relaxed["legacy_binding_pending"] is True
    assert relaxed["completed_current_manifest_run_count"] == 0
    assert relaxed["completed_manifest_entry_count"] == 0

    with pytest.raises(attester.QmtAttestationSchemaError, match="binding marker"):
        attester.validate_attestation_schema(
            _FrozenSchemaConnection(completed_runs=[row]),
            **legacy_expectation,
        )

    strict = attester.validate_attestation_schema(
        _FrozenSchemaConnection(
            completed_runs=[row],
            legacy_marker_hash=plan["plan_hash"],
        ),
        **legacy_expectation,
    )
    assert strict["legacy_binding_marker_verified"] is True
    assert strict["legacy_binding_plan_hash"] == plan["plan_hash"]
    assert strict["completed_manifest_entry_count"] == 0


def test_production_legacy_grandfather_contract_is_frozen():
    assert attester.EXPECTED_LEGACY_MANIFEST_GRANDFATHER_RUN_COUNT == 11
    assert attester.EXPECTED_LEGACY_MANIFEST_GRANDFATHER_PLAN_HASH == (
        "fc8328550615413445edf1055b3b88d70fa8b37e45ee331f682cf8f654779b54"
    )
    plan = attester.legacy_completed_run_binding_plan([
        _legacy_completed_row()
    ])

    with pytest.raises(ValueError, match="release contract differs"):
        attester.validate_legacy_completed_run_release_contract(
            plan,
            expected_run_count=(
                attester.EXPECTED_LEGACY_MANIFEST_GRANDFATHER_RUN_COUNT
            ),
            expected_plan_hash=(
                attester.EXPECTED_LEGACY_MANIFEST_GRANDFATHER_PLAN_HASH
            ),
        )


@pytest.mark.parametrize(
    "changes",
    (
        {"tolerance_json": "{"},
        {"tolerance_json": attester.LEGACY_TOLERANCE_JSON + " "},
        {"tolerance_json": json.dumps({
            "amount_relative": 0.001,
            "price_absolute": 0.0001,
            "volume_absolute": 100.0,
            "volume_relative": 0.0001,
            "unexpected": True,
        }, sort_keys=True)},
        {"matched_rows": 93518},
        {"qmt_rows": 93518},
    ),
)
def test_relaxed_legacy_gate_still_rejects_json_hash_fields_and_counters(
    changes,
):
    with pytest.raises(attester.QmtAttestationSchemaError):
        attester.validate_attestation_schema(
            _FrozenSchemaConnection(
                completed_runs=[_legacy_completed_row(**changes)]
            ),
            require_current_manifests=False,
        )


def test_run_manifest_capacity_is_frozen_as_mediumtext():
    source = "\n".join(attester.attestation_table_ddl_statements()).upper()
    assert "TOLERANCE_JSON MEDIUMTEXT NOT NULL" in source
    contract = dict(
        (row[0], row)
        for row in attester._ATTESTATION_COLUMN_CONTRACTS[
            "qmt_kline_attestation_run"
        ]
    )["tolerance_json"]
    assert contract[1:3] == ("mediumtext", 16777215)


def test_completed_status_persists_daily_manifest_in_same_transaction():
    source = inspect.getsource(attester.attest_range)
    assert "daily_universe =" in source
    assert '"universe_manifest_schema": run_tolerances[' in source
    assert "manifest_complete" in source
    assert "and manifest_complete" in source
    assert "tolerance_json=:tolerance_json" in source


@pytest.mark.skipif(
    not os.environ.get("PROBIGA_MYSQL57_QMT_TEST_URL"),
    reason="PROBIGA_MYSQL57_QMT_TEST_URL is not configured",
)
def test_legacy_text_upgrade_and_frozen_rows_on_mysql57():
    engine = create_engine(os.environ["PROBIGA_MYSQL57_QMT_TEST_URL"])
    assert "test" in str(engine.url.database or "").lower()
    tables = (
        "qmt_kline_attestation_row",
        "qmt_kline_attestation_mismatch",
        "qmt_kline_attestation_run",
        "qmt_kline_attestation_schema_migration",
    )
    try:
        with engine.begin() as connection:
            assert str(connection.execute(text("SELECT @@version")).scalar()).startswith("5.7.")
            for table_name in tables:
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            connection.execute(text("""
                CREATE TABLE qmt_kline_attestation_run (
                    run_id VARCHAR(64) PRIMARY KEY,
                    provider VARCHAR(32) NOT NULL,
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    status VARCHAR(40) NOT NULL,
                    target_rows BIGINT NOT NULL DEFAULT 0,
                    qmt_rows BIGINT NOT NULL DEFAULT 0,
                    matched_rows BIGINT NOT NULL DEFAULT 0,
                    missing_qmt_rows BIGINT NOT NULL DEFAULT 0,
                    mismatched_rows BIGINT NOT NULL DEFAULT 0,
                    already_attested_rows BIGINT NOT NULL DEFAULT 0,
                    updated_rows BIGINT NOT NULL DEFAULT 0,
                    tolerance_json TEXT NOT NULL,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME NULL,
                    error_message TEXT NULL,
                    KEY idx_qmt_kline_attestation_range
                        (start_date, end_date, status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

        def trigger_executor(statement):
            with engine.begin() as connection:
                connection.execute(text(statement))

        attester.privileged_migrate_attestation_tables(
            engine,
            trigger_ddl_executor=trigger_executor,
        )
        first = attester.validate_attestation_schema(engine)
        attester.privileged_migrate_attestation_tables(
            engine,
            trigger_ddl_executor=trigger_executor,
        )
        second = attester.validate_attestation_schema(engine)
        assert first["errors"] == second["errors"] == []
        with engine.begin() as connection:
            column = connection.execute(text(
                "SELECT DATA_TYPE AS data_type, "
                "CHARACTER_MAXIMUM_LENGTH AS max_length, "
                "IS_NULLABLE AS is_nullable "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=DATABASE() "
                "AND TABLE_NAME='qmt_kline_attestation_run' "
                "AND COLUMN_NAME='tolerance_json'"
            )).mappings().first()
            assert dict(column) == {
                "data_type": "mediumtext",
                "max_length": 16777215,
                "is_nullable": "NO",
            }
            marker = connection.execute(text(
                "SELECT migration_hash FROM "
                "qmt_kline_attestation_schema_migration "
                "WHERE migration_key=:migration_key"
            ), {
                "migration_key": attester.TOLERANCE_MEDIUMTEXT_MIGRATION_KEY,
            }).scalar()
            assert marker == attester.TOLERANCE_MEDIUMTEXT_MIGRATION_HASH
            connection.execute(text("""
                INSERT INTO qmt_kline_attestation_row
                (attestation_id, run_id, target_id, qmt_id, trade_date,
                 stock_code, protocol_version, source_data_version,
                 source_pre_close_origin, source_pre_close, attested_open,
                 attested_close, attested_high, attested_low,
                 attested_volume, attested_amount, created_at)
                VALUES
                (:attestation_id, 'run', 1, 2, '2026-08-21', '000001',
                 :protocol, 'v1', 'NATIVE_QMT', 10, 10, 10.5, 10.8,
                 9.9, 100, 1000, NOW())
            """), {
                "attestation_id": "a" * 64,
                "protocol": ATTESTATION_PROTOCOL_VERSION,
            })
        for statement in (
            "UPDATE qmt_kline_attestation_row SET stock_code='000002'",
            "DELETE FROM qmt_kline_attestation_row",
        ):
            with pytest.raises(DatabaseError):
                with engine.begin() as connection:
                    connection.execute(text(statement))
    finally:
        with engine.begin() as connection:
            for table_name in tables:
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        engine.dispose()
