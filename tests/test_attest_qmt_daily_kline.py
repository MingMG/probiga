import inspect
import os
import re

import pytest
from sqlalchemy import create_engine, text
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


def test_append_only_triggers_are_never_dropped_during_attestation_setup():
    source = inspect.getsource(attester.ensure_attestation_tables).upper()
    assert "DROP TRIGGER" not in source
    assert "INFORMATION_SCHEMA.TRIGGERS" in source


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
        table_collation="utf8mb4_general_ci",
        omit_index="",
        migration_hash=None,
    ):
        self.nullable_attested_open = nullable_attested_open
        self.extra_trigger = extra_trigger
        self.update_trigger_body = update_trigger_body
        self.completed_runs = list(completed_runs)
        self.table_engine = table_engine
        self.table_collation = table_collation
        self.omit_index = omit_index
        self.migration_hash = (
            attester.TOLERANCE_MEDIUMTEXT_MIGRATION_HASH
            if migration_hash is None
            else migration_hash
        )

    def execute(self, statement, params=None):
        sql = str(statement)
        if "qmt_kline_attestation_schema_migration" in sql:
            return _SchemaResult(
                []
                if not self.migration_hash
                else [{"migration_hash": self.migration_hash}]
            )
        if "information_schema.TABLES" in sql:
            return _SchemaResult(
                [
                    {
                        "table_name": table_name,
                        "engine": self.table_engine,
                        # Both 5.7 and 8.x utf8mb4 collations are valid.
                        "table_collation": (
                            self.table_collation
                            if index == 0
                            else "utf8mb4_0900_ai_ci"
                        ),
                    }
                    for index, table_name in enumerate(
                        attester.ATTESTATION_TABLE_NAMES
                    )
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
                                "utf8mb4_bin"
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


def test_frozen_schema_validator_accepts_mysql57_and_mysql8_collations():
    detail = attester.validate_attestation_schema(
        _FrozenSchemaConnection()
    )
    assert detail["protocol_version"] == ATTESTATION_PROTOCOL_VERSION
    assert detail["table_count"] == 3
    assert detail["trigger_count"] == 2
    assert detail["errors"] == []


@pytest.mark.parametrize(
    ("connection", "expected_error"),
    [
        (
            _FrozenSchemaConnection(nullable_attested_open=True),
            "qmt_kline_attestation_row column contract differs",
        ),
        (
            _FrozenSchemaConnection(extra_trigger=True),
            "immutable trigger inventory differs",
        ),
        (
            _FrozenSchemaConnection(
                update_trigger_body="BEGIN SET @unsafe=1; END"
            ),
            "immutable trigger differs",
        ),
        (
            _FrozenSchemaConnection(table_engine="MyISAM"),
            "engine is not InnoDB",
        ),
        (
            _FrozenSchemaConnection(table_collation="latin1_swedish_ci"),
            "character set is not utf8mb4",
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
                    "collation_name": "utf8mb4_general_ci",
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

    attester._ensure_tolerance_json_mediumtext_migration(connection)
    attester._ensure_tolerance_json_mediumtext_migration(connection)

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

    attester._ensure_tolerance_json_mediumtext_migration(connection)

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
        attester._ensure_tolerance_json_mediumtext_migration(connection)

    assert not any(
        "ALTER TABLE" in sql for sql, _params in connection.statements
    )


def test_v2_attested_market_values_are_frozen_not_null():
    source = inspect.getsource(attester.ensure_attestation_tables).upper()
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
    tolerance_json = {
        "attestation_protocol": ATTESTATION_PROTOCOL_VERSION,
        "universe_manifest_schema": attester.UNIVERSE_MANIFEST_SCHEMA,
        "daily_universe": {"2026-08-21": contract},
    }
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


def test_run_manifest_capacity_is_frozen_as_mediumtext():
    source = inspect.getsource(attester.ensure_attestation_tables).upper()
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
    assert '"universe_manifest_schema": UNIVERSE_MANIFEST_SCHEMA' in source
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

        attester.ensure_attestation_tables(engine)
        first = attester.validate_attestation_schema(engine)
        attester.ensure_attestation_tables(engine)
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
