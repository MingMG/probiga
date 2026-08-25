from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from server.common.qmt_history_coverage import (
    COVERAGE_ENTITY_TABLE,
    COVERAGE_EXACT,
    COVERAGE_INCOMPLETE,
    COVERAGE_UNAVAILABLE,
    COVERAGE_TABLE,
    COVERAGE_TRIGGER_NAMES,
    DATASET_STOCK_DAILY,
    QMT_MINUTE_GRID_PROFILE,
    QMT_MINUTE_GRID_NATIVE_FIXTURE_HASH,
    QmtHistoryCoverageError,
    assess_daily_coverage,
    assess_minute_coverage,
    canonical_digest,
    combine_minute_coverage_partitions,
    coverage_table_ddl_statements,
    coverage_trigger_ddl_statements,
    minute_time_grid,
    require_exact_coverage,
    unavailable_coverage_bundle,
    validate_coverage_authority,
    validate_coverage_bundle,
    validate_coverage_schema,
)


TRADE_DATE = "2026-08-21"
PROVIDER = "gj_big_qmt_inner"
SOURCE_BATCH = "qmt_source_batch_20260821"
HASH_A = "a" * 64
HASH_B = "b" * 64


def _context() -> dict:
    return {
        "trade_date": TRADE_DATE,
        "provider": PROVIDER,
        "run_id": "qmt_history_run_20260821",
        "catalog_batch_id": "catalog_20260821",
        "catalog_manifest_hash": HASH_A,
        "calendar_batch_id": "calendar_2026",
        "calendar_manifest_hash": HASH_B,
        "source_batch_id": SOURCE_BATCH,
        "captured_at": "2026-08-22 01:30:00",
    }


def _minute_context() -> dict:
    return {
        **_context(),
        "daily_provider": PROVIDER,
        "daily_source_batch_id": SOURCE_BATCH,
    }


def _daily_row(
    code: str,
    *,
    volume: float = 100.0,
    amount: float = 1000.0,
    provider: str = PROVIDER,
    trade_date: str = TRADE_DATE,
) -> dict:
    return {
        "stock_code": code,
        "trade_date": trade_date,
        "period": "1d",
        "k_type": 1,
        "adjust_type": 0,
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "volume": volume,
        "amount": amount,
        "pre_close": 10,
        "pre_close_origin": "NATIVE_QMT",
        "provider": provider,
        "batch_id": SOURCE_BATCH,
    }


def _minute_rows(code: str, *, provider: str = PROVIDER) -> list[dict]:
    return [
        {
            "stock_code": code,
            "trade_time": f"{TRADE_DATE} {value}",
            "period": "1m",
            "price": 10.0,
            "avg_price": 10.0,
            "volume": 100,
            "amount": 1000,
            "provider": provider,
            "batch_id": _context()["run_id"],
        }
        for value in minute_time_grid()
    ]


def test_qmt_minute_grid_matches_native_qmt_241_fixture():
    grid = minute_time_grid()

    assert len(grid) == 241
    assert len(set(grid)) == 241
    assert grid[0] == "09:30:00"
    assert grid[120] == "11:30:00"
    assert grid[121] == "13:01:00"
    assert grid[-1] == "15:00:00"
    assert canonical_digest(list(grid)) == QMT_MINUTE_GRID_NATIVE_FIXTURE_HASH
    with pytest.raises(QmtHistoryCoverageError, match="unsupported"):
        minute_time_grid("guessed-grid")


def test_daily_exact_manifest_binds_universe_source_and_every_entity():
    bundle = assess_daily_coverage(
        expected_codes=["000001", "600000"],
        rows=[_daily_row("000001"), _daily_row("600000")],
        **_context(),
    )

    manifest = require_exact_coverage(bundle)
    assert manifest["status"] == COVERAGE_EXACT
    assert manifest["strategy_eligible"] is True
    assert manifest["expected_entity_count"] == 2
    assert manifest["entity_count"] == 2
    assert manifest["bar_count"] == 2
    assert manifest["expected_entity_set_hash"] == canonical_digest(
        ["000001", "600000"]
    )
    assert [row["stock_code"] for row in bundle["entities"]] == [
        "000001",
        "600000",
    ]


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda rows: rows.pop(), "MISSING_CODE"),
        (
            lambda rows: rows.append(_daily_row("000002")),
            "UNEXPECTED_CODE",
        ),
        (
            lambda rows: rows[0].update(pre_close_origin="SYNTHETIC"),
            "NON_NATIVE_PRE_CLOSE",
        ),
        (
            lambda rows: rows[0].update(adjust_type=1),
            "ADJUSTED_DAILY_ROW",
        ),
        (
            lambda rows: rows[0].update(provider="public_fallback"),
            "WRONG_PROVIDER",
        ),
        (
            lambda rows: rows[0].update(batch_id="other_batch"),
            "WRONG_SOURCE_BATCH",
        ),
    ],
)
def test_daily_partial_or_wrong_provenance_is_never_exact(mutate, reason):
    rows = [_daily_row("000001"), _daily_row("600000")]
    mutate(rows)

    bundle = assess_daily_coverage(
        expected_codes=["000001", "600000"],
        rows=rows,
        **_context(),
    )

    assert bundle["manifest"]["status"] == COVERAGE_INCOMPLETE
    assert bundle["manifest"]["strategy_eligible"] is False
    assert reason in {
        row["code"] for row in bundle["manifest"]["reasons"]
    }
    with pytest.raises(QmtHistoryCoverageError, match="not exact"):
        require_exact_coverage(bundle)


def test_empty_daily_response_is_recorded_incomplete_and_fails_closed():
    bundle = assess_daily_coverage(
        expected_codes=["000001"], rows=[], **_context()
    )

    assert bundle["manifest"]["status"] == COVERAGE_INCOMPLETE
    assert {row["code"] for row in bundle["manifest"]["reasons"]} == {
        "EMPTY_SOURCE_ROWS",
        "MISSING_CODE",
    }


def test_minute_exact_requires_full_grid_and_native_no_trade_explanation():
    bundle = assess_minute_coverage(
        expected_codes=["000001", "600000"],
        daily_rows=[
            _daily_row("000001"),
            _daily_row("600000", volume=0, amount=0),
        ],
        minute_rows=_minute_rows("000001"),
        **_minute_context(),
    )

    manifest = require_exact_coverage(bundle)
    assert manifest["status"] == COVERAGE_EXACT
    assert manifest["grid_profile"] == QMT_MINUTE_GRID_PROFILE
    assert manifest["expected_traded_count"] == 1
    assert manifest["actual_traded_count"] == 1
    assert manifest["no_trade_count"] == 1
    assert manifest["bar_count"] == 241
    entities = {row["stock_code"]: row for row in bundle["entities"]}
    assert entities["000001"]["classification"] == "TRADED"
    assert entities["000001"]["bar_count"] == 241
    assert entities["600000"]["classification"] == "NO_TRADE"
    assert entities["600000"]["bar_count"] == 0


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda rows: rows.clear(), "EMPTY_SOURCE_ROWS"),
        (lambda rows: rows.pop(), "MINUTE_GRID_MISMATCH"),
        (lambda rows: rows.append(deepcopy(rows[-1])), "DUPLICATE_MINUTE_TIME"),
        (
            lambda rows: rows[0].update(
                trade_time="2026-08-20 09:30:00"
            ),
            "WRONG_TRADE_TIME",
        ),
        (
            lambda rows: rows[0].update(provider="public_fallback"),
            "WRONG_PROVIDER",
        ),
    ],
)
def test_minute_empty_partial_duplicate_or_wrong_source_is_ineligible(
    mutate, reason
):
    rows = _minute_rows("000001")
    mutate(rows)

    bundle = assess_minute_coverage(
        expected_codes=["000001"],
        daily_rows=[_daily_row("000001")],
        minute_rows=rows,
        **_minute_context(),
    )

    assert bundle["manifest"]["status"] == COVERAGE_INCOMPLETE
    assert reason in {
        row["code"] for row in bundle["manifest"]["reasons"]
    }
    with pytest.raises(QmtHistoryCoverageError, match="not exact"):
        require_exact_coverage(bundle)


def test_minute_uses_exact_date_universe_not_period_union():
    rows = _minute_rows("000001") + _minute_rows("600000")

    bundle = assess_minute_coverage(
        expected_codes=["000001"],
        daily_rows=[_daily_row("000001")],
        minute_rows=rows,
        **_minute_context(),
    )

    assert bundle["manifest"]["status"] == COVERAGE_INCOMPLETE
    assert {
        (row["code"], row["stock_code"])
        for row in bundle["manifest"]["reasons"]
    } >= {("UNEXPECTED_CODE", "600000")}


@pytest.mark.parametrize(
    ("context_update", "reason"),
    [
        ({"daily_provider": "other_qmt"}, "DAILY_EVIDENCE_NOT_NATIVE_RAW"),
        (
            {"daily_source_batch_id": "other_daily_batch"},
            "DAILY_EVIDENCE_NOT_NATIVE_RAW",
        ),
    ],
)
def test_minute_no_trade_evidence_is_bound_to_daily_provider_and_batch(
    context_update, reason
):
    context = _minute_context()
    context.update(context_update)

    bundle = assess_minute_coverage(
        expected_codes=["000001"],
        daily_rows=[_daily_row("000001")],
        minute_rows=_minute_rows("000001"),
        **context,
    )

    assert bundle["manifest"]["status"] == COVERAGE_INCOMPLETE
    assert reason in {row["code"] for row in bundle["manifest"]["reasons"]}


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda row: row.update(batch_id="unrelated_capture"), "WRONG_CAPTURE_RUN"),
        (lambda row: row.update(price=0), "INVALID_MINUTE_VALUE"),
        (lambda row: row.update(amount=-1), "INVALID_MINUTE_VALUE"),
    ],
)
def test_minute_rows_require_capture_run_and_valid_values(mutation, reason):
    rows = _minute_rows("000001")
    mutation(rows[0])

    bundle = assess_minute_coverage(
        expected_codes=["000001"],
        daily_rows=[_daily_row("000001")],
        minute_rows=rows,
        **_minute_context(),
    )

    assert bundle["manifest"]["status"] == COVERAGE_INCOMPLETE
    assert reason in {row["code"] for row in bundle["manifest"]["reasons"]}


def test_manifest_hash_is_stable_when_native_rows_arrive_in_reverse_order():
    rows = _minute_rows("000001")
    forward = assess_minute_coverage(
        expected_codes=["000001"],
        daily_rows=[_daily_row("000001")],
        minute_rows=rows,
        **_minute_context(),
    )
    reverse = assess_minute_coverage(
        expected_codes=["000001"],
        daily_rows=[_daily_row("000001")],
        minute_rows=list(reversed(rows)),
        **_minute_context(),
    )

    assert forward["manifest"]["manifest_hash"] == reverse["manifest"][
        "manifest_hash"
    ]


def test_bounded_minute_partitions_combine_to_one_exact_full_market_root():
    first = assess_minute_coverage(
        expected_codes=["000001"],
        daily_rows=[_daily_row("000001")],
        minute_rows=_minute_rows("000001"),
        **_minute_context(),
    )
    second = assess_minute_coverage(
        expected_codes=["600000"],
        daily_rows=[_daily_row("600000")],
        minute_rows=_minute_rows("600000"),
        **_minute_context(),
    )

    combined = combine_minute_coverage_partitions(
        expected_codes=["000001", "600000"],
        partitions=[first, second],
    )

    manifest = require_exact_coverage(combined)
    assert manifest["entity_count"] == 2
    assert manifest["bar_count"] == 482
    assert manifest["expected_entity_set_hash"] == canonical_digest(
        ["000001", "600000"]
    )


def test_same_day_source_is_never_certified_from_system_clock_alone():
    context = _context()
    context["captured_at"] = f"{TRADE_DATE} 23:59:59"

    bundle = assess_daily_coverage(
        expected_codes=["000001"],
        rows=[_daily_row("000001")],
        **context,
    )

    assert bundle["manifest"]["status"] == COVERAGE_INCOMPLETE
    assert "SAME_DAY_SOURCE_NOT_FINAL" in {
        row["code"] for row in bundle["manifest"]["reasons"]
    }


def test_validator_rejects_rehashed_same_day_exact_claim():
    bundle = assess_daily_coverage(
        expected_codes=["000001"],
        rows=[_daily_row("000001")],
        **_context(),
    )
    manifest = bundle["manifest"]
    core = {
        key: value
        for key, value in manifest.items()
        if key not in {"manifest_hash", "manifest_json"}
    }
    core["captured_at"] = f"{TRADE_DATE}T23:59:59"
    forged_hash = canonical_digest(core)
    bundle["manifest"] = {
        **core,
        "manifest_hash": forged_hash,
        "manifest_json": json.dumps(
            core,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ),
    }
    for row in bundle["entities"]:
        row["manifest_hash"] = forged_hash

    with pytest.raises(QmtHistoryCoverageError, match="same-day"):
        validate_coverage_bundle(bundle)


def test_no_trade_classification_rejects_any_minute_bars():
    bundle = assess_minute_coverage(
        expected_codes=["000001"],
        daily_rows=[_daily_row("000001", volume=0, amount=0)],
        minute_rows=_minute_rows("000001"),
        **_minute_context(),
    )

    assert bundle["manifest"]["status"] == COVERAGE_INCOMPLETE
    assert "NO_TRADE_CODE_HAS_BARS" in {
        row["code"] for row in bundle["manifest"]["reasons"]
    }


def test_missing_or_non_native_daily_activity_cannot_explain_minute_absence():
    daily = _daily_row("000001", volume=0, amount=0)
    daily["pre_close_origin"] = "UNVERIFIED_LEGACY"

    bundle = assess_minute_coverage(
        expected_codes=["000001"],
        daily_rows=[daily],
        minute_rows=[],
        **_minute_context(),
    )

    assert bundle["manifest"]["status"] == COVERAGE_INCOMPLETE
    reason_codes = {row["code"] for row in bundle["manifest"]["reasons"]}
    assert "DAILY_EVIDENCE_NOT_NATIVE_RAW" in reason_codes
    assert "MISSING_CODE" in reason_codes


@pytest.mark.parametrize(
    "status", ["NO_DATA", "NOT_AUTHORIZED", "UNSUPPORTED_CLIENT"]
)
def test_provider_limitations_are_explicit_unavailable_not_synthetic(status):
    bundle = unavailable_coverage_bundle(
        dataset="stock_l2_order",
        trade_date=TRADE_DATE,
        provider="gj_qmt",
        capability_key="native:get_market_data_ex:l2order",
        capability_status=status,
        probed_at="2026-08-22 01:10:00",
        reason="provider did not expose history",
    )

    manifest = validate_coverage_bundle(bundle)
    assert manifest["status"] == COVERAGE_UNAVAILABLE
    assert manifest["strategy_eligible"] is False
    assert manifest["entity_count"] == 0
    with pytest.raises(QmtHistoryCoverageError, match="not exact"):
        require_exact_coverage(bundle)


def test_unavailable_manifest_rejects_unverified_or_supported_claim():
    with pytest.raises(QmtHistoryCoverageError, match="provider limitation"):
        unavailable_coverage_bundle(
            dataset="stock_l2_order",
            trade_date=TRADE_DATE,
            provider="gj_qmt",
            capability_key="native:get_market_data_ex:l2order",
            capability_status="SUPPORTED",
            probed_at="2026-08-22 01:10:00",
            reason="",
        )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda bundle: bundle["manifest"].update(entity_count=99),
        lambda bundle: bundle["manifest"].update(manifest_hash="f" * 64),
        lambda bundle: bundle["entities"][0].update(bar_count=240),
        lambda bundle: bundle["entities"][0].update(row_hash="e" * 64),
        lambda bundle: bundle["entities"][0].update(manifest_hash="d" * 64),
        lambda bundle: bundle["entities"][0].update(expected_state="NO_TRADE"),
    ],
)
def test_manifest_or_entity_tampering_is_detected(mutation):
    bundle = assess_minute_coverage(
        expected_codes=["000001"],
        daily_rows=[_daily_row("000001")],
        minute_rows=_minute_rows("000001"),
        **_minute_context(),
    )
    mutation(bundle)

    with pytest.raises(QmtHistoryCoverageError):
        validate_coverage_bundle(bundle)


def test_coverage_schema_declares_two_append_only_tables_and_four_triggers():
    tables = coverage_table_ddl_statements()
    triggers = coverage_trigger_ddl_statements()
    normalized = " ".join([*tables, *triggers])

    assert len(tables) == 2
    assert len(triggers) == 4
    assert "qmt_history_coverage_manifest" in normalized
    assert "qmt_history_coverage_entity" in normalized
    assert "expected_state VARCHAR(32) NOT NULL" in normalized
    assert "FOREIGN KEY (manifest_hash)" in normalized
    assert "FOREIGN KEY (catalog_batch_id)" in normalized
    assert "FOREIGN KEY (calendar_batch_id)" in normalized
    assert normalized.count("SIGNAL SQLSTATE '45000'") == 4
    assert normalized.count("BEFORE UPDATE") == 2
    assert normalized.count("BEFORE DELETE") == 2


class _SchemaRows:
    def __init__(self, rows):
        self.rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)

    def fetchall(self):
        return list(self.rows)


def _coverage_schema_columns():
    manifest = {
        "manifest_hash": ("char", "char(64)", "NO", 64),
        "schema_version": ("varchar", "varchar(64)", "NO", 64),
        "dataset": ("varchar", "varchar(64)", "NO", 64),
        "period": ("varchar", "varchar(16)", "NO", 16),
        "provider": ("varchar", "varchar(32)", "NO", 32),
        "trade_date": ("date", "date", "NO", None),
        "status": ("varchar", "varchar(32)", "NO", 32),
        "strategy_eligible": ("tinyint", "tinyint(1)", "NO", None),
        "run_id": ("varchar", "varchar(64)", "NO", 64),
        "catalog_batch_id": ("varchar", "varchar(64)", "YES", 64),
        "catalog_manifest_hash": ("char", "char(64)", "YES", 64),
        "calendar_batch_id": ("varchar", "varchar(64)", "YES", 64),
        "calendar_manifest_hash": ("char", "char(64)", "YES", 64),
        "source_batch_id": ("varchar", "varchar(64)", "NO", 64),
        "grid_profile": ("varchar", "varchar(64)", "NO", 64),
        "expected_entity_count": ("int", "int", "NO", None),
        "entity_count": ("int", "int", "NO", None),
        "expected_traded_count": ("int", "int", "NO", None),
        "actual_traded_count": ("int", "int", "NO", None),
        "no_trade_count": ("int", "int", "NO", None),
        "bar_count": ("bigint", "bigint", "NO", None),
        "expected_entity_set_hash": ("char", "char(64)", "NO", 64),
        "expected_traded_set_hash": ("char", "char(64)", "NO", 64),
        "actual_traded_set_hash": ("char", "char(64)", "NO", 64),
        "no_trade_set_hash": ("char", "char(64)", "NO", 64),
        "entity_root_hash": ("char", "char(64)", "NO", 64),
        "reason_count": ("int", "int", "NO", None),
        "captured_at": ("datetime", "datetime", "NO", None),
        "manifest_json": ("mediumtext", "mediumtext", "NO", None),
        "created_at": ("datetime", "datetime", "NO", None),
    }
    entity = {
        "manifest_hash": ("char", "char(64)", "NO", 64),
        "stock_code": ("varchar", "varchar(16)", "NO", 16),
        "expected_state": ("varchar", "varchar(32)", "NO", 32),
        "classification": ("varchar", "varchar(32)", "NO", 32),
        "bar_count": ("int", "int", "NO", None),
        "time_set_hash": ("char", "char(64)", "NO", 64),
        "first_time": ("varchar", "varchar(32)", "NO", 32),
        "last_time": ("varchar", "varchar(32)", "NO", 32),
        "source_row_hash": ("char", "char(64)", "NO", 64),
        "row_hash": ("char", "char(64)", "NO", 64),
        "created_at": ("datetime", "datetime", "NO", None),
    }
    rows = []
    for table, fields in (
        (COVERAGE_TABLE, manifest),
        (COVERAGE_ENTITY_TABLE, entity),
    ):
        for ordinal, (name, spec) in enumerate(fields.items(), start=1):
            is_character = spec[0] in {"char", "varchar", "mediumtext"}
            rows.append(
                {
                    "TABLE_NAME": table,
                    "COLUMN_NAME": name,
                    "DATA_TYPE": spec[0],
                    "COLUMN_TYPE": spec[1],
                    "IS_NULLABLE": spec[2],
                    "CHARACTER_MAXIMUM_LENGTH": spec[3],
                    "ORDINAL_POSITION": ordinal,
                    "CHARACTER_SET_NAME": "utf8mb4" if is_character else None,
                    "COLLATION_NAME": (
                        "utf8mb4_unicode_ci" if is_character else None
                    ),
                }
            )
    return rows


def _coverage_index_rows():
    contracts = {
        (COVERAGE_TABLE, "PRIMARY", 0): ("manifest_hash",),
        (COVERAGE_TABLE, "idx_qmt_history_coverage_lookup", 1): (
            "dataset",
            "trade_date",
            "provider",
            "status",
            "captured_at",
        ),
        (COVERAGE_ENTITY_TABLE, "PRIMARY", 0): (
            "manifest_hash",
            "stock_code",
        ),
        (
            COVERAGE_ENTITY_TABLE,
            "idx_qmt_history_coverage_entity_code",
            1,
        ): ("stock_code", "manifest_hash"),
    }
    return [
        {
            "TABLE_NAME": table,
            "INDEX_NAME": name,
            "NON_UNIQUE": non_unique,
            "SEQ_IN_INDEX": sequence,
            "COLUMN_NAME": column,
            "SUB_PART": None,
            "INDEX_TYPE": "BTREE",
        }
        for (table, name, non_unique), columns in contracts.items()
        for sequence, column in enumerate(columns, start=1)
    ]


def _coverage_trigger_rows():
    return [
        {
            "TRIGGER_NAME": name,
            "EVENT_OBJECT_TABLE": (
                COVERAGE_TABLE if index < 2 else COVERAGE_ENTITY_TABLE
            ),
            "EVENT_MANIPULATION": "UPDATE" if index % 2 == 0 else "DELETE",
            "ACTION_TIMING": "BEFORE",
            "ACTION_STATEMENT": "SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='sealed'",
        }
        for index, name in enumerate(COVERAGE_TRIGGER_NAMES)
    ]


class _CoverageSchemaConnection:
    def __init__(self, *, triggers=None):
        self.triggers = (
            _coverage_trigger_rows() if triggers is None else triggers
        )

    def execute(self, statement, params=None):
        sql = str(statement)
        if "information_schema.TABLES" in sql:
            return _SchemaRows(
                [
                    {
                        "TABLE_NAME": COVERAGE_ENTITY_TABLE,
                        "ENGINE": "InnoDB",
                        "TABLE_COLLATION": "utf8mb4_unicode_ci",
                    },
                    {
                        "TABLE_NAME": COVERAGE_TABLE,
                        "ENGINE": "InnoDB",
                        "TABLE_COLLATION": "utf8mb4_unicode_ci",
                    },
                ]
            )
        if "information_schema.COLUMNS" in sql:
            return _SchemaRows(_coverage_schema_columns())
        if "information_schema.STATISTICS" in sql:
            return _SchemaRows(_coverage_index_rows())
        if "information_schema.KEY_COLUMN_USAGE" in sql:
            return _SchemaRows(
                [
                    {
                        "TABLE_NAME": COVERAGE_TABLE,
                        "CONSTRAINT_NAME": "fk_qmt_history_coverage_catalog",
                        "COLUMN_NAME": "catalog_batch_id",
                        "REFERENCED_TABLE_NAME": "qmt_stock_catalog_batch",
                        "REFERENCED_COLUMN_NAME": "batch_id",
                        "UPDATE_RULE": "RESTRICT",
                        "DELETE_RULE": "RESTRICT",
                    },
                    {
                        "TABLE_NAME": COVERAGE_TABLE,
                        "CONSTRAINT_NAME": "fk_qmt_history_coverage_calendar",
                        "COLUMN_NAME": "calendar_batch_id",
                        "REFERENCED_TABLE_NAME": "qmt_trade_calendar_batch",
                        "REFERENCED_COLUMN_NAME": "batch_id",
                        "UPDATE_RULE": "RESTRICT",
                        "DELETE_RULE": "RESTRICT",
                    },
                    {
                        "TABLE_NAME": COVERAGE_ENTITY_TABLE,
                        "CONSTRAINT_NAME": (
                            "fk_qmt_history_coverage_entity_manifest"
                        ),
                        "COLUMN_NAME": "manifest_hash",
                        "REFERENCED_TABLE_NAME": COVERAGE_TABLE,
                        "REFERENCED_COLUMN_NAME": "manifest_hash",
                        "UPDATE_RULE": "RESTRICT",
                        "DELETE_RULE": "RESTRICT",
                    },
                ]
            )
        if "information_schema.TRIGGERS" in sql:
            return _SchemaRows(self.triggers)
        raise AssertionError(sql)


def test_coverage_schema_validator_proves_primary_topology_and_seals():
    detail = validate_coverage_schema(_CoverageSchemaConnection())

    assert detail["database"] == "probiga"
    assert detail["table_count"] == 2
    assert detail["foreign_key_count"] == 3
    assert detail["trigger_count"] == 4
    assert detail["runtime_ddl_required"] is False
    assert detail["physical_seal_verified"] is True


def test_coverage_schema_validator_rejects_missing_or_misbound_trigger():
    triggers = _coverage_trigger_rows()
    triggers[0] = {**triggers[0], "EVENT_OBJECT_TABLE": "wrong_table"}

    with pytest.raises(QmtHistoryCoverageError, match="trigger contract"):
        validate_coverage_schema(
            _CoverageSchemaConnection(triggers=triggers)
        )


def test_context_rejects_invalid_hash_identity_and_capture_before_trade_date():
    values = _context()
    values["catalog_manifest_hash"] = "not-a-hash"
    with pytest.raises(QmtHistoryCoverageError, match="SHA-256"):
        assess_daily_coverage(
            expected_codes=["000001"],
            rows=[_daily_row("000001")],
            **values,
        )

    values = _context()
    values["captured_at"] = datetime(2026, 8, 20, 23, 59)
    with pytest.raises(QmtHistoryCoverageError, match="precedes"):
        assess_daily_coverage(
            expected_codes=["000001"],
            rows=[_daily_row("000001")],
            **values,
        )


def test_daily_dataset_constant_remains_frozen():
    assert DATASET_STOCK_DAILY == "stock_daily"


def test_authority_validation_reloads_catalog_and_calendar_receipts(monkeypatch):
    from server.common import qmt_stock_catalog, qmt_trade_calendar

    class Catalog:
        manifest_hash = HASH_A

        @staticmethod
        def eligible_codes(trade_date):
            assert trade_date == TRADE_DATE
            return ["000001", "600000"]

    class Calendar:
        manifest_hash = HASH_B

        @staticmethod
        def sessions_between(start_date, end_date):
            assert start_date == end_date == TRADE_DATE
            return [TRADE_DATE]

    monkeypatch.setattr(
        qmt_stock_catalog,
        "load_stock_catalog",
        lambda connection, **kwargs: Catalog(),
    )
    monkeypatch.setattr(
        qmt_trade_calendar,
        "load_trade_calendar_receipt",
        lambda connection, **kwargs: Calendar(),
    )
    bundle = assess_daily_coverage(
        expected_codes=["000001", "600000"],
        rows=[_daily_row("000001"), _daily_row("600000")],
        **_context(),
    )

    assert validate_coverage_authority(object(), bundle)["status"] == COVERAGE_EXACT

    Catalog.eligible_codes = staticmethod(lambda trade_date: ["000001"])
    with pytest.raises(QmtHistoryCoverageError, match="universe differs"):
        validate_coverage_authority(object(), bundle)


def test_historical_bridge_disables_synthetic_minute_fill(monkeypatch):
    from integrations.qmt import bridge as qmt_bridge

    captured = {}

    def fake_run(payload, *, timeout=None):
        captured.update(payload)
        captured["timeout"] = timeout
        return {"rows": []}

    monkeypatch.setattr(qmt_bridge, "_run", fake_run)

    frame = qmt_bridge.minute(
        ["000001.SZ"],
        trade_date=TRADE_DATE,
        fill_data=False,
        timeout=17,
    )

    assert frame.empty
    assert captured["fill_data"] is False
    assert captured["timeout"] == 17


def test_empty_native_minute_batch_marks_local_run_failed(monkeypatch):
    from integrations.qmt import local_history

    source_engine = SimpleNamespace()
    local_engine = SimpleNamespace(
        url="mysql+pymysql://user:" + "password@localhost/probiga_qmt_history"
    )
    finishes = []
    captured = {}
    monkeypatch.setattr(
        local_history, "validate_local_history_tables", lambda _engine: None
    )
    monkeypatch.setattr(local_history, "_record_run_start", lambda *_a, **_k: None)
    monkeypatch.setattr(
        local_history,
        "_record_run_finish",
        lambda *_a, **kwargs: finishes.append(kwargs),
    )

    def empty_minute(*_args, **kwargs):
        captured.update(kwargs)
        return pd.DataFrame()

    monkeypatch.setattr(local_history.bridge, "minute", empty_minute)

    with pytest.raises(RuntimeError, match="returned no native rows"):
        local_history.backfill_minute_local(
            source_engine=source_engine,
            local_engine=local_engine,
            stock_codes=["000001"],
            trade_dates=[TRADE_DATE],
            batch_size=1,
        )

    assert captured["fill_data"] is False
    assert finishes[-1]["status"] == "FAILED"
    assert finishes[-1]["fetched_rows"] == 0
