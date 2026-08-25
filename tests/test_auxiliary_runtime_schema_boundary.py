from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, text

from integrations.bigqmt import membership_snapshot
from server.common import auxiliary_runtime_schema as schema


ROOT = Path(__file__).resolve().parents[1]


def _create_surface(engine) -> None:
    surfaces = {
        "sm_market_overview_daily": schema.MARKET_OVERVIEW_REQUIRED_COLUMNS,
        "st_hot_stats": schema.HOT_STATS_REQUIRED_COLUMNS,
        "st_qmt_realtime_sync_receipt_v2": (
            schema.QMT_REALTIME_SYNC_RECEIPT_REQUIRED_COLUMNS
        ),
        "si_all_index_code": schema.SI_ALL_INDEX_CODE_REQUIRED_COLUMNS,
        **schema.HOT_RANK_FUSION_REQUIRED_COLUMNS,
        **schema.QMT_MEMBERSHIP_REQUIRED_COLUMNS,
    }
    with engine.begin() as connection:
        for table_name, columns in surfaces.items():
            definitions = ", ".join(f'"{column}" TEXT' for column in sorted(columns))
            connection.execute(text(f'CREATE TABLE "{table_name}" ({definitions})'))


def test_auxiliary_runtime_validators_are_metadata_reads_only() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    _create_surface(engine)
    statements: list[str] = []

    @event.listens_for(engine, "before_cursor_execute")
    def _capture(_conn, _cursor, statement, _parameters, _context, _many):
        statements.append(str(statement))

    result = schema.validate_auxiliary_runtime_schema(engine)

    assert result["read_only"] is True
    assert statements
    normalized = "\n".join(statements).upper()
    assert "CREATE TABLE" not in normalized
    assert "ALTER TABLE" not in normalized
    assert "DROP TABLE" not in normalized
    assert "TRUNCATE TABLE" not in normalized
    assert "INSERT INTO" not in normalized
    assert "UPDATE " not in normalized
    assert "DELETE FROM" not in normalized


def test_auxiliary_runtime_validator_fails_closed_when_table_is_missing() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    with pytest.raises(RuntimeError, match="missing_tables"):
        schema.validate_hot_stats_runtime_schema(engine)


@pytest.mark.parametrize(
    ("migration_name", "validator_name", "expected_table", "statement_count"),
    (
        (
            "privileged_migrate_market_overview_daily_schema",
            "validate_market_overview_daily_runtime_schema",
            "sm_market_overview_daily",
            1,
        ),
        (
            "privileged_migrate_hot_stats_schema",
            "validate_hot_stats_runtime_schema",
            "st_hot_stats",
            1,
        ),
        (
            "privileged_migrate_qmt_realtime_sync_receipt_schema",
            "validate_qmt_realtime_sync_receipt_runtime_schema",
            "st_qmt_realtime_sync_receipt_v2",
            1,
        ),
        (
            "privileged_migrate_qmt_membership_snapshot_schema",
            "validate_qmt_membership_snapshot_runtime_schema",
            "qmt_membership_snapshot_run",
            3,
        ),
        (
            "privileged_migrate_hot_rank_fusion_schema",
            "validate_hot_rank_fusion_runtime_schema",
            "st_hot_rank_fused",
            2,
        ),
        (
            "privileged_migrate_si_all_index_code_schema",
            "validate_si_all_index_code_runtime_schema",
            "si_all_index_code",
            1,
        ),
    ),
)
def test_persistent_ddl_is_exposed_only_through_privileged_migrations(
    monkeypatch,
    migration_name: str,
    validator_name: str,
    expected_table: str,
    statement_count: int,
) -> None:
    captured: list[str] = []
    monkeypatch.setattr(
        schema,
        "_privileged_create",
        lambda _engine, statements: captured.extend(statements),
    )
    monkeypatch.setattr(
        schema,
        validator_name,
        lambda _engine: {"read_only": True},
    )

    result = getattr(schema, migration_name)(object())

    assert result["privileged_migration"] is True
    assert len(captured) == statement_count
    assert expected_table in "\n".join(captured)
    assert all("CREATE TABLE IF NOT EXISTS" in statement for statement in captured)


def test_membership_compatibility_guard_is_read_only(monkeypatch) -> None:
    sentinel = {"read_only": True}
    monkeypatch.setattr(
        membership_snapshot,
        "validate_qmt_membership_snapshot_runtime_schema",
        lambda engine: sentinel if engine == "engine" else None,
    )

    assert membership_snapshot.ensure_membership_snapshot_tables("engine") is sentinel


@pytest.mark.parametrize(
    "relative_path",
    (
        "tools/refresh_market_overview_daily.py",
        "tools/hot_data_stats.py",
        "tools/run_big_qmt_bridge.py",
        "integrations/bigqmt/membership_snapshot.py",
        "tools/merge_hot_rank.py",
        "tools/fetch_si_all_index_code_sina.py",
    ),
)
def test_auxiliary_runtime_modules_have_no_persistent_ddl(relative_path: str) -> None:
    source = (ROOT / relative_path).read_text(encoding="utf-8").upper()

    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source
    assert "TRUNCATE TABLE" not in source


def test_collectors_do_not_reference_missing_or_legacy_ddl_entrypoints() -> None:
    merge_source = (ROOT / "tools/merge_hot_rank.py").read_text(encoding="utf-8")
    assert "replace_table_rows(" in merge_source
    assert "DELETE FROM `st_hot_rank_fused`" not in merge_source
    assert "DELETE FROM `st_hot_rank_multi_day`" not in merge_source
    sina_source = (ROOT / "tools/fetch_si_all_index_code_sina.py").read_text(
        encoding="utf-8"
    )

    assert "02_hot_rank_extra_tables.sql" not in merge_source
    assert "run_ddl" not in merge_source
    assert "run_ddl" not in sina_source
    assert "validate_hot_rank_fusion_runtime_schema" in merge_source
    assert "validate_si_all_index_code_runtime_schema" in sina_source
