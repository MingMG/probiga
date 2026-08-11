from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import re
from typing import Any

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool

from server.db.migrations_v4 import (
    MIGRATION_TABLE_DDL,
    MIGRATIONS,
    _expected_schema,
    _validate_schema_on_connection,
    run_v4_migrations,
)
import server.db.migrations_v4 as migrations_v4
from server.trading_v4.domain import (
    DataManifest,
    DecisionClock,
    DecisionContext,
    QualityStatus,
    SourceWatermark,
)
from server.trading_v4.infrastructure.repository import (
    RUN_COMMITTED,
    RUN_CREATED,
    RUN_VALIDATING,
    DecisionRunConflictError,
    HeadPublishConflictError,
    HeadPublishError,
    InvalidRunTransitionError,
    TradingV4Repository,
)


pytestmark = pytest.mark.filterwarnings(
    "ignore:The default (date|datetime) adapter is deprecated:DeprecationWarning"
)


SQLITE_SCHEMA = (
    """
    CREATE TABLE st_decision_context_v4 (
        context_id TEXT PRIMARY KEY,
        trade_date TEXT NOT NULL,
        decision_at TEXT NOT NULL,
        knowledge_cutoff_at TEXT NOT NULL,
        decision_clock TEXT NOT NULL DEFAULT 'INTRADAY',
        feature_as_of TEXT NOT NULL,
        universe_version TEXT NOT NULL DEFAULT 'test-universe',
        account_snapshot_id TEXT NOT NULL DEFAULT 'test-account-snapshot',
        run_mode TEXT NOT NULL DEFAULT 'INTRADAY',
        is_realtime INTEGER NOT NULL DEFAULT 1,
        freshness_status TEXT NOT NULL,
        fallback_used INTEGER NOT NULL DEFAULT 0,
        data_manifest_json TEXT NOT NULL DEFAULT '{}',
        source_manifest_json TEXT NOT NULL DEFAULT '{}',
        quality_json TEXT NOT NULL DEFAULT '{}',
        factor_spec_versions_json TEXT NOT NULL DEFAULT '{}',
        forecast_contract_ids_json TEXT NOT NULL DEFAULT '[]',
        model_versions_json TEXT NOT NULL DEFAULT '{}',
        model_artifact_hashes_json TEXT NOT NULL DEFAULT '{}',
        model_training_cutoffs_json TEXT NOT NULL DEFAULT '{}',
        model_available_at_json TEXT NOT NULL DEFAULT '{}',
        calibration_versions_json TEXT NOT NULL DEFAULT '{}',
        calibration_artifact_hashes_json TEXT NOT NULL DEFAULT '{}',
        calibration_training_cutoffs_json TEXT NOT NULL DEFAULT '{}',
        calibration_available_at_json TEXT NOT NULL DEFAULT '{}',
        capability_statuses_json TEXT NOT NULL DEFAULT '{}',
        context_json TEXT NOT NULL DEFAULT '{}',
        context_hash TEXT NOT NULL UNIQUE,
        data_snapshot_hash TEXT NOT NULL,
        feature_version TEXT NOT NULL DEFAULT 'test-features',
        model_set_version TEXT NOT NULL,
        config_version TEXT NOT NULL,
        portfolio_policy_version TEXT NOT NULL DEFAULT 'test-portfolio',
        execution_contract_version TEXT NOT NULL DEFAULT 'test-execution',
        fee_schedule_version TEXT NOT NULL DEFAULT 'test-fees',
        code_commit_sha TEXT NOT NULL,
        random_seed INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE st_source_watermark_v4 (
        context_id TEXT NOT NULL,
        source_key TEXT NOT NULL,
        knowledge_time TEXT NOT NULL,
        source_event_at TEXT,
        first_seen_at TEXT,
        received_at TEXT,
        available_at TEXT,
        record_count INTEGER NOT NULL DEFAULT 0,
        snapshot_id TEXT NOT NULL DEFAULT '',
        coverage TEXT,
        lag_seconds INTEGER,
        batch_id TEXT NOT NULL DEFAULT '',
        schema_version TEXT NOT NULL DEFAULT '',
        content_hash TEXT NOT NULL DEFAULT '',
        quality_status TEXT NOT NULL,
        details_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (context_id, source_key)
    )
    """,
    """
    CREATE TABLE st_decision_run_v4 (
        run_uid TEXT PRIMARY KEY,
        run_idempotency_key TEXT NOT NULL UNIQUE,
        context_id TEXT NOT NULL,
        account_id TEXT NOT NULL DEFAULT '',
        channel TEXT NOT NULL,
        run_type TEXT NOT NULL,
        trigger_type TEXT NOT NULL,
        trigger_ref_id TEXT NOT NULL DEFAULT '',
        parent_run_uid TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL,
        model_set_version TEXT NOT NULL,
        config_version TEXT NOT NULL,
        code_commit_sha TEXT NOT NULL,
        result_hash TEXT,
        error_code TEXT,
        error_message TEXT,
        started_at TEXT,
        validated_at TEXT,
        committed_at TEXT,
        finished_at TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE (run_uid, context_id)
    )
    """,
    """
    CREATE TABLE st_decision_channel_head_v4 (
        channel TEXT NOT NULL,
        account_id TEXT NOT NULL DEFAULT '',
        run_uid TEXT NOT NULL,
        context_id TEXT NOT NULL,
        head_version INTEGER NOT NULL,
        published_at TEXT NOT NULL,
        published_by TEXT NOT NULL DEFAULT '',
        updated_at TEXT NOT NULL,
        PRIMARY KEY (channel, account_id),
        FOREIGN KEY (run_uid, context_id)
            REFERENCES st_decision_run_v4 (run_uid, context_id)
            ON DELETE RESTRICT
    )
    """,
)


@pytest.fixture()
def repository() -> TradingV4Repository:
    engine = create_engine(
        "sqlite+pysqlite://",
        future=True,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    with engine.begin() as connection:
        connection.execute(text("PRAGMA foreign_keys = ON"))
        for statement in SQLITE_SCHEMA:
            connection.execute(text(statement))
    try:
        yield TradingV4Repository(engine)
    finally:
        engine.dispose()


def _decision_context(decision_at: datetime) -> DecisionContext:
    knowledge_cutoff = decision_at - timedelta(seconds=2)
    watermark_at = knowledge_cutoff - timedelta(seconds=3)
    return DecisionContext(
        decision_time=decision_at,
        decision_clock=DecisionClock.INTRADAY,
        knowledge_cutoff=knowledge_cutoff,
        trade_date=decision_at.date(),
        universe_version="cn-a-share-v1",
        data_manifest=DataManifest(
            record_hashes={"quotes-snapshot-v1": "a" * 64}
        ),
        portfolio_policy_version="portfolio-v1",
        execution_contract_version="execution-v1",
        fee_schedule_version="fees-v1",
        account_snapshot_id="account-snapshot-v1",
        code_commit_sha="b" * 40,
        config_hash="c" * 64,
        random_seed=7,
        source_watermarks={
            "realtime_quotes": SourceWatermark(
                source="realtime_quotes",
                knowledge_time=watermark_at,
                record_count=512,
                quality_status=QualityStatus.PASS,
                snapshot_id="quotes-snapshot-v1",
                valid_until=decision_at + timedelta(minutes=5),
                coverage=Decimal("1"),
                batch_id="quotes-batch-v1",
                schema_version="quotes-schema-v1",
                content_hash="f" * 64,
            )
        },
        factor_spec_versions={"momentum": "factor-v1"},
        forecast_contract_ids=("forecast-v1",),
        model_versions={"cross_sectional": "v4:model-v1"},
        model_artifact_hashes={"cross_sectional": "d" * 64},
        model_training_cutoffs={
            "cross_sectional": knowledge_cutoff - timedelta(days=30)
        },
        model_available_at={
            "cross_sectional": knowledge_cutoff - timedelta(days=1)
        },
        calibration_versions={"cross_sectional": "v4:calibration-v1"},
        calibration_artifact_hashes={"cross_sectional": "e" * 64},
        calibration_training_cutoffs={
            "cross_sectional": knowledge_cutoff - timedelta(days=30)
        },
        calibration_available_at={
            "cross_sectional": knowledge_cutoff - timedelta(days=1)
        },
    )


def _insert_context(
    repository: TradingV4Repository,
    context_id: str,
    *,
    decision_at: datetime,
    freshness_status: str = "PASS",
) -> None:
    with repository.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_decision_context_v4 (
                    context_id, trade_date, decision_at,
                    knowledge_cutoff_at, feature_as_of,
                    freshness_status, context_hash, data_snapshot_hash,
                    model_set_version, config_version, code_commit_sha
                ) VALUES (
                    :context_id, :trade_date, :decision_at,
                    :knowledge_cutoff_at, :feature_as_of,
                    :freshness_status, :context_hash, :data_snapshot_hash,
                    :model_set_version, :config_version, :code_commit_sha
                )
                """
            ),
            {
                "context_id": context_id,
                "trade_date": decision_at.date().isoformat(),
                "decision_at": decision_at,
                "knowledge_cutoff_at": decision_at,
                "feature_as_of": decision_at.date().isoformat(),
                "freshness_status": freshness_status,
                "context_hash": (context_id.encode().hex() + "0" * 64)[:64],
                "data_snapshot_hash": (
                    context_id.encode().hex() + "1" * 64
                )[:64],
                "model_set_version": "v4-model-test",
                "config_version": "v4-config-test",
                "code_commit_sha": "test-commit",
            },
        )


def _create_run(
    repository: TradingV4Repository,
    *,
    context_id: str,
    run_uid: str,
    account_id: str = "paper-v4",
    channel: str = "shadow",
    created_at: datetime | None = None,
):
    return repository.create_or_get_run(
        context_id=context_id,
        account_id=account_id,
        channel=channel,
        run_type="INTRADAY",
        trigger_type="SCHEDULED",
        model_set_version="v4-model-test",
        config_version="v4-config-test",
        code_commit_sha="test-commit",
        run_uid=run_uid,
        created_at=created_at,
    )


def test_v4_migration_plan_is_expand_only_and_dry_runnable() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        result = run_v4_migrations(engine, dry_run=True)
        assert [item.status for item in result] == [
            "would_apply",
            "would_apply",
            "would_apply",
            "would_apply",
            "would_apply",
            "would_apply",
            "would_apply",
        ]
        assert result[0].statement_count == 7
        assert result[1].statement_count == 6
        assert result[2].statement_count == 4
        assert result[3].statement_count == 16
        assert result[4].statement_count == 3
        assert result[5].statement_count == 9
        assert result[6].statement_count == 5
        assert all(
            re.fullmatch(r"[0-9a-f]{64}", item.checksum)
            for item in result
        )

        statements = tuple(MIGRATIONS[0]["statements"])
        expected_tables = {
            "st_decision_context_v4",
            "st_source_watermark_v4",
            "st_decision_run_v4",
            "st_job_run_v4",
            "st_decision_channel_head_v4",
            "st_runtime_control_v4",
            "st_runtime_control_transition_v4",
        }
        found_tables = {
            match.group(1)
            for statement in statements
            if (
                match := re.search(
                    r"CREATE TABLE IF NOT EXISTS\s+(\w+)",
                    statement,
                    flags=re.IGNORECASE,
                )
            )
        }
        assert found_tables == expected_tables
        assert found_tables.isdisjoint(
            {
                "st_account_v4",
                "st_cash_ledger_v4",
                "st_fill_v4",
                "st_order_v4",
                "st_position_v4",
                "st_risk_ledger_v4",
            }
        )
        assert all("CREATE TABLE IF NOT EXISTS" in item for item in statements)
        assert all("ALTER TABLE" not in item.upper() for item in statements)
        assert all("DROP TABLE" not in item.upper() for item in statements)
        assert all("COLLATE=utf8mb4_bin" in item for item in statements)
        lease_statements = tuple(MIGRATIONS[1]["statements"])
        assert "lease_token CHAR(64) NULL" in lease_statements[0]
        assert "max_attempts INT UNSIGNED NOT NULL DEFAULT 3" in lease_statements[1]
        assert "idx_v4_job_claim_due" in lease_statements[2]
        assert "uk_v4_job_lease_token" in lease_statements[3]
        assert "CREATE TRIGGER trg_v4_job_lease_bi" in lease_statements[4]
        assert "CREATE TRIGGER trg_v4_job_lease_bu" in lease_statements[5]
        assert all("DROP " not in item.upper() for item in lease_statements)
        assert all("UPDATE st_job_run_v4" not in item for item in lease_statements)
        registry_statements = tuple(MIGRATIONS[2]["statements"])
        assert "CREATE TABLE IF NOT EXISTS st_job_claim_token_v4" in (
            registry_statements[0]
        )
        assert "PRIMARY KEY (lease_token)" in registry_statements[0]
        assert "UNIQUE KEY uk_v4_job_claim_attempt" in registry_statements[0]
        assert "BEFORE INSERT ON st_job_claim_token_v4" in registry_statements[1]
        assert "BEFORE UPDATE ON st_job_claim_token_v4" in registry_statements[2]
        assert "BEFORE DELETE ON st_job_claim_token_v4" in registry_statements[3]
        guard_statements = tuple(MIGRATIONS[3]["statements"])
        assert len(guard_statements) == 16
        assert "BEFORE UPDATE ON st_decision_context_v4" in guard_statements[0]
        assert "BEFORE DELETE ON st_source_watermark_v4" in guard_statements[3]
        assert "BEFORE INSERT ON st_decision_run_v4" in guard_statements[4]
        assert "BEFORE UPDATE ON st_decision_channel_head_v4" in (
            guard_statements[8]
        )
        assert "BEFORE UPDATE ON st_runtime_control_v4" in guard_statements[11]
        assert "BEFORE INSERT ON st_runtime_control_transition_v4" in (
            guard_statements[13]
        )
        assert "BEFORE DELETE ON st_runtime_control_transition_v4" in (
            guard_statements[15]
        )
        assert all("DROP " not in item.upper() for item in guard_statements)
        assert all("ALTER TABLE" not in item.upper() for item in guard_statements)
        combined_ddl = "\n".join(statements)
        for foreign_key in (
            "fk_v4_watermark_context",
            "fk_v4_run_context",
            "fk_v4_head_run_context",
        ):
            assert foreign_key in combined_ddl
        signatures = _expected_schema((MIGRATION_TABLE_DDL, *statements))
        assert signatures["schema_migration_v4"]["indexes"]["PRIMARY"] == {
            "unique": True,
            "columns": ("version",),
        }
        assert signatures["st_decision_context_v4"]["columns"][
            "context_id"
        ] == {
            "type": "varchar(64)",
            "nullable": False,
            "default": None,
        }
        assert signatures["st_decision_context_v4"]["columns"][
            "is_realtime"
        ] == {
            "type": "tinyint(1)",
            "nullable": False,
            "default": "0",
        }
        assert signatures["st_decision_context_v4"]["columns"][
            "random_seed"
        ]["type"] == "bigint unsigned"
        assert signatures["st_decision_context_v4"]["indexes"][
            "uk_v4_context_hash"
        ] == {
            "unique": True,
            "columns": ("context_hash",),
        }
        assert signatures["st_decision_run_v4"]["indexes"][
            "uk_v4_run_context_identity"
        ] == {
            "unique": True,
            "columns": ("run_uid", "context_id"),
        }
        assert signatures["st_decision_channel_head_v4"]["indexes"][
            "idx_v4_head_run"
        ]["columns"] == ("run_uid", "context_id")
        assert signatures["st_decision_channel_head_v4"]["constraints"] == {
            "fk_v4_head_run_context": {
                "columns": ("run_uid", "context_id"),
                "referenced_table": "st_decision_run_v4",
                "referenced_columns": ("run_uid", "context_id"),
                "on_delete": "RESTRICT",
            }
        }
        context_ddl = next(
            item
            for item in statements
            if "st_decision_context_v4" in item
        )
        for required_column in (
            "decision_clock",
            "universe_version",
            "account_snapshot_id",
            "factor_spec_versions_json",
            "forecast_contract_ids_json",
            "model_versions_json",
            "model_artifact_hashes_json",
            "model_training_cutoffs_json",
            "calibration_versions_json",
            "calibration_artifact_hashes_json",
            "capability_statuses_json",
            "portfolio_policy_version",
            "execution_contract_version",
            "fee_schedule_version",
            "random_seed",
        ):
            assert re.search(
                rf"\b{required_column}\b",
                context_ddl,
                flags=re.IGNORECASE,
            )
    finally:
        engine.dispose()


def test_v4_migration_apply_rejects_non_mysql() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with pytest.raises(RuntimeError, match="require MySQL"):
            run_v4_migrations(engine)
    finally:
        engine.dispose()


class _SchemaRows:
    def __init__(self, rows: list[dict[str, Any]]):
        self.rows = rows

    def mappings(self) -> "_SchemaRows":
        return self

    def first(self) -> dict[str, Any] | None:
        return self.rows[0] if self.rows else None

    def __iter__(self):
        return iter(self.rows)


class _SchemaConnection:
    def __init__(self, signature: dict[str, Any]):
        self.signature = signature

    def execute(self, statement, parameters=None) -> _SchemaRows:
        sql = str(statement)
        if "information_schema.TABLES" in sql:
            return _SchemaRows(
                [{"ENGINE": "InnoDB", "TABLE_COLLATION": "utf8mb4_bin"}]
            )
        if "information_schema.COLUMNS" in sql:
            return _SchemaRows(
                [
                    {
                        "COLUMN_NAME": name,
                        "COLUMN_TYPE": details["type"],
                        "IS_NULLABLE": (
                            "YES" if details["nullable"] else "NO"
                        ),
                        "COLUMN_DEFAULT": details["default"],
                    }
                    for name, details in self.signature["columns"].items()
                ]
            )
        if "information_schema.STATISTICS" in sql:
            return _SchemaRows(
                [
                    {
                        "INDEX_NAME": name,
                        "NON_UNIQUE": 0 if details["unique"] else 1,
                        "SEQ_IN_INDEX": position,
                        "COLUMN_NAME": column,
                    }
                    for name, details in self.signature["indexes"].items()
                    for position, column in enumerate(
                        details["columns"], start=1
                    )
                ]
            )
        if "information_schema.KEY_COLUMN_USAGE" in sql:
            return _SchemaRows(
                [
                    {
                        "CONSTRAINT_NAME": name,
                        "COLUMN_NAME": column,
                        "REFERENCED_TABLE_NAME": details[
                            "referenced_table"
                        ],
                        "REFERENCED_COLUMN_NAME": details[
                            "referenced_columns"
                        ][position - 1],
                        "ORDINAL_POSITION": position,
                        "DELETE_RULE": details["on_delete"],
                    }
                    for name, details in self.signature[
                        "constraints"
                    ].items()
                    for position, column in enumerate(
                        details["columns"], start=1
                    )
                ]
            )
        raise AssertionError(f"unexpected schema query: {sql}")


def test_schema_validation_compares_full_column_index_and_fk_signatures() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS child_v4 (
        child_id BIGINT PRIMARY KEY,
        parent_id BIGINT NOT NULL DEFAULT 0,
        label VARCHAR(40) NULL DEFAULT '',
        CONSTRAINT fk_child_parent
            FOREIGN KEY (parent_id)
            REFERENCES parent_v4 (parent_id)
            ON DELETE RESTRICT,
        UNIQUE KEY uk_child_parent_label (parent_id, label),
        KEY idx_child_label (label, parent_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
    """
    expected = _expected_schema((ddl,))["child_v4"]
    _validate_schema_on_connection(
        _SchemaConnection(deepcopy(expected)),
        (ddl,),
    )

    mutations = (
        ("columns", "parent_id", "nullable", True, "column drift"),
        ("columns", "parent_id", "default", "1", "column drift"),
        ("indexes", "uk_child_parent_label", "unique", False, "index drift"),
        (
            "indexes",
            "idx_child_label",
            "columns",
            ("parent_id", "label"),
            "index drift",
        ),
        (
            "constraints",
            "fk_child_parent",
            "on_delete",
            "CASCADE",
            "foreign-key drift",
        ),
    )
    for section, name, field, replacement, message in mutations:
        actual = deepcopy(expected)
        actual[section][name][field] = replacement
        with pytest.raises(RuntimeError, match=message):
            _validate_schema_on_connection(
                _SchemaConnection(actual),
                (ddl,),
            )


def test_migration_apply_reuses_named_lock_connection(monkeypatch) -> None:
    lock_connection = object()
    captured: dict[str, Any] = {}

    class _Dialect:
        name = "mysql"

    class _Engine:
        dialect = _Dialect()

    @contextmanager
    def fake_named_lock(engine, name, *, timeout_seconds):
        captured["lock"] = (engine, name, timeout_seconds)
        yield lock_connection

    def fake_run(engine, *, dry_run, connection=None):
        captured["run"] = (engine, dry_run, connection)
        return []

    monkeypatch.setattr(migrations_v4, "mysql_named_lock", fake_named_lock)
    monkeypatch.setattr(
        migrations_v4,
        "_run_v4_migrations_unlocked",
        fake_run,
    )

    engine = _Engine()
    assert migrations_v4.run_v4_migrations(engine) == []
    assert captured["lock"] == (
        engine,
        "probiga:trading_v4_schema",
        30,
    )
    assert captured["run"] == (engine, False, lock_connection)


def test_context_and_watermarks_are_immutable_and_idempotent(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 0, 30, 0, tzinfo=timezone.utc)
    context = _decision_context(now)

    first = repository.create_or_get_context(context, created_at=now)
    second = repository.create_or_get_context(
        context,
        created_at=now + timedelta(seconds=1),
    )

    assert first.created is True
    assert second.created is False
    assert first.context["context_id"] == context.context_id
    assert first.context["context_hash"] == context.context_hash
    assert first.context["decision_clock"] == DecisionClock.INTRADAY.value
    assert first.context["config_version"] == context.config_hash
    assert json.loads(first.context["data_manifest_json"]) == (
        context.data_manifest.as_dict()
    )
    assert json.loads(first.context["model_available_at_json"]) == (
        context.as_dict()["model_available_at"]
    )
    assert json.loads(first.context["calibration_available_at_json"]) == (
        context.as_dict()["calibration_available_at"]
    )

    with repository.engine.connect() as connection:
        watermark = connection.execute(
            text(
                "SELECT source_key, record_count, snapshot_id, "
                "quality_status, content_hash "
                "FROM st_source_watermark_v4 "
                "WHERE context_id = :context_id"
            ),
            {"context_id": context.context_id},
        ).mappings().one()
    assert watermark["source_key"] == "realtime_quotes"
    assert watermark["record_count"] == 512
    assert watermark["snapshot_id"] == "quotes-snapshot-v1"
    assert watermark["quality_status"] == QualityStatus.PASS.value
    assert re.fullmatch(r"[0-9a-f]{64}", watermark["content_hash"])

    with pytest.raises(DecisionRunConflictError, match="immutable metadata"):
        repository.create_or_get_context(
            context,
            run_mode="AFTER_CLOSE",
            created_at=now + timedelta(seconds=2),
        )


def test_repository_persists_explicit_pit_watermark_evidence(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 0, 30, 0, tzinfo=timezone.utc)
    base = _decision_context(now)
    knowledge_time = now - timedelta(seconds=5)
    watermark = SourceWatermark(
        source="exchange_daily_bar",
        knowledge_time=knowledge_time,
        source_event_at=knowledge_time - timedelta(days=1),
        first_seen_at=knowledge_time - timedelta(seconds=3),
        received_at=knowledge_time - timedelta(seconds=2),
        available_at=knowledge_time - timedelta(seconds=1),
        valid_until=knowledge_time + timedelta(hours=1),
        record_count=512,
        quality_status=QualityStatus.PASS,
        snapshot_id="daily-bars-20260802",
        coverage=Decimal("1"),
        batch_id="batch-20260802-v1",
        schema_version="sm_stock_kline-v2",
        content_hash="f" * 64,
        reason_codes=("ACQUISITION_TIME_VERIFIED",),
    )
    context = replace(
        base,
        source_watermarks={"exchange_daily_bar": watermark},
    )

    repository.create_or_get_context(context, created_at=now)

    with repository.engine.connect() as connection:
        stored = connection.execute(
            text(
                "SELECT source_event_at, first_seen_at, received_at, "
                "available_at, coverage, batch_id, schema_version, "
                "content_hash, details_json FROM st_source_watermark_v4 "
                "WHERE context_id = :context_id "
                "AND source_key = 'exchange_daily_bar'"
            ),
            {"context_id": context.context_id},
        ).mappings().one()
    assert stored["source_event_at"] is not None
    assert stored["first_seen_at"] is not None
    assert stored["received_at"] is not None
    assert stored["available_at"] is not None
    assert Decimal(str(stored["coverage"])) == Decimal("1")
    assert stored["batch_id"] == "batch-20260802-v1"
    assert stored["schema_version"] == "sm_stock_kline-v2"
    assert stored["content_hash"] == "f" * 64
    details = json.loads(stored["details_json"])
    assert details["valid_until"].startswith("2026-08-03T01:29:55")
    assert details["reason_codes"] == ["ACQUISITION_TIME_VERIFIED"]


def test_repository_rejects_decision_context_subclasses(
    repository: TradingV4Repository,
) -> None:
    class ForgedDecisionContext(DecisionContext):
        pass

    forged = object.__new__(ForgedDecisionContext)
    with pytest.raises(TypeError, match="exactly DecisionContext"):
        repository.create_or_get_context(forged)


@pytest.mark.parametrize(
    ("field_name", "artifact_name"),
    (
        ("model_training_cutoffs", "cross_sectional"),
        ("model_available_at", "cross_sectional"),
        ("calibration_training_cutoffs", "cross_sectional"),
        ("calibration_available_at", "cross_sectional"),
    ),
)
def test_artifact_timing_is_part_of_persisted_model_set_identity(
    repository: TradingV4Repository,
    field_name: str,
    artifact_name: str,
) -> None:
    now = datetime(2026, 8, 3, 0, 30, 0, tzinfo=timezone.utc)
    first_context = _decision_context(now)
    original_mapping = getattr(first_context, field_name)
    second_context = replace(
        first_context,
        **{
            field_name: {
                artifact_name: (
                    original_mapping[artifact_name] - timedelta(hours=1)
                )
            }
        },
    )

    first = repository.create_or_get_context(first_context, created_at=now)
    second = repository.create_or_get_context(
        second_context,
        created_at=now + timedelta(seconds=1),
    )

    assert first.context["context_hash"] != second.context["context_hash"]
    assert (
        first.context["model_set_version"]
        != second.context["model_set_version"]
    )


@pytest.mark.parametrize(
    ("column", "replacement"),
    (
        ("source_event_at", "2026-08-03 00:29:59"),
        ("available_at", "2026-08-03 00:29:58"),
        ("record_count", 999),
        ("coverage", "0.50000000"),
        ("lag_seconds", 999),
        ("schema_version", "tampered"),
        ("details_json", '{"tampered":true}'),
        ("created_at", "2026-08-03 00:00:00"),
    ),
)
def test_watermark_retry_checks_every_persisted_field(
    repository: TradingV4Repository,
    column: str,
    replacement: Any,
) -> None:
    now = datetime(2026, 8, 3, 0, 30, 0, tzinfo=timezone.utc)
    context = _decision_context(now)
    repository.create_or_get_context(context, created_at=now)
    with repository.engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE st_source_watermark_v4 SET {column} = :replacement "
                "WHERE context_id = :context_id"
            ),
            {
                "replacement": replacement,
                "context_id": context.context_id,
            },
        )

    with pytest.raises(DecisionRunConflictError, match="immutable fields"):
        repository.create_or_get_context(
            context,
            created_at=now + timedelta(seconds=1),
        )


def test_run_versions_must_match_context(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 0, 45, 0, tzinfo=timezone.utc)
    _insert_context(repository, "context-version", decision_at=now)

    with pytest.raises(DecisionRunConflictError, match="immutable context"):
        repository.create_or_get_run(
            context_id="context-version",
            account_id="paper-v4",
            channel="shadow",
            run_type="INTRADAY",
            trigger_type="SCHEDULED",
            model_set_version="wrong-model-version",
            config_version="v4-config-test",
            code_commit_sha="test-commit",
            run_uid="run-version-mismatch",
            created_at=now,
        )


def test_head_composite_foreign_key_binds_run_to_its_context(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 0, 50, 0, tzinfo=timezone.utc)
    _insert_context(repository, "context-fk-run", decision_at=now)
    _insert_context(repository, "context-fk-other", decision_at=now)
    _create_run(
        repository,
        context_id="context-fk-run",
        run_uid="run-fk",
        created_at=now,
    )

    with pytest.raises(IntegrityError):
        with repository.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO st_decision_channel_head_v4 (
                        channel, account_id, run_uid, context_id,
                        head_version, published_at, published_by, updated_at
                    ) VALUES (
                        'shadow', 'paper-v4', 'run-fk', 'context-fk-other',
                        1, :published_at, 'test', :published_at
                    )
                    """
                ),
                {"published_at": now},
            )


def test_create_run_is_idempotent_and_rejects_key_reuse(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 1, 0, 0, tzinfo=timezone.utc)
    _insert_context(repository, "context-a", decision_at=now)

    first = _create_run(
        repository,
        context_id="context-a",
        run_uid="run-a",
        created_at=now,
    )
    second = _create_run(
        repository,
        context_id="context-a",
        run_uid="run-b",
        created_at=now + timedelta(seconds=1),
    )

    assert first.created is True
    assert second.created is False
    assert first.run["run_uid"] == second.run["run_uid"] == "run-a"
    assert second.run["status"] == RUN_CREATED

    with pytest.raises(DecisionRunConflictError, match="does not match run inputs"):
        repository.create_or_get_run(
            context_id="context-a",
            account_id="paper-v4",
            channel="paper",
            run_type="INTRADAY",
            trigger_type="SCHEDULED",
            model_set_version="v4-model-test",
            config_version="v4-config-test",
            code_commit_sha="test-commit",
            run_uid="run-c",
            run_idempotency_key=first.run["run_idempotency_key"],
            created_at=now,
        )


def test_run_state_machine_and_idempotent_head_publication(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 2, 0, 0, tzinfo=timezone.utc)
    _insert_context(repository, "context-b", decision_at=now)
    created = _create_run(
        repository,
        context_id="context-b",
        run_uid="run-state",
        created_at=now,
    )

    with pytest.raises(InvalidRunTransitionError, match="expected VALIDATING"):
        repository.commit_run(
            created.run["run_uid"],
            result_hash="a" * 64,
            occurred_at=now,
        )
    with pytest.raises(HeadPublishError, match="only COMMITTED"):
        repository.publish_committed_head(created.run["run_uid"])

    repository.mark_running(
        created.run["run_uid"], occurred_at=now + timedelta(seconds=1)
    )
    repository.mark_validating(
        created.run["run_uid"], occurred_at=now + timedelta(seconds=2)
    )
    committed = repository.commit_and_publish_head(
        created.run["run_uid"],
        result_hash="b" * 64,
        published_by="test",
        occurred_at=now + timedelta(seconds=3),
        expected_head_version=0,
    )

    assert committed.run["status"] == RUN_COMMITTED
    assert committed.publication.changed is True
    assert committed.publication.previous_run_uid is None
    assert committed.publication.head["head_version"] == 1
    assert committed.publication.head["run_uid"] == "run-state"

    retried_commit = repository.commit_and_publish_head(
        created.run["run_uid"],
        result_hash="b" * 64,
        published_by="test-retry",
        occurred_at=now + timedelta(seconds=3),
        expected_head_version=0,
    )
    assert retried_commit.run["status"] == RUN_COMMITTED
    assert retried_commit.publication.changed is False
    assert retried_commit.publication.head["head_version"] == 1

    with pytest.raises(DecisionRunConflictError, match="different result_hash"):
        repository.commit_and_publish_head(
            created.run["run_uid"],
            result_hash="9" * 64,
            occurred_at=now + timedelta(seconds=4),
        )

    repeated = repository.publish_committed_head(
        "run-state",
        published_by="test-retry",
        published_at=now + timedelta(seconds=4),
        expected_head_version=0,
    )
    assert repeated.changed is False
    assert repeated.head["head_version"] == 1

    with pytest.raises(HeadPublishError, match="published_at"):
        repository.publish_committed_head(
            "run-state",
            published_at=now + timedelta(seconds=2),
        )


def test_run_transitions_reject_time_rollback(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 2, 30, 0, tzinfo=timezone.utc)
    _insert_context(repository, "context-clock", decision_at=now)
    _create_run(
        repository,
        context_id="context-clock",
        run_uid="run-clock",
        created_at=now,
    )

    with pytest.raises(InvalidRunTransitionError, match="time-monotonic"):
        repository.mark_running(
            "run-clock",
            occurred_at=now - timedelta(microseconds=1),
        )
    assert repository.get_run("run-clock")["status"] == RUN_CREATED


def test_fail_context_cannot_commit_or_publish(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 3, 0, 0, tzinfo=timezone.utc)
    _insert_context(
        repository,
        "context-fail",
        decision_at=now,
        freshness_status="FAIL",
    )
    _create_run(
        repository,
        context_id="context-fail",
        run_uid="run-fail-context",
        created_at=now,
    )
    repository.mark_running("run-fail-context", occurred_at=now)
    repository.mark_validating("run-fail-context", occurred_at=now)

    with pytest.raises(InvalidRunTransitionError, match="FAIL decision"):
        repository.commit_and_publish_head(
            "run-fail-context",
            result_hash="c" * 64,
            occurred_at=now,
        )

    stored = repository.get_run("run-fail-context")
    assert stored is not None
    assert stored["status"] == RUN_VALIDATING
    assert repository.get_head("shadow", account_id="paper-v4") is None


def test_head_cas_failure_rolls_back_commit(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 4, 0, 0, tzinfo=timezone.utc)
    _insert_context(repository, "context-head-1", decision_at=now)
    _create_run(
        repository,
        context_id="context-head-1",
        run_uid="run-head-1",
        created_at=now,
    )
    repository.mark_running("run-head-1", occurred_at=now)
    repository.mark_validating("run-head-1", occurred_at=now)
    repository.commit_and_publish_head(
        "run-head-1",
        result_hash="d" * 64,
        occurred_at=now + timedelta(seconds=1),
        expected_head_version=0,
    )

    _insert_context(
        repository,
        "context-head-2",
        decision_at=now + timedelta(minutes=1),
    )
    _create_run(
        repository,
        context_id="context-head-2",
        run_uid="run-head-2",
        created_at=now + timedelta(minutes=1),
    )
    repository.mark_running(
        "run-head-2",
        occurred_at=now + timedelta(minutes=1),
    )
    repository.mark_validating(
        "run-head-2",
        occurred_at=now + timedelta(minutes=1),
    )

    with pytest.raises(HeadPublishConflictError, match="head version changed"):
        repository.commit_and_publish_head(
            "run-head-2",
            result_hash="e" * 64,
            occurred_at=now + timedelta(minutes=1, seconds=1),
            expected_head_version=0,
        )

    run = repository.get_run("run-head-2")
    head = repository.get_head("shadow", account_id="paper-v4")
    assert run is not None and run["status"] == RUN_VALIDATING
    assert head is not None and head["run_uid"] == "run-head-1"
    assert head["head_version"] == 1


def test_head_published_at_cannot_move_backwards(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 4, 30, 0, tzinfo=timezone.utc)
    _insert_context(repository, "context-published-1", decision_at=now)
    _create_run(
        repository,
        context_id="context-published-1",
        run_uid="run-published-1",
        created_at=now,
    )
    repository.mark_running("run-published-1", occurred_at=now)
    repository.mark_validating("run-published-1", occurred_at=now)
    repository.commit_run(
        "run-published-1",
        result_hash="7" * 64,
        occurred_at=now + timedelta(seconds=1),
    )
    repository.publish_committed_head(
        "run-published-1",
        published_at=now + timedelta(seconds=10),
        expected_head_version=0,
    )

    _insert_context(repository, "context-published-2", decision_at=now)
    _create_run(
        repository,
        context_id="context-published-2",
        run_uid="run-published-2",
        created_at=now,
    )
    repository.mark_running("run-published-2", occurred_at=now)
    repository.mark_validating(
        "run-published-2",
        occurred_at=now + timedelta(seconds=1),
    )
    repository.commit_run(
        "run-published-2",
        result_hash="8" * 64,
        occurred_at=now + timedelta(seconds=2),
    )

    with pytest.raises(
        HeadPublishConflictError,
        match="published_at cannot precede the current head",
    ):
        repository.publish_committed_head(
            "run-published-2",
            published_at=now + timedelta(seconds=5),
            expected_head_version=1,
        )
    head = repository.get_head("shadow", account_id="paper-v4")
    assert head is not None and head["run_uid"] == "run-published-1"
    assert head["head_version"] == 1


def test_older_committed_run_cannot_replace_newer_head(
    repository: TradingV4Repository,
) -> None:
    now = datetime(2026, 8, 3, 5, 0, 0, tzinfo=timezone.utc)
    _insert_context(repository, "context-old", decision_at=now)
    _create_run(
        repository,
        context_id="context-old",
        run_uid="run-old",
        created_at=now,
    )
    repository.mark_running("run-old", occurred_at=now)
    repository.mark_validating("run-old", occurred_at=now)
    repository.commit_run(
        "run-old",
        result_hash="f" * 64,
        occurred_at=now + timedelta(seconds=1),
    )

    _insert_context(
        repository,
        "context-new",
        decision_at=now + timedelta(minutes=1),
    )
    _create_run(
        repository,
        context_id="context-new",
        run_uid="run-new",
        created_at=now + timedelta(minutes=1),
    )
    repository.mark_running(
        "run-new",
        occurred_at=now + timedelta(minutes=1),
    )
    repository.mark_validating(
        "run-new",
        occurred_at=now + timedelta(minutes=1),
    )
    repository.commit_and_publish_head(
        "run-new",
        result_hash="1" * 64,
        occurred_at=now + timedelta(minutes=1, seconds=1),
    )

    with pytest.raises(HeadPublishConflictError, match="older decision context"):
        repository.publish_committed_head(
            "run-old",
            published_at=now + timedelta(minutes=2),
        )
    assert repository.get_head(
        "shadow", account_id="paper-v4"
    )["run_uid"] == "run-new"
