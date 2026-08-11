from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import server.trading_v4.infrastructure.factor_store as factor_store_module

from server.trading_v4.infrastructure import (
    FactorStoreConflictError,
    FactorStoreIntegrityError,
    FactorStoreRepository,
    FactorStoreTransactionError,
    run_factor_store_transaction,
)
from server.trading_v4.domain import (
    AvailabilityStatus,
    FactorDefinition,
    FactorRole,
    QualityStatus,
    ResearchStatus,
    ScopeType,
)
from server.trading_v4.ports import (
    EntityFeatureSnapshotRecord,
    FactorDefinitionRecord,
    FactorStorePort,
    SourceCertificationRecord,
)


NOW = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
H1 = "1" * 64
H2 = "2" * 64
H3 = "3" * 64
H4 = "4" * 64


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        for ddl in (
            """CREATE TABLE st_data_source_certification_v4 (
                source_key TEXT NOT NULL,
                certification_version TEXT NOT NULL,
                source_table TEXT NOT NULL,
                event_time_column TEXT NOT NULL,
                knowledge_time_columns_json TEXT NOT NULL,
                replay_eligibility TEXT NOT NULL,
                certification_status TEXT NOT NULL,
                availability_status TEXT NOT NULL,
                research_status TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                valid_from DATETIME NOT NULL,
                valid_to DATETIME,
                contract_json TEXT NOT NULL,
                evidence_hash TEXT NOT NULL,
                certified_by TEXT NOT NULL,
                certified_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (source_key, certification_version)
            )""",
            """CREATE TABLE st_factor_definition_v4 (
                factor_key TEXT NOT NULL,
                factor_version TEXT NOT NULL,
                feature_set_version TEXT NOT NULL,
                factor_role TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                availability_status TEXT NOT NULL,
                research_status TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                missing_policy TEXT NOT NULL,
                pit_eligible INTEGER NOT NULL,
                max_age_seconds INTEGER NOT NULL,
                required_source_keys_json TEXT NOT NULL,
                required_source_certifications_json TEXT NOT NULL,
                formula_json TEXT NOT NULL,
                output_schema_json TEXT NOT NULL,
                definition_hash TEXT NOT NULL,
                available_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL,
                PRIMARY KEY (factor_key, factor_version),
                UNIQUE (factor_key, feature_set_version)
            )""",
            """CREATE TABLE st_decision_context_v4 (
                context_id TEXT PRIMARY KEY,
                knowledge_cutoff_at DATETIME NOT NULL,
                decision_at DATETIME NOT NULL,
                data_snapshot_hash TEXT NOT NULL
            )""",
            """CREATE TABLE st_decision_run_v4 (
                run_uid TEXT PRIMARY KEY,
                context_id TEXT NOT NULL,
                status TEXT NOT NULL
            )""",
            """CREATE TABLE st_entity_feature_snapshot_v4 (
                snapshot_id TEXT PRIMARY KEY,
                run_uid TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                feature_set_version TEXT NOT NULL,
                knowledge_cutoff_at DATETIME NOT NULL,
                computed_at DATETIME NOT NULL,
                available_at DATETIME NOT NULL,
                factor_count INTEGER NOT NULL,
                values_json TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                quality_json TEXT NOT NULL,
                source_certifications_json TEXT NOT NULL,
                factor_definitions_json TEXT NOT NULL,
                source_manifest_hash TEXT NOT NULL,
                feature_hash TEXT NOT NULL,
                created_at DATETIME NOT NULL,
                UNIQUE (run_uid, scope_type, scope_id, feature_set_version)
            )""",
        ):
            connection.execute(text(ddl))
    return engine


def _source() -> SourceCertificationRecord:
    return SourceCertificationRecord(
        source_key="daily_price",
        certification_version="v4:cert:1",
        source_table="daily_price_pit",
        event_time_column="trade_at",
        knowledge_time_columns=("ingested_at", "known_at"),
        replay_eligibility="PIT_CERTIFIED",
        certification_status="PASSED",
        availability_status="ACTIVE",
        research_status="BACKTEST_READY",
        quality_status="PASS",
        valid_from=NOW - timedelta(days=1),
        valid_to=None,
        contract={
            "primary_key": ["symbol", "trade_at"],
            "revision_policy": "APPEND_ONLY_REVISION_CHAIN",
        },
        evidence_hash=H1,
        certified_by="stage3-test",
        certified_at=NOW,
        created_at=NOW,
    )


def _factor() -> FactorDefinitionRecord:
    return FactorDefinitionRecord(
        factor_key="momentum_20d",
        factor_version="v4:factor:1",
        feature_set_version="v4:features:1",
        factor_role="ALPHA",
        scope_type="INSTRUMENT",
        availability_status="ACTIVE",
        research_status="BACKTEST_READY",
        quality_status="PASS",
        missing_policy="BLOCK",
        pit_eligible=True,
        max_age_seconds=86_400,
        required_source_keys=("daily_price",),
        required_source_certifications=(
            {
                "source_key": "daily_price",
                "certification_version": "v4:cert:1",
                "evidence_hash": H1,
            },
        ),
        formula={"op": "return", "window": 20},
        output_schema={"fields": ["momentum_20d"]},
        definition_hash=H2,
        available_at=NOW,
        created_at=NOW,
    )


def _snapshot(*, cutoff: datetime = NOW) -> EntityFeatureSnapshotRecord:
    return EntityFeatureSnapshotRecord(
        snapshot_id=H3,
        run_uid="run-stage3",
        scope_type="INSTRUMENT",
        scope_id="600000.SH",
        feature_set_version="v4:features:1",
        knowledge_cutoff_at=cutoff,
        computed_at=NOW + timedelta(seconds=1),
        available_at=NOW + timedelta(seconds=2),
        factor_count=1,
        values={"momentum_20d": "0.125"},
        quality_status="PASS",
        quality={
            "status": "PASS",
            "feature_knowledge_time": NOW.isoformat(),
            "valid_until": (NOW + timedelta(minutes=5)).isoformat(),
        },
        source_certifications=(
            {
                "source_key": "daily_price",
                "certification_version": "v4:cert:1",
                "evidence_hash": H1,
            },
        ),
        factor_definitions=(
            {
                "factor_key": "momentum_20d",
                "factor_version": "v4:factor:1",
                "definition_hash": H2,
            },
        ),
        source_manifest_hash=H1,
        feature_hash=H4,
        created_at=NOW + timedelta(seconds=3),
    )


def _insert_run(connection, *, cutoff: datetime = NOW, status: str = "RUNNING") -> None:
    connection.execute(
        text(
            "INSERT INTO st_decision_context_v4 "
            "VALUES (:id, :cutoff, :decision, :data_snapshot_hash)"
        ),
        {
            "id": "context-stage3",
            "cutoff": cutoff.replace(tzinfo=None),
            "decision": NOW.replace(tzinfo=None),
            "data_snapshot_hash": H1,
        },
    )
    connection.execute(
        text("INSERT INTO st_decision_run_v4 VALUES (:run_uid, :context_id, :status)"),
        {"run_uid": "run-stage3", "context_id": "context-stage3", "status": status},
    )


def _append_source_and_factor(
    store: FactorStoreRepository,
    connection,
    *,
    source: SourceCertificationRecord | None = None,
    factor: FactorDefinitionRecord | None = None,
) -> None:
    store.append_source_certification(connection, source or _source())
    store.append_factor_definition(connection, factor or _factor())


class _MysqlRecordingConnection:
    """Exercise the MySQL branch against SQLite while retaining SQL order."""

    dialect = SimpleNamespace(name="mysql")

    def __init__(self, connection):
        self.connection = connection
        self.statements: list[str] = []

    def in_transaction(self) -> bool:
        return self.connection.in_transaction()

    def execute(self, statement, parameters=None):
        sql = str(statement)
        self.statements.append(sql)
        sqlite_sql = sql.rstrip()
        for suffix in ("FOR UPDATE", "LOCK IN SHARE MODE"):
            if sqlite_sql.endswith(suffix):
                sqlite_sql = sqlite_sql[: -len(suffix)].rstrip()
                break
        return self.connection.execute(text(sqlite_sql), parameters or {})


def test_port_and_adapter_are_connection_explicit() -> None:
    store = FactorStoreRepository()
    assert isinstance(store, FactorStorePort)
    engine = _engine()
    with engine.connect() as connection:
        with pytest.raises(FactorStoreTransactionError, match="caller-owned"):
            store.append_source_certification(connection, _source())


def test_mysql_idempotency_reads_are_current_locking_reads() -> None:
    for statement in (
        factor_store_module._SOURCE_BY_ID_FOR_UPDATE,
        factor_store_module._FACTOR_BY_ID_FOR_UPDATE,
        factor_store_module._FACTOR_BY_NATURAL_KEY_FOR_UPDATE,
        factor_store_module._SNAPSHOT_BY_ID_FOR_UPDATE,
        factor_store_module._SNAPSHOT_BY_NATURAL_KEY_FOR_UPDATE,
    ):
        assert statement.rstrip().endswith("FOR UPDATE")
    for statement in (
        factor_store_module._SOURCE_BY_ID_LOCK_IN_SHARE_MODE,
        factor_store_module._FACTOR_BY_ID_LOCK_IN_SHARE_MODE,
        factor_store_module._FACTOR_BY_NATURAL_KEY_LOCK_IN_SHARE_MODE,
        factor_store_module._SNAPSHOT_BY_ID_LOCK_IN_SHARE_MODE,
        factor_store_module._SNAPSHOT_BY_NATURAL_KEY_LOCK_IN_SHARE_MODE,
    ):
        assert statement.rstrip().endswith("LOCK IN SHARE MODE")


def test_mysql_first_writer_probes_do_not_take_missing_gap_locks() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.connect() as raw:
        transaction = raw.begin()
        connection = _MysqlRecordingConnection(raw)

        first = store.append_source_certification(connection, _source())
        assert first.created is True
        assert connection.statements[:3] == [
            factor_store_module._SOURCE_BY_ID,
            factor_store_module._SOURCE_INSERT,
            factor_store_module._SOURCE_BY_ID_LOCK_IN_SHARE_MODE,
        ]

        connection.statements.clear()
        replay = store.append_source_certification(connection, _source())
        assert replay.created is False
        assert connection.statements == [
            factor_store_module._SOURCE_BY_ID,
            factor_store_module._SOURCE_BY_ID_LOCK_IN_SHARE_MODE,
        ]

        connection.statements.clear()
        factor = store.append_factor_definition(connection, _factor())
        assert factor.created is True
        assert connection.statements == [
            factor_store_module._SOURCE_BY_ID,
            factor_store_module._SOURCE_BY_ID_LOCK_IN_SHARE_MODE,
            factor_store_module._FACTOR_BY_ID,
            factor_store_module._FACTOR_BY_NATURAL_KEY,
            factor_store_module._FACTOR_INSERT,
            factor_store_module._FACTOR_BY_ID_LOCK_IN_SHARE_MODE,
            factor_store_module._FACTOR_BY_NATURAL_KEY_LOCK_IN_SHARE_MODE,
        ]
        transaction.commit()


def test_mysql_missing_lineage_probe_is_nonlocking() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.connect() as raw:
        transaction = raw.begin()
        connection = _MysqlRecordingConnection(raw)
        with pytest.raises(FactorStoreIntegrityError, match="does not exist"):
            store.append_factor_definition(connection, _factor())
        assert connection.statements == [factor_store_module._SOURCE_BY_ID]
        assert raw.in_transaction() is True
        transaction.rollback()


def test_transaction_retry_wrapper_restarts_the_complete_mysql_transaction() -> None:
    class _Deadlock(Exception):
        pass

    class _Begin:
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            return self.connection

        def __exit__(self, *_args):
            return False

    class _Engine:
        dialect = SimpleNamespace(name="mysql")

        def __init__(self):
            self.begins = 0

        def begin(self):
            self.begins += 1
            return _Begin(object())

    engine = _Engine()
    calls = 0

    def operation(_connection):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OperationalError("INSERT", {}, _Deadlock(1213, "deadlock"))
        return "committed"

    assert run_factor_store_transaction(
        engine, operation, max_attempts=2, base_delay_seconds=0
    ) == "committed"
    assert calls == 2
    assert engine.begins == 2


@pytest.mark.skipif(
    not os.environ.get("V4_FACTOR_STORE_TEST_MYSQL_URL"),
    reason="requires an explicit isolated FactorStore MySQL test URL",
)
def test_real_mysql_rr_four_writer_source_and_factor_idempotency() -> None:
    url = os.environ["V4_FACTOR_STORE_TEST_MYSQL_URL"]
    engine = create_engine(url, future=True, pool_size=8, max_overflow=0)
    database = str(engine.url.database or "")
    assert "_v4_test" in database or "_v4_ci" in database
    assert engine.dialect.name == "mysql"
    store = FactorStoreRepository()
    suffix = uuid4().hex[:12]
    source = replace(
        _source(),
        source_key=f"rr_source_{suffix}",
        certification_version=f"v4:cert:rr:{suffix}",
    )
    factor = replace(
        _factor(),
        factor_key=f"rr_factor_{suffix}",
        factor_version=f"v4:factor:rr:{suffix}",
        feature_set_version=f"v4:features:rr:{suffix}",
        required_source_keys=(source.source_key,),
        required_source_certifications=(
            {
                "source_key": source.source_key,
                "certification_version": source.certification_version,
                "evidence_hash": source.evidence_hash,
            },
        ),
    )

    def race(operation):
        barrier = Barrier(4)

        def worker(_index: int):
            with engine.begin() as connection:
                assert connection.execute(text("SELECT @@tx_isolation")).scalar_one() == "REPEATABLE-READ"
                barrier.wait(timeout=10)
                return operation(connection)

        with ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(worker, range(4)))

    try:
        with engine.connect() as connection:
            grants = [str(row[0]).upper() for row in connection.execute(text("SHOW GRANTS"))]
        scoped = [grant for grant in grants if database.upper() in grant]
        assert scoped
        assert all("SELECT" in grant and "INSERT" in grant for grant in scoped)
        assert all(
            forbidden not in " ".join(scoped)
            for forbidden in (" UPDATE ", " DELETE ", " DROP ", " ALTER ")
        )

        source_fresh = race(
            lambda connection: store.append_source_certification(connection, source)
        )
        source_replay = race(
            lambda connection: store.append_source_certification(connection, source)
        )
        factor_fresh = race(
            lambda connection: store.append_factor_definition(connection, factor)
        )
        factor_replay = race(
            lambda connection: store.append_factor_definition(connection, factor)
        )

        assert sum(result.created for result in source_fresh) == 1
        assert sum(result.created for result in factor_fresh) == 1
        assert all(not result.created for result in source_replay)
        assert all(not result.created for result in factor_replay)
        with engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT COUNT(*) FROM st_data_source_certification_v4 "
                    "WHERE source_key = :source_key"
                ),
                {"source_key": source.source_key},
            ).scalar_one() == 1
            assert connection.execute(
                text(
                    "SELECT COUNT(*) FROM st_factor_definition_v4 "
                    "WHERE factor_key = :factor_key"
                ),
                {"factor_key": factor.factor_key},
            ).scalar_one() == 1
    finally:
        engine.dispose()


def test_source_and_factor_appends_are_exactly_idempotent_and_caller_owned() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.connect() as connection:
        transaction = connection.begin()
        first_source = store.append_source_certification(connection, _source())
        second_source = store.append_source_certification(connection, _source())
        first_factor = store.append_factor_definition(connection, _factor())
        second_factor = store.append_factor_definition(connection, _factor())

        assert first_source.created is True
        assert second_source.created is False
        assert first_factor.created is True
        assert second_factor.created is False
        assert connection.in_transaction() is True
        assert second_source.record == _source()
        assert second_factor.record == _factor()
        transaction.commit()


def test_domain_factor_pit_flag_follows_research_status_not_actionable() -> None:
    definition = FactorDefinition(
        factor_key="risk_gate",
        factor_version="v4:factor:risk-gate:1",
        role=FactorRole.RISK,
        scope_type=ScopeType.INSTRUMENT,
        feature_set_version="v4:features:risk:1",
        builder_version="v4:builder:risk:1",
        required_source_versions={"daily_price": "v4:source:daily-price:1"},
        required_fields={"daily_price": ("close",)},
        output_fields=("risk_gate",),
        missing_policy="BLOCK",
        availability_status=AvailabilityStatus.BLOCKED,
        research_status=ResearchStatus.BACKTEST_READY,
        quality_status=QualityStatus.PASS,
        available_at=NOW,
        formula_hash=H1,
        max_age_seconds=60,
    )
    assert definition.actionable is False

    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        store.append_source_certification(connection, _source())
        result = store.append_factor_definition(
            connection,
            definition,
            created_at=NOW,
            source_certifications=_factor().required_source_certifications,
        )
        assert result.created is True
        assert result.record.pit_eligible is True
        assert result.record.formula["definition_hash"] == definition.definition_hash


def test_same_source_identity_or_factor_natural_key_with_other_content_fails_closed() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        store.append_source_certification(connection, _source())
        with pytest.raises(FactorStoreConflictError, match="different immutable content"):
            store.append_source_certification(
                connection, replace(_source(), evidence_hash=H2)
            )

        store.append_factor_definition(connection, _factor())
        with pytest.raises(FactorStoreConflictError, match="different immutable content"):
            store.append_factor_definition(
                connection,
                replace(_factor(), factor_version="v4:factor:2", definition_hash=H3),
            )


def test_raw_pit_source_requires_uppercase_allowlisted_revision_policy() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        for policy, message in (
            ("current_only", "uppercase"),
            ("CURRENT_ONLY", "safe revision"),
            ("UNKNOWN_FUTURE_POLICY", "safe revision"),
        ):
            with pytest.raises(ValueError, match=message):
                store.append_source_certification(
                    connection,
                    replace(
                        _source(),
                        contract={"revision_policy": policy},
                    ),
                )


def test_feature_snapshot_binds_exact_run_cutoff_and_is_idempotent() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        _insert_run(connection)
        _append_source_and_factor(store, connection)
        first = store.append_feature_snapshot(connection, _snapshot())
        second = store.append_feature_snapshot(connection, _snapshot())
        assert first.created is True
        assert second.created is False
        assert second.record == _snapshot()

        with pytest.raises(FactorStoreConflictError, match="different immutable content"):
            store.append_feature_snapshot(
                connection,
                replace(_snapshot(), snapshot_id="5" * 64, feature_hash="6" * 64),
            )


@pytest.mark.parametrize("status", ["CREATED", "FAILED", "CANCELLED"])
def test_feature_snapshot_rejects_ineligible_run_state(status: str) -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        _insert_run(connection, status=status)
        with pytest.raises(FactorStoreIntegrityError, match="eligible state"):
            store.append_feature_snapshot(connection, _snapshot())


def test_feature_snapshot_rejects_a_different_run_cutoff() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        _insert_run(connection, cutoff=NOW - timedelta(seconds=1))
        with pytest.raises(FactorStoreIntegrityError, match="cutoff"):
            store.append_feature_snapshot(connection, _snapshot())


def test_feature_snapshot_rejects_a_manifest_outside_its_run_context() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        _insert_run(connection)
        store.append_source_certification(connection, _source())
        with pytest.raises(FactorStoreIntegrityError, match="source manifest"):
            store.append_feature_snapshot(
                connection,
                replace(_snapshot(), source_manifest_hash=H2),
            )


def test_feature_snapshot_rejects_missing_expired_or_conflicting_lineage() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        _insert_run(connection)
        with pytest.raises(FactorStoreIntegrityError, match="does not exist"):
            store.append_feature_snapshot(connection, _snapshot())

        expired = replace(_source(), valid_to=NOW - timedelta(seconds=1))
        store.append_source_certification(connection, expired)
        with pytest.raises(FactorStoreIntegrityError, match="invalid at cutoff"):
            store.append_feature_snapshot(connection, _snapshot())

    engine = _engine()
    with engine.begin() as connection:
        _insert_run(connection)
        store.append_source_certification(connection, _source())
        conflicting = replace(
            _snapshot(),
            source_certifications=(
                {
                    "source_key": "daily_price",
                    "certification_version": "v4:cert:1",
                    "evidence_hash": H2,
                },
            ),
        )
        with pytest.raises(FactorStoreIntegrityError, match="hash conflicts"):
            store.append_feature_snapshot(connection, conflicting)


def test_forward_only_source_and_factor_support_an_explicitly_degraded_snapshot() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    forward_source = replace(
        _source(),
        replay_eligibility="FORWARD_ONLY",
        research_status="FORWARD_ONLY",
    )
    forward_factor = replace(
        _factor(),
        research_status="FORWARD_ONLY",
        pit_eligible=False,
    )
    degraded = replace(
        _snapshot(),
        quality_status="WARN",
        quality={
            "status": "WARN",
            "reason_codes": ["SOURCE_FORWARD_ONLY"],
            "feature_knowledge_time": NOW.isoformat(),
            "valid_until": (NOW + timedelta(minutes=5)).isoformat(),
        },
    )
    with engine.begin() as connection:
        _insert_run(connection)
        store.append_source_certification(connection, forward_source)
        factor_result = store.append_factor_definition(connection, forward_factor)
        snapshot_result = store.append_feature_snapshot(connection, degraded)

        assert factor_result.record.pit_eligible is False
        assert snapshot_result.created is True
        assert snapshot_result.record.quality_status == "WARN"


@pytest.mark.parametrize(
    ("quality_status", "quality", "message"),
    [
        (
            "PASS",
            {
                "status": "PASS",
                "feature_knowledge_time": NOW.isoformat(),
                "valid_until": (NOW + timedelta(minutes=5)).isoformat(),
            },
            "cannot support a PASS",
        ),
        (
            "WARN",
            {
                "status": "WARN",
                "reason_codes": [],
                "feature_knowledge_time": NOW.isoformat(),
                "valid_until": (NOW + timedelta(minutes=5)).isoformat(),
            },
            "SOURCE_FORWARD_ONLY",
        ),
    ],
)
def test_forward_only_source_cannot_silently_produce_snapshot_quality(
    quality_status: str,
    quality: dict[str, object],
    message: str,
) -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        _insert_run(connection)
        store.append_source_certification(
            connection,
            replace(
                _source(),
                replay_eligibility="FORWARD_ONLY",
                research_status="FORWARD_ONLY",
            ),
        )
        with pytest.raises(FactorStoreIntegrityError, match=message):
            store.append_feature_snapshot(
                connection,
                replace(
                    _snapshot(),
                    quality_status=quality_status,
                    quality=quality,
                ),
            )


@pytest.mark.parametrize(
    ("replay", "research"),
    [
        ("DISPLAY_ONLY", "DISPLAY_ONLY"),
        ("REPLAY_INELIGIBLE", "FORWARD_ONLY"),
    ],
)
def test_noncomputational_source_cannot_enter_a_feature_snapshot(
    replay: str,
    research: str,
) -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        _insert_run(connection)
        store.append_source_certification(
            connection,
            replace(
                _source(),
                replay_eligibility=replay,
                research_status=research,
            ),
        )
        with pytest.raises(FactorStoreIntegrityError, match="display-only"):
            store.append_feature_snapshot(
                connection,
                replace(
                    _snapshot(),
                    quality_status="WARN",
                    quality={
                        "status": "WARN",
                        "reason_codes": ["SOURCE_FORWARD_ONLY"],
                        "feature_knowledge_time": NOW.isoformat(),
                        "valid_until": (NOW + timedelta(minutes=5)).isoformat(),
                    },
                ),
            )


def test_committed_run_allows_only_an_exact_existing_snapshot_replay() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.begin() as connection:
        _insert_run(connection, status="COMMITTED")
        store.append_source_certification(connection, _source())
        with pytest.raises(FactorStoreIntegrityError, match="eligible state"):
            store.append_feature_snapshot(connection, _snapshot())

    engine = _engine()
    with engine.begin() as connection:
        _insert_run(connection)
        _append_source_and_factor(store, connection)
        store.append_feature_snapshot(connection, _snapshot())
        connection.execute(
            text("UPDATE st_decision_run_v4 SET status = 'COMMITTED' WHERE run_uid = 'run-stage3'")
        )
        replay = store.append_feature_snapshot(connection, _snapshot())
        assert replay.created is False


def test_caller_can_roll_back_the_entire_factor_batch() -> None:
    engine = _engine()
    store = FactorStoreRepository()
    with engine.connect() as connection:
        transaction = connection.begin()
        store.append_source_certification(connection, _source())
        store.append_factor_definition(connection, _factor())
        transaction.rollback()

    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_data_source_certification_v4")
        ).scalar_one() == 0
        assert connection.execute(
            text("SELECT COUNT(*) FROM st_factor_definition_v4")
        ).scalar_one() == 0


def test_factor_definition_requires_exact_certified_source_lineage() -> None:
    store = FactorStoreRepository()
    engine = _engine()
    with engine.begin() as connection:
        with pytest.raises(FactorStoreIntegrityError, match="does not exist"):
            store.append_factor_definition(connection, _factor())
        store.append_source_certification(connection, _source())
        bad_hash = replace(
            _factor(),
            required_source_certifications=(
                {
                    "source_key": "daily_price",
                    "certification_version": "v4:cert:1",
                    "evidence_hash": H4,
                },
            ),
        )
        with pytest.raises(FactorStoreIntegrityError, match="hash conflicts"):
            store.append_factor_definition(connection, bad_hash)


def test_backtest_factor_rejects_forward_only_source() -> None:
    store = FactorStoreRepository()
    engine = _engine()
    with engine.begin() as connection:
        store.append_source_certification(
            connection,
            replace(
                _source(),
                replay_eligibility="FORWARD_ONLY",
                research_status="FORWARD_ONLY",
            ),
        )
        with pytest.raises(FactorStoreIntegrityError, match="incompatible"):
            store.append_factor_definition(connection, _factor())


@pytest.mark.parametrize(
    ("snapshot", "message"),
    [
        (
            replace(
                _snapshot(),
                factor_definitions=(
                    {
                        "factor_key": "missing",
                        "factor_version": "v4:factor:1",
                        "definition_hash": H2,
                    },
                ),
            ),
            "does not exist",
        ),
        (
            replace(
                _snapshot(),
                factor_definitions=(
                    {
                        "factor_key": "momentum_20d",
                        "factor_version": "v4:factor:1",
                        "definition_hash": H4,
                    },
                ),
            ),
            "definition hash conflicts",
        ),
        (
            replace(_snapshot(), feature_set_version="v4:features:other"),
            "different feature set",
        ),
        (
            replace(_snapshot(), values={"wrong_output": "0.125"}),
            "values do not match",
        ),
    ],
)
def test_snapshot_requires_exact_factor_definition_and_outputs(
    snapshot: EntityFeatureSnapshotRecord,
    message: str,
) -> None:
    store = FactorStoreRepository()
    engine = _engine()
    with engine.begin() as connection:
        _insert_run(connection)
        _append_source_and_factor(store, connection)
        with pytest.raises(FactorStoreIntegrityError, match=message):
            store.append_feature_snapshot(connection, snapshot)


def test_snapshot_rejects_unavailable_or_self_extended_factor() -> None:
    store = FactorStoreRepository()
    for factor, message in (
        (
            replace(
                _factor(),
                available_at=NOW + timedelta(seconds=2),
                created_at=NOW + timedelta(seconds=3),
            ),
            "unavailable",
        ),
        (replace(_factor(), max_age_seconds=60), "max age"),
    ):
        engine = _engine()
        with engine.begin() as connection:
            _insert_run(connection)
            _append_source_and_factor(store, connection, factor=factor)
            with pytest.raises(FactorStoreIntegrityError, match=message):
                store.append_feature_snapshot(connection, _snapshot())


def test_snapshot_must_cover_every_factor_source_certification() -> None:
    store = FactorStoreRepository()
    engine = _engine()
    alternate = replace(
        _source(),
        source_key="alternate_price",
        certification_version="v4:cert:alternate:1",
    )
    snapshot = replace(
        _snapshot(),
        source_certifications=(
            {
                "source_key": "alternate_price",
                "certification_version": "v4:cert:alternate:1",
                "evidence_hash": H1,
            },
        ),
    )
    with engine.begin() as connection:
        _insert_run(connection)
        store.append_source_certification(connection, _source())
        store.append_source_certification(connection, alternate)
        store.append_factor_definition(connection, _factor())
        with pytest.raises(FactorStoreIntegrityError, match="does not cover"):
            store.append_feature_snapshot(connection, snapshot)
