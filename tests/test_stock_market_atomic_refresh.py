from __future__ import annotations

import inspect
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import date, datetime, timezone

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biz.stock_market import sync_stock_market
from server.common.mysql_lock import (
    STOCK_MINUTE_FREEZE_LOCK_NAME,
    mysql_named_lock,
    supersede_overlapping_qmt_minute_forward_receipts,
)
from server.integrations.v2_execution_evidence_authority.verifier import (
    AuthorityClaim,
    MySQLRegistryBackedAuthorityVerifier,
    MySQLReceiptRegistryAuthorityVerifier,
)
from server.trading_v2.execution_evidence import QuoteReceiptType
from tools import crawl_minute_kline


def _source(function) -> str:
    return inspect.getsource(function).lower()


def test_main_and_refresh_steps_have_no_preclear_or_unscoped_append_calls():
    functions = (
        sync_stock_market.main,
        sync_stock_market.step_dividend,
        sync_stock_market._step_stock_kline_adata,
        sync_stock_market._step_stock_kline_akshare,
        sync_stock_market._step_stock_kline_myquant,
        sync_stock_market._step_stock_kline_qmt,
        sync_stock_market._try_step_stock_kline_registry,
        sync_stock_market.step_stock_minute,
        sync_stock_market._step_stock_minute_myquant,
        sync_stock_market.step_stock_current,
        sync_stock_market._legacy_step_stock_current,
        sync_stock_market.step_stock_five,
        sync_stock_market.step_stock_bar,
        sync_stock_market.step_stock_flow_min,
        sync_stock_market.step_stock_flow_daily,
        sync_stock_market.step_concept_ths_kline,
        sync_stock_market.step_concept_ths_minute,
        sync_stock_market.step_concept_ths_current,
        sync_stock_market.step_concept_east_kline,
        sync_stock_market.step_concept_east_minute,
        sync_stock_market.step_concept_east_current,
        sync_stock_market.step_concept_flow_east,
        sync_stock_market._legacy_step_index_kline,
        sync_stock_market._legacy_step_index_minute,
        sync_stock_market.step_index_kline,
        sync_stock_market.step_index_minute,
        sync_stock_market.step_index_current,
    )
    forbidden = (
        "truncate_all(",
        "truncate_only(",
        "delete_stock_minute_dates(",
        "df_to_table(",
    )
    for function in functions:
        source = _source(function)
        for call in forbidden:
            assert call not in source, f"{function.__name__} still calls {call}"


def test_large_kline_and_minute_paths_use_session_local_staging():
    for function in (
        sync_stock_market._step_stock_kline_adata,
        sync_stock_market._step_stock_kline_myquant,
        sync_stock_market._step_stock_kline_qmt,
        sync_stock_market._step_stock_minute_myquant,
    ):
        source = _source(function)
        assert "_create_temporary_stage(" in source
        assert "_publish_temporary_stage(" in source

    generic_stage = _source(sync_stock_market._create_temporary_stage)
    assert "create temporary table" in generic_stage
    assert "create table" not in generic_stage.replace("create temporary table", "")
    publish = _source(sync_stock_market._publish_temporary_stage)
    assert "with connection.begin()" in publish
    assert "delete from" in publish
    assert "insert into" in publish


def test_code_date_refresh_replaces_only_exact_staged_identities():
    refresh = _source(sync_stock_market._replace_validated_code_date_frame)
    publish = _source(
        sync_stock_market._publish_temporary_stage_exact_keys
    )

    assert "_publish_temporary_stage_exact_keys(" in refresh
    assert "replace_table_rows(" not in refresh
    assert "scope_start" not in refresh
    assert "scope_end" not in refresh
    assert "delete target from" in publish
    assert "inner join" in publish
    assert "insert into" in publish
    assert "with connection.begin()" in publish
    assert ">= :" not in publish
    assert "<= :" not in publish


def test_duplicate_exact_identity_fails_before_stage_publish(monkeypatch):
    staged = False

    def _unexpected_stage(*_args, **_kwargs):
        nonlocal staged
        staged = True
        raise AssertionError("duplicate identities must not reach staging")

    monkeypatch.setattr(
        sync_stock_market,
        "_create_temporary_stage",
        _unexpected_stage,
    )
    frame = pd.DataFrame(
        [
            {
                "stock_code": "000001",
                "trade_date": "2026-08-25",
                "k_type": 1,
                "adjust_type": 0,
                "close": 10,
            },
            {
                "stock_code": "000001",
                "trade_date": "2026-08-25",
                "k_type": 1,
                "adjust_type": 0,
                "close": 11,
            },
        ]
    )

    with pytest.raises(RuntimeError, match="duplicate exact identities"):
        sync_stock_market._replace_validated_code_date_frame(
            object(),
            frame,
            table_name="sm_stock_kline",
            requested_codes=["000001"],
            code_column="stock_code",
            date_column="trade_date",
            label="test exact kline",
            coverage_env="TEST_STOCK_MARKET_MIN_COVERAGE",
            default_coverage=1.0,
            identity_columns=(
                "stock_code", "trade_date", "k_type", "adjust_type",
            ),
            extra_where=(
                "k_type=:scope_k_type AND adjust_type=:scope_adjust_type"
            ),
            extra_params={"scope_k_type": 1, "scope_adjust_type": 0},
        )

    assert staged is False


def test_qmt_minute_multibatch_publish_is_receipt_barriered():
    source = _source(sync_stock_market._step_stock_minute_qmt)
    generation_lock = source.index("with mysql_named_lock(")
    publishing = source.index('quality_status="publishing"')
    commit = source.index("_commit_qmt_minute_stage(")
    failed = source.index('quality_status="failed"')
    passed = source.index("quality_status=final_quality_status")

    assert generation_lock < publishing < commit < failed < passed
    commit_source = _source(sync_stock_market._commit_qmt_minute_stage)
    assert "stock_minute_freeze_lock_name" in commit_source
    assert "connection_id()" in commit_source
    receipt = _source(sync_stock_market._record_qmt_minute_receipt)
    assert "receipt_id=values(receipt_id)" in receipt
    assert "status != \"pass\" and forward_eligible" in receipt
    assert "first_trade_time<=:last_trade_time" in receipt
    assert "last_trade_time>=:first_trade_time" in receipt
    update = receipt[receipt.index("update st_qmt_minute_sync_receipt_v2") :]
    update = update[: update.index("insert into st_qmt_minute_sync_receipt_v2")]
    assert "quality_status='superseded'" in update
    assert "source_provider" not in update


def test_every_stock_minute_publisher_uses_one_freeze_lock_on_target_connection():
    qmt = _source(sync_stock_market._step_stock_minute_qmt)
    exact_dispatch = _source(sync_stock_market._replace_validated_code_date_frame)
    partition_publish = _source(sync_stock_market._publish_temporary_stage)
    exact_publish = _source(sync_stock_market._publish_temporary_stage_exact_keys)
    legacy_window = _source(sync_stock_market._replace_qmt_minute_window)
    crawler = _source(crawl_minute_kline._publish_kline_stage)

    assert STOCK_MINUTE_FREEZE_LOCK_NAME == "probiga:stock_minute"
    assert "stock_minute_freeze_lock_name" in qmt
    assert "stock_minute_freeze_lock_name" in exact_dispatch
    assert "connection=connection" in exact_publish
    for publisher in (partition_publish, exact_publish):
        assert "supersede_overlapping_qmt_minute_forward_receipts(" in publisher
        assert publisher.index("with mysql_named_lock(") < publisher.index(
            "supersede_overlapping_qmt_minute_forward_receipts("
        ) < publisher.index("with connection.begin()")
    assert "stock_minute_freeze_lock_name" in legacy_window
    assert "connection=conn" in legacy_window
    assert legacy_window.index("with mysql_named_lock(") < legacy_window.index(
        "supersede_overlapping_qmt_minute_forward_receipts("
    ) < legacy_window.index("with conn.begin()")
    assert "stock_minute_freeze_lock_name" in crawler
    assert "connection=connection" in crawler
    assert crawler.index("with mysql_named_lock(") < crawler.index(
        "supersede_overlapping_qmt_minute_forward_receipts("
    ) < crawler.index("with connection.begin()")
    assert "probiga:exact:sm_stock_minute" not in exact_dispatch
    assert "probiga:sm_stock_minute" not in crawler
    assert "receipt_engine=engine" in _source(sync_stock_market.step_stock_minute)
    assert "receipt_engine=engine" in _source(
        sync_stock_market._step_stock_minute_myquant
    )
    assert "receipt_engine=engine" in _source(crawl_minute_kline.main)


class _NamedLockScalar:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _NamedLockConnection:
    def __init__(self, locks):
        self._locks = locks
        self._owned = None

    def execute(self, statement, params):
        sql = str(statement).upper()
        name = params["lock_name"]
        lock = self._locks.setdefault(name, threading.Lock())
        if "GET_LOCK" in sql:
            acquired = lock.acquire(timeout=params["timeout_seconds"])
            self._owned = lock if acquired else None
            return _NamedLockScalar(1 if acquired else 0)
        if "RELEASE_LOCK" in sql:
            if self._owned is not None:
                self._owned.release()
                self._owned = None
            return _NamedLockScalar(1)
        raise AssertionError(sql)

    def close(self):
        if self._owned is not None:
            self._owned.release()
            self._owned = None


class _NamedLockEngine:
    def __init__(self):
        self.locks = {}

    def connect(self):
        return _NamedLockConnection(self.locks)


def test_overlapping_stock_minute_freeze_sections_cannot_run_together():
    engine = _NamedLockEngine()
    state_lock = threading.Lock()
    active = 0
    maximum_active = 0

    def _writer():
        nonlocal active, maximum_active
        with mysql_named_lock(
            engine,
            STOCK_MINUTE_FREEZE_LOCK_NAME,
            timeout_seconds=1,
        ):
            with state_lock:
                active += 1
                maximum_active = max(maximum_active, active)
            time.sleep(0.02)
            with state_lock:
                active -= 1

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(_writer) for _ in range(2)]
        for future in futures:
            future.result()

    assert maximum_active == 1


@pytest.mark.filterwarnings(
    "ignore:The default datetime adapter is deprecated:DeprecationWarning"
)
def test_sequential_non_qmt_publish_supersedes_all_provider_pass_receipts():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE st_qmt_minute_sync_receipt_v2 ("
                "receipt_id TEXT PRIMARY KEY, source_provider TEXT NOT NULL, "
                "first_trade_time DATETIME NOT NULL, "
                "last_trade_time DATETIME NOT NULL, "
                "forward_eligible INTEGER NOT NULL, "
                "quality_status TEXT NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_qmt_minute_sync_receipt_v2 VALUES "
                "('mini', 'guojin_miniqmt_gateway', "
                " '2026-08-25 09:30:00', '2026-08-25 10:00:00', 1, 'PASS'), "
                "('big', 'gj_big_qmt_inner', "
                " '2026-08-25 09:31:00', '2026-08-25 10:01:00', 1, 'PASS'), "
                "('later', 'gj_big_qmt_inner', "
                " '2026-08-25 13:00:00', '2026-08-25 13:30:00', 1, 'PASS'), "
                "('partial', 'guojin_miniqmt_gateway', "
                " '2026-08-25 09:30:00', '2026-08-25 10:00:00', 0, 'PARTIAL')"
            )
        )

    revoked = supersede_overlapping_qmt_minute_forward_receipts(
        engine,
        first_trade_time=datetime(2026, 8, 25, 9, 45),
        last_trade_time=datetime(2026, 8, 25, 10, 15),
        reason="sequential registry overwrite",
    )

    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT receipt_id, forward_eligible, quality_status "
                "FROM st_qmt_minute_sync_receipt_v2 ORDER BY receipt_id"
            )
        ).fetchall()
    assert revoked == 2
    assert [tuple(row) for row in rows] == [
        ("big", 0, "SUPERSEDED"),
        ("later", 1, "PASS"),
        ("mini", 0, "SUPERSEDED"),
        ("partial", 0, "PARTIAL"),
    ]


class _PublishMetadataResult:
    rowcount = 1

    def __init__(self, *, scalar_value=None, rows=None):
        self._scalar_value = scalar_value
        self._rows = rows or []

    def scalar(self):
        return self._scalar_value

    def fetchall(self):
        return self._rows

    def one(self):
        return self._rows[0]

    def first(self):
        return self._rows[0] if self._rows else None


class _PublishMetadataConnection:
    def __init__(self):
        self.target_dml = []

    def execute(self, statement, params=None):
        del params
        sql = str(statement)
        upper = sql.upper()
        if upper.lstrip().startswith(("DELETE", "INSERT")):
            self.target_dml.append(sql)
            return _PublishMetadataResult()
        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            return _PublishMetadataResult(
                rows=[("stock_code",), ("trade_time",), ("price",)]
            )
        if "MIN(TRADE_TIME)" in upper or "MIN(TRADE_DATE)" in upper:
            return _PublishMetadataResult(
                rows=[
                    (
                        datetime(2026, 8, 25, 9, 30),
                        datetime(2026, 8, 25, 9, 31),
                    )
                ]
            )
        if "HAVING COUNT(*) > 1" in upper:
            return _PublishMetadataResult(rows=[])
        if "IS NULL" in upper:
            return _PublishMetadataResult(scalar_value=0)
        if "SELECT COUNT(*)" in upper:
            return _PublishMetadataResult(scalar_value=1)
        raise AssertionError(sql)

    def commit(self):
        return None

    def rollback(self):
        return None

    def begin(self):
        return nullcontext(self)


@pytest.mark.parametrize("exact_keys", [False, True])
def test_receipt_database_failure_blocks_registry_and_partition_target_dml(
    monkeypatch,
    exact_keys,
):
    connection = _PublishMetadataConnection()
    monkeypatch.setattr(
        sync_stock_market,
        "mysql_named_lock",
        lambda *args, **kwargs: nullcontext(),
    )

    def _receipt_database_down(*_args, **_kwargs):
        raise RuntimeError("receipt database unavailable")

    monkeypatch.setattr(
        sync_stock_market,
        "supersede_overlapping_qmt_minute_forward_receipts",
        _receipt_database_down,
    )
    with pytest.raises(RuntimeError, match="receipt database unavailable"):
        if exact_keys:
            sync_stock_market._publish_temporary_stage_exact_keys(
                object(),
                connection,
                stage_table="minute_stage",
                target_table="sm_stock_minute",
                identity_columns=("stock_code", "trade_time"),
                lock_name=STOCK_MINUTE_FREEZE_LOCK_NAME,
                receipt_engine=object(),
            )
        else:
            sync_stock_market._publish_temporary_stage(
                object(),
                connection,
                stage_table="minute_stage",
                target_table="sm_stock_minute",
                where_sql="trade_time >= :start_time",
                params={"start_time": datetime(2026, 8, 25, 9, 30)},
                lock_name=STOCK_MINUTE_FREEZE_LOCK_NAME,
                receipt_engine=object(),
            )

    assert connection.target_dml == []


def test_unfinished_qmt_minute_receipt_cannot_be_forward_eligible():
    with pytest.raises(ValueError, match="cannot be forward eligible"):
        sync_stock_market._record_qmt_minute_receipt(
            object(),
            trade_date="2026-08-25",
            first_trade_time=pd.Timestamp("2026-08-25 09:30:00").to_pydatetime(),
            last_trade_time=pd.Timestamp("2026-08-25 15:00:00").to_pydatetime(),
            expected_count=2,
            observed_count=2,
            row_count=0,
            source_provider="guojin_miniqmt_gateway",
            capture_mode="LIVE_FORWARD_CAPTURE",
            forward_eligible=True,
            quality_status="PUBLISHING",
            evidence={},
        )


def test_85_percent_qmt_minute_receipt_cannot_gain_forward_authority():
    requested = [f"{code:06d}" for code in range(20)]
    responded = set(requested[:17])
    published = set(responded)
    evidence = sync_stock_market._qmt_minute_universe_evidence(
        requested,
        responded,
        published,
    )

    assert evidence["responded_stock_code_count"] == 17
    assert evidence["published_stock_code_count"] == 17
    assert evidence["requested_stock_code_count"] == 20
    assert evidence["full_requested_response_coverage"] is False
    disposition_evidence, status, forward_eligible = (
        sync_stock_market._qmt_minute_receipt_disposition(
            requested,
            responded,
            published,
            live_forward_capture=True,
        )
    )
    assert disposition_evidence == evidence
    assert status == "PARTIAL"
    assert forward_eligible is False
    with pytest.raises(ValueError, match="exact requested coverage"):
        sync_stock_market._record_qmt_minute_receipt(
            object(),
            trade_date="2026-08-25",
            first_trade_time=datetime(2026, 8, 25, 9, 30),
            last_trade_time=datetime(2026, 8, 25, 15, 0),
            expected_count=20,
            observed_count=17,
            row_count=170,
            source_provider="guojin_miniqmt_gateway",
            capture_mode="LIVE_FORWARD",
            forward_eligible=True,
            quality_status="PASS",
            evidence=evidence,
        )

    full_evidence, full_status, full_forward = (
        sync_stock_market._qmt_minute_receipt_disposition(
            requested,
            set(requested),
            set(requested[:-1]),
            live_forward_capture=True,
        )
    )
    assert full_evidence["full_requested_response_coverage"] is True
    assert full_evidence["responded_stock_code_count"] == 20
    assert full_evidence["published_stock_code_count"] == 19
    assert full_status == "PASS"
    assert full_forward is True
    with pytest.raises(ValueError, match="frozen universe evidence"):
        sync_stock_market._record_qmt_minute_receipt(
            object(),
            trade_date="2026-08-25",
            first_trade_time=datetime(2026, 8, 25, 9, 30),
            last_trade_time=datetime(2026, 8, 25, 15, 0),
            expected_count=20,
            observed_count=20,
            row_count=200,
            source_provider="guojin_miniqmt_gateway",
            capture_mode="LIVE_FORWARD",
            forward_eligible=True,
            quality_status="PASS",
            evidence={},
        )


class _AuthorityMappings:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _AuthorityResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return _AuthorityMappings(self._rows)


class _AuthorityConnection:
    def __init__(self, rows):
        self._rows = rows
        self.calls = []

    def execute(self, statement, params):
        self.calls.append((str(statement), params))
        return _AuthorityResult(self._rows)


def _qmt_minute_claim(stock_code: str) -> AuthorityClaim:
    return AuthorityClaim(
        evidence_type="QUOTE_RECEIPT",
        evidence_id="a" * 64,
        source_provider="guojin_miniqmt_gateway",
        source_payload_hash="b" * 64,
        receipt_type=QuoteReceiptType.QMT_MINUTE.value,
        receipt_id="receipt-1",
        receipt_hash="c" * 64,
        available_at=datetime(2026, 8, 25, 2, 0, tzinfo=timezone.utc),
        trade_date=date(2026, 8, 25),
        event_at=datetime(2026, 8, 25, 1, 31, tzinfo=timezone.utc),
        received_at=datetime(2026, 8, 25, 1, 32, tzinfo=timezone.utc),
        stock_code=stock_code,
    )


def test_qmt_minute_authority_verifier_binds_exact_published_stock_code_set():
    responded, published = sync_stock_market._qmt_minute_validated_code_sets(
        pd.DataFrame(
            [
                {"stock_code": "000001", "volume": 100, "amount": 1000},
                {"stock_code": "000002", "volume": 0, "amount": 0},
            ]
        )
    )
    assert responded == {"000001", "000002"}
    assert published == {"000001"}
    _, status, forward_eligible = sync_stock_market._qmt_minute_receipt_disposition(
        ["000001", "000002"],
        responded,
        published,
        live_forward_capture=True,
    )
    assert status == "PASS"
    assert forward_eligible is True

    evidence = sync_stock_market._qmt_minute_universe_evidence(
        ["000002", "000001"],
        responded,
        published,
    )
    row = {
        "expected_count": 2,
        "observed_count": 2,
        "evidence_json": json.dumps(evidence, separators=(",", ":")),
    }
    verifier = MySQLReceiptRegistryAuthorityVerifier(
        clock=lambda: datetime(2026, 8, 25, 2, 1, tzinfo=timezone.utc)
    )
    accepted_claim = _qmt_minute_claim("000001")
    # 000002 was a validated provider response (for example, a suspended
    # zero-volume symbol) but had no physically published minute rows.
    rejected_claim = _qmt_minute_claim("000002")

    accepted = verifier.verify(_AuthorityConnection([row]), accepted_claim)
    rejected = verifier.verify(_AuthorityConnection([row]), rejected_claim)

    assert accepted_claim.claim_hash != rejected_claim.claim_hash
    assert accepted.verified is True
    assert rejected.verified is False
    assert rejected.reason_code == "QMT_MINUTE_CODE_NOT_PUBLISHED"

    forged = dict(evidence)
    forged["published_stock_codes"] = ["000001", "000003"]
    forged_row = {**row, "evidence_json": json.dumps(forged)}
    forged_decision = verifier.verify(
        _AuthorityConnection([forged_row]),
        _qmt_minute_claim("000001"),
    )
    assert forged_decision.verified is False
    assert forged_decision.reason_code == "QMT_MINUTE_UNIVERSE_MISMATCH"

    production_verifier = MySQLRegistryBackedAuthorityVerifier(
        clock=lambda: datetime(2026, 8, 25, 2, 1, tzinfo=timezone.utc)
    )
    production_denial = production_verifier.verify(
        _AuthorityConnection([row]),
        rejected_claim,
    )
    assert production_denial.verified is False
    assert production_denial.reason_code == "QMT_MINUTE_CODE_NOT_PUBLISHED"


def test_empty_or_undercovered_snapshot_never_reaches_replacement(monkeypatch):
    called = False

    def _unexpected_replace(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("replacement must not run")

    monkeypatch.setattr(sync_stock_market, "replace_table_rows", _unexpected_replace)
    with pytest.raises(RuntimeError, match="coverage below threshold"):
        sync_stock_market._replace_validated_code_snapshot(
            object(),
            pd.DataFrame([{"stock_code": "000001", "value": 1}]),
            table_name="snapshot_table",
            requested_codes=["000001", "000002"],
            code_column="stock_code",
            label="test snapshot",
            coverage_env="TEST_STOCK_MARKET_MIN_COVERAGE",
            default_coverage=1.0,
        )
    assert called is False


def test_failed_atomic_snapshot_insert_rolls_back_old_rows():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE snapshot_table ("
                "stock_code TEXT PRIMARY KEY, value INTEGER NOT NULL, "
                "etl_sync_at TEXT NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO snapshot_table(stock_code, value, etl_sync_at) "
                "VALUES ('000001', 7, '2026-08-25 09:00:00')"
            )
        )

    bad_frame = pd.DataFrame(
        [{"stock_code": "000001", "value": 99, "unexpected_column": "boom"}]
    )
    with pytest.raises(Exception):
        sync_stock_market._replace_validated_code_snapshot(
            engine,
            bad_frame,
            table_name="snapshot_table",
            requested_codes=["000001"],
            code_column="stock_code",
            label="test snapshot",
            coverage_env="TEST_STOCK_MARKET_MIN_COVERAGE",
            default_coverage=1.0,
        )

    with engine.connect() as connection:
        row = connection.execute(
            text("SELECT stock_code, value FROM snapshot_table")
        ).one()
    assert tuple(row) == ("000001", 7)


def test_accepted_partial_snapshot_replaces_only_observed_codes():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE snapshot_table ("
                "stock_code TEXT PRIMARY KEY, value INTEGER NOT NULL, "
                "etl_sync_at TEXT NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO snapshot_table(stock_code, value, etl_sync_at) VALUES "
                "('000001', 7, '2026-08-25 09:00:00'), "
                "('000002', 8, '2026-08-25 09:00:00')"
            )
        )

    sync_stock_market._replace_validated_code_snapshot(
        engine,
        pd.DataFrame([{"stock_code": "000001", "value": 99}]),
        table_name="snapshot_table",
        requested_codes=["000001", "000002"],
        code_column="stock_code",
        label="test partial snapshot",
        coverage_env="TEST_STOCK_MARKET_PARTIAL_MIN_COVERAGE",
        default_coverage=0.50,
    )

    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT stock_code, value FROM snapshot_table ORDER BY stock_code")
        ).fetchall()
    assert [tuple(row) for row in rows] == [("000001", 99), ("000002", 8)]


def test_runtime_refresh_surface_contains_no_persistent_ddl():
    runtime_functions = (
        sync_stock_market.run_ddl,
        sync_stock_market.main,
        sync_stock_market._create_temporary_stage,
        sync_stock_market._create_qmt_minute_stage,
        sync_stock_market._publish_temporary_stage,
        sync_stock_market._step_stock_kline_adata,
        sync_stock_market._step_stock_kline_myquant,
        sync_stock_market._step_stock_kline_qmt,
        sync_stock_market.step_stock_minute,
        sync_stock_market._step_stock_minute_myquant,
    )
    for function in runtime_functions:
        source = _source(function)
        without_temporary = source.replace("create temporary table", "")
        assert "create table" not in without_temporary
        assert "alter table" not in source
        assert "drop table" not in source
        assert "truncate table" not in source
