from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text

from server.integrations.v4_pit_sources import (
    ANNOUNCEMENT_SOURCE,
    CONCEPT_SNAPSHOT_SOURCE,
    DAILY_KLINE_SOURCE,
    FINANCIAL_SOURCE,
    MINUTE_KLINE_SOURCE,
    NEWS_FLASH_SOURCE,
    CHINA_TIMEZONE,
    AnnouncementPitAdapter,
    ConceptSnapshotPitAdapter,
    DailyKlinePitAdapter,
    FinancialPitAdapter,
    MinuteKlinePitAdapter,
    NewsFlashPitAdapter,
    PitSourceDataError,
    PitSourceReadError,
    PitSourceRowLimitExceeded,
    source_contract,
)
from server.trading_v4.domain import (
    AsOfDataset,
    DataManifest,
    DatasetResult,
    ScopeRef,
    ScopeType,
)
from server.trading_v4.domain.enums import (
    QualityStatus,
    ReplayEligibility,
    ResearchStatus,
)
from server.trading_v4.factors import build_chase_risk_feature_vector


def _kline_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE sm_stock_kline (
                    stock_code TEXT NOT NULL,
                    short_name TEXT,
                    trade_date TEXT NOT NULL,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    pre_close NUMERIC,
                    volume NUMERIC,
                    amount NUMERIC,
                    change_pct NUMERIC,
                    turnover_ratio NUMERIC,
                    k_type INTEGER NOT NULL,
                    adjust_type INTEGER NOT NULL,
                    data_source TEXT,
                    source_time TEXT,
                    received_at TEXT,
                    etl_sync_at TEXT,
                    batch_id TEXT,
                    data_version TEXT,
                    quality_status TEXT,
                    permission_status TEXT
                )
                """
            )
        )
        rows = (
            # Observable prior close.
            {
                "code": "000001",
                "day": "2026-08-03",
                "close": 10,
                "source": "2026-08-03 15:00:00",
                "received": "2026-08-03 15:01:00",
                "etl": "2026-08-03 15:02:00",
                "version": "v1",
            },
            # Same-day close is physically present but cannot be known at 14:30.
            {
                "code": "000001",
                "day": "2026-08-04",
                "close": 99,
                "source": "2026-08-04 14:00:00",
                "received": "2026-08-04 14:01:00",
                "etl": "2026-08-04 14:02:00",
                "version": "v2",
            },
            # Prior-day bar arrived after the first decision cutoff.
            {
                "code": "000002",
                "day": "2026-08-03",
                "close": 20,
                "source": "2026-08-03 15:00:00",
                "received": "2026-08-04 15:30:00",
                "etl": "2026-08-04 15:31:00",
                "version": "late-v1",
            },
            # A future trade date is not a candidate row for the cutoff at all.
            {
                "code": "000003",
                "day": "2026-08-05",
                "close": 30,
                "source": "2026-08-05 15:00:00",
                "received": "2026-08-05 15:01:00",
                "etl": "2026-08-05 15:02:00",
                "version": "future-v1",
            },
        )
        for row in rows:
            connection.execute(
                text(
                    """
                    INSERT INTO sm_stock_kline (
                        stock_code, short_name, trade_date,
                        open, high, low, close, pre_close, volume, amount,
                        change_pct, turnover_ratio, k_type, adjust_type,
                        data_source, source_time, received_at, etl_sync_at,
                        batch_id, data_version, quality_status, permission_status
                    ) VALUES (
                        :code, :code, :day,
                        :close, :close, :close, :close, :close, 100, 1000,
                        0, 1, 1, 0,
                        'test', :source, :received, :etl,
                        :version, :version, 'VERIFIED', 'SUPPORTED'
                    )
                    """
                ),
                row,
            )
    return engine


def _at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 8, 4, hour, minute, tzinfo=CHINA_TIMEZONE)


def test_daily_kline_strict_cutoff_blocks_same_day_close_late_and_future_rows():
    result = DailyKlinePitAdapter(_kline_engine()).load_market_data(
        ("000001", "000002", "000003"),
        knowledge_cutoff=_at(14, 30),
        fields=("close",),
    )

    assert type(result) is DatasetResult
    assert type(result.dataset) is AsOfDataset
    assert result.returned_entities == ("000001",)
    assert result.missing_entities == ("000002", "000003")
    assert result.coverage == Decimal(1) / Decimal(3)
    assert result.freshness_status == QualityStatus.WARN
    assert set(result.reason_codes) == {
        "PARTIAL_COVERAGE",
        "SOURCE_FORWARD_ONLY",
    }
    assert len(result.dataset.records) == 1
    record = result.dataset.records[0]
    assert record.payload["trade_date"] == "2026-08-03"
    assert record.payload["close"] == 10
    assert record.knowledge_time.isoformat() == "2026-08-03T15:02:00+08:00"
    assert record.knowledge_time <= result.requested_cutoff
    assert all(item.payload["close"] != 99 for item in result.dataset.records)


def test_daily_kline_late_arrival_becomes_visible_only_after_its_arrival():
    adapter = DailyKlinePitAdapter(_kline_engine())
    before = adapter.load_market_data(
        ("000002",), knowledge_cutoff=_at(14, 30), fields=("close",)
    )
    after = adapter.load_market_data(
        ("000002",), knowledge_cutoff=_at(16), fields=("close",)
    )

    assert before.returned_entities == ()
    assert before.freshness_status == QualityStatus.FAIL
    assert "NO_RECORDS_BEFORE_CUTOFF" in before.reason_codes
    assert after.returned_entities == ("000002",)
    assert after.freshness_status == QualityStatus.WARN
    assert after.reason_codes == ("SOURCE_FORWARD_ONLY",)
    assert after.dataset.records[0].payload["close"] == 20
    assert after.dataset.records[0].knowledge_time == datetime(
        2026, 8, 4, 15, 31, tzinfo=CHINA_TIMEZONE
    )


def test_daily_kline_history_feeds_chase_risk_without_payload_conversion():
    engine = _kline_engine()
    rows = []
    for index in range(25):
        session = date(2026, 7, 1) + timedelta(days=index)
        previous_close = 100 + index
        close = previous_close + 1
        rows.append(
            {
                "day": session.isoformat(),
                "open": previous_close,
                "high": close + 1,
                "low": previous_close - 1,
                "close": close,
                "pre_close": 0 if index == 24 else previous_close,
                "volume": 0 if index == 24 else 1_000_000,
                "amount": 0 if index == 24 else close * 1_000_000,
                "turnover": 8,
                "source_time": f"{session.isoformat()} 15:00:00",
                "received_at": f"{session.isoformat()} 15:01:00",
                "etl_sync_at": f"{session.isoformat()} 15:02:00",
                "version": f"window-{index:02d}",
            }
        )
    with engine.begin() as connection:
        connection.execute(
            text(
                """INSERT INTO sm_stock_kline (
                    stock_code, short_name, trade_date,
                    open, high, low, close, pre_close, volume, amount,
                    change_pct, turnover_ratio, k_type, adjust_type,
                    data_source, source_time, received_at, etl_sync_at,
                    batch_id, data_version, quality_status, permission_status
                ) VALUES (
                    '600001', 'window', :day,
                    :open, :high, :low, :close, :pre_close, :volume, :amount,
                    1, :turnover, 1, 0,
                    'test', :source_time, :received_at, :etl_sync_at,
                    :version, :version, 'VERIFIED', 'SUPPORTED'
                )"""
            ),
            rows,
        )

    result = DailyKlinePitAdapter(engine).load_market_data(
        ("600001",),
        knowledge_cutoff=_at(16),
    )
    feature = build_chase_risk_feature_vector(
        result.dataset,
        instrument="600001",
        valid_until=_at(17),
    )

    assert len(result.dataset.records) == 25
    assert all(record.payload["instrument"] == "600001" for record in result.dataset.records)
    assert result.dataset.records[-1].payload["previous_close"] == 0
    assert result.dataset.records[-1].payload["turnover_pct"] == 8
    assert feature.values["bar_count"] == 25
    assert feature.values["return_20d_pct"] is None
    assert feature.values["candidate_status"] == "EXECUTION_BLOCKED"
    assert feature.values["no_capacity"] is True
    assert "previous_close" in feature.missing_fields
    assert "PREVIOUS_CLOSE_MISSING" in feature.reason_codes
    assert feature.source_manifest_hash == DataManifest(
        feature.source_record_hashes
    ).manifest_hash


def test_daily_kline_raw_row_ceiling_fails_closed():
    adapter = DailyKlinePitAdapter(_kline_engine(), max_rows=2)
    with pytest.raises(PitSourceRowLimitExceeded, match="max_rows=2"):
        adapter.load_market_data(
            ("000001", "000002"),
            knowledge_cutoff=_at(16),
            fields=("close",),
        )


def test_daily_kline_rejects_source_time_without_acquisition_evidence():
    engine = _kline_engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO sm_stock_kline (
                    stock_code, short_name, trade_date,
                    open, high, low, close, pre_close, volume, amount,
                    change_pct, turnover_ratio, k_type, adjust_type,
                    data_source, source_time, received_at, etl_sync_at,
                    batch_id, data_version, quality_status, permission_status
                ) VALUES (
                    '000004', '000004', '2026-08-03',
                    40, 40, 40, 40, 40, 100, 4000,
                    0, 1, 1, 0,
                    'test', '2026-08-03 15:00:00', NULL, NULL,
                    'legacy', 'legacy', 'VERIFIED', 'SUPPORTED'
                )
                """
            )
        )

    with pytest.raises(PitSourceDataError, match="observable timestamp"):
        DailyKlinePitAdapter(engine).load_market_data(
            ("000004",), knowledge_cutoff=_at(16), fields=("close",)
        )


def test_daily_kline_never_falls_back_to_another_legacy_table():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE sm_stock_snapshot "
                "(stock_code TEXT, trade_date TEXT, close NUMERIC)"
            )
        )


def _minute_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE sm_stock_minute (
                    stock_code TEXT NOT NULL,
                    trade_time TEXT NOT NULL,
                    price NUMERIC,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume NUMERIC,
                    amount NUMERIC,
                    change_pct NUMERIC,
                    avg_price NUMERIC,
                    data_source TEXT NOT NULL,
                    source_time TEXT,
                    received_at TEXT,
                    etl_sync_at TEXT,
                    batch_id TEXT,
                    data_version TEXT,
                    quality_status TEXT,
                    permission_status TEXT
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO sm_stock_minute VALUES
                ('000001','2026-08-04 09:31:00',10,10,10,10,10,100,1000,0,10,
                 'qmt','2026-08-04 09:31:00','2026-08-04 09:31:02',
                 '2026-08-04 09:31:03','b1','v1','VERIFIED','SUPPORTED'),
                ('000002','2026-08-04 09:32:00',20,20,20,20,20,100,2000,0,20,
                 'qmt','2026-08-04 09:32:00','2026-08-04 10:05:00',
                 '2026-08-04 10:06:00','b2','v1','VERIFIED','SUPPORTED')
                """
            )
        )
    return engine


def test_minute_kline_receipt_cutoff_prefix_invariance_and_partial_coverage():
    engine = _minute_engine()
    adapter = MinuteKlinePitAdapter(engine)
    cutoff = datetime(2026, 8, 4, 10, 0, tzinfo=CHINA_TIMEZONE)
    before_append = adapter.load_market_data(
        ("000001", "000002"),
        knowledge_cutoff=cutoff,
        fields=("close", "volume"),
    )

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO sm_stock_minute VALUES
                ('000001','2026-08-04 10:01:00',99,99,99,99,99,100,9900,0,99,
                 'qmt','2026-08-04 10:01:00','2026-08-04 10:01:01',
                 '2026-08-04 10:01:02','future','v2','VERIFIED','SUPPORTED')
                """
            )
        )
    after_append = adapter.load_market_data(
        ("000001", "000002"),
        knowledge_cutoff=cutoff,
        fields=("close", "volume"),
    )

    assert before_append.dataset.dataset_id == after_append.dataset.dataset_id
    assert before_append.dataset.manifest_hash == after_append.dataset.manifest_hash
    assert before_append.returned_entities == ("000001",)
    assert before_append.missing_entities == ("000002",)
    assert [record.payload["close"] for record in before_append.dataset.records] == [
        10
    ]
    assert before_append.dataset.records[0].knowledge_time == datetime(
        2026, 8, 4, 9, 31, 3, tzinfo=CHINA_TIMEZONE
    )
    assert set(before_append.reason_codes) == {
        "PARTIAL_COVERAGE",
        "SOURCE_FORWARD_ONLY",
    }


def test_minute_kline_late_arrival_uses_receipt_not_trade_time():
    adapter = MinuteKlinePitAdapter(_minute_engine())
    before = adapter.load_market_data(
        ("000002",),
        knowledge_cutoff=datetime(2026, 8, 4, 10, 0, tzinfo=CHINA_TIMEZONE),
        fields=("close",),
    )
    after = adapter.load_market_data(
        ("000002",),
        knowledge_cutoff=datetime(2026, 8, 4, 10, 7, tzinfo=CHINA_TIMEZONE),
        fields=("close",),
    )

    assert before.dataset.records == ()
    assert after.returned_entities == ("000002",)
    assert after.dataset.records[0].knowledge_time == datetime(
        2026, 8, 4, 10, 6, tzinfo=CHINA_TIMEZONE
    )
    assert after.freshness_status == QualityStatus.WARN
    assert after.reason_codes == ("SOURCE_FORWARD_ONLY",)


def test_minute_kline_has_bounded_reads_and_no_table_fallback():
    with pytest.raises(PitSourceRowLimitExceeded, match="sm_stock_minute"):
        MinuteKlinePitAdapter(_minute_engine(), max_rows=1).load_market_data(
            ("000001", "000002"),
            knowledge_cutoff=datetime(2026, 8, 4, 10, 0, tzinfo=CHINA_TIMEZONE),
            fields=("close",),
        )

    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE sm_stock_current (stock_code TEXT, price NUMERIC)")
        )
    with pytest.raises(PitSourceReadError, match="sm_stock_minute"):
        MinuteKlinePitAdapter(engine).load_market_data(
            ("000001",),
            knowledge_cutoff=datetime(2026, 8, 4, 10, 0, tzinfo=CHINA_TIMEZONE),
            fields=("close",),
        )


def test_minute_kline_rejects_trade_time_without_receipt_evidence():
    engine = _minute_engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO sm_stock_minute VALUES
                ('000003','2026-08-04 09:33:00',30,30,30,30,30,100,3000,0,30,
                 'qmt','2026-08-04 09:33:00',NULL,NULL,
                 'missing-receipt','v1','VERIFIED','SUPPORTED')
                """
            )
        )
    with pytest.raises(PitSourceDataError, match="observable timestamp"):
        MinuteKlinePitAdapter(engine).load_market_data(
            ("000003",),
            knowledge_cutoff=datetime(2026, 8, 4, 10, 0, tzinfo=CHINA_TIMEZONE),
            fields=("close",),
        )
        connection.execute(
            text("INSERT INTO sm_stock_snapshot VALUES ('000001','2026-08-03',10)")
        )

    with pytest.raises(PitSourceReadError, match="sm_stock_kline"):
        DailyKlinePitAdapter(engine).load_market_data(
            ("000001",), knowledge_cutoff=_at(16), fields=("close",)
        )


def _news_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_news_flash (
                    id INTEGER PRIMARY KEY,
                    source TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    title TEXT,
                    content TEXT,
                    publish_time TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    level TEXT,
                    stocks TEXT,
                    subjects TEXT,
                    reading_num INTEGER,
                    is_top INTEGER,
                    jpush INTEGER,
                    extra TEXT,
                    etl_sync_at TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO st_news_flash VALUES
                (1,'wire','visible','visible','body','2026-08-04 10:00:00',
                 '2026-08-04 10:01:00','B','[{"code":"000001"}]','[]',0,0,0,
                 '{}','2026-08-04 10:02:00'),
                (2,'wire','late','late','body','2026-08-04 10:30:00',
                 '2026-08-04 15:00:00','B','[{"code":"000001"}]','[]',0,0,0,
                 '{}','2026-08-04 15:01:00'),
                (3,'wire','future','future','body','2026-08-04 18:00:00',
                 '2026-08-04 18:00:00','B','[{"code":"000001"}]','[]',0,0,0,
                 '{}','2026-08-04 18:01:00')
                """
            )
        )
    return engine


def test_news_first_seen_cutoff_is_strict_and_remains_forward_only():
    scope = ScopeRef(scope_type=ScopeType.INSTRUMENT, scope_id="000001")
    adapter = NewsFlashPitAdapter(_news_engine())

    before = adapter.load_events((scope,), knowledge_cutoff=_at(14, 30))
    after = adapter.load_events((scope,), knowledge_cutoff=_at(16))

    assert [record.record_id for record in before.dataset.records] == ["wire:visible"]
    assert {record.record_id for record in after.dataset.records} == {
        "wire:late",
        "wire:visible",
    }
    assert all(record.knowledge_time <= after.requested_cutoff for record in after.dataset.records)
    assert after.freshness_status == QualityStatus.WARN
    assert "SOURCE_FORWARD_ONLY" in after.reason_codes


def _finance_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE si_stock_finance (
                    id INTEGER PRIMARY KEY,
                    stock_code TEXT NOT NULL,
                    report_date TEXT NOT NULL,
                    notice_date TEXT NOT NULL,
                    net_asset_ps NUMERIC,
                    oper_cf_ps NUMERIC,
                    total_rev_yoy_gr NUMERIC,
                    net_profit_yoy_gr NUMERIC,
                    roe_wtd NUMERIC,
                    gross_margin NUMERIC,
                    net_margin NUMERIC,
                    cash_flow_ratio NUMERIC,
                    asset_liab_ratio NUMERIC,
                    etl_sync_at TEXT NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO si_stock_finance VALUES
                (1,'000001','2026-03-31','2026-04-30',1,1,1,1,1,1,1,1,1,
                 '2026-04-30 16:00:00'),
                (2,'000002','2026-06-30','2026-08-04',2,2,2,2,2,2,2,2,2,
                 '2026-08-04 14:00:00')
                """
            )
        )
    return engine


def test_financial_contract_is_display_only_and_uses_conservative_notice_close():
    result = FinancialPitAdapter(_finance_engine()).load_fundamentals(
        ("000001", "000002"),
        knowledge_cutoff=_at(14, 30),
        fields=("roe_wtd",),
    )

    assert result.returned_entities == ("000001",)
    assert result.missing_entities == ("000002",)
    assert result.freshness_status == QualityStatus.WARN
    assert set(result.reason_codes) == {"PARTIAL_COVERAGE", "SOURCE_DISPLAY_ONLY"}


def test_announcement_and_concept_snapshots_do_not_leak_same_day_close():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE si_notice_eastmoney (
                    id INTEGER PRIMARY KEY,
                    stock_code TEXT,
                    notice_date TEXT,
                    title TEXT,
                    column_name TEXT,
                    display_time TEXT,
                    detail_url TEXT,
                    art_code TEXT,
                    association_validated INTEGER,
                    etl_sync_at TEXT
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO si_notice_eastmoney VALUES
                (1,'000001','2026-08-03','prior','notice',
                 '2026-08-03 16:00:00','u1','a1',1,'2026-08-03 16:01:00'),
                (2,'000001','2026-08-04','today','notice',
                 '2026-08-04 13:00:00','u2','a2',1,'2026-08-04 13:01:00')
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE qmt_concept_member_snapshot (
                    id INTEGER PRIMARY KEY,
                    snapshot_date TEXT,
                    source TEXT,
                    concept_code TEXT,
                    concept_name TEXT,
                    stock_code TEXT,
                    short_name TEXT,
                    quality_status TEXT,
                    captured_at TEXT
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO qmt_concept_member_snapshot VALUES
                (1,'2026-08-03','qmt','C1','AI','000001','one','QMT_VALIDATED',
                 '2026-08-03 15:10:00'),
                (2,'2026-08-03','qmt','C2','robot','000001','one','QMT_VALIDATED',
                 '2026-08-03 15:10:00'),
                (3,'2026-08-04','qmt','FUTURE_CLOSE','future','000001','one',
                 'QMT_VALIDATED','2026-08-04 14:00:00')
                """
            )
        )

    scope = ScopeRef(scope_type=ScopeType.INSTRUMENT, scope_id="000001")
    notices = AnnouncementPitAdapter(engine).load_events(
        (scope,), knowledge_cutoff=_at(14, 30)
    )
    concepts = ConceptSnapshotPitAdapter(engine).load_market_data(
        ("000001", "000002"),
        knowledge_cutoff=_at(14, 30),
        fields=("concept_code", "concept_name"),
    )

    assert [record.payload["title"] for record in notices.dataset.records] == [
        "prior"
    ]
    assert notices.reason_codes == ("SOURCE_DISPLAY_ONLY",)
    assert {record.payload["concept_code"] for record in concepts.dataset.records} == {
        "C1",
        "C2",
    }
    assert "FUTURE_CLOSE" not in {
        record.payload["concept_code"] for record in concepts.dataset.records
    }
    assert concepts.missing_entities == ("000002",)
    assert set(concepts.reason_codes) == {
        "PARTIAL_COVERAGE",
        "SOURCE_FORWARD_ONLY",
    }


def test_source_registry_is_explicit_and_unknown_sources_have_no_fallback():
    assert source_contract("sm_stock_kline.daily.unadjusted") is DAILY_KLINE_SOURCE
    with pytest.raises(ValueError, match="unregistered PIT source"):
        source_contract("sm_stock_snapshot")

    assert DAILY_KLINE_SOURCE.table_name == "sm_stock_kline"
    assert MINUTE_KLINE_SOURCE.table_name == "sm_stock_minute"
    assert MINUTE_KLINE_SOURCE.default_replay_eligibility == ReplayEligibility.FORWARD_ONLY
    assert DAILY_KLINE_SOURCE.default_replay_eligibility == ReplayEligibility.FORWARD_ONLY
    assert DAILY_KLINE_SOURCE.default_research_status == ResearchStatus.FORWARD_ONLY
    assert NEWS_FLASH_SOURCE.default_research_status == ResearchStatus.FORWARD_ONLY
    assert FINANCIAL_SOURCE.default_replay_eligibility == ReplayEligibility.DISPLAY_ONLY
    assert ANNOUNCEMENT_SOURCE.default_replay_eligibility == ReplayEligibility.DISPLAY_ONLY
    assert CONCEPT_SNAPSHOT_SOURCE.default_replay_eligibility == ReplayEligibility.FORWARD_ONLY
