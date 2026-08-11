from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

from server.trading_v2.config import load_frozen_json
from server.trading_v2.intraday_activation import (
    MarketPoint,
    _failover_opening_fraction,
    assess_market,
)
from server.trading_v2.execution import _paper_snapshot_quote
from server.trading_v2.public_quote_failover import (
    qmt_primary_health,
    reconcile_provider_quotes,
)


NOW = datetime(2026, 7, 30, 10, 15, 20)


def _qmt_health_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    raw = engine.raw_connection()
    raw.create_function(
        "regexp",
        2,
        lambda pattern, value: __import__("re").search(
            pattern, value or ""
        )
        is not None,
    )
    raw.close()
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE si_all_code "
                "(stock_code VARCHAR(16) PRIMARY KEY)"
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE sm_stock_current (
                    stock_code VARCHAR(16) PRIMARY KEY,
                    price REAL,
                    data_source VARCHAR(80),
                    source_time DATETIME,
                    snapshot_at DATETIME
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE st_qmt_realtime_sync_receipt_v2 (
                    receipt_id VARCHAR(64) PRIMARY KEY,
                    source_provider VARCHAR(80),
                    source_snapshot_token VARCHAR(128),
                    source_full_file_token VARCHAR(160),
                    source_generated_at DATETIME,
                    heartbeat_at DATETIME,
                    expected_count INT,
                    observed_count INT,
                    coverage REAL,
                    published_at DATETIME,
                    capture_mode VARCHAR(32),
                    quality_status VARCHAR(16),
                    created_at DATETIME
                )
                """
            )
        )
        connection.execute(
            text(
                "INSERT INTO si_all_code (stock_code) "
                "VALUES ('000001'),('600000')"
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO sm_stock_current (
                    stock_code, price, data_source,
                    source_time, snapshot_at
                )
                VALUES
                    ('000001', 10, 'gj_big_qmt_inner', :at, :at),
                    ('600000', 11, 'gj_big_qmt_inner', :at, :at)
                """
            ),
            {"at": NOW - timedelta(seconds=2)},
        )
    return engine


def test_fresh_database_rows_do_not_bypass_missing_end_to_end_receipt(
    monkeypatch,
):
    engine = _qmt_health_engine()
    monkeypatch.setattr(
        "server.trading_v2.public_quote_failover.get_current_engine",
        lambda: engine,
    )
    config = {
        "required_provider": "GJ_BIG_QMT_INNER",
        "minimum_universe_coverage": 0.85,
        "maximum_minute_age_seconds": 15,
    }

    missing = qmt_primary_health(
        engine,
        now=NOW,
        config=config,
    )
    assert missing["healthy"] is False
    assert missing["reason"] == (
        "QMT_END_TO_END_RECEIPT_MISSING_OR_STALE"
    )

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_qmt_realtime_sync_receipt_v2 (
                    receipt_id, source_provider, source_snapshot_token,
                    source_full_file_token, source_generated_at,
                    heartbeat_at, expected_count, observed_count,
                    coverage, published_at, capture_mode,
                    quality_status, created_at
                )
                VALUES (
                    'r1', 'gj_big_qmt_inner', 's1', 'f1',
                    :at, :at, 2, 2, 1.0, :at,
                    'LIVE_FORWARD', 'PASS', :at
                )
                """
            ),
            {"at": NOW - timedelta(seconds=2)},
        )

    healthy = qmt_primary_health(
        engine,
        now=NOW,
        config=config,
    )
    assert healthy["healthy"] is True
    assert healthy["reason"] == "QMT_END_TO_END_HEALTHY"


def test_qmt_receipt_is_read_from_the_current_data_plane(monkeypatch):
    current_engine = _qmt_health_engine()
    with current_engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_qmt_realtime_sync_receipt_v2 (
                    receipt_id, source_provider, source_snapshot_token,
                    source_full_file_token, source_generated_at,
                    heartbeat_at, expected_count, observed_count,
                    coverage, published_at, capture_mode,
                    quality_status, created_at
                )
                VALUES (
                    'current-r1', 'gj_big_qmt_inner', 's1', 'f1',
                    :at, :at, 2, 2, 1.0, :at,
                    'LIVE_FORWARD', 'PASS', :at
                )
                """
            ),
            {"at": NOW - timedelta(seconds=2)},
        )
    primary_engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
    )
    raw = primary_engine.raw_connection()
    raw.create_function(
        "regexp",
        2,
        lambda pattern, value: __import__("re").search(
            pattern,
            value or "",
        )
        is not None,
    )
    raw.close()
    with primary_engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE si_all_code "
                "(stock_code VARCHAR(16) PRIMARY KEY)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO si_all_code (stock_code) "
                "VALUES ('000001'),('600000')"
            )
        )
    monkeypatch.setattr(
        "server.trading_v2.public_quote_failover.get_current_engine",
        lambda: current_engine,
    )

    result = qmt_primary_health(
        primary_engine,
        now=NOW,
        config={
            "required_provider": "GJ_BIG_QMT_INNER",
            "minimum_universe_coverage": 0.85,
            "maximum_minute_age_seconds": 15,
        },
    )

    assert result["healthy"] is True
    assert result["reason"] == "QMT_END_TO_END_HEALTHY"


def _config(**overrides):
    config = {
        "providers": ["sina", "tencent", "eastmoney"],
        "minimum_provider_count": 2,
        "minimum_sources_per_symbol": 2,
        "minimum_observed_stocks": 2,
        "minimum_universe_coverage": 1.0,
        "minimum_agreement_ratio": 0.98,
        "maximum_price_deviation_pct": 0.35,
        "maximum_change_deviation_pct": 0.35,
        "maximum_source_age_seconds": 30,
    }
    config.update(overrides)
    return config


def _quote(
    code,
    provider,
    *,
    price=10.0,
    pre_close=9.8,
    source_time=NOW,
):
    return {
        "stock_code": code,
        "short_name": f"测试{code}",
        "price": price,
        "pre_close": pre_close,
        "change_pct": (price / pre_close - 1) * 100,
        "volume": 1_000_000,
        "amount": 10_000_000,
        "source_time": source_time,
        "provider": provider,
    }


def test_two_of_three_fresh_sources_can_form_a_pass_snapshot():
    expected = ["600001", "000001"]
    provider_quotes = {
        provider: {
            code: _quote(
                code,
                provider,
                price=10.0 + offset * 0.001,
            )
            for code in expected
        }
        for offset, provider in enumerate(
            ("sina", "tencent", "eastmoney")
        )
    }

    result = reconcile_provider_quotes(
        provider_quotes,
        expected_codes=expected,
        short_name_map={},
        now=NOW,
        config=_config(),
    )

    assert result["quality_status"] == "PASS"
    assert result["observed_count"] == 2
    assert result["coverage"] == 1.0
    assert result["provider_count"] == 3
    assert result["agreement_ratio"] == 1.0
    assert all(row["source_count"] == 3 for row in result["rows"])


def test_single_public_source_is_never_tradeable():
    result = reconcile_provider_quotes(
        {
            "sina": {
                code: _quote(code, "sina")
                for code in ("600001", "000001")
            }
        },
        expected_codes=["600001", "000001"],
        short_name_map={},
        now=NOW,
        config=_config(),
    )

    assert result["quality_status"] == "BLOCK"
    assert result["observed_count"] == 0
    assert result["provider_count"] == 1


def test_provider_price_disagreement_is_excluded_and_blocks():
    result = reconcile_provider_quotes(
        {
            "sina": {
                "600001": _quote("600001", "sina", price=10.0)
            },
            "tencent": {
                "600001": _quote(
                    "600001",
                    "tencent",
                    price=10.2,
                )
            },
        },
        expected_codes=["600001"],
        short_name_map={},
        now=NOW,
        config=_config(
            minimum_observed_stocks=1,
            minimum_universe_coverage=1.0,
        ),
    )

    assert result["quality_status"] == "BLOCK"
    assert result["observed_count"] == 0
    assert result["agreement_ratio"] == 0.0


def test_stale_public_quotes_cannot_form_a_snapshot():
    stale = NOW - timedelta(seconds=31)
    result = reconcile_provider_quotes(
        {
            provider: {
                code: _quote(
                    code,
                    provider,
                    source_time=stale,
                )
                for code in ("600001", "000001")
            }
            for provider in ("sina", "tencent")
        },
        expected_codes=["600001", "000001"],
        short_name_map={},
        now=NOW,
        config=_config(),
    )

    assert result["quality_status"] == "BLOCK"
    assert result["observed_count"] == 0


def test_public_quorum_can_confirm_market_but_is_identified_as_fallback():
    config = load_frozen_json(
        "strategies/intraday_activation_v2.json"
    )[0]
    points = [
        MarketPoint(
            observed_at=NOW - timedelta(seconds=60 * (2 - index)),
            observed_count=5300,
            expected_count=5533,
            coverage=5300 / 5533,
            positive_breadth_pct=breadth,
            equal_weight_return_pct=market_return,
            median_return_pct=market_return,
            source="PUBLIC_QUOTE_QUORUM_V1",
        )
        for index, (breadth, market_return) in enumerate(
            ((66.0, 0.4), (70.0, 0.5), (75.0, 0.7))
        )
    ]

    result = assess_market(
        points,
        previous_regime="RISK_OFF",
        now=NOW,
        config=config,
    )

    assert result.quality_status == "PASS"
    assert result.actionable is True
    assert any("双源一致性替补" in item for item in result.evidence)
    assert _failover_opening_fraction(0.25, config=config) == 0.05
    assert _failover_opening_fraction(0.08, config=config) == 0.04


class _FakeResult:
    def __init__(self, row=None):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row


class _FallbackExecutionConnection:
    def __init__(self):
        self.inserted = False

    def execute(self, statement, params=None):
        sql = str(statement)
        if "FROM sm_stock_current" in sql:
            return _FakeResult(None)
        if "FROM st_public_quote_current_v2" in sql:
            return _FakeResult(
                {
                    "stock_code": "600001",
                    "price": 10.0,
                    "pre_close": 9.8,
                    "volume": 1_000_000,
                    "snapshot_at": NOW,
                    "etl_sync_at": NOW,
                    "data_source": "PUBLIC_QUOTE_QUORUM_V1",
                    "source_time": NOW,
                    "received_at": NOW,
                    "batch_id": "batch-public",
                }
            )
        if "INSERT IGNORE INTO st_quote_event_v2" in sql:
            self.inserted = True
            return _FakeResult()
        raise AssertionError(sql)


def test_paper_matching_uses_public_quorum_when_qmt_is_missing():
    connection = _FallbackExecutionConnection()

    quote, liquidity, provider = _paper_snapshot_quote(
        connection,
        stock_code="600001",
        now=NOW,
        lot_size=100,
        already_filled_quantity=0,
    )

    assert quote is not None
    assert quote.last_price == 10
    assert liquidity == 1000
    assert provider.startswith("paper_public_quorum_snapshot:")
    assert connection.inserted is True
