# -*- coding: utf-8 -*-
from contextlib import nullcontext
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biz.stock_info import sync_stock_info
from biz.stock_market import sync_stock_market
from biz.stock_market import sync_stock_snapshot
from biz.stock_market.sync_stock_snapshot import _coerce_numeric_columns
from tools import crawl_realtime_batch
from tools import crawl_minute_kline
from tools import fetch_sm_stock_capital_flow_daily as daily_flow
from tools import fetch_hot_rank_xq
from tools import fetch_hot_pop_rank_east
from tools import refresh_market_overview_daily
from tools.fetch_sm_stock_kline_daily import (
    _apply_reference_prices,
    _batch_items_to_daily_frame,
    _build_source_trace,
    _parse_tencent_reference,
    _rows_match,
    _validate_daily_frame,
    _with_data_source,
)
from tools.refresh_market_overview_daily import resolve_dates


def test_qmt_source_is_ignored_when_runtime_is_missing():
    with patch.dict(sync_stock_info.os.environ, {"DATA_SOURCE_CODE_LIST": "qmt"}, clear=True), patch(
        "integrations.qmt.bridge.is_configured",
        return_value=False,
    ):
        assert sync_stock_info._source_value("DATA_SOURCE_CODE_LIST", default="adata") == "adata"


def test_stock_snapshot_numeric_columns_accept_object_values():
    df = pd.DataFrame({"close": ["10.5", "bad"], "volume": ["1000", None], "name": ["a", "b"]})

    out = _coerce_numeric_columns(df, ["close", "volume"])

    assert out["close"].iloc[0] == 10.5
    assert pd.isna(out["close"].iloc[1])
    assert out["volume"].iloc[0] == 1000


def test_stock_snapshot_get_engine_uses_batch_engine():
    engine = object()

    with patch("biz.stock_market.sync_stock_snapshot.create_batch_engine", return_value=engine) as create_batch_engine:
        assert sync_stock_snapshot.get_engine() is engine

    create_batch_engine.assert_called_once_with(pool_size=5, max_overflow=10)


def test_stock_snapshot_read_frame_uses_batch_reader():
    engine = object()
    frame = pd.DataFrame([{"stock_code": "000001"}])
    sql = text("SELECT stock_code FROM sm_stock_kline")

    with patch("biz.stock_market.sync_stock_snapshot.read_frame", return_value=frame) as read_frame:
        assert sync_stock_snapshot._read_frame(sql, engine, params={"d": "2026-07-01"}) is frame

    read_frame.assert_called_once_with(sql, engine, params={"d": "2026-07-01"})


def test_stock_snapshot_merges_each_stocks_latest_current_quote():
    trade_date = "2026-08-08"
    daily = pd.DataFrame([
        {
            "stock_code": "000001",
            "short_name": "Alpha",
            "trade_date": trade_date,
            "open": 10.0,
            "close": 10.0,
            "high": 10.2,
            "low": 9.8,
            "pre_close": 9.9,
            "change": 0.1,
            "change_pct": 1.01,
            "volume": 100.0,
            "amount": 1000.0,
            "turnover_ratio": 1.0,
        },
        {
            "stock_code": "600000",
            "short_name": "Beta",
            "trade_date": trade_date,
            "open": 20.0,
            "close": 20.0,
            "high": 20.2,
            "low": 19.8,
            "pre_close": 19.9,
            "change": 0.1,
            "change_pct": 0.5,
            "volume": 200.0,
            "amount": 2000.0,
            "turnover_ratio": 2.0,
        },
    ])
    current = pd.DataFrame([
        {
            "stock_code": "000001",
            "cur_price": 10.5,
            "cur_change_pct": 5.0,
            "snapshot_at": "2026-08-10 10:00:01",
        },
        {
            "stock_code": "600000",
            "cur_price": 20.8,
            "cur_change_pct": 4.0,
            "snapshot_at": "2026-08-10 10:00:02",
        },
        {
            "stock_code": "000001",
            "cur_price": 10.9,
            "cur_change_pct": 9.0,
            "snapshot_at": "2026-08-10 10:00:03",
        },
    ])
    history = pd.DataFrame([
        {"stock_code": "000001", "close_n": 9.5},
        {"stock_code": "600000", "close_n": 19.5},
    ])
    shares = pd.DataFrame([
        {"stock_code": "000001", "total_shares": 1000},
        {"stock_code": "600000", "total_shares": 2000},
    ])
    industry = pd.DataFrame([
        {"stock_code": "000001", "industry_name": "Bank"},
        {"stock_code": "600000", "industry_name": "Bank"},
    ])
    queries = []

    def fake_read(statement, _engine, params=None):
        sql = " ".join(str(statement).split())
        queries.append(sql)
        if "FROM sm_stock_kline k" in sql:
            return daily.copy()
        if "FROM sm_stock_current" in sql:
            return current.copy()
        if "MAX(trade_date) AS d FROM sm_stock_capital_flow_daily" in sql:
            return pd.DataFrame([{"d": None}])
        if "close AS close_n FROM sm_stock_kline" in sql:
            return history.copy()
        if "FROM si_stock_shares" in sql:
            return shares.copy()
        if "FROM si_industry_sw" in sql:
            return industry.copy()
        raise AssertionError("unexpected snapshot query: %s" % sql)

    with patch(
        "biz.stock_market.sync_stock_snapshot._read_frame",
        side_effect=fake_read,
    ), patch(
        "biz.stock_market.sync_stock_snapshot.get_nth_trade_date",
        return_value=trade_date,
    ):
        result = sync_stock_snapshot.fetch_snapshot(object(), trade_date)

    by_code = result.set_index("stock_code")
    assert len(result) == 2
    assert by_code.loc["000001", "price"] == 10.9
    assert by_code.loc["600000", "price"] == 20.8
    assert "snapshot_at" not in result.columns
    current_sql = next(sql for sql in queries if "FROM sm_stock_current" in sql)
    assert "MAX(snapshot_at)" not in current_sql
    assert "ORDER BY stock_code, snapshot_at DESC" in current_sql


def test_stock_snapshot_write_uses_atomic_staging_swap():
    engine = SimpleNamespace()
    begin_conn = SimpleNamespace(execute=lambda statement, params=None: executed.append(str(statement)))
    connect_result = SimpleNamespace(scalar=lambda: 1)
    connect_conn = SimpleNamespace(execute=lambda statement, params=None: connect_result)
    engine.begin = lambda: nullcontext(begin_conn)
    engine.connect = lambda: nullcontext(connect_conn)
    executed = []
    frame = pd.DataFrame([{"stock_code": "000001", "trade_date": "2026-07-17"}])

    with patch(
        "biz.stock_market.sync_stock_snapshot._read_frame",
        side_effect=RuntimeError("portfolio unavailable"),
    ), patch("biz.stock_market.sync_stock_snapshot.write_frame") as write_frame:
        sync_stock_snapshot.write_snapshot(engine, frame)

    sql = "\n".join(executed).upper()
    assert "TRUNCATE TABLE SM_STOCK_SNAPSHOT" not in sql
    assert "CREATE TABLE SM_STOCK_SNAPSHOT_STAGE LIKE SM_STOCK_SNAPSHOT" in sql
    assert "RENAME TABLE SM_STOCK_SNAPSHOT TO SM_STOCK_SNAPSHOT_BACKUP" in sql
    assert write_frame.call_args.args[1] == "sm_stock_snapshot_stage"


def _fake_replace_engine(executed):
    begin_conn = SimpleNamespace(execute=lambda statement, params=None: executed.append(str(statement)))
    count_result = SimpleNamespace(scalar=lambda: 1)
    connect_conn = SimpleNamespace(execute=lambda statement, params=None: count_result)
    engine = SimpleNamespace(
        begin=lambda: nullcontext(begin_conn),
        connect=lambda: nullcontext(connect_conn),
    )
    return engine


def test_realtime_current_uses_atomic_staging_swap():
    executed = []
    engine = _fake_replace_engine(executed)
    frame = pd.DataFrame([{"stock_code": "000001", "price": 10.0}])

    with patch(
        "tools.crawl_realtime_batch.mysql_named_lock",
        return_value=nullcontext(engine),
    ), patch("tools.crawl_realtime_batch.write_frame") as write_frame:
        assert crawl_realtime_batch._replace_stock_current_snapshot(engine, frame) == 1

    sql = "\n".join(executed).upper()
    assert "TRUNCATE" not in sql
    assert "CREATE TABLE SM_STOCK_CURRENT_STAGE LIKE SM_STOCK_CURRENT" in sql
    assert "RENAME TABLE SM_STOCK_CURRENT TO SM_STOCK_CURRENT_BACKUP" in sql
    assert write_frame.call_args.args[1] == "sm_stock_current_stage"


def test_legacy_stock_current_uses_the_same_atomic_writer():
    executed = []
    engine = _fake_replace_engine(executed)
    frame = pd.DataFrame([{"stock_code": "000001", "price": 10.0}])

    with patch(
        "biz.stock_market.sync_stock_market.mysql_named_lock",
        return_value=nullcontext(engine),
    ), patch("biz.stock_market.sync_stock_market.write_frame") as write_frame:
        assert sync_stock_market.replace_stock_current_snapshot(engine, frame) == 1

    sql = "\n".join(executed).upper()
    assert "TRUNCATE" not in sql
    assert "CREATE TABLE SM_STOCK_CURRENT_STAGE LIKE SM_STOCK_CURRENT" in sql
    assert "RENAME TABLE SM_STOCK_CURRENT TO SM_STOCK_CURRENT_BACKUP" in sql
    assert write_frame.call_args.args[1] == "sm_stock_current_stage"


def test_realtime_snapshot_empty_source_fails_when_coverage_is_required():
    with patch("tools.crawl_realtime_batch.fetch_batch", return_value=[]):
        try:
            crawl_realtime_batch.refresh_snapshot(object(), min_coverage=0.90)
        except RuntimeError as exc:
            assert "returned no rows" in str(exc)
        else:
            raise AssertionError("empty realtime source must fail quality-gated refresh")


def test_realtime_target_universe_uses_latest_available_daily_kline():
    main = create_engine("sqlite://")
    history = create_engine("sqlite://")
    with history.begin() as conn:
        conn.execute(text(
            "CREATE TABLE sm_stock_kline "
            "(stock_code TEXT, trade_date TEXT, k_type INT, adjust_type INT)"
        ))
        conn.execute(text(
            "INSERT INTO sm_stock_kline VALUES "
            "('000001','2026-07-22',1,0),"
            "('000001','2026-07-23',1,0),"
            "('600000','2026-07-23',1,0)"
        ))

    with patch(
        "tools.crawl_realtime_batch.get_kline_engine",
        return_value=history,
    ):
        codes = crawl_realtime_batch._read_target_stock_codes(
            main,
            "2026-07-24",
        )

    assert codes == {"000001", "600000"}


def test_eastmoney_batch_paginates_by_stable_security_code():
    response = MagicMock()
    response.json.return_value = {"data": {"diff": []}}

    with patch.object(
        crawl_realtime_batch.SESSION,
        "get",
        return_value=response,
    ) as get:
        assert crawl_realtime_batch.fetch_batch("demo", "f12") == []

    assert get.call_args.kwargs["params"]["fid"] == "f12"


def test_qmt_current_validation_rejects_incomplete_quote_batch(monkeypatch):
    monkeypatch.setenv("CURRENT_MIN_COVERAGE", "0.98")
    frame = pd.DataFrame([
        {"stock_code": "000001", "price": 10.0, "volume": 1, "amount": 10},
    ])

    with pytest.raises(RuntimeError, match="coverage below threshold"):
        sync_stock_market._validate_stock_current_frame(
            frame,
            ["000001", "600519"],
            source="qmt",
        )


def test_qmt_current_validation_rejects_bad_prices(monkeypatch):
    monkeypatch.setenv("CURRENT_MIN_COVERAGE", "0.0")
    frame = pd.DataFrame([
        {"stock_code": "000001", "price": 0, "volume": 1, "amount": 10},
    ])

    with pytest.raises(RuntimeError, match="invalid values"):
        sync_stock_market._validate_stock_current_frame(
            frame,
            ["000001"],
            source="qmt",
        )


def test_sina_current_fallback_drops_bad_rows_and_keeps_valid_quotes(monkeypatch):
    batches = []

    def fake_fetch(codes):
        batches.append(codes)
        return pd.DataFrame([
            {
                "stock_code": codes[0],
                "short_name": "平安银行",
                "price": 10.5,
                "change": 0.1,
                "change_pct": 0.95,
                "volume": 100,
                "amount": 1000,
            },
            {
                "stock_code": codes[-1],
                "short_name": "坏值",
                "price": 0,
                "change": 0,
                "change_pct": 0,
                "volume": 0,
                "amount": 0,
            },
        ])

    with patch("biz.stock_market.realtime_quotes.fetch_list_market_current", side_effect=fake_fetch):
        frame = sync_stock_market._fetch_sina_stock_current(
            ["000001", "600519"],
            {"000001": "平安银行", "600519": "贵州茅台"},
        )

    assert batches == [["000001", "600519"]]
    assert frame["stock_code"].tolist() == ["000001"]
    assert frame["price"].tolist() == [10.5]


def test_quote_values_fall_back_to_previous_close_for_suspended_rows():
    quote = crawl_realtime_batch._quote_values({
        "f2": "-", "f3": "-", "f4": "-", "f15": "-",
        "f16": "-", "f17": "-", "f18": "12.34",
    })

    assert quote == {
        "price": 12.34,
        "change": 0.0,
        "change_pct": 0.0,
        "open": 12.34,
        "high": 12.34,
        "low": 12.34,
    }


def test_eastmoney_stock_filters_include_bse_equities_only():
    assert "m:0+t:81+s:2048" in crawl_realtime_batch.EASTMONEY_A_SHARE_FS
    assert crawl_realtime_batch._normalize_a_share_code("920799") == "920799"
    assert crawl_realtime_batch._normalize_a_share_code("830799") == "830799"
    assert crawl_realtime_batch._normalize_a_share_code("899050") == ""
    assert crawl_realtime_batch._normalize_a_share_code("810011") == ""
    assert crawl_realtime_batch._normalize_a_share_code("900901") == ""


def test_daily_flow_universe_is_read_from_routed_kline_engine():
    main = create_engine("sqlite://")
    history = create_engine("sqlite://")
    with main.begin() as conn:
        conn.execute(text("CREATE TABLE si_all_code (stock_code TEXT)"))
        conn.execute(text("INSERT INTO si_all_code VALUES ('600000')"))
    with history.begin() as conn:
        conn.execute(text(
            "CREATE TABLE sm_stock_kline (stock_code TEXT, trade_date TEXT, k_type INT, adjust_type INT)"
        ))
        conn.execute(text(
            "INSERT INTO sm_stock_kline VALUES "
            "('000001','2026-07-17',1,0),('920799','2026-07-17',1,0),"
            "('899050','2026-07-17',1,0),('600519','2026-07-17',1,1)"
        ))

    codes = daily_flow._read_target_stock_codes(
        main, "2026-07-17", kline_engine=history,
    )

    assert codes == ["000001", "920799"]


def test_daily_flow_baidu_scope_excludes_unsupported_bse_and_non_stocks():
    sources = [("baidu", "百度API", object())]

    codes = daily_flow._filter_source_supported_codes(
        ["000001", "300001", "600000", "688001", "920799", "899050"],
        sources,
    )

    assert codes == ["000001", "300001", "600000", "688001"]


def test_daily_flow_minute_close_only_fills_missing_target_rows():
    primary = pd.DataFrame([{
        "stock_code": "000001", "trade_date": "2026-07-17",
        "main_net_inflow": 10, "data_source": "east_batch",
    }])
    fallback = pd.DataFrame([
        {
            "stock_code": "000001", "trade_date": "2026-07-17",
            "main_net_inflow": 999, "data_source": "east_min_close",
        },
        {
            "stock_code": "920799", "trade_date": "2026-07-17",
            "main_net_inflow": 20, "data_source": "east_min_close",
        },
        {
            "stock_code": "899050", "trade_date": "2026-07-17",
            "main_net_inflow": 30, "data_source": "east_min_close",
        },
    ])

    merged = daily_flow._merge_missing_daily_rows(
        primary, fallback, {"000001", "920799"},
    ).sort_values("stock_code")

    assert merged["stock_code"].tolist() == ["000001", "920799"]
    assert merged["main_net_inflow"].tolist() == [10, 20]
    assert merged["data_source"].tolist() == ["east_batch", "east_min_close"]


def test_realtime_and_minute_market_time_boundaries_are_exact():
    cases = {
        (9, 24): False,
        (9, 25): True,
        (11, 35): True,
        (11, 36): False,
        (12, 54): False,
        (12, 55): True,
        (15, 5): True,
        (15, 6): False,
    }

    with patch("tools.crawl_realtime_batch._is_trade_day", return_value=True), patch(
        "tools.crawl_minute_kline._is_trade_day",
        return_value=True,
    ):
        for (hour, minute), expected in cases.items():
            now = datetime(2026, 7, 17, hour, minute)
            assert crawl_realtime_batch.is_trading_time(object(), now) is expected
            assert crawl_minute_kline.is_trading_time(object(), now) is expected


def test_realtime_and_minute_market_time_reject_non_trade_day():
    now = datetime(2026, 7, 18, 10, 0)
    with patch("tools.crawl_realtime_batch._is_trade_day", return_value=False), patch(
        "tools.crawl_minute_kline._is_trade_day",
        return_value=False,
    ):
        assert crawl_realtime_batch.is_trading_time(object(), now) is False
        assert crawl_minute_kline.is_trading_time(object(), now) is False


def test_xueqiu_hot_rank_filters_non_a_shares_and_reranks():
    response = SimpleNamespace(
        raise_for_status=lambda: None,
        json=lambda: {
            "error_code": 0,
            "data": {
                "items": [
                    {"symbol": "NVDA", "name": "NVIDIA"},
                    {"symbol": "SH600000", "name": "浦发银行", "current": 10.0},
                    {"symbol": "HK00700", "name": "腾讯控股"},
                    {"symbol": "SZ000001", "name": "平安银行", "current": 12.0},
                    {"symbol": "BJ430047", "name": "诺思兰德", "current": 18.0},
                ]
            },
        },
    )

    with patch.object(fetch_hot_rank_xq._SESSION, "get", return_value=response):
        frame = fetch_hot_rank_xq._fetch_hot_rank_xq()

    assert frame is not None
    assert frame["stock_code"].tolist() == ["600000", "000001", "430047"]
    assert frame["rank"].tolist() == [1, 2, 3]


def test_eastmoney_hot_rank_has_environment_threshold_dependency_loaded():
    assert fetch_hot_pop_rank_east.os.environ is not None


def test_stock_flow_mode_runs_price_and_flow_with_the_same_universe():
    summary_price = {
        "table": "sm_stock_minute", "total": 2, "ok": 2, "coverage": 1.0,
    }
    summary_flow = {
        "table": "sm_stock_capital_flow_min", "total": 2, "ok": 2, "coverage": 1.0,
    }
    codes = [("000001", 0), ("600000", 1)]
    primary_engine = object()
    minute_engine = object()
    kline_engine = object()

    with patch.object(
        crawl_minute_kline.sys,
        "argv",
        ["crawl_minute_kline.py", "--type", "stock_flow", "--min-coverage", "0.95"],
    ), patch("tools.crawl_minute_kline.create_batch_engine", return_value=primary_engine), patch(
        "tools.crawl_minute_kline.get_minute_engine", return_value=minute_engine,
    ), patch(
        "tools.crawl_minute_kline.get_kline_engine", return_value=kline_engine,
    ), patch(
        "tools.crawl_minute_kline.get_latest_kline_stock_codes",
        side_effect=[codes, codes],
    ) as get_universe, patch(
        "tools.crawl_minute_kline.crawl_kline",
        return_value=summary_price,
    ) as crawl_price, patch(
        "tools.crawl_minute_kline.crawl_flow",
        return_value=summary_flow,
    ) as crawl_flow, patch(
        "tools.crawl_minute_kline._mirror_selected_tables",
        return_value=[],
    ) as mirror:
        assert crawl_minute_kline.main() == 0

    assert crawl_price.call_args.args[0] is minute_engine
    assert crawl_price.call_args.args[1] == codes
    assert crawl_flow.call_args.args[0] is primary_engine
    assert crawl_flow.call_args.args[1] == codes
    assert get_universe.call_args_list[0].args == (kline_engine,)
    assert get_universe.call_args_list[0].kwargs == {"fallback_engine": primary_engine}
    assert get_universe.call_args_list[1].args == (kline_engine,)
    assert get_universe.call_args_list[1].kwargs == {"fallback_engine": primary_engine}
    mirror.assert_not_called()


def test_stock_flow_mirrors_both_canonical_minute_tables():
    assert crawl_minute_kline._minute_mirror_tables("stock_flow") == [
        "sm_stock_minute",
        "sm_stock_capital_flow_min",
    ]


def test_new_bse_codes_try_eastmoney_market_two_first():
    assert crawl_minute_kline._market_candidates("920079", 0) == [0, 2, 1]
    assert crawl_minute_kline._primary_market("920079") == 2


def test_stock_minute_request_uses_unadjusted_prices():
    response = SimpleNamespace(
        json=lambda: {"data": {"klines": ["2026-07-24 15:00,1,1,1,1,1,1,0,0,0,0"]}},
    )
    with patch.object(
        crawl_minute_kline.SESSION,
        "get",
        return_value=response,
    ) as request:
        rows = crawl_minute_kline.fetch_minute_kline("000001", 0)

    assert rows
    assert request.call_args.kwargs["params"]["fqt"] == "0"


def test_incomplete_close_repair_selects_missing_and_preclose_stocks():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        ("000001", datetime(2026, 7, 24, 15, 0)),
        ("000002", datetime(2026, 7, 24, 14, 18)),
    ]
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    result = crawl_minute_kline.incomplete_close_stock_codes(
        engine,
        [("000001", 0), ("000002", 0), ("600000", 1)],
        trade_date="2026-07-24",
    )

    assert result == [("000002", 0), ("600000", 1)]


def test_mirror_stage_cleanup_only_drops_exact_generated_names():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        ("sm_stock_minute_stage_012345abcdef",),
        ("sm_stock_minute_stage_012345ABCDEf",),
        ("sm_stock_minute_stage_012345abcdef_extra",),
        ("other_stage_012345abcdef",),
    ]
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    with patch("tools.crawl_minute_kline._drop_run_stage") as drop_stage:
        dropped = crawl_minute_kline._cleanup_mirror_stages(engine, "sm_stock_minute")

    assert dropped == ["sm_stock_minute_stage_012345abcdef"]
    drop_stage.assert_called_once_with(engine, "sm_stock_minute_stage_012345abcdef")


def test_abandoned_stage_cleanup_is_age_bounded_and_name_safe():
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        ("sm_stock_minute_stage_012345abcdef",),
        ("sm_stock_minute_stage_012345abcdef_extra",),
        ("other_stage_012345abcdef",),
    ]
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    with patch("tools.crawl_minute_kline._drop_run_stage") as drop_stage:
        dropped = crawl_minute_kline._cleanup_abandoned_run_stages(
            engine,
            "sm_stock_minute",
        )

    assert dropped == ["sm_stock_minute_stage_012345abcdef"]
    sql = str(conn.execute.call_args.args[0])
    assert "CREATE_TIME < :cutoff" in sql
    drop_stage.assert_called_once_with(engine, "sm_stock_minute_stage_012345abcdef")


def test_cleanup_only_mode_does_not_open_primary_business_db():
    with patch.object(
        crawl_minute_kline.sys,
        "argv",
        ["crawl_minute_kline.py", "--type", "flow", "--cleanup-mirror-stages-only"],
    ), patch(
        "tools.crawl_minute_kline._cleanup_selected_mirror_stages",
        return_value=[],
    ) as cleanup, patch(
        "tools.crawl_minute_kline.create_batch_engine",
    ) as create_primary:
        assert crawl_minute_kline.main() == 0

    cleanup.assert_called_once_with("flow")
    create_primary.assert_not_called()


def test_prune_only_mode_uses_primary_engine_and_skips_fetching():
    primary_engine = object()
    with patch.object(
        crawl_minute_kline.sys,
        "argv",
        ["crawl_minute_kline.py", "--type", "stock_flow", "--prune-orphans-only"],
    ), patch(
        "tools.crawl_minute_kline.create_batch_engine",
        return_value=primary_engine,
    ), patch(
        "tools.crawl_minute_kline._prune_selected_orphans",
        return_value=[],
    ) as prune, patch(
        "tools.crawl_minute_kline.crawl_kline",
    ) as crawl_price, patch(
        "tools.crawl_minute_kline.crawl_flow",
    ) as crawl_flow:
        assert crawl_minute_kline.main() == 0

    prune.assert_called_once_with(primary_engine, "stock_flow")
    crawl_price.assert_not_called()
    crawl_flow.assert_not_called()


def test_daily_kline_reference_prices_fill_change_fields_and_detect_mismatch():
    frame = pd.DataFrame([
        {"stock_code": "000001", "close": 10.5, "pre_close": None, "change": None, "change_pct": None},
        {"stock_code": "600000", "close": 8.0, "pre_close": None, "change": None, "change_pct": None},
    ])

    out, stats = _apply_reference_prices(
        frame,
        {"000001": (10.5, 10.0), "600000": (9.0, 8.5)},
        {"600000": 7.5},
    )

    assert out.loc[0, "pre_close"] == 10.0
    assert round(out.loc[0, "change_pct"], 4) == 5.0
    assert out.loc[1, "pre_close"] == 7.5
    assert stats["compared"] == 2
    assert stats["mismatches"] == 1
    assert stats["valid_change_fields"] == 2


def test_latest_daily_kline_batch_builds_accurate_active_and_suspended_bars():
    frame = _batch_items_to_daily_frame(
        [
            {
                "f12": "000001", "f14": "平安银行", "f2": "12.00", "f18": "10.00",
                "f17": "10.50", "f15": "12.20", "f16": "10.20", "f5": "1234",
                "f6": "1500000", "f8": "1.25",
            },
            {
                "f12": "600000", "f14": "浦发银行", "f2": "-", "f18": "8.50",
                "f17": "-", "f15": "-", "f16": "-", "f5": "0", "f6": "0",
                "f8": "0",
            },
            {"f12": "430047", "f14": "北交所样本", "f2": "20", "f18": "19"},
        ],
        "2026-07-17",
    )

    assert frame["stock_code"].tolist() == ["000001", "600000", "430047"]
    active = frame.iloc[0]
    assert active["volume"] == 123400.0
    assert active["open"] == 10.5
    assert active["high"] == 12.2
    assert active["low"] == 10.2
    assert round(active["change_pct"], 4) == 20.0
    suspended = frame.iloc[1]
    assert suspended["open"] == suspended["high"] == suspended["low"] == 8.5
    assert suspended["change"] == 0.0
    assert suspended["change_pct"] == 0.0
    assert frame.iloc[2]["close"] == 20.0


def test_daily_kline_structural_gate_rejects_impossible_ohlc():
    frame = pd.DataFrame([{
        "stock_code": "603031",
        "trade_date": "2026-07-10",
        "k_type": 1,
        "adjust_type": 0,
        "open": 49.15,
        "high": 50.65,
        "low": 47.94,
        "close": 47.90,
        "volume": 100,
        "amount": 1000,
    }])

    with pytest.raises(RuntimeError, match="bad_ohlc_range"):
        _validate_daily_frame(frame, "2026-07-10")


def test_daily_kline_cross_source_comparison_accepts_rounding_only():
    primary = {
        "open": 49.15,
        "high": 50.65,
        "low": 47.53,
        "close": 47.67,
        "amount": 900809249.0,
    }
    reference = {
        "open": 49.15,
        "high": 50.65,
        "low": 47.53,
        "close": 47.67,
        "amount": 900809200.0,
    }

    matched, differences = _rows_match(primary, reference)

    assert matched is True
    assert differences["close"] == 0


def test_tencent_daily_kline_reference_parser_uses_unadjusted_day_bar():
    payload = {
        "code": 0,
        "data": {
            "sh603031": {
                "day": [
                    ["2026-07-10", "49.150", "47.670", "50.650", "47.530", "182811.000"],
                ],
            },
        },
    }

    assert _parse_tencent_reference(payload, "sh603031", "2026-07-10") == {
        "open": 49.15,
        "close": 47.67,
        "high": 50.65,
        "low": 47.53,
    }


def test_daily_kline_source_trace_marks_only_verified_sample_rows():
    fetched_at = datetime(2026, 7, 23, 18, 0, 0)
    frame = _with_data_source(pd.DataFrame([{
        "stock_code": "000001",
        "trade_date": "2026-07-23",
        "k_type": 1,
        "adjust_type": 0,
        "open": 10,
        "high": 11,
        "low": 9,
        "close": 10.5,
        "volume": 100,
        "amount": 1000,
        "pre_close": 9.8,
    }, {
        "stock_code": "600000",
        "trade_date": "2026-07-23",
        "k_type": 1,
        "adjust_type": 0,
        "open": 8,
        "high": 8.2,
        "low": 7.9,
        "close": 8.1,
        "volume": 200,
        "amount": 1600,
        "pre_close": 8,
    }]), "sina")

    trace = _build_source_trace(
        frame,
        run_id="run-1",
        verified_codes={"000001": "east"},
        fetched_at=fetched_at,
    ).set_index("stock_code")

    assert trace.loc["000001", "verification_status"] == "cross_checked"
    assert trace.loc["000001", "verified_source"] == "east"
    assert trace.loc["600000", "verification_status"] == "source_only"
    assert trace.loc["600000", "data_source"] == "sina"


def test_market_overview_defaults_to_latest_trade_date():
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE sm_stock_kline (trade_date TEXT, k_type INT, adjust_type INT)"))
        conn.execute(
            text("INSERT INTO sm_stock_kline VALUES (:d, 1, 0)"),
            [{"d": "2026-06-29"}, {"d": "2026-06-30"}],
        )
        assert resolve_dates(conn, dates=[], start_date="", end_date="") == ["2026-06-30"]


def test_market_overview_main_uses_batch_engine():
    primary_conn = object()
    kline_conn = object()
    primary_engine = SimpleNamespace(begin=lambda: nullcontext(primary_conn))
    kline_engine = SimpleNamespace(connect=lambda: nullcontext(kline_conn))

    with patch.object(
        refresh_market_overview_daily.sys,
        "argv",
        ["refresh_market_overview_daily.py", "--dates", "2026-06-30"],
    ), patch("tools.refresh_market_overview_daily.load_project_env"), patch(
        "tools.refresh_market_overview_daily.create_batch_engine",
        return_value=primary_engine,
    ) as create_batch_engine, patch(
        "tools.refresh_market_overview_daily.get_kline_engine",
        return_value=kline_engine,
    ), patch("tools.refresh_market_overview_daily.ensure_table") as ensure_table, patch(
        "tools.refresh_market_overview_daily.resolve_dates",
        return_value=["2026-06-30"],
    ) as resolve_dates_mock, patch(
        "tools.refresh_market_overview_daily.refresh_one",
        return_value={"date": "2026-06-30", "status": "ok"},
    ) as refresh_one:
        assert refresh_market_overview_daily.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    ensure_table.assert_called_once_with(primary_conn)
    resolve_dates_mock.assert_called_once_with(kline_conn, dates=["2026-06-30"], start_date="", end_date="")
    refresh_one.assert_called_once_with(primary_conn, kline_conn, "2026-06-30")
