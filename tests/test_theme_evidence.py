import pytest
from sqlalchemy import create_engine, event, text

from biz.review import theme_evidence as theme


DAYS = ["2026-10-26", "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30"]
DAY = DAYS[-1]
ASOF = "2026-11-01T18:00:00+08:00"


@pytest.fixture
def source(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    monkeypatch.setattr(theme, "routed_read_engine", lambda sql, supplied: supplied)
    statements = []
    event.listen(engine, "before_cursor_execute", lambda conn, cursor, statement, parameters, context, many: statements.append(statement))
    with engine.begin() as conn:
        for sql in [
            "CREATE TABLE si_trade_calendar(trade_date TEXT,trade_status INTEGER)",
            "CREATE TABLE st_hot_concept_ths_daily(snapshot_date TEXT,plate_type INTEGER,rank INTEGER,concept_code TEXT,concept_name TEXT,change_pct REAL,hot_tag TEXT,etl_sync_at TEXT)",
            "CREATE TABLE st_hot_rank_fused(snapshot_date TEXT,fused_rank INTEGER,stock_code TEXT,short_name TEXT,change_pct REAL,industry_name TEXT,etl_sync_at TEXT)",
            "CREATE TABLE st_hot_rank_ths(snapshot_date TEXT,rank INTEGER,stock_code TEXT,short_name TEXT,change_pct REAL,pop_tag TEXT,concept_tag TEXT,etl_sync_at TEXT)",
            "CREATE TABLE sm_stock_kline(stock_code TEXT,short_name TEXT,trade_date TEXT,adjust_type INTEGER,k_type INTEGER,open REAL,close REAL,pre_close REAL,high REAL,low REAL,amount REAL,volume REAL,data_source TEXT,quality_status TEXT,received_at TEXT,etl_sync_at TEXT)",
        ]:
            conn.execute(text(sql))
        for day in DAYS:
            conn.execute(text("INSERT INTO si_trade_calendar VALUES(:d,1)"), {"d": day})
            for kind, code, name, ret in [(1, "886015", "创新药", 1), (1, "886021", "商业航天", -4),
                                           (2, "881156", "化学制药", 0.5), (3, "BK1201", "电子", -4.62),
                                           (4, "BK1036", "半导体", -5.45)]:
                conn.execute(text("INSERT INTO st_hot_concept_ths_daily VALUES(:d,:kind,:rank,:code,:name,:ret,'来源标签',:received)"),
                             {"d": day, "kind": kind, "rank": 2 if name == "商业航天" else 1, "code": code, "name": name,
                              "ret": ret, "received": f"{day} 17:15:00"})
            conn.execute(text("INSERT INTO sm_stock_kline VALUES('000001','样本药业',:d,0,1,10,:close,10,11,10,1050,100,'fixture','VERIFIED',:received,:received)"),
                         {"d": day, "close": 11 if day == DAY else 10, "received": f"{day} 18:00:00"})
        conn.execute(text("INSERT INTO st_hot_rank_fused VALUES(:d,1,'000001','样本药业',10,'化学制药',:received)"),
                     {"d": DAY, "received": DAY + " 20:40:00"})
        conn.execute(text("INSERT INTO st_hot_rank_ths VALUES(:d,1,'000001','样本药业',10,'持续上榜','创新药;医药概念',:received)"),
                     {"d": DAY, "received": DAY + " 17:15:00"})
    statements.clear()
    return engine, statements


def test_date_exact_bulk_sources_price_checks_and_source_semantics(source):
    engine, statements = source
    result = theme.collect_theme_evidence(engine, DAY, asof=ASOF)
    assert result["status"] == "OBSERVED"
    assert result["history_dates"] == DAYS
    sector = next(row for row in result["sectors"] if row["name"] == "创新药")
    assert sector["cumulative_return_5d_pct"] == pytest.approx((1.01 ** 5 - 1) * 100, abs=1e-6)
    assert sector["history_complete_5d"] is True
    assert sector["rank_basis"] == "source_attention_rank"
    east = next(row for row in result["sectors"] if row["plate_type"] == 3)
    assert east["source"] == "东财一级行业行情榜"
    assert east["rank_basis"] == "source_quoted_turnover_rank"
    stock = result["stocks"][0]
    assert stock["sector_names"] == ["创新药", "化学制药"]
    assert stock["change_pct"] == 10
    assert stock["price_verification"]["status"] == "OBSERVED"
    assert stock["price_verification"]["same_day_source_checks"]["fused"]["matches_unadjusted_close"] is True
    assert stock["identity"] == "身份待确认"
    assert "非PIT" in stock["tag_source"]
    assert result["amount_quality"]["target"]["status"] == "OBSERVED"
    assert result["amount_quality"]["previous"]["trade_date"] == DAYS[-2]
    assert "not_whole_market" in result["amount_quality"]["target"]["coverage_scope"]
    assert len(statements) == 5  # No N+1 lookups or retry paths.
    assert all("MAX_EXECUTION_TIME(15000)" in statement for statement in statements)
    assert len([statement for statement in statements if "FROM sm_stock_kline" in statement]) == 1


def test_missing_top_rank_date_is_unknown_not_zero_return(source):
    engine, _ = source
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM st_hot_concept_ths_daily WHERE concept_name='创新药' AND snapshot_date=:d"), {"d": DAYS[1]})
    result = theme.collect_theme_evidence(engine, DAY, asof=ASOF)
    sector = next(row for row in result["sectors"] if row["name"] == "创新药")
    assert len(sector["history"]) == 4
    assert sector["history_complete_5d"] is False
    assert sector["cumulative_return_5d_pct"] is None
    assert DAYS[1] not in [row["trade_date"] for row in sector["history"]]


def test_actual_receipt_cutoff_and_retrospective_backfill_are_preserved(source):
    engine, _ = source
    with engine.begin() as conn:
        conn.execute(text("UPDATE st_hot_concept_ths_daily SET etl_sync_at='2026-11-01 10:50:00' WHERE plate_type=3 AND snapshot_date=:d"), {"d": DAY})
        conn.execute(text("UPDATE sm_stock_kline SET received_at='2026-11-01 10:00:00' WHERE trade_date=:d"), {"d": DAY})
    earlier = theme.collect_theme_evidence(engine, DAY, asof=DAY + "T23:59:59+08:00")
    assert not any(row["plate_type"] == 3 for row in earlier["sectors"])
    assert earlier["stocks"][0]["change_pct"] is None
    later = theme.collect_theme_evidence(engine, DAY, asof=ASOF)
    east = next(row for row in later["sectors"] if row["plate_type"] == 3)
    assert east["received_at"] == "2026-11-01T10:50:00+08:00"
    assert east["received_after_session_close"] is True
    assert later["stocks"][0]["received_at"]["price"] == "2026-11-01T10:00:00+08:00"


def test_bad_monetary_field_cannot_poison_verified_price_or_create_capacity(source):
    engine, _ = source
    with engine.begin() as conn:
        conn.execute(text("UPDATE sm_stock_kline SET amount=1 WHERE trade_date=:d"), {"d": DAY})
    result = theme.collect_theme_evidence(engine, DAY, asof=ASOF)
    stock = result["stocks"][0]
    assert stock["price_verification"]["status"] == "OBSERVED"
    assert stock["change_pct"] == 10
    assert stock["amount"] is None
    assert stock["amount_quality"]["status"] == "UNKNOWN"
    assert result["amount_quality"]["target"]["inconsistent_count"] == 1
    assert result["amount_quality"]["previous"]["status"] == "OBSERVED"


def test_stale_or_future_tags_are_never_used_to_associate_history(source):
    engine, _ = source
    with engine.begin() as conn:
        conn.execute(text("UPDATE st_hot_rank_ths SET snapshot_date='2026-11-02'"))
    result = theme.collect_theme_evidence(engine, DAY, asof=ASOF)
    stock = result["stocks"][0]
    assert stock["concept_tag"] is None
    assert "创新药" not in stock["sector_names"]
    assert stock["sector_names"] == ["化学制药"]


def test_optional_source_failure_is_named_and_not_retried(source):
    engine, statements = source
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE st_hot_rank_ths"))
    statements.clear()
    result = theme.collect_theme_evidence(engine, DAY, asof=ASOF)
    assert result["status"] == "OBSERVED"
    assert any(item.startswith("ths_tags:OperationalError") for item in result["limits"])
    assert sum("FROM st_hot_rank_ths" in statement for statement in statements) == 1
    assert result["stocks"][0]["price_verification"]["status"] == "OBSERVED"


def test_no_calendar_fallback_or_reads_for_non_session(source):
    engine, statements = source
    result = theme.collect_theme_evidence(engine, "2026-10-31", asof=ASOF)
    assert result["status"] == "UNKNOWN"
    assert result["sectors"] == []
    assert len(statements) == 1


def test_total_deadline_stops_remaining_stages_without_retry(source, monkeypatch):
    engine, statements = source
    now = [0]
    monkeypatch.setattr(theme.time, "monotonic", lambda: now[0])

    def exhausted(conn, cursor, statement, parameters, context, many):
        now[0] = 121

    event.listen(engine, "after_cursor_execute", exhausted)
    result = theme.collect_theme_evidence(engine, DAY, asof=ASOF)
    assert result["status"] == "UNKNOWN"
    assert len(statements) == 1
    assert "calendar:TimeoutError" in result["limits"]


def test_input_dates_are_validated_before_queries(source):
    engine, statements = source
    with pytest.raises(ValueError):
        theme.collect_theme_evidence(engine, DAY, asof="2026-10-29T23:59:59+08:00")
    assert statements == []


def test_candidate_and_sector_output_stays_bounded_and_tags_remain_observations(source):
    engine, _ = source
    with engine.begin() as conn:
        for n in range(2, 102):
            conn.execute(text("INSERT INTO st_hot_rank_fused VALUES(:d,:rank,:code,'样本',10,'化学制药',:received)"),
                         {"d": DAY, "rank": n, "code": str(n).zfill(6), "received": DAY + " 20:40:00"})
    result = theme.collect_theme_evidence(engine, DAY, asof=ASOF)
    assert len(result["stocks"]) <= 16
    assert len(result["sectors"]) <= 12
    assert result["coverage"]["candidate_codes"] <= 16
    assert all(stock["identity"] == "身份待确认" for stock in result["stocks"])


def test_parenthesized_source_alias_matches_same_day_tag_without_fuzzy_membership(source):
    engine, _ = source
    with engine.begin() as conn:
        conn.execute(text("UPDATE st_hot_concept_ths_daily SET concept_name='共封装光学(CPO)' WHERE concept_name='创新药'"))
        conn.execute(text("UPDATE st_hot_rank_ths SET concept_tag='CPO概念;6G概念'"))
    result = theme.collect_theme_evidence(engine, DAY, asof=ASOF)
    assert "共封装光学(CPO)" in result["stocks"][0]["sector_names"]
    assert "商业航天" not in result["stocks"][0]["sector_names"]


@pytest.mark.parametrize("change", ["pre_close=9", "high=9", "quality_status='UNVERIFIED'", "close=0"])
def test_price_quality_and_continuity_failures_cannot_register_observed_ohlc_or_five_day_return(source, change):
    engine, _ = source
    with engine.begin() as conn:
        conn.execute(text(f"UPDATE sm_stock_kline SET {change} WHERE trade_date=:d"), {"d": DAY})
    result = theme.collect_theme_evidence(engine, DAY, asof=ASOF)
    stock = result["stocks"][0]
    assert stock["price_verification"]["status"] == "UNKNOWN"
    assert stock["ohlc"]["status"] == "UNKNOWN"
    assert stock["change_pct"] is None
    assert stock["amount"] is None
    assert stock["cumulative_return_5d_pct"] is None
    assert stock["history"][-1]["status"] == "UNKNOWN"


def test_previous_close_continuity_is_checked_independently_of_matching_heat_quotes(source):
    engine, _ = source
    with engine.begin() as conn:
        conn.execute(text("UPDATE sm_stock_kline SET close=10.1 WHERE trade_date=:d"), {"d": DAYS[-2]})
    stock = theme.collect_theme_evidence(engine, DAY, asof=ASOF)["stocks"][0]
    assert stock["price_verification"]["same_day_source_checks"]["fused"]["matches_unadjusted_close"] is True
    assert stock["price_verification"]["previous_close_matches_pre_close"] is False
    assert stock["price_verification"]["status"] == "UNKNOWN"


def test_price_crosscheck_tolerance_rejects_more_than_one_hundredth_percentage_point(source):
    engine, _ = source
    with engine.begin() as conn:
        conn.execute(text("UPDATE st_hot_rank_fused SET change_pct=10.02"))
    stock = theme.collect_theme_evidence(engine, DAY, asof=ASOF)["stocks"][0]
    assert stock["price_verification"]["same_day_source_checks"]["fused"]["matches_unadjusted_close"] is False
    assert stock["change_pct"] is None


def test_invalid_older_price_keeps_history_gap_and_forbids_five_day_compounding(source):
    engine, _ = source
    with engine.begin() as conn:
        conn.execute(text("UPDATE sm_stock_kline SET low=20 WHERE trade_date=:d"), {"d": DAYS[1]})
    stock = theme.collect_theme_evidence(engine, DAY, asof=ASOF)["stocks"][0]
    assert stock["price_verification"]["status"] == "OBSERVED"
    assert stock["history"][1]["status"] == "UNKNOWN"
    assert stock["history"][1]["change_pct"] is None
    assert stock["cumulative_return_5d_pct"] is None
