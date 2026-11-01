from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from biz.review import research_inputs as subject
from server.common.pit_facts import PITFactBatch

TZ = ZoneInfo("Asia/Shanghai")


def dates(end="2026-10-30", count=122):
    day, output = date.fromisoformat(end), []
    while len(output) < count:
        if day.weekday() < 5:
            output.append(day.isoformat())
        day -= timedelta(days=1)
    return sorted(output)


def bars(code="000678", *, key="stock_code", days=None):
    return [{key: code, "trade_date": day, "adjust_type": 0, "k_type": 1,
             "open": 10, "high": 11, "low": 9, "close": 10, "pre_close": 10,
             "volume": 100, "amount": 1000, "quality_status": "VERIFIED"}
            for day in days or dates()]


def report(known="2026-10-29T10:00:00", published="2026-10-28T20:00:00", **extra):
    return {"finance_report_date": "2026-09-30", "finance_published_at": published,
            "finance_known_at": known, "finance_publication_time_verified": True,
            "finance_publication_precision": "EXACT_TIME", "finance_pit_verified": True,
            "finance_revision_id": "revision", "finance_content_hash": "a" * 64,
            "finance_source": "verified_source", "total_rev_yoy_gr": 12.5,
            "roe_wtd": 8, "basic_eps": 1.2, "pe_ttm": 99, **extra}


def batch(codes, history=None):
    return PITFactBatch(facts={code: deepcopy(history if history is not None else [report()]) for code in codes},
                        status_by_code={code: "AVAILABLE" for code in codes},
                        coverage_by_code={code: {"coverage_id": "original-coverage"} for code in codes},
                        manifest_hash="b" * 64)


class Reader:
    def __init__(self, rows=None, finance=None):
        self.rows = rows if rows is not None else bars()
        self.calls, self.finance_calls, self.fail_at = [], [], None
        self.batch = finance
        self.column_overrides = {}

    def query(self, stage, sql, params=None, expanding=(), cap=1):
        self.calls.append((stage, sql, deepcopy(params), expanding, cap))
        if self.fail_at == stage:
            raise subject.EvidenceReadError("observed_read_timeout")
        if "si_trade_calendar" in sql:
            selected = [day for day in dates() if day < params["target"] or ("<=:target" in sql and day == params["target"])]
            return [{"trade_date": day} for day in selected[-121:][::-1]]
        if "information_schema" in sql:
            names = self.column_overrides.get(params["table"], set(self.rows[0]))
            return [{"COLUMN_NAME": key} for key in names]
        if sql.startswith("EXPLAIN"):
            return [{"key": "uk_sm_stock_kline_business", "rows": 60}]
        key = "index_code" if "sm_index_kline" in sql else "stock_code"
        return [deepcopy(row) for row in self.rows if row.get(key) in params["codes"] and row["trade_date"] in params["dates"]]

    def finance(self, codes, asof, target):
        self.finance_calls.append((list(codes), asof, target))
        if self.fail_at == "finance":
            raise subject.EvidenceReadError("finance_deadline_exceeded")
        return self.batch if self.batch is not None else batch(codes)


def context(codes=("000678",)):
    return {"cutoff_at": "2026-10-30T23:59:59+08:00", "events": {"rows": [{"title": "unchanged"}]},
            "theme_evidence": {"stocks": [{"stock_code": code, "short_name": "候选", "trade_date": "2026-10-30",
                                           "ohlc": {"close": 10, "open": 10, "high": 11, "low": 9, "pre_close": 10}}
                                          for code in codes]}, "indices": {"rows": []}}


def enrich(monkeypatch, current=None, *, rows=None, finance=None, kind="review", reader=None):
    primary = Reader(finance=finance)
    kline = reader or Reader(rows)

    @contextmanager
    def sources(engine, deadline):
        assert deadline > subject.time.monotonic()
        yield primary, kline

    monkeypatch.setattr(subject, "_sources", sources)
    result = subject.enrich_context(SimpleNamespace(url="not-used"), {"review_date": "2026-10-30"},
                                    current or context(), report_kind=kind, now=datetime(2026, 11, 2, tzinfo=TZ))
    return result, primary, kline


def test_closed_daily_indicators_are_added_without_mutating_frozen_prices_or_events(monkeypatch):
    original = context()
    original["events"]["rows"][0]["publish_time"] = datetime(2026, 10, 30, 12)
    snapshot = deepcopy(original)
    result, primary, kline = enrich(monkeypatch, original)
    assert original == snapshot
    assert result["events"] == original["events"]
    assert result["theme_evidence"] == original["theme_evidence"]
    fact = result["technical_facts"]["stocks"][0]
    assert fact["ma"]["20"]["last"] == 10
    assert fact["macd"]["status"] == fact["kdj"]["status"] == "OBSERVED"
    assert fact["coverage"]["provided_rows"] == 121
    assert len(fact["source_snapshot_sha256"]) == 64
    assert result["technical_facts"]["acquired_at"].startswith("2026-11-02")
    assert primary.finance_calls[0][1] == datetime(2026, 10, 30, 23, 59, 59, tzinfo=TZ)
    assert result["fundamental_facts"]["stocks"][0]["latest"]["metrics"] == {
        "basic_eps": 1.2, "total_rev_yoy_gr": 12.5, "roe_wtd": 8}
    assert "pe_ttm" not in str(result["fundamental_facts"])
    assert fact["volume"]["status"] == "UNKNOWN"
    assert fact["volume"]["ratio5"] is None


def test_intraday_never_reads_current_day_close_and_finance_uses_exact_cutoff(monkeypatch):
    current = context()
    current.update(cutoff_at="2026-10-30T13:52:02+08:00", intraday_observation={
        "key_stocks": [{"stock_code": "000678", "short_name": "候选", "ohlc": {"close": 99}}],
        "market": {"indices": []}})
    result, primary, kline = enrich(monkeypatch, current, kind="intraday")
    assert result["technical_facts"]["baseline_trade_date"] == "2026-10-29"
    queries = [call for call in kline.calls if call[0] == "technical_sm_stock_kline"]
    assert all("2026-10-30" not in call[2]["dates"] for call in queries)
    assert result["technical_facts"]["stocks"][0]["ma"]["5"]["last"] == 10
    assert result["technical_facts"]["stocks"][0]["price_revision_matches_frozen"] is None
    assert primary.finance_calls[0][1] == datetime(2026, 10, 30, 13, 52, 2, tzinfo=TZ)


def test_briefing_reuses_last_closed_baseline_but_accepts_newly_known_pit_report(monkeypatch):
    current = context()
    current["cutoff_at"] = "2026-11-02T08:20:00+08:00"
    later = report(known="2026-10-31T09:00:00", published="2026-10-30T22:00:00")
    result, primary, _ = enrich(monkeypatch, current, kind="briefing", finance=batch(["000678"], [later]))
    assert result["technical_facts"]["baseline_trade_date"] == "2026-10-30"
    assert result["fundamental_facts"]["status"] == "OBSERVED"
    assert primary.finance_calls[0][2] == "2026-10-30"


def test_target_revision_conflict_disables_new_indicator_series_without_replacing_original(monkeypatch):
    rows = bars()
    rows[-1].update(close=10.5)
    original = context()
    result, _, _ = enrich(monkeypatch, original, rows=rows)
    fact = result["technical_facts"]["stocks"][0]
    assert fact["status"] == "UNKNOWN"
    assert fact["price_revision_matches_frozen"] is False
    assert all(value["last"] is None for value in fact["ma"].values())
    assert any(row["code"] == "frozen_target_price_revision_conflict" for row in fact["issues"])
    assert result["theme_evidence"]["stocks"][0]["ohlc"]["close"] == 10


def test_real_preclose_discontinuity_is_unknown_without_corporate_action_repair(monkeypatch):
    rows = bars()
    rows[10]["pre_close"] = 9.9
    result, _, _ = enrich(monkeypatch, rows=rows)
    fact = result["technical_facts"]["stocks"][0]
    assert fact["status"] == "UNKNOWN"
    assert fact["macd"]["last"] is None
    assert any(row["code"] == "previous_close_discontinuity_pending_verification" for row in fact["issues"])


@pytest.mark.parametrize("changed", [
    {"finance_known_at": "2026-10-31T00:00:00"},
    {"finance_published_at": "2026-10-31T00:00:00"},
    {"finance_pit_verified": False},
    {"finance_publication_time_verified": False, "finance_published_at": ""},
])
def test_future_or_unproven_finance_is_rejected_even_if_helper_status_says_available(monkeypatch, changed):
    result, _, _ = enrich(monkeypatch, finance=batch(["000678"], [report(**changed)]))
    facts = result["fundamental_facts"]
    assert facts["status"] == "UNKNOWN"
    assert facts["stocks"][0]["latest"] is None
    assert facts["stocks"][0]["history"] == []


def test_data_blocked_source_never_leaks_payload_metrics(monkeypatch):
    source = batch(["000678"])
    source.status_by_code["000678"] = "DATA_BLOCKED"
    source.reason_by_code["000678"] = "bad_chain"
    result, _, _ = enrich(monkeypatch, finance=source)
    assert result["fundamental_facts"]["stocks"][0]["reason"] == "bad_chain"
    assert result["fundamental_facts"]["stocks"][0]["history"] == []


def test_only_canonical_finance_metrics_are_projected_without_inventing_cash_flow_total(monkeypatch):
    source = report(oper_cf_ps=2.4, non_gaap_net_profit=300, cash_flow_ratio=0.5,
                    non_gaap_net_profit_yoy_gr=8.1, operating_cash_flow=999, net_profit_excl_nr=888)
    result, _, _ = enrich(monkeypatch, finance=batch(["000678"], [source]))
    metrics = result["fundamental_facts"]["stocks"][0]["latest"]["metrics"]
    assert metrics["oper_cf_ps"] == 2.4
    assert metrics["non_gaap_net_profit"] == 300
    assert metrics["cash_flow_ratio"] == 0.5
    assert metrics["non_gaap_net_profit_yoy_gr"] == 8.1
    assert "operating_cash_flow" not in metrics and "net_profit_excl_nr" not in metrics


def test_bounded_batches_include_all_codes_and_stop_without_retry_on_first_source_failure(monkeypatch):
    codes = [str(index).zfill(6) for index in range(13)]
    rows = sum((bars(code) for code in codes), [])
    source = Reader(rows)
    calls = source.query
    counter = 0

    def fail_second(stage, *args, **kwargs):
        nonlocal counter
        if stage == "technical_sm_stock_kline":
            counter += 1
            if counter == 2:
                raise subject.EvidenceReadError("measured_timeout")
        return calls(stage, *args, **kwargs)

    source.query = fail_second
    result, _, _ = enrich(monkeypatch, context(codes), reader=source)
    assert len(result["technical_facts"]["stocks"]) == 12
    receipt = result["technical_facts"]["coverage"]["stocks_read"]
    assert receipt["state"] == "FAILED"
    assert receipt["completed_units"] == 1
    assert counter == 2
    reads = [call for call in source.calls if call[0] == "technical_sm_stock_kline"]
    assert len(reads) == 1 and len(reads[0][2]["codes"]) == 12 and len(reads[0][2]["dates"]) == 5
    assert reads[0][4] == 60
    assert all(row["ma"]["5"]["status"] == "OBSERVED" for row in result["technical_facts"]["stocks"])
    assert all(row["macd"]["status"] == "UNKNOWN" for row in result["technical_facts"]["stocks"])
    assert result["fundamental_facts"]["status"] == "OBSERVED"


def test_index_schema_without_adjustment_and_preclose_is_not_fabricated(monkeypatch):
    current = context()
    current["indices"]["rows"] = [{"index_code": "000001"}]
    source = Reader(bars() + bars("000001", key="index_code"))
    source.column_overrides["sm_index_kline"] = {"index_code", "trade_date", "open", "high", "low", "close", "k_type"}
    result, _, _ = enrich(monkeypatch, current, reader=source)
    fact = result["technical_facts"]["indices"][0]
    assert fact["status"] == "UNKNOWN"
    assert not any(call[0] == "technical_sm_index_kline" for call in source.calls)
    assert result["technical_facts"]["coverage"]["indices_read"]["reason"] == "unadjusted_or_previous_close_not_proven"


def test_review_index_flat_frozen_close_conflict_disables_its_technical_series(monkeypatch):
    current = context()
    current["indices"]["rows"] = [{"index_code": "000001", "trade_date": "2026-10-30", "close": 99}]
    source = Reader(bars() + bars("000001", key="index_code"))
    source.column_overrides["sm_index_kline"] = set(source.rows[-1])
    result, _, _ = enrich(monkeypatch, current, reader=source)
    index = result["technical_facts"]["indices"][0]
    assert index["status"] == "UNKNOWN"
    assert index["price_revision_matches_frozen"] is False
    assert all(value["last"] is None for value in index["ma"].values())
    assert result["indices"]["rows"][0]["close"] == 99


def test_no_frozen_index_prices_means_not_compared_instead_of_a_false_match(monkeypatch):
    current = context()
    current["indices"]["rows"] = [{"index_code": "000001"}]
    source = Reader(bars() + bars("000001", key="index_code"))
    source.column_overrides["sm_index_kline"] = set(source.rows[-1])
    result, _, _ = enrich(monkeypatch, current, reader=source)
    index = result["technical_facts"]["indices"][0]
    assert index["status"] == "OBSERVED"
    assert index["price_revision_matches_frozen"] is None
    assert index["price_revision_comparison"] == "NOT_COMPARED"


def test_finance_deadline_is_named_and_price_evidence_still_independent(monkeypatch):
    primary = Reader()
    primary.fail_at = "finance"

    @contextmanager
    def sources(engine, deadline):
        yield primary, Reader()

    monkeypatch.setattr(subject, "_sources", sources)
    result = subject.enrich_context(None, {"review_date": "2026-10-30"}, context(), report_kind="review")
    assert result["technical_facts"]["status"] == "OBSERVED"
    assert result["fundamental_facts"]["status"] == "UNKNOWN"
    assert result["fundamental_facts"]["issues"] == [{"stage": "finance_history", "reason": "finance_deadline_exceeded"}]


def test_missing_cutoff_cannot_default_to_now(monkeypatch):
    current = context()
    del current["cutoff_at"]
    with pytest.raises(KeyError):
        enrich(monkeypatch, current)


def test_review_label_does_not_authorize_reading_an_unclosed_current_day_bar(monkeypatch):
    current = context()
    current["cutoff_at"] = "2026-10-30T13:52:00+08:00"
    result, primary, kline = enrich(monkeypatch, current)
    assert result["technical_facts"]["status"] == "UNKNOWN"
    assert result["technical_facts"]["issues"] == [{"stage": "technical_calendar", "reason": "target_session_not_closed_at_asof"}]
    assert not kline.calls


def test_no_finance_retry_after_official_helper_swallows_a_budget_failure(monkeypatch):
    reader = subject._Reader(None, subject.time.monotonic() + 20)
    calls = []

    def helper(*args, **kwargs):
        calls.append(kwargs)
        reader.failure = "finance_result_budget_exceeded"
        return batch(["000678"])

    monkeypatch.setattr(subject, "load_finance_history_facts", helper)
    with pytest.raises(subject.EvidenceReadError, match="finance_result_budget_exceeded"):
        reader.finance(["000678"], datetime(2026, 10, 30, tzinfo=TZ), "2026-10-30")
    assert len(calls) == 1


def test_expired_total_deadline_stops_before_opening_database():
    reader = subject._Reader(None, subject.time.monotonic() - 1)
    with pytest.raises(subject.EvidenceReadError, match="test_deadline_exceeded"):
        reader.query("test", "SELECT 1")


def test_remaining_budget_must_cover_one_full_query_before_any_socket_is_opened():
    reader = subject._Reader(None, subject.time.monotonic() + 14)
    with pytest.raises(subject.EvidenceReadError, match="test_deadline_exceeded"):
        reader.query("test", "SELECT 1")


def test_intraday_uses_short_input_budget_without_changing_its_asof(monkeypatch):
    seen = []

    @contextmanager
    def sources(engine, deadline):
        seen.append(deadline - subject.time.monotonic())
        yield Reader(), Reader()

    monkeypatch.setattr(subject, "_sources", sources)
    current = context()
    current.update(cutoff_at="2026-10-30T13:52:00+08:00", intraday_observation={"key_stocks": [], "market": {}})
    result = subject.enrich_context(None, {"review_date": "2026-10-30"}, current, report_kind="intraday")
    assert 29 < seen[0] <= 30
    assert result["technical_facts"]["as_of_at"] == current["cutoff_at"]


def test_primary_and_kline_routes_are_separate_private_pools_and_both_disposed(monkeypatch):
    opened, disposed = [], []

    def make(url, deadline):
        opened.append((url, deadline))
        return SimpleNamespace(engine=SimpleNamespace(dispose=lambda: disposed.append(url)))

    monkeypatch.setattr(subject, "_dedicated_reader", make)
    monkeypatch.setattr(subject, "get_kline_mysql_url", lambda: "history-route")
    with subject._sources(SimpleNamespace(url="primary-route"), 123):
        pass
    assert opened == [("primary-route", 123), ("history-route", 123)]
    assert disposed == ["primary-route", "history-route"]
