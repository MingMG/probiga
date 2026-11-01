"""Semantic boundaries for frozen optional evidence in the research packet."""
from copy import deepcopy

import pytest

from biz.review.research_contract import ContractError, build_packet
from test_research_contract import NOW, answer, condition, material, run


def _build(context, *, kind="review"):
    digest, _ = material()
    return build_packet(digest, report_kind=kind,
                        next_trade_date="2026-10-30" if kind == "intraday" else "2026-11-02",
                        context=context, previous_research=None, now=NOW,
                        source_digest_sha256="projection-fixture")


def _scope():
    _, context = material()
    packet = _build(context)
    stock = next(item for item in packet["entities"].values()
                 if item["kind"] == "stock" and item.get("sector_ids"))
    return context, stock["code"]


def _technical_row(code, day="2026-10-30"):
    return {"stock_code": code, "status": "OBSERVED", "trade_date": day,
            "source": "sm_stock_kline", "source_snapshot_sha256": "a" * 64,
            "price_revision_matches_frozen": True,
            "coverage": {"price_status": "OBSERVED", "calendar_verified": True},
            "ma": {str(period): {"status": "OBSERVED", "last": 10.0,
                                  "last_deviation_pct": 2.0} for period in (5, 10, 20, 60)},
            "macd": {"status": "OBSERVED", "last": {"dif": 0.12, "dea": 0.10, "hist": 0.04},
                     "previous": {"dif": 0.11, "dea": 0.10, "hist": 0.02}},
            "kdj": {"status": "OBSERVED", "last": {"k": 61.0, "d": 58.0, "j": 67.0},
                    "previous": {"k": 59.0, "d": 57.0, "j": 63.0}},
            # Numeric residue must not override the UNKNOWN receipt.
            "volume": {"status": "UNKNOWN", "ratio5": 98765.0, "ratio20": 98766.0}}


def _technical(context, rows, *, day="2026-10-30"):
    context["technical_facts"] = {
        "status": "OBSERVED", "baseline_trade_date": day,
        "acquired_at": NOW.isoformat(), "stocks": rows, "indices": []}
    return context


def _finance(context, code, **changes):
    latest = {"finance_report_date": "2026-09-30",
              "finance_published_at": "2026-10-30T20:00:00+08:00",
              "finance_known_at": "2026-10-30T20:01:00+08:00",
              "finance_publication_time_verified": True,
              "finance_publication_precision": "EXACT_TIME",
              "finance_revision_id": "revision-fixture", "finance_content_hash": "b" * 64,
              "finance_pit_verified": True,
              "metrics": {"basic_eps": 1.2, "oper_cf_ps": 1.5,
                          "non_gaap_net_profit": 800_000_000.0,
                          "non_gaap_net_profit_yoy_gr": -12.5, "cash_flow_ratio": 32.8,
                          "pe_ttm": 99999.0, "operating_cash_flow": 9e99}}
    latest.update(changes)
    context["fundamental_facts"] = {
        "status": "OBSERVED", "manifest_hash": "c" * 64,
        "acquired_at": NOW.isoformat(),
        "stocks": [{"stock_code": code, "status": "OBSERVED", "latest": latest}]}
    return context


def _cards(packet, metric):
    return [(fid, card) for fid, card in packet["facts"].items() if card["metric"] == metric]


def _intraday(context):
    context["cutoff_at"] = "2026-10-30T13:52:02+08:00"
    stocks = deepcopy(context["theme_evidence"]["stocks"])
    for row in stocks:
        row.setdefault("ohlc", {})["close"] = 99.0
    context["intraday_observation"] = {
        "observation_id": "frozen-observation", "source_snapshot_at": "2026-10-30T13:51:33+08:00",
        "observed_at": context["cutoff_at"], "coverage": 0.99,
        "market": {"up_share": 0.4, "up_count": 1980, "indices": []},
        "sectors": deepcopy(context["theme_evidence"]["sectors"]), "key_stocks": stocks}
    return context


def test_canonical_pit_fields_are_measurements_without_invented_cash_flow_totals_or_valuation():
    context, code = _scope()
    _finance(context, code)
    packet = _build(context)
    [(fid, card)] = _cards(packet, "fundamental_metrics")
    assert card["values"] == {
        "basic_eps": 1.2, "oper_cf_ps": 1.5, "non_gaap_net_profit": 800_000_000.0,
        "non_gaap_net_profit_yoy_gr": -12.5, "cash_flow_ratio": 32.8,
        "report_date": "2026-09-30", "known_at": "2026-10-30T20:01:00+08:00"}
    assert card["kind"] == "market_measurement"
    assert card["scope"] == "certified_public_financial_report"
    assert card["revision_id"] == "revision-fixture"
    assert card["content_hash"] == "b" * 64 and card["manifest_hash"] == "c" * 64
    assert packet["evidence_coverage"]["fundamental_stock_count"] == 1
    assert packet["entities"][card["entity_id"]]["code"] == code


@pytest.mark.parametrize("changes", [
    {"finance_known_at": "2026-10-31T00:00:00+08:00"},
    {"finance_published_at": "2026-10-31T00:00:00+08:00"},
    {"finance_report_date": "2026-12-31"},
    {"finance_pit_verified": False},
    {"finance_known_at": None},
])
def test_after_cutoff_or_unproven_finance_has_no_projected_numeric_card(changes):
    context, code = _scope()
    _finance(context, code, **changes)
    packet = _build(context)
    assert not _cards(packet, "fundamental_metrics")
    assert packet["evidence_coverage"]["fundamental_stock_count"] == 0


def test_finance_manifest_is_required_even_when_row_self_reports_verified():
    context, code = _scope()
    _finance(context, code)
    context["fundamental_facts"]["manifest_hash"] = ""
    assert not _cards(_build(context), "fundamental_metrics")


@pytest.mark.parametrize("known, available", [
    ("2026-10-30T13:51:00+08:00", True),
    ("2026-10-30T13:53:00+08:00", False),
])
def test_date_only_publication_is_usable_only_after_its_certified_known_time(known, available):
    context, code = _scope()
    _intraday(context)
    _finance(context, code, finance_published_at="2026-10-30", finance_known_at=known,
             finance_publication_time_verified=False, finance_publication_precision="DATE_ONLY")
    packet = _build(context, kind="intraday")
    cards = _cards(packet, "fundamental_metrics")
    assert bool(cards) is available
    if available:
        assert cards[0][1]["published_at"] == "2026-10-30"
        assert cards[0][1]["publication_precision"] == "DATE_ONLY"
        assert cards[0][1]["values"]["known_at"] == known


@pytest.mark.parametrize("proof", ["row_unknown", "price_unknown", "calendar_unverified",
                                   "source_hash_missing", "frozen_revision_conflict", "wrong_row_date"])
def test_unproven_technical_series_does_not_project_residual_numeric_values(proof):
    context, code = _scope()
    row = _technical_row(code)
    if proof == "row_unknown":
        row["status"] = "UNKNOWN"
    elif proof == "price_unknown":
        row["coverage"]["price_status"] = "UNKNOWN"
    elif proof == "calendar_unverified":
        row["coverage"]["calendar_verified"] = False
    elif proof == "source_hash_missing":
        row["source_snapshot_sha256"] = ""
    elif proof == "frozen_revision_conflict":
        row["price_revision_matches_frozen"] = False
    else:
        row["trade_date"] = "2026-10-29"
    _technical(context, [row])
    packet = _build(context)
    assert not _cards(packet, "technical_indicators")
    assert all(packet["evidence_coverage"][name] == 0 for name in
               ("ma_stock_count", "macd_stock_count", "kdj_stock_count", "volume_stock_count"))


def test_independent_indicator_status_and_unverified_volume_are_respected():
    context, code = _scope()
    row = _technical_row(code)
    row["ma"]["60"].update(status="UNKNOWN", last=7654321.0)
    row["macd"]["status"] = row["kdj"]["status"] = "UNKNOWN"
    _technical(context, [row])
    packet = _build(context)
    [(fid, card)] = _cards(packet, "technical_indicators")
    assert card["values"]["ma5"] == 10.0
    assert "ma60" not in card["values"]
    assert not any(field.startswith(("macd_", "kdj_", "previous_", "volume_ratio")) for field in card["values"])
    assert packet["evidence_coverage"]["ma_stock_count"] == 1
    assert packet["evidence_coverage"]["macd_stock_count"] == 0
    assert packet["evidence_coverage"]["volume_stock_count"] == 0


@pytest.mark.parametrize("baseline_day, available", [("2026-10-29", True), ("2026-10-30", False)])
def test_intraday_technical_baseline_is_prior_closed_session_and_current_quote_stays_separate(baseline_day, available):
    context, code = _scope()
    _intraday(context)
    row = _technical_row(code, baseline_day)
    row["price_revision_matches_frozen"] = None  # Different time windows were never compared.
    _technical(context, [row], day=baseline_day)
    packet = _build(context, kind="intraday")
    cards = _cards(packet, "technical_indicators")
    assert bool(cards) is available
    quotes = [card for _, card in _cards(packet, "stock_prices")
              if packet["entities"][card["entity_id"]]["code"] == code]
    assert quotes[0]["values"]["price"] == 99.0
    assert "close" not in quotes[0]["values"]
    if available:
        assert cards[0][1]["trade_date"] == "2026-10-29"
        assert cards[0][1]["window"] == "closed_daily_baseline"
        assert cards[0][1]["values"]["ma5"] == 10.0
        assert "不证明当时盘中已知" in packet["technical_methods"]["availability"]


def test_closed_daily_card_retains_source_hash_calendar_gated_values_and_acquisition_time():
    context, code = _scope()
    _technical(context, [_technical_row(code)])
    [(fid, card)] = _cards(_build(context), "technical_indicators")
    assert card["source_snapshot_sha256"] == "a" * 64
    assert card["acquired_at"] == NOW.isoformat()
    assert card["trade_date"] == "2026-10-30"
    assert card["values"]["previous_macd_dif"] == 0.11
    assert card["values"]["previous_kdj_k"] == 59.0
    assert not any(key.startswith("volume_ratio") for key in card["values"])


def test_ma_condition_uses_the_same_stock_and_rejects_a_different_stock_threshold():
    context, code = _scope()
    other = next(row["stock_code"] for row in context["theme_evidence"]["stocks"] if row["stock_code"] != code)
    _technical(context, [_technical_row(code), _technical_row(other)])
    packet = _build(context)
    draft = answer(packet)
    watch = draft["report"]["directions"][0]["watches"][0]
    assert packet["entities"][watch["entity_id"]]["code"] == code
    own = next(fid for fid, card in _cards(packet, "technical_indicators") if card["entity_id"] == watch["entity_id"])
    foreign = next(fid for fid, card in _cards(packet, "technical_indicators") if card["entity_id"] != watch["entity_id"])
    observable = next(oid for oid, row in packet["observables"].items()
                      if row["entity_id"] == watch["entity_id"] and row["metric"] == "price")
    unit = condition("Cma", "open", observable, operator="holds_above",
                     reference={"kind": "fact", "fact_id": own, "field": "ma5"})
    unit["statement"] = [{"text": "在该标的已闭市均线上方保持才继续观察，参考均线为"}, {"fact_id": own, "field": "ma5"}]
    draft["units"].append(unit)
    watch["trigger_units"].append("Cma")
    assert run(draft, packet) == draft
    unit["conditions"][0]["reference"]["fact_id"] = foreign
    unit["statement"][-1]["fact_id"] = foreign
    with pytest.raises(ContractError, match="threshold_entity_or_unit_mismatch"):
        run(draft, packet)


def test_policy_quote_survives_many_related_stock_headlines_without_becoming_official_evidence():
    context, code = _scope()
    stock_name = next(row.get("short_name") for row in context["theme_evidence"]["stocks"] if row["stock_code"] == code)
    rows = [{"title": stock_name + "相关新闻" + str(index), "content": stock_name + "公司新闻" + str(index),
             "source": "公司快讯", "publish_time": "2026-10-30T21:00:00+08:00",
             "first_seen_at": "2026-10-30T21:01:00+08:00"} for index in range(20)]
    policy_quote = "商务部解读贸易政策，实际传导仍需价格验证。"
    rows.append({"title": "商务部解读贸易政策", "content": policy_quote,
                 "source": "媒体转述", "publish_time": "2026-10-30T20:00:00+08:00",
                 "first_seen_at": "2026-10-30T20:01:00+08:00"})
    context["events"] = {"status": "OBSERVED", "rows": rows}
    packet = _build(context)
    news = [card for _, card in _cards(packet, "news")]
    assert len(news) == packet["coverage"]["news_selected"] == 8
    policy = next(card for card in news if card["values"]["quote"] == policy_quote)
    assert policy["kind"] == "source_statement"
    assert policy["scope"] == "observed_news_sample"
    assert packet["sources"][policy["source_id"]]["name"] == "媒体转述"
    assert packet["coverage"]["news_available"] == 21
