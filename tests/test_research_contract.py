from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path

import pytest

from biz.review.amount_integrity import apply_amount_integrity
from biz.review.research_contract import (
    ContractError, DRAFT_VERSION, INPUT_VERSION, MAX_PACKET_BYTES, ROLES, TZ,
    build_packet, draft_json_schema, render_draft, validate_draft,
)

NOW = datetime(2026, 11, 1, 20, 0, tzinfo=TZ)


def material():
    frozen = json.loads((Path(__file__).parent / "fixtures" / "market_decision_context_20261030.json").read_text(encoding="utf-8"))
    quality = frozen["context"]["theme_evidence"]["amount_quality"]
    data = apply_amount_integrity(frozen["digest"], target_quality=quality["target"], previous_quality=quality["previous"])
    return data, frozen["context"]


def packet(**kwargs):
    data, context = material()
    return build_packet(data, report_kind="review", next_trade_date="2026-11-02", context=context,
                        previous_research=None, now=NOW, source_digest_sha256="source-hash", **kwargs)


def find_fact(p, metric, eid=None):
    return next(key for key, card in p["facts"].items() if card["metric"] == metric and (eid is None or card.get("entity_id") == eid))


def inferred(uid, topic, fid, text):
    return {"id": uid, "kind": "market_inference", "topic": topic, "statement": [{"text": text}], "evidence_ids": [fid], "confidence": "medium"}


def condition(uid, window, oid, *, topic="action", operator="improves", reference=None):
    return {"id": uid, "kind": "future_condition", "topic": topic, "statement": [{"text": "观察上涨扩散是否较开盘改善；改善失败则继续等待。"}],
            "logic": "all", "conditions": [{"observable_id": oid, "window": window, "operator": operator,
                                              "reference": reference or {"kind": "same_session_open", "fact_id": "", "field": ""}}]}


def answer(p):
    market = find_fact(p, "market_prices")
    feedback = find_fact(p, "previous_strength")
    amount = find_fact(p, "daily_amount")
    audit = find_fact(p, "audit_windows")
    stock_id, stock = next((key, entity) for key, entity in p["entities"].items() if entity["kind"] == "stock" and entity.get("sector_ids"))
    theme_id = stock["sector_ids"][0]
    tf = find_fact(p, "theme_prices", theme_id)
    sf = find_fact(p, "stock_prices", stock_id)
    observable = next(key for key, value in p["observables"].items() if value["metric"] == "up_share")
    units = [inferred("Uenv", "environment", market, "当前广度偏弱，等待同步修复证据。"),
             inferred("Usent", "sentiment", feedback, "强势反馈偏弱，情绪仍需观察修复持续性。"),
             inferred("Umain", "mainline", tf, "该方向具有样本内相对反馈，防守与反抽解释仍需区分。"),
             inferred("Ustock", "identity", sf, "列为有自身行情证据的观察对象，尚不能确认带动身份。"),
             inferred("Urisk", "risk", market, "若后续下跌扩散，方向性判断需继续保持谨慎。"),
             inferred("Uaction", "action", market, "只观察方向能否形成持续反馈。"),
             {"id": "Ulimit", "kind": "data_limitation", "topic": "risk", "evidence_ids": [amount], "effect": "disable_turnover", "statement": [{"fact_id": amount, "field": "summary"}]},
             {"id": "Uaudit", "kind": "data_limitation", "topic": "audit", "evidence_ids": [audit], "effect": "missing_window", "statement": [{"fact_id": audit, "field": "summary"}]}]
    for uid, window in (("Copen", "open"), ("Cearly", "first_30m"), ("Cmid", "midday"), ("Cclose", "close")):
        units.append(condition(uid, window, observable))
    units.append(condition("Ccounter", "close", observable, topic="counter_evidence"))
    roles = dict(zip(ROLES, ["Uenv", "Usent", "Umain", "Urisk", "Ustock", "Urisk", "Ccounter", "Uaudit"]))
    return {"schema_version": DRAFT_VERSION, "units": units, "roles": [{"role": role, "unit_ids": [uid]} for role, uid in roles.items()],
            "report": {"conclusion_units": ["Uenv", "Usent"], "permission": {"action": "WAIT", "basis_units": ["Uenv", "Urisk"]},
                       "market_reading_units": ["Umain", "Urisk"], "directions": [{"entity_id": theme_id, "classification": "unconfirmed",
                           "assessment_units": ["Umain"], "watches": [{"entity_id": stock_id, "identity": "unconfirmed", "assessment_units": ["Ustock"],
                                                                      "trigger_units": ["Cearly"], "abandon_units": ["Cclose"]}]}],
                       "event_units": [], "scenarios": [{"name": "修复待确认", "trigger_units": ["Cearly"], "action_units": ["Uaction"], "abandon_units": ["Cclose"]}],
                       "checks": {"open": ["Copen"], "first_30m": ["Cearly"], "midday": ["Cmid"], "close": ["Cclose"]},
                       "counter_evidence_units": ["Ccounter"], "limitation_units": ["Ulimit"], "continuity": [], "audit_units": ["Uaudit"]}}


def run(draft=None, p=None):
    p = packet() if p is None else p
    draft = answer(p) if draft is None else draft
    return validate_draft(json.dumps(draft, ensure_ascii=False), p)


def test_real_snapshot_is_compact_and_preserves_raw_material():
    data, context = material()
    before = deepcopy((data, context))
    p = build_packet(data, report_kind="review", next_trade_date="2026-11-02", context=context, previous_research=None, now=NOW, source_digest_sha256="source-hash")
    assert len(json.dumps(p, ensure_ascii=False, separators=(",", ":")).encode()) <= MAX_PACKET_BYTES
    assert len([e for e in p["entities"].values() if e["kind"] == "theme"]) <= 10
    assert len([e for e in p["entities"].values() if e["kind"] == "stock"]) <= 12
    assert all("amount" not in c["values"] for c in p["facts"].values() if c["metric"] == "index_prices")
    assert all("total_amount" not in c["values"] for c in p["facts"].values() if c["metric"] == "market_prices")
    assert p["facts"][find_fact(p, "daily_amount")]["validity"] == "usable"
    assert p["facts"][find_fact(p, "daily_amount")]["dataset_status"] == "unavailable"
    assert (data, context) == before


def test_valid_draft_is_returned_without_relabelling_or_rewriting_and_renders_one_report():
    p = packet()
    draft = answer(p)
    draft["units"][0]["statement"] = [{"text": "上涨占比为"}, {"fact_id": find_fact(p, "market_prices"), "field": "up_coverage"}, {"text": "，修复仍待验证。"}]
    original = deepcopy(draft)
    validated = run(draft, p)
    assert validated == original
    content = render_draft(validated, p, {})
    assert "16.03%" in content and "补记" in content
    assert "最强反证" in content and "放弃条件" in content
    assert all(role not in content for role in ROLES)
    assert "source-hash" not in content and "WAIT" not in content


@pytest.mark.parametrize("text", ["上涨占比99.9%", "插混-17.22%", "科创50跌幅较大", "18:33发布", "前30分钟确认修复", "上涨率９９％"])
def test_inferred_text_cannot_bypass_numeric_and_time_evidence(text):
    p = packet()
    draft = answer(p)
    draft["units"][0]["statement"] = [{"text": text}]
    with pytest.raises(ContractError, match="free_text_number_requires_reference"):
        run(draft, p)


def test_source_quote_with_decline_keeps_original_semantics_and_timestamp_reference():
    p = packet()
    p["facts"]["Fnews"] = {"kind": "source_statement", "values": {"quote": "插混汽车销量同比下降17.22%。", "published_at": "2026-11-01T18:33:00+08:00"}, "validity": "usable"}
    draft = answer(p)
    draft["units"].append({"id": "Unews", "kind": "market_inference", "topic": "event", "confidence": "medium", "evidence_ids": ["Fnews"],
                           "statement": [{"fact_id": "Fnews", "field": "quote"}, {"text": "发布时间为"}, {"fact_id": "Fnews", "field": "published_at"}, {"text": "；传导仍需价格验证。"}]})
    draft["report"]["event_units"] = ["Unews"]
    content = render_draft(run(draft, p), p, {})
    assert "下降17.22%" in content and "18:33" in content
    assert "-17.22%" not in content


def test_truthful_quality_limitation_is_supported_without_becoming_market_evidence():
    p = packet()
    draft = answer(p)
    assert run(draft, p)["units"][6]["kind"] == "data_limitation"
    draft["units"][0]["evidence_ids"] = [find_fact(p, "daily_amount")]
    with pytest.raises(ContractError, match="market_inference_requires_market_fact"):
        run(draft, p)


def test_future_volume_or_extreme_comparison_cannot_smuggle_full_session_baseline():
    p = packet()
    draft = answer(p)
    unit = next(u for u in draft["units"] if u["id"] == "Cearly")
    # A future conditional is legal without future data; a cumulative daily
    # extreme count cannot become the first-half-hour comparison reference.
    assert run(draft, p)
    unit["conditions"][0]["reference"] = {"kind": "fact", "fact_id": find_fact(p, "extremes"), "field": "down_count"}
    with pytest.raises(ContractError, match="incomparable_metric_window"):
        run(draft, p)


@pytest.mark.parametrize("mutate,reason", [
    (lambda d: d["units"][0]["evidence_ids"].append("invented"), "invalid_evidence_id"),
    (lambda d: d["units"][0]["statement"].append({"fact_id": "invented", "field": "value"}), "unknown_fact_or_field"),
    (lambda d: d["roles"].pop(), "schema_array"),
    (lambda d: d["report"].update(directions=[]), "available_directions_not_analysed"),
    (lambda d: d["report"]["directions"][0].update(watches=[]), "linked_stock_observation_required"),
    (lambda d: d["report"]["directions"][0]["watches"][0].update(identity="leader"), "confirmed_identity_requires_relationship"),
    (lambda d: d["report"]["checks"].update(open=[]), "required_time_window_missing"),
    (lambda d: d["report"].update(counter_evidence_units=["Urisk"]), "required_topic_missing"),
    (lambda d: d["report"].update(audit_units=["Urisk"]), "wrong_unit_kind"),
])
def test_research_requirements_are_semantic_types_not_unknown_placeholders(mutate, reason):
    p = packet()
    draft = answer(p)
    mutate(draft)
    with pytest.raises(ContractError, match=reason):
        run(draft, p)


def test_capacity_requires_measurement_from_same_stock():
    p = packet()
    draft = answer(p)
    watch = draft["report"]["directions"][0]["watches"][0]
    fid = find_fact(p, "stock_prices", watch["entity_id"])
    p["facts"][fid]["values"].pop("amount", None)
    watch["identity"] = "capacity_candidate"
    with pytest.raises(ContractError, match="capacity_requires_own_quantity"):
        run(draft, p)
    p["facts"][fid]["values"]["market_cap"] = 1_000_000_000
    assert run(draft, p)


def test_v3_parent_is_inherited_without_claiming_new_validation_and_stays_in_budget():
    p = packet()
    previous = {"schema_version": DRAFT_VERSION, "draft": answer(p), "input_packet": p, "research_sha256": "parent-hash"}
    data, context = material()
    b = build_packet(data, report_kind="briefing", next_trade_date="2026-11-02", context=context, previous_research=previous, now=NOW, source_digest_sha256="source-hash")
    assert len(json.dumps(b, ensure_ascii=False, separators=(",", ":")).encode()) <= MAX_PACKET_BYTES
    draft = answer(b)
    bid = next(iter(b["baseline"]["unit_facts"]))
    draft["units"].append({"id": "Uprior", "kind": "baseline_restatement", "topic": "continuity", "evidence_ids": [bid], "statement": [{"fact_id": bid, "field": "statement"}]})
    draft["report"]["continuity"] = [{"baseline_fact_id": bid, "change": "inherited", "new_fact_ids": [], "reason_units": ["Uprior"]}]
    assert run(draft, b)
    draft["report"]["continuity"][0]["change"] = "strengthened"
    with pytest.raises(ContractError, match="baseline_change_requires_new_facts"):
        run(draft, b)


def test_old_v2_is_only_a_limitation_and_not_a_parallel_briefing_parser():
    data, context = material()
    old = {"schema_version": "probiga.market-decision.v2", "status": "COMPLETE", "conclusion": {"text": "old"}}
    p = build_packet(data, report_kind="review", next_trade_date="2026-11-02", context=context, previous_research=old, now=NOW, source_digest_sha256="source-hash")
    assert p["baseline"] is None
    with pytest.raises(ContractError, match="v3_parent_required_for_briefing"):
        build_packet(data, report_kind="briefing", next_trade_date="2026-11-02", context=context, previous_research=old, now=NOW, source_digest_sha256="source-hash")


def test_intraday_uses_only_verified_snapshot_and_never_daily_close_or_ths():
    data, context = material()
    now = datetime(2026, 10, 30, 10, 15, tzinfo=TZ)
    context["cutoff_at"] = now.isoformat()
    context["intraday_observation"] = {"source_snapshot_at": "2026-10-30T10:14:00+08:00", "observed_at": now.isoformat(),
                                        "coverage": 0.99, "expected_count": 5000, "observed_count": 4950,
                                        "market": {"up_share": 0.4, "up_count": 1980}, "sectors": [], "key_stocks": []}
    p = build_packet(data, report_kind="intraday", next_trade_date="2026-10-30", context=context, previous_research=None, now=now, source_digest_sha256="source-hash")
    assert p["task"]["condition_horizon"] == "remaining_session"
    assert p["task"]["data_cutoff_at"] == "2026-10-30T10:14:00+08:00"
    assert p["task"]["as_of_at"] == now.isoformat()
    assert p["task"]["context_cutoff_at"] == now.isoformat()
    assert p["facts"][find_fact(p, "market_prices")]["values"]["up_share"] == 0.4
    assert not any(card["metric"] in {"theme_prices", "stock_prices", "daily_range", "previous_strength"} for card in p["facts"].values())
    assert not any(e["kind"] == "stock" for e in p["entities"].values())


def test_intraday_replay_is_explicit_and_source_time_is_not_observation_time():
    p = packet()
    draft = answer(p)
    p["task"].update(kind="intraday", mode="replay", review_date="2026-10-30", next_trade_date="2026-10-30",
                     as_of_at="2026-10-30T13:52:02+08:00", context_cutoff_at="2026-10-30T13:52:02+08:00",
                     data_cutoff_at="2026-10-30T13:51:33+08:00")
    content = render_draft(draft, p, {})
    assert "A股盘中播报｜2026-10-30 13:52（回放）" in content
    assert "行情同步：10月30日 13:51" in content
    assert "新闻截止：10月30日 13:52" in content


def test_real_provider_alphabetical_check_keys_render_in_trading_time_order():
    p = packet()
    draft = answer(p)
    draft["report"]["checks"] = dict(sorted(draft["report"]["checks"].items()))
    content = render_draft(run(draft, p), p, {})
    positions = [content.index("- **" + name + "**") for name in ("开盘", "前半小时", "午间", "尾盘")]
    assert positions == sorted(positions)


def test_cli_schema_has_closed_required_objects_and_anyof():
    def check(node):
        if isinstance(node, dict):
            assert "oneOf" not in node
            if "enum" in node or "const" in node:
                assert node.get("type") == "string"
            if node.get("type") == "object":
                assert node["additionalProperties"] is False
                assert set(node["properties"]) == set(node["required"])
            for value in node.values():
                check(value)
        elif isinstance(node, list):
            for value in node:
                check(value)
    check(draft_json_schema())
