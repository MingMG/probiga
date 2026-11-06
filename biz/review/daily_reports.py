"""Publish the scheduled daily facts, then add evidence-checked Codex commentary."""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, time, timedelta
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from biz.market_context.external_market import NEW_YORK, US_CASH_INDICES, fetch_us_cash_close
from biz.review.market_decision import (
    ResearchError, _call_codex, _load_rules, _messages, source_digest_identity,
)
from biz.review.quant_digest import generate_quant_digest, persist_quant_digest, render_compact_review
from biz.review.research_contract import build_packet, draft_json_schema, render_draft, validate_draft
from biz.review.research_flow import (
    TZ, artifact_hash, assert_digest, collect_context, decode_digest, load_digest, next_session, read_records,
)
from server.ai_bridge.research import ResearchBridgeError


log = logging.getLogger(__name__)
FACTS_NOTE = "> 已发布量化事实。AI 解读暂未生成。"


def _update_report(engine, digest, *, review=None, briefing=None, expected_briefing=None):
    """Attach to the exact facts used by the producer and preserve the other report."""
    with engine.begin() as conn:
        suffix = " FOR UPDATE" if conn.dialect.name == "mysql" else ""
        row = conn.execute(text(
            "SELECT * FROM st_quant_review_digest WHERE review_date=:d AND adjust_type=0" + suffix
        ), {"d": digest["review_date"]}).mappings().one()
        current = decode_digest(dict(row))
        assert_digest(current, digest["review_date"])
        if source_digest_identity(current) != source_digest_identity(digest):
            raise RuntimeError("DATA_BLOCKED stage=daily_report_write: review facts changed")
        factors = current["factor_validation_json"]
        compact = current["compact_review"]
        if briefing is not None:
            previous = factors.get("early_briefing") or {}
            if expected_briefing is not None and previous != expected_briefing:
                raise RuntimeError("DATA_BLOCKED stage=daily_report_write: briefing changed")
            if previous.get("generated_at", "") > briefing["generated_at"]:
                raise RuntimeError("DATA_BLOCKED stage=daily_report_write: newer briefing exists")
            factors["early_briefing"] = briefing
        if review is not None:
            if factors.get("market_decision"):
                return
            factors["market_decision"] = review
            compact = compact.removesuffix("\n\n" + FACTS_NOTE)
            compact += "\n\n## Codex 解读\n\n" + review["content"]
        conn.execute(text(
            "UPDATE st_quant_review_digest SET compact_review=:content, factor_validation_json=:f "
            "WHERE review_date=:d AND adjust_type=0"
        ), {"content": compact, "f": json.dumps(factors, ensure_ascii=False, default=str),
            "d": digest["review_date"]})


def _interpret(engine, digest, *, kind, target, context, now):
    """One existing bridge request followed by the normal evidence validation."""
    source = source_digest_identity(digest)
    packet = build_packet(
        digest, report_kind=kind, next_trade_date=target, context=context,
        previous_research=None, now=now, source_digest_sha256=source,
        allow_fact_baseline=True,
    )
    rules, _ = _load_rules()
    # The shared report schema and evidence requirements apply. The independent
    # cross-provider review procedure belongs to the separate research workflow.
    before, _, arbitration = rules.partition("## 自动审查与证据仲裁")
    _, remaining, after = arbitration.partition("## 机会推演与权限")
    if remaining:
        rules = before + remaining + after
    schema = draft_json_schema()
    instruction = "你是本报告的唯一解读模型。根据提供的量化事实分析，缺失材料如实说明。"
    if kind == "briefing":
        instruction += "先处理隔夜美股收盘与已核实的显著外盘风险，再评估A股延续条件。外盘涨跌不能写成A股已发生的走势。"
    messages = _messages(
        instruction,
        {"input": packet}, schema, rules,
    )
    rules_sha256 = artifact_hash({"rules": rules, "schema": schema, "workflow": "codex_daily_report"})
    reply = _call_codex(
        engine, messages, schema, timeout_seconds=150, stage="codex_daily_report",
        request_id=str(uuid4()), rules_sha256=rules_sha256,
    )
    draft = validate_draft(reply.raw_response, packet)
    return {
        "mode": "codex", "status": "COMPLETE", "report_kind": kind,
        "review_date": digest["review_date"], "next_trade_date": target,
        "generated_at": now.isoformat(), "source_digest_sha256": source,
        "content": render_draft(draft, packet, {}), "draft": draft, "raw_response": reply.raw_response,
        "input_packet": packet, "provider_receipt": reply.provider_receipt,
    }


def _optional_interpretation(engine, digest, *, kind, target, context, now):
    from biz.review.research_flow import prepare_research_facts
    try:
        verified = prepare_research_facts(digest, context)
        report = _interpret(engine, verified, kind=kind, target=target, context=context, now=now)
        report["base_digest_sha256"] = source_digest_identity(digest)
        return report
    except (ResearchError, ResearchBridgeError, SQLAlchemyError, ValueError) as exc:
        log.warning("Codex %s %s unavailable after facts publication: %s", kind, target, str(exc)[:300])
        return None


def publish_review(engine, target, *, force=False):
    existing = load_digest(engine, target, required=False)
    if existing and existing["publish_status"] == "ready" and not force:
        assert_digest(existing, target)
        if existing["factor_validation_json"].get("market_decision"):
            return existing
    else:
        result = generate_quant_digest(engine, target, persist=False)
        if result["publish_status"] == "ready":
            result["compact_review"] += "\n\n" + FACTS_NOTE
        persist_quant_digest(engine, result)
        if result["publish_status"] != "ready":
            return result
        existing = load_digest(engine, target)
        assert_digest(existing, target)
        if existing["compact_review"] != result["compact_review"]:
            raise RuntimeError("FAILED stage=review_readback: published facts differ")

    now = datetime.now(TZ)
    # The source data is already visible while optional research runs.
    try:
        context = collect_context(engine, target, now=now)
        interpretation = _optional_interpretation(
            engine, existing, kind="review", target=next_session(engine, target), context=context, now=now,
        )
    except (RuntimeError, ValueError, SQLAlchemyError) as exc:
        log.warning("Review %s facts published; Codex inputs unavailable: %s", target, str(exc)[:300])
        return existing
    if interpretation:
        _update_report(engine, existing, review=interpretation)
        return load_digest(engine, target)
    return existing


def _briefing_facts(digest, target, context):
    us = context.get("us_market_close") or {}
    quotes = us.get("rows") or []
    lines = [f"# A股早报｜{target}", "", f"> 收盘行情日 {digest['review_date']}",
             f"> 隔夜资料截至 {context['cutoff_at']}（北京时间）", "", "## 隔夜风险｜美股收盘", ""]
    if quotes:
        lines.append(f"美东 {us['session_date']} 收盘。")
        for row in quotes:
            lines.append(f"- {row['display_name']} {row['change_pct']:+.2f}%（{row['source']}）")
    if len(quotes) < len(US_CASH_INDICES):
        lines.append("美股三大指数收盘行情尚未全部核实。")
    for row in us.get("headlines", []):
        title = " ".join(str(row.get("title") or "").split())[:180]
        lines.append(f"- 收盘快讯（{row['source']}） {title}")
    if sum(row["change_pct"] <= -2 for row in quotes) >= 2 or any(
        row["symbol"] == "nasdaq" and row["change_pct"] <= -3 for row in quotes
    ):
        lines.extend(["", "**外盘风险升高。** 开盘先观察 A 股科技方向是否承压，以及上涨家数和前日强势股能否企稳，再评估普涨延续。"])
    lines.extend(["", render_compact_review(digest), "", "## 隔夜新闻", ""])
    events = context["events"]
    if events["status"] == "OBSERVED":
        seen = set()
        for row in events["rows"]:
            title = " ".join(str(row.get("title") or "").split())
            if not title or title in seen:
                continue
            seen.add(title)
            stamp = str(row.get("publish_time") or "").replace("T", " ")[:16]
            lines.append(f"- {stamp} {title[:180]}")
            if len(seen) == 10:
                break
        if not seen:
            lines.append("现有记录未提供可展示的标题。")
    elif events["status"] == "EMPTY":
        lines.append("已采集来源在本窗口没有新增记录。")
    else:
        lines.append("隔夜新闻暂不可用。")
    external = [row for row in (context.get("external_market") or {}).get("rows", [])
                if row.get("symbol") not in US_CASH_INDICES and row.get("availability") == "available"
                and row.get("change_pct") is not None]
    if external:
        lines.extend(["", "## 隔夜市场", ""])
        for row in external[:10]:
            label = " ".join(str(row.get("display_name") or row.get("symbol") or "").split())
            stamp = str(row.get("market_time") or "").replace("T", " ")[:16]
            lines.append(f"- {label} {row['change_pct']}%（{stamp}）")
    notices = context.get("notices") or {}
    lines.extend(["", "## 隔夜公司公告", ""])
    if notices.get("status") == "OBSERVED":
        for row in notices["rows"]:
            title = " ".join(str(row.get("title") or "").split())[:180]
            lines.append(f"- {row['stock_code']} {title}")
    elif notices.get("status") == "EMPTY":
        lines.append("本窗口未读到带可核实发布时间的公告。")
    else:
        lines.append("本窗口公告资料暂不可用。")
    return "\n".join(lines) + "\n\n" + FACTS_NOTE


def published_review_content(digest):
    """Return the scheduled review format after checking its stored source and body."""
    content = str(digest.get("compact_review") or "")
    research = digest["factor_validation_json"].get("market_decision") or {}
    facts_only = not research and content.endswith("\n\n" + FACTS_NOTE)
    if not facts_only and research.get("mode") != "codex":
        return None
    facts = render_compact_review(digest)
    if facts_only and content == facts + "\n\n" + FACTS_NOTE:
        return content
    if (research.get("status") != "COMPLETE" or research.get("report_kind") != "review"
            or research.get("review_date") != digest["review_date"]
            or research.get("base_digest_sha256") != source_digest_identity(digest)):
        raise RuntimeError("DATA_BLOCKED stage=review_publication: Codex source differs")
    packet = research["input_packet"]
    draft = validate_draft(research["raw_response"], packet)
    interpretation = render_draft(draft, packet, {})
    if interpretation != research["content"] or content != facts + "\n\n## Codex 解读\n\n" + interpretation:
        raise RuntimeError("DATA_BLOCKED stage=review_publication: Codex content differs")
    return content


def _briefing_us_close(engine, digest, target, opening, now):
    session = opening.astimezone(NEW_YORK).date()
    while session.weekday() >= 5:
        session -= timedelta(days=1)
    close = datetime.combine(session, time(16), NEW_YORK).astimezone(TZ)
    rows = []
    cutoff = min(now, opening)
    if cutoff >= close:
        prior = (digest["factor_validation_json"].get("early_briefing") or {}).get("us_market_close") or {}
        if prior.get("session_date") == session.isoformat() and all(
            datetime.fromisoformat(row["captured_at"]) <= cutoff for row in prior.get("rows", [])
        ):
            rows = prior.get("rows") or []
        if now <= opening and len(rows) < len(US_CASH_INDICES):
            fresh = fetch_us_cash_close(session, cutoff=opening)
            by_symbol = {row["symbol"]: row for row in [*rows, *fresh]}
            rows = [by_symbol[symbol] for symbol in US_CASH_INDICES if symbol in by_symbol]
    cutoff = min(datetime.now(TZ), opening)
    headlines = []
    if cutoff >= close and len(rows) < len(US_CASH_INDICES):
        try:
            headlines = read_records(text(
                "SELECT /*+ MAX_EXECUTION_TIME(5000) */ source,title,publish_time,first_seen_at "
                "FROM st_news_flash WHERE publish_time BETWEEN :close AND :cutoff "
                "AND first_seen_at<=:cutoff AND (title LIKE '%纳指%收盘%' OR title LIKE '%美股%收盘%' "
                "OR title LIKE '%标普%收盘%' OR title LIKE '%道指%收盘%') "
                "ORDER BY publish_time DESC LIMIT 3"
            ), engine, params={"close": close.replace(tzinfo=None), "cutoff": cutoff.replace(tzinfo=None)})
            headlines = [{**row, "publish_time": str(row["publish_time"]),
                          "first_seen_at": str(row["first_seen_at"])} for row in headlines]
        except SQLAlchemyError as exc:
            log.warning("US market close news unavailable for %s: %s", target, type(exc).__name__)
    return {"session_date": session.isoformat(), "rows": rows, "headlines": headlines}, cutoff


def publish_briefing(engine, review_date, *, persist=True):
    digest = load_digest(engine, review_date)
    assert_digest(digest, review_date)
    target = next_session(engine, review_date)
    now = datetime.now(TZ)
    # A late recovery still uses information available before the opening.
    opening = datetime.combine(date.fromisoformat(target), time(9, 30), TZ)
    us_close, cutoff = _briefing_us_close(engine, digest, target, opening, now)
    context = collect_context(engine, review_date, now=cutoff, overnight=True, include_research=False)
    context["us_market_close"] = us_close
    now = datetime.now(TZ)
    facts = {
        "mode": "facts", "status": "COMPLETE", "report_kind": "briefing",
        "review_date": review_date, "next_trade_date": target, "generated_at": now.isoformat(),
        "source_digest_sha256": source_digest_identity(digest),
        "us_market_close": us_close,
        "content": _briefing_facts(digest, target, context),
    }
    if persist:
        _update_report(engine, digest, briefing=facts)
    interpretation = _optional_interpretation(
        engine, digest, kind="briefing", target=target, context=context, now=now,
    )
    if interpretation:
        facts_with_ai = {**facts, "mode": "codex", "interpretation": interpretation,
                         "content": facts["content"].removesuffix("\n\n" + FACTS_NOTE)
                         + "\n\n## Codex 解读\n\n" + interpretation["content"]}
        if persist:
            _update_report(engine, digest, briefing=facts_with_ai, expected_briefing=facts)
        return facts_with_ai
    return facts
