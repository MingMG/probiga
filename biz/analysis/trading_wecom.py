"""Fail-soft WeCom notifications for internal paper-trading results."""
from __future__ import annotations

import logging
from typing import Any

from integrations.wecom.webhook import send_markdown
from server.common.config import get_wecom_webhook

logger = logging.getLogger(__name__)


def _money(value: Any) -> str:
    try:
        return f"{float(value):,.2f}"
    except (TypeError, ValueError):
        return "—"


def notify_sim_trade_result(result: dict[str, Any]) -> dict[str, Any]:
    """Push only newly completed simulated fills; an empty tick stays silent."""
    fills = [
        row for row in (result.get("match_results") or [])
        if str(row.get("status") or "").lower() == "filled"
    ]
    if not fills:
        return {"status": "skipped", "reason": "no_new_fill"}
    webhook = get_wecom_webhook("briefing", required=False)
    if not webhook:
        return {"status": "skipped", "reason": "webhook_not_configured"}
    lines = [
        "### ProBigA 模拟成交",
        f"> 交易日：{result.get('trade_date') or '—'}",
        "> **仅内部模拟盘，真实交易保持关闭**",
    ]
    for row in fills[:10]:
        side = "买入" if str(row.get("side") or "").upper() == "BUY" else "卖出"
        code = str(row.get("stock_code") or "")
        if not code and row.get("position_id"):
            code = f"持仓#{row.get('position_id')}"
        shares = row.get("shares") or row.get("filled_shares") or 0
        price = row.get("price") or row.get("filled_price")
        lines.append(
            f"> {side} `{code or '未知代码'}` {shares} 股 @ {_money(price)}，订单 #{row.get('order_id') or '—'}"
        )
    try:
        response = send_markdown(webhook, "\n".join(lines))
        return {"status": "sent", "fill_count": len(fills), "response": response}
    except Exception as exc:  # notification must never break the scheduler tick
        logger.warning("paper trading WeCom notification failed: %s", exc)
        return {"status": "error", "error": str(exc)[:300]}


def notify_v3_decision_result(result: dict[str, Any]) -> dict[str, Any]:
    webhook = get_wecom_webhook("briefing", required=False)
    if not webhook:
        return {"status": "skipped", "reason": "webhook_not_configured"}
    lines = [
        "### ProBigA V3 日级决策完成",
        f"> 数据交易日：{result.get('trade_date') or '—'}",
        f"> 市场状态：{result.get('market_regime') or '—'}",
        f"> 组合结论：{result.get('portfolio_status') or '—'}",
        f"> 新建模拟订单：**{int(result.get('paper_order_count') or 0)}**",
        "> **真实交易保持关闭**",
    ]
    try:
        response = send_markdown(webhook, "\n".join(lines))
        return {"status": "sent", "response": response}
    except Exception as exc:
        logger.warning("V3 decision WeCom notification failed: %s", exc)
        return {"status": "error", "error": str(exc)[:300]}


def notify_screener_result(result: dict[str, Any]) -> dict[str, Any]:
    """Deliver a persisted top-five ranking to the briefing robot."""
    rows = result.get("data") or []
    if not rows:
        return {"status": "skipped", "reason": "no_screener_result"}
    run = result.get("run") or {}
    if not run.get("persisted"):
        return {"status": "error", "error": "候选榜未落库，禁止发送无追溯结果"}
    webhook = get_wecom_webhook("briefing", required=False)
    if not webhook:
        return {"status": "skipped", "reason": "webhook_not_configured"}
    freshness = {
        "live": "实时盘中快照",
        "historical_close": "收盘复盘快照",
        "exact": "指定交易日数据",
        "fallback": "最近可用交易日数据",
    }.get(str(result.get("freshness") or ""), str(result.get("freshness") or "未知"))
    preset = str(result.get("preset") or "")
    title = "盘前生产候选榜" if preset == "capital_support" else "开盘生产融合候选榜" if preset == "intraday_sector" else "生产融合候选榜"
    lines = [
        f"### ProBigA {title}",
        f"> 数据日期：{result.get('data_date') or '—'}；证据时间：{result.get('observed_at') or run.get('generated_at') or '—'}",
        f"> 数据状态：{freshness}；策略：{preset or '—'}",
    ]
    for index, row in enumerate(rows[:5], 1):
        code = str(row.get("stock_code") or "")[:6]
        name = str(row.get("stock_name") or row.get("short_name") or code or "未知证券")
        score = row.get("ensemble_score", row.get("score"))
        change = row.get("change_pct")
        score_text = "—" if score is None else f"{float(score):.2f}"
        change_text = "—" if change is None else f"{float(change):+.2f}%"
        lines.append(f"> {index}. **{name}（{code}）** · 综合分 {score_text} · 涨幅 {change_text}")
    lines.extend(
        [
            f"> 批次：`{run.get('run_uid') or '—'}`（已固定落库，可按日期追溯）",
            "> **仅用于研究与模拟，真实下单保持关闭。**",
        ]
    )
    try:
        response = send_markdown(webhook, "\n".join(lines))
        if int((response or {}).get("errcode") or 0) != 0:
            return {"status": "error", "error": str(response)[:300], "response": response}
        return {"status": "sent", "result_count": len(rows), "response": response}
    except Exception as exc:
        logger.warning("screener WeCom notification failed: %s", exc)
        return {"status": "error", "error": str(exc)[:300]}


def notify_screener_failure(
    *,
    preset: str,
    reason: str,
    stage: str = "generate",
) -> dict[str, Any]:
    """Tell the briefing channel when a promised ranking was not delivered."""
    webhook = get_wecom_webhook("briefing", required=False)
    if not webhook:
        return {"status": "skipped", "reason": "webhook_not_configured"}
    title = "09:08 盘前榜" if preset == "capital_support" else "09:32 开盘融合榜"
    content = "\n".join(
        [
            f"### ProBigA {title}生成失败",
            f"> 失败阶段：{stage}",
            f"> 原因：{str(reason or '未知错误')[:500]}",
            "> 系统会保留日志并按调度规则重试；本次没有可供使用的新榜单。",
        ]
    )
    try:
        response = send_markdown(webhook, content)
        return {"status": "sent", "response": response}
    except Exception as exc:
        logger.warning("screener failure notification failed: %s", exc)
        return {"status": "error", "error": str(exc)[:300]}
