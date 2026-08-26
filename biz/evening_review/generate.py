#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A股每日晚报 — 盘后全市场复盘，推送到企微早报机器人

执行:
  python -m biz.evening_review.generate 2026-05-19  # 指定日期
  python -m biz.evening_review.generate               # 默认最新交易日
  python -m biz.evening_review.generate --test        # 不推送，仅打印
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

import httpx
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("evening")

from server.common.batch_db import create_batch_engine, read_records
from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.config import get_settings, get_wecom_webhook
from server.common.daily_stock_universe import (
    load_daily_stock_universe,
    validate_daily_stock_coverage,
)
from server.common.tech_risk import append_tech_risk_markdown, fetch_tech_risk_signal
from integrations.wecom.delivery import deliver_markdown
try:
    from biz.research_radar.radar import build_research_radar, format_radar_markdown
except Exception:
    build_research_radar = None
    format_radar_markdown = None

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"
EXPECTED_A_SHARE_INDEX_CODES = frozenset(
    {"000001", "399001", "399006", "000688", "000300"}
)
MIN_HOT_CONCEPT_ROWS = 20
MIN_FUSED_HOT_STOCK_ROWS = 20


def get_engine():
    return create_batch_engine()


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "-"
    sign = "+" if v > 0 else ""
    return f"{sign}{v:.2f}%"


def _fmt_amt(v: float | None) -> str:
    if v is None or v == 0:
        return "-"
    if abs(v) >= 1e8:
        return f"{v/1e8:.1f}亿"
    if abs(v) >= 1e4:
        return f"{v/1e4:.0f}万"
    return f"{v:.0f}"


def _color_tag(v: float | None) -> str:
    if v is None:
        return ""
    return '<font color="info">' if v > 0 else '<font color="warning">' if v < 0 else ""


def _color_close(v: float | None) -> str:
    if v is None:
        return ""
    return "</font>" if v != 0 else ""


def _red(v: float | None) -> str:
    if v is None:
        return ""
    return '<font color="warning">' if v > 0 else ""  # A股红涨绿跌


def _green(v: float | None) -> str:
    if v is None:
        return ""
    return '<font color="info">' if v < 0 else ""


def _iso_trade_date(value: object, *, field: str) -> str:
    raw = str(value or "")[:10]
    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"DATA_BLOCKED: {field} is not a valid trade date") from exc
    if parsed.isoformat() != raw:
        raise RuntimeError(f"DATA_BLOCKED: {field} is not a valid trade date")
    return raw


def resolve_review_trade_date(
    engine, explicit_date: str = "", *, now: datetime | None = None
) -> str:
    """Resolve a closed exchange session; never infer readiness from K-line MAX."""

    authoritative = authoritative_closed_trade_date(engine, now=now)
    if not authoritative:
        raise RuntimeError(
            "DATA_BLOCKED: authoritative closed trade date is unavailable"
        )
    authoritative = _iso_trade_date(
        authoritative, field="authoritative closed trade date"
    )
    if not explicit_date:
        return authoritative
    target = _iso_trade_date(explicit_date, field="requested review date")
    if target > authoritative:
        raise RuntimeError(
            "DATA_BLOCKED: requested review date is not a closed session: "
            f"requested={target}, authoritative={authoritative}"
        )
    rows = _query(
        engine,
        "SELECT COUNT(*) AS n FROM si_trade_calendar "
        "WHERE trade_status=1 AND trade_date=:d",
        {"d": target},
    )
    if not rows or int(rows[0].get("n") or 0) != 1:
        raise RuntimeError(
            f"DATA_BLOCKED: requested review date is not an open session: {target}"
        )
    return target


def _assert_rows_on_target(
    rows: list[dict], *, date_field: str, target: str, source: str
) -> None:
    wrong = sorted(
        {
            str(row.get(date_field) or "")[:10] or "<empty>"
            for row in rows
            if str(row.get(date_field) or "")[:10] != target
        }
    )
    if wrong:
        raise RuntimeError(
            f"DATA_BLOCKED: {source} contains mixed dates: "
            f"target={target}, actual_dates={wrong[:10]}"
        )


def _assert_legacy_market_data_contract(data: dict, target_date: str) -> dict:
    contract = data.get("_data_contract") if isinstance(data, dict) else None
    if not isinstance(contract, dict):
        raise RuntimeError("DATA_BLOCKED: legacy evening data contract is missing")
    target = _iso_trade_date(target_date, field="target review date")
    if (
        contract.get("status") != "PASS"
        or contract.get("target_trade_date") != target
        or int(contract.get("expected_stock_count") or 0) <= 0
        or float(contract.get("kline_coverage") or 0) != 1.0
        or float(contract.get("traded_flow_coverage") or 0) != 1.0
        or int(contract.get("index_count") or 0) != len(EXPECTED_A_SHARE_INDEX_CODES)
        or int(contract.get("hot_concept_count") or 0) < MIN_HOT_CONCEPT_ROWS
        or int(contract.get("fused_stock_count") or 0) < MIN_FUSED_HOT_STOCK_ROWS
    ):
        raise RuntimeError(
            "DATA_BLOCKED: legacy evening data contract is stale or incomplete"
        )
    return contract


def _assert_digest_target_contract(digest: dict, target_date: str) -> dict:
    """Prove that a ready digest was built from the requested target session."""

    target = _iso_trade_date(target_date, field="target review date")
    quality = digest.get("quality_json") if isinstance(digest, dict) else None
    source_dates = quality.get("source_dates") if isinstance(quality, dict) else None
    actual = {
        "review_date": str(digest.get("review_date") or "")[:10]
        if isinstance(digest, dict)
        else "",
        "quality_target_date": str(quality.get("target_date") or "")[:10]
        if isinstance(quality, dict)
        else "",
        "target_bars": str(source_dates.get("target_bars") or "")[:10]
        if isinstance(source_dates, dict)
        else "",
    }
    if any(value != target for value in actual.values()):
        raise RuntimeError(
            "DATA_BLOCKED: quant review input dates do not match target: "
            f"target={target}, actual={actual}"
        )
    return quality


def collect_market_data(engine, date_str: str) -> dict:
    """采集一个已闭市目标交易日的盘面数据。"""
    target = _iso_trade_date(date_str, field="target review date")
    params = {"d": target}
    daily_rows = _query(
        engine,
        "SELECT stock_code, trade_date, volume, amount FROM sm_stock_kline "
        "WHERE trade_date=:d AND k_type=1 AND adjust_type=0 ORDER BY stock_code",
        params,
    )
    flow_coverage_rows = _query(
        engine,
        "SELECT stock_code, trade_date FROM sm_stock_capital_flow_daily "
        "WHERE trade_date=:d ORDER BY stock_code",
        params,
    )
    _assert_rows_on_target(
        daily_rows, date_field="trade_date", target=target, source="daily K-line"
    )
    _assert_rows_on_target(
        flow_coverage_rows,
        date_field="trade_date",
        target=target,
        source="capital flow",
    )
    universe = load_daily_stock_universe(
        engine,
        target,
        decision_known_at=datetime.now().replace(microsecond=0),
    )
    coverage = validate_daily_stock_coverage(
        universe,
        kline_rows=daily_rows,
        flow_rows=flow_coverage_rows,
    )
    data = {"A股数据日期": target}

    # 1. 指数表现
    rows = _query(engine, """
        SELECT i.index_code, i.price, i.change_pct, i.trade_date
        FROM sm_index_current i
        WHERE i.trade_date=:d
          AND i.index_code IN ('000001','399001','399006','000688','000300')
        ORDER BY FIELD(i.index_code,'000001','399001','399006','000688','000300')
    """, params)
    _assert_rows_on_target(
        rows, date_field="trade_date", target=target, source="A-share index"
    )
    index_codes = {
        str(row.get("index_code") or "").strip()
        for row in rows
        if row.get("price") is not None
    }
    if index_codes != EXPECTED_A_SHARE_INDEX_CODES:
        raise RuntimeError(
            "DATA_BLOCKED: target-date A-share index snapshot is incomplete: "
            f"target={target}, missing={sorted(EXPECTED_A_SHARE_INDEX_CODES-index_codes)}"
        )
    nm = {"000001": "上证指数", "399001": "深证成指", "399006": "创业板指", "000688": "科创50", "000300": "沪深300"}
    data["指数"] = {nm.get(r["index_code"], r["index_code"]): r for r in rows if r.get("price")}

    # 2. 涨跌家数 & 成交额
    rows = _query(engine, """
        SELECT
            COUNT(*) AS total_cnt,
            SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) AS up_cnt,
            SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) AS down_cnt,
            SUM(CASE WHEN change_pct = 0 THEN 1 ELSE 0 END) AS flat_cnt,
            COALESCE(SUM(amount), 0) AS total_amt,
            AVG(change_pct) AS avg_chg,
            AVG(turnover_ratio) AS avg_turnover
        FROM sm_stock_kline
        WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0
    """, {"d": date_str})
    if rows and rows[0]["total_cnt"]:
        r = rows[0]
        total = max(int(r["total_cnt"] or 0), 1)
        data["涨跌"] = {
            "上涨": int(r["up_cnt"] or 0),
            "下跌": int(r["down_cnt"] or 0),
            "平盘": int(r["flat_cnt"] or 0),
            "上涨比例": round(int(r["up_cnt"] or 0) / total * 100, 1),
        }
        data["成交额"] = float(r["total_amt"] or 0)
        data["均价涨幅"] = float(r["avg_chg"] or 0)
        data["均换手率"] = float(r["avg_turnover"] or 0)

    # 3. 资金流向TOP
    rows = _query(engine, """
        SELECT f.stock_code, COALESCE(s.short_name, f.stock_code) AS sn, f.main_net_inflow
        FROM sm_stock_capital_flow_daily f
        LEFT JOIN si_all_code s ON f.stock_code = s.stock_code
        WHERE f.trade_date = :d
        ORDER BY f.main_net_inflow DESC LIMIT 6
    """, {"d": date_str})
    data["净流入TOP"] = [(r["sn"], float(r["main_net_inflow"] or 0)) for r in rows]

    rows = _query(engine, """
        SELECT f.stock_code, COALESCE(s.short_name, f.stock_code) AS sn, f.main_net_inflow
        FROM sm_stock_capital_flow_daily f
        LEFT JOIN si_all_code s ON f.stock_code = s.stock_code
        WHERE f.trade_date = :d
        ORDER BY f.main_net_inflow ASC LIMIT 6
    """, {"d": date_str})
    data["净流出TOP"] = [(r["sn"], float(r["main_net_inflow"] or 0)) for r in rows]

    # 4. 热门板块
    rows = _query(engine, """
        SELECT concept_name, change_pct, hot_value, plate_type, snapshot_date
        FROM st_hot_concept_ths_daily
        WHERE snapshot_date = :d AND plate_type IN (1,2)
        ORDER BY plate_type, `rank`
    """, {"d": date_str})
    _assert_rows_on_target(
        rows, date_field="snapshot_date", target=target, source="hot concept"
    )
    if (
        len(rows) < MIN_HOT_CONCEPT_ROWS
        or not {1, 2} <= {int(row.get("plate_type") or 0) for row in rows}
    ):
        raise RuntimeError(
            f"DATA_BLOCKED: target-date hot concept snapshot is incomplete: target={target}, rows={len(rows)}"
        )
    hot_concept_count = len(rows)
    data["热门行业"] = [r for r in rows if r.get("plate_type") == 2][:8]
    data["热门概念"] = [r for r in rows if r.get("plate_type") == 1][:10]

    # 5. 融合榜热门个股TOP10
    rows = _query(engine, """
        SELECT f.short_name, f.stock_code, f.change_pct, f.fused_rank, f.snapshot_date,
               t.pop_tag, t.concept_tag
        FROM st_hot_rank_fused f
        LEFT JOIN st_hot_rank_ths t ON t.stock_code = f.stock_code COLLATE utf8mb4_unicode_ci
            AND t.snapshot_date = f.snapshot_date
        WHERE f.snapshot_date = :d
        ORDER BY f.fused_rank LIMIT 20
    """, {"d": date_str})
    _assert_rows_on_target(
        rows, date_field="snapshot_date", target=target, source="fused hot stock"
    )
    fused_stock_count = len(
        {str(row.get("stock_code") or "").strip() for row in rows}
    )
    if fused_stock_count < MIN_FUSED_HOT_STOCK_ROWS:
        raise RuntimeError(
            f"DATA_BLOCKED: target-date fused hot-stock snapshot is incomplete: target={target}"
        )
    data["热门个股"] = rows

    # 6. 龙虎榜概要
    rows = _query(engine, """
        SELECT stock_code, short_name, a_net_amount, change_cpt AS change_pct, reason
        FROM st_a_list_daily
        WHERE trade_date = :d
        ORDER BY ABS(a_net_amount) DESC LIMIT 8
    """, {"d": date_str})
    data["龙虎榜"] = rows

    # 7. 涨跌停
    rows = _query(engine, """
        SELECT stock_code, short_name, close, change_pct
        FROM sm_stock_kline
        WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0 AND change_pct >= 0.09
        ORDER BY change_pct DESC LIMIT 8
    """, {"d": date_str})
    data["涨停板"] = rows

    rows = _query(engine, """
        SELECT stock_code, short_name, close, change_pct
        FROM sm_stock_kline
        WHERE trade_date = :d AND k_type = 1 AND adjust_type = 0 AND change_pct <= -0.09
        ORDER BY change_pct ASC LIMIT 5
    """, {"d": date_str})
    data["跌停板"] = rows

    data["_data_contract"] = {
        "status": "PASS",
        "target_trade_date": target,
        "expected_stock_count": int(coverage["expected_count"]),
        "kline_coverage": float(coverage["kline_coverage"]),
        "traded_flow_coverage": float(coverage["traded_flow_coverage"]),
        "index_count": len(index_codes),
        "hot_concept_count": hot_concept_count,
        "fused_stock_count": fused_stock_count,
    }
    _assert_legacy_market_data_contract(data, target)
    return data


def build_report(data: dict, date_str: str, ai_analysis: str = "") -> str:
    """组装晚报markdown"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_display = dt.strftime("%m月%d日")
    lines = [f"## 📊 A股复盘晚报 | {date_display}"]

    # ── 指数 ──
    idx = data.get("指数", {})
    if idx:
        parts = []
        for name, r in idx.items():
            chg = r.get("change_pct")
            color = "warning" if (chg or 0) > 0 else "info" if (chg or 0) < 0 else "comment"
            parts.append(f'<font color="{color}">{name} {_fmt_pct(chg)}</font>')
        lines.append(f"\n**📐 主要指数**  {' · '.join(parts)}")

    # ── 市场总览 ──
    ud = data.get("涨跌", {})
    amt = data.get("成交额", 0)
    if ud:
        total = ud["上涨"] + ud["下跌"] + ud["平盘"]
        lines.append(f"\n**📈 市场总览**")
        lines.append(f'> 成交额 <font color="comment">{_fmt_amt(amt)}</font>　'
                     f'上涨 <font color="warning">{ud["上涨"]}</font> 家　'
                     f'下跌 <font color="info">{ud["下跌"]}</font> 家　'
                     f'上涨比例 {ud["上涨比例"]}%')
        if data.get("均价涨幅"):
            lines.append(f'> 全市场均价涨幅 {_fmt_pct(data["均价涨幅"])}　均换手率 {data["均换手率"]:.2f}%')

    # ── AI 分析（核心） ──
    if ai_analysis:
        lines.append(f"\n> ━━━━ **🤖 AI 盘面分析** ━━━━")
        for aline in ai_analysis.strip().split("\n"):
            aline = aline.strip()
            if aline:
                if aline.startswith("**") and "**" in aline[2:]:
                    parts2 = aline.split("**")
                    lines.append(f'\n**{parts2[1]}**{parts2[2] if len(parts2) > 2 else ""}')
                else:
                    lines.append(aline)
        lines.append("> ━━━━━━━━━━━━━━━━━━━━")

    # ── 资金流向 ──
    inflows = data.get("净流入TOP", [])
    outflows = data.get("净流出TOP", [])
    if inflows or outflows:
        lines.append(f"\n**💰 主力资金**")
        if inflows:
            # 过滤掉负数值（净流出的不应该出现在净流入列表）
            valid_inflows = [(sn, amt) for sn, amt in inflows if amt > 0]
            if valid_inflows:
                items = ", ".join([f'{sn}(<font color="warning">{_fmt_amt(amt)}</font>)' for sn, amt in valid_inflows[:5]])
                lines.append(f'> 净流入: {items}')
        if outflows:
            # 过滤掉正数值（净流入的不应该出现在净流出列表）
            valid_outflows = [(sn, amt) for sn, amt in outflows if amt < 0]
            if valid_outflows:
                items = ", ".join([f'{sn}(<font color="info">{_fmt_amt(abs(amt))}</font>)' for sn, amt in valid_outflows[:5]])
                lines.append(f'> 净流出: {items}')

    # ── 热门概念 ──
    concepts = data.get("热门概念", [])
    if concepts:
        items = []
        for r in concepts[:8]:
            chg = r.get("change_pct") or 0
            color = "warning" if float(chg) > 0 else "info"
            items.append(f'<font color="{color}">{r["concept_name"]}({_fmt_pct(float(chg))})</font>')
        lines.append(f"\n**🔥 热门概念**  {' · '.join(items)}")

    # ── 热门行业 ──
    industries = data.get("热门行业", [])
    if industries:
        items = []
        for r in industries[:6]:
            chg = r.get("change_pct") or 0
            color = "warning" if float(chg) > 0 else "info"
            items.append(f'<font color="{color}">{r["concept_name"]}({_fmt_pct(float(chg))})</font>')
        lines.append(f"\n**🏭 热门行业**  {' · '.join(items)}")

    # ── 热门个股 ──
    hot_stocks = data.get("热门个股", [])
    if hot_stocks:
        lines.append(f"\n**⭐ 融合热门个股 TOP{len(hot_stocks)}**")
        items = []
        for r in hot_stocks[:10]:
            chg = r.get("change_pct") or 0
            name = r.get("short_name", "") or r.get("stock_code", "")
            code = r.get("stock_code", "")
            tag = r.get("pop_tag", "") or r.get("concept_tag", "") or ""
            color = "warning" if float(chg or 0) > 0 else "info"
            items.append(f'<font color="{color}">{name}({code}) {_fmt_pct(float(chg))}</font>{(" "+tag) if tag else ""}')
        lines.append("> " + "\n> ".join(items))

    # ── 龙虎榜 ──
    lhb = data.get("龙虎榜", [])
    if lhb:
        lines.append(f"\n**🐲 龙虎榜**")
        items = []
        for r in lhb[:6]:
            amt = float(r.get("a_net_amount") or 0)
            name = r.get("short_name", "") or r.get("stock_code", "")
            chg = r.get("change_pct") or 0
            color = "warning" if float(chg or 0) > 0 else "info"
            items.append(f'<font color="{color}">{name} 净{_fmt_amt(amt)} ({_fmt_pct(float(chg))})</font>')
        lines.append("> " + "\n> ".join(items))

    # ── 涨跌停 ──
    zt = data.get("涨停板", [])
    dt2 = data.get("跌停板", [])
    if zt:
        names = [r.get("short_name", "") or r["stock_code"] for r in zt[:5]]
        lines.append(f'\n**📈 涨停板** <font color="warning">{", ".join(names)}</font>')
    if dt2:
        names = [r.get("short_name", "") or r["stock_code"] for r in dt2[:5]]
        lines.append(f'\n**📉 跌停板** <font color="info">{", ".join(names)}</font>')

    lines.append(f'\n<font color="comment">数据基于 {date_str} 收盘 · ProBigA 智能晚报 · 仅供参考不构成投资建议</font>')

    return "\n".join(lines)


def push_to_wecom(content: str, engine=None) -> bool:
    """可靠投递晚报；任何缺配置或部分失败都会抛错。"""

    result = deliver_markdown(
        get_wecom_webhook("briefing", required=False),
        content,
        engine=engine or get_engine(),
        delivery_kind="evening_review",
        webhook_kind="briefing",
        title="## 📊 盘后复盘",
    )
    log.info(
        "企微推送完成: delivery_id=%s, %d/%d 段",
        result.delivery_id,
        result.delivered_count,
        result.segment_count,
    )
    return result.success


def _query(engine, sql: str, params: dict = None):
    # SQLAlchemy 2.x does not accept a plain SQL string through every
    # execution path. Bind explicitly so named parameters such as ``:d`` are
    # preserved when pandas and SQLAlchemy versions differ.
    return read_records(text(sql), engine, params=params)


def _find_latest_trade_date(engine) -> str:
    """Compatibility wrapper for callers; authoritative calendar is the source."""

    return resolve_review_trade_date(engine)


def analyze_with_deepseek(data: dict, date_str: str) -> str:
    """用 DeepSeek 对盘面数据进行 AI 复盘分析"""
    settings = get_settings()
    api_key = (settings.deepseek_api_key or "").strip()
    if not api_key:
        log.warning("未配置 DEEPSEEK_API_KEY，跳过 AI 分析")
        return ""

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_display = dt.strftime("%m月%d日")

    # Build prompt
    prompt_parts = [f"## A股盘面数据 ({date_display}收盘)\n"]

    idx = data.get("指数", {})
    if idx:
        idx_lines = ["**主要指数表现**:"]
        for name, r in idx.items():
            idx_lines.append(f"- {name}: {_fmt_pct(r.get('change_pct'))}")
        prompt_parts.append("\n".join(idx_lines))

    ud = data.get("涨跌", {})
    amt = data.get("成交额", 0)
    if ud:
        prompt_parts.append(f"\n**涨跌家数**: 上涨{ud.get('上涨',0)}家, 下跌{ud.get('下跌',0)}家, 平盘{ud.get('平盘',0)}家")
        prompt_parts.append(f"**成交额**: {_fmt_amt(amt)}")
        prompt_parts.append(f"**均价涨幅**: {_fmt_pct(data.get('均价涨幅'))}")

    inflows = data.get("净流入TOP", [])
    outflows = data.get("净流出TOP", [])
    if inflows:
        items = [f"{sn}({_fmt_amt(flow_amt)})" for sn, flow_amt in inflows[:5]]
        prompt_parts.append(f"\n**主力净流入TOP5**: {', '.join(items)}")
    if outflows:
        items = [f"{sn}({_fmt_amt(abs(flow_amt))})" for sn, flow_amt in outflows[:5]]
        prompt_parts.append(f"**主力净流出TOP5**: {', '.join(items)}")

    concepts = data.get("热门概念", [])
    if concepts:
        items = [f'{r["concept_name"]}({_fmt_pct(float(r.get("change_pct") or 0))})' for r in concepts[:8]]
        prompt_parts.append(f"\n**热门概念**: {', '.join(items)}")

    industries = data.get("热门行业", [])
    if industries:
        items = [f'{r["concept_name"]}({_fmt_pct(float(r.get("change_pct") or 0))})' for r in industries[:6]]
        prompt_parts.append(f"**热门行业**: {', '.join(items)}")

    hot_stocks = data.get("热门个股", [])
    if hot_stocks:
        items = []
        for r in hot_stocks[:10]:
            chg = r.get("change_pct") or 0
            name = r.get("short_name", "") or r.get("stock_code", "")
            tag = r.get("pop_tag", "") or r.get("concept_tag", "") or ""
            items.append(f'{name}({_fmt_pct(float(chg))}){(" "+tag) if tag else ""}')
        prompt_parts.append(f"\n**融合热门个股TOP10**: {', '.join(items)}")

    lhb = data.get("龙虎榜", [])
    if lhb:
        items = [f'{r.get("short_name","")}(净{_fmt_amt(float(r.get("a_net_amount") or 0))})' for r in lhb[:6]]
        prompt_parts.append(f"\n**龙虎榜**: {', '.join(items)}")

    zt = data.get("涨停板", [])
    dt2 = data.get("跌停板", [])
    if zt:
        names = [r.get("short_name", "") or r["stock_code"] for r in zt[:5]]
        prompt_parts.append(f"\n**涨停板**: {', '.join(names)}")
    if dt2:
        names = [r.get("short_name", "") or r["stock_code"] for r in dt2[:5]]
        prompt_parts.append(f"**跌停板**: {', '.join(names)}")

    prompt_parts.append(f"""
\n## 分析要求
你是资深A股分析师，请基于以上数据，对今日盘面进行全面复盘分析，控制在400字以内，格式如下：

**📌 大盘总评**（一句话总结今日市场特征）
**📈 趋势研判**（判断市场趋势，多空力量对比，短期方向）
**💰 资金信号**（资金流向释放什么信号？主力偏好哪些方向？）
**🔥 板块轮动**（热门板块涨跌逻辑，轮动特征）
**⚠️ 风险提示**（当前最需关注的1-2个风险点）
**🎯 明日看点**（明天重点关注什么）""")

    try:
        resp = httpx.post(
            DEEPSEEK_URL,
            json={"model": settings.deepseek_model, "messages": [
                {"role": "system", "content": "你是资深A股首席分析师，擅长大盘复盘和趋势研判。用简洁专业的语言，给出明确的分析结论。控制在400字以内。"},
                {"role": "user", "content": "\n".join(prompt_parts)},
            ], "temperature": 0.6, "max_tokens": 800},
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=60,
        )
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"]
        log.error("DeepSeek 调用失败: %s", resp.status_code)
        return ""
    except Exception as e:
        log.error("DeepSeek 异常: %s", e)
        return ""


def pro_review_to_wecom(pro_text: str, date_str: str) -> str:
    """将专业复盘Markdown转为企微markdown格式（加颜色标签）"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    date_display = dt.strftime("%m月%d日")

    lines = []
    for line in pro_text.split("\n"):
        # 标题行
        if line.startswith("【") and line.endswith("】"):
            lines.append(f"## 📊 {line}")
        # 数字标题
        elif line and line[0].isdigit() and ". " in line[:4]:
            lines.append(f"\n**{line}**")
        # 列表项
        elif line.startswith("- "):
            content = line[2:]
            # 给关键数据加颜色
            content = _colorize_pro_line(content)
            lines.append(f"> {content}")
        # 空行
        elif not line.strip():
            lines.append("")
        # 其他
        else:
            lines.append(line)

    result = "\n".join(lines)
    result += f'\n\n<font color="comment">数据基于 {date_str} 收盘 · ProBigA 专业复盘 · 仅供参考不构成投资建议</font>'
    return result


def append_research_radar(content: str, engine, date_str: str) -> str:
    if build_research_radar is None or format_radar_markdown is None:
        return content
    radar = build_research_radar(engine, date_str)
    return content.rstrip() + "\n\n" + format_radar_markdown(radar, title="🧭 研报趋势雷达")


def append_decision_radar(content: str, engine, date_str: str) -> str:
    signal = fetch_tech_risk_signal(lambda sql, params=None: _query(engine, sql, params), date_str)
    return append_tech_risk_markdown(content, signal)


def _safe_print(text_value: str) -> None:
    try:
        print(text_value)
    except UnicodeEncodeError:
        sys.stdout.buffer.write((text_value + "\n").encode("utf-8", errors="replace"))


def _colorize_pro_line(content: str) -> str:
    """给专业复盘行中的关键数据加企微颜色标签"""
    import re
    # 涨幅数字 +XX% → 红色
    content = re.sub(r'(\+\d+\.?\d*%)', r'<font color="warning">\1</font>', content)
    # 跌幅数字 -XX% → 绿色
    content = re.sub(r'(-\d+\.?\d*%)', r'<font color="info">\1</font>', content)
    # 温度分数 XX 分 → 蓝色
    content = re.sub(r'(\d+\.?\d*)\s*分', r'<font color="comment">\1分</font>', content)
    # "强势"/"偏强" → 红色, "弱势"/"偏弱" → 绿色
    content = content.replace('"强势"', '<font color="warning">"强势"</font>')
    content = content.replace('"偏强"', '<font color="warning">"偏强"</font>')
    content = content.replace('"弱势"', '<font color="info">"弱势"</font>')
    content = content.replace('"偏弱"', '<font color="info">"偏弱"</font>')
    content = content.replace('"冰点"', '<font color="info">"冰点"</font>')
    content = content.replace('"发酵"', '<font color="warning">"发酵"</font>')
    content = content.replace('"高潮"', '<font color="warning">"高潮"</font>')
    content = content.replace('"退潮"', '<font color="info">"退潮"</font>')
    return content


def main():
    p = argparse.ArgumentParser(description="A股每日晚报")
    p.add_argument("date", nargs="?", help="日期 YYYY-MM-DD，默认最新交易日")
    p.add_argument("--test", action="store_true", help="不推送，仅打印")
    p.add_argument("--legacy", action="store_true", help="使用旧版AI分析模式")
    args = p.parse_args()

    engine = get_engine()

    date_str = resolve_review_trade_date(engine, args.date or "")
    log.info("使用权威闭市交易日: %s", date_str)

    if args.legacy:
        # 旧版只保留为显式诊断入口，不再作为质量门禁失败时的发布回退。
        log.info("采集盘面数据...")
        data = collect_market_data(engine, date_str)
        _assert_legacy_market_data_contract(data, date_str)

        log.info("DeepSeek AI 盘面分析中...")
        ai_analysis = analyze_with_deepseek(data, date_str)
        if ai_analysis:
            log.info("AI 分析 %d 字", len(ai_analysis))

        log.info("生成晚报报告...")
        report = build_report(data, date_str, ai_analysis)
        report = append_decision_radar(report, engine, date_str)
        report = append_research_radar(report, engine, date_str)
    else:
        from biz.review.quant_digest import PUBLISH_READY, generate_quant_digest

        log.info("生成三段式盘后量化复盘...")
        digest = generate_quant_digest(engine, date_str, persist=not args.test)
        _assert_digest_target_contract(digest, date_str)
        if digest.get("publish_status") != PUBLISH_READY:
            quality = digest.get("quality_json") or {}
            reasons = quality.get("errors") or ["未知质量门禁错误"]
            raise RuntimeError("量化复盘未通过发布门禁: " + "；".join(map(str, reasons)))
        report = str(digest.get("compact_review") or "").strip()
        if not report:
            raise RuntimeError("量化复盘通过门禁但正文为空")
        log.info("三段式量化复盘生成完成 (%d 字)", len(report))

    if args.test:
        _safe_print(report)
        return 0

    log.info("推送到企微...")
    push_to_wecom(report, engine=engine)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
