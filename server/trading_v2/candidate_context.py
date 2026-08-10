"""Point-in-time context overlay for V2 stock candidates.

The sector/price model discovers candidates.  This module then applies only
facts that were available at ``decision_at``: exact stock-linked news,
validated announcements, capital flow, financial quality, attention rank and
external markets.  Missing sources are explicit and always score zero.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from biz.market_context.external_market import load_latest_external_market_context


CRITICAL_EVENT_TERMS = (
    "退市风险",
    "强制退市",
    "重大违法",
    "立案调查",
    "财务造假",
    "债务违约",
    "暂停上市",
)
NEGATIVE_EVENT_TERMS = (
    "减持",
    "业绩预减",
    "业绩下修",
    "亏损",
    "诉讼",
    "处罚",
    "问询函",
    "终止",
    "风险提示",
    "股份冻结",
    "质押风险",
    "解禁",
)
POSITIVE_EVENT_TERMS = (
    "增持",
    "回购",
    "业绩预增",
    "扭亏",
    "中标",
    "获批",
    "订单",
    "签订合同",
    "战略合作",
    "超预期",
)
MARKET_RISK_TERMS = (
    "战争升级",
    "大规模制裁",
    "金融危机",
    "市场熔断",
    "黑天鹅",
)
MARKET_SUPPORT_TERMS = (
    "降准",
    "降息",
    "扩大内需",
    "超预期宽松",
    "重大利好",
)


def _number(value: Any, default: float | None = None) -> float | None:
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


def _canonical_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _columns(engine: Engine, table_name: str) -> set[str]:
    try:
        return {
            str(item["name"])
            for item in inspect(engine).get_columns(table_name)
        }
    except Exception:
        return set()


def _rows(
    engine: Engine,
    sql: str,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                text(sql),
                params,
            ).mappings().all()
        ]


def _code_filter(codes: list[str]) -> tuple[str, dict[str, str]]:
    params = {f"code_{index}": code for index, code in enumerate(codes)}
    return (
        ", ".join(f":{key}" for key in params),
        params,
    )


def _classify_text(value: Any) -> dict[str, int]:
    content = str(value or "")
    critical = sum(term in content for term in CRITICAL_EVENT_TERMS)
    negative = sum(term in content for term in NEGATIVE_EVENT_TERMS)
    positive = sum(term in content for term in POSITIVE_EVENT_TERMS)
    return {
        "critical": int(critical),
        "negative": int(negative),
        "positive": int(positive),
    }


def _parse_stock_codes(raw_value: Any) -> set[str]:
    try:
        items = (
            json.loads(raw_value)
            if isinstance(raw_value, str)
            else (raw_value or [])
        )
    except Exception:
        return set()
    codes: set[str] = set()
    for item in items if isinstance(items, list) else []:
        value = (
            item.get("code") or item.get("symbol") or ""
            if isinstance(item, dict)
            else item
        )
        digits = "".join(ch for ch in str(value or "") if ch.isdigit())
        if len(digits) >= 6:
            codes.add(digits[-6:])
    return codes


def _source(
    status: str,
    *,
    row_count: int = 0,
    latest_at: Any = "",
    note: str = "",
    **extra: Any,
) -> dict[str, Any]:
    return {
        "status": status,
        "row_count": int(row_count),
        "latest_at": str(latest_at or "")[:19],
        "note": note,
        **extra,
    }


def _load_flows(
    engine: Engine,
    codes: list[str],
    trade_date: str,
    decision_at: datetime,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    columns = _columns(engine, "sm_stock_capital_flow_daily")
    required = {"stock_code", "trade_date", "main_net_inflow"}
    if not required.issubset(columns):
        return {}, _source("NOT_CONFIGURED", note="缺少个股日资金流表或必要字段")
    placeholders, params = _code_filter(codes)
    params.update(
        {
            "start_date": (
                date.fromisoformat(trade_date) - timedelta(days=20)
            ).isoformat(),
            "trade_date": trade_date,
            "decision_at": decision_at,
        }
    )
    point_in_time = (
        " AND etl_sync_at <= :decision_at"
        if "etl_sync_at" in columns
        else ""
    )
    etl_select = ", etl_sync_at" if "etl_sync_at" in columns else ""
    rows = _rows(
        engine,
        f"""
        SELECT stock_code, trade_date, main_net_inflow{etl_select}
        FROM sm_stock_capital_flow_daily
        WHERE stock_code IN ({placeholders})
          AND trade_date BETWEEN :start_date AND :trade_date
          {point_in_time}
        ORDER BY stock_code, trade_date, {"etl_sync_at," if "etl_sync_at" in columns else ""}
                 main_net_inflow
        """,
        params,
    )
    by_code_date: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        day = str(row.get("trade_date") or "")[:10]
        if code and day:
            by_code_date[code][day] = row
    output: dict[str, dict[str, Any]] = {}
    for code, by_day in by_code_date.items():
        ordered = [by_day[key] for key in sorted(by_day)][-5:]
        values = [float(_number(row.get("main_net_inflow"), 0.0) or 0.0) for row in ordered]
        output[code] = {
            "flow_trade_date": str(ordered[-1].get("trade_date") or "")[:10],
            "main_net_inflow_1d": values[-1],
            "main_net_inflow_3d": sum(values[-3:]),
            "main_net_inflow_5d": sum(values),
            "main_inflow_days_3d": sum(value > 0 for value in values[-3:]),
            "main_outflow_days_3d": sum(value < 0 for value in values[-3:]),
        }
    latest = max(
        (item.get("flow_trade_date") or "" for item in output.values()),
        default="",
    )
    status = "AVAILABLE" if output else "NO_ROWS"
    return output, _source(status, row_count=len(rows), latest_at=latest)


def _load_finance(
    engine: Engine,
    codes: list[str],
    trade_date: str,
    decision_at: datetime,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    columns = _columns(engine, "si_stock_finance")
    if not {"stock_code", "report_date"}.issubset(columns):
        return {}, _source("NOT_CONFIGURED", note="缺少财务表或报告期字段")
    fields = [
        field
        for field in (
            "net_profit_yoy_gr",
            "total_rev_yoy_gr",
            "roe_wtd",
            "gross_margin",
            "asset_liab_ratio",
            "quick_ratio",
            "etl_sync_at",
        )
        if field in columns
    ]
    placeholders, params = _code_filter(codes)
    params.update(
        {
            "trade_date": trade_date,
            "decision_at": decision_at,
        }
    )
    point_in_time = (
        " AND etl_sync_at <= :decision_at"
        if "etl_sync_at" in columns
        else ""
    )
    selected = ", ".join(["stock_code", "report_date", *fields])
    rows = _rows(
        engine,
        f"""
        SELECT {selected}
        FROM si_stock_finance
        WHERE stock_code IN ({placeholders})
          AND report_date <= :trade_date
          {point_in_time}
        ORDER BY stock_code, report_date, {"etl_sync_at" if "etl_sync_at" in columns else "report_date"}
        """,
        params,
    )
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        row["report_date"] = str(row.get("report_date") or "")[:10]
        output[code] = row
    latest = max(
        (item.get("report_date") or "" for item in output.values()),
        default="",
    )
    return output, _source(
        "AVAILABLE" if output else "NO_ROWS",
        row_count=len(rows),
        latest_at=latest,
    )


def _load_hot_rank(
    engine: Engine,
    codes: list[str],
    trade_date: str,
    decision_at: datetime,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    columns = _columns(engine, "st_hot_rank_fused")
    if not {"stock_code", "snapshot_date", "fused_rank"}.issubset(columns):
        return {}, _source("NOT_CONFIGURED", note="融合热度榜未配置")
    placeholders, params = _code_filter(codes)
    params.update({"trade_date": trade_date, "decision_at": decision_at})
    point_in_time = (
        " AND etl_sync_at <= :decision_at"
        if "etl_sync_at" in columns
        else ""
    )
    optional = ", total_score" if "total_score" in columns else ""
    rows = _rows(
        engine,
        f"""
        SELECT stock_code, snapshot_date, fused_rank{optional}
        FROM st_hot_rank_fused
        WHERE stock_code IN ({placeholders})
          AND snapshot_date <= :trade_date
          {point_in_time}
        ORDER BY stock_code, snapshot_date, fused_rank DESC
        """,
        params,
    )
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        row["snapshot_date"] = str(row.get("snapshot_date") or "")[:10]
        output[code] = row
    latest = max(
        (item.get("snapshot_date") or "" for item in output.values()),
        default="",
    )
    return output, _source(
        "AVAILABLE" if output else "NO_ROWS",
        row_count=len(rows),
        latest_at=latest,
    )


def _load_news(
    engine: Engine,
    codes: list[str],
    decision_at: datetime,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, Any],
    dict[str, Any],
]:
    columns = _columns(engine, "st_news_flash")
    if not {"publish_time", "stocks"}.issubset(columns):
        missing = _source("NOT_CONFIGURED", note="快讯表未配置股票关联字段")
        return {}, missing, missing
    title = "title" if "title" in columns else "NULL AS title"
    content = "content" if "content" in columns else "NULL AS content"
    source = "source" if "source" in columns else "'unknown' AS source"
    rows = _rows(
        engine,
        f"""
        SELECT {source}, {title}, {content}, publish_time, stocks
        FROM st_news_flash
        WHERE publish_time BETWEEN :start_at AND :decision_at
        ORDER BY publish_time DESC
        LIMIT 5000
        """,
        {
            "start_at": decision_at - timedelta(days=3),
            "decision_at": decision_at,
        },
    )
    tracked = set(codes)
    output: dict[str, dict[str, Any]] = {}
    market_sources: dict[str, set[str]] = {
        "risk": set(),
        "support": set(),
    }
    market_titles: dict[str, list[str]] = {
        "risk": [],
        "support": [],
    }
    for row in rows:
        linked_codes = _parse_stock_codes(row.get("stocks"))
        body = f"{row.get('title') or ''} {row.get('content') or ''}"[:1000]
        classification = _classify_text(body)
        publish_time = str(row.get("publish_time") or "")[:19]
        title_text = str(row.get("title") or row.get("content") or "")[:100]
        for code in sorted(linked_codes & tracked):
            item = output.setdefault(
                code,
                {
                    "news_count": 0,
                    "news_positive": 0,
                    "news_negative": 0,
                    "news_critical": 0,
                    "latest_news_time": "",
                    "positive_titles": [],
                    "risk_titles": [],
                },
            )
            item["news_count"] += 1
            item["news_positive"] += classification["positive"]
            item["news_negative"] += classification["negative"]
            item["news_critical"] += classification["critical"]
            item["latest_news_time"] = max(
                item["latest_news_time"],
                publish_time,
            )
            if classification["positive"] and len(item["positive_titles"]) < 3:
                item["positive_titles"].append(title_text)
            if (
                classification["negative"] or classification["critical"]
            ) and len(item["risk_titles"]) < 3:
                item["risk_titles"].append(title_text)

        source_name = str(row.get("source") or "unknown")
        for side, terms in (
            ("risk", MARKET_RISK_TERMS),
            ("support", MARKET_SUPPORT_TERMS),
        ):
            if any(term in body for term in terms):
                market_sources[side].add(source_name)
                if len(market_titles[side]) < 3:
                    market_titles[side].append(title_text)
    latest = max(
        (str(row.get("publish_time") or "")[:19] for row in rows),
        default="",
    )
    market_news = {
        "risk_source_count": len(market_sources["risk"]),
        "support_source_count": len(market_sources["support"]),
        "risk_titles": market_titles["risk"],
        "support_titles": market_titles["support"],
    }
    return (
        output,
        _source(
            "AVAILABLE" if rows else "NO_ROWS",
            row_count=len(rows),
            latest_at=latest,
        ),
        market_news,
    )


def _load_notices(
    engine: Engine,
    codes: list[str],
    trade_date: str,
    decision_at: datetime,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    columns = _columns(engine, "si_notice_eastmoney")
    required = {
        "stock_code",
        "notice_date",
        "title",
        "association_validated",
    }
    if not required.issubset(columns):
        return {}, _source(
            "NOT_CONFIGURED",
            note="公告关联核验字段不存在，旧公告不进入策略",
        )
    placeholders, params = _code_filter(codes)
    event_cutoff_date = min(
        decision_at.date(),
        date.today(),
    )
    params.update(
        {
            "start_date": (
                event_cutoff_date - timedelta(days=14)
            ).isoformat(),
            "event_cutoff_date": event_cutoff_date.isoformat(),
            "decision_at": decision_at,
        }
    )
    point_in_time = (
        " AND etl_sync_at <= :decision_at"
        if "etl_sync_at" in columns
        else ""
    )
    sync_columns = _columns(engine, "si_notice_sync_run")
    coverage_codes: set[str] = set()
    if {
        "stock_code",
        "completed_at",
        "status",
    }.issubset(sync_columns):
        coverage_rows = _rows(
            engine,
            f"""
            SELECT DISTINCT stock_code
            FROM si_notice_sync_run
            WHERE stock_code IN ({placeholders})
              AND status = 'SUCCESS'
              AND completed_at <= :decision_at
              AND completed_at >= :coverage_start_at
            """,
            {
                **{
                    key: value
                    for key, value in params.items()
                    if key.startswith("code_")
                },
                "decision_at": decision_at,
                "coverage_start_at": decision_at - timedelta(days=4),
            },
        )
        coverage_codes = {
            str(row.get("stock_code") or "").zfill(6)
            for row in coverage_rows
        }
    rows = _rows(
        engine,
        f"""
        SELECT stock_code, notice_date, title
        FROM si_notice_eastmoney
        WHERE stock_code IN ({placeholders})
          AND association_validated = 1
          AND notice_date BETWEEN :start_date AND :event_cutoff_date
          {point_in_time}
        ORDER BY notice_date DESC
        """,
        params,
    )
    output: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = str(row.get("stock_code") or "").zfill(6)
        classification = _classify_text(row.get("title"))
        item = output.setdefault(
            code,
            {
                "notice_count": 0,
                "notice_positive": 0,
                "notice_negative": 0,
                "notice_critical": 0,
                "latest_notice_date": "",
                "positive_titles": [],
                "risk_titles": [],
            },
        )
        item["notice_count"] += 1
        item["notice_positive"] += classification["positive"]
        item["notice_negative"] += classification["negative"]
        item["notice_critical"] += classification["critical"]
        item["latest_notice_date"] = max(
            item["latest_notice_date"],
            str(row.get("notice_date") or "")[:10],
        )
        title_text = str(row.get("title") or "")[:100]
        if classification["positive"] and len(item["positive_titles"]) < 3:
            item["positive_titles"].append(title_text)
        if (
            classification["negative"] or classification["critical"]
        ) and len(item["risk_titles"]) < 3:
            item["risk_titles"].append(title_text)
    latest = max(
        (item.get("latest_notice_date") or "" for item in output.values()),
        default="",
    )
    coverage_ratio = len(coverage_codes) / max(1, len(codes))
    coverage_status = (
        "AVAILABLE"
        if coverage_ratio >= 0.90
        else ("PARTIAL" if coverage_codes else "NO_COVERAGE")
    )
    return output, _source(
        coverage_status,
        row_count=len(rows),
        latest_at=latest,
        note=(
            "仅使用源接口明确关联到该股票代码的公告；"
            f"近4日成功同步覆盖{len(coverage_codes)}/{len(codes)}只候选"
        ),
        coverage_count=len(coverage_codes),
        expected_count=len(codes),
        coverage_ratio=round(coverage_ratio, 4),
    )


def load_candidate_context(
    engine: Engine,
    *,
    candidates: list[dict[str, Any]],
    trade_date: str,
    decision_at: datetime,
    external_maximum_age_minutes: int = 180,
) -> dict[str, Any]:
    codes = sorted(
        {
            str(item.get("stock_code") or "").zfill(6)
            for item in candidates
            if item.get("stock_code")
        }
    )
    if not codes:
        return {
            "by_code": {},
            "sources": {},
            "market": {},
            "context_hash": _canonical_hash({}),
        }
    flows, flow_source = _load_flows(
        engine, codes, trade_date, decision_at
    )
    finance, finance_source = _load_finance(
        engine, codes, trade_date, decision_at
    )
    hot_rank, hot_source = _load_hot_rank(
        engine, codes, trade_date, decision_at
    )
    news, news_source, market_news = _load_news(
        engine, codes, decision_at
    )
    notices, notice_source = _load_notices(
        engine, codes, trade_date, decision_at
    )
    external = load_latest_external_market_context(
        engine,
        as_of=decision_at,
    )
    captured_at = str(
        external.get("external_market_captured_at") or ""
    )[:19]
    try:
        captured_dt = datetime.fromisoformat(captured_at)
    except (TypeError, ValueError):
        captured_dt = None
    if (
        captured_dt is not None
        and decision_at >= captured_dt
        and decision_at - captured_dt
        > timedelta(minutes=max(1, int(external_maximum_age_minutes)))
    ):
        original_quality = str(
            external.get("external_market_data_quality") or "UNKNOWN"
        )
        external["external_market_data_quality"] = "STALE"
        external["external_market_reason"] = (
            f"{external.get('external_market_reason') or ''}；"
            f"快照已超过{max(1, int(external_maximum_age_minutes))}分钟"
            f"（原质量{original_quality}），本次不计分"
        ).strip("；")
    external_source = _source(
        str(external.get("external_market_data_quality") or "UNKNOWN"),
        row_count=1 if external.get("external_market_captured_at") else 0,
        latest_at=external.get("external_market_captured_at"),
        note=str(external.get("external_market_reason") or ""),
    )
    by_code = {
        code: {
            "capital_flow": flows.get(code) or {},
            "finance": finance.get(code) or {},
            "hot_rank": hot_rank.get(code) or {},
            "news": news.get(code) or {},
            "notice": notices.get(code) or {},
        }
        for code in codes
    }
    sources = {
        "qmt_price_sector": _source(
            "AVAILABLE",
            note="由板块预热主模型提供，是入池前提",
        ),
        "capital_flow": flow_source,
        "financial_quality": finance_source,
        "market_attention": hot_source,
        "linked_news": news_source,
        "validated_notice": notice_source,
        "external_market": external_source,
        "northbound_flow": _source(
            "NOT_CONFIGURED",
            note="生产表无可用记录，不参与评分",
        ),
        "structured_macro": _source(
            "NOT_CONFIGURED",
            note="尚无带发布时间和修订时间的结构化宏观序列",
        ),
        "research_consensus": _source(
            "NOT_CONFIGURED",
            note="尚无可回放的研报一致预期快照",
        ),
        "etf_fund_flow": _source(
            "NOT_CONFIGURED",
            note="股票候选层尚无可靠 ETF 申赎/份额资金流",
        ),
    }
    payload = {
        "by_code": by_code,
        "sources": sources,
        "market": {
            "external": external,
            "market_news": market_news,
        },
    }
    payload["context_hash"] = _canonical_hash(payload)
    return payload


def _event_adjustment(
    item: dict[str, Any],
) -> tuple[float, bool, list[str]]:
    positive = int(item.get("positive") or 0)
    negative = int(item.get("negative") or 0)
    critical = int(item.get("critical") or 0)
    adjustment = min(2.0, positive * 1.0) - min(5.0, negative * 2.0)
    reasons: list[str] = []
    if positive:
        reasons.append(f"正向{positive}条")
    if negative:
        reasons.append(f"负向{negative}条")
    if critical:
        reasons.append(f"重大风险{critical}条")
    return adjustment, critical > 0, reasons


def apply_candidate_context(
    snapshot: dict[str, Any],
    *,
    engine: Engine,
    trade_date: str,
    decision_at: datetime,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Apply bounded context adjustments without promoting weak price setups."""
    result = dict(snapshot)
    candidates = [
        dict(item)
        for item in (snapshot.get("candidates") or [])
    ]
    overlay = dict(config.get("context_overlay") or {})
    if not bool(overlay.get("enabled", True)) or not candidates:
        return result
    context = load_candidate_context(
        engine,
        candidates=candidates,
        trade_date=trade_date,
        decision_at=decision_at,
        external_maximum_age_minutes=int(
            overlay.get("external_market_maximum_age_minutes", 180)
        ),
    )
    external = dict((context.get("market") or {}).get("external") or {})
    market_news = dict(
        (context.get("market") or {}).get("market_news") or {}
    )
    external_quality = str(
        external.get("external_market_data_quality") or "UNKNOWN"
    ).upper()
    external_status = str(
        external.get("external_market_status") or "UNKNOWN"
    ).upper()
    external_adjustment = 0.0
    if external_quality in {"PASS", "WATCH"}:
        external_adjustment = {
            "SUPPORT": 2.0 if external_quality == "PASS" else 1.0,
            "RISK": -3.0 if external_quality == "PASS" else -2.0,
        }.get(external_status, 0.0)
    market_news_adjustment = 0.0
    if int(market_news.get("risk_source_count") or 0) >= 2:
        market_news_adjustment -= 3.0
    if int(market_news.get("support_source_count") or 0) >= 2:
        market_news_adjustment += 2.0

    maximum_positive = float(overlay.get("maximum_positive_adjustment", 8.0))
    maximum_negative = float(overlay.get("maximum_negative_adjustment", -12.0))
    by_code = context.get("by_code") or {}
    adjusted: list[dict[str, Any]] = []
    for signal in candidates:
        code = str(signal.get("stock_code") or "").zfill(6)
        facts = dict(by_code.get(code) or {})
        components = {
            "external_market": external_adjustment,
            "market_news": market_news_adjustment,
            "capital_flow": 0.0,
            "financial_quality": 0.0,
            "linked_news": 0.0,
            "validated_notice": 0.0,
            "market_attention": 0.0,
        }
        summaries: list[str] = []
        evidence = list(signal.get("evidence_chain") or [])
        hard_block = False
        downgrade = False
        block_titles: list[str] = []

        if external_status != "UNKNOWN":
            summaries.append(
                f"外围{external_status}/{external_quality}"
            )
        if market_news_adjustment:
            summaries.append(
                f"大盘消息{market_news_adjustment:+.1f}分"
            )
        evidence.append(
            {
                "module": "external_market",
                "text": str(
                    external.get("external_market_reason")
                    or "外围数据不可用，不计分"
                ),
                "source": "st_external_market_context",
                "status": external_quality,
                "score_adjustment": external_adjustment,
                "captured_at": external.get(
                    "external_market_captured_at"
                ),
            }
        )

        evidence.append(
            {
                "module": "market_news",
                "text": (
                    "大盘级消息至少需要两个独立来源交叉确认；"
                    f"风险来源{int(market_news.get('risk_source_count') or 0)}个，"
                    f"支持来源{int(market_news.get('support_source_count') or 0)}个"
                ),
                "source": "st_news_flash",
                "status": "AVAILABLE",
                "score_adjustment": market_news_adjustment,
                "risk_titles": market_news.get("risk_titles") or [],
                "support_titles": market_news.get("support_titles") or [],
            }
        )

        flow = dict(facts.get("capital_flow") or {})
        amount = float(_number(signal.get("candidate_amount_cny"), 0.0) or 0.0)
        flow_3d = float(_number(flow.get("main_net_inflow_3d"), 0.0) or 0.0)
        flow_ratio = (
            flow_3d / (amount * 3.0) * 100.0
            if amount > 0
            else 0.0
        )
        if flow:
            if flow_ratio >= 5.0 and int(flow.get("main_inflow_days_3d") or 0) >= 2:
                components["capital_flow"] = 4.0
            elif flow_ratio >= 2.0:
                components["capital_flow"] = 2.0
            elif flow_ratio <= -5.0 and int(flow.get("main_outflow_days_3d") or 0) >= 2:
                components["capital_flow"] = -5.0
                downgrade = True
            elif flow_ratio <= -2.0:
                components["capital_flow"] = -2.0
            summaries.append(f"3日主力{flow_3d / 1e8:+.2f}亿")
        evidence.append(
            {
                "module": "capital_flow",
                "text": (
                    f"3日主力净流入{flow_3d / 1e8:+.2f}亿元，"
                    f"占近似3日成交额{flow_ratio:+.2f}%"
                    if flow
                    else "没有时点可用的个股资金流记录，不计分"
                ),
                "source": "sm_stock_capital_flow_daily",
                "status": "AVAILABLE" if flow else "UNKNOWN",
                "score_adjustment": components["capital_flow"],
                "data_date": flow.get("flow_trade_date"),
            }
        )

        finance = dict(facts.get("finance") or {})
        profit_growth = _number(finance.get("net_profit_yoy_gr"))
        roe = _number(finance.get("roe_wtd"))
        debt = _number(finance.get("asset_liab_ratio"))
        finance_penalty = 0.0
        if profit_growth is not None and profit_growth <= -50.0:
            finance_penalty -= 3.0
        if debt is not None and debt >= 85.0:
            finance_penalty -= 2.0
        if roe is not None and roe < 0.0:
            finance_penalty -= 1.0
        if finance_penalty:
            components["financial_quality"] = max(-4.0, finance_penalty)
            downgrade = components["financial_quality"] <= -3.0
        elif (
            profit_growth is not None
            and profit_growth >= 20.0
            and roe is not None
            and roe >= 8.0
        ):
            components["financial_quality"] = 1.0
        if finance:
            summaries.append(
                "财务"
                + (
                    "承压"
                    if components["financial_quality"] < 0
                    else "稳健"
                )
            )
        evidence.append(
            {
                "module": "financial_quality",
                "text": (
                    f"净利润同比{profit_growth if profit_growth is not None else '未知'}%，"
                    f"ROE {roe if roe is not None else '未知'}%，"
                    f"资产负债率{debt if debt is not None else '未知'}%"
                    if finance
                    else "没有时点可用的最新财务记录，不计分"
                ),
                "source": "si_stock_finance",
                "status": "AVAILABLE" if finance else "UNKNOWN",
                "score_adjustment": components["financial_quality"],
                "report_date": finance.get("report_date"),
            }
        )

        for key, module, source_name, label in (
            ("news", "linked_news", "st_news_flash", "个股消息"),
            (
                "notice",
                "validated_notice",
                "si_notice_eastmoney",
                "核验公告",
            ),
        ):
            event = dict(facts.get(key) or {})
            prefix = "news" if key == "news" else "notice"
            normalized = {
                "positive": event.get(f"{prefix}_positive"),
                "negative": event.get(f"{prefix}_negative"),
                "critical": event.get(f"{prefix}_critical"),
            }
            event_adjustment, event_block, event_reasons = (
                _event_adjustment(normalized)
            )
            components[module] = event_adjustment
            hard_block = hard_block or event_block
            if event_reasons:
                summaries.append(f"{label}{'/'.join(event_reasons)}")
            if int(normalized.get("negative") or 0) >= 2:
                downgrade = True
            block_titles.extend(event.get("risk_titles") or [])
            evidence.append(
                {
                    "module": module,
                    "text": (
                        f"{label}：{'，'.join(event_reasons)}"
                        if event_reasons
                        else f"{label}没有识别到明确利好或风险，不计分"
                    ),
                    "source": source_name,
                    "status": "AVAILABLE" if event else "NO_LINKED_ROWS",
                    "score_adjustment": event_adjustment,
                    "latest_at": event.get(
                        "latest_news_time"
                        if key == "news"
                        else "latest_notice_date"
                    ),
                    "positive_titles": event.get("positive_titles") or [],
                    "risk_titles": event.get("risk_titles") or [],
                }
            )

        hot = dict(facts.get("hot_rank") or {})
        rank = int(_number(hot.get("fused_rank"), 0.0) or 0)
        if 0 < rank <= 20:
            components["market_attention"] = 2.0
        elif 0 < rank <= 50:
            components["market_attention"] = 1.0
        if rank:
            summaries.append(f"热度榜第{rank}")
        evidence.append(
            {
                "module": "market_attention",
                "text": (
                    f"融合热度榜第{rank}名"
                    if rank
                    else "未进入可用融合热度榜，不计分"
                ),
                "source": "st_hot_rank_fused",
                "status": "AVAILABLE" if rank else "UNKNOWN",
                "score_adjustment": components["market_attention"],
                "snapshot_date": hot.get("snapshot_date"),
            }
        )

        adjustment = max(
            maximum_negative,
            min(maximum_positive, sum(components.values())),
        )
        base_score = float(_number(signal.get("raw_score"), 0.0) or 0.0)
        signal["context_base_score"] = round(base_score, 2)
        signal["context_adjustment"] = round(adjustment, 2)
        signal["context_components"] = {
            key: round(value, 2) for key, value in components.items()
        }
        signal["raw_score"] = round(
            max(0.0, min(100.0, base_score + adjustment)),
            2,
        )
        signal["effective_score"] = signal["raw_score"]
        signal["model_confidence"] = signal["raw_score"]
        signal["context_summary"] = summaries or [
            "上下文源没有可用个股记录，保持技术面原分"
        ]
        signal["context_sources"] = context.get("sources") or {}
        signal["context_hash"] = context.get("context_hash")
        if hard_block:
            signal["signal_direction"] = "HOLD"
            signal["signal_status"] = "BLOCKED"
            signal["gate_status"] = "BLOCK"
            signal["risk_level"] = "HIGH"
            signal["gate_reason"] = (
                "个股存在已核验重大消息风险，禁止新开仓"
                + (
                    f"：{'；'.join(block_titles[:2])}"
                    if block_titles
                    else ""
                )
            )
            signal["today_signal"] = signal["gate_reason"]
        elif (
            signal.get("signal_status") == "READY"
            and (downgrade or adjustment <= -5.0)
        ):
            signal["signal_status"] = "WATCH"
            signal["gate_status"] = "REDUCE"
            signal["gate_reason"] = (
                f"{signal.get('gate_reason') or ''}；"
                "消息、资金或财务上下文出现明显风险，降级为观察"
            ).strip("；")
            signal["today_signal"] = signal["gate_reason"]
        signal["evidence_chain"] = evidence
        adjusted.append(signal)

    execution_codes = {
        str(item.get("stock_code") or "")
        for item in snapshot.get("execution_candidates") or []
    }
    status_order = {"READY": 0, "WATCH": 1, "BLOCKED": 2}
    execution_adjusted = [
        item for item in adjusted if item.get("stock_code") in execution_codes
    ]
    discovery_adjusted = [
        item for item in adjusted if item.get("stock_code") not in execution_codes
    ]
    execution_adjusted.sort(
        key=lambda item: (
            status_order.get(str(item.get("signal_status") or ""), 3),
            -float(_number(item.get("raw_score"), 0.0) or 0.0),
            str(item.get("stock_code") or ""),
        )
    )
    discovery_adjusted.sort(
        key=lambda item: (
            status_order.get(str(item.get("signal_status") or ""), 3),
            -float(_number(item.get("raw_score"), 0.0) or 0.0),
            str(item.get("stock_code") or ""),
        )
    )
    result["execution_candidates"] = execution_adjusted
    result["discovery_candidates"] = discovery_adjusted
    result["candidates"] = execution_adjusted + discovery_adjusted
    result["ready_count"] = sum(
        item.get("signal_status") == "READY"
        for item in result["execution_candidates"]
    )
    result["context_sources"] = context.get("sources") or {}
    result["context_hash"] = context.get("context_hash")
    result["context_applied_count"] = len(adjusted)
    return result
