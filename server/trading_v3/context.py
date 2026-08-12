from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import date, datetime, time, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


POSITIVE_POLICY_WORDS = (
    "支持",
    "促进",
    "加快",
    "专项资金",
    "降准",
    "降息",
    "回购",
    "增持",
    "扩内需",
    "稳增长",
    "设备更新",
)
LIQUIDITY_WORDS = (
    "降准",
    "降息",
    "逆回购",
    "流动性",
    "公开市场操作",
    "中期借贷便利",
)
RISK_WORDS = (
    "制裁",
    "冲突",
    "加征关税",
    "暴跌",
    "熔断",
    "立案",
    "退市",
    "地缘风险",
    "流动性风险",
    "下调评级",
    "违约",
)
OVERSEAS_WORDS = (
    "美股",
    "纳指",
    "标普",
    "道指",
    "日经",
    "恒生",
    "欧洲股市",
    "美元指数",
    "美债",
    "美联储",
    "英伟达",
)
NEGATIONS = (
    "不支持",
    "未支持",
    "否认",
    "不属实",
    "辟谣",
    "取消",
    "终止",
)
EVENT_TAXONOMY = {
    "POLICY_SUPPORT": POSITIVE_POLICY_WORDS,
    "LIQUIDITY": LIQUIDITY_WORDS,
    "GEOPOLITICAL_RISK": ("冲突", "制裁", "地缘", "战争"),
    "TRADE_FRICTION": ("关税", "贸易摩擦", "出口管制", "实体清单"),
    "MARKET_STRESS": ("暴跌", "熔断", "流动性风险", "违约"),
    "REGULATORY_RISK": ("立案", "退市", "处罚", "问询函"),
    "OVERSEAS_MARKET": OVERSEAS_WORDS,
}
THEME_TAXONOMY = {
    "电力与电网": ("电网", "电力", "特高压", "储能", "电改"),
    "创新药": ("创新药", "新药", "临床试验", "医药", "医保"),
    "光通信与算力": (
        "光模块",
        "光通信",
        "CPO",
        "算力",
        "数据中心",
        "英伟达",
    ),
    "半导体与电子": ("半导体", "芯片", "电子元件", "PCB", "封装"),
    "商业航天": ("商业航天", "卫星", "火箭", "低轨"),
    "贵金属": ("黄金", "白银", "贵金属"),
    "化工": ("化工", "氟化工", "锂盐", "农药", "纯碱"),
    "锂产业链": (
        "锂产业链",
        "锂电",
        "锂矿",
        "锂资源",
        "锂盐",
        "碳酸锂",
        "氢氧化锂",
    ),
    "人工智能": (
        "人工智能",
        "AI应用",
        "AIGC",
        "大模型",
        "智能体",
        "生成式AI",
    ),
    "机器人": (
        "机器人",
        "具身智能",
        "人形机器人",
        "机器视觉",
        "工业自动化",
        "伺服系统",
        "减速器",
    ),
}


def theme_context_score(
    theme_label: str,
    scores: dict[str, Any],
) -> float:
    """Map a business theme/concept label to structured news scores."""
    normalized = _normalize_text(theme_label)
    if not normalized:
        return 0.0
    matched = []
    for category, words in THEME_TAXONOMY.items():
        if (
            _normalize_text(category) in normalized
            or any(
                _normalize_text(word) in normalized
                for word in words
            )
        ):
            matched.append(float(scores.get(category) or 0.0))
    if not matched:
        return 0.0
    return max(-1.0, min(1.0, max(matched, key=abs)))


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "")).lower()


def _source_reliability(source: Any, content: str) -> float:
    value = f"{source or ''} {content}"
    if any(
        name in value
        for name in (
            "国务院",
            "证监会",
            "财政部",
            "人民银行",
            "国家发改委",
            "上交所",
            "深交所",
            "新华社",
        )
    ):
        return 1.0
    if any(
        name in value
        for name in (
            "证券时报",
            "中国证券报",
            "上海证券报",
            "财联社",
        )
    ):
        return 0.88
    return 0.68


def _freshness_weight(
    publish_time: Any,
    cutoff: datetime,
) -> float:
    if not isinstance(publish_time, datetime):
        return 0.55
    age_hours = max(
        0.0,
        (cutoff - publish_time).total_seconds() / 3600.0,
    )
    return max(0.18, math.pow(0.5, age_hours / 18.0))


def _importance_weight(row: dict[str, Any]) -> float:
    weight = 1.0
    if int(row.get("is_top") or 0) == 1:
        weight += 0.35
    if int(row.get("jpush") or 0) == 1:
        weight += 0.25
    level = str(row.get("level") or "").upper()
    if level in {"A", "1", "IMPORTANT", "HIGH"}:
        weight += 0.25
    return weight


def _event_types(content: str) -> list[str]:
    return [
        event_type
        for event_type, words in EVENT_TAXONOMY.items()
        if any(word.lower() in content for word in words)
    ]


def _theme_types(
    content: str,
    dynamic_aliases: Mapping[str, Iterable[str]] | None = None,
) -> list[str]:
    matched = {
        theme
        for theme, words in THEME_TAXONOMY.items()
        if any(word.lower() in content for word in words)
    }
    for cluster_key, aliases in (dynamic_aliases or {}).items():
        if any(str(alias) in content for alias in aliases):
            matched.add(str(cluster_key))
    return sorted(matched)


def _event_sentiment(
    normalized_content: str,
    event_types: list[str],
) -> float:
    positive_hits = sum(
        word.lower() in normalized_content
        for word in POSITIVE_POLICY_WORDS
    )
    risk_hits = sum(
        word.lower() in normalized_content
        for word in RISK_WORDS
    )
    negated = any(
        word.lower() in normalized_content for word in NEGATIONS
    )
    score = min(1.0, positive_hits * 0.45) - min(
        1.0,
        risk_hits * 0.55,
    )
    if negated and score > 0:
        score *= -0.65
    if {
        "GEOPOLITICAL_RISK",
        "TRADE_FRICTION",
        "MARKET_STRESS",
        "REGULATORY_RISK",
    } & set(event_types):
        score = min(score, -0.45)
    return max(-1.0, min(1.0, score))


def load_asof_context(
    engine: Engine,
    *,
    as_of: date,
    cutoff_at: datetime | None = None,
    lookback_hours: int = 36,
    theme_aliases: Mapping[str, Iterable[str]] | None = None,
) -> dict[str, Any]:
    """Build deterministic, strictly as-of structured news context."""
    cutoff = cutoff_at or datetime.combine(as_of, time.max)
    start = cutoff - timedelta(hours=lookback_hours)
    normalized_theme_aliases = {
        str(cluster_key): tuple(sorted({
            normalized
            for alias in aliases
            if (
                len(normalized := _normalize_text(alias)) >= 2
                and not normalized.replace(".", "").isdigit()
            )
        }))
        for cluster_key, aliases in (theme_aliases or {}).items()
    }
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT source, title, content, publish_time,
                       first_seen_at, level, is_top, jpush
                FROM st_news_flash
                WHERE publish_time > :start
                  AND publish_time <= :cutoff
                  AND first_seen_at <= :cutoff
                ORDER BY publish_time DESC, id DESC
                LIMIT 4000
                """
            ),
            {"start": start, "cutoff": cutoff},
        ).mappings().all()
    if not rows:
        empty_hash = hashlib.sha256(
            f"{start.isoformat()}|{cutoff.isoformat()}|EMPTY".encode(
                "utf-8"
            )
        ).hexdigest()
        return {
            "policy_support_score": 0.0,
            "news_risk_score": 0.0,
            "overseas_risk_score": None,
            "context_quality_status": "PARTIAL",
            "context_evidence": [
                f"截至{cutoff:%Y-%m-%d %H:%M}，近{lookback_hours}小时没有可用新闻快讯"
            ],
            "context_news_count": 0,
            "context_unique_event_count": 0,
            "overseas_news_count": 0,
            "context_events": [],
            "context_theme_scores": {},
            "context_theme_novelty": {},
            "context_cutoff_at": cutoff.isoformat(sep=" "),
            "context_hash": empty_hash,
            "context_model_version": "structured_context_v3.1.0",
            "context_evidence_status": "POINT_IN_TIME_VERIFIED",
            "context_knowledge_time_column": "first_seen_at",
        }

    seen: set[str] = set()
    events: list[dict[str, Any]] = []
    theme_impacts: dict[str, list[float]] = defaultdict(list)
    for raw_row in rows:
        row = dict(raw_row)
        title = str(row.get("title") or "").strip()
        raw_content = (
            title + " " + str(row.get("content") or "")[:1000]
        )
        normalized = _normalize_text(raw_content)
        if not normalized:
            continue
        dedupe_key = hashlib.sha256(
            normalized[:280].encode("utf-8")
        ).hexdigest()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        event_types = _event_types(normalized)
        themes = _theme_types(normalized, normalized_theme_aliases)
        if not event_types and not themes:
            continue
        reliability = _source_reliability(
            row.get("source"),
            raw_content,
        )
        freshness = _freshness_weight(
            row.get("publish_time"),
            cutoff,
        )
        importance = _importance_weight(row)
        sentiment = _event_sentiment(normalized, event_types)
        weight = reliability * freshness * importance
        for theme in themes:
            theme_impacts[theme].append(sentiment * weight)
        events.append(
            {
                "event_id": dedupe_key[:16],
                "publish_time": (
                    row["publish_time"].isoformat(sep=" ")
                    if isinstance(row.get("publish_time"), datetime)
                    else str(row.get("publish_time") or "")
                ),
                "source": str(row.get("source") or ""),
                "title": title[:180],
                "event_types": event_types,
                "themes": themes,
                "sentiment": round(sentiment, 6),
                "reliability": round(reliability, 6),
                "freshness": round(freshness, 6),
                "importance": round(importance, 6),
                "evidence_weight": round(weight, 6),
                "weighted_impact": round(sentiment * weight, 6),
            }
        )
    events.sort(
        key=lambda item: (
            -abs(float(item["weighted_impact"])),
            str(item["publish_time"]),
        )
    )
    policy_impacts = sorted(
        (
            max(0.0, float(item["weighted_impact"]))
            for item in events
            if "POLICY_SUPPORT" in item["event_types"]
        ),
        reverse=True,
    )[:8]
    risk_impacts = sorted(
        (
            max(0.0, -float(item["weighted_impact"]))
            for item in events
        ),
        reverse=True,
    )[:8]
    positive_mass = sum(policy_impacts)
    risk_mass = sum(risk_impacts)
    policy_score = 1.0 - math.exp(-positive_mass / 8.0)
    risk_score = 1.0 - math.exp(-risk_mass / 8.0)
    overseas_events = [
        item
        for item in events
        if "OVERSEAS_MARKET" in item["event_types"]
    ][:8]
    overseas_mass = sum(
        float(item["evidence_weight"])
        for item in overseas_events
    )
    overseas_risk_mass = sum(
        max(0.0, -float(item["weighted_impact"]))
        for item in overseas_events
    )
    overseas_score = (
        min(1.0, overseas_risk_mass / overseas_mass)
        if overseas_mass > 0
        else None
    )
    normalized_theme_scores = {
        theme: round(
            math.tanh(
                sum(
                    sorted(
                        impacts,
                        key=abs,
                        reverse=True,
                    )[:5]
                )
                / 5.0
            ),
            6,
        )
        for theme, impacts in sorted(theme_impacts.items())
    }
    theme_novelty_mass: dict[str, float] = defaultdict(float)
    for item in events:
        novelty_weight = (
            float(item["freshness"])
            * float(item["reliability"])
            * float(item["importance"])
        )
        for theme in item["themes"]:
            theme_novelty_mass[str(theme)] += novelty_weight
    normalized_theme_novelty = {
        theme: round(1.0 - math.exp(-mass / 3.0), 6)
        for theme, mass in sorted(theme_novelty_mass.items())
    }
    hash_payload = {
        "cutoff_at": cutoff.isoformat(sep=" "),
        "events": events,
        "theme_scores": normalized_theme_scores,
        "theme_novelty": normalized_theme_novelty,
    }
    context_hash = hashlib.sha256(
        json.dumps(
            hash_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    evidence = [
        item["title"]
        for item in events[:8]
        if item["title"]
    ]
    return {
        "policy_support_score": round(policy_score, 6),
        "news_risk_score": round(risk_score, 6),
        "overseas_risk_score": (
            round(overseas_score, 6)
            if overseas_score is not None
            else None
        ),
        "context_quality_status": (
            "PASS" if events and overseas_mass > 0 else "PARTIAL"
        ),
        "context_evidence": evidence,
        "context_news_count": len(rows),
        "context_unique_event_count": len(events),
        "overseas_news_count": sum(
            "OVERSEAS_MARKET" in item["event_types"]
            for item in events
        ),
        "context_events": events[:30],
        "context_theme_scores": normalized_theme_scores,
        "context_theme_novelty": normalized_theme_novelty,
        "context_cutoff_at": cutoff.isoformat(sep=" "),
        "context_hash": context_hash,
        "context_model_version": "structured_context_v3.1.0",
        "context_evidence_status": "POINT_IN_TIME_VERIFIED",
        "context_knowledge_time_column": "first_seen_at",
    }
