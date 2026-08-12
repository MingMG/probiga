from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd


Membership = tuple[str, str, str]


THEME_COMPOSITE_WEIGHTS = {
    "advance_breadth": 0.23,
    "capital_acceleration": 0.20,
    "relative_strength": 0.22,
    "news_novelty": 0.15,
    "topk_member_median": 0.20,
}


THEME_CLUSTER_TAXONOMY = {
    "AI_APPLICATION": {
        "canonical_label": "AI应用",
        "keywords": (
            "AI应用",
            "人工智能",
            "AIGC",
            "CHATGPT",
            "大模型",
            "生成式AI",
            "多模态",
            "智能体",
        ),
    },
    "ROBOTICS": {
        "canonical_label": "机器人",
        "keywords": (
            "机器人",
            "智能机器",
            "机器视觉",
            "具身智能",
            "工业自动化",
            "人形机器人",
        ),
    },
    "CLOUD_COMPUTING": {
        "canonical_label": "云计算",
        "keywords": ("云计算", "云服务", "云办公", "云原生", "SaaS"),
    },
    "DOMESTIC_SOFTWARE": {
        "canonical_label": "国产软件",
        "keywords": ("国产软件", "信创", "操作系统", "数据库", "工业软件"),
    },
    "COMPUTING_INFRASTRUCTURE": {
        "canonical_label": "算力基础设施",
        "keywords": ("算力", "数据中心", "IDC", "液冷", "光模块", "CPO"),
    },
    "SEMICONDUCTOR": {
        "canonical_label": "半导体",
        "keywords": ("半导体", "芯片", "集成电路", "先进封装", "光刻机"),
    },
    "DATA_ECONOMY": {
        "canonical_label": "数据要素",
        "keywords": ("数据要素", "数字经济", "数据确权", "数据安全"),
    },
    "DIGITAL_MEDIA": {
        "canonical_label": "数字传媒",
        "keywords": ("传媒", "短剧", "影视", "IP经济", "知识产权"),
    },
    "GAMING": {
        "canonical_label": "游戏",
        "keywords": ("游戏", "网络游戏", "云游戏", "电竞"),
    },
    "INTERNET_COMMERCE": {
        "canonical_label": "互联网商业",
        "keywords": ("电商", "互联网营销", "网红经济", "直播带货", "在线教育"),
    },
    "CONSUMER": {
        "canonical_label": "消费",
        "keywords": ("消费", "食品饮料", "白酒", "乳业", "零售", "旅游"),
    },
    "AUTOMOTIVE": {
        "canonical_label": "汽车产业链",
        "keywords": ("汽车", "智能驾驶", "无人驾驶", "汽车零部件"),
    },
    "NEW_ENERGY_VEHICLE": {
        "canonical_label": "新能源汽车",
        "keywords": ("新能源汽车", "新能源车", "充电桩"),
    },
    "BATTERY": {
        "canonical_label": "电池",
        "keywords": ("电池", "锂电", "固态电池", "钠离子电池"),
        "name_markers": ("电池",),
    },
    "LITHIUM": {
        "canonical_label": "锂产业链",
        "keywords": (
            "锂产业链",
            "锂电原料",
            "锂矿",
            "锂资源",
            "锂盐",
            "碳酸锂",
        ),
        "name_markers": ("锂",),
    },
    "PHOTOVOLTAIC": {
        "canonical_label": "光伏",
        "keywords": ("光伏", "太阳能", "HJT", "TOPCON"),
        "name_markers": ("光伏",),
    },
    "ENERGY_STORAGE": {
        "canonical_label": "储能",
        "keywords": ("储能", "抽水蓄能"),
    },
    "POWER_GRID": {
        "canonical_label": "电网电力",
        "keywords": ("电网", "电力", "特高压", "智能电网", "虚拟电厂"),
    },
    "LOW_ALTITUDE_ECONOMY": {
        "canonical_label": "低空经济",
        "keywords": ("低空经济", "飞行汽车", "eVTOL", "无人机"),
    },
    "AEROSPACE": {
        "canonical_label": "商业航天",
        "keywords": ("商业航天", "卫星", "火箭", "低轨互联网"),
    },
    "DEFENSE": {
        "canonical_label": "国防军工",
        "keywords": ("军工", "国防", "军民融合", "航空装备"),
    },
    "INNOVATIVE_DRUG": {
        "canonical_label": "创新药",
        "keywords": ("创新药", "新药", "医药", "CXO"),
    },
    "MEDICAL_DEVICE": {
        "canonical_label": "医疗器械",
        "keywords": ("医疗器械", "医疗设备", "体外诊断"),
    },
    "FINANCE": {
        "canonical_label": "大金融",
        "keywords": ("银行", "证券", "保险", "多元金融"),
    },
    "REAL_ESTATE": {
        "canonical_label": "地产产业链",
        "keywords": ("房地产", "地产", "物业管理", "家居建材"),
    },
    "AGRICULTURE": {
        "canonical_label": "农业",
        "keywords": ("农业", "种业", "养殖", "粮食", "乡村振兴"),
    },
    "CHEMICAL": {
        "canonical_label": "化工",
        "keywords": ("化工", "氟化工", "磷化工", "农药", "纯碱"),
    },
    "NONFERROUS_METAL": {
        "canonical_label": "有色金属",
        "keywords": ("有色", "稀土", "铜", "铝", "小金属"),
    },
    "PRECIOUS_METAL": {
        "canonical_label": "贵金属",
        "keywords": ("黄金", "白银", "贵金属"),
        "name_markers": ("黄金",),
    },
}


_THEME_SUFFIXES = ("概念指数", "行业指数", "主题指数", "概念", "板块", "主题")


def normalize_theme_label(value: Any) -> str:
    """Return a stable label used only for taxonomy comparison.

    Display labels are retained separately.  Normalization removes cosmetic
    punctuation and provider suffixes, so aliases from different data vendors
    can be clustered without merging unrelated AI and robotics families.
    """

    normalized = unicodedata.normalize("NFKC", str(value or "")).upper()
    normalized = re.sub(r"[\s\-_/·•:：,，.。()（）\[\]【】]+", "", normalized)
    for suffix in _THEME_SUFFIXES:
        if normalized.endswith(suffix) and len(normalized) > len(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return normalized


def _theme_cluster_matches(label: Any) -> tuple[str, ...]:
    normalized = normalize_theme_label(label)
    if not normalized:
        return ()
    matches = []
    for cluster_key, settings in THEME_CLUSTER_TAXONOMY.items():
        keywords = settings["keywords"]
        if any(
            normalize_theme_label(keyword) in normalized
            for keyword in keywords
        ):
            matches.append(cluster_key)
    return tuple(matches)


def cluster_theme_labels(labels: Iterable[Any]) -> list[dict[str, Any]]:
    """Cluster synonymous labels while preserving distinct theme families.

    A cross-theme label may support more than one cluster.  This deliberately
    keeps AI applications and robotics as separate keys instead of flattening
    both into a generic technology tag.
    """

    clustered: dict[str, dict[str, Any]] = {}
    for raw_label in labels:
        display = str(raw_label or "").strip()
        normalized = normalize_theme_label(display)
        if not normalized:
            continue
        matches = _theme_cluster_matches(display)
        if not matches:
            matches = (f"LABEL:{normalized}",)
        for cluster_key in matches:
            settings = THEME_CLUSTER_TAXONOMY.get(cluster_key, {})
            canonical = str(settings.get("canonical_label") or display)
            entry = clustered.setdefault(
                cluster_key,
                {
                    "cluster_key": cluster_key,
                    "canonical_label": canonical,
                    "labels": set(),
                    "normalized_labels": set(),
                },
            )
            entry["labels"].add(display)
            entry["normalized_labels"].add(normalized)
    return [
        {
            "cluster_key": cluster_key,
            "canonical_label": item["canonical_label"],
            "labels": tuple(sorted(item["labels"])),
            "normalized_labels": tuple(
                sorted(item["normalized_labels"])
            ),
        }
        for cluster_key, item in sorted(clustered.items())
    ]


def infer_name_theme_memberships(
    stock_names: Mapping[Any, Any],
) -> dict[str, list[Membership]]:
    """Infer narrow business themes from immutable security short names.

    This is a fallback for providers whose concept snapshot omits a company's
    most obvious business identity.  Only explicit ``name_markers`` from the
    shared taxonomy are eligible; broad taxonomy keywords are deliberately
    not scanned, which prevents generic words from inventing memberships.
    """

    inferred: dict[str, list[Membership]] = defaultdict(list)
    for raw_code, raw_name in stock_names.items():
        code = str(raw_code or "")[:6]
        name = str(raw_name or "").strip()
        if not code or not name:
            continue
        for cluster_key, settings in THEME_CLUSTER_TAXONOMY.items():
            markers = tuple(settings.get("name_markers") or ())
            if not markers or not any(str(marker) in name for marker in markers):
                continue
            inferred[code].append((
                f"NAME_CLUSTER:{cluster_key}",
                str(settings.get("canonical_label") or cluster_key),
                "name_keyword",
            ))
    return dict(inferred)


def cluster_themes_by_component_overlap(
    theme_components: Mapping[Any, Iterable[Any]],
    *,
    theme_labels: Mapping[Any, Any] | None = None,
    minimum_overlap_ratio: float = 0.70,
    minimum_shared_components: int = 2,
) -> list[dict[str, Any]]:
    """Deterministically group theme aliases backed by similar constituents.

    Label normalization remains the first identity check.  Differently named
    themes are grouped only when their constituent-stock Jaccard overlap meets
    the supplied threshold and they share enough stocks.  Complete-linkage is
    used deliberately: every theme in a group must overlap every other theme,
    preventing a chain of partial overlaps from collapsing distinct themes.

    Unknown themes do not need a taxonomy entry.  They receive a stable
    ``LABEL:`` semantic key and can still be grouped automatically when their
    observed constituents provide sufficient evidence.
    """

    if not 0.0 <= float(minimum_overlap_ratio) <= 1.0:
        raise ValueError("minimum_overlap_ratio must be between 0 and 1")
    if int(minimum_shared_components) < 1:
        raise ValueError("minimum_shared_components must be positive")

    labels = theme_labels or {}
    rows: list[dict[str, Any]] = []
    for raw_theme_id, raw_components in theme_components.items():
        theme_id = str(raw_theme_id or "").strip()
        display = str(labels.get(raw_theme_id, theme_id) or theme_id).strip()
        normalized = normalize_theme_label(display)
        if not theme_id or not normalized:
            continue
        components = frozenset(
            str(component).strip()
            for component in (raw_components or ())
            if str(component).strip()
        )
        rows.append({
            "theme_id": theme_id,
            "label": display,
            "normalized_label": normalized,
            "semantic_keys": frozenset(_theme_cluster_matches(display)),
            "components": components,
        })
    rows.sort(
        key=lambda item: (
            item["normalized_label"],
            item["label"],
            item["theme_id"],
        )
    )

    def pair_overlap(left: Mapping[str, Any], right: Mapping[str, Any]) -> float:
        union = left["components"] | right["components"]
        if not union:
            return 0.0
        return len(left["components"] & right["components"]) / len(union)

    def compatible(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
        if left["normalized_label"] == right["normalized_label"]:
            return True
        left_semantics = left["semantic_keys"]
        right_semantics = right["semantic_keys"]
        if (
            left_semantics
            and right_semantics
            and left_semantics.isdisjoint(right_semantics)
        ):
            return False
        shared = left["components"] & right["components"]
        return (
            len(shared) >= int(minimum_shared_components)
            and pair_overlap(left, right) + 1e-12
            >= float(minimum_overlap_ratio)
        )

    groups: list[list[dict[str, Any]]] = []
    for row in rows:
        matching_group = next(
            (
                group
                for group in groups
                if all(compatible(row, member) for member in group)
            ),
            None,
        )
        if matching_group is None:
            groups.append([row])
        else:
            matching_group.append(row)

    result = []
    for group in groups:
        ordered = sorted(
            group,
            key=lambda item: (
                item["normalized_label"],
                item["label"],
                item["theme_id"],
            ),
        )
        common_semantics = set(ordered[0]["semantic_keys"])
        for item in ordered[1:]:
            common_semantics &= set(item["semantic_keys"])
        common_semantic_keys = tuple(sorted(common_semantics))
        if common_semantic_keys:
            taxonomy_label = str(
                THEME_CLUSTER_TAXONOMY[common_semantic_keys[0]][
                    "canonical_label"
                ]
            )
            canonical_label = (
                taxonomy_label
                if len(ordered) > 1
                or normalize_theme_label(ordered[0]["label"])
                == normalize_theme_label(taxonomy_label)
                else str(ordered[0]["label"])
            )
            semantic_cluster_keys = common_semantic_keys
        else:
            canonical_label = str(ordered[0]["label"])
            semantic_cluster_keys = (
                f"LABEL:{normalize_theme_label(canonical_label)}",
            )
        pairwise_overlaps = [
            pair_overlap(left, right)
            for index, left in enumerate(ordered)
            for right in ordered[index + 1:]
        ]
        stable_identity = "|".join(
            sorted(
                f'{item["normalized_label"]}:{item["theme_id"]}'
                for item in ordered
            )
        )
        cluster_digest = hashlib.sha256(
            stable_identity.encode("utf-8")
        ).hexdigest()[:16]
        all_components = set().union(
            *(item["components"] for item in ordered)
        )
        result.append({
            "component_cluster_key": f"COMPONENT:{cluster_digest}",
            "canonical_label": canonical_label,
            "theme_ids": tuple(item["theme_id"] for item in ordered),
            "labels": tuple(item["label"] for item in ordered),
            "normalized_labels": tuple(
                item["normalized_label"] for item in ordered
            ),
            "semantic_cluster_keys": semantic_cluster_keys,
            "component_count": len(all_components),
            "minimum_pairwise_overlap": round(
                min(pairwise_overlaps, default=1.0),
                8,
            ),
        })
    return sorted(
        result,
        key=lambda item: (
            normalize_theme_label(item["canonical_label"]),
            item["component_cluster_key"],
        ),
    )


def build_theme_alias_index(
    memberships: Mapping[str, Iterable[Membership]],
) -> dict[str, tuple[str, ...]]:
    """Build a provider-neutral alias index for every observed theme.

    Frozen semantic families are used when known. Unknown labels still get a
    stable normalized ``LABEL:`` cluster, so no theme depends on a hand-made
    allowlist in order to receive news and shadow evidence.
    """

    aliases: dict[str, set[str]] = defaultdict(set)
    for values in memberships.values():
        for theme_code, theme_name, _source in values:
            display = str(theme_name or theme_code or "").strip()
            if not display:
                continue
            for cluster in cluster_theme_labels((display,)):
                key = str(cluster["cluster_key"])
                taxonomy_aliases = (
                    THEME_CLUSTER_TAXONOMY.get(key, {}).get("keywords")
                    or ()
                )
                aliases[key].update({
                    display,
                    str(theme_code or "").strip(),
                    str(cluster["canonical_label"]),
                    *[str(item) for item in cluster["labels"]],
                    *[str(item) for item in taxonomy_aliases],
                })
    return {
        key: tuple(sorted(value for value in values if value))
        for key, values in sorted(aliases.items())
    }


PAPER_RESEARCH_THEME_KEYWORDS = {
    "AI应用": (
        "人工智能",
        "AIGC",
        "CHATGPT",
        "大模型",
        "智能体",
        "AI应用",
        "生成式AI",
        "多模态",
    ),
    "机器人": (
        "机器人",
        "智能机器",
        "机器视觉",
        "具身智能",
        "工业自动化",
    ),
}


def _paper_research_theme_keywords() -> dict[str, tuple[str, ...]]:
    """Load the frozen research taxonomy with safe code defaults."""

    try:
        from .config import load_v3_config

        raw_groups = (
            load_v3_config().get("theme_research", {}).get("groups", {})
        )
    except (FileNotFoundError, KeyError, TypeError, ValueError):
        raw_groups = {}
    configured = {
        str(group): tuple(
            str(keyword)
            for keyword in (settings.get("keywords") or ())
            if str(keyword)
        )
        for group, settings in raw_groups.items()
        if isinstance(settings, dict)
    }
    return configured or PAPER_RESEARCH_THEME_KEYWORDS


def paper_research_groups(theme_names: Any) -> list[str]:
    """Classify auditable paper-only research groups from all theme labels."""

    values = (
        theme_names
        if isinstance(theme_names, (list, tuple, set))
        else ()
    )
    normalized = "|".join(str(value).upper() for value in values if value)
    return [
        group
        for group, keywords in _paper_research_theme_keywords().items()
        if any(keyword.upper() in normalized for keyword in keywords)
    ]


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def _scaled(value: float, lower: float, upper: float) -> float:
    if upper <= lower:
        return 0.0
    return _clamp((float(value) - lower) / (upper - lower))


def _band(
    value: float,
    lower: float,
    upper: float,
    shoulder: float,
) -> float:
    if lower <= value <= upper:
        return 1.0
    if value < lower:
        return _clamp((value - (lower - shoulder)) / shoulder)
    return _clamp(((upper + shoulder) - value) / shoulder)


def _date_key(value: Any) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def calculate_theme_composite_score(
    *,
    advance_breadth_pct: float,
    capital_acceleration_pct: float,
    relative_strength_pct: float,
    news_novelty: float | None,
    topk_member_score_median: float,
    breadth_acceleration_pct: float = 0.0,
    crowding: float = 0.0,
) -> dict[str, float]:
    """Calculate an auditable sector score from five explicit components."""

    breadth_component = _clamp(
        0.72 * _scaled(advance_breadth_pct, 42.0, 82.0)
        + 0.28 * _scaled(breadth_acceleration_pct, 0.0, 22.0)
    )
    capital_component = _scaled(
        capital_acceleration_pct,
        0.0,
        65.0,
    )
    relative_component = _scaled(relative_strength_pct, 0.0, 6.0)
    novelty_available = news_novelty is not None
    novelty_component = _clamp(news_novelty or 0.0)
    topk_component = _clamp(topk_member_score_median)
    components = {
        "theme_score_advance_breadth": breadth_component,
        "theme_score_capital_acceleration": capital_component,
        "theme_score_relative_strength": relative_component,
        "theme_score_news_novelty": novelty_component,
        "theme_score_news_novelty_available": float(novelty_available),
        "theme_score_topk_member_median": topk_component,
    }
    weighted = (
        THEME_COMPOSITE_WEIGHTS["advance_breadth"] * breadth_component
        + THEME_COMPOSITE_WEIGHTS["capital_acceleration"] * capital_component
        + THEME_COMPOSITE_WEIGHTS["relative_strength"] * relative_component
        + THEME_COMPOSITE_WEIGHTS["topk_member_median"] * topk_component
    )
    available_weight = 1.0 - THEME_COMPOSITE_WEIGHTS["news_novelty"]
    if novelty_available:
        weighted += (
            THEME_COMPOSITE_WEIGHTS["news_novelty"] * novelty_component
        )
        available_weight = 1.0
    score = (
        weighted / max(available_weight, 1e-9)
        - 0.08 * _scaled(crowding, 0.78, 1.0)
    )
    return {
        **components,
        "theme_composite_score": _clamp(score),
    }


def _theme_news_novelty(
    values: Mapping[str, float],
    *,
    theme_code: str,
    theme_name: str,
    evidence_available: bool,
) -> tuple[float | None, str, str]:
    if not evidence_available:
        return None, "MISSING", ""
    candidates = [theme_code, theme_name]
    for cluster in cluster_theme_labels(candidates):
        candidates.extend(
            [cluster["cluster_key"], cluster["canonical_label"]]
        )
    normalized_values = {
        normalize_theme_label(key): (str(key), float(value or 0.0))
        for key, value in values.items()
        if normalize_theme_label(key)
    }
    matches = []
    for candidate in candidates:
        if candidate in values:
            matches.append((str(candidate), float(values[candidate] or 0.0)))
            continue
        normalized = normalize_theme_label(candidate)
        if normalized in normalized_values:
            matches.append(normalized_values[normalized])
    if not matches:
        return 0.0, "CONFIRMED_ZERO", ""
    matched_key, value = max(matches, key=lambda item: item[1])
    return _clamp(value), "OBSERVED", matched_key


def calculate_theme_statistics(
    frame: pd.DataFrame,
    *,
    as_of: Any,
    memberships: dict[str, list[Membership]],
    minimum_members: int = 3,
    member_scores: Mapping[str, float] | None = None,
    theme_news_novelty: Mapping[str, float] | None = None,
    theme_news_available: bool = True,
    top_k: int = 5,
) -> dict[str, dict[str, Any]]:
    """Calculate point-in-time sector features and the five-part score.

    ``memberships`` must already be resolved as-of by the caller.  The
    calculation uses equal-horizon comparisons: current breadth versus prior
    breadth, five-day theme return versus five-day market return, and current
    theme amount versus its own previous five-session mean.  News novelty and
    member scores must also be point-in-time values supplied by the caller;
    deterministic return-derived member scores are used when they are absent.
    """

    if frame.empty or not memberships:
        return {}
    work = frame[
        ["stock_code", "trade_date", "change_pct", "amount"]
    ].copy()
    work["stock_code"] = work["stock_code"].astype(str).str[:6]
    work["trade_date"] = pd.to_datetime(work["trade_date"]).dt.normalize()
    work["change_pct"] = pd.to_numeric(
        work["change_pct"],
        errors="coerce",
    ).fillna(0.0)
    work["amount"] = pd.to_numeric(
        work["amount"],
        errors="coerce",
    ).fillna(0.0)
    current_date = _date_key(as_of)
    dates = sorted(
        value
        for value in work["trade_date"].drop_duplicates()
        if value <= current_date
    )
    if not dates or dates[-1] != current_date:
        return {}
    prior_date = dates[-2] if len(dates) >= 2 else current_date
    prior3_date = dates[-4] if len(dates) >= 4 else dates[0]
    return_dates = dates[-5:]
    amount_dates = dates[-6:-1]
    current = work[work["trade_date"] == current_date].set_index(
        "stock_code"
    )
    prior = work[work["trade_date"] == prior_date].set_index("stock_code")
    prior3 = work[work["trade_date"] == prior3_date].set_index("stock_code")
    recent = work[work["trade_date"].isin(return_dates)]
    growth = recent.assign(
        growth=(1.0 + recent["change_pct"] / 100.0).clip(lower=0.01)
    )
    stock_return_5d = (
        growth.groupby("stock_code", observed=True)["growth"].prod() - 1.0
    ) * 100.0
    market_return_5d = float(stock_return_5d.median() or 0.0)
    prior_amount = work[work["trade_date"].isin(amount_dates)]
    market_amount = max(float(current["amount"].sum()), 1.0)

    theme_members: dict[str, set[str]] = defaultdict(set)
    theme_meta: dict[str, tuple[str, str]] = {}
    for stock_code, values in memberships.items():
        code = str(stock_code)[:6]
        for theme_code, theme_name, source in values:
            key = str(theme_code or "").strip()
            if not key:
                continue
            theme_members[key].add(code)
            theme_meta.setdefault(
                key,
                (str(theme_name or key), str(source or "industry")),
            )

    result: dict[str, dict[str, Any]] = {}
    for theme_code, raw_members in theme_members.items():
        source = theme_meta[theme_code][1]
        # Broad concepts (for example AI and digital economy) are still
        # investable taxonomies.  Size is normalized in the breadth/leadership
        # features; silently dropping them creates a thematic blind spot.
        maximum_members = 2000 if source == "concept" else 1200
        members = sorted(
            code for code in raw_members if code in current.index
        )
        if not minimum_members <= len(members) <= maximum_members:
            continue

        def breadth(source_frame: pd.DataFrame) -> float:
            available = source_frame.loc[
                source_frame.index.intersection(members)
            ]
            if available.empty:
                return 0.0
            return float(
                (available["change_pct"] > 0).mean() * 100.0
            )

        current_breadth = breadth(current)
        prior_breadth = breadth(prior)
        prior3_breadth = breadth(prior3)
        breadth_acceleration = (
            0.65 * (current_breadth - prior_breadth)
            + 0.35
            * (current_breadth - prior3_breadth)
            / max(1, min(3, len(dates) - 1))
        )
        member_returns = stock_return_5d.reindex(members).dropna()
        sector_return_5d = float(
            member_returns.median() if not member_returns.empty else 0.0
        )
        sector_relative_return = sector_return_5d - market_return_5d
        current_amount = float(
            current.loc[current.index.intersection(members), "amount"].sum()
        )
        amount_history = (
            prior_amount[
                prior_amount["stock_code"].isin(members)
            ]
            .groupby("trade_date", observed=True)["amount"]
            .sum()
        )
        amount_base = float(
            amount_history.mean() if not amount_history.empty else 0.0
        )
        amount_acceleration = (
            (current_amount / amount_base - 1.0) * 100.0
            if amount_base > 0
            else 0.0
        )
        leader_count = int((member_returns >= 5.0).sum())
        leadership_depth = min(
            1.0,
            leader_count / max(1, min(5, len(members))),
        )
        crowding = min(1.0, current_amount / market_amount * 12.0)
        supplied_member_scores = member_scores or {}
        scored_members = [
            (
                code,
                _clamp(
                    float(supplied_member_scores[code])
                    if code in supplied_member_scores
                    else _scaled(
                        float(stock_return_5d.get(code, 0.0)),
                        0.0,
                        15.0,
                    )
                ),
            )
            for code in members
        ]
        top_member_scores = sorted(
            (score for _code, score in scored_members),
            reverse=True,
        )[: max(1, int(top_k))]
        topk_member_median = float(
            pd.Series(top_member_scores).median()
            if top_member_scores
            else 0.0
        )
        novelty, novelty_status, novelty_match_key = _theme_news_novelty(
            theme_news_novelty or {},
            theme_code=theme_code,
            theme_name=theme_meta[theme_code][0],
            evidence_available=theme_news_available,
        )
        composite = calculate_theme_composite_score(
            advance_breadth_pct=current_breadth,
            breadth_acceleration_pct=breadth_acceleration,
            capital_acceleration_pct=amount_acceleration,
            relative_strength_pct=sector_relative_return,
            news_novelty=novelty,
            topk_member_score_median=topk_member_median,
            crowding=crowding,
        )
        result[theme_code] = {
            "theme_code": theme_code,
            "theme_name": theme_meta[theme_code][0],
            "theme_source": source,
            "member_count": len(members),
            "sector_breadth_pct": current_breadth,
            "sector_breadth_prior_pct": prior_breadth,
            "sector_breadth_3d_prior_pct": prior3_breadth,
            "sector_breadth_acceleration_pct": breadth_acceleration,
            "sector_return_5d_pct": sector_return_5d,
            "sector_relative_return_pct": sector_relative_return,
            "sector_amount_acceleration_pct": amount_acceleration,
            "sector_leadership_depth": leadership_depth,
            "sector_crowding": crowding,
            "theme_news_novelty_score": float(novelty or 0.0),
            "theme_news_novelty_status": novelty_status,
            "theme_news_novelty_match_key": novelty_match_key,
            "theme_topk_member_score_median": topk_member_median,
            "theme_topk_member_count": len(top_member_scores),
            **composite,
            # Compatibility alias: downstream sleeves can migrate without
            # changing the meaning or ordering of the new board-level score.
            "theme_opportunity_score": composite[
                "theme_composite_score"
            ],
        }
    return result


def attach_best_theme(
    base: dict[str, dict[str, Any]],
    *,
    memberships: dict[str, list[Membership]],
    statistics: dict[str, dict[str, Any]],
) -> None:
    """Attach the best theme and retain every independently scored theme.

    ``theme_signal_candidates`` is the authoritative per-stock/per-theme
    feature ledger. The selected top candidate remains overlaid on the stock
    for compatibility with non-theme sleeves, but downstream theme research
    must use the candidate-specific key and features.
    """

    for stock_code, item in base.items():
        all_memberships = memberships.get(stock_code, [])
        all_theme_codes = sorted({
            str(theme_code)
            for theme_code, _theme_name, _source in all_memberships
            if str(theme_code)
        })
        all_theme_names = sorted({
            str(theme_name or theme_code)
            for theme_code, theme_name, _source in all_memberships
            if str(theme_name or theme_code)
        })
        all_theme_clusters = cluster_theme_labels(all_theme_names)
        all_theme_cluster_keys = [
            str(cluster["cluster_key"])
            for cluster in all_theme_clusters
        ]
        all_theme_cluster_labels = [
            str(cluster["canonical_label"])
            for cluster in all_theme_clusters
        ]
        candidates: list[dict[str, Any]] = []
        seen_theme_codes: set[str] = set()
        for theme_code, theme_name, source in all_memberships:
            theme_code = str(theme_code or "").strip()
            if not theme_code or theme_code in seen_theme_codes:
                continue
            seen_theme_codes.add(theme_code)
            stats = statistics.get(theme_code)
            if not stats:
                continue
            display_name = str(
                stats.get("theme_name") or theme_name or theme_code
            )
            clusters = cluster_theme_labels((display_name,))
            cluster_keys = [
                str(cluster["cluster_key"]) for cluster in clusters
            ]
            cluster_labels = [
                str(cluster["canonical_label"]) for cluster in clusters
            ]
            sector_return = float(stats["sector_return_5d_pct"])
            stock_excess = (
                float(item.get("return_5d_pct") or 0.0) - sector_return
            )
            amount_ratio = float(item.get("amount_ratio_5_20") or 0.0)
            breakout = float(item.get("breakout_20d_proximity") or 0.0)
            return_5d = float(item.get("return_5d_pct") or 0.0)
            latest_change = float(item.get("latest_change_pct") or 0.0)
            leadership = _clamp(
                0.30 * _scaled(stock_excess, -1.0, 8.0)
                + 0.25 * _scaled(amount_ratio, 0.9, 2.4)
                + 0.20 * _scaled(breakout, 0.92, 1.0)
                + 0.15 * _scaled(return_5d, 0.0, 15.0)
                + 0.10 * _scaled(latest_change, 0.0, 6.0)
            )
            feature_key = hashlib.sha256(
                "|".join((
                    str(stock_code),
                    theme_code,
                    normalize_theme_label(display_name),
                    str(source or stats.get("theme_source") or ""),
                    ",".join(cluster_keys),
                )).encode("utf-8")
            ).hexdigest()
            candidates.append({
                **stats,
                "theme_code": theme_code,
                "theme_name": display_name,
                "theme_source": str(
                    stats.get("theme_source") or source or ""
                ),
                "theme_member_count": int(stats["member_count"]),
                "theme_cluster_keys": cluster_keys,
                "theme_cluster_labels": cluster_labels,
                "theme_feature_key": feature_key,
                "stock_relative_to_theme_5d_pct": stock_excess,
                "stock_leadership_score": leadership,
                "leadership_quality": leadership,
            })
        candidates.sort(
            key=lambda candidate: (
                -float(candidate.get("theme_composite_score") or 0.0),
                -float(candidate.get("stock_leadership_score") or 0.0),
                str(candidate.get("theme_code") or ""),
            )
        )
        if not candidates:
            item.setdefault("theme_code", "")
            item.setdefault("theme_name", "")
            item.setdefault("theme_source", "")
            item["theme_codes"] = all_theme_codes
            item["theme_names"] = all_theme_names
            item["theme_cluster_keys"] = []
            item["theme_cluster_labels"] = []
            item["all_theme_cluster_keys"] = all_theme_cluster_keys
            item["all_theme_cluster_labels"] = all_theme_cluster_labels
            item["theme_signal_candidates"] = []
            item["paper_research_groups"] = paper_research_groups(
                all_theme_names
            )
            item["stock_leadership_score"] = 0.0
            item["leadership_quality"] = 0.0
            item["theme_opportunity_score"] = 0.0
            continue
        item["theme_codes"] = all_theme_codes
        item["theme_names"] = all_theme_names
        item["all_theme_cluster_keys"] = all_theme_cluster_keys
        item["all_theme_cluster_labels"] = all_theme_cluster_labels
        item["theme_signal_candidates"] = candidates
        item["paper_research_groups"] = paper_research_groups(
            item["theme_names"]
        )
        stats = candidates[0]
        for key, value in stats.items():
            if key not in {
                "theme_code",
                "theme_name",
                "theme_source",
                "member_count",
                "theme_signal_candidates",
            }:
                item[key] = value
        item["theme_code"] = str(stats["theme_code"])
        item["theme_name"] = str(stats["theme_name"])
        item["theme_source"] = str(stats["theme_source"])
        item["theme_member_count"] = int(stats["theme_member_count"])
        item["theme_cluster_keys"] = list(stats["theme_cluster_keys"])
        item["theme_cluster_labels"] = list(
            stats["theme_cluster_labels"]
        )
        item["stock_relative_to_theme_5d_pct"] = float(
            stats["stock_relative_to_theme_5d_pct"]
        )
        item["stock_leadership_score"] = float(
            stats["stock_leadership_score"]
        )
        # Kept for formula compatibility; it is now stock-specific rather
        # than the same aggregate value copied to every theme member.
        item["leadership_quality"] = float(stats["leadership_quality"])


def diversified_universe_codes(
    base: dict[str, dict[str, Any]],
    *,
    limit: int,
) -> list[str]:
    """Reserve room for trend, ignition, theme and oversold reversal setups.

    The theme reserve is intentionally applied before the shared blend.  A
    newly strengthening board therefore reaches V3/V4/V5/V6 evaluation even
    when its leaders are not yet top-market momentum names.  This changes
    observation coverage only; it does not bypass any risk or entry gate.
    """

    if not base:
        return []
    rows = []
    for code, item in base.items():
        momentum = (
            float(item.get("return_20d_pct") or 0.0)
            + float(item.get("relative_strength_20d_pct") or 0.0)
            + min(
                float(item.get("latest_amount") or 0.0) / 100_000_000,
                10.0,
            )
        )
        distance = float(item.get("distance_ma20_pct") or 0.0)
        ignition = (
            0.24
            * _scaled(float(item.get("amount_ratio_5_20") or 0.0), 0.9, 2.8)
            + 0.18
            * _scaled(float(item.get("latest_change_pct") or 0.0), -1.0, 7.0)
            + 0.18
            * float(item.get("theme_opportunity_score") or 0.0)
            + 0.16
            * float(item.get("stock_leadership_score") or 0.0)
            + 0.16
            * float(item.get("news_theme_context_score") or 0.0)
            + 0.08 * (1.0 - _scaled(abs(distance - 3.0), 0.0, 12.0))
        )
        theme_leader = (
            0.50 * float(item.get("theme_opportunity_score") or 0.0)
            + 0.32 * float(item.get("stock_leadership_score") or 0.0)
            + 0.18 * float(item.get("news_theme_context_score") or 0.0)
        )
        ret20 = float(item.get("return_20d_pct") or 0.0)
        drawdown = float(item.get("drawdown_20d_pct") or 0.0)
        latest_relative = float(
            item.get("latest_relative_to_market_pct") or 0.0
        )
        reversal = (
            0.27 * _band(ret20, -45.0, -10.0, 15.0)
            + 0.24 * _band(drawdown, -48.0, -12.0, 16.0)
            + 0.18
            * _scaled(
                float(item.get("rebound_from_low_pct") or 0.0),
                0.5,
                5.0,
            )
            + 0.15
            * _scaled(
                float(item.get("amount_ratio_1_20") or 0.0),
                0.8,
                2.8,
            )
            + 0.10 * _scaled(latest_relative, -1.0, 6.0)
            + 0.06
            * float(item.get("theme_opportunity_score") or 0.0)
        )
        rows.append({
            "stock_code": code,
            "momentum": momentum,
            "ignition": ignition,
            "theme_leader": theme_leader,
            "reversal": reversal,
        })
    ranked = pd.DataFrame(rows).set_index("stock_code")
    for column in (
        "momentum",
        "ignition",
        "theme_leader",
        "reversal",
    ):
        ranked[column + "_pct"] = ranked[column].rank(
            method="average",
            pct=True,
        )
    ranked["priority"] = ranked[
        [
            "momentum_pct",
            "ignition_pct",
            "theme_leader_pct",
            "reversal_pct",
        ]
    ].max(axis=1)
    ranked["blend"] = (
        0.27 * ranked["momentum_pct"]
        + 0.28 * ranked["ignition_pct"]
        + 0.24 * ranked["theme_leader_pct"]
        + 0.21 * ranked["reversal_pct"]
    )
    blended = ranked.sort_values(
        ["priority", "blend"],
        ascending=[False, False],
    )
    requested = max(1, int(limit))
    theme_reserve_quota = min(requested, max(12, int(requested * 0.20)))
    per_theme_limit = 3
    theme_groups: dict[
        str,
        list[tuple[str, float, float, float]],
    ] = defaultdict(list)
    semantic_groups: dict[
        str,
        dict[str, tuple[str, float, float, float]],
    ] = defaultdict(dict)
    name_theme_groups: dict[
        str,
        dict[str, tuple[str, float, float, float]],
    ] = defaultdict(dict)
    for code, item in base.items():
        context_score = float(item.get("news_theme_context_score") or 0.0)
        for candidate in item.get("theme_signal_candidates") or ():
            theme_code = str(candidate.get("theme_code") or "").strip()
            board_score = float(
                candidate.get("theme_composite_score")
                or candidate.get("theme_opportunity_score")
                or 0.0
            )
            stock_score = float(candidate.get("stock_leadership_score") or 0.0)
            novelty_score = float(
                candidate.get("theme_news_novelty_score") or 0.0
            )
            trigger_score = max(
                board_score,
                context_score,
                novelty_score,
            )
            if not theme_code or trigger_score < 0.35:
                continue
            theme_groups[theme_code].append((
                str(code),
                stock_score,
                float(ranked.at[code, "ignition"]),
                trigger_score,
            ))
            if str(candidate.get("theme_source") or "") == "name_keyword":
                name_theme_groups[theme_code][str(code)] = (
                    str(code),
                    stock_score,
                    float(ranked.at[code, "ignition"]),
                    trigger_score,
                )
            for cluster_key in candidate.get("theme_cluster_keys") or ():
                normalized_cluster = str(cluster_key or "").strip()
                if normalized_cluster in THEME_CLUSTER_TAXONOMY:
                    row = (
                        str(code),
                        stock_score,
                        float(ranked.at[code, "ignition"]),
                        trigger_score,
                    )
                    previous = semantic_groups[normalized_cluster].get(
                        str(code)
                    )
                    if previous is None or row[3:] > previous[3:]:
                        semantic_groups[normalized_cluster][str(code)] = row
    ordered_themes = sorted(
        theme_groups,
        key=lambda theme_code: (
            -max(row[3] for row in theme_groups[theme_code]),
            theme_code,
        ),
    )
    theme_codes: list[str] = []
    for theme_code in sorted(name_theme_groups):
        leaders = sorted(
            name_theme_groups[theme_code].values(),
            key=lambda row: (-row[1], -row[3], -row[2], row[0]),
        )[:3]
        for code, _stock_score, _ignition, _trigger_score in leaders:
            if code not in theme_codes:
                theme_codes.append(code)
            if len(theme_codes) >= theme_reserve_quota:
                break
        if len(theme_codes) >= theme_reserve_quota:
            break
    semantic_quota = theme_reserve_quota
    semantic_leaders = {
        cluster_key: sorted(
            rows,
            key=lambda row: (-row[3], -row[1], -row[2], row[0]),
        )
        for cluster_key, by_code in semantic_groups.items()
        for rows in (list(by_code.values()),)
    }
    cluster_opportunities = sorted(
        (
            semantic_leaders[cluster_key][0][3],
            cluster_key,
        )
        for cluster_key in semantic_leaders
    )
    opportunity_values = pd.Series(
        [row[0] for row in cluster_opportunities],
        dtype=float,
    )
    opportunity_percentiles = opportunity_values.rank(
        method="average",
        pct=True,
    ).tolist()
    ordered_clusters = [
        cluster_key
        for percentile, (_opportunity, cluster_key) in sorted(
            zip(opportunity_percentiles, cluster_opportunities),
            key=lambda row: (
                -row[0],
                row[1][1],
            ),
        )
        if percentile >= 0.50
    ]
    # Keep three independently ranked leaders when available.  One stock alone
    # cannot demonstrate board breadth, while three still leave the majority of
    # the universe to the normal multi-sleeve blend.
    for cluster_key in ordered_clusters:
        for leader in semantic_leaders[cluster_key][:3]:
            code = leader[0]
            if code not in theme_codes:
                theme_codes.append(code)
            if len(theme_codes) >= semantic_quota:
                break
        if len(theme_codes) >= semantic_quota:
            break
    for theme_code in ordered_themes:
        leaders = sorted(
            theme_groups[theme_code],
            key=lambda row: (-row[3], -row[1], -row[2], row[0]),
        )[:per_theme_limit]
        for code, _stock_score, _ignition, _trigger_score in leaders:
            if code not in theme_codes:
                theme_codes.append(code)
            if len(theme_codes) >= theme_reserve_quota:
                break
        if len(theme_codes) >= theme_reserve_quota:
            break
    # A broad sell-off can put hundreds of liquid stocks into a bottoming
    # process at once.  Reserve half of the retail universe for this distinct
    # shape; the remaining half still comes from the multi-sleeve blend.
    reversal_quota = min(
        max(0, requested - len(theme_codes)),
        max(50, int(requested * 0.50)),
    )
    reversal_codes = ranked.sort_values(
        ["reversal_pct", "reversal", "blend"],
        ascending=[False, False, False],
    ).head(reversal_quota).index.tolist()
    result = list(theme_codes)
    for code in reversal_codes:
        normalized = str(code)
        if normalized not in result:
            result.append(normalized)
    for code in blended.index.tolist():
        normalized = str(code)
        if normalized not in result:
            result.append(normalized)
        if len(result) >= requested:
            break
    # ``limit`` is the shared ranking budget, not permission for one sleeve to
    # erase another sleeve's valid setup.  Append every liquid stock already
    # inside the frozen left-side preparation zone so the oversold strategy
    # can make its own prepare/trigger decision.  This is normally a modest
    # extension of the shared universe and remains deterministic.
    preparation_codes = [
        str(code)
        for code, item in base.items()
        if (
            -52.0
            <= float(item.get("return_20d_pct") or 0.0)
            <= -8.0
            and -55.0
            <= float(item.get("drawdown_20d_pct") or 0.0)
            <= -10.0
            and -34.0
            <= float(item.get("distance_ma20_pct") or 0.0)
            <= -2.0
            and -25.0
            <= float(item.get("return_5d_pct") or 0.0)
            <= 12.0
            and 1.2
            <= float(item.get("atr_14d_pct") or 0.0)
            <= 15.0
        )
    ]
    for code in sorted(preparation_codes):
        if code not in result:
            result.append(code)
    return result
