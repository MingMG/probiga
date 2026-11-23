"""Pure theme context scoring shared by live and frozen research."""
from __future__ import annotations
import re
from typing import Any

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
