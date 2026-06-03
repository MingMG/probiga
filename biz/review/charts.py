#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""复盘图表生成 — matplotlib 绘制，返回 base64 PNG"""
from __future__ import annotations

import io
import base64
import warnings
from pathlib import Path

warnings.filterwarnings("ignore", category=UserWarning)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

_FONT_PATH = "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"
if Path(_FONT_PATH).exists():
    _fp = fm.FontProperties(fname=_FONT_PATH)
    plt.rcParams["font.family"] = _fp.get_name()
else:
    plt.rcParams["font.sans-serif"] = ["DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

BG = "#1a1a2e"
CARD_BG = "#16213e"
RED = "#e53935"
GREEN = "#43a047"
BLUE = "#1a73e8"
ORANGE = "#ff9800"
TEXT = "#e0e0e0"
SUBTEXT = "#888"

FIG_W = 8
FIG_H = 6


def _to_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100, bbox_inches="tight", facecolor=BG, edgecolor="none")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode()
    plt.close(fig)
    return f"data:image/png;base64,{b64}"


def chart_market_heat(heat_pct: float, up_count: int, down_count: int, total_amt: float,
                      idx_name: str, idx_price: float, idx_chg: float, sideline: float) -> str:
    """市场热度仪表盘"""
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.5), facecolor=BG)
    fig.subplots_adjust(wspace=0.3)

    # Left: heat gauge
    ax = axes[0]
    ax.set_facecolor(CARD_BG)
    ax.set_xlim(-1, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    colors = [GREEN if heat_pct < 30 else ORANGE if heat_pct < 60 else RED]
    ax.barh(0.4, 1, 0.25, color=SUBTEXT, left=0, alpha=0.2)
    ax.barh(0.4, heat_pct / 100, 0.25, color=colors[0])
    ax.text(0.5, 0.65, f"{heat_pct:.1f}%", ha="center", va="center", fontsize=28, fontweight="bold", color=colors[0])
    ax.text(0.5, 0.35, "市场热度", ha="center", va="center", fontsize=11, color=SUBTEXT)

    # Middle: up/down ratio
    ax2 = axes[1]
    ax2.set_facecolor(CARD_BG)
    total = up_count + down_count
    ax2.pie([up_count, down_count], labels=["上涨", "下跌"], colors=[RED, GREEN], autopct="%1.1f%%",
            startangle=90, textprops={"color": TEXT, "fontsize": 11}, wedgeprops={"width": 0.5})
    ax2.set_title(f"{idx_name}\n{idx_price} {idx_chg:+.2f}%", fontsize=11, color=SUBTEXT, pad=10)

    # Right: volume
    ax3 = axes[2]
    ax3.set_facecolor(CARD_BG)
    ax3.axis("off")
    amt_str = f"{total_amt / 1e8:.0f}亿"
    ax3.text(0.5, 0.7, amt_str, ha="center", va="center", fontsize=24, fontweight="bold", color=ORANGE)
    ax3.text(0.5, 0.45, "成交额", ha="center", va="center", fontsize=11, color=SUBTEXT)
    ax3.text(0.5, 0.25, f"观望资金 {sideline:.1f}%", ha="center", va="center", fontsize=10, color=SUBTEXT)

    return _to_b64(fig)


def chart_sector_bars(hot: list[dict], cold: list[dict]) -> str:
    """板块涨跌幅条形图：左红（涨）右绿（跌）"""
    n = max(len(hot), len(cold))
    fig, ax = plt.subplots(figsize=(8, max(4, n * 0.45)), facecolor=BG)
    ax.set_facecolor(CARD_BG)

    labels = []
    vals = []
    colors_list = []
    for i in range(n):
        if i < len(hot) and isinstance(hot[i], dict):
            labels.append(hot[i].get("name", ""))
            v = float(hot[i].get("change_pct", 0) or 0)
            vals.append(v)
            colors_list.append(RED if v >= 0 else GREEN)
        else:
            labels.append("")
            vals.append(0)
            colors_list.append("none")

    bars = ax.barh(range(n), vals, color=colors_list, height=0.55)
    for i, (v, c) in enumerate(zip(vals, colors_list)):
        if c != "none" and v != 0:
            ax.text(v + (0.2 if v >= 0 else -0.2), i, f"{v:+.1f}%", va="center",
                    ha="left" if v >= 0 else "right", fontsize=9, color=colors_list[i])

    ax.set_yticks(range(n))
    ax.set_yticklabels(labels, fontsize=9, color=TEXT)
    ax.invert_yaxis()
    ax.set_title("板块涨跌幅", fontsize=13, color=TEXT, pad=10)
    ax.tick_params(colors=SUBTEXT, bottom=False, left=False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_color(SUBTEXT)
    ax.xaxis.label.set_color(SUBTEXT)

    return _to_b64(fig)


def chart_index_position(price: float, ma20: float, level: str, idx_name: str) -> str:
    """指数技术位置图"""
    fig, ax = plt.subplots(figsize=(8, 2.8), facecolor=BG)
    ax.set_facecolor(CARD_BG)

    # Simple bar representation
    ax.set_xlim(0, max(price * 1.15, ma20 * 1.15))
    ax.set_ylim(0, 2)
    ax.axis("off")

    # Price bar
    ax.barh(1.3, price, 0.35, color=RED if price > ma20 else GREEN, left=0)
    ax.text(price + max(price, ma20) * 0.01, 1.3, f"{idx_name} 现价: {price}", va="center", fontsize=11, color=TEXT)

    # MA20 reference
    ax.axvline(ma20, color=ORANGE, linewidth=1.5, linestyle="--")
    ax.text(ma20 + max(price, ma20) * 0.01, 0.7, f"MA20: {ma20}", va="center", fontsize=11, color=ORANGE)

    # Level label
    lc = RED if "突破" in level else GREEN if "跌破" in level else BLUE
    ax.text(max(price, ma20) / 2, 0.2, level, ha="center", fontsize=12, fontweight="bold", color=lc)

    return _to_b64(fig)


def chart_volume_tags(vol_up: list[dict], vol_down: list[dict]) -> str:
    """量能变化对比"""
    fig, ax = plt.subplots(figsize=(8, 2.5), facecolor=BG)
    ax.set_facecolor(CARD_BG)
    ax.axis("off")

    y = 0.7
    if vol_up:
        names = " | ".join(s.get("name", "") for s in vol_up[:6] if isinstance(s, dict))
        ax.text(0.5, y, f"📈 放量: {names}", ha="center", fontsize=10, color=ORANGE,
                wrap=True, transform=ax.transAxes)
    y = 0.3
    if vol_down:
        names = " | ".join(s.get("name", "") for s in vol_down[:6] if isinstance(s, dict))
        ax.text(0.5, y, f"📉 缩量: {names}", ha="center", fontsize=10, color=BLUE,
                wrap=True, transform=ax.transAxes)

    return _to_b64(fig)
