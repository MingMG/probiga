# -*- coding: utf-8 -*-
"""QMT (xtquant) connectivity verification script.

Tests whether xtquant can connect to a running miniQMT client and retrieve
intraday individual stock data. Run this on a machine where the QMT client
is already logged in and running.

Usage:
    python integrations/qmt/test_qmt.py [--codes 000001,600519]

Prerequisites:
    1. Install QMT client (迅投极速交易终端) and log in with a broker account
    2. Start miniQMT mode (客户端最小化运行即可)
    3. pip install xtquant (or use the one bundled with QMT installation)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime


def check_xtquant_import():
    """Step 1: Check if xtquant is importable."""
    print("=" * 60)
    print("[Step 1] 检查 xtquant 是否可导入")
    print("=" * 60)
    try:
        from xtquant import xtdata
        print(f"  ✅ xtquant 导入成功")
        print(f"  模块路径: {xtdata.__file__}")
        return xtdata
    except ImportError as e:
        print(f"  ❌ 导入失败: {e}")
        print()
        print("  解决方案:")
        print("  1. pip install xtquant")
        print("  2. 或将 QMT 安装目录下的 xtquant 文件夹复制到 Python site-packages")
        print("  3. 或设置 PYTHONPATH 包含 QMT 安装目录")
        return None


def check_connection(xtdata):
    """Step 2: Check if xtquant can connect to miniQMT."""
    print()
    print("=" * 60)
    print("[Step 2] 检查与 miniQMT 客户端的连接")
    print("=" * 60)
    try:
        # connect to miniQMT (default localhost:58610)
        xtdata.connect()
        print("  ✅ 连接 miniQMT 成功")
        return True
    except Exception as e:
        print(f"  ❌ 连接失败: {e}")
        print()
        print("  可能原因:")
        print("  1. QMT 客户端未启动 — 请先打开迅投极速交易终端并登录")
        print("  2. 未进入 miniQMT 模式 — 在 QMT 客户端中切换到 miniQMT")
        print("  3. 端口被占用 — 默认端口 58610，检查是否有其他进程使用")
        return False


def test_get_instrument_list(xtdata):
    """Step 3: Test getting instrument list."""
    print()
    print("=" * 60)
    print("[Step 3] 获取股票列表")
    print("=" * 60)
    try:
        # get all A-share stock codes
        instruments = xtdata.get_stock_list_in_sector("沪深A股")
        count = len(instruments) if instruments else 0
        print(f"  ✅ 获取成功，共 {count} 只股票")
        if instruments and count > 0:
            print(f"  前5只: {instruments[:5]}")
        return instruments
    except Exception as e:
        print(f"  ❌ 获取失败: {e}")
        return None


def test_subscribe_quote(xtdata, codes: list[str]):
    """Step 4: Test subscribing to real-time quotes."""
    print()
    print("=" * 60)
    print("[Step 4] 订阅实时行情")
    print("=" * 60)

    qmt_codes = []
    for code in codes:
        qmt_code = to_qmt_symbol(code)
        if qmt_code:
            qmt_codes.append(qmt_code)
            print(f"  输入: {code} -> QMT: {qmt_code}")

    if not qmt_codes:
        print("  ❌ 无有效股票代码")
        return False

    try:
        for code in qmt_codes:
            xtdata.subscribe_quote(code, period="1m", count=-1)
            print(f"  ✅ 订阅 {code} 1分钟K线成功")
        # wait a moment for data to arrive
        print("  等待 3 秒让数据到达...")
        time.sleep(3)
        return True
    except Exception as e:
        print(f"  ❌ 订阅失败: {e}")
        return False


def test_get_market_data(xtdata, codes: list[str]):
    """Step 5: Test getting minute K-line data."""
    print()
    print("=" * 60)
    print("[Step 5] 获取分钟K线数据 (1m)")
    print("=" * 60)

    qmt_codes = [to_qmt_symbol(c) for c in codes if to_qmt_symbol(c)]
    if not qmt_codes:
        print("  ❌ 无有效股票代码")
        return None

    try:
        data = xtdata.get_market_data_ex(
            field_list=[],       # empty = all fields
            stock_list=qmt_codes,
            period="1m",
            count=10,            # last 10 bars
            dividend_type="none",
            fill_data=True,
        )
        if data is None or data.empty:
            print("  ⚠️ 返回数据为空（可能非交易时间）")
            return None

        print(f"  ✅ 获取成功")
        print(f"  数据形状: {data.shape}")
        print(f"  列: {list(data.columns)}")
        print()
        print("  最近5条数据:")
        print(data.tail(5).to_string(index=False))
        return data
    except Exception as e:
        print(f"  ❌ 获取失败: {e}")
        return None


def test_get_full_tick(xtdata, codes: list[str]):
    """Step 6: Test getting real-time tick snapshot."""
    print()
    print("=" * 60)
    print("[Step 6] 获取实时行情快照 (full_tick)")
    print("=" * 60)

    qmt_codes = [to_qmt_symbol(c) for c in codes if to_qmt_symbol(c)]
    if not qmt_codes:
        print("  ❌ 无有效股票代码")
        return None

    try:
        tick = xtdata.get_full_tick(qmt_codes)
        if tick is None:
            print("  ⚠️ 返回为空（可能非交易时间）")
            return None

        print(f"  ✅ 获取成功")
        print(f"  数据类型: {type(tick)}")
        if isinstance(tick, dict):
            for code, info in tick.items():
                print(f"\n  [{code}]")
                if isinstance(info, dict):
                    for k, v in list(info.items())[:10]:
                        print(f"    {k}: {v}")
                else:
                    print(f"    {info}")
        else:
            print(f"  数据: {str(tick)[:500]}")
        return tick
    except Exception as e:
        print(f"  ❌ 获取失败: {e}")
        return None


def test_get_tick_data(xtdata, codes: list[str]):
    """Step 7: Test getting tick-by-tick data."""
    print()
    print("=" * 60)
    print("[Step 7] 获取逐笔成交数据 (tick)")
    print("=" * 60)

    qmt_codes = [to_qmt_symbol(c) for c in codes if to_qmt_symbol(c)]
    if not qmt_codes:
        print("  ❌ 无有效股票代码")
        return None

    try:
        # subscribe tick first
        for code in qmt_codes:
            xtdata.subscribe_quote(code, period="tick", count=-1)
        time.sleep(2)

        data = xtdata.get_market_data_ex(
            field_list=[],
            stock_list=qmt_codes,
            period="tick",
            count=10,
            dividend_type="none",
            fill_data=True,
        )
        if data is None or data.empty:
            print("  ⚠️ 返回数据为空（可能非交易时间）")
            return None

        print(f"  ✅ 获取成功")
        print(f"  数据形状: {data.shape}")
        print(f"  列: {list(data.columns)}")
        print()
        print("  最近5条数据:")
        print(data.tail(5).to_string(index=False))
        return data
    except Exception as e:
        print(f"  ❌ 获取失败: {e}")
        return None


def to_qmt_symbol(code: str) -> str | None:
    """Convert 6-digit code to QMT symbol format.

    QMT uses formats like:
    - 000001.SZ (Shenzhen)
    - 600519.SH (Shanghai)
    """
    text = str(code or "").strip()
    if not text:
        return None
    if "." in text:
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return None
    if digits.startswith("6"):
        return f"{digits}.SH"
    if digits.startswith(("0", "3")):
        return f"{digits}.SZ"
    if digits.startswith("8") or digits.startswith("4"):
        return f"{digits}.BJ"
    return None


def main():
    parser = argparse.ArgumentParser(description="QMT connectivity test")
    parser.add_argument(
        "--codes",
        default="000001,600519",
        help="Comma-separated stock codes to test (default: 000001,600519)",
    )
    args = parser.parse_args()

    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    now = datetime.now()
    print(f"QMT 连通性验证 — {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"测试股票: {codes}")
    print()

    # Step 1: import
    xtdata = check_xtquant_import()
    if xtdata is None:
        sys.exit(1)

    # Step 2: connect
    if not check_connection(xtdata):
        sys.exit(1)

    # Step 3: instrument list
    test_get_instrument_list(xtdata)

    # Step 4: subscribe
    test_subscribe_quote(xtdata, codes)

    # Step 5: minute K-line
    test_get_market_data(xtdata, codes)

    # Step 6: full tick snapshot
    test_get_full_tick(xtdata, codes)

    # Step 7: tick data
    test_get_tick_data(xtdata, codes)

    print()
    print("=" * 60)
    print("验证完成！")
    print("=" * 60)
    print()
    print("如果所有步骤都通过，说明 QMT 可以用于获取盘中个股数据。")
    print("下一步: 参考 integrations/myquant/bridge.py 模式集成到项目中。")


if __name__ == "__main__":
    main()
