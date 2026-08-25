#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
个股资金流向全量爬取脚本
=============================

数据源:
  1. 东财 push2his 日K线接口 — 每只股票全量历史（最快，但 TLS 重协商可能不稳定）
  2. 东财 push2 批量接口 — 一次请求全市场当天数据（作为当天数据的兜底）
  3. 东财 datacenter-web — 备用（如果报表名匹配的话）

用法:
  # 爬取全量历史（逐只股票，每只拿所有日K线）
  python tools/crawl_stock_fund_flow.py

  # 只爬今天的数据（批量模式，一次全市场）
  python tools/crawl_stock_fund_flow.py --today-only

  # 指定日期范围
  python tools/crawl_stock_fund_flow.py --start 2024-01-01 --end 2026-06-04

  # 限制股票数量（测试用）
  python tools/crawl_stock_fund_flow.py --limit 10

  # 慢速模式（被限流时使用）
  python tools/crawl_stock_fund_flow.py --slow

环境变量:
  MYSQL_URL        MySQL 连接串
  FLOW_PROXY       代理地址（如 http://user:pass@host:port）
  FLOW_DELAY       请求间隔秒数（默认按模式自动设置）
"""

import argparse
import json
import os
import random
import re
import ssl
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from tools.env_config import create_tool_engine, resolve_tool_mysql_url
from server.common.batch_db import replace_table_rows_exact_keys
from server.common.mysql_lock import CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME

# ═══════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════

# 标准模式参数
NORMAL_DELAY = 0.5       # 请求间隔
NORMAL_JITTER = 0.3      # 随机抖动
NORMAL_BATCH_PAUSE = 30  # 每批暂停
NORMAL_BATCH_EVERY = 50  # 每N只暂停

# 慢速模式参数（被限流时）
SLOW_DELAY = 3.0
SLOW_JITTER = 2.0
SLOW_BATCH_PAUSE = 120
SLOW_BATCH_EVERY = 10

MAX_RETRIES = 2
RETRY_BASE_DELAY = 3.0
CONSECUTIVE_FAIL_THRESHOLD = 10
COOLDOWN_WAIT = 180

UNIT_MULTIPLIERS = {"亿": 1e8, "万": 1e4}


# ═══════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════

def _mysql_url() -> str:
    return resolve_tool_mysql_url()


def _read_stock_codes(engine) -> list[str]:
    """从 si_all_code 读取所有股票代码"""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT stock_code FROM si_all_code ORDER BY stock_code")
        ).fetchall()
    return [str(r[0]).strip().zfill(6) for r in rows]


def _convert_value(val) -> float:
    """转换带单位的数值字符串（如 '1.23亿' -> 123000000）"""
    if not isinstance(val, str):
        return float(val) if val is not None else 0.0
    val = val.replace("元", "").strip()
    if not val or val == "--":
        return 0.0
    for unit, mul in UNIT_MULTIPLIERS.items():
        if unit in val:
            num = re.findall(r"([-+]?\d*\.?\d+)", val)
            return float(num[0]) * mul if num else 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _classify_error(e: Exception) -> str:
    msg = str(e).lower()
    if "connection" in msg or "timeout" in msg or "remote" in msg:
        return "网络/连接"
    if "tls" in msg or "ssl" in msg or "schannel" in msg or "renegotiat" in msg:
        return "TLS/SSL"
    if "json" in msg or "keyerror" in msg:
        return "API返回异常"
    if "429" in msg or "403" in msg:
        return "限流/被拦截"
    return type(e).__name__


# ═══════════════════════════════════════════
# 数据源 1: push2his — 个股全量历史资金流向
# ═══════════════════════════════════════════

def _create_httpx_client():
    """创建 httpx 客户端，绕过系统代理"""
    import httpx
    proxy = os.environ.get("FLOW_PROXY", "")
    kwargs = {
        "verify": False,
        "follow_redirects": True,
        "timeout": 20,
        "http2": False,
    }
    if proxy:
        kwargs["proxy"] = proxy
    return httpx.Client(**kwargs)


def _fetch_push2his_httpx(stock_code: str) -> list[dict] | None:
    """
    用 httpx 从 push2his 获取单只股票全量历史资金流向。
    lmt=0 表示返回所有历史K线。
    返回 [{"stock_code", "trade_date", "main_net_inflow", "sm_net_inflow",
            "mid_net_inflow", "lg_net_inflow", "max_net_inflow"}, ...]
    """
    import httpx

    cid = 1 if stock_code.startswith('6') else 0
    url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
    params = {
        "lmt": "0",
        "klt": "101",
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "secid": f"{cid}.{stock_code}",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }

    with _create_httpx_client() as client:
        resp = client.get(url, params=params, headers=headers)
        resp.raise_for_status()
        j = resp.json()

    if not j.get("data") or not j["data"].get("klines"):
        return None

    rows = []
    for line in j["data"]["klines"]:
        parts = line.split(",")
        if len(parts) < 6:
            continue
        rows.append({
            "stock_code": stock_code,
            "trade_date": parts[0],
            "main_net_inflow": float(parts[1]),
            "sm_net_inflow": float(parts[2]),
            "mid_net_inflow": float(parts[3]),
            "lg_net_inflow": float(parts[4]),
            "max_net_inflow": float(parts[5]),
        })
    return rows if rows else None


def _fetch_push2his_curl(stock_code: str) -> list[dict] | None:
    """
    用 curl 子进程从 push2his 获取（绕过 Python TLS 问题）。
    某些环境下 curl 能处理 TLS 重协商。
    """
    cid = 1 if stock_code.startswith('6') else 0
    url = (
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?"
        "lmt=0&klt=101&fields1=f1,f2,f3,f7"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&secid={cid}.{stock_code}"
    )

    proc = subprocess.Popen(
        ["curl", "-s", "--max-time", "20",
         "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
         url],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    stdout, _ = proc.communicate()
    if proc.returncode != 0 or not stdout:
        return None

    data = json.loads(stdout.decode("utf-8"))
    if not data.get("data") or not data["data"].get("klines"):
        return None

    rows = []
    for line in data["data"]["klines"]:
        parts = line.split(",")
        if len(parts) < 6:
            continue
        rows.append({
            "stock_code": stock_code,
            "trade_date": parts[0],
            "main_net_inflow": float(parts[1]),
            "sm_net_inflow": float(parts[2]),
            "mid_net_inflow": float(parts[3]),
            "lg_net_inflow": float(parts[4]),
            "max_net_inflow": float(parts[5]),
        })
    return rows if rows else None


def _fetch_push2his_socket(stock_code: str) -> list[dict] | None:
    """
    用原始 socket + SSL 直连 push2his（绕过所有 HTTP 库）。
    手动发送 HTTP 请求，处理 TLS 1.3。
    """
    import socket

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2

    hostname = "push2his.eastmoney.com"
    sock = socket.create_connection((hostname, 443), timeout=15)
    try:
        ssock = ctx.wrap_socket(sock, server_hostname=hostname)
    except Exception:
        sock.close()
        return None

    cid = 1 if stock_code.startswith('6') else 0
    path = (
        "/api/qt/stock/fflow/daykline/get?"
        "lmt=0&klt=101&fields1=f1,f2,f3,f7"
        "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
        f"&secid={cid}.{stock_code}"
    )
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {hostname}\r\n"
        f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\n"
        f"Accept: */*\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    )

    try:
        ssock.sendall(request.encode())
        response = b""
        while True:
            try:
                chunk = ssock.recv(8192)
                if not chunk:
                    break
                response += chunk
            except Exception:
                break
    finally:
        ssock.close()

    if not response:
        return None

    # 解析 chunked 或 content-length 响应
    header_end = response.find(b"\r\n\r\n")
    if header_end < 0:
        return None
    body = response[header_end + 4:]

    # 处理 chunked encoding
    header_str = response[:header_end].decode("utf-8", errors="replace")
    if "chunked" in header_str.lower():
        # 简单的 chunked 解码
        decoded = b""
        while body:
            size_end = body.find(b"\r\n")
            if size_end < 0:
                break
            size_str = body[:size_end].decode("utf-8", errors="replace").strip()
            try:
                size = int(size_str, 16)
            except ValueError:
                break
            if size == 0:
                break
            decoded += body[size_end + 2: size_end + 2 + size]
            body = body[size_end + 2 + size + 2:]
        body = decoded

    if not body:
        return None

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return None

    if not data.get("data") or not data["data"].get("klines"):
        return None

    rows = []
    for line in data["data"]["klines"]:
        parts = line.split(",")
        if len(parts) < 6:
            continue
        rows.append({
            "stock_code": stock_code,
            "trade_date": parts[0],
            "main_net_inflow": float(parts[1]),
            "sm_net_inflow": float(parts[2]),
            "mid_net_inflow": float(parts[3]),
            "lg_net_inflow": float(parts[4]),
            "max_net_inflow": float(parts[5]),
        })
    return rows if rows else None


# ═══════════════════════════════════════════
# 数据源 2: push2 批量接口 — 全市场当天资金流向
# ═══════════════════════════════════════════

def _fetch_batch_today(page: int = 1, page_size: int = 5000) -> list[dict] | None:
    """
    从 push2 批量接口获取全市场当天个股资金流向。
    一次请求最多约 5000+ 只股票。
    字段: f12=代码, f14=名称, f62=主力净流入, f184=主力净占比,
          f66=超大单净流入, f69=超大单净占比, f72=大单净流入, f75=大单净占比,
          f78=中单净流入, f81=中单净占比, f84=小单净流入, f87=小单净占比
    """
    import httpx

    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "fid": "f62",
        "po": "1",
        "pz": str(page_size),
        "pn": str(page),
        "np": "1",
        "fltt": "2",
        "invt": "2",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "fs": "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,"
              "m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:7+f:!2,m:1+t:3+f:!2",
        "fields": "f12,f14,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://data.eastmoney.com/",
    }

    with _create_httpx_client() as client:
        resp = client.get(url, params=params, headers=headers)
        resp.raise_for_status()
        j = resp.json()

    data = j.get("data")
    if not data or not data.get("diff"):
        return None

    today = datetime.now().strftime("%Y-%m-%d")
    rows = []
    for item in data["diff"]:
        code = str(item.get("f12", "")).zfill(6)
        if not code or code == "000000":
            continue
        rows.append({
            "stock_code": code,
            "trade_date": today,
            "main_net_inflow": float(item.get("f62", 0) or 0),
            "sm_net_inflow": float(item.get("f84", 0) or 0),   # 小单
            "mid_net_inflow": float(item.get("f78", 0) or 0),   # 中单
            "lg_net_inflow": float(item.get("f72", 0) or 0),    # 大单
            "max_net_inflow": float(item.get("f66", 0) or 0),   # 超大单
        })
    return rows if rows else None


# ═══════════════════════════════════════════
# 主逻辑: 全量历史爬取
# ═══════════════════════════════════════════

def crawl_full_history(engine, stock_codes: list[str], slow: bool = False):
    """
    逐只股票爬取全量历史资金流向。
    每只股票调用 push2his 获取全部历史日K线。
    """
    delay = SLOW_DELAY if slow else NORMAL_DELAY
    jitter = SLOW_JITTER if slow else NORMAL_JITTER
    batch_pause = SLOW_BATCH_PAUSE if slow else NORMAL_BATCH_PAUSE
    batch_every = SLOW_BATCH_EVERY if slow else NORMAL_BATCH_EVERY

    total = len(stock_codes)
    print(f"\n{'='*60}")
    print(f"  个股资金流向全量历史爬取")
    print(f"  股票总数: {total}")
    print(f"  模式: {'慢速' if slow else '标准'}")
    print(f"  请求间隔: {delay}s, 每{batch_every}只暂停{batch_pause}s")
    print(f"{'='*60}\n")

    # 数据源优先级: httpx -> curl -> socket
    fetch_methods = [
        ("httpx", _fetch_push2his_httpx),
        ("curl", _fetch_push2his_curl),
        ("socket", _fetch_push2his_socket),
    ]
    method_idx = 0

    all_rows = []
    success = 0
    failed = 0
    no_data = 0
    total_rows = 0
    error_types: Counter[str] = Counter()
    consecutive_fail = 0

    t_start = time.time()

    for i, code in enumerate(stock_codes):
        rows = None
        last_error = None
        method_name, fetch_func = fetch_methods[method_idx]

        for attempt in range(1 + MAX_RETRIES):
            try:
                rows = fetch_func(code)
                if rows is not None:
                    break
            except Exception as e:
                last_error = e
                if attempt < MAX_RETRIES:
                    retry_delay = RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 2)
                    time.sleep(retry_delay)

        if rows:
            all_rows.extend(rows)
            success += 1
            total_rows += len(rows)
            consecutive_fail = 0
        elif last_error is not None:
            failed += 1
            consecutive_fail += 1
            category = _classify_error(last_error)
            error_types[f"[{method_name}] {category}"] += 1
            if failed <= 15:
                print(f"  {code} 失败 [{method_name}/{category}]: {last_error}")
        else:
            no_data += 1
            consecutive_fail = 0

        # 连续失败 → 切换数据源
        if consecutive_fail >= CONSECUTIVE_FAIL_THRESHOLD:
            if method_idx < len(fetch_methods) - 1:
                method_idx += 1
                method_name, fetch_func = fetch_methods[method_idx]
                print(f"\n  [!] 连续 {consecutive_fail} 次失败，切换到: {method_name}")
                consecutive_fail = 0
            else:
                cooldown = COOLDOWN_WAIT + random.uniform(0, 60)
                print(f"\n  [!] 所有方法连续失败，冷却 {cooldown:.0f}s ...")
                time.sleep(cooldown)
                method_idx = 0
                method_name, fetch_func = fetch_methods[method_idx]
                consecutive_fail = 0

        # 请求间隔
        time.sleep(delay + random.uniform(0, jitter))

        # 进度报告
        if (i + 1) % 100 == 0:
            elapsed = time.time() - t_start
            rate = (i + 1) / elapsed
            eta = (total - i - 1) / rate if rate > 0 else 0
            print(f"  进度: {i+1}/{total}  成功={success}  失败={failed}  "
                  f"无数据={no_data}  总行数={total_rows}  "
                  f"[{method_name}] {elapsed:.0f}s 已用, 预计还需 {eta/60:.1f}min")

        # 批次暂停
        if batch_every > 0 and (i + 1) % batch_every == 0:
            pause = batch_pause + random.uniform(0, 10)
            print(f"  批次暂停 {pause:.0f}s（已处理 {i+1} 只）...")
            time.sleep(pause)

    # ── 写入数据库 ──
    elapsed_total = time.time() - t_start
    print(f"\n{'='*60}")
    print(f"  爬取完成")
    print(f"  总数: {total}")
    print(f"  成功: {success} ({success/total*100:.1f}%)")
    print(f"  失败: {failed} ({failed/total*100:.1f}%)")
    if error_types:
        print(f"  失败分类:")
        for cat, cnt in error_types.most_common():
            print(f"    - {cat}: {cnt}")
    print(f"  无数据: {no_data} ({no_data/total*100:.1f}%)")
    print(f"  总数据行数: {total_rows}")
    print(f"  耗时: {elapsed_total:.0f}s ({elapsed_total/60:.1f}min)")
    print(f"{'='*60}")

    if not all_rows:
        print("\n未获取到任何数据")
        return

    # 写入数据库
    _save_to_db(engine, all_rows, mode="full")


def crawl_today_batch(engine):
    """
    用批量接口一次获取全市场当天资金流向。
    """
    print(f"\n{'='*60}")
    print(f"  全市场当天资金流向（批量模式）")
    print(f"{'='*60}\n")

    rows = None
    for attempt in range(3):
        try:
            rows = _fetch_batch_today()
            if rows:
                break
        except Exception as e:
            print(f"  尝试 {attempt+1}/3 失败: {e}")
            time.sleep(5)

    if not rows:
        print("  批量接口获取失败")
        return

    today = rows[0]["trade_date"]
    print(f"  获取到 {len(rows)} 只股票的资金流向数据")
    print(f"  日期: {today}")

    # 统计
    main_inflow = sum(r["main_net_inflow"] for r in rows)
    positive = sum(1 for r in rows if r["main_net_inflow"] > 0)
    print(f"  主力净流入合计: {main_inflow:,.0f}")
    print(f"  主力流入个股: {positive}/{len(rows)}")

    _save_to_db(engine, rows, mode="today", target_date=today)


def _save_to_db(engine, rows: list[dict], mode: str = "full", target_date: str = None):
    """保存数据到数据库"""
    df = pd.DataFrame(rows)
    df = df.replace({np.nan: None, pd.NaT: None})
    df = df.drop_duplicates(subset=["stock_code", "trade_date"], keep="last")

    df["etl_sync_at"] = datetime.now().replace(microsecond=0)
    replace_table_rows_exact_keys(
        df,
        "sm_stock_capital_flow_daily",
        engine,
        key_columns=("stock_code", "trade_date"),
        lock_name=CAPITAL_FLOW_DAILY_FREEZE_LOCK_NAME,
        chunksize=500,
    )

    print(f"\n  ✅ 写入完成: sm_stock_capital_flow_daily, 共 {len(df)} 行")


# ═══════════════════════════════════════════
# 入口
# ═══════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="个股资金流向全量爬取",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--today-only", action="store_true",
                        help="只获取今天的数据（批量模式，一次全市场）")
    parser.add_argument("--start", default=None,
                        help="起始日期 YYYY-MM-DD（全量模式用）")
    parser.add_argument("--end", default=None,
                        help="结束日期 YYYY-MM-DD（全量模式用）")
    parser.add_argument("--limit", type=int, default=0,
                        help="限制股票数量（测试用，0=不限制）")
    parser.add_argument("--slow", action="store_true",
                        help="慢速模式（被限流时使用）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只显示计划，不实际执行")
    args = parser.parse_args()

    engine = create_tool_engine(_mysql_url())

    if args.today_only:
        crawl_today_batch(engine)
    else:
        stock_codes = _read_stock_codes(engine)
        if args.limit > 0:
            stock_codes = stock_codes[:args.limit]
        if args.dry_run:
            print(f"计划爬取 {len(stock_codes)} 只股票的全量历史资金流向")
            return
        crawl_full_history(engine, stock_codes, slow=args.slow)


if __name__ == "__main__":
    main()
