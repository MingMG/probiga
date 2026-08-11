#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取指定快照日期的雪球热股TOP100，写入 st_hot_rank_xq。
自动为表添加 snapshot_date 列，不删除历史数据。

数据源: xueqiu.com/service/v5/stock/hot_stock/list
  - 需要先访问 xueqiu.com 获取cookie
  - 返回100只雪球热搜股票（API上限100）
  - 字段: 排名、股票代码、名称、涨跌幅、当前价、成交额、市值、关注人数等
"""

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests as http
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from server.common.batch_db import create_batch_engine, replace_table_rows

_SESSION = http.Session()
_SESSION.trust_env = False
_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
})


def _ensure_snapshot_date_column(engine):
    with engine.connect() as conn:
        r = conn.execute(
            text("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'st_hot_rank_xq' AND column_name = 'snapshot_date'")
        ).scalar()
    if int(r or 0) == 0:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE `st_hot_rank_xq` ADD COLUMN `snapshot_date` DATE NOT NULL COMMENT '快照日期' AFTER `diff`"))
        print("已为 st_hot_rank_xq 添加 snapshot_date 列")


def _init_cookie():
    for attempt in range(3):
        try:
            _SESSION.get("https://xueqiu.com/", timeout=30)
            return
        except Exception:
            if attempt == 2:
                raise


def _fetch_hot_rank_xq() -> pd.DataFrame | None:
    url = "https://xueqiu.com/service/v5/stock/hot_stock/list"
    params = {
        "size": 100,
        "_type": 10,
        "type": 10,
    }
    hdrs = {
        "Referer": "https://xueqiu.com/",
        "Origin": "https://xueqiu.com",
        "X-Requested-With": "XMLHttpRequest",
    }
    resp = _SESSION.get(url, params=params, headers=hdrs, timeout=30)
    resp.raise_for_status()
    j = resp.json()

    error_code = j.get("error_code", 0)
    if error_code != 0:
        print(f"  雪球API返回错误: code={error_code}, msg={j.get('error_description', '')}")
        return None

    items = j.get("data", {}).get("items", [])
    if not items:
        return None

    rows = []
    for item in items:
        symbol = str(item.get("symbol", "")).upper().strip()
        matched = re.fullmatch(r"(?:SH|SZ|BJ)([0-9]{6})", symbol)
        if not matched:
            continue
        stock_code = matched.group(1)
        if not stock_code.startswith(("0", "3", "6", "4", "8")):
            continue
        rows.append({
            "rank": len(rows) + 1,
            "stock_code": stock_code,
            "short_name": item.get("name", ""),
            "current": float(item.get("current", 0) or 0),
            "percent": float(item.get("percent", 0) or 0),
            "chg": float(item.get("chg", 0) or 0),
            "amount": float(item.get("amount", 0) or 0),
            "market_capital": float(item.get("market_capital", 0) or 0),
            "followers": int(item.get("following", 0) or 0),
            "sector": item.get("level1", ""),
            "exchange": item.get("exchange", ""),
            "increment": int(item.get("increment", 0) or 0),
            "diff": int(item.get("diff", 0) or 0),
        })

    return pd.DataFrame(rows)


def fetch_hot_rank_xq(snapshot_date: str):
    print(f"开始获取雪球热股TOP100，快照日期: {snapshot_date}")

    engine = create_batch_engine()

    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'st_hot_rank_xq'")
        ).scalar()
    if int(exists or 0) == 0:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE `st_hot_rank_xq` (
                    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                    `snapshot_date` DATE NOT NULL COMMENT '快照日期',
                    `rank` INT DEFAULT NULL COMMENT '排名',
                    `stock_code` VARCHAR(10) DEFAULT NULL COMMENT '股票代码',
                    `short_name` VARCHAR(50) DEFAULT NULL COMMENT '股票简称',
                    `current` DECIMAL(12,4) DEFAULT NULL COMMENT '当前价',
                    `percent` DECIMAL(8,4) DEFAULT NULL COMMENT '涨跌幅(%)',
                    `chg` DECIMAL(12,4) DEFAULT NULL COMMENT '涨跌额',
                    `amount` DECIMAL(20,2) DEFAULT NULL COMMENT '成交额',
                    `market_capital` DECIMAL(20,2) DEFAULT NULL COMMENT '市值',
                    `followers` INT DEFAULT NULL COMMENT '关注人数',
                    `sector` VARCHAR(50) DEFAULT NULL COMMENT '所属行业',
                    `exchange` VARCHAR(10) DEFAULT NULL COMMENT '交易所(SH/SZ)',
                    `increment` INT DEFAULT NULL COMMENT '新增关注',
                    `diff` INT DEFAULT NULL COMMENT '排名变化',
                    `etl_sync_at` DATETIME DEFAULT NULL COMMENT '同步时间',
                    PRIMARY KEY (`id`),
                    KEY `idx_snapshot_date` (`snapshot_date`),
                    KEY `idx_stock_code` (`stock_code`),
                    KEY `idx_snapshot_stock` (`snapshot_date`, `stock_code`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='雪球热股TOP100'
            """))
        print("已创建 st_hot_rank_xq 表")
    else:
        _ensure_snapshot_date_column(engine)

    _init_cookie()

    df = None
    for attempt in range(3):
        try:
            df = _fetch_hot_rank_xq()
            break
        except Exception as e:
            if attempt == 2:
                raise
            print(f"  雪球获取失败(第{attempt+1}次): {e}")
            import time
            time.sleep(3)
            _init_cookie()

    if df is None or df.empty:
        raise RuntimeError("no Xueqiu hot rank rows fetched")

    df = df.copy()
    df["snapshot_date"] = snapshot_date
    for c in ["current", "percent", "chg", "amount", "market_capital"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["followers", "increment", "diff"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    df = df.replace({np.nan: None, pd.NaT: None})
    df["etl_sync_at"] = datetime.now().replace(microsecond=0)

    df = df[["snapshot_date", "rank", "stock_code", "short_name", "current", "percent",
             "chg", "amount", "market_capital", "followers", "sector", "exchange",
             "increment", "diff", "etl_sync_at"]]

    if len(df) < int(os.environ.get("HOT_RANK_XQ_MIN_ROWS", "50")):
        raise RuntimeError(f"Xueqiu hot rank returned too few rows: {len(df)}")
    replace_table_rows(
        df, "st_hot_rank_xq", engine,
        where_sql="snapshot_date = :d", params={"d": snapshot_date}, chunksize=500,
    )

    print(f"写入完成: st_hot_rank_xq, 共 {len(df)} 行, 快照日期: {snapshot_date}")
    if not df.empty:
        print(f"  示例: {df.iloc[0]['stock_code']} {df.iloc[0]['short_name']} 涨跌:{df.iloc[0]['percent']}%")


def main() -> int:
    parser = argparse.ArgumentParser(description="获取指定日期的雪球热股TOP100（写入 st_hot_rank_xq）")
    parser.add_argument("date", help="快照日期，格式：YYYY-MM-DD")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}")
        return 1

    try:
        fetch_hot_rank_xq(args.date)
    except Exception as exc:
        print(f"Xueqiu hot rank sync failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
