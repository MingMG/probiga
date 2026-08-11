#!/usr/bin/env python3
from env_config import create_tool_engine, resolve_tool_mysql_url
# -*- coding: utf-8 -*-
"""
获取指定快照日期的东财人气榜TOP100，写入 st_hot_pop_rank_east。
自动为表添加 snapshot_date 列，不删除历史数据。
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
_ROOT_STR = str(ROOT)
if _ROOT_STR not in sys.path:
    sys.path.insert(0, _ROOT_STR)

from server.common.batch_db import replace_table_rows

def _ensure_snapshot_date_column(engine):
    with engine.connect() as conn:
        r = conn.execute(
            text("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'st_hot_pop_rank_east' AND column_name = 'snapshot_date'")
        ).scalar()
    if int(r or 0) == 0:
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE `st_hot_pop_rank_east` ADD COLUMN `snapshot_date` DATE NOT NULL COMMENT '快照日期' AFTER `change_pct`"))
        print("已为 st_hot_pop_rank_east 添加 snapshot_date 列")


def fetch_hot_pop_rank_east(snapshot_date: str):
    import requests
    import time as _time

    print(f"开始获取东财人气榜TOP100，快照日期: {snapshot_date}")

    engine = create_tool_engine(resolve_tool_mysql_url())
    _ensure_snapshot_date_column(engine)

    url = "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Origin": "https://guba.eastmoney.com",
        "Referer": "https://guba.eastmoney.com/",
    }
    params = {
        "appId": "appId01",
        "globalId": "786e4c21-70dc-435a-93bb-38",
        "marketType": "",
        "pageNo": 1,
        "pageSize": 100,
        "date": snapshot_date.replace("-", ""),
    }

    data = None
    session = requests.Session()
    session.trust_env = False
    for attempt in range(1, 4):
        try:
            r = session.post(url, json=params, headers=headers, timeout=15)
            r.raise_for_status()
            data = r.json().get("data") or []
            if data:
                break
        except Exception as e:
            print(f"  第{attempt}次请求失败: {e}")
        if attempt < 3:
            wait = attempt * 5
            print(f"  等待{wait}秒后重试...")
            _time.sleep(wait)

    if not data:
        raise RuntimeError("no Eastmoney popularity rows fetched")

    rows = []
    for item in data:
        sc = item.get("sc", "")
        stock_code = sc[2:] if len(sc) > 2 else sc
        rc = item.get("rc", 0)
        his_rc = item.get("hisRc", 0)
        # 人气标签
        if rc > 0:
            pop_tag = "排名上升"
        elif rc < 0:
            pop_tag = "排名下降"
        else:
            pop_tag = "排名持平"
        rows.append({
            "rank": item.get("rk", 0),
            "stock_code": stock_code,
            "short_name": "",
            "rank_change": rc,
            "his_rank": his_rc,
            "price": None,
            "price_change": None,
            "change_pct": None,
            "hot_value": round((101 - int(item.get("rk", 100))) / 100 * 100, 1),
            "pop_tag": pop_tag,
            "concept_tag": None,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        code_list = df["stock_code"].tolist()
        code_params = {f"code_{idx}": code for idx, code in enumerate(code_list)}
        in_clause = ",".join(f":code_{idx}" for idx in range(len(code_list)))
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT stock_code, short_name FROM si_all_code WHERE stock_code IN ({in_clause})"),
                    code_params,
                )
                name_map = {row[0]: row[1] for row in result.fetchall()}
            df["short_name"] = df["stock_code"].map(name_map).fillna("")
        except Exception as exc:
            print(f"[WARN] short_name enrichment failed: {exc}", flush=True)

        try:
            with engine.connect() as conn:
                concept_sql = f"""
                    SELECT c.stock_code, GROUP_CONCAT(DISTINCT cp.concept_name ORDER BY cp.`rank` SEPARATOR ';') AS concepts
                    FROM si_concept_constituent_ths c
                    JOIN st_hot_concept_ths_daily cp ON cp.concept_code = c.query_key AND cp.snapshot_date = :d AND cp.plate_type = 1
                    WHERE c.stock_code IN ({in_clause})
                    GROUP BY c.stock_code
                """
                result = conn.execute(text(concept_sql), {"d": snapshot_date, **code_params})
                concept_map = {row[0]: row[1] for row in result.fetchall()}
            df["concept_tag"] = df["stock_code"].map(concept_map)
            concept_filled = sum(1 for c in code_list if c in concept_map)
            print(f"  概念板块关联: {concept_filled}/{len(code_list)} 只")
        except Exception as e:
            print(f"  概念板块查询略过: {e}")

        sina_codes = ",".join([
            ("sh" + c if c.startswith("6") else "sz" + c) for c in code_list
        ])
        try:
            sr = requests.get(
                f"https://hq.sinajs.cn/list={sina_codes}",
                headers={"Referer": "https://finance.sina.com.cn"},
                timeout=15,
            )
            quote_map = {}
            for line in sr.text.strip().split("\n"):
                if "=" not in line or '""' in line:
                    continue
                var_part, val_part = line.split("=", 1)
                code = var_part.split("_")[-1]
                code6 = code[2:]
                fields = val_part.strip('";\r ').split(",")
                if len(fields) >= 4:
                    try:
                        prev_close = float(fields[2])
                        cur_price = float(fields[3])
                        if cur_price <= 0:
                            cur_price = float(fields[1]) or prev_close
                        chg = cur_price - prev_close
                        pct = (chg / prev_close * 100) if prev_close else 0
                        quote_map[code6] = {
                            "price": round(cur_price, 2),
                            "price_change": round(chg, 2),
                            "change_pct": round(pct, 2),
                        }
                    except (ValueError, IndexError):
                        pass
            for col in ["price", "price_change", "change_pct"]:
                df[col] = df["stock_code"].map(lambda c: quote_map.get(c, {}).get(col))
            filled = sum(1 for c in code_list if c in quote_map)
            print(f"  新浪行情补充: {filled}/{len(code_list)} 只股票获取成功")
        except Exception as e:
            print(f"  新浪行情获取失败: {e}")

    df["snapshot_date"] = snapshot_date
    df["etl_sync_at"] = datetime.now().replace(microsecond=0)
    df = df[["snapshot_date", "rank", "stock_code", "short_name", "rank_change", "his_rank", "price", "price_change", "change_pct", "hot_value", "pop_tag", "concept_tag", "etl_sync_at"]]
    df = df.replace({np.nan: None, pd.NaT: None})

    if len(df) < int(os.environ.get("HOT_POP_RANK_MIN_ROWS", "50")):
        raise RuntimeError(f"Eastmoney popularity returned too few rows: {len(df)}")
    replace_table_rows(
        df, "st_hot_pop_rank_east", engine,
        where_sql="snapshot_date = :d", params={"d": snapshot_date}, chunksize=500,
    )

    print(f"写入完成: st_hot_pop_rank_east, 共 {len(df)} 行, 快照日期: {snapshot_date}")
    top10 = ", ".join([f"{r['rank']}.{r['short_name']}" for _, r in df.head(10).iterrows()])
    print(f"  TOP10: {top10}")


def main() -> int:
    parser = argparse.ArgumentParser(description="获取指定日期的东财人气榜TOP100（写入 st_hot_pop_rank_east）")
    parser.add_argument("date", help="快照日期，格式：YYYY-MM-DD")
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"日期格式错误，应为 YYYY-MM-DD，输入: {args.date}")
        return 1

    try:
        fetch_hot_pop_rank_east(args.date)
    except Exception as exc:
        print(f"Eastmoney popularity sync failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
