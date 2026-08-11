#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接调用东财 API 同步个股日度资金流向，绕过 adata 限流。

用法：
  python tools/sync_capital_flow_direct.py
  python tools/sync_capital_flow_direct.py --limit 100
  python tools/sync_capital_flow_direct.py --date 2026-05-29

环境变量：MYSQL_URL（默认 root:123456@localhost/probiga）
"""
from __future__ import annotations
from env_config import create_tool_engine, resolve_tool_mysql_url

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import bindparam, text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import write_frame

def _engine():
    return create_tool_engine(resolve_tool_mysql_url())


def fetch_flow_east(stock_code: str) -> pd.DataFrame | None:
    """直接调东财 push2his API 获取个股日度资金流（最近120天）"""
    cid = 1 if stock_code.startswith("6") else 0
    url = (
        f"https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get?"
        f"lmt=0&klt=101&fields1=f1,f2,f3,f7&"
        f"fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61&"
        f"secid={cid}.{stock_code}"
    )
    try:
        data = None
        urls = [url.replace("push2his.eastmoney.com", "push2delay.eastmoney.com"), url]
        for candidate in urls:
            try:
                resp = requests.get(candidate, headers={}, timeout=15)
                resp.raise_for_status()
                data = resp.json().get("data")
            except Exception:
                # Eastmoney may reset Python TLS connections while curl still
                # succeeds on the same machine. Keep the source fallback local.
                curl_bin = shutil.which("curl") or shutil.which("curl.exe")
                if not curl_bin:
                    continue
                proc = subprocess.run(
                    [curl_bin, "-sS", "--max-time", "15", "-A", "Mozilla/5.0", candidate],
                    capture_output=True,
                    timeout=20,
                    check=False,
                )
                if proc.returncode == 0 and proc.stdout:
                    try:
                        data = json.loads(proc.stdout.decode("utf-8", errors="replace")).get("data")
                    except json.JSONDecodeError:
                        data = None
            if data and data.get("klines"):
                break
        if not data or "klines" not in data:
            return None
        lines = data["klines"]
        if not lines:
            return None
        # 格式: '2026-05-29,-58234405.0,47874618.0,10359788.0,-13362003.0,-44872402.0,...'
        rows = []
        for line in lines:
            parts = line.split(",")
            if len(parts) >= 6:
                rows.append([stock_code, parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]])
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=[
            "stock_code", "trade_date", "main_net_inflow", "max_net_inflow",
            "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"
        ])
        for col in ["main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser(description="直接调东财 API 同步资金流数据")
    p.add_argument("--limit", type=int, default=0, help="最多处理几只股票（0=全部）")
    p.add_argument("--offset", type=int, default=0, help="股票池偏移量，用于并行分片补数")
    p.add_argument("--date", type=str, default="", help="只保留指定日期的数据（YYYY-MM-DD）")
    p.add_argument("--sleep", type=float, default=0.3, help="每只股票间隔秒数")
    p.add_argument("--skip-truncate", action="store_true", help="不清空表，增量写入")
    p.add_argument("--codes", type=str, default="", help="逗号分隔的股票代码，仅修复指定代码")
    args = p.parse_args()

    eng = _engine()

    # 获取股票列表
    if args.codes.strip():
        codes = sorted({
            code.strip().zfill(6)
            for code in args.codes.split(",")
            if code.strip()
        })
    else:
        with eng.connect() as conn:
            codes = [row[0] for row in conn.execute(
                text("SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|3|6)' ORDER BY stock_code")
            ).fetchall()]

    if args.offset > 0:
        codes = codes[args.offset:]
    if args.limit > 0:
        codes = codes[:args.limit]

    print(f"共 {len(codes)} 只股票，开始同步资金流...")

    # 清空表
    if not args.skip_truncate:
        with eng.begin() as conn:
            conn.execute(text("TRUNCATE TABLE sm_stock_capital_flow_daily"))
        print("已清空 sm_stock_capital_flow_daily")

    total_rows = 0
    success = 0
    fail = 0
    pending: list[dict] = []

    def flush_pending() -> None:
        if not pending:
            return
        frame = pd.DataFrame(pending)
        if args.skip_truncate:
            # Fetch first, then replace only successfully fetched rows in one
            # transaction. A network failure can no longer delete good data.
            frame_codes = sorted(frame["stock_code"].astype(str).str.zfill(6).unique().tolist())
            with eng.begin() as conn:
                if args.date:
                    delete_stmt = text("""
                        DELETE FROM sm_stock_capital_flow_daily
                        WHERE trade_date = :trade_date AND stock_code IN :codes
                    """).bindparams(bindparam("codes", expanding=True))
                    conn.execute(
                        delete_stmt,
                        {"trade_date": args.date[:10], "codes": frame_codes},
                    )
                else:
                    delete_stmt = text("""
                        DELETE FROM sm_stock_capital_flow_daily
                        WHERE stock_code = :stock_code AND trade_date IN :dates
                    """).bindparams(bindparam("dates", expanding=True))
                    for stock_code, group in frame.groupby("stock_code"):
                        dates = sorted(group["trade_date"].astype(str).str[:10].unique().tolist())
                        conn.execute(
                            delete_stmt,
                            {"stock_code": str(stock_code).zfill(6), "dates": dates},
                        )
                write_frame(
                    frame,
                    "sm_stock_capital_flow_daily",
                    conn,
                    if_exists="append",
                    index=False,
                    chunksize=500,
                    method="multi",
                )
        else:
            write_frame(
                frame,
                "sm_stock_capital_flow_daily",
                eng,
                if_exists="append",
                index=False,
                chunksize=500,
                method="multi",
            )
        pending.clear()

    for i, code in enumerate(codes):
        code = str(code).strip().zfill(6)
        df = fetch_flow_east(code)

        if df is not None and not df.empty:
            # 按日期筛选
            if args.date:
                df = df[df["trade_date"].astype(str).str[:10] == args.date[:10]]

            if not df.empty:
                df["etl_sync_at"] = datetime.now().replace(microsecond=0)
                pending.extend(df.to_dict(orient="records"))
                total_rows += len(df)
                success += 1
        else:
            fail += 1

        if (i + 1) % 100 == 0:
            flush_pending()
            print(f"  进度: {i+1}/{len(codes)} | 成功: {success} | 失败: {fail} | 写入: {total_rows} 行")

        time.sleep(args.sleep)

    print(f"\n完成！共 {success} 只成功，{fail} 只失败，写入 {total_rows} 行")
    print(f"最新日期: {args.date or '全部'}")
    flush_pending()


if __name__ == "__main__":
    main()
