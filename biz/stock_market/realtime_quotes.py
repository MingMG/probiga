# -*- coding: utf-8 -*-
"""
A 股多股「最新价」快照（新浪 ``hq.sinajs.cn``，与 adata 文档中 ``list_market_current`` 同源思路）。

- **盘中**：价格、涨跌、量额随交易刷新，适合验证实时链路。
- **非交易时段**：多为昨收或延迟数据，属正常现象。

不依赖 ``import adata.stock.market``（避免整包行情依赖 py_mini_racer 等），仅需 ``requests``、``pandas``。

示例::

    python -m biz.stock_market.realtime_quotes
    python -m biz.stock_market.realtime_quotes --codes 000001,600519,300750,920001
    python -m biz.stock_market.realtime_quotes --codes 000001 --csv quotes.csv
    python -m biz.stock_market.realtime_quotes --mysql
    python -m biz.stock_market.realtime_quotes --mysql --no-rt-ddl

``--mysql``：追加写入 ``probiga.sm_rt_quote_snapshot``（见 ``biz/stock_market/sql/01_sm_rt_quote_snapshot.sql``），连接串使用 ``MYSQL_URL`` 或项目根目录 ``.env``。

与官方文档对齐的字段：stock_code, short_name, price, change, change_pct, volume, amount。
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine, write_frame

# 与 adata.common.utils.code_utils.exchange_suffix 一致（6 位代码前两位 -> 交易所字母）
_EXCHANGE_SUFFIX = {
    "00": "SZ",
    "20": "SZ",
    "30": "SZ",
    "43": "BJ",
    "60": "SH",
    "68": "SH",
    "83": "BJ",
    "87": "BJ",
    "90": "SH",
    "92": "BJ",
}

_MARKET_CURRENT_COLUMNS = [
    "stock_code",
    "short_name",
    "price",
    "change",
    "change_pct",
    "volume",
    "amount",
]

_HEADERS = {
    "Host": "hq.sinajs.cn",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://finance.sina.com.cn/",
}


def _exchange_for_code(code: str) -> str:
    c = str(code).strip().zfill(6)
    p = c[0:2]
    if p not in _EXCHANGE_SUFFIX:
        return "SZ"
    return _EXCHANGE_SUFFIX[p]


def fetch_list_market_current(
    code_list: list[str],
    *,
    timeout_seconds: float = 30,
) -> pd.DataFrame:
    """拉取多股最新行情（新浪）。"""
    codes = [str(c).strip().zfill(6) for c in code_list if str(c).strip()]
    if not codes:
        return pd.DataFrame(columns=_MARKET_CURRENT_COLUMNS)

    api_url = "https://hq.sinajs.cn/list="
    for code in codes:
        ex = _exchange_for_code(code).lower()
        api_url += f"s_{ex}{code},"

    res = requests.get(api_url, headers=_HEADERS, timeout=max(1.0, float(timeout_seconds)))
    if res.status_code != 200 or len(res.text) < 1:
        return pd.DataFrame(columns=_MARKET_CURRENT_COLUMNS)

    data_list = res.text.split(";")
    rows: list[list] = []
    for data_str in data_list:
        if len(data_str) < 8 or "=" not in data_str:
            continue
        idx = data_str.index("=")
        code = [data_str[idx - 6 : idx]]
        code.extend(data_str[idx + 2 : -1].split(","))
        if len(code) == 7:
            rows.append(code)

    if not rows:
        return pd.DataFrame(columns=_MARKET_CURRENT_COLUMNS)

    result_df = pd.DataFrame(data=rows, columns=_MARKET_CURRENT_COLUMNS)
    for col in ("price", "change", "change_pct", "volume", "amount"):
        result_df[col] = pd.to_numeric(result_df[col], errors="coerce")
    mask = result_df["stock_code"].astype(str).str.startswith(("0", "3", "6", "9"))
    result_df.loc[mask, "volume"] = result_df.loc[mask, "volume"].fillna(0) * 100
    result_df.loc[mask, "amount"] = result_df.loc[mask, "amount"].fillna(0) * 10000
    return result_df


_RT_DDL_PATH = Path(__file__).resolve().parent / "sql" / "01_sm_rt_quote_snapshot.sql"
def _ensure_rt_snapshot_table(engine) -> None:
    if not _RT_DDL_PATH.is_file():
        raise FileNotFoundError(f"缺少建表 SQL：{_RT_DDL_PATH}")
    sql = _RT_DDL_PATH.read_text(encoding="utf-8")
    lines = []
    for line in sql.splitlines():
        s = line.strip()
        if s.startswith("--") or s.upper().startswith("USE "):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)
    parts = [p.strip() for p in re.split(r";\s*\n", cleaned) if p.strip()]
    with engine.begin() as conn:
        for stmt in parts:
            conn.execute(text(stmt))


def save_to_mysql(df: pd.DataFrame, *, run_ddl: bool, engine=None) -> int:
    engine = engine or create_batch_engine(future=True)
    if run_ddl:
        _ensure_rt_snapshot_table(engine)
    ts = datetime.now().replace(microsecond=0)
    out = df.copy()
    out["snapshot_at"] = ts
    return write_frame(out, "sm_rt_quote_snapshot", engine, if_exists="append", index=False, chunksize=500, method="multi")


def main() -> None:
    p = argparse.ArgumentParser(description="A股多股最新价快照（新浪，适合盘中验证）")
    p.add_argument(
        "--codes",
        default="000001,600519",
        help="逗号分隔 6 位股票代码，建议单次不超过 500 个（与文档一致）",
    )
    p.add_argument("--csv", default="", help="若指定路径则写入 CSV")
    p.add_argument(
        "--mysql",
        action="store_true",
        help="追加写入 MySQL 表 sm_rt_quote_snapshot（需 pymysql；见 MYSQL_URL）",
    )
    p.add_argument(
        "--no-rt-ddl",
        action="store_true",
        help="与 --mysql 合用：不执行建表 SQL（表已存在时）",
    )
    args = p.parse_args()
    code_list = [x for x in args.codes.split(",") if x.strip()]
    df = fetch_list_market_current(code_list)
    if df.empty:
        print("未取到数据（检查代码、网络，或非交易时段数据源未更新）。")
        sys.exit(1)
    print(df.to_string(index=False))
    if args.csv:
        df.to_csv(args.csv, index=False, encoding="utf-8-sig")
        print(f"\n已写入: {args.csv}")
    if args.mysql:
        n = save_to_mysql(df, run_ddl=not args.no_rt_ddl)
        print(f"\n已写入 MySQL：sm_rt_quote_snapshot，{n} 行（snapshot_at 为抓取时间）。")


if __name__ == "__main__":
    main()
