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

``--mysql``：只向发布期已准备并验证的 ``probiga.sm_rt_quote_snapshot`` 追加写入，连接串使用 ``MYSQL_URL`` 或项目根目录 ``.env``。

与官方文档对齐的字段：stock_code, short_name, price, change, change_pct, volume, amount。
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.config import get_mysql_url
from server.common.runtime_table_schema import (
    RuntimeColumn,
    RuntimeIndex,
    RuntimeTable,
    privileged_normalize_mysql_storage,
    validate_runtime_tables,
)

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


def fetch_list_market_current(code_list: list[str]) -> pd.DataFrame:
    """拉取多股最新行情（新浪）。"""
    codes = [str(c).strip().zfill(6) for c in code_list if str(c).strip()]
    if not codes:
        return pd.DataFrame(columns=_MARKET_CURRENT_COLUMNS)

    api_url = "https://hq.sinajs.cn/list="
    for code in codes:
        ex = _exchange_for_code(code).lower()
        api_url += f"s_{ex}{code},"

    res = requests.get(api_url, headers=_HEADERS, timeout=30)
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


_RT_SNAPSHOT_DDL = """
CREATE TABLE IF NOT EXISTS sm_rt_quote_snapshot (
    id BIGINT NOT NULL AUTO_INCREMENT,
    stock_code VARCHAR(16) NOT NULL,
    short_name VARCHAR(128) NULL,
    price DECIMAL(50,6) NULL,
    `change` DECIMAL(50,6) NULL,
    change_pct DECIMAL(50,6) NULL,
    volume DECIMAL(50,6) NULL,
    amount DECIMAL(50,6) NULL,
    snapshot_at DATETIME NOT NULL,
    PRIMARY KEY (id),
    KEY idx_rt_snap_code (stock_code),
    KEY idx_rt_snap_time (snapshot_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""
_RT_SNAPSHOT_CONTRACT = {
    "sm_rt_quote_snapshot": RuntimeTable(
        columns={
            "id": RuntimeColumn("bigint", False, numeric_precision=19, numeric_scale=0, auto_increment=True),
            "stock_code": RuntimeColumn("varchar", False, character_length=16),
            "short_name": RuntimeColumn("varchar", True, character_length=128),
            "price": RuntimeColumn("decimal", True, numeric_precision=50, numeric_scale=6),
            "change": RuntimeColumn("decimal", True, numeric_precision=50, numeric_scale=6),
            "change_pct": RuntimeColumn("decimal", True, numeric_precision=50, numeric_scale=6),
            "volume": RuntimeColumn("decimal", True, numeric_precision=50, numeric_scale=6),
            "amount": RuntimeColumn("decimal", True, numeric_precision=50, numeric_scale=6),
            "snapshot_at": RuntimeColumn("datetime", False, datetime_precision=0),
        },
        indexes=(
            RuntimeIndex(("id",), unique=True),
            RuntimeIndex(("stock_code",), unique=False),
            RuntimeIndex(("snapshot_at",), unique=False),
        ),
    )
}


def validate_rt_snapshot_runtime(engine) -> dict[str, object]:
    validate_runtime_tables(
        engine,
        _RT_SNAPSHOT_CONTRACT,
        context="realtime quote snapshot",
    )
    return {
        "schema": "probiga.realtime-quote-snapshot.v1",
        "status": "HEALTHY",
        "table_count": 1,
        "physical_schema_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def privileged_migrate_rt_snapshot_table(engine) -> dict[str, object]:
    with engine.begin() as conn:
        conn.execute(text(_RT_SNAPSHOT_DDL))
        privileged_normalize_mysql_storage(conn, _RT_SNAPSHOT_CONTRACT)
    return validate_rt_snapshot_runtime(engine)


def _ensure_rt_snapshot_table(engine) -> None:
    """Compatibility runtime guard; persistent DDL is release-only."""
    validate_rt_snapshot_runtime(engine)


def save_to_mysql(df: pd.DataFrame, *, run_ddl: bool) -> int:
    engine = create_engine(get_mysql_url(required=True), pool_pre_ping=True, future=True)
    if run_ddl:
        raise RuntimeError(
            "runtime DDL is disabled; run privileged_migrate_rt_snapshot_table during release"
        )
    _ensure_rt_snapshot_table(engine)
    ts = datetime.now().replace(microsecond=0)
    out = df.copy()
    out["snapshot_at"] = ts
    out.to_sql("sm_rt_quote_snapshot", engine, if_exists="append", index=False, chunksize=500, method="multi")
    return len(out)


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
        help="兼容旧参数；运行时始终不执行DDL，表结构由发布迁移准备",
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
        n = save_to_mysql(df, run_ddl=False)
        print(f"\n已写入 MySQL：sm_rt_quote_snapshot，{n} 行（snapshot_at 为抓取时间）。")


if __name__ == "__main__":
    main()
