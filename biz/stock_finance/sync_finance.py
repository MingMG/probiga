"""
股票财务核心指标同步脚本

调用 adata stock.finance.get_core_index() 获取东方财富财务数据，
写入 si_stock_finance 表。

用法：
    python biz/stock_finance/sync_finance.py                 # 全量同步（增量：只拉新报告期）
    python biz/stock_finance/sync_finance.py --code 600396   # 同步单只股票
    python biz/stock_finance/sync_finance.py --limit 100      # 只同步前100只
"""

import argparse
import sys
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import text

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

from server.common.batch_db import create_batch_engine, read_frame
from server.common.pit_facts import append_finance_revision, append_source_coverage


def get_engine():
    return create_batch_engine(pool_size=5, max_overflow=10)


def get_all_stock_codes(engine) -> list:
    """获取全市场股票代码"""
    df = read_frame(
        text("SELECT stock_code FROM si_all_code WHERE stock_code REGEXP '^(0|3|6)' ORDER BY stock_code"),
        engine,
    )
    return df["stock_code"].tolist()


def fetch_finance(stock_code: str) -> pd.DataFrame:
    """调用 adata 获取单只股票的财务核心指标"""
    try:
        from adata.stock.finance import finance
        df = finance.get_core_index(stock_code)
        if df is None or df.empty:
            return pd.DataFrame()
        return df
    except Exception as e:
        raise RuntimeError(f"{stock_code} 财务源请求失败") from e


def upsert_finance(
    engine,
    df: pd.DataFrame,
    *,
    stock_code: str | None = None,
    observed_at: datetime | None = None,
) -> int:
    """Append immutable PIT revisions before refreshing the legacy cache."""
    df = df if df is not None else pd.DataFrame()
    now_dt = (observed_at or datetime.now()).replace(microsecond=0)
    now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    batch_id = f"adata-finance-{now_dt.strftime('%Y%m%dT%H%M%S')}"
    cols = [
        "stock_code", "short_name", "report_date", "report_type", "notice_date",
        "basic_eps", "diluted_eps", "non_gaap_eps", "net_asset_ps",
        "cap_reserve_ps", "undist_profit_ps", "oper_cf_ps",
        "total_rev", "gross_profit", "net_profit_attr_sh", "non_gaap_net_profit",
        "total_rev_yoy_gr", "net_profit_yoy_gr", "non_gaap_net_profit_yoy_gr",
        "total_rev_qoq_gr", "net_profit_qoq_gr",
        "roe_wtd", "roe_non_gaap_wtd", "roa_wtd", "gross_margin", "net_margin",
        "curr_ratio", "quick_ratio", "cash_flow_ratio", "asset_liab_ratio",
    ]

    # 只保留存在的列
    available = [c for c in cols if c in df.columns]
    frame = df[available].copy() if available else pd.DataFrame()

    # 数值列转为 float（防止 pandas object 类型）
    for c in frame.columns:
        if c not in ("stock_code", "short_name", "report_date", "report_type", "notice_date"):
            frame[c] = pd.to_numeric(frame[c], errors="coerce")

    codes = sorted({
        str(value).strip().zfill(6)
        for value in (frame.get("stock_code", pd.Series(dtype=str)).tolist())
        if str(value).strip()
    })
    requested_code = str(stock_code or "").strip().zfill(6)
    if requested_code and requested_code != "000000":
        if codes and codes != [requested_code]:
            raise ValueError("finance response stock identity differs from request")
        codes = [requested_code]
    if not codes:
        raise ValueError("finance coverage requires the requested stock code")
    if len(codes) != 1:
        raise ValueError("finance coverage transaction must contain one stock")

    if frame.empty:
        with engine.begin() as conn:
            append_source_coverage(
                conn,
                fact_kind="finance",
                stock_code=codes[0],
                window_start="1900-01-01",
                window_end=now_dt.date(),
                known_at=now_dt,
                received_at=now_dt,
                covered_through_at=now_dt,
                watermark_kind="CAPTURED_AT",
                watermark_evidence={
                    "provider": "adata.finance.core_index",
                    "capture": "successful_empty_function_return",
                },
                source_rows=[],
                fact_bindings=[],
                source="adata.finance.core_index",
                batch_id=batch_id,
            )
        return 0

    # 构造 INSERT SQL
    placeholders = ", ".join([f":{c}" for c in available])
    col_names = ", ".join(available)
    update_clause = ", ".join([f"{c} = VALUES({c})" for c in available if c not in ("stock_code", "report_date")])
    update_clause += ", etl_sync_at = VALUES(etl_sync_at)"

    sql = text(f"""
        INSERT INTO si_stock_finance ({col_names}, etl_sync_at)
        VALUES ({placeholders}, :etl_sync_at)
        ON DUPLICATE KEY UPDATE {update_clause}
    """)

    count = 0
    with engine.begin() as conn:
        source_rows: list[dict] = []
        fact_bindings: list[dict] = []
        for _, row in frame.iterrows():
            params = {c: (None if pd.isna(row[c]) else row[c]) for c in available}
            # The append-only fact is the strategy source of truth.  A missing
            # PIT schema or an invalid identity aborts the transaction before
            # the mutable display cache can advance on its own.
            receipt = append_finance_revision(
                conn,
                params,
                known_at=now_dt,
                received_at=now_dt,
                source="adata.finance.core_index",
                batch_id=batch_id,
            )
            source_rows.append(dict(params))
            fact_bindings.append({
                "revision_id": receipt.revision_id,
                "content_hash": receipt.content_hash,
            })
            params["etl_sync_at"] = now
            conn.execute(sql, params)
            count += 1
        append_source_coverage(
            conn,
            fact_kind="finance",
            stock_code=codes[0],
            window_start="1900-01-01",
            window_end=now_dt.date(),
            known_at=now_dt,
            received_at=now_dt,
            covered_through_at=now_dt,
            watermark_kind="CAPTURED_AT",
            watermark_evidence={
                "provider": "adata.finance.core_index",
                "capture": "successful_function_return",
            },
            source_rows=source_rows,
            fact_bindings=fact_bindings,
            source="adata.finance.core_index",
            batch_id=batch_id,
        )

    return count


def main():
    parser = argparse.ArgumentParser(description="同步股票财务核心指标")
    parser.add_argument("--code", type=str, default=None, help="同步单只股票代码")
    parser.add_argument("--limit", type=int, default=None, help="只同步前N只")
    parser.add_argument("--sleep", type=float, default=0.3, help="每只股票间隔秒数（防限流）")
    args = parser.parse_args()

    engine = get_engine()

    if args.code:
        codes = [args.code.strip().zfill(6)]
    else:
        codes = get_all_stock_codes(engine)
        if args.limit:
            codes = codes[: args.limit]

    print(f"[INFO] 待同步 {len(codes)} 只股票")

    total_rows = 0
    fail_count = 0
    for i, code in enumerate(codes):
        try:
            df = fetch_finance(code)
            rows = upsert_finance(engine, df, stock_code=code)
            total_rows += rows
        except Exception as exc:
            print(f"  [WARN] {code} 获取/写入失败: {exc}")
            fail_count += 1

        if (i + 1) % 50 == 0:
            print(f"[PROGRESS] {i + 1}/{len(codes)}, 已写入 {total_rows} 条, 失败 {fail_count}")

        if args.sleep > 0 and i < len(codes) - 1:
            time.sleep(args.sleep)

    print(f"[OK] 同步完成: {len(codes)} 只股票, 写入 {total_rows} 条报告期, 失败 {fail_count}")


if __name__ == "__main__":
    main()
