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
import json
import sys
import time
from datetime import date, datetime

import pandas as pd
from sqlalchemy import text

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

from server.common.batch_db import create_batch_engine, read_frame
from server.common.finance_coverage import (
    FinanceDisclosureGate,
    coerce_optional_date,
    finance_disclosure_gate,
    report_period_gate_applies,
)
from server.common.pit_facts import append_finance_revision, append_source_coverage


def get_engine():
    return create_batch_engine(pool_size=5, max_overflow=10)


def get_finance_stock_universe(engine) -> dict[str, date | None]:
    """Load the current authoritative A-share universe and listing dates."""

    df = read_frame(
        text(
            "SELECT stock_code, list_date FROM si_all_code "
            "WHERE stock_code REGEXP '^(0|3|4|6|8|9)[0-9]{5}$' "
            "AND (list_date IS NULL OR list_date <= CURRENT_DATE) "
            "ORDER BY stock_code"
        ),
        engine,
    )
    universe: dict[str, date | None] = {}
    for row in df.to_dict("records"):
        raw = str(row.get("stock_code") or "").strip()
        if not raw:
            raise RuntimeError("DATA_BLOCKED: finance universe contains empty code")
        code = raw.zfill(6)
        if code in universe:
            raise RuntimeError(
                f"DATA_BLOCKED: finance universe contains duplicate code {code}"
            )
        universe[code] = coerce_optional_date(row.get("list_date"))
    return universe


def get_all_stock_codes(engine) -> list[str]:
    """Backward-compatible ordered code list from the finance universe."""

    return list(get_finance_stock_universe(engine))


def fetch_finance(stock_code: str) -> pd.DataFrame:
    """调用 adata 获取单只股票的财务核心指标"""
    try:
        from adata.stock.finance import finance
        df = finance.get_core_index(stock_code)
        if df is None or df.empty:
            raise RuntimeError(
                f"DATA_BLOCKED: {stock_code} 财务源返回空结果"
            )
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
        raise ValueError("finance provider response must contain at least one row")
    missing_identity = {"stock_code", "report_date"} - set(frame.columns)
    if missing_identity:
        raise ValueError(
            "finance response is missing required identity columns: "
            + ", ".join(sorted(missing_identity))
        )

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


def minimum_expected_report_date(as_of: date) -> date:
    """Backward-compatible accessor for the current disclosure-period floor."""

    return finance_disclosure_gate(as_of).minimum_report_date


def validate_finance_response(
    stock_code: str,
    frame: pd.DataFrame,
    *,
    as_of: date,
    minimum_report_date: date,
    listing_date: date | None = None,
    disclosure_deadline: date | None = None,
) -> date:
    """Validate non-empty provider identity and a reasonable latest period."""

    if frame is None or frame.empty:
        raise RuntimeError(
            f"DATA_BLOCKED: {stock_code} 财务源返回空结果，禁止记录完整覆盖"
        )
    required = {"stock_code", "report_date"}
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(
            f"DATA_BLOCKED: {stock_code} 财务响应缺少字段: "
            + ", ".join(sorted(missing))
        )
    requested = str(stock_code).strip().zfill(6)
    observed_codes = {
        str(value or "").strip().zfill(6)
        for value in frame["stock_code"].tolist()
    }
    if observed_codes != {requested}:
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 财务响应代码集合不一致: "
            f"{sorted(observed_codes)}"
        )
    report_dates = pd.to_datetime(frame["report_date"], errors="coerce").dt.date
    if report_dates.isna().any():
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 财务响应含无效报告期"
        )
    invalid_periods = [
        value for value in report_dates
        if (value.month, value.day) not in {(3, 31), (6, 30), (9, 30), (12, 31)}
    ]
    if invalid_periods:
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 财务响应含非标准报告期: "
            f"{sorted(set(invalid_periods))[:5]}"
        )
    latest = max(report_dates)
    if latest > as_of:
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 最新报告期 {latest} 晚于采集日 {as_of}"
        )
    gate = FinanceDisclosureGate(
        minimum_report_date=minimum_report_date,
        disclosure_deadline=disclosure_deadline or minimum_report_date,
    )
    if report_period_gate_applies(listing_date, gate) and latest < minimum_report_date:
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 最新报告期过旧: latest={latest}, "
            f"minimum={minimum_report_date}"
        )
    return latest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="同步股票财务核心指标")
    parser.add_argument("--code", type=str, default=None, help="同步单只股票代码")
    parser.add_argument("--limit", type=int, default=None, help="只同步前N只")
    parser.add_argument("--sleep", type=float, default=0.3, help="每只股票间隔秒数（防限流）")
    parser.add_argument(
        "--min-code-coverage",
        type=float,
        default=1.0,
        help="非空股票覆盖率；正式任务固定要求 1.0，不能降低",
    )
    parser.add_argument(
        "--min-report-date",
        default="",
        help="最新报告期下限 YYYY-MM-DD；默认按法定披露窗口推导",
    )
    args = parser.parse_args(argv)

    if args.min_code_coverage != 1.0:
        print(
            "[ERROR] DATA_BLOCKED: finance production code coverage is fixed at 1.0",
            file=sys.stderr,
        )
        return 2
    run_as_of = datetime.now().date()
    try:
        disclosure_gate = finance_disclosure_gate(run_as_of)
        if args.min_report_date:
            explicit_minimum = datetime.strptime(
                args.min_report_date, "%Y-%m-%d"
            ).date()
            disclosure_gate = FinanceDisclosureGate(
                minimum_report_date=explicit_minimum,
                disclosure_deadline=run_as_of,
            )
        min_report_date = disclosure_gate.minimum_report_date
    except ValueError:
        print("[ERROR] --min-report-date 必须为 YYYY-MM-DD", file=sys.stderr)
        return 2
    if min_report_date > run_as_of:
        print("[ERROR] --min-report-date 不能晚于当前日期", file=sys.stderr)
        return 2

    engine = get_engine()
    try:
        universe = get_finance_stock_universe(engine)
        if args.code:
            requested_code = args.code.strip().zfill(6)
            if requested_code not in universe:
                print(
                    "[ERROR] DATA_BLOCKED: requested finance stock is absent "
                    f"from si_all_code: {requested_code}"
                )
                return 2
            codes = [requested_code]
        else:
            codes = list(universe)
            if args.limit and args.limit > 0:
                codes = codes[: args.limit]

        codes = list(dict.fromkeys(str(code).strip().zfill(6) for code in codes))
        print(f"[INFO] 待同步 {len(codes)} 只股票")
        if not codes:
            print("[ERROR] DATA_BLOCKED: finance stock universe is empty")
            return 2

        total_rows = 0
        failures: list[dict[str, str]] = []
        completed_codes: list[str] = []
        latest_periods: dict[str, str] = {}
        applicable_latest_periods: dict[str, str] = {}
        exempt_new_listing_codes: list[str] = []
        for i, code in enumerate(codes):
            try:
                df = fetch_finance(code)
                latest = validate_finance_response(
                    code,
                    df,
                    as_of=run_as_of,
                    minimum_report_date=min_report_date,
                    listing_date=universe[code],
                    disclosure_deadline=disclosure_gate.disclosure_deadline,
                )
                rows = upsert_finance(engine, df, stock_code=code)
                if rows <= 0:
                    raise RuntimeError(
                        f"DATA_BLOCKED: {code} 财务源未提交任何报告期"
                    )
                total_rows += rows
                completed_codes.append(code)
                latest_periods[code] = latest.isoformat()
                if report_period_gate_applies(universe[code], disclosure_gate):
                    applicable_latest_periods[code] = latest.isoformat()
                else:
                    exempt_new_listing_codes.append(code)
            except Exception as exc:
                print(f"  [WARN] {code} 获取/写入失败: {exc}")
                failures.append({"stock_code": code, "error": str(exc)})

            if (i + 1) % 50 == 0:
                print(
                    f"[PROGRESS] {i + 1}/{len(codes)}, 已写入 {total_rows} 条, "
                    f"失败 {len(failures)}"
                )

            if args.sleep > 0 and i < len(codes) - 1:
                time.sleep(args.sleep)

        coverage = len(completed_codes) / len(codes)
        report = {
            "schema": "probiga.finance-sync-result.v1",
            "status": "PASS" if not failures and coverage == 1.0 else "DATA_BLOCKED",
            "as_of": run_as_of.isoformat(),
            "minimum_report_date": min_report_date.isoformat(),
            "minimum_report_disclosure_deadline": (
                disclosure_gate.disclosure_deadline.isoformat()
            ),
            "requested_code_count": len(codes),
            "nonempty_code_count": len(completed_codes),
            "nonempty_code_coverage": coverage,
            "written_report_count": total_rows,
            "failure_count": len(failures),
            "failure_sample": failures[:20],
            "report_period_applicable_code_count": len(applicable_latest_periods),
            "new_listing_period_exempt_code_count": len(exempt_new_listing_codes),
            "oldest_latest_report_date": (
                min(latest_periods.values()) if latest_periods else None
            ),
            "oldest_latest_applicable_report_date": (
                min(applicable_latest_periods.values())
                if applicable_latest_periods else None
            ),
        }
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        if failures or coverage != 1.0:
            print(
                f"[FAILED] DATA_BLOCKED: 财务同步未完整: {len(codes)} 只股票, "
                f"非空覆盖 {len(completed_codes)}/{len(codes)} ({coverage:.2%}), "
                f"写入 {total_rows} 条报告期, 失败 {len(failures)}"
            )
            return 1
        print(
            f"[OK] 同步完成: {len(codes)} 只股票, "
            f"非空覆盖 100%, 写入 {total_rows} 条报告期, 失败 0"
        )
        return 0
    finally:
        dispose = getattr(engine, "dispose", None)
        if callable(dispose):
            dispose()


if __name__ == "__main__":
    raise SystemExit(main())
