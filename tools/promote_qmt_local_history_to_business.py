from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import make_url

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.local_history import get_local_history_engine
from server.common.batch_db import create_batch_engine, qualified_table_name
from tools.env_config import load_project_env


def _date_list(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _source_tables(business_engine) -> tuple[str, str, str, str]:
    local_engine = get_local_history_engine()
    business_db = make_url(str(business_engine.url)).database
    local_db = make_url(str(local_engine.url)).database
    if not business_db or not local_db:
        raise RuntimeError("business/local database name is required")
    return (
        qualified_table_name(business_db, "sm_stock_kline"),
        qualified_table_name(business_db, "sm_stock_minute"),
        qualified_table_name(local_db, "qmt_local_stock_kline"),
        qualified_table_name(local_db, "qmt_local_stock_minute"),
    )


def promote_daily(engine, *, dates: list[str], min_rows: int) -> list[dict]:
    target_kline, _, local_kline, _ = _source_tables(engine)
    results: list[dict] = []
    for trade_date in dates:
        with engine.begin() as conn:
            local_rows = int(
                conn.execute(
                    text(f"SELECT COUNT(*) FROM {local_kline} WHERE trade_date=:d"),
                    {"d": trade_date},
                ).scalar()
                or 0
            )
            if local_rows < min_rows:
                results.append(
                    {
                        "table": "sm_stock_kline",
                        "date": trade_date,
                        "status": "skip_local_incomplete",
                        "local_rows": local_rows,
                    }
                )
                continue
            before = int(
                conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM {target_kline} "
                        "WHERE trade_date=:d AND k_type=1 AND adjust_type=0"
                    ),
                    {"d": trade_date},
                ).scalar()
                or 0
            )
            conn.execute(
                text(f"DELETE FROM {target_kline} WHERE trade_date=:d AND k_type=1 AND adjust_type=0"),
                {"d": trade_date},
            )
            inserted = conn.execute(
                text(
                    f"""
                    INSERT INTO {target_kline} (
                        stock_code, short_name, trade_time, trade_date, k_type, adjust_type,
                        open, close, high, low, volume, amount, `change`, change_pct,
                        turnover_ratio, pre_close, etl_sync_at, qmt_code, data_source,
                        source_time, received_at, batch_id, data_version, quality_status,
                        permission_status
                    )
                    SELECT
                        stock_code, short_name, trade_time, trade_date, 1 AS k_type, 0 AS adjust_type,
                        open, close, high, low, volume, amount, `change`, change_pct,
                        turnover_ratio, pre_close, NOW() AS etl_sync_at, qmt_code,
                        provider AS data_source, source_time, received_at, batch_id, data_version,
                        quality_status, permission_status
                    FROM {local_kline}
                    WHERE trade_date=:d
                    """
                ),
                {"d": trade_date},
            ).rowcount or 0
            after = int(
                conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM {target_kline} "
                        "WHERE trade_date=:d AND k_type=1 AND adjust_type=0"
                    ),
                    {"d": trade_date},
                ).scalar()
                or 0
            )
            results.append(
                {
                    "table": "sm_stock_kline",
                    "date": trade_date,
                    "status": "ok",
                    "before": before,
                    "local_rows": local_rows,
                    "inserted": int(inserted),
                    "after": after,
                }
            )
    return results


def _stock_code_chunks(conn, local_minute: str, trade_date: str, batch_size: int) -> list[list[str]]:
    rows = conn.execute(
        text(
            f"""
            SELECT DISTINCT stock_code
            FROM {local_minute}
            WHERE trade_date=:d
            ORDER BY stock_code
            """
        ),
        {"d": trade_date},
    ).fetchall()
    codes = [str(row[0]).zfill(6) for row in rows]
    return [codes[idx : idx + batch_size] for idx in range(0, len(codes), batch_size)]


def promote_minute(engine, *, dates: list[str], min_rows: int, stock_batch_size: int) -> list[dict]:
    _, target_minute, _, local_minute = _source_tables(engine)
    results: list[dict] = []
    for trade_date in dates:
        with engine.begin() as conn:
            local_rows = int(
                conn.execute(
                    text(f"SELECT COUNT(*) FROM {local_minute} WHERE trade_date=:d"),
                    {"d": trade_date},
                ).scalar()
                or 0
            )
            if local_rows < min_rows:
                results.append(
                    {
                        "table": "sm_stock_minute",
                        "date": trade_date,
                        "status": "skip_local_incomplete",
                        "local_rows": local_rows,
                    }
                )
                continue
            before = int(
                conn.execute(
                    text(f"SELECT COUNT(*) FROM {target_minute} WHERE trade_date=:d"),
                    {"d": trade_date},
                ).scalar()
                or 0
            )
            conn.execute(text(f"DELETE FROM {target_minute} WHERE trade_date=:d"), {"d": trade_date})

        inserted_total = 0
        with engine.connect() as conn:
            chunks = _stock_code_chunks(conn, local_minute, trade_date, max(1, int(stock_batch_size)))
        for idx, codes in enumerate(chunks, start=1):
            params = {"d": trade_date, **{f"code_{i}": code for i, code in enumerate(codes)}}
            code_sql = ", ".join(f":code_{i}" for i in range(len(codes)))
            with engine.begin() as conn:
                inserted = conn.execute(
                    text(
                        f"""
                        INSERT INTO {target_minute} (
                            stock_code, trade_time, trade_date, price, avg_price, `change`, change_pct,
                            volume, amount, etl_sync_at, qmt_code, data_source, source_time,
                            received_at, batch_id, data_version, quality_status, permission_status
                        )
                        SELECT
                            stock_code, trade_time, trade_date, price, avg_price, `change`, change_pct,
                            volume, amount, NOW() AS etl_sync_at, qmt_code, provider AS data_source,
                            source_time, received_at, batch_id, data_version, quality_status,
                            permission_status
                        FROM {local_minute}
                        WHERE trade_date=:d AND stock_code IN ({code_sql})
                        """
                    ),
                    params,
                ).rowcount or 0
                inserted_total += int(inserted)
            print(
                {
                    "table": "sm_stock_minute",
                    "date": trade_date,
                    "chunk": idx,
                    "chunks": len(chunks),
                    "inserted_total": inserted_total,
                },
                flush=True,
            )

        with engine.connect() as conn:
            after = int(
                conn.execute(
                    text(f"SELECT COUNT(*) FROM {target_minute} WHERE trade_date=:d"),
                    {"d": trade_date},
                ).scalar()
                or 0
            )
        results.append(
            {
                "table": "sm_stock_minute",
                "date": trade_date,
                "status": "ok",
                "before": before,
                "local_rows": local_rows,
                "inserted": inserted_total,
                "after": after,
            }
        )
    return results


def derive_daily_from_business_minute(
    engine,
    *,
    dates: list[str],
    min_rows: int,
    complete_rows: int,
) -> list[dict]:
    target_kline, target_minute, _, _ = _source_tables(engine)
    results: list[dict] = []
    for trade_date in dates:
        with engine.begin() as conn:
            minute_stats = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) AS rows_count, COUNT(DISTINCT stock_code) AS code_count
                    FROM {target_minute}
                    WHERE trade_date=:d AND data_source='gj_qmt'
                    """
                ),
                {"d": trade_date},
            ).mappings().first() or {}
            minute_rows = int(minute_stats.get("rows_count") or 0)
            code_count = int(minute_stats.get("code_count") or 0)
            if minute_rows < min_rows or code_count <= 0:
                results.append(
                    {
                        "table": "sm_stock_kline",
                        "date": trade_date,
                        "status": "skip_minute_incomplete",
                        "minute_rows": minute_rows,
                        "code_count": code_count,
                    }
                )
                continue

            quality = "minute_agg_complete" if minute_rows >= complete_rows else "minute_agg_partial"
            before = int(
                conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM {target_kline} "
                        "WHERE trade_date=:d AND k_type=1 AND adjust_type=0"
                    ),
                    {"d": trade_date},
                ).scalar()
                or 0
            )

            conn.execute(
                text(f"DROP TEMPORARY TABLE IF EXISTS tmp_qmt_minute_daily_{trade_date.replace('-', '')}")
            )
            tmp_table = f"tmp_qmt_minute_daily_{trade_date.replace('-', '')}"
            conn.execute(
                text(
                    f"""
                    CREATE TEMPORARY TABLE `{tmp_table}` AS
                    SELECT
                        stock_code,
                        trade_date,
                        MIN(trade_time) AS first_time,
                        MAX(trade_time) AS last_time,
                        MAX(price) AS high_price,
                        MIN(price) AS low_price,
                        SUM(volume) AS total_volume,
                        SUM(amount) AS total_amount,
                        COUNT(*) AS bar_count,
                        MAX(qmt_code) AS qmt_code,
                        MAX(source_time) AS source_time
                    FROM {target_minute}
                    WHERE trade_date=:d AND data_source='gj_qmt' AND price > 0
                    GROUP BY stock_code, trade_date
                    """
                ),
                {"d": trade_date},
            )
            conn.execute(text(f"ALTER TABLE `{tmp_table}` ADD PRIMARY KEY (`stock_code`)"))

            conn.execute(
                text(f"DELETE FROM {target_kline} WHERE trade_date=:d AND k_type=1 AND adjust_type=0"),
                {"d": trade_date},
            )
            inserted = conn.execute(
                text(
                    f"""
                    INSERT INTO {target_kline} (
                        stock_code, short_name, trade_time, trade_date, k_type, adjust_type,
                        open, close, high, low, volume, amount, `change`, change_pct,
                        turnover_ratio, pre_close, etl_sync_at, qmt_code, data_source,
                        source_time, received_at, batch_id, data_version, quality_status,
                        permission_status
                    )
                    SELECT
                        s.stock_code,
                        COALESCE(NULLIF(c.short_name, ''), s.stock_code) AS short_name,
                        lm.trade_time AS trade_time,
                        s.trade_date,
                        1 AS k_type,
                        0 AS adjust_type,
                        fm.price AS open,
                        lm.price AS close,
                        s.high_price AS high,
                        s.low_price AS low,
                        s.total_volume AS volume,
                        s.total_amount AS amount,
                        lm.`change` AS `change`,
                        lm.change_pct AS change_pct,
                        NULL AS turnover_ratio,
                        CASE
                            WHEN lm.change_pct IS NOT NULL AND lm.change_pct <> 0
                                THEN lm.price / NULLIF(1 + lm.change_pct / 100, 0)
                            WHEN lm.`change` IS NOT NULL
                                THEN lm.price - lm.`change`
                            ELSE NULL
                        END AS pre_close,
                        NOW() AS etl_sync_at,
                        s.qmt_code,
                        'gj_qmt' AS data_source,
                        s.source_time,
                        NOW() AS received_at,
                        CONCAT('qmt_minute_agg_', :d) AS batch_id,
                        CONCAT('minute_agg_', :d) AS data_version,
                        :quality AS quality_status,
                        'ok' AS permission_status
                    FROM `{tmp_table}` s
                    JOIN {target_minute} fm
                      ON fm.stock_code=s.stock_code AND fm.trade_date=s.trade_date
                     AND fm.trade_time=s.first_time AND fm.data_source='gj_qmt'
                    JOIN {target_minute} lm
                      ON lm.stock_code=s.stock_code AND lm.trade_date=s.trade_date
                     AND lm.trade_time=s.last_time AND lm.data_source='gj_qmt'
                    LEFT JOIN si_all_code c ON c.stock_code=s.stock_code
                    """
                ),
                {"d": trade_date, "quality": quality},
            ).rowcount or 0

            after = int(
                conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM {target_kline} "
                        "WHERE trade_date=:d AND k_type=1 AND adjust_type=0"
                    ),
                    {"d": trade_date},
                ).scalar()
                or 0
            )
            results.append(
                {
                    "table": "sm_stock_kline",
                    "date": trade_date,
                    "status": "ok",
                    "mode": "derive_daily_from_minute",
                    "quality_status": quality,
                    "before": before,
                    "minute_rows": minute_rows,
                    "code_count": code_count,
                    "inserted": int(inserted),
                    "after": after,
                }
            )
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote completed local QMT history dates into business tables.")
    parser.add_argument("--daily-dates", default="")
    parser.add_argument("--minute-dates", default="")
    parser.add_argument("--derive-daily-from-minute-dates", default="")
    parser.add_argument("--min-daily-rows", type=int, default=4441)
    parser.add_argument("--min-minute-rows", type=int, default=1_070_425)
    parser.add_argument("--min-derive-minute-rows", type=int, default=1_070_425)
    parser.add_argument("--complete-minute-rows", type=int, default=1_250_000)
    parser.add_argument("--minute-stock-batch-size", type=int, default=200)
    args = parser.parse_args()

    load_project_env()
    engine = create_batch_engine(future=True)
    daily_dates = _date_list(args.daily_dates)
    minute_dates = _date_list(args.minute_dates)
    derive_dates = _date_list(args.derive_daily_from_minute_dates)

    for result in promote_daily(engine, dates=daily_dates, min_rows=args.min_daily_rows):
        print(result, flush=True)
    for result in promote_minute(
        engine,
        dates=minute_dates,
        min_rows=args.min_minute_rows,
        stock_batch_size=args.minute_stock_batch_size,
    ):
        print(result, flush=True)
    for result in derive_daily_from_business_minute(
        engine,
        dates=derive_dates,
        min_rows=args.min_derive_minute_rows,
        complete_rows=args.complete_minute_rows,
    ):
        print(result, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
