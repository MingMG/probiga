"""Publish the 2026 A-share calendar from SSE/SZSE holiday notices."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.api.routers._engine import get_engine
from server.common.qmt_trade_calendar import (
    calendar_source_batch_id,
    insert_trade_calendar_receipt,
    validate_trade_calendar_runtime_schema,
)


SOURCE_PROVIDER = "SSE_SZSE_OFFICIAL"
SOURCE_PUBLISHED_AT = datetime(2025, 12, 22)
SOURCE_URLS = (
    "https://www.sse.com.cn/disclosure/dealinstruc/closed/",
    "https://investor.szse.cn/disclosure/notice/general/t20251222_618087.html",
)
HOLIDAY_CLOSURES = (
    (date(2026, 1, 1), date(2026, 1, 3)),
    (date(2026, 2, 15), date(2026, 2, 23)),
    (date(2026, 4, 4), date(2026, 4, 6)),
    (date(2026, 5, 1), date(2026, 5, 5)),
    (date(2026, 6, 19), date(2026, 6, 21)),
    (date(2026, 9, 25), date(2026, 9, 27)),
    (date(2026, 10, 1), date(2026, 10, 7)),
)


def official_calendar_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    current = date(2026, 1, 1)
    end = date(2026, 12, 31)
    while current <= end:
        holiday = any(start <= current <= finish for start, finish in HOLIDAY_CLOSURES)
        rows.append({
            "calendar_year": 2026,
            "trade_date": current.isoformat(),
            "trade_status": int(current.weekday() < 5 and not holiday),
            "day_week": current.isoweekday(),
        })
        current += timedelta(days=1)
    return rows


def publish(*, apply: bool) -> dict[str, Any]:
    rows = official_calendar_rows()
    sessions = [row["trade_date"] for row in rows if row["trade_status"] == 1]
    source_batch_id = calendar_source_batch_id(
        start_date="2026-01-01",
        end_date="2026-12-31",
        sessions=sessions,
        source_provider=SOURCE_PROVIDER,
    )
    known_at = datetime.now().replace(microsecond=0)
    batch_id = (
        f"sse_szse_2026_{source_batch_id[:12]}_"
        f"{known_at.strftime('%Y%m%d%H%M%S')}"
    )
    result: dict[str, Any] = {
        "status": "DRY_RUN" if not apply else "COMPLETE",
        "batch_id": batch_id,
        "source_provider": SOURCE_PROVIDER,
        "source_urls": list(SOURCE_URLS),
        "start_date": "2026-01-01",
        "end_date": "2026-12-31",
        "session_count": len(sessions),
        "source_batch_id": source_batch_id,
    }
    if not apply:
        return result

    engine = get_engine()
    validate_trade_calendar_runtime_schema(engine)
    payload = [
        {
            **row,
            "etl_sync_at": known_at,
            "data_source": "sse_szse_official",
            "source_time": SOURCE_PUBLISHED_AT,
            "received_at": known_at,
            "batch_id": batch_id,
            "data_version": source_batch_id,
            "quality_status": "OFFICIAL_EXCHANGE",
            "permission_status": "SUPPORTED",
        }
        for row in rows
    ]
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO si_trade_calendar
            (calendar_year, trade_date, trade_status, day_week, etl_sync_at,
             qmt_code, data_source, source_time, received_at, batch_id,
             data_version, quality_status, permission_status)
            VALUES
            (:calendar_year, :trade_date, :trade_status, :day_week,
             :etl_sync_at, NULL, :data_source, :source_time, :received_at,
             :batch_id, :data_version, :quality_status, :permission_status)
            ON DUPLICATE KEY UPDATE
              trade_status=VALUES(trade_status),
              day_week=VALUES(day_week),
              etl_sync_at=VALUES(etl_sync_at),
              qmt_code=VALUES(qmt_code),
              data_source=VALUES(data_source),
              source_time=VALUES(source_time),
              received_at=VALUES(received_at),
              batch_id=VALUES(batch_id),
              data_version=VALUES(data_version),
              quality_status=VALUES(quality_status),
              permission_status=VALUES(permission_status)
        """), payload)
        receipt = insert_trade_calendar_receipt(
            connection,
            batch_id=batch_id,
            source_batch_id=source_batch_id,
            known_at=known_at,
            start_date="2026-01-01",
            end_date="2026-12-31",
            sessions=sessions,
            source_provider=SOURCE_PROVIDER,
        )
    result["receipt"] = receipt
    result["updated_rows"] = len(rows)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(json.dumps(publish(apply=args.apply), ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
