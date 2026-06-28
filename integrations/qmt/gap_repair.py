from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from integrations.qmt.diagnostics import PROVIDER_ID


CHINA_STANDARD_TIME = timezone(timedelta(hours=8), name="Asia/Shanghai")


@dataclass(frozen=True)
class GapRepairItem:
    id: int
    dataset: str
    symbol: str
    period: str
    gap_start: str | None
    gap_end: str | None
    reason: str | None
    action: str
    status: str


@dataclass(frozen=True)
class GapRepairPlan:
    mode: str
    selected: int
    locked: int
    items: list[GapRepairItem]


def _now() -> datetime:
    return datetime.now(CHINA_STANDARD_TIME).replace(tzinfo=None, microsecond=0)


def _action_for_gap(dataset: str, period: str) -> str:
    if dataset == "sm_stock_kline.1d":
        return "qmt_download_history_data_1d_then_safe_upsert"
    if dataset == "sm_stock_minute.1m":
        return "qmt_download_history_data_1m_then_safe_upsert"
    return "manual_review"


def plan_gap_repairs(engine: Engine, *, limit: int = 20, apply: bool = False) -> GapRepairPlan:
    now = _now()
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, dataset, symbol, period, gap_start, gap_end, reason, status
                FROM sys_data_gap
                WHERE provider = :provider
                  AND status IN ('PENDING', 'RETRYING')
                  AND (next_retry_at IS NULL OR next_retry_at <= :now OR status = 'PENDING')
                ORDER BY
                  CASE status WHEN 'PENDING' THEN 0 ELSE 1 END,
                  COALESCE(next_retry_at, created_at),
                  id
                LIMIT :limit
                """
            ),
            {"provider": PROVIDER_ID, "now": now, "limit": max(1, int(limit))},
        ).mappings().fetchall()

        items: list[GapRepairItem] = []
        for row in rows:
            action = _action_for_gap(str(row["dataset"] or ""), str(row["period"] or ""))
            items.append(
                GapRepairItem(
                    id=int(row["id"]),
                    dataset=str(row["dataset"] or ""),
                    symbol=str(row["symbol"] or ""),
                    period=str(row["period"] or ""),
                    gap_start=str(row["gap_start"]) if row["gap_start"] else None,
                    gap_end=str(row["gap_end"]) if row["gap_end"] else None,
                    reason=row["reason"],
                    action=action,
                    status="RETRYING" if apply else str(row["status"] or ""),
                )
            )

        locked = 0
        if apply and items:
            ids = [item.id for item in items]
            placeholders = ", ".join(f":id{idx}" for idx, _ in enumerate(ids))
            params: dict[str, Any] = {f"id{idx}": item_id for idx, item_id in enumerate(ids)}
            params.update(
                {
                    "now": now,
                    "next_retry_at": now + timedelta(hours=6),
                    "last_error": "queued_for_qmt_history_backfill_worker",
                }
            )
            result = conn.execute(
                text(
                    f"""
                    UPDATE sys_data_gap
                    SET status = 'RETRYING',
                        retry_count = retry_count + 1,
                        last_error = :last_error,
                        next_retry_at = :next_retry_at,
                        updated_at = :now
                    WHERE id IN ({placeholders})
                    """
                ),
                params,
            )
            locked = int(result.rowcount or 0)

    return GapRepairPlan(mode="apply" if apply else "dry_run", selected=len(items), locked=locked, items=items)


def result_dict(plan: GapRepairPlan) -> dict[str, Any]:
    payload = asdict(plan)
    payload["items"] = [asdict(item) for item in plan.items]
    return payload
