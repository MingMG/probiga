# -*- coding: utf-8 -*-
"""Hash-verified exact-date QMT industry facts for strategy governance.

Historical governance must never reconstruct an earlier industry map from the
mutable ``si_industry_sw`` table.  This module accepts only the immutable QMT
membership snapshot for the exact target session and verifies its published
row count and canonical industry hash before copying the L1 facts into the
append-only strategy-governance ledger.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from integrations.bigqmt.reference import PROVIDER_ID


QMT_VALIDATED = "QMT_VALIDATED"
L1_INDUSTRY_TYPES = frozenset({"L1", "一级行业", "申万一级", "SW2021"})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class IndustrySnapshotNotReady(RuntimeError):
    """The exact-date immutable QMT snapshot has not arrived yet."""


class IndustrySnapshotIntegrityError(RuntimeError):
    """The immutable QMT snapshot or copied ledger differs from its contract."""


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")).hexdigest()


def _canonical_qmt_industry_hash(rows: Iterable[Mapping[str, Any]]) -> str:
    """Reproduce ``membership_snapshot._canonical_hash`` without pandas."""

    values = [
        tuple(
            str(row.get(column) or "")
            for column in (
                "industry_code",
                "industry_name",
                "industry_type",
                "stock_code",
                "short_name",
            )
        )
        for row in rows
    ]
    payload = json.dumps(
        sorted(values), ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _iso_datetime(value: Any, *, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip().replace(" ", "T")
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise IndustrySnapshotIntegrityError(
                f"QMT行业快照{field}不是有效时间"
            ) from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(_SHANGHAI).replace(tzinfo=None)
    return parsed.isoformat(timespec="seconds")


def _exact_snapshot_contract(
    engine, *, trade_date: str, source: str = PROVIDER_ID,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    target = date.fromisoformat(str(trade_date)[:10]).isoformat()
    with engine.connect() as connection:
        runs = [dict(row) for row in connection.execute(text("""
            SELECT snapshot_date, source, quality_status, capture_mode,
                   industry_count, industry_relation_count, industry_hash,
                   captured_at
            FROM qmt_membership_snapshot_run
            WHERE snapshot_date=:trade_date AND source=:source
            ORDER BY captured_at, source
        """), {
            "trade_date": target,
            "source": source,
        }).mappings().all()]
        if not runs:
            raise IndustrySnapshotNotReady(
                f"{target}的QMT精确日期行业快照尚未发布"
            )
        if len(runs) != 1:
            raise IndustrySnapshotIntegrityError(
                f"{target}存在重复QMT行业快照运行记录"
            )
        run = runs[0]
        rows = [dict(row) for row in connection.execute(text("""
            SELECT snapshot_date, source, industry_code, industry_name,
                   industry_type, stock_code, short_name, quality_status,
                   captured_at
            FROM qmt_industry_member_snapshot
            WHERE snapshot_date=:trade_date AND source=:source
            ORDER BY industry_code, stock_code
        """), {
            "trade_date": target,
            "source": source,
        }).mappings().all()]

    if str(run.get("quality_status") or "") != QMT_VALIDATED:
        raise IndustrySnapshotNotReady(
            f"{target}的QMT行业快照尚未达到{QMT_VALIDATED}"
        )
    capture_mode = str(run.get("capture_mode") or "")
    if capture_mode != "qmt_close_full_refresh":
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照不是收盘全量冻结模式"
        )
    published_hash = str(run.get("industry_hash") or "").lower()
    if not _SHA256.fullmatch(published_hash):
        raise IndustrySnapshotIntegrityError("QMT行业快照缺少有效发布哈希")
    expected_count = int(run.get("industry_relation_count") or 0)
    if expected_count <= 0 or len(rows) != expected_count:
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照关系数不完整："
            f"发布{expected_count}，实际{len(rows)}"
        )
    expected_industries = int(run.get("industry_count") or 0)
    actual_industries = len({str(row.get("industry_code") or "") for row in rows})
    if expected_industries <= 0 or actual_industries != expected_industries:
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照行业数不完整："
            f"发布{expected_industries}，实际{actual_industries}"
        )
    run_captured_at = _iso_datetime(run.get("captured_at"), field="运行时间")
    for row in rows:
        if str(row.get("snapshot_date") or "")[:10] != target:
            raise IndustrySnapshotIntegrityError("QMT行业成员混入其他数据日")
        if str(row.get("source") or "") != source:
            raise IndustrySnapshotIntegrityError("QMT行业成员来源与运行记录不一致")
        if str(row.get("quality_status") or "") != QMT_VALIDATED:
            raise IndustrySnapshotIntegrityError("QMT行业成员存在未验证记录")
        if _iso_datetime(row.get("captured_at"), field="成员时间") != run_captured_at:
            raise IndustrySnapshotIntegrityError("QMT行业成员时间与运行记录不一致")
    actual_hash = _canonical_qmt_industry_hash(rows)
    if actual_hash != published_hash:
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照canonical hash校验失败"
        )
    captured_value = datetime.fromisoformat(run_captured_at)
    earliest = datetime.combine(
        date.fromisoformat(target), datetime.min.time()
    ).replace(hour=15)
    cutoff_value = datetime.combine(
        date.fromisoformat(target) + timedelta(days=1), datetime.min.time()
    )
    if not earliest <= captured_value < cutoff_value:
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照不是目标交易日收盘后的精确日期事实"
        )
    return ({
        "trade_date": target,
        "source": source,
        "quality_status": QMT_VALIDATED,
        "capture_mode": capture_mode,
        "industry_count": expected_industries,
        "industry_relation_count": expected_count,
        "industry_hash": published_hash,
        "captured_at": run_captured_at,
    }, rows)


def build_history_rows(
    source_rows: Iterable[Mapping[str, Any]], *, trade_date: str,
    source: str, industry_hash: str, captured_at: Any,
) -> tuple[str, list[dict[str, Any]]]:
    """Build the strategy ledger only from one verified QMT snapshot."""

    target = date.fromisoformat(str(trade_date)[:10]).isoformat()
    published_hash = str(industry_hash or "").lower()
    if not _SHA256.fullmatch(published_hash):
        raise IndustrySnapshotIntegrityError("QMT行业快照发布哈希无效")
    normalized_time = _iso_datetime(captured_at, field="运行时间")
    cutoff = (
        date.fromisoformat(target) + timedelta(days=1)
    ).isoformat() + "T00:00:00"
    captured_value = datetime.fromisoformat(normalized_time)
    earliest = datetime.combine(
        date.fromisoformat(target), datetime.min.time()
    ).replace(hour=15)
    if not earliest <= captured_value < datetime.fromisoformat(cutoff):
        raise IndustrySnapshotIntegrityError(
            "QMT行业快照不是目标交易日收盘后的精确日期事实"
        )
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in source_rows:
        industry_type = str(raw.get("industry_type") or "").strip()
        if industry_type not in L1_INDUSTRY_TYPES:
            continue
        code = str(raw.get("stock_code") or "").strip().zfill(6)
        industry_code = str(raw.get("industry_code") or "").strip()
        name = str(raw.get("industry_name") or "").strip()
        if (
            not re.fullmatch(r"[0-9]{6}", code)
            or code in seen
            or not industry_code
            or not name
            or len(name) > 120
            or len(industry_type) > 40
            or len(str(source)) > 80
        ):
            raise IndustrySnapshotIntegrityError(
                "QMT一级行业快照存在重复或不完整证券事实"
            )
        seen.add(code)
        fact_digest = _digest({
            "trade_date": target,
            "source": source,
            "industry_hash": published_hash,
            "industry_code": industry_code,
            "industry_name": name,
            "industry_type": industry_type,
            "stock_code": code,
        })
        normalized.append({
            "stock_code": code,
            "industry_name": name,
            "industry_type": industry_type,
            "source_system": str(source)[:80],
            "source_fact_id": f"qmt:{published_hash}:{fact_digest}",
            "source_effective_at": normalized_time,
            "source_etl_sync_at": normalized_time,
        })
    normalized.sort(key=lambda row: row["stock_code"])
    if not normalized:
        raise IndustrySnapshotIntegrityError("QMT行业快照没有一级行业证券事实")
    snapshot_id = _digest({
        "schema": "probiga.strategy-industry-qmt-snapshot.v2",
        "trade_date": target,
        "as_of_exclusive": cutoff,
        "qmt_source": source,
        "qmt_industry_hash": published_hash,
        "qmt_captured_at": normalized_time,
        "facts": normalized,
    })
    rows: list[dict[str, Any]] = []
    for item in normalized:
        payload = {
            "snapshot_id": snapshot_id,
            "trade_date": target,
            "as_of_exclusive": cutoff,
            **item,
        }
        rows.append({**payload, "row_hash": _digest(payload)})
    return snapshot_id, rows


def prepare_industry_history(
    engine, *, trade_date: str, source: str = PROVIDER_ID,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    run, source_rows = _exact_snapshot_contract(
        engine, trade_date=trade_date, source=source,
    )
    snapshot_id, rows = build_history_rows(
        source_rows,
        trade_date=run["trade_date"],
        source=run["source"],
        industry_hash=run["industry_hash"],
        captured_at=run["captured_at"],
    )
    report = {
        "status": "VALIDATED",
        "trade_date": run["trade_date"],
        "snapshot_id": snapshot_id,
        "source": run["source"],
        "source_snapshot_hash": run["industry_hash"],
        "source_snapshot_captured_at": run["captured_at"],
        "source_relation_count": run["industry_relation_count"],
        "selected_row_count": len(rows),
        "row_count": len(rows),
        "historical_backfill_allowed": False,
        "mutable_current_table_backfill_allowed": False,
        "immutable_exact_date_recovery_allowed": True,
        "historical_recovery_source": "IMMUTABLE_QMT_EXACT_DATE",
    }
    return report, rows


def _verify_existing_rows(connection, *, report: Mapping[str, Any], rows: list[dict[str, Any]]) -> bool:
    existing = [dict(row) for row in connection.execute(text("""
        SELECT snapshot_id, stock_code, row_hash
        FROM st_strategy_industry_history
        WHERE trade_date=:trade_date
        ORDER BY stock_code
    """), {"trade_date": report["trade_date"]}).mappings().all()]
    if not existing:
        return False
    expected = [
        {
            "snapshot_id": row["snapshot_id"],
            "stock_code": row["stock_code"],
            "row_hash": row["row_hash"],
        }
        for row in rows
    ]
    if existing != expected:
        raise IndustrySnapshotIntegrityError(
            "当日策略行业历史与QMT精确日期快照不一致，拒绝覆盖"
        )
    return True


def capture_industry_history(
    engine, *, trade_date: str, source: str = PROVIDER_ID,
) -> dict[str, Any]:
    """Copy one verified exact-date QMT snapshot into the append-only ledger."""

    report, rows = prepare_industry_history(
        engine, trade_date=trade_date, source=source,
    )
    try:
        with engine.begin() as connection:
            if _verify_existing_rows(connection, report=report, rows=rows):
                return {
                    **report,
                    "status": "COMPLETED",
                    "idempotent_replay": True,
                }
            insert = text("""
                INSERT INTO st_strategy_industry_history
                (snapshot_id, trade_date, as_of_exclusive, stock_code,
                 industry_name, industry_type, source_system,
                 source_fact_id, source_effective_at,
                 source_etl_sync_at, row_hash)
                VALUES
                (:snapshot_id, :trade_date, :as_of_exclusive,
                 :stock_code, :industry_name, :industry_type,
                 :source_system, :source_fact_id,
                 :source_effective_at, :source_etl_sync_at, :row_hash)
            """)
            for row in rows:
                connection.execute(insert, row)
    except IntegrityError as exc:
        # A concurrent exact replay is acceptable only when every immutable row
        # now matches; all other duplicate/collision cases remain integrity errors.
        with engine.connect() as connection:
            if _verify_existing_rows(connection, report=report, rows=rows):
                return {
                    **report,
                    "status": "COMPLETED",
                    "idempotent_replay": True,
                }
        raise IndustrySnapshotIntegrityError(
            "策略行业历史并发写入发生不可验证冲突"
        ) from exc
    return {
        **report,
        "status": "COMPLETED",
        "idempotent_replay": False,
    }


__all__ = [
    "IndustrySnapshotIntegrityError",
    "IndustrySnapshotNotReady",
    "L1_INDUSTRY_TYPES",
    "build_history_rows",
    "capture_industry_history",
    "prepare_industry_history",
]
