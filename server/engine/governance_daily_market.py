"""Read native QMT DAT closing evidence for strategy governance."""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
import time
from typing import Any

from sqlalchemy import bindparam, text

from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_daily_coverage import (
    MANIFEST_TABLE,
    NATIVE_EVIDENCE_DATASET,
    SCHEMA as DATADIR_COVERAGE_SCHEMA,
    canonical_bar,
    load_daily_manifest,
    load_daily_manifest_completion,
    load_daily_native_evidence,
)
from server.common.qmt_datadir_result import PROVIDER


@dataclass(frozen=True)
class NativeDatadirClose:
    manifest_hash: str
    run_id: str
    completed_at: datetime
    rows: tuple[dict[str, Any], ...]


def read_close_batches(batches, read, *, deadline):
    """Keep a bounded number of exact reads in flight on separate connections."""
    parallelism = 4
    remaining = iter(batches)
    workers = ThreadPoolExecutor(max_workers=parallelism, thread_name_prefix="governance-close")
    try:
        pending = {workers.submit(read, batch): batch
                   for _, batch in zip(range(parallelism), remaining)}
        while pending:
            done, _ = wait(pending, timeout=max(0, deadline - time.monotonic()), return_when=FIRST_COMPLETED)
            if not done:
                raise TimeoutError(f"QMT 收盘证据读取超时：{next(iter(pending.values()))}")
            for future in done:
                batch = pending.pop(future)
                result = future.result()
                next_batch = next(remaining, None)
                if next_batch is not None:
                    pending[workers.submit(read, next_batch)] = next_batch
                yield batch, result
    finally:
        workers.shutdown(wait=False, cancel_futures=True)


def datadir_manifest_days(connection, days: list[str], *, cutoff: datetime) -> set[str]:
    """Find dates with a published manifest without transferring manifest bodies."""
    if not days:
        return set()
    query = text(f"""
        SELECT /*+ MAX_EXECUTION_TIME(5000) */ DISTINCT trade_date
        FROM {MANIFEST_TABLE}
        WHERE schema_version=:schema AND dataset IN ('stock_daily', :archive_dataset) AND provider=:provider
          AND status='EXACT' AND strategy_eligible=1 AND captured_at<=:cutoff
          AND trade_date IN :days
    """).bindparams(bindparam("days", expanding=True))
    return {
        str(day)[:10]
        for day in connection.execute(query, {
            "schema": DATADIR_COVERAGE_SCHEMA, "archive_dataset": NATIVE_EVIDENCE_DATASET,
            "provider": PROVIDER, "cutoff": cutoff, "days": days,
        }).scalars()
    }


def load_native_datadir_close(
    connection, day: str, *, cutoff: datetime,
) -> NativeDatadirClose:
    """Verify the completed DAT manifest and exact current native database rows."""
    archived = load_daily_native_evidence(connection, day, decision_known_at=cutoff)
    if archived is not None:
        manifest, completion = archived
        completed_at = datetime.fromisoformat(str(completion["finished_at"]))
        rows = tuple({
            **entity["evidence"],
            "data_source": PROVIDER,
            "quality_status": "VERIFIED",
            "permission_status": "SUPPORTED",
            "source_time": f"{day}T15:00:00",
            "received_at": completed_at.isoformat(timespec="seconds"),
            "batch_id": manifest["run_id"],
            "data_version": entity["row_hash"],
            "attestation_id": canonical_digest({
                "manifest_hash": manifest["manifest_hash"], "entity_row_hash": entity["row_hash"],
            }),
            "pre_close_attestation_protocol": DATADIR_COVERAGE_SCHEMA,
            "source_pre_close_origin": "NATIVE_QMT",
        } for entity in manifest["entities"] if entity["classification"] == "TRADED")
        return NativeDatadirClose(manifest["manifest_hash"], manifest["run_id"], completed_at, rows)
    manifest = load_daily_manifest(connection, day, decision_known_at=cutoff, verify_rows=False)
    if manifest is None:
        raise ValueError(f"{day} 缺少截止时点前的 QMT DAT 全市场清单")
    public_codes = sorted(manifest.get("historical_public_bars") or {})
    if public_codes:
        raise ValueError(
            f"{day} 有 {len(public_codes)} 只股票缺少 QMT 原生收盘证据："
            + "、".join(public_codes[:5])
        )
    traded = [item for item in manifest["entities"] if item["classification"] == "TRADED"]
    expected = [item["evidence"] for item in traded]
    receipt = load_daily_manifest_completion(
        connection, day, manifest=manifest, decision_known_at=cutoff,
    ) or {}
    details = receipt.get("detail") or {}
    count = len(expected)
    if (
        not count or receipt.get("status") != "COMPLETE" or receipt.get("run_id") != manifest["run_id"]
        or details.get("full_market_manifest_hash") != manifest["manifest_hash"]
        or details.get("coverage") != 1.0
        or any(value != count for value in (
            receipt.get("row_count"), receipt.get("code_count"),
            details.get("readback_rows"), details.get("readback_codes"),
        ))
    ):
        raise ValueError(f"{day} 的 QMT DAT 完成回执与清单不一致")
    close_time = datetime.fromisoformat(f"{day}T15:00:00")
    completed_at = datetime.fromisoformat(str(receipt["finished_at"]))
    if not close_time <= datetime.fromisoformat(manifest["captured_at"]) <= completed_at <= cutoff:
        raise ValueError(f"{day} 的 QMT DAT 完成时间超出知识截止范围")
    query = text("""
        SELECT /*+ MAX_EXECUTION_TIME(10000) */ stock_code, trade_date,
               `open`, `close`, high, low, volume, amount, pre_close,
               data_source, quality_status, permission_status,
               source_time, received_at, batch_id, data_version
        FROM sm_stock_kline
        WHERE trade_date=:day AND k_type=1 AND adjust_type=0
        ORDER BY stock_code
    """)
    rows = [dict(row) for row in connection.execute(query, {"day": day}).mappings()]
    if [canonical_bar(row) for row in rows] != expected:
        raise ValueError(f"{day} 的 QMT DAT 全市场行情与清单不一致")
    binding_rows = []
    for row, entity in zip(rows, traded, strict=True):
        source = datetime.fromisoformat(str(row["source_time"]))
        received = datetime.fromisoformat(str(row["received_at"]))
        if (
            (row["data_source"], row["quality_status"], row["permission_status"])
            != (PROVIDER, "VERIFIED", "SUPPORTED")
            or not close_time <= source <= received <= completed_at
            or not row["batch_id"] or not row["data_version"]
        ):
            raise ValueError(f"{day} 的 QMT DAT 行情来源或收盘时间不一致")
        binding_rows.append({
            **row,
            "attestation_id": canonical_digest({
                "manifest_hash": manifest["manifest_hash"], "entity_row_hash": entity["row_hash"],
            }),
            "pre_close_attestation_protocol": DATADIR_COVERAGE_SCHEMA,
            "source_pre_close_origin": "NATIVE_QMT",
        })
    return NativeDatadirClose(manifest["manifest_hash"], manifest["run_id"], completed_at, tuple(binding_rows))
