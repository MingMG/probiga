"""Hourly official announcement watch for the system's current stock exposure.

This is a targeted, low-latency complement to the full-market QMT announcement
batch.  It monitors only holdings/watchlist names and the latest canonical
CONFIRMATION/TRADABLE pools, checkpoints each official CNInfo response, then
publishes the complete scope atomically into the existing PIT event ledger.
"""
from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text

from server.common.announcement_provider import (
    CninfoMarketAnnouncementProvider,
    ProviderBackedAnnouncementAdapter,
)
from server.common.exposure_risk_watch_task_contract import (
    CHECKPOINT_ROOT,
    SCRIPT_ARGS as SCHEDULER_SCRIPT_ARGS,
    SCRIPT_PATH as SCHEDULER_SCRIPT_PATH,
    TASK_TYPE as SCHEDULER_TASK_TYPE,
)
from server.common.pit_facts import (
    CNINFO_EVENT_SOURCE,
    EVENT_CAPTURE_PURPOSE_EXPOSURE_MONITOR,
    PIT_AVAILABLE,
    append_event_revision,
    append_source_coverage,
    canonical_hash,
    load_event_facts,
)
from server.common.qmt_announcement_pit import (
    AnnouncementCatalog,
    AnnouncementCheckpoint,
    assert_announcement_pit_schema_prepared,
    parse_qmt_announcement_frame,
)
from server.common.qmt_stock_catalog import load_stock_catalog
from server.engine.event_risk_engine import EventRiskEngine


SHANGHAI = ZoneInfo("Asia/Shanghai")
CAPTURE_SCHEMA = "probiga.exposure-event-risk-watch.v1"
CAPTURE_PURPOSE = EVENT_CAPTURE_PURPOSE_EXPOSURE_MONITOR
DEFAULT_LOOKBACK_DAYS = 3
DEFAULT_DEADLINE_MINUTES = 20
DEFAULT_BATCH_SIZE = 4
DEFAULT_CHECKPOINT_ROOT = Path(CHECKPOINT_ROOT)


class ExposureRiskWatchError(RuntimeError):
    """A stable terminal failure for one monitoring stage."""

    def __init__(self, stage: str, reason: str) -> None:
        self.stage = str(stage or "unknown")
        self.reason = str(reason or "FAILED")[:1000]
        super().__init__(f"{self.stage}:{self.reason}")


def _now_shanghai(value: datetime | None = None) -> datetime:
    observed = value or datetime.now(SHANGHAI)
    if observed.tzinfo is not None:
        observed = observed.astimezone(SHANGHAI).replace(tzinfo=None)
    return observed


def _dt_text(value: datetime) -> str:
    return _now_shanghai(value).strftime("%Y-%m-%dT%H:%M:%S.%f")


def load_exposure_universe(engine: Any) -> dict[str, list[str]]:
    """Read the exact current portfolio and latest canonical formal pools."""

    sources: dict[str, set[str]] = {}
    with engine.connect() as connection:
        portfolio = connection.execute(text("""
            SELECT stock_code, MAX(COALESCE(is_holding, 0)) AS is_holding
              FROM st_user_portfolio
             WHERE stock_code IS NOT NULL AND stock_code<>''
             GROUP BY stock_code
        """)).mappings().all()
        for row in portfolio:
            code = str(row.get("stock_code") or "").strip().zfill(6)
            source = (
                "USER_HOLDING"
                if int(row.get("is_holding") or 0) == 1
                else "USER_WATCHLIST"
            )
            sources.setdefault(code, set()).add(source)
        pools = connection.execute(text("""
            SELECT DISTINCT pool.stock_code, pool.pool_level
              FROM st_strategy_pool_snapshot pool
              JOIN st_strategy_governance_run run
                ON run.run_uid=pool.run_uid
             WHERE run.status='COMPLETED'
               AND run.is_canonical=1
               AND pool.pool_level IN ('CONFIRMATION','TRADABLE')
               AND run.run_uid=(
                   SELECT latest.run_uid
                     FROM st_strategy_governance_run latest
                    WHERE latest.status='COMPLETED'
                      AND latest.is_canonical=1
                    ORDER BY latest.trade_date DESC,
                             latest.run_revision DESC,
                             latest.created_at DESC,
                             latest.run_uid DESC
                    LIMIT 1
               )
        """)).mappings().all()
        for row in pools:
            code = str(row.get("stock_code") or "").strip().zfill(6)
            sources.setdefault(code, set()).add(
                f"CANONICAL_{str(row.get('pool_level') or '').upper()}"
            )
    invalid = sorted(code for code in sources if not (len(code) == 6 and code.isdigit()))
    if invalid:
        raise ExposureRiskWatchError("exposure_scope", f"invalid_codes={invalid[:10]}")
    return {code: sorted(values) for code, values in sorted(sources.items())}


def read_exposure_watch_checkpoint_progress(
    checkpoint_root: Path = DEFAULT_CHECKPOINT_ROOT,
) -> dict[str, Any]:
    """Read the newest durable checkpoint without treating it as completion."""

    try:
        manifests = list(Path(checkpoint_root).glob("*/manifest.json"))
    except OSError:
        manifests = []
    candidates: list[tuple[datetime, Path, dict[str, Any]]] = []
    for manifest_path in manifests:
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            cutoff = datetime.fromisoformat(
                str(manifest.get("fact_cutoff_at") or "")
            )
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        codes = manifest.get("stock_codes")
        if (
            manifest.get("source") != CNINFO_EVENT_SOURCE
            or manifest.get("capture_purpose") != CAPTURE_PURPOSE
            or not isinstance(codes, list)
            or not codes
            or len(set(map(str, codes))) != len(codes)
        ):
            continue
        candidates.append((cutoff, manifest_path, manifest))
    if not candidates:
        return {
            "status": "NOT_STARTED",
            "total_units": 0,
            "completed_units": 0,
            "current_unit": None,
            "last_progress_at": None,
            "checkpoint_complete": False,
        }
    _, manifest_path, manifest = max(candidates, key=lambda item: item[0])
    checkpoint = AnnouncementCheckpoint(manifest_path.parent, manifest)
    codes = [str(code) for code in manifest["stock_codes"]]
    completed: list[str] = []
    latest_mtime = manifest_path.stat().st_mtime
    for code in codes:
        try:
            value = checkpoint.load(code)
        except Exception:
            value = None
        if value is not None:
            completed.append(code)
            try:
                latest_mtime = max(
                    latest_mtime,
                    (manifest_path.parent / "results" / f"{code}.json").stat().st_mtime,
                )
            except OSError:
                pass
    complete_path = manifest_path.parent / "complete.json"
    checkpoint_complete = False
    if len(completed) == len(codes) and complete_path.exists():
        try:
            completion = json.loads(complete_path.read_text(encoding="utf-8"))
            checkpoint_complete = (
                completion.get("schema")
                == "probiga.qmt-announcement-checkpoint-complete.v1"
                and completion.get("batch_id") == manifest.get("batch_id")
                and completion.get("fact_cutoff_at")
                == manifest.get("fact_cutoff_at")
                and isinstance(completion.get("batch_root_hash"), str)
                and len(completion["batch_root_hash"]) == 64
            )
            latest_mtime = max(latest_mtime, complete_path.stat().st_mtime)
        except (OSError, ValueError, json.JSONDecodeError):
            checkpoint_complete = False
    remaining = [code for code in codes if code not in set(completed)]
    last_progress = datetime.fromtimestamp(
        latest_mtime, tz=SHANGHAI
    ).replace(tzinfo=None)
    return {
        "status": "COMPLETE" if checkpoint_complete else "RUNNING",
        "batch_id": str(manifest.get("batch_id") or ""),
        "fact_cutoff_at": str(manifest.get("fact_cutoff_at") or ""),
        "total_units": len(codes),
        "completed_units": len(completed),
        "current_unit": remaining[0] if remaining else None,
        "last_progress_at": _dt_text(last_progress),
        "checkpoint_complete": checkpoint_complete,
    }


def build_exposure_catalog(
    engine: Any,
    *,
    exposure_sources: Mapping[str, Sequence[str]],
    decision_at: datetime,
) -> tuple[AnnouncementCatalog, dict[str, Any]]:
    """Bind the targeted scope to the immutable QMT instrument catalogue."""

    codes = tuple(sorted(str(code).zfill(6) for code in exposure_sources))
    with engine.connect() as connection:
        stock_catalog = load_stock_catalog(
            connection,
            decision_known_at=decision_at,
        )
    target = decision_at.date().isoformat()
    eligible = set(stock_catalog.eligible_codes(target))
    qmt_by_code = {
        str(member["stock_code"]): str(member["qmt_code"])
        for member in stock_catalog.members
        if str(member["stock_code"]) in codes
        and str(member["stock_code"]) in eligible
    }
    missing = sorted(set(codes) - set(qmt_by_code))
    if missing:
        raise ExposureRiskWatchError(
            "exposure_catalog", f"catalog_mapping_missing={missing[:20]}"
        )
    scope = {
        "schema": "probiga.exposure-event-risk-scope.v1",
        "codes": list(codes),
        "exposure_sources": {
            code: sorted(str(item) for item in exposure_sources[code])
            for code in codes
        },
        "qmt_by_code": qmt_by_code,
        "upstream_catalog_batch_id": stock_catalog.batch_id,
        "upstream_catalog_manifest_hash": stock_catalog.manifest_hash,
        "upstream_catalog_member_set_hash": stock_catalog.member_set_hash,
    }
    scope_hash = canonical_hash(scope)
    catalog = AnnouncementCatalog(
        batch_id=f"exposure-scope-{scope_hash[:32]}",
        manifest_hash=scope_hash,
        member_set_hash=canonical_hash({
            "schema": "probiga.exposure-event-risk-members.v1",
            "codes": list(codes),
            "qmt_by_code": qmt_by_code,
        }),
        codes=codes,
        qmt_by_code=qmt_by_code,
    )
    return catalog, {**scope, "scope_hash": scope_hash}


def _checkpoint_extra(scope: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "capture_purpose": CAPTURE_PURPOSE,
        "exposure_scope": dict(scope),
    }


def _find_resumable_checkpoint(
    checkpoint_root: Path,
    *,
    catalog: AnnouncementCatalog,
    scope: Mapping[str, Any],
    observed_at: datetime,
    deadline_minutes: int,
) -> tuple[datetime, str] | None:
    candidates: list[tuple[int, datetime, str]] = []
    try:
        manifests = list(checkpoint_root.glob("*/manifest.json"))
    except OSError:
        return None
    for manifest_path in manifests:
        if (manifest_path.parent / "complete.json").exists():
            continue
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            cutoff = datetime.fromisoformat(str(payload.get("fact_cutoff_at") or ""))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if (
            payload.get("source") != CNINFO_EVENT_SOURCE
            or payload.get("capture_purpose") != CAPTURE_PURPOSE
            or payload.get("exposure_scope") != dict(scope)
            or payload.get("catalog_batch_id") != catalog.batch_id
            or payload.get("catalog_manifest_hash") != catalog.manifest_hash
            or payload.get("catalog_member_set_hash") != catalog.member_set_hash
            or payload.get("stock_codes") != list(catalog.codes)
            or observed_at > cutoff + timedelta(minutes=deadline_minutes)
        ):
            continue
        try:
            completed = len(list((manifest_path.parent / "results").glob("*.json")))
        except OSError:
            completed = 0
        candidates.append((completed, cutoff, manifest_path.parent.name))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    _, cutoff, batch_id = candidates[0]
    return cutoff, batch_id


def _batch_identity(
    *,
    fact_cutoff_at: datetime,
    window_start: date,
    catalog: AnnouncementCatalog,
) -> str:
    digest = canonical_hash({
        "schema": CAPTURE_SCHEMA,
        "capture_purpose": CAPTURE_PURPOSE,
        "fact_cutoff_at": _dt_text(fact_cutoff_at),
        "window_start": window_start.isoformat(),
        "window_end": fact_cutoff_at.date().isoformat(),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
    })
    return f"exposure-risk-{digest[:48]}"


def _source_response_hash(events: Sequence[Mapping[str, Any]]) -> str:
    normalized = sorted((dict(item) for item in events), key=lambda item: json.dumps(
        item, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ))
    return canonical_hash({
        "schema": "probiga.pit-event-source-response.v1",
        "rows": normalized,
    })


def _build_batch_root(
    *,
    batch_id: str,
    fact_cutoff_at: datetime,
    received_at: datetime,
    window_start: date,
    catalog: AnnouncementCatalog,
    results: Mapping[str, Sequence[Mapping[str, Any]]],
    receipts: Mapping[str, Mapping[str, Any]],
    scope: Mapping[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    entries = [
        {
            "stock_code": code,
            "source_response_hash": _source_response_hash(results[code]),
            "result_count": len(results[code]),
            "provider_receipt_hash": canonical_hash(dict(receipts[code])),
        }
        for code in catalog.codes
    ]
    payload = {
        "schema": CAPTURE_SCHEMA,
        "capture_purpose": CAPTURE_PURPOSE,
        "source": CNINFO_EVENT_SOURCE,
        "batch_id": batch_id,
        "fact_cutoff_at": _dt_text(fact_cutoff_at),
        "received_at": _dt_text(received_at),
        "window_start": window_start.isoformat(),
        "window_end": fact_cutoff_at.date().isoformat(),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "catalog_member_count": len(catalog.codes),
        "exposure_scope_hash": scope["scope_hash"],
        "entries": entries,
    }
    return canonical_hash(payload), entries


def _publish_batch(
    engine: Any,
    *,
    batch_id: str,
    batch_root_hash: str,
    entries: Sequence[Mapping[str, Any]],
    fact_cutoff_at: datetime,
    received_at: datetime,
    window_start: date,
    catalog: AnnouncementCatalog,
    results: Mapping[str, Sequence[Mapping[str, Any]]],
    receipts: Mapping[str, Mapping[str, Any]],
    scope: Mapping[str, Any],
    deadline_at: datetime,
    now_fn: Callable[[], datetime],
    stage_fn: Callable[[str], None] | None = None,
) -> list[str]:
    response_hashes = {
        str(item["stock_code"]): str(item["source_response_hash"])
        for item in entries
    }
    coverage_ids: list[str] = []

    def check_deadline(stage: str) -> None:
        if stage_fn is not None:
            stage_fn(stage)
        if _now_shanghai(now_fn()) > deadline_at:
            raise ExposureRiskWatchError(stage, "CAPTURE_DEADLINE_EXPIRED")

    def write_rows(connection: Any) -> None:
        for code in catalog.codes:
            check_deadline("database_publish")
            source_rows = [dict(item) for item in results[code]]
            bindings = []
            for item in source_rows:
                check_deadline("database_event_append")
                revision = append_event_revision(
                    connection,
                    item,
                    known_at=received_at,
                    received_at=received_at,
                    source=CNINFO_EVENT_SOURCE,
                    batch_id=batch_id,
                )
                bindings.append({
                    "revision_id": revision.revision_id,
                    "content_hash": revision.content_hash,
                })
            provider_receipt = dict(receipts[code])
            check_deadline("database_coverage_append")
            coverage = append_source_coverage(
                connection,
                fact_kind="event",
                stock_code=code,
                window_start=window_start,
                window_end=fact_cutoff_at.date(),
                known_at=received_at,
                received_at=received_at,
                covered_through_at=fact_cutoff_at,
                watermark_kind="QUERY_CUTOFF",
                watermark_evidence={
                    "provider": CNINFO_EVENT_SOURCE,
                    "period": "announcement",
                    "capture_purpose": CAPTURE_PURPOSE,
                    "primary_provider": CNINFO_EVENT_SOURCE,
                    "fallback_reason": "",
                    "provider_receipt": provider_receipt,
                    "provider_receipt_hash": canonical_hash(provider_receipt),
                    "fact_cutoff_at": _dt_text(fact_cutoff_at),
                    "decision_at": _dt_text(received_at),
                    "received_at": _dt_text(received_at),
                    "query_end_time": fact_cutoff_at.strftime("%Y%m%d%H%M%S"),
                    "source_response_hash": response_hashes[code],
                    "global_batch_root_hash": batch_root_hash,
                    "catalog_batch_id": catalog.batch_id,
                    "catalog_manifest_hash": catalog.manifest_hash,
                    "catalog_member_set_hash": catalog.member_set_hash,
                    "catalog_member_count": len(catalog.codes),
                    "window_start": window_start.isoformat(),
                    "window_end": fact_cutoff_at.date().isoformat(),
                    "exposure_scope": dict(scope),
                },
                source_rows=source_rows,
                fact_bindings=bindings,
                source=CNINFO_EVENT_SOURCE,
                batch_id=batch_id,
            )
            if coverage.source_response_hash != response_hashes[code]:
                raise ExposureRiskWatchError(
                    "database_publish", f"source_response_hash_drift={code}"
                )
            coverage_ids.append(coverage.coverage_id)

    check_deadline("database_publish_start")
    with engine.connect() as connection:
        lock_acquired = False
        try:
            if connection.dialect.name == "mysql":
                check_deadline("database_publisher_lock")
                acquired = connection.execute(
                    text("SELECT GET_LOCK('probiga:qmt-announcement-pit', 0)")
                ).scalar_one()
                if int(acquired or 0) != 1:
                    raise ExposureRiskWatchError(
                        "database_publish", "ANNOUNCEMENT_PUBLISHER_ALREADY_RUNNING"
                    )
                lock_acquired = True
                check_deadline("database_lock_commit")
                connection.commit()
            with connection.begin():
                write_rows(connection)
                check_deadline("database_commit")
        finally:
            if lock_acquired:
                try:
                    if connection.in_transaction():
                        connection.rollback()
                    if stage_fn is not None:
                        stage_fn("database_publisher_unlock")
                    connection.execute(
                        text("SELECT RELEASE_LOCK('probiga:qmt-announcement-pit')")
                    )
                    connection.commit()
                except Exception:
                    connection.invalidate()
                    raise
    return coverage_ids


def _risk_hits(results: Mapping[str, Sequence[Mapping[str, Any]]]) -> list[dict[str, Any]]:
    evaluator = EventRiskEngine()
    output: list[dict[str, Any]] = []
    for code, rows in sorted(results.items()):
        risk = evaluator.analyze({"news": {"notices": list(rows), "news": []}})
        if risk["event_risk_level"] in {"MEDIUM", "HIGH", "CRITICAL"}:
            output.append({
                "stock_code": code,
                "event_risk_level": risk["event_risk_level"],
                "triggered_events": risk["triggered_events"],
            })
    return output


def _published_coverage_ids(
    engine: Any,
    *,
    batch_id: str,
    catalog: AnnouncementCatalog,
) -> list[str]:
    """Return exact committed coverage, never a partial batch."""

    with engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT stock_code, coverage_id
              FROM st_pit_source_coverage
             WHERE batch_id=:batch_id
               AND fact_kind='event'
               AND source=:source
               AND coverage_status='COMPLETE'
             ORDER BY stock_code
        """), {
            "batch_id": batch_id,
            "source": CNINFO_EVENT_SOURCE,
        }).mappings().all()
    by_code = {
        str(row.get("stock_code") or "").zfill(6): str(
            row.get("coverage_id") or ""
        )
        for row in rows
    }
    if len(rows) != len(catalog.codes) or set(by_code) != set(catalog.codes):
        return []
    return [by_code[code] for code in catalog.codes]


def _require_committed_batch_coverage(
    engine: Any, *, batch_id: str, catalog: AnnouncementCatalog,
    expected_ids: Sequence[str],
) -> None:
    observed = _published_coverage_ids(engine, batch_id=batch_id, catalog=catalog)
    if not observed or observed != list(expected_ids):
        raise ExposureRiskWatchError("database_batch_readback", "COMMITTED_COVERAGE_DIFFERS")


def run_exposure_event_risk_watch(
    engine: Any,
    *,
    checkpoint_root: Path,
    now_fn: Callable[[], datetime] = lambda: datetime.now(SHANGHAI),
    provider_factory: Callable[[], Any] = CninfoMarketAnnouncementProvider,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    deadline_minutes: int = DEFAULT_DEADLINE_MINUTES,
    batch_size: int = DEFAULT_BATCH_SIZE,
    progress_fn: Callable[[dict[str, Any]], None] | None = None,
    stage_fn: Callable[[str], None] | None = None,
    monotonic_fn: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Capture, atomically publish, and read back one complete exposure scope."""

    if not 1 <= int(lookback_days) <= 14:
        raise ValueError("lookback_days must be 1..14")
    if not 5 <= int(deadline_minutes) <= 30:
        raise ValueError("deadline_minutes must be 5..30")
    if not 1 <= int(batch_size) <= 4:
        raise ValueError("batch_size must be 1..4")
    job_deadline = monotonic_fn() + int(deadline_minutes) * 60

    def check_stage(stage: str) -> None:
        if stage_fn is not None:
            stage_fn(stage)
        if monotonic_fn() >= job_deadline:
            raise ExposureRiskWatchError(stage, "JOB_DEADLINE_EXPIRED")

    observed_at = _now_shanghai(now_fn())
    check_stage("schema_validation")
    assert_announcement_pit_schema_prepared(engine)
    check_stage("exposure_scope")
    exposure_sources = load_exposure_universe(engine)
    check_stage("exposure_scope_readback")
    if not exposure_sources:
        return {
            "schema": CAPTURE_SCHEMA,
            "status": "COMPLETE",
            "reason": "NO_CURRENT_EXPOSURE",
            "total_units": 0,
            "completed_units": 0,
            "database_writes": False,
            "coverage_readback": True,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
    check_stage("exposure_catalog")
    current_catalog, current_scope = build_exposure_catalog(
        engine,
        exposure_sources=exposure_sources,
        decision_at=observed_at,
    )
    check_stage("checkpoint_identity")
    resumed = _find_resumable_checkpoint(
        checkpoint_root,
        catalog=current_catalog,
        scope=current_scope,
        observed_at=observed_at,
        deadline_minutes=deadline_minutes,
    )
    if resumed is None:
        fact_cutoff = observed_at.replace(microsecond=0)
        catalog, scope = current_catalog, current_scope
        window_start = fact_cutoff.date() - timedelta(days=int(lookback_days))
        batch_id = _batch_identity(
            fact_cutoff_at=fact_cutoff,
            window_start=window_start,
            catalog=catalog,
        )
    else:
        fact_cutoff, batch_id = resumed
        catalog, scope = current_catalog, current_scope
        window_start = fact_cutoff.date() - timedelta(days=int(lookback_days))
    window_end = fact_cutoff.date()
    deadline_at = fact_cutoff + timedelta(minutes=int(deadline_minutes))
    checkpoint = AnnouncementCheckpoint.open(
        checkpoint_root,
        batch_id=batch_id,
        fact_cutoff_at=fact_cutoff,
        window_start=window_start,
        window_end=window_end,
        catalog=catalog,
        resume=True,
        source=CNINFO_EVENT_SOURCE,
        fallback_reason="",
        manifest_extra=_checkpoint_extra(scope),
    )

    results: dict[str, list[dict[str, Any]]] = {}
    receipts: dict[str, dict[str, Any]] = {}
    for code in catalog.codes:
        staged = checkpoint.load(code)
        staged_receipt = checkpoint.load_provider_receipt(code)
        if staged is not None and staged_receipt is not None:
            results[code] = staged
            receipts[code] = staged_receipt
    total = len(catalog.codes)
    completed = len(results)
    last_progress_at = observed_at

    def progress(current: str | None = None) -> None:
        nonlocal last_progress_at
        last_progress_at = _now_shanghai(now_fn())
        if progress_fn is not None:
            progress_fn({
                "schema": CAPTURE_SCHEMA,
                "status": "RUNNING",
                "batch_id": batch_id,
                "total_units": total,
                "completed_units": completed,
                "current_unit": current,
                "last_progress_at": _dt_text(last_progress_at),
                "checkpoint": str(checkpoint.root),
            })

    prepared = checkpoint.load_prepared_publish()
    if prepared is None:
        remaining = [code for code in catalog.codes if code not in results]
        if remaining:
            check_stage("official_provider_capture")
            adapter = ProviderBackedAnnouncementAdapter(
                provider_factory(), workers=min(4, int(batch_size))
            )
            try:
                adapter.bind_capture_deadline(
                    fact_cutoff_at=fact_cutoff,
                    max_capture_delay=timedelta(minutes=int(deadline_minutes)),
                )
                if receipts:
                    adapter.restore_capture_receipts(receipts)
                start_time = window_start.strftime("%Y%m%d000000")
                end_time = fact_cutoff.strftime("%Y%m%d%H%M%S")
                for offset in range(0, len(remaining), int(batch_size)):
                    check_stage("official_provider_capture")
                    chunk = remaining[offset:offset + int(batch_size)]
                    qmt_codes = [catalog.qmt_by_code[code] for code in chunk]
                    for qmt_code in qmt_codes:
                        adapter.download_history_data(
                            qmt_code,
                            period="announcement",
                            start_time=start_time,
                            end_time=end_time,
                        )
                    frames = adapter.get_market_data_ex(
                        field_list=[],
                        stock_list=qmt_codes,
                        period="announcement",
                        start_time=start_time,
                        end_time=end_time,
                        count=-1,
                        dividend_type="none",
                        fill_data=False,
                    )
                    staged_receipts = adapter.staged_capture_receipts()
                    for code in chunk:
                        check_stage("checkpoint_write")
                        qmt_code = catalog.qmt_by_code[code]
                        frame = frames.get(qmt_code, pd.DataFrame())
                        events = parse_qmt_announcement_frame(
                            stock_code=code,
                            qmt_code=qmt_code,
                            frame=frame,
                            fact_cutoff_at=fact_cutoff,
                            window_start=window_start,
                            source=CNINFO_EVENT_SOURCE,
                        )
                        receipt = dict(staged_receipts[code])
                        checkpoint.save(code, events, provider_receipt=receipt)
                        results[code] = events
                        receipts[code] = receipt
                        completed += 1
                        progress(code)
                receipts = adapter.capture_receipts()
            finally:
                adapter.close()
            for code in catalog.codes:
                checkpoint.save(
                    code,
                    results[code],
                    provider_receipt=receipts[code],
                )
        received_at = _now_shanghai(now_fn())
        check_stage("capture_receipt_sealing")
        if received_at > deadline_at:
            raise ExposureRiskWatchError("capture", "CAPTURE_DEADLINE_EXPIRED")
        batch_root_hash, entries = _build_batch_root(
            batch_id=batch_id,
            fact_cutoff_at=fact_cutoff,
            received_at=received_at,
            window_start=window_start,
            catalog=catalog,
            results=results,
            receipts=receipts,
            scope=scope,
        )
        checkpoint.prepare_publish(
            batch_root_hash=batch_root_hash,
            received_at=received_at,
        )
    else:
        received_at, batch_root_hash = prepared
        results = checkpoint.load_complete()
        receipts = {
            code: dict(checkpoint.load_provider_receipt(code) or {})
            for code in catalog.codes
        }
        rebuilt_root, entries = _build_batch_root(
            batch_id=batch_id,
            fact_cutoff_at=fact_cutoff,
            received_at=received_at,
            window_start=window_start,
            catalog=catalog,
            results=results,
            receipts=receipts,
            scope=scope,
        )
        if rebuilt_root != batch_root_hash:
            raise ExposureRiskWatchError("checkpoint", "PREPARED_ROOT_DRIFT")

    check_stage("database_publish_start")
    try:
        coverage_ids = _publish_batch(
            engine,
            batch_id=batch_id,
            batch_root_hash=batch_root_hash,
            entries=entries,
            fact_cutoff_at=fact_cutoff,
            received_at=received_at,
            window_start=window_start,
            catalog=catalog,
            results=results,
            receipts=receipts,
            scope=scope,
            deadline_at=deadline_at,
            now_fn=now_fn,
            stage_fn=check_stage,
        )
    except Exception:
        # A connection-scoped lock release or process boundary can fail after
        # the atomic commit.  Prove the exact full scope before deciding that
        # another publication attempt is needed; partial coverage never passes.
        check_stage("database_commit_recovery_readback")
        coverage_ids = _published_coverage_ids(
            engine,
            batch_id=batch_id,
            catalog=catalog,
        )
        if not coverage_ids:
            raise
    # General PIT availability may come from an older full-market batch. It
    # does not prove this job's atomic publication survived the commit.
    check_stage("database_batch_readback")
    _require_committed_batch_coverage(
        engine, batch_id=batch_id, catalog=catalog, expected_ids=coverage_ids,
    )
    check_stage("coverage_readback")
    readback = load_event_facts(
        engine,
        codes=catalog.codes,
        decision_at=received_at,
        fact_cutoff_at=fact_cutoff,
        start_date=window_start,
        end_date=window_end,
        require_qmt_complete_batch=False,
    )
    unavailable = [
        code for code in catalog.codes if readback.status_for(code) != PIT_AVAILABLE
    ]
    if unavailable:
        raise ExposureRiskWatchError(
            "coverage_readback", f"unavailable_codes={unavailable[:20]}"
        )
    check_stage("checkpoint_completion")
    checkpoint.mark_complete(
        batch_root_hash=batch_root_hash,
        received_at=received_at,
    )
    hits = _risk_hits(results)
    check_stage("completion_receipt")
    return {
        "schema": CAPTURE_SCHEMA,
        "status": "COMPLETE",
        "batch_id": batch_id,
        "batch_root_hash": batch_root_hash,
        "fact_cutoff_at": _dt_text(fact_cutoff),
        "received_at": _dt_text(received_at),
        "total_units": total,
        "completed_units": total,
        "current_unit": None,
        "last_progress_at": _dt_text(last_progress_at),
        "checkpoint": str(checkpoint.root),
        "event_count": sum(len(rows) for rows in results.values()),
        "coverage_count": len(coverage_ids),
        "coverage_readback": True,
        "database_writes": True,
        "risk_hit_count": len(hits),
        "risk_hits": hits,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


__all__ = [
    "CAPTURE_PURPOSE",
    "CAPTURE_SCHEMA",
    "DEFAULT_CHECKPOINT_ROOT",
    "ExposureRiskWatchError",
    "SCHEDULER_SCRIPT_ARGS",
    "SCHEDULER_SCRIPT_PATH",
    "SCHEDULER_TASK_TYPE",
    "build_exposure_catalog",
    "load_exposure_universe",
    "read_exposure_watch_checkpoint_progress",
    "run_exposure_event_risk_watch",
]
