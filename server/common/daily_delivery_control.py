"""Durable control plane for one authoritative daily stock-pool delivery.

The scheduler task table remains configuration and a compact status view.  The
tables in this module are the durable runtime truth: one session per trade date
and release, append-only stage attempts with leases/fencing tokens, and an
append-only terminal receipt for both successful and blocked deliveries.

Runtime callers are DML-only.  DDL belongs to the privileged production schema
bundle.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from types import MappingProxyType
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text
from server.common.authoritative_market_clock import (
    PRODUCTION_TIMEZONE,
    STRATEGY_POOL_INITIAL_TIME,
    STRATEGY_POOL_PREMARKET_REVISION_TIME,
    StrategyPoolDecisionSlot,
)
from server.common.component_release_attestation import load_component_attestation
from tools.qmt_host_ownership_contract import (
    WINDOWS_QMT_EDGE_TASK_TYPES,
    WINDOWS_NON_QMT_EGRESS_TASK_TYPES,
)


SESSION_TABLE = "st_daily_delivery_session"
ATTEMPT_TABLE = "st_daily_stage_attempt"
RECEIPT_TABLE = "st_daily_delivery_receipt"
DELIVERY_RECEIPT_SCHEMA = "probiga.daily-delivery.v1"
SESSION_SCHEMA = "probiga.daily-run.v1"
STRATEGY_RELEASE_SCHEMA = "probiga.strategy-release-binding.v1"
SCORE_SNAPSHOT_SCHEMA = "probiga.score-snapshot-identity.v1"
SCHEDULER_VALIDATION_EVIDENCE_SCHEMA = (
    "probiga.scheduler-validation-evidence.v1"
)
DAILY_STAGE_IDEMPOTENT_REPLAY_SCHEMA = (
    "probiga.daily-stage-idempotent-replay.v1"
)

TERMINAL_STATUSES = frozenset({"PASS", "DEGRADED", "BLOCKED"})
ATTEMPT_TERMINAL_STATUSES = frozenset(
    {"SUCCESS", "DEGRADED", "BLOCKED", "FAILED", "TIMEOUT", "STOPPED", "SUPERSEDED"}
)


def completed_data_stage_status(stage_name: object, status: object) -> bool:
    """A sealed finance exclusion is usable data, with its degradation intact."""
    normalized = str(status or "").strip().lower()
    return normalized == "success" or (
        str(stage_name or "").strip() == "stock_finance"
        and normalized == "degraded"
    )

SHA40_RE = re.compile(r"^[0-9a-f]{40}$")
SHA64_RE = re.compile(r"^[0-9a-f]{64}$")
RUN_UID_RE = re.compile(r"^[0-9a-f]{32,64}$")
CONTROL_TIMEZONE = ZoneInfo("Asia/Shanghai")

_REQUIRED_COLUMNS = {
    SESSION_TABLE: frozenset(
        {
            "id",
            "session_uid",
            "run_id",
            "trade_date",
            "release_id",
            "strategy_release_id",
            "status",
            "latest_generation",
            "latest_fencing_token",
            "canonical_receipt_uid",
            "started_at",
            "terminal_at",
            "updated_at",
        }
    ),
    ATTEMPT_TABLE: frozenset(
        {
            "id",
            "attempt_uid",
            "session_uid",
            "scheduler_run_uid",
            "stage_name",
            "shard_id",
            "attempt_no",
            "status",
            "input_root_sha256",
            "output_dataset_id",
            "lease_owner",
            "lease_until",
            "fencing_token",
            "checkpoint_json",
            "error_code",
            "error_detail",
            "started_at",
            "finished_at",
        }
    ),
    RECEIPT_TABLE: frozenset(
        {
            "id",
            "receipt_uid",
            "session_uid",
            "generation",
            "status",
            "scheduler_run_uid",
            "stage_name",
            "release_id",
            "strategy_release_id",
            "analysis_run_uid",
            "governance_run_uid",
            "score_snapshot_id",
            "canonical_pool_sha256",
            "recommendation_count",
            "retryable",
            "blocking_stage",
            "error_code",
            "error_detail",
            "receipt_json",
            "receipt_sha256",
            "created_at",
        }
    ),
}


class DailyDeliveryFenceLost(RuntimeError):
    """The caller no longer owns the newest live attempt for one stage."""


class DailyDeliveryLeaseHeld(RuntimeError):
    """A different owner still has a live lease for the requested stage."""


class DailyDeliveryControlError(RuntimeError):
    """A published dataset is missing or its sealed identity is invalid."""


ANALYSIS_PUBLICATION_REFERENCE_SCHEMA = "probiga.analysis-publication-reference.v1"


def analysis_publication_reference(receipt: Mapping[str, object]) -> dict[str, object]:
    """Bounded history identity; the full publication belongs to its checkpoint."""
    from server.common.analysis_pool_receipt import publication_receipt_is_valid

    if not publication_receipt_is_valid(receipt):
        raise DailyDeliveryControlError("analysis publication receipt is invalid")
    return {
        "schema": ANALYSIS_PUBLICATION_REFERENCE_SCHEMA,
        "run_uid": receipt["run_uid"], "trade_date": receipt["trade_date"],
        "build_sha": receipt["build_sha"],
        "receipt_sha256": canonical_sha256(receipt),
    }


def analysis_publication_degradations(receipt: Mapping[str, object]) -> list[dict[str, object]]:
    """Report exclusions from this score snapshot even after a source refresh."""
    from server.common.analysis_pool_receipt import decode_score_snapshot

    snapshot = decode_score_snapshot(receipt.get("score_snapshot"), trade_date=str(receipt.get("trade_date")))
    exclusions = {row["stock_code"]: row["finance_data_exclusion"]
                  for row in snapshot["scored_rows"] if row.get("finance_data_exclusion")}
    result = []
    if exclusions:
        codes = sorted(exclusions)
        result.append({"stage": "stock_finance", "reason_code": "STOCK_FINANCE_DATA_EXCLUDED",
            "count": len(codes), "stock_code_sample": codes[:20],
            "stock_code_sample_complete": len(codes) <= 20,
            "reason_sample": {code: exclusions[code].get("reason_code") for code in codes[:20]},
            "exclusions_sha256": canonical_sha256(exclusions),
            "score_snapshot_sha256": receipt["score_snapshot"]["payload_sha256"]})
    if snapshot["data_blocked_count"]:
        result.append({"stage": "analysis_fast", "reason_code": "STOCK_EVIDENCE_UNAVAILABLE",
            "count": snapshot["data_blocked_count"],
            "score_snapshot_sha256": receipt["score_snapshot"]["payload_sha256"]})
    return result


def bind_analysis_publication_checkpoint(
    checkpoint: Mapping[str, object], receipt: Mapping[str, object],
) -> dict[str, object]:
    reference = analysis_publication_reference(receipt)
    replay = str(checkpoint.get("replay_output") or "")
    references = _publication_references(replay)
    if reference not in references:
        raise DailyDeliveryControlError("analysis publication differs from validated history")
    core = {key: value for key, value in checkpoint.items() if key != "evidence_sha256"}
    core["publication_receipt"] = dict(receipt)
    if len(json.dumps(core, ensure_ascii=False).encode("utf-8")) > 15 * 1024 * 1024:
        raise DailyDeliveryControlError("analysis checkpoint exceeds MEDIUMTEXT budget")
    return {**core, "evidence_sha256": canonical_sha256(core)}


def _publication_references(output: str) -> list[dict]:
    result = []
    def visit(value):
        if isinstance(value, dict):
            if value.get("schema") == ANALYSIS_PUBLICATION_REFERENCE_SCHEMA:
                result.append(value)
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)
    for line in output.splitlines():
        if line.strip().startswith("{"):
            visit(json.loads(line))
    return result


def load_completed_market_capture_receipt(
    engine, *, raw_run_id: str, stage_name: str, target_date: object,
    build_sha: str, decision_at: datetime,
) -> dict[str, object]:
    """Require the scheduler's verified completion before consuming a capture.

    Capture-row publication time describes its publication transaction. The
    completed stage is the downstream availability boundary, after the raw
    batch committed and the independent validator accepted it.
    """
    if stage_name not in {"target_turnover_snapshot", "analysis_upper_evidence_prepare"}:
        raise DailyDeliveryControlError("market capture stage is invalid")
    target, source_build = _normalized_trade_date(target_date), _normalized_release_id(build_sha)
    if RUN_UID_RE.fullmatch(str(raw_run_id)) is None:
        raise DailyDeliveryControlError("market capture run identity is invalid")
    cutoff = decision_at
    if cutoff.tzinfo is not None:
        cutoff = cutoff.astimezone(CONTROL_TIMEZONE).replace(tzinfo=None)
    with (nullcontext(engine) if hasattr(engine, "execute") else engine.connect()) as connection:
        rows = connection.execute(text(f"""
            SELECT a.*, s.trade_date, s.release_id
            FROM {ATTEMPT_TABLE} a JOIN {SESSION_TABLE} s ON s.session_uid=a.session_uid
            WHERE s.trade_date=:target
              AND a.stage_name=:stage AND a.status='SUCCESS' AND a.finished_at<=:cutoff
            ORDER BY a.finished_at DESC, a.id DESC LIMIT 100
        """), {"target": target, "stage": stage_name, "cutoff": cutoff}).mappings().all()
    for row in rows:
        try:
            checkpoint = _validated_completed_stage_checkpoint(row, session=row, stage_name=stage_name, engine=engine)
        except DailyDeliveryFenceLost:
            continue
        pending = [str(checkpoint["replay_output"])]
        matched = []
        while pending:
            value = pending.pop()
            if isinstance(value, str):
                for line in value.splitlines():
                    if line.strip().startswith("{"):
                        try:
                            pending.append(json.loads(line))
                        except ValueError:
                            continue
            elif isinstance(value, list):
                pending.extend(value)
            elif isinstance(value, dict):
                if (value.get("schema") == "probiga.market-field-capture.v1"
                        and value.get("run_id") == raw_run_id):
                    matched.append(value)
                else:
                    pending.extend(item for item in value.values() if isinstance(item, (dict, list)))
        if not matched:
            continue
        if (len(matched) != 1 or matched[0].get("status") != "COMPLETED"
                or matched[0].get("target_date") != target
                or matched[0].get("collector_build_sha") != source_build
                or (matched[0].get("validated_by_build_sha") or matched[0].get("collector_build_sha")) != checkpoint["build_sha"]):
            raise DailyDeliveryControlError("MARKET_CAPTURE_COMPLETION_BINDING_INVALID")
        return {"receipt": matched[0], "known_at": _datetime_value(row["finished_at"]),
                "attempt_uid": row["attempt_uid"], "validated_by_build_sha": checkpoint["build_sha"],
                "evidence_sha256": checkpoint["evidence_sha256"],
                "input_receipt_root_sha256": checkpoint["input_receipt_root_sha256"]}
    raise DailyDeliveryControlError("MARKET_CAPTURE_COMPLETION_UNAVAILABLE")


@dataclass(frozen=True)
class FrozenInitialAnnouncement:
    receipt: Mapping[str, object]
    score_binding: Mapping[str, object]


def signed_initial_announcement_window_matches(
    receipt: Mapping[str, Any], target: date,
) -> bool:
    """Check the source window without confusing its cutoff with availability."""
    from server.common.qmt_announcement_pit import (
        CNINFO_ANNOUNCEMENT_SOURCE,
        QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN,
    )

    cutoff = _datetime_value(receipt.get("fact_cutoff_at"))
    if (
        cutoff is None or cutoff.date() != target
        or receipt.get("window_start") != (target - timedelta(days=30)).isoformat()
        or receipt.get("window_end") != target.isoformat()
        or not receipt.get("catalog_batch_id")
        or any(SHA64_RE.fullmatch(str(receipt.get(key) or "")) is None for key in (
            "catalog_manifest_hash", "catalog_member_set_hash",
        ))
    ):
        return False
    mode = receipt.get("mode")
    if not mode:
        return True
    provenance = receipt.get("reconstruction_provenance")
    return (
        mode in ("HISTORICAL_RECONSTRUCTION", "HISTORICAL_RECONSTRUCTION_EXISTING")
        and receipt.get("trade_date") == target.isoformat()
        and receipt.get("source") == CNINFO_ANNOUNCEMENT_SOURCE
        and receipt.get("execution_origin") == QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN
        and isinstance(provenance, Mapping)
        and provenance.get("execution_origin") == QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN
        and cutoff.time() == time.max
        and _datetime_value(receipt.get("source_query_cutoff_at")) == cutoff
    )


def _read_signed_announcement_completion(
    engine, *, target: str, decision_at: datetime, batch: Mapping[str, Any] | None,
) -> FrozenInitialAnnouncement | None:
    from server.common.analysis_pool_receipt import FROZEN_INITIAL_ANNOUNCEMENT_SCHEMA
    from server.common.qmt_announcement_pit import QMT_ANNOUNCEMENT_TASK_SCHEMA, validate_task_result

    stage = "qmt_announcement_pit"
    session = date.fromisoformat(target)
    if decision_at.tzinfo is not None:
        decision_at = decision_at.astimezone(CONTROL_TIMEZONE).replace(tzinfo=None)
    if batch is None:
        if decision_at < datetime.combine(session, STRATEGY_POOL_INITIAL_TIME):
            raise ValueError("initial announcement selection precedes its scheduled slot")
        not_before = datetime.combine(session, time.min)
    else:
        received = _datetime_value(batch.get("received_at"))
        fact_cutoff = _datetime_value(batch.get("fact_cutoff_at"))
        if fact_cutoff is None or received is None or not fact_cutoff <= received <= decision_at:
            return None
        not_before = received
    with (nullcontext(engine) if hasattr(engine, "execute") else engine.connect()) as connection:
        attempts = connection.execute(text(f"""
            SELECT a.*, s.trade_date, s.release_id
            FROM {ATTEMPT_TABLE} a JOIN {SESSION_TABLE} s ON s.session_uid=a.session_uid
            WHERE s.trade_date=:target AND a.stage_name=:stage AND a.status='SUCCESS'
              AND a.finished_at BETWEEN :not_before AND :decision
            ORDER BY a.finished_at DESC, a.id DESC LIMIT 12
        """), {
            "target": target, "stage": stage, "not_before": not_before, "decision": decision_at,
        }).mappings().all()
    for attempt in attempts:
        try:
            checkpoint = _validated_completed_stage_checkpoint(
                attempt, session=attempt, stage_name=stage, engine=engine,
            )
        except DailyDeliveryFenceLost:
            continue
        matches = []
        for raw in str(checkpoint["replay_output"]).splitlines():
            try:
                item = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if (
                isinstance(item, dict)
                and item.get("schema") == QMT_ANNOUNCEMENT_TASK_SCHEMA
                and (batch is None or item.get("batch_id") == batch["batch_id"])
            ):
                matches.append(item)
        if not matches:
            continue
        if len(matches) != 1:
            if batch is None:
                continue
            raise DailyDeliveryControlError("announcement completion receipt is ambiguous")
        item = matches[0]
        try:
            received = _datetime_value(item.get("received_at"))
            fact_cutoff = _datetime_value(item.get("fact_cutoff_at"))
            finished = _datetime_value(attempt["finished_at"])
            if (
                item.get("window_end") != target or validate_task_result(item, 0) != "complete"
                or fact_cutoff is None or received is None or finished is None
                or not fact_cutoff <= received <= finished <= decision_at
                or (batch is None and not signed_initial_announcement_window_matches(item, session))
                or (batch is not None and (
                    item.get("source") != batch["source"]
                    or item.get("batch_root_hash") != batch["batch_root_hash"]
                    or fact_cutoff != _datetime_value(batch.get("fact_cutoff_at"))
                    or received != _datetime_value(batch.get("received_at"))
                    or item.get("stock_count") != batch["catalog_member_count"]
                ))
            ):
                raise ValueError("nightly announcement completion differs")
            if batch is None and item.get("mode"):
                validation_uid = str(attempt["scheduler_run_uid"])
                validation_build = checkpoint["build_sha"]
                provenance = item["reconstruction_provenance"]
                if (
                    item.get("validation_run_uid") != validation_uid
                    or item.get("validation_build_sha") != validation_build
                    or (item["mode"] == "HISTORICAL_RECONSTRUCTION" and (
                        provenance.get("scheduler_run_uid") != validation_uid
                        or provenance.get("build_sha") != validation_build
                    ))
                ):
                    raise ValueError("historical announcement scheduler identity differs")
        except (TypeError, ValueError) as exc:
            if batch is None:
                continue
            raise DailyDeliveryControlError("announcement completion receipt differs") from exc
        binding = {
            "schema": FROZEN_INITIAL_ANNOUNCEMENT_SCHEMA, "task_type": stage,
            "trade_date": target, "scheduler_run_uid": str(attempt["scheduler_run_uid"]),
            "evidence_sha256": checkpoint["evidence_sha256"],
            "input_receipt_root_sha256": checkpoint["input_receipt_root_sha256"],
            "producer_build_sha": checkpoint["build_sha"],
            "contract_release_id": str(attempt["release_id"]),
            "batch_id": item["batch_id"], "batch_root_hash": item["batch_root_hash"],
            "source": item["source"], "fact_cutoff_at": fact_cutoff.isoformat(timespec="microseconds"),
            "received_at": received.isoformat(timespec="microseconds"),
            "finished_at": finished.isoformat(timespec="microseconds"),
        }
        return FrozenInitialAnnouncement(
            MappingProxyType({
                **item, "stage_finished_at": finished.replace(tzinfo=CONTROL_TIMEZONE).isoformat(),
                "stage_attempt_uid": attempt["attempt_uid"], "stage_evidence_sha256": checkpoint["evidence_sha256"],
            }),
            MappingProxyType(binding),
        )
    return None


def load_signed_announcement_source(
    engine, *, batch: Mapping[str, Any], decision_at: datetime,
) -> dict[str, Any] | None:
    """Resolve the exact signed nightly completion for an already validated batch."""
    result = _read_signed_announcement_completion(
        engine, target=_normalized_trade_date(batch["window_end"]), decision_at=decision_at, batch=batch,
    )
    return dict(result.score_binding) if result else None


def select_frozen_initial_announcement_source(
    engine, *, trade_date: str, decision_at: datetime,
) -> FrozenInitialAnnouncement | None:
    """Choose the latest signed batch available by the actual initial cutoff."""
    return _read_signed_announcement_completion(
        engine, target=_normalized_trade_date(trade_date), decision_at=decision_at, batch=None,
    )


def load_completed_cninfo_announcement_at(
    engine, *, batch: Mapping[str, Any], decision_at: datetime,
) -> datetime | None:
    """Return the validated publication time for this exact durable batch."""
    source = load_signed_announcement_source(engine, batch=batch, decision_at=decision_at)
    return _datetime_value(source["finished_at"]) if source else None


def load_published_analysis_receipt(
    engine, target_date: object, decision_at: datetime | None = None,
    *, run_uid: str | None = None,
    capture_score: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, object]:
    """Read the exact successful scoring publication, independent of governance.

    An invalid newest publication fails closed. Older mutable projections never
    substitute for its immutable, complete scoring universe.
    """
    from server.common.analysis_pool_receipt import PREMARKET_POOL_TASK, decode_score_snapshot

    target = _normalized_trade_date(target_date)
    cutoff = decision_at or _control_now()
    if cutoff.tzinfo is not None:
        cutoff = cutoff.astimezone(CONTROL_TIMEZONE).replace(tzinfo=None)
    params = {"target": target, "cutoff": cutoff, "premarket_stage": PREMARKET_POOL_TASK}
    run_clause = ""
    if run_uid is not None:
        if RUN_UID_RE.fullmatch(run_uid) is None:
            raise DailyDeliveryControlError("analysis publication run identity is invalid")
        run_clause = " AND a.output_dataset_id=:run_uid"
        params["run_uid"] = run_uid
    with (nullcontext(engine) if hasattr(engine, "execute") else engine.connect()) as connection:
        row = connection.execute(text(f"""
            SELECT a.*, s.trade_date, s.release_id
            FROM {ATTEMPT_TABLE} a JOIN {SESSION_TABLE} s
              ON s.session_uid=a.session_uid
            WHERE s.trade_date=:target AND a.stage_name IN ('analysis_fast', :premarket_stage)
              AND a.status='SUCCESS' AND a.finished_at<=:cutoff
              {run_clause}
            ORDER BY a.finished_at DESC, a.id DESC LIMIT 1
        """), params).mappings().first()
    if row is None:
        raise DailyDeliveryControlError("ANALYSIS_PUBLISHED_SNAPSHOT_UNAVAILABLE")
    try:
        checkpoint = _validated_completed_stage_checkpoint(row, session=row, stage_name=str(row["stage_name"]), engine=engine)
        receipt = dict(checkpoint.get("publication_receipt") or {})
        reference = analysis_publication_reference(receipt)
        snapshot = decode_score_snapshot(receipt.get("score_snapshot"), trade_date=target)
        published = datetime.fromisoformat(str(receipt.get("published_at")))
        if (receipt["trade_date"] != target or receipt["build_sha"] != checkpoint["build_sha"]
                or receipt["publisher_task_type"] != row["stage_name"]
                or receipt["run_uid"] != row["output_dataset_id"]
                or snapshot["run_uid"] != receipt["run_uid"]
                or snapshot["build_sha"] != receipt["build_sha"]
                or published > cutoff):
            raise ValueError("publication identity differs")
        references = _publication_references(str(checkpoint["replay_output"]))
        if reference not in references:
            raise ValueError("publication history binding differs")
    except (ValueError, TypeError, KeyError, DailyDeliveryFenceLost) as exc:
        raise DailyDeliveryControlError("ANALYSIS_PUBLISHED_SNAPSHOT_INVALID") from exc
    if capture_score is not None:
        capture_score(snapshot)
    return receipt


def canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _control_now() -> datetime:
    """Return the shared naive database wall clock used by Linux and Windows."""

    return datetime.now(CONTROL_TIMEZONE).replace(tzinfo=None)


def _normalized_trade_date(value: object) -> str:
    raw = str(value or "").strip()[:10]
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError("daily delivery trade date is invalid") from exc
    if parsed.isoformat() != raw:
        raise ValueError("daily delivery trade date is invalid")
    return raw


def _normalized_release_id(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if SHA40_RE.fullmatch(normalized) is None or normalized == "0" * 40:
        raise ValueError("daily delivery release id is invalid")
    return normalized


def strategy_release_identity() -> str:
    """Hash the immutable built-in strategy/config manifests.

    Dynamic strategy versions remain independently hash-bound inside the
    governance result.  This identity prevents a silent built-in parameter
    change from sharing a daily session with the prior configuration.
    """

    from server.common.versioned_strategy_config import (
        load_market_state_config,
        load_stock_manifest,
        market_state_config_hash,
        stock_manifest_hash,
    )

    stock = load_stock_manifest()
    market = load_market_state_config()
    return canonical_sha256(
        {
            "schema": STRATEGY_RELEASE_SCHEMA,
            "stock_config_version": str(stock.get("config_version") or ""),
            "stock_manifest_sha256": stock_manifest_hash(),
            "market_config_version": str(market.get("config_version") or ""),
            "market_state_config_sha256": market_state_config_hash(),
        }
    )


def daily_session_identity(trade_date: object, release_id: object) -> dict[str, str]:
    target = _normalized_trade_date(trade_date)
    release = _normalized_release_id(release_id)
    session_uid = canonical_sha256(
        {"schema": SESSION_SCHEMA, "trade_date": target, "release_id": release}
    )
    return {
        "session_uid": session_uid,
        "run_id": f"{target.replace('-', '')}-{release[:12]}",
        "trade_date": target,
        "release_id": release,
    }


def score_snapshot_identity(delivery_receipt: Mapping[str, object]) -> str:
    """Create a stable ID for the exact full-market scoring publication."""

    full_snapshot = str(delivery_receipt.get("score_snapshot_sha256") or "").lower()
    if full_snapshot:
        if SHA64_RE.fullmatch(full_snapshot) is None:
            raise ValueError("full score snapshot root is invalid")
        return full_snapshot
    analysis_run_uid = str(delivery_receipt.get("analysis_run_uid") or "").lower()
    pool_root = str(delivery_receipt.get("canonical_pool_sha256") or "").lower()
    if RUN_UID_RE.fullmatch(analysis_run_uid) is None:
        raise ValueError("analysis run identity is invalid")
    if SHA64_RE.fullmatch(pool_root) is None:
        raise ValueError("canonical pool root is invalid")
    return canonical_sha256(
        {
            "schema": SCORE_SNAPSHOT_SCHEMA,
            "trade_date": _normalized_trade_date(
                delivery_receipt.get("target_trade_date")
            ),
            "analysis_run_uid": analysis_run_uid,
            "analysis_count": int(delivery_receipt.get("analysis_count") or 0),
            "recommendation_count": int(
                delivery_receipt.get("recommendation_count") or 0
            ),
            "canonical_pool_sha256": pool_root,
        }
    )


def _core_input_bindings(
    delivery_receipt: Mapping[str, object],
) -> dict[str, dict[str, object]]:
    aliases = {
        "qmt_stock_daily_canonical": "kline",
        "target_turnover_snapshot": "turnover",
        "stock_finance": "financial",
        "qmt_announcement_pit": "announcement",
    }
    result: dict[str, dict[str, object]] = {}
    rows = delivery_receipt.get("base_data_receipts")
    for raw in rows if isinstance(rows, list) else []:
        row = dict(raw) if isinstance(raw, Mapping) else {}
        stage = str(row.get("task_type") or "")
        key = aliases.get(stage)
        dataset_id = str(row.get("run_uid") or "").lower()
        root = str(row.get("evidence_sha256") or "").lower()
        if (
            key
            and RUN_UID_RE.fullmatch(dataset_id) is not None
            and SHA64_RE.fullmatch(root) is not None
        ):
            result[key] = {
                "dataset_id": dataset_id,
                "root": root,
                "stage": stage,
                "input_root": (
                    str(row.get("input_receipt_root_sha256") or "").lower()
                    or None
                ),
            }
    return result


def _dialect_name(connection) -> str:
    return str(getattr(getattr(connection, "dialect", None), "name", "")).lower()


def _column_names(connection, table_name: str) -> set[str]:
    if _dialect_name(connection) == "sqlite":
        return {
            str(row[1])
            for row in connection.execute(
                text(f"PRAGMA table_info({table_name})")
            ).fetchall()
        }
    return {
        str(row[0])
        for row in connection.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
    }


def validate_daily_delivery_runtime_schema(engine) -> dict[str, object]:
    """Read-only validation for the complete delivery control-plane schema."""

    observed: dict[str, list[str]] = {}
    with engine.connect() as connection:
        for table_name, required in _REQUIRED_COLUMNS.items():
            columns = _column_names(connection, table_name)
            missing = sorted(required - columns)
            if missing:
                raise RuntimeError(
                    f"daily delivery table {table_name} differs: missing_columns={missing}"
                )
            observed[table_name] = sorted(columns)
    return {
        "tables": observed,
        "table_count": len(observed),
        "physical_contract_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def privileged_migrate_daily_delivery_schema(engine) -> dict[str, object]:
    """Create the control-plane tables using the privileged release engine."""

    with engine.begin() as connection:
        sqlite = _dialect_name(connection) == "sqlite"
        if sqlite:
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {SESSION_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_uid TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL,
                    trade_date DATE NOT NULL,
                    release_id TEXT NOT NULL,
                    strategy_release_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'RUNNING',
                    latest_generation INTEGER NOT NULL DEFAULT 0,
                    latest_fencing_token INTEGER NOT NULL DEFAULT 0,
                    canonical_receipt_uid TEXT NULL,
                    started_at DATETIME NOT NULL,
                    terminal_at DATETIME NULL,
                    updated_at DATETIME NOT NULL,
                    UNIQUE (trade_date, release_id)
                )
            """))
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {ATTEMPT_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    attempt_uid TEXT NOT NULL UNIQUE,
                    session_uid TEXT NOT NULL,
                    scheduler_run_uid TEXT NOT NULL UNIQUE,
                    stage_name TEXT NOT NULL,
                    shard_id TEXT NOT NULL DEFAULT 'main',
                    attempt_no INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    input_root_sha256 TEXT NULL,
                    output_dataset_id TEXT NULL,
                    lease_owner TEXT NOT NULL,
                    lease_until DATETIME NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    checkpoint_json TEXT NULL,
                    error_code TEXT NULL,
                    error_detail TEXT NULL,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME NULL,
                    UNIQUE (session_uid, stage_name, shard_id, attempt_no)
                )
            """))
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {RECEIPT_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    receipt_uid TEXT NOT NULL UNIQUE,
                    session_uid TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    scheduler_run_uid TEXT NOT NULL UNIQUE,
                    stage_name TEXT NOT NULL,
                    release_id TEXT NOT NULL,
                    strategy_release_id TEXT NOT NULL,
                    analysis_run_uid TEXT NULL,
                    governance_run_uid TEXT NULL,
                    score_snapshot_id TEXT NULL,
                    canonical_pool_sha256 TEXT NULL,
                    recommendation_count INTEGER NULL,
                    retryable INTEGER NOT NULL DEFAULT 0,
                    blocking_stage TEXT NULL,
                    error_code TEXT NULL,
                    error_detail TEXT NULL,
                    receipt_json TEXT NOT NULL,
                    receipt_sha256 TEXT NOT NULL,
                    created_at DATETIME NOT NULL,
                    UNIQUE (session_uid, generation)
                )
            """))
        else:
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {SESSION_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    session_uid CHAR(64) NOT NULL,
                    run_id VARCHAR(96) NOT NULL,
                    trade_date DATE NOT NULL,
                    release_id CHAR(40) NOT NULL,
                    strategy_release_id CHAR(64) NOT NULL,
                    status VARCHAR(16) NOT NULL DEFAULT 'RUNNING',
                    latest_generation INT NOT NULL DEFAULT 0,
                    latest_fencing_token BIGINT NOT NULL DEFAULT 0,
                    canonical_receipt_uid CHAR(64) NULL,
                    started_at DATETIME NOT NULL,
                    terminal_at DATETIME NULL,
                    updated_at DATETIME NOT NULL,
                    UNIQUE KEY uk_daily_delivery_session_uid (session_uid),
                    UNIQUE KEY uk_daily_delivery_date_release (trade_date, release_id),
                    KEY idx_daily_delivery_status_date (status, trade_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {ATTEMPT_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    attempt_uid CHAR(64) NOT NULL,
                    session_uid CHAR(64) NOT NULL,
                    scheduler_run_uid VARCHAR(64) NOT NULL,
                    stage_name VARCHAR(64) NOT NULL,
                    shard_id VARCHAR(128) NOT NULL DEFAULT 'main',
                    attempt_no INT NOT NULL,
                    status VARCHAR(24) NOT NULL,
                    input_root_sha256 CHAR(64) NULL,
                    output_dataset_id VARCHAR(128) NULL,
                    lease_owner VARCHAR(128) NOT NULL,
                    lease_until DATETIME NOT NULL,
                    fencing_token BIGINT NOT NULL,
                    checkpoint_json MEDIUMTEXT NULL,
                    error_code VARCHAR(128) NULL,
                    error_detail VARCHAR(1000) NULL,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME NULL,
                    UNIQUE KEY uk_daily_stage_attempt_uid (attempt_uid),
                    UNIQUE KEY uk_daily_stage_scheduler_run (scheduler_run_uid),
                    UNIQUE KEY uk_daily_stage_attempt_no
                        (session_uid, stage_name, shard_id, attempt_no),
                    KEY idx_daily_stage_fence
                        (session_uid, stage_name, shard_id, fencing_token),
                    KEY idx_daily_stage_lease (status, lease_until)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {RECEIPT_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    receipt_uid CHAR(64) NOT NULL,
                    session_uid CHAR(64) NOT NULL,
                    generation INT NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    scheduler_run_uid VARCHAR(64) NOT NULL,
                    stage_name VARCHAR(64) NOT NULL,
                    release_id CHAR(40) NOT NULL,
                    strategy_release_id CHAR(64) NOT NULL,
                    analysis_run_uid VARCHAR(64) NULL,
                    governance_run_uid VARCHAR(64) NULL,
                    score_snapshot_id CHAR(64) NULL,
                    canonical_pool_sha256 CHAR(64) NULL,
                    recommendation_count INT NULL,
                    retryable TINYINT(1) NOT NULL DEFAULT 0,
                    blocking_stage VARCHAR(64) NULL,
                    error_code VARCHAR(128) NULL,
                    error_detail VARCHAR(1000) NULL,
                    receipt_json MEDIUMTEXT NOT NULL,
                    receipt_sha256 CHAR(64) NOT NULL,
                    created_at DATETIME NOT NULL,
                    UNIQUE KEY uk_daily_delivery_receipt_uid (receipt_uid),
                    UNIQUE KEY uk_daily_delivery_scheduler_run (scheduler_run_uid),
                    UNIQUE KEY uk_daily_delivery_generation (session_uid, generation),
                    KEY idx_daily_delivery_receipt_status (status, created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
    return validate_daily_delivery_runtime_schema(engine)


def _one_mapping(result) -> dict[str, Any] | None:
    row = result.mappings().first()
    return dict(row) if row is not None else None


def _datetime_value(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.strip().replace(" ", "T"))
        except ValueError:
            return None
    return None


def _text_sha256(value: object) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _validated_completed_stage_checkpoint(
    attempt: Mapping[str, object],
    *,
    session: Mapping[str, object],
    stage_name: str,
    engine=None,
) -> dict[str, object]:
    """Verify the sealed validator evidence behind an immutable stage success."""

    try:
        checkpoint = json.loads(str(attempt.get("checkpoint_json") or ""))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DailyDeliveryFenceLost(
            "completed daily stage checkpoint is unavailable"
        ) from exc
    if not isinstance(checkpoint, dict):
        raise DailyDeliveryFenceLost(
            "completed daily stage checkpoint is unavailable"
        )
    supplied_hash = str(checkpoint.get("evidence_sha256") or "").lower()
    core = {
        key: value
        for key, value in checkpoint.items()
        if key != "evidence_sha256"
    }
    replay_output = str(checkpoint.get("replay_output") or "")
    input_root = str(attempt.get("input_root_sha256") or "").lower()
    exit_code = checkpoint.get("exit_code")
    producer_build_sha = str(checkpoint.get("build_sha") or "").lower()
    contract_release_id = str(checkpoint.get("contract_release_id") or "").lower()
    production = os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production"
    if production:
        try:
            if engine is None:
                raise RuntimeError("component attestation connection is unavailable")
            with (nullcontext(engine) if hasattr(engine, "execute") else engine.connect()) as connection:
                identity = load_component_attestation(connection, producer_build_sha)
            owner = "windows_build_sha" if stage_name in WINDOWS_QMT_EDGE_TASK_TYPES or stage_name in WINDOWS_NON_QMT_EGRESS_TASK_TYPES else "linux_build_sha"
            if identity[owner] != producer_build_sha or identity["contract_build_sha"] != contract_release_id:
                raise RuntimeError("checkpoint producer contract differs")
        except (RuntimeError, TypeError, ValueError, KeyError) as exc:
            raise DailyDeliveryFenceLost("completed daily stage component identity differs") from exc
    else:
        contract_release_id = contract_release_id or producer_build_sha
    if (
        checkpoint.get("schema") != SCHEDULER_VALIDATION_EVIDENCE_SCHEMA
        or SHA64_RE.fullmatch(supplied_hash) is None
        or canonical_sha256(core) != supplied_hash
        or str(checkpoint.get("run_uid") or "").lower()
        != str(attempt.get("scheduler_run_uid") or "").lower()
        or str(checkpoint.get("task_type") or "") != stage_name
        or SHA40_RE.fullmatch(producer_build_sha) is None
        or contract_release_id != str(session.get("release_id") or "").lower()
        or str(checkpoint.get("target_trade_date") or "")
        != str(session.get("trade_date") or "")[:10]
        or (
            checkpoint.get("release_target_date") not in (None, "")
            and str(checkpoint.get("release_target_date") or "")
            != str(session.get("trade_date") or "")[:10]
        )
        or not completed_data_stage_status(stage_name, checkpoint.get("status"))
        or str(checkpoint.get("status") or "").upper()
        != str(attempt.get("status") or "").upper()
        or isinstance(exit_code, bool)
        or not isinstance(exit_code, int)
        or exit_code != 0
        or checkpoint.get("validation_checked") is not True
        or checkpoint.get("validation_ok") is not True
        or SHA64_RE.fullmatch(input_root) is None
        or str(checkpoint.get("input_receipt_root_sha256") or "").lower()
        != input_root
        or str(checkpoint.get("replay_output_sha256") or "").lower()
        != _text_sha256(replay_output)
        or input_root != _text_sha256(replay_output)
    ):
        raise DailyDeliveryFenceLost(
            "completed daily stage checkpoint identity differs"
        )
    return checkpoint


def _completed_stage_replay_checkpoint(
    checkpoint: Mapping[str, object],
    *,
    scheduler_run_uid: str,
    attempt_uid: str,
    fencing_token: int,
    source_attempt: Mapping[str, object],
    now: datetime,
) -> dict[str, object]:
    marker = {
        "schema": DAILY_STAGE_IDEMPOTENT_REPLAY_SCHEMA,
        "status": str(source_attempt.get("status") or ""),
        "task_type": str(checkpoint.get("task_type") or ""),
        "trade_date": str(checkpoint.get("target_trade_date") or ""),
        "release_id": str(source_attempt.get("release_id") or checkpoint.get("contract_release_id") or checkpoint.get("build_sha") or "").lower(),
        "producer_build_sha": str(checkpoint.get("build_sha") or "").lower(),
        "scheduler_run_uid": scheduler_run_uid,
        "attempt_uid": attempt_uid,
        "fencing_token": int(fencing_token),
        "source_attempt_uid": str(source_attempt.get("attempt_uid") or ""),
        "source_scheduler_run_uid": str(
            source_attempt.get("scheduler_run_uid") or ""
        ),
        "source_fencing_token": int(
            source_attempt.get("fencing_token") or 0
        ),
        "input_receipt_root_sha256": str(
            source_attempt.get("input_root_sha256") or ""
        ).lower(),
        "child_process_started": False,
    }
    machine_output = json.dumps(
        marker,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    core = {
        key: value
        for key, value in dict(checkpoint).items()
        if key != "evidence_sha256"
    }
    core.update(
        {
            "run_uid": scheduler_run_uid,
            "started_at": now.replace(microsecond=0).isoformat(sep=" "),
            "validation_message": "idempotent completed daily stage replay",
            "machine_output_sha256": _text_sha256(machine_output),
            "idempotent_replay": marker,
        }
    )
    return {**core, "evidence_sha256": canonical_sha256(core)}


def _select_session(connection, session_uid: str, *, for_update: bool) -> dict[str, Any] | None:
    suffix = " FOR UPDATE" if for_update and _dialect_name(connection) != "sqlite" else ""
    return _one_mapping(
        connection.execute(
            text(
                f"SELECT * FROM {SESSION_TABLE} WHERE session_uid=:session_uid{suffix}"
            ),
            {"session_uid": session_uid},
        )
    )


def ensure_daily_delivery_session(
    connection,
    *,
    trade_date: object,
    release_id: object,
    strategy_release_id: object,
    now: datetime | None = None,
) -> dict[str, Any]:
    identity = daily_session_identity(trade_date, release_id)
    strategy_id = str(strategy_release_id or "").strip().lower()
    if SHA64_RE.fullmatch(strategy_id) is None:
        raise ValueError("strategy release identity is invalid")
    current = now or _control_now()
    params = {
        **identity,
        "strategy_release_id": strategy_id,
        "now": current,
    }
    if _dialect_name(connection) == "sqlite":
        statement = text(f"""
            INSERT OR IGNORE INTO {SESSION_TABLE}
                (session_uid, run_id, trade_date, release_id,
                 strategy_release_id, status, started_at, updated_at)
            VALUES (:session_uid, :run_id, :trade_date, :release_id,
                    :strategy_release_id, 'RUNNING', :now, :now)
        """)
    else:
        statement = text(f"""
            INSERT INTO {SESSION_TABLE}
                (session_uid, run_id, trade_date, release_id,
                 strategy_release_id, status, started_at, updated_at)
            VALUES (:session_uid, :run_id, :trade_date, :release_id,
                    :strategy_release_id, 'RUNNING', :now, :now)
            ON DUPLICATE KEY UPDATE session_uid=session_uid
        """)
    connection.execute(statement, params)
    session = _select_session(connection, identity["session_uid"], for_update=True)
    if session is None:
        raise RuntimeError("daily delivery session could not be created")
    if (
        str(session.get("run_id") or "") != identity["run_id"]
        or str(session.get("trade_date") or "")[:10] != identity["trade_date"]
        or str(session.get("release_id") or "").lower() != identity["release_id"]
        or str(session.get("strategy_release_id") or "").lower() != strategy_id
    ):
        raise RuntimeError("daily delivery session identity differs")
    return session


def load_daily_delivery_session(connection, session_uid: object) -> dict[str, Any]:
    normalized = str(session_uid or "").strip().lower()
    if SHA64_RE.fullmatch(normalized) is None:
        raise ValueError("daily delivery session identity is invalid")
    session = _select_session(connection, normalized, for_update=True)
    if session is None:
        raise RuntimeError("daily delivery session is unavailable")
    return session


def start_daily_stage_attempt(
    engine,
    *,
    scheduler_run_uid: object,
    stage_name: object,
    trade_date: object,
    release_id: object,
    strategy_release_id: object,
    lease_owner: object,
    lease_seconds: int = 90,
    shard_id: object = "main",
    input_root_sha256: object = None,
    reuse_completed_stage: bool = False,
    preserve_session_status: bool = False,
) -> dict[str, Any]:
    run_uid = str(scheduler_run_uid or "").strip().lower()
    stage = str(stage_name or "").strip()[:64]
    shard = str(shard_id or "main").strip()[:128] or "main"
    owner = str(lease_owner or "").strip()[:128]
    input_root = str(input_root_sha256 or "").strip().lower() or None
    if RUN_UID_RE.fullmatch(run_uid) is None or not stage or not owner:
        raise ValueError("daily stage attempt identity is invalid")
    if input_root is not None and SHA64_RE.fullmatch(input_root) is None:
        raise ValueError("daily stage input root is invalid")
    current = _control_now()
    with engine.begin() as connection:
        session = ensure_daily_delivery_session(
            connection,
            trade_date=trade_date,
            release_id=release_id,
            strategy_release_id=strategy_release_id,
            now=current,
        )
        existing = _one_mapping(
            connection.execute(
                text(
                    f"SELECT * FROM {ATTEMPT_TABLE} "
                    "WHERE scheduler_run_uid=:scheduler_run_uid"
                ),
                {"scheduler_run_uid": run_uid},
            )
        )
        if existing is not None:
            if (
                str(existing.get("session_uid") or "") != session["session_uid"]
                or str(existing.get("stage_name") or "") != stage
                or str(existing.get("lease_owner") or "") != owner
            ):
                raise RuntimeError("daily stage scheduler identity differs")
            if (
                reuse_completed_stage
                and completed_data_stage_status(stage, existing.get("status"))
            ):
                checkpoint = _validated_completed_stage_checkpoint(
                    existing,
                    session=session,
                    stage_name=stage,
                    engine=connection,
                )
                existing_input_root = str(
                    existing.get("input_root_sha256") or ""
                ).lower()
                if input_root is not None and input_root != existing_input_root:
                    raise DailyDeliveryFenceLost(
                        "completed daily stage input identity differs"
                    )
                replay_checkpoint = _completed_stage_replay_checkpoint(
                    checkpoint,
                    scheduler_run_uid=run_uid,
                    attempt_uid=str(existing.get("attempt_uid") or ""),
                    fencing_token=int(existing.get("fencing_token") or 0),
                    source_attempt=existing,
                    now=current,
                )
                return {
                    **existing,
                    "run_id": session["run_id"],
                    "trade_date": session["trade_date"],
                    "idempotent_replay": True,
                    "idempotent_replay_evidence": json.dumps(
                        replay_checkpoint,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "idempotent_source_attempt_uid": existing["attempt_uid"],
                    "idempotent_source_scheduler_run_uid": existing[
                        "scheduler_run_uid"
                    ],
                    "idempotent_source_fencing_token": int(
                        existing.get("fencing_token") or 0
                    ),
                }
            return existing
        if reuse_completed_stage:
            completed = _one_mapping(
                connection.execute(
                    text(f"""
                        SELECT * FROM {ATTEMPT_TABLE}
                        WHERE session_uid=:session_uid
                          AND stage_name=:stage_name
                          AND shard_id=:shard_id
                          AND (status='SUCCESS' OR
                               (stage_name='stock_finance' AND status='DEGRADED'))
                        ORDER BY fencing_token DESC LIMIT 1
                    """),
                    {
                        "session_uid": session["session_uid"],
                        "stage_name": stage,
                        "shard_id": shard,
                    },
                )
            )
            if completed is not None:
                checkpoint = _validated_completed_stage_checkpoint(
                    completed,
                    session=session,
                    stage_name=stage,
                    engine=connection,
                )
                completed_input_root = str(
                    completed.get("input_root_sha256") or ""
                ).lower()
                if input_root is not None and input_root != completed_input_root:
                    raise DailyDeliveryFenceLost(
                        "completed daily stage input identity differs"
                    )
                counters = _one_mapping(
                    connection.execute(
                        text(f"""
                            SELECT COALESCE(MAX(attempt_no), 0) AS attempt_no
                            FROM {ATTEMPT_TABLE}
                            WHERE session_uid=:session_uid
                              AND stage_name=:stage_name
                              AND shard_id=:shard_id
                        """),
                        {
                            "session_uid": session["session_uid"],
                            "stage_name": stage,
                            "shard_id": shard,
                        },
                    )
                ) or {"attempt_no": 0}
                attempt_no = int(counters.get("attempt_no") or 0) + 1
                fencing_token = int(
                    session.get("latest_fencing_token") or 0
                ) + 1
                attempt_uid = canonical_sha256(
                    {
                        "session_uid": session["session_uid"],
                        "scheduler_run_uid": run_uid,
                        "stage_name": stage,
                        "shard_id": shard,
                        "attempt_no": attempt_no,
                        "fencing_token": fencing_token,
                    }
                )
                replay_checkpoint = _completed_stage_replay_checkpoint(
                    checkpoint,
                    scheduler_run_uid=run_uid,
                    attempt_uid=attempt_uid,
                    fencing_token=fencing_token,
                    source_attempt=completed,
                    now=current,
                )
                advanced = connection.execute(
                    text(f"""
                        UPDATE {SESSION_TABLE}
                        SET latest_fencing_token=:fencing_token,
                            status=CASE
                                WHEN :preserve_session_status=1 THEN status
                                WHEN status IN ('PASS','DEGRADED') THEN status
                                ELSE 'RUNNING'
                            END,
                            updated_at=:now
                        WHERE session_uid=:session_uid
                          AND latest_fencing_token=:previous_fencing_token
                    """),
                    {
                        "session_uid": session["session_uid"],
                        "fencing_token": fencing_token,
                        "previous_fencing_token": fencing_token - 1,
                        "preserve_session_status": int(preserve_session_status),
                        "now": current,
                    },
                )
                if int(getattr(advanced, "rowcount", 0) or 0) != 1:
                    raise DailyDeliveryFenceLost(
                        "daily stage idempotent replay lost its session fence"
                    )
                connection.execute(
                    text(f"""
                        INSERT INTO {ATTEMPT_TABLE}
                            (attempt_uid, session_uid, scheduler_run_uid,
                             stage_name, shard_id, attempt_no, status,
                             input_root_sha256, output_dataset_id, lease_owner,
                             lease_until, fencing_token, checkpoint_json,
                             started_at, finished_at)
                        VALUES
                            (:attempt_uid, :session_uid, :scheduler_run_uid,
                             :stage_name, :shard_id, :attempt_no, :status,
                             :input_root_sha256, :output_dataset_id, :lease_owner,
                             :lease_until, :fencing_token, :checkpoint_json,
                             :started_at, :finished_at)
                    """),
                    {
                        "attempt_uid": attempt_uid,
                        "session_uid": session["session_uid"],
                        "scheduler_run_uid": run_uid,
                        "stage_name": stage,
                        "shard_id": shard,
                        "attempt_no": attempt_no,
                        "status": completed["status"],
                        "input_root_sha256": completed_input_root,
                        "output_dataset_id": completed.get("output_dataset_id"),
                        "lease_owner": owner,
                        "lease_until": current,
                        "fencing_token": fencing_token,
                        "checkpoint_json": json.dumps(
                            replay_checkpoint,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        "started_at": current,
                        "finished_at": current,
                    },
                )
                replay = _one_mapping(
                    connection.execute(
                        text(
                            f"SELECT * FROM {ATTEMPT_TABLE} "
                            "WHERE attempt_uid=:attempt_uid"
                        ),
                        {"attempt_uid": attempt_uid},
                    )
                )
                if replay is None:
                    raise RuntimeError(
                        "daily stage idempotent replay could not be created"
                    )
                return {
                    **replay,
                    "run_id": session["run_id"],
                    "trade_date": session["trade_date"],
                    "idempotent_replay": True,
                    "idempotent_replay_evidence": json.dumps(
                        replay_checkpoint,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    "idempotent_source_attempt_uid": completed["attempt_uid"],
                    "idempotent_source_scheduler_run_uid": completed[
                        "scheduler_run_uid"
                    ],
                    "idempotent_source_fencing_token": int(
                        completed.get("fencing_token") or 0
                    ),
                }
        active = _one_mapping(
            connection.execute(
                text(f"""
                    SELECT * FROM {ATTEMPT_TABLE}
                    WHERE session_uid=:session_uid AND stage_name=:stage_name
                      AND shard_id=:shard_id AND status='RUNNING'
                    ORDER BY fencing_token DESC LIMIT 1
                """),
                {
                    "session_uid": session["session_uid"],
                    "stage_name": stage,
                    "shard_id": shard,
                },
            )
        )
        if active is not None:
            active_lease_until = _datetime_value(active.get("lease_until"))
            if active_lease_until is not None and active_lease_until >= current:
                raise DailyDeliveryLeaseHeld(
                    "daily stage already has an unexpired lease: "
                    f"stage={stage} shard={shard} "
                    f"owner={active.get('lease_owner')} "
                    f"lease_until={active_lease_until.isoformat()}"
                )
            superseded = connection.execute(
                text(f"""
                    UPDATE {ATTEMPT_TABLE}
                    SET status='SUPERSEDED', finished_at=:finished_at,
                        error_code='LEASE_EXPIRED_TAKEOVER',
                        error_detail='expired lease superseded by a newer attempt'
                    WHERE attempt_uid=:attempt_uid AND status='RUNNING'
                      AND fencing_token=:fencing_token
                """),
                {
                    "attempt_uid": active["attempt_uid"],
                    "fencing_token": int(active.get("fencing_token") or 0),
                    "finished_at": current,
                },
            )
            if int(getattr(superseded, "rowcount", 0) or 0) != 1:
                raise DailyDeliveryFenceLost(
                    "expired daily stage lease changed during takeover"
                )
        counters = _one_mapping(
            connection.execute(
                text(f"""
                    SELECT COALESCE(MAX(attempt_no), 0) AS attempt_no
                    FROM {ATTEMPT_TABLE}
                    WHERE session_uid=:session_uid AND stage_name=:stage_name
                      AND shard_id=:shard_id
                """),
                {
                    "session_uid": session["session_uid"],
                    "stage_name": stage,
                    "shard_id": shard,
                },
            )
        ) or {"attempt_no": 0}
        attempt_no = int(counters.get("attempt_no") or 0) + 1
        fencing_token = int(session.get("latest_fencing_token") or 0) + 1
        attempt_uid = canonical_sha256(
            {
                "session_uid": session["session_uid"],
                "scheduler_run_uid": run_uid,
                "stage_name": stage,
                "shard_id": shard,
                "attempt_no": attempt_no,
                "fencing_token": fencing_token,
            }
        )
        connection.execute(
            text(f"""
                UPDATE {SESSION_TABLE}
                SET latest_fencing_token=:fencing_token,
                    status=CASE
                        WHEN :preserve_session_status=1 THEN status
                        WHEN status IN ('PASS','DEGRADED') THEN status
                        ELSE 'RUNNING'
                    END,
                    updated_at=:now
                WHERE session_uid=:session_uid
                  AND latest_fencing_token=:previous_fencing_token
            """),
            {
                "session_uid": session["session_uid"],
                "fencing_token": fencing_token,
                "previous_fencing_token": fencing_token - 1,
                "preserve_session_status": int(preserve_session_status),
                "now": current,
            },
        )
        connection.execute(
            text(f"""
                INSERT INTO {ATTEMPT_TABLE}
                    (attempt_uid, session_uid, scheduler_run_uid, stage_name,
                     shard_id, attempt_no, status, input_root_sha256,
                     lease_owner, lease_until, fencing_token, started_at)
                VALUES
                    (:attempt_uid, :session_uid, :scheduler_run_uid, :stage_name,
                     :shard_id, :attempt_no, 'RUNNING', :input_root_sha256,
                     :lease_owner, :lease_until, :fencing_token, :started_at)
            """),
            {
                "attempt_uid": attempt_uid,
                "session_uid": session["session_uid"],
                "scheduler_run_uid": run_uid,
                "stage_name": stage,
                "shard_id": shard,
                "attempt_no": attempt_no,
                "input_root_sha256": input_root,
                "lease_owner": owner,
                "lease_until": current + timedelta(seconds=max(30, int(lease_seconds))),
                "fencing_token": fencing_token,
                "started_at": current,
            },
        )
        attempt = _one_mapping(
            connection.execute(
                text(f"SELECT * FROM {ATTEMPT_TABLE} WHERE attempt_uid=:attempt_uid"),
                {"attempt_uid": attempt_uid},
            )
        )
        if attempt is None:
            raise RuntimeError("daily stage attempt could not be created")
        return {**attempt, "run_id": session["run_id"], "trade_date": session["trade_date"]}


def renew_daily_stage_lease(
    engine,
    *,
    attempt_uid: object,
    fencing_token: int,
    lease_owner: object,
    lease_seconds: int = 90,
) -> bool:
    current = _control_now()
    with engine.begin() as connection:
        attempt = _one_mapping(
            connection.execute(
                text(
                    f"SELECT * FROM {ATTEMPT_TABLE} "
                    "WHERE attempt_uid=:attempt_uid"
                ),
                {"attempt_uid": str(attempt_uid or "")},
            )
        )
        if attempt is None:
            return False
        # Claim/takeover locks the same session first.  Serializing renewal on
        # that row makes the newest-token check and lease update one decision.
        if _select_session(
            connection,
            str(attempt.get("session_uid") or ""),
            for_update=True,
        ) is None:
            return False
        newest = connection.execute(
            text(f"""
                SELECT MAX(fencing_token) FROM {ATTEMPT_TABLE}
                WHERE session_uid=:session_uid AND stage_name=:stage_name
                  AND shard_id=:shard_id
            """),
            {
                "session_uid": attempt["session_uid"],
                "stage_name": attempt["stage_name"],
                "shard_id": attempt["shard_id"],
            },
        ).scalar()
        if int(newest or 0) != int(fencing_token):
            return False
        result = connection.execute(
            text(f"""
                UPDATE {ATTEMPT_TABLE}
                SET lease_until=:lease_until
                WHERE attempt_uid=:attempt_uid AND fencing_token=:fencing_token
                  AND lease_owner=:lease_owner AND status='RUNNING'
            """),
            {
                "attempt_uid": str(attempt_uid or ""),
                "fencing_token": int(fencing_token),
                "lease_owner": str(lease_owner or ""),
                "lease_until": current + timedelta(seconds=max(30, int(lease_seconds))),
            },
        )
    return int(getattr(result, "rowcount", 0) or 0) == 1


def _attempt_terminal_status(status: object) -> str:
    normalized = str(status or "failed").strip().lower()
    return {
        "success": "SUCCESS",
        "degraded": "DEGRADED",
        "blocked": "BLOCKED",
        "timeout": "TIMEOUT",
        "stopped": "STOPPED",
    }.get(normalized, "FAILED")


def finish_daily_stage_attempt(
    connection,
    *,
    scheduler_run_uid: object,
    status: object,
    output_dataset_id: object = None,
    input_root_sha256: object = None,
    error_code: object = None,
    error_detail: object = None,
    checkpoint: Mapping[str, object] | None = None,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """Fence and terminalize an attempt inside its publisher transaction."""

    run_uid = str(scheduler_run_uid or "").strip().lower()
    attempt = _one_mapping(
        connection.execute(
            text(
                f"SELECT * FROM {ATTEMPT_TABLE} "
                "WHERE scheduler_run_uid=:scheduler_run_uid"
            ),
            {"scheduler_run_uid": run_uid},
        )
    )
    if attempt is None:
        return None
    # Use the same session-first lock order as claim and renewal to avoid a
    # claim/finish deadlock while making the fence decision atomic.
    session = _select_session(
        connection,
        str(attempt.get("session_uid") or ""),
        for_update=True,
    )
    if session is None:
        raise RuntimeError("daily delivery session is unavailable")
    suffix = " FOR UPDATE" if _dialect_name(connection) != "sqlite" else ""
    attempt = _one_mapping(
        connection.execute(
            text(
                f"SELECT * FROM {ATTEMPT_TABLE} "
                f"WHERE scheduler_run_uid=:scheduler_run_uid{suffix}"
            ),
            {"scheduler_run_uid": run_uid},
        )
    )
    if attempt is None:
        return None
    if str(attempt.get("status") or "") != "RUNNING":
        if str(attempt.get("status") or "") in ATTEMPT_TERMINAL_STATUSES:
            return attempt
        raise RuntimeError("daily stage attempt status is invalid")
    current = now or _control_now()
    newest = connection.execute(
        text(f"""
            SELECT MAX(fencing_token) FROM {ATTEMPT_TABLE}
            WHERE session_uid=:session_uid AND stage_name=:stage_name
              AND shard_id=:shard_id
        """),
        {
            "session_uid": attempt["session_uid"],
            "stage_name": attempt["stage_name"],
            "shard_id": attempt["shard_id"],
        },
    ).scalar()
    owns_fence = int(newest or 0) == int(attempt.get("fencing_token") or 0)
    lease_until = _datetime_value(attempt.get("lease_until"))
    successful = str(status or "").strip().lower() in {"success", "degraded"}
    terminal_status = _attempt_terminal_status(status)
    if not owns_fence:
        terminal_status = "SUPERSEDED"
    elif successful and (not isinstance(lease_until, datetime) or lease_until < current):
        raise DailyDeliveryFenceLost("daily stage lease expired before publication")
    checkpoint_json = (
        json.dumps(
            dict(checkpoint),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        if checkpoint
        else None
    )
    if successful and owns_fence and os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production":
        _validated_completed_stage_checkpoint(
            {**attempt, "status": terminal_status, "checkpoint_json": checkpoint_json,
             "input_root_sha256": input_root_sha256 or attempt.get("input_root_sha256")},
            session=session, stage_name=str(attempt["stage_name"]), engine=connection,
        )
    result = connection.execute(
        text(f"""
            UPDATE {ATTEMPT_TABLE}
            SET status=:status, output_dataset_id=:output_dataset_id,
                input_root_sha256=COALESCE(:input_root_sha256, input_root_sha256),
                checkpoint_json=:checkpoint_json, error_code=:error_code,
                error_detail=:error_detail, finished_at=:finished_at
            WHERE attempt_uid=:attempt_uid AND status='RUNNING'
              AND fencing_token=:fencing_token
        """),
        {
            "attempt_uid": attempt["attempt_uid"],
            "fencing_token": int(attempt["fencing_token"]),
            "status": terminal_status,
            "output_dataset_id": str(output_dataset_id or "")[:128] or None,
            "input_root_sha256": str(input_root_sha256 or "").lower() or None,
            "checkpoint_json": checkpoint_json,
            "error_code": str(error_code or "")[:128] or None,
            "error_detail": str(error_detail or "")[:1000] or None,
            "finished_at": current,
        },
    )
    if int(getattr(result, "rowcount", 0) or 0) != 1:
        raise DailyDeliveryFenceLost("daily stage attempt terminalization lost its fence")
    return {**attempt, "status": terminal_status, "finished_at": current}


def build_terminal_delivery_receipt(
    *,
    session: Mapping[str, object],
    scheduler_run_uid: object,
    stage_name: object,
    status: str,
    strategy_release_id: object,
    legacy_receipt: Mapping[str, object] | None = None,
    exact_core_inputs: Mapping[str, Mapping[str, object]] | None = None,
    degradations: list[Mapping[str, object]] | None = None,
    retryable: bool = False,
    error_code: object = None,
    error_detail: object = None,
) -> dict[str, object]:
    normalized_status = str(status or "").upper()
    if normalized_status not in TERMINAL_STATUSES:
        raise ValueError("daily delivery terminal status is invalid")
    strategy_id = str(strategy_release_id or "").strip().lower()
    if SHA64_RE.fullmatch(strategy_id) is None:
        raise ValueError("strategy release identity is invalid")
    if str(session.get("strategy_release_id") or "").lower() != strategy_id:
        raise RuntimeError("daily delivery strategy release identity differs")
    run_uid = str(scheduler_run_uid or "").strip().lower()
    if RUN_UID_RE.fullmatch(run_uid) is None:
        raise ValueError("scheduler run identity is invalid")
    stage = str(stage_name or "").strip()[:64]
    if not stage:
        raise ValueError("daily delivery blocking stage is invalid")
    legacy = dict(legacy_receipt or {})
    score_id = score_snapshot_identity(legacy) if normalized_status != "BLOCKED" else None
    core_inputs = _core_input_bindings(legacy)
    if exact_core_inputs is not None:
        expected_stages = {
            "kline": "qmt_stock_daily_canonical", "turnover": "target_turnover_snapshot",
            "financial": "stock_finance", "announcement": None,
        }
        if normalized_status == "BLOCKED" or set(exact_core_inputs) != set(expected_stages):
            raise ValueError("fixed revision core source bindings differ")
        core_inputs = {}
        for name, expected_stage in expected_stages.items():
            source = dict(exact_core_inputs[name])
            if (
                (source.get("stage") not in {"qmt_announcement_pit", "qmt_announcement_premarket_prepare"}
                 if name == "announcement" else source.get("stage") != expected_stage)
                or SHA64_RE.fullmatch(str(source.get("root") or "")) is None
                or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", str(source.get("dataset_id") or ""))
                or (source.get("input_root") is not None and
                    SHA64_RE.fullmatch(str(source["input_root"])) is None)
            ):
                raise ValueError(f"fixed revision {name} source binding differs")
            core_inputs[name] = source
    if normalized_status != "BLOCKED" and set(core_inputs) != {
        "kline",
        "turnover",
        "financial",
        "announcement",
    }:
        raise ValueError("daily delivery core input bindings are incomplete")
    normalized_degradations = [
        {
            str(key): value
            for key, value in sorted(dict(item).items())
        }
        for item in (degradations or [])
        if isinstance(item, Mapping)
    ]
    if normalized_status == "DEGRADED" and not normalized_degradations:
        raise ValueError("degraded daily delivery requires explicit degradations")
    if normalized_status == "PASS" and normalized_degradations:
        raise ValueError("pass daily delivery cannot contain degradations")
    core: dict[str, object] = {
        "schema": DELIVERY_RECEIPT_SCHEMA,
        "run_id": str(session.get("run_id") or ""),
        "session_uid": str(session.get("session_uid") or ""),
        "trade_date": _normalized_trade_date(session.get("trade_date")),
        "release_id": _normalized_release_id(session.get("release_id")),
        "strategy_release_id": strategy_id,
        "status": normalized_status,
        "delivery_mode": (
            str(legacy.get("status") or "").removeprefix("VERIFIED_")
            if legacy
            else "UNAVAILABLE"
        ),
        "scheduler_run_uid": run_uid,
        "terminal_stage": stage,
        "core_inputs": core_inputs,
        "feature_snapshot_id": (
            canonical_sha256(core_inputs) if exact_core_inputs is not None
            else str(legacy.get("base_data_receipt_root_sha256") or "") or None
        ),
        "score_snapshot_id": score_id,
        "strategy_pool": {
            "status": legacy.get("strategy_pool_status"),
            "count": legacy.get("governance_tradable_count"),
            "root": legacy.get("governance_result_sha256"),
        },
        "formal_pool": {
            "status": legacy.get("ticket_pool_status"),
            "count": legacy.get("recommendation_count"),
            "executable_count": legacy.get("executable_count"),
            "root": legacy.get("canonical_pool_sha256"),
        },
        "analysis_run_uid": legacy.get("analysis_run_uid"),
        "governance_run_uid": legacy.get("governance_run_uid"),
        "canonical_batch_status": legacy.get("governance_status"),
        "api_checks": {
            "strategy_pool": (
                "PASS" if legacy.get("strategy_pool_api_verified") is True else None
            ),
            "formal_pool": (
                "PASS" if legacy.get("ticket_pool_api_verified") is True else None
            ),
        },
        "retryable": bool(retryable),
        "blocking_stage": stage if normalized_status == "BLOCKED" else None,
        "error_code": str(error_code or "")[:128] or None,
        "error_detail": str(error_detail or "")[:1000] or None,
        "degradations": normalized_degradations,
        "legacy_delivery_receipt_sha256": (
            legacy.get("delivery_receipt_sha256") if legacy else None
        ),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    return {**core, "receipt_sha256": canonical_sha256(core)}


def validate_frozen_pool_decision(
    value: object, *, trade_date: object,
) -> dict[str, object]:
    if not isinstance(value, Mapping) or set(value) != {
        "schema", "phase", "execution_session_date", "cutoff_at", "evaluated_at",
        "parent_receipt_uid", "diff_sha256",
    } or value.get("schema") != "probiga.frozen-pool-decision.v1":
        raise ValueError("frozen pool decision is invalid")
    target = date.fromisoformat(_normalized_trade_date(trade_date))
    execution = date.fromisoformat(_normalized_trade_date(value["execution_session_date"]))
    cutoff = datetime.fromisoformat(str(value["cutoff_at"]))
    evaluated = datetime.fromisoformat(str(value["evaluated_at"]))
    if (
        execution <= target
        or cutoff.utcoffset() is None or evaluated.utcoffset() is None
        or cutoff.astimezone(PRODUCTION_TIMEZONE).isoformat(timespec="seconds") != value["cutoff_at"]
        or evaluated.astimezone(PRODUCTION_TIMEZONE).isoformat(timespec="seconds") != value["evaluated_at"]
        or evaluated < cutoff
    ):
        raise ValueError("frozen pool decision time identity differs")
    phase = value["phase"]
    if phase == "INITIAL":
        expected = datetime.combine(target, STRATEGY_POOL_INITIAL_TIME, PRODUCTION_TIMEZONE)
        if value["parent_receipt_uid"] is not None or value["diff_sha256"] is not None:
            raise ValueError("initial frozen pool cannot supersede a receipt")
    elif phase == "PREMARKET_REVISION":
        expected = datetime.combine(execution, STRATEGY_POOL_PREMARKET_REVISION_TIME, PRODUCTION_TIMEZONE)
        if any(
            SHA64_RE.fullmatch(str(value[field] or "")) is None
            for field in ("parent_receipt_uid", "diff_sha256")
        ):
            raise ValueError("premarket frozen pool requires the prior receipt and stock changes")
    else:
        raise ValueError("frozen pool decision phase is invalid")
    if cutoff < expected or (phase == "PREMARKET_REVISION" and cutoff != expected):
        raise ValueError("frozen pool decision cutoff differs from its scheduled slot")
    return dict(value)


def bind_frozen_pool_decision(
    receipt: Mapping[str, object],
    *,
    slot: StrategyPoolDecisionSlot,
    score_snapshot: Mapping[str, object],
    parent_receipt_uid: str = "",
    diff_sha256: str = "",
) -> dict[str, object]:
    """Bind a verified pool and its exact input cutoff to one publication slot."""
    core = dict(receipt)
    seal = str(core.pop("receipt_sha256", "") or "")
    if canonical_sha256(core) != seal or core.get("status") not in {"PASS", "DEGRADED"}:
        raise ValueError("frozen pool decision requires a verified delivery receipt")
    if "frozen_pool_decision" in core:
        raise ValueError("frozen pool decision is already bound")
    input_cutoff = slot.input_cutoff_at or slot.scheduled_at
    if (
        slot.scheduled_at.utcoffset() is None or slot.evaluated_at.utcoffset() is None
        or input_cutoff.utcoffset() is None or input_cutoff.microsecond
        or not slot.scheduled_at <= input_cutoff <= slot.evaluated_at
        or core.get("trade_date") != slot.data_trade_date
        or score_snapshot.get("trade_date") != slot.data_trade_date
        or SHA64_RE.fullmatch(str(core.get("score_snapshot_id") or "")) is None
        or core.get("score_snapshot_id") != score_snapshot.get("payload_sha256")
    ):
        raise ValueError("frozen pool decision does not match the score snapshot")
    cutoff_at = input_cutoff.astimezone(PRODUCTION_TIMEZONE)
    actual = slot.evaluated_at.astimezone(PRODUCTION_TIMEZONE)
    cutoff = cutoff_at.replace(tzinfo=None).isoformat(timespec="seconds")
    if any(score_snapshot.get(key) != cutoff for key in ("decision_at", "input_decision_at")):
        raise ValueError("frozen pool score input cutoff differs from its slot")
    core["frozen_pool_decision"] = validate_frozen_pool_decision({
        "schema": "probiga.frozen-pool-decision.v1",
        "phase": slot.phase,
        "execution_session_date": slot.execution_session_date,
        "cutoff_at": cutoff_at.isoformat(timespec="seconds"),
        "evaluated_at": actual.isoformat(timespec="seconds"),
        "parent_receipt_uid": parent_receipt_uid or None,
        "diff_sha256": diff_sha256 or None,
    }, trade_date=slot.data_trade_date)
    return {**core, "receipt_sha256": canonical_sha256(core)}


def _read_sealed_canonical_delivery(
    connection, session: Mapping[str, object],
) -> dict[str, object] | None:
    receipt_uid = str(session.get("canonical_receipt_uid") or "")
    if not receipt_uid:
        return None
    row = _one_mapping(connection.execute(
        text(f"SELECT receipt_json, receipt_sha256 FROM {RECEIPT_TABLE} "
             "WHERE receipt_uid=:receipt_uid"),
        {"receipt_uid": receipt_uid},
    ))
    try:
        receipt = json.loads(str((row or {}).get("receipt_json") or "{}"))
        supplied = str(receipt.pop("receipt_sha256", "")).lower()
    except (AttributeError, TypeError, ValueError) as exc:
        raise RuntimeError("materialized daily delivery receipt seal differs") from exc
    if (
        SHA64_RE.fullmatch(supplied) is None
        or supplied != str((row or {}).get("receipt_sha256") or "").lower()
        or canonical_sha256(receipt) != supplied
        or str(receipt.get("receipt_uid") or "") != receipt_uid
        or any(
            str(receipt.get(key) or "").lower() != str(session.get(key) or "").lower()
            for key in ("session_uid", "run_id", "release_id", "strategy_release_id")
        )
        or str(receipt.get("trade_date") or "")[:10] != str(session.get("trade_date") or "")[:10]
        or (session.get("status") in TERMINAL_STATUSES and receipt.get("status") != session.get("status"))
    ):
        raise RuntimeError("materialized daily delivery receipt seal differs")
    return {**receipt, "receipt_sha256": supplied}


def load_frozen_initial_delivery(connection, session_uid: str) -> dict[str, object] | None:
    """Read the initial receipt under the session lock used by revision CAS."""
    session = load_daily_delivery_session(connection, session_uid)
    if session.get("status") not in {"PASS", "DEGRADED"}:
        return None
    receipt = _read_sealed_canonical_delivery(connection, session)
    if not receipt or receipt.get("frozen_pool_decision") is None:
        return None
    phase = validate_frozen_pool_decision(receipt["frozen_pool_decision"], trade_date=receipt["trade_date"])
    return receipt if phase["phase"] == "INITIAL" else None


def load_frozen_initial_announcement_parent(engine, trade_date, release_id):
    """Resolve the actual signed source batch used by the current frozen first pool."""
    from server.common.qmt_announcement_pit import QMT_ANNOUNCEMENT_TASK_SCHEMA, validate_task_result

    identity = daily_session_identity(trade_date, release_id)
    target, session_uid = identity["trade_date"], identity["session_uid"]
    with engine.connect() as connection:
        session = _select_session(connection, session_uid, for_update=False)
        if session is None or session.get("status") not in {"PASS", "DEGRADED"}:
            return None
        initial = _read_sealed_canonical_delivery(connection, session)
        if not initial or initial.get("frozen_pool_decision") is None:
            return None
        phase = validate_frozen_pool_decision(initial["frozen_pool_decision"], trade_date=target)
        if phase["phase"] != "INITIAL":
            return None
        announcement = (initial.get("core_inputs") or {}).get("announcement") or {}
        stage, source_run = announcement.get("stage"), announcement.get("dataset_id")
        if stage != "qmt_announcement_pit" or RUN_UID_RE.fullmatch(str(source_run or "")) is None:
            raise DailyDeliveryControlError("initial frozen announcement source identity differs")
        attempt = _one_mapping(connection.execute(text(f"""
            SELECT * FROM {ATTEMPT_TABLE}
            WHERE session_uid=:session_uid AND scheduler_run_uid=:source_run
              AND stage_name=:stage AND status='SUCCESS'
        """), {"session_uid": session_uid, "source_run": source_run, "stage": stage}))
        if attempt is None:
            raise DailyDeliveryControlError("initial frozen announcement attempt is unavailable")
        cutoff = datetime.fromisoformat(str(phase["cutoff_at"])).replace(tzinfo=None)
        finished = _datetime_value(attempt["finished_at"])
        if finished is not None and finished.utcoffset() is not None:
            finished = finished.astimezone(CONTROL_TIMEZONE).replace(tzinfo=None)
        if finished is None or finished > cutoff:
            raise DailyDeliveryControlError("initial frozen announcement completion exceeds its input cutoff")
        evidence = _validated_completed_stage_checkpoint(
            attempt, session=session, stage_name=stage, engine=connection,
        )
    if evidence.get("evidence_sha256") != announcement.get("root"):
        raise DailyDeliveryControlError("initial frozen announcement evidence root differs")
    matches = []
    for line in str(evidence.get("replay_output") or "").splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if isinstance(item, dict) and item.get("schema") == QMT_ANNOUNCEMENT_TASK_SCHEMA:
            matches.append(item)
    if (
        len(matches) != 1 or validate_task_result(matches[0], 0) != "complete"
        or matches[0].get("window_end") != target
    ):
        raise DailyDeliveryControlError("initial frozen announcement result differs")
    result = matches[0]
    return {key: result[key] for key in (
        "batch_id", "batch_root_hash", "source", "fallback_reason", "received_at",
        "window_start", "window_end", "fact_cutoff_at",
    )}


@dataclass(frozen=True)
class FrozenPremarketAnnouncement:
    receipt: Mapping[str, object]


def load_frozen_premarket_announcement(
    engine, trade_date: str, release_id: str, slot: StrategyPoolDecisionSlot,
) -> FrozenPremarketAnnouncement | None:
    """Read the signed next-session source receipt that had completed by the slot."""
    from server.common.qmt_announcement_pit import QMT_ANNOUNCEMENT_TASK_SCHEMA
    from tools.qmt_announcement_task_contract import (
        PREMARKET_ANNOUNCEMENT_SOURCE_TIME, PREMARKET_ANNOUNCEMENT_TASK_TYPE,
    )
    from tools.sync_qmt_announcement_pit import validate_premarket_source_task_result

    identity = daily_session_identity(trade_date, release_id)
    if slot.phase != "PREMARKET_REVISION" or slot.data_trade_date != identity["trade_date"]:
        raise ValueError("premarket announcement requires its exact fixed decision slot")
    decision = slot.scheduled_at.astimezone(PRODUCTION_TIMEZONE)
    source = datetime.combine(
        date.fromisoformat(slot.execution_session_date),
        time.fromisoformat(PREMARKET_ANNOUNCEMENT_SOURCE_TIME),
        PRODUCTION_TIMEZONE,
    )
    parent = load_frozen_initial_announcement_parent(engine, trade_date, release_id)
    if not parent:
        return None
    stage = PREMARKET_ANNOUNCEMENT_TASK_TYPE
    with engine.connect() as connection:
        attempts = connection.execute(text(f"""
            SELECT a.*, s.trade_date, s.release_id
            FROM {ATTEMPT_TABLE} a JOIN {SESSION_TABLE} s ON s.session_uid=a.session_uid
            WHERE a.session_uid=:session_uid AND a.stage_name=:stage AND a.status='SUCCESS'
              AND a.started_at>=:source AND a.finished_at<=:decision
            ORDER BY a.finished_at DESC, a.id DESC LIMIT 2
        """), {
            "session_uid": identity["session_uid"], "stage": stage,
            "source": source.replace(tzinfo=None), "decision": decision.replace(tzinfo=None),
        }).mappings().all()
        if not attempts:
            return None
        if len(attempts) != 1:
            raise DailyDeliveryControlError("premarket announcement completion is ambiguous")
        attempt = attempts[0]
        evidence = _validated_completed_stage_checkpoint(
            attempt, session=attempt, stage_name=stage, engine=connection,
        )
    results = []
    for line in str(evidence["replay_output"]).splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if isinstance(item, dict) and item.get("schema") == QMT_ANNOUNCEMENT_TASK_SCHEMA:
            results.append(item)
    started, finished = _datetime_value(attempt["started_at"]), _datetime_value(attempt["finished_at"])
    if len(results) != 1 or started is None or finished is None:
        raise DailyDeliveryControlError("premarket announcement result is unavailable")
    if validate_premarket_source_task_result(
        results[0], 0,
        expected_trade_date=identity["trade_date"],
        expected_source_cutoff=source.isoformat(timespec="seconds"),
        expected_decision_cutoff=decision.isoformat(timespec="seconds"),
        expected_scheduler_run_uid=str(attempt["scheduler_run_uid"]),
        expected_build_sha=str(evidence["build_sha"]),
        started_at=started, finished_at=finished, expected_parent=parent,
    ) != "complete":
        raise DailyDeliveryControlError("premarket announcement result differs")
    return FrozenPremarketAnnouncement(MappingProxyType({
        **results[0],
        "stage_finished_at": finished.replace(tzinfo=PRODUCTION_TIMEZONE).isoformat(),
        "stage_evidence_sha256": evidence["evidence_sha256"],
        "stage_attempt_uid": attempt["attempt_uid"],
    }))


def load_frozen_premarket_external_context(
    engine, trade_date: str, release_id: str, slot: StrategyPoolDecisionSlot,
) -> dict[str, object] | None:
    """Return the stored external market snapshot certified before this slot."""
    from biz.market_context.external_market import (
        PREMARKET_EXTERNAL_SNAPSHOT_SCHEMA, PREMARKET_EXTERNAL_TASK_TYPE,
        load_verified_premarket_external_market_context,
    )

    identity = daily_session_identity(trade_date, release_id)
    if slot.phase != "PREMARKET_REVISION" or slot.data_trade_date != identity["trade_date"]:
        raise ValueError("external market context requires its exact fixed decision slot")
    cutoff = slot.scheduled_at.astimezone(PRODUCTION_TIMEZONE)
    start = datetime.combine(cutoff.date(), time(8, 15))
    with (nullcontext(engine) if hasattr(engine, "execute") else engine.connect()) as connection:
        attempts = connection.execute(text(f"""
            SELECT a.*, s.trade_date, s.release_id FROM {ATTEMPT_TABLE} a
            JOIN {SESSION_TABLE} s ON s.session_uid=a.session_uid
            WHERE a.session_uid=:uid AND a.stage_name=:stage AND a.status='SUCCESS'
              AND a.started_at>=:start AND a.finished_at<=:cutoff
            ORDER BY a.finished_at DESC, a.id DESC LIMIT 2
        """), {
            "uid": identity["session_uid"], "stage": PREMARKET_EXTERNAL_TASK_TYPE,
            "start": start, "cutoff": cutoff.replace(tzinfo=None),
        }).mappings().all()
        if len(attempts) != 1:
            return None
        attempt = attempts[0]
        evidence = _validated_completed_stage_checkpoint(
            attempt, session=attempt, stage_name=PREMARKET_EXTERNAL_TASK_TYPE, engine=connection,
        )
        matches = []
        for line in str(evidence["replay_output"]).splitlines():
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if isinstance(item, dict) and item.get("schema") == PREMARKET_EXTERNAL_SNAPSHOT_SCHEMA:
                matches.append(item)
        if len(matches) != 1:
            raise DailyDeliveryControlError("external market signed receipt differs")
    return load_verified_premarket_external_market_context(
        getattr(engine, "engine", engine), receipt=matches[0], cutoff=cutoff,
        finished_at=_datetime_value(attempt["finished_at"]),
    )


def persist_terminal_delivery_receipt(
    connection,
    *,
    receipt: Mapping[str, object],
    now: datetime | None = None,
) -> dict[str, object]:
    payload = dict(receipt)
    supplied_hash = str(payload.pop("receipt_sha256", "")).lower()
    if SHA64_RE.fullmatch(supplied_hash) is None or canonical_sha256(payload) != supplied_hash:
        raise ValueError("daily delivery receipt hash is invalid")
    status = str(payload.get("status") or "").upper()
    if status not in TERMINAL_STATUSES:
        raise ValueError("daily delivery receipt status is invalid")
    session_uid = str(payload.get("session_uid") or "")
    session = _select_session(connection, session_uid, for_update=True)
    if session is None:
        raise RuntimeError("daily delivery receipt session is unavailable")
    scheduler_run_uid = str(payload.get("scheduler_run_uid") or "").lower()
    if (
        str(payload.get("run_id") or "") != str(session.get("run_id") or "")
        or str(payload.get("trade_date") or "")[:10]
        != str(session.get("trade_date") or "")[:10]
        or str(payload.get("release_id") or "").lower()
        != str(session.get("release_id") or "").lower()
        or str(payload.get("strategy_release_id") or "").lower()
        != str(session.get("strategy_release_id") or "").lower()
    ):
        raise RuntimeError("daily delivery receipt session identity differs")
    attempt = _one_mapping(
        connection.execute(
            text(
                f"SELECT session_uid, status FROM {ATTEMPT_TABLE} "
                "WHERE scheduler_run_uid=:scheduler_run_uid"
            ),
            {"scheduler_run_uid": scheduler_run_uid},
        )
    )
    attempt_status = str((attempt or {}).get("status") or "")
    expected_attempt_statuses = (
        {"SUCCESS", "DEGRADED"}
        if status in {"PASS", "DEGRADED"}
        else {"BLOCKED", "FAILED", "TIMEOUT", "STOPPED"}
    )
    if (
        attempt is None
        or str(attempt.get("session_uid") or "") != session_uid
        or attempt_status not in expected_attempt_statuses
    ):
        raise RuntimeError("daily delivery receipt stage attempt differs")
    existing = _one_mapping(
        connection.execute(
            text(
                f"SELECT receipt_json, receipt_sha256 FROM {RECEIPT_TABLE} "
                "WHERE scheduler_run_uid=:scheduler_run_uid"
            ),
            {"scheduler_run_uid": scheduler_run_uid},
        )
    )
    if existing is not None:
        stored = json.loads(str(existing.get("receipt_json") or "{}"))
        stored_final_hash = str(stored.pop("receipt_sha256", "")).lower()
        if (
            str(existing.get("receipt_sha256") or "").lower()
            != stored_final_hash
            or SHA64_RE.fullmatch(stored_final_hash) is None
            or canonical_sha256(stored) != stored_final_hash
        ):
            raise RuntimeError("stored daily delivery receipt seal differs")
        source = {
            key: value
            for key, value in stored.items()
            if key not in {"generation", "receipt_uid"}
        }
        if canonical_sha256(source) != supplied_hash:
            raise RuntimeError("scheduler run already has a different delivery receipt")
        return {**stored, "receipt_sha256": stored_final_hash}
    frozen_decision = payload.get("frozen_pool_decision")
    promote_revision = False
    if frozen_decision is not None:
        if status not in {"PASS", "DEGRADED"}:
            raise ValueError("frozen pool decision requires a verified result")
        frozen_decision = validate_frozen_pool_decision(
            frozen_decision, trade_date=payload["trade_date"],
        )
        phase = frozen_decision["phase"]
        current_receipt_uid = str(session.get("canonical_receipt_uid") or "")
        if phase == "INITIAL":
            if current_receipt_uid and session.get("status") in {"PASS", "DEGRADED"}:
                raise RuntimeError("initial frozen pool already has a canonical receipt")
        elif phase == "PREMARKET_REVISION":
            if not current_receipt_uid or current_receipt_uid != frozen_decision.get("parent_receipt_uid"):
                raise RuntimeError("frozen pool revision parent changed before publication")
            prior_receipt = _read_sealed_canonical_delivery(connection, session)
            if prior_receipt is None or prior_receipt.get("status") not in {"PASS", "DEGRADED"}:
                raise RuntimeError("initial frozen pool is unavailable")
            prior_decision = validate_frozen_pool_decision(
                prior_receipt.get("frozen_pool_decision"), trade_date=payload["trade_date"],
            )
            if (
                not isinstance(prior_decision, Mapping)
                or prior_decision.get("phase") != "INITIAL"
                or prior_decision.get("execution_session_date") != frozen_decision.get("execution_session_date")
                or str(prior_decision.get("cutoff_at") or "") >= str(frozen_decision.get("cutoff_at") or "")
            ):
                raise RuntimeError("premarket frozen pool already published or the initial receipt differs")
            promote_revision = True
        else:
            raise ValueError("frozen pool decision phase is invalid")
    generation = int(session.get("latest_generation") or 0) + 1
    receipt_uid = canonical_sha256(
        {
            "session_uid": session_uid,
            "generation": generation,
            "scheduler_run_uid": scheduler_run_uid,
            "receipt_sha256": supplied_hash,
        }
    )
    complete = {**payload, "generation": generation, "receipt_uid": receipt_uid}
    final_hash = canonical_sha256(complete)
    complete["receipt_sha256"] = final_hash
    encoded = json.dumps(
        complete,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    current = now or _control_now()
    formal_pool = complete.get("formal_pool")
    formal = dict(formal_pool) if isinstance(formal_pool, Mapping) else {}
    connection.execute(
        text(f"""
            INSERT INTO {RECEIPT_TABLE}
                (receipt_uid, session_uid, generation, status,
                 scheduler_run_uid, stage_name, release_id,
                 strategy_release_id, analysis_run_uid, governance_run_uid,
                 score_snapshot_id, canonical_pool_sha256,
                 recommendation_count, retryable, blocking_stage, error_code,
                 error_detail, receipt_json, receipt_sha256, created_at)
            VALUES
                (:receipt_uid, :session_uid, :generation, :status,
                 :scheduler_run_uid, :stage_name, :release_id,
                 :strategy_release_id, :analysis_run_uid, :governance_run_uid,
                 :score_snapshot_id, :canonical_pool_sha256,
                 :recommendation_count, :retryable, :blocking_stage, :error_code,
                 :error_detail, :receipt_json, :receipt_sha256, :created_at)
        """),
        {
            "receipt_uid": receipt_uid,
            "session_uid": session_uid,
            "generation": generation,
            "status": status,
            "scheduler_run_uid": scheduler_run_uid,
            "stage_name": str(complete.get("terminal_stage") or "")[:64],
            "release_id": str(complete.get("release_id") or "").lower(),
            "strategy_release_id": str(
                complete.get("strategy_release_id") or ""
            ).lower(),
            "analysis_run_uid": str(complete.get("analysis_run_uid") or "") or None,
            "governance_run_uid": str(complete.get("governance_run_uid") or "") or None,
            "score_snapshot_id": str(complete.get("score_snapshot_id") or "") or None,
            "canonical_pool_sha256": str(formal.get("root") or "") or None,
            "recommendation_count": formal.get("count"),
            "retryable": 1 if complete.get("retryable") is True else 0,
            "blocking_stage": str(complete.get("blocking_stage") or "")[:64] or None,
            "error_code": str(complete.get("error_code") or "")[:128] or None,
            "error_detail": str(complete.get("error_detail") or "")[:1000] or None,
            "receipt_json": encoded,
            "receipt_sha256": final_hash,
            "created_at": current,
        },
    )
    updated = connection.execute(
        text(f"""
            UPDATE {SESSION_TABLE}
            SET status=CASE
                    WHEN :promote_revision=1 THEN :status
                    WHEN status='PASS' AND :status<>'PASS' THEN status
                    WHEN status='DEGRADED' AND :status='BLOCKED'
                    THEN status
                    ELSE :status
                END,
                latest_generation=:generation,
                canonical_receipt_uid=CASE
                    WHEN :promote_revision=1 THEN :receipt_uid
                    WHEN (status='PASS' AND :status<>'PASS')
                      OR (status='DEGRADED' AND :status='BLOCKED')
                    THEN canonical_receipt_uid ELSE :receipt_uid
                END,
                terminal_at=CASE
                    WHEN :promote_revision=1 THEN :terminal_at
                    WHEN (status='PASS' AND :status<>'PASS')
                      OR (status='DEGRADED' AND :status='BLOCKED')
                    THEN terminal_at ELSE :terminal_at
                END,
                updated_at=:terminal_at
            WHERE session_uid=:session_uid
              AND latest_generation=:previous_generation
        """),
        {
            "status": status,
            "generation": generation,
            "previous_generation": generation - 1,
            "promote_revision": int(promote_revision),
            "receipt_uid": receipt_uid,
            "terminal_at": current,
            "session_uid": session_uid,
        },
    )
    if int(getattr(updated, "rowcount", 0) or 0) != 1:
        raise RuntimeError("daily delivery session generation changed")
    return complete


def read_daily_delivery(
    engine,
    *,
    trade_date: object = "",
    release_id: object = "",
    attempt_limit: int = 20,
) -> dict[str, object] | None:
    """Read one materialized session, canonical receipt and recent attempts."""

    clauses: list[str] = []
    params: dict[str, object] = {"attempt_limit": max(1, min(100, int(attempt_limit)))}
    if str(trade_date or "").strip():
        params["trade_date"] = _normalized_trade_date(trade_date)
        clauses.append("trade_date=:trade_date")
    if str(release_id or "").strip():
        params["release_id"] = _normalized_release_id(release_id)
        clauses.append("release_id=:release_id")
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with engine.connect() as connection:
        session = _one_mapping(
            connection.execute(
                text(
                    f"SELECT * FROM {SESSION_TABLE}{where} "
                    "ORDER BY trade_date DESC, updated_at DESC, id DESC LIMIT 1"
                ),
                params,
            )
        )
        if session is None:
            return None
        receipt = _read_sealed_canonical_delivery(connection, session)
        attempts = [
            dict(row)
            for row in connection.execute(
                text(f"""
                    SELECT attempt_uid, scheduler_run_uid, stage_name, shard_id,
                           attempt_no, status, input_root_sha256,
                           output_dataset_id, lease_owner, lease_until,
                           fencing_token, error_code, error_detail,
                           started_at, finished_at
                    FROM {ATTEMPT_TABLE}
                    WHERE session_uid=:session_uid
                    ORDER BY fencing_token DESC LIMIT :attempt_limit
                """),
                {
                    "session_uid": session["session_uid"],
                    "attempt_limit": params["attempt_limit"],
                },
            ).mappings().all()
        ]
    return {"session": session, "receipt": receipt, "attempts": attempts}


__all__ = [
    "ATTEMPT_TABLE",
    "DELIVERY_RECEIPT_SCHEMA",
    "DailyDeliveryFenceLost",
    "DailyDeliveryLeaseHeld",
    "RECEIPT_TABLE",
    "SESSION_TABLE",
    "build_terminal_delivery_receipt",
    "canonical_sha256",
    "daily_session_identity",
    "ensure_daily_delivery_session",
    "finish_daily_stage_attempt",
    "load_daily_delivery_session",
    "persist_terminal_delivery_receipt",
    "privileged_migrate_daily_delivery_schema",
    "read_daily_delivery",
    "renew_daily_stage_lease",
    "score_snapshot_identity",
    "start_daily_stage_attempt",
    "strategy_release_identity",
    "validate_daily_delivery_runtime_schema",
]
