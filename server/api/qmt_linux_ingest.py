"""Linux-owned planning and transactional commit for Windows QMT results."""
from __future__ import annotations

from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from acquisition.config import Config
from acquisition.datasets import get_spec
from acquisition.models import WorkUnit
from acquisition.plan import eligible_codes, plan_units, sessions, summarize
from acquisition.qmt_model import MAX_CODES, parse_instant, validate_request
from acquisition.qmt_transport import validate_result
from acquisition.runner import Runner, units_from_request
from server.common.component_release import runtime_component_build_sha
from server.common.qmt_linux_ingest_protocol import (
    COMMIT_SCHEMA,
    PLAN_SCHEMA,
    canonical_sha256,
    validate_sha256,
)


ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = ROOT / "acquisition" / "qmt_model.py"
CONFIG_TEMPLATE = ROOT / "acquisition" / "config.example.json"
SUPPORTED_DATASETS = frozenset({
    "stock_daily",
    "stock_minute",
    "index_daily",
    "index_minute",
})


class QmtLinuxIngestError(ValueError):
    """The submitted edge plan/result cannot be committed authoritatively."""


def expected_edge_identity() -> dict[str, str]:
    return {
        "edge_build_sha": runtime_component_build_sha("windows"),
        "model_sha256": hashlib.sha256(MODEL_PATH.read_bytes()).hexdigest(),
    }


def _validate_edge_identity(payload: Mapping[str, Any]) -> dict[str, str]:
    expected = expected_edge_identity()
    supplied_build = str(payload.get("edge_build_sha") or "").strip().lower()
    supplied_model = validate_sha256(payload.get("model_sha256"), field="model_sha256")
    if supplied_build != expected["edge_build_sha"] or supplied_model != expected["model_sha256"]:
        raise QmtLinuxIngestError("QMT edge release identity differs")
    return expected


def _configuration() -> Config:
    data = json.loads(CONFIG_TEMPLATE.read_text(encoding="utf-8"))
    data.update({
        "write_enabled": True,
        "state_dir": "/var/lib/probiga/qmt-linux-ingest",
        "start_date": "2026-09-01",
        "datasets": sorted(SUPPORTED_DATASETS),
    })
    return Config(data=data, path=CONFIG_TEMPLATE)


def _dataset(value: object):
    name = str(value or "").strip()
    if name not in SUPPORTED_DATASETS:
        raise QmtLinuxIngestError("unsupported QMT Linux ingestion dataset")
    return get_spec(name)


def _date_range(start_value: object, end_value: object) -> tuple[str, str]:
    try:
        start = date.fromisoformat(str(start_value or ""))
        end = date.fromisoformat(str(end_value or ""))
    except ValueError as exc:
        raise QmtLinuxIngestError("QMT ingestion date range differs") from exc
    if start > end or end > datetime.now().date() or (end - start).days > 62:
        raise QmtLinuxIngestError("QMT ingestion date range differs")
    return start.isoformat(), end.isoformat()


def build_plan(payload: Mapping[str, Any]) -> dict[str, Any]:
    if set(payload) != {
        "schema", "edge_build_sha", "model_sha256", "dataset", "start_date", "end_date",
    } or payload.get("schema") != PLAN_SCHEMA:
        raise QmtLinuxIngestError("QMT ingestion plan contract differs")
    identity = _validate_edge_identity(payload)
    spec = _dataset(payload.get("dataset"))
    start, end = _date_range(payload.get("start_date"), payload.get("end_date"))
    runner = Runner(_configuration())
    try:
        runner.config.require_writes()
        calendar = runner.store("primary").calendar(start, end)
        target_sessions = sessions(calendar, start, end)
        catalog = runner.catalog(spec)
        now = runner.clock()
        cooldowns = [item for item in runner.store(spec.database).retrying_sources(now)
                     if item["source"] == spec.source]
        batches: list[dict[str, Any]] = []
        coverage: list[dict[str, Any]] = []
        size = 20 if spec.period == "1m" else MAX_CODES
        for target in reversed(target_sessions):
            if runner._target(spec, target) != target:
                raise QmtLinuxIngestError("QMT ingestion target is not closed")
            states = runner.store(spec.database).states(spec.name, target)
            coverage.append(summarize(spec, target, catalog, states))
            units = [] if cooldowns else plan_units(spec, target, catalog, states, now=now)
            # Normal backfill fills gaps, not previously observed errors. Keep
            # those original outcomes in coverage and defer their resolution;
            # a due retry timestamp is not permission to recapture this lane.
            deferred_errors = {
                (str(state["target_date"])[:10], state["partition_key"])
                for state in states
                if state["status"] == "error"
                and (not state.get("source") or state["source"] == spec.source)
            }
            units = [unit for unit in units
                     if (unit.target_date, unit.partition_key) not in deferred_errors]
            for adjustment in spec.adjustments:
                selected = [unit for unit in units if unit.adjustment == adjustment]
                for offset in range(0, len(selected), size):
                    part = selected[offset:offset + size]
                    batches.append({
                        "dataset": spec.name,
                        "source": spec.source,
                        "target_date": target,
                        "period": spec.period,
                        "adjustment": adjustment,
                        "codes": [unit.code for unit in part],
                    })
        core = {
            "status": "ready",
            **identity,
            "dataset": spec.name,
            "start_date": start,
            "end_date": end,
            "session_count": len(target_sessions),
            "batch_count": len(batches),
            "coverage": list(reversed(coverage)),
            "source_cooldown": bool(cooldowns),
            "source_retry_at": max(str(item["next_retry_at"]) for item in cooldowns)
                               if cooldowns else None,
            "batches": batches,
        }
        core["plan_sha256"] = canonical_sha256(core)
        return core
    finally:
        runner.close()


def _validate_commit_payload(payload: Mapping[str, Any]) -> tuple[dict[str, Any], Any]:
    if set(payload) != {
        "schema", "edge_build_sha", "model_sha256", "result",
    } or payload.get("schema") != COMMIT_SCHEMA:
        raise QmtLinuxIngestError("QMT ingestion commit contract differs")
    _validate_edge_identity(payload)
    raw = payload.get("result")
    if not isinstance(raw, dict):
        raise QmtLinuxIngestError("QMT ingestion result is missing")
    validate_result(raw)
    request = raw["request"]
    validate_request(request)
    spec = _dataset(request.get("dataset"))
    if request.get("period") != spec.period or request.get("adjustment") not in spec.adjustments:
        raise QmtLinuxIngestError("QMT ingestion product identity differs")
    received = parse_instant(raw.get("received_at"))
    requested = parse_instant(request.get("requested_at"))
    if (
        received < requested
        or received > datetime.now(received.tzinfo) + timedelta(seconds=5)
    ):
        raise QmtLinuxIngestError("QMT ingestion receipt time differs")
    return raw, spec


def commit_result(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw, spec = _validate_commit_payload(payload)
    request = raw["request"]
    runner = Runner(_configuration())
    units = units_from_request(request)
    store = None
    try:
        runner.config.require_writes()
        target = str(request["start_date"])
        if runner._target(spec, target) != target:
            raise QmtLinuxIngestError("QMT ingestion target is not closed")
        catalog = runner.catalog(spec)
        allowed = set(eligible_codes(spec, catalog, target))
        if not units or any(unit.code not in allowed for unit in units):
            raise QmtLinuxIngestError("QMT ingestion security scope differs")
        store = runner.store(spec.database)
        store.validate_spec(spec)
        store.begin_request(units, request["request_id"], runner.clock())
        # Business rows and outcomes commit together. An infrastructure
        # exception leaves the prepared request running so the retained raw
        # result can retry; it must not become a successful error replay.
        counts = runner._consume(raw)
        return {
            "status": "committed",
            "request_id": request["request_id"],
            "dataset": spec.name,
            "target_date": target,
            "result_sha256": canonical_sha256(raw),
            "counts": counts,
        }
    finally:
        runner.close()
