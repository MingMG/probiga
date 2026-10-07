"""One issued-input and verified-result product for QMT simulation research.

This product does not write strategy governance, paper accounts or orders.
Windows reports are accepted only after exact replay of a server-issued input.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import re
import uuid
from pathlib import Path
from threading import BoundedSemaphore, Event, Thread
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import and_, bindparam, func, inspect, or_, select, text
from sqlalchemy.exc import IntegrityError

from server.api.routers._engine import get_engine
from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.component_release import runtime_component_build_sha
from server.common.kline_data import get_kline_engine
from server.common.qmt_linux_ingest_protocol import canonical_json, canonical_sha256
from server.common.qmt_strategy_result_schema import (
    INPUTS, JOBS, RESULTS, validate_qmt_strategy_result_schema,
)
from server.common.qmt_strategy_bridge_proof import validate_qmt_strategy_bridge_identity


INPUT_REQUEST_SCHEMA = "probiga.qmt-strategy-input-request.v1"
RESULT_COMMIT_SCHEMA = "probiga.qmt-strategy-result-commit.v1"
INPUT_SCHEMA = "probiga.qmt-strategy-simulation-input.v1"
RESULT_SCHEMA = "probiga.qmt-strategy-simulation-result.v1"
TASK_TYPE = "qmt_strategy_simulation_daily"
TIMEZONE = ZoneInfo("Asia/Shanghai")
_SHA40 = re.compile(r"[0-9a-f]{40}\Z")
_SHA64 = re.compile(r"[0-9a-f]{64}\Z")
_UID = re.compile(r"[0-9a-f]{32}\Z")
_CODE = re.compile(r"[0-9]{6}\Z")
_SAFE = {"simulation_only": True, "real_order_allowed": False,
         "automatic_real_order_submission": False, "real_order_authority": False}
_MAX_SNAPSHOT_BYTES = 32 * 1024 * 1024
_LOGGER = logging.getLogger(__name__)
_ROOT = Path(__file__).resolve().parents[2]
_PREPARE_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="qmt-input-prepare")
_PREPARE_SLOTS = BoundedSemaphore(2)
_LEASE_SECONDS = 120
_HEARTBEAT_SECONDS = 20


class QmtStrategyResultError(ValueError):
    """A result request violates the final simulation result contract."""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _instant(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise QmtStrategyResultError("QMT simulation timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise QmtStrategyResultError("QMT simulation timestamp needs a timezone")
    return parsed.astimezone(timezone.utc)


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value or "")


def _day(value: Any) -> str:
    raw = str(value or "")
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise QmtStrategyResultError("QMT simulation trade date is invalid") from exc
    if parsed.isoformat() != raw:
        raise QmtStrategyResultError("QMT simulation trade date is invalid")
    return raw


def _build(value: Any) -> str:
    expected = runtime_component_build_sha("windows")
    if not _SHA40.fullmatch(str(expected)) or value != expected:
        raise QmtStrategyResultError("QMT simulation Windows release differs")
    return expected


def _decode(raw: str, digest: str, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
        if not isinstance(value, dict) or canonical_sha256(value) != digest:
            raise ValueError("hash differs")
        return value
    except (TypeError, ValueError) as exc:
        raise QmtStrategyResultError(f"stored QMT simulation {label} integrity differs") from exc


def _read_input(row: Mapping[str, Any]) -> dict[str, Any]:
    from server.engine.qmt_strategy_simulation import snapshot_input_hash

    snapshot = _decode(row["snapshot_json"], row["snapshot_sha256"], "input")
    contract = snapshot.get("formula_contract") or {}
    if (snapshot.get("schema") != INPUT_SCHEMA or snapshot.get("trade_date") != row["trade_date"]
            or snapshot.get("mode") != row["run_mode"]
            or snapshot.get("input_hash") != row["input_hash"]
            or snapshot_input_hash(snapshot) != row["input_hash"]
            or contract.get("executor_sha256") != row["executor_sha256"]
            or row["simulation_only"] != 1 or row["real_order_allowed"] != 0):
        raise QmtStrategyResultError("stored QMT simulation issued identity differs")
    return snapshot


def _counts(result: Mapping[str, Any]) -> dict[str, Any]:
    strategies, combinations = result.get("strategy_rows"), result.get("combination_rows")
    if (result.get("schema") != RESULT_SCHEMA or result.get("simulation_only") is not True
            or result.get("real_order_allowed") is not False
            or not isinstance(strategies, list) or len(strategies) != 10
            or not isinstance(combinations, list) or len(combinations) != 4):
        raise QmtStrategyResultError("QMT simulation result scope differs")
    selected = 0
    blocked = 0
    seen: set[str] = set()
    for row in strategies + combinations:
        if not isinstance(row, dict):
            raise QmtStrategyResultError("QMT simulation entity is invalid")
        key = row.get("strategy_key")
        picks = row.get("selected")
        status = row.get("status")
        if (not isinstance(key, str) or key in seen
                or key in {"intraday_surprise", "weak_market_structural_mainline"}
                or status not in {"DATA_BLOCKED", "COMPLETED", "COMPLETED_EMPTY"}
                or not isinstance(picks, list)
                or type(row.get("selected_count")) is not int
                or row["selected_count"] != len(picks)
                or type(row.get("candidate_count")) is not int
                or row["candidate_count"] < len(picks)):
            raise QmtStrategyResultError("QMT simulation entity state differs")
        seen.add(key)
        codes = [item.get("stock_code") for item in picks if isinstance(item, dict)]
        if (len(codes) != len(picks) or len(set(codes)) != len(codes)
                or any(not _CODE.fullmatch(str(code)) for code in codes)):
            raise QmtStrategyResultError("QMT simulation selected securities differ")
        if status == "DATA_BLOCKED":
            if picks or not row.get("blocked_reasons"):
                raise QmtStrategyResultError("blocked QMT simulation must retain its reasons")
            blocked += 1
        elif status == "COMPLETED_EMPTY" and picks:
            raise QmtStrategyResultError("empty QMT simulation has selected securities")
        elif status == "COMPLETED" and not picks:
            raise QmtStrategyResultError("completed QMT simulation needs selected securities")
        selected += len(picks)
    return {"strategy_count": 10, "combination_count": 4,
            "selected_count": selected, "blocked_count": blocked,
            "status": "DATA_BLOCKED" if blocked == 14 else "PARTIAL" if blocked else
                      "COMPLETED" if selected else "COMPLETED_EMPTY"}


def _execution(value: Any, edge_build_sha: str, issued: datetime, now: datetime) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"origin", "started_at", "finished_at", "bridge_identity"}:
        raise QmtStrategyResultError("QMT simulation execution fields differ")
    if value["origin"] not in {"QMT_ENTRY", "WINDOWS_DAILY"}:
        raise QmtStrategyResultError("QMT simulation execution origin differs")
    started, finished = _instant(value["started_at"]), _instant(value["finished_at"])
    if (finished < started or finished < issued or finished > now + timedelta(minutes=5)
            or started < issued - timedelta(minutes=30)):
        raise QmtStrategyResultError("QMT simulation execution time differs")
    bridge = value["bridge_identity"]
    fields = {"source", "model_instance_id", "strategy_build_sha", "strategy_git_blob",
              "strategy_source_sha256", "strategy_artifact_sha256", "strategy_loaded_identity_sha256",
              "direct_acquisition_model_sha256", "strategy_identity_frozen", "strategy_identity_status", "updated_at"}
    if (not isinstance(bridge, dict) or set(bridge) != fields
            or bridge["source"] != "gj_big_qmt_inner"
            or bridge["strategy_identity_frozen"] is not True
            or bridge["strategy_identity_status"] != "BOUND"
            or not _UID.fullmatch(str(bridge["model_instance_id"]))
            or not _SHA40.fullmatch(str(bridge["strategy_git_blob"]))
            or any(not _SHA64.fullmatch(str(bridge[key])) for key in
                   ("strategy_source_sha256", "strategy_artifact_sha256", "strategy_loaded_identity_sha256", "direct_acquisition_model_sha256"))):
        raise QmtStrategyResultError("QMT simulation bridge release identity differs")
    try:
        validate_qmt_strategy_bridge_identity(bridge, expected_app_build_sha=edge_build_sha, root=_ROOT)
    except ValueError as exc:
        raise QmtStrategyResultError("QMT simulation bridge release identity differs") from exc
    heartbeat = _instant(bridge["updated_at"])
    if heartbeat < started - timedelta(seconds=90) or heartbeat > finished + timedelta(seconds=5):
        raise QmtStrategyResultError("QMT simulation bridge observation is stale")
    return value


def prepare_strategy_inputs(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Create or poll one durable request; never prepare inside the HTTP call."""
    if (set(payload) != {"schema", "request_id", "edge_build_sha", "trade_date"}
            or payload.get("schema") != INPUT_REQUEST_SCHEMA
            or not _UID.fullmatch(str(payload.get("request_id")))
            or not isinstance(payload.get("trade_date"), str)):
        raise QmtStrategyResultError("QMT simulation input request fields differ")
    build = _build(payload.get("edge_build_sha"))
    engine = get_engine()
    validate_qmt_strategy_result_schema(engine)
    request_hash = canonical_sha256(dict(payload))
    with engine.connect() as connection:
        job = connection.execute(select(JOBS).where(JOBS.c.request_id == payload["request_id"])).mappings().one_or_none()
    if job is not None:
        _read_job(job)
        if job["request_hash"] != request_hash:
            raise QmtStrategyResultError("QMT simulation request identity is immutable")
        _start_preparation(engine, job)
        return _job_response(engine, payload["request_id"])
    current = _now()
    closed = _day(authoritative_closed_trade_date(engine, now=current.astimezone(TIMEZONE)))
    requested = str(payload.get("trade_date") or "")
    target = _day(requested) if requested else closed
    if target > closed:
        raise QmtStrategyResultError("QMT simulation session has not closed")
    mode = "REPLAY" if target < closed or (requested and target < current.astimezone(TIMEZONE).date().isoformat()) else "DAILY"
    values = {"request_id": payload["request_id"], "request_hash": request_hash,
              "request_json": canonical_json(dict(payload)).decode("utf-8"),
              "trade_date": target, "run_mode": mode, "edge_build_sha": build,
              "status": "QUEUED", "snapshot_id": None, "snapshot_existing": None, "lease_token": None,
              "lease_expires_at": None, "heartbeat_at": None, "attempt_count": 0,
              "error_code": None, "created_at": current.replace(tzinfo=None),
              "updated_at": current.replace(tzinfo=None)}
    values["binding_hash"] = _job_binding_hash(values)
    try:
        with engine.begin() as connection:
            connection.execute(JOBS.insert().values(**values))
    except IntegrityError:
        with engine.connect() as connection:
            values = connection.execute(select(JOBS).where(JOBS.c.request_id == payload["request_id"])).mappings().one()
        _read_job(values)
        if values["request_hash"] != request_hash:
            raise QmtStrategyResultError("QMT simulation request identity is immutable") from None
    _start_preparation(engine, values)
    return _job_response(engine, payload["request_id"])


def _job_binding_hash(job: Mapping[str, Any]) -> str:
    return canonical_sha256({key: job[key] for key in ("request_id", "request_hash", "trade_date", "run_mode", "edge_build_sha")})


def _read_job(job: Mapping[str, Any]) -> dict[str, Any]:
    request = _decode(job["request_json"], job["request_hash"], "preparation request")
    if (request.get("request_id") != job["request_id"]
            or request.get("schema") != INPUT_REQUEST_SCHEMA
            or request.get("edge_build_sha") != job["edge_build_sha"]
            or (request.get("trade_date") and request["trade_date"] != job["trade_date"])
            or job["binding_hash"] != _job_binding_hash(job)
            or job["run_mode"] not in {"DAILY", "REPLAY"}):
        raise QmtStrategyResultError("stored QMT preparation binding differs")
    return request


def _job_response(engine: Any, request_id: str) -> dict[str, Any]:
    with engine.connect() as connection:
        job = connection.execute(select(JOBS).where(JOBS.c.request_id == request_id)).mappings().one()
        issued = connection.execute(select(INPUTS).where(INPUTS.c.snapshot_id == job["snapshot_id"])).mappings().one_or_none() if job["snapshot_id"] else None
    _read_job(job)
    common = {"request_id": request_id, "trade_date": job["trade_date"], "run_mode": job["run_mode"],
              "edge_build_sha": job["edge_build_sha"], "request_hash": job["request_hash"],
              "created_at": _iso(job["created_at"]), "updated_at": _iso(job["updated_at"]),
              "attempt_count": job["attempt_count"], **_SAFE}
    if job["status"] == "ISSUED":
        if (issued is None or issued["trade_date"] != job["trade_date"] or issued["run_mode"] != job["run_mode"]
                or issued["edge_build_sha"] != job["edge_build_sha"]):
            raise QmtStrategyResultError("stored QMT preparation snapshot binding differs")
        return {**_issued_response(dict(issued), _read_input(issued), engine=engine, existing=bool(job["snapshot_existing"])), **common}
    return {**common, "status": job["status"], "error_code": job["error_code"],
            "reason": _preparation_reason(job),
            "poll_after_seconds": 5 if job["status"] in {"QUEUED", "PREPARING"} else None}


def _preparation_reason(job: Mapping[str, Any]) -> str:
    reasons = {"CLOSED_SESSION_ROLLOVER": "固定交易日已跨越下一收盘会话；须新建请求，不会改用新日期",
               "INPUT_PREPARATION_FAILED": "策略事实输入准备失败；本请求失败记录保留，可新建请求重试",
               "WINDOWS_RELEASE_CHANGED": "准备请求所属Windows版本已变化；须使用当前版本新建请求"}
    return reasons.get(job["error_code"], "策略事实输入已发出" if job["status"] == "ISSUED" else "策略事实输入排队或准备中")


def _preparation_summary(job: Mapping[str, Any]) -> dict[str, Any]:
    _read_job(job)
    return {**{key: job[key] for key in ("request_id", "trade_date", "run_mode", "edge_build_sha", "status", "snapshot_id", "attempt_count", "error_code")},
            "created_at": _iso(job["created_at"]), "updated_at": _iso(job["updated_at"]),
            "heartbeat_at": _iso(job["heartbeat_at"]), "reason": _preparation_reason(job),
            "lease_stale": bool(job["status"] == "PREPARING" and job["lease_expires_at"] <= _now().replace(tzinfo=None)), **_SAFE}


def _start_preparation(engine: Any, job: Mapping[str, Any]) -> None:
    """A database CAS plus local capacity prevents an unbounded web queue."""
    if job["status"] not in {"QUEUED", "PREPARING"} or not _PREPARE_SLOTS.acquire(blocking=False):
        return
    token, current = uuid.uuid4().hex, _now().replace(tzinfo=None)
    try:
        with engine.begin() as connection:
            claimed = connection.execute(JOBS.update().where(
                JOBS.c.request_id == job["request_id"],
                or_(JOBS.c.status == "QUEUED", and_(JOBS.c.status == "PREPARING", JOBS.c.lease_expires_at <= current)),
            ).values(status="PREPARING", lease_token=token,
                     lease_expires_at=current + timedelta(seconds=_LEASE_SECONDS), heartbeat_at=current,
                     updated_at=current, attempt_count=JOBS.c.attempt_count + 1, error_code=None)).rowcount
        if not claimed:
            _PREPARE_SLOTS.release()
            return
        _PREPARE_EXECUTOR.submit(_run_preparation, engine, job["request_id"], token)
    except Exception:
        _PREPARE_SLOTS.release()
        raise


def _lease_predicate(request_id: str, token: str, current: datetime):
    return and_(JOBS.c.request_id == request_id, JOBS.c.status == "PREPARING",
                JOBS.c.lease_token == token, JOBS.c.lease_expires_at > current)


def _renew_preparation_lease(engine: Any, request_id: str, token: str) -> bool:
    current = _now().replace(tzinfo=None)
    with engine.begin() as connection:
        return bool(connection.execute(JOBS.update().where(_lease_predicate(request_id, token, current))
                    .values(heartbeat_at=current, updated_at=current,
                            lease_expires_at=current + timedelta(seconds=_LEASE_SECONDS))).rowcount)


def _fail_preparation(engine: Any, request_id: str, token: str, code: str) -> None:
    current = _now().replace(tzinfo=None)
    with engine.begin() as connection:
        connection.execute(JOBS.update().where(_lease_predicate(request_id, token, current))
                           .values(status="FAILED", error_code=code, updated_at=current,
                                   lease_token=None, lease_expires_at=None))


def _run_preparation(engine: Any, request_id: str, token: str) -> None:
    stopped, lease_lost = Event(), Event()
    monitor = None
    def heartbeat():
        while not stopped.wait(_HEARTBEAT_SECONDS):
            try:
                if not _renew_preparation_lease(engine, request_id, token):
                    lease_lost.set()
                    return
            except Exception as exc:
                _LOGGER.warning("QMT input heartbeat unavailable: exception_type=%s", type(exc).__name__)
                lease_lost.set()
                return
    try:
        from server.engine.qmt_strategy_simulation import prepare_inputs

        monitor = Thread(target=heartbeat, name="qmt-input-lease", daemon=True)
        monitor.start()
        with engine.connect() as connection:
            job = connection.execute(select(JOBS).where(JOBS.c.request_id == request_id)).mappings().one()
        _read_job(job)
        if job["lease_token"] != token or job["status"] != "PREPARING":
            return
        if runtime_component_build_sha("windows") != job["edge_build_sha"]:
            _fail_preparation(engine, request_id, token, "WINDOWS_RELEASE_CHANGED")
            return
        if job["run_mode"] == "DAILY" and authoritative_closed_trade_date(engine, now=_now().astimezone(TIMEZONE)) != job["trade_date"]:
            _fail_preparation(engine, request_id, token, "CLOSED_SESSION_ROLLOVER")
            return
        snapshot = prepare_inputs(engine, get_kline_engine(), job["trade_date"], run_mode=job["run_mode"])
        if not lease_lost.is_set():
            _publish_preparation(engine, job, token, snapshot)
    except Exception as exc:
        _LOGGER.warning("QMT input preparation failed: exception_type=%s", type(exc).__name__)
        try:
            _fail_preparation(engine, request_id, token, "INPUT_PREPARATION_FAILED")
        except Exception as failed:
            _LOGGER.warning("QMT input failure record unavailable: exception_type=%s", type(failed).__name__)
    finally:
        stopped.set()
        if monitor is not None and monitor.ident is not None:
            monitor.join(timeout=1)
        _PREPARE_SLOTS.release()


def _publish_preparation(engine: Any, job: Mapping[str, Any], token: str, snapshot: dict[str, Any]) -> None:
    from server.engine.qmt_strategy_simulation import snapshot_input_hash

    target, mode = job["trade_date"], job["run_mode"]
    if (snapshot.get("schema") != INPUT_SCHEMA or snapshot.get("trade_date") != target
            or snapshot.get("mode") != mode
            or snapshot.get("input_hash") != snapshot_input_hash(snapshot)):
        raise QmtStrategyResultError("prepared QMT simulation input identity differs")
    executor_sha = (snapshot.get("formula_contract") or {}).get("executor_sha256")
    if not _SHA64.fullmatch(str(executor_sha)):
        raise QmtStrategyResultError("prepared QMT simulation formula identity is missing")
    raw = canonical_json(snapshot)
    if len(raw) > _MAX_SNAPSHOT_BYTES:
        raise QmtStrategyResultError("QMT simulation input snapshot exceeds the result budget")
    snapshot_hash = canonical_sha256(snapshot)
    current = _now()
    values = {"snapshot_id": uuid.uuid4().hex, "trade_date": target, "run_mode": mode,
              "edge_build_sha": job["edge_build_sha"], "executor_sha256": executor_sha,
              "input_hash": snapshot["input_hash"], "snapshot_sha256": snapshot_hash,
              "snapshot_json": raw.decode("utf-8"), "issued_at": current.replace(tzinfo=None),
              "simulation_only": 1, "real_order_allowed": 0}
    with engine.begin() as connection:
        # This CAS is both the publication fence and an actual DML transaction
        # start before the nested uniqueness savepoint (including on SQLite).
        # The row stays locked until the input and ISSUED link commit together.
        locked = connection.execute(JOBS.update().where(_lease_predicate(job["request_id"], token, current.replace(tzinfo=None)))
                                    .values(updated_at=current.replace(tzinfo=None))).rowcount
        if not locked:
            return
        active = connection.execute(select(JOBS).where(_lease_predicate(job["request_id"], token, current.replace(tzinfo=None))).with_for_update()).mappings().one_or_none()
        if active is None:
            return
        _read_job(active)
        if mode == "DAILY" and authoritative_closed_trade_date(engine, now=current.astimezone(TIMEZONE)) != target:
            connection.execute(JOBS.update().where(JOBS.c.request_id == job["request_id"]).values(
                status="FAILED", error_code="CLOSED_SESSION_ROLLOVER", updated_at=current.replace(tzinfo=None), lease_token=None, lease_expires_at=None))
            return
        existing = connection.execute(select(INPUTS).where(INPUTS.c.edge_build_sha == job["edge_build_sha"], INPUTS.c.snapshot_sha256 == snapshot_hash)).mappings().one_or_none()
        snapshot_existing = existing is not None
        if existing is None:
            try:
                with connection.begin_nested():
                    connection.execute(INPUTS.insert().values(**values))
            except IntegrityError:
                # Another job may have just published the exact same snapshot.
                # A locking read sees that committed identity in MySQL too.
                existing = connection.execute(select(INPUTS).where(
                    INPUTS.c.edge_build_sha == job["edge_build_sha"], INPUTS.c.snapshot_sha256 == snapshot_hash,
                ).with_for_update()).mappings().one_or_none()
                if existing is None:
                    raise
                snapshot_existing = True
                _read_input(existing)
                values = dict(existing)
        else:
            _read_input(existing)
            values = dict(existing)
        connection.execute(JOBS.update().where(JOBS.c.request_id == job["request_id"], JOBS.c.lease_token == token).values(
            status="ISSUED", snapshot_id=values["snapshot_id"], snapshot_existing=int(snapshot_existing), lease_token=None, lease_expires_at=None,
            updated_at=current.replace(tzinfo=None), error_code=None))


def _issued_response(values: dict[str, Any], snapshot: dict[str, Any], *, engine: Any, existing: bool) -> dict[str, Any]:
    with engine.connect() as connection:
        saved = connection.execute(select(RESULTS).where(RESULTS.c.snapshot_id == values["snapshot_id"])).mappings().one_or_none()
    receipt = None
    if saved is not None:
        detail = read_strategy_run(saved["run_uid"], engine=engine)
        receipt = {"status": "COMMITTED", "idempotent": True, "run_uid": saved["run_uid"],
                   "snapshot_id": values["snapshot_id"], "trade_date": values["trade_date"],
                   "input_hash": values["input_hash"], "result_hash": saved["result_hash"],
                   "execution_hash": saved["execution_hash"], "execution_status": saved["status"],
                   "run_mode": values["run_mode"], "edge_build_sha": values["edge_build_sha"],
                   "execution": detail["execution"],
                   "strategy_count": saved["strategy_count"], "combination_count": saved["combination_count"],
                   "selected_count": saved["selected_count"], "blocked_count": saved["blocked_count"], **_SAFE}
    return {"status": "ISSUED", "snapshot_id": values["snapshot_id"],
            "edge_build_sha": values["edge_build_sha"], "trade_date": values["trade_date"], "run_mode": values["run_mode"],
            "issued_at": _iso(values["issued_at"]), "snapshot_sha256": values["snapshot_sha256"],
            "snapshot": snapshot, "existing": existing, "committed_receipt": receipt, **_SAFE}


def commit_strategy_result(payload: Mapping[str, Any]) -> dict[str, Any]:
    from server.engine.qmt_strategy_simulation import evaluate_snapshot

    if (set(payload) != {"schema", "edge_build_sha", "snapshot_id", "result", "execution"}
            or payload.get("schema") != RESULT_COMMIT_SCHEMA
            or not _UID.fullmatch(str(payload.get("snapshot_id")))):
        raise QmtStrategyResultError("QMT simulation commit fields differ")
    build = _build(payload.get("edge_build_sha"))
    engine = get_engine()
    validate_qmt_strategy_result_schema(engine)
    with engine.connect() as connection:
        issued = connection.execute(select(INPUTS).where(INPUTS.c.snapshot_id == payload["snapshot_id"])).mappings().one_or_none()
        existing = connection.execute(select(RESULTS).where(RESULTS.c.snapshot_id == payload["snapshot_id"])).mappings().one_or_none()
    if issued is None or issued["edge_build_sha"] != build:
        raise QmtStrategyResultError("QMT simulation result has no matching issued input")
    snapshot = _read_input(issued)
    result = payload["result"]
    counts = _counts(result) if isinstance(result, dict) else None
    if counts is None or result.get("trade_date") != issued["trade_date"] or result.get("input_hash") != issued["input_hash"]:
        raise QmtStrategyResultError("QMT simulation result input binding differs")
    result_hash, execution_hash = canonical_sha256(result), canonical_sha256(payload["execution"])
    if existing is not None:
        _decode(existing["result_json"], existing["result_hash"], "result")
        _decode(existing["execution_json"], existing["execution_hash"], "execution")
        if existing["result_hash"] != result_hash or existing["execution_hash"] != execution_hash:
            raise QmtStrategyResultError("QMT simulation committed result is immutable")
        return {**counts, "execution_status": counts["status"], "status": "COMMITTED", "idempotent": True, "run_uid": existing["run_uid"],
                "snapshot_id": issued["snapshot_id"], "trade_date": issued["trade_date"],
                "result_hash": result_hash, "execution_hash": execution_hash, **_SAFE}
    now = _now()
    execution = _execution(payload["execution"], build, issued["issued_at"].replace(tzinfo=timezone.utc), now)
    if issued["run_mode"] == "DAILY" and authoritative_closed_trade_date(engine, now=now.astimezone(TIMEZONE)) != issued["trade_date"]:
        raise QmtStrategyResultError("QMT simulation daily input has expired across the closed-session rollover")
    # Replaying the exact immutable input rejects forged picks, extra strategies,
    # changed thresholds and caller-supplied explanations in one comparison.
    if canonical_sha256(evaluate_snapshot(snapshot)) != result_hash:
        raise QmtStrategyResultError("QMT simulation result differs from the issued formula replay")
    values = {"run_uid": issued["snapshot_id"], "snapshot_id": issued["snapshot_id"],
              "trade_date": issued["trade_date"], "run_mode": issued["run_mode"],
              "origin": execution["origin"], "input_hash": issued["input_hash"],
              "result_hash": result_hash, "execution_hash": execution_hash,
              "result_json": canonical_json(result).decode("utf-8"),
              "execution_json": canonical_json(execution).decode("utf-8"),
              "completed_at": _instant(execution["finished_at"]).replace(tzinfo=None),
              "received_at": now.replace(tzinfo=None), "simulation_only": 1,
              "real_order_allowed": 0, **counts}
    try:
        with engine.begin() as connection:
            connection.execute(RESULTS.insert().values(**values))
    except IntegrityError:
        # A concurrent retry may only recover the exact already committed fact.
        with engine.connect() as connection:
            raced = connection.execute(select(RESULTS).where(RESULTS.c.snapshot_id == issued["snapshot_id"])).mappings().one_or_none()
        if raced is None or raced["result_hash"] != result_hash or raced["execution_hash"] != execution_hash:
            raise QmtStrategyResultError("QMT simulation result identity collision") from None
    return {**counts, "execution_status": counts["status"], "status": "COMMITTED", "idempotent": False, "run_uid": values["run_uid"],
            "snapshot_id": issued["snapshot_id"], "trade_date": issued["trade_date"],
            "result_hash": result_hash, "execution_hash": execution_hash, **_SAFE}


def _summary(row: Mapping[str, Any]) -> dict[str, Any]:
    fields = ("snapshot_id", "trade_date", "run_mode", "edge_build_sha", "input_hash", "snapshot_sha256", "issued_at",
              "run_uid", "origin", "status", "selected_count", "blocked_count", "result_hash", "completed_at", "received_at")
    value = {field: _iso(row.get(field)) if field.endswith("_at") else row.get(field) for field in fields}
    if not value.get("run_uid"):
        value.update({"run_uid": row["snapshot_id"], "status": "AWAITING_EXECUTION", "origin": None})
    return {**value, "execution_label": "QMT数据上的同版本策略公式模拟", **_SAFE}


def _schedule(engine: Any) -> dict[str, Any]:
    schedule = {"owner": "WINDOWS_QMT", "cron_time": "22:50", "timezone": "Asia/Shanghai",
                "execution_mode": "SIMULATION", "status": "NOT_REGISTERED", "enabled": None,
                "last_run_status": None, "last_run_at": None, "last_run_duration": None,
                "last_run_summary": "尚无每日策略模拟调度记录"}
    if not inspect(engine).has_table("st_scheduled_tasks"):
        return schedule
    columns = {column["name"] for column in inspect(engine).get_columns("st_scheduled_tasks")}
    if not {"task_name", "task_type", "cron_time", "enabled", "script_path", "script_args",
            "interval_minutes", "last_run_at", "last_run_status", "last_run_duration"}.issubset(columns):
        return {**schedule, "status": "CONTRACT_MISMATCH"}
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT task_name, cron_time, enabled, script_path, script_args, interval_minutes, last_run_at, last_run_status, last_run_duration FROM st_scheduled_tasks WHERE task_type=:task_type"), {"task_type": TASK_TYPE}).mappings().all()
    if not rows:
        return schedule
    if len(rows) != 1:
        return {**schedule, "status": "CONTRACT_MISMATCH"}
    row = rows[0]
    if row["script_path"] != "tools/run_qmt_strategy_daily.py" or row["script_args"] != "--json" or row["cron_time"] != "22:50" or row["interval_minutes"] != 0:
        return {**schedule, "status": "CONTRACT_MISMATCH"}
    summaries = {"running": "每日策略模拟正在执行", "success": "最近一次执行完成；各策略的缺数及选股状态见结果",
                 "failed": "最近一次执行失败，尚未产生完整结果", "error": "最近一次执行异常，尚未产生完整结果",
                 "blocked": "最近一次执行被数据或运行条件阻断", "": "尚无每日策略模拟执行记录"}
    last_status = str(row["last_run_status"] or "").lower()
    return {**schedule, "status": "REGISTERED", "enabled": bool(row["enabled"]), "task_name": row["task_name"],
            "last_run_status": last_status if last_status in summaries else "unknown",
            "last_run_at": str(row["last_run_at"]) if row["last_run_at"] is not None else None,
            "last_run_duration": max(0, int(row["last_run_duration"] or 0)),
            "last_run_summary": summaries.get(last_status, "最近一次执行状态暂不可确认")}


def _input_overview(snapshot: dict[str, Any]) -> dict[str, Any]:
    v2, v3 = snapshot.get("v2") or {}, snapshot.get("v3") or {}
    return {"prepared_at": snapshot.get("prepared_at"), "decision_at": snapshot.get("decision_at"),
            "market_clock": snapshot.get("market_clock"), "input_hash": snapshot.get("input_hash"),
            "formula_contract": snapshot.get("formula_contract"),
            "v2": {key: v2.get(key) for key in ("status", "reasons", "flow_date", "proofs")},
            "v3": {key: v3.get(key) for key in ("status", "reasons", "feature_time", "source", "data_snapshot_hash", "proofs")}}


def read_strategy_run(run_uid: str, *, engine: Any = None) -> dict[str, Any]:
    if not _UID.fullmatch(run_uid):
        raise QmtStrategyResultError("QMT simulation run identity is invalid")
    engine = get_engine() if engine is None else engine
    validate_qmt_strategy_result_schema(engine)
    with engine.connect() as connection:
        issued = connection.execute(select(INPUTS).where(INPUTS.c.snapshot_id == run_uid)).mappings().one_or_none()
        saved = connection.execute(select(RESULTS).where(RESULTS.c.run_uid == run_uid)).mappings().one_or_none()
    if issued is None:
        raise KeyError("QMT simulation run not found")
    snapshot = _read_input(issued)
    row = dict(issued)
    result = None
    execution = None
    if saved is not None:
        row.update(dict(saved))
        result = _decode(saved["result_json"], saved["result_hash"], "result")
        execution = _decode(saved["execution_json"], saved["execution_hash"], "execution")
        if (saved["snapshot_id"] != issued["snapshot_id"] or saved["trade_date"] != issued["trade_date"]
                or saved["input_hash"] != issued["input_hash"] or saved["run_mode"] != issued["run_mode"]
                or result.get("input_hash") != issued["input_hash"] or result.get("trade_date") != issued["trade_date"]
                or saved["simulation_only"] != 1 or saved["real_order_allowed"] != 0
                or any(saved[key] != value for key, value in _counts(result).items())):
            raise QmtStrategyResultError("stored QMT simulation result binding differs")
    return {**_summary(row), "input": {**_input_overview(snapshot), "snapshot_sha256": issued["snapshot_sha256"]},
            "result": result, "execution": execution,
            "performance": {"status": "NOT_LOADED", "endpoint": f"/api/strategy-center/qmt-results/performance?run_uid={run_uid}"}}


def read_strategy_results(trade_date: str = "", limit: int = 30) -> dict[str, Any]:
    from server.engine.qmt_strategy_simulation import strategy_catalog

    if trade_date:
        _day(trade_date)
    if not 1 <= limit <= 100:
        raise QmtStrategyResultError("QMT simulation result page limit is invalid")
    engine = get_engine()
    base = {"trade_date": trade_date, "catalog": strategy_catalog(), "schedule": _schedule(engine), **_SAFE}
    if any(not inspect(engine).has_table(table.name) for table in (INPUTS, RESULTS, JOBS)):
        return {**base, "status": "UNAVAILABLE", "reason": "策略模拟结果数据库尚未完成受控发布", "dates": [], "runs": [], "latest": None, "preparation_jobs": []}
    validate_qmt_strategy_result_schema(engine)
    with engine.connect() as connection:
        dates = connection.execute(select(INPUTS.c.trade_date, func.count().label("run_count"))
                                   .group_by(INPUTS.c.trade_date).order_by(INPUTS.c.trade_date.desc()).limit(120)).mappings().all()
        job_dates = connection.execute(select(JOBS.c.trade_date).distinct().order_by(JOBS.c.trade_date.desc()).limit(120)).scalars().all()
        date_counts = {str(row["trade_date"]): int(row["run_count"]) for row in dates}
        date_counts.update({day: date_counts.get(day, 0) for day in job_dates})
        dates = [{"trade_date": day, "run_count": date_counts[day]} for day in sorted(date_counts, reverse=True)[:120]]
        selected_date = trade_date or (str(dates[0]["trade_date"]) if dates else "")
        jobs = connection.execute(select(JOBS).where(JOBS.c.trade_date == selected_date)
                                  .order_by(JOBS.c.created_at.desc(), JOBS.c.request_id.desc()).limit(limit)).mappings().all()
        columns = [INPUTS.c[key] for key in ("snapshot_id", "trade_date", "run_mode", "edge_build_sha", "input_hash", "snapshot_sha256", "issued_at")]
        columns += [RESULTS.c[key] for key in ("run_uid", "origin", "status", "selected_count", "blocked_count", "result_hash", "completed_at", "received_at")]
        rows = connection.execute(select(*columns).select_from(INPUTS.outerjoin(RESULTS, INPUTS.c.snapshot_id == RESULTS.c.snapshot_id))
                                  .where(INPUTS.c.trade_date == selected_date).order_by(INPUTS.c.issued_at.desc(), INPUTS.c.snapshot_id.desc()).limit(limit)).mappings().all()
    latest = read_strategy_run(rows[0]["snapshot_id"]) if rows else None
    return {**base, "trade_date": selected_date, "status": "AVAILABLE" if latest and latest["result"] is not None else "PENDING",
            "dates": [dict(row) for row in dates], "runs": [_summary(row) for row in rows], "latest": latest,
            "preparation_jobs": [_preparation_summary(job) for job in jobs],
            "reason": "" if latest else _preparation_reason(jobs[0]) if jobs else "所选交易日尚无已发出的策略模拟输入"}


def _price_reference(detail: dict[str, Any], sessions: list[str], price_rows: list[Mapping[str, Any]], cutoff: datetime) -> dict[str, Any]:
    """Observe prices strictly after knowledge/issue/execution, with no fill claim."""
    available = [day for day in sessions if datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=TIMEZONE) > cutoff]
    base = {"run_uid": detail["run_uid"], "trade_date": detail["trade_date"], "run_mode": detail["run_mode"], "knowledge_observed_at": cutoff.isoformat(),
            "label": "次一可用开盘价至收盘的税费前价格观察", "is_execution_return": False,
            "dividends_and_costs_included": False, "real_order_allowed": False, "simulation_only": True,
            "method": "NEXT_POST_DECISION_OPEN_TO_CLOSE", "entities": []}
    result = detail.get("result")
    if result is None:
        return {**base, "status": "PENDING_EXECUTION", "reason": "策略尚未执行"}
    if not available:
        return {**base, "status": "PENDING_FORWARD_DATA", "reason": "等待实际观察时点之后的交易日；历史重放不使用过去的开盘价假作成交"}
    entry_day, last_day = available[0], available[-1]
    prices = {(str(row["stock_code"]).split(".", 1)[0], str(row["trade_date"])[:10]): row for row in price_rows}
    for entity in result["strategy_rows"] + result["combination_rows"]:
        picks = []
        for stock in entity["selected"]:
            code = stock["stock_code"]
            entry, last = prices.get((code, entry_day)), prices.get((code, last_day))
            value = {"stock_code": code, "stock_name": stock.get("stock_name", ""), "entry_date": entry_day,
                     "last_date": last_day, "entry_price": None, "last_price": None, "return_pct": None,
                     "status": "DATA_BLOCKED", "reason": "缺少已验真的后续开盘价或收盘价"}
            if entry and last and float(entry.get("open") or 0) > 0 and float(last.get("close") or 0) > 0 and float(entry.get("volume") or 0) > 0:
                value.update({"entry_price": float(entry["open"]), "last_price": float(last["close"]),
                              "return_pct": round((float(last["close"]) / float(entry["open"]) - 1) * 100, 4),
                              "status": "AVAILABLE", "reason": "价格观察值；未证明实际可成交，也未计税费、滑点或分红"})
            picks.append(value)
        verified = [pick["return_pct"] for pick in picks if pick["status"] == "AVAILABLE"]
        horizons = []
        for count in (1, 5, 10, 20):
            exit_day = available[count - 1] if len(available) >= count else None
            changes = []
            if exit_day:
                for stock in entity["selected"]:
                    entry, last = prices.get((stock["stock_code"], entry_day)), prices.get((stock["stock_code"], exit_day))
                    if entry and last and float(entry.get("open") or 0) > 0 and float(entry.get("volume") or 0) > 0 and float(last.get("close") or 0) > 0:
                        changes.append((float(last["close"]) / float(entry["open"]) - 1) * 100)
            horizons.append({"sessions": count, "exit_date": exit_day, "verified_count": len(changes),
                             "status": "AVAILABLE" if changes and len(changes) == len(entity["selected"]) else "PENDING_FORWARD_DATA" if not exit_day else "DATA_BLOCKED",
                             "avg_return_pct": round(sum(changes) / len(changes), 4) if changes and len(changes) == len(entity["selected"]) else None})
        status = "DATA_BLOCKED" if entity["status"] == "DATA_BLOCKED" else "NO_SELECTION" if not picks else "AVAILABLE" if len(verified) == len(picks) else "DATA_BLOCKED"
        base["entities"].append({"strategy_key": entity["strategy_key"], "name": entity["name"], "status": status,
                                 "selected_count": len(picks), "verified_count": len(verified),
                                 "avg_return_pct": round(sum(verified) / len(verified), 4) if verified and len(verified) == len(picks) else None,
                                 "picks": picks, "horizons": horizons})
    return {**base, "status": "AVAILABLE" if any(row["status"] == "AVAILABLE" for row in base["entities"]) else "DATA_BLOCKED",
            "reason": "价格观察只在真实观察时点之后开始；不等同于策略账户收益"}


def read_strategy_performance(run_uid: str = "", trade_date: str = "") -> dict[str, Any]:
    if not run_uid:
        overview = read_strategy_results(trade_date, limit=1)
        if overview.get("latest") is None:
            return {"status": "PENDING_EXECUTION", "reason": "尚无策略执行结果", "entities": [], **_SAFE}
        run_uid = overview["latest"]["run_uid"]
    detail = read_strategy_run(run_uid)
    clock = detail["input"].get("market_clock") or {}
    # The existing fact loaders express their knowledge cutoff in Shanghai
    # civil time. Prepared/issued/execution observations use explicit offsets.
    decision = detail["input"].get("decision_at")
    if decision and datetime.fromisoformat(str(decision)).tzinfo is None:
        decision = datetime.fromisoformat(str(decision)).replace(tzinfo=TIMEZONE).isoformat()
    instants = [detail["issued_at"], detail["input"].get("prepared_at"), clock.get("observed_at"), detail.get("completed_at"), decision]
    cutoff = max(_instant(value) for value in instants if value)
    engine, kline = get_engine(), get_kline_engine()
    current = _now().astimezone(TIMEZONE)
    closed = authoritative_closed_trade_date(engine, now=current)
    if not closed or closed < cutoff.astimezone(TIMEZONE).date().isoformat():
        return _price_reference(detail, [], [], cutoff)
    try:
        from server.common.qmt_trade_calendar import load_trade_calendar_receipt
        from server.common.qmt_daily_market_truth import load_qmt_daily_market_truth
        with engine.connect() as connection:
            receipt = load_trade_calendar_receipt(connection, start_date=detail["trade_date"], end_date=closed, decision_known_at=current)
        sessions = receipt.sessions_between(detail["trade_date"], closed)
        forward = [day for day in sessions if datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=TIMEZONE) > cutoff]
        if not forward:
            return _price_reference(detail, [], [], cutoff)
        codes = sorted({stock["stock_code"] for entity in (detail.get("result") or {}).get("strategy_rows", []) + (detail.get("result") or {}).get("combination_rows", []) for stock in entity["selected"]})
        if not codes:
            return _price_reference(detail, forward, [], cutoff)
        target_days = sorted({forward[0], forward[-1]} | {forward[count - 1] for count in (1, 5, 10, 20) if len(forward) >= count})
        proofs = {}
        # A repeatable view binds the independently verified partitions and all
        # consumed prices, including catalogue eligibility, to the same read.
        with kline.connect() as connection:
            if connection.dialect.name == "mysql":
                connection = connection.execution_options(isolation_level="REPEATABLE READ")
            with connection.begin():
                for day in target_days:
                    proof = load_qmt_daily_market_truth(connection, start_date=day, end_date=day, decision_known_at=current)
                    proofs[day] = proof.as_dict()
                rows = _load_forward_prices(connection, proofs, codes, current)
        return {**_price_reference(detail, forward, rows, cutoff), "price_proofs": proofs, "calendar_manifest_hash": receipt.manifest_hash}
    except Exception as exc:
        _LOGGER.warning("QMT simulation forward observation unavailable: exception_type=%s", type(exc).__name__)
        return {"status": "DATA_BLOCKED", "reason": "后续价格或交易日历尚未通过QMT来源验真", "run_uid": run_uid,
                "run_mode": detail["run_mode"], "is_execution_return": False, "entities": [], **_SAFE}


def _load_forward_prices(connection: Any, proofs: dict[str, Any], codes: list[str], cutoff: datetime) -> list[Mapping[str, Any]]:
    """Read only exact row IDs that the daily truth proof actually attested.

    An unrelated/unattested row with the same code/date must never overwrite a
    verified native quote during projection into the observation basket.
    """
    from server.common.qmt_attestation_contract import ATTESTATION_PROTOCOL_VERSION
    from server.common.qmt_daily_market_truth import QMT_DAILY_PROVIDER

    fragments = []
    params: dict[str, Any] = {"codes": codes, "cutoff": cutoff.replace(tzinfo=None),
                              "provider": QMT_DAILY_PROVIDER, "protocol_version": ATTESTATION_PROTOCOL_VERSION}
    for index, (day, proof) in enumerate(sorted(proofs.items())):
        fragments.append(f"SELECT :day_{index} AS trade_date, :finished_{index} AS run_finished_at, :catalog_{index} AS catalog_batch_id")
        params.update({f"day_{index}": day, f"finished_{index}": proof["run_finished_at"], f"catalog_{index}": proof["catalog_batch_id"]})
    scope = " UNION ALL ".join(fragments)
    statement = text(f"""
        SELECT k.stock_code,k.trade_date,k.`open`,k.`close`,k.volume,k.received_at
        FROM sm_stock_kline AS k
        JOIN ({scope}) AS price_scope ON price_scope.trade_date=k.trade_date
        JOIN qmt_stock_catalog_member AS member
          ON member.batch_id=price_scope.catalog_batch_id
         AND member.stock_code=SUBSTR(k.stock_code,1,6)
         AND member.instrument_type='STOCK' AND member.list_date<=k.trade_date
         AND (member.expire_date IS NULL OR member.expire_date>=k.trade_date)
        WHERE k.k_type=1 AND k.adjust_type=0
          AND SUBSTR(k.stock_code,1,6) IN :codes AND k.received_at<=:cutoff
          AND EXISTS (
            SELECT 1 FROM qmt_kline_attestation_row AS attestation
            JOIN qmt_kline_attestation_run AS source_run
              ON source_run.run_id=attestation.run_id AND source_run.provider=:provider
             AND source_run.status='COMPLETED' AND source_run.finished_at IS NOT NULL
             AND source_run.finished_at<=price_scope.run_finished_at
            WHERE attestation.target_id=k.id AND attestation.protocol_version=:protocol_version
              AND attestation.created_at<=price_scope.run_finished_at
              AND attestation.source_data_version=k.data_version
              AND attestation.source_pre_close_origin='NATIVE_QMT'
              AND attestation.trade_date=k.trade_date
              AND attestation.stock_code=SUBSTR(k.stock_code,1,6)
              AND attestation.source_pre_close=k.pre_close
              AND attestation.attested_open=k.`open` AND attestation.attested_close=k.`close`
              AND attestation.attested_high=k.high AND attestation.attested_low=k.low
              AND attestation.attested_volume=k.volume AND attestation.attested_amount=k.amount
          )
    """).bindparams(bindparam("codes", expanding=True))
    rows = connection.execute(statement, params).mappings().all()
    identities = [(str(row["stock_code"]).split(".", 1)[0], str(row["trade_date"])[:10]) for row in rows]
    if len(identities) != len(set(identities)):
        raise QmtStrategyResultError("QMT forward price security/session identity is duplicated")
    return rows
