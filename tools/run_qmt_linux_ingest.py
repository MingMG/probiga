# -*- coding: utf-8 -*-
"""Capture on logged-in Windows QMT and commit only through the Linux API."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Mapping

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from acquisition.models import WorkUnit
from acquisition.qmt_model import history_allowed
from acquisition.qmt_transport import QmtTransport
from acquisition.runner import make_request
from integrations.bigqmt.spool import bridge_paths, read_json, resolve_big_qmt_home
from server.common.component_release import runtime_component_build_sha
from server.common.config import get_ai_bridge_config
from server.common.qmt_linux_ingest_protocol import (
    COMMIT_SCHEMA,
    PLAN_SCHEMA,
    canonical_json,
    canonical_sha256,
    new_request_headers,
    verify_signed_response,
)
from tools.env_config import load_project_env
from tools.remote_support import remote_host


SUPPORTED_DATASETS = (
    "stock_daily",
    "stock_minute",
    "index_daily",
    "index_minute",
)

# A reverse proxy can return 504 while the Linux worker is still committing the
# exact immutable result.  The store owns request_id and makes that replay
# idempotent, so bounded retries are safer than abandoning the retained QMT
# result and requiring an operator to restart the client.
COMMIT_RETRY_DELAYS_SECONDS = (15, 30, 60, 120)
RETRYABLE_COMMIT_STATUS_CODES = frozenset({500, 502, 503, 504})
SOURCE_BLOCKING_ERRORS = frozenset({
    "SOURCE_ACCESS_DENIED", "SOURCE_UNAVAILABLE", "INVALID_RETRY_AFTER", "NATIVE_CALL_FAILED",
})


class QmtLinuxIngestClientError(RuntimeError):
    pass


class QmtIngestBudgetExpired(RuntimeError):
    """A retained request must be resumed, not cancelled or called complete."""


class QmtSourceCooldown(RuntimeError):
    """A prepared request is retained without dispatching new native work."""

    def __init__(self, request_id: str, source_retry_at: str | None):
        super().__init__("QMT source cooldown blocks an undispatched request")
        self.request_id = request_id
        self.source_retry_at = source_retry_at


def _remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise QmtIngestBudgetExpired("QMT ingestion budget ended")
    return remaining


def _retry_wait(delay: int, deadline: float | None) -> None:
    remaining = _remaining(deadline)
    if remaining is not None and delay >= remaining:
        raise QmtIngestBudgetExpired("QMT retry exceeds ingestion budget")
    time.sleep(delay)


def _wait_result(transport: QmtTransport, request_id: str, deadline: float):
    remaining = _remaining(deadline)
    try:
        return transport.wait_result(request_id, timeout=min(1200, remaining))
    except TimeoutError:
        _remaining(deadline)  # A run-budget stop is partial, not a new source failure.
        raise


class Client:
    def __init__(self, server_url: str, secret: str, *, timeout: int = 120):
        self.server_url = server_url.rstrip("/")
        self.secret = secret
        self.timeout = int(timeout)
        self.session = requests.Session()

    def close(self) -> None:
        self.session.close()

    def post(
        self,
        endpoint: str,
        payload: Mapping[str, Any],
        *,
        retry_delays: tuple[int, ...] = (),
        deadline: float | None = None,
    ) -> dict[str, Any]:
        body = canonical_json(dict(payload))
        delays = tuple(max(0, int(value)) for value in retry_delays)
        for attempt in range(len(delays) + 1):
            remaining = _remaining(deadline)
            # Every retry is a new authenticated HTTP request for the same
            # immutable payload.  Reusing a nonce would correctly be rejected
            # by the Linux replay guard.
            headers = {
                "Content-Type": "application/json",
                **new_request_headers(self.secret, payload),
            }
            try:
                response = self.session.post(
                    self.server_url + endpoint,
                    data=body,
                    headers=headers,
                    timeout=min(self.timeout, remaining) if remaining is not None else self.timeout,
                )
            except requests.RequestException as exc:
                _remaining(deadline)
                if attempt < len(delays):
                    _retry_wait(delays[attempt], deadline)
                    continue
                raise QmtLinuxIngestClientError(
                    "Linux ingestion API is unavailable"
                ) from exc
            if response.status_code == 200:
                break
            if (
                response.status_code in RETRYABLE_COMMIT_STATUS_CODES
                and attempt < len(delays)
            ):
                _retry_wait(delays[attempt], deadline)
                continue
            raise QmtLinuxIngestClientError(
                f"Linux ingestion API rejected the request: HTTP {response.status_code}"
            )
        try:
            payload = response.json()
            if not isinstance(payload, dict):
                raise TypeError
            result = verify_signed_response(self.secret, payload)
            _remaining(deadline)
            return result
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise QmtLinuxIngestClientError("Linux ingestion response proof differs") from exc


def _edge_identity() -> tuple[dict[str, str], Path]:
    home = resolve_big_qmt_home(required=True)
    paths = bridge_paths(home)
    heartbeat = read_json(paths["heartbeat"])
    model_sha = str(heartbeat.get("direct_acquisition_model_sha256") or "").strip().lower()
    if heartbeat.get("direct_acquisition_status") not in {"idle", "busy", "awaiting_commit"}:
        raise QmtLinuxIngestClientError("QMT direct acquisition model is unavailable")
    if len(model_sha) != 64:
        raise QmtLinuxIngestClientError("QMT direct acquisition model identity is unavailable")
    identity = {
        "edge_build_sha": runtime_component_build_sha("windows"),
        "model_sha256": model_sha,
    }
    direct_root = paths["root"].parent / "probiga_direct_acquisition" / "qmt"
    return identity, direct_root


def _plan(
    client: Client,
    identity: Mapping[str, str],
    *,
    dataset: str,
    start_date: str,
    end_date: str,
    deadline: float | None = None,
) -> dict[str, Any]:
    result = client.post("/api/qmt-ingest/plan", {
        "schema": PLAN_SCHEMA,
        **identity,
        "dataset": dataset,
        "start_date": start_date,
        "end_date": end_date,
    }, retry_delays=COMMIT_RETRY_DELAYS_SECONDS, deadline=deadline)
    core = dict(result)
    core.pop("schema", None)
    supplied = str(core.pop("plan_sha256", ""))
    if (
        result.get("status") != "ready"
        or result.get("dataset") != dataset
        or result.get("start_date") != start_date
        or result.get("end_date") != end_date
        or type(result.get("source_cooldown")) is not bool
        or result.get("edge_build_sha") != identity["edge_build_sha"]
        or result.get("model_sha256") != identity["model_sha256"]
        or supplied != canonical_sha256(core)
        or int(result.get("batch_count") or 0) != len(result.get("batches") or [])
    ):
        raise QmtLinuxIngestClientError("Linux ingestion plan proof differs")
    coverage = result.get("coverage")
    if not isinstance(coverage, list) or len(coverage) != result.get("session_count"):
        raise QmtLinuxIngestClientError("Linux ingestion coverage proof differs")
    seen: set[str] = set()
    for item in coverage:
        if not isinstance(item, dict):
            raise QmtLinuxIngestClientError("Linux ingestion coverage proof differs")
        day = str(item.get("target_date") or "")
        try:
            canonical_day = date.fromisoformat(day).isoformat() == day
        except ValueError:
            canonical_day = False
        counts = [item.get(key) for key in ("expected", "complete", "no_data", "missing")]
        if (
            item.get("dataset") != dataset
            or not canonical_day
            or not start_date <= day <= end_date
            or day in seen
            or any(type(value) is not int or value < 0 for value in counts)
            or counts[0] != sum(counts[1:])
            or item.get("status") != ("complete" if counts[0] and not counts[3] else "partial")
        ):
            raise QmtLinuxIngestClientError("Linux ingestion coverage proof differs")
        seen.add(day)
    return result


def _commit(
    client: Client,
    identity: Mapping[str, str],
    raw: Mapping[str, Any],
    *,
    deadline: float | None = None,
) -> dict[str, Any]:
    response = client.post(
        "/api/qmt-ingest/commit",
        {
            "schema": COMMIT_SCHEMA,
            **identity,
            "result": dict(raw),
        },
        retry_delays=COMMIT_RETRY_DELAYS_SECONDS,
        deadline=deadline,
    )
    if (
        response.get("status") != "committed"
        or response.get("request_id") != raw.get("request", {}).get("request_id")
        or response.get("result_sha256") != canonical_sha256(raw)
    ):
        raise QmtLinuxIngestClientError("Linux commit receipt differs")
    counts = response.get("counts")
    outcomes = len(raw.get("outcomes") or {})
    if (
        not isinstance(counts, dict)
        or any(type(counts.get(key)) is not int or counts[key] < 0
               for key in ("complete", "no_data", "error", "replayed"))
        or sum(counts[key] for key in ("complete", "no_data", "error")) != outcomes
        or counts["replayed"] > counts["complete"] + counts["no_data"]
    ):
        raise QmtLinuxIngestClientError("Linux commit outcome proof differs")
    return response


def _recover(
    transport: QmtTransport,
    client: Client,
    identity: Mapping[str, str],
    *,
    deadline: float,
    committed: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    committed = [] if committed is None else committed
    inventory = transport.recover()
    active = inventory.get("active")
    if active:
        request_id = str(active["request_id"])
        raw = transport.read_result(request_id)
        if raw is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise QmtIngestBudgetExpired("QMT ingestion budget ended with an active request")
            raw = _wait_result(transport, request_id, deadline)
        committed.append(_commit(client, identity, raw, deadline=deadline))
        transport.archive(request_id)
    inventory = transport.recover()
    pending: list[str] = []
    # Receiving already captured data is independent of permission to capture
    # more. Finish every retained result before considering undispatched plans.
    for request_id in inventory.get("prepared", []):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        raw = transport.read_result(request_id)
        if raw is None:
            pending.append(request_id)
            continue
        transport.activate(request_id)
        committed.append(_commit(client, identity, raw, deadline=deadline))
        transport.archive(request_id)
    for request_id in pending:
        _remaining(deadline)
        if not history_allowed(datetime.now().astimezone()):
            break  # Persisted plans do not authorize a new native call in the live window.
        raw = transport.read_result(request_id)
        if raw is None:
            request = transport.read_request(request_id)
            if request is None:
                raise QmtLinuxIngestClientError("QMT prepared request identity is unavailable")
            source_plan = _plan(
                client, identity, dataset=request["dataset"],
                start_date=request["start_date"], end_date=request["end_date"],
                deadline=deadline,
            )
            if source_plan["source_cooldown"]:
                raise QmtSourceCooldown(request_id, source_plan["source_retry_at"])
            # A signed plan request can take long enough to cross the capture
            # window or exhaust the run budget. Neither permits activation.
            _remaining(deadline)
            if not history_allowed(datetime.now().astimezone()):
                break
        transport.activate(request_id)
        if raw is None:
            raw = _wait_result(transport, request_id, deadline)
        committed.append(_commit(client, identity, raw, deadline=deadline))
        transport.archive(request_id)
    return committed


def run(
    *,
    server_url: str,
    datasets: list[str],
    start_date: str,
    end_date: str,
    apply: bool,
    budget_seconds: int,
) -> dict[str, Any]:
    identity, direct_root = _edge_identity()
    secret = str(get_ai_bridge_config().get("token") or "")
    if not secret:
        raise QmtLinuxIngestClientError("QMT ingestion machine credential is unavailable")
    client = Client(server_url, secret)
    transport = QmtTransport(str(direct_root))
    deadline = time.monotonic() + max(1, min(int(budget_seconds), 21600))
    receipts: list[dict[str, Any]] = []
    plans: dict[str, int] = {}
    coverage: dict[str, list[dict[str, Any]]] = {}
    verified: set[str] = set()

    def progress(status: str) -> dict[str, Any]:
        checked = set(datasets).issubset(verified)
        successful = sum(int(item["counts"]["complete"]) + int(item["counts"]["no_data"])
                         for item in receipts)
        replayed = sum(int(item["counts"]["replayed"]) for item in receipts)
        try:
            active = transport.recover().get("active")
            retained_request_id = active.get("request_id") if active else None
        except Exception:
            retained_request_id = None
        return {
            "status": status,
            **identity,
            "datasets": list(datasets),
            "start_date": start_date,
            "end_date": end_date,
            "planned_batches": dict(plans),
            "committed_batches": len(receipts),
            "committed_units": successful,
            "newly_committed_units": successful - replayed,
            "replayed_units": replayed,
            "error_units": sum(int(item["counts"]["error"]) for item in receipts),
            "coverage": dict(coverage),
            "coverage_verified": checked,
            "pending_units": sum(item["missing"] for days in coverage.values() for item in days)
                             if checked else None,
            "retained_request_id": retained_request_id,
        }

    try:
        if apply:
            _recover(transport, client, identity, deadline=deadline, committed=receipts)
        if apply and not history_allowed(datetime.now().astimezone()):
            return progress("waiting_history_window")
        for dataset in datasets:
            plan = _plan(
                client,
                identity,
                dataset=dataset,
                start_date=start_date,
                end_date=end_date,
                deadline=deadline,
            )
            batches = list(plan["batches"])
            plans[dataset] = len(batches)
            coverage[dataset] = plan["coverage"]
            if not apply:
                verified.add(dataset)
                continue
            if plan["source_cooldown"]:
                result = progress("source_cooldown")
                result["source_retry_at"] = plan["source_retry_at"]
                return result
            for batch in batches:
                remaining = _remaining(deadline)
                if remaining < 1:
                    raise QmtIngestBudgetExpired("QMT ingestion budget ended before capture")
                if not history_allowed(datetime.now().astimezone()):
                    return progress("waiting_history_window")
                units = [
                    WorkUnit(
                        batch["dataset"],
                        batch["source"],
                        batch["target_date"],
                        code,
                        batch["period"],
                        batch["adjustment"],
                    )
                    for code in batch["codes"]
                ]
                request = make_request(
                    units,
                    datetime.now().astimezone(),
                    timeout=min(1200, int(remaining)),
                )
                transport.prepare(request)
                transport.activate(request["request_id"])
                raw = _wait_result(transport, request["request_id"], deadline)
                receipt = _commit(client, identity, raw, deadline=deadline)
                receipts.append(receipt)
                transport.archive(request["request_id"])
                source_errors = SOURCE_BLOCKING_ERRORS.intersection(
                    receipt["counts"].get("error_codes") or [])
                if source_errors:
                    # A source failure is not permission to exhaust the rest
                    # of a precomputed queue. Preserve its retry state and
                    # let the monitored recovery verify source health first.
                    result = progress("source_cooldown")
                    result["source_error_codes"] = sorted(source_errors)
                    return result
            # A zero-batch plan can mean running/cooldown, not completion.
            # Re-read authoritative state after applying all eligible batches.
            coverage[dataset] = _plan(
                client, identity, dataset=dataset,
                start_date=start_date, end_date=end_date, deadline=deadline,
            )["coverage"]
            verified.add(dataset)
        if not apply:
            return progress("planned")
        _remaining(deadline)
        unfinished = any(item["status"] != "complete"
                         for days in coverage.values() for item in days)
        return progress("partial" if unfinished else "complete")
    except QmtSourceCooldown as exc:
        result = progress("source_cooldown")
        result["source_retry_at"] = exc.source_retry_at
        result["retained_request_id"] = exc.request_id
        return result
    except QmtIngestBudgetExpired:
        return progress("partial")
    except Exception as exc:
        # Preserve acknowledged progress on an infrastructure failure; never
        # log exception strings that can contain credentials or raw SQL.
        detail = progress("error")
        detail["error"] = type(exc).__name__
        detail["winerror"] = getattr(exc, "winerror", None)
        exc.ingestion_progress = detail
        raise
    finally:
        client.close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-url", default="")
    parser.add_argument("--dataset", action="append", choices=SUPPORTED_DATASETS, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--budget-seconds", type=int, default=7200)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    load_project_env(ROOT / ".env")
    args = _parser().parse_args(argv)
    server_url = args.server_url.strip() or os.environ.get(
        "PROBIGA_QMT_INGEST_SERVER_URL", ""
    ).strip() or f"http://{remote_host()}"
    try:
        result = run(
            server_url=server_url,
            datasets=list(dict.fromkeys(args.dataset)),
            start_date=args.start_date,
            end_date=args.end_date,
            apply=args.apply,
            budget_seconds=args.budget_seconds,
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if result["status"] in {"complete", "planned", "waiting_history_window"} else 2
    except Exception as exc:
        print(json.dumps(getattr(exc, "ingestion_progress", None) or {
            "status": "error",
            "error": type(exc).__name__,
        }, ensure_ascii=False, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
