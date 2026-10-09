# -*- coding: utf-8 -*-
"""Capture on logged-in Windows QMT and commit only through the Linux API."""
from __future__ import annotations

import argparse
import base64
from datetime import date, datetime
import hashlib
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
from acquisition.qmt_model import (
    MAX_REQUEST_BYTES, publish_json, read_json as read_spool_json,
    trusted_root, validate_id,
)
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
    decode_object,
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
MAX_COMMIT_RESPONSE_BYTES = 64 * 1024


class QmtLinuxIngestClientError(RuntimeError):
    pass


class QmtIngestBudgetExpired(RuntimeError):
    """A retained request must be resumed, not cancelled or called complete."""


class QmtIngestBatchStop(RuntimeError):
    """A completed handoff boundary or retained scope stops further capture."""

    def __init__(self, reason: str, *, request_id: str | None = None,
                 source_errors: tuple[str, ...] = ()):
        super().__init__("QMT ingestion stopped at a retained batch boundary")
        self.reason = reason
        self.request_id = request_id
        self.source_errors = source_errors


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

    @staticmethod
    def _report_commit_attempt(attempt, started, *, status=None, failure=None,
                               configured_retry_delay=None):
        """Bounded transport diagnostics, never a commit or coverage receipt."""
        record = {
            "event": "QMT_COMMIT_HTTP_ATTEMPT",
            "attempt": attempt + 1,
            "elapsed_ms": max(0, int((time.monotonic() - started) * 1000)),
            "http_status": status if type(status) is int else None,
            "failure": failure,
            "configured_retry_delay_seconds": configured_retry_delay,
            "business_completion_inferred": False,
        }
        # No URL, credentials, request/response body, or exception message is
        # written. Losing diagnostic stderr must not replay a successful POST.
        stream = sys.stderr
        if stream is None:
            return
        try:
            print(json.dumps(record, sort_keys=True), file=stream, flush=True)
        except Exception:
            pass

    def post(
        self,
        endpoint: str,
        payload: Mapping[str, Any],
        *,
        retry_delays: tuple[int, ...] = (),
        deadline: float | None = None,
        retain_response=None,
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
            started = time.monotonic()
            try:
                response = self.session.post(
                    self.server_url + endpoint,
                    data=body,
                    headers=headers,
                    timeout=min(self.timeout, remaining) if remaining is not None else self.timeout,
                )
            except requests.RequestException as exc:
                if endpoint == "/api/qmt-ingest/commit":
                    failure = ("timeout" if isinstance(exc, requests.Timeout) else
                               "connection" if isinstance(exc, requests.ConnectionError) else
                               "request")
                    self._report_commit_attempt(
                        attempt, started, failure=failure,
                        configured_retry_delay=delays[attempt] if attempt < len(delays) else None,
                    )
                _remaining(deadline)
                if attempt < len(delays):
                    _retry_wait(delays[attempt], deadline)
                    continue
                raise QmtLinuxIngestClientError(
                    "Linux ingestion API is unavailable"
                ) from exc
            if endpoint == "/api/qmt-ingest/commit":
                retry = response.status_code in RETRYABLE_COMMIT_STATUS_CODES and attempt < len(delays)
                self._report_commit_attempt(
                    attempt, started, status=response.status_code,
                    configured_retry_delay=delays[attempt] if retry else None,
                )
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
            if retain_response is not None:
                retain_response(response.content)
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


def _validate_commit_receipt(response, raw):
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


def _commit_response_path(transport, raw):
    request_id = validate_id(raw["request"]["request_id"])
    trusted_root(transport.processed)
    directory = os.path.join(transport.processed, request_id)
    if os.path.lexists(directory):
        trusted_root(directory)
    return directory, os.path.join(directory, request_id + ".commit-response.json")


def _read_commit_response(client, transport, raw):
    _directory, path = _commit_response_path(transport, raw)
    retained = read_spool_json(path, MAX_REQUEST_BYTES)
    if retained is None:
        return None
    try:
        if (type(retained) is not dict or set(retained) != {
                "schema", "request_id", "result_sha256", "http_body_base64", "http_body_sha256"}
                or retained["schema"] != "probiga.qmt-linux-ingest-commit-response.v1"
                or retained["request_id"] != raw["request"]["request_id"]
                or retained["result_sha256"] != canonical_sha256(raw)):
            raise ValueError
        body = base64.b64decode(retained["http_body_base64"], validate=True)
        if hashlib.sha256(body).hexdigest() != retained["http_body_sha256"]:
            raise ValueError
        envelope = decode_object(body, limit=MAX_COMMIT_RESPONSE_BYTES)
        return _validate_commit_receipt(verify_signed_response(client.secret, envelope), raw)
    except (TypeError, ValueError, KeyError) as exc:
        raise QmtLinuxIngestClientError("Retained Linux commit response proof differs") from exc


def _commit(client: Client, identity: Mapping[str, str], raw: Mapping[str, Any], *,
            transport: QmtTransport, deadline: float | None = None) -> dict[str, Any]:
    _remaining(deadline)
    retained = _read_commit_response(client, transport, raw)
    if retained is not None:
        _remaining(deadline)
        return retained

    def preserve(body):
        if type(body) is not bytes:
            raise QmtLinuxIngestClientError("Linux commit HTTP original is unavailable")
        # Preserve actual HTTP bytes only, never reconstruct or re-sign the
        # envelope. A late successful HTTP response still has its original
        # retained before the run-budget check, for exact recovery next run.
        envelope = decode_object(body, limit=MAX_COMMIT_RESPONSE_BYTES)
        _validate_commit_receipt(verify_signed_response(client.secret, envelope), raw)
        directory, path = _commit_response_path(transport, raw)
        os.makedirs(directory, exist_ok=True)
        trusted_root(directory)
        record = dict(schema="probiga.qmt-linux-ingest-commit-response.v1",
                      request_id=raw["request"]["request_id"], result_sha256=canonical_sha256(raw),
                      http_body_base64=base64.b64encode(body).decode("ascii"),
                      http_body_sha256=hashlib.sha256(body).hexdigest())
        try:
            publish_json(path, record, MAX_REQUEST_BYTES, immutable=True)
        except FileExistsError:
            pass  # Only an independently reverified original can satisfy replay.
        if _read_commit_response(client, transport, raw) is None:
            raise QmtLinuxIngestClientError("Linux commit HTTP original was not retained")

    response = client.post("/api/qmt-ingest/commit", {
        "schema": COMMIT_SCHEMA, **identity, "result": dict(raw),
    }, retry_delays=COMMIT_RETRY_DELAYS_SECONDS, deadline=deadline, retain_response=preserve)
    _validate_commit_receipt(response, raw)
    retained = _read_commit_response(client, transport, raw)
    if retained is None:
        raise QmtLinuxIngestClientError("Linux commit HTTP original was not retained")
    return retained


def _batch_boundary(committed: list[dict[str, Any]], max_batches: int | None) -> None:
    """Call only AFTER the exact raw's signed commit and successful archive."""
    receipt = committed[-1]
    if receipt["counts"]["error"]:
        source_errors = tuple(sorted(SOURCE_BLOCKING_ERRORS.intersection(
            receipt["counts"].get("error_codes") or [])))
        raise QmtIngestBatchStop("error_units", source_errors=source_errors)
    if max_batches is not None and len(committed) >= max_batches:
        raise QmtIngestBatchStop("batch_limit")


def _request_in_scope(request: Mapping[str, Any] | None, *, datasets: list[str],
                      start_date: str, end_date: str) -> bool:
    if not isinstance(request, Mapping) or request.get("dataset") not in datasets:
        return False
    try:
        start, end = request["start_date"], request["end_date"]
        return (type(start) is str and type(end) is str
                and date.fromisoformat(start_date).isoformat() == start_date
                and date.fromisoformat(end_date).isoformat() == end_date
                and date.fromisoformat(start).isoformat() == start
                and date.fromisoformat(end).isoformat() == end
                and start_date <= start == end <= end_date)
    except (KeyError, TypeError, ValueError):
        return False


def _recover(
    transport: QmtTransport,
    client: Client,
    identity: Mapping[str, str],
    *,
    deadline: float,
    datasets: list[str],
    start_date: str,
    end_date: str,
    max_batches: int | None,
    committed: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    committed = [] if committed is None else committed
    inventory = transport.recover()
    active = inventory.get("active")
    if active:
        request_id = str(active["request_id"])
        if not _request_in_scope(active, datasets=datasets, start_date=start_date,
                                 end_date=end_date):
            raise QmtIngestBatchStop("retained_scope", request_id=request_id)
        raw = transport.read_result(request_id)
        if raw is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise QmtIngestBudgetExpired("QMT ingestion budget ended with an active request")
            raw = _wait_result(transport, request_id, deadline)
        committed.append(_commit(client, identity, raw, transport=transport, deadline=deadline))
        transport.archive(request_id)
        _batch_boundary(committed, max_batches)
    inventory = transport.recover()
    pending: list[str] = []
    blocked: list[str] = []
    # Receiving already captured data is independent of permission to capture
    # more. Finish every retained result before considering undispatched plans.
    for request_id in inventory.get("prepared", []):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        request = transport.read_request(request_id)
        if not _request_in_scope(request, datasets=datasets, start_date=start_date,
                                 end_date=end_date):
            blocked.append(request_id)
            continue
        raw = transport.read_result(request_id)
        if raw is None:
            pending.append(request_id)
            continue
        transport.activate(request_id)
        committed.append(_commit(client, identity, raw, transport=transport, deadline=deadline))
        transport.archive(request_id)
        _batch_boundary(committed, max_batches)
    # Unknown or out-of-scope retained originals are not authorization to
    # dispatch another dataset/day. In-scope returned raw above can still drain.
    if blocked:
        raise QmtIngestBatchStop("retained_scope", request_id=blocked[0])
    if inventory.get("temporary") or set(inventory.get("ready", [])) - set(inventory.get("prepared", [])):
        raise QmtIngestBatchStop("retained_scope")
    for request_id in pending:
        _remaining(deadline)
        raw = transport.read_result(request_id)
        if raw is None:
            request = transport.read_request(request_id)
            if not _request_in_scope(request, datasets=datasets, start_date=start_date,
                                     end_date=end_date):
                raise QmtIngestBatchStop("retained_scope", request_id=request_id)
            source_plan = _plan(
                client, identity, dataset=request["dataset"],
                start_date=request["start_date"], end_date=request["end_date"],
                deadline=deadline,
            )
            if source_plan["source_cooldown"]:
                raise QmtSourceCooldown(request_id, source_plan["source_retry_at"])
            # A signed plan request can exhaust the run budget. It does not
            # authorize activation after that deadline.
            _remaining(deadline)
        transport.activate(request_id)
        if raw is None:
            raw = _wait_result(transport, request_id, deadline)
        committed.append(_commit(client, identity, raw, transport=transport, deadline=deadline))
        transport.archive(request_id)
        _batch_boundary(committed, max_batches)
    return committed


def run(
    *,
    server_url: str,
    datasets: list[str],
    start_date: str,
    end_date: str,
    apply: bool,
    budget_seconds: int,
    max_batches: int | None = None,
) -> dict[str, Any]:
    if max_batches is not None and (type(max_batches) is not int or max_batches <= 0):
        raise ValueError("max_batches must be a positive integer or None")
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
            _recover(transport, client, identity, deadline=deadline, committed=receipts,
                     datasets=datasets, start_date=start_date, end_date=end_date,
                     max_batches=max_batches)
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
                receipt = _commit(client, identity, raw, transport=transport, deadline=deadline)
                receipts.append(receipt)
                transport.archive(request["request_id"])
                _batch_boundary(receipts, max_batches)
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
    except QmtIngestBatchStop as exc:
        result = progress("source_cooldown" if exc.source_errors else "partial")
        result["stop_reason"] = exc.reason
        result["max_batches"] = max_batches
        # No post-handoff authoritative coverage read was completed. A limit
        # or error stop is never whole-day/month completion, even for one batch.
        result["coverage_verified"] = False
        result["pending_units"] = None
        if exc.request_id is not None:
            result["retained_request_id"] = exc.request_id
        if exc.source_errors:
            result["source_error_codes"] = list(exc.source_errors)
        return result
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
    parser.add_argument("--max-batches", type=_positive_batch_count, default=None,
                        help="Stop after this many signed commits and successful raw archives")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def _positive_batch_count(value: str) -> int:
    try:
        count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("max-batches must be a positive integer") from exc
    if count <= 0:
        raise argparse.ArgumentTypeError("max-batches must be a positive integer")
    return count


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
            max_batches=args.max_batches,
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0 if result["status"] in {"complete", "planned"} else 2
    except Exception as exc:
        print(json.dumps(getattr(exc, "ingestion_progress", None) or {
            "status": "error",
            "error": type(exc).__name__,
        }, ensure_ascii=False, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
