# -*- coding: utf-8 -*-
"""Capture on logged-in Windows QMT and commit only through the Linux API."""
from __future__ import annotations

import argparse
from datetime import datetime
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


class QmtLinuxIngestClientError(RuntimeError):
    pass


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
    ) -> dict[str, Any]:
        body = canonical_json(dict(payload))
        delays = tuple(max(0, int(value)) for value in retry_delays)
        for attempt in range(len(delays) + 1):
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
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                if attempt < len(delays):
                    time.sleep(delays[attempt])
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
                time.sleep(delays[attempt])
                continue
            raise QmtLinuxIngestClientError(
                f"Linux ingestion API rejected the request: HTTP {response.status_code}"
            )
        try:
            payload = response.json()
            if not isinstance(payload, dict):
                raise TypeError
            return verify_signed_response(self.secret, payload)
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
) -> dict[str, Any]:
    result = client.post("/api/qmt-ingest/plan", {
        "schema": PLAN_SCHEMA,
        **identity,
        "dataset": dataset,
        "start_date": start_date,
        "end_date": end_date,
    })
    core = dict(result)
    core.pop("schema", None)
    supplied = str(core.pop("plan_sha256", ""))
    if (
        result.get("status") != "ready"
        or result.get("edge_build_sha") != identity["edge_build_sha"]
        or result.get("model_sha256") != identity["model_sha256"]
        or supplied != canonical_sha256(core)
        or int(result.get("batch_count") or 0) != len(result.get("batches") or [])
    ):
        raise QmtLinuxIngestClientError("Linux ingestion plan proof differs")
    return result


def _commit(
    client: Client,
    identity: Mapping[str, str],
    raw: Mapping[str, Any],
) -> dict[str, Any]:
    response = client.post(
        "/api/qmt-ingest/commit",
        {
            "schema": COMMIT_SCHEMA,
            **identity,
            "result": dict(raw),
        },
        retry_delays=COMMIT_RETRY_DELAYS_SECONDS,
    )
    if (
        response.get("status") != "committed"
        or response.get("request_id") != raw.get("request", {}).get("request_id")
        or response.get("result_sha256") != canonical_sha256(raw)
    ):
        raise QmtLinuxIngestClientError("Linux commit receipt differs")
    return response


def _recover(
    transport: QmtTransport,
    client: Client,
    identity: Mapping[str, str],
    *,
    deadline: float,
) -> list[dict[str, Any]]:
    committed: list[dict[str, Any]] = []
    inventory = transport.recover()
    active = inventory.get("active")
    if active:
        request_id = str(active["request_id"])
        raw = transport.read_result(request_id)
        if raw is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise QmtLinuxIngestClientError("QMT ingestion budget ended with an active request")
            raw = transport.wait_result(request_id, timeout=min(1200, remaining))
        committed.append(_commit(client, identity, raw))
        transport.archive(request_id)
    inventory = transport.recover()
    for request_id in inventory.get("prepared", []):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        transport.activate(request_id)
        raw = transport.read_result(request_id)
        if raw is None:
            raw = transport.wait_result(request_id, timeout=min(1200, remaining))
        committed.append(_commit(client, identity, raw))
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
    try:
        if apply:
            receipts.extend(_recover(transport, client, identity, deadline=deadline))
        if apply and not history_allowed(datetime.now().astimezone()):
            return {
                "status": "waiting_history_window",
                "edge_build_sha": identity["edge_build_sha"],
                "model_sha256": identity["model_sha256"],
                "planned_batches": {},
                "committed_batches": len(receipts),
                "committed_units": sum(
                    int(item.get("counts", {}).get("complete") or 0)
                    + int(item.get("counts", {}).get("no_data") or 0)
                    for item in receipts
                ),
                "error_units": sum(
                    int(item.get("counts", {}).get("error") or 0)
                    for item in receipts
                ),
            }
        for dataset in datasets:
            plan = _plan(
                client,
                identity,
                dataset=dataset,
                start_date=start_date,
                end_date=end_date,
            )
            batches = list(plan["batches"])
            plans[dataset] = len(batches)
            if not apply:
                continue
            for batch in batches:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
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
                    timeout=min(1200, max(60, int(remaining))),
                )
                transport.prepare(request)
                transport.activate(request["request_id"])
                raw = transport.wait_result(
                    request["request_id"],
                    timeout=min(1200, remaining),
                )
                receipts.append(_commit(client, identity, raw))
                transport.archive(request["request_id"])
            if time.monotonic() >= deadline:
                break
        error_units = sum(int(item.get("counts", {}).get("error") or 0) for item in receipts)
        return {
            "status": "partial" if time.monotonic() >= deadline or error_units else "complete",
            "edge_build_sha": identity["edge_build_sha"],
            "model_sha256": identity["model_sha256"],
            "planned_batches": plans,
            "committed_batches": len(receipts),
            "committed_units": sum(
                int(item.get("counts", {}).get("complete") or 0)
                + int(item.get("counts", {}).get("no_data") or 0)
                for item in receipts
            ),
            "error_units": error_units,
        }
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
        return 0 if result["status"] in {"complete", "waiting_history_window"} else 2
    except Exception as exc:
        print(json.dumps({
            "status": "error",
            "error": type(exc).__name__,
        }, ensure_ascii=False, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
