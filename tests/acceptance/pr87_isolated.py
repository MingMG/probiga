"""Run the two PR #87 checks against the same extracted Git archive.

Run this script explicitly. It is separate from normal pytest discovery
because it reports an observed failure on the historical candidate.
"""

import argparse
import builtins
from datetime import datetime, timezone
import faulthandler
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import socket
import sys
import tarfile
import threading
import time
import types
from xml.etree import ElementTree


CANDIDATE = "94d1b4c939d42f506c3c019a6e358f2cb9fead45"
TABLES = ("st_sim_position", "st_sim_order", "st_trade_flow", "st_sim_event")
REGRESSION = ("tests/test_sim_trade_manual_close.py", "tests/test_premarket_theme_forecast.py")


def isolate():
    """Keep application settings and real order SDKs outside the test process."""
    for key in set(os.environ) - {"SYSTEMROOT", "WINDIR", "PATH", "TEMP", "TMP", "TZ"}:
        del os.environ[key]
    calls = []

    def forbidden_order(*args, **kwargs):
        calls.append("broker_order")
        raise AssertionError("An isolated test tried to submit a broker order")

    class BrokerStub:
        def __init__(self, *args, **kwargs):
            pass

        order_stock = staticmethod(forbidden_order)
        order_stock_async = staticmethod(forbidden_order)

    sdk = types.ModuleType("xtquant")
    sdk.__path__ = []
    trader = types.ModuleType("xtquant.xttrader")
    trader.XtQuantTrader = BrokerStub
    sdk.xttrader = trader
    sys.modules.update({"xtquant": sdk, "xtquant.xttrader": trader})
    builtins.passorder = forbidden_order

    def block_external_sockets(event, args):
        if event in {"socket.connect", "socket.bind", "socket.sendto"}:
            address = args[1]
            if not isinstance(address, tuple) or address[0] != "127.0.0.1":
                raise AssertionError("External network is unavailable in this isolated run")
        if event in {"socket.gethostbyname", "socket.getaddrinfo"} and args[0] not in {None, "127.0.0.1"}:
            raise AssertionError("External DNS is unavailable in this isolated run")

    sys.addaudithook(block_external_sockets)
    return calls


def regression(pytest, report_path):
    junit = report_path.with_suffix(".junit.xml")
    command = ["-q", *REGRESSION, f"--junitxml={junit}"]
    exit_code = int(pytest.main(command))
    suite = ElementTree.parse(junit).getroot().find("testsuite")
    return exit_code, {"pytest_arguments": command[:-1], "counts": {
        key: int(suite.attrib[key]) for key in ("tests", "failures", "errors", "skipped")
    }}


def http_acceptance(pytest):
    import httpx
    import uvicorn
    from fastapi import FastAPI
    from test_sim_trade_manual_close import simulated_market
    from server.api.routers import sim_trade

    app = FastAPI()
    app.include_router(sim_trade.router, prefix="/api")
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="warning"))
    with pytest.MonkeyPatch.context() as monkeypatch, socket.socket() as listener:
        fixture = simulated_market.__wrapped__(monkeypatch)
        engine, _ = next(fixture)
        with engine.begin() as connection:
            connection.exec_driver_sql("""
                INSERT INTO st_sim_position
                (id, stock_code, short_name, strategy_type, buy_date, buy_price, buy_shares, ai_score, status)
                SELECT 2, stock_code, short_name, strategy_type, buy_date, buy_price, buy_shares, ai_score, status
                FROM st_sim_position WHERE id = 1
            """)

        def ledger():
            with engine.connect() as connection:
                return {table: connection.exec_driver_sql(f"SELECT * FROM {table}").all() for table in TABLES}

        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        worker = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        worker.start()
        try:
            deadline = time.monotonic() + 5
            while not server.started and worker.is_alive() and time.monotonic() < deadline:
                time.sleep(0.025)
            if not server.started:
                raise RuntimeError("Isolated HTTP listener did not start within 5 seconds")
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=5) as client:
                simulation = client.post("/api/sim-trade/close/1")
                before = ledger()
                real = client.post("/api/sim-trade/close/2", json={
                    "account_type": "real", "account_id": "isolated-fixture-only",
                })
                after = ledger()
        finally:
            server.should_exit = True
            worker.join(5)
            fixture.close()
        if worker.is_alive():
            raise RuntimeError("Isolated HTTP listener did not stop within 5 seconds")

    simulated_fill = (
        simulation.status_code == 200 and simulation.json().get("status") == "ok"
        and len(before["st_sim_order"]) == len(before["st_trade_flow"]) == 1
    )
    rejected = real.status_code in {400, 403, 422} or real.json().get("status") == "error"
    unchanged = before == after
    return int(not (simulated_fill and rejected and unchanged)), {
        "route": "POST /api/sim-trade/close/{position_id}",
        "simulated_sell": {"http": simulation.status_code, "result": simulation.json().get("status"), "filled": simulated_fill},
        "real_account_request": {
            "body_account_type": "real", "account_id_is_synthetic": True,
            "http": real.status_code, "result": real.json().get("status"), "rejected": rejected,
        },
        "ledger_unchanged_after_real_account_request": unchanged,
        "ledger_changed_tables": [table for table in TABLES if before[table] != after[table]],
        "simulated_order_row_delta": len(after["st_sim_order"]) - len(before["st_sim_order"]),
        "simulated_flow_row_delta": len(after["st_trade_flow"]) - len(before["st_trade_flow"]),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("regression", "http"))
    parser.add_argument("archive", type=Path, help="git archive --format=tar of the exact candidate")
    parser.add_argument("source", type=Path, help="Fresh source directory from extracting that archive")
    parser.add_argument("report", type=Path)
    parser.add_argument("--run-id", required=True, help="Same identifier for both stages of one isolated run")
    args = parser.parse_args()
    faulthandler.dump_traceback_later(90, exit=True)
    source, report_path = args.source.resolve(), args.report.resolve()
    with tarfile.open(args.archive) as archive:
        if archive.pax_headers.get("comment") != CANDIDATE or (source / ".env").exists():
            raise ValueError("Expected the exact PR #87 Git archive and a source tree without .env")
        for member in archive:
            if member.isfile() and (source / member.name).read_bytes() != archive.extractfile(member).read():
                raise ValueError(f"Extracted candidate differs at {member.name}")
    with args.archive.open("rb") as stream:
        archive_digest = hashlib.file_digest(stream, "sha256").hexdigest()
    os.chdir(source)
    sys.path[:0] = [str(source), str(source / "tests")]
    calls = isolate()
    import pytest

    started = datetime.now(timezone.utc)
    exit_code, details = regression(pytest, report_path) if args.kind == "regression" else http_acceptance(pytest)
    faulthandler.cancel_dump_traceback_later()
    result = {
        "pull_request": 87, "kind": args.kind, "commit": CANDIDATE,
        "environment": "pr87-isolated-loopback-sqlite",
        "run_id": args.run_id,
        "candidate_archive_sha256": archive_digest,
        "python": sys.version.split()[0], "pytest": pytest.__version__,
        "uvicorn": importlib.metadata.version("uvicorn"),
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "result": "failed" if exit_code or calls else "passed",
        "broker_order_stub_calls": len(calls),
        **details,
    }
    report_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))
    return exit_code or bool(calls)


if __name__ == "__main__":
    raise SystemExit(main())
