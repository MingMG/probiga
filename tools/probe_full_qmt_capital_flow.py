#!/usr/bin/env python3
"""One-shot, read-only probe of native full-QMT daily capital-flow fields."""
from __future__ import annotations

import argparse
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from acquisition.config import Config  # noqa: E402
from acquisition.datasets import get_spec  # noqa: E402
from acquisition.models import WorkUnit  # noqa: E402
from acquisition.qmt_transport import QmtTransport  # noqa: E402
from acquisition.runner import make_request, process_lock  # noqa: E402


SHANGHAI = ZoneInfo("Asia/Shanghai")
FLOW_FIELDS = (
    "bidMostAmount",
    "offMostAmount",
    "bidBigAmount",
    "offBigAmount",
    "bidMediumAmount",
    "offMediumAmount",
    "bidSmallAmount",
    "offSmallAmount",
)


def _summary(result: dict, qualified_code: str) -> dict[str, object]:
    outcome = result["outcomes"][qualified_code]
    if outcome.get("status") != "data" or not outcome.get("rows"):
        raise ValueError("full QMT returned no capital-flow data")
    rows = outcome["rows"]
    for row in rows:
        for field in FLOW_FIELDS:
            if field not in row:
                raise ValueError("full QMT capital-flow field is missing")
            try:
                value = Decimal(str(row[field]))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise ValueError("full QMT capital-flow field is not numeric") from exc
            if not value.is_finite():
                raise ValueError("full QMT capital-flow field is not finite")
    return {
        "status": "ok",
        "source_method": result["source_method"],
        "field_names": list(FLOW_FIELDS),
        "row_count": len(rows),
    }


def run_probe(
    config_path: str,
    qualified_code: str,
    trade_date: str,
    timeout: float,
) -> dict[str, object]:
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    config = Config.load(config_path)
    spec = get_spec("capital_flow_daily")
    unit = WorkUnit(
        spec.name,
        spec.source,
        trade_date,
        qualified_code,
        spec.period,
        "none",
    )
    request = make_request([unit], datetime.now(SHANGHAI), timeout=timeout)
    with process_lock(config.state_dir / "daily.lock"):
        transport = QmtTransport(str(config.state_dir / "qmt"))
        if transport.recover()["active"]:
            raise RuntimeError("another QMT request is active")
        transport.prepare(request)
        transport.activate(request["request_id"])
        result = transport.wait_result(request["request_id"], timeout=timeout)
        try:
            return _summary(result, qualified_code)
        finally:
            transport.archive(request["request_id"])


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--qualified-code", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--timeout", type=float, default=180.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_probe(
            args.config,
            args.qualified_code,
            args.trade_date,
            args.timeout,
        )
        exit_code = 0
    except Exception:
        result = {
            "status": "error",
            "source_method": "",
            "field_names": [],
            "row_count": 0,
        }
        exit_code = 1
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
