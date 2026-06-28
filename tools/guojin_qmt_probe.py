#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt import bridge
from integrations.qmt.diagnostics import capabilities, core_probe, diagnostics


REFERENCE_OPERATIONS = (
    "download_sector_data",
    "download_index_weight",
    "download_holiday_data",
    "download_history_contracts",
    "download_his_st_data",
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe the locally installed Guojin QMT client and native SDK")
    parser.add_argument("--refresh-reference", action="store_true", help="Download QMT reference datasets before probing")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    result: dict[str, object] = {
        "diagnostics": diagnostics(timeout=min(args.timeout, 20), force=True),
    }
    if args.refresh_reference:
        refresh_rows: list[dict[str, object]] = []
        for operation in REFERENCE_OPERATIONS:
            try:
                operation_result = bridge.refresh_reference_data([operation], timeout=args.timeout)
            except Exception as exc:
                refresh_rows.append(
                    {
                        "api_name": operation,
                        "status": "TIMEOUT" if "timed out" in str(exc).casefold() else "FAILED",
                        "error": str(exc),
                    }
                )
                continue
            rows = operation_result.get("rows") or []
            refresh_rows.extend(rows if isinstance(rows, list) else [])
        result["reference_refresh"] = {"rows": refresh_rows}
    result["capabilities"] = capabilities(timeout=min(args.timeout, 30), force=True)
    result["core_probe"] = core_probe(timeout=args.timeout, force=True)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))

    status = str((result["diagnostics"] or {}).get("status") if isinstance(result["diagnostics"], dict) else "error")
    core_status = str((result["core_probe"] or {}).get("status") if isinstance(result["core_probe"], dict) else "error")
    return 0 if status == "ok" and core_status in {"ok", "warn"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
