#!/usr/bin/env python3
"""Apply the legacy simulator schema migration through an explicit write gate."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.engine.sim_trade_engine import migrate_sim_trade_schema
from tools.env_config import load_project_env


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create or upgrade the simulator tables (schema writes)."
    )
    parser.add_argument(
        "--allow-schema-change",
        action="store_true",
        help="explicitly authorize CREATE/ALTER statements",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.allow_schema_change:
        parser.error("--allow-schema-change is required")

    load_project_env()
    migrate_sim_trade_schema(allow_schema_change=True)
    print(json.dumps({"status": "ok", "schema": "sim_trade"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
