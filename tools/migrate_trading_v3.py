#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.db.migrations_v3 import run_v3_migrations
from tools.env_config import create_tool_engine, load_project_env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    load_project_env()
    engine = create_tool_engine()
    try:
        results = run_v3_migrations(engine, dry_run=args.dry_run)
    finally:
        engine.dispose()
    print(json.dumps(
        {
            "status": "ok",
            "dry_run": args.dry_run,
            "migrations": [asdict(item) for item in results],
        },
        ensure_ascii=False,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
