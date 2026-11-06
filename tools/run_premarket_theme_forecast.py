#!/usr/bin/env python3
"""Publish the exact 09:08 browser research forecast."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.premarket.theme_forecast import PREMARKET_PUBLICATION_SCHEMA, publish_current_premarket_theme_forecast
from server.common.batch_db import create_batch_engine
from tools.env_config import load_project_env


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--json", action="store_true", required=True)
    args = parser.parse_args(argv)
    load_project_env()
    engine = create_batch_engine()
    try:
        receipt = publish_current_premarket_theme_forecast(engine, session_date=args.session_date)
    except Exception as exc:
        receipt = {
            "schema": PREMARKET_PUBLICATION_SCHEMA, "status": "BLOCKED",
            "session_date": args.session_date, "error": str(exc)[:500],
            "notification_allowed": False, "canonical_recommendation_writes": False,
        }
    finally:
        engine.dispose()
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0 if receipt["status"] == "COMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
