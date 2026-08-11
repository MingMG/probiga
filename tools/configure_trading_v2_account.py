#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.account_configuration import (
    apply_fee_configuration,
    apply_permission_configuration,
    refresh_account_activation,
)
from tools.env_config import load_project_env


def _load(path: str) -> dict:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("configuration file must contain one JSON object")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Apply user-confirmed V2 broker evidence."
    )
    parser.add_argument("--fee-json")
    parser.add_argument("--permission-json")
    args = parser.parse_args()
    if not args.fee_json and not args.permission_json:
        raise SystemExit(
            "provide --fee-json and/or --permission-json; defaults are prohibited"
        )
    load_project_env()
    engine = create_batch_engine()
    results = []
    if args.fee_json:
        results.append(apply_fee_configuration(engine, _load(args.fee_json)))
    if args.permission_json:
        results.append(
            apply_permission_configuration(
                engine,
                _load(args.permission_json),
            )
        )
    activation = refresh_account_activation(engine)
    print(
        json.dumps(
            {"status": "ok", "changes": results, "activation": activation},
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
