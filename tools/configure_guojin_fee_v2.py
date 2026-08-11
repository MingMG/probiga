#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Apply user-confirmed Guojin fees to ProBigA's isolated paper account."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.account_configuration import (
    apply_fee_configuration,
    refresh_account_activation,
)
from server.trading_v2.paper_configuration import (
    CONFIRMED_FEE_PAPER_RULE_VERSION,
    bind_confirmed_fee_to_internal_paper,
)
from tools.env_config import load_project_env


DEFAULT_CONFIG = ROOT / "strategies" / "guojin_fee_profile_v2.json"


def _load(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("fee configuration must be one JSON object")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Apply confirmed Guojin fees and bind them to the isolated "
            "ProBigA paper account."
        )
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG),
        help="auditable fee JSON path",
    )
    parser.add_argument(
        "--paper-rule-version",
        default=CONFIRMED_FEE_PAPER_RULE_VERSION,
    )
    args = parser.parse_args()

    payload = _load(Path(args.config))
    effective_from = date.fromisoformat(str(payload["effective_from"]))
    fee_profile_version = str(payload["fee_profile_version"])

    load_project_env()
    engine = create_batch_engine()
    try:
        fee_result = apply_fee_configuration(engine, payload)
        binding_result = bind_confirmed_fee_to_internal_paper(
            engine,
            fee_profile_version=fee_profile_version,
            effective_from=effective_from,
            rule_version=args.paper_rule_version,
        )
        activation = refresh_account_activation(engine)
    finally:
        engine.dispose()

    result = {
        "status": "ok",
        "fee_configuration": fee_result,
        "paper_binding": binding_result,
        "activation": activation,
        "real_trading_enabled": False,
    }
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )
    return 0 if activation["status"] == "ACTIVE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
