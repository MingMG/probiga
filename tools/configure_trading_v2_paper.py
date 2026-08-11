#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Configure and enable ProBigA's isolated V2 paper account."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from server.trading_v2.account_configuration import (
    refresh_account_activation,
)
from server.trading_v2.jobs import transition_strategy
from server.trading_v2.paper_configuration import (
    install_internal_paper_configuration,
)
from tools.env_config import load_project_env


STOCK_STRATEGY_IDS = (
    "main_wave",
    "short_term",
    "swing",
    "ultra_short",
)


def _enable_paper_trials(engine) -> list[dict]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT strategy_id, version, lifecycle_status
                FROM st_strategy_version_v2
                WHERE strategy_id IN
                    ('main_wave','short_term','swing','ultra_short')
                  AND lifecycle_status IN
                    ('RESEARCH','SHADOW','PAPER_TRIAL','PAPER_ACTIVE')
                ORDER BY strategy_id, version
                """
            )
        ).mappings().all()
    found = {str(row["strategy_id"]) for row in rows}
    missing = sorted(set(STOCK_STRATEGY_IDS) - found)
    if missing:
        raise RuntimeError(
            "paper trial strategy registry is incomplete: "
            + ",".join(missing)
        )
    results: list[dict] = []
    for row in rows:
        lifecycle = str(row["lifecycle_status"])
        if lifecycle in {"PAPER_TRIAL", "PAPER_ACTIVE"}:
            results.append(
                {
                    "strategy_id": row["strategy_id"],
                    "strategy_version": row["version"],
                    "previous_status": lifecycle,
                    "next_status": lifecycle,
                    "status": "already_enabled",
                }
            )
            continue
        if lifecycle not in {"RESEARCH", "SHADOW"}:
            raise RuntimeError(
                "strategy cannot enter paper trial from "
                f"{row['strategy_id']}:{lifecycle}"
            )
        now = datetime.now().astimezone()
        results.append(
            transition_strategy(
                engine,
                strategy_id=str(row["strategy_id"]),
                strategy_version=str(row["version"]),
                next_status="PAPER_TRIAL",
                reason=(
                    "User-authorized ProBigA forward paper trial; "
                    "profitability remains unproven and real trading is off"
                ),
                operator="codex-user-authorized",
                validation_patch={
                    "paper_trial_authorized": True,
                    "paper_trial_started_at": now.isoformat(
                        timespec="seconds"
                    ),
                    "paper_account_id": "paper-main-v2",
                    "paper_trial_purpose": (
                        "collect real forward simulation evidence"
                    ),
                    "profitability_status": (
                        "UNPROVEN_FORWARD_TRIAL"
                    ),
                    "profitability_unproven": True,
                    "paper_drawdown_limit": "0.10",
                    "real_trading_enabled": False,
                },
            )
        )
    return results


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Enable ProBigA's internal V2 paper account."
    )
    parser.add_argument(
        "--effective-from",
        default="2026-07-27",
        help="frozen paper configuration start date (YYYY-MM-DD)",
    )
    args = parser.parse_args()
    effective_from = date.fromisoformat(args.effective_from)
    load_project_env()
    engine = create_batch_engine()
    try:
        configuration = install_internal_paper_configuration(
            engine,
            effective_from=effective_from,
        )
        strategies = _enable_paper_trials(engine)
        activation = refresh_account_activation(engine)
    finally:
        engine.dispose()
    result = {
        "status": "ok",
        "execution_mode": "PROBIGA_INTERNAL_PAPER",
        "configuration": configuration,
        "strategies": strategies,
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
