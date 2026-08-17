#!/usr/bin/env python3
"""Train immutable T+1/T+5/T+20 Trading V3 Shadow model artifacts.

BLOCK is a valid research result and exits zero unless ``--require-pass`` is
specified.  Runtime/integrity failures exit one; a completed but blocked suite
exits two with ``--require-pass``.  Existing artifacts are never overwritten.
"""

from __future__ import annotations

import argparse
import gc
from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import bindparam, text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import read_frame_chunks
from server.common.kline_data import get_kline_engine
from server.trading_v3.config import config_hash
from server.trading_v3.horizon_models import (
    SUPPORTED_HORIZONS,
    artifact_manifest,
    build_horizon_dataset,
    load_horizon_artifact,
    HORIZON_MODEL_SPECS,
    current_training_window_contract,
    horizon_governance_release_id,
    train_independent_horizon_model,
    load_horizon_suite,
    write_horizon_suite,
)
from tools.env_config import load_project_env


MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
DAILY_BAR_FINALIZATION_TIME = time(15, 30)


def _require_closed_training_cutoff(
    cutoff: date,
    *,
    observed_at: datetime | None = None,
) -> None:
    """Reject a same-session cutoff before the daily bar can be final."""

    observed = observed_at or datetime.now(timezone.utc)
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise ValueError("observed_at must include a timezone")
    market_now = observed.astimezone(MARKET_TIMEZONE)
    if cutoff > market_now.date():
        raise ValueError("training cutoff cannot be in the future")
    if (
        cutoff == market_now.date()
        and market_now.time().replace(tzinfo=None)
        < DAILY_BAR_FINALIZATION_TIME
    ):
        raise RuntimeError(
            "same-session training requires finalized post-close daily bars"
        )


def _latest_trade_date(engine) -> date:
    with engine.connect() as connection:
        value = connection.execute(
            text(
                "SELECT MAX(trade_date) FROM sm_stock_kline "
                "WHERE k_type=1 AND trade_date <= CURDATE()"
            )
        ).scalar()
    if value is None:
        raise RuntimeError("sm_stock_kline has no daily history")
    return value if isinstance(value, date) else date.fromisoformat(str(value))


def _bounded_codes(engine, start: date, end: date, limit: int) -> tuple[str, ...]:
    if limit <= 0:
        return ()
    # Stable liquidity sampling is for resource-bounded model smoke evidence
    # only.  Its manifest is explicitly blocked from production readiness.
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT stock_code
                FROM sm_stock_kline
                WHERE k_type=1
                  AND trade_date BETWEEN :start_date AND :end_date
                  AND (
                      stock_code LIKE '00%%' OR stock_code LIKE '30%%'
                      OR stock_code LIKE '60%%' OR stock_code LIKE '68%%'
                      OR stock_code LIKE '92%%'
                  )
                GROUP BY stock_code
                ORDER BY AVG(COALESCE(amount, 0)) DESC, stock_code
                LIMIT :row_limit
                """
            ),
            {"start_date": start, "end_date": end, "row_limit": limit},
        ).all()
    return tuple(str(row[0]) for row in rows)


def _load_bars(
    engine,
    *,
    start: date,
    end: date,
    maximum_stocks: int,
) -> pd.DataFrame:
    load_start = start - timedelta(days=220)
    codes = _bounded_codes(engine, load_start, end, maximum_stocks)
    code_filter = " AND stock_code IN :stock_codes" if codes else ""
    statement = text(
        """
        SELECT stock_code, trade_date, open, high, low, close,
               pre_close, amount, change_pct, data_source, quality_status,
               short_name
        FROM sm_stock_kline
        WHERE k_type=1
          AND trade_date BETWEEN :start_date AND :end_date
          AND (
              stock_code LIKE '00%%' OR stock_code LIKE '30%%'
              OR stock_code LIKE '60%%' OR stock_code LIKE '68%%'
              OR stock_code LIKE '92%%'
          )
        """ + code_filter + " ORDER BY stock_code, trade_date"
    )
    if codes:
        statement = statement.bindparams(bindparam("stock_codes", expanding=True))
    params: dict[str, object] = {"start_date": load_start, "end_date": end}
    if codes:
        params["stock_codes"] = codes
    chunks: list[pd.DataFrame] = []
    with engine.connect() as connection:
        for chunk in read_frame_chunks(
            statement,
            connection.execution_options(stream_results=True),
            params=params,
            chunksize=100_000,
        ):
            chunks.append(chunk)
    if not chunks:
        raise RuntimeError("sm_stock_kline query returned no rows")
    return pd.concat(chunks, ignore_index=True)


def _existing_suite(
    release_root: Path,
    *,
    require_current_config: bool,
) -> dict[int, dict] | None:
    paths = {horizon: release_root / f"T{horizon}.json" for horizon in SUPPORTED_HORIZONS}
    suite_path = release_root / "suite.json"
    all_paths = (suite_path, *paths.values())
    if not any(path.exists() for path in all_paths):
        return None
    if not all(path.exists() for path in all_paths):
        raise RuntimeError("horizon release is partial; immutable retraining refused")
    load_horizon_suite(
        suite_path,
        require_current_config=require_current_config,
    )
    return {
        horizon: load_horizon_artifact(
            path,
            require_current_config=require_current_config,
        )
        for horizon, path in paths.items()
    }


def _resolve_training_start(
    raw_start: str,
    *,
    maximum_stocks: int,
    configured_start: date,
) -> tuple[date, bool]:
    start = date.fromisoformat(raw_start) if raw_start else configured_start
    non_default = start != configured_start
    if non_default and maximum_stocks == 0:
        raise ValueError(
            "full-universe training --start must equal frozen config "
            f"history_start={configured_start.isoformat()}"
        )
    return start, non_default


def _verify_existing_suite_request(
    artifacts: dict[int, dict],
    *,
    suite_release_id: str,
    signal_start: date,
    training_cutoff: date,
    universe_scope: str,
    configured_history_start: date,
    training_window_protocol: str,
    current_config_hash: str,
) -> None:
    if set(artifacts) != set(SUPPORTED_HORIZONS):
        raise RuntimeError("existing horizon suite is incomplete")
    for horizon, artifact in artifacts.items():
        window = dict(artifact.get("training_window") or {})
        manifest = dict(artifact.get("dataset_manifest") or {})
        if (
            str(artifact.get("suite_release_id") or "")
            != suite_release_id
            or str(artifact.get("training_cutoff") or "")
            != training_cutoff.isoformat()
            or str(window.get("signal_start") or "")
            != signal_start.isoformat()
            or str(window.get("configured_history_start") or "")
            != configured_history_start.isoformat()
            or str(window.get("protocol") or "")
            != training_window_protocol
            or str(window.get("signal_end") or "")
            != training_cutoff.isoformat()
            or str(manifest.get("universe_scope") or "") != universe_scope
            or str(artifact.get("config_hash") or "") != current_config_hash
            or int(artifact.get("horizon_days") or 0) != horizon
        ):
            raise RuntimeError(
                "existing immutable suite differs from requested training window"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start",
        default="",
        help=(
            "inclusive signal start; defaults to the frozen V3 config "
            "history_start"
        ),
    )
    parser.add_argument("--end", default="")
    parser.add_argument("--training-cutoff", default="")
    parser.add_argument("--release-id", default="")
    parser.add_argument(
        "--output-root",
        default="artifacts/trading_v3/horizon_models",
    )
    parser.add_argument(
        "--max-stocks",
        type=int,
        default=300,
        help="0 means full A-share point-in-time universe; nonzero is smoke-only/BLOCK",
    )
    parser.add_argument("--minimum-universe", type=int, default=20)
    parser.add_argument("--require-pass", action="store_true")
    args = parser.parse_args()
    if args.max_stocks < 0:
        parser.error("--max-stocks must not be negative")
    load_project_env()
    window_contract = current_training_window_contract()
    engine = get_kline_engine()
    try:
        end = date.fromisoformat(args.end) if args.end else _latest_trade_date(engine)
        configured_start = date.fromisoformat(window_contract["history_start"])
        start, non_default_window = _resolve_training_start(
            args.start,
            maximum_stocks=args.max_stocks,
            configured_start=configured_start,
        )
        cutoff = (
            date.fromisoformat(args.training_cutoff)
            if args.training_cutoff else end
        )
        if not start < cutoff <= end:
            raise ValueError("training dates must satisfy start < cutoff <= end")
        _require_closed_training_cutoff(cutoff)
        release_id = args.release_id.strip() or (
            f"independent-horizons-{cutoff.isoformat()}-v3"
        )
        release_root = ROOT / args.output_root / release_id
        artifacts = _existing_suite(
            release_root,
            require_current_config=not non_default_window,
        )
        reused = artifacts is not None
        scope = (
            "FULL_A_SHARE_POINT_IN_TIME"
            if args.max_stocks == 0
            else "BOUNDED_SMOKE_RESEARCH_ONLY"
        )
        if artifacts is not None:
            _verify_existing_suite_request(
                artifacts,
                suite_release_id=release_id,
                signal_start=start,
                training_cutoff=cutoff,
                universe_scope=scope,
                configured_history_start=configured_start,
                training_window_protocol=window_contract[
                    "training_window_protocol"
                ],
                current_config_hash=config_hash(),
            )
        if artifacts is None:
            bars = _load_bars(
                engine,
                start=start,
                end=end,
                maximum_stocks=args.max_stocks,
            )
            calendar = sorted(pd.to_datetime(bars["trade_date"]).dt.normalize().unique())
            artifacts = {}
            for horizon in SUPPORTED_HORIZONS:
                dataset = build_horizon_dataset(
                    bars,
                    horizon,
                    trade_calendar=calendar,
                    signal_start=start,
                    signal_end=cutoff,
                    minimum_universe_per_session=args.minimum_universe,
                    universe_scope=scope,
                )
                spec = HORIZON_MODEL_SPECS[horizon]
                artifacts[horizon] = train_independent_horizon_model(
                    dataset,
                    release_id=horizon_governance_release_id(
                        suite_release_id=release_id,
                        model_key=spec.model_key,
                        model_version=spec.model_version,
                        horizon_days=horizon,
                    ),
                    suite_release_id=release_id,
                    training_cutoff=cutoff,
                    created_at=datetime.now(timezone.utc).replace(microsecond=0),
                    candidate_ledger_root=release_root,
                )
                del dataset
                gc.collect()
            write_horizon_suite(
                artifacts,
                release_root,
                require_current_config=not non_default_window,
            )
        manifests = [
            artifact_manifest(
                artifacts[item],
                require_current_config=not non_default_window,
            )
            for item in SUPPORTED_HORIZONS
        ]
        all_pass = all(item["gate_status"] == "PASS" for item in manifests)
        summary = {
            "status": "PASS" if all_pass else "BLOCK",
            "release_id": release_id,
            "release_root": str(release_root.resolve()),
            "reused_immutable_release": reused,
            "universe_scope": (
                "FULL_A_SHARE_POINT_IN_TIME"
                if args.max_stocks == 0 else "BOUNDED_SMOKE_RESEARCH_ONLY"
            ),
            "training_window_protocol": window_contract[
                "training_window_protocol"
            ],
            "configured_history_start": configured_start.isoformat(),
            "signal_start": start.isoformat(),
            "training_window_status": (
                "NON_DEFAULT_TRAINING_WINDOW"
                if non_default_window
                else "FROZEN_DEFAULT_TRAINING_WINDOW"
            ),
            "models": manifests,
            "automatic_promotion_allowed": False,
            "order_authority": False,
        }
        print(json.dumps(summary, ensure_ascii=False, allow_nan=False, indent=2))
        return 2 if args.require_pass and not all_pass else 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        print(json.dumps({
            "status": "ERROR",
            "error_type": type(exc).__name__,
            "message": str(exc),
        }, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(1)
