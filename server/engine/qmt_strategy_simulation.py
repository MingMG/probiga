"""Immutable inputs and exact existing formulas for QMT daily simulations.

Preparation reads the production fact loaders on Linux. Evaluation consumes
only its JSON snapshot, so the Windows worker and server verify the same result.
No broker, order, portfolio cash, or guessed financial inputs are consumed here.
Selections are formula-confirmed simulation signals, not reported trade fills.
"""
from __future__ import annotations

import ast
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


INPUT_SCHEMA = "probiga.qmt-strategy-simulation-input.v1"
RESULT_SCHEMA = "probiga.qmt-strategy-simulation-result.v1"
SHANGHAI = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parents[2]
V2_KEYS = ("ultra_short", "short_term", "swing", "main_wave")
V3_KEYS = (
    "theme_diffusion", "low_base_ignition", "right_side_trend",
    "event_drift", "quality_momentum", "oversold_reversal",
)
STRATEGY_KEYS = V2_KEYS + V3_KEYS
EXCLUDED_KEYS = ("intraday_surprise", "weak_market_structural_mainline")
_V3_NAMES = {
    "theme_diffusion": "板块扩散", "low_base_ignition": "低位点火",
    "right_side_trend": "右侧主升", "event_drift": "事件漂移",
    "quality_momentum": "质量动量", "oversold_reversal": "超跌修复",
    "intraday_surprise": "盘中超预期",
    "weak_market_structural_mainline": "弱市结构性主线",
}
_SOURCE_FILES = (
    "server/engine/qmt_strategy_simulation.py",
    "biz/analysis/sync_analysis_fast.py",
    "server/common/chase_risk_policy.py",
    "server/common/versioned_strategy_config.py",
    "server/common/strategy_daily_input_window.py",
    "server/trading_v3/config.py",
    "server/trading_v3/calibration.py",
    "server/trading_v3/domain.py",
    "server/trading_v3/engine.py",
    "server/trading_v3/shadow_portfolio.py",
    "server/trading_v3/repository.py",
    "server/trading_v3/validation.py",
    "server/trading_v3/sleeves.py",
    "server/trading_v3/right_side_policy.py",
    "server/trading_v3/regime.py",
    "server/trading_v3/theme_features.py",
    "server/trading_v3/daily_features.py",
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def snapshot_input_hash(snapshot: Mapping[str, Any]) -> str:
    """Bind every snapshot field except the digest field itself."""
    return canonical_hash({key: value for key, value in snapshot.items()
                           if key != "input_hash"})


def _plain(value: Any) -> Any:
    """Preserve values across pandas/SQL/JSON; missing numbers stay missing."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if type(value).__name__ in {"NAType", "NaTType"}:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (float, Decimal)):
        number = float(value)
        return number if math.isfinite(number) else None
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    # NumPy scalar types expose item(); pandas.NA and NaT are missing values.
    item = getattr(value, "item", None)
    if callable(item):
        return _plain(item())
    raise ValueError(f"unsupported snapshot value type: {type(value).__name__}")


def _date(value: str | date) -> str:
    if isinstance(value, datetime):
        raise ValueError("trade_date must be an exact date")
    if isinstance(value, date):
        return value.isoformat()
    parsed = date.fromisoformat(str(value))
    if parsed.isoformat() != str(value):
        raise ValueError("trade_date must be an exact ISO date")
    return parsed.isoformat()


def _configs() -> tuple[dict[str, Any], dict[str, Any]]:
    return (
        json.loads((ROOT / "strategies/stock_strategy_v2.json").read_text("utf-8")),
        json.loads((ROOT / "strategies/trading_v3.json").read_text("utf-8")),
    )


@lru_cache(maxsize=1)
def _parse_combinations(source: str) -> list[tuple[str, str, str, dict[str, float]]]:
    # Read the authoritative frozen seed literal without importing governance's
    # database and execution-ledger machinery into a simulation worker.
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name)
            and target.id == "DEFAULT_SEEDED_COMBINATIONS"
            for target in node.targets
        ):
            recipes = ast.literal_eval(node.value)
            return [recipe for recipe in recipes
                    if set(recipe[3]).issubset(STRATEGY_KEYS)]
    raise ValueError("authoritative combination seed is unavailable")


def _combinations() -> list[tuple[str, str, str, dict[str, float]]]:
    source = (ROOT / "server/engine/strategy_governance.py").read_text("utf-8")
    return _parse_combinations(source)


def formula_contract() -> dict[str, Any]:
    manifest, config = _configs()
    files = {
        name: hashlib.sha256((ROOT / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
        for name in _SOURCE_FILES
    }
    return {
        "manifest_version": manifest["manifest_version"],
        "v2_manifest_hash": canonical_hash(manifest),
        "v3_version": config["strategy_version"],
        "v3_config_hash": canonical_hash(config),
        "executor_sha256": files[_SOURCE_FILES[0]],
        "source_sha256": files,
        "combination_recipes_hash": canonical_hash(_plain(_combinations())),
        "v2_formula_authority": "biz.analysis.sync_analysis_fast.compute_scores",
        "v3_formula_authority": "server.trading_v3.sleeves.SLEEVE_BUILDERS",
        "v3_right_side_builder": "right_side_trend_v304",
    }


def strategy_catalog() -> dict[str, Any]:
    manifest, config = _configs()
    by_key = {item["key"]: item for item in manifest["strategies"]}
    strategies = [
        {"strategy_key": key, "name": by_key[key]["name"],
         "version": manifest["manifest_version"], "family": "V2"}
        for key in V2_KEYS
    ] + [
        {"strategy_key": key, "name": _V3_NAMES[key],
         "version": str(config["calibration_version_tokens"][key]), "family": "V3"}
        for key in V3_KEYS
    ]
    catalog = {item["strategy_key"]: item for item in strategies}
    combinations = []
    for key, name, description, weights in _combinations():
        members = [{**catalog[member], "weight": weight}
                   for member, weight in weights.items()]
        combinations.append({
            "strategy_key": key, "name": name, "description": description,
            "version": "frozen-" + canonical_hash(members)[:16],
            "members": members,
        })
    return {
        "strategies": strategies, "combinations": combinations,
        "excluded": [{"strategy_key": key, "name": _V3_NAMES[key],
                      "reason": "按用户要求排除"} for key in EXCLUDED_KEYS],
    }


def _failure(stage: str, exc: Exception) -> dict[str, Any]:
    # SQL connection exceptions can include connection settings and SQL args.
    # Only established data-contract errors may become public diagnostic text.
    detail = " ".join(str(exc).split())[:600]
    prefixes = ("DATA_BLOCKED", "PIT_", "QMT_", "ANALYSIS_INPUT_",
                "KLINE_", "至少需要", "immutable QMT", "No K-line")
    reason = detail if detail.startswith(prefixes) else type(exc).__name__
    return {"status": "DATA_BLOCKED", "reasons": [f"{stage}: {reason}"]}


def _require_target_daily_truth(engine: Any, target: str, decision_at: datetime,
                                source_label: str) -> None:
    """Fail fast on a necessary missing day, never replace window validation.

    Production history validation proceeds oldest to newest and reconstructs
    immutable catalog/calendar roots for each day. If the requested last day
    is unavailable, none of that historical work can produce valid inputs.
    A successful check grants no shortcut: the original complete 60/70-day
    loaders still validate every partition in their own immutable DB view.
    """
    from server.common.qmt_daily_market_truth import load_qmt_daily_market_truth
    from server.common.strategy_daily_input_window import daily_input_snapshot

    try:
        with daily_input_snapshot(engine) as connection:
            truth = load_qmt_daily_market_truth(
                connection, start_date=target, end_date=target,
                decision_known_at=decision_at,
            )
        if list(truth.requested_sessions) != [target] or truth.attested_row_count <= 0:
            raise RuntimeError("current QMT target rows/attestations are incomplete")
    except Exception as exc:
        detail = " ".join(str(exc).split())[:400]
        safe_prefixes = ("no completed QMT daily attestation", "current QMT target",
                         "QMT daily source", "QMT attestation", "completed QMT attestation",
                         "QMT no-row", "no complete independent QMT stock catalog",
                         "no immutable QMT calendar")
        diagnostic = detail if detail.startswith(safe_prefixes) else type(exc).__name__
        raise RuntimeError(
            f"DATA_BLOCKED: {source_label}_TARGET_DAY_TRUTH_UNAVAILABLE:{target}: {diagnostic}"
        ) from exc


def _prepare_v2(engine: Any, target: str, decision_at: datetime) -> dict[str, Any]:
    import pandas as pd
    from biz.analysis import sync_analysis_fast as production
    from server.common.pit_facts import PIT_AVAILABLE, resolve_common_fact_cutoff

    _require_target_daily_truth(engine, target, decision_at, "V2_PRIMARY")
    kline = production.load_kline_features(engine, target, decision_known_at=decision_at)
    if kline.empty:
        raise RuntimeError("DATA_BLOCKED: target-date QMT daily universe is empty")
    codes = sorted(kline["stock_code"].astype(str).str.zfill(6).unique())
    cutoff = resolve_common_fact_cutoff(
        engine, codes=codes, decision_at=decision_at,
        finance_start_date="1900-01-01", finance_end_date=target,
        event_start_date=date.fromisoformat(target) - timedelta(days=14),
        event_end_date=target, require_qmt_event_batch=True,
    )
    if cutoff.get("status") != PIT_AVAILABLE:
        raise RuntimeError("ANALYSIS_INPUT_BATCH_BLOCKED: " + str(
            cutoff.get("reason") or "PIT_INPUT_BATCH_UNAVAILABLE"))
    fact_cutoff = cutoff["fact_cutoff_at"]
    finance = production.load_finance(
        engine, target, decision_at=decision_at,
        fact_cutoff_at=fact_cutoff, stock_codes=codes,
    )
    for field in ("pit_common_cutoff_status", "pit_common_cutoff_reason",
                  "pit_common_receipt_root_hash"):
        finance[field] = cutoff.get({
            "pit_common_cutoff_status": "status",
            "pit_common_cutoff_reason": "reason",
            "pit_common_receipt_root_hash": "receipt_root_hash",
        }[field])
    reconstruction = dict((cutoff.get("qmt_event_batch") or {}).get(
        "reconstruction_provenance") or {})
    for field, value in {
        "pit_reconstruction_mode": (cutoff.get("qmt_event_batch") or {}).get("mode") or "",
        "pit_reconstruction_sha256": (cutoff.get("qmt_event_batch") or {}).get("reconstruction_sha256") or "",
        "pit_reconstructed_at": reconstruction.get("reconstructed_at") or "",
    }.items():
        finance[field] = value
    flow, flow_date = production.load_flow_features(engine, target, decision_known_at=decision_at)
    flow_proof = production.validate_exact_daily_flow_coverage(
        engine, trade_date=target, kline=kline, flow=flow, decision_known_at=decision_at,
    )
    turnover_proof = production._verify_full_market_turnover_inputs(kline, trade_date=target)
    hot, hot_date = production.load_hot_rank(engine, target, decision_at=decision_at)
    notices = production.load_notice_features(
        engine, target, decision_at=decision_at,
        fact_cutoff_at=fact_cutoff, stock_codes=codes,
    )
    notices = production.merge_event_features(notices, pd.DataFrame({"stock_code": []}))
    sector = production.load_sector_rotation_features(engine, target, decision_known_at=decision_at)
    sector = production._complete_membership_proof_scope(sector, codes)
    frames = {
        "kline": kline, "finance": finance, "flow": flow, "hot": hot,
        "notices": notices, "sector": sector,
        "confidence": production.load_confidence_features(engine, target, decision_at=decision_at),
        "rec_history": production.load_recommendation_history(engine, target, decision_at=decision_at),
        "failures": production.load_failure_features(engine, target, decision_at=decision_at),
    }
    return _plain({
        "status": "READY", "reasons": [], "trade_date": target,
        "flow_date": flow_date, "hot_date": hot_date,
        "market_mood_score": production.compute_market_mood(kline),
        "frames": {key: frame.to_dict("records") for key, frame in frames.items()},
        "proofs": {
            "qmt_daily_input_window": kline.attrs.get("qmt_daily_input_window"),
            "common_fact_cutoff": cutoff, "flow": flow_proof, "turnover": turnover_proof,
            "optional_formal_factors": {
                "hot": "DISABLED_MUTABLE_HISTORY_BY_PRODUCTION",
                "news": "DISABLED_MUTABLE_HISTORY_BY_PRODUCTION",
                "confidence": "PRODUCTION_NEUTRAL_62",
                "rec_history": "DISABLED_MUTABLE_HISTORY_BY_PRODUCTION",
                "failures": "PRODUCTION_NEUTRAL_100",
            },
        },
    })


def _prepare_v3(primary: Any, kline: Any, target: str, decision_at: datetime) -> dict[str, Any]:
    from server.trading_v3.daily_features import load_daily_feature_universe
    from server.trading_v3.repository import TradingV3Repository
    from sqlalchemy import text

    _require_target_daily_truth(kline, target, decision_at, "V3_KLINE")
    dataset = load_daily_feature_universe(
        primary, kline, as_of=date.fromisoformat(target),
        context_cutoff_at=decision_at, limit=5000,
    )
    if _date(dataset["trade_date"]) != target:
        raise RuntimeError("DATA_BLOCKED: V3 source trade date differs from requested date")
    # The existing shadow ledger ranks calibrated forecasts, not disconnected
    # raw-score thresholds. Keep exactly the registry's production acceptance
    # gates and bind the accepted tables plus their observation-time metadata.
    status = TradingV3Repository(primary).active_calibration_status()
    with primary.connect() as connection:
        registry = connection.execute(text("""
            SELECT strategy_key, model_version, dataset_hash, calibration_json,
                   created_at, activated_at
            FROM st_model_registry_v3 WHERE lifecycle_status = 'PAPER_ACTIVE'
            ORDER BY activated_at DESC, created_at DESC
        """)).mappings().all()
    latest: dict[str, dict[str, Any]] = {}
    for raw in registry:
        latest.setdefault(str(raw["strategy_key"]), dict(raw))
    for key, table in status["calibrations"].items():
        registered = latest.get(key)
        if (registered is None or registered["model_version"] != table.model_version
                or registered["dataset_hash"] != table.dataset_hash
                or canonical_hash(json.loads(str(registered["calibration_json"]))) != canonical_hash(_plain(table.as_dict()))):
            raise RuntimeError("DATA_BLOCKED: calibration registry changed during input preparation")
    for key, registered in latest.items():
        if key not in V3_KEYS:
            continue
        for field in ("created_at", "activated_at"):
            observed = registered.get(field)
            if observed is None or observed > decision_at:
                raise RuntimeError("DATA_BLOCKED: calibration registry was not known at the requested cutoff")
    # Lifecycle status is mutable and has no retirement-time history. A current
    # registry cannot prove a historical active-model set, even if surviving
    # models themselves were created before the replay date.
    if decision_at.date() < datetime.now(SHANGHAI).date():
        raise RuntimeError("DATA_BLOCKED: historical calibration registry state is unproven")
    return _plain({"status": "READY", "reasons": [], **dataset,
                   "calibration_inputs": {
                       "status": "READY", "authority": "TradingV3Repository.active_calibration_status",
                       "observed_at": datetime.now(SHANGHAI).isoformat(),
                       "calibrations": {key: table.as_dict() for key, table in status["calibrations"].items() if key in V3_KEYS},
                       "rejections": {key: value for key, value in status["rejections"].items() if key in V3_KEYS},
                       "registry": [{key: value for key, value in row.items() if key != "calibration_json"}
                                    for row in latest.values() if row["strategy_key"] in V3_KEYS],
                   }})


def prepare_inputs(primary_engine: Any, kline_engine: Any,
                   trade_date: str | date, *, run_mode: str | None = None) -> dict[str, Any]:
    """Read exact production inputs; a missing family yields explicit blocking."""
    from server.common.authoritative_market_clock import authoritative_closed_trade_date

    target = _date(trade_date)
    now = datetime.now(SHANGHAI).replace(microsecond=0)
    current_closed = authoritative_closed_trade_date(primary_engine, now=now)
    if not current_closed or target > current_closed:
        raise ValueError("requested trade date has not closed")
    historical_cutoff = datetime.combine(date.fromisoformat(target), time(23, 59, 59))
    if authoritative_closed_trade_date(primary_engine, now=historical_cutoff.replace(tzinfo=SHANGHAI)) != target:
        raise ValueError("requested date is not an exchange trading session")
    mode = run_mode if run_mode is not None else "DAILY" if target == current_closed else "REPLAY"
    if mode not in {"DAILY", "REPLAY"}:
        raise ValueError("simulation mode is invalid")
    if mode == "DAILY" and target != current_closed:
        raise ValueError("daily simulation must use the latest closed session")
    decision_at = now.replace(tzinfo=None) if mode == "DAILY" else historical_cutoff
    snapshot: dict[str, Any] = {
        "schema": INPUT_SCHEMA, "trade_date": target, "mode": mode,
        "prepared_at": now.isoformat(), "decision_at": decision_at.isoformat(),
        "market_clock": {"expected_trade_date": target,
                         "current_closed_trade_date": current_closed,
                         "observed_at": now.isoformat()},
        "formula_contract": formula_contract(),
        "simulation_only": True, "real_order_allowed": False,
    }
    try:
        snapshot["v2"] = _prepare_v2(primary_engine, target, decision_at)
    except Exception as exc:
        snapshot["v2"] = {**_failure("V2_INPUTS", exc), "trade_date": target,
                          "frames": {}, "proofs": {}}
    try:
        snapshot["v3"] = _prepare_v3(primary_engine, kline_engine, target, decision_at)
    except Exception as exc:
        snapshot["v3"] = {**_failure("V3_INPUTS", exc), "trade_date": target,
                          "stocks": [], "market_features": {}}
    snapshot["input_hash"] = snapshot_input_hash(snapshot)
    return snapshot


def validate_snapshot(snapshot: Mapping[str, Any]) -> None:
    if snapshot.get("schema") != INPUT_SCHEMA:
        raise ValueError("unsupported simulation input schema")
    target = _date(snapshot.get("trade_date"))
    if snapshot.get("input_hash") != snapshot_input_hash(snapshot):
        raise ValueError("simulation input hash differs")
    if snapshot.get("formula_contract") != formula_contract():
        raise ValueError("simulation formula contract differs from installed code/config")
    if snapshot.get("simulation_only") is not True or snapshot.get("real_order_allowed") is not False:
        raise ValueError("simulation input cannot grant real order authority")
    clock = dict(snapshot.get("market_clock") or {})
    if clock.get("expected_trade_date") != target or str(clock.get("current_closed_trade_date") or "") < target:
        raise ValueError("simulation date is not bound to its market clock")
    decision = datetime.fromisoformat(str(snapshot.get("decision_at") or ""))
    if decision.tzinfo is not None or decision.date().isoformat() < target:
        raise ValueError("simulation decision cutoff is invalid")
    mode = snapshot.get("mode")
    if mode not in {"DAILY", "REPLAY"}:
        raise ValueError("simulation mode is invalid")
    if mode == "DAILY" and clock.get("current_closed_trade_date") != target:
        raise ValueError("daily simulation must use the latest closed session")
    if mode == "REPLAY" and decision != datetime.combine(date.fromisoformat(target), time(23, 59, 59)):
        raise ValueError("replay must use the historical knowledge cutoff")
    for family in ("v2", "v3"):
        source = snapshot.get(family)
        if not isinstance(source, dict) or source.get("trade_date") != target:
            raise ValueError(f"{family} source date differs")
        if source.get("status") not in {"READY", "DATA_BLOCKED"}:
            raise ValueError(f"{family} source status is invalid")


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _condition(key: str, label: str, passed: bool, value: Any,
               required: Any) -> dict[str, Any]:
    return {"key": key, "label": label, "status": "PASS" if passed else "BLOCK",
            "value": _plain(value), "required": _plain(required)}


def _row(catalog: dict[str, Any], source: dict[str, Any], target: str,
         selected: list[dict[str, Any]], rejected: list[dict[str, Any]],
         blocked: list[str] | None = None) -> dict[str, Any]:
    blocked = list(dict.fromkeys(blocked or []))
    counts = Counter(item["status"] for item in rejected)
    summary = []
    for status in sorted(counts):
        reasons = list(dict.fromkeys(
            reason for item in rejected if item["status"] == status
            for reason in item["reasons"]
        ))[:5]
        summary.append({"status": status, "count": counts[status], "reasons": reasons})
    return {
        **catalog, "trade_date": target,
        "status": "DATA_BLOCKED" if blocked else "COMPLETED" if selected else "COMPLETED_EMPTY",
        "selected": sorted(selected, key=lambda item: (item["rank_no"], item["stock_code"]))
                    if selected and all("rank_no" in item for item in selected)
                    else sorted(selected, key=lambda item: (-item["score"], item["stock_code"])),
        "candidate_count": len(selected) + len(rejected), "selected_count": len(selected),
        "blocked_reasons": blocked, "rejected_summary": summary, "source": source,
        "simulation_only": True, "real_order_allowed": False,
        "performance": {"status": "NO_FILL_EVIDENCE", "return_pct": None},
    }


def _source_summary(source: dict[str, Any], target: str, family: str) -> dict[str, Any]:
    market = dict(source.get("market_features") or {})
    return {
        "status": source["status"], "reasons": list(source.get("reasons") or []),
        "trade_date": target, "provider": "gj_big_qmt_inner", "family": family,
        "market_eligible_stock_count": market.get("market_eligible_stock_count"),
        "market_latest_coverage_ratio": market.get("market_latest_coverage_ratio"),
        "market_tradable_coverage_ratio": market.get("market_tradable_coverage_ratio"),
        "qmt_attestation_current": market.get("qmt_attestation_current"),
        "optional_formal_factors": (source.get("proofs") or {}).get("optional_formal_factors"),
    }


def _evaluate_v2(snapshot: Mapping[str, Any], catalog: dict[str, dict[str, Any]],
                 keys: tuple[str, ...]) -> list[dict[str, Any]]:
    if not keys:
        return []
    import pandas as pd
    from biz.analysis import sync_analysis_fast as production
    from server.common.pit_facts import PIT_AVAILABLE

    source = snapshot["v2"]
    target = snapshot["trade_date"]
    summary = _source_summary(source, target, "V2")
    if source["status"] != "READY":
        return [_row(catalog[key], summary, target, [], [], source["reasons"]) for key in keys]
    frames = source.get("frames") or {}
    required_frames = {"kline", "finance", "flow", "hot", "notices", "sector",
                       "confidence", "rec_history", "failures"}
    if set(frames) != required_frames:
        raise ValueError("V2 snapshot factor frames differ")
    if source.get("flow_date") != target:
        raise ValueError("V2 capital flow date differs")
    bars = frames["kline"]
    if any(str(item.get("trade_date") or "")[:10] != target for item in bars):
        raise ValueError("V2 stock daily date differs")
    if not bars:
        return [_row(catalog[key], summary, target, [], [], ["QMT_DAILY_UNIVERSE_EMPTY"]) for key in keys]
    window = dict((source.get("proofs") or {}).get("qmt_daily_input_window") or {})
    if not window.get("sessions") or window["sessions"][-1] != target:
        raise ValueError("V2 daily input window is not bound to target date")
    scored = production.compute_scores(
        **{key: pd.DataFrame(rows) if rows else pd.DataFrame({"stock_code": []})
           for key, rows in frames.items()},
        market_mood_score=source["market_mood_score"], flow_date=source["flow_date"],
        trade_date=target, min_score=62.0,
    )
    scored = production.apply_canonical_execution_eligibility(scored)
    manifest, _ = _configs()
    profiles = {item["key"]: item for item in manifest["strategies"]}
    rows = scored.to_dict("records")
    result = []
    for key in keys:
        selected, rejected = [], []
        spec = profiles[key]
        score_field = spec["score_field"]
        threshold = float(spec["parameters"]["confirm_score"])
        for raw in rows:
            plan = production.build_strategy_trade_plan(raw, key)
            score = _number(raw.get(score_field))
            failures = [f"{field}={raw.get(field) or 'MISSING'}"
                        for field in ("finance_pit_status", "event_pit_status", "industry_pit_status")
                        if raw.get(field) != PIT_AVAILABLE]
            for field in (
                "roe_wtd", "gross_margin", "net_margin", "oper_cf_ps",
                "total_rev_yoy_gr", "net_profit_yoy_gr", "non_gaap_net_profit_yoy_gr",
                "net_asset_ps", "asset_liab_ratio", "cash_flow_ratio", "curr_ratio",
                "close", "amount", "turnover_ratio", "pct_5", "dist_ma20",
                "volatility_20", "amount_ratio_5", "amount_ma5", "amount_ma20",
                "ma5", "ma10", "ma20", "ma60", "high_20", "high_60",
                "main_net_inflow", "main_net_inflow_5d", "main_net_inflow_20d",
            ):
                if _number(raw.get(field)) is None:
                    failures.append(f"REQUIRED_FACTOR_MISSING:{field}")
            if str(raw.get("flow_trade_date") or "")[:10] != target:
                failures.append("CAPITAL_FLOW_EXACT_DATE_REQUIRED")
            if raw.get("chase_risk_status") != "ALLOW" or raw.get("ordinary_buy_eligible") != 1:
                failures.append("QMT_CANONICAL_CHASE_GATE_NOT_ALLOW")
            confirmed = plan["signal_status"] in {"CONFIRM", "BUY_READY"}
            if score is None or score < threshold:
                failures.append(f"INDEPENDENT_SCORE_BELOW_CONFIRM:{threshold:g}")
            if not confirmed:
                failures.append(str(plan["signal_reason"]))
            if failures:
                data_blocked = raw.get("chase_risk_status") == "DATA_BLOCKED" or any(
                    "pit_status=" in reason or "REQUIRED_FACTOR_MISSING" in reason for reason in failures)
                rejected.append({"status": "DATA_BLOCKED" if data_blocked else plan["signal_status"],
                                 "reasons": failures})
                continue
            measured = {field: _plain(raw.get(field)) for field in (
                score_field, "technical_score", "capital_score", "sentiment_score", "event_score",
                "fundamental_score", "growth_score", "valuation_score", "risk_score",
                "long_term_score", "final_trade_score", "entry_score", "data_quality_score",
                "main_wave_score", "trend_hold_score", "sector_rotation_score", "amount",
                "main_net_inflow", "main_net_inflow_5d", "main_net_inflow_20d",
                "close", "ma5", "ma10", "ma20", "ma60", "change_pct",
            )}
            conditions = [
                _condition(score_field, "独立策略确认分", True, score, threshold),
                _condition("signal_status", "原策略买点确认", True, plan["signal_status"], "CONFIRM / BUY_READY"),
                _condition("chase_risk_status", "QMT追高与成交能力证据", True, raw["chase_risk_status"], "ALLOW"),
            ] + [_condition(field, "时点数据覆盖", True, raw[field], PIT_AVAILABLE)
                 for field in ("finance_pit_status", "event_pit_status", "industry_pit_status")]
            selected.append({
                "stock_code": str(raw["stock_code"]).zfill(6), "stock_name": str(raw["short_name"]),
                "score": score, "score_scale": 100, "status": plan["signal_status"],
                "reasons": [str(plan["signal_reason"]),
                            f"{spec['name']}独立策略分{score:.1f}，确认门槛{threshold:.1f}"],
                "conditions": conditions, "feature_values": measured,
                "entry_rules": json.loads(plan["entry_conditions_json"]),
                "price": _number(raw.get("close")),
                "entry_price_low": plan["entry_price_low"], "entry_price_high": plan["entry_price_high"],
                "stop_loss_price": plan["stop_loss_price"], "horizon_days": plan["max_holding_days"],
                "selection_kind": "FROZEN_V2_CONFIRMED_SIMULATION_SIGNAL", "simulation_only": True,
            })
        blocking = ["ALL_STOCKS_LACK_REQUIRED_PIT_DATA"] if rejected and all(item["status"] == "DATA_BLOCKED" for item in rejected) else []
        result.append(_row(catalog[key], summary, target, selected, rejected, blocking))
    return result


def _evaluate_v3(snapshot: Mapping[str, Any], catalog: dict[str, dict[str, Any]],
                 keys: tuple[str, ...]) -> list[dict[str, Any]]:
    if not keys:
        return []
    from server.common.pit_facts import PIT_AVAILABLE
    from server.trading_v3.calibration import CalibrationTable
    from server.trading_v3.engine import TradingV3Engine
    from server.trading_v3.regime import classify_regime_probabilities
    from server.trading_v3.shadow_portfolio import build_shadow_portfolio_rows
    from server.trading_v3.sleeves import SLEEVE_BUILDERS

    source = snapshot["v3"]
    target = snapshot["trade_date"]
    summary = _source_summary(source, target, "V3")
    if source["status"] != "READY":
        return [_row(catalog[key], summary, target, [], [], source["reasons"]) for key in keys]
    feature_time = datetime.fromisoformat(source["feature_time"])
    if feature_time.date().isoformat() != target:
        raise ValueError("V3 feature date differs")
    market = source.get("market_features") or {}
    window = dict(market.get("qmt_daily_input_window") or {})
    if not window.get("sessions") or window["sessions"][-1] != target:
        raise ValueError("V3 daily input window is not bound to target date")
    regime = classify_regime_probabilities(market)
    required_market = ("market_eligible_stock_count", "market_latest_coverage_ratio",
                       "market_tradable_coverage_ratio", "qmt_attestation_current")
    missing_market = [f"MARKET_PROOF_MISSING:{key}" for key in required_market if market.get(key) is None]
    if regime.quality_status != "PASS" or missing_market:
        failures = list(regime.evidence) + missing_market
        summary.update(status="DATA_BLOCKED", reasons=failures)
        return [_row(catalog[key], summary, target, [], [], failures) for key in keys]
    _, config = _configs()
    calibration_inputs = source.get("calibration_inputs") or {}
    if calibration_inputs.get("status") != "READY":
        failures = ["ORIGINAL_V3_CALIBRATION_INPUTS_UNPROVEN"]
        summary.update(status="DATA_BLOCKED", reasons=failures)
        return [_row(catalog[key], summary, target, [], [], failures) for key in keys]
    calibrations = {key: CalibrationTable.from_dict(value)
                    for key, value in calibration_inputs.get("calibrations", {}).items()}
    if set(calibrations) - set(V3_KEYS):
        raise ValueError("calibration snapshot contains an excluded strategy")
    engine = TradingV3Engine(calibrations)
    summary["calibration_source"] = {
        "status": "READY", "authority": calibration_inputs.get("authority"),
        "observed_at": calibration_inputs.get("observed_at"),
        "model_versions": {key: table.model_version for key, table in calibrations.items()},
        "rejections": calibration_inputs.get("rejections") or {},
    }
    result = []
    for key in keys:
        selected, rejected, forecasts = [], [], []
        eligible_features: dict[str, dict[str, Any]] = {}
        for original in source.get("stocks") or []:
            features = dict(original)
            themes = features.pop("theme_signal_candidates", ())
            alternatives = [features]
            if key == "theme_diffusion" and themes:
                alternatives = [{**features, **item} for item in themes
                                if isinstance(item, dict) and item.get("theme_feature_key")]
                alternatives = alternatives or [features]
            signals = [SLEEVE_BUILDERS[key](
                str(features["stock_code"]), str(features["stock_name"]), item,
                feature_time, feature_time + timedelta(days=30),
            ) for item in alternatives]
            # Exact production engine theme-candidate winner ordering.
            signal = min(signals, key=lambda item: (
                0 if item.status == "SCORED" else 1, -item.score,
                str(item.features.get("theme_feature_key") or ""),
            ))
            failures = [f"{field}={features.get(field) or 'MISSING'}"
                        for field in ("finance_pit_status", "event_pit_status", "industry_pit_status")
                        if features.get(field) != PIT_AVAILABLE]
            if features.get("entry_eligible") != 1 or features.get("latest_tradable") != 1:
                failures.append("TARGET_DATE_ENTRY_DATA_NOT_ELIGIBLE")
            if failures:
                rejected.append({"status": "DATA_BLOCKED", "reasons": failures})
                continue
            if signal.status in {"INSUFFICIENT_DATA", "DATA_BLOCKED", "FEATURE_QUALITY_BLOCKED"}:
                rejected.append({"status": "DATA_BLOCKED", "reasons": list(signal.reasons)})
                continue
            price = _number(features.get("price"))
            if price is None or price <= 0:
                rejected.append({"status": "DATA_BLOCKED", "reasons": ["EXACT_QMT_PRICE_REQUIRED"]})
                continue
            forecast = engine.forecast(signal)
            forecasts.append(forecast)
            eligible_features[signal.stock_code] = features
        identifiers = {(item.stock_code, key): canonical_hash([target, item.stock_code, key]) for item in forecasts}
        shadow_rows = build_shadow_portfolio_rows(
            forecasts, run_uid=snapshot["input_hash"], trade_date=date.fromisoformat(target),
            forecast_ids=identifiers, policy=dict(config.get("shadow_portfolios") or {}),
        )
        strategy_shadow = {row["stock_code"]: row for row in shadow_rows
                           if row["portfolio_kind"] == "STRATEGY" and row["group_key"] == key}
        for forecast in forecasts:
            shadow = strategy_shadow.get(forecast.stock_code)
            if shadow is None:
                rejected.append({"status": "OUTSIDE_ORIGINAL_SHADOW_TOP_K",
                                 "reasons": ["未进入原策略影子观察组合的排序名额"]})
                continue
            features = eligible_features[forecast.stock_code]
            price = _number(features["price"])
            calibrated = forecast.expected_return_net_pct is not None
            conditions = [
                _condition("shadow_rank", "原策略影子观察组合排序", True, shadow["rank_no"],
                           config.get("shadow_portfolios", {}).get("strategy_top_k", 20)),
                _condition("forecast_status", "原策略预测状态（观察不代表买入）", forecast.status == "VALIDATED_POSITIVE",
                           forecast.status, "VALIDATED_POSITIVE 才有校准正期望证据"),
                _condition("entry_eligible", "目标日入场数据完整", True, features["entry_eligible"], 1),
                _condition("latest_tradable", "目标日可交易日K", True, features["latest_tradable"], 1),
            ] + [_condition(field, "时点数据覆盖", True, features[field], PIT_AVAILABLE)
                 for field in ("finance_pit_status", "event_pit_status", "industry_pit_status")]
            selected.append({
                "stock_code": forecast.stock_code, "stock_name": forecast.stock_name,
                "score": forecast.raw_score, "score_scale": 1, "status": forecast.status,
                "rank_no": shadow["rank_no"], "selection_score": shadow["selection_score"],
                "ranking_basis": "CALIBRATED_EXPECTED_RETURN_NET_PCT" if calibrated else "UNCALIBRATED_RAW_SCORE_RESEARCH_ONLY",
                "expected_return_net_pct": forecast.expected_return_net_pct,
                "model_version": forecast.model_version, "dataset_hash": forecast.dataset_hash,
                "sample_count": forecast.sample_count, "confidence": forecast.confidence,
                "reasons": list(forecast.reasons), "conditions": conditions,
                "feature_values": _plain(forecast.features), "theme_code": forecast.theme_code,
                "price": price, "entry_price_low": None, "entry_price_high": None,
                "stop_loss_price": round(price * (1 + forecast.initial_stop_pct / 100), 2),
                "initial_stop_pct": forecast.initial_stop_pct, "horizon_days": forecast.horizon_days,
                "selection_kind": "ORIGINAL_V3_SHADOW_PORTFOLIO_OBSERVATION", "simulation_only": True,
            })
        blocking = ["ALL_STOCKS_LACK_REQUIRED_PIT_DATA"] if rejected and all(item["status"] == "DATA_BLOCKED" for item in rejected) else []
        if not source.get("stocks"):
            blocking = ["QMT_DAILY_UNIVERSE_EMPTY"]
        result.append(_row(catalog[key], summary, target, selected, rejected, blocking))
    return result


def _evaluate_combinations(catalog: list[dict[str, Any]], rows: list[dict[str, Any]],
                           target: str) -> list[dict[str, Any]]:
    by_key = {row["strategy_key"]: row for row in rows}
    result = []
    for recipe in catalog:
        member_keys = [item["strategy_key"] for item in recipe["members"]]
        if not set(member_keys).issubset(by_key):
            continue
        blocked = [f"{key}: {reason}" for key in member_keys
                   for reason in by_key[key]["blocked_reasons"]]
        source = {"status": "DATA_BLOCKED" if blocked else "READY",
                  "reasons": blocked, "trade_date": target, "provider": "FROZEN_MEMBER_FACTS"}
        selected = []
        if not blocked:
            facts: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
            for member in recipe["members"]:
                for fact in by_key[member["strategy_key"]]["selected"]:
                    facts[fact["stock_code"]].append((member, fact))
            for code, contributions in facts.items():
                first = contributions[0][1]
                selected.append({
                    "stock_code": code, "stock_name": first["stock_name"],
                    "score": round(sum(member["weight"] * fact["score"] / fact["score_scale"]
                                       for member, fact in contributions), 8),
                    "score_scale": 1, "status": "MEMBER_SELECTED",
                    "reasons": [f"{member['name']}（冻结权重{member['weight']:.0%}）：{reason}"
                                for member, fact in contributions for reason in fact["reasons"]],
                    "conditions": [_condition(member["strategy_key"], member["name"], True,
                                               fact["status"], "原成员策略已选出")
                                   for member, fact in contributions],
                    "feature_values": {member["strategy_key"]: fact["feature_values"]
                                       for member, fact in contributions},
                    "member_contributions": [{"strategy_key": member["strategy_key"],
                                              "weight": member["weight"], "score": fact["score"],
                                              "score_scale": fact["score_scale"]}
                                             for member, fact in contributions],
                    "price": first["price"], "entry_price_low": None, "entry_price_high": None,
                    "stop_loss_price": None, "horizon_days": None,
                    "selection_kind": "FROZEN_MEMBER_UNION", "simulation_only": True,
                })
        result.append(_row(recipe, source, target, selected, [], blocked))
    return result


def evaluate_snapshot(snapshot: Mapping[str, Any],
                      strategy_keys: list[str] | tuple[str, ...] | None = None) -> dict[str, Any]:
    """Deterministically run installed original formulas without database reads."""
    validate_snapshot(snapshot)
    requested = tuple(STRATEGY_KEYS if strategy_keys is None else strategy_keys)
    if len(set(requested)) != len(requested) or set(requested) - set(STRATEGY_KEYS):
        raise ValueError("unknown, excluded, or duplicate simulation strategy")
    catalog = strategy_catalog()
    by_key = {item["strategy_key"]: item for item in catalog["strategies"]}
    rows = _evaluate_v2(snapshot, by_key, tuple(key for key in V2_KEYS if key in requested))
    rows += _evaluate_v3(snapshot, by_key, tuple(key for key in V3_KEYS if key in requested))
    target = snapshot["trade_date"]
    combinations = _evaluate_combinations(catalog["combinations"], rows, target)
    states = [row["status"] for row in rows + combinations]
    status = "DATA_BLOCKED" if states and all(state == "DATA_BLOCKED" for state in states) else "PARTIAL_DATA_BLOCKED" if "DATA_BLOCKED" in states else "COMPLETED"
    return {
        "schema": RESULT_SCHEMA, "trade_date": target, "mode": snapshot["mode"],
        "status": status,
        "input_hash": snapshot["input_hash"], "formula_contract": snapshot["formula_contract"],
        "strategy_rows": rows, "combination_rows": combinations,
        "excluded": catalog["excluded"],
        "input_status": {family: next(
            (row["source"] for row in rows if row["family"] == family.upper()),
            _source_summary(snapshot[family], target, family.upper()),
        ) for family in ("v2", "v3")},
        "simulation_only": True, "research_only": True, "real_order_allowed": False,
        "summary": {"strategy_count": len(rows), "combination_count": len(combinations),
                    "selected_stock_count": len({item["stock_code"] for row in rows for item in row["selected"]}),
                    "selected_signal_count": sum(row["selected_count"] for row in rows),
                    "data_blocked_count": states.count("DATA_BLOCKED")},
        "performance": {"status": "NO_FILL_EVIDENCE", "return_pct": None},
    }
