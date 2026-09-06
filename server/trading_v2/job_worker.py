"""Single-owner asynchronous worker for V2 decision and research jobs."""
from __future__ import annotations

import json
import math
import os
import socket
import uuid
from datetime import date, datetime, timedelta
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from .config import canonical_json_hash
from .decision_worker import run_daily_decision
from .versioning import code_version


WORKER_NAME = "trading-v2-job-worker"
RESEARCH_PROTOCOL_VERSION = "v2_research_protocol_20260725_5"
ETF_RESEARCH_DATA_START = "2019-01-01"
ETF_RESEARCH_MINIMUM_START = "2021-01-04"
ETF_UNIVERSE_CUTOFF = "2020-12-31"
ETF_RESEARCH_DATA_SOURCE = "gj_big_qmt_inner"
ETF_RESEARCH_CALENDAR_CODE = "510300"
ETF_RESEARCH_FEE_ACCOUNT_ID = "paper-main-v2"
ETF_MUTABLE_INPUT_BLOCKERS = (
    "ETF_PIT_CLASSIFICATION_LEDGER_UNAVAILABLE",
    "ETF_RAW_BAR_REVISION_LEDGER_UNAVAILABLE",
)


def _json(payload: Any) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _heartbeat(
    engine: Engine,
    *,
    status: str,
    current_job_id: str | None = None,
    success: bool = False,
    error_code: str | None = None,
    error_message: str | None = None,
) -> None:
    now = datetime.now()
    instance = f"{socket.gethostname()}:{os.getpid()}"
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO st_worker_heartbeat_v2
                (worker_name, worker_instance, status, current_job_id,
                 last_success_at, last_error_code, last_error_message,
                 heartbeat_at, updated_at)
                VALUES
                (:worker_name, :instance, :status, :job_id,
                 :success_at, :error_code, :error_message, :now, :now)
                ON DUPLICATE KEY UPDATE
                    worker_instance = VALUES(worker_instance),
                    status = VALUES(status),
                    current_job_id = VALUES(current_job_id),
                    last_success_at = CASE
                        WHEN VALUES(last_success_at) IS NOT NULL
                        THEN VALUES(last_success_at)
                        ELSE last_success_at END,
                    last_error_code = VALUES(last_error_code),
                    last_error_message = VALUES(last_error_message),
                    heartbeat_at = VALUES(heartbeat_at),
                    updated_at = VALUES(updated_at)
                """
            ),
            {
                "worker_name": WORKER_NAME,
                "instance": instance,
                "status": status,
                "job_id": current_job_id,
                "success_at": now if success else None,
                "error_code": error_code,
                "error_message": (error_message or "")[:500] or None,
                "now": now,
            },
        )


def _claim_job(engine: Engine) -> dict[str, Any] | None:
    with engine.begin() as connection:
        lock = connection.execute(
            text("SELECT GET_LOCK('probiga:trading_v2_job_worker', 0)")
        ).scalar()
        if int(lock or 0) != 1:
            return None
        try:
            row = connection.execute(
                text(
                    """
                    SELECT * FROM st_job_v2
                    WHERE status = 'PENDING'
                    ORDER BY requested_at, job_id
                    LIMIT 1 FOR UPDATE
                    """
                )
            ).mappings().first()
            if not row:
                return None
            connection.execute(
                text(
                    """
                    UPDATE st_job_v2
                    SET status = 'RUNNING', started_at = :now,
                        error_code = NULL, error_message = NULL
                    WHERE job_id = :job_id AND status = 'PENDING'
                    """
                ),
                {"now": datetime.now(), "job_id": row["job_id"]},
            )
            return dict(row)
        finally:
            connection.execute(
                text("SELECT RELEASE_LOCK('probiga:trading_v2_job_worker')")
            )


def research_backtest_adapter(
    *,
    strategy_id: str,
    strategy_version: str,
    instrument_scope: str,
) -> dict[str, Any]:
    """Return the existing reproducible adapter for one registered version.

    A strategy registration alone does not mean that the generic screener or
    ETF replay can reproduce it. Keep that distinction explicit so an
    unsupported strategy can never inherit another strategy's report.
    """

    strategy_id = str(strategy_id or "")
    strategy_version = str(strategy_version or "")
    instrument_scope = str(instrument_scope or "").upper()
    if instrument_scope == "EXCHANGE_TRADED_FUND":
        supported = (
            strategy_id == "etf_trend_risk"
            and strategy_version == "etf_trend_risk_v2.0.0"
        )
        return {
            "supported": supported,
            "status": "AVAILABLE" if supported else "UNAVAILABLE",
            "adapter": "etf_trade_level_replay_v2" if supported else None,
            "minimum_start_date": (
                ETF_RESEARCH_MINIMUM_START if supported else None
            ),
            "reason": (
                "已绑定 ETF 成交级回放"
                if supported
                else "该 ETF 策略没有可复算回测适配器"
            ),
        }

    return {
        "supported": False,
        "status": "UNAVAILABLE",
        "adapter": None,
        "reason": "当前登记股票策略暂无可复算历史回测适配器",
    }


def registered_etf_universe_contract(
    strategy: dict[str, Any],
) -> dict[str, Any]:
    """Validate and freeze the ETF universe carried by an exact registration."""

    manifest = strategy.get("manifest")
    if not isinstance(manifest, dict):
        try:
            manifest = json.loads(str(strategy.get("manifest_json") or ""))
        except json.JSONDecodeError as exc:
            raise ValueError("registered ETF strategy manifest is invalid") from exc
    if not isinstance(manifest, dict):
        raise ValueError("registered ETF strategy manifest is unavailable")
    expected_identity = {
        "strategy_id": str(strategy.get("strategy_id") or ""),
        "strategy_version": str(
            strategy.get("version") or strategy.get("strategy_version") or ""
        ),
        "instrument_scope": str(strategy.get("instrument_scope") or ""),
    }
    if any(
        str(manifest.get(key) or "") != value
        for key, value in expected_identity.items()
    ):
        raise ValueError("registered ETF strategy manifest identity mismatch")
    registered_config_hash = str(strategy.get("config_hash") or "")
    semantic_config_hash = canonical_json_hash({
        key: value
        for key, value in manifest.items()
        if key not in {"code_commit_sha", "config_hash"}
    })
    if registered_config_hash and registered_config_hash != semantic_config_hash:
        raise ValueError("registered ETF strategy manifest config hash mismatch")
    universe = manifest.get("universe")
    if not isinstance(universe, dict):
        universe = manifest.get("universe_definition")
    if not isinstance(universe, dict):
        raise ValueError("registered ETF strategy universe is unavailable")
    raw_codes = universe.get("eligible_codes")
    if not isinstance(raw_codes, list) or not raw_codes:
        raise ValueError("registered ETF eligible universe is empty")
    codes = [str(code).strip() for code in raw_codes]
    if (
        len(codes) != len(set(codes))
        or any(len(code) != 6 or not code.isdecimal() for code in codes)
    ):
        raise ValueError("registered ETF eligible universe is invalid")
    return {
        "eligible_codes": tuple(sorted(codes)),
        "universe": universe,
        "universe_hash": canonical_json_hash(universe),
    }


def _require_registered_etf_universe(
    universe_audit: Any,
    eligible_codes: tuple[str, ...],
) -> list[str]:
    derived = sorted(
        str(code)
        for code in universe_audit.loc[
            universe_audit["eligible"], "etf_code"
        ].tolist()
    )
    expected = sorted(eligible_codes)
    if derived != expected:
        missing = sorted(set(expected) - set(derived))
        unexpected = sorted(set(derived) - set(expected))
        raise RuntimeError(
            "registered ETF universe freeze evidence mismatch: "
            f"missing={','.join(missing) or '-'}; "
            f"unexpected={','.join(unexpected) or '-'}"
        )
    return derived


def _require_expected_registration_binding(
    request: dict[str, Any],
    *,
    config_hash: str,
    universe_hash: str,
) -> None:
    expected_config_hash = str(request.get("expected_config_hash") or "")
    expected_universe_hash = str(request.get("expected_universe_hash") or "")
    if expected_config_hash and expected_config_hash != config_hash:
        raise ValueError("registered strategy config changed after API preflight")
    if expected_universe_hash and expected_universe_hash != universe_hash:
        raise ValueError("registered ETF universe changed after API preflight")


def _etf_dependency_start(start_date: str) -> str:
    """Return the frozen adapter's declared history window.

    Recent evaluation windows still need the same pre-cutoff observations used
    to freeze the ETF universe and calculate the first signals.  Loading only
    ``start_date - 550 days`` would put a 2025 request entirely after the 2020
    cutoff and make every product ineligible.
    """

    parsed_start = datetime.fromisoformat(str(start_date)).date().isoformat()
    if parsed_start < ETF_RESEARCH_MINIMUM_START:
        raise ValueError(
            "ETF formal backtest start_date must be on or after "
            f"{ETF_RESEARCH_MINIMUM_START}"
        )
    return ETF_RESEARCH_DATA_START


def _resolved_execution_inputs(
    request: dict[str, Any],
    *,
    instrument_scope: str,
) -> tuple[float, float]:
    is_etf = str(instrument_scope).upper() == "EXCHANGE_TRADED_FUND"
    capital_raw = request.get("initial_capital")
    if "round_trip_cost" in request:
        raise ValueError(
            "formal backtest trading costs are fixed to the confirmed fee profile"
        )
    normalized_multiplier = request.get("cost_scenario_multiplier", 1.0)
    try:
        normalized_multiplier = float(normalized_multiplier)
    except (TypeError, ValueError) as exc:
        raise ValueError("cost_scenario_multiplier must equal 1.0") from exc
    if not math.isfinite(normalized_multiplier) or normalized_multiplier != 1.0:
        raise ValueError("cost_scenario_multiplier must equal 1.0")
    initial_capital = float(
        (200_000.0 if is_etf else 1_000_000.0)
        if capital_raw is None
        else capital_raw
    )
    if not math.isfinite(initial_capital) or initial_capital <= 0:
        raise ValueError("initial_capital must be finite and positive")
    return initial_capital, 1.0


def _confirmed_etf_fee_coverage(connection: Any) -> dict[str, Any]:
    """Resolve the one validated ETF fee period bound to the paper account."""

    rows = connection.execute(
        text(
            """
            SELECT f.fee_profile_version, f.effective_from, f.effective_to,
                   f.security_type, f.buy_commission_rate,
                   f.sell_commission_rate, f.minimum_commission,
                   f.stamp_tax_sell_rate, f.transfer_fee_buy_rate,
                   f.transfer_fee_sell_rate, f.other_fee_json,
                   f.evidence_hash, f.confirmation_status
            FROM st_trade_account_v2 a
            JOIN st_fee_profile_v2 f
              ON f.fee_profile_version = a.fee_profile_version
            WHERE a.account_id = :account_id
              AND f.security_type = 'ETF'
              AND f.confirmation_status = 'CONFIRMED'
            ORDER BY f.effective_from DESC
            """
        ),
        {
            "account_id": ETF_RESEARCH_FEE_ACCOUNT_ID,
        },
    ).mappings().all()
    if len(rows) != 1:
        return {
            "status": "MISSING" if not rows else "AMBIGUOUS",
            "account_id": ETF_RESEARCH_FEE_ACCOUNT_ID,
            "profile_count": len(rows),
            "usable": False,
        }
    row = dict(rows[0])
    evidence_hash = str(row.get("evidence_hash") or "").lower()
    try:
        effective_from = date.fromisoformat(
            str(row.get("effective_from") or "")[:10]
        ).isoformat()
        effective_to = (
            date.fromisoformat(str(row["effective_to"])[:10]).isoformat()
            if row.get("effective_to")
            else None
        )
        if effective_to and effective_to < effective_from:
            raise ValueError("effective_to precedes effective_from")
    except (TypeError, ValueError):
        return {
            "status": "INVALID",
            "account_id": ETF_RESEARCH_FEE_ACCOUNT_ID,
            "profile_count": 1,
            "usable": False,
        }
    try:
        other_fees = json.loads(str(row.get("other_fee_json") or "{}"))
    except json.JSONDecodeError:
        return {
            "status": "INVALID",
            "account_id": ETF_RESEARCH_FEE_ACCOUNT_ID,
            "profile_count": 1,
            "usable": False,
        }
    numeric_fields = (
        "buy_commission_rate",
        "sell_commission_rate",
        "minimum_commission",
        "stamp_tax_sell_rate",
        "transfer_fee_buy_rate",
        "transfer_fee_sell_rate",
    )
    numeric: dict[str, float] = {}
    try:
        for field in numeric_fields:
            value = float(row[field])
            if not math.isfinite(value) or value < 0:
                raise ValueError(field)
            numeric[field] = value
    except (KeyError, TypeError, ValueError):
        return {
            "status": "INVALID",
            "account_id": ETF_RESEARCH_FEE_ACCOUNT_ID,
            "profile_count": 1,
            "usable": False,
        }
    if (
        len(evidence_hash) != 64
        or any(character not in "0123456789abcdef" for character in evidence_hash)
        or other_fees != {}
    ):
        return {
            "status": "UNSUPPORTED_EVIDENCE",
            "account_id": ETF_RESEARCH_FEE_ACCOUNT_ID,
            "profile_count": 1,
            "usable": False,
        }
    return {
        "status": "CONFIRMED",
        "usable": True,
        "account_id": ETF_RESEARCH_FEE_ACCOUNT_ID,
        "fee_profile_version": str(row["fee_profile_version"]),
        "effective_from": effective_from,
        "effective_to": effective_to,
        "security_type": "ETF",
        "evidence_hash": evidence_hash,
        **numeric,
    }


def _confirmed_etf_fee_profile(
    connection: Any,
    *,
    start_date: str,
    end_date: str,
) -> dict[str, Any]:
    """Require the confirmed account fee period to cover the whole replay."""

    profile = _confirmed_etf_fee_coverage(connection)
    if not profile["usable"]:
        return profile
    effective_from = str(profile["effective_from"])[:10]
    effective_to = str(profile.get("effective_to") or "")[:10]
    if start_date < effective_from or (effective_to and end_date > effective_to):
        return {
            **profile,
            "status": "OUTSIDE_COVERAGE",
            "usable": False,
        }
    return profile


def _etf_research_truth_contract(
    snapshot_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Quarantine mutable ETF reference inputs from promotion authority."""

    price_rows: list[dict[str, Any]] = []
    classifications: dict[str, dict[str, Any]] = {}
    for raw in snapshot_rows:
        row = dict(raw)
        code = str(row.get("etf_code") or "")
        data_source = str(row.get("data_source") or "")
        if data_source != ETF_RESEARCH_DATA_SOURCE:
            raise RuntimeError(
                "ETF research row is not from the frozen data source"
            )
        data_version = str(row.get("data_version") or "")
        if not code or not data_version:
            raise RuntimeError("ETF native snapshot row is unversioned")
        raw_adjust_type = row.get("adjust_type")
        if raw_adjust_type is None or int(raw_adjust_type) != 0:
            raise RuntimeError("ETF research must use native adjust_type=0 rows")
        received_at = str(row.get("received_at") or "")
        if not received_at:
            raise RuntimeError("ETF native snapshot row lacks received_at")
        numeric_values: dict[str, float] = {}
        for field in ("open", "close", "pre_close", "amount"):
            try:
                value = float(row.get(field))
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"ETF native snapshot row has invalid {field}"
                ) from exc
            if not math.isfinite(value) or (
                field != "amount" and value <= 0
            ) or (field == "amount" and value < 0):
                raise RuntimeError(
                    f"ETF native snapshot row has invalid {field}"
                )
            numeric_values[field] = value
        asset_class = str(row.get("asset_class") or "")
        classification_updated_at = str(
            row.get("classification_updated_at") or ""
        )
        if not asset_class or not classification_updated_at:
            raise RuntimeError(
                "ETF current classification snapshot lacks provenance"
            )
        price_rows.append({
            "etf_code": code,
            "trade_date": str(row.get("trade_date") or ""),
            "data_source": data_source,
            "adjust_type": 0,
            "data_version": data_version,
            "validation_status": str(row.get("validation_status") or ""),
            "quality_status": str(row.get("quality_status") or ""),
            "received_at": received_at,
            **numeric_values,
        })
        classification = {
            "etf_code": code,
            "asset_class": asset_class,
            "classification_updated_at": classification_updated_at,
        }
        previous = classifications.setdefault(code, classification)
        if previous != classification:
            raise RuntimeError(
                "ETF current classification changed within one snapshot"
            )
    price_rows.sort(key=lambda item: (item["trade_date"], item["etf_code"]))
    classification_rows = [
        classifications[key] for key in sorted(classifications)
    ]
    price_hash = canonical_json_hash(price_rows)
    classification_hash = canonical_json_hash(classification_rows)
    payload = {
        "schema": "probiga.etf-research-input-truth.v1",
        "status": "MUTABLE_INPUTS_QUARANTINED_RESEARCH_ONLY",
        "native_unadjusted_prices_only": True,
        "adjusted_history_rows_consumed": False,
        "derived_price_protocol": (
            "NATIVE_UNADJUSTED_CLOSE_PRE_CLOSE_RETURN_CHAIN_AND_OPEN_RATIO_V1"
        ),
        "native_price_snapshot_hash": price_hash,
        "native_price_row_count": len(price_rows),
        "native_price_revision_ledger_available": False,
        "current_classification_snapshot_hash": classification_hash,
        "current_classification_count": len(classification_rows),
        "historical_classification_verified": False,
        "current_classification_can_authorize_promotion": False,
        "activation_eligible": False,
        "promotion_blockers": list(ETF_MUTABLE_INPUT_BLOCKERS),
    }
    return {**payload, "contract_hash": canonical_json_hash(payload)}


def _require_complete_etf_end(
    calendar: Any,
    *,
    start_date: str,
    end_date: str,
) -> str:
    if len(calendar) == 0:
        raise RuntimeError("ETF validated source has no trading sessions")
    actual_end = max(calendar).date().isoformat()
    if actual_end < start_date:
        raise RuntimeError(
            "ETF validated source has no complete session in requested period"
        )
    if actual_end < end_date:
        raise ValueError(
            "end_date exceeds the latest validated ETF session "
            f"({actual_end}); choose that session or an earlier date"
        )
    return end_date


def _require_registered_etf_end_session(
    data: Any,
    *,
    end_date: str,
    eligible_codes: tuple[str, ...],
) -> str:
    """Require the requested end to be a valid session for every registered ETF."""

    requested = datetime.fromisoformat(end_date).date()
    common_dates: set[date] | None = None
    for code in eligible_codes:
        if code not in data.close:
            valid_dates: set[date] = set()
        else:
            valid_dates = {
                item.date()
                for item, value in data.close[code].items()
                if value is not None
                and math.isfinite(float(value))
                and float(value) > 0
            }
        common_dates = (
            valid_dates
            if common_dates is None
            else common_dates & valid_dates
        )
    common_dates = common_dates or set()
    if requested in common_dates:
        return end_date
    previous = max(
        (item for item in common_dates if item < requested),
        default=None,
    )
    raise ValueError(
        "end_date must be a complete validated ETF trading session; "
        "nearest previous session is "
        f"{previous.isoformat() if previous else 'none'}"
    )


def _formal_etf_expected_sessions(
    connection: Any,
    *,
    start_date: str,
    end_date: str,
) -> list[str]:
    rows = connection.execute(
        text(
            """
            SELECT trade_date
            FROM si_trade_calendar
            WHERE trade_status = 1
              AND trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date
            """
        ),
        {"start_date": start_date, "end_date": end_date},
    ).mappings().all()
    sessions = sorted({
        str(row.get("trade_date") or "")[:10]
        for row in rows
        if row.get("trade_date")
    })
    if not sessions:
        raise RuntimeError("formal ETF trade calendar has no requested sessions")
    if end_date not in sessions:
        previous = max(
            (session for session in sessions if session < end_date),
            default="none",
        )
        raise ValueError(
            "end_date must be a formal ETF trading session; "
            f"nearest previous session is {previous}"
        )
    return sessions


def _native_etf_session_dates(
    snapshot_rows: list[dict[str, Any]],
) -> dict[str, set[str]]:
    sessions: dict[str, set[str]] = {}
    for row in snapshot_rows:
        try:
            close = float(row["close"])
            pre_close = float(row["pre_close"])
        except (KeyError, TypeError, ValueError):
            continue
        if (
            not math.isfinite(close)
            or close <= 0
            or not math.isfinite(pre_close)
            or pre_close <= 0
        ):
            continue
        code = str(row.get("etf_code") or "")
        session = str(row.get("trade_date") or "")[:10]
        if code and session:
            sessions.setdefault(code, set()).add(session)
    return sessions


def _registered_etf_expected_session_contract(
    snapshot_rows: list[dict[str, Any]],
    *,
    eligible_codes: tuple[str, ...],
    formal_sessions: list[str],
    dependency_start: str,
    end_date: str,
) -> tuple[dict[str, set[str]], dict[str, Any]]:
    instruments: dict[str, dict[str, str | None]] = {}
    for row in snapshot_rows:
        code = str(row.get("etf_code") or "")
        if code not in eligible_codes:
            continue
        contract = {
            "list_date": str(row.get("list_date") or "")[:10] or None,
            "last_trade_date": (
                str(row.get("last_trade_date") or "")[:10] or None
            ),
            "status": str(row.get("instrument_status") or "") or None,
        }
        previous = instruments.setdefault(code, contract)
        if previous != contract:
            raise RuntimeError(
                f"registered ETF instrument contract changed in snapshot: {code}"
            )

    sessions_by_code: dict[str, set[str]] = {}
    evidence: dict[str, dict[str, Any]] = {}
    for code in eligible_codes:
        contract = instruments.get(code)
        if not contract or not contract.get("list_date") or not contract.get("status"):
            raise RuntimeError(
                f"registered ETF listing/status contract unavailable: {code}"
            )
        list_date = str(contract["list_date"])
        if list_date > end_date:
            raise RuntimeError(
                f"registered ETF was not listed in the replay window: {code}"
            )
        coverage_start = max(dependency_start, list_date)
        code_sessions = {
            session
            for session in formal_sessions
            if coverage_start <= session <= end_date
        }
        if not code_sessions:
            raise RuntimeError(
                f"registered ETF has no formal sessions in replay window: {code}"
            )
        sessions_by_code[code] = code_sessions
        evidence[code] = {
            **contract,
            "coverage_start_date": coverage_start,
            "coverage_end_date": end_date,
            "expected_session_count": len(code_sessions),
            "expected_session_hash": canonical_json_hash(sorted(code_sessions)),
        }
    payload = {
        "schema": "probiga.etf-registered-session-contract.v1",
        "dependency_start": dependency_start,
        "end_date": end_date,
        "instruments": evidence,
    }
    return sessions_by_code, {
        **payload,
        "contract_hash": canonical_json_hash(payload),
    }


def registered_etf_dependency_data_contract(
    connection: Any,
    *,
    eligible_codes: tuple[str, ...],
    end_date: str,
    dependency_start: str = ETF_RESEARCH_DATA_START,
    snapshot_rows: list[dict[str, Any]] | None = None,
    backtest_start: str | None = None,
) -> dict[str, Any]:
    """Validate the full native warmup window used by the formal ETF replay."""

    if not eligible_codes:
        raise ValueError("registered ETF eligible universe is empty")
    formal_sessions = _formal_etf_expected_sessions(
        connection,
        start_date=dependency_start,
        end_date=end_date,
    )
    rows = snapshot_rows
    if rows is None:
        rows = [
            dict(row)
            for row in connection.execute(
                text(
                    """
                    SELECT k.etf_code, k.short_name, k.trade_date,
                           k.adjust_type, k.data_source, k.data_version,
                           k.received_at, k.open, k.close, k.pre_close,
                           k.amount, k.validation_status, k.quality_status,
                           c.asset_class,
                           c.list_date, c.last_trade_date,
                           c.status AS instrument_status,
                           c.updated_at AS classification_updated_at
                    FROM sm_etf_kline k
                    JOIN si_etf_code c ON c.etf_code = k.etf_code
                    WHERE k.adjust_type = 0
                      AND k.k_type = 1
                      AND k.validation_status = 'passed'
                      AND k.quality_status = 'validated'
                      AND k.data_source = :data_source
                      AND k.etf_code IN :eligible_codes
                      AND k.trade_date BETWEEN :start_date AND :end_date
                    ORDER BY k.trade_date, k.etf_code
                    """
                ).bindparams(bindparam("eligible_codes", expanding=True)),
                {
                    "start_date": dependency_start,
                    "end_date": end_date,
                    "data_source": ETF_RESEARCH_DATA_SOURCE,
                    "eligible_codes": list(eligible_codes),
                },
            ).mappings().all()
        ]
    from tools.backtest_etf_ensemble import (
        build_target_schedule,
        market_data_from_rows,
    )
    from tools.backtest_etf_robust import freeze_universe

    research_truth = _etf_research_truth_contract(rows)
    native_session_dates = _native_etf_session_dates(rows)
    (
        expected_sessions_by_code,
        registered_session_contract,
    ) = _registered_etf_expected_session_contract(
        rows,
        eligible_codes=eligible_codes,
        formal_sessions=formal_sessions,
        dependency_start=dependency_start,
        end_date=end_date,
    )
    per_code: dict[str, dict[str, Any]] = {}
    gaps: list[dict[str, Any]] = []
    for code in eligible_codes:
        expected = expected_sessions_by_code[code]
        covered = native_session_dates.get(code, set())
        missing = sorted(expected - covered)
        row = {
            "expected_session_count": len(expected),
            "covered_session_count": len(expected & covered),
            "missing_native_close_pre_close_dates": missing,
        }
        per_code[code] = row
        if missing:
            gaps.append({"etf_code": code, **row})
    if gaps:
        details = []
        for row in gaps:
            missing = row["missing_native_close_pre_close_dates"]
            sample = ",".join(missing[:5])
            remainder = len(missing) - min(5, len(missing))
            details.append(
                f"{row['etf_code']}[{sample}"
                + (f"(+{remainder})" if remainder else "")
                + "]"
            )
        raise RuntimeError(
            "registered ETF native dependency coverage incomplete: "
            + "; ".join(details)
        )
    source_data = market_data_from_rows(rows)
    effective_end = _require_complete_etf_end(
        source_data.calendar,
        start_date=backtest_start or dependency_start,
        end_date=end_date,
    )
    _require_registered_etf_end_session(
        source_data,
        end_date=effective_end,
        eligible_codes=eligible_codes,
    )
    data, universe_audit = freeze_universe(
        source_data,
        cutoff_date=ETF_UNIVERSE_CUTOFF,
    )
    derived_eligible_codes = _require_registered_etf_universe(
        universe_audit,
        eligible_codes,
    )
    universe_audit_records = universe_audit.to_dict(orient="records")
    monthly_targets: dict[Any, Any] | None = None
    target_records: list[dict[str, Any]] | None = None
    schedule_viability: dict[str, Any] | None = None
    if backtest_start:
        monthly_targets, target_records = build_target_schedule(
            data,
            backtest_start=backtest_start,
            end_date=effective_end,
            mode="trend_risk",
            execution_lag=1,
        )
        execution_dates = sorted(monthly_targets)
        schedule_viability = {
            "backtest_start": backtest_start,
            "end_date": effective_end,
            "execution_count": len(execution_dates),
            "first_execution_date": str(execution_dates[0].date()),
            "last_execution_date": str(execution_dates[-1].date()),
        }
    payload = {
        "schema": "probiga.etf-dependency-data-contract.v1",
        "dependency_start": dependency_start,
        "end_date": end_date,
        "formal_sessions": formal_sessions,
        "expected_sessions_by_code": {
            code: sorted(sessions)
            for code, sessions in expected_sessions_by_code.items()
        },
        "registered_session_contract": registered_session_contract,
        "native_coverage": per_code,
        "research_truth_contract_hash": research_truth["contract_hash"],
        "derived_eligible_codes": derived_eligible_codes,
        "universe_audit_hash": canonical_json_hash(universe_audit_records),
        "schedule_viability": schedule_viability,
    }
    return {
        **payload,
        "contract_hash": canonical_json_hash(payload),
        "expected_sessions_by_code": expected_sessions_by_code,
        "native_session_dates": native_session_dates,
        "research_truth": research_truth,
        "source_data": source_data,
        "data": data,
        "universe_audit": universe_audit,
        "derived_eligible_codes": derived_eligible_codes,
        "effective_end": effective_end,
        "monthly_targets": monthly_targets,
        "target_records": target_records,
    }


def _target_data_coverage_audit(
    data: Any,
    targets: dict[Any, Any],
    *,
    end_date: str,
    target_codes: list[str] | None = None,
    expected_sessions: list[str] | None = None,
    expected_sessions_by_code: dict[str, set[str]] | None = None,
    native_session_dates: dict[str, set[str]] | None = None,
) -> dict[str, Any]:
    """Audit prices for every ETF that entered the formal target schedule."""

    discovered_codes = {
        str(code)
        for weights in targets.values()
        for code, weight in weights.items()
        if math.isfinite(float(weight)) and float(weight) > 1e-8
    }
    codes = sorted(set(target_codes or discovered_codes))
    expected_session_set = set(expected_sessions or [])
    expected_sessions_by_code = expected_sessions_by_code or {}
    expected_contract = (
        {
            code: sorted(expected_sessions_by_code.get(code, set()))
            for code in codes
        }
        if expected_sessions_by_code
        else sorted(expected_session_set)
    )
    expected_session_hash = (
        canonical_json_hash(expected_contract)
        if expected_session_set or expected_sessions_by_code
        else None
    )
    native_session_dates = native_session_dates or {}
    required_open_dates: dict[str, set[Any]] = {code: set() for code in codes}
    previous_active: set[str] = set()
    for execution_date, weights in sorted(targets.items()):
        active = {
            str(code)
            for code, weight in weights.items()
            if str(code) in codes
            and math.isfinite(float(weight))
            and float(weight) > 1e-8
        }
        for code in previous_active | active:
            required_open_dates[code].add(execution_date)
        previous_active = active

    per_code: dict[str, dict[str, Any]] = {}
    gaps: list[dict[str, Any]] = []
    for code in codes:
        close_dates: list[Any] = []
        open_dates: list[Any] = []
        if code in data.close:
            close_dates = [
                day
                for day, value in data.close[code].items()
                if value is not None
                and math.isfinite(float(value))
                and float(value) > 0
            ]
        if code in data.open:
            open_dates = [
                day
                for day, value in data.open[code].items()
                if value is not None
                and math.isfinite(float(value))
                and float(value) > 0
            ]
        close_cutoff = (
            max(close_dates).date().isoformat() if close_dates else None
        )
        open_cutoff = max(open_dates).date().isoformat() if open_dates else None
        valid_open_dates = set(open_dates)
        missing_open_dates = sorted(
            str(day.date())
            for day in required_open_dates[code]
            if day not in valid_open_dates
        )
        code_expected_sessions = expected_sessions_by_code.get(
            code,
            expected_session_set,
        )
        available_native_sessions = native_session_dates.get(code, set())
        missing_native_sessions = sorted(
            code_expected_sessions - available_native_sessions
        )
        reasons: list[str] = []
        if close_cutoff is None or close_cutoff < end_date:
            reasons.append("CLOSE_CUTOFF_BEFORE_END")
        if missing_open_dates:
            reasons.append("SCHEDULE_OPEN_MISSING")
        if missing_native_sessions:
            reasons.append("NATIVE_CLOSE_PRE_CLOSE_MISSING")
        row = {
            "close_cutoff": close_cutoff,
            "open_cutoff": open_cutoff,
            "required_end_date": end_date,
            "required_execution_open_dates": sorted(
                str(day.date()) for day in required_open_dates[code]
            ),
            "missing_execution_open_dates": missing_open_dates,
            "expected_native_session_count": len(code_expected_sessions),
            "covered_native_session_count": len(
                code_expected_sessions & available_native_sessions
            ),
            "missing_native_close_pre_close_dates": missing_native_sessions,
            "gaps": reasons,
        }
        per_code[code] = row
        if reasons:
            gaps.append({"etf_code": code, **row})
    return {
        "schema": "probiga.etf-target-data-coverage.v2",
        "complete": not gaps,
        "required_end_date": end_date,
        "expected_session_start": min(
            (
                session
                for sessions in (
                    expected_sessions_by_code.values()
                    if expected_sessions_by_code
                    else (expected_session_set,)
                )
                for session in sessions
            ),
            default=None,
        ),
        "expected_session_count": (
            sum(len(sessions) for sessions in expected_sessions_by_code.values())
            if expected_sessions_by_code
            else len(expected_session_set)
        ),
        "expected_session_hash": expected_session_hash,
        "target_codes": codes,
        "per_code": per_code,
        "gaps": gaps,
    }


def _require_target_data_coverage(audit: dict[str, Any]) -> None:
    if audit.get("complete") is True:
        return
    details: list[str] = []
    for row in audit.get("gaps") or []:
        code = str(row.get("etf_code") or "UNKNOWN")
        reasons: list[str] = []
        if "CLOSE_CUTOFF_BEFORE_END" in (row.get("gaps") or []):
            reasons.append(
                "close_cutoff="
                f"{row.get('close_cutoff') or 'NONE'}<"
                f"{row.get('required_end_date')}"
            )
        missing_open = row.get("missing_execution_open_dates") or []
        if missing_open:
            reasons.append("missing_open=" + ",".join(map(str, missing_open)))
        missing_native = row.get("missing_native_close_pre_close_dates") or []
        if missing_native:
            sample = ",".join(map(str, missing_native[:5]))
            remainder = len(missing_native) - min(5, len(missing_native))
            reasons.append(
                "missing_native_close_pre_close="
                + sample
                + (f"(+{remainder})" if remainder else "")
            )
        details.append(f"{code}[{' '.join(reasons)}]")
    raise RuntimeError(
        "ETF target data coverage incomplete: " + "; ".join(details)
    )


def _etf_backtest(engine: Engine, request: dict[str, Any]) -> dict[str, Any]:
    from decimal import Decimal

    from server.trading_v2.research import (
        evaluate_oos_gate,
        nav_records_from_equity,
    )
    from server.trading_v2.research_replay import (
        annual_trade_metrics,
        fifo_completed_trade_rows,
        metrics_for_trade_rows,
        remove_best_n_net_pnl,
        remove_largest_profit_security_net_pnl,
    )
    from tools.backtest_etf_ensemble import performance_metrics
    from tools.backtest_etf_robust import (
        ExecutionAssumptions,
        build_fast_risk_schedule,
        moving_block_bootstrap,
        simulate_realistic,
    )

    start = str(request["start_date"])
    end = str(request["end_date"])
    if end > date.today().isoformat():
        raise ValueError("end_date must not be in the future")
    seed = int(request["random_seed"])
    registered_codes = tuple(request.get("_registered_etf_eligible_codes") or ())
    registered_universe = request.get("_registered_etf_universe")
    registered_universe_hash = str(
        request.get("_registered_etf_universe_hash") or ""
    )
    if not registered_codes or not isinstance(registered_universe, dict):
        raise ValueError("registered ETF universe contract is unavailable")
    if registered_universe_hash != canonical_json_hash(registered_universe):
        raise ValueError("registered ETF universe hash mismatch")
    initial_capital, cost_multiplier = _resolved_execution_inputs(
        request,
        instrument_scope="EXCHANGE_TRADED_FUND",
    )
    dependency_start = _etf_dependency_start(start)
    with engine.begin() as connection:
        snapshot_rows = connection.execute(
            text(
                """
                SELECT k.etf_code, k.short_name, k.trade_date, k.adjust_type,
                       k.data_source,
                       k.data_version, k.received_at,
                       k.open, k.close, k.pre_close, k.amount,
                       k.validation_status, k.quality_status,
                       c.asset_class,
                       c.list_date, c.last_trade_date,
                       c.status AS instrument_status,
                       c.updated_at AS classification_updated_at
                FROM sm_etf_kline k
                JOIN si_etf_code c ON c.etf_code = k.etf_code
                WHERE k.adjust_type = 0
                  AND k.k_type = 1
                  AND k.validation_status = 'passed'
                  AND k.quality_status = 'validated'
                  AND k.data_source = :data_source
                  AND k.etf_code IN :eligible_codes
                  AND k.trade_date BETWEEN :start_date AND :end_date
                ORDER BY k.trade_date, k.etf_code
                """
            ).bindparams(bindparam("eligible_codes", expanding=True)),
            {
                "start_date": dependency_start,
                "end_date": end,
                "data_source": ETF_RESEARCH_DATA_SOURCE,
                "eligible_codes": list(registered_codes),
            },
        ).mappings().all()
        if not snapshot_rows:
            raise RuntimeError("ETF data snapshot has no validated source rows")
        exact_snapshot_rows = [dict(row) for row in snapshot_rows]
        dependency_data_contract = registered_etf_dependency_data_contract(
            connection,
            eligible_codes=registered_codes,
            dependency_start=dependency_start,
            end_date=end,
            snapshot_rows=exact_snapshot_rows,
            backtest_start=start,
        )
        expected_dependency_hash = str(
            request.get("expected_dependency_contract_hash") or ""
        )
        if (
            expected_dependency_hash
            and expected_dependency_hash
            != dependency_data_contract["contract_hash"]
        ):
            raise ValueError(
                "ETF dependency data changed after API preflight"
            )
        research_truth = dependency_data_contract["research_truth"]
        expected_sessions = dependency_data_contract["formal_sessions"]
        native_session_dates = dependency_data_contract["native_session_dates"]
        expected_sessions_by_code = dependency_data_contract[
            "expected_sessions_by_code"
        ]
        registered_session_contract = dependency_data_contract[
            "registered_session_contract"
        ]
        source_data = dependency_data_contract["source_data"]
        effective_end = dependency_data_contract["effective_end"]
        data = dependency_data_contract["data"]
        universe_audit = dependency_data_contract["universe_audit"]
        derived_eligible_codes = dependency_data_contract[
            "derived_eligible_codes"
        ]
        monthly_targets = dependency_data_contract["monthly_targets"]
        target_records = dependency_data_contract["target_records"]
        fee_profile = _confirmed_etf_fee_profile(
            connection,
            start_date=start,
            end_date=effective_end,
        )
        if not fee_profile["usable"]:
            raise RuntimeError(
                "formal ETF replay requires one confirmed account fee profile"
            )
    data_snapshot_hash = canonical_json_hash(
        {
            "market_data_contract_hash": research_truth["contract_hash"],
            "fee_evidence": fee_profile,
            "registered_universe_hash": registered_universe_hash,
            "formal_calendar_session_hash": canonical_json_hash(
                expected_sessions
            ),
            "registered_session_contract_hash": registered_session_contract[
                "contract_hash"
            ],
            "dependency_data_contract_hash": dependency_data_contract[
                "contract_hash"
            ],
        }
    )
    registered_data_audit = _target_data_coverage_audit(
        data,
        {},
        end_date=effective_end,
        target_codes=list(registered_codes),
        expected_sessions=expected_sessions,
        expected_sessions_by_code=expected_sessions_by_code,
        native_session_dates=native_session_dates,
    )
    _require_target_data_coverage(registered_data_audit)
    target_codes = sorted({
        str(code)
        for weights in monthly_targets.values()
        for code, weight in weights.items()
        if math.isfinite(float(weight)) and float(weight) > 1e-8
    })
    monthly_data_audit = _target_data_coverage_audit(
        data,
        monthly_targets,
        end_date=effective_end,
        target_codes=target_codes,
        expected_sessions=expected_sessions,
        expected_sessions_by_code=expected_sessions_by_code,
        native_session_dates=native_session_dates,
    )
    _require_target_data_coverage(monthly_data_audit)

    def run_case(
        assumptions: ExecutionAssumptions,
        *,
        risk_mode: str = "daily_vol_stop",
        volatility_multiplier: float = 3.0,
        minimum_stop: float = 0.06,
        maximum_stop: float = 0.15,
        reentry_mode: str = "none",
        reentry_cooldown_days: int = 0,
    ) -> dict[str, Any]:
        schedule, contexts, exits = build_fast_risk_schedule(
            data,
            monthly_targets,
            end_date=effective_end,
            risk_mode=risk_mode,
            volatility_multiplier=volatility_multiplier,
            minimum_stop=minimum_stop,
            maximum_stop=maximum_stop,
            reentry_mode=reentry_mode,
            reentry_cooldown_days=reentry_cooldown_days,
        )
        data_audit = _target_data_coverage_audit(
            data,
            schedule,
            end_date=effective_end,
            target_codes=target_codes,
            expected_sessions=expected_sessions,
            expected_sessions_by_code=expected_sessions_by_code,
            native_session_dates=native_session_dates,
        )
        _require_target_data_coverage(data_audit)
        equity, rebalances, fills = simulate_realistic(
            data,
            schedule,
            contexts=contexts,
            end_date=effective_end,
            assumptions=assumptions,
            start_date=start,
        )
        rows = fifo_completed_trade_rows(
            fills,
            data,
            volatility_multiplier=Decimal(
                str(volatility_multiplier)
            ),
            minimum_stop=Decimal(str(minimum_stop)),
            maximum_stop=Decimal(str(maximum_stop)),
        )
        open_position_count = 0
        if (
            not fills.empty
            and {"etf_code", "side", "filled_units"}.issubset(fills.columns)
        ):
            signed_units = fills["filled_units"].fillna(0).astype(float).where(
                fills["side"].eq("BUY"),
                -fills["filled_units"].fillna(0).astype(float),
            )
            open_position_count = int(
                (signed_units.groupby(fills["etf_code"]).sum() > 0).sum()
            )
        return {
            "equity": equity,
            "rebalances": rebalances,
            "fills": fills,
            "trades": rows,
            "metrics": metrics_for_trade_rows(rows, equity=equity),
            "performance": performance_metrics(
                equity / assumptions.initial_capital,
                evaluation_start_date=start,
            ),
            "risk_exit_events": int(len(exits)),
            "risk_reentry_events": (
                int(
                    rebalances["event_type"]
                    .isin(
                        [
                            "fast_risk_reentry",
                            "fast_risk_exit_and_reentry",
                        ]
                    )
                    .sum()
                )
                if not rebalances.empty
                else 0
            ),
            "blocked_orders": (
                int(rebalances["blocked_orders"].sum())
                if not rebalances.empty
                else 0
            ),
            "partial_orders": (
                int(rebalances["partial_orders"].sum())
                if not rebalances.empty
                else 0
            ),
            "open_position_count": open_position_count,
            "data_audit": data_audit,
        }

    confirmed_fee_inputs = {
        "minimum_commission": float(fee_profile["minimum_commission"]),
        "buy_commission_rate": float(fee_profile["buy_commission_rate"]),
        "sell_commission_rate": float(fee_profile["sell_commission_rate"]),
        "stamp_tax_sell_rate": float(fee_profile["stamp_tax_sell_rate"]),
        "transfer_fee_buy_rate": float(fee_profile["transfer_fee_buy_rate"]),
        "transfer_fee_sell_rate": float(fee_profile["transfer_fee_sell_rate"]),
    }

    def execution_assumptions(**overrides: Any) -> ExecutionAssumptions:
        values: dict[str, Any] = {
            "initial_capital": initial_capital,
            "cost_multiplier": cost_multiplier,
            **confirmed_fee_inputs,
        }
        values.update(overrides)
        return ExecutionAssumptions(**values)

    base_assumptions = execution_assumptions()
    proposed = run_case(base_assumptions)
    no_overlay = run_case(
        base_assumptions,
        risk_mode="none",
        reentry_mode="none",
    )
    doubled_cost = run_case(
        execution_assumptions(
            cost_multiplier=cost_multiplier * 2.0,
        )
    )
    half_capacity = run_case(
        execution_assumptions(
            max_adv_participation=0.01,
        )
    )
    adverse_gap = run_case(
        execution_assumptions(
            adverse_open_gap_rate=0.005,
        )
    )
    neighborhood_results: list[dict[str, Any]] = []
    for multiplier in (2.7, 3.0, 3.3):
        for minimum_stop in (0.055, 0.060, 0.065):
            case = run_case(
                base_assumptions,
                volatility_multiplier=multiplier,
                minimum_stop=minimum_stop,
            )
            pnl = Decimal(
                str(case["metrics"]["cumulative_net_pnl"])
            )
            neighborhood_results.append(
                {
                    "volatility_multiplier": multiplier,
                    "minimum_stop": minimum_stop,
                    "cumulative_net_pnl": str(pnl),
                    "positive": pnl > 0,
                }
            )
    positive_neighborhood_ratio = (
        Decimal(
            sum(
                1
                for item in neighborhood_results
                if item["positive"]
            )
        )
        / Decimal(len(neighborhood_results))
    )
    bootstrap = moving_block_bootstrap(
        proposed["equity"],
        simulations=2000,
        block_days=20,
        seed=seed,
    )
    future_violations = sum(
        1
        for row in target_records
        if str(row["execution_date"]) <= str(row["signal_date"])
    )
    robustness = {
        "complete": True,
        "block_bootstrap_paths": int(bootstrap["simulations"]),
        "positive_parameter_neighborhood_ratio": str(
            positive_neighborhood_ratio
        ),
        "fee_and_slippage_2x": doubled_cost["metrics"],
        "capacity_half": half_capacity["metrics"],
        "adverse_next_open_gap_0_5pct": adverse_gap["metrics"],
        "remove_best_1_net_pnl": str(
            remove_best_n_net_pnl(proposed["trades"], 1)
        ),
        "remove_best_3_net_pnl": str(
            remove_best_n_net_pnl(proposed["trades"], 3)
        ),
        "remove_best_5_net_pnl": str(
            remove_best_n_net_pnl(proposed["trades"], 5)
        ),
        "remove_largest_security_net_pnl": str(
            remove_largest_profit_security_net_pnl(
                proposed["trades"]
            )
        ),
        "annual_trade_metrics": annual_trade_metrics(
            proposed["trades"]
        ),
        "parameter_neighborhood": neighborhood_results,
        "bootstrap": bootstrap,
    }
    statistical_gate = evaluate_oos_gate(
        security_scope="ETF",
        trading_days=max(0, int(len(proposed["equity"])) - 1),
        oos_windows=int(len(target_records)),
        metrics=proposed["metrics"],
        doubled_cost_metrics=doubled_cost["metrics"],
        remove_best_three_net_pnl=remove_best_n_net_pnl(
            proposed["trades"], 3
        ),
        robustness=robustness,
        future_data_violations=future_violations,
        impossible_fill_profit=Decimal("0"),
        nav_records=nav_records_from_equity(proposed["equity"]),
        doubled_cost_nav_records=nav_records_from_equity(
            doubled_cost["equity"]
        ),
    )
    proposed_risk_adjusted = (
        float(proposed["performance"]["total_return"])
        / max(
            abs(float(proposed["performance"]["max_drawdown"])),
            1e-12,
        )
    )
    baseline_risk_adjusted = (
        float(no_overlay["performance"]["total_return"])
        / max(
            abs(float(no_overlay["performance"]["max_drawdown"])),
            1e-12,
        )
    )
    baseline_comparison = {
        "same_data_account_cost_execution": True,
        "dynamic_performance": proposed["performance"],
        "no_overlay_performance": no_overlay["performance"],
        "dynamic_trade_metrics": proposed["metrics"],
        "no_overlay_trade_metrics": no_overlay["metrics"],
        "dynamic_risk_adjusted_return": proposed_risk_adjusted,
        "no_overlay_risk_adjusted_return": baseline_risk_adjusted,
        "dynamic_improves_risk_adjusted_return": (
            proposed_risk_adjusted > baseline_risk_adjusted
        ),
        "dynamic_max_drawdown_not_worse": (
            float(proposed["performance"]["max_drawdown"])
            >= float(no_overlay["performance"]["max_drawdown"])
        ),
    }
    promotion_blockers: list[str] = []
    if statistical_gate["status"] != "PASS":
        promotion_blockers.append("STATISTICAL_GATE_BLOCKED")
    if not (
        baseline_comparison["same_data_account_cost_execution"]
        and baseline_comparison[
            "dynamic_improves_risk_adjusted_return"
        ]
        and baseline_comparison["dynamic_max_drawdown_not_worse"]
    ):
        promotion_blockers.append("BASELINE_COMPARISON_BLOCKED")
    promotion_blockers.extend(ETF_MUTABLE_INPUT_BLOCKERS)
    # The frozen holdout has already been inspected during strategy
    # development, so it cannot honestly be relabelled as a one-shot,
    # untouched test set after the fact.
    promotion_blockers.append("FROZEN_HOLDOUT_NOT_PRISTINE")
    return {
        "adapter": "etf_trade_level_replay_v2",
        "strategy_key": "etf_trend_risk",
        "data_dependency_start": dependency_start,
        "data_snapshot": {
            "hash": data_snapshot_hash,
            "row_count": len(snapshot_rows),
            "start_date": dependency_start,
            "end_date": effective_end,
            "requested_end_date": end,
            "end_date_clamped": effective_end != end,
            "source": ETF_RESEARCH_DATA_SOURCE,
            "validation_status": "passed",
            "quality_status": "validated",
            "adjust_type": 0,
            "stored_adjusted_history_consumed": False,
            "row_identity": (
                "etf_code+trade_date+data_source+adjust_type+data_version+received_at"
            ),
            "formal_calendar_source": "si_trade_calendar",
            "formal_calendar_start": dependency_start,
            "formal_calendar_session_count": len(expected_sessions),
            "formal_calendar_session_hash": canonical_json_hash(
                expected_sessions
            ),
            "registered_session_contract": registered_session_contract,
            "dependency_data_contract_hash": dependency_data_contract[
                "contract_hash"
            ],
        },
        "research_input_truth": research_truth,
        "data_audit": {
            **proposed["data_audit"],
            "registered_universe": registered_universe,
            "registered_universe_hash": registered_universe_hash,
            "derived_eligible_codes": derived_eligible_codes,
            "registered_code_coverage": registered_data_audit,
        },
        "account_initial_cash": f"{initial_capital:.2f}",
        "execution_assumptions": {
            "initial_capital_cny": initial_capital,
            "cost_basis": "CONFIRMED_FEE_PROFILE_BASELINE_WITH_2X_STRESS",
            "cost_scenario_multiplier": cost_multiplier,
            "stress_cost_scenario_multiplier": 2.0,
            "fee_profile": fee_profile,
        },
        "data_source": ETF_RESEARCH_DATA_SOURCE,
        "universe_cutoff": ETF_UNIVERSE_CUTOFF,
        "eligible_universe": derived_eligible_codes,
        "trade_dates": max(0, int(len(proposed["equity"])) - 1),
        "rebalance_windows": int(len(target_records)),
        "completed_trade_count": len(proposed["trades"]),
        "open_position_count": proposed["open_position_count"],
        "final_equity_cny": (
            float(proposed["equity"].iloc[-1])
            if not proposed["equity"].empty
            else initial_capital
        ),
        "metrics": proposed["metrics"],
        "performance": proposed["performance"],
        "blocked_orders": proposed["blocked_orders"],
        "partial_orders": proposed["partial_orders"],
        "risk_exit_events": proposed["risk_exit_events"],
        "risk_reentry_events": proposed["risk_reentry_events"],
        "dynamic_exit_policy": {
            "exit": "volatility_scaled_trailing_stop",
            "execution": "next_trading_day_open",
            "reentry": "next_monthly_rebalance_only",
            "reentry_cooldown_trading_days": None,
            "fixed_holding_days": False,
        },
        "robustness": robustness,
        "baseline_comparison": baseline_comparison,
        "statistical_gate": statistical_gate,
        "promotion_protocol": {
            "status": (
                "PASS" if not promotion_blockers else "BLOCK"
            ),
            "blockers": promotion_blockers,
            "research_assumption_fees": False,
            "oos_passed": False,
            "mutable_etf_inputs_quarantined": True,
        },
        "_data_snapshot_hash": data_snapshot_hash,
        "_trade_rows": proposed["trades"],
    }


def _backtest_request_identity(request: dict[str, Any]) -> dict[str, Any]:
    """Normalize every user-controlled input used by the formal ETF replay."""

    strategy_id = str(request["strategy_id"])
    strategy_version = str(request["strategy_version"])
    if (
        strategy_id != "etf_trend_risk"
        or strategy_version != "etf_trend_risk_v2.0.0"
    ):
        raise ValueError("unsupported formal backtest request identity")
    initial_capital, cost_scenario_multiplier = _resolved_execution_inputs(
        request,
        instrument_scope="EXCHANGE_TRADED_FUND",
    )
    return {
        "run_request_uid": str(request.get("run_request_uid") or ""),
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "start_date": str(request["start_date"]),
        "end_date": str(request["end_date"]),
        "random_seed": int(request["random_seed"]),
        "initial_capital": initial_capital,
        "cost_scenario_multiplier": cost_scenario_multiplier,
        "top_per_day": int(
            request.get("top_per_day") or request.get("top") or 10
        ),
    }


def _run_backtest_job_impl(
    engine: Engine,
    request: dict[str, Any],
) -> dict[str, Any]:
    strategy_id = str(request.get("strategy_id") or "")
    strategy_version = str(request["strategy_version"])
    start_date = str(request["start_date"])
    end_date = str(request["end_date"])
    random_seed = int(request["random_seed"])
    with engine.connect() as connection:
        if strategy_id:
            strategy_rows = connection.execute(
                text(
                    """
                    SELECT strategy_id, version, instrument_scope, config_hash,
                           manifest_json
                    FROM st_strategy_version_v2
                    WHERE BINARY strategy_id = BINARY :strategy_id
                      AND BINARY version = BINARY :version
                    """
                ),
                {"strategy_id": strategy_id, "version": strategy_version},
            ).mappings().all()
        else:
            # Compatibility for jobs queued before strategy_id became part of
            # the request.  Ambiguous versions fail closed rather than binding
            # to whichever row the database happens to return first.
            strategy_rows = connection.execute(
                text(
                    """
                    SELECT strategy_id, version, instrument_scope, config_hash,
                           manifest_json
                    FROM st_strategy_version_v2
                    WHERE BINARY version = BINARY :version
                    """
                ),
                {"version": strategy_version},
            ).mappings().all()
    if not strategy_rows:
        raise ValueError("exact strategy version is not registered")
    if len(strategy_rows) != 1:
        raise ValueError("strategy version is ambiguous; strategy_id is required")
    strategy = strategy_rows[0]
    strategy_id = str(strategy["strategy_id"])
    request["strategy_id"] = strategy_id
    adapter = research_backtest_adapter(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        instrument_scope=str(strategy["instrument_scope"]),
    )
    if not adapter["supported"]:
        raise ValueError(str(adapter["reason"]))
    universe_contract = registered_etf_universe_contract(dict(strategy))
    _require_expected_registration_binding(
        request,
        config_hash=str(strategy["config_hash"]),
        universe_hash=str(universe_contract["universe_hash"]),
    )
    request["_registered_etf_eligible_codes"] = universe_contract[
        "eligible_codes"
    ]
    request["_registered_etf_universe"] = universe_contract["universe"]
    request["_registered_etf_universe_hash"] = universe_contract[
        "universe_hash"
    ]
    request["_backtest_adapter"] = str(adapter["adapter"])
    code_sha = code_version()[0]
    strategy_binding = {
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "config_hash": str(strategy["config_hash"]),
        "code_commit_sha": code_sha,
        "protocol_version": RESEARCH_PROTOCOL_VERSION,
        "registered_universe": universe_contract["universe"],
        "registered_universe_hash": universe_contract["universe_hash"],
    }
    initial_capital, cost_scenario_multiplier = _resolved_execution_inputs(
        request,
        instrument_scope=str(strategy["instrument_scope"]),
    )
    request_identity = _backtest_request_identity(request)
    running_evidence = {
        "adapter": str(adapter["adapter"]),
        "strategy_binding": strategy_binding,
        "run_request_uid": str(request.get("run_request_uid") or ""),
        "request_identity": request_identity,
        "execution_assumptions": {
            "initial_capital_cny": initial_capital,
            "cost_scenario_multiplier": cost_scenario_multiplier,
        },
    }
    request_hash = canonical_json_hash(
        {
            **request_identity,
            "protocol_version": RESEARCH_PROTOCOL_VERSION,
            "code_commit_sha": code_sha,
            "config_hash": strategy["config_hash"],
        }
    )
    backtest_uid = request_hash[:32]
    request["_backtest_uid"] = backtest_uid
    now = datetime.now()
    with engine.begin() as connection:
        existing = connection.execute(
            text(
                """
                SELECT status, result_hash FROM st_backtest_run_v2
                WHERE request_hash = :request_hash
                """
            ),
            {"request_hash": request_hash},
        ).mappings().first()
        if existing and existing["status"] == "COMPLETED":
            return {
                "backtest_uid": backtest_uid,
                "status": "idempotent_hit",
                "result_hash": existing["result_hash"],
            }
        connection.execute(
            text(
                """
                INSERT INTO st_backtest_run_v2
                (backtest_uid, strategy_id, strategy_version,
                 start_date, end_date,
                 random_seed, status, request_hash, data_snapshot_hash,
                 code_commit_sha, config_hash, protocol_version,
                 result_json, gate_status, started_at)
                VALUES
                (:uid, :strategy_id, :version,
                 :start_date, :end_date, :seed, 'RUNNING',
                 :request_hash, :empty_hash, :code_sha, :config_hash,
                 :protocol_version, :result_json, 'BLOCK', :started_at)
                ON DUPLICATE KEY UPDATE
                    strategy_id = VALUES(strategy_id),
                    status = 'RUNNING', started_at = VALUES(started_at),
                    result_json = VALUES(result_json),
                    result_hash = NULL,
                    data_snapshot_hash = VALUES(data_snapshot_hash),
                    gate_status = 'BLOCK', finished_at = NULL,
                    error_code = NULL, error_message = NULL
                """
            ),
            {
                "uid": backtest_uid,
                "strategy_id": strategy_id,
                "version": strategy_version,
                "start_date": start_date,
                "end_date": end_date,
                "seed": random_seed,
                "request_hash": request_hash,
                "empty_hash": "0" * 64,
                "code_sha": code_sha,
                "config_hash": strategy["config_hash"],
                "protocol_version": RESEARCH_PROTOCOL_VERSION,
                "result_json": _json(running_evidence),
                "started_at": now,
            },
        )
    if adapter["adapter"] != "etf_trade_level_replay_v2":
        raise ValueError("registered strategy has no reproducible backtest adapter")
    report = _etf_backtest(engine, request)
    report["strategy_binding"] = strategy_binding
    report["run_request_uid"] = str(request.get("run_request_uid") or "")
    trade_rows = list(report.pop("_trade_rows", []))
    data_hash = str(report.pop("_data_snapshot_hash", "") or "")
    data_hash = data_hash or canonical_json_hash(
        {
            "data_audit": report.get("data_audit"),
            "data_dependency_start": report.get(
                "data_dependency_start"
            ),
            "trade_dates": report.get("trade_dates"),
        }
    )
    result_hash = canonical_json_hash(report)
    gate_status = str(
        (report.get("promotion_protocol") or {}).get("status") or "BLOCK"
    )
    with engine.begin() as connection:
        for row in trade_rows:
            connection.execute(
                text(
                    """
                    INSERT INTO st_backtest_trade_v2
                    (backtest_uid, trade_id, stock_code, entry_date,
                     exit_date, quantity, buy_fill_amount,
                     sell_fill_amount, buy_fees, sell_fees,
                     initial_risk_amount, trade_net_pnl,
                     evidence_json, created_at)
                    VALUES
                    (:backtest_uid, :trade_id, :stock_code, :entry_date,
                     :exit_date, :quantity, :buy_fill_amount,
                     :sell_fill_amount, :buy_fees, :sell_fees,
                     :initial_risk_amount, :trade_net_pnl,
                     :evidence_json, :created_at)
                    """
                ),
                {
                    "backtest_uid": backtest_uid,
                    "trade_id": row["trade_id"],
                    "stock_code": row["stock_code"],
                    "entry_date": row["entry_date"],
                    "exit_date": row["exit_date"],
                    "quantity": row["quantity"],
                    "buy_fill_amount": row["buy_fill_amount"],
                    "sell_fill_amount": row["sell_fill_amount"],
                    "buy_fees": row["buy_fees"],
                    "sell_fees": row["sell_fees"],
                    "initial_risk_amount": row[
                        "initial_risk_amount"
                    ],
                    "trade_net_pnl": row["trade_net_pnl"],
                    "evidence_json": _json(row["evidence"]),
                    "created_at": datetime.now(),
                },
            )
        connection.execute(
            text(
                """
                UPDATE st_backtest_run_v2
                SET status = 'COMPLETED',
                    data_snapshot_hash = :data_hash,
                    result_json = :result_json,
                    result_hash = :result_hash,
                    gate_status = :gate_status,
                    finished_at = :finished_at
                WHERE backtest_uid = :uid
                """
            ),
            {
                "data_hash": data_hash,
                "result_json": _json(report),
                "result_hash": result_hash,
                "gate_status": gate_status,
                "finished_at": datetime.now(),
                "uid": backtest_uid,
            },
        )
    return {
        "backtest_uid": backtest_uid,
        "status": "COMPLETED",
        "gate_status": gate_status,
        "result_hash": result_hash,
    }


def _mark_latest_matching_backtest_failed(
    engine: Engine,
    request: dict[str, Any],
    error: Exception,
) -> int:
    backtest_uid = str(request.get("_backtest_uid") or "")
    if backtest_uid:
        with engine.begin() as connection:
            result = connection.execute(
                text(
                    """
                    UPDATE st_backtest_run_v2
                    SET status = 'FAILED',
                        error_code = :error_code,
                        error_message = :error_message,
                        finished_at = :finished_at
                    WHERE backtest_uid = :backtest_uid
                      AND status = 'RUNNING'
                    """
                ),
                {
                    "error_code": type(error).__name__.upper()[:80],
                    "error_message": str(error)[:500],
                    "finished_at": datetime.now(),
                    "backtest_uid": backtest_uid,
                },
            )
        return int(result.rowcount or 0)
    if request.get("run_request_uid"):
        # New jobs identify their own run before inserting it. If validation
        # fails earlier, never mark an older run with matching dates as failed.
        return 0
    required = (
        "strategy_version",
        "start_date",
        "end_date",
        "random_seed",
    )
    if any(key not in request for key in required):
        return 0
    with engine.begin() as connection:
        result = connection.execute(
            text(
                """
                UPDATE st_backtest_run_v2
                SET status = 'FAILED',
                    error_code = :error_code,
                    error_message = :error_message,
                    finished_at = :finished_at
                WHERE strategy_version = :strategy_version
                  AND start_date = :start_date
                  AND end_date = :end_date
                  AND random_seed = :random_seed
                  AND status = 'RUNNING'
                ORDER BY started_at DESC
                LIMIT 1
                """
            ),
            {
                "error_code": type(error).__name__.upper()[:80],
                "error_message": str(error)[:500],
                "finished_at": datetime.now(),
                "strategy_version": str(request["strategy_version"]),
                "start_date": str(request["start_date"]),
                "end_date": str(request["end_date"]),
                "random_seed": int(request["random_seed"]),
            },
        )
    return int(result.rowcount or 0)


def _run_backtest_job(
    engine: Engine,
    request: dict[str, Any],
) -> dict[str, Any]:
    try:
        return _run_backtest_job_impl(engine, request)
    except Exception as exc:
        _mark_latest_matching_backtest_failed(engine, request, exc)
        raise


def repair_orphaned_backtests(
    engine: Engine,
    *,
    stale_after_minutes: int = 15,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    cutoff = (now or datetime.now()) - timedelta(
        minutes=max(1, stale_after_minutes)
    )
    with engine.connect() as connection:
        running = connection.execute(
            text(
                """
                SELECT backtest_uid, strategy_version, start_date,
                       end_date, random_seed, started_at, result_json
                FROM st_backtest_run_v2
                WHERE status = 'RUNNING'
                  AND started_at <= :cutoff
                ORDER BY started_at
                """
            ),
            {"cutoff": cutoff},
        ).mappings().all()
        failed_jobs = connection.execute(
            text(
                """
                SELECT job_id, request_json, error_code, error_message,
                       finished_at
                FROM st_job_v2
                WHERE job_type = 'BACKTEST'
                  AND status = 'FAILED'
                ORDER BY finished_at DESC
                """
            )
        ).mappings().all()
    failed_by_run_uid: dict[
        str, tuple[dict[str, Any], dict[str, Any]]
    ] = {}
    legacy_failed_by_request: dict[
        tuple[str, str, str, int], dict[str, Any]
    ] = {}
    for job in failed_jobs:
        try:
            request = json.loads(str(job["request_json"]))
            run_uid = str(request.get("run_request_uid") or "")
            if run_uid:
                identity = _backtest_request_identity(request)
                failed_by_run_uid.setdefault(
                    run_uid,
                    (identity, dict(job)),
                )
                continue
            key = (
                str(request["strategy_version"]),
                str(request["start_date"]),
                str(request["end_date"]),
                int(request["random_seed"]),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
        legacy_failed_by_request.setdefault(key, dict(job))

    repaired: list[dict[str, Any]] = []
    for row in running:
        try:
            running_evidence = json.loads(str(row.get("result_json") or "{}"))
        except json.JSONDecodeError:
            running_evidence = {}
        stored_identity = running_evidence.get("request_identity")
        run_uid = str(running_evidence.get("run_request_uid") or "")
        job = None
        if run_uid and isinstance(stored_identity, dict):
            matched = failed_by_run_uid.get(run_uid)
            if matched:
                failed_identity, candidate_job = matched
                try:
                    normalized_stored = _backtest_request_identity(
                        stored_identity
                    )
                except (KeyError, TypeError, ValueError):
                    normalized_stored = None
                row_matches_identity = (
                    normalized_stored is not None
                    and str(row["strategy_version"])
                    == normalized_stored["strategy_version"]
                    and str(row["start_date"])
                    == normalized_stored["start_date"]
                    and str(row["end_date"])
                    == normalized_stored["end_date"]
                    and int(row["random_seed"])
                    == normalized_stored["random_seed"]
                )
                if row_matches_identity and normalized_stored == failed_identity:
                    job = candidate_job
        elif not run_uid:
            key = (
                str(row["strategy_version"]),
                str(row["start_date"]),
                str(row["end_date"]),
                int(row["random_seed"]),
            )
            job = legacy_failed_by_request.get(key)
        if not job:
            continue
        finished_at = job.get("finished_at") or datetime.now()
        with engine.begin() as connection:
            result = connection.execute(
                text(
                    """
                    UPDATE st_backtest_run_v2
                    SET status = 'FAILED',
                        error_code = :error_code,
                        error_message = :error_message,
                        finished_at = :finished_at
                    WHERE backtest_uid = :backtest_uid
                      AND status = 'RUNNING'
                    """
                ),
                {
                    "error_code": str(
                        job.get("error_code") or "ORPHANED_BACKTEST"
                    )[:80],
                    "error_message": str(
                        job.get("error_message")
                        or "backtest worker failed before finalization"
                    )[:500],
                    "finished_at": finished_at,
                    "backtest_uid": str(row["backtest_uid"]),
                },
            )
        if int(result.rowcount or 0) == 1:
            repaired.append(
                {
                    "backtest_uid": str(row["backtest_uid"]),
                    "job_id": str(job["job_id"]),
                    "status": "FAILED",
                }
            )
    return repaired


def run_one_job(engine: Engine) -> dict[str, Any]:
    job = _claim_job(engine)
    if not job:
        _heartbeat(engine, status="IDLE")
        return {"status": "idle"}
    job_id = str(job["job_id"])
    _heartbeat(
        engine,
        status="RUNNING",
        current_job_id=job_id,
    )
    try:
        request = json.loads(str(job["request_json"]))
        job_type = str(job["job_type"])
        if job_type == "DECISION_RUN":
            result = run_daily_decision(
                engine,
                trade_date=str(request["trade_date"]),
            )
            result_ref = str(result["run_uid"])
        elif job_type == "BACKTEST":
            result = _run_backtest_job(engine, request)
            result_ref = str(result["backtest_uid"])
        else:
            raise ValueError(f"unsupported V2 job type: {job_type}")
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE st_job_v2
                    SET status = 'COMPLETED', result_ref = :result_ref,
                        finished_at = :finished_at
                    WHERE job_id = :job_id
                    """
                ),
                {
                    "result_ref": result_ref,
                    "finished_at": datetime.now(),
                    "job_id": job_id,
                },
            )
        _heartbeat(engine, status="IDLE", success=True)
        return {
            "status": "COMPLETED",
            "job_id": job_id,
            "job_type": job_type,
            "result_ref": result_ref,
            "result": result,
        }
    except Exception as exc:
        error_code = type(exc).__name__.upper()
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE st_job_v2
                    SET status = 'FAILED', error_code = :error_code,
                        error_message = :error_message,
                        finished_at = :finished_at
                    WHERE job_id = :job_id
                    """
                ),
                {
                    "error_code": error_code[:80],
                    "error_message": str(exc)[:500],
                    "finished_at": datetime.now(),
                    "job_id": job_id,
                },
            )
        _heartbeat(
            engine,
            status="ERROR",
            error_code=error_code,
            error_message=str(exc),
        )
        return {
            "status": "FAILED",
            "job_id": job_id,
            "error_code": error_code,
            "error_message": str(exc),
        }
