from __future__ import annotations

import hashlib
import json
import math
import sys
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import date, datetime
from typing import Any, Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from .calibration import CalibrationTable
from .config import config_hash as current_config_hash
from .config import load_v3_config
from .decision_truth import (
    canonical_hash,
    canonical_target_ledger,
    decision_result_hash,
)
from .domain import AlphaForecast, HypothesisEvidence, TradeHypothesis
from .right_side_policy import right_side_model_contract_hash
from .shadow_portfolio import build_shadow_portfolio_rows
from .validation import model_gate_failures
from .versioning import code_version


V3_TABLES = (
    "st_decision_run_v3",
    "st_alpha_forecast_v3",
    "st_target_portfolio_v3",
    "st_position_state_v3",
    "st_execution_plan_v3",
    "st_counterfactual_v3",
    "st_counterfactual_queue_v3",
    "st_theme_signal_v3",
    "st_shadow_portfolio_v3",
    "st_opportunity_recall_v3",
    "st_forward_trade_evidence_v3",
    "st_tca_v3",
    "st_model_registry_v3",
    "st_validation_result_v3",
    "st_trade_hypothesis_v3",
    "st_hypothesis_evidence_v3",
    "st_horizon_model_artifact_v3",
    "st_horizon_forecast_contract_v3",
    "st_horizon_outcome_v3",
    "st_shadow_release_v3",
    "st_calibration_gate_v3",
    "st_counterfactual_learning_run_v3",
)


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )


def _decimal(value: Any) -> float | None:
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _uuid() -> str:
    return uuid.uuid4().hex


def _theme_signal_rank(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -int(item.get("selected_as_primary") or 0),
        -float(item.get("raw_score") or 0.0),
        str(item.get("stock_code") or ""),
        str(item.get("strategy_key") or ""),
        str(item.get("theme_feature_key") or ""),
    )


def _select_theme_signal_evidence(
    raw_rows: Iterable[dict[str, Any]],
    *,
    forecast_keys: set[tuple[str, str]],
    maximum_rows: int,
    top_k_per_theme_strategy: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Retain scalable evidence without silently dropping a theme.

    Every candidate is evaluated before this function is called. Persistence
    keeps each stock's selected primary signal plus a balanced Top-K for every
    exact source-theme/strategy pair.  Depth is filled round-robin so a row
    cap cannot favour alphabetically early themes.  If the cap cannot retain
    even one row for every pair, fail closed instead of producing incomplete
    thematic evidence.
    """

    valid_rows: list[dict[str, Any]] = []
    identities: set[tuple[str, str, str]] = set()
    for item in sorted(raw_rows, key=_theme_signal_rank):
        stock_code = str(item.get("stock_code") or "")
        strategy_key = str(item.get("strategy_key") or "")
        feature_key = str(item.get("theme_feature_key") or "")
        identity = (stock_code, strategy_key, feature_key)
        if not all(identity) or (stock_code, strategy_key) not in forecast_keys:
            continue
        if identity in identities:
            raise RuntimeError(
                "duplicate theme signal ownership key: "
                f"{stock_code}/{strategy_key}/{feature_key}"
            )
        identities.add(identity)
        valid_rows.append(item)

    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in valid_rows:
        exact_theme = str(
            item.get("theme_code")
            or item.get("theme_name")
            or item.get("theme_feature_key")
            or ""
        )
        groups[(str(item["strategy_key"]), exact_theme)].append(item)
    for rows in groups.values():
        rows.sort(key=_theme_signal_rank)

    retained: dict[tuple[str, str, str], dict[str, Any]] = {}

    def retain(item: dict[str, Any]) -> None:
        retained.setdefault(
            (
                str(item["stock_code"]),
                str(item["strategy_key"]),
                str(item["theme_feature_key"]),
            ),
            item,
        )

    for item in valid_rows:
        if int(item.get("selected_as_primary") or 0) == 1:
            retain(item)

    ordered_groups = sorted(groups)
    for group in ordered_groups:
        if groups[group]:
            retain(groups[group][0])
    if len(retained) > maximum_rows:
        raise RuntimeError(
            "theme signal evidence cap cannot cover every exact theme and "
            f"strategy: required={len(retained)} cap={maximum_rows}"
        )

    for depth in range(1, max(1, top_k_per_theme_strategy)):
        for group in ordered_groups:
            rows = groups[group]
            if depth >= len(rows):
                continue
            identity = (
                str(rows[depth]["stock_code"]),
                str(rows[depth]["strategy_key"]),
                str(rows[depth]["theme_feature_key"]),
            )
            if identity in retained:
                continue
            if len(retained) >= maximum_rows:
                break
            retained[identity] = rows[depth]
        if len(retained) >= maximum_rows:
            break

    selected = sorted(retained.values(), key=_theme_signal_rank)
    return selected, {
        "evaluated_count": len(valid_rows),
        "retained_count": len(selected),
        "exact_theme_strategy_group_count": len(groups),
        "selected_primary_count": sum(
            int(item.get("selected_as_primary") or 0) == 1
            for item in selected
        ),
    }


def _save_progress(stage: str, started_at: float, **counts: Any) -> None:
    payload = {
        "event": "trading_v3_save_progress",
        "stage": stage,
        "elapsed_seconds": round(time.perf_counter() - started_at, 3),
        **counts,
    }
    print(_json(payload), file=sys.stderr, flush=True)


class TradingV3Repository:
    def __init__(self, engine: Engine):
        self.engine = engine

    def table_readiness(self) -> dict[str, bool]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT TABLE_NAME
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME IN :names
                    """
                ).bindparams(
                    bindparam("names", expanding=True)
                ),
                {"names": list(V3_TABLES)},
            ).scalars().all()
        present = set(rows)
        return {name: name in present for name in V3_TABLES}

    def production_column_readiness(self) -> dict[str, bool]:
        expected = {
            "decision_config_hash": (
                "st_decision_run_v3",
                "config_hash",
            ),
            "decision_code_commit": (
                "st_decision_run_v3",
                "code_commit_sha",
            ),
            "decision_calibration_set": (
                "st_decision_run_v3",
                "calibration_set_hash",
            ),
            "target_theme_codes": (
                "st_target_portfolio_v3",
                "theme_codes_json",
            ),
            "target_primary_sample_owner": (
                "st_target_portfolio_v3",
                "primary_forecast_id",
            ),
            "forecast_features": (
                "st_alpha_forecast_v3",
                "features_json",
            ),
            "forward_evidence_protocol": (
                "st_forward_trade_evidence_v3",
                "protocol_version",
            ),
            "forward_evidence_ownership": (
                "st_forward_trade_evidence_v3",
                "ownership_hash",
            ),
            "counterfactual_evidence_kind": (
                "st_counterfactual_v3",
                "evidence_kind",
            ),
            "recall_strategy_attribution": (
                "st_opportunity_recall_v3",
                "strategy_key",
            ),
            "recall_evidence_protocol": (
                "st_opportunity_recall_v3",
                "protocol_version",
            ),
        }
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT TABLE_NAME, COLUMN_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME IN :names
                    """
                ).bindparams(
                    bindparam("names", expanding=True)
                ),
                {
                    "names": sorted({
                        table_name
                        for table_name, _ in expected.values()
                    })
                },
            ).all()
        present = {
            (str(row[0]), str(row[1]))
            for row in rows
        }
        return {
            key: pair in present
            for key, pair in expected.items()
        }

    def real_trading_guard_readiness(self) -> dict[str, bool]:
        expected = {
            "account_insert": "trg_trade_account_v2_real_disabled_bi",
            "account_update": "trg_trade_account_v2_real_disabled_bu",
            "execution_plan_insert": (
                "trg_execution_plan_v3_real_disabled_bi"
            ),
            "execution_plan_update": (
                "trg_execution_plan_v3_real_disabled_bu"
            ),
        }
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT TRIGGER_NAME
                    FROM information_schema.TRIGGERS
                    WHERE TRIGGER_SCHEMA = DATABASE()
                      AND TRIGGER_NAME IN :names
                    """
                ).bindparams(
                    bindparam("names", expanding=True)
                ),
                {"names": list(expected.values())},
            ).scalars().all()
        present = {str(item) for item in rows}
        return {
            key: trigger_name in present
            for key, trigger_name in expected.items()
        }

    def active_calibration_status(self) -> dict[str, Any]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT strategy_key, model_version, dataset_hash,
                           feature_schema_hash, calibration_json,
                           metrics_json, config_json
                    FROM st_model_registry_v3
                    WHERE lifecycle_status = 'PAPER_ACTIVE'
                    ORDER BY activated_at DESC, created_at DESC
                    """
                )
            ).mappings().all()
        config = load_v3_config()
        version_tokens = dict(
            config.get("calibration_version_tokens") or {}
        )
        default_token = str(
            config.get("calibration_version_token") or ""
        )
        minimum_bucket_count = int(
            config.get("calibration", {}).get(
                "minimum_bucket_count",
                2,
            )
        )
        accepted: dict[str, CalibrationTable] = {}
        rejected: dict[str, list[str]] = {}
        seen: set[str] = set()
        for row in rows:
            key = str(row["strategy_key"])
            if key in seen:
                continue
            seen.add(key)
            reasons: list[str] = []
            try:
                table = CalibrationTable.from_dict(
                    json.loads(str(row["calibration_json"]))
                )
            except Exception:
                rejected[key] = ["CALIBRATION_JSON_INVALID"]
                continue
            try:
                registered_config = json.loads(
                    str(row["config_json"])
                )
                registered_config_hash = hashlib.sha256(
                    _json(registered_config).encode("utf-8")
                ).hexdigest()
            except Exception:
                registered_config_hash = ""
            required_token = str(
                version_tokens.get(key) or default_token
            )
            if required_token and required_token not in table.model_version:
                reasons.append("MODEL_VERSION_MISMATCH")
            if table.strategy_key != key:
                reasons.append("MODEL_REGISTRY_STRATEGY_MISMATCH")
            if table.model_version != str(row["model_version"]):
                reasons.append("MODEL_REGISTRY_VERSION_MISMATCH")
            if table.dataset_hash != str(row["dataset_hash"]):
                reasons.append("MODEL_REGISTRY_DATASET_HASH_MISMATCH")
            if registered_config_hash != current_config_hash():
                reasons.append("MODEL_CONFIG_HASH_MISMATCH")
            feature_schema_hash = str(
                row["feature_schema_hash"] or ""
            )
            if len(feature_schema_hash) != 64:
                reasons.append("FEATURE_SCHEMA_HASH_INVALID")
            elif (
                key == "right_side_trend"
                and feature_schema_hash
                != right_side_model_contract_hash(config)
            ):
                reasons.append("MODEL_CONTRACT_HASH_MISMATCH")
            if len(table.buckets) < minimum_bucket_count:
                reasons.append("CALIBRATION_TOO_COARSE")
            if not table.has_valid_score_direction():
                reasons.append("CALIBRATION_DIRECTION_FAILED")
            try:
                registered_metrics = json.loads(
                    str(row["metrics_json"])
                )
                reasons.extend(
                    model_gate_failures(
                        validation=dict(
                            registered_metrics.get("validation")
                            or {}
                        ),
                        portfolio=dict(
                            registered_metrics.get("portfolio")
                            or {}
                        ),
                        config=config,
                    )
                )
            except Exception:
                reasons.append("MODEL_METRICS_INVALID")
            if reasons:
                rejected[key] = list(dict.fromkeys(reasons))
                continue
            accepted[key] = table
        return {
            "calibrations": accepted,
            "rejections": rejected,
        }

    def active_calibrations(self) -> dict[str, CalibrationTable]:
        return dict(
            self.active_calibration_status()["calibrations"]
        )

    def register_model(
        self,
        *,
        calibration: CalibrationTable,
        lifecycle_status: str,
        training_start: date,
        training_end: date,
        validation_start: date,
        validation_end: date,
        feature_schema_hash: str,
        metrics: dict[str, Any],
        config: dict[str, Any],
        activated_at: datetime | None = None,
    ) -> str:
        active_config = load_v3_config()
        supplied_hash = hashlib.sha256(
            _json(config).encode("utf-8")
        ).hexdigest()
        if supplied_hash != current_config_hash():
            raise RuntimeError(
                "MODEL_CONFIG_HASH_MISMATCH: "
                "只能注册当前冻结配置生成的模型"
            )
        required_token = str(
            dict(
                active_config.get("calibration_version_tokens")
                or {}
            ).get(calibration.strategy_key)
            or active_config.get("calibration_version_token")
            or ""
        )
        if (
            required_token
            and required_token not in calibration.model_version
        ):
            raise RuntimeError(
                "MODEL_VERSION_MISMATCH: "
                f"{calibration.model_version} 不属于 {required_token}"
            )
        minimum_bucket_count = int(
            active_config.get("calibration", {}).get(
                "minimum_bucket_count",
                2,
            )
        )
        if len(calibration.buckets) < minimum_bucket_count:
            raise RuntimeError(
                "CALIBRATION_TOO_COARSE: "
                f"至少需要{minimum_bucket_count}个分数桶"
            )
        if not calibration.has_valid_score_direction():
            raise RuntimeError("CALIBRATION_DIRECTION_FAILED")
        if len(calibration.dataset_hash) != 64:
            raise RuntimeError("DATASET_HASH_INVALID")
        if len(str(feature_schema_hash or "")) != 64:
            raise RuntimeError("FEATURE_SCHEMA_HASH_INVALID")
        if (
            calibration.strategy_key == "right_side_trend"
            and str(feature_schema_hash)
            != right_side_model_contract_hash(active_config)
        ):
            raise RuntimeError("MODEL_CONTRACT_HASH_MISMATCH")
        if lifecycle_status == "PAPER_ACTIVE":
            failures = model_gate_failures(
                validation=dict(metrics.get("validation") or {}),
                portfolio=dict(metrics.get("portfolio") or {}),
                config=active_config,
            )
            if failures:
                raise RuntimeError(
                    "MODEL_PRODUCTION_GATE_FAILED: "
                    + ",".join(failures)
                )
        model_id = _uuid()
        now = datetime.now().replace(microsecond=0)
        with self.engine.begin() as connection:
            if lifecycle_status == "PAPER_ACTIVE":
                connection.execute(
                    text(
                        """
                        UPDATE st_model_registry_v3
                        SET lifecycle_status = 'RETIRED'
                        WHERE strategy_key = :strategy_key
                          AND lifecycle_status = 'PAPER_ACTIVE'
                        """
                    ),
                    {"strategy_key": calibration.strategy_key},
                )
            connection.execute(
                text(
                    """
                    INSERT INTO st_model_registry_v3 (
                        model_id, strategy_key, model_version,
                        lifecycle_status, training_start, training_end,
                        validation_start, validation_end, dataset_hash,
                        feature_schema_hash, calibration_json, metrics_json,
                        config_json, created_at, activated_at
                    ) VALUES (
                        :model_id, :strategy_key, :model_version,
                        :lifecycle_status, :training_start, :training_end,
                        :validation_start, :validation_end, :dataset_hash,
                        :feature_schema_hash, :calibration_json, :metrics_json,
                        :config_json, :created_at, :activated_at
                    )
                    """
                ),
                {
                    "model_id": model_id,
                    "strategy_key": calibration.strategy_key,
                    "model_version": calibration.model_version,
                    "lifecycle_status": lifecycle_status,
                    "training_start": training_start,
                    "training_end": training_end,
                    "validation_start": validation_start,
                    "validation_end": validation_end,
                    "dataset_hash": calibration.dataset_hash,
                    "feature_schema_hash": feature_schema_hash,
                    "calibration_json": _json(calibration.as_dict()),
                    "metrics_json": _json(metrics),
                    "config_json": _json(config),
                    "created_at": now,
                    "activated_at": (
                        activated_at
                        if lifecycle_status == "PAPER_ACTIVE"
                        else None
                    ),
                },
            )
        return model_id

    def save_decision(
        self,
        *,
        run_uid: str,
        trade_date: date,
        requested_as_of: date,
        decision_at: datetime,
        mode: str,
        model_version: str,
        lifecycle_status: str,
        regime: dict[str, Any],
        portfolio: dict[str, Any],
        forecasts: Iterable[AlphaForecast],
        data_snapshot_hash: str,
        theme_signals: Iterable[dict[str, Any]] = (),
        hypotheses: Iterable[TradeHypothesis] = (),
        run_status: str = "COMPLETED",
        actionable_status: str | None = None,
        snapshot_manifest: dict[str, Any] | None = None,
        defer_completion: bool = False,
    ) -> dict[str, Any]:
        if isinstance(requested_as_of, datetime) or not isinstance(
            requested_as_of, date
        ):
            raise ValueError("requested_as_of must be a date")
        if snapshot_manifest is not None:
            manifest_requested = str(
                snapshot_manifest.get("requested_as_of") or ""
            ).strip()
            if (
                manifest_requested
                and manifest_requested != requested_as_of.isoformat()
            ):
                raise ValueError(
                    "requested_as_of does not match the frozen snapshot manifest"
                )
        normalized_run_status = str(run_status or "").upper()
        if normalized_run_status not in {"COMPLETED", "BLOCKED"}:
            raise ValueError(
                "decision run_status must be COMPLETED or BLOCKED"
            )
        forecast_rows = list(forecasts)
        raw_theme_signal_rows = list(theme_signals)
        hypothesis_rows = list(hypotheses)
        portfolio = dict(portfolio)
        targets = list(portfolio.get("targets") or [])
        normalized_actionable_status = str(
            actionable_status
            or (
                "DATA_BLOCKED"
                if str(portfolio.get("status") or "") == "DATA_BLOCKED"
                else "PAPER_ACTIONABLE"
                if targets
                else "NO_ACTION"
            )
        ).upper()
        normalized_lifecycle = str(lifecycle_status or "").upper()
        paper_lifecycle = normalized_lifecycle in {
            "PAPER_TRIAL",
            "PAPER_ACTIVE",
        }
        snapshot_verified_at_save = bool(
            isinstance(snapshot_manifest, dict)
            and str(snapshot_manifest.get("manifest_hash") or "")
        )
        portfolio["decision_truth"] = {
            "schema_version": "probiga.trading-v3.decision-truth.v1",
            "run_status": normalized_run_status,
            "actionable_status": normalized_actionable_status,
            "decision_scope": (
                "INTERNAL_PAPER_TRIAL"
                if paper_lifecycle
                else "RESEARCH_ONLY"
            ),
            "paper_order_authority": (
                "V2_GATED"
                if paper_lifecycle
                and snapshot_verified_at_save
                and normalized_run_status == "COMPLETED"
                and normalized_actionable_status == "PAPER_ACTIONABLE"
                else "NONE"
            ),
            "execution_authority": "V2_CANONICAL_LEDGER",
            "order_authority": False,
            "real_order_allowed": False,
        }
        if snapshot_manifest is not None:
            portfolio["decision_snapshot"] = dict(snapshot_manifest)
        persisted_run_status = (
            "PROCESSING" if defer_completion else normalized_run_status
        )
        decision_config_hash = current_config_hash()
        code_commit_sha, _code_version_source = code_version()
        calibration_set_hash = hashlib.sha256(
            _json(
                sorted({
                    (
                        item.strategy_key,
                        item.model_version,
                        item.dataset_hash,
                    )
                    for item in forecast_rows
                    if item.model_version and item.dataset_hash
                })
            ).encode("utf-8")
        ).hexdigest()
        now = datetime.now().replace(microsecond=0)
        ranked = sorted(
            forecast_rows,
            key=lambda item: (
                -float(item.expected_return_net_pct or -10**9),
                -float(item.raw_score or 0),
                item.stock_code,
                item.strategy_key,
            ),
        )
        forecast_ids_by_key: dict[tuple[str, str], str] = {}
        for item in ranked:
            key = (str(item.stock_code), str(item.strategy_key))
            if key in forecast_ids_by_key:
                raise RuntimeError(
                    "duplicate forecast ownership key: "
                    f"{item.stock_code}/{item.strategy_key}"
                )
            forecast_ids_by_key[key] = _uuid()
        theme_signal_policy = dict(
            load_v3_config().get("theme_signals") or {}
        )
        maximum_theme_signal_rows = max(
            1,
            min(
                500_000,
                int(
                    theme_signal_policy.get(
                        "maximum_rows_per_run",
                        150_000,
                    )
                ),
            ),
        )
        top_k_per_theme_strategy = max(
            1,
            min(
                100,
                int(
                    theme_signal_policy.get(
                        "evidence_top_k_per_theme_strategy",
                        10,
                    )
                ),
            ),
        )
        selected_theme_signal_rows, theme_signal_selection = (
            _select_theme_signal_evidence(
                raw_theme_signal_rows,
                forecast_keys=set(forecast_ids_by_key),
                maximum_rows=maximum_theme_signal_rows,
                top_k_per_theme_strategy=top_k_per_theme_strategy,
            )
        )
        theme_signal_rows: list[dict[str, Any]] = []
        for item in selected_theme_signal_rows:
            stock_code = str(item.get("stock_code") or "")
            strategy_key = str(item.get("strategy_key") or "")
            theme_feature_key = str(
                item.get("theme_feature_key") or ""
            )
            identity = (stock_code, strategy_key, theme_feature_key)
            theme_signal_id = hashlib.sha256(
                "|".join((run_uid, *identity)).encode("utf-8")
            ).hexdigest()
            theme_signal_rows.append({
                **item,
                "theme_signal_id": theme_signal_id,
                "run_uid": run_uid,
                "trade_date": trade_date,
                "source_forecast_id": forecast_ids_by_key[
                    (stock_code, strategy_key)
                ],
            })
        target_ownership: dict[str, dict[str, str]] = {}
        for target in targets:
            code = str(target.get("stock_code") or "")
            supporting = sorted({
                str(value)
                for value in (target.get("strategy_keys") or ())
                if str(value) and str(value) != "paper_discovery"
            })
            primary_strategy_key = str(
                target.get("primary_strategy_key") or ""
            )
            if not primary_strategy_key and len(supporting) == 1:
                primary_strategy_key = supporting[0]
            if (
                not code
                or not primary_strategy_key
                or primary_strategy_key not in supporting
            ):
                raise RuntimeError(
                    "ambiguous target sample ownership: "
                    f"{code or 'UNKNOWN'}/{supporting}"
                )
            primary_forecast_id = forecast_ids_by_key.get(
                (code, primary_strategy_key),
                "",
            )
            if not primary_forecast_id:
                raise RuntimeError(
                    "target owner forecast missing from decision run: "
                    f"{code}/{primary_strategy_key}"
                )
            ownership_hash = hashlib.sha256(
                (
                    f"{run_uid}|{primary_forecast_id}|{code}|"
                    f"{primary_strategy_key}"
                ).encode("utf-8")
            ).hexdigest()
            target_ownership[code] = {
                "primary_strategy_key": primary_strategy_key,
                "primary_forecast_id": primary_forecast_id,
                "attribution_snapshot_hash": ownership_hash,
            }
        target_ledger = canonical_target_ledger(
            targets,
            run_uid=run_uid,
            trade_date=trade_date,
            ownership_by_code=target_ownership,
        )
        portfolio["decision_integrity"] = {
            "schema_version": (
                "probiga.trading-v3.decision-integrity.v1"
            ),
            "forecast_count": len(forecast_rows),
            "raw_theme_signal_count": len(raw_theme_signal_rows),
            "persisted_theme_signal_count": len(theme_signal_rows),
            "hypothesis_count": len(hypothesis_rows),
            "target_count": len(targets),
            "target_ledger_hash": canonical_hash(target_ledger),
        }
        result_hash = decision_result_hash(
            regime=regime,
            portfolio=portfolio,
            forecast_count=len(forecast_rows),
            theme_signal_count=len(raw_theme_signal_rows),
            hypothesis_count=len(hypothesis_rows),
        )
        shadow_rows = build_shadow_portfolio_rows(
            ranked,
            run_uid=run_uid,
            trade_date=trade_date,
            forecast_ids=forecast_ids_by_key,
            policy=load_v3_config().get("shadow_portfolios", {}),
            theme_signals=theme_signal_rows,
        )
        save_started_at = time.perf_counter()
        _save_progress(
            "prepared",
            save_started_at,
            forecast_count=len(ranked),
            theme_signal_evaluated_count=theme_signal_selection[
                "evaluated_count"
            ],
            theme_signal_retained_count=len(theme_signal_rows),
            exact_theme_strategy_group_count=theme_signal_selection[
                "exact_theme_strategy_group_count"
            ],
            shadow_position_count=len(shadow_rows),
        )
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO st_decision_run_v3 (
                        run_uid, trade_date, requested_as_of,
                        decision_at, mode,
                        model_version, config_hash, code_commit_sha,
                        calibration_set_hash, lifecycle_status, status,
                        dominant_regime, risk_asset_cap, regime_json,
                        portfolio_json, forecast_count, validated_count,
                        target_count, data_snapshot_hash, result_hash,
                        created_at, completed_at
                    ) VALUES (
                        :run_uid, :trade_date, :requested_as_of,
                        :decision_at, :mode,
                        :model_version, :config_hash, :code_commit_sha,
                        :calibration_set_hash, :lifecycle_status, :status,
                        :dominant_regime, :risk_asset_cap, :regime_json,
                        :portfolio_json, :forecast_count, :validated_count,
                        :target_count, :data_snapshot_hash, :result_hash,
                        :created_at, :completed_at
                    )
                    """
                ),
                {
                    "run_uid": run_uid,
                    "trade_date": trade_date,
                    "requested_as_of": requested_as_of,
                    "decision_at": decision_at,
                    "mode": mode,
                    "model_version": model_version,
                    "config_hash": decision_config_hash,
                    "code_commit_sha": code_commit_sha,
                    "calibration_set_hash": calibration_set_hash,
                    "lifecycle_status": lifecycle_status,
                    "status": persisted_run_status,
                    "dominant_regime": str(
                        regime.get("dominant_state") or "DATA_BLOCKED"
                    ),
                    "risk_asset_cap": float(
                        regime.get("risk_asset_cap") or 0
                    ),
                    "regime_json": _json(regime),
                    "portfolio_json": _json(portfolio),
                    "forecast_count": len(ranked),
                    "validated_count": sum(
                        item.status == "VALIDATED_POSITIVE"
                        for item in ranked
                    ),
                    "target_count": len(targets),
                    "data_snapshot_hash": data_snapshot_hash,
                    "result_hash": result_hash,
                    "created_at": now,
                    "completed_at": None if defer_completion else now,
                },
            )
            forecast_insert = text(
                """
                INSERT INTO st_alpha_forecast_v3 (
                    forecast_id, run_uid, trade_date, rank_no,
                    stock_code, short_name, strategy_key,
                    horizon_days, raw_score,
                    expected_return_net_pct, return_q10_pct,
                    return_q50_pct, return_q90_pct,
                    probability_positive, expected_mae_pct,
                    expected_mfe_pct, profit_factor, payoff_ratio,
                    sample_count, confidence, forecast_status,
                    theme_code, model_version, dataset_hash,
                    feature_time, valid_until, initial_stop_pct,
                    reasons_json, features_json, created_at
                ) VALUES (
                    :forecast_id, :run_uid, :trade_date, :rank_no,
                    :stock_code, :short_name, :strategy_key,
                    :horizon_days, :raw_score,
                    :expected_return_net_pct, :return_q10_pct,
                    :return_q50_pct, :return_q90_pct,
                    :probability_positive, :expected_mae_pct,
                    :expected_mfe_pct, :profit_factor, :payoff_ratio,
                    :sample_count, :confidence, :forecast_status,
                    :theme_code, :model_version, :dataset_hash,
                    :feature_time, :valid_until, :initial_stop_pct,
                    :reasons_json, :features_json, :created_at
                )
                """
            )
            for batch_start in range(0, len(ranked), 500):
                batch = ranked[batch_start : batch_start + 500]
                parameters = []
                for offset, item in enumerate(batch, batch_start + 1):
                    parameters.append({
                        "forecast_id": forecast_ids_by_key[(
                            str(item.stock_code),
                            str(item.strategy_key),
                        )],
                        "run_uid": run_uid,
                        "trade_date": trade_date,
                        "rank_no": offset,
                        "stock_code": item.stock_code,
                        "short_name": item.stock_name,
                        "strategy_key": item.strategy_key,
                        "horizon_days": item.horizon_days,
                        "raw_score": _decimal(item.raw_score),
                        "expected_return_net_pct": _decimal(
                            item.expected_return_net_pct
                        ),
                        "return_q10_pct": _decimal(item.return_q10_pct),
                        "return_q50_pct": _decimal(item.return_q50_pct),
                        "return_q90_pct": _decimal(item.return_q90_pct),
                        "probability_positive": _decimal(
                            item.probability_positive
                        ),
                        "expected_mae_pct": _decimal(
                            item.expected_mae_pct
                        ),
                        "expected_mfe_pct": _decimal(
                            item.expected_mfe_pct
                        ),
                        "profit_factor": _decimal(item.profit_factor),
                        "payoff_ratio": _decimal(item.payoff_ratio),
                        "sample_count": item.sample_count,
                        "confidence": item.confidence,
                        "forecast_status": item.status,
                        "theme_code": item.theme_code,
                        "model_version": item.model_version,
                        "dataset_hash": item.dataset_hash,
                        "feature_time": item.feature_time,
                        "valid_until": item.valid_until,
                        "initial_stop_pct": item.initial_stop_pct,
                        "reasons_json": _json(item.reasons),
                        "features_json": _json(item.features),
                        "created_at": now,
                    })
                connection.execute(
                    forecast_insert,
                    parameters,
                )
            _save_progress(
                "forecasts_inserted",
                save_started_at,
                forecast_count=len(ranked),
            )
            theme_signal_insert = text(
                """
                INSERT INTO st_theme_signal_v3 (
                    theme_signal_id, run_uid, trade_date,
                    source_forecast_id, theme_feature_key,
                    stock_code, short_name, strategy_key,
                    theme_code, theme_name, theme_source,
                    theme_cluster_keys_json, horizon_days,
                    raw_score, signal_status, forecast_status,
                    expected_return_net_pct, selected_as_primary,
                    feature_time, valid_until, features_json,
                    created_at
                ) VALUES (
                    :theme_signal_id, :run_uid, :trade_date,
                    :source_forecast_id, :theme_feature_key,
                    :stock_code, :short_name, :strategy_key,
                    :theme_code, :theme_name, :theme_source,
                    :theme_cluster_keys_json, :horizon_days,
                    :raw_score, :signal_status, :forecast_status,
                    :expected_return_net_pct,
                    :selected_as_primary, :feature_time,
                    :valid_until, :features_json, :created_at
                )
                """
            )
            theme_signal_parameters = [
                {
                    **item,
                    "theme_cluster_keys_json": _json(
                        item.get("theme_cluster_keys") or ()
                    ),
                    "raw_score": _decimal(item.get("raw_score")),
                    "expected_return_net_pct": _decimal(
                        item.get("expected_return_net_pct")
                    ),
                    "features_json": _json(item.get("features") or {}),
                    "created_at": now,
                }
                for item in theme_signal_rows
            ]
            for offset in range(0, len(theme_signal_parameters), 1000):
                connection.execute(
                    theme_signal_insert,
                    theme_signal_parameters[offset : offset + 1000],
                )
            _save_progress(
                "theme_signals_inserted",
                save_started_at,
                theme_signal_count=len(theme_signal_rows),
            )
            shadow_insert = text(
                """
                INSERT INTO st_shadow_portfolio_v3 (
                    shadow_position_id, run_uid, trade_date,
                    portfolio_kind, group_key, rank_no,
                    source_forecast_id, strategy_result_key,
                    source_theme_signal_id,
                    stock_code, short_name, strategy_key,
                    theme_code, horizon_days, selection_score,
                    valid_until, evidence_kind, protocol_version,
                    order_allowed, can_activate_model,
                    result_status, created_at, updated_at
                ) VALUES (
                    :shadow_position_id, :run_uid, :trade_date,
                    :portfolio_kind, :group_key, :rank_no,
                    :source_forecast_id, :strategy_result_key,
                    :source_theme_signal_id,
                    :stock_code, :short_name, :strategy_key,
                    :theme_code, :horizon_days, :selection_score,
                    :valid_until, :evidence_kind, :protocol_version,
                    :order_allowed, :can_activate_model,
                    :result_status, :created_at, :updated_at
                )
                """
            )
            shadow_parameters = [
                {
                    **item,
                    "created_at": now,
                    "updated_at": now,
                }
                for item in shadow_rows
            ]
            for offset in range(0, len(shadow_parameters), 1000):
                connection.execute(
                    shadow_insert,
                    shadow_parameters[offset : offset + 1000],
                )
            _save_progress(
                "shadow_positions_inserted",
                save_started_at,
                shadow_position_count=len(shadow_rows),
            )
            for rank_no, item in enumerate(targets, 1):
                connection.execute(
                    text(
                        """
                        INSERT INTO st_target_portfolio_v3 (
                            target_id, run_uid, trade_date, rank_no,
                            stock_code, short_name, target_weight,
                            target_value, target_quantity,
                            estimated_roundtrip_cost_pct,
                            expected_return_net_pct,
                            conservative_return_pct, expected_mae_pct,
                            theme_code, theme_codes_json,
                            strategy_keys_json, primary_strategy_key,
                            primary_forecast_id,
                            attribution_snapshot_hash, reason,
                            status, created_at
                        ) VALUES (
                            :target_id, :run_uid, :trade_date, :rank_no,
                            :stock_code, :short_name, :target_weight,
                            :target_value, :target_quantity,
                            :estimated_roundtrip_cost_pct,
                            :expected_return_net_pct,
                            :conservative_return_pct, :expected_mae_pct,
                            :theme_code, :theme_codes_json,
                            :strategy_keys_json, :primary_strategy_key,
                            :primary_forecast_id,
                            :attribution_snapshot_hash, :reason,
                            'PLANNED', :created_at
                        )
                        """
                    ),
                    {
                        "target_id": _uuid(),
                        "run_uid": run_uid,
                        "trade_date": trade_date,
                        "rank_no": rank_no,
                        "stock_code": item["stock_code"],
                        "short_name": item["stock_name"],
                        "target_weight": item["target_weight"],
                        "target_value": item["target_value"],
                        "target_quantity": item["target_quantity"],
                        "estimated_roundtrip_cost_pct": item[
                            "estimated_roundtrip_cost_pct"
                        ],
                        "expected_return_net_pct": item[
                            "expected_return_net_pct"
                        ],
                        "conservative_return_pct": item[
                            "conservative_return_pct"
                        ],
                        "expected_mae_pct": item["expected_mae_pct"],
                        "theme_code": item.get("theme_code") or "",
                        "theme_codes_json": _json(
                            item.get("theme_codes") or []
                        ),
                        "strategy_keys_json": _json(
                            item.get("strategy_keys") or []
                        ),
                        **target_ownership[str(item["stock_code"])],
                        "reason": item.get("reason") or "",
                        "created_at": now,
                    },
                )
            for item in hypothesis_rows:
                connection.execute(
                    text(
                        """
                        INSERT INTO st_trade_hypothesis_v3 (
                            hypothesis_id, hypothesis_key, run_uid,
                            trade_date, scope_type, scope_code, scope_name,
                            direction, state, prior_probability,
                            current_probability, probability_kind,
                            confidence, score, horizon_minutes,
                            alpha_half_life_minutes, proposed_action,
                            max_position_weight, theme_code, role, thesis,
                            counter_thesis, supporting_evidence_json,
                            opposing_evidence_json, triggers_json,
                            invalidations_json, strategy_keys_json,
                            feature_time, valid_until,
                            source_forecast_count, last_evidence_at,
                            created_at, updated_at
                        ) VALUES (
                            :hypothesis_id, :hypothesis_key, :run_uid,
                            :trade_date, :scope_type, :scope_code, :scope_name,
                            :direction, :state, :prior_probability,
                            :current_probability, :probability_kind,
                            :confidence, :score, :horizon_minutes,
                            :alpha_half_life_minutes, :proposed_action,
                            :max_position_weight, :theme_code, :role, :thesis,
                            :counter_thesis, :supporting_evidence_json,
                            :opposing_evidence_json, :triggers_json,
                            :invalidations_json, :strategy_keys_json,
                            :feature_time, :valid_until,
                            :source_forecast_count, NULL, :created_at,
                            :updated_at
                        )
                        """
                    ),
                    {
                        "hypothesis_id": _uuid(),
                        "hypothesis_key": item.hypothesis_key,
                        "run_uid": item.run_uid,
                        "trade_date": item.trade_date,
                        "scope_type": item.scope_type,
                        "scope_code": item.scope_code,
                        "scope_name": item.scope_name,
                        "direction": item.direction,
                        "state": item.state,
                        "prior_probability": item.prior_probability,
                        "current_probability": item.probability,
                        "probability_kind": item.probability_kind,
                        "confidence": item.confidence,
                        "score": item.score,
                        "horizon_minutes": item.horizon_minutes,
                        "alpha_half_life_minutes": (
                            item.alpha_half_life_minutes
                        ),
                        "proposed_action": item.proposed_action,
                        "max_position_weight": item.max_position_weight,
                        "theme_code": item.theme_code,
                        "role": item.role,
                        "thesis": item.thesis,
                        "counter_thesis": item.counter_thesis,
                        "supporting_evidence_json": _json(
                            item.supporting_evidence
                        ),
                        "opposing_evidence_json": _json(
                            item.opposing_evidence
                        ),
                        "triggers_json": _json(item.triggers),
                        "invalidations_json": _json(item.invalidations),
                        "strategy_keys_json": _json(item.strategy_keys),
                        "feature_time": item.feature_time,
                        "valid_until": item.valid_until,
                        "source_forecast_count": (
                            item.source_forecast_count
                        ),
                        "created_at": now,
                        "updated_at": now,
                    },
                )
            _save_progress(
                "transaction_ready",
                save_started_at,
                target_count=len(targets),
                hypothesis_count=len(hypothesis_rows),
            )
        _save_progress("committed", save_started_at)
        return {
            "run_uid": run_uid,
            "run_status": normalized_run_status,
            "persisted_run_status": persisted_run_status,
            "actionable_status": normalized_actionable_status,
            "snapshot_manifest_hash": str(
                (snapshot_manifest or {}).get("manifest_hash") or ""
            ),
            "result_hash": result_hash,
            "forecast_count": len(ranked),
            "theme_signal_count": len(theme_signal_rows),
            "theme_signal_truncated_count": max(
                0,
                len(raw_theme_signal_rows) - len(theme_signal_rows),
            ),
            "theme_signal_evaluated_count": theme_signal_selection[
                "evaluated_count"
            ],
            "theme_signal_exact_group_count": theme_signal_selection[
                "exact_theme_strategy_group_count"
            ],
            "validated_count": sum(
                item.status == "VALIDATED_POSITIVE" for item in ranked
            ),
            "target_count": len(targets),
            "shadow_position_count": len(shadow_rows),
            "hypothesis_count": len(hypothesis_rows),
        }

    def mark_run_failed(
        self,
        run_uid: str,
        *,
        stage: str,
        error: BaseException | str,
    ) -> None:
        """Make a committed decision visibly non-successful after a saga step."""

        message = f"{str(stage or 'DOWNSTREAM').upper()}: {error}"[:4000]
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE st_decision_run_v3
                    SET status = 'FAILED',
                        error_message = :error_message,
                        completed_at = :completed_at
                    WHERE run_uid = :run_uid
                      AND status IN (
                          'PROCESSING', 'COMPLETED', 'BLOCKED'
                      )
                    """
                ),
                {
                    "run_uid": run_uid,
                    "error_message": message,
                    "completed_at": datetime.now().replace(microsecond=0),
                },
            )

    def finalize_run(self, run_uid: str, *, status: str) -> None:
        normalized = str(status or "").upper()
        if normalized not in {"COMPLETED", "BLOCKED"}:
            raise ValueError("final run status must be COMPLETED or BLOCKED")
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    UPDATE st_decision_run_v3
                    SET status = :status,
                        error_message = NULL,
                        completed_at = :completed_at
                    WHERE run_uid = :run_uid
                      AND status = 'PROCESSING'
                    """
                ),
                {
                    "run_uid": run_uid,
                    "status": normalized,
                    "completed_at": datetime.now().replace(microsecond=0),
                },
            )

    def _latest_run(
        self,
        trade_date: date | None = None,
    ) -> dict[str, Any] | None:
        where = ""
        params: dict[str, Any] = {}
        if trade_date is not None:
            dialect_name = str(
                getattr(getattr(self.engine, "dialect", None), "name", "")
            ).casefold()
            if dialect_name == "sqlite":
                requested_expression = (
                    "COALESCE(requested_as_of, "
                    "json_extract(portfolio_json, "
                    "'$.decision_snapshot.requested_as_of'), "
                    "date(decision_at))"
                )
            else:
                requested_expression = (
                    "COALESCE(requested_as_of, "
                    "STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT("
                    "portfolio_json, '$.decision_snapshot.requested_as_of'"
                    ")), '%Y-%m-%d'), DATE(decision_at))"
                )
            where = f"WHERE {requested_expression} = :trade_date"
            params["trade_date"] = trade_date
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    f"""
                    SELECT *
                    FROM st_decision_run_v3
                    {where}
                    ORDER BY decision_at DESC, created_at DESC
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()
        if not row:
            return None
        result = dict(row)
        if result.get("requested_as_of") is None:
            snapshot = {}
            try:
                snapshot = json.loads(
                    str(result.get("portfolio_json") or "{}")
                ).get("decision_snapshot") or {}
            except (TypeError, ValueError):
                snapshot = {}
            decision_value = result["decision_at"]
            decision_date = (
                decision_value.date()
                if isinstance(decision_value, datetime)
                else date.fromisoformat(str(decision_value)[:10])
            )
            result["requested_as_of"] = snapshot.get(
                "requested_as_of"
            ) or decision_date
        for key in ("regime_json", "portfolio_json"):
            result[key.removesuffix("_json")] = json.loads(
                str(result.pop(key))
            )
        return result

    def overview(self) -> dict[str, Any]:
        run = self._latest_run()
        with self.engine.connect() as connection:
            positions = connection.execute(
                text(
                    """
                    SELECT *
                    FROM st_position_state_v3
                    WHERE account_id = 'paper-main-v2'
                      AND quantity > 0
                    ORDER BY current_weight DESC, stock_code
                    """
                )
            ).mappings().all()
        return {
            "run": run,
            "validation": self.latest_validation(),
            "positions": [dict(item) for item in positions],
            "real_trading_enabled": False,
        }

    def latest_forecasts(
        self,
        *,
        limit: int = 200,
        status: str = "",
        trade_date: date | None = None,
        strategy_key: str = "",
        query: str = "",
    ) -> list[dict[str, Any]]:
        run = self._latest_run(trade_date)
        if not run:
            return []
        where = "run_uid = :run_uid"
        params: dict[str, Any] = {
            "run_uid": run["run_uid"],
            "limit": max(1, min(5000, int(limit))),
        }
        if status:
            where += " AND forecast_status = :status"
            params["status"] = status
        if strategy_key:
            where += " AND strategy_key = :strategy_key"
            params["strategy_key"] = strategy_key
        if query:
            normalized_query = str(query).strip()
            if normalized_query.isdigit():
                # Numeric searches are security-code searches.  Matching a
                # six-digit number against JSON decimals produced unrelated
                # candidates and hid the exact stock in noisy results.
                where += " AND stock_code LIKE :query"
            else:
                where += (
                    " AND (stock_code LIKE :query "
                    "OR short_name LIKE :query "
                    "OR theme_code LIKE :query "
                    "OR features_json LIKE :query)"
                )
            params["query"] = f"%{normalized_query}%"
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    f"""
                    SELECT *
                    FROM st_alpha_forecast_v3
                    WHERE {where}
                    ORDER BY rank_no, stock_code, strategy_key
                    LIMIT :limit
                    """
                ),
                params,
            ).mappings().all()
        result = []
        for row in rows:
            item = dict(row)
            item["reasons"] = json.loads(
                str(item.pop("reasons_json") or "[]")
            )
            item["features"] = json.loads(
                str(item.pop("features_json") or "{}")
            )
            result.append(item)
        return result

    @staticmethod
    def _hypothesis_row(
        row: dict[str, Any],
    ) -> dict[str, Any]:
        item = dict(row)
        item["probability"] = float(
            item.pop("current_probability") or 0.0
        )
        for source, target in (
            ("supporting_evidence_json", "supporting_evidence"),
            ("opposing_evidence_json", "opposing_evidence"),
            ("triggers_json", "triggers"),
            ("invalidations_json", "invalidations"),
            ("strategy_keys_json", "strategy_keys"),
        ):
            item[target] = json.loads(str(item.pop(source) or "[]"))
        return item

    @classmethod
    def _hypothesis_domain(
        cls,
        row: dict[str, Any],
    ) -> TradeHypothesis:
        item = cls._hypothesis_row(row)
        return TradeHypothesis(
            hypothesis_key=str(item["hypothesis_key"]),
            run_uid=str(item["run_uid"]),
            trade_date=str(item["trade_date"])[:10],
            scope_type=str(item["scope_type"]),
            scope_code=str(item["scope_code"]),
            scope_name=str(item["scope_name"]),
            direction=str(item["direction"]),
            state=str(item["state"]),
            probability=float(item["probability"]),
            prior_probability=float(item["prior_probability"]),
            probability_kind=str(item["probability_kind"]),
            confidence=float(item["confidence"]),
            score=float(item["score"]),
            horizon_minutes=int(item["horizon_minutes"]),
            alpha_half_life_minutes=int(
                item["alpha_half_life_minutes"]
            ),
            proposed_action=str(item["proposed_action"]),
            max_position_weight=float(item["max_position_weight"]),
            theme_code=str(item["theme_code"] or ""),
            role=str(item["role"]),
            thesis=str(item["thesis"]),
            counter_thesis=str(item["counter_thesis"]),
            supporting_evidence=tuple(
                item["supporting_evidence"]
            ),
            opposing_evidence=tuple(item["opposing_evidence"]),
            triggers=tuple(item["triggers"]),
            invalidations=tuple(item["invalidations"]),
            strategy_keys=tuple(item["strategy_keys"]),
            feature_time=item["feature_time"],
            valid_until=item["valid_until"],
            source_forecast_count=int(
                item["source_forecast_count"] or 0
            ),
        )

    def stock_pool(
        self,
        *,
        trade_date: date | None = None,
    ) -> dict[str, Any]:
        """Return one read-only, de-duplicated stock-pool snapshot.

        Forecasts are emitted once for each strategy sleeve, while the desk
        needs to reason about a security only once. This projection keeps the
        immutable run data intact and only consolidates it for display.
        """
        run = self._latest_run(trade_date)
        if not run:
            return {
                "run_uid": None,
                "trade_date": trade_date.isoformat() if trade_date else None,
                "generated_at": None,
                "items": [],
                "summary": {
                    "stock_count": 0,
                    "forecast_count": 0,
                    "strategy_candidate_count": 0,
                    "target_count": 0,
                    "rejected_count": 0,
                },
            }

        with self.engine.connect() as connection:
            forecast_rows = connection.execute(
                text(
                    """
                    SELECT forecast_id, rank_no, stock_code, short_name,
                           strategy_key, raw_score,
                           expected_return_net_pct, probability_positive,
                           confidence, forecast_status, theme_code,
                           valid_until, reasons_json
                    FROM st_alpha_forecast_v3
                    WHERE run_uid = :run_uid
                    ORDER BY rank_no, stock_code, strategy_key
                    """
                ),
                {"run_uid": run["run_uid"]},
            ).mappings().all()
            target_rows = connection.execute(
                text(
                    """
                    SELECT *
                    FROM st_target_portfolio_v3
                    WHERE run_uid = :run_uid
                    ORDER BY rank_no, stock_code
                    """
                ),
                {"run_uid": run["run_uid"]},
            ).mappings().all()

        def _list_json(value: Any) -> list[Any]:
            try:
                parsed = json.loads(str(value or "[]"))
            except (TypeError, ValueError, json.JSONDecodeError):
                return []
            return parsed if isinstance(parsed, list) else []

        pool: dict[str, dict[str, Any]] = {}

        def _item_for(stock_code: Any, stock_name: Any = "") -> dict[str, Any]:
            code = str(stock_code or "").zfill(6)
            item = pool.get(code)
            if item is None:
                item = {
                    "stock_code": code,
                    "stock_name": str(stock_name or code),
                    "rank_no": None,
                    "strategy_keys": [],
                    "theme_codes": [],
                    "forecast_statuses": [],
                    "raw_score": None,
                    "expected_return_net_pct": None,
                    "probability_positive": None,
                    "confidence": None,
                    "valid_until": None,
                    "reasons": [],
                    "is_strategy_candidate": False,
                    "target": None,
                    "rejection": None,
                }
                pool[code] = item
            elif stock_name and (
                not item["stock_name"]
                or item["stock_name"] == item["stock_code"]
            ):
                item["stock_name"] = str(stock_name)
            return item

        candidate_statuses = {
            "VALIDATED_POSITIVE",
            "PAPER_DISCOVERY_CANDIDATE",
            "LEFT_SIDE_PREPARE",
        }
        for row in forecast_rows:
            forecast = dict(row)
            item = _item_for(
                forecast.get("stock_code"),
                forecast.get("short_name"),
            )
            rank_no = forecast.get("rank_no")
            if rank_no is not None and (
                item["rank_no"] is None
                or int(rank_no) < int(item["rank_no"])
            ):
                item["rank_no"] = int(rank_no)
            for field in ("strategy_key", "theme_code", "forecast_status"):
                value = str(forecast.get(field) or "").strip()
                target_field = {
                    "strategy_key": "strategy_keys",
                    "theme_code": "theme_codes",
                    "forecast_status": "forecast_statuses",
                }[field]
                if value and value not in item[target_field]:
                    item[target_field].append(value)
            if str(forecast.get("forecast_status") or "") in candidate_statuses:
                item["is_strategy_candidate"] = True
            for field in (
                "raw_score",
                "expected_return_net_pct",
                "probability_positive",
                "confidence",
            ):
                value = forecast.get(field)
                if value is not None and (
                    item[field] is None or float(value) > float(item[field])
                ):
                    item[field] = float(value)
            valid_until = forecast.get("valid_until")
            if valid_until is not None:
                serialized = str(valid_until)
                if (
                    item["valid_until"] is None
                    or serialized > str(item["valid_until"])
                ):
                    item["valid_until"] = serialized
            for reason in _list_json(forecast.get("reasons_json")):
                text_reason = str(reason).strip()
                if text_reason and text_reason not in item["reasons"]:
                    item["reasons"].append(text_reason)

        for row in target_rows:
            target = dict(row)
            item = _item_for(
                target.get("stock_code"),
                target.get("short_name"),
            )
            item["is_strategy_candidate"] = True
            target_projection = {
                key: target.get(key)
                for key in (
                    "rank_no",
                    "target_weight",
                    "target_value",
                    "target_quantity",
                    "expected_return_net_pct",
                    "conservative_return_pct",
                    "expected_mae_pct",
                    "theme_code",
                    "reason",
                    "status",
                )
            }
            target_projection["strategy_keys"] = _list_json(
                target.get("strategy_keys_json")
            )
            target_projection["theme_codes"] = _list_json(
                target.get("theme_codes_json")
            )
            item["target"] = target_projection
            for strategy_key in target_projection["strategy_keys"]:
                value = str(strategy_key).strip()
                if value and value not in item["strategy_keys"]:
                    item["strategy_keys"].append(value)
            for theme_code in target_projection["theme_codes"]:
                value = str(theme_code).strip()
                if value and value not in item["theme_codes"]:
                    item["theme_codes"].append(value)
            for value in (
                target_projection.get("theme_code"),
                target_projection.get("reason"),
            ):
                normalized = str(value or "").strip()
                if normalized and normalized not in item["reasons"]:
                    item["reasons"].append(normalized)

        for rejection in list((run.get("portfolio") or {}).get("rejected") or []):
            if not isinstance(rejection, dict):
                continue
            code = rejection.get("stock_code")
            if not code:
                continue
            item = _item_for(
                code,
                rejection.get("short_name") or rejection.get("stock_name"),
            )
            item["rejection"] = {
                "reason_code": str(rejection.get("reason_code") or ""),
                "reason": str(rejection.get("reason") or ""),
            }
            strategy_key = str(rejection.get("strategy_key") or "").strip()
            if strategy_key and strategy_key not in item["strategy_keys"]:
                item["strategy_keys"].append(strategy_key)

        items = list(pool.values())
        for item in items:
            item["reasons"] = item["reasons"][:8]
            item["strategy_keys"].sort()
            item["theme_codes"].sort()
            item["forecast_statuses"].sort()
        items.sort(
            key=lambda item: (
                0 if item["target"] else 1,
                0 if item["is_strategy_candidate"] else 1,
                0 if item["rejection"] else 1,
                item["rank_no"] if item["rank_no"] is not None else 999999,
                item["stock_code"],
            )
        )
        return {
            "run_uid": run["run_uid"],
            "trade_date": str(run["trade_date"]),
            "generated_at": str(
                run.get("completed_at") or run.get("decision_at") or ""
            ),
            "items": items,
            "summary": {
                "stock_count": len(items),
                "forecast_count": len(forecast_rows),
                "strategy_candidate_count": sum(
                    1 for item in items if item["is_strategy_candidate"]
                ),
                "target_count": sum(
                    1 for item in items if item["target"]
                ),
                "rejected_count": sum(
                    1 for item in items if item["rejection"]
                ),
            },
        }

    @staticmethod
    def _hypothesis_row(
        row: dict[str, Any],
    ) -> dict[str, Any]:
        item = dict(row)
        item["probability"] = float(
            item.pop("current_probability") or 0.0
        )
        for source, target in (
            ("supporting_evidence_json", "supporting_evidence"),
            ("opposing_evidence_json", "opposing_evidence"),
            ("triggers_json", "triggers"),
            ("invalidations_json", "invalidations"),
            ("strategy_keys_json", "strategy_keys"),
        ):
            item[target] = json.loads(str(item.pop(source) or "[]"))
        return item

    @classmethod
    def _hypothesis_domain(
        cls,
        row: dict[str, Any],
    ) -> TradeHypothesis:
        item = cls._hypothesis_row(row)
        return TradeHypothesis(
            hypothesis_key=str(item["hypothesis_key"]),
            run_uid=str(item["run_uid"]),
            trade_date=str(item["trade_date"])[:10],
            scope_type=str(item["scope_type"]),
            scope_code=str(item["scope_code"]),
            scope_name=str(item["scope_name"]),
            direction=str(item["direction"]),
            state=str(item["state"]),
            probability=float(item["probability"]),
            prior_probability=float(item["prior_probability"]),
            probability_kind=str(item["probability_kind"]),
            confidence=float(item["confidence"]),
            score=float(item["score"]),
            horizon_minutes=int(item["horizon_minutes"]),
            alpha_half_life_minutes=int(
                item["alpha_half_life_minutes"]
            ),
            proposed_action=str(item["proposed_action"]),
            max_position_weight=float(item["max_position_weight"]),
            theme_code=str(item["theme_code"] or ""),
            role=str(item["role"]),
            thesis=str(item["thesis"]),
            counter_thesis=str(item["counter_thesis"]),
            supporting_evidence=tuple(
                item["supporting_evidence"]
            ),
            opposing_evidence=tuple(item["opposing_evidence"]),
            triggers=tuple(item["triggers"]),
            invalidations=tuple(item["invalidations"]),
            strategy_keys=tuple(item["strategy_keys"]),
            feature_time=item["feature_time"],
            valid_until=item["valid_until"],
            source_forecast_count=int(
                item["source_forecast_count"] or 0
            ),
        )
    def latest_hypotheses(
        self,
        *,
        limit: int = 300,
        trade_date: date | None = None,
        scope_type: str = "",
        state: str = "",
        query: str = "",
    ) -> list[dict[str, Any]]:
        run = self._latest_run(trade_date)
        if not run:
            return []
        where = "run_uid = :run_uid"
        params: dict[str, Any] = {
            "run_uid": run["run_uid"],
            "limit": max(1, min(1000, int(limit))),
        }
        if scope_type:
            where += " AND scope_type = :scope_type"
            params["scope_type"] = scope_type
        if state:
            where += " AND state = :state"
            params["state"] = state
        if query:
            where += (
                " AND (scope_code LIKE :query "
                "OR scope_name LIKE :query "
                "OR theme_code LIKE :query)"
            )
            params["query"] = f"%{query}%"
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    f"""
                    SELECT *
                    FROM st_trade_hypothesis_v3
                    WHERE {where}
                    ORDER BY
                        CASE state
                            WHEN 'ACTIVE' THEN 0
                            WHEN 'TRIGGER_READY' THEN 1
                            WHEN 'PREPARE' THEN 2
                            WHEN 'WATCH' THEN 3
                            WHEN 'WEAKEN' THEN 4
                            ELSE 5
                        END,
                        current_probability DESC,
                        score DESC,
                        scope_code
                    LIMIT :limit
                    """
                ),
                params,
            ).mappings().all()
        return [self._hypothesis_row(dict(row)) for row in rows]

    def latest_run_metadata(
        self,
        trade_date: date | None = None,
    ) -> dict[str, Any] | None:
        """Return the current immutable decision-run metadata."""
        run = self._latest_run(trade_date)
        if not run:
            return None
        run["decision_integrity_verified"] = False
        run["decision_integrity_reason"] = (
            "DECISION_RESULT_OR_TARGET_LEDGER_UNVERIFIED"
        )
        try:
            portfolio = dict(run.get("portfolio") or {})
            integrity = dict(portfolio.get("decision_integrity") or {})
            if str(integrity.get("schema_version") or "") != (
                "probiga.trading-v3.decision-integrity.v1"
            ):
                return run
            run_uid = str(run.get("run_uid") or "")
            source_date = run.get("trade_date")
            source_date = (
                source_date
                if isinstance(source_date, date)
                else date.fromisoformat(str(source_date))
            )
            with self.engine.connect() as connection:
                targets = [
                    dict(row)
                    for row in connection.execute(
                        text(
                            """
                            SELECT *
                            FROM st_target_portfolio_v3
                            WHERE run_uid = :run_uid
                            ORDER BY rank_no, stock_code
                            """
                        ),
                        {"run_uid": run_uid},
                    ).mappings().all()
                ]
                counts = connection.execute(
                    text(
                        """
                        SELECT
                            (SELECT COUNT(*) FROM st_alpha_forecast_v3
                             WHERE run_uid = :run_uid) AS forecast_count,
                            (SELECT COUNT(*) FROM st_theme_signal_v3
                             WHERE run_uid = :run_uid) AS theme_signal_count,
                            (SELECT COUNT(*) FROM st_trade_hypothesis_v3
                             WHERE run_uid = :run_uid) AS hypothesis_count
                        """
                    ),
                    {"run_uid": run_uid},
                ).mappings().first()
            ledger = canonical_target_ledger(
                targets,
                run_uid=run_uid,
                trade_date=source_date,
                persisted=True,
            )
            forecast_count = int(integrity.get("forecast_count") or 0)
            hypothesis_count = int(integrity.get("hypothesis_count") or 0)
            checks = (
                canonical_hash(ledger)
                == str(integrity.get("target_ledger_hash") or ""),
                len(targets) == int(integrity.get("target_count") or 0),
                len(targets) == int(run.get("target_count") or 0),
                bool(counts),
                int((counts or {}).get("forecast_count") or 0)
                == forecast_count,
                int((counts or {}).get("theme_signal_count") or 0)
                == int(
                    integrity.get("persisted_theme_signal_count") or 0
                ),
                int((counts or {}).get("hypothesis_count") or 0)
                == hypothesis_count,
                int(run.get("forecast_count") or 0) == forecast_count,
                decision_result_hash(
                    regime=dict(run.get("regime") or {}),
                    portfolio=portfolio,
                    forecast_count=forecast_count,
                    theme_signal_count=int(
                        integrity.get("raw_theme_signal_count") or 0
                    ),
                    hypothesis_count=hypothesis_count,
                )
                == str(run.get("result_hash") or ""),
            )
            if all(checks):
                run["decision_integrity_verified"] = True
                run["decision_integrity_reason"] = ""
        except (
            AttributeError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
            SQLAlchemyError,
        ):
            pass
        return run

    def ensure_intraday_hypothesis(
        self,
        hypothesis: TradeHypothesis,
    ) -> tuple[str, TradeHypothesis] | None:
        """Persist a market-wide intraday discovery without creating orders."""
        now = datetime.now().replace(microsecond=0)
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_trade_hypothesis_v3 (
                        hypothesis_id, hypothesis_key, run_uid,
                        trade_date, scope_type, scope_code, scope_name,
                        direction, state, prior_probability,
                        current_probability, probability_kind,
                        confidence, score, horizon_minutes,
                        alpha_half_life_minutes, proposed_action,
                        max_position_weight, theme_code, role, thesis,
                        counter_thesis, supporting_evidence_json,
                        opposing_evidence_json, triggers_json,
                        invalidations_json, strategy_keys_json,
                        feature_time, valid_until,
                        source_forecast_count, last_evidence_at,
                        created_at, updated_at
                    ) VALUES (
                        :hypothesis_id, :hypothesis_key, :run_uid,
                        :trade_date, :scope_type, :scope_code, :scope_name,
                        :direction, :state, :prior_probability,
                        :current_probability, :probability_kind,
                        :confidence, :score, :horizon_minutes,
                        :alpha_half_life_minutes, :proposed_action,
                        :max_position_weight, :theme_code, :role, :thesis,
                        :counter_thesis, :supporting_evidence_json,
                        :opposing_evidence_json, :triggers_json,
                        :invalidations_json, :strategy_keys_json,
                        :feature_time, :valid_until,
                        :source_forecast_count, :last_evidence_at,
                        :created_at, :updated_at
                    )
                    """
                ),
                {
                    "hypothesis_id": _uuid(),
                    "hypothesis_key": hypothesis.hypothesis_key,
                    "run_uid": hypothesis.run_uid,
                    "trade_date": hypothesis.trade_date,
                    "scope_type": hypothesis.scope_type,
                    "scope_code": hypothesis.scope_code,
                    "scope_name": hypothesis.scope_name,
                    "direction": hypothesis.direction,
                    "state": hypothesis.state,
                    "prior_probability": hypothesis.prior_probability,
                    "current_probability": hypothesis.probability,
                    "probability_kind": hypothesis.probability_kind,
                    "confidence": hypothesis.confidence,
                    "score": hypothesis.score,
                    "horizon_minutes": hypothesis.horizon_minutes,
                    "alpha_half_life_minutes": (
                        hypothesis.alpha_half_life_minutes
                    ),
                    "proposed_action": hypothesis.proposed_action,
                    "max_position_weight": hypothesis.max_position_weight,
                    "theme_code": hypothesis.theme_code,
                    "role": hypothesis.role,
                    "thesis": hypothesis.thesis,
                    "counter_thesis": hypothesis.counter_thesis,
                    "supporting_evidence_json": _json(
                        hypothesis.supporting_evidence
                    ),
                    "opposing_evidence_json": _json(
                        hypothesis.opposing_evidence
                    ),
                    "triggers_json": _json(hypothesis.triggers),
                    "invalidations_json": _json(
                        hypothesis.invalidations
                    ),
                    "strategy_keys_json": _json(
                        hypothesis.strategy_keys
                    ),
                    "feature_time": hypothesis.feature_time,
                    "valid_until": hypothesis.valid_until,
                    "source_forecast_count": (
                        hypothesis.source_forecast_count
                    ),
                    "last_evidence_at": hypothesis.feature_time,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            row = connection.execute(
                text(
                    """
                    SELECT *
                    FROM st_trade_hypothesis_v3
                    WHERE run_uid = :run_uid
                      AND hypothesis_key = :hypothesis_key
                    LIMIT 1
                    """
                ),
                {
                    "run_uid": hypothesis.run_uid,
                    "hypothesis_key": hypothesis.hypothesis_key,
                },
            ).mappings().first()
        if not row:
            return None
        return (
            str(row["hypothesis_id"]),
            self._hypothesis_domain(dict(row)),
        )

    def active_hypotheses_for_intraday(
        self,
        *,
        trade_date: date,
        limit: int = 500,
    ) -> list[tuple[str, TradeHypothesis]]:
        run = self._latest_run(trade_date)
        if not run:
            run = self._latest_run()
        if not run:
            return []
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT *
                    FROM st_trade_hypothesis_v3
                    WHERE run_uid = :run_uid
                      AND scope_type IN ('MARKET', 'STOCK')
                      AND state <> 'INVALIDATED'
                      AND valid_until >= :now
                    ORDER BY current_probability DESC, score DESC
                    LIMIT :limit
                    """
                ),
                {
                    "run_uid": run["run_uid"],
                    "now": datetime.now(),
                    "limit": max(1, min(2000, int(limit))),
                },
            ).mappings().all()
        return [
            (
                str(row["hypothesis_id"]),
                self._hypothesis_domain(dict(row)),
            )
            for row in rows
        ]

    def save_hypothesis_evidence(
        self,
        *,
        hypothesis_id: str,
        updated: TradeHypothesis,
        evidence: HypothesisEvidence,
    ) -> bool:
        payload = {
            "hypothesis_key": evidence.hypothesis_key,
            "observed_at": evidence.observed_at,
            "evidence_type": evidence.evidence_type,
            "polarity": evidence.polarity,
            "strength": evidence.strength,
            "source": evidence.source,
            "summary": evidence.summary,
            "payload": evidence.payload,
        }
        evidence_hash = hashlib.sha256(
            _json(payload).encode("utf-8")
        ).hexdigest()
        now = datetime.now().replace(microsecond=0)
        with self.engine.begin() as connection:
            result = connection.execute(
                text(
                    """
                    INSERT IGNORE INTO st_hypothesis_evidence_v3 (
                        evidence_id, hypothesis_id, hypothesis_key,
                        run_uid, trade_date, observed_at, evidence_type,
                        polarity, strength, source, summary,
                        probability_before, probability_after,
                        state_before, state_after, payload_json,
                        evidence_hash, created_at
                    ) VALUES (
                        :evidence_id, :hypothesis_id, :hypothesis_key,
                        :run_uid, :trade_date, :observed_at, :evidence_type,
                        :polarity, :strength, :source, :summary,
                        :probability_before, :probability_after,
                        :state_before, :state_after, :payload_json,
                        :evidence_hash, :created_at
                    )
                    """
                ),
                {
                    "evidence_id": _uuid(),
                    "hypothesis_id": hypothesis_id,
                    "hypothesis_key": evidence.hypothesis_key,
                    "run_uid": updated.run_uid,
                    "trade_date": updated.trade_date,
                    "observed_at": evidence.observed_at,
                    "evidence_type": evidence.evidence_type,
                    "polarity": evidence.polarity,
                    "strength": evidence.strength,
                    "source": evidence.source,
                    "summary": evidence.summary[:500],
                    "probability_before": (
                        evidence.probability_before
                    ),
                    "probability_after": evidence.probability_after,
                    "state_before": evidence.state_before,
                    "state_after": evidence.state_after,
                    "payload_json": _json(evidence.payload),
                    "evidence_hash": evidence_hash,
                    "created_at": now,
                },
            )
            inserted = int(result.rowcount or 0) > 0
            if inserted:
                connection.execute(
                    text(
                        """
                        UPDATE st_trade_hypothesis_v3
                        SET state = :state,
                            current_probability = :probability,
                            proposed_action = :proposed_action,
                            max_position_weight = :max_position_weight,
                            supporting_evidence_json =
                                :supporting_evidence_json,
                            opposing_evidence_json =
                                :opposing_evidence_json,
                            last_evidence_at = :observed_at,
                            updated_at = :updated_at
                        WHERE hypothesis_id = :hypothesis_id
                        """
                    ),
                    {
                        "state": updated.state,
                        "probability": updated.probability,
                        "proposed_action": updated.proposed_action,
                        "max_position_weight": (
                            updated.max_position_weight
                        ),
                        "supporting_evidence_json": _json(
                            updated.supporting_evidence
                        ),
                        "opposing_evidence_json": _json(
                            updated.opposing_evidence
                        ),
                        "observed_at": evidence.observed_at,
                        "updated_at": now,
                        "hypothesis_id": hypothesis_id,
                    },
                )
        return inserted

    def hypothesis_timeline(
        self,
        hypothesis_id: str,
        *,
        limit: int = 500,
    ) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT *
                    FROM st_trade_hypothesis_v3
                    WHERE hypothesis_id = :hypothesis_id
                    LIMIT 1
                    """
                ),
                {"hypothesis_id": hypothesis_id},
            ).mappings().first()
            if not row:
                return None
            events = connection.execute(
                text(
                    """
                    SELECT *
                    FROM st_hypothesis_evidence_v3
                    WHERE hypothesis_id = :hypothesis_id
                    ORDER BY observed_at, created_at
                    LIMIT :limit
                    """
                ),
                {
                    "hypothesis_id": hypothesis_id,
                    "limit": max(1, min(2000, int(limit))),
                },
            ).mappings().all()
        parsed_events = []
        for event in events:
            item = dict(event)
            item["payload"] = json.loads(
                str(item.pop("payload_json") or "{}")
            )
            parsed_events.append(item)
        return {
            "hypothesis": self._hypothesis_row(dict(row)),
            "events": parsed_events,
        }

    def decision_runs(
        self,
        *,
        limit: int = 60,
    ) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT run_uid, requested_as_of, trade_date,
                           decision_at, mode,
                           model_version, lifecycle_status, status,
                           dominant_regime, risk_asset_cap,
                           forecast_count, validated_count, target_count,
                           data_snapshot_hash, result_hash
                    FROM st_decision_run_v3
                    ORDER BY COALESCE(
                                 requested_as_of, DATE(decision_at)
                             ) DESC,
                             decision_at DESC, run_uid DESC
                    LIMIT :limit
                    """
                ),
                {"limit": max(1, min(500, int(limit)))},
            ).mappings().all()
        result = []
        for row in rows:
            item = dict(row)
            item["data_date"] = item.get("trade_date")
            decision_at = item.get("decision_at")
            item["decision_session_date"] = (
                item.get("requested_as_of")
                or (
                    decision_at.date()
                    if isinstance(decision_at, datetime)
                    else str(decision_at or "")[:10] or None
                )
            )
            result.append(item)
        return result

    def latest_targets(self) -> list[dict[str, Any]]:
        run = self._latest_run()
        if not run:
            return []
        with self.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT *
                    FROM st_target_portfolio_v3
                    WHERE run_uid = :run_uid
                    ORDER BY rank_no
                    """
                ),
                {"run_uid": run["run_uid"]},
            ).mappings().all()
        result = []
        for row in rows:
            item = dict(row)
            item["strategy_keys"] = json.loads(
                str(item.pop("strategy_keys_json") or "[]")
            )
            item["theme_codes"] = json.loads(
                str(item.pop("theme_codes_json") or "[]")
            )
            result.append(item)
        return result

    def latest_validation(self) -> dict[str, Any] | None:
        with self.engine.connect() as connection:
            row = connection.execute(
                text(
                    """
                    SELECT *
                    FROM st_validation_result_v3
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                )
            ).mappings().first()
        if not row:
            return None
        item = dict(row)
        item["block_reasons"] = json.loads(
            str(item.pop("block_reasons_json") or "[]")
        )
        item["evidence"] = json.loads(
            str(item.pop("evidence_json") or "{}")
        )
        return item

    def latest_validations_for_models(
        self,
        model_versions: Iterable[str],
    ) -> dict[str, dict[str, Any]]:
        versions = list(
            dict.fromkeys(
                str(item).strip()
                for item in model_versions
                if str(item).strip()
            )
        )
        if not versions:
            return {}
        statement = text(
            """
            SELECT *
            FROM st_validation_result_v3
            WHERE model_version IN :model_versions
            ORDER BY created_at DESC
            """
        ).bindparams(bindparam("model_versions", expanding=True))
        with self.engine.connect() as connection:
            rows = connection.execute(
                statement,
                {"model_versions": versions},
            ).mappings().all()
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            model_version = str(row["model_version"])
            if model_version in result:
                continue
            item = dict(row)
            item["block_reasons"] = json.loads(
                str(item.pop("block_reasons_json") or "[]")
            )
            item["evidence"] = json.loads(
                str(item.pop("evidence_json") or "{}")
            )
            result[model_version] = item
        return result

    def latest_opportunity_recall(
        self,
        strategy_key: str = "",
    ) -> dict[str, Any] | None:
        params = {"strategy_key": str(strategy_key or "").strip()}
        with self.engine.connect() as connection:
            anchor = connection.execute(
                text(
                    """
                    SELECT trade_date, horizon_days,
                           winner_threshold_pct
                    FROM st_opportunity_recall_v3
                    WHERE (:strategy_key = ''
                           OR strategy_key = :strategy_key)
                    ORDER BY trade_date DESC, created_at DESC
                    LIMIT 1
                    """
                ),
                params,
            ).mappings().first()
            if not anchor:
                return None
            rows = connection.execute(
                text(
                    """
                    SELECT *
                    FROM st_opportunity_recall_v3
                    WHERE trade_date = :trade_date
                      AND horizon_days = :horizon_days
                      AND winner_threshold_pct =
                          :winner_threshold_pct
                      AND (:strategy_key = ''
                           OR strategy_key = :strategy_key)
                    ORDER BY strategy_key
                    """
                ),
                {
                    **params,
                    "trade_date": anchor["trade_date"],
                    "horizon_days": anchor["horizon_days"],
                    "winner_threshold_pct": anchor[
                        "winner_threshold_pct"
                    ],
                },
            ).mappings().all()
        if not rows:
            return None
        if params["strategy_key"]:
            item = dict(rows[0])
            item["missed_reason_counts"] = json.loads(
                str(item.pop("missed_reason_json") or "{}")
            )
            return item

        missed_reasons: Counter[str] = Counter()
        for row in rows:
            missed_reasons.update(
                json.loads(str(row["missed_reason_json"] or "{}"))
            )

        def _winner_weighted(field: str) -> float | None:
            weighted = [
                (float(row[field]), int(row["winner_count"] or 0))
                for row in rows
                if row[field] is not None
                and int(row["winner_count"] or 0) > 0
            ]
            denominator = sum(weight for _, weight in weighted)
            return (
                sum(value * weight for value, weight in weighted)
                / denominator
                if denominator
                else None
            )

        accepted_weighted = [
            (
                float(row["accepted_average_net_return_pct"]),
                int(row["accepted_winner_count"] or 0),
            )
            for row in rows
            if row["accepted_average_net_return_pct"] is not None
            and int(row["accepted_winner_count"] or 0) > 0
        ]
        accepted_denominator = sum(
            weight for _, weight in accepted_weighted
        )
        evidence_kinds = sorted({
            str(row["evidence_kind"] or "UNKNOWN") for row in rows
        })
        protocols = sorted({
            str(row["protocol_version"] or "UNKNOWN")
            for row in rows
        })
        return {
            "trade_date": anchor["trade_date"],
            "horizon_days": anchor["horizon_days"],
            "strategy_key": "ALL_STRATEGIES",
            "strategy_count": len(rows),
            "winner_threshold_pct": anchor[
                "winner_threshold_pct"
            ],
            "winner_count": sum(
                int(row["winner_count"] or 0) for row in rows
            ),
            "accepted_winner_count": sum(
                int(row["accepted_winner_count"] or 0)
                for row in rows
            ),
            "missed_winner_count": sum(
                int(row["missed_winner_count"] or 0)
                for row in rows
            ),
            "recall_at_20": _winner_weighted("recall_at_20"),
            "recall_at_50": _winner_weighted("recall_at_50"),
            "accepted_average_net_return_pct": (
                sum(
                    value * weight
                    for value, weight in accepted_weighted
                )
                / accepted_denominator
                if accepted_denominator
                else None
            ),
            "missed_reason_counts": dict(missed_reasons),
            "evidence_kind": (
                evidence_kinds[0]
                if len(evidence_kinds) == 1
                else "MIXED"
            ),
            "protocol_version": (
                protocols[0]
                if len(protocols) == 1
                else "MIXED:" + ",".join(protocols)
            ),
        }

    def strategy_learning_summary(
        self,
        strategy_key: str,
        *,
        limit: int = 5000,
    ) -> dict[str, Any]:
        query_limit = max(1, min(20_000, int(limit)))
        with self.engine.connect() as connection:
            # Only actual internal-paper fills may mature a forward sample.
            # Counterfactual rows remain a separate shadow ledger used for
            # missed-opportunity diagnostics; they cannot activate or tune a
            # production model.
            rows = connection.execute(
                text(
                    """
                    SELECT e.evidence_status,
                           e.exit_reason AS reason_code,
                           e.realized_net_return_pct,
                           e.realized_mae_pct, e.realized_mfe_pct,
                           DATE(e.exit_at) AS outcome_date,
                           f.features_json
                    FROM st_forward_trade_evidence_v3 e
                    LEFT JOIN st_alpha_forecast_v3 f
                      ON f.forecast_id = e.source_forecast_id
                    WHERE e.strategy_key = :strategy_key
                      AND e.evidence_kind = 'EXECUTED_PAPER'
                      AND e.protocol_version =
                          'PAPER_EXECUTED_LEDGER_V1'
                      AND e.sample_owner_role = 'PRIMARY'
                      AND e.attribution_status IN (
                          'VERIFIED_SNAPSHOT',
                          'LEGACY_SINGLE_STRATEGY_RESOLVED'
                      )
                      AND f.forecast_id IS NOT NULL
                      AND f.run_uid = e.source_run_uid
                      AND f.stock_code = e.stock_code
                      AND f.strategy_key = e.strategy_key
                    ORDER BY e.entry_at DESC, e.updated_at DESC
                    LIMIT :limit
                    """
                ),
                {
                    "strategy_key": strategy_key,
                    "limit": query_limit,
                },
            ).mappings().all()
            shadow = connection.execute(
                text(
                    """
                    SELECT COUNT(*) AS observed_count,
                           COALESCE(SUM(missed_opportunity), 0)
                               AS missed_count,
                           COALESCE(SUM(false_positive), 0)
                               AS false_positive_count
                    FROM st_counterfactual_v3
                    WHERE strategy_key = :strategy_key
                      AND evidence_kind = 'SHADOW'
                    """
                ),
                {"strategy_key": strategy_key},
            ).mappings().first()
        stage_counts: dict[str, int] = {}
        accepted_returns = []
        accepted_mae = []
        accepted_mfe = []
        winning_features: list[dict[str, float]] = []
        losing_features: list[dict[str, float]] = []
        matured_dates = []
        for row in rows:
            evidence_status = str(
                row["evidence_status"] or "UNKNOWN"
            )
            reason = str(row["reason_code"] or evidence_status)
            stage_counts[reason] = stage_counts.get(reason, 0) + 1
            if evidence_status != "MATURED":
                continue
            if row["realized_net_return_pct"] is None:
                continue
            accepted_returns.append(
                float(row["realized_net_return_pct"])
            )
            if row["outcome_date"] is not None:
                matured_dates.append(row["outcome_date"])
            try:
                features = json.loads(
                    str(row["features_json"] or "{}")
                )
            except (TypeError, ValueError, json.JSONDecodeError):
                features = {}
            if float(row["realized_net_return_pct"]) > 0:
                winning_features.append(features)
            elif float(row["realized_net_return_pct"]) < 0:
                losing_features.append(features)
            if row["realized_mae_pct"] is not None:
                accepted_mae.append(float(row["realized_mae_pct"]))
            if row["realized_mfe_pct"] is not None:
                accepted_mfe.append(float(row["realized_mfe_pct"]))
        gains = [value for value in accepted_returns if value > 0]
        losses = [value for value in accepted_returns if value < 0]
        profit_factor = (
            sum(gains) / abs(sum(losses))
            if losses
            else (math.inf if gains else None)
        )
        payoff_ratio = (
            (sum(gains) / len(gains))
            / abs(sum(losses) / len(losses))
            if gains and losses
            else None
        )
        accepted_count = len(accepted_returns)
        diagnostic_keys = (
            "return_20d_pct",
            "latest_change_pct",
            "amount_ratio_1_20",
            "rebound_from_low_pct",
            "latest_relative_to_market_pct",
            "sector_relative_return_pct",
            "sector_breadth_pct",
        )
        feature_diagnostics = {}
        if winning_features and losing_features:
            for key in diagnostic_keys:
                win_values = [
                    float(item[key])
                    for item in winning_features
                    if item.get(key) is not None
                ]
                loss_values = [
                    float(item[key])
                    for item in losing_features
                    if item.get(key) is not None
                ]
                if not win_values or not loss_values:
                    continue
                win_average = sum(win_values) / len(win_values)
                loss_average = sum(loss_values) / len(loss_values)
                feature_diagnostics[key] = {
                    "winner_average": win_average,
                    "loser_average": loss_average,
                    "difference": win_average - loss_average,
                }
        forward_policy = dict(
            load_v3_config().get("forward_learning") or {}
        )
        minimum_samples = int(
            forward_policy.get("minimum_mature_trades", 80)
        )
        if accepted_count < 20:
            conclusion = "样本很少：继续小仓前向收集，不调整公式"
        elif accepted_count < minimum_samples:
            conclusion = "样本积累中：只做分层复盘，未到重新校准门槛"
        elif (
            profit_factor is not None
            and profit_factor >= 1.3
            and sum(accepted_returns) / accepted_count > 0
        ):
            conclusion = "达到候选校准门槛：应生成新冻结版本并做样本外验收"
        else:
            conclusion = "样本已够但结果不达标：收紧触发条件或停用该实验版本"
        return {
            "strategy_key": strategy_key,
            "observed_count": len(rows),
            "open_count": sum(
                str(row["evidence_status"]) == "OPEN" for row in rows
            ),
            "partially_closed_count": sum(
                str(row["evidence_status"]) == "PARTIALLY_CLOSED"
                for row in rows
            ),
            "matured_count": accepted_count,
            "accepted_count": accepted_count,
            "winning_count": len(gains),
            "losing_count": len(losses),
            "win_rate": (
                len(gains) / accepted_count
                if accepted_count
                else None
            ),
            "average_net_return_pct": (
                sum(accepted_returns) / accepted_count
                if accepted_count
                else None
            ),
            "profit_factor": profit_factor,
            "payoff_ratio": payoff_ratio,
            "average_mae_pct": (
                sum(accepted_mae) / len(accepted_mae)
                if accepted_mae
                else None
            ),
            "average_mfe_pct": (
                sum(accepted_mfe) / len(accepted_mfe)
                if accepted_mfe
                else None
            ),
            "missed_opportunity_count": int(
                (shadow or {}).get("missed_count") or 0
            ),
            "false_positive_count": int(
                (shadow or {}).get("false_positive_count") or 0
            ),
            "shadow_observed_count": int(
                (shadow or {}).get("observed_count") or 0
            ),
            "evidence_source": "EXECUTED_PAPER_FILLS_ONLY",
            "protocol_version": "PAPER_EXECUTED_LEDGER_V1",
            "shadow_can_activate_model": False,
            "minimum_samples_before_calibration": minimum_samples,
            "latest_outcome_date": (
                max(matured_dates) if matured_dates else None
            ),
            "stage_counts": stage_counts,
            "feature_diagnostics": feature_diagnostics,
            "conclusion": conclusion,
        }
