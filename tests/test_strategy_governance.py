from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
import inspect
import json
import os
from types import SimpleNamespace
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError

from server.api.routers import strategy_center as strategy_center_router
from server.common.sql_reader import bind_sql_connection, read_sql_rows
from server.engine import strategy_center as strategy_center_engine
from server.engine.strategy_governance import (
    LIFECYCLE_LABELS,
    PROFIT_GATE_POLICY,
    calculate_health_score,
    calculate_return_metrics,
    evaluate_profit_gate,
    recommend_lifecycle_status,
    transition_lifecycle,
    validate_strategy_key,
)
from server.engine import strategy_governance as governance_module
from server.common.scheduler_args import NO_DEFAULT_DATE_TASK_TYPES
from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS
from tools.add_strategy_governance_task import TASK as GOVERNANCE_TASK
from tools import add_strategy_governance_task as governance_task_installer


ARTIFACT_MAX_HOLDING_DAYS = 2
REAL_AUTHORITATIVE_SESSION_WINDOWS = (
    governance_module._authoritative_session_windows
)


def _admin_request():
    return SimpleNamespace(state=SimpleNamespace(
        auth_kind="account_session",
        auth_user=SimpleNamespace(
            id=1, role="ADMIN", username="owner", is_active=True,
        ),
    ))


@pytest.fixture(autouse=True)
def _authoritative_test_calendar(monkeypatch):
    monkeypatch.setattr(
        governance_module,
        "_trading_sessions_between",
        lambda start, end: max(
            0,
            (
                date.fromisoformat(end) - date.fromisoformat(start)
            ).days - 1,
        ),
    )
    def session_windows(as_of_date):
        end = date.fromisoformat(as_of_date)
        result = {}
        for window in governance_module.WINDOWS:
            sessions = [
                (end - timedelta(days=offset)).isoformat()
                for offset in range(window - 1, -1, -1)
            ]
            payload = {
                "schema": "probiga.authoritative-session-window.v1",
                "window_days": window,
                "start_date": sessions[0],
                "end_date": sessions[-1],
                "session_count": window,
                "sessions": sessions,
            }
            result[window] = {
                **payload, "session_hash": governance_module._digest(payload),
            }
        return result
    monkeypatch.setattr(
        governance_module, "_authoritative_session_windows", session_windows,
    )


def _profitable_records(count: int = 100) -> list[dict]:
    start = date(2026, 1, 1)
    return [
        {
            "trade_date": (start + timedelta(days=index)).isoformat(),
            "return_pct": 1.5 if index % 10 < 7 else -0.5,
        }
        for index in range(count)
    ]


def _attest_funding_metrics(
    metrics: dict, *, revision_at: str = "2026-08-21T15:00:00"
) -> dict:
    metrics.update({
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "artifact_hash": "a" * 64,
        "source_dataset_hash": "b" * 64,
        "evidence_revision_at": revision_at,
        "verification_status": "CONFIRMED",
        "submitted_by": "evidence_submitter",
        "reviewed_by": "independent_reviewer",
        "reviewed_at": revision_at,
        "review_audit_valid": True,
        "funding_provenance": "INTERNAL_PORTFOLIO_LEDGER_V1",
        "internal_ledger_hash": "c" * 64,
        "internal_ledger_schema": "probiga.internal-strategy-portfolio-ledger.v1",
        "drawdown_basis": "internal_version_bound_portfolio_equity",
        "cost_basis": "actual_ledger_fees",
        "evidence_fresh": True,
        "selection_validation_fresh": True,
        "selection_validation_independent_oos": True,
        "selection_validation_scope": "VERSION_SELECTION_ONLY",
        "selection_validation_completed_trades": 80,
        "selection_validation_coverage_days": 60,
        "portfolio_coverage_days": 60,
        "session_window_valid": True,
        "session_window_count": int(metrics.get("window_days") or 0),
        "session_window_hash": "d" * 64,
    })
    return metrics


def _validation_artifact_fixture(
    *, label_horizon_days: int = 2,
    max_holding_days: int = ARTIFACT_MAX_HOLDING_DAYS,
    test_label_delay_days: int | None = None,
    embargo_days: int | None = None,
) -> tuple[dict, dict, str, str, str]:
    version_created_at = "2025-12-31T00:00:00"
    test_label_delay_days = (
        max_holding_days
        if test_label_delay_days is None else test_label_delay_days
    )
    embargo_days = (
        label_horizon_days if embargo_days is None else embargo_days
    )
    purge_days = max(label_horizon_days, max_holding_days)
    start = date(2026, 1, 1)
    test_windows = []
    test_start = start
    for _index in range(5):
        test_end = test_start + timedelta(days=15)
        test_windows.append((test_start, test_end))
        # Leave two complete calendar days between sample-out folds. This is
        # the fixture's explicit embargo, not an accidental date gap.
        test_start = test_end + timedelta(days=3)
    trades = []
    trade_index = 0
    for test_start, test_end in test_windows:
        trade_day = test_start
        while trade_day <= test_end:
            net_return = 1.5 if trade_index % 10 < 7 else -0.5
            label_day = trade_day + timedelta(days=test_label_delay_days)
            observed_at = f"{label_day.isoformat()}T15:00:00"
            trades.append({
                "evidence_id": governance_module._digest({
                    "schema": "probiga.validation-sample-id.v1",
                    "source_key": f"trade-{trade_index:03d}",
                }),
                "trade_date": trade_day.isoformat(),
                "label_available_at": observed_at,
                "observed_at": observed_at,
                "net_return_pct": net_return,
                "cost_pct": 0.25,
            })
            trade_index += 1
            trade_day += timedelta(days=1)
    equity_curve = governance_module._rebuild_equity_curve(trades)
    peak = equity_curve[0]["equity"]
    max_drawdown = 0.0
    for point in equity_curve:
        peak = max(peak, point["equity"])
        max_drawdown = max(
            max_drawdown,
            (peak - point["equity"]) / peak * 100.0,
        )
    wins = [item["net_return_pct"] for item in trades if item["net_return_pct"] > 0]
    losses = [item["net_return_pct"] for item in trades if item["net_return_pct"] < 0]
    net_profit = sum(item["net_return_pct"] for item in trades)
    raw_metrics = {
        "completed_trades": len(trades),
        "coverage_days": len(trades),
        "win_rate_pct": len(wins) / len(trades) * 100.0,
        "average_win_pct": sum(wins) / len(wins),
        "average_loss_pct": abs(sum(losses) / len(losses)),
        "payoff_ratio": (sum(wins) / len(wins)) / abs(sum(losses) / len(losses)),
        "gross_expectancy_pct": sum(
            item["net_return_pct"] + item["cost_pct"] for item in trades
        ) / len(trades),
        "estimated_cost_pct": 0.25,
        "net_expectancy_pct": net_profit / len(trades),
        "profit_factor": sum(wins) / abs(sum(losses)),
        "max_drawdown_pct": max_drawdown,
        "walk_forward_segments": 5,
        "positive_segments": 5,
        "cost_stress_expectancy_pct": (
            sum(item["net_return_pct"] + item["cost_pct"] for item in trades)
            / len(trades)
            - 0.25 * PROFIT_GATE_POLICY["cost_stress_multiple"]
        ),
        "top5_profit_contribution_pct": (
            sum(sorted(wins, reverse=True)[:5]) / net_profit * 100.0
        ),
        "market_match_score": 100.0,
        "walk_forward_verified": True,
        "independent_oos": True,
        "drawdown_basis": "sequential_trade_compounded_equity",
        "cost_basis": "validated_fee_model_v1",
    }
    metrics = governance_module._validated_metric_evidence(raw_metrics)
    segments = []
    for index, (test_start, test_end) in enumerate(test_windows, 1):
        segment_rows = [
            item for item in trades
            if test_start.isoformat() <= item["trade_date"] <= test_end.isoformat()
        ]
        train_start = "2025-09-01"
        train_end = (
            test_start - timedelta(days=purge_days + 1)
        ).isoformat()
        train_dataset = [
            {
                "observation_id": governance_module._digest({
                    "schema": "probiga.validation-sample-id.v1",
                    "source_key": f"train-{index:02d}-{row_index:02d}",
                }),
                "observed_at": (
                    date(2025, 9, 1) + timedelta(days=row_index)
                ).isoformat() + "T15:00:00",
                "label_available_at": (
                    date(2025, 9, 1)
                    + timedelta(days=row_index + label_horizon_days)
                ).isoformat() + "T15:00:00",
                "feature_snapshot_hash": governance_module._digest({
                    "segment": index, "row": row_index, "kind": "feature",
                }),
                "label_snapshot_hash": governance_module._digest({
                    "segment": index, "row": row_index, "kind": "label",
                }),
            }
            for row_index in range(3)
        ]
        segments.append({
            "train_start": train_start,
            "train_end": train_end,
            "test_start": test_start.isoformat(),
            "test_end": test_end.isoformat(),
            "completed_trades": len(segment_rows),
            "net_expectancy_pct": sum(
                item["net_return_pct"] for item in segment_rows
            ) / len(segment_rows),
            "train_dataset": train_dataset,
            "train_dataset_hash": governance_module._digest({
                "segment_index": index,
                "train_start": train_start,
                "train_end": train_end,
                "observations": train_dataset,
            }),
            "test_dataset_hash": governance_module._digest({
                "segment_index": index,
                "test_start": test_start.isoformat(),
                "test_end": test_end.isoformat(),
                "trades": segment_rows,
            }),
        })
    as_of_date = trades[-1]["label_available_at"][:10]
    revision_at = trades[-1]["observed_at"]
    artifact = {
        "schema_version": "probiga.strategy-validation-artifact.v3",
        "entity_type": "STRATEGY",
        "entity_key": "artifact_test_strategy",
        "entity_version": "v1",
        "as_of_date": as_of_date,
        "window_days": 120,
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "evidence_revision_at": revision_at,
        "metrics_hash": governance_module._digest(metrics),
        "trades": trades,
        "equity_curve": equity_curve,
        "segments": segments,
        "validation_protocol": {
            "label_horizon_days": label_horizon_days,
            "max_holding_days": max_holding_days,
            "purge_days": purge_days,
            "embargo_days": embargo_days,
        },
    }
    session_window = governance_module._authoritative_session_windows(
        as_of_date
    )[120]
    artifact.update({
        "window_session_start": session_window["start_date"],
        "window_session_end": session_window["end_date"],
        "window_session_count": session_window["session_count"],
        "window_session_hash": session_window["session_hash"],
    })
    artifact["source_dataset_hash"] = governance_module._digest({
        "trades": trades,
        "equity_curve": equity_curve,
    })
    artifact_hash = governance_module._digest(artifact)
    return metrics, artifact, artifact_hash, revision_at, version_created_at


def test_lifecycle_labels_are_exact_chinese_product_values():
    assert LIFECYCLE_LABELS == {
        "ACTIVE": "正常运行",
        "REDUCE": "降权运行",
        "SHADOW": "影子观察",
        "SUSPENDED": "暂停使用",
        "RETIRED": "已淘汰",
    }


def test_dynamic_strategy_key_validation_has_no_fixed_catalog_dependency():
    assert validate_strategy_key("earnings_surprise_v2") == "earnings_surprise_v2"
    try:
        validate_strategy_key("固定 策略")
    except ValueError as exc:
        assert "策略代码" in str(exc)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("invalid strategy key should be rejected")


def test_authoritative_windows_require_exact_calendar_qmt_date_set(monkeypatch):
    end = date(2026, 8, 21)
    descending = [
        (end - timedelta(days=index * 2)).isoformat()
        for index in range(120)
    ]

    def attestation(day):
        return {
            "trade_date": day,
            "attested_bar_count": 2,
            "batch_count": 1,
            "min_data_version": "qmt-v1",
            "max_data_version": "qmt-v1",
            "latest_received_at": day + "T15:01:00",
        }

    qmt_rows = [attestation(day) for day in descending]
    universe_rows = [
        {
            "trade_date": day,
            "stock_code": stock_code,
            "in_target": 1,
            "in_completed_attestation": 1,
            "in_exact_attestation": 1,
        }
        for day in descending
        for stock_code in ("000001", "600000")
    ]
    daily_universe = {
        day: {
            "stock_count": 2,
            "stock_set_hash": governance_module._digest({
                "schema": "probiga.qmt-expected-stock-set.v1",
                "trade_date": day,
                "stock_codes": ["000001", "600000"],
            }),
        }
        for day in descending
    }
    valid_tolerance = {
        "attestation_protocol": (
            governance_module.QMT_PRECLOSE_ATTESTATION_PROTOCOL
        ),
        "universe_manifest_schema": "probiga.qmt-daily-universe.v1",
        "daily_universe": daily_universe,
    }
    completed_run_rows = [
        {
            "run_id": "completed-v2-full-universe",
            "start_date": descending[-1],
            "end_date": descending[0],
            "tolerance_json": json.dumps(valid_tolerance),
        },
        {
            # A stale partial run may still own rows through the immutable
            # unique key.  It cannot validate a day, but must not shadow the
            # later full-universe run either.
            "run_id": "stale-partial-owner",
            "start_date": descending[-1],
            "end_date": descending[0],
            "tolerance_json": json.dumps({
                "attestation_protocol": (
                    governance_module.QMT_PRECLOSE_ATTESTATION_PROTOCOL
                ),
            }),
        },
    ]
    observed_sql = []

    def fake_read(sql, _params=None):
        observed_sql.append(sql)
        if "FROM si_trade_calendar" in sql:
            return [{"trade_date": day} for day in descending]
        if "MAX(u.in_target)" in sql:
            return list(universe_rows)
        if "FROM qmt_kline_attestation_run" in sql:
            return list(completed_run_rows)
        if "COUNT(DISTINCT k.id)" in sql:
            return list(qmt_rows)
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    windows = REAL_AUTHORITATIVE_SESSION_WINDOWS(descending[0])
    assert windows[120]["session_count"] == 120
    assert windows[20]["end_date"] == descending[0]
    assert len(windows[120]["session_attestations"]) == 120
    latest_attestation = windows[120]["session_attestations"][-1]
    assert latest_attestation["expected_stock_count"] == 2
    assert latest_attestation["expected_stock_set_hash"] == (
        governance_module._digest({
            "schema": "probiga.qmt-expected-stock-set.v1",
            "trade_date": descending[0],
            "stock_codes": ["000001", "600000"],
        })
    )
    session_payload = dict(windows[120])
    session_payload.pop("session_hash")
    assert windows[120]["session_hash"] == governance_module._digest(
        session_payload
    )
    universe_sql = next(sql for sql in observed_sql if "MAX(u.in_target)" in sql)
    assert "qmt_kline_attestation_run" not in universe_sql
    assert "a.run_id=r.run_id" not in universe_sql.replace(" ", "")
    completed_sql = next(
        sql for sql in observed_sql
        if "FROM qmt_kline_attestation_run" in sql
    )
    assert "BINARY status=BINARY 'COMPLETED'" in completed_sql
    assert "BINARY provider=BINARY 'gj_big_qmt_inner'" in completed_sql
    assert "start_date<=:as_of_date" in completed_sql
    assert "end_date>=:start_date" in completed_sql
    aggregate_sql = next(
        sql for sql in observed_sql if "COUNT(DISTINCT k.id)" in sql
    )
    for exact_join_sql in (universe_sql, aggregate_sql):
        assert "a.qmt_id>0" in exact_join_sql
        assert "a.trade_date=k.trade_date" in exact_join_sql
        assert "BINARY a.stock_code=BINARY k.stock_code" in exact_join_sql
        assert "BINARY a.attestation_id=BINARY SHA2(CONCAT_WS('|'," in (
            exact_join_sql
        )
        for bound_field in (
            "a.protocol_version", "a.target_id", "a.qmt_id",
            "a.source_data_version", "a.source_pre_close",
            "a.attested_open", "a.attested_close", "a.attested_high",
            "a.attested_low", "a.attested_volume", "a.attested_amount",
        ):
            assert bound_field in exact_join_sql

    invalid_manifest = deepcopy(valid_tolerance)
    invalid_manifest["daily_universe"][descending[-1]]["stock_count"] = 1
    completed_run_rows[0]["tolerance_json"] = json.dumps(invalid_manifest)
    with pytest.raises(ValueError, match="全集清单"):
        REAL_AUTHORITATIVE_SESSION_WINDOWS(descending[0])
    completed_run_rows[0]["tolerance_json"] = json.dumps(valid_tolerance)

    missing_manifest = deepcopy(valid_tolerance)
    missing_manifest["daily_universe"].pop(descending[-1])
    completed_run_rows[0]["tolerance_json"] = json.dumps(missing_manifest)
    with pytest.raises(ValueError, match="全集清单"):
        REAL_AUTHORITATIVE_SESSION_WINDOWS(descending[0])
    completed_run_rows[0]["tolerance_json"] = json.dumps(valid_tolerance)

    universe_rows[-1]["in_completed_attestation"] = 0
    with pytest.raises(ValueError, match="股票集合"):
        REAL_AUTHORITATIVE_SESSION_WINDOWS(descending[0])
    universe_rows[-1]["in_completed_attestation"] = 1

    universe_rows[-1]["in_exact_attestation"] = 0
    with pytest.raises(ValueError, match="股票集合"):
        REAL_AUTHORITATIVE_SESSION_WINDOWS(descending[0])
    universe_rows[-1]["in_exact_attestation"] = 1

    removed_day = descending[-1]
    removed_rows = [
        row for row in universe_rows if row["trade_date"] == removed_day
    ]
    universe_rows[:] = [
        row for row in universe_rows if row["trade_date"] != removed_day
    ]
    with pytest.raises(ValueError, match="非空"):
        REAL_AUTHORITATIVE_SESSION_WINDOWS(descending[0])
    universe_rows.extend(removed_rows)

    qmt_rows.pop()
    with pytest.raises(ValueError, match="缺少"):
        REAL_AUTHORITATIVE_SESSION_WINDOWS(descending[0])

    qmt_rows.append(attestation(descending[-1]))
    qmt_rows.append(attestation((end - timedelta(days=1)).isoformat()))
    with pytest.raises(ValueError, match="额外"):
        REAL_AUTHORITATIVE_SESSION_WINDOWS(descending[0])


def test_reduce_lifecycle_halves_competitive_budget_and_keeps_cash():
    base = {
        "paper_allocation_eligible": True,
        "strategy_key": "positive_strategy",
        "current_version": "v1",
        "funding_gate_hash": "a" * 64,
        "strategy_name": "正期望策略",
        "ranking_score": 90.0,
        "market_route": {
            "market_state": "trend_bullish",
            "market_match_score": 100.0,
            "router_decision_hash": "b" * 64,
        },
    }
    active = governance_module._allocation(
        [{**base, "current_status": "ACTIVE"}], [],
        "trend_bullish", trading_allowed=True,
    )
    reduced = governance_module._allocation(
        [{**base, "current_status": "REDUCE"}], [],
        "trend_bullish", trading_allowed=True,
    )
    assert active[0]["simulated_weight_pct"] == 85.0
    assert active[-1]["simulated_weight_pct"] == 15.0
    assert reduced[0]["base_competitive_weight_pct"] == 85.0
    assert reduced[0]["lifecycle_risk_multiplier"] == 0.5
    assert reduced[0]["simulated_weight_pct"] == 42.5
    assert reduced[-1]["simulated_weight_pct"] == 57.5
    assert sum(row["simulated_weight_pct"] for row in reduced) == 100.0


def test_current_version_evidence_queries_do_not_silently_truncate():
    metric_source = inspect.getsource(governance_module._load_metric_inputs)
    forward_source = inspect.getsource(governance_module._load_forward_records)
    assert "LIMIT" not in metric_source.upper()
    assert "LIMIT" not in forward_source.upper()
    assert "INNER JOIN st_strategy_registry current_entity" in metric_source
    assert "INNER JOIN st_strategy_registry current_strategy" in forward_source
    assert "current_version" in metric_source and "current_version" in forward_source
    assert "cash_event_payload_hash" in forward_source
    assert "fill_payload_hash" in forward_source
    assert "trading-v2.canonical-json.v1" in forward_source
    assert "exit_order_binding_count" in forward_source
    assert "st_forward_exit_allocation_v3" in forward_source
    assert "PAPER_FIFO_EXIT_ALLOCATION_V1" in forward_source
    assert "global_allocation.allocated_quantity=" in forward_source
    assert "global_allocation.allocated_gross_cny=" in forward_source
    assert "global_allocation.allocated_fee_cny=" in forward_source
    assert "global_allocation.minimum_sequence=0" in forward_source
    assert "UNATTRIBUTED" in forward_source
    assert "source_intent_buy_fill_count" in forward_source
    assert "COUNT(DISTINCT raw_buy.fill_id)" in forward_source
    assert "source_intent.intent_id=e.source_intent_id" in forward_source


def test_dynamic_registry_and_combination_loaders_do_not_limit_object_count():
    registry_source = inspect.getsource(governance_module.load_registry)
    combination_source = inspect.getsource(governance_module.load_combinations)
    assert "LIMIT" not in registry_source.upper()
    assert "LIMIT" not in combination_source.upper()
    assert "ORDER BY r.created_at, r.strategy_key" in registry_source
    assert "ORDER BY c.created_at, c.combination_key" in combination_source


def test_validation_artifact_is_recomputed_from_trades_and_equity_curve():
    metrics, artifact, artifact_hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    validated = governance_module._validate_metric_artifact(
        artifact,
        entity_type="STRATEGY",
        entity_key="artifact_test_strategy",
        entity_version="v1",
        as_of_date=artifact["as_of_date"],
        window_days=120,
        evidence_protocol="PURGED_WALK_FORWARD_V2",
        evidence_revision_at=revision_at,
        metrics=metrics,
        artifact_hash=artifact_hash,
        version_created_at=version_created_at,
        expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
        expected_label_horizon_days=2,
    )
    assert validated["source_dataset_hash"] == artifact["source_dataset_hash"]


def test_validation_artifact_v3_rejects_unmatured_or_too_early_labels():
    metrics, artifact, _artifact_hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    future = deepcopy(artifact)
    future_day = (
        date.fromisoformat(future["as_of_date"]) + timedelta(days=1)
    ).isoformat()
    future["trades"][-1]["label_available_at"] = future_day + "T15:00:00"
    future["trades"][-1]["observed_at"] = future_day + "T15:00:00"
    future["evidence_revision_at"] = future_day + "T15:00:00"
    with pytest.raises(ValueError, match="标签"):
        governance_module._validate_metric_artifact(
            future,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=future["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=future_day + "T15:00:00",
            metrics=metrics,
            artifact_hash=governance_module._digest(future),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )

    too_early = deepcopy(artifact)
    first_trade = too_early["trades"][0]
    first_trade["label_available_at"] = (
        first_trade["trade_date"] + "T15:00:00"
    )
    first_trade["observed_at"] = first_trade["label_available_at"]
    with pytest.raises(ValueError, match="标签成熟期"):
        governance_module._validate_metric_artifact(
            too_early,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=too_early["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(too_early),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


def test_validation_artifact_v2_rejects_insufficient_purge():
    metrics, artifact, _artifact_hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    invalid = deepcopy(artifact)
    first_test_start = date.fromisoformat(
        invalid["segments"][0]["test_start"]
    )
    # Only one complete day remains between train and test, while the frozen
    # label/holding protocol requires two.
    invalid["segments"][0]["train_end"] = (
        first_test_start - timedelta(days=2)
    ).isoformat()
    with pytest.raises(ValueError, match="purge"):
        governance_module._validate_metric_artifact(
            invalid,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=invalid["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(invalid),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


def test_validation_artifact_v2_rejects_insufficient_embargo():
    metrics, artifact, _artifact_hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    invalid = deepcopy(artifact)
    prior_segment = invalid["segments"][0]
    prior_rows = [
        item for item in invalid["trades"]
        if prior_segment["test_start"]
        <= item["trade_date"] <= prior_segment["test_end"]
    ]
    prior_sample = prior_rows[0]
    prior_maturity_day = max(
        item["label_available_at"][:10] for item in prior_rows
    )
    segment = invalid["segments"][2]
    # Exactly one authoritative session follows prior maturity, one short of
    # the frozen two-session embargo while purge before this test still holds.
    segment["train_end"] = (
        date.fromisoformat(prior_maturity_day) + timedelta(days=1)
    ).isoformat()
    segment["train_dataset"].append({
        "observation_id": prior_sample["evidence_id"],
        "observed_at": prior_sample["trade_date"] + "T15:00:00",
        "label_available_at": prior_sample["label_available_at"],
        "feature_snapshot_hash": "d" * 64,
        "label_snapshot_hash": "e" * 64,
    })
    normalized_train = sorted(
        segment["train_dataset"],
        key=lambda item: (
            item["observed_at"], item["label_available_at"],
            item["observation_id"],
        ),
    )
    segment["train_dataset_hash"] = governance_module._digest({
        "segment_index": 3,
        "train_start": segment["train_start"],
        "train_end": segment["train_end"],
        "observations": normalized_train,
    })
    with pytest.raises(ValueError, match="embargo"):
        governance_module._validate_metric_artifact(
            invalid,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=invalid["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(invalid),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


def _consume_first_test_fold_sample(
    artifact: dict, *, target_segment_index: int,
) -> str:
    prior_segment = artifact["segments"][0]
    prior_rows = [
        item for item in artifact["trades"]
        if prior_segment["test_start"]
        <= item["trade_date"] <= prior_segment["test_end"]
    ]
    sample = max(prior_rows, key=lambda item: item["label_available_at"])
    target = artifact["segments"][target_segment_index]
    target["train_dataset"].append({
        "observation_id": sample["evidence_id"],
        "observed_at": sample["trade_date"] + "T15:00:00",
        "label_available_at": sample["label_available_at"],
        "feature_snapshot_hash": governance_module._digest({
            "kind": "consumed-feature", "sample": sample["evidence_id"],
        }),
        "label_snapshot_hash": governance_module._digest({
            "kind": "consumed-label", "sample": sample["evidence_id"],
        }),
    })
    normalized = sorted(
        target["train_dataset"],
        key=lambda item: (
            item["observed_at"], item["label_available_at"],
            item["observation_id"],
        ),
    )
    target["train_dataset_hash"] = governance_module._digest({
        "segment_index": target_segment_index + 1,
        "train_start": target["train_start"],
        "train_end": target["train_end"],
        "observations": normalized,
    })
    return max(item["label_available_at"] for item in prior_rows)


def test_walk_forward_5_20_rejects_training_before_actual_label_maturity():
    metrics, artifact, _hash, revision_at, version_created_at = (
        _validation_artifact_fixture(
            label_horizon_days=5,
            max_holding_days=20,
            test_label_delay_days=20,
            embargo_days=5,
        )
    )
    invalid = deepcopy(artifact)
    _consume_first_test_fold_sample(invalid, target_segment_index=2)

    with pytest.raises(ValueError, match="标签未成熟|训练高水位"):
        governance_module._validate_metric_artifact(
            invalid,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=invalid["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(invalid),
            version_created_at=version_created_at,
            expected_max_holding_days=20,
            expected_label_horizon_days=5,
        )


def test_walk_forward_5_20_accepts_consumption_after_maturity_and_embargo():
    metrics, artifact, _hash, revision_at, version_created_at = (
        _validation_artifact_fixture(
            label_horizon_days=5,
            max_holding_days=20,
            test_label_delay_days=20,
            embargo_days=5,
        )
    )
    _consume_first_test_fold_sample(artifact, target_segment_index=4)

    validated = governance_module._validate_metric_artifact(
        artifact,
        entity_type="STRATEGY",
        entity_key="artifact_test_strategy",
        entity_version="v1",
        as_of_date=artifact["as_of_date"],
        window_days=120,
        evidence_protocol="PURGED_WALK_FORWARD_V2",
        evidence_revision_at=revision_at,
        metrics=metrics,
        artifact_hash=governance_module._digest(artifact),
        version_created_at=version_created_at,
        expected_max_holding_days=20,
        expected_label_horizon_days=5,
    )

    assert validated["validation_protocol"]["max_holding_days"] == 20


def test_walk_forward_5_20_rejects_one_session_short_of_embargo():
    metrics, artifact, _hash, revision_at, version_created_at = (
        _validation_artifact_fixture(
            label_horizon_days=5,
            max_holding_days=20,
            test_label_delay_days=20,
            embargo_days=5,
        )
    )
    invalid = deepcopy(artifact)
    maturity_at = _consume_first_test_fold_sample(
        invalid, target_segment_index=4,
    )
    target = invalid["segments"][4]
    target["train_end"] = (
        date.fromisoformat(maturity_at[:10]) + timedelta(days=4)
    ).isoformat()
    normalized = sorted(
        target["train_dataset"],
        key=lambda item: (
            item["observed_at"], item["label_available_at"],
            item["observation_id"],
        ),
    )
    target["train_dataset_hash"] = governance_module._digest({
        "segment_index": 5,
        "train_start": target["train_start"],
        "train_end": target["train_end"],
        "observations": normalized,
    })

    with pytest.raises(ValueError, match="embargo"):
        governance_module._validate_metric_artifact(
            invalid,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=invalid["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(invalid),
            version_created_at=version_created_at,
            expected_max_holding_days=20,
            expected_label_horizon_days=5,
        )


def test_walk_forward_rejects_training_sample_canonical_id_alias():
    metrics, artifact, _hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    invalid = deepcopy(artifact)
    source = dict(invalid["segments"][0]["train_dataset"][0])
    source["observation_id"] = governance_module._digest({
        "schema": "probiga.validation-sample-id.v1",
        "source_key": "alias-of-existing-training-sample",
    })
    target = invalid["segments"][1]
    target["train_dataset"].append(source)
    normalized = sorted(
        target["train_dataset"],
        key=lambda item: (
            item["observed_at"], item["label_available_at"],
            item["observation_id"],
        ),
    )
    target["train_dataset_hash"] = governance_module._digest({
        "segment_index": 2,
        "train_start": target["train_start"],
        "train_end": target["train_end"],
        "observations": normalized,
    })

    with pytest.raises(ValueError, match="编号别名"):
        governance_module._validate_metric_artifact(
            invalid,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=invalid["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(invalid),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


def test_validation_artifact_v2_rejects_forged_training_hash_and_label_horizon():
    metrics, artifact, _artifact_hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    forged = deepcopy(artifact)
    forged["segments"][0]["train_dataset_hash"] = "f" * 64
    with pytest.raises(ValueError, match="训练集或测试集哈希无效"):
        governance_module._validate_metric_artifact(
            forged,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=forged["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(forged),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )
    wrong_label = deepcopy(artifact)
    wrong_label["validation_protocol"]["label_horizon_days"] = 1
    with pytest.raises(ValueError, match="不可变版本协议"):
        governance_module._validate_metric_artifact(
            wrong_label,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=wrong_label["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(wrong_label),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


def _ledger_record(
    *, evidence_id: str, stock_code: str, entry_day: str,
    entry_price: float, quantity: int, exit_day: str = "",
    exit_price: float = 0.0, strategy_key: str = "ledger_strategy",
    strategy_version: str = "v1",
) -> dict:
    entry_gross = entry_price * quantity
    matured = bool(exit_day)
    exit_gross = exit_price * quantity if matured else 0.0
    entry_fee = 1.0
    exit_fee = 1.0 if matured else 0.0
    realized = (
        (exit_gross - exit_fee - entry_gross - entry_fee)
        / (entry_gross + entry_fee) * 100.0
        if matured else None
    )
    return {
        "evidence_id": evidence_id,
        "source_intent_id": f"intent-{evidence_id}"[:64],
        "source_run_uid": f"run-{evidence_id}"[:64],
        "entry_fill_id": f"entry-{evidence_id}"[:64],
        "account_id": "paper-test",
        "stock_code": stock_code,
        "strategy_key": strategy_key,
        "bound_strategy_version": strategy_version,
        "entry_at": entry_day + "T09:30:00",
        "entry_trade_date": entry_day,
        "entry_quantity": quantity,
        "closed_quantity": quantity if matured else 0,
        "entry_price": entry_price,
        "exit_average_price": exit_price if matured else None,
        "exit_at": exit_day + "T15:00:00" if matured else None,
        "trade_date": exit_day if matured else None,
        "return_pct": realized,
        "entry_gross_cny": entry_gross,
        "entry_fee_cny": entry_fee,
        "source_intent_buy_fill_count": 1,
        "source_intent_entry_quantity": quantity,
        "source_intent_entry_gross_cny": entry_gross,
        "source_intent_entry_fee_cny": entry_fee,
        "exit_gross_cny": exit_gross,
        "exit_fee_cny": exit_fee,
        "evidence_status": "MATURED" if matured else "OPEN",
        "entry_cash_binding_count": 1,
        "exit_fill_ids_json": (
            json.dumps([f"sell-{evidence_id}"]) if matured else "[]"
        ),
        "exit_order_ids_json": (
            json.dumps([f"sell-order-{evidence_id}"]) if matured else "[]"
        ),
        "exit_fill_id_count": 1 if matured else 0,
        "exit_allocation_count": 1 if matured else 0,
        "exit_allocated_fill_count": 1 if matured else 0,
        "exit_fill_binding_count": 1 if matured else 0,
        "exit_order_id_count": 1 if matured else 0,
        "exit_allocated_order_count": 1 if matured else 0,
        "exit_order_binding_count": 1 if matured else 0,
        "exit_cash_binding_count": 1 if matured else 0,
        "exit_global_conservation_count": 1 if matured else 0,
        "exit_allocation_protocol_count": 1 if matured else 0,
        "exit_fill_trade_day_count": 1 if matured else 0,
        "exit_fill_quantity_sum": quantity if matured else 0,
        "exit_fill_gross_sum": exit_gross if matured else 0,
        "exit_fill_fee_sum": exit_fee if matured else 0,
        "exit_fill_latest_at": (
            exit_day + "T15:00:00" if matured else None
        ),
        "is_net_return": matured,
        "actual_cost_pct": (
            (entry_fee + exit_fee) / entry_gross * 100.0
            if matured else None
        ),
        "evidence_revision_at": (
            exit_day + "T15:00:00" if matured else entry_day + "T15:00:00"
        ),
    }


def _attested_bar(stock_code, trade_date, close, pre_close=None):
    return {
        "stock_code": stock_code,
        "trade_date": trade_date,
        "close": close,
        "pre_close": close if pre_close is None else pre_close,
        "data_source": "gj_big_qmt_inner",
        "quality_status": "QMT_ATTESTED",
        "permission_status": "SUPPORTED",
        "source_time": trade_date + "T15:00:00",
        "received_at": trade_date + "T15:01:00",
        "batch_id": "qmt-test-batch",
        "data_version": "qmt-test-v1",
        "attestation_id": "e" * 64,
        "pre_close_attestation_protocol": (
            governance_module.QMT_PRECLOSE_ATTESTATION_PROTOCOL
        ),
        "source_pre_close_origin": "NATIVE_QMT",
    }


def test_internal_ledger_qmt_join_recomputes_row_attestation_identity():
    source = inspect.getsource(
        governance_module._internal_strategy_portfolio_ledger
    )
    for required_fragment in (
        "a.qmt_id>0",
        "a.trade_date=k.trade_date",
        "BINARY a.stock_code=BINARY k.stock_code",
        "BINARY a.attestation_id=BINARY SHA2(CONCAT_WS",
        "a.protocol_version, a.target_id, a.qmt_id",
        "a.source_data_version, a.source_pre_close",
        "a.attested_open, a.attested_close, a.attested_high",
        "a.attested_low, a.attested_volume, a.attested_amount",
    ):
        assert required_fragment in source


def test_internal_ledger_rebuilds_overlapping_positions_and_marks_open_lot(monkeypatch):
    records = [
        _ledger_record(
            evidence_id="fill-a", stock_code="000001",
            entry_day="2026-08-18", entry_price=10.0, quantity=100,
            exit_day="2026-08-20", exit_price=12.0,
        ),
        _ledger_record(
            evidence_id="fill-b", stock_code="000002",
            entry_day="2026-08-19", entry_price=20.0, quantity=100,
        ),
    ]

    def fake_read(sql, _params=None):
        if "FROM si_trade_calendar" in sql:
            return [
                {"trade_date": day}
                for day in ("2026-08-18", "2026-08-19", "2026-08-20")
            ]
        if "FROM sm_stock_kline" in sql:
            return [
                _attested_bar("000001", "2026-08-18", 10, 10),
                _attested_bar("000001", "2026-08-19", 11, 10),
                _attested_bar("000001", "2026-08-20", 12, 11),
                _attested_bar("000002", "2026-08-19", 20, 20),
                _attested_bar("000002", "2026-08-20", 19, 20),
            ]
        if "FROM st_trade_account_v2" in sql:
            return [{
                "account_id": "paper-test", "status": "PAPER",
                "initial_cash": 10000, "policy_version": "paper-v1",
                "policy_hash": "a" * 64, "real_trading_enabled": 0,
                "created_at": "2026-01-01T00:00:00",
            }]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module,
        "load_v3_config",
        lambda: {"account": {"initial_cash_cny": 10000}},
    )
    ledger = governance_module._internal_strategy_portfolio_ledger(
        records,
        as_of_date="2026-08-20",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
    )
    assert ledger["valid"] is True
    assert [point["equity"] for point in ledger["equity_curve"]] == [
        99.99, 100.98, 100.97,
    ]
    assert ledger["open_position_count"] == 1
    assert ledger["funding_evidence_revision_at"] == "2026-08-20T15:00:00"
    assert ledger["max_drawdown_pct"] == pytest.approx(0.01, abs=0.0001)
    assert len(ledger["internal_ledger_hash"]) == 64
    replay = governance_module._internal_strategy_portfolio_ledger(
        records,
        as_of_date="2026-08-20",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
    )
    assert replay["internal_ledger_hash"] == ledger["internal_ledger_hash"]
    sliced = governance_module._slice_internal_ledger(
        ledger, start_date="2026-08-19", as_of_date="2026-08-20",
    )
    assert sliced["valid"] is True
    assert sliced["exposure_basis"] == (
        "TIME_WEIGHTED_DAILY_MAX_CLOSE_OR_TURNOVER_PROXY_V2"
    )
    # fill-a was opened before the sliced window but remained held on its
    # first day; the overlap/concentration proof must not lose that exposure.
    assert Decimal(sliced["stock_exposure"]["000001"]) > 0
    assert Decimal(sliced["stock_exposure"]["000002"]) > 0

    mixed = deepcopy(records)
    mixed[0]["bound_strategy_version"] = "other-version"
    rejected = governance_module._internal_strategy_portfolio_ledger(
        mixed,
        as_of_date="2026-08-20",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
    )
    assert rejected["valid"] is False
    assert "其他策略或版本" in rejected["reason"]


def test_internal_ledger_window_drawdown_includes_first_session_loss():
    ledger = {
        "valid": True,
        "internal_ledger_hash": "a" * 64,
        "equity_curve": [
            {"trade_date": "2026-08-19", "equity": 100.0},
            {"trade_date": "2026-08-20", "equity": 50.0},
            {"trade_date": "2026-08-21", "equity": 51.0},
        ],
        "daily_records": [
            {"trade_date": "2026-08-19", "return_pct": 0.0},
            {"trade_date": "2026-08-20", "return_pct": -50.0},
            {"trade_date": "2026-08-21", "return_pct": 2.0},
        ],
        "daily_stock_market_values": [
            {"trade_date": day, "stock_risk_exposure": {"000001": "100"}}
            for day in ("2026-08-19", "2026-08-20", "2026-08-21")
        ],
        "trade_exposures": [
            {
                "trade_date": "2026-08-20",
                "source_intent_id": "one-intent-eighty-fills",
                "status": "MATURED",
            }
            for _index in range(80)
        ],
    }

    sliced = governance_module._slice_internal_ledger(
        ledger,
        start_date="2026-08-20",
        as_of_date="2026-08-21",
    )

    assert sliced["valid"] is True
    assert sliced["opening_equity"] == 100.0
    assert sliced["opening_equity_date"] == "2026-08-19"
    assert sliced["opening_equity_source"] == "PREVIOUS_AUTHORITATIVE_SESSION"
    assert sliced["max_drawdown_pct"] == 50.0
    assert sliced["completed_trade_count"] == 1


def test_flat_cash_days_do_not_advance_funding_evidence_high_water(monkeypatch):
    records = [_ledger_record(
        evidence_id="closed-fill", stock_code="000001",
        entry_day="2026-08-18", entry_price=10.0, quantity=100,
        exit_day="2026-08-20", exit_price=11.0,
    )]

    def fake_read(sql, params=None):
        if "FROM si_trade_calendar" in sql:
            end = str((params or {}).get("as_of_date") or "2026-08-20")
            return [
                {"trade_date": day}
                for day in (
                    "2026-08-18", "2026-08-19", "2026-08-20",
                    "2026-08-21", "2026-08-22",
                )
                if day <= end
            ]
        if "FROM sm_stock_kline" in sql:
            return [
                _attested_bar("000001", "2026-08-18", 10, 10),
                _attested_bar("000001", "2026-08-19", 10.5, 10),
                _attested_bar("000001", "2026-08-20", 11, 10.5),
            ]
        if "FROM st_trade_account_v2" in sql:
            return [{
                "account_id": "paper-test", "status": "PAPER",
                "initial_cash": 10000, "policy_version": "paper-v1",
                "policy_hash": "a" * 64, "real_trading_enabled": 0,
                "created_at": "2026-01-01T00:00:00",
            }]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module, "load_v3_config",
        lambda: {"account": {"initial_cash_cny": 10000}},
    )
    first = governance_module._internal_strategy_portfolio_ledger(
        records, as_of_date="2026-08-20", strategy_key="ledger_strategy",
        strategy_version="v1", version_hash="f" * 64,
    )
    later = governance_module._internal_strategy_portfolio_ledger(
        records, as_of_date="2026-08-22", strategy_key="ledger_strategy",
        strategy_version="v1", version_hash="f" * 64,
    )
    assert first["valid"] is True and later["valid"] is True
    assert first["internal_ledger_hash"] != later["internal_ledger_hash"]
    assert first["funding_evidence_revision_at"] == (
        later["funding_evidence_revision_at"]
    ) == "2026-08-20T15:00:00"


def test_internal_ledger_rejects_cross_session_aggregate_exit():
    record = _ledger_record(
        evidence_id="cross-session", stock_code="000001",
        entry_day="2026-08-18", entry_price=10.0, quantity=100,
        exit_day="2026-08-20", exit_price=11.0,
    )
    record["exit_fill_trade_day_count"] = 2
    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date="2026-08-20",
        strategy_key="ledger_strategy", strategy_version="v1",
        version_hash="f" * 64,
    )
    assert ledger["valid"] is False
    assert "跨交易日分批平仓" in ledger["reason"]


def test_internal_ledger_rejects_forged_sell_fill_economics():
    record = _ledger_record(
        evidence_id="forged-exit", stock_code="000001",
        entry_day="2026-08-18", entry_price=10.0, quantity=100,
        exit_day="2026-08-20", exit_price=11.0,
    )
    record["exit_fill_gross_sum"] = 999999
    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date="2026-08-20",
        strategy_key="ledger_strategy", strategy_version="v1",
        version_hash="f" * 64,
    )
    assert ledger["valid"] is False
    assert "正规化SELL分摊账本重建" in ledger["reason"]


def test_internal_ledger_rejects_unbound_extra_exit_order():
    record = _ledger_record(
        evidence_id="forged-exit-order", stock_code="000001",
        entry_day="2026-08-18", entry_price=10.0, quantity=100,
        exit_day="2026-08-20", exit_price=11.0,
    )
    record["exit_order_ids_json"] = json.dumps([
        "sell-order-forged-exit-order", "unrelated-order",
    ])
    record["exit_order_id_count"] = 2
    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date="2026-08-20",
        strategy_key="ledger_strategy", strategy_version="v1",
        version_hash="f" * 64,
    )
    assert ledger["valid"] is False
    assert "正规化逐笔卖出分摊" in ledger["reason"]


def test_internal_ledger_rejects_exit_allocation_overflow():
    record = _ledger_record(
        evidence_id="allocation-overflow", stock_code="000001",
        entry_day="2026-08-18", entry_price=10.0, quantity=100,
        exit_day="2026-08-20", exit_price=11.0,
    )
    record["exit_global_conservation_count"] = 0
    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date="2026-08-20",
        strategy_key="ledger_strategy", strategy_version="v1",
        version_hash="f" * 64,
    )
    assert ledger["valid"] is False
    assert "全局守恒" in ledger["reason"]


def test_internal_ledger_quarantines_corporate_action_gap(monkeypatch):
    record = _ledger_record(
        evidence_id="open-across-action", stock_code="000001",
        entry_day="2026-08-18", entry_price=10.0, quantity=100,
    )

    def fake_read(sql, _params=None):
        if "FROM si_trade_calendar" in sql:
            return [
                {"trade_date": "2026-08-18"},
                {"trade_date": "2026-08-19"},
            ]
        if "FROM sm_stock_kline" in sql:
            return [
                _attested_bar("000001", "2026-08-18", 10, 10),
                _attested_bar("000001", "2026-08-19", 5, 5),
            ]
        if "FROM st_trade_account_v2" in sql:
            return [{
                "account_id": "paper-test", "status": "PAPER",
                "initial_cash": 10000, "policy_version": "paper-v1",
                "policy_hash": "a" * 64, "real_trading_enabled": 0,
                "created_at": "2026-01-01T00:00:00",
            }]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module, "load_v3_config",
        lambda: {"account": {"initial_cash_cny": 10000}},
    )
    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date="2026-08-19",
        strategy_key="ledger_strategy", strategy_version="v1",
        version_hash="f" * 64,
    )
    assert ledger["valid"] is False
    assert "公司行动账本" in ledger["reason"]


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("permission_status", "UNKNOWN"),
        ("source_time", "2026-08-18T14:59:59"),
        ("received_at", "2026-08-18T14:59:59"),
        ("attestation_id", ""),
        ("pre_close_attestation_protocol", "LEGACY_BATCH_V1"),
        ("source_pre_close_origin", "UNVERIFIED_LEGACY"),
    ),
)
def test_internal_ledger_rejects_incomplete_held_bar_attestation(
    monkeypatch, field, value,
):
    record = _ledger_record(
        evidence_id="open-unattested", stock_code="000001",
        entry_day="2026-08-18", entry_price=10.0, quantity=100,
    )
    bar = _attested_bar("000001", "2026-08-18", 10, 10)
    bar[field] = value

    def fake_read(sql, _params=None):
        if "FROM si_trade_calendar" in sql:
            return [{"trade_date": "2026-08-18"}]
        if "FROM sm_stock_kline" in sql:
            return [bar]
        if "FROM st_trade_account_v2" in sql:
            return [{
                "account_id": "paper-test", "status": "PAPER",
                "initial_cash": 10000, "policy_version": "paper-v1",
                "policy_hash": "a" * 64, "real_trading_enabled": 0,
                "created_at": "2026-01-01T00:00:00",
            }]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module, "load_v3_config",
        lambda: {"account": {"initial_cash_cny": 10000}},
    )
    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date="2026-08-18",
        strategy_key="ledger_strategy", strategy_version="v1",
        version_hash="f" * 64,
    )
    assert ledger["valid"] is False
    assert "QMT权威收盘日线认证" in ledger["reason"]


def test_internal_economics_plus_external_selection_can_pass_funding(monkeypatch):
    start = date(2026, 1, 1)
    records = []
    for index in range(100):
        day = (start + timedelta(days=index)).isoformat()
        for sequence in range(4):
            records.append(_ledger_record(
                evidence_id=f"profitable-{index:03d}-{sequence}",
                stock_code="000001",
                entry_day=day,
                entry_price=10.0,
                quantity=100,
                exit_day=day,
                exit_price=10.17 if index % 10 < 7 else 9.97,
                strategy_key="profitable_internal",
                strategy_version="v1",
            ))
    trade_date = records[-1]["trade_date"]
    selection = {}
    for window, token in zip((20, 60, 120), ("a", "b", "c")):
        selection[("profitable_internal", window)] = {
            "completed_trades": 400,
            "coverage_days": 100,
            "net_expectancy_pct": -99.0,
            "max_drawdown_pct": 99.0,
            "funding_provenance": "EXTERNAL_SUBMITTED",
            "walk_forward_verified": True,
            "walk_forward_segments": 5,
            "positive_segments": 5,
            "independent_oos": True,
            "evidence_protocol": "PURGED_WALK_FORWARD_V2",
            "artifact_hash": token * 64,
            "source_dataset_hash": token.upper().lower() * 64,
            "evidence_revision_at": f"{trade_date}T15:00:00",
            "verification_status": "CONFIRMED",
            "submitted_by": "user-id:1",
            "reviewed_by": "user-id:2",
            "reviewed_at": f"{trade_date}T16:00:00",
            "review_audit_valid": True,
            "evidence_hash": governance_module._digest({
                "window": window, "token": token,
            }),
        }

    def fake_read(sql, _params=None):
        if "FROM si_trade_calendar" in sql:
            return [
                {"trade_date": (start + timedelta(days=index)).isoformat()}
                for index in range(100)
            ]
        if "FROM sm_stock_kline" in sql:
            return []
        if "FROM st_trade_account_v2" in sql:
            return [{
                "account_id": "paper-test", "status": "PAPER",
                "initial_cash": 200000, "policy_version": "paper-v1",
                "policy_hash": "d" * 64, "real_trading_enabled": 0,
                "created_at": "2025-12-01T00:00:00",
            }]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module, "_load_forward_records",
        lambda *_args, **_kwargs: {"profitable_internal": records},
    )
    monkeypatch.setattr(
        governance_module, "_load_metric_inputs",
        lambda *_args, **_kwargs: selection,
    )
    monkeypatch.setattr(
        governance_module,
        "load_v3_config",
        lambda: {"account": {"initial_cash_cny": 200000}},
    )
    registry = [{
        "strategy_key": "profitable_internal",
        "current_version": "v1",
        "version_hash": "f" * 64,
        "market_route": {
            "market_match_score": 100.0,
            "router_decision_hash": "e" * 64,
        },
    }]
    metrics = governance_module._metrics_for_registry(
        {}, registry, trade_date
    )["profitable_internal"]
    assert metrics[60]["profit_gate"]["passed"] is True, (
        metrics[60]["profit_gate"]["failed_checks"],
        metrics[60].get("coverage_days"),
        metrics[60].get("session_window_start"),
    )
    assert metrics[120]["profit_gate"]["passed"] is True
    assert metrics[60]["funding_provenance"] == (
        "INTERNAL_PORTFOLIO_LEDGER_V1"
    )
    assert metrics[60]["net_expectancy_pct"] > 0
    assert metrics[60]["max_drawdown_pct"] < 12
    assert metrics[60]["selection_validation_scope"] == (
        "VERSION_SELECTION_ONLY"
    )
    assert metrics[60]["selection_evidence_hash"] == selection[
        ("profitable_internal", 60)
    ]["evidence_hash"]
    assert governance_module._HASH_PATTERN.fullmatch(
        metrics[60]["internal_trade_evidence_hash"]
    )
    assert metrics[60]["evidence_hash"] == governance_module._digest({
        "internal_trade_evidence_hash": metrics[60][
            "internal_trade_evidence_hash"
        ],
        "internal_ledger_hash": metrics[60]["internal_ledger_hash"],
        "selection_evidence_hash": metrics[60]["selection_evidence_hash"],
        "selection_artifact_hash": metrics[60]["artifact_hash"],
        "strategy_key": "profitable_internal",
        "strategy_version": "v1",
        "window_days": 60,
    })
    # External research values can attest selection but cannot replace money,
    # fees, drawdown or provenance from the internal ledger.
    assert metrics[60]["net_expectancy_pct"] != -99.0
    assert metrics[60]["max_drawdown_pct"] != 99.0


def test_combination_builds_independent_nav_and_enforces_correlation_overlap(monkeypatch):
    start = date(2026, 1, 1)

    def daily(pattern):
        return [
            {
                "trade_date": (start + timedelta(days=index)).isoformat(),
                "return_pct": pattern[index % len(pattern)],
                "actual_cost_pct": 0.01,
                "is_net_return": True,
                "evidence_revision_at": (
                    start + timedelta(days=index)
                ).isoformat() + "T15:00:00",
            }
            for index in range(60)
        ]

    def equity_curve(rows):
        # The member accumulated gains before this 60-session window.  The
        # combination must rebase at the window open instead of treating the
        # absolute 120 NAV level as the first day's return.
        equity = 120.0
        result = []
        for row in rows:
            equity *= 1.0 + float(row["return_pct"]) / 100.0
            result.append({
                "trade_date": row["trade_date"], "equity": equity,
            })
        return result

    left_daily = daily([1.0, -1.0, 1.0, -1.0])
    right_daily = daily([1.0, 1.0, -1.0, -1.0])

    left_metrics = {
        "funding_provenance": "INTERNAL_PORTFOLIO_LEDGER_V1",
        "internal_ledger_hash": "a" * 64,
        "internal_daily_records": left_daily,
        "internal_equity_curve": equity_curve(left_daily),
        "internal_stock_exposure": {"000001": "10000"},
        "internal_stock_exposure_basis": (
            "TIME_WEIGHTED_DAILY_MAX_CLOSE_OR_TURNOVER_PROXY_V2"
        ),
        "completed_trades": 100,
        "evidence_revision_at": "2026-03-21T15:00:00",
        "session_window_valid": True,
        "session_window_start": "2026-01-01",
        "session_window_end": "2026-03-01",
        "session_window_count": 60,
        "session_window_hash": "e" * 64,
    }
    right_metrics = {
        "funding_provenance": "INTERNAL_PORTFOLIO_LEDGER_V1",
        "internal_ledger_hash": "b" * 64,
        "internal_daily_records": right_daily,
        "internal_equity_curve": equity_curve(right_daily),
        "internal_stock_exposure": {"000002": "10000"},
        "internal_stock_exposure_basis": (
            "TIME_WEIGHTED_DAILY_MAX_CLOSE_OR_TURNOVER_PROXY_V2"
        ),
        "completed_trades": 100,
        "evidence_revision_at": "2026-03-21T15:00:00",
        "session_window_valid": True,
        "session_window_start": "2026-01-01",
        "session_window_end": "2026-03-01",
        "session_window_count": 60,
        "session_window_hash": "e" * 64,
    }
    members = [
        {
            "weight": 0.5,
            "frozen_version": "v1",
            "version_match": True,
            "strategy": {
                "strategy_key": "left_strategy",
                "current_version": "v1",
                "metrics": {"60": left_metrics},
            },
        },
        {
            "weight": 0.5,
            "frozen_version": "v1",
            "version_match": True,
            "strategy": {
                "strategy_key": "right_strategy",
                "current_version": "v1",
                "metrics": {"60": right_metrics},
            },
        },
    ]
    constraints = governance_module._validated_combination_constraints({
        "maximum_industry_weight_pct": 60.0,
    })
    combination = {
        "combination_key": "diversified_combo",
        "current_version": "v1",
        "config_hash": "c" * 64,
        "constraints": constraints,
    }

    monkeypatch.setattr(
        governance_module,
        "_db_read",
        lambda sql, _params=None: [
            {
                "stock_code": "000001", "industry_name": "银行",
                "etl_sync_at": "2026-01-01", "id": 1,
            },
            {
                "stock_code": "000002", "industry_name": "电子",
                "etl_sync_at": "2026-01-01", "id": 2,
            },
        ] if "FROM si_industry_sw" in sql else [],
    )
    ledger = governance_module._internal_combination_portfolio_ledger(
        combination, members, window=60, trade_date="2026-03-21"
    )
    assert ledger["valid"] is True
    assert ledger["internal_ledger_schema"] == (
        "probiga.internal-combination-portfolio-ledger.v2"
    )
    assert ledger["allocation_semantics"] == (
        "WINDOW_OPEN_REBASED_FIXED_SLEEVES_NATURAL_WEIGHT_DRIFT_V2"
    )
    assert len(ledger["equity_curve"]) == 60
    assert ledger["daily_records"][0]["return_pct"] == pytest.approx(1.0)
    assert ledger["equity_curve"][0]["equity"] == pytest.approx(101.0)
    evaluation = governance_module._combination_constraint_evaluation(
        combination, members
    )
    assert evaluation["passed"] is True
    assert evaluation["pairwise_correlations"][0]["correlation"] == (
        pytest.approx(0.0, abs=1e-9)
    )

    concentrated = deepcopy(members)
    concentrated[1]["strategy"]["metrics"]["60"][
        "internal_daily_records"
    ] = deepcopy(left_metrics["internal_daily_records"])
    concentrated[1]["strategy"]["metrics"]["60"][
        "internal_equity_curve"
    ] = deepcopy(left_metrics["internal_equity_curve"])
    concentrated[1]["strategy"]["metrics"]["60"][
        "internal_stock_exposure"
    ] = {"000001": "10000"}
    rejected = governance_module._combination_constraint_evaluation(
        combination, concentrated
    )
    assert rejected["passed"] is False
    assert rejected["pairwise_correlations"][0]["passed"] is False
    assert rejected["pairwise_stock_overlaps"][0]["passed"] is False


def test_validation_artifact_rejects_forged_constant_equity_curve():
    metrics, artifact, _artifact_hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    forged_metrics = dict(metrics)
    forged_metrics["max_drawdown_pct"] = 0.0
    forged = deepcopy(artifact)
    forged["metrics_hash"] = governance_module._digest(forged_metrics)
    forged["equity_curve"] = [
        {"trade_date": point["trade_date"], "equity": 100.0}
        for point in forged["equity_curve"]
    ]
    forged["source_dataset_hash"] = governance_module._digest({
        "trades": forged["trades"],
        "equity_curve": forged["equity_curve"],
    })

    with pytest.raises(ValueError, match="逐笔净收益序列逐点精确重建"):
        governance_module._validate_metric_artifact(
            forged,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=forged["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=forged_metrics,
            artifact_hash=governance_module._digest(forged),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


def test_validation_artifact_rejects_overlapping_oos_folds():
    metrics, artifact, _artifact_hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    forged = deepcopy(artifact)
    first = forged["segments"][0]
    second = forged["segments"][1]
    second["test_start"] = first["test_end"]
    second["train_end"] = (
        date.fromisoformat(second["test_start"]) - timedelta(days=3)
    ).isoformat()
    with pytest.raises(ValueError, match="严格按时间排序且不得重叠"):
        governance_module._validate_metric_artifact(
            forged,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=forged["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(forged),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


def test_validation_artifact_rejects_oos_sample_omitted_from_all_folds():
    metrics, artifact, _artifact_hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    forged = deepcopy(artifact)
    segment = forged["segments"][1]
    segment["test_start"] = (
        date.fromisoformat(segment["test_start"]) + timedelta(days=1)
    ).isoformat()
    segment_rows = [
        item for item in forged["trades"]
        if segment["test_start"] <= item["trade_date"] <= segment["test_end"]
    ]
    segment["completed_trades"] = len(segment_rows)
    segment["net_expectancy_pct"] = sum(
        item["net_return_pct"] for item in segment_rows
    ) / len(segment_rows)
    segment["test_dataset_hash"] = governance_module._digest({
        "segment_index": 2,
        "test_start": segment["test_start"],
        "test_end": segment["test_end"],
        "trades": segment_rows,
    })
    with pytest.raises(ValueError, match="必须且只能归属一个测试段"):
        governance_module._validate_metric_artifact(
            forged,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=forged["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(forged),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


@pytest.mark.parametrize(
    ("claimed_column", "owner_type", "owner_key"),
    (
        ("artifact_hash", "STRATEGY", "other_strategy"),
        ("source_dataset_hash", "COMBINATION", "other_combination"),
    ),
)
def test_metric_artifact_and_dataset_have_one_global_entity_version_owner(
    claimed_column,
    owner_type,
    owner_key,
):
    class Result:
        def __init__(self, row):
            self.row = row

        def mappings(self):
            return self

        def first(self):
            return self.row

    class Connection:
        def __init__(self):
            self.queries = []

        def execute(self, statement, _params):
            sql = str(statement)
            self.queries.append(sql)
            row = None
            if f"WHERE {claimed_column}=:" in sql:
                row = {
                    "evidence_id": "existing-evidence",
                    "entity_type": owner_type,
                    "strategy_key": owner_key,
                    "strategy_version": "owner-v1",
                }
            return Result(row)

    connection = Connection()
    with pytest.raises(
        ValueError,
        match=f"{owner_type}:{owner_key}:owner-v1",
    ):
        governance_module._assert_global_metric_evidence_unclaimed(
            connection,
            {
                "entity_type": "STRATEGY",
                "strategy_key": "claiming_strategy",
                "strategy_version": "claim-v1",
                "artifact_hash": "a" * 64,
                "source_dataset_hash": "b" * 64,
            },
        )
    assert all("strategy_key=:strategy_key" not in sql for sql in connection.queries)
    assert {
        (index_name, column)
        for index_name, column, _label
        in governance_module._METRIC_GLOBAL_UNIQUE_INDEXES
    } == {
        ("uk_strategy_metric_artifact_global", "artifact_hash"),
        ("uk_strategy_metric_dataset_global", "source_dataset_hash"),
    }


def test_metric_artifact_global_owner_has_additive_database_constraints():
    source = inspect.getsource(
        governance_module.ensure_strategy_governance_tables
    )
    assert (
        "UNIQUE KEY uk_strategy_metric_artifact_global (artifact_hash)"
        in source
    )
    assert (
        "UNIQUE KEY uk_strategy_metric_dataset_global (source_dataset_hash)"
        in source
    )
    assert "GROUP BY {hash_column} HAVING COUNT(*)>1" in source


def test_validation_artifact_rejects_self_reported_inflated_profit_factor():
    metrics, artifact, _artifact_hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    inflated = dict(metrics)
    inflated["profit_factor"] = 999.0
    repackaged = deepcopy(artifact)
    repackaged["metrics_hash"] = governance_module._digest(inflated)
    with pytest.raises(ValueError, match="profit_factor"):
        governance_module._validate_metric_artifact(
            repackaged,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=repackaged["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=inflated,
            artifact_hash=governance_module._digest(repackaged),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


def test_validation_artifact_rejects_samples_not_after_version_freeze():
    metrics, artifact, artifact_hash, revision_at, _version_created_at = (
        _validation_artifact_fixture()
    )
    with pytest.raises(ValueError, match="晚于版本冻结日"):
        governance_module._validate_metric_artifact(
            artifact,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=artifact["as_of_date"],
            window_days=120,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=artifact_hash,
            version_created_at="2026-01-10T00:00:00",
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


def test_validation_artifact_rejects_reusing_long_history_as_short_window():
    metrics, artifact, _artifact_hash, revision_at, version_created_at = (
        _validation_artifact_fixture()
    )
    short_window_artifact = deepcopy(artifact)
    short_window_artifact["window_days"] = 20
    short_window = governance_module._authoritative_session_windows(
        short_window_artifact["as_of_date"]
    )[20]
    short_window_artifact.update({
        "window_session_start": short_window["start_date"],
        "window_session_end": short_window["end_date"],
        "window_session_count": short_window["session_count"],
        "window_session_hash": short_window["session_hash"],
    })
    with pytest.raises(ValueError, match="位于声明窗口"):
        governance_module._validate_metric_artifact(
            short_window_artifact,
            entity_type="STRATEGY",
            entity_key="artifact_test_strategy",
            entity_version="v1",
            as_of_date=short_window_artifact["as_of_date"],
            window_days=20,
            evidence_protocol="PURGED_WALK_FORWARD_V2",
            evidence_revision_at=revision_at,
            metrics=metrics,
            artifact_hash=governance_module._digest(short_window_artifact),
            version_created_at=version_created_at,
            expected_max_holding_days=ARTIFACT_MAX_HOLDING_DAYS,
            expected_label_horizon_days=2,
        )


def test_profitable_forward_records_pass_every_hard_gate():
    metrics = calculate_return_metrics(
        _profitable_records(),
        window_days=120,
        estimated_cost_pct=0.25,
        market_match_score=100.0,
        walk_forward_verified=True,
        version_bound_evidence=True,
        independent_oos=True,
    )
    _attest_funding_metrics(metrics)
    gate = evaluate_profit_gate(metrics)
    assert metrics["completed_trades"] == 100
    assert metrics["coverage_days"] >= PROFIT_GATE_POLICY["minimum_coverage_days"]
    assert metrics["net_expectancy_pct"] > 0
    assert metrics["payoff_ratio"] >= 1.10
    assert metrics["profit_factor"] >= 1.30
    assert metrics["positive_segments"] == 5
    assert gate["passed"] is True
    assert gate["failed_checks"] == []
    assert calculate_health_score(metrics) >= 80
    status, reason = recommend_lifecycle_status("SHADOW", metrics)
    assert status == "ACTIVE"
    assert "盈利硬门槛全部通过" in reason


def _valid_decay_gate_20_metrics() -> dict:
    metrics = calculate_return_metrics(
        _profitable_records(20),
        window_days=20,
        estimated_cost_pct=0.25,
        market_match_score=100.0,
        walk_forward_verified=True,
        version_bound_evidence=True,
        independent_oos=True,
    )
    _attest_funding_metrics(metrics)
    metrics.update({
        "completed_trades": 20,
        "portfolio_coverage_days": 20,
        "selection_validation_completed_trades": 20,
        "selection_validation_coverage_days": 20,
    })
    return metrics


def test_decay_gate_20_accepts_exact_twenty_session_evidence():
    metrics = _valid_decay_gate_20_metrics()

    gate = governance_module.evaluate_window_gate(metrics)

    assert metrics["session_window_count"] == 20
    assert metrics["completed_trades"] == 20
    assert metrics["portfolio_coverage_days"] == 20
    assert metrics["selection_validation_completed_trades"] == 20
    assert metrics["selection_validation_coverage_days"] == 20
    assert gate["passed"] is True
    assert gate["failed_checks"] == []


@pytest.mark.parametrize(
    ("field", "value", "failed_check"),
    (
        ("session_window_count", 19, "精确交易日窗口"),
        ("completed_trades", 19, "成熟交易"),
        ("portfolio_coverage_days", 19, "组合净值覆盖"),
        ("selection_validation_completed_trades", 19, "Walk-Forward"),
        ("selection_validation_coverage_days", 19, "Walk-Forward"),
        ("net_expectancy_pct", 0.0, "扣费后净期望"),
        ("profit_factor", 1.0, "利润因子"),
        ("review_audit_valid", False, "独立复核"),
    ),
)
def test_decay_gate_20_rejects_each_boundary(
    field, value, failed_check,
):
    metrics = _valid_decay_gate_20_metrics()
    metrics[field] = value

    gate = governance_module.evaluate_window_gate(metrics)

    assert gate["passed"] is False
    assert failed_check in gate["failed_checks"]


def test_decay_gate_20_rejects_zero_and_inexact_cost_stress():
    metrics = _valid_decay_gate_20_metrics()
    metrics.update({
        "gross_expectancy_pct": 0.3,
        "estimated_cost_pct": 0.2,
        "cost_stress_expectancy_pct": 0.0,
    })
    zero_gate = governance_module.evaluate_window_gate(metrics)
    assert zero_gate["passed"] is False
    assert "成本压力" in zero_gate["failed_checks"]

    metrics.update({
        "gross_expectancy_pct": 0.60004,
        "estimated_cost_pct": 0.2,
        "cost_stress_expectancy_pct": 0.3001,
    })
    inexact_gate = governance_module.evaluate_window_gate(metrics)
    assert inexact_gate["passed"] is False
    assert "成本压力" in inexact_gate["failed_checks"]


def test_window_gate_dispatch_keeps_sixty_and_one_twenty_policies_unchanged():
    for window in (60, 120):
        metrics = _valid_decay_gate_20_metrics()
        metrics.update({
            "window_days": window,
            "session_window_count": window,
        })

        gate = governance_module.evaluate_window_gate(metrics)

        assert gate == evaluate_profit_gate(metrics)
        assert gate["passed"] is False
        assert "成熟交易" in gate["failed_checks"]
        assert "组合净值覆盖" in gate["failed_checks"]


def test_strategy_and_combination_generation_use_same_window_gate_dispatcher():
    strategy_source = inspect.getsource(governance_module._metrics_for_registry)
    combination_source = inspect.getsource(
        governance_module._combination_rankings
    )

    canonical_call = 'metrics["profit_gate"] = evaluate_window_gate(metrics)'
    assert canonical_call in strategy_source
    assert canonical_call in combination_source
    assert 'short.get("completed_trades")' not in combination_source


def test_historical_metrics_never_include_trades_closed_after_as_of_date(
    monkeypatch,
):
    records = [
        {
            "evidence_id": "closed-in-time",
            "source_intent_id": "intent-closed-in-time",
            "source_run_uid": "run-closed-in-time",
            "entry_fill_id": "fill-closed-in-time",
            "account_id": "paper-test",
            "stock_code": "000001",
            "strategy_key": "cutoff_strategy",
            "bound_strategy_version": "v1",
            "evidence_status": "MATURED",
            "entry_trade_date": "2026-08-01",
            "entry_at": "2026-08-01T09:30:00",
            "entry_quantity": 100,
            "closed_quantity": 100,
            "entry_gross_cny": 1000,
            "entry_fee_cny": 1,
            "exit_gross_cny": 1012.01,
            "exit_fee_cny": 1,
            "exit_at": "2026-08-09T15:00:00",
            "entry_cash_binding_count": 1,
            "source_intent_buy_fill_count": 1,
            "source_intent_entry_quantity": 100,
            "source_intent_entry_gross_cny": 1000,
            "source_intent_entry_fee_cny": 1,
            "trade_date": "2026-08-09",
            "return_pct": 1.0,
            "is_net_return": True,
            "actual_cost_pct": 0.1,
        },
        {
            "evidence_id": "closed-in-the-future",
            "source_intent_id": "intent-closed-in-future",
            "source_run_uid": "run-closed-in-future",
            "entry_fill_id": "fill-closed-in-future",
            "account_id": "paper-test",
            "stock_code": "000001",
            "strategy_key": "cutoff_strategy",
            "bound_strategy_version": "v1",
            "evidence_status": "MATURED",
            "entry_trade_date": "2026-08-02",
            "entry_at": "2026-08-02T09:30:00",
            "entry_quantity": 100,
            "closed_quantity": 100,
            "entry_gross_cny": 1000,
            "entry_fee_cny": 1,
            "exit_gross_cny": 1992.99,
            "exit_fee_cny": 1,
            "exit_at": "2026-08-12T15:00:00",
            "entry_cash_binding_count": 1,
            "source_intent_buy_fill_count": 1,
            "source_intent_entry_quantity": 100,
            "source_intent_entry_gross_cny": 1000,
            "source_intent_entry_fee_cny": 1,
            "trade_date": "2026-08-12",
            "return_pct": 99.0,
            "is_net_return": True,
            "actual_cost_pct": 0.1,
        },
    ]
    monkeypatch.setattr(
        governance_module,
        "_load_forward_records",
        lambda _as_of_date, _registry: {"cutoff_strategy": records},
    )
    monkeypatch.setattr(
        governance_module,
        "_load_metric_inputs",
        lambda _trade_date, current_versions: {},
    )
    monkeypatch.setattr(
        governance_module,
        "_internal_strategy_portfolio_ledger",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        governance_module,
        "_slice_internal_ledger",
        lambda *_args, **_kwargs: {
            "valid": False,
            "reason": "test ledger intentionally unavailable",
        },
    )
    registry = [{
        "strategy_key": "cutoff_strategy",
        "current_version": "v1",
        "version_hash": "a" * 64,
        "market_route": {"market_match_score": 100.0},
    }]

    metrics = governance_module._metrics_for_registry(
        {}, registry, "2026-08-10",
    )

    for window in governance_module.WINDOWS:
        assert metrics["cutoff_strategy"][window]["completed_trades"] == 1
        assert metrics["cutoff_strategy"][window]["net_expectancy_pct"] == 1.0
        assert (
            metrics["cutoff_strategy"][window]["evidence_as_of_date"]
            == "2026-08-09"
        )


def test_confirmed_external_artifact_is_version_selection_only_not_funding():
    metrics, artifact, artifact_hash, revision_at, _version_created_at = (
        _validation_artifact_fixture()
    )
    metrics.update({
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "artifact_hash": artifact_hash,
        "source_dataset_hash": artifact["source_dataset_hash"],
        "evidence_revision_at": revision_at,
        "verification_status": "CONFIRMED",
        "submitted_by": "evidence_submitter",
        "reviewed_by": "independent_reviewer",
        "funding_provenance": "EXTERNAL_SUBMITTED",
        "evidence_fresh": True,
        "selection_validation_fresh": True,
        "selection_validation_independent_oos": True,
        "selection_validation_scope": "VERSION_SELECTION_ONLY",
        "selection_validation_completed_trades": 80,
        "selection_validation_coverage_days": 80,
    })
    gate = evaluate_profit_gate(metrics)
    assert gate["passed"] is False
    assert "不可伪造来源" in gate["failed_checks"]
    assert "组合权益回撤" in gate["failed_checks"]
    assert "成本口径" in gate["failed_checks"]


def test_negative_recent_evidence_suspends_instead_of_dead_holding():
    records = [
        {
            "trade_date": (date(2026, 6, 1) + timedelta(days=index)).isoformat(),
            "return_pct": -0.8 if index % 3 else 0.2,
        }
        for index in range(30)
    ]
    metrics = calculate_return_metrics(
        records,
        window_days=60,
        estimated_cost_pct=0.25,
        market_match_score=80.0,
        walk_forward_verified=True,
        version_bound_evidence=True,
        independent_oos=True,
    )
    _attest_funding_metrics(metrics)
    status, reason = recommend_lifecycle_status("ACTIVE", metrics)
    assert status == "SUSPENDED"
    assert "失效" in reason
    assert evaluate_profit_gate(metrics)["passed"] is False


def test_sample_shortage_stays_shadow_and_displays_reason():
    metrics = calculate_return_metrics(
        _profitable_records(12),
        window_days=20,
        market_match_score=100.0,
        walk_forward_verified=True,
        version_bound_evidence=True,
        independent_oos=True,
    )
    _attest_funding_metrics(metrics)
    status, reason = recommend_lifecycle_status("SHADOW", metrics)
    assert status == "SHADOW"
    assert "成熟交易" in reason


def test_executed_trade_sequence_is_diagnostic_not_verified_walk_forward():
    metrics = calculate_return_metrics(
        _profitable_records(),
        window_days=120,
        estimated_cost_pct=0.25,
        market_match_score=100.0,
        walk_forward_verified=False,
        version_bound_evidence=True,
        independent_oos=True,
    )
    metrics["evidence_fresh"] = True
    gate = evaluate_profit_gate(metrics)
    assert gate["passed"] is False
    assert "Walk-Forward" in gate["failed_checks"]
    assert "验证协议" in gate["failed_checks"]
    assert "组合权益回撤" in gate["failed_checks"]
    assert metrics["drawdown_basis"] == "sequential_trade_diagnostic"


def test_legacy_self_declared_metric_evidence_fails_closed():
    metrics = _attest_funding_metrics(calculate_return_metrics(
        _profitable_records(),
        window_days=120,
        walk_forward_verified=True,
        version_bound_evidence=True,
        independent_oos=True,
    ))
    metrics.update({
        "evidence_protocol": "LEGACY_UNVERIFIED",
        "artifact_hash": "",
        "evidence_revision_at": None,
    })
    gate = evaluate_profit_gate(metrics)
    assert gate["passed"] is False
    assert "验证协议" in gate["failed_checks"]
    assert "验证产物" in gate["failed_checks"]


def test_suspended_strategy_can_only_recover_to_shadow():
    metrics = _attest_funding_metrics(calculate_return_metrics(
        _profitable_records(),
        window_days=120,
        walk_forward_verified=True,
        version_bound_evidence=True,
        independent_oos=True,
    ))
    assert evaluate_profit_gate(metrics)["passed"] is True
    status, reason = recommend_lifecycle_status("SUSPENDED", metrics)
    assert status == "SHADOW"
    assert "影子观察" in reason


def test_strategy_funding_requires_the_same_confirmed_gate_in_all_three_windows():
    registry = [{
        "strategy_key": "three_window_gate",
        "strategy_name": "三窗口准入",
        "current_version": "v1",
        "current_status": "ACTIVE",
        "enabled": True,
        "market_route": {
            "eligible": True,
            "reason": "适配",
            "router_decision_hash": "a" * 64,
        },
    }]
    windows = {
        window: {
            "health_score": 90.0,
            "profit_gate": {
                "passed": window != 20,
                "reason": "通过" if window != 20 else "缺少20日确认产物",
                "failed_checks": [] if window != 20 else ["独立样本外验证"],
            },
            # These values satisfy the former hand-written short-window
            # shortcut, proving it can no longer bypass the canonical gate.
            "version_bound_evidence": True,
            "independent_oos": True,
            "session_window_valid": True,
            "funding_provenance": "INTERNAL_PORTFOLIO_LEDGER_V1",
            "evidence_fresh": True,
            "completed_trades": 100,
            "net_expectancy_pct": 1.0,
            "profit_factor": 1.5,
            "cost_stress_expectancy_pct": 0.5,
            "evidence_hash": str(window) * 32,
            "evidence_revision_at": "2026-08-21T15:00:00",
        }
        for window in (20, 60, 120)
    }
    ranked = governance_module._strategy_rankings(
        registry, {"three_window_gate": windows},
    )[0]
    assert ranked["profit_gate_passed"] is False
    assert ranked["paper_allocation_eligible"] is False
    assert ranked["multi_window_gate"]["20"]["passed"] is False

    windows[20]["profit_gate"] = {
        "passed": True,
        "reason": "通过",
        "failed_checks": [],
    }
    ranked = governance_module._strategy_rankings(
        registry, {"three_window_gate": windows},
    )[0]
    assert ranked["profit_gate_passed"] is True
    assert ranked["paper_allocation_eligible"] is True


def test_runtime_recovery_rejects_same_evidence_and_accepts_new_evidence_only_to_shadow():
    metrics = _attest_funding_metrics(calculate_return_metrics(
        _profitable_records(),
        window_days=120,
        walk_forward_verified=True,
        version_bound_evidence=True,
        independent_oos=True,
    ))
    boundary = {
        "evidence_revision_at": "2026-08-21T15:00:00",
        "trade_date": "2026-08-21",
    }
    status, reason = governance_module._apply_suspended_recovery_rule(
        current_status="SUSPENDED",
        recommended_status="ACTIVE",
        reason="gate passed",
        primary_metrics=metrics,
        funding_evidence_revision_at="2026-08-21T15:00:00",
        suspension_boundary=boundary,
    )
    assert status == "SUSPENDED"
    assert "旧证据" in reason

    metrics["evidence_revision_at"] = "2026-08-22T15:00:00"
    status, reason = governance_module._apply_suspended_recovery_rule(
        current_status="SUSPENDED",
        recommended_status="ACTIVE",
        reason="gate passed",
        primary_metrics=metrics,
        funding_evidence_revision_at="2026-08-22T15:00:00",
        suspension_boundary=boundary,
    )
    assert status == "SHADOW"
    assert "不能直接取得资金资格" in reason


def test_post_suspension_confirmation_does_not_count_pre_suspension_passes(monkeypatch):
    monkeypatch.setattr(governance_module, "_table_exists", lambda _name: True)
    rows = [
        {
            "trade_date": "2026-08-21",
            "profit_gate_passed": 1,
            "evidence_json": {
                "overall_profit_gate_passed": True,
                "funding_gate_hash": "post-suspension",
                "funding_evidence_revision_at": "2026-08-21T15:00:00",
            },
        },
        {
            "trade_date": "2026-08-19",
            "profit_gate_passed": 1,
            "evidence_json": {
                "overall_profit_gate_passed": True,
                "funding_gate_hash": "pre-suspension",
                "funding_evidence_revision_at": "2026-08-19T15:00:00",
            },
        },
    ]
    monkeypatch.setattr(governance_module, "_db_read", lambda *_a, **_k: rows)
    count = governance_module._prior_consecutive_gate_passes(
        "short_term",
        "v1",
        "2026-08-23",
        "current",
        "2026-08-22T15:00:00",
        limit=2,
        minimum_revision_exclusive="2026-08-20T15:00:00",
        minimum_trade_date_exclusive="2026-08-20",
    )
    assert count == 1


def test_confirmation_chain_stops_when_a_session_has_no_new_evidence(monkeypatch):
    monkeypatch.setattr(governance_module, "_table_exists", lambda _name: True)
    rows = [
        {
            "trade_date": "2026-08-20",
            "profit_gate_passed": 1,
            "evidence_json": {
                "overall_profit_gate_passed": True,
                "funding_gate_hash": "hash-b",
                "funding_evidence_revision_at": "2026-08-20T15:00:00",
            },
        },
        {
            "trade_date": "2026-08-19",
            "profit_gate_passed": 1,
            "evidence_json": {
                "overall_profit_gate_passed": True,
                "funding_gate_hash": "hash-a-window-shifted",
                # A changed window hash with no new exit must not count.
                "funding_evidence_revision_at": "2026-08-20T15:00:00",
            },
        },
        {
            "trade_date": "2026-08-18",
            "profit_gate_passed": 1,
            "evidence_json": {
                "overall_profit_gate_passed": True,
                "funding_gate_hash": "hash-a",
                "funding_evidence_revision_at": "2026-08-18T15:00:00",
            },
        },
    ]
    monkeypatch.setattr(governance_module, "_db_read", lambda *_a, **_k: rows)
    count = governance_module._prior_consecutive_gate_passes(
        "short_term", "v1", "2026-08-22", "hash-current",
        "2026-08-21T15:00:00", limit=2,
    )
    assert count == 1


def test_confirmation_chain_stops_at_a_missing_authoritative_session(monkeypatch):
    health_rows = [
        {
            "trade_date": "2026-08-20",
            "profit_gate_passed": 1,
            "evidence_json": {
                "overall_profit_gate_passed": True,
                "funding_gate_hash": "hash-20",
                "funding_evidence_revision_at": "2026-08-20T15:00:00",
            },
        },
        {
            "trade_date": "2026-08-18",
            "profit_gate_passed": 1,
            "evidence_json": {
                "overall_profit_gate_passed": True,
                "funding_gate_hash": "hash-18",
                "funding_evidence_revision_at": "2026-08-18T15:00:00",
            },
        },
    ]

    def fake_read(sql, _params=None):
        if "FROM si_trade_calendar" in sql:
            return [
                {"trade_date": "2026-08-20"},
                {"trade_date": "2026-08-19"},
            ]
        return health_rows

    monkeypatch.setattr(governance_module, "_table_exists", lambda _name: True)
    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    assert governance_module._prior_consecutive_gate_passes(
        "short_term", "v1", "2026-08-21", "hash-current",
        "2026-08-21T15:00:00", limit=2,
    ) == 1
    assert governance_module._prior_consecutive_combination_gate_passes(
        "balanced", "v1", "2026-08-21", "hash-current",
        "2026-08-21T15:00:00", limit=2,
    ) == 1


def test_governance_input_rejects_invalid_or_missing_dates():
    ready, reason = governance_module.governance_input_ready({
        "source_status": "fresh",
        "is_stale": False,
        "trade_date": "bad-date",
        "data_date": "bad-date",
    })
    assert ready is False
    assert "格式无效" in reason
    ready, reason = governance_module.governance_input_ready({
        "source_status": "fresh",
        "is_stale": False,
        "trade_date": "2026-08-21",
    })
    assert ready is False
    assert "缺少" in reason


def test_governance_get_paths_do_not_create_or_seed_tables(monkeypatch):
    calls = []
    monkeypatch.setattr(
        governance_module,
        "ensure_and_seed_governance",
        lambda: calls.append("mutated"),
    )
    monkeypatch.setattr(governance_module, "_table_exists", lambda _name: False)
    with pytest.raises(RuntimeError, match="读取接口不会现场建表"):
        governance_module.governance_snapshot(persist=False)
    with pytest.raises(RuntimeError, match="读取接口不会现场建表"):
        governance_module.governance_history()
    with pytest.raises(RuntimeError, match="尚未由部署流程创建"):
        governance_module.metric_evidence_detail("a" * 32)
    assert calls == []


def test_bound_sql_snapshot_routes_cross_module_reads_and_writes_into_one_transaction():
    engine = create_engine("sqlite:///:memory:")
    other_engine = create_engine("sqlite:///:memory:")
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE atomic_probe (value INTEGER)"))
        with other_engine.begin() as connection:
            connection.execute(text("CREATE TABLE atomic_probe (value INTEGER)"))
            connection.execute(text("INSERT INTO atomic_probe VALUES (99)"))
        with engine.connect() as connection:
            transaction = connection.begin()
            with bind_sql_connection(connection):
                strategy_center_engine._db_write(
                    "INSERT INTO atomic_probe (value) VALUES (:value)",
                    {"value": 7},
                )
                rows = read_sql_rows(
                    engine,
                    "SELECT value FROM atomic_probe",
                    context="atomic-governance-test",
                )
                assert rows == [{"value": 7}]
                assert read_sql_rows(
                    other_engine,
                    "SELECT value FROM atomic_probe",
                    context="separate-database-test",
                ) == [{"value": 99}]
            transaction.rollback()
        with engine.connect() as connection:
            assert connection.execute(
                text("SELECT COUNT(*) FROM atomic_probe")
            ).scalar() == 0
    finally:
        engine.dispose()
        other_engine.dispose()


def test_idempotent_decision_is_checked_before_strategy_center_facts_are_written():
    source = inspect.getsource(governance_module.governance_snapshot)
    assert source.index("if existing is not None:") < source.index(
        "persist_strategy_center_snapshot("
    )


def test_transition_plan_is_deterministic_and_not_part_of_retry_identity():
    base = {
        "entity_type": "STRATEGY",
        "entity_key": "strategy_a",
        "entity_version": "v1",
        "previous_status": "SHADOW",
        "next_status": "ACTIVE",
        "reason": "confirmed",
    }
    first_plan, first_hash = (
        governance_module._automatic_transition_plan_contract(
            "2026-08-21",
            "a" * 32,
            [{
                **base,
                "evidence": {
                    "run_uid": "a" * 32,
                    "trade_date": "2026-08-21",
                    "funding_gate_hash": "1" * 64,
                },
            }],
        )
    )
    retry_plan, retry_hash = (
        governance_module._automatic_transition_plan_contract(
            "2026-08-21",
            "b" * 32,
            [{
                **base,
                "evidence": {
                    "run_uid": "b" * 32,
                    "trade_date": "2026-08-21",
                    "funding_gate_hash": "1" * 64,
                },
            }],
        )
    )
    assert first_plan == retry_plan
    assert first_hash == retry_hash
    assert "run_uid" not in first_plan
    assert "run_uid" not in first_plan["transitions"][0]["evidence"]

    source = inspect.getsource(governance_module.governance_snapshot)
    decision_start = source.index("decision_hash = _digest({")
    decision_end = source.index("\n    payload = {", decision_start)
    assert "automatic_transition_plan_hash" not in source[
        decision_start:decision_end
    ]


def test_completed_governance_run_writes_hash_valid_audit_in_same_connection():
    class CaptureConnection:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((str(statement), dict(params or {})))
            return object()

    connection = CaptureConnection()
    empty_pools = {
        "observation": [],
        "confirmation": [],
        "tradable": [],
    }
    _pool_snapshot, pool_snapshot_hash, _row_hashes = (
        governance_module._pool_snapshot_contract(
            "2026-08-21", empty_pools
        )
    )
    summary = {
        "strategy_count": 1,
        "formal_count": 0,
        "shadow_count": 1,
        "combination_count": 0,
        "observation_count": 0,
        "confirmation_count": 0,
        "tradable_count": 0,
        "pool_row_count": 0,
        "pool_snapshot_hash": pool_snapshot_hash,
        "allocation_count": 1,
    }
    transition_plan = {
        "schema": governance_module.AUTOMATIC_TRANSITION_PLAN_SCHEMA,
        "trade_date": "2026-08-21",
        "transition_count": 0,
        "transitions": [],
    }
    transition_plan_hash = governance_module._digest(transition_plan)
    summary["automatic_transition_count"] = 0
    summary["automatic_transition_plan_hash"] = transition_plan_hash
    governance_module._persist_run(connection, {
        "run_uid": "a" * 32,
        "trade_date": "2026-08-21",
        "run_revision": 1,
        "supersedes_run_uid": "",
        "market_state": {"key": "range"},
        "source_status": "fresh",
        "input_hash": "b" * 64,
        "build_commit_sha": "c" * 40,
        "router_policy_version": "strategy_market_router.v1",
        "router_snapshot_hash": "e" * 64,
        "decision_hash": "d" * 64,
        "pool_snapshot_hash": pool_snapshot_hash,
        "automatic_transition_plan": transition_plan,
        "automatic_transition_plan_hash": transition_plan_hash,
        "summary": summary,
        "pools": empty_pools,
        "allocations": [{
            "target_type": "CASH",
            "target_key": "cash",
            "target_version": "",
            "funding_gate_hash": "",
            "market_state": "range",
            "market_match_score": 100.0,
            "router_decision_hash": "",
            "simulated_weight_pct": 100.0,
            "reason": "没有合格策略，保持现金",
        }],
        "operator": "scheduled_daily_governance",
    })
    audit_calls = [
        params for sql, params in connection.calls
        if "st_strategy_governance_audit" in sql
    ]
    assert len(audit_calls) == 1
    audit = audit_calls[0]
    payload = governance_module._json(audit["payload_json"], {})
    assert audit["action"] == "RUN_GOVERNANCE"
    assert audit["operator"] == "scheduled_daily_governance"
    assert governance_module._digest(payload) == audit["audit_hash"]
    evidence = governance_module._json(audit["evidence_json"], {})
    assert evidence["run_uid"] == "a" * 32
    assert evidence["automatic_real_order_submission"] is False


def test_governance_api_keeps_real_order_authority_closed(monkeypatch):
    expected = {
        "status": "ok",
        "summary": {"strategy_count": 9, "tradable_count": 0},
        "strategies": [],
        "combinations": [],
        "pools": {"observation": [], "confirmation": [], "tradable": []},
        "allocations": [
            {
                "target_type": "CASH",
                "target_key": "cash",
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }
        ],
        "automatic_real_order_submission": False,
    }
    monkeypatch.setattr(
        strategy_center_router,
        "governance_snapshot",
        lambda **_kwargs: expected,
    )
    app = FastAPI()
    app.include_router(strategy_center_router.router, prefix="/api")
    response = TestClient(app).get("/api/strategy-center/governance?trade_date=2026-08-21")
    assert response.status_code == 200
    payload = response.json()
    assert payload == expected
    assert payload["allocations"][0]["simulated_weight_pct"] == 100.0
    assert payload["automatic_real_order_submission"] is False


def test_governance_run_blocks_lifecycle_mutation_when_pool_data_missing(monkeypatch):
    monkeypatch.setattr(
        strategy_center_router,
        "governance_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(
            governance_module.GovernanceEvidenceNotReady(
                "治理输入未通过新鲜度校验：底层票池数据缺失"
            )
        ),
    )
    result = strategy_center_router.strategy_center_run_governance(
        strategy_center_router.StrategyRunRequest(trade_date="2026-08-21"),
        _admin_request(),
    )
    assert result["status"] == "blocked"
    assert "底层票池数据缺失" in result["reason"]
    assert result["allocations"][0]["simulated_weight_pct"] == 100.0


def test_governance_run_exposes_authoritative_evidence_block_and_keeps_cash(
    monkeypatch,
):
    snapshot = {
        "source_status": "fresh",
        "trade_date": "2026-08-21",
        "data_date": "2026-08-21",
        "candidates": [],
    }
    monkeypatch.setattr(
        strategy_center_router,
        "build_strategy_center_snapshot",
        lambda *_args, **_kwargs: snapshot,
    )
    monkeypatch.setattr(
        strategy_center_router,
        "persist_strategy_center_snapshot",
        lambda value: value,
    )
    monkeypatch.setattr(
        strategy_center_router,
        "governance_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(
            governance_module.GovernanceEvidenceNotReady(
                "公司行动权威账本未建立，相关持仓资金证据暂停"
            )
        ),
    )

    result = strategy_center_router.strategy_center_run_governance(
        strategy_center_router.StrategyRunRequest(trade_date="2026-08-21"),
        _admin_request(),
    )

    assert result["status"] == "blocked"
    assert "公司行动权威账本未建立" in result["reason"]
    assert result["allocations"][0]["simulated_weight_pct"] == 100.0
    assert result["automatic_real_order_submission"] is False


def test_manual_transition_cannot_grant_funded_status(monkeypatch):
    monkeypatch.setattr(
        "server.engine.strategy_governance.ensure_and_seed_governance",
        lambda: None,
    )
    monkeypatch.setattr(
        "server.engine.strategy_governance._db_read",
        lambda *_args, **_kwargs: [
            {
                "current_version": "v1",
                "current_status": "SHADOW",
                "status_reason": "等待验证",
            }
        ],
    )
    try:
        transition_lifecycle(
            "earnings_surprise",
            "ACTIVE",
            reason="人工直接开资金",
            operator="tester",
        )
    except ValueError as exc:
        assert "只能由盈利硬门槛自动授予" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("manual ACTIVE transition must fail closed")


def test_manual_transition_cannot_bypass_suspension_recovery_gate(monkeypatch):
    monkeypatch.setattr(
        "server.engine.strategy_governance.ensure_and_seed_governance",
        lambda: None,
    )
    monkeypatch.setattr(
        "server.engine.strategy_governance._db_read",
        lambda *_args, **_kwargs: [{
            "current_version": "v1",
            "current_status": "SUSPENDED",
            "status_reason": "盈利证据失效",
        }],
    )
    with pytest.raises(ValueError, match="人工不能绕过恢复门槛"):
        transition_lifecycle(
            "earnings_surprise",
            "SHADOW",
            reason="人工恢复",
            operator="tester",
        )


def test_strategy_toggle_evidence_alone_cannot_bypass_suspension_recovery(
    monkeypatch,
):
    class Result:
        rowcount = 1

        def __init__(self, row=None):
            self._row = row

        def mappings(self):
            return self

        def first(self):
            return self._row

        def scalar(self):
            return 0

    class Connection:
        def __init__(self):
            self.statements = []

        def execute(self, statement, _params=None):
            sql = str(statement)
            self.statements.append(sql)
            if "SELECT current_version" in sql:
                return Result({
                    "current_version": "v1",
                    "current_status": "SUSPENDED",
                    "status_reason": "人工关闭策略信号",
                    "enabled": 0,
                })
            return Result()

    connection = Connection()
    monkeypatch.setattr(
        governance_module, "ensure_and_seed_governance", lambda: None,
    )
    with pytest.raises(ValueError, match="人工不能绕过恢复门槛"):
        transition_lifecycle(
            "earnings_surprise",
            "SHADOW",
            reason="重新启用后先进入影子观察",
            operator="tester",
            evidence={"source": "strategy_toggle", "enabled": True},
            _connection=connection,
        )


def test_only_hash_valid_manual_disable_event_can_authorize_shadow_restart():
    evidence = {"source": "strategy_toggle", "enabled": False}
    payload = {
        "entity_type": "STRATEGY",
        "entity_key": "earnings_surprise",
        "entity_version": "v1",
        "previous_status": "ACTIVE",
        "next_status": "SUSPENDED",
        "reason": "人工关闭策略信号",
        "evidence": evidence,
        "nonce": "n",
    }
    row = {
        "previous_status": "ACTIVE",
        "next_status": "SUSPENDED",
        "trigger_type": "MANUAL_GOVERNANCE",
        "evidence_json": governance_module._json_text(evidence),
        "payload_json": governance_module._json_text(payload),
        "event_hash": governance_module._digest(payload),
    }
    assert governance_module._is_verified_manual_disable_suspension(row) is True

    automatic = dict(row, trigger_type="AUTOMATIC_GATE")
    assert governance_module._is_verified_manual_disable_suspension(automatic) is False
    tampered = dict(row, payload_json=governance_module._json_text({**payload, "reason": "changed"}))
    assert governance_module._is_verified_manual_disable_suspension(tampered) is False


def test_retired_strategy_version_cannot_be_reenabled(monkeypatch):
    monkeypatch.setattr(
        governance_module, "ensure_and_seed_governance", lambda: None,
    )
    monkeypatch.setattr(
        governance_module,
        "_db_read",
        lambda *_args, **_kwargs: [{
            "current_version": "v1",
            "current_status": "RETIRED",
            "enabled": 0,
        }],
    )
    with pytest.raises(ValueError, match="请注册新版本"):
        governance_module.toggle_strategy_enabled(
            "earnings_surprise",
            True,
            reason="尝试重新打开旧版本",
            operator="tester",
        )


def test_daily_governance_task_is_scheduler_safe_and_validated():
    assert GOVERNANCE_TASK["task_type"] == "strategy_governance_daily"
    assert GOVERNANCE_TASK["script_path"] == "tools/run_strategy_governance_daily.py"
    assert GOVERNANCE_TASK["enabled"] == 1
    assert "strategy_governance_daily" in NO_DEFAULT_DATE_TASK_TYPES
    requirement = TASK_OUTPUT_REQUIREMENTS["strategy_governance_daily"][0]
    assert requirement.table == "st_strategy_governance_run"
    assert requirement.where_sql == "status = 'COMPLETED'"
    installer_source = inspect.getsource(governance_task_installer.main)
    assert installer_source.index("run_v3_migrations(engine)") < (
        installer_source.index("ensure_attestation_tables(engine)")
    )
    assert installer_source.index("ensure_attestation_tables(engine)") < (
        installer_source.index("validate_attestation_schema(engine)")
    )
    assert installer_source.index("validate_attestation_schema(engine)") < (
        installer_source.index("ensure_and_seed_governance()")
    )
    assert installer_source.index("if args.restore_snapshot:") < (
        installer_source.index("run_v3_migrations(engine)")
    )
    assert installer_source.index("_write_snapshot(") < (
        installer_source.index("run_v3_migrations(engine)")
    )


class _MetricTriggerResult:
    def __init__(self, rows=()):
        self._rows = [dict(row) for row in rows]

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _MetricTriggerConnection:
    def __init__(self, rows=()):
        self.rows = {
            str(row["trigger_name"]): dict(row) for row in rows
        }
        self.created: list[str] = []

    def execute(self, statement, _params=None):
        sql = str(statement)
        if "FROM information_schema.TRIGGERS" in sql:
            return _MetricTriggerResult(
                self.rows[name] for name in sorted(self.rows)
            )
        if sql.startswith("CREATE TRIGGER "):
            trigger_name = sql.split()[2]
            contract = (
                governance_module.METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS[
                    trigger_name
                ]
            )
            self.created.append(trigger_name)
            self.rows[trigger_name] = {
                "trigger_name": trigger_name,
                "action_timing": contract["timing"],
                "event_manipulation": contract["event"],
                "event_object_table": contract["table"],
                "action_orientation": "ROW",
                "action_statement": contract["body"],
            }
            return _MetricTriggerResult()
        raise AssertionError(f"unexpected SQL: {sql}")


def _metric_trigger_rows():
    return [
        {
            "trigger_name": trigger_name,
            "action_timing": contract["timing"],
            "event_manipulation": contract["event"],
            "event_object_table": contract["table"],
            "action_orientation": "ROW",
            "action_statement": contract["body"],
        }
        for trigger_name, contract in (
            governance_module.METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS.items()
        )
    ]


def test_metric_input_review_trigger_contract_covers_every_core_column():
    update_body = (
        governance_module.METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS[
            "trg_strategy_metric_input_review_bu"
        ]["body"]
    )
    for column_name in (
        "evidence_id",
        "entity_type",
        "strategy_key",
        "strategy_version",
        "as_of_date",
        "window_days",
        "metrics_json",
        "source",
        "evidence_protocol",
        "artifact_hash",
        "artifact_json",
        "source_dataset_hash",
        "evidence_revision_at",
        "funding_provenance",
        "submitted_by",
        "evidence_hash",
        "created_at",
    ):
        assert f"OLD.{column_name}" in update_body
        assert f"NEW.{column_name}" in update_body
    assert "OLD.verification_status = BINARY 'PENDING'" in update_body
    assert "NEW.verification_status = BINARY 'CONFIRMED'" in update_body
    assert "NEW.verification_status = BINARY 'REJECTED'" in update_body
    assert "NEW.reviewed_by <> BINARY NEW.submitted_by" in update_body


def test_metric_input_review_trigger_ensure_creates_only_missing_contracts():
    connection = _MetricTriggerConnection()

    governance_module._ensure_metric_input_review_triggers(connection)

    assert set(connection.created) == set(
        governance_module.METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS
    )
    detail = governance_module.validate_metric_input_review_triggers(
        connection
    )
    assert detail["trigger_count"] == 2
    assert detail["errors"] == []


def test_metric_input_review_trigger_ensure_rejects_drift_without_replacing():
    rows = _metric_trigger_rows()
    rows[0]["action_statement"] += " SET @drift=1;"
    connection = _MetricTriggerConnection(rows)

    with pytest.raises(RuntimeError, match="触发器正文漂移"):
        governance_module._ensure_metric_input_review_triggers(connection)

    assert connection.created == []


class _GovernanceTriggerConnection:
    def __init__(self, rows=()):
        self.rows = {
            str(row["trigger_name"]): dict(row) for row in rows
        }
        self.created: list[str] = []

    def execute(self, statement, _params=None):
        sql = str(statement).strip()
        if "FROM information_schema.TRIGGERS" in sql:
            return _MetricTriggerResult(
                self.rows[name] for name in sorted(self.rows)
            )
        if sql.startswith("CREATE TRIGGER "):
            trigger_name = sql.split()[2]
            timing, event, table_name, body = (
                governance_module
                .GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS[trigger_name]
            )
            self.created.append(trigger_name)
            self.rows[trigger_name] = {
                "trigger_name": trigger_name,
                "action_timing": timing,
                "event_manipulation": event,
                "event_object_table": table_name,
                "action_orientation": "ROW",
                "action_statement": body,
            }
            return _MetricTriggerResult()
        raise AssertionError(f"unexpected SQL: {sql}")


def _governance_trigger_rows():
    return [
        {
            "trigger_name": trigger_name,
            "action_timing": timing,
            "event_manipulation": event,
            "event_object_table": table_name,
            "action_orientation": "ROW",
            "action_statement": body,
        }
        for trigger_name, (timing, event, table_name, body) in (
            governance_module
            .GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS.items()
        )
    ]


def test_governance_ledger_trigger_ensure_creates_only_missing_contracts():
    connection = _GovernanceTriggerConnection()

    governance_module._ensure_governance_append_only_triggers(connection)

    assert set(connection.created) == set(
        governance_module.GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS
    )
    detail = governance_module.validate_governance_append_only_triggers(
        connection
    )
    assert detail["trigger_count"] == 18
    assert detail["errors"] == []
    assert "DROP TRIGGER" not in inspect.getsource(
        governance_module._ensure_governance_append_only_triggers
    ).upper()


@pytest.mark.parametrize("drift_kind", ["missing", "body", "extra"])
def test_governance_ledger_trigger_validator_rejects_every_drift(
    drift_kind,
):
    rows = _governance_trigger_rows()
    if drift_kind == "missing":
        rows.pop()
    elif drift_kind == "body":
        rows[0]["action_statement"] = "BEGIN SET @unsafe=1; END"
    else:
        rows.append({
            "trigger_name": "trg_strategy_unreviewed_extra",
            "action_timing": "BEFORE",
            "event_manipulation": "DELETE",
            "event_object_table": "st_strategy_version",
            "action_orientation": "ROW",
            "action_statement": "BEGIN SIGNAL SQLSTATE '45000'; END",
        })

    with pytest.raises(governance_module.GovernanceAppendOnlySchemaError):
        governance_module.validate_governance_append_only_triggers(
            _GovernanceTriggerConnection(rows)
        )


def test_governance_run_trigger_allows_only_null_safe_canonical_demotion():
    body = governance_module.GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS[
        "trg_strategy_governance_run_frozen_bu"
    ][3]
    assert "OLD.is_canonical <=> 1" in body
    assert "NEW.is_canonical <=> 0" in body
    for column_name in (
        "run_uid",
        "trade_date",
        "run_revision",
        "supersedes_run_uid",
        "input_hash",
        "decision_hash",
        "summary_json",
        "created_at",
        "finished_at",
    ):
        assert f"OLD.{column_name}" in body
        assert f"NEW.{column_name}" in body
    assert "SIGNAL SQLSTATE '45000'" in body


def _stored_metric_audit(params, created_at):
    return {
        "audit_id": params["audit_id"],
        "entity_type": params["entity_type"],
        "entity_key": params["entity_key"],
        "action": params["action"],
        "reason": params["reason"],
        "operator_name": params["operator"],
        "before_json": params["before_json"],
        "after_json": params["after_json"],
        "evidence_json": params["evidence_json"],
        "payload_json": params["payload_json"],
        "audit_hash": params["audit_hash"],
        "created_at": created_at,
    }


class _ConcurrentMetricResult:
    def __init__(self, rows=()):
        self._rows = [dict(row) for row in rows]

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None


class _ConcurrentMetricConnection:
    def execute(self, statement, _params=None):
        sql = str(statement)
        if "SELECT current_version FROM st_strategy_registry" in sql:
            assert "FOR UPDATE" in sql
            return _ConcurrentMetricResult([{"current_version": "v1"}])
        if "SELECT evidence_revision_at, artifact_hash" in sql:
            return _ConcurrentMetricResult()
        if "SELECT evidence_id, entity_type, strategy_key" in sql:
            return _ConcurrentMetricResult()
        if "INSERT INTO st_strategy_metric_input" in sql:
            raise governance_module.IntegrityError(
                "INSERT INTO st_strategy_metric_input",
                _params or {},
                RuntimeError("Duplicate entry for uq_metric_entity_window"),
            )
        raise AssertionError(f"unexpected concurrent metric SQL: {sql}")


class _ConcurrentMetricBegin:
    def __init__(self):
        self.connection = _ConcurrentMetricConnection()

    def __enter__(self):
        return self.connection

    def __exit__(self, *_args):
        return False


class _ConcurrentMetricEngine:
    def begin(self):
        return _ConcurrentMetricBegin()


def _install_concurrent_metric_submission(
    monkeypatch,
    *,
    conflict_mode: str = "identical",
):
    submitted_by = "user-id:metric-submitter"
    evidence_id = "c" * 32
    duplicate_reads = 0
    stored_row = None
    stored_audit = None

    monkeypatch.setattr(
        governance_module, "ensure_and_seed_governance", lambda: None,
    )
    monkeypatch.setattr(
        governance_module,
        "load_registry",
        lambda: [{
            "strategy_key": "artifact_test_strategy",
            "current_version": "v1",
            "version_created_at": "2025-12-31T00:00:00",
        }],
    )
    monkeypatch.setattr(
        governance_module,
        "_validated_metric_evidence",
        lambda metrics: dict(metrics),
    )
    monkeypatch.setattr(
        governance_module,
        "_validate_metric_artifact",
        lambda *_args, **_kwargs: {"source_dataset_hash": "b" * 64},
    )
    monkeypatch.setattr(
        governance_module,
        "_version_max_holding_days",
        lambda *_args, **_kwargs: 5,
    )
    monkeypatch.setattr(
        governance_module,
        "_version_label_horizon_days",
        lambda *_args, **_kwargs: 2,
    )
    monkeypatch.setattr(
        governance_module, "get_engine", lambda: _ConcurrentMetricEngine(),
    )

    def fake_read(sql, params=None):
        nonlocal duplicate_reads, stored_row, stored_audit
        if "FROM st_strategy_metric_input i" in sql:
            duplicate_reads += 1
            if duplicate_reads == 1:
                return []
            submission = dict(params or {})
            evidence_hash = governance_module._digest(submission)
            stored_row = {
                "evidence_id": evidence_id,
                "entity_type": submission["entity_type"],
                "strategy_key": submission["strategy_key"],
                "strategy_version": submission["strategy_version"],
                "registry_current_version": (
                    "v2" if conflict_mode == "version_changed" else "v1"
                ),
                "as_of_date": submission["as_of_date"],
                "window_days": submission["window_days"],
                "metrics_json": governance_module._json_text(
                    submission["metrics"]
                ),
                "source": submission["source"],
                "evidence_protocol": submission["evidence_protocol"],
                "artifact_hash": submission["artifact_hash"],
                "source_dataset_hash": submission["source_dataset_hash"],
                "evidence_revision_at": submission["evidence_revision_at"],
                "verification_status": "PENDING",
                "funding_provenance": "EXTERNAL_SUBMITTED",
                "submitted_by": submitted_by,
                "reviewed_by": None,
                "reviewed_at": None,
                "evidence_hash": (
                    "f" * 64
                    if conflict_mode == "different_content"
                    else evidence_hash
                ),
                "created_at": "2026-08-21T15:01:00",
            }
            _sql, audit_params = governance_module._audit_record(
                entity_type="STRATEGY",
                entity_key="artifact_test_strategy",
                action="ADD_METRIC_EVIDENCE",
                reason="并发提交获胜者审计",
                operator=submitted_by,
                before={},
                after=submission,
                evidence={
                    "evidence_id": evidence_id,
                    "evidence_hash": evidence_hash,
                    "artifact_hash": submission["artifact_hash"],
                    "source_dataset_hash": submission[
                        "source_dataset_hash"
                    ],
                    "verification_status": "PENDING",
                },
            )
            stored_audit = _stored_metric_audit(
                audit_params, "2026-08-21T15:01:00",
            )
            return [stored_row]
        if "FROM st_strategy_governance_audit" in sql:
            return [] if conflict_mode == "missing_audit" else [stored_audit]
        raise AssertionError(f"unexpected concurrent read SQL: {sql}")

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    payload = {
        "strategy_key": "artifact_test_strategy",
        "entity_type": "STRATEGY",
        "bound_strategy_version": "v1",
        "as_of_date": "2026-08-21",
        "window_days": 60,
        "metrics": {
            "walk_forward_verified": True,
            "independent_oos": True,
        },
        "source": "concurrent_test",
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "artifact_hash": "a" * 64,
        "artifact_manifest": {},
        "evidence_revision_at": "2026-08-21T15:00:00",
        "reason": "并发提交",
    }
    return payload, submitted_by


def test_concurrent_identical_metric_first_submission_is_audited_idempotent(
    monkeypatch,
):
    payload, submitted_by = _install_concurrent_metric_submission(monkeypatch)

    result = governance_module.record_metric_input(
        payload, operator=submitted_by,
    )

    assert result["evidence_id"] == "c" * 32
    assert result["verification_status"] == "PENDING"
    assert result["idempotent_replay"] is True


@pytest.mark.parametrize(
    ("conflict_mode", "error_type", "message"),
    (
        ("different_content", ValueError, "不可覆盖"),
        ("version_changed", RuntimeError, "已非当前版本"),
        ("missing_audit", RuntimeError, "缺少完整不可变"),
    ),
)
def test_concurrent_metric_conflict_never_bypasses_content_version_or_audit(
    monkeypatch,
    conflict_mode,
    error_type,
    message,
):
    payload, submitted_by = _install_concurrent_metric_submission(
        monkeypatch, conflict_mode=conflict_mode,
    )

    with pytest.raises(error_type, match=message):
        governance_module.record_metric_input(
            payload, operator=submitted_by,
        )


def test_confirmed_metric_requires_exact_add_and_review_audit_binding():
    evidence_id = "e" * 32
    reviewed_at = "2026-08-21T16:00:00"
    metric = {
        "evidence_id": evidence_id,
        "entity_type": "STRATEGY",
        "strategy_key": "strategy_a",
        "strategy_version": "v1",
        "as_of_date": "2026-08-21",
        "window_days": 60,
        "metrics_json": {},
        "source": "reviewed_selection_validation",
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "artifact_hash": "a" * 64,
        "source_dataset_hash": "b" * 64,
        "evidence_revision_at": "2026-08-21T15:00:00",
        "verification_status": "CONFIRMED",
        "funding_provenance": "EXTERNAL_SUBMITTED",
        "submitted_by": "user-id:1",
        "reviewed_by": "user-id:2",
        "reviewed_at": reviewed_at,
        "created_at": "2026-08-21T15:01:00",
    }
    submission = governance_module._metric_submission_contract(metric)
    metric["evidence_hash"] = governance_module._digest(submission)
    _sql, add_params = governance_module._audit_record(
        entity_type="STRATEGY",
        entity_key="strategy_a",
        action="ADD_METRIC_EVIDENCE",
        reason="提交独立证据",
        operator="user-id:1",
        before={},
        after=submission,
        evidence={
            "evidence_id": evidence_id,
            "evidence_hash": metric["evidence_hash"],
            "artifact_hash": metric["artifact_hash"],
            "source_dataset_hash": metric["source_dataset_hash"],
            "verification_status": "PENDING",
        },
    )
    _sql, review_params = governance_module._audit_record(
        entity_type="STRATEGY",
        entity_key="strategy_a",
        action="CONFIRM_METRIC_EVIDENCE",
        reason="已复核逐笔产物",
        operator="user-id:2",
        before={"verification_status": "PENDING"},
        after={
            "verification_status": "CONFIRMED",
            "reviewed_by": "user-id:2",
            "reviewed_at": reviewed_at,
        },
        evidence={
            "evidence_id": evidence_id,
            "evidence_hash": metric["evidence_hash"],
            "artifact_hash": metric["artifact_hash"],
            "submitted_by": "user-id:1",
            "reviewed_by": "user-id:2",
            "reviewed_at": reviewed_at,
        },
    )
    add_audit = _stored_metric_audit(
        add_params, "2026-08-21T15:01:00",
    )
    review_audit = _stored_metric_audit(review_params, reviewed_at)

    valid, detail = governance_module.metric_evidence_audit_binding(
        metric, [add_audit, review_audit],
    )
    assert valid is True, detail

    forged_valid, forged_detail = (
        governance_module.metric_evidence_audit_binding(metric, [add_audit])
    )
    assert forged_valid is False
    assert forged_detail["confirm_audit_count"] == 0

    duplicate_valid, duplicate_detail = (
        governance_module.metric_evidence_audit_binding(
            metric, [add_audit, review_audit, dict(review_audit)],
        )
    )
    assert duplicate_valid is False
    assert duplicate_detail["confirm_audit_count"] == 2


def test_metric_loader_rejects_confirmed_fields_without_review_audit(
    monkeypatch,
):
    metric = {
        "evidence_id": "f" * 32,
        "entity_type": "STRATEGY",
        "strategy_key": "strategy_a",
        "strategy_version": "v1",
        "as_of_date": "2026-08-21",
        "window_days": 60,
        "metrics_json": {},
        "source": "forged_direct_review",
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "artifact_hash": "a" * 64,
        "source_dataset_hash": "b" * 64,
        "evidence_revision_at": "2026-08-21T15:00:00",
        "verification_status": "CONFIRMED",
        "funding_provenance": "EXTERNAL_SUBMITTED",
        "submitted_by": "user-id:1",
        "reviewed_by": "user-id:2",
        "reviewed_at": "2026-08-21T16:00:00",
        "created_at": "2026-08-21T15:01:00",
    }
    metric["evidence_hash"] = governance_module._digest(
        governance_module._metric_submission_contract(metric)
    )
    monkeypatch.setattr(governance_module, "_table_exists", lambda _name: True)

    def fake_read(sql, _params=None):
        if "FROM st_strategy_metric_input i" in sql:
            return [metric]
        if "FROM st_strategy_governance_audit" in sql:
            return []
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    assert governance_module._load_metric_inputs("2026-08-21") == {}


@pytest.mark.skipif(
    not os.environ.get("PROBIGA_MYSQL57_TEST_URL"),
    reason="PROBIGA_MYSQL57_TEST_URL is not configured",
)
def test_metric_input_review_triggers_enforce_one_way_review_on_mysql57():
    """Destructive only to a unique table in an explicitly named test DB."""

    engine = create_engine(os.environ["PROBIGA_MYSQL57_TEST_URL"])
    database_name = str(engine.url.database or "")
    assert "test" in database_name.lower()
    suffix = uuid.uuid4().hex[:8]
    table_name = f"st_metric_trigger_test_{suffix}"
    trigger_names: list[str] = []
    try:
        with engine.begin() as connection:
            version = str(connection.execute(text(
                "SELECT @@version"
            )).scalar() or "")
            assert version.startswith("5.7.")
            connection.execute(text(f"""
                CREATE TABLE {table_name} (
                    evidence_id CHAR(32) PRIMARY KEY,
                    entity_type VARCHAR(24) NOT NULL,
                    strategy_key VARCHAR(80) NOT NULL,
                    strategy_version VARCHAR(160) NOT NULL,
                    as_of_date DATE NOT NULL,
                    window_days INT NOT NULL,
                    metrics_json LONGTEXT NOT NULL,
                    source VARCHAR(80) NOT NULL,
                    evidence_protocol VARCHAR(80) NOT NULL,
                    artifact_hash CHAR(64) NOT NULL,
                    artifact_json LONGTEXT NOT NULL,
                    source_dataset_hash CHAR(64) NOT NULL,
                    evidence_revision_at DATETIME NOT NULL,
                    verification_status VARCHAR(24) NOT NULL,
                    funding_provenance VARCHAR(40) NOT NULL,
                    submitted_by VARCHAR(80) NOT NULL,
                    reviewed_by VARCHAR(80) NOT NULL DEFAULT '',
                    reviewed_at DATETIME NULL,
                    evidence_hash CHAR(64) NOT NULL,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            for index, contract in enumerate(
                governance_module
                .METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS.values(),
                1,
            ):
                trigger_name = f"trg_metric_guard_{suffix}_{index}"
                trigger_names.append(trigger_name)
                connection.execute(text(
                    f"CREATE TRIGGER {trigger_name} {contract['timing']} "
                    f"{contract['event']} ON {table_name} FOR EACH ROW "
                    f"{contract['body']}"
                ))
            trigger_rows = {
                str(row["trigger_name"]): dict(row)
                for row in connection.execute(text(
                    "SELECT TRIGGER_NAME AS trigger_name, "
                    "ACTION_STATEMENT AS action_statement "
                    "FROM information_schema.TRIGGERS "
                    "WHERE TRIGGER_SCHEMA=DATABASE() "
                    "AND EVENT_OBJECT_TABLE=:table_name"
                ), {"table_name": table_name}).mappings().all()
            }
            assert set(trigger_rows) == set(trigger_names)
            for trigger_name, contract in zip(
                trigger_names,
                governance_module
                .METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS.values(),
            ):
                assert (
                    governance_module._normalized_metric_input_trigger_body(
                        trigger_rows[trigger_name]["action_statement"]
                    )
                    == governance_module._normalized_metric_input_trigger_body(
                        contract["body"]
                    )
                )
            for evidence_id in ("1" * 32, "2" * 32):
                connection.execute(text(f"""
                    INSERT INTO {table_name}
                    (evidence_id, entity_type, strategy_key,
                     strategy_version, as_of_date, window_days,
                     metrics_json, source, evidence_protocol,
                     artifact_hash, artifact_json,
                     source_dataset_hash, evidence_revision_at,
                     verification_status, funding_provenance,
                     submitted_by, evidence_hash)
                    VALUES
                    (:evidence_id, 'STRATEGY', 'strategy_a', 'v1',
                     '2026-08-21', 60, '{{}}', 'test',
                     'PURGED_WALK_FORWARD_V2', :artifact_hash, '{{}}',
                     :dataset_hash, '2026-08-21 15:00:00', 'PENDING',
                     'EXTERNAL_SUBMITTED', 'submitter', :evidence_hash)
                """), {
                    "evidence_id": evidence_id,
                    "artifact_hash": evidence_id[0] * 64,
                    "dataset_hash": evidence_id[0] * 64,
                    "evidence_hash": evidence_id[0] * 64,
                })
        with engine.begin() as connection:
            connection.execute(text(f"""
                UPDATE {table_name}
                SET verification_status='CONFIRMED',
                    reviewed_by='reviewer', reviewed_at=NOW()
                WHERE evidence_id=:evidence_id
            """), {"evidence_id": "1" * 32})
            connection.execute(text(f"""
                UPDATE {table_name}
                SET verification_status='REJECTED',
                    reviewed_by='reviewer', reviewed_at=NOW()
                WHERE evidence_id=:evidence_id
            """), {"evidence_id": "2" * 32})
        forbidden = (
            "SET source='tampered' WHERE evidence_id='11111111111111111111111111111111'",
            "SET verification_status='PENDING', reviewed_by='', reviewed_at=NULL "
            "WHERE evidence_id='11111111111111111111111111111111'",
            "SET verification_status='REJECTED', reviewed_by='reviewer2', "
            "reviewed_at=NOW() "
            "WHERE evidence_id='11111111111111111111111111111111'",
            "SET verification_status='CONFIRMED', reviewed_by='reviewer2', "
            "reviewed_at=NOW() "
            "WHERE evidence_id='22222222222222222222222222222222'",
        )
        for clause in forbidden:
            with pytest.raises(DatabaseError):
                with engine.begin() as connection:
                    connection.execute(text(
                        f"UPDATE {table_name} {clause}"
                    ))
        with pytest.raises(DatabaseError):
            with engine.begin() as connection:
                connection.execute(text(
                    f"DELETE FROM {table_name} "
                    "WHERE evidence_id='11111111111111111111111111111111'"
                ))
    finally:
        with engine.begin() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        engine.dispose()


@pytest.mark.skipif(
    not os.environ.get("PROBIGA_MYSQL57_TEST_URL"),
    reason="PROBIGA_MYSQL57_TEST_URL is not configured",
)
def test_governance_ledgers_are_frozen_and_new_versions_insert_on_mysql57():
    engine = create_engine(os.environ["PROBIGA_MYSQL57_TEST_URL"])
    assert "test" in str(engine.url.database or "").lower()
    simple_tables = (
        "st_strategy_version",
        "st_strategy_combination_version",
        "st_strategy_lifecycle_event",
        "st_strategy_governance_audit",
        "st_strategy_health_snapshot",
        "st_strategy_combination_health_snapshot",
        "st_strategy_pool_snapshot",
        "st_strategy_allocation_snapshot",
    )
    all_tables = (*simple_tables, "st_strategy_governance_run")
    try:
        with engine.begin() as connection:
            assert str(connection.execute(text("SELECT @@version")).scalar()).startswith("5.7.")
            for table_name in all_tables:
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            connection.execute(text("""
                CREATE TABLE st_strategy_version (
                    strategy_key VARCHAR(80) NOT NULL,
                    version VARCHAR(160) NOT NULL,
                    version_hash CHAR(64) NOT NULL,
                    content_hash CHAR(64) NOT NULL,
                    payload VARCHAR(80) NOT NULL,
                    PRIMARY KEY (strategy_key, version),
                    UNIQUE KEY uk_strategy_version_content
                        (strategy_key, content_hash)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            connection.execute(text("""
                CREATE TABLE st_strategy_combination_version (
                    combination_key VARCHAR(80) NOT NULL,
                    version VARCHAR(160) NOT NULL,
                    config_hash CHAR(64) NOT NULL,
                    payload VARCHAR(80) NOT NULL,
                    PRIMARY KEY (combination_key, version),
                    UNIQUE KEY uk_strategy_combination_hash
                        (combination_key, config_hash)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            for table_name in simple_tables[2:]:
                connection.execute(text(
                    f"CREATE TABLE {table_name} ("
                    "id INT NOT NULL PRIMARY KEY, payload VARCHAR(80) NOT NULL"
                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
                ))
            connection.execute(text("""
                CREATE TABLE st_strategy_governance_run (
                    run_uid CHAR(32) PRIMARY KEY, trade_date DATE NOT NULL,
                    run_revision INT NOT NULL, supersedes_run_uid CHAR(32) NOT NULL,
                    is_canonical TINYINT NOT NULL, market_state VARCHAR(40) NOT NULL,
                    source_status VARCHAR(24) NOT NULL, input_ready TINYINT NOT NULL,
                    input_hash CHAR(64) NOT NULL, build_commit_sha VARCHAR(64) NOT NULL,
                    router_policy_version VARCHAR(80) NOT NULL,
                    router_snapshot_hash CHAR(64) NOT NULL,
                    decision_hash CHAR(64) NOT NULL, status VARCHAR(24) NOT NULL,
                    strategy_count INT NOT NULL, formal_count INT NOT NULL,
                    shadow_count INT NOT NULL, combination_count INT NOT NULL,
                    observation_count INT NOT NULL, confirmation_count INT NOT NULL,
                    tradable_count INT NOT NULL, allocation_count INT NOT NULL,
                    summary_json LONGTEXT NOT NULL, created_at DATETIME NOT NULL,
                    finished_at DATETIME NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            for statement in (
                governance_module
                .GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS.values()
            ):
                connection.execute(text(statement))
            detail = governance_module.validate_governance_append_only_triggers(
                connection
            )
            assert detail["trigger_count"] == 18
            connection.execute(text(
                "INSERT INTO st_strategy_version VALUES "
                "('alpha','v1',:version_hash,:content_hash,'content-v1')"
            ), {"version_hash": "1" * 64, "content_hash": "2" * 64})
            connection.execute(text(
                "INSERT INTO st_strategy_combination_version VALUES "
                "('combo','v1',:config_hash,'content-v1')"
            ), {"config_hash": "3" * 64})
            for table_name in simple_tables[2:]:
                connection.execute(text(
                    f"INSERT INTO {table_name} VALUES (1,'content-v1')"
                ))
            connection.execute(text("""
                INSERT INTO st_strategy_governance_run VALUES
                (:run_uid, '2026-08-21', 1, '', 1, 'range', 'fresh', 1,
                 :input_hash, :build_sha, 'router-v1', :router_hash,
                 :decision_hash, 'COMPLETED', 1, 0, 1, 0, 0, 0, 0, 0,
                 '{}', NOW(), NOW())
            """), {
                "run_uid": "a" * 32,
                "input_hash": "4" * 64,
                "build_sha": "5" * 40,
                "router_hash": "6" * 64,
                "decision_hash": "7" * 64,
            })

        forbidden = (
            "UPDATE st_strategy_version SET payload='changed'",
            "UPDATE st_strategy_version SET version_hash=REPEAT('8',64)",
            "UPDATE st_strategy_version SET payload='changed', version_hash=REPEAT('8',64)",
            "DELETE FROM st_strategy_version",
            "UPDATE st_strategy_combination_version SET payload='changed'",
            "UPDATE st_strategy_combination_version SET config_hash=REPEAT('8',64)",
            "UPDATE st_strategy_combination_version SET payload='changed', config_hash=REPEAT('8',64)",
            "DELETE FROM st_strategy_combination_version",
            *(
                statement
                for table_name in simple_tables[2:]
                for statement in (
                    f"UPDATE {table_name} SET payload='changed'",
                    f"DELETE FROM {table_name}",
                )
            ),
            "DELETE FROM st_strategy_governance_run",
        )
        for statement in forbidden:
            with pytest.raises(DatabaseError):
                with engine.begin() as connection:
                    connection.execute(text(statement))
        with engine.begin() as connection:
            connection.execute(text(
                "INSERT INTO st_strategy_version VALUES "
                "('alpha','v2',:version_hash,:content_hash,'content-v2')"
            ), {"version_hash": "8" * 64, "content_hash": "9" * 64})
            connection.execute(text(
                "UPDATE st_strategy_governance_run SET is_canonical=0 "
                "WHERE run_uid=:run_uid"
            ), {"run_uid": "a" * 32})
        with pytest.raises(DatabaseError):
            with engine.begin() as connection:
                connection.execute(text(
                    "INSERT INTO st_strategy_version VALUES "
                    "('alpha','v3',:version_hash,:content_hash,'duplicate')"
                ), {"version_hash": "a" * 64, "content_hash": "9" * 64})
    finally:
        with engine.begin() as connection:
            for table_name in all_tables:
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        engine.dispose()
