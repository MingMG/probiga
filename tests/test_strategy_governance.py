from __future__ import annotations

from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
import io
import inspect
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
from types import SimpleNamespace
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError

from server.api.routers import strategy_center as strategy_center_router
from server.common.sql_reader import bind_sql_connection, read_sql_rows
from server.common.qmt_stock_catalog import A_SHARE_STOCK_CODE_SQL_REGEXP
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
_REAL_V3_ACCOUNT_CONFIG = deepcopy(
    governance_module.load_v3_config()["account"]
)
_FUNDING_EXECUTION_POLICY = (
    governance_module._authoritative_funding_execution_policy()
)


def test_combination_recipe_ready_copy_matches_available_canonical_paging():
    reason = governance_module.COMBINATION_RECIPE_READY_REASON

    assert "取得本轮模拟资金资格后" in reason
    assert "canonical分页复算" in reason
    assert "尚未开放" not in reason


def _ledger_v3_config(initial_cash: int | float) -> dict:
    return {
        "account": {
            **_REAL_V3_ACCOUNT_CONFIG,
            "initial_cash_cny": initial_cash,
        }
    }


def _ledger_account_fact(initial_cash: int | float) -> dict:
    return {
        "account_id": "paper-main-v2",
        "status": "ACTIVE",
        "initial_cash": initial_cash,
        "policy_version": _FUNDING_EXECUTION_POLICY["policy_version"],
        "policy_hash": _FUNDING_EXECUTION_POLICY["policy_hash"],
        "fee_profile_version": _FUNDING_EXECUTION_POLICY[
            "fee_profile_version"
        ],
        "instrument_rule_version": _FUNDING_EXECUTION_POLICY[
            "instrument_rule_version"
        ],
        "real_trading_enabled": 0,
        "created_at": "2026-01-01T00:00:00",
    }


def _pit_industry_snapshot(
    trade_day: str, stock_codes: list[str],
    industry_by_code: dict[str, str],
) -> dict:
    codes = sorted(stock_codes)
    cutoff = (
        date.fromisoformat(trade_day) + timedelta(days=1)
    ).isoformat() + "T00:00:00"
    snapshot_id = governance_module._digest({
        "schema": "unit-test-qmt-industry-snapshot",
        "trade_date": trade_day,
        "stock_codes": codes,
        "industry_by_code": {
            code: industry_by_code.get(code, "") for code in codes
        },
    })
    rows = []
    for code in codes:
        name = str(industry_by_code.get(code) or "")
        if not name:
            continue
        source_row_hash = governance_module._digest({
            "trade_date": trade_day, "stock_code": code, "industry": name,
        })
        row_payload = {
            "snapshot_id": snapshot_id,
            "trade_date": trade_day,
            "as_of_exclusive": cutoff,
            "stock_code": code,
            "industry_name": name,
            "industry_type": "L1",
            "source_system": "unit-test-qmt",
            "source_fact_id": f"qmt:{'a' * 64}:{source_row_hash}",
            "source_effective_at": f"{trade_day}T15:05:00",
            "source_etl_sync_at": f"{trade_day}T15:05:00",
        }
        rows.append({
            **row_payload,
            "row_hash": governance_module._digest(row_payload),
        })
    payload = {
        "schema": governance_module.INDUSTRY_SNAPSHOT_SCHEMA,
        "snapshot_id": snapshot_id if rows else "",
        "trade_date": trade_day,
        "as_of_exclusive": cutoff,
        "status": "COMPLETED" if len(rows) == len(codes) else "INCOMPLETE",
        "requested_stock_codes": codes,
        "rows": rows,
        "reason": "unit-test exact-date QMT industry snapshot",
    }
    return {**payload, "snapshot_hash": governance_module._digest(payload)}


def _pit_industry_path(
    risk_ledger: dict, industry_by_day: dict[str, dict[str, str]],
) -> dict:
    snapshots = []
    trade_dates = []
    for row in risk_ledger["daily_risk_exposures"]:
        trade_day = row["trade_date"]
        codes = sorted(row["combined_stock_weights"])
        snapshot = _pit_industry_snapshot(
            trade_day, codes, industry_by_day[trade_day],
        )
        snapshots.append({
            "trade_date": trade_day,
            "requested_stock_codes": codes,
            "snapshot_hash": snapshot["snapshot_hash"],
            "snapshot": snapshot,
        })
        trade_dates.append(trade_day)
    payload = {
        "schema": governance_module.INDUSTRY_SNAPSHOT_PATH_SCHEMA,
        "window_days": 60,
        "trade_dates": trade_dates,
        "snapshots": snapshots,
        "status": "COMPLETED",
        "reason": "unit-test complete 60-session industry PIT path",
    }
    return {**payload, "path_hash": governance_module._digest(payload)}


def _portfolio_risk_metrics(seed: int, stock_code: str) -> dict:
    start = date(2026, 1, 1)
    daily = [
            {
                "trade_date": (start + timedelta(days=index)).isoformat(),
                "return_pct": round(
                    math.sin(
                        2 * math.pi * (seed + 1) * (index + 1) / 60
                    ),
                    8,
                ),
            }
            for index in range(60)
        ]
    return {
        "internal_daily_records": daily,
        "internal_daily_stock_market_values": [{
            "trade_date": row["trade_date"],
            "stock_risk_exposure": {stock_code: "10000"},
        } for row in daily],
        "internal_stock_exposure": {stock_code: "10000"},
    }


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
    def immutable_receipt(*, start_date, end_date, decision_known_at):
        del decision_known_at
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        sessions = tuple(
            (start + timedelta(days=offset)).isoformat()
            for offset in range((end - start).days + 1)
        )
        session_set_hash = governance_module._digest({
            "schema": "unit-test-calendar-sessions",
            "start_date": start_date,
            "end_date": end_date,
            "sessions": sessions,
        })
        receipt = SimpleNamespace(
            batch_id="unit-test-calendar",
            source_batch_id="a" * 64,
            known_at=f"{end_date} 15:01:00",
            start_date=start_date,
            end_date=end_date,
            session_count=len(sessions),
            session_set_hash=session_set_hash,
            manifest_hash=governance_module._digest({
                "schema": "unit-test-calendar-manifest",
                "session_set_hash": session_set_hash,
            }),
            sessions=sessions,
        )
        receipt.sessions_between = lambda first, last: [
            day for day in sessions if first <= day <= last
        ]
        return receipt
    monkeypatch.setattr(
        governance_module, "_immutable_calendar_receipt", immutable_receipt,
    )
    def session_windows(as_of_date):
        end = date.fromisoformat(as_of_date)
        result = {}
        for window in governance_module.WINDOWS:
            sessions = [
                (end - timedelta(days=offset)).isoformat()
                for offset in range(window - 1, -1, -1)
            ]
            calendar_payload = {
                "schema": "probiga.governance-calendar-receipt-binding.v1",
                "batch_id": "unit-test-calendar",
                "known_at": f"{as_of_date} 15:01:00",
                "start_date": sessions[0],
                "end_date": sessions[-1],
                "session_count": window,
                "session_set_hash": "e" * 64,
                "manifest_hash": "f" * 64,
            }
            calendar = {
                **calendar_payload,
                "binding_hash": governance_module._digest(calendar_payload),
            }
            payload = {
                "schema": "probiga.authoritative-session-window.v1",
                "window_days": window,
                "start_date": sessions[0],
                "end_date": sessions[-1],
                "session_count": window,
                "sessions": sessions,
                "calendar_manifest_hash": calendar["manifest_hash"],
                "calendar_session_set_hash": calendar[
                    "session_set_hash"
                ],
                "calendar_receipt_binding_hash": calendar["binding_hash"],
                "calendar_receipt": calendar,
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


def _test_calendar_binding(sessions: list[str]) -> dict:
    payload = {
        "schema": "probiga.governance-calendar-receipt-binding.v1",
        "batch_id": "ledger-test-calendar",
        "known_at": f"{max(sessions)} 15:01:00",
        "start_date": min(sessions),
        "end_date": max(sessions),
        "session_count": len(sessions),
        "session_set_hash": "e" * 64,
        "manifest_hash": "f" * 64,
    }
    return {**payload, "binding_hash": governance_module._digest(payload)}


def _attest_funding_metrics(
    metrics: dict, *, revision_at: str = "2026-08-21T15:00:00"
) -> dict:
    window = int(metrics.get("window_days") or 0)
    original_completed = int(metrics.get("completed_trades") or 0)
    original_coverage = int(metrics.get("coverage_days") or 0)
    win_days = max(2, min(
        window - 2,
        round(window * float(metrics.get("win_rate_pct") or 0) / 100),
    ))
    average_win = max(0.0001, float(metrics.get("average_win_pct") or 0.1))
    average_loss = max(
        0.0001, float(metrics.get("average_loss_pct") or 0.1)
    )
    end = date.fromisoformat(revision_at[:10])
    internal_records = [{
        "trade_date": (
            end - timedelta(days=window - index - 1)
        ).isoformat(),
        "return_pct": (
            average_win
            if (index * win_days) % window < win_days
            else -average_loss
        ),
        "actual_cost_pct": float(metrics.get("estimated_cost_pct") or 0.0),
        "is_net_return": True,
    } for index in range(window)]
    recalculated = calculate_return_metrics(
        internal_records,
        window_days=window,
        market_match_score=metrics.get("market_match_score"),
        version_bound_evidence=True,
        independent_oos=True,
    )
    for field in (
        "win_rate_pct", "average_win_pct", "average_loss_pct",
        "payoff_ratio", "gross_expectancy_pct", "estimated_cost_pct",
        "net_expectancy_pct", "profit_factor", "max_drawdown_pct",
        "cost_stress_expectancy_pct", "top5_profit_contribution_pct",
    ):
        metrics[field] = recalculated[field]
    session_window = governance_module._authoritative_session_windows(
        revision_at[:10]
    )[window]
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
        "funding_provenance": governance_module.CANONICAL_FUNDING_PROVENANCE,
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
        "session_window_count": window,
        "session_window_hash": session_window["session_hash"],
        "calendar_receipt_binding_hash": session_window[
            "calendar_receipt_binding_hash"
        ],
        "internal_daily_records": internal_records,
        "completed_trades": original_completed,
        "coverage_days": original_coverage,
    })
    governance_module._apply_statistical_health(
        metrics, session_window=session_window,
    )
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
    valid_tolerance = governance_module.build_qmt_v2_manifest(
        daily_universe
    )
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
    def exact_receipt(*, start_date, end_date, decision_known_at):
        del decision_known_at
        sessions = tuple(sorted(descending))
        receipt = SimpleNamespace(
            batch_id="exact-calendar",
            source_batch_id="1" * 64,
            known_at=f"{end_date} 15:01:00",
            start_date=start_date,
            end_date=end_date,
            session_count=len(sessions),
            session_set_hash="2" * 64,
            manifest_hash="3" * 64,
            sessions=sessions,
        )
        receipt.sessions_between = lambda first, last: [
            day for day in sessions if first <= day <= last
        ]
        return receipt
    monkeypatch.setattr(
        governance_module, "_immutable_calendar_receipt", exact_receipt,
    )
    windows, row_binding_proof = (
        governance_module._authoritative_session_windows_with_proof(
            descending[0]
        )
    )
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
    assert row_binding_proof["row_run_binding"] == "SAME_COMPLETED_RUN_ID"
    proof_payload = dict(row_binding_proof)
    proof_hash = proof_payload.pop("proof_hash")
    assert proof_hash == governance_module._digest(proof_payload)
    assert len(row_binding_proof["sessions"]) == 120
    universe_sql = next(sql for sql in observed_sql if "MAX(u.in_target)" in sql)
    assert "EXISTS (" not in universe_sql
    assert universe_sql.count("JOIN qmt_kline_attestation_run r") == 2
    assert universe_sql.count("r.run_id=a.run_id") == 2
    assert universe_sql.count("BINARY r.run_id=BINARY a.run_id") == 2
    assert universe_sql.count(A_SHARE_STOCK_CODE_SQL_REGEXP) == 3
    assert "REGEXP '^(0|3|6)'" not in universe_sql
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
    assert "JOIN qmt_kline_attestation_run r" in aggregate_sql
    assert "r.run_id=a.run_id" in aggregate_sql
    assert "BINARY r.run_id=BINARY a.run_id" in aggregate_sql
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
        "enabled": True,
        "profit_gate_passed": True,
        "metrics": {"60": _portfolio_risk_metrics(0, "000001")},
        "market_route": {
            "market_state": "trend_bullish",
            "eligible": True,
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
    assert "ROW_NUMBER() OVER" in metric_source
    assert "WHERE evidence_rank=1" in metric_source
    assert "_metric_evidence_audit_index" in metric_source
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
    assert "entry_fee_policy_binding_count" in forward_source
    assert "exit_fee_policy_binding_count" in forward_source
    assert "funding_fee_profile_version" in forward_source
    assert "fee_schedule_hash" in forward_source
    assert "fee_schedule_json" in forward_source
    for field, parameter in (
        ("security_type", "funding_fee_security_type"),
        ("buy_commission_rate", "funding_buy_commission_rate"),
        ("sell_commission_rate", "funding_sell_commission_rate"),
        ("minimum_commission", "funding_minimum_commission"),
        ("stamp_tax_sell_rate", "funding_stamp_tax_sell_rate"),
        ("transfer_fee_buy_rate", "funding_transfer_fee_buy_rate"),
        ("transfer_fee_sell_rate", "funding_transfer_fee_sell_rate"),
        ("other_fee_json", "funding_other_fee_json"),
    ):
        assert f"$.{field}" in forward_source
        assert parameter in forward_source
    assert "policy_bound_execution" in forward_source
    assert "funding_instrument_rule_version" in forward_source
    assert "instrument_rule_json" in forward_source
    assert "instrument_rule_hash" in forward_source
    assert "settlement_evidence_json" in forward_source
    assert "settlement_evidence_hash" in forward_source
    assert "accounting_request_hash" in forward_source
    assert "accounting_request_json" in forward_source


def test_metric_prefetch_is_bounded_to_three_latest_rows_per_750_entities(
    monkeypatch,
):
    # Model 400 append-only revisions in every entity/window partition.  The
    # database query must reduce those 900,000 historical rows before Python
    # receives the fixed 750 * 3 current evidence rows.
    historical_evidence_count = 750 * 3 * 400
    selected = [{
        "evidence_id": f"{index:032x}",
        "entity_type": "STRATEGY",
        "strategy_key": f"strategy_{index // 3:04d}",
        "strategy_version": "v1",
        "as_of_date": "2026-08-21",
        "window_days": (20, 60, 120)[index % 3],
        "metrics_json": "{}",
        "source": "EXTERNAL_SUBMITTED",
        "evidence_hash": "a" * 64,
        "evidence_protocol": "probiga.strategy-validation-artifact.v3",
        "artifact_hash": "b" * 64,
        "source_dataset_hash": "c" * 64,
        "evidence_revision_at": "2026-08-21T15:00:00",
        "verification_status": "CONFIRMED",
        "funding_provenance": "EXTERNAL_SUBMITTED",
        "submitted_by": "submitter",
        "reviewed_by": "reviewer",
        "reviewed_at": "2026-08-21T16:00:00",
        "created_at": "2026-08-21T15:01:00",
    } for index in range(750 * 3)]
    calls = []

    def fake_read(sql, params=None):
        calls.append((sql, params or {}))
        if "ranked_current_evidence" in sql:
            return selected
        assert "JOIN JSON_TABLE(:evidence_ids" in sql
        return []

    monkeypatch.setattr(governance_module, "_table_exists", lambda _name: True)
    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module,
        "metric_evidence_audit_binding",
        lambda _row, audit_index: (isinstance(audit_index, dict), {}),
    )

    result = governance_module._load_metric_inputs(
        "2026-08-21",
        current_versions={f"strategy_{index:04d}": "v1" for index in range(750)},
    )

    assert len(calls) == 2
    assert historical_evidence_count == 900_000
    assert len(result) == 750 * 3
    assert len(result) < historical_evidence_count
    assert len(json.loads(calls[1][1]["evidence_ids"])) == 750 * 3
    assert "ROW_NUMBER() OVER" in calls[0][0]
    assert "WHERE evidence_rank=1" in calls[0][0]
    assert "FROM st_strategy_governance_audit audit" in calls[1][0]
    assert "JSON_EXTRACT(audit.evidence_json" in calls[1][0]


def test_invalid_latest_metric_audit_does_not_fall_back_to_older_evidence(
    monkeypatch,
):
    latest = {
        "evidence_id": "f" * 32,
        "entity_type": "STRATEGY",
        "strategy_key": "alpha",
        "strategy_version": "v1",
        "as_of_date": "2026-08-21",
        "window_days": 60,
        "metrics_json": "{}",
        "verification_status": "CONFIRMED",
        "funding_provenance": "EXTERNAL_SUBMITTED",
    }
    calls = 0

    def fake_read(sql, _params=None):
        nonlocal calls
        calls += 1
        return [latest] if "ranked_current_evidence" in sql else []

    monkeypatch.setattr(governance_module, "_table_exists", lambda _name: True)
    monkeypatch.setattr(governance_module, "_db_read", fake_read)

    assert governance_module._load_metric_inputs(
        "2026-08-21", current_versions={"alpha": "v1"},
    ) == {}
    assert calls == 2


def test_dynamic_registry_and_combination_loaders_do_not_limit_object_count():
    from server.engine import strategy_execution_adapters as adapter_module

    registry_wrapper = inspect.getsource(governance_module.load_registry)
    registry_source = inspect.getsource(
        governance_module._load_registry_snapshot
    )
    combination_source = inspect.getsource(governance_module.load_combinations)
    assert "bind_sql_connection(connection)" in registry_wrapper
    assert "SELECT COUNT(*) AS cnt FROM st_strategy_registry" in registry_source
    assert "LIMIT :authoritative_registry_scan_limit" in registry_source
    assert "registry_count + 1" in registry_source
    assert "len(rows) != registry_count" in registry_source
    assert "LIMIT" not in combination_source.upper()
    assert "ORDER BY r.created_at, r.strategy_key" in registry_source
    assert "ORDER BY c.created_at, c.combination_key" in combination_source
    assert registry_source.count("batch_dynamic_shadow_ledger_readiness(") == 1
    assert "ledger_readiness=None" in registry_source
    capability_source = inspect.getsource(
        adapter_module.strategy_execution_adapter_capabilities
    )
    assert capability_source.count("load_registry()") == 1
    assert "dynamic_shadow_ledger_readiness(" not in capability_source
    governance_source = inspect.getsource(governance_module.governance_snapshot)
    assert "registry_rows=registry" in governance_source


def test_global_allocation_caps_funded_sleeves_without_fixed_registry_size():
    rankings = []
    for index in range(12):
        key = f"dynamic_{index:02d}"
        rankings.append({
            "paper_allocation_eligible": True,
            "strategy_key": key,
            "current_version": "v1",
            "current_status": "ACTIVE",
            "funding_gate_hash": governance_module._digest({"gate": key}),
            "strategy_name": key,
            "ranking_score": 80.0 - index,
            "enabled": True,
            "profit_gate_passed": True,
            "metrics": {
                "60": _portfolio_risk_metrics(index, f"{index:06d}")
            },
            "market_route": {
                "market_state": "trend_bullish",
                "eligible": True,
                "market_match_score": 100.0,
                "router_decision_hash": governance_module._digest({
                    "route": key
                }),
            },
        })

    allocations = governance_module._allocation(
        rankings, [], "trend_bullish", trading_allowed=True,
    )
    funded = [
        row for row in allocations if row["target_type"] != "CASH"
    ]

    assert len(rankings) == 12
    assert len(funded) == governance_module.GLOBAL_PORTFOLIO_POLICY[
        "maximum_funded_sleeves"
    ]
    assert {row["target_key"] for row in funded} == {
        f"dynamic_{index:02d}" for index in range(8)
    }
    assert sum(row["simulated_weight_pct"] for row in allocations) == 100.0


def test_unified_competition_does_not_let_weak_combination_displace_member():
    member = {
        "paper_allocation_eligible": True,
        "strategy_key": "strong_member",
        "current_version": "v1",
        "current_status": "ACTIVE",
        "funding_gate_hash": governance_module._digest({"gate": "member"}),
        "strategy_name": "强成员策略",
        "ranking_score": 95.0,
        "enabled": True,
        "profit_gate_passed": True,
        "metrics": {"60": _portfolio_risk_metrics(0, "000001")},
        "market_route": {
            "market_state": "trend_bullish",
            "eligible": True,
            "market_match_score": 100.0,
            "router_decision_hash": governance_module._digest({
                "route": "member"
            }),
        },
    }
    weak_combination = {
        "paper_allocation_eligible": True,
        "combination_key": "weak_combo",
        "current_version": "v1",
        "current_status": "ACTIVE",
        "funding_gate_hash": governance_module._digest({"gate": "combo"}),
        "combination_name": "弱组合",
        "ranking_score": 10.0,
        "enabled": True,
        "profit_gate_passed": True,
        "metrics": {"60": _portfolio_risk_metrics(3, "000001")},
        "member_details": [{
            "strategy_key": "strong_member",
            "strategy_version": "v1",
            "current_strategy_version": "v1",
            "version_match": True,
            "weight": 1.0,
            "lifecycle_status": "ACTIVE",
            "lifecycle_risk_multiplier": 1.0,
        }],
        "member_sleeve_risk_multiplier": 1.0,
        "constraint_evaluation": {"passed": True},
        "market_route": {
            "market_state": "trend_bullish",
            "eligible": True,
            "market_match_score": 100.0,
            "router_decision_hash": governance_module._digest({
                "route": "combo"
            }),
        },
    }

    allocations = governance_module._allocation(
        [member], [weak_combination], "trend_bullish",
        trading_allowed=True,
    )

    funded = [row for row in allocations if row["target_type"] != "CASH"]
    assert [(row["target_type"], row["target_key"])
            for row in funded] == [("STRATEGY", "strong_member")]
    assert funded[0]["simulated_weight_pct"] == 85.0
    cash = allocations[-1]
    assert cash["simulated_weight_pct"] == 15.0
    assert any(
        row["target_key"] == "weak_combo"
        and "重复成员" in row["reason"]
        for row in cash["global_portfolio_rejections"]
    )


def test_global_allocation_fails_closed_on_pair_risk_and_missing_evidence():
    def ranking(key: str, seed: int, code: str) -> dict:
        return {
            "paper_allocation_eligible": True,
            "strategy_key": key,
            "current_version": "v1",
            "current_status": "ACTIVE",
            "funding_gate_hash": governance_module._digest({"gate": key}),
            "strategy_name": key,
            "ranking_score": 90.0 - seed,
            "enabled": True,
            "profit_gate_passed": True,
            "metrics": {"60": _portfolio_risk_metrics(seed, code)},
            "market_route": {
                "market_state": "trend_bullish",
                "eligible": True,
                "market_match_score": 100.0,
                "router_decision_hash": governance_module._digest({
                    "route": key
                }),
            },
        }

    left = ranking("left", 0, "000001")
    left["ranking_score"] = 100.0
    correlated = ranking("correlated", 0, "000001")
    missing = ranking("missing", 2, "000003")
    missing.pop("metrics")

    allocations = governance_module._allocation(
        [left, correlated, missing], [], "trend_bullish",
        trading_allowed=True,
    )
    funded = [
        row for row in allocations if row["target_type"] != "CASH"
    ]

    assert [row["target_key"] for row in funded] == ["left"]
    cash = next(row for row in allocations if row["target_type"] == "CASH")
    reasons = {
        row["target_key"]: row["reason"]
        for row in cash["global_portfolio_rejections"]
    }
    assert "相关性或持仓重叠" in reasons["correlated"]
    assert "缺少有效" in reasons["missing"]
    assert all(row["real_order_authority"] is False for row in allocations)


def test_combination_expansion_cannot_bypass_eight_strategy_sleeves():
    members = [
        {
            "strategy_key": f"member_{index}",
            "strategy_version": "v1",
            "current_strategy_version": "v1",
            "version_match": True,
            "weight": 1 / 9,
            "lifecycle_status": "ACTIVE",
            "lifecycle_risk_multiplier": 1.0,
        }
        for index in range(9)
    ]
    combination = {
        "paper_allocation_eligible": True,
        "combination_key": "nine_member_combo",
        "current_version": "v1",
        "current_status": "ACTIVE",
        "funding_gate_hash": governance_module._digest({"gate": "combo"}),
        "combination_name": "九成员组合",
        "ranking_score": 90.0,
        "enabled": True,
        "profit_gate_passed": True,
        "metrics": {"60": _portfolio_risk_metrics(0, "000001")},
        "member_details": members,
        "member_sleeve_risk_multiplier": 1.0,
        "constraint_evaluation": {"passed": True},
        "market_route": {
            "market_state": "trend_bullish",
            "eligible": True,
            "market_match_score": 100.0,
            "router_decision_hash": governance_module._digest({
                "route": "combo"
            }),
        },
    }

    allocations = governance_module._allocation(
        [], [combination], "trend_bullish", trading_allowed=True,
    )

    assert len(allocations) == 1
    assert allocations[0]["target_type"] == "CASH"
    assert "超过单日上限" in allocations[0][
        "global_portfolio_rejections"
    ][0]["reason"]


def _bind_pool_rows_to_exact_industry(
    trade_date: str, rows: list[dict],
) -> dict:
    from server.engine.strategy_industry_history import build_history_rows

    source_hash = governance_module._digest({
        "schema": "test.qmt-industry-source.v1",
        "trade_date": trade_date,
        "rows": [
            {
                "stock_code": str(row.get("stock_code") or ""),
                "industry_name": str(row.get("industry_name") or ""),
            }
            for row in rows
        ],
    })
    if rows:
        _snapshot_id, history_rows = build_history_rows(
            [
                {
                    "industry_code": f"TEST-{index:03d}",
                    "industry_name": str(row.get("industry_name") or ""),
                    "industry_type": "L1",
                    "stock_code": str(row.get("stock_code") or ""),
                }
                for index, row in enumerate(rows)
            ],
            trade_date=trade_date,
            source="QMT_TEST",
            industry_hash=source_hash,
            captured_at=f"{trade_date}T15:05:00",
        )
        snapshot_id = history_rows[0]["snapshot_id"]
        status = "COMPLETED"
    else:
        history_rows = []
        snapshot_id = ""
        status = "INCOMPLETE"
    payload = {
        "schema": governance_module.INDUSTRY_SNAPSHOT_SCHEMA,
        "snapshot_id": snapshot_id,
        "trade_date": trade_date,
        "as_of_exclusive": (
            date.fromisoformat(trade_date) + timedelta(days=1)
        ).isoformat() + "T00:00:00",
        "status": status,
        "requested_stock_codes": sorted(
            str(row.get("stock_code") or "") for row in rows
        ),
        "rows": history_rows,
        "reason": "测试目标日QMT一级行业冻结事实",
    }
    snapshot = {
        **payload,
        "snapshot_hash": governance_module._digest(payload),
    }
    bindings, _reason, valid = governance_module._industry_snapshot_binding_map(
        snapshot, trade_date,
    )
    assert valid is True
    for row in rows:
        binding = bindings[str(row.get("stock_code") or "")]
        row.update({
            "industry_name": binding["industry_name"],
            "industry_type": binding["industry_type"],
            "industry_snapshot_id": binding["snapshot_id"],
            "industry_snapshot_hash": binding["snapshot_hash"],
            "industry_row_hash": binding["row_hash"],
            "industry_source_system": binding["source_system"],
            "industry_source_fact_id": binding["source_fact_id"],
            "industry_binding": binding,
        })
    return snapshot


def test_governance_industry_wrapper_keeps_fallback_provenance(monkeypatch):
    from server.engine.strategy_industry_history import build_history_rows

    target = "2026-08-28"
    source_date = "2026-08-27"
    _snapshot_id, history_rows = build_history_rows(
        [{
            "industry_code": "801780",
            "industry_name": "银行",
            "industry_type": "L1",
            "stock_code": "000001",
        }],
        trade_date=target,
        source="QMT_TEST",
        industry_hash="a" * 64,
        captured_at=f"{source_date}T15:12:00",
        source_snapshot_date=source_date,
        capture_mode="qmt_close_full_refresh",
        fallback_reason="QMT_HISTORICAL_SECTOR_API_UNAVAILABLE",
    )
    monkeypatch.setattr(
        governance_module, "_strict_table_exists", lambda _table: True,
    )

    def read(sql, params=None):
        if "FROM qmt_membership_snapshot_run" in sql:
            return [{"run_count": 0}]
        if "FROM si_trade_calendar" in sql:
            return [{"trade_date": source_date}]
        if "FROM st_strategy_industry_history" in sql:
            assert params["industry_trade_date"] == target
            assert params["industry_cutoff"] == "2026-08-29T00:00:00"
            return history_rows
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", read)

    snapshot = governance_module._frozen_industry_snapshot(
        target, ["000001"],
    )
    bindings, reason, valid = governance_module._industry_snapshot_binding_map(
        snapshot, target, expected_codes=["000001"],
    )

    assert valid is True
    assert snapshot["status"] == "COMPLETED"
    assert set(bindings) == {"000001"}
    assert bindings["000001"]["source_effective_at"] == (
        f"{source_date}T15:12:00"
    )
    assert "source_snapshot_date=2026-08-27" in reason
    assert "capture_mode=qmt_close_full_refresh" in reason
    assert "fallback_reason=QMT_HISTORICAL_SECTOR_API_UNAVAILABLE" in reason


def test_governance_fallback_is_invalid_after_target_run_arrives(monkeypatch):
    from server.engine.strategy_industry_history import build_history_rows

    target = "2026-08-28"
    source_date = "2026-08-27"
    _snapshot_id, history_rows = build_history_rows(
        [{
            "industry_code": "801780",
            "industry_name": "银行",
            "industry_type": "L1",
            "stock_code": "000001",
        }],
        trade_date=target,
        source="QMT_TEST",
        industry_hash="a" * 64,
        captured_at=f"{source_date}T15:12:00",
        source_snapshot_date=source_date,
        fallback_reason="QMT_HISTORICAL_SECTOR_API_UNAVAILABLE",
    )
    monkeypatch.setattr(
        governance_module, "_strict_table_exists", lambda _table: True,
    )

    def read(sql, _params=None):
        if "FROM qmt_membership_snapshot_run" in sql:
            return [{"run_count": 1}]
        if "FROM si_trade_calendar" in sql:
            return [{"trade_date": source_date}]
        if "FROM st_strategy_industry_history" in sql:
            return history_rows
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", read)

    snapshot = governance_module._frozen_industry_snapshot(
        target, ["000001"],
    )
    bindings, reason, valid = governance_module._industry_snapshot_binding_map(
        snapshot, target, expected_codes=["000001"],
    )

    assert snapshot["status"] == "INVALID"
    assert "目标日已存在原始QMT run" in snapshot["reason"]
    # The wrapper remains structurally valid evidence of an INVALID selection;
    # callers separately require status=COMPLETED before using bindings.
    assert valid is True
    assert bindings == {}
    assert "拒绝前一日行业降级" in snapshot["reason"]


@pytest.mark.parametrize("inactive_status", ("SHADOW", "SUSPENDED"))
def test_funding_pool_recomputes_active_lane_without_mixed_status_poison(
    monkeypatch, inactive_status,
):
    monkeypatch.setattr(
        governance_module,
        "_snapshot_trading_gate",
        lambda _snapshot: {
            "status": "ALLOW_NEW_BUY",
            "trading_allowed": True,
            "market_risk_cap_pct": 85.0,
            "candidate_source_hash": "a" * 64,
        },
    )
    strategies = [{
        "strategy_key": "active_lane",
        "strategy_name": "active",
        "current_status": "ACTIVE",
        "ranking_score": 90.0,
        "enabled": True,
        "execution_adapter_executable": True,
        "paper_allocation_eligible": True,
    }, {
        "strategy_key": "inactive_lane",
        "strategy_name": "inactive",
        "current_status": inactive_status,
        "ranking_score": 100.0,
        "enabled": True,
        "execution_adapter_executable": True,
        "paper_allocation_eligible": False,
    }]
    candidate = {
        "stock_code": "000001",
        "stock_name": "测试股",
        "industry_name": "银行",
        "strategies": ["active_lane", "inactive_lane"],
        # These candidate-wide fields are deliberately poisoned by the
        # non-funding contributor and must not reach the funded lane.
        "dominant_strategy": "inactive_lane",
        "model_confidence": 99.0,
        "risk_reward_ratio": 9.0,
        "final_status": "BLOCKED",
        "blocking_reasons": ["inactive veto"],
        "strategy_signals": [{
            "strategy_key": "active_lane",
            "signal_direction": "BUY",
            "effective_weight": 1.0,
            "effective_score": 80.0,
            "model_confidence": 80.0,
            "risk_reward_ratio": 2.0,
            "entry_low": 10.0,
            "entry_high": 10.2,
            "stop_loss": 9.5,
            "gate_status": "PASS",
            "risk_level": "LOW",
        }, {
            "strategy_key": "inactive_lane",
            "signal_direction": "SELL",
            "effective_weight": 1.0,
            "effective_score": 99.0,
            "model_confidence": 99.0,
            "risk_reward_ratio": 9.0,
            "gate_status": "BLOCK",
            "gate_reason": "inactive veto",
            "risk_level": "CRITICAL",
        }],
    }
    industry_snapshot = _bind_pool_rows_to_exact_industry(
        "2026-08-21", [candidate],
    )
    pools = governance_module._build_pools(
        {
            "trade_date": "2026-08-21",
            "source_status": "fresh",
            "market_state": {"key": "trend_bullish"},
            "candidates": [candidate],
        },
        strategies,
        industry_snapshot=industry_snapshot,
    )

    assert len(pools["tradable"]) == 1
    funded = pools["tradable"][0]
    assert funded["strategies"] == ["active_lane"]
    assert funded["dominant_strategy"] == "active_lane"
    assert funded["model_confidence"] == 80.0
    assert funded["risk_reward_ratio"] == 2.0
    assert funded["blocking_reasons"] == []
    assert funded["evidence"]["excluded_contributor_keys"] == [
        "inactive_lane"
    ]
    assert funded["real_order_authority"] is False


def test_metrics_prefetch_query_count_is_bounded_for_750_strategies(
    monkeypatch,
):
    inventory_size = 750
    registry = []
    records = {}
    for index in range(inventory_size):
        key = f"strategy_{index:04d}"
        registry.append({
            "strategy_key": key,
            "current_version": "v1",
            "version_hash": "a" * 64,
            "market_route": {"market_match_score": 100.0},
        })
        records[key] = [{
            "entry_trade_date": "2026-08-21",
            "entry_at": "2026-08-21T09:30:00",
            "account_id": f"paper-{index:04d}",
            "stock_code": f"{index:06d}",
        }]
    queries = []

    def fake_read(sql, params=None):
        queries.append(sql)
        if "FROM si_trade_calendar" in sql:
            return [{"trade_date": "2026-08-21"}]
        if "FROM sm_stock_kline" in sql:
            return []
        if "FROM st_trade_account_v2" in sql:
            return [
                {"account_id": value}
                for key, value in (params or {}).items()
                if key.startswith("account_")
            ]
        raise AssertionError(sql)

    ledger_calls = 0
    maximum_accounts_per_prefetch = 0

    def fake_ledger(*_args, prefetched_facts=None, **_kwargs):
        nonlocal ledger_calls, maximum_accounts_per_prefetch
        assert prefetched_facts is not None
        ledger_calls += 1
        maximum_accounts_per_prefetch = max(
            maximum_accounts_per_prefetch,
            len(prefetched_facts.get("account_rows") or []),
        )
        return {"valid": False, "reason": "query-count fixture"}

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module,
        "_load_forward_records",
        lambda _day, batch, **_kwargs: {
            row["strategy_key"]: records[row["strategy_key"]]
            for row in batch
        },
    )
    monkeypatch.setattr(
        governance_module, "_load_metric_inputs", lambda *_a, **_k: {},
    )
    monkeypatch.setattr(
        governance_module, "_internal_strategy_portfolio_ledger", fake_ledger,
    )
    monkeypatch.setattr(
        governance_module,
        "_load_funding_replay_index",
        lambda _day, rows: {
            row["strategy_key"]: {
                "mode": "BOUNDED_INCREMENTAL",
                "checkpoint_id": hashlib.sha256(
                    row["strategy_key"].encode("utf-8")
                ).hexdigest(),
                "trade_date": "2026-08-20",
                "version_checkpoint_count": 1,
            }
            for row in rows
        },
    )
    monkeypatch.setattr(
        governance_module,
        "_load_funding_replay_plans",
        lambda _day, rows: {
            row["strategy_key"]: {
                "mode": "BOUNDED_INCREMENTAL",
                "state": {"holdings": []},
                "checkpoint_id": hashlib.sha256(
                    row["strategy_key"].encode("utf-8")
                ).hexdigest(),
                "trade_date": "2026-08-20",
                "account_id": "paper-" + row["strategy_key"].split("_")[-1],
            }
            for row in rows
        },
    )
    windows = {
        window: {
            "start_date": "2026-08-21",
            "end_date": "2026-08-21",
            "session_count": window,
        }
        for window in governance_module.WINDOWS
    }

    result = governance_module._metrics_for_registry(
        {}, registry, "2026-08-21", authoritative_windows=windows,
    )

    assert len(result) == inventory_size + 1
    assert ledger_calls == inventory_size
    assert maximum_accounts_per_prefetch <= (
        governance_module.FUNDING_METRICS_STRATEGY_BATCH_SIZE
    )
    assert result["__funding_persistence__"]["strategy_batch_count"] == 30
    # Calendar sessions come from one validated append-only receipt, so each
    # 25-strategy batch performs only one price and one account read here.
    # Growth remains bounded by batch, never by strategy.
    assert len(queries) == 30 * 2


def test_750_first_bootstraps_use_configured_batch_budget_without_registry_cap(
    monkeypatch,
):
    registry = [{
        "strategy_key": f"new_strategy_{index:04d}",
        "current_version": "v1",
        "version_created_at": f"2026-01-{(index % 28) + 1:02d}",
        "current_status": "SHADOW",
        "market_route": {"market_match_score": 0.0},
    } for index in range(750)]
    monkeypatch.setattr(
        governance_module,
        "_load_funding_replay_index",
        lambda _day, rows: {
            row["strategy_key"]: {
                "mode": "FULL_BOOTSTRAP",
                "checkpoint_id": "",
                "trade_date": "",
                "version_checkpoint_count": 0,
            }
            for row in rows
        },
    )
    processed_batches = []
    selected_count = 0

    def fake_metrics(_snapshot, rows, _day, **kwargs):
        nonlocal selected_count
        assert kwargs["_batching_disabled"] is True
        processed_batches.append([row["strategy_key"] for row in rows])
        selected_count += 1
        return {
            rows[0]["strategy_key"]: {
                window: {"window_days": window} for window in governance_module.WINDOWS
            },
            "__funding_persistence__": {
                "selected_storage_bytes": 1024 * selected_count,
                "bootstrap_selected": True,
                "bootstrap_selected_count": selected_count,
                "bootstrap_selected_storage_bytes": 1024 * selected_count,
            },
        }

    monkeypatch.setattr(governance_module, "_metrics_for_registry", fake_metrics)
    windows = {
        window: {
            "start_date": "2026-01-01", "end_date": "2026-08-21",
            "session_count": window, "session_hash": "a" * 64,
        }
        for window in governance_module.WINDOWS
    }
    result = governance_module._metrics_for_registry_batched(
        {}, registry, "2026-08-21", authoritative_windows=windows,
        candidate_run_uid="1" * 32, manual={},
    )

    assert processed_batches and sum(map(len, processed_batches)) == 8
    assert len(result) == 751
    summary = result["__funding_persistence__"]
    assert summary["bootstrap_processed_count"] == 8
    assert summary["bootstrap_selected_count"] == 8
    assert summary["deferred_bootstrap_count"] == 742
    assert summary["bootstrap_selected"] is True
    deferred = [
        row for key, row in result.items()
        if key != "__funding_persistence__"
        and row.get("funding_checkpoint_persistence_reason_code")
        == "CHECKPOINT_BOOTSTRAP_COUNT_BUDGET_DEFERRED"
    ]
    assert len(deferred) == 742
    assert all("funding_checkpoint_candidate" not in row for row in deferred)
    assert all(
        "internal_daily_records" not in row[120]
        and "internal_equity_curve" not in row[120]
        for row in deferred
    )


def test_bootstrap_time_budget_explicitly_defers_remaining_strategies(
    monkeypatch,
):
    registry = [{
        "strategy_key": f"timed_strategy_{index}",
        "current_version": "v1",
        "version_created_at": f"2026-01-0{index + 1}",
        "current_status": "SHADOW",
        "market_route": {"market_match_score": 0.0},
    } for index in range(3)]
    monkeypatch.setattr(
        governance_module,
        "_load_funding_replay_index",
        lambda _day, rows: {
            row["strategy_key"]: {"mode": "FULL_BOOTSTRAP"}
            for row in rows
        },
    )
    monkeypatch.setattr(
        governance_module,
        "_funding_bootstrap_budget",
        lambda: {
            "maximum_strategies": 3,
            "time_budget_seconds": 1,
            "byte_budget": 4096,
        },
    )
    monotonic_values = iter((0.0, 0.0, 2.0))
    monkeypatch.setattr(
        governance_module.time, "monotonic", lambda: next(monotonic_values),
    )
    processed = []

    def fake_metrics(_snapshot, rows, _day, **_kwargs):
        processed.extend(row["strategy_key"] for row in rows)
        return {
            rows[0]["strategy_key"]: {
                window: {"window_days": window}
                for window in governance_module.WINDOWS
            },
            "__funding_persistence__": {
                "selected_storage_bytes": 1024,
                "bootstrap_selected_count": 1,
                "bootstrap_selected_storage_bytes": 1024,
            },
        }

    monkeypatch.setattr(governance_module, "_metrics_for_registry", fake_metrics)
    result = governance_module._metrics_for_registry_batched(
        {}, registry, "2026-08-21", authoritative_windows={},
        candidate_run_uid="1" * 32, manual={},
    )

    assert processed == ["timed_strategy_0"]
    summary = result["__funding_persistence__"]
    assert summary["bootstrap_processed_count"] == 1
    assert summary["deferred_bootstrap_by_reason"] == {
        "CHECKPOINT_BOOTSTRAP_TIME_BUDGET_DEFERRED": 2,
    }
    assert all(
        result[f"timed_strategy_{index}"][
            "funding_checkpoint_persistence_reason_code"
        ] == "CHECKPOINT_BOOTSTRAP_TIME_BUDGET_DEFERRED"
        for index in (1, 2)
    )


def test_bootstrap_candidate_byte_budget_defer_is_counted_and_fail_closed(
    monkeypatch,
):
    registry = [{
        "strategy_key": f"large_strategy_{index}",
        "current_version": "v1",
        "version_created_at": f"2026-01-0{index + 1}",
        "current_status": "SHADOW",
        "market_route": {"market_match_score": 0.0},
    } for index in range(2)]
    monkeypatch.setattr(
        governance_module,
        "_load_funding_replay_index",
        lambda _day, rows: {
            row["strategy_key"]: {"mode": "FULL_BOOTSTRAP"}
            for row in rows
        },
    )
    monkeypatch.setattr(
        governance_module,
        "_funding_bootstrap_budget",
        lambda: {
            "maximum_strategies": 2,
            "time_budget_seconds": 120,
            "byte_budget": 1024,
        },
    )

    def fake_metrics(_snapshot, rows, _day, **_kwargs):
        key = rows[0]["strategy_key"]
        return {
            key: {
                **{
                    window: {"window_days": window}
                    for window in governance_module.WINDOWS
                },
                "funding_checkpoint_persistence_reason_code": (
                    "CHECKPOINT_BOOTSTRAP_BYTE_BUDGET_DEFERRED"
                ),
                "funding_checkpoint_persistence_reason": (
                    "候选检查点超过本轮字节预算"
                ),
                "funding_checkpoint_storage_bytes": 2048,
            },
            "__funding_persistence__": {
                "selected_storage_bytes": 0,
                "bootstrap_selected_count": 0,
                "bootstrap_selected_storage_bytes": 0,
            },
        }

    monkeypatch.setattr(governance_module, "_metrics_for_registry", fake_metrics)
    result = governance_module._metrics_for_registry_batched(
        {}, registry, "2026-08-21", authoritative_windows={},
        candidate_run_uid="1" * 32, manual={},
    )

    summary = result["__funding_persistence__"]
    assert summary["bootstrap_processed_count"] == 2
    assert summary["bootstrap_selected_count"] == 0
    assert summary["deferred_bootstrap_count"] == 2
    assert summary["deferred_bootstrap_by_reason"] == {
        "CHECKPOINT_BOOTSTRAP_BYTE_BUDGET_DEFERRED": 2,
    }
    assert all(
        "funding_checkpoint_candidate" not in result[row["strategy_key"]]
        for row in registry
    )


def test_replay_plan_memory_budget_uses_exact_strict_serialized_bytes():
    plan = {
        "mode": "BOUNDED_INCREMENTAL",
        "state": {"closing_cash_cny": "123.45", "holdings": []},
        "rolling_history": {
            "daily_records": [{
                "trade_date": "2026-08-21", "return_pct": "0.1",
            }],
        },
        "checkpoint_id": "1" * 64,
        "checkpoint_hash": "2" * 64,
        "chain_hash": "3" * 64,
        "account_id": "paper-main-v2",
        "trade_date": "2026-08-21",
        "max_holding_days": 20,
        "bootstrap_full_history_scan": False,
    }
    expected_payload = {
        key: plan[key] for key in (
            "mode", "state", "rolling_history", "checkpoint_id",
            "checkpoint_hash", "chain_hash", "account_id", "trade_date",
            "max_holding_days", "bootstrap_full_history_scan",
        )
    }
    assert governance_module._funding_replay_plan_serialized_bytes(plan) == len(
        governance_module._checkpoint_canonical_json(expected_payload).encode(
            "utf-8"
        )
    )
    broken = deepcopy(plan)
    broken["rolling_history"] = {"unknown": object()}
    with pytest.raises(TypeError):
        governance_module._funding_replay_plan_serialized_bytes(broken)
    source = inspect.getsource(governance_module._load_funding_replay_plans)
    assert "FUNDING_REPLAY_BATCH_MAX_SERIALIZED_BYTES" in source
    assert "runtime_serialized_bytes" in source


def test_metrics_checkpoint_capacity_probe_binds_ref_and_selects_candidate(
    monkeypatch,
):
    strategy = {
        "strategy_key": "checkpoint_probe",
        "current_version": "v1",
        "version_hash": "a" * 64,
        "current_status": "SHADOW",
        "market_route": {"market_match_score": 100.0},
    }
    candidate = {
        "anchor_run_uid": "run-checkpoint-probe",
        "state": {"replay_mode": "FULL_BOOTSTRAP"},
    }
    checkpoint_ref = {
        "checkpoint_id": "cp-checkpoint-probe",
        "strategy_key": "checkpoint_probe",
        "strategy_version": "v1",
        "account_id": "paper-checkpoint-probe",
        "trade_date": "2026-08-21",
    }
    monkeypatch.setattr(
        governance_module,
        "_load_funding_replay_plans",
        lambda *_args, **_kwargs: {
            "checkpoint_probe": {"mode": "FULL_BOOTSTRAP"},
        },
    )
    monkeypatch.setattr(
        governance_module,
        "_load_forward_records",
        lambda *_args, **_kwargs: {"checkpoint_probe": []},
    )
    monkeypatch.setattr(
        governance_module, "_load_metric_inputs", lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        governance_module,
        "_prefetch_internal_strategy_ledger_facts",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        governance_module,
        "_internal_strategy_portfolio_ledger",
        lambda *_args, **_kwargs: {
            "valid": True,
            "funding_checkpoint_candidate": candidate,
            "funding_checkpoint_ref": checkpoint_ref,
        },
    )
    monkeypatch.setattr(
        governance_module,
        "_slice_internal_ledger",
        lambda *_args, **_kwargs: {
            "valid": False,
            "reason": "capacity probe fixture",
        },
    )

    def probe_manifest(*, strategies, **_kwargs):
        assert strategies == [{
            "strategy_key": "checkpoint_probe",
            "current_version": "v1",
            "funding_checkpoint_candidate": candidate,
            "funding_checkpoint_ref": checkpoint_ref,
        }]
        return {"total_storage_bytes": 1024}, [candidate]

    monkeypatch.setattr(
        governance_module,
        "_build_funding_checkpoint_manifest",
        probe_manifest,
    )
    windows = {
        window: {
            "start_date": "2026-08-21",
            "end_date": "2026-08-21",
            "session_count": window,
            "session_hash": "b" * 64,
        }
        for window in governance_module.WINDOWS
    }

    result = governance_module._metrics_for_registry(
        {}, [strategy], "2026-08-21", authoritative_windows=windows,
        candidate_run_uid="run-checkpoint-probe",
    )

    assert result["checkpoint_probe"][
        "funding_checkpoint_persistence_reason_code"
    ] == "CHECKPOINT_SELECTED"
    assert result["checkpoint_probe"]["funding_checkpoint_candidate"] is candidate
    assert result["checkpoint_probe"]["funding_checkpoint_ref"] is checkpoint_ref
    assert result["__funding_persistence__"]["bootstrap_selected"] is True


def test_canonical_competition_projection_bounds_750_strategy_json_bytes():
    start = date(2026, 1, 1)

    def window_metrics(window):
        daily = [{
            "trade_date": (start + timedelta(days=index)).isoformat(),
            "return_pct": 0.12345678,
            "actual_cost_pct": 0.01234567,
            "is_net_return": True,
            "evidence_revision_at": (
                start + timedelta(days=index)
            ).isoformat() + "T15:00:00",
        } for index in range(window)]
        return {
            "window_days": window,
            "completed_trades": window,
            "coverage_days": window,
            "win_rate_pct": 55.0,
            "average_win_pct": 1.2,
            "average_loss_pct": -0.8,
            "payoff_ratio": 1.5,
            "gross_expectancy_pct": 0.2,
            "estimated_cost_pct": 0.05,
            "net_expectancy_pct": 0.15,
            "profit_factor": 1.4,
            "max_drawdown_pct": 5.0,
            "health_score": 80.0,
            "evidence_hash": "a" * 64,
            "internal_ledger_hash": "b" * 64,
        "internal_ledger_schema": "probiga.internal-strategy-portfolio-ledger.v3",
            "internal_daily_records": daily,
            "internal_equity_curve": [{
                "trade_date": item["trade_date"],
                "equity": round(100.0 + index * 0.1, 4),
            } for index, item in enumerate(daily)],
            "internal_stock_exposure": {
                f"{600000 + index:06d}": str(10_000 + index * 10)
                for index in range(20)
            },
            "profit_gate": {"passed": True, "reason": "测试通过"},
        }

    metrics = {
        str(window): window_metrics(window)
        for window in governance_module.WINDOWS
    }
    template = {
        "strategy_key": "strategy_0000",
        "strategy_name": "容量测试策略",
        "current_version": "v1",
        "current_status": "ACTIVE",
        "enabled": True,
        "ranking_score": 80.0,
        "funding_gate_hash": "c" * 64,
        "market_route": {
            "router_decision_hash": "d" * 64,
            "eligible": True,
        },
        "metrics": metrics,
        "primary_metrics": metrics["60"],
        "profit_gate_passed": True,
        "paper_allocation_eligible": True,
        "real_order_authority": False,
    }
    estimated_unbounded_bytes = len(
        governance_module._json_text({"strategies": [template]}).encode("utf-8")
    ) * 750
    assert estimated_unbounded_bytes > 32 * 1024 * 1024

    rows = [
        {**template, "strategy_key": f"strategy_{index:04d}"}
        for index in range(750)
    ]
    projected = governance_module._canonical_competition_rows(
        rows, entity_type="STRATEGY",
    )
    serialized = governance_module._json_text({"strategies": projected})

    assert len(serialized.encode("utf-8")) < 4 * 1024 * 1024
    assert "internal_daily_records" not in serialized
    assert "internal_equity_curve" not in serialized
    assert "internal_stock_exposure" not in serialized
    assert len(projected) == 750
    assert projected[0]["metric_detail_inline"] is False
    assert projected[0]["metrics"]["120"]["detail_ref"][
        "source_table"
    ] == "st_strategy_health_snapshot"
    assert re.fullmatch(
        r"[0-9a-f]{64}",
        projected[0]["metrics"]["120"]["detail_ref"]["detail_hash"],
    )
    # Projection must not destroy the in-memory evidence needed by combination
    # calculation and health/checkpoint persistence.
    assert len(rows[0]["metrics"]["120"]["internal_daily_records"]) == 120

    for row in rows:
        row.update({
            "recommended_status": "ACTIVE",
            "funding_evidence_revision_at": "2026-08-24T15:00:00",
        })

    class CaptureConnection:
        def __init__(self):
            self.calls = []

        def execute(self, statement, parameters):
            self.calls.append((str(statement), parameters))

    connection = CaptureConnection()
    governance_module._persist_health(
        connection, "a" * 32, "2026-08-24", rows,
    )
    persisted_rows = [
        item for _statement, batch in connection.calls for item in batch
    ]
    assert len(connection.calls) == 23
    assert len(persisted_rows) == 750 * 3
    assert all(len(batch) <= 100 for _statement, batch in connection.calls)
    assert all(
        len(governance_module._json_text(batch).encode("utf-8"))
        <= 4 * 1024 * 1024
        for _statement, batch in connection.calls
    )
    assert all(
        "internal_daily_records" not in item["evidence_json"]
        and "internal_equity_curve" not in item["evidence_json"]
        and "internal_stock_exposure" not in item["evidence_json"]
        for item in persisted_rows
    )


def test_verified_checkpoint_detail_is_windowed_paged_and_hash_bound():
    start = date(2026, 1, 1)
    days = [
        (start + timedelta(days=index)).isoformat() for index in range(120)
    ]
    rolling = {
        "schema": "probiga.strategy-funding-rolling-history.v1",
        "opening_normalized_equity": "100.0",
        "opening_equity_date": "2025-12-31",
        "daily_records": [{
            "trade_date": day,
            "return_pct": 0.1,
            "actual_cost_pct": 0.01,
            "is_net_return": True,
            "evidence_revision_at": f"{day}T15:00:00",
        } for day in days],
        "equity_curve": [{
            "trade_date": day, "equity": 100.0 + index * 0.1,
        } for index, day in enumerate(days)],
        "daily_stock_market_values": [{
            "trade_date": day,
            "stock_closing_market_values": {"600000": "1000.0"},
            "stock_intraday_turnover_proxy": {},
            "stock_risk_exposure": {"600000": "1000.0"},
        } for day in days],
        "trade_exposures": [{
            "evidence_id": f"evidence-{index:03d}",
            "trade_date": day,
            "source_intent_id": f"intent-{index:03d}",
            "stock_code": "600000",
            "entry_gross_cny": "1000.0",
            "status": "MATURED",
        } for index, day in enumerate(days)],
    }
    fact_members = [{
        "fact_id": f"{index + 1:064x}",
        "fact_hash": f"{index + 1001:064x}",
        "trade_date": day,
    } for index, day in enumerate(days)]
    state = {
        "schema": governance_module.FUNDING_CHECKPOINT_SCHEMA,
        "strategy_key": "dynamic_alpha",
        "strategy_version": "v1",
        "strategy_version_hash": "a" * 64,
        "execution_binding_hash": "b" * 64,
        "account_id": "paper-account",
        "trade_date": days[-1],
        "replay_mode": "BOUNDED_INCREMENTAL",
        "replay_start_date": days[-1],
        "replay_session_count": 1,
        "max_holding_days": 20,
        "history_start_date": days[0],
        "history_end_date": days[-1],
        "history_fact_count": len(days),
        "history_fact_set_hash": (
            governance_module.ordered_funding_fact_set_hash(fact_members)
        ),
        "opening_cash_cny": "1000000.000000",
        "closing_cash_cny": "999000.000000",
        "opening_equity_cny": "1000000.000000",
        "closing_equity_cny": "1001000.000000",
        "cumulative_fee_cny": "10.000000",
        "high_watermark_equity_cny": "1001000.000000",
        "holdings": [{
            "evidence_id": "open-evidence",
            "source_intent_id": "open-intent",
            "stock_code": "600000",
            "quantity": 100,
            "entry_day": days[-2],
            "entry_at": f"{days[-2]}T10:00:00",
            "entry_price": "10.0",
            "entry_gross_cny": "1000.0",
            "entry_fee_cny": "1.0",
            "mark_price": "10.1",
            "holding_session_age": 2,
        }],
        "evidence_watermark": f"{days[-1]}T15:00:00",
        "input_set_hash": "c" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    checkpoint_hash = governance_module.checkpoint_state_hash(state)
    verified = {
        "state": state,
        "checkpoint_id": "d" * 64,
        "checkpoint_hash": checkpoint_hash,
        "chain_hash": "e" * 64,
    }
    verified_fact_chain = {
        "opening_normalized_equity": rolling[
            "opening_normalized_equity"
        ],
        "opening_equity_date": rolling["opening_equity_date"],
        "daily_records": rolling["daily_records"],
        "equity_curve": rolling["equity_curve"],
        "daily_stock_market_values": rolling[
            "daily_stock_market_values"
        ],
        "closed_evidence_by_day": [{
            "trade_date": day,
            "evidence_ids": [f"evidence-{index:03d}"],
        } for index, day in enumerate(days)],
        "fact_members": fact_members,
    }

    first = governance_module._funding_checkpoint_detail_page(
        verified,
        verified_fact_chain,
        series="daily_records",
        window_days=20,
        cursor="",
        limit=7,
    )
    second = governance_module._funding_checkpoint_detail_page(
        verified,
        verified_fact_chain,
        series="daily_records",
        window_days=20,
        cursor=first["next_cursor"],
        limit=7,
    )

    assert first["total_count"] == 20
    assert len(first["items"]) == 7
    assert first["items"][0]["trade_date"] == days[-20]
    assert second["offset"] == 7
    assert first["replay_mode"] == "BOUNDED_INCREMENTAL"
    assert first["source_kind"] == (
        "VERIFIED_NORMALIZED_FUNDING_FACT_CHAIN_V3"
    )
    assert first["automatic_real_order_submission"] is False
    assert first["real_order_authority"] is False
    assert re.fullmatch(r"[0-9a-f]{64}", first["page_hash"])

    holdings = governance_module._funding_checkpoint_detail_page(
        verified,
        verified_fact_chain,
        series="holdings",
        window_days=120,
        cursor="",
        limit=50,
    )
    assert holdings["total_count"] == 1
    assert holdings["items"][0]["evidence_id"] == "open-evidence"


def test_combination_health_persistence_is_bounded_and_summary_only():
    metrics = {
        str(window): {
            "window_days": window,
            "completed_trades": window,
            "coverage_days": window,
            "health_score": 80.0,
            "net_expectancy_pct": 0.1,
            "payoff_ratio": 1.2,
            "profit_factor": 1.3,
            "internal_ledger_hash": "b" * 64,
            "internal_daily_records": [{
                "trade_date": "2026-08-24", "return_pct": 0.1,
            }],
            "internal_equity_curve": [{
                "trade_date": "2026-08-24", "equity": 100.1,
            }],
            "internal_stock_exposure": {"600000": "1000"},
            "profit_gate": {"passed": True, "reason": "测试通过"},
        }
        for window in governance_module.WINDOWS
    }
    combinations = [{
        "combination_key": f"combo_{index:04d}",
        "current_version": "v1",
        "metrics": metrics,
        "primary_metrics": metrics["60"],
        "multi_window_gate": {
            str(window): metrics[str(window)]["profit_gate"]
            for window in governance_module.WINDOWS
        },
        "funding_gate_hash": "c" * 64,
        "funding_evidence_revision_at": "2026-08-24T15:00:00",
        "profit_gate_passed": True,
        "market_route": {"router_decision_hash": "d" * 64},
        "paper_allocation_eligible": False,
        "member_details": [],
        "constraint_evaluation": {},
        "combination_recipe_ref": {
            "schema": "probiga.combination-member-fact-recipe.v1",
            "recipe_hash": "e" * 64,
            "detail_available": False,
        },
        "ranking_score": 80.0,
        "gate_reason": "组合只做事实链配方摘要",
        "recommended_status": "SHADOW",
    } for index in range(750)]

    class CaptureConnection:
        def __init__(self):
            self.calls = []

        def execute(self, statement, parameters):
            self.calls.append((str(statement), parameters))

    connection = CaptureConnection()
    governance_module._persist_combinations(connection, {
        "run_uid": "a" * 32,
        "trade_date": "2026-08-24",
        "combinations": combinations,
    })
    persisted_rows = [
        item for _statement, batch in connection.calls for item in batch
    ]
    assert len(connection.calls) == 8
    assert len(persisted_rows) == 750
    assert all(len(batch) <= 100 for _statement, batch in connection.calls)
    assert all(
        len(governance_module._json_text(batch).encode("utf-8"))
        <= 4 * 1024 * 1024
        for _statement, batch in connection.calls
    )
    assert all(
        "internal_daily_records" not in item["evidence_json"]
        and "internal_equity_curve" not in item["evidence_json"]
        and "internal_stock_exposure" not in item["evidence_json"]
        and '"combination_cash_fact_materialized":false'
        in item["evidence_json"]
        for item in persisted_rows
    )


def test_checkpoint_detail_rejects_unverified_hash_and_unbounded_page():
    start = date(2026, 1, 1)
    days = [
        (start + timedelta(days=index)).isoformat() for index in range(120)
    ]
    fact_members = [{
        "fact_id": f"{index + 1:064x}",
        "fact_hash": f"{index + 1001:064x}",
        "trade_date": day,
    } for index, day in enumerate(days)]
    state = {
        "schema": governance_module.FUNDING_CHECKPOINT_SCHEMA,
        "strategy_key": "dynamic_alpha",
        "strategy_version": "v1",
        "account_id": "paper-account",
        "trade_date": days[-1],
        "replay_mode": "BOUNDED_INCREMENTAL",
        "replay_session_count": 1,
        "max_holding_days": 20,
        "history_start_date": days[0],
        "history_end_date": days[-1],
        "history_fact_count": len(days),
        "history_fact_set_hash": (
            governance_module.ordered_funding_fact_set_hash(fact_members)
        ),
        "holdings": [],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    verified_facts = {
        "daily_records": [{"trade_date": day} for day in days],
        "equity_curve": [{"trade_date": day} for day in days],
        "daily_stock_market_values": [
            {"trade_date": day} for day in days
        ],
        "closed_evidence_by_day": [
            {"trade_date": day} for day in days
        ],
        "fact_members": fact_members,
    }
    forged = {
        "state": state,
        "checkpoint_id": "a" * 64,
        "checkpoint_hash": "b" * 64,
        "chain_hash": "c" * 64,
    }
    with pytest.raises(ValueError, match="哈希"):
        governance_module._funding_checkpoint_detail_page(
            forged,
            verified_facts,
            series="daily_records",
            window_days=20,
            cursor="",
            limit=20,
        )
    forged["checkpoint_hash"] = governance_module.checkpoint_state_hash(state)
    with pytest.raises(ValueError, match="1至50"):
        governance_module._funding_checkpoint_detail_page(
            forged,
            verified_facts,
            series="daily_records",
            window_days=20,
            cursor="",
            limit=51,
        )


def test_lifecycle_prefetch_query_count_is_constant_for_750_strategies(
    monkeypatch,
):
    inventory_size = 750
    queries = []

    def fake_read(sql, _params=None):
        queries.append(sql)
        if "FROM information_schema.tables" in sql:
            return [{"cnt": 1}]
        if "FROM st_strategy_lifecycle_event" in sql:
            return [{
                "entity_key": f"strategy_{index:04d}",
                "entity_version": "v1",
                "occurred_at": "2026-08-20T15:00:00",
                "evidence_json": {
                    "trade_date": "2026-08-20",
                    "funding_evidence_revision_at": "2026-08-20T15:00:00",
                },
                "event_hash": "a" * 64,
            } for index in range(inventory_size)]
        if "FROM st_strategy_health_snapshot" in sql:
            return [{
                "entity_key": f"strategy_{index:04d}",
                "entity_version": "v1",
                "trade_date": "2026-08-20",
                "profit_gate_passed": 1,
                "evidence_json": {
                    "overall_profit_gate_passed": True,
                    "funding_gate_hash": f"prior-{index:04d}",
                    "funding_evidence_revision_at": "2026-08-20T15:00:00",
                },
            } for index in range(inventory_size)]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    facts = governance_module._prefetch_lifecycle_facts(
        "STRATEGY", "2026-08-21", history_limit=2,
        expected_session_rows=[
            {"trade_date": "2026-08-20"},
            {"trade_date": "2026-08-19"},
        ],
    )

    assert len(facts["boundaries"]) == inventory_size
    assert len(facts["histories"]) == inventory_size
    assert governance_module._prior_consecutive_gate_passes(
        "strategy_0000", "v1", "2026-08-21", "current-hash",
        "2026-08-21T15:00:00", limit=2,
        history_rows=facts["histories"][("strategy_0000", "v1")],
        expected_session_rows=facts["expected_session_rows"],
    ) == 1
    # Two table-presence checks and two set queries, independent of inventory.
    assert len(queries) == 4


def test_stock_paper_plan_enforces_positions_turnover_and_cash(monkeypatch):
    monkeypatch.setattr(
        governance_module, "_previous_paper_plan_weights", lambda _day: {}
    )
    monkeypatch.setattr(
        governance_module,
        "_paper_plan_portfolio_risk",
        lambda *_args: {
            "valid": True,
            "observations": 60,
            "annualized_volatility_pct": 10.0,
            "expected_shortfall_95_pct": 1.0,
            "risk_multiplier": 1.0,
            "reason": "fixture",
        },
    )
    pools = {
        "tradable": [
            {
                "stock_code": f"{index:06d}",
                "stock_name": f"股票{index}",
                "industry_name": f"行业{index // 2}",
                "strategies": ["dynamic_strategy"],
                "dominant_strategy": "dynamic_strategy",
                "opportunity_score": 100 - index,
                "execution_score": 90,
                "entry_price_low": 9.9,
                "entry_price_high": 10.1,
                "risk_reward_ratio": 2.0,
                "stop_loss_price": 9.0,
                "take_profit_1": 11.0,
                "take_profit_2": 12.0,
                "evidence": {
                    "candidate_source_hash": governance_module._digest({
                        "candidate": index
                    })
                },
            }
            for index in range(30)
        ]
    }
    allocations = [{
        "target_type": "STRATEGY",
        "target_key": "dynamic_strategy",
        "target_version": "v1",
        "simulated_weight_pct": 85.0,
    }, {
        "target_type": "CASH",
        "target_key": "cash",
        "target_version": "",
        "simulated_weight_pct": 15.0,
    }]
    industry_snapshot = _bind_pool_rows_to_exact_industry(
        "2026-08-21", pools["tradable"]
    )

    plan = governance_module._build_allocation_backed_paper_plan(
        "2026-08-21", pools, allocations, [],
        industry_snapshot=industry_snapshot,
    )

    assert plan["target_count"] == 25
    assert plan["requested_new_buy_turnover_bp"] == 8500
    assert plan["actual_new_buy_turnover_bp"] <= 3000
    assert plan["invested_bp"] == plan["actual_new_buy_turnover_bp"]
    assert plan["cash_bp"] == 10_000 - plan["invested_bp"]
    assert all(row["target_bp"] <= 500 for row in plan["targets"])
    assert all(
        row["new_buy_delta_bp"]
        == row["target_bp"] - row["previous_target_bp"]
        for row in plan["targets"]
    )
    assert {row["stock_code"] for row in plan["targets"]} == {
        row["stock_code"] for row in pools["tradable"]
    }
    assert plan["plan_hash"] == governance_module._digest({
        key: value for key, value in plan.items() if key != "plan_hash"
    })


def test_stock_paper_plan_enforces_industry_cap(monkeypatch):
    monkeypatch.setattr(
        governance_module, "_previous_paper_plan_weights", lambda _day: {}
    )
    monkeypatch.setattr(
        governance_module,
        "_paper_plan_portfolio_risk",
        lambda *_args: {
            "valid": True,
            "observations": 60,
            "annualized_volatility_pct": 10.0,
            "expected_shortfall_95_pct": 1.0,
            "risk_multiplier": 1.0,
            "reason": "fixture",
        },
    )
    pools = {
        "tradable": [{
            "stock_code": f"{index:06d}",
            "stock_name": f"银行股{index}",
            "industry_name": "银行",
            "strategies": ["bank_strategy"],
            "dominant_strategy": "bank_strategy",
            "opportunity_score": 90,
            "execution_score": 90,
            "entry_price_low": 10.0,
            "entry_price_high": 10.0,
            "evidence": {"candidate_source_hash": "a" * 64},
        } for index in range(10)]
    }
    allocations = [{
        "target_type": "STRATEGY",
        "target_key": "bank_strategy",
        "target_version": "v1",
        "simulated_weight_pct": 85.0,
    }]
    industry_snapshot = _bind_pool_rows_to_exact_industry(
        "2026-08-21", pools["tradable"]
    )

    plan = governance_module._build_allocation_backed_paper_plan(
        "2026-08-21", pools, allocations, [],
        industry_snapshot=industry_snapshot,
    )

    assert sum(row["target_bp"] for row in plan["targets"]) == 2000
    assert plan["invested_bp"] == 2000
    assert plan["cash_bp"] == 8000


def test_new_buy_turnover_cap_never_blocks_full_exits(monkeypatch):
    previous = {f"{index:06d}": 500 for index in range(20)}
    monkeypatch.setattr(
        governance_module,
        "_previous_paper_plan_weights",
        lambda _day: previous,
    )

    plan = governance_module._build_allocation_backed_paper_plan(
        "2026-08-21",
        {"tradable": []},
        [{
            "target_type": "CASH",
            "target_key": "cash",
            "target_version": "",
            "simulated_weight_pct": 100.0,
        }],
        [],
        industry_snapshot=_bind_pool_rows_to_exact_industry(
            "2026-08-21", []
        ),
    )

    assert "maximum_daily_turnover_pct" not in plan["policy"]
    assert plan["policy"]["maximum_new_buy_turnover_pct"] == 30.0
    assert plan["actual_new_buy_turnover_bp"] == 0
    assert sum(
        row["previous_target_bp"] for row in plan["exit_targets"]
    ) == 10_000
    assert all(row["exit_always_allowed"] is True for row in plan["exit_targets"])
    assert plan["automatic_real_order_submission"] is False
    assert plan["real_order_authority"] is False


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
        "account_id": "paper-main-v2",
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
        "entry_fee_policy_binding_count": 1,
        "entry_fee_profile_version": _FUNDING_EXECUTION_POLICY[
            "fee_profile_version"
        ],
        "entry_fee_schedule_hash": "9" * 64,
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
        "exit_fee_policy_binding_count": 1 if matured else 0,
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


def _single_day_funding_ledger_fixture():
    trade_day = "2026-08-18"
    record = _ledger_record(
        evidence_id="fee-policy-proof",
        stock_code="000001",
        entry_day=trade_day,
        entry_price=10.0,
        quantity=100,
        exit_day=trade_day,
        exit_price=11.0,
    )
    facts = {
        "calendar_rows": [{"trade_date": trade_day}],
        "calendar_receipt": _test_calendar_binding([trade_day]),
        "price_rows": [
            _attested_bar("000001", trade_day, 11.0, 11.0),
        ],
        "account_rows": [_ledger_account_fact(10000)],
    }
    return record, facts


def test_authoritative_funding_policy_rejects_v3_v2_fee_drift(monkeypatch):
    drifted = _ledger_v3_config(10000)
    drifted["account"]["commission_rate"] = "0.00010"
    monkeypatch.setattr(
        governance_module, "load_v3_config", lambda: drifted,
    )

    with pytest.raises(RuntimeError, match="V3与V2冻结生产费用/滑点策略不一致"):
        governance_module._authoritative_funding_execution_policy()


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("buy_commission_rate", "0.00026"),
        ("sell_commission_rate", "0.00026"),
        ("minimum_commission", "6.00"),
        ("stamp_tax_sell_rate", "0.0006"),
        ("transfer_fee_buy_rate", "0.00002"),
        ("transfer_fee_sell_rate", "0.00002"),
        ("other_fees", {"buy_fixed": "0.01"}),
    ),
)
def test_authoritative_funding_policy_rejects_same_version_fee_content_drift(
    monkeypatch, field, value,
):
    profile = deepcopy(
        next(
            item for item in governance_module.PAPER_FEE_PROFILES
            if item["security_type"] == "A_SHARE"
        )
    )
    profile[field] = value
    monkeypatch.setattr(
        governance_module, "PAPER_FEE_PROFILES", (profile,),
    )

    with pytest.raises(RuntimeError, match="费用"):
        governance_module._authoritative_funding_execution_policy()


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("policy_hash", "0" * 64),
        ("fee_profile_version", "legacy-fee-profile"),
        ("instrument_rule_version", "legacy-instrument-rule"),
    ),
)
def test_internal_ledger_rejects_account_execution_policy_drift(
    monkeypatch, field, value,
):
    record, facts = _single_day_funding_ledger_fixture()
    facts["account_rows"][0][field] = value
    monkeypatch.setattr(
        governance_module,
        "load_v3_config",
        lambda: _ledger_v3_config(10000),
    )

    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record],
        as_of_date="2026-08-18",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
        prefetched_facts=facts,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
    )

    assert ledger["valid"] is False
    assert "政策哈希或纸面交易边界无效" in ledger["reason"]


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("entry_fee_policy_binding_count", 0),
        ("entry_fee_profile_version", "legacy-fee-profile"),
        ("entry_fee_schedule_hash", ""),
    ),
)
def test_internal_ledger_rejects_missing_or_drifted_entry_fee_proof(
    field, value,
):
    record, _facts = _single_day_funding_ledger_fixture()
    record[field] = value

    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record],
        as_of_date="2026-08-18",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
    )

    assert ledger["valid"] is False
    assert "冻结费用策略绑定" in ledger["reason"]


def test_internal_ledger_rejects_missing_exit_fee_policy_proof():
    record, _facts = _single_day_funding_ledger_fixture()
    record["exit_fee_policy_binding_count"] = 0

    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record],
        as_of_date="2026-08-18",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
    )

    assert ledger["valid"] is False
    assert "现金绑定或全局守恒" in ledger["reason"]


def test_incremental_ledger_rejects_checkpoint_account_fact_drift(
    monkeypatch,
):
    record, facts = _single_day_funding_ledger_fixture()
    monkeypatch.setattr(
        governance_module,
        "load_v3_config",
        lambda: _ledger_v3_config(10000),
    )
    bootstrap = governance_module._internal_strategy_portfolio_ledger(
        [record],
        as_of_date="2026-08-18",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
        prefetched_facts=facts,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
    )
    assert bootstrap["valid"] is True, bootstrap
    checkpoint_state = deepcopy(
        bootstrap["funding_checkpoint_candidate"]["state"]
    )
    checkpoint_state["account_fact_hash"] = "0" * 64
    incremental_facts = {
        "calendar_rows": [
            {"trade_date": "2026-08-18"},
            {"trade_date": "2026-08-19"},
        ],
        "calendar_receipt": _test_calendar_binding(
            ["2026-08-18", "2026-08-19"]
        ),
        "price_rows": [],
        "account_rows": [_ledger_account_fact(10000)],
    }

    ledger = governance_module._internal_strategy_portfolio_ledger(
        [],
        as_of_date="2026-08-19",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
        prefetched_facts=incremental_facts,
        replay_plan={
            "mode": "BOUNDED_INCREMENTAL",
            "state": checkpoint_state,
            "max_holding_days": 20,
        },
    )

    assert ledger["valid"] is False
    assert "检查点与当前权威账户事实不一致" in ledger["reason"]


def test_checkpoint_incremental_replay_equals_full_history_with_fees_and_carry(
    monkeypatch,
):
    records = [
        _ledger_record(
            evidence_id="prior-realized", stock_code="000001",
            entry_day="2026-08-18", entry_price=10.0, quantity=100,
            exit_day="2026-08-20", exit_price=12.0,
        ),
        _ledger_record(
            evidence_id="carry-holding", stock_code="000002",
            entry_day="2026-08-19", entry_price=20.0, quantity=100,
            exit_day="2026-08-21", exit_price=22.0,
        ),
        _ledger_record(
            evidence_id="same-day-roundtrip", stock_code="000003",
            entry_day="2026-08-21", entry_price=5.0, quantity=100,
            exit_day="2026-08-21", exit_price=5.5,
        ),
    ]
    for record in records:
        record["account_id"] = "paper-main-v2"
    sessions = [
        "2026-08-18", "2026-08-19", "2026-08-20", "2026-08-21",
    ]
    facts = {
        "calendar_rows": [{"trade_date": day} for day in sessions],
        "calendar_receipt": _test_calendar_binding(sessions),
        "price_rows": [
            _attested_bar("000001", "2026-08-18", 10, 10),
            _attested_bar("000001", "2026-08-19", 11, 10),
            _attested_bar("000001", "2026-08-20", 12, 11),
            _attested_bar("000002", "2026-08-19", 20, 20),
            _attested_bar("000002", "2026-08-20", 21, 20),
            _attested_bar("000002", "2026-08-21", 22, 21),
            _attested_bar("000003", "2026-08-21", 5.5, 5.5),
        ],
        "account_rows": [_ledger_account_fact(10000)],
    }
    monkeypatch.setattr(
        governance_module,
        "load_v3_config",
        lambda: _ledger_v3_config(10000),
    )
    bootstrap = governance_module._internal_strategy_portfolio_ledger(
        records[:2],
        as_of_date="2026-08-20",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
        prefetched_facts=facts,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
        candidate_run_uid="1" * 32,
    )
    assert bootstrap["valid"] is True, bootstrap
    candidate = bootstrap["funding_checkpoint_candidate"]
    candidate_members = [{
        "fact_id": item["fact_id"],
        "fact_hash": item["fact_hash"],
        "trade_date": item["fact"]["trade_date"],
    } for item in candidate["daily_fact_candidates"]]
    incremental_plan = {
        "mode": "BOUNDED_INCREMENTAL",
        "state": candidate["state"],
        "checkpoint_id": candidate["checkpoint_id"],
        "checkpoint_hash": candidate["checkpoint_hash"],
        "chain_hash": candidate["chain_hash"],
        "account_id": "paper-main-v2",
        "trade_date": "2026-08-20",
        "max_holding_days": 20,
        "rolling_history": {
            "opening_normalized_equity": str(
                bootstrap["history_opening_equity"]
            ),
            "opening_equity_date": bootstrap[
                "history_opening_equity_date"
            ],
            "equity_curve": bootstrap["equity_curve"],
            "daily_records": bootstrap["daily_records"],
            "daily_stock_market_values": bootstrap[
                "daily_stock_market_values"
            ],
            "closed_evidence_by_day": bootstrap["closed_evidence_by_day"],
            "fact_members": candidate_members,
        },
    }
    incremental = governance_module._internal_strategy_portfolio_ledger(
        records[1:],
        as_of_date="2026-08-21",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
        prefetched_facts=facts,
        replay_plan=incremental_plan,
        candidate_run_uid="2" * 32,
    )
    full = governance_module._internal_strategy_portfolio_ledger(
        records,
        as_of_date="2026-08-21",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
        prefetched_facts=facts,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
        candidate_run_uid="3" * 32,
    )
    assert incremental["valid"] is True, incremental
    assert full["valid"] is True, full
    for field in (
        "equity_curve", "daily_records", "daily_stock_market_values",
        "closed_evidence_by_day", "completed_trade_count",
        "open_position_count", "funding_evidence_revision_at",
    ):
        assert incremental[field] == full[field]
    for field in (
        "closing_cash_cny", "closing_equity_cny", "cumulative_fee_cny",
        "high_watermark_equity_cny", "history_opening_equity",
    ):
        assert Decimal(str(candidate["state"].get(field, "0"))) >= 0
        assert Decimal(str(
            incremental["funding_checkpoint_candidate"]["state"][field]
        )) == Decimal(str(full["funding_checkpoint_candidate"]["state"][field]))
    assert incremental["funding_checkpoint_candidate"]["state"][
        "opening_cash_cny"
    ] == candidate["state"]["closing_cash_cny"]
    assert incremental["funding_checkpoint_candidate"]["state"][
        "opening_equity_cny"
    ] == candidate["state"]["closing_equity_cny"]
    for window_start in ("2026-08-18", "2026-08-19", "2026-08-20"):
        left = governance_module._slice_internal_ledger(
            incremental, start_date=window_start, as_of_date="2026-08-21",
        )
        right = governance_module._slice_internal_ledger(
            full, start_date=window_start, as_of_date="2026-08-21",
        )
        for field in (
            "expectancy_pct", "profit_factor", "win_rate_pct",
            "max_drawdown_pct", "completed_trade_count",
        ):
            assert left.get(field) == right.get(field)


def test_checkpoint_incremental_preserves_non_100_opening_boundary_over_120_days(
    monkeypatch,
):
    sessions = []
    cursor = date(2025, 1, 2)
    while len(sessions) < 130:
        if cursor.weekday() < 5:
            sessions.append(cursor.isoformat())
        cursor += timedelta(days=1)
    record = _ledger_record(
        evidence_id="old-profit", stock_code="000001",
        entry_day=sessions[0], entry_price=10.0, quantity=100,
        exit_day=sessions[1], exit_price=12.0,
    )
    record["account_id"] = "paper-main-v2"
    facts = {
        "calendar_rows": [{"trade_date": day} for day in sessions],
        "calendar_receipt": _test_calendar_binding(sessions),
        "price_rows": [
            _attested_bar("000001", sessions[0], 10, 10),
            _attested_bar("000001", sessions[1], 12, 10),
        ],
        "account_rows": [{
            **_ledger_account_fact(10000),
            "created_at": "2025-01-01T00:00:00",
        }],
    }
    monkeypatch.setattr(
        governance_module,
        "load_v3_config",
        lambda: _ledger_v3_config(10000),
    )
    bootstrap = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date=sessions[-2], strategy_key="ledger_strategy",
        strategy_version="v1", version_hash="f" * 64,
        prefetched_facts=facts,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
        candidate_run_uid="4" * 32,
    )
    assert bootstrap["valid"] is True, bootstrap
    candidate = bootstrap["funding_checkpoint_candidate"]
    assert len(bootstrap["equity_curve"]) == 120
    assert Decimal(str(bootstrap["history_opening_equity"])) != Decimal("100")
    incremental_plan = {
        "mode": "BOUNDED_INCREMENTAL",
        "state": candidate["state"],
        "checkpoint_id": candidate["checkpoint_id"],
        "checkpoint_hash": candidate["checkpoint_hash"],
        "chain_hash": candidate["chain_hash"],
        "account_id": "paper-main-v2",
        "trade_date": sessions[-2],
        "max_holding_days": 20,
        "rolling_history": {
            "opening_normalized_equity": str(
                bootstrap["history_opening_equity"]
            ),
            "opening_equity_date": bootstrap[
                "history_opening_equity_date"
            ],
            "equity_curve": bootstrap["equity_curve"],
            "daily_records": bootstrap["daily_records"],
            "daily_stock_market_values": bootstrap[
                "daily_stock_market_values"
            ],
            "closed_evidence_by_day": bootstrap["closed_evidence_by_day"],
            "fact_members": [{
                "fact_id": item["fact_id"],
                "fact_hash": item["fact_hash"],
                "trade_date": item["fact"]["trade_date"],
            } for item in candidate["daily_fact_candidates"]],
        },
    }
    incremental = governance_module._internal_strategy_portfolio_ledger(
        [], as_of_date=sessions[-1], strategy_key="ledger_strategy",
        strategy_version="v1", version_hash="f" * 64,
        prefetched_facts=facts, replay_plan=incremental_plan,
        candidate_run_uid="5" * 32,
    )
    full = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date=sessions[-1], strategy_key="ledger_strategy",
        strategy_version="v1", version_hash="f" * 64,
        prefetched_facts=facts,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
        candidate_run_uid="6" * 32,
    )
    assert incremental["valid"] is True, incremental
    assert full["valid"] is True, full
    assert incremental["equity_curve"] == full["equity_curve"]
    assert incremental["daily_records"] == full["daily_records"]
    assert incremental["history_opening_equity"] == full[
        "history_opening_equity"
    ]
    assert incremental["history_opening_equity_date"] == full[
        "history_opening_equity_date"
    ]
    assert incremental["funding_evidence_revision_at"] == full[
        "funding_evidence_revision_at"
    ] == f"{sessions[1]}T15:00:00"
    start = incremental["equity_curve"][0]["trade_date"]
    sliced_incremental = governance_module._slice_internal_ledger(
        incremental, start_date=start, as_of_date=sessions[-1],
    )
    sliced_full = governance_module._slice_internal_ledger(
        full, start_date=start, as_of_date=sessions[-1],
    )
    assert sliced_incremental["opening_equity"] == sliced_full[
        "opening_equity"
    ] == incremental["history_opening_equity"]
    assert sliced_incremental["opening_equity_source"] == (
        "CHECKPOINT_ROLLING_HISTORY_OPENING_EQUITY"
    )


def test_same_day_first_revision_restarts_full_bootstrap_without_old_same_day_cp(
    monkeypatch,
):
    queries = []

    def fake_read(sql, _params=None):
        queries.append(sql)
        assert "latest.trade_date<:as_of_date" in sql
        return [{
            "registry_strategy_key": "same_day_first",
            "registry_current_version": "v1",
            "checkpoint_id": None,
            # A superseded checkpoint on the target day is intentionally not
            # counted: only strictly earlier days may seed the revision.
            "version_checkpoint_count": 0,
        }]

    monkeypatch.setattr(
        governance_module, "_strict_table_exists", lambda _name: True,
    )
    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module,
        "_load_funding_origin_context",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        governance_module,
        "_load_authoritative_checkpoint_sessions",
        lambda _selected, **_kwargs: {},
    )

    plans = governance_module._load_funding_replay_plans(
        "2026-08-21",
        [{
            "strategy_key": "same_day_first",
            "current_version": "v1",
            "parameters": {"max_holding_days": 20},
        }],
    )

    assert len(queries) == 1
    assert plans["same_day_first"]["mode"] == "FULL_BOOTSTRAP"
    assert plans["same_day_first"]["checkpoint_id"] == ""
    assert plans["same_day_first"]["checkpoint"] is None


def test_same_day_revision_fails_closed_when_older_checkpoint_is_unusable(
    monkeypatch,
):
    monkeypatch.setattr(
        governance_module, "_strict_table_exists", lambda _name: True,
    )
    monkeypatch.setattr(
        governance_module,
        "_db_read",
        lambda *_args, **_kwargs: [{
            "registry_strategy_key": "broken_parent",
            "registry_current_version": "v1",
            "checkpoint_id": None,
            "version_checkpoint_count": 1,
        }],
    )

    with pytest.raises(RuntimeError, match="已有当前版本检查点"):
        governance_module._load_funding_replay_plans(
            "2026-08-21",
            [{
                "strategy_key": "broken_parent",
                "current_version": "v1",
                "parameters": {"max_holding_days": 20},
            }],
        )


def test_same_day_revisions_both_seed_from_prior_canonical_checkpoint(
    monkeypatch,
):
    prior_checkpoint_id = "a" * 64
    main_queries = []

    def fake_read(sql, _params=None):
        if "st_strategy_registry current_strategy" in sql:
            main_queries.append(sql)
            assert "latest.trade_date<:as_of_date" in sql
            assert "latest.trade_date<=:as_of_date" not in sql
            return [{
                "registry_strategy_key": "same_day_incremental",
                "registry_current_version": "v1",
                "checkpoint_id": prior_checkpoint_id,
                "history_tip_fact_id": "b" * 64,
                "history_tip_fact_hash": "c" * 64,
                "history_fact_count": 1,
                "version_checkpoint_count": 1,
            }]
        if "WITH RECURSIVE requested AS" in sql:
            return []
        raise AssertionError(sql)

    monkeypatch.setattr(
        governance_module, "_strict_table_exists", lambda _name: True,
    )
    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module,
        "_load_funding_origin_context",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        governance_module,
        "_load_authoritative_checkpoint_sessions",
        lambda selected, **_kwargs: {
            str(row["checkpoint_id"]): {"HISTORY": [], "REPLAY": []}
            for row in selected.values()
        },
    )
    seen = []

    def verify(source, **_kwargs):
        seen.append(str(source.get("checkpoint_id") or ""))
        return {
            "mode": "BOUNDED_INCREMENTAL",
            "checkpoint_id": str(source["checkpoint_id"]),
        }

    monkeypatch.setattr(
        governance_module, "_verify_funding_checkpoint_row", verify,
    )
    registry = [{
        "strategy_key": "same_day_incremental",
        "current_version": "v1",
        "parameters": {"max_holding_days": 20},
    }]

    first_revision = governance_module._load_funding_replay_plans(
        "2026-08-21", registry,
    )
    second_revision = governance_module._load_funding_replay_plans(
        "2026-08-21", registry,
    )

    assert seen == [prior_checkpoint_id, prior_checkpoint_id]
    assert first_revision == second_revision
    plan = first_revision["same_day_incremental"]
    assert plan["mode"] == "BOUNDED_INCREMENTAL"
    assert plan["checkpoint_id"] == prior_checkpoint_id
    assert plan["runtime_serialized_bytes"] > 0
    assert len(main_queries) == 2


class _FundingReadbackResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _FundingReadbackConnection:
    def __init__(self, *, corrupt_checkpoint=False):
        self.corrupt_checkpoint = corrupt_checkpoint
        self.facts = {}
        self.checkpoints = {}
        self.statements = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        if "INSERT INTO st_strategy_funding_daily_fact" in sql:
            for source in params:
                row = dict(source)
                row.update({
                    "entity_type": "STRATEGY",
                    "automatic_real_order_submission": 0,
                    "real_order_authority": 0,
                })
                self.facts[row["fact_id"]] = row
            return _FundingReadbackResult([])
        if "INSERT INTO st_strategy_funding_checkpoint" in sql:
            for source in params:
                row = dict(source)
                row.update({
                    "automatic_real_order_submission": 0,
                    "real_order_authority": 0,
                })
                self.checkpoints[row["checkpoint_id"]] = row
            return _FundingReadbackResult([])
        if "FROM st_strategy_funding_daily_fact stored" in sql:
            ids = json.loads(params["ids"])
            return _FundingReadbackResult([
                dict(self.facts[row_id]) for row_id in ids
            ])
        if "FROM st_strategy_funding_checkpoint stored" in sql:
            ids = json.loads(params["ids"])
            rows = [dict(self.checkpoints[row_id]) for row_id in ids]
            if self.corrupt_checkpoint and rows:
                rows[0]["closing_cash_cny"] = str(
                    Decimal(str(rows[0]["closing_cash_cny"])) + Decimal("1")
                )
            return _FundingReadbackResult(rows)
        # The manifest audit INSERT has no read result.
        if "INSERT INTO st_strategy_governance_audit" in sql:
            return _FundingReadbackResult([])
        raise AssertionError(sql)


def test_checkpoint_persistence_atomically_reads_back_exact_rows_roots_and_bytes(
    monkeypatch,
):
    record = _ledger_record(
        evidence_id="atomic-readback", stock_code="000001",
        entry_day="2026-08-20", entry_price=10.0, quantity=100,
        exit_day="2026-08-21", exit_price=11.0,
        strategy_key="atomic_readback", strategy_version="v1",
    )
    record["account_id"] = "paper-main-v2"
    facts = {
        "calendar_rows": [
            {"trade_date": "2026-08-20"},
            {"trade_date": "2026-08-21"},
        ],
        "calendar_receipt": _test_calendar_binding([
            "2026-08-20", "2026-08-21",
        ]),
        "price_rows": [
            _attested_bar("000001", "2026-08-20", 10, 10),
            _attested_bar("000001", "2026-08-21", 11, 10),
        ],
        "account_rows": [_ledger_account_fact(10000)],
    }
    monkeypatch.setattr(
        governance_module,
        "load_v3_config",
        lambda: _ledger_v3_config(10000),
    )
    run_uid = "1" * 32
    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date="2026-08-21",
        strategy_key="atomic_readback", strategy_version="v1",
        version_hash="f" * 64, prefetched_facts=facts,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
        candidate_run_uid=run_uid,
    )
    assert ledger["valid"] is True, ledger
    strategy = {
        "strategy_key": "atomic_readback",
        "current_version": "v1",
        "funding_checkpoint_candidate": ledger[
            "funding_checkpoint_candidate"
        ],
        "funding_checkpoint_ref": ledger["funding_checkpoint_ref"],
    }
    manifest, candidates = governance_module._build_funding_checkpoint_manifest(
        run_uid=run_uid, trade_date="2026-08-21",
        strategies=[strategy], combinations=[],
    )
    payload = {
        "run_uid": run_uid,
        "trade_date": "2026-08-21",
        "operator": "pytest",
        "funding_checkpoint_manifest": manifest,
    }
    connection = _FundingReadbackConnection()

    result = governance_module._persist_funding_checkpoint_candidates(
        connection, payload=payload, canonical_result_hash="2" * 64,
        candidates=candidates,
    )

    assert result["checkpoint_count"] == 1
    assert result["daily_fact_count"] == 2
    assert result["checkpoint_storage_bytes"] == manifest[
        "checkpoint_storage_bytes"
    ]
    assert result["fact_storage_bytes"] == manifest["fact_storage_bytes"]
    assert result["checkpoint_root_hash"] == manifest[
        "checkpoint_root"
    ]["root_hash"]
    assert len(connection.checkpoints) == 1
    assert len(connection.facts) == 2
    assert any(
        "FROM st_strategy_funding_checkpoint stored" in sql
        for sql in connection.statements
    )
    assert any(
        "FROM st_strategy_funding_daily_fact stored" in sql
        for sql in connection.statements
    )

    with pytest.raises(RuntimeError, match="物化列与写入参数不一致"):
        governance_module._persist_funding_checkpoint_candidates(
            _FundingReadbackConnection(corrupt_checkpoint=True),
            payload=payload, canonical_result_hash="2" * 64,
            candidates=candidates,
        )


def test_exact_checkpoint_detail_loader_binds_version_ref_and_one_fact_batch(
    monkeypatch,
):
    key = "detail_source"
    version = "v1"
    evaluator = {"score_field": "score"}
    parameters = {"max_holding_days": 20}
    version_hash = governance_module._strategy_version_digest(
        strategy_key=key, version=version,
        evaluator_type="manifest_score_adapter",
        evaluator_config=evaluator, parameters=parameters,
        source_kind="immutable_manifest",
    )
    content_hash = governance_module._strategy_content_digest(
        strategy_key=key, evaluator_type="manifest_score_adapter",
        evaluator_config=evaluator, parameters=parameters,
        source_kind="immutable_manifest",
    )
    checkpoint_id = "3" * 64
    anchor_run_uid = "4" * 32
    row = {
        "checkpoint_id": checkpoint_id,
        "strategy_key": key,
        "strategy_version": version,
        "strategy_version_hash": version_hash,
        "execution_binding_hash": None,
        "account_id": "paper-main-v2",
        "trade_date": "2026-08-21",
        "replay_mode": "FULL_BOOTSTRAP",
        "replay_start_date": "2026-08-21",
        "replay_session_count": 1,
        "max_holding_days": 20,
        "checkpoint_hash": "5" * 64,
        "chain_hash": "6" * 64,
        "history_fact_count": 1,
        "history_fact_set_hash": "7" * 64,
        "history_tip_fact_id": "8" * 64,
        "history_tip_fact_hash": "9" * 64,
        "new_fact_count": 1,
        "new_fact_set_hash": "a" * 64,
        "new_fact_first_id": "8" * 64,
        "new_fact_tip_id": "8" * 64,
        "anchor_run_uid": anchor_run_uid,
        "automatic_real_order_submission": 0,
        "real_order_authority": 0,
        "latest_day_checkpoint_count": 1,
        "version_account_count": 1,
        "frozen_version_hash": version_hash,
        "frozen_content_hash": content_hash,
        "frozen_evaluator_type": "manifest_score_adapter",
        "frozen_evaluator_config_json": governance_module._json_text(
            evaluator
        ),
        "frozen_parameters_json": governance_module._json_text(parameters),
        "frozen_source_kind": "immutable_manifest",
    }
    ref = {
        "checkpoint_id": checkpoint_id,
        "strategy_key": key,
        "strategy_version": version,
        "account_id": "paper-main-v2",
        "trade_date": "2026-08-21",
        "checkpoint_hash": "5" * 64,
        "history_fact_set_hash": "7" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    queries = []

    def fake_read(sql, _params=None):
        queries.append(sql)
        if "frozen_version_hash" in sql:
            return [row]
        if "WITH RECURSIVE requested AS" in sql:
            return [{"checkpoint_id": checkpoint_id, "chain_depth": 1}]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module,
        "current_bound_sql_connection",
        lambda: object(),
    )
    monkeypatch.setattr(
        governance_module,
        "_load_funding_origin_context",
        lambda *_args, **_kwargs: {
            "runs": {anchor_run_uid: {"is_canonical": 1}},
        },
    )
    monkeypatch.setattr(
        governance_module,
        "_load_authoritative_checkpoint_sessions",
        lambda _rows, **_kwargs: {
            checkpoint_id: {"HISTORY": [], "REPLAY": []}
        },
    )

    def verify(source, *, registry_row, fact_rows, require_current_canonical,
               **_kwargs):
        assert source == row
        assert registry_row["version_hash"] == version_hash
        assert registry_row["current_version"] == version
        assert len(fact_rows) == 1
        assert require_current_canonical is True
        return {
            "checkpoint_id": checkpoint_id,
            "checkpoint_hash": row["checkpoint_hash"],
            "chain_hash": row["chain_hash"],
            "state": {"schema": governance_module.FUNDING_CHECKPOINT_SCHEMA},
            "rolling_history": {"daily_records": []},
        }

    monkeypatch.setattr(
        governance_module, "_verify_funding_checkpoint_row", verify,
    )

    source = governance_module.load_verified_funding_checkpoint_detail_source(
        ref,
    )

    assert source["schema"] == (
        governance_module.FUNDING_CHECKPOINT_DETAIL_SOURCE_SCHEMA
    )
    assert source["checkpoint_id"] == checkpoint_id
    assert source["anchor_run_uid"] == anchor_run_uid
    assert source["anchor_current_canonical"] is True
    assert source["automatic_real_order_submission"] is False
    assert source["real_order_authority"] is False
    assert len(queries) == 2


def test_combination_recipe_loader_recomputes_compact_root_and_batches_members(
    monkeypatch,
):
    run_uid = "b" * 32
    trade_day = "2026-08-21"
    members = [{
        "strategy_key": f"member_{index:02d}",
        "strategy_version": "v1",
        "weight": 0.02,
        "checkpoint_id": hashlib.sha256(
            f"member-checkpoint-{index}".encode("utf-8")
        ).hexdigest(),
        "account_id": "paper-main-v2",
        "checkpoint_hash": hashlib.sha256(
            f"member-state-{index}".encode("utf-8")
        ).hexdigest(),
        "chain_hash": hashlib.sha256(
            f"member-chain-{index}".encode("utf-8")
        ).hexdigest(),
        "history_fact_set_hash": hashlib.sha256(
            f"member-history-{index}".encode("utf-8")
        ).hexdigest(),
        "checkpoint_trade_date": trade_day,
    } for index in range(50)]
    risk_binding_payload = {
        "schema": "probiga.combination-drift-risk-binding.v2",
        "window_days": 60,
        "risk_path_hash": "d" * 64,
        "constraint_evaluation_hash": "e" * 64,
        "constraint_passed": True,
        "peak_member_weight": 0.02,
        "current_member_weight": 0.02,
        "peak_pairwise_stock_overlap_pct": 0.0,
        "current_pairwise_stock_overlap_pct": 0.0,
        "peak_industry_weight_pct": 2.0,
        "current_industry_weight_pct": 2.0,
        "industry_snapshot_path_hash": "f" * 64,
        "industry_trade_dates_hash": "1" * 64,
        "industry_stock_code_sets_hash": "2" * 64,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    risk_binding = {
        **risk_binding_payload,
        "binding_hash": governance_module._digest(risk_binding_payload),
    }
    recipe_payload = {
        "schema": "probiga.combination-member-fact-recipe.v1",
        "combination_key": "combo_detail",
        "combination_version": "v1",
        "trade_date": trade_day,
        "members": members,
        "risk_constraint_binding": risk_binding,
        "cash_fact_materialized": False,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    recipe_hash = governance_module._digest(recipe_payload)
    pre_recipe_hash = "c" * 64
    recipe_gate_hash = governance_module._digest({
        "schema": "probiga.combination-recipe-funding-gate.v1",
        "combination_key": "combo_detail",
        "combination_version": "v1",
        "trade_date": trade_day,
        "pre_recipe_funding_gate_hash": pre_recipe_hash,
        "recipe_hash": recipe_hash,
        "recipe_ready": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    })
    recipe_ref = {
        **recipe_payload,
        "recipe_hash": recipe_hash,
        "pre_recipe_funding_gate_hash": pre_recipe_hash,
        "recipe_gate_hash": recipe_gate_hash,
        "member_fact_sets_ready": True,
        "detail_available": False,
        "status": "MEMBER_FACTS_VERIFIED_RECONSTRUCTION_RESERVED",
        "reason": "fixture",
    }
    combination = {
        "combination_key": "combo_detail",
        "current_version": "v1",
        "current_status": "ACTIVE",
        "projected_status": "ACTIVE",
        "funding_recipe_ready": True,
        "paper_allocation_eligible": True,
        "pre_confirmation_funding_gate_hash": recipe_gate_hash,
        "statistical_family_decision": {
            "valid": True,
            "passed": True,
            "decision_hash": "8" * 64,
        },
        "confirmation_guard": {
            "valid": True,
            "passed": True,
            "compact_hash": "9" * 64,
        },
        "combination_recipe_ref": recipe_ref,
    }
    combination["funding_gate_hash"] = (
        governance_module._finalize_funding_gate_hash(
            combination, entity_type="COMBINATION",
        )
    )
    recipe_entry = governance_module._combination_recipe_manifest_entry(
        combination, trade_date=trade_day,
    )
    manifest_payload = {
        "schema": "probiga.strategy-funding-checkpoint-manifest.v2",
        "run_uid": run_uid,
        "trade_date": trade_day,
        "checkpoint_root": governance_module._funding_manifest_batch_root(
            [], kind="CHECKPOINT",
        ),
        "combination_recipe_root": (
            governance_module._funding_manifest_batch_root(
                [recipe_entry], kind="COMBINATION_RECIPE",
            )
        ),
        "ineligible_root": governance_module._funding_manifest_batch_root(
            [], kind="INELIGIBLE",
        ),
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    manifest = {
        **manifest_payload,
        "manifest_hash": governance_module._checkpoint_canonical_hash(
            manifest_payload
        ),
    }
    canonical = {
        "run_uid": run_uid,
        "trade_date": trade_day,
        "is_canonical": True,
        "decision_contract_version": (
            governance_module.STATISTICAL_DECISION_CONTRACT
        ),
        "combinations": [combination],
        "funding_checkpoint_manifest": manifest,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    result_json = governance_module._json_text(canonical)
    run_row = {
        "run_uid": run_uid,
        "trade_date": trade_day,
        "status": "COMPLETED",
        "is_canonical": 1,
        "result_json": result_json,
        "result_hash": hashlib.sha256(result_json.encode("utf-8")).hexdigest(),
    }
    monkeypatch.setattr(
        governance_module, "_db_read", lambda *_args, **_kwargs: [run_row],
    )
    batches = []

    def load_members(refs, *, require_current_canonical):
        batches.append(list(refs))
        assert require_current_canonical is True
        return {
            ref["checkpoint_id"]: {
                "anchor_run_uid": run_uid,
                "strategy_key": ref["strategy_key"],
                "strategy_version": ref["strategy_version"],
                "account_id": ref["account_id"],
                "trade_date": ref["trade_date"],
            }
            for ref in refs
        }

    monkeypatch.setattr(
        governance_module,
        "_load_verified_funding_checkpoint_sources",
        load_members,
    )

    source = governance_module._load_verified_combination_recipe_detail_source(
        run_uid=run_uid, recipe_ref=recipe_ref,
        require_current_canonical=True,
    )

    assert source["recipe_entry"] == recipe_entry
    assert source["member_count"] == 50
    assert len(batches) == 5
    assert all(len(batch) == 10 for batch in batches)
    assert source["cash_fact_materialized"] is False
    assert source["automatic_real_order_submission"] is False
    assert source["real_order_authority"] is False

    superseded_run = deepcopy(run_row)
    superseded_run["is_canonical"] = 0
    monkeypatch.setattr(
        governance_module, "_db_read",
        lambda *_args, **_kwargs: [superseded_run],
    )
    with pytest.raises(RuntimeError, match="治理运行锚无效"):
        governance_module._load_verified_combination_recipe_detail_source(
            run_uid=run_uid, recipe_ref=recipe_ref,
            require_current_canonical=True,
        )

    broken_run = deepcopy(run_row)
    broken_result = deepcopy(canonical)
    broken_result["funding_checkpoint_manifest"][
        "combination_recipe_root"
    ]["root_hash"] = "f" * 64
    broken_json = governance_module._json_text(broken_result)
    broken_run["result_json"] = broken_json
    broken_run["result_hash"] = hashlib.sha256(
        broken_json.encode("utf-8")
    ).hexdigest()
    monkeypatch.setattr(
        governance_module, "_db_read", lambda *_args, **_kwargs: [broken_run],
    )
    with pytest.raises(RuntimeError, match="canonical清单无效"):
        governance_module._load_verified_combination_recipe_detail_source(
            run_uid=run_uid, recipe_ref=recipe_ref,
            require_current_canonical=True,
        )


def test_origin_context_uses_compact_run_projection_and_batched_manifest_roots(
    monkeypatch,
):
    """120 origin runs stay fixed-size and never download canonical JSON."""

    checkpoint_rows = {}
    fact_rows = []
    for index in range(120):
        run_uid = f"{index + 1:032x}"
        audit_id = f"{index + 1001:032x}"
        checkpoint_id = hashlib.sha256(
            f"checkpoint-{index}".encode("utf-8")
        ).hexdigest()
        fact_id = hashlib.sha256(f"fact-{index}".encode("utf-8")).hexdigest()
        fact_hash = hashlib.sha256(
            f"fact-hash-{index}".encode("utf-8")
        ).hexdigest()
        trade_day = (date(2026, 1, 1) + timedelta(days=index)).isoformat()
        checkpoint_rows[checkpoint_id] = {
            "checkpoint_id": checkpoint_id,
            "strategy_key": "bounded_origin",
            "strategy_version": "v1",
            "strategy_version_hash": "1" * 64,
            "execution_binding_hash": "2" * 64,
            "account_id": "paper-main-v2",
            "trade_date": trade_day,
            "replay_mode": "BOUNDED_INCREMENTAL",
            "replay_session_count": 1,
            "max_holding_days": 20,
            "checkpoint_hash": hashlib.sha256(
                f"state-{index}".encode("utf-8")
            ).hexdigest(),
            "chain_hash": hashlib.sha256(
                f"chain-{index}".encode("utf-8")
            ).hexdigest(),
            "history_fact_count": 1,
            "history_fact_set_hash": hashlib.sha256(
                f"history-{index}".encode("utf-8")
            ).hexdigest(),
            "history_tip_fact_id": fact_id,
            "history_tip_fact_hash": fact_hash,
            "new_fact_count": 1,
            "new_fact_set_hash": hashlib.sha256(
                f"new-{index}".encode("utf-8")
            ).hexdigest(),
            "new_fact_first_id": fact_id,
            "new_fact_tip_id": fact_id,
            "previous_checkpoint_id": None,
            "previous_checkpoint_hash": None,
            "previous_chain_hash": None,
            "anchor_run_uid": run_uid,
            "canonical_result_hash": "3" * 64,
            "anchor_audit_id": audit_id,
            "anchor_audit_hash": "4" * 64,
            "automatic_real_order_submission": 0,
            "real_order_authority": 0,
        }
        fact_rows.append({
            "entity_key": "bounded_origin",
            "entity_version": "v1",
            "account_id": "paper-main-v2",
            "trade_date": trade_day,
            "fact_id": fact_id,
            "fact_hash": fact_hash,
            "origin_checkpoint_id": checkpoint_id,
            "anchor_run_uid": run_uid,
        })

    calls = []

    def fake_read(sql, params=None):
        params = params or {}
        calls.append((sql, dict(params)))
        if "origin_ids" in params:
            return [
                checkpoint_rows[item]
                for item in json.loads(params["origin_ids"])
            ]
        if "run_uids" in params:
            assert "SELECT run.*" not in sql
            assert "run.result_json AS" not in sql
            assert "SHA2(run.result_json,256)" in sql
            assert "JSON_EXTRACT(run.result_json" in sql
            return [{
                "run_uid": item,
                "status": "COMPLETED",
                "is_canonical": 1,
            } for item in json.loads(params["run_uids"])]
        if "bindings" in params:
            return [{
                "checkpoint_id": item["checkpoint_id"],
                "anchor_run_uid": item["run_uid"],
                "manifest_position": 0,
                "run_checkpoint_count": 1,
            } for item in json.loads(params["bindings"])]
        if "batch_requests" in params:
            requested = json.loads(params["batch_requests"])
            assert len(requested) <= 100
            by_run = {
                row["anchor_run_uid"]: row for row in checkpoint_rows.values()
            }
            return [{
                **by_run[item["run_uid"]],
                "batch_index": item["batch_index"],
                "expected_total": item["expected_total"],
                "manifest_position": 0,
                "run_checkpoint_count": 1,
            } for item in requested]
        if "audit_ids" in params:
            return [{"audit_id": item} for item in json.loads(
                params["audit_ids"]
            )]
        if "origin_bindings" in params:
            requested = json.loads(params["origin_bindings"])
            by_checkpoint = {
                row["origin_checkpoint_id"]: row for row in fact_rows
            }
            return [{
                **by_checkpoint[item["checkpoint_id"]],
                "checkpoint_id": item["checkpoint_id"],
                "chain_depth": 1,
                "fact_hash_valid": 1,
            } for item in requested]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    context = governance_module._load_funding_origin_context(fact_rows)

    assert len(context["checkpoints"]) == 120
    assert len(context["runs"]) == 120
    assert len(context["audits"]) == 120
    assert len(context["batch_members"]) == 120
    assert len(calls) == 11
    assert sum("batch_requests" in params for _sql, params in calls) == 2
    assert all(
        len(json.loads(params["batch_requests"])) <= 100
        for _sql, params in calls if "batch_requests" in params
    )


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
            return [_ledger_account_fact(10000)]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module,
        "load_v3_config",
        lambda: _ledger_v3_config(10000),
    )
    ledger = governance_module._internal_strategy_portfolio_ledger(
        records,
        as_of_date="2026-08-20",
        strategy_key="ledger_strategy",
        strategy_version="v1",
        version_hash="f" * 64,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
    )
    assert ledger["valid"] is True, ledger
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
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
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
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
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
        "closed_evidence_by_day": [{
            "trade_date": "2026-08-20",
            "evidence_ids": ["one-intent-eighty-fills"],
        }],
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
            return [_ledger_account_fact(10000)]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module, "load_v3_config",
        lambda: _ledger_v3_config(10000),
    )
    first = governance_module._internal_strategy_portfolio_ledger(
        records, as_of_date="2026-08-20", strategy_key="ledger_strategy",
        strategy_version="v1", version_hash="f" * 64,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
    )
    later = governance_module._internal_strategy_portfolio_ledger(
        records, as_of_date="2026-08-22", strategy_key="ledger_strategy",
        strategy_version="v1", version_hash="f" * 64,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
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
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
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
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
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
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
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
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
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
            return [_ledger_account_fact(10000)]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module, "load_v3_config",
        lambda: _ledger_v3_config(10000),
    )
    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date="2026-08-19",
        strategy_key="ledger_strategy", strategy_version="v1",
        version_hash="f" * 64,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
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
            return [_ledger_account_fact(10000)]
        raise AssertionError(sql)

    monkeypatch.setattr(governance_module, "_db_read", fake_read)
    monkeypatch.setattr(
        governance_module, "load_v3_config",
        lambda: _ledger_v3_config(10000),
    )
    ledger = governance_module._internal_strategy_portfolio_ledger(
        [record], as_of_date="2026-08-18",
        strategy_key="ledger_strategy", strategy_version="v1",
        version_hash="f" * 64,
        replay_plan={"mode": "FULL_BOOTSTRAP", "max_holding_days": 20},
    )
    assert ledger["valid"] is False
    assert "QMT权威收盘日线认证" in ledger["reason"]


def test_internal_economics_plus_external_selection_can_pass_funding(monkeypatch):
    start = date(2026, 1, 1)
    records = []
    for index in range(130):
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
            "completed_trades": 520,
            "coverage_days": 130,
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
                for index in range(130)
            ]
        if "FROM sm_stock_kline" in sql:
            return []
        if "FROM st_trade_account_v2" in sql:
            return [{
                **_ledger_account_fact(200000),
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
        "_load_funding_replay_plans",
        lambda _day, rows: {
            row["strategy_key"]: {
                "mode": "FULL_BOOTSTRAP", "max_holding_days": 20,
            }
            for row in rows
        },
    )
    monkeypatch.setattr(
        governance_module,
        "load_v3_config",
        lambda: _ledger_v3_config(200000),
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
        governance_module.CANONICAL_FUNDING_PROVENANCE
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
        "execution_binding_hash": "",
        "adapter_artifact_sha256": "",
        "cost_model_hash": "",
        "window_days": 60,
    })
    # External research values can attest selection but cannot replace money,
    # fees, drawdown or provenance from the internal ledger.
    assert metrics[60]["net_expectancy_pct"] != -99.0
    assert metrics[60]["max_drawdown_pct"] != 99.0


def test_strategy_funding_uses_capital_weighted_nav_not_equal_weighted_trades(
    monkeypatch,
):
    # The tiny trade wins 10%, while the 1,000-times-larger trade loses 1%.
    # Equal-weighted intent returns are positive, but the actual sleeve NAV is
    # negative and therefore must be the only economics used by the gate/rank.
    records = [
        _ledger_record(
            evidence_id="tiny-winner", stock_code="000001",
            entry_day="2026-08-20", entry_price=10.0, quantity=100,
            exit_day="2026-08-21", exit_price=11.0,
            strategy_key="capital_weighted", strategy_version="v1",
        ),
        _ledger_record(
            evidence_id="large-loser", stock_code="000002",
            entry_day="2026-08-20", entry_price=10.0, quantity=100000,
            exit_day="2026-08-21", exit_price=9.9,
            strategy_key="capital_weighted", strategy_version="v1",
        ),
    ]
    monkeypatch.setattr(
        governance_module, "_load_forward_records",
        lambda *_args, **_kwargs: {"capital_weighted": records},
    )
    monkeypatch.setattr(
        governance_module, "_load_metric_inputs",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        governance_module,
        "_load_funding_replay_plans",
        lambda _day, rows: {
            row["strategy_key"]: {
                "mode": "FULL_BOOTSTRAP", "max_holding_days": 20,
            }
            for row in rows
        },
    )
    monkeypatch.setattr(
        governance_module, "_prefetch_internal_strategy_ledger_facts",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        governance_module, "_internal_strategy_portfolio_ledger",
        lambda *_args, **_kwargs: {
            "valid": False,
            "reason": "fixture delegates window economics to sliced ledger",
        },
    )

    def sliced_ledger(*_args, session_window=None, **_kwargs):
        count = int((session_window or {}).get("session_count") or 0)
        end_day = date(2026, 8, 21)
        days = [
            (end_day - timedelta(days=count - index - 1)).isoformat()
            for index in range(count)
        ]
        daily = [{
            "trade_date": day,
            "return_pct": -0.05 if index == count - 1 else 0.0,
            "actual_cost_pct": 0.0,
            "is_net_return": True,
            "evidence_revision_at": f"{day}T15:00:00",
        } for index, day in enumerate(days)]
        return {
            "valid": True,
            "funding_provenance": governance_module.CANONICAL_FUNDING_PROVENANCE,
            "drawdown_basis": "internal_version_bound_portfolio_equity",
            "cost_basis": "actual_ledger_fees",
            "max_drawdown_pct": 0.05,
            "portfolio_coverage_days": count,
            "completed_trade_count": 2,
            "internal_ledger_hash": "a" * 64,
            "internal_ledger_schema": (
                "probiga.internal-strategy-portfolio-ledger.v3"
            ),
            "parent_internal_ledger_hash": "b" * 64,
            "daily_records": daily,
            "equity_curve": [
                {"trade_date": day, "equity": 99.95 if index == count - 1 else 100.0}
                for index, day in enumerate(days)
            ],
            "daily_stock_market_values": [{
                "trade_date": day,
                "stock_risk_exposure": {"000002": "10000"},
            } for day in days],
            "stock_exposure": {"000002": "10000"},
            "exposure_basis": (
                "TIME_WEIGHTED_DAILY_MAX_CLOSE_OR_TURNOVER_PROXY_V2"
            ),
            "open_position_count": 0,
            "funding_evidence_revision_at": "2026-08-21T15:00:00",
        }

    monkeypatch.setattr(
        governance_module, "_slice_internal_ledger", sliced_ledger,
    )
    windows = {
        window: {
            "start_date": "2026-08-01",
            "end_date": "2026-08-21",
            "session_count": window,
            "session_hash": "c" * 64,
        }
        for window in governance_module.WINDOWS
    }
    registry = [{
        "strategy_key": "capital_weighted",
        "current_version": "v1",
        "version_hash": "f" * 64,
        "market_route": {
            "market_match_score": 100.0,
            "router_decision_hash": "e" * 64,
        },
    }]

    metrics = governance_module._metrics_for_registry(
        {}, registry, "2026-08-21", authoritative_windows=windows,
    )["capital_weighted"][60]

    assert metrics["trade_episode_diagnostics"][
        "net_expectancy_pct"
    ] > 0
    assert metrics["net_expectancy_pct"] < 0
    assert metrics["profit_factor"] == 0.0
    assert metrics["cost_stress_expectancy_pct"] < 0
    assert metrics["source"] == "internal_strategy_virtual_nav"
    assert metrics["profit_gate"]["passed"] is False


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
        "funding_provenance": governance_module.CANONICAL_FUNDING_PROVENANCE,
        "internal_ledger_hash": "a" * 64,
        "internal_daily_records": left_daily,
        "internal_equity_curve": equity_curve(left_daily),
        "internal_daily_stock_market_values": [{
            "trade_date": row["trade_date"],
            "stock_risk_exposure": {"000001": "10000"},
        } for row in left_daily],
        "internal_stock_exposure": {"000001": "10000"},
        "internal_stock_exposure_basis": (
            "TIME_WEIGHTED_DAILY_MAX_CLOSE_OR_TURNOVER_PROXY_V2"
        ),
        "completed_trades": 100,
        "evidence_revision_at": "2026-03-01T15:00:00",
        "session_window_valid": True,
        "session_window_start": "2026-01-01",
        "session_window_end": "2026-03-01",
        "session_window_count": 60,
        "session_window_hash": "e" * 64,
    }
    right_metrics = {
        "funding_provenance": governance_module.CANONICAL_FUNDING_PROVENANCE,
        "internal_ledger_hash": "b" * 64,
        "internal_daily_records": right_daily,
        "internal_equity_curve": equity_curve(right_daily),
        "internal_daily_stock_market_values": [{
            "trade_date": row["trade_date"],
            "stock_risk_exposure": {"000002": "10000"},
        } for row in right_daily],
        "internal_stock_exposure": {"000002": "10000"},
        "internal_stock_exposure_basis": (
            "TIME_WEIGHTED_DAILY_MAX_CLOSE_OR_TURNOVER_PROXY_V2"
        ),
        "completed_trades": 100,
        "evidence_revision_at": "2026-03-01T15:00:00",
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
        combination, members, window=60, trade_date="2026-03-01"
    )
    assert ledger["valid"] is True, ledger
    assert ledger["internal_ledger_schema"] == (
        "probiga.internal-combination-portfolio-ledger.v3"
    )
    assert ledger["allocation_semantics"] == (
        "WINDOW_OPEN_REBASED_FIXED_SLEEVES_NATURAL_WEIGHT_DRIFT_V3"
    )
    assert len(ledger["equity_curve"]) == 60
    assert ledger["daily_records"][0]["return_pct"] == pytest.approx(1.0)
    assert ledger["equity_curve"][0]["equity"] == pytest.approx(101.0)
    snapshot_id = "d" * 64
    industry_rows = []
    for code, name, source_id in (
        ("000001", "银行", "1" * 64), ("000002", "电子", "2" * 64),
    ):
        row_payload = {
            "snapshot_id": snapshot_id,
            "trade_date": "2026-03-01",
            "as_of_exclusive": "2026-03-02T00:00:00",
            "stock_code": code,
            "industry_name": name,
            "industry_type": "L1",
            "source_system": "test",
            "source_fact_id": f"qmt:{'a' * 64}:{source_id}",
            "source_effective_at": "2026-03-01T15:05:00",
            "source_etl_sync_at": "2026-03-01T15:05:00",
        }
        industry_rows.append({
            **row_payload, "row_hash": governance_module._digest(row_payload),
        })
    industry_payload = {
        "schema": "probiga.governance-industry-snapshot.v2",
        "snapshot_id": snapshot_id,
        "trade_date": "2026-03-01",
        "as_of_exclusive": "2026-03-02T00:00:00",
        "status": "COMPLETED",
        "requested_stock_codes": ["000001", "000002"],
        "rows": industry_rows,
        "reason": "行业快照已按治理交易日冻结",
    }
    industry_snapshot = {
        **industry_payload,
        "snapshot_hash": governance_module._digest(industry_payload),
    }
    industry_snapshot_path = _pit_industry_path(
        ledger,
        {
            row["trade_date"]: {
                "000001": "银行", "000002": "电子",
            }
            for row in ledger["daily_risk_exposures"]
        },
    )
    evaluation = governance_module._combination_constraint_evaluation(
        combination, members, trade_date="2026-03-01",
        industry_snapshot=industry_snapshot,
        industry_snapshot_path=industry_snapshot_path,
        internal_combo_ledger=ledger,
    )
    assert evaluation["drift_risk_path"]["risk_path_valid"] is True, (
        evaluation["drift_risk_path"]
    )
    assert evaluation["passed"] is True, evaluation
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
    concentrated[1]["strategy"]["metrics"]["60"][
        "internal_daily_stock_market_values"
    ] = [{
        "trade_date": row["trade_date"],
        "stock_risk_exposure": {"000001": "10000"},
    } for row in left_metrics["internal_daily_records"]]
    rejected = governance_module._combination_constraint_evaluation(
        combination, concentrated, trade_date="2026-03-01",
        industry_snapshot=industry_snapshot,
    )
    assert rejected["passed"] is False
    assert rejected["pairwise_correlations"][0]["passed"] is False
    assert rejected["pairwise_stock_overlaps"][0]["passed"] is False


def test_combination_constraints_use_drifted_60_day_peak_and_require_daily_exposure():
    start = date(2026, 1, 1)
    days = [
        (start + timedelta(days=index)).isoformat()
        for index in range(60)
    ]
    target_day = days[-1]

    def metrics(key, code, pattern, ledger_hash):
        daily = [{
            "trade_date": day,
            "return_pct": pattern[index % len(pattern)],
            "actual_cost_pct": 0.0,
            "is_net_return": True,
            "evidence_revision_at": f"{day}T15:00:00",
        } for index, day in enumerate(days)]
        equity = Decimal("100")
        curve = []
        for row in daily:
            equity *= Decimal("1") + Decimal(
                str(row["return_pct"])
            ) / Decimal("100")
            curve.append({
                "trade_date": row["trade_date"],
                "equity": float(equity),
            })
        return {
            "funding_provenance": (
                governance_module.CANONICAL_FUNDING_PROVENANCE
            ),
            "internal_ledger_hash": ledger_hash,
            "internal_daily_records": daily,
            "internal_equity_curve": curve,
            "internal_daily_stock_market_values": [{
                "trade_date": day,
                "stock_risk_exposure": {code: "10000"},
            } for day in days],
            "internal_stock_exposure": {code: "10000"},
            "internal_stock_exposure_basis": (
                "TIME_WEIGHTED_DAILY_MAX_CLOSE_OR_TURNOVER_PROXY_V2"
            ),
            "completed_trades": 100,
            "evidence_revision_at": f"{target_day}T15:00:00",
            "session_window_valid": True,
            "session_window_start": days[0],
            "session_window_end": target_day,
            "session_window_count": 60,
            "session_window_hash": "e" * 64,
        }

    members = [{
        "weight": 0.5,
        "frozen_version": "v1",
        "version_match": True,
        "strategy": {
            "strategy_key": "drift_winner",
            "current_version": "v1",
            "metrics": {"60": metrics(
                "drift_winner", "000001", [5.0, 4.9], "a" * 64,
            )},
        },
    }, {
        "weight": 0.5,
        "frozen_version": "v1",
        "version_match": True,
        "strategy": {
            "strategy_key": "drift_loser",
            "current_version": "v1",
            "metrics": {"60": metrics(
                "drift_loser", "000002", [-5.0, -4.9], "b" * 64,
            )},
        },
    }]
    combination = {
        "combination_key": "drift_peak_combo",
        "current_version": "v1",
        "config_hash": "c" * 64,
        "constraints": governance_module._validated_combination_constraints({
            "maximum_member_weight": 0.60,
            "maximum_pairwise_correlation": 0.80,
            "minimum_pairwise_observations": 60,
            "maximum_stock_overlap_pct": 0.0,
            "maximum_industry_weight_pct": 100.0,
        }),
    }
    snapshot_id = "d" * 64
    industry_rows = []
    for code, name, source_id in (
        ("000001", "银行", "1" * 64),
        ("000002", "电子", "2" * 64),
    ):
        row_payload = {
            "snapshot_id": snapshot_id,
            "trade_date": target_day,
            "as_of_exclusive": (
                date.fromisoformat(target_day) + timedelta(days=1)
            ).isoformat() + "T00:00:00",
            "stock_code": code,
            "industry_name": name,
            "industry_type": "L1",
            "source_system": "test",
            "source_fact_id": f"qmt:{'a' * 64}:{source_id}",
            "source_effective_at": f"{target_day}T15:05:00",
            "source_etl_sync_at": f"{target_day}T15:05:00",
        }
        industry_rows.append({
            **row_payload,
            "row_hash": governance_module._digest(row_payload),
        })
    industry_payload = {
        "schema": "probiga.governance-industry-snapshot.v2",
        "snapshot_id": snapshot_id,
        "trade_date": target_day,
        "as_of_exclusive": (
            date.fromisoformat(target_day) + timedelta(days=1)
        ).isoformat() + "T00:00:00",
        "status": "COMPLETED",
        "requested_stock_codes": ["000001", "000002"],
        "rows": industry_rows,
        "reason": "行业快照已按治理交易日冻结",
    }
    industry_snapshot = {
        **industry_payload,
        "snapshot_hash": governance_module._digest(industry_payload),
    }

    ledger = governance_module._internal_combination_portfolio_ledger(
        combination, members, window=60, trade_date=target_day,
    )
    assert ledger["valid"] is True, ledger
    evaluation = governance_module._combination_constraint_evaluation(
        combination, members, trade_date=target_day,
        industry_snapshot=industry_snapshot,
        internal_combo_ledger=ledger,
    )
    member_check = next(
        check for check in evaluation["checks"]
        if check["name"] == "最大成员权重"
    )
    assert member_check["passed"] is False
    assert member_check["peak_weight"] > 0.99
    assert member_check["current_weight"] > 0.99
    assert member_check["actual"] != 0.5
    assert evaluation["pairwise_correlations"][0]["passed"] is True
    assert evaluation["pairwise_stock_overlaps"][0]["passed"] is True
    assert evaluation["drift_risk_path"]["risk_path_valid"] is True
    assert evaluation["risk_binding"]["risk_path_hash"] == (
        ledger["risk_path_hash"]
    )
    assert evaluation["risk_binding"]["real_order_authority"] is False
    assert evaluation["passed"] is False

    missing_exposure_members = deepcopy(members)
    del missing_exposure_members[0]["strategy"]["metrics"]["60"][
        "internal_daily_stock_market_values"
    ]
    missing_ledger = governance_module._internal_combination_portfolio_ledger(
        combination, missing_exposure_members,
        window=60, trade_date=target_day,
    )
    assert missing_ledger["valid"] is False
    assert "逐日真实持仓市值敞口" in missing_ledger["reason"]
    missing_evaluation = governance_module._combination_constraint_evaluation(
        combination, missing_exposure_members, trade_date=target_day,
        industry_snapshot=industry_snapshot,
        internal_combo_ledger=missing_ledger,
    )
    assert missing_evaluation["passed"] is False
    assert missing_evaluation["drift_risk_path"]["risk_path_valid"] is False


def test_combination_industry_concentration_uses_each_historical_pit_date():
    start = date(2026, 1, 1)
    days = [
        (start + timedelta(days=index)).isoformat()
        for index in range(60)
    ]
    member_keys = ["industry_left", "industry_right"]
    daily_weights = [{
        "trade_date": day,
        "member_weights": {
            member_keys[0]: "0.500000000000",
            member_keys[1]: "0.500000000000",
        },
    } for day in days]
    daily_exposures = [{
        "trade_date": day,
        "member_stock_weights": {
            member_keys[0]: {"000001": "1.000000000000"},
            member_keys[1]: {"000002": "1.000000000000"},
        },
        "combined_stock_weights": {
            "000001": "0.500000000000",
            "000002": "0.500000000000",
        },
    } for day in days]
    combination = {
        "combination_key": "historical_industry_migration",
        "current_version": "v1",
        "constraints": governance_module._validated_combination_constraints({
            "maximum_member_weight": 0.75,
            "maximum_pairwise_correlation": 0.80,
            "minimum_pairwise_observations": 60,
            "maximum_stock_overlap_pct": 0.0,
            "maximum_industry_weight_pct": 60.0,
        }),
    }
    risk_payload = {
        "schema": "probiga.combination-drift-risk-path.v1",
        "combination_key": combination["combination_key"],
        "combination_version": combination["current_version"],
        "window_days": 60,
        "trade_date": days[-1],
        "daily_member_weights": daily_weights,
        "daily_risk_exposures": daily_exposures,
    }
    ledger = {
        "valid": True,
        "reason": "unit-test exact 60-session drift path",
        "internal_ledger_schema": (
            "probiga.internal-combination-portfolio-ledger.v3"
        ),
        "combination_key": combination["combination_key"],
        "combination_version": combination["current_version"],
        "window_days": 60,
        "trade_date": days[-1],
        "daily_member_weights": daily_weights,
        "daily_risk_exposures": daily_exposures,
        "risk_path_hash": governance_module._digest(risk_payload),
    }
    left_returns = [1.0, -1.0, 1.0, -1.0]
    right_returns = [1.0, 1.0, -1.0, -1.0]
    members = []
    for key, pattern in zip(
        member_keys, (left_returns, right_returns), strict=True,
    ):
        members.append({
            "weight": 0.5,
            "strategy": {
                "strategy_key": key,
                "metrics": {"60": {"internal_daily_records": [{
                    "trade_date": day,
                    "return_pct": pattern[index % len(pattern)],
                } for index, day in enumerate(days)]}},
            },
        })

    migration_day = days[17]
    industry_by_day = {
        day: {"000001": "银行", "000002": "电子"}
        for day in days
    }
    # On one historical day both stocks belonged to the same exact-date
    # industry.  A target-day/current mapping would incorrectly report 50%.
    industry_by_day[migration_day] = {
        "000001": "银行", "000002": "银行",
    }
    path = _pit_industry_path(ledger, industry_by_day)
    evaluation = governance_module._combination_constraint_evaluation(
        combination, members, trade_date=days[-1],
        industry_snapshot_path=path,
        internal_combo_ledger=ledger,
    )
    industry_check = next(
        item for item in evaluation["checks"]
        if item["name"] == "单一行业集中度"
    )
    assert industry_check["industry_snapshot_path_valid"] is True
    assert industry_check["peak_pct"] == 100.0
    assert industry_check["peak_trade_date"] == migration_day
    assert industry_check["current_pct"] == 50.0
    assert industry_check["passed"] is False
    compact = evaluation["industry_snapshot_path"]
    assert compact["status"] == "COMPLETED"
    assert compact["trade_dates"] == days
    assert len(compact["snapshot_hashes"]) == 60
    assert compact["path_hash"] == path["path_hash"]
    assert evaluation["risk_binding"]["schema"] == (
        "probiga.combination-drift-risk-binding.v2"
    )
    assert evaluation["risk_binding"]["industry_snapshot_path_hash"] == (
        path["path_hash"]
    )

    incomplete = deepcopy(path)
    incomplete["trade_dates"] = incomplete["trade_dates"][:-1]
    incomplete["snapshots"] = incomplete["snapshots"][:-1]
    incomplete_payload = {
        key: value for key, value in incomplete.items()
        if key != "path_hash"
    }
    incomplete["path_hash"] = governance_module._digest(incomplete_payload)
    blocked = governance_module._combination_constraint_evaluation(
        combination, members, trade_date=days[-1],
        industry_snapshot_path=incomplete,
        internal_combo_ledger=ledger,
    )
    blocked_check = next(
        item for item in blocked["checks"]
        if item["name"] == "单一行业集中度"
    )
    assert blocked_check["industry_snapshot_path_valid"] is False
    assert blocked_check["passed"] is False
    assert blocked["passed"] is False


def test_global_portfolio_uses_complete_daily_overlap_and_capacity_peaks():
    start = date(2026, 1, 1)
    days = [
        (start + timedelta(days=index)).isoformat()
        for index in range(60)
    ]
    overlap_day = days[37]
    capacity_day = days[21]

    def entity(key, pattern, *, side):
        exposure_rows = []
        for day in days:
            code = "000003" if day == overlap_day else (
                "000001" if side == "left" else "000002"
            )
            value = "50000" if (
                side == "left" and day == capacity_day
            ) else "30000" if (
                side == "right" and day == capacity_day
            ) else "10000"
            exposure_rows.append({
                "trade_date": day,
                "stock_risk_exposure": {code: value},
            })
        row = {
            "target_type": "STRATEGY",
            "target_key": key,
            "metrics": {"60": {
                "internal_daily_records": [{
                    "trade_date": day,
                    "return_pct": pattern[index % len(pattern)],
                } for index, day in enumerate(days)],
                "internal_daily_stock_market_values": exposure_rows,
            }},
        }
        row["portfolio_risk_evidence"] = (
            governance_module._portfolio_entity_risk_evidence(row)
        )
        return row

    left = entity("left", [1.0, -1.0, 1.0, -1.0], side="left")
    right = entity("right", [1.0, 1.0, -1.0, -1.0], side="right")
    pair = governance_module._portfolio_pair_check(left, right)
    assert pair["observations"] == 60
    assert pair["current_stock_overlap_pct"] == 0.0
    assert pair["peak_stock_overlap_pct"] == 100.0
    assert pair["peak_stock_overlap_trade_date"] == overlap_day
    assert pair["current_combined_gross_exposure_value"] == 20000.0
    assert pair["peak_combined_gross_exposure_value"] == 80000.0
    assert pair["peak_capacity_trade_date"] == capacity_day
    assert pair["capacity_path_valid"] is True
    assert re.fullmatch(r"[0-9a-f]{64}", pair["daily_exposure_path_hash"])
    assert pair["passed"] is False

    left["exposure_keys"] = ["left"]
    right["exposure_keys"] = ["right"]
    accepted, rejected, gate_hash = (
        governance_module._select_global_portfolio_candidates(
            [[left], [right]],
        )
    )
    assert [row["target_key"] for row in accepted] == ["left"]
    assert [row["target_key"] for row in rejected] == ["right"]
    first_gate = accepted[0]["global_portfolio_gate"]
    assert first_gate["daily_exposure_path_hash"] == left[
        "portfolio_risk_evidence"
    ]["exposure_path_hash"]
    assert first_gate["current_gross_exposure_value"] == "10000.00000000"
    assert first_gate["peak_60d_gross_exposure_value"] == "50000.00000000"
    assert rejected[0]["comparisons"][0]["peak_stock_overlap_pct"] == 100.0
    assert re.fullmatch(r"[0-9a-f]{64}", gate_hash)

    tampered = deepcopy(left["portfolio_risk_evidence"])
    tampered["daily_stock_exposures"] = tampered[
        "daily_stock_exposures"
    ][:-1]
    tampered_payload = {
        key: value for key, value in tampered.items()
        if key != "evidence_hash"
    }
    tampered["evidence_hash"] = governance_module._digest(tampered_payload)
    assert governance_module._validated_portfolio_risk_evidence(
        tampered
    ) is None


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


def _valid_challenger_submission_audit(
    *, artifact_hash="a" * 64, source_dataset_hash="b" * 64,
):
    evidence = {
        "schema": "probiga.strategy-challenger-evidence-submission.v1",
        "challenger_id": "c" * 32,
        "proposal_hash": "d" * 64,
        "proposed_version_hash": "e" * 64,
        "proposal_submitted_at": "2026-08-20T10:00:00",
        "submitted_by": "user-id:challenger-submitter",
        "as_of_date": "2026-08-21",
        "window_days": 120,
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "evidence_revision_at": "2026-08-21T15:00:00",
        "metrics": {"net_expectancy_pct": 1.0},
        "artifact_manifest": {
            "source_dataset_hash": source_dataset_hash,
        },
        "artifact_hash": artifact_hash,
        "source_dataset_hash": source_dataset_hash,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "server_replay_validation_hash": "f" * 64,
    }
    evidence["evidence_submission_hash"] = governance_module._digest(evidence)
    before = {"challenger_id": "c" * 32, "status": "VALIDATING"}
    after = {
        "challenger_id": "c" * 32,
        "status": "REVIEW_PENDING",
        "evidence_submission_hash": evidence["evidence_submission_hash"],
        "artifact_hash": artifact_hash,
        "source_dataset_hash": source_dataset_hash,
    }
    _sql, params = governance_module._audit_record(
        entity_type="STRATEGY",
        entity_key="challenger_strategy",
        action="SUBMIT_CHALLENGER_EVIDENCE",
        reason="提交挑战者证据",
        operator=evidence["submitted_by"],
        before=before,
        after=after,
        evidence=evidence,
    )
    return _stored_metric_audit(params, "2026-08-21T15:01:00")


@pytest.mark.parametrize("duplicate_namespace", ("artifact", "dataset"))
def test_ordinary_metric_rejects_hash_verified_challenger_claim(
    duplicate_namespace,
):
    class Result:
        def __init__(self, rows=()):
            self.rows = [dict(row) for row in rows]

        def mappings(self):
            return self

        def first(self):
            return self.rows[0] if self.rows else None

        def all(self):
            return list(self.rows)

    challenger = _valid_challenger_submission_audit()

    class Connection:
        def execute(self, statement, _params=None):
            sql = str(statement)
            if "FROM st_strategy_metric_input" in sql:
                return Result()
            if "action='SUBMIT_CHALLENGER_EVIDENCE'" in sql:
                assert "FOR UPDATE" in sql
                return Result([challenger])
            raise AssertionError(f"unexpected evidence claim SQL: {sql}")

    incoming = {
        "artifact_hash": (
            "a" * 64 if duplicate_namespace == "artifact" else "1" * 64
        ),
        "source_dataset_hash": (
            "b" * 64 if duplicate_namespace == "dataset" else "2" * 64
        ),
    }
    with pytest.raises(ValueError, match="挑战者证据占用"):
        governance_module._assert_global_metric_evidence_unclaimed(
            Connection(), incoming,
        )


def test_ordinary_metric_rejects_tampered_challenger_claim_history():
    class Result:
        def __init__(self, rows=()):
            self.rows = [dict(row) for row in rows]

        def mappings(self):
            return self

        def first(self):
            return self.rows[0] if self.rows else None

        def all(self):
            return list(self.rows)

    challenger = _valid_challenger_submission_audit()
    challenger["evidence_json"] = json.loads(challenger["evidence_json"])
    challenger["evidence_json"]["artifact_hash"] = "9" * 64

    class Connection:
        def execute(self, statement, _params=None):
            if "FROM st_strategy_metric_input" in str(statement):
                return Result()
            return Result([challenger])

    with pytest.raises(RuntimeError, match="审计"):
        governance_module._assert_global_metric_evidence_unclaimed(
            Connection(), {
                "artifact_hash": "1" * 64,
                "source_dataset_hash": "2" * 64,
            },
        )


def test_metric_artifact_global_owner_has_additive_database_constraints():
    source = "\n".join(
        governance_module.governance_table_ddl_statements()
    ) + inspect.getsource(governance_module.ensure_strategy_governance_tables)
    assert (
        "UNIQUE KEY uk_strategy_metric_artifact_global (artifact_hash)"
        in source
    )
    assert (
        "UNIQUE KEY uk_strategy_metric_dataset_global (source_dataset_hash)"
        in source
    )
    assert "GROUP BY {hash_column} HAVING COUNT(*)>1" in source


def test_governance_schema_contract_covers_every_frozen_table_column_and_index():
    contract = governance_module._governance_table_schema_contract()

    assert set(contract) == set(governance_module.GOVERNANCE_TABLE_NAMES)
    assert len(contract) == 15
    assert sum(len(item["columns"]) for item in contract.values()) == 222
    assert sum(len(item["indexes"]) for item in contract.values()) == 45
    assert all(item["columns"] for item in contract.values())
    assert all("PRIMARY" in item["indexes"] for item in contract.values())
    for item in contract.values():
        primary_columns = set(item["indexes"]["PRIMARY"][1])
        assert all(
            column["nullable"] == "NO"
            for column in item["columns"]
            if column["name"] in primary_columns
        )
    metric_columns = {
        column["name"]: column
        for column in contract["st_strategy_metric_input"]["columns"]
    }
    assert metric_columns["reviewed_at"]["nullable"] == "YES"


class _GovernanceSchemaResult:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


class _GovernanceSchemaConnection:
    def __init__(self, *, table_collation=None, column_collation=None):
        self.contracts = governance_module._governance_table_schema_contract()
        self.table_collation = (
            table_collation or governance_module.GOVERNANCE_SCHEMA_COLLATION
        )
        self.column_collation = (
            column_collation or governance_module.GOVERNANCE_SCHEMA_COLLATION
        )

    def execute(self, statement, _params=None):
        sql = str(statement)
        if "information_schema.TABLES" in sql:
            return _GovernanceSchemaResult([
                {
                    "table_name": table_name,
                    "engine": "InnoDB",
                    "table_collation": self.table_collation,
                }
                for table_name in sorted(self.contracts)
            ])
        if "information_schema.COLUMNS" in sql:
            rows = []
            for table_name in sorted(self.contracts):
                for ordinal, expected in enumerate(
                    self.contracts[table_name]["columns"], 1,
                ):
                    base_type = expected["column_type"].split("(", 1)[0]
                    character_type = base_type in {
                        "char", "varchar", "text", "mediumtext", "longtext",
                    }
                    extra = []
                    if expected["auto_increment"]:
                        extra.append("auto_increment")
                    if expected["on_update_current_timestamp"]:
                        extra.append("on update CURRENT_TIMESTAMP")
                    rows.append({
                        "table_name": table_name,
                        "column_name": expected["name"],
                        "ordinal_position": ordinal,
                        "column_type": expected["column_type"],
                        "is_nullable": expected["nullable"],
                        "column_default": expected["default"],
                        "extra": " ".join(extra),
                        "character_set_name": (
                            "utf8mb4" if character_type else None
                        ),
                        "collation_name": (
                            self.column_collation if character_type else None
                        ),
                    })
            return _GovernanceSchemaResult(rows)
        if "information_schema.STATISTICS" in sql:
            return _GovernanceSchemaResult([
                {
                    "table_name": table_name,
                    "index_name": index_name,
                    "non_unique": non_unique,
                    "seq_in_index": sequence,
                    "column_name": column_name,
                    "sub_part": None,
                    "index_type": "BTREE",
                }
                for table_name in sorted(self.contracts)
                for index_name, (non_unique, columns) in (
                    self.contracts[table_name]["indexes"].items()
                )
                for sequence, column_name in enumerate(columns, 1)
            ])
        raise AssertionError(sql)


def test_governance_schema_requires_exact_utf8mb4_collation():
    detail = governance_module.validate_governance_table_schema(
        _GovernanceSchemaConnection()
    )
    assert detail == {"table_count": 15, "column_count": 222, "index_count": 45}

    with pytest.raises(RuntimeError, match="表引擎或字符集漂移"):
        governance_module.validate_governance_table_schema(
            _GovernanceSchemaConnection(
                table_collation="utf8mb4_0900_ai_ci"
            )
        )
    with pytest.raises(RuntimeError, match="字段契约漂移"):
        governance_module.validate_governance_table_schema(
            _GovernanceSchemaConnection(
                column_collation="utf8mb4_general_ci"
            )
        )


def test_default_seed_contract_binds_every_combination_to_current_versions():
    contract = governance_module._default_governance_seed_contract()
    strategies = contract["strategies"]
    combinations = contract["combinations"]

    assert len(strategies) == 12
    assert len(combinations) == 6
    for key, strategy in strategies.items():
        assert strategy["strategy_key"] == key
        assert strategy["version_hash"] == governance_module._strategy_version_digest(
            strategy_key=key,
            version=strategy["current_version"],
            evaluator_type=strategy["evaluator_type"],
            evaluator_config=strategy["evaluator_config"],
            parameters=strategy["parameters"],
            source_kind=strategy["source_kind"],
        )
    for key, combination in combinations.items():
        assert combination["combination_key"] == key
        assert len(combination["members"]) >= 2
        assert sum(item["weight"] for item in combination["members"]) == pytest.approx(1)
        assert all(
            item["strategy_version"]
            == strategies[item["strategy_key"]]["current_version"]
            for item in combination["members"]
        )
        expected_hash = governance_module._digest({
            "members": combination["members"],
            "constraints": combination["constraints"],
        })
        assert combination["config_hash"] == expected_hash
        assert combination["current_version"] == f"seed-{expected_hash[:16]}"


class _SeedContractResult:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


class _SeedContractConnection:
    def __init__(
        self,
        strategy_registry_rows,
        strategy_version_rows,
        combination_registry_rows,
        combination_version_rows,
    ):
        self.strategy_registry_rows = strategy_registry_rows
        self.strategy_version_rows = strategy_version_rows
        self.combination_registry_rows = combination_registry_rows
        self.combination_version_rows = combination_version_rows

    def execute(self, statement, _params=None):
        sql = str(statement)
        if "FROM st_strategy_registry r" in sql:
            return _SeedContractResult(self.strategy_registry_rows)
        if "FROM st_strategy_version v WHERE" in sql:
            return _SeedContractResult(self.strategy_version_rows)
        if "FROM st_strategy_combination c" in sql:
            return _SeedContractResult(self.combination_registry_rows)
        if "FROM st_strategy_combination_version v WHERE" in sql:
            return _SeedContractResult(self.combination_version_rows)
        raise AssertionError(sql)


class _SeedContractEngine:
    def __init__(
        self,
        strategy_registry_rows,
        strategy_version_rows,
        combination_registry_rows,
        combination_version_rows,
    ):
        self.connection = _SeedContractConnection(
            strategy_registry_rows,
            strategy_version_rows,
            combination_registry_rows,
            combination_version_rows,
        )

    def connect(self):
        return __import__("contextlib").nullcontext(self.connection)


def _seed_contract_rows():
    contract = governance_module._default_governance_seed_contract()
    strategy_registries = [
        {
            "strategy_key": item["strategy_key"],
            "enabled": 1,
            "current_version": item["current_version"],
            "current_status": "SHADOW",
            "version_count": 1,
            "current_version_exists": 1,
            "post_seed_lifecycle_count": 0,
        }
        for item in contract["strategies"].values()
    ]
    strategy_versions = [
        {
            **item,
            "version": item["current_version"],
            "evaluator_config_json": item["evaluator_config"],
            "parameters_json": item["parameters"],
        }
        for item in contract["strategies"].values()
    ]
    combination_registries = [
        {
            "combination_key": item["combination_key"],
            "enabled": 1,
            "current_version": item["current_version"],
            "current_status": "SHADOW",
            "version_count": 1,
            "current_version_exists": 1,
            "post_seed_lifecycle_count": 0,
        }
        for item in contract["combinations"].values()
    ]
    combination_versions = [
        {
            **item,
            "version": item["current_version"],
            "members_json": item["members"],
            "constraints_json": item["constraints"],
        }
        for item in contract["combinations"].values()
    ]
    return (
        strategy_registries,
        strategy_versions,
        combination_registries,
        combination_versions,
    )


def test_exact_default_seed_validator_preserves_frozen_history_and_allows_advancement():
    (
        strategy_registries,
        strategy_versions,
        combination_registries,
        combination_versions,
    ) = _seed_contract_rows()
    detail = governance_module.validate_default_governance_seed_contract(
        _SeedContractEngine(
            strategy_registries,
            strategy_versions,
            combination_registries,
            combination_versions,
        ),
        require_initial_shadow=True,
    )
    assert detail["seeded_strategy_count"] == 12
    assert detail["seeded_combination_count"] == 6
    assert len(detail["seed_contract_hash"]) == 64
    assert detail["initial_strategy_count"] == 12
    assert detail["initial_combination_count"] == 6

    advanced_strategies = deepcopy(strategy_registries)
    advanced_strategies[0].update({
        "current_status": "ACTIVE",
        "post_seed_lifecycle_count": 1,
    })
    advanced_strategies[1].update({
        "current_version": "challenger-v2",
        "current_status": "ACTIVE",
        "version_count": 2,
    })
    advanced_combinations = deepcopy(combination_registries)
    advanced_combinations[0].update({
        "enabled": 0,
        "current_status": "RETIRED",
        "post_seed_lifecycle_count": 1,
    })
    advanced_combinations[1].update({
        "current_version": "combination-v2",
        "current_status": "REDUCE",
        "version_count": 2,
    })
    prepared_detail = governance_module.validate_default_governance_seed_contract(
        _SeedContractEngine(
            advanced_strategies,
            strategy_versions,
            advanced_combinations,
            combination_versions,
        ),
        require_initial_shadow=True,
    )
    runtime_detail = governance_module.validate_default_governance_seed_contract(
        _SeedContractEngine(
            advanced_strategies,
            strategy_versions,
            advanced_combinations,
            combination_versions,
        )
    )
    assert prepared_detail["initial_strategy_count"] == 10
    assert prepared_detail["initial_combination_count"] == 4
    assert runtime_detail["seed_contract_hash"] == detail["seed_contract_hash"]

    invalid_initial = deepcopy(strategy_registries)
    invalid_initial[0]["current_status"] = "ACTIVE"
    with pytest.raises(RuntimeError, match="默认治理策略初始播种状态漂移"):
        governance_module.validate_default_governance_seed_contract(
            _SeedContractEngine(
                invalid_initial,
                strategy_versions,
                combination_registries,
                combination_versions,
            ),
            require_initial_shadow=True,
        )

    drifted_strategy_versions = deepcopy(strategy_versions)
    drifted_strategy_versions[0]["version_hash"] = "0" * 64
    with pytest.raises(RuntimeError, match="默认治理策略冻结版本漂移"):
        governance_module.validate_default_governance_seed_contract(
            _SeedContractEngine(
                strategy_registries,
                drifted_strategy_versions,
                combination_registries,
                combination_versions,
            )
        )

    drifted_combinations = deepcopy(combination_versions)
    drifted_combinations[0]["members_json"][0]["strategy_version"] += ":drift"
    with pytest.raises(RuntimeError, match="默认治理组合冻结版本内容漂移"):
        governance_module.validate_default_governance_seed_contract(
            _SeedContractEngine(
                strategy_registries,
                strategy_versions,
                combination_registries,
                drifted_combinations,
            )
        )


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
        ("net_expectancy_pct", 0.0, "日均净收益"),
        ("profit_factor", 1.0, "日频利润因子"),
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


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("selection_validation_completed_trades", 0),
        ("selection_validation_coverage_days", 0),
        ("review_audit_valid", False),
    ),
)
def test_decay_gate_20_external_selection_diagnostics_have_no_funding_authority(
    field, value,
):
    metrics = _valid_decay_gate_20_metrics()
    metrics[field] = value

    gate = governance_module.evaluate_window_gate(metrics)

    assert gate["passed"] is True
    assert gate["failed_checks"] == []


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
        lambda _as_of_date, _registry, **_kwargs: {
            "cutoff_strategy": records
        },
    )
    monkeypatch.setattr(
        governance_module,
        "_load_metric_inputs",
        lambda _trade_date, current_versions: {},
    )
    monkeypatch.setattr(
        governance_module,
        "_load_funding_replay_plans",
        lambda _day, rows: {
            row["strategy_key"]: {
                "mode": "FULL_BOOTSTRAP", "max_holding_days": 20,
            }
            for row in rows
        },
    )
    monkeypatch.setattr(
        governance_module,
        "_internal_strategy_portfolio_ledger",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(
        governance_module,
        "_prefetch_internal_strategy_ledger_facts",
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
    assert "内部时序分段合同" in gate["failed_checks"]
    assert "HAC统计合同" in gate["failed_checks"]
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
    assert "内部证据高水位" in gate["failed_checks"]
    assert "验证协议" not in gate["failed_checks"]
    assert "验证产物" not in gate["failed_checks"]


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
        "execution_adapter_executable": True,
        "execution_adapter_reason": "测试适配器已就绪",
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
            "funding_provenance": governance_module.CANONICAL_FUNDING_PROVENANCE,
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
    checkpoint_id = "f" * 64
    windows["funding_checkpoint_candidate"] = {
        "schema": "probiga.strategy-funding-checkpoint-candidate.v2",
        "checkpoint_id": checkpoint_id,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    windows["funding_checkpoint_ref"] = {
        "checkpoint_id": checkpoint_id,
    }
    family_decision = {
        "valid": True,
        "passed": True,
        "decision_hash": "b" * 64,
    }
    ranked = governance_module._strategy_rankings(
        registry, {"three_window_gate": windows},
        statistical_decisions={"three_window_gate": family_decision},
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
        statistical_decisions={"three_window_gate": family_decision},
    )[0]
    assert ranked["profit_gate_passed"] is True
    # Ranking only establishes the pre-confirmation gate.  v7 funding is
    # granted later, after spaced authoritative-session confirmations.
    assert ranked["paper_allocation_eligible"] is False


def test_strategy_competition_separates_signal_and_fill_backed_rankings():
    def registry_row(key):
        return {
            "strategy_key": key,
            "strategy_name": key,
            "current_version": "v1",
            "current_status": "SHADOW",
            "enabled": True,
            "execution_adapter_executable": True,
            "execution_adapter_reason": "测试适配器已就绪",
            "funding_pipeline_ready": True,
            "market_route": {
                "eligible": True,
                "reason": "适配",
                "router_decision_hash": "a" * 64,
            },
        }

    def windows(execution_score, signal_score):
        result = {}
        for window in (20, 60, 120):
            minimum_trades = 20 if window == 20 else 80
            minimum_coverage = 20 if window == 20 else 60
            result[window] = {
                "window_days": window,
                "health_score": execution_score,
                "profit_gate": {
                    "passed": False,
                    "reason": "测试只比较，不授资",
                    "failed_checks": ["盈利门槛"],
                },
                "completed_trades": minimum_trades,
                "portfolio_coverage_days": minimum_coverage,
                "funding_provenance": governance_module.CANONICAL_FUNDING_PROVENANCE,
                "drawdown_basis": "internal_version_bound_portfolio_equity",
                "cost_basis": "actual_ledger_fees",
                "session_window_valid": True,
                "session_window_count": window,
                "internal_ledger_hash": str(window)[0] * 64,
                "evidence_hash": str(window)[-1] * 64,
                "evidence_revision_at": "2026-08-21T15:00:00",
                "selection_validation": {
                    "window_days": window,
                    "health_score": signal_score,
                    "evidence_ready": True,
                    "evidence_scope": "VERSION_SELECTION_ONLY",
                    "completed_trades": minimum_trades,
                    "coverage_days": minimum_coverage,
                    "session_window_valid": True,
                    "session_window_count": window,
                    "funding_authority": False,
                    "real_order_authority": False,
                },
            }
        return result

    unverified_windows = windows(99.0, 99.0)
    for metrics in unverified_windows.values():
        metrics["completed_trades"] = 0
        metrics["portfolio_coverage_days"] = 0
        metrics["selection_validation"]["evidence_ready"] = False
        metrics["selection_validation"]["completed_trades"] = 0
        metrics["selection_validation"]["coverage_days"] = 0
    ranked = governance_module._strategy_rankings(
        [
            registry_row("alpha"), registry_row("beta"),
            registry_row("unverified"),
        ],
        {
            "alpha": windows(90.0, 40.0),
            "beta": windows(60.0, 85.0),
            "unverified": unverified_windows,
        },
    )
    by_key = {row["strategy_key"]: row for row in ranked}

    assert by_key["alpha"]["execution_evidence_rank"] == 1
    assert by_key["beta"]["execution_evidence_rank"] == 2
    assert by_key["beta"]["signal_validation_rank"] == 1
    assert by_key["alpha"]["signal_validation_rank"] == 2
    assert by_key["unverified"]["lane_rank"] == 1
    assert by_key["unverified"]["execution_evidence_rank"] is None
    assert by_key["unverified"]["signal_validation_rank"] is None
    assert by_key["alpha"]["lane_rank"] != by_key["alpha"][
        "execution_evidence_rank"
    ]
    assert by_key["alpha"]["competition_disclosure"].endswith(
        "声明信号成绩不授予模拟资金。"
    )
    assert by_key["alpha"]["real_order_authority"] is False


def test_combination_official_rank_excludes_member_reference_only_rows():
    rows = [
        {
            "combination_key": "member_reference_only",
            "has_independent_evidence": False,
            "ranking_score_display": None,
            "ranking_score": 99.0,
            "lane_rank": 1,
        },
        {
            "combination_key": "comparable_low",
            "has_independent_evidence": True,
            "ranking_score_display": 60.0,
            "ranking_score": 60.0,
            "lane_rank": 3,
        },
        {
            "combination_key": "comparable_high",
            "has_independent_evidence": True,
            "ranking_score_display": 80.0,
            "ranking_score": 80.0,
            "lane_rank": 2,
        },
    ]

    governance_module._assign_combination_independent_evidence_ranks(rows)
    by_key = {row["combination_key"]: row for row in rows}

    assert by_key["member_reference_only"]["independent_evidence_rank"] is None
    assert by_key["comparable_high"]["independent_evidence_rank"] == 1
    assert by_key["comparable_low"]["independent_evidence_rank"] == 2


def test_client_declared_signal_summary_discloses_unattested_source_and_no_authority():
    summary = governance_module._selection_validation_summary({
        "window_days": 60,
        "completed_trades": 80,
        "coverage_days": 60,
        "win_rate_pct": 55.0,
        "payoff_ratio": 1.4,
        "net_expectancy_pct": 0.4,
        "profit_factor": 1.5,
        "max_drawdown_pct": 8.0,
        "cost_stress_expectancy_pct": 0.2,
        "market_match_score": 80.0,
        "walk_forward_segments": 5,
        "positive_segments": 4,
        "independent_oos": True,
        "walk_forward_verified": True,
        "session_window_valid": True,
        "session_window_count": 60,
        "session_window_hash": "c" * 64,
        "evidence_protocol": "PURGED_WALK_FORWARD_V2",
        "artifact_hash": "a" * 64,
        "source_dataset_hash": "b" * 64,
        "evidence_hash": "d" * 64,
        "verification_status": "CONFIRMED",
        "review_audit_valid": True,
    })

    assert summary["status"] == "COMPARABLE"
    assert summary["evidence_scope"] == "VERSION_SELECTION_ONLY"
    assert summary["source_authority"] == "CLIENT_DECLARED_UNATTESTED"
    assert summary["review_scope"] == "STRUCTURE_AND_REPRODUCIBILITY_ONLY"
    assert "未与权威行情逐行认证" in summary["source_authority_label"]
    assert summary["funding_authority"] is False
    assert summary["real_order_authority"] is False


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
        expected_session_rows=[
            {"trade_date": "2026-08-21"},
            {"trade_date": "2026-08-20"},
        ],
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
        expected_session_rows=[
            {"trade_date": "2026-08-20"},
            {"trade_date": "2026-08-19"},
        ],
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
        expected_session_rows=[
            {"trade_date": "2026-08-20"},
            {"trade_date": "2026-08-19"},
        ],
    ) == 1
    assert governance_module._prior_consecutive_combination_gate_passes(
        "balanced", "v1", "2026-08-21", "hash-current",
        "2026-08-21T15:00:00", limit=2,
        expected_session_rows=[
            {"trade_date": "2026-08-20"},
            {"trade_date": "2026-08-19"},
        ],
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
    funding_manifest, funding_candidates = (
        governance_module._build_funding_checkpoint_manifest(
            run_uid="a" * 32,
            trade_date="2026-08-21",
            strategies=[],
            combinations=[],
        )
    )
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
        "funding_checkpoint_manifest": funding_manifest,
        "summary": summary,
        "strategies": [],
        "combinations": [],
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
    }, funding_checkpoint_candidates=funding_candidates)
    audit_calls = [
        params for sql, params in connection.calls
        if "st_strategy_governance_audit" in sql
        and (params or {}).get("action") == "RUN_GOVERNANCE"
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
    canonical_snapshot = {
        "status": "ok",
        "run_uid": "a" * 32,
        "canonical_result_hash": "b" * 64,
        "trade_date": "2026-08-21",
        "result_mode": "CANONICAL_PERSISTED",
        "is_canonical": True,
        "summary": {"strategy_count": 9, "tradable_count": 0},
        "strategies": [],
        "combinations": [],
        "pools": {"observation": [], "confirmation": [], "tradable": []},
        "allocations": [
            {
                "target_type": "CASH",
                "target_key": "cash",
                "simulated_weight_pct": 100.0,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }
        ],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    monkeypatch.setattr(
        strategy_center_router,
        "load_canonical_governance_snapshot",
        lambda **_kwargs: canonical_snapshot,
    )
    app = FastAPI()
    app.include_router(strategy_center_router.router, prefix="/api")
    response = TestClient(app).get("/api/strategy-center/governance?trade_date=2026-08-21")
    assert response.status_code == 200
    payload = response.json()
    assert payload == strategy_center_router._bounded_governance_overview(
        canonical_snapshot
    )
    assert payload["allocations"][0]["simulated_weight_pct"] == 100.0
    assert payload["automatic_real_order_submission"] is False


def test_governance_run_blocks_lifecycle_mutation_when_pool_data_missing(monkeypatch):
    monkeypatch.setattr(
        strategy_center_router,
        "orchestrate_strategy_governance",
        lambda **_kwargs: {
            "status": "blocked",
            "orchestration_status": "NOT_READY",
            "reason": "治理输入未通过新鲜度校验：底层票池数据缺失",
            "allocations": [{
                "simulated_weight_pct": 100.0,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )
    result = strategy_center_router.strategy_center_run_governance(
        strategy_center_router.StrategyRunRequest(trade_date="2026-08-21"),
        _admin_request(),
    )
    assert result.status_code == 503
    payload = json.loads(result.body)
    assert payload["status"] == "blocked"
    assert "底层票池数据缺失" in payload["reason"]
    assert payload["allocations"][0]["simulated_weight_pct"] == 100.0


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
        "orchestrate_strategy_governance",
        lambda **_kwargs: {
            "status": "blocked",
            "orchestration_status": "NOT_READY",
            "reason": "公司行动权威账本未建立，相关持仓资金证据暂停",
            "allocations": [{
                "simulated_weight_pct": 100.0,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )

    result = strategy_center_router.strategy_center_run_governance(
        strategy_center_router.StrategyRunRequest(trade_date="2026-08-21"),
        _admin_request(),
    )

    assert result.status_code == 503
    payload = json.loads(result.body)
    assert payload["status"] == "blocked"
    assert "公司行动权威账本未建立" in payload["reason"]
    assert payload["allocations"][0]["simulated_weight_pct"] == 100.0
    assert payload["automatic_real_order_submission"] is False


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
    assert '"--schema-prepared"' in installer_source
    assert '"--writers-fenced-schema-preparation"' in installer_source
    assert "args.writers_fenced_schema_preparation" in installer_source
    prepared_start = installer_source.index("if args.schema_prepared:")
    fallback_start = installer_source.index("else:", prepared_start)
    prepared_branch = installer_source[prepared_start:fallback_start]
    assert "run_v3_migrations(engine, dry_run=True)" in prepared_branch
    assert not re.search(
        r"run_v3_migrations\(engine\s*\)", prepared_branch
    )
    assert "ensure_attestation_tables" not in prepared_branch
    assert "ensure_and_seed_governance" not in prepared_branch
    assert "validate_attestation_schema(engine)" in prepared_branch
    assert "validate_prepared_governance_runtime(engine)" in prepared_branch
    assert "validate_metric_input_review_triggers" not in prepared_branch
    assert "validate_governance_append_only_triggers" not in prepared_branch
    fallback_branch = installer_source[fallback_start:]
    assert "run_v3_migrations(engine)" in fallback_branch
    assert "ensure_attestation_tables(engine)" in fallback_branch
    assert "migrate_legacy_attestation_collation(" in fallback_branch
    assert "writers_fenced=True" in fallback_branch
    assert "apply_legacy_completed_run_binding(engine)" in fallback_branch
    assert (
        fallback_branch.index("migrate_legacy_attestation_collation(")
        < fallback_branch.index("apply_legacy_completed_run_binding(engine)")
        < fallback_branch.index("ensure_attestation_tables(engine)")
    )
    assert "ensure_strategy_governance_tables(engine=engine)" in fallback_branch
    assert "seed_governance_registry()" in fallback_branch
    assert "validate_prepared_governance_runtime(engine)" in fallback_branch
    assert "CREATE TRIGGER" not in fallback_branch.upper()
    assert "parser.error" not in installer_source[prepared_start:]
    assert installer_source.index("if args.restore_snapshot:") < (
        prepared_start
    )
    assert installer_source.index("_write_snapshot(") < (
        prepared_start
    )


def test_production_governance_task_install_rejects_runtime_schema_migration(
    monkeypatch,
):
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setattr(sys, "argv", ["add_strategy_governance_task.py"])
    monkeypatch.setattr(
        governance_task_installer,
        "load_project_env",
        lambda: pytest.fail("production guard ran after environment loading"),
    )

    with pytest.raises(SystemExit) as caught:
        governance_task_installer.main()

    assert caught.value.code == 2


def test_governance_task_snapshot_can_be_read_from_sealed_stdin(monkeypatch):
    payload = {
        "format_version": 1,
        "task_type": GOVERNANCE_TASK["task_type"],
        "script_path": GOVERNANCE_TASK["script_path"],
        "rows": [{"task_name": "动态策略治理每日更新"}],
    }
    stdin = io.TextIOWrapper(
        io.BytesIO(json.dumps(
            payload,
            ensure_ascii=False,
        ).encode("utf-8")),
        encoding="ascii",
    )
    monkeypatch.setattr(
        sys,
        "stdin",
        stdin,
    )

    assert governance_task_installer._read_snapshot(Path("-")) == payload


def test_production_runtime_never_reenters_governance_schema_ddl(monkeypatch):
    calls: list[str] = []
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setattr(governance_module, "_SEED_READY", False)
    monkeypatch.setattr(
        governance_module,
        "validate_prepared_governance_runtime",
        lambda: calls.append("validate") or {"table_count": 15},
    )
    monkeypatch.setattr(
        governance_module,
        "ensure_strategy_governance_tables",
        lambda: pytest.fail("production runtime attempted governance DDL"),
    )
    monkeypatch.setattr(
        governance_module,
        "seed_governance_registry",
        lambda: pytest.fail("production runtime attempted schema seeding"),
    )

    governance_module.ensure_and_seed_governance()
    governance_module.ensure_and_seed_governance()

    assert calls == ["validate"]
    assert governance_module._SEED_READY is True


class _TriggerInventoryResult:
    def __init__(self, rows):
        self.rows = [dict(row) for row in rows]

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


class _ExactTriggerDatabaseAccess:
    def execute(self, statement, _params=None):
        sql = str(statement)
        if "EVENT_OBJECT_TABLE='st_strategy_metric_input'" in sql:
            return _TriggerInventoryResult([{
                "trigger_name": name,
                "action_timing": contract["timing"],
                "event_manipulation": contract["event"],
                "event_object_table": contract["table"],
                "action_orientation": "ROW",
                "action_statement": contract["body"],
            } for name, contract in sorted(
                governance_module.METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS.items()
            )])
        if "information_schema.TRIGGERS" in sql:
            return _TriggerInventoryResult([{
                "trigger_name": name,
                "action_timing": contract[0],
                "event_manipulation": contract[1],
                "event_object_table": contract[2],
                "action_orientation": "ROW",
                "action_statement": contract[3],
            } for name, contract in sorted(
                governance_module.GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS.items()
            )])
        raise AssertionError(sql)


def test_managed_mysql_governance_setup_freezes_exact_38_plus_2_triggers():
    assert len(governance_module.GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS) == 38
    assert len(governance_module.GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS) == 38
    assert len(governance_module.METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS) == 2
    assert set(governance_module.GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS) == (
        governance_module.EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
    )
    assert set(governance_module.METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS) == (
        governance_module.EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES
    )
    noop_bodies = [
        contract[3]
        for name, contract in (
            governance_module.GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS.items()
        )
        if name.endswith("_immutable_bu")
        and contract[2] in {
            "st_strategy_version", "st_strategy_lifecycle_event",
            "st_strategy_governance_audit", "st_strategy_health_snapshot",
        }
    ]
    assert noop_bodies
    assert all("IF NOT (" in body and "OLD." in body for body in noop_bodies)


def test_trigger_validators_require_exact_database_metadata():
    connection = _ExactTriggerDatabaseAccess()
    metric = governance_module.validate_metric_input_review_triggers(connection)
    append_only = governance_module.validate_governance_append_only_triggers(
        connection
    )

    assert metric["trigger_count"] == 2
    assert append_only["trigger_count"] == 38
    assert metric["database_triggers_required"] is True
    assert append_only["database_triggers_required"] is True
    assert metric["metadata_frozen"] is True
    assert append_only["metadata_frozen"] is True


@pytest.mark.parametrize("drift", ["missing", "extra", "body", "metadata"])
def test_trigger_validators_fail_closed_on_inventory_or_metadata_drift(drift):
    class DriftedAccess(_ExactTriggerDatabaseAccess):
        def execute(self, statement, params=None):
            result = super().execute(statement, params)
            sql = str(statement)
            rows = list(result.rows)
            if "EVENT_OBJECT_TABLE='st_strategy_metric_input'" in sql:
                if drift == "metadata":
                    rows[0]["action_timing"] = "AFTER"
                return _TriggerInventoryResult(rows)
            if drift == "missing":
                rows.pop()
            elif drift == "extra":
                rows.append({
                    **rows[0],
                    "trigger_name": "trg_unmanaged_governance_mutation",
                })
            elif drift == "body":
                rows[0]["action_statement"] += " SET @tampered=1;"
            return _TriggerInventoryResult(rows)

    connection = DriftedAccess()
    if drift == "metadata":
        with pytest.raises(RuntimeError):
            governance_module.validate_metric_input_review_triggers(connection)
    else:
        with pytest.raises(RuntimeError):
            governance_module.validate_governance_append_only_triggers(
                connection
            )


def test_prepared_runtime_contract_requires_exact_40_database_triggers():
    source = inspect.getsource(
        governance_module.validate_prepared_governance_runtime
    )
    assert "_validate_governance_append_only_triggers_connection" in source
    assert "validate_metric_input_review_triggers" in source
    assert '"database_triggers_required": True' in source
    assert '"trigger_count": 0' not in source
    assert governance_module.METRIC_INPUT_REVIEW_TRIGGER_CONTRACT_HASH == (
        "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84"
    )


class _RuntimeSealResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)


class _RuntimeSealConnection:
    def __init__(self, *, grants=None, migrations=None):
        self.grants = grants or [
            "GRANT USAGE ON *.* TO `runtime`@`localhost`",
            (
                "GRANT SELECT, INSERT, UPDATE, DELETE, "
                "CREATE TEMPORARY TABLES ON `probiga`.* "
                "TO `runtime`@`localhost`"
            ),
        ]
        self.migrations = migrations or {
            governance_module.RUN_REVISION_MIGRATION_KEY: (
                governance_module.RUN_REVISION_MIGRATION_HASH
            ),
            governance_module.STRATEGY_CONTENT_HASH_MIGRATION_KEY: (
                governance_module.STRATEGY_CONTENT_HASH_MIGRATION_HASH
            ),
            governance_module.FUNDING_CHECKPOINT_MIGRATION_KEY: (
                governance_module.FUNDING_CHECKPOINT_MIGRATION_HASH
            ),
        }

    def execute(self, statement, _params=None):
        sql = str(statement)
        if sql == "SHOW GRANTS FOR CURRENT_USER":
            return _RuntimeSealResult([(grant,) for grant in self.grants])
        if "FROM st_strategy_governance_schema_migration" in sql:
            return _RuntimeSealResult([
                {"migration_key": key, "migration_hash": value}
                for key, value in self.migrations.items()
            ])
        raise AssertionError(sql)


def test_runtime_trigger_seal_accepts_exact_markers_without_trigger_metadata(
    monkeypatch,
):
    sha = "a" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", sha)
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", sha)

    result = governance_module.validate_privileged_trigger_migration_seal(
        _RuntimeSealConnection()
    )

    assert result["authority"] == "PRIVILEGED_CUTOVER_MIGRATION_SEAL"
    assert result["live_trigger_metadata_checked"] is False
    assert result["runtime_least_privilege_verified"] is True
    assert result["funding_trigger_count"] == 4
    assert result["governance_trigger_count"] == 40


@pytest.mark.parametrize("failure", ["marker", "trigger_grant", "build"])
def test_runtime_trigger_seal_fails_closed(monkeypatch, failure):
    sha = "b" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", sha)
    monkeypatch.setenv(
        "PROBIGA_BUILD_COMMIT_SHA", "c" * 40 if failure == "build" else sha
    )
    connection = _RuntimeSealConnection()
    if failure == "marker":
        connection.migrations.pop(
            governance_module.FUNDING_CHECKPOINT_MIGRATION_KEY
        )
    elif failure == "trigger_grant":
        connection.grants = [
            "GRANT SELECT, TRIGGER ON `probiga`.* TO `runtime`@`localhost`"
        ]

    with pytest.raises(RuntimeError):
        governance_module.validate_privileged_trigger_migration_seal(
            connection
        )


def test_metric_review_application_state_machine_is_atomic_and_audited():
    source = inspect.getsource(governance_module.review_metric_input)

    assert "WHERE evidence_id=:evidence_id FOR UPDATE" in source
    assert 'if current_status != "PENDING"' in source
    assert "submitter == reviewer" in source
    assert "AND verification_status='PENDING'" in source
    assert "updated.rowcount != 1" in source
    assert "_append_audit_connection(" in source
    assert '"CONFIRM_METRIC_EVIDENCE"' in source
    assert '"REJECT_METRIC_EVIDENCE"' in source


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

    def all(self):
        return list(self._rows)


class _ConcurrentMetricConnection:
    def execute(self, statement, _params=None):
        sql = str(statement)
        if "FROM st_strategy_governance_schema_migration" in sql:
            assert "FOR UPDATE" in sql
            return _ConcurrentMetricResult([{"migration_key": "global"}])
        if "SELECT current_version FROM st_strategy_registry" in sql:
            assert "FOR UPDATE" in sql
            return _ConcurrentMetricResult([{"current_version": "v1"}])
        if "SELECT evidence_revision_at, artifact_hash" in sql:
            return _ConcurrentMetricResult()
        if "SELECT evidence_id, entity_type, strategy_key" in sql:
            return _ConcurrentMetricResult()
        if "action='SUBMIT_CHALLENGER_EVIDENCE'" in sql:
            assert "FOR UPDATE" in sql
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
        "st_strategy_adapter_run_receipt",
        "st_strategy_industry_history",
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
                    summary_json LONGTEXT NOT NULL, result_json LONGTEXT NOT NULL,
                    result_hash CHAR(64) NOT NULL, created_at DATETIME NOT NULL,
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
            assert detail["trigger_count"] == 22
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
                 '{}', '{}', :result_hash, NOW(), NOW())
            """), {
                "run_uid": "a" * 32,
                "input_hash": "4" * 64,
                "build_sha": "5" * 40,
                "router_hash": "6" * 64,
                "decision_hash": "7" * 64,
                "result_hash": hashlib.sha256(b"{}").hexdigest(),
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
