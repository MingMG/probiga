from __future__ import annotations

import json
import sys
from types import SimpleNamespace

from server.engine import strategy_center
from tools import ensure_quality_gate
from tools import run_strategy_external_overlay as overlay


def test_external_market_adjustment_is_bounded_and_neutral_by_default():
    assert strategy_center.external_market_score_adjustment({
        "external_market_data_quality": "PASS",
        "external_market_status": "SUPPORT",
        "external_market_score": 70,
    }) == 3.0
    assert strategy_center.external_market_score_adjustment({
        "external_market_data_quality": "WATCH",
        "external_market_status": "RISK",
        "external_market_score": 30,
    }) == -3.0
    assert strategy_center.external_market_score_adjustment({
        "external_market_data_quality": "UNKNOWN",
        "external_market_status": "SUPPORT",
        "external_market_score": 80,
    }) == 0.0
    assert strategy_center.external_market_score_adjustment(None) == 0.0


def test_external_market_overlay_adjusts_strategy_scores_without_mutating_source():
    source = [{
        "stock_code": "000001",
        "ai_score": 61.0,
        "final_trade_score": 62.0,
        "short_term_score": 63.0,
        "ultra_short_score": 64.0,
        "swing_score": 65.0,
        "main_wave_score": 66.0,
        "trend_hold_score": 67.0,
    }]
    context = {
        "snapshot_id": "snapshot-1",
        "captured_at": "2026-08-31 08:30:05",
        "external_market_data_quality": "PASS",
        "external_market_status": "SUPPORT",
        "external_market_score": 70.0,
    }

    result = strategy_center.apply_external_market_score_overlay(source, context)

    assert source[0]["ai_score"] == 61.0
    assert result[0]["ai_score"] == 64.0
    assert result[0]["final_trade_score"] == 65.0
    assert result[0]["main_wave_score"] == 69.0
    assert result[0]["external_market_adjustment"] == 3.0
    assert result[0]["external_market_snapshot_id"] == "snapshot-1"


def test_0830_external_overlay_scheduler_contract_is_enabled():
    task = next(
        item for item in ensure_quality_gate.TASKS
        if item.get("task_type") == "strategy_external_overlay"
    )
    assert task["cron_time"] == "08:30"
    assert task["enabled"] == 1
    assert task["script_path"] == "tools/run_strategy_external_overlay.py"
    assert "QMT" in task["description"]


def test_overlay_emits_only_staged_work_reference(monkeypatch, capsys, tmp_path):
    engine = SimpleNamespace(dispose=lambda: None)
    reference = {"schema": "probiga.premarket-pool-work-reference.v1", "trade_date": "2026-08-28"}
    captured = {}
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    monkeypatch.setenv("PROBIGA_SCHEDULER_HISTORY_RUN_UID", "a" * 32)
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", "b" * 40)
    monkeypatch.setenv("PROBIGA_SCHEDULER_TASK_ID", "128")
    monkeypatch.setattr(overlay, "_load_project_env", lambda: None)
    monkeypatch.setattr(overlay, "create_batch_engine", lambda: engine)

    def stage(_engine, journal, **identity):
        captured.update(identity)
        assert journal.root.parent == tmp_path
        return reference, "2026-08-28", ""

    monkeypatch.setattr(overlay, "prepare_revision", stage)
    monkeypatch.setattr(sys, "argv", ["run_strategy_external_overlay.py", "--json"])
    assert overlay.main() == 0
    assert captured == {"run_uid": "a" * 32, "build_sha": "b" * 40, "task_id": "128"}
    assert json.loads(capsys.readouterr().out) == reference


def test_overlay_missing_source_has_explicit_terminal_result(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    monkeypatch.setattr(overlay, "_load_project_env", lambda: None)
    monkeypatch.setattr(overlay, "create_batch_engine", lambda: SimpleNamespace(dispose=lambda: None))
    monkeypatch.setattr(
        overlay, "prepare_revision", lambda *_args, **_kw: (
            None, "2026-08-28", "PREMARKET_ANNOUNCEMENT_UNAVAILABLE_AT_SLOT",
        ),
    )
    monkeypatch.setattr(sys, "argv", ["run_strategy_external_overlay.py", "--json"])
    assert overlay.main() == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["trade_date"] == "2026-08-28"
    assert payload["real_order_authority"] is False
