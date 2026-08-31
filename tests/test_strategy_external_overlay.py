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


def test_overlay_capture_failure_becomes_neutral(monkeypatch):
    monkeypatch.setattr(
        overlay,
        "fetch_external_market_snapshot",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    result = overlay.capture_external_context(object())
    assert result["external_market_score"] == 50.0
    assert result["external_market_data_quality"] == "UNKNOWN"


def test_overlay_main_passes_exact_snapshot_to_governance(monkeypatch, capsys):
    engine = SimpleNamespace(dispose=lambda: None)
    context = {
        "snapshot_id": "snapshot-2",
        "captured_at": "2026-08-31 08:30:05",
        "external_market_data_quality": "PASS",
        "external_market_status": "SUPPORT",
        "external_market_score": 60.0,
    }
    governance = {
        "status": "ok",
        "orchestration_status": "COMPLETED",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    captured = {}
    monkeypatch.setattr(overlay, "_load_project_env", lambda: None)
    monkeypatch.setattr(overlay, "create_batch_engine", lambda: engine)
    monkeypatch.setattr(overlay, "previous_trade_date", lambda *_args: "2026-08-28")
    monkeypatch.setattr(overlay, "capture_external_context", lambda _engine: context)

    def run_governance(**kwargs):
        captured.update(kwargs)
        return governance, 0

    monkeypatch.setattr(overlay, "run_daily_governance", run_governance)
    monkeypatch.setattr(sys, "argv", ["run_strategy_external_overlay.py", "--json"])

    assert overlay.main() == 0
    payload = json.loads(capsys.readouterr().out)
    assert captured["requested_trade_date"] == "2026-08-28"
    assert captured["external_market_context"] is context
    assert payload["score_adjustment"] == 1.5
    assert payload["status"] == "ok"
