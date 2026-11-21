from datetime import datetime
import json
import sys

from sqlalchemy import create_engine


def test_observation_persists_actual_receive_time_and_verifiable_receipt(tmp_path, monkeypatch, capsys):
    from biz.market_context import external_market as external
    from tools import refresh_external_market_context as capture
    from tools import ensure_quality_gate
    engine = create_engine(f"sqlite:///{tmp_path / 'observations.db'}")
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE st_external_market_context (snapshot_id TEXT,context_date TEXT,captured_at TEXT,source TEXT,symbol TEXT,display_name TEXT,price REAL,change_pct REAL,previous_close REAL,market_time TEXT,availability TEXT,payload_json TEXT)")
    monkeypatch.setattr(external, "ensure_external_market_table", lambda engine: None)
    monkeypatch.setattr(capture, "create_tool_engine", lambda **kwargs: engine)
    monkeypatch.setattr(capture, "load_project_env", lambda: None)
    now = datetime(2026, 11, 21, 16)
    monkeypatch.setattr(capture, "_now_shanghai", lambda: now)
    snapshot = {"snapshot_id": "test-observation", "captured_at": datetime(2026, 11, 20, 8, 15),
                "context_date": "2026-11-20", "available_count": 41, "expected_count": 41,
                "missing_symbols": [], "acquisition_status": "COMPLETE", "items": [
                    {"symbol": symbol, "display_name": name, "price": 10, "previous_close": 9,
                     "change_pct": 11, "market_time": "2026-11-20 15:00:00", "source": "verified.native",
                     "availability": "available"} for symbol,name in external.EXTERNAL_MARKET_SYMBOLS]}
    monkeypatch.setattr(capture, "fetch_external_market_snapshot", lambda: snapshot)
    premarket = next(row for row in ensure_quality_gate.TASKS if row["task_type"] == external.PREMARKET_EXTERNAL_TASK_TYPE)
    assert premarket["cron_time"] == "08:15" and premarket["script_args"] == "--premarket --attempts 1 --json"
    monkeypatch.setattr(sys, "argv", ["refresh_external_market_context.py", "--attempts", "1", "--json"])
    assert capture.main() == 0
    output = capsys.readouterr().out
    receipt = json.loads(output)
    assert receipt["captured_at"] == "2026-11-21 16:00:00"
    assert receipt["context_date"] == "2026-11-21"
    assert receipt["available_count"] == 41 and receipt["status"] == "COMPLETE"
    digest, missing = external._external_premarket_persisted_proof(engine, snapshot_id=receipt["snapshot_id"], captured_at=now)
    assert not missing and digest == receipt["persisted_sha256"]
