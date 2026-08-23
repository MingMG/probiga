from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from integrations.bigqmt.health import evaluate_spool_health, file_token
from integrations.bigqmt.spool import bridge_paths


ROOT = Path(__file__).resolve().parents[1]


def _home(tmp_path: Path) -> Path:
    home = tmp_path / "QMT"
    (home / "bin.x64").mkdir(parents=True)
    (home / "userdata").mkdir()
    (home / "bin.x64" / "XtItClient.exe").write_bytes(b"")
    bridge_paths(home)["root"].mkdir(parents=True)
    return home


def _write(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )


def _healthy_files(tmp_path: Path, now_ts: float = 1_000.0) -> Path:
    home = _home(tmp_path)
    paths = bridge_paths(home)
    _write(
        paths["heartbeat"],
        {
            "status": "running",
            "updated_ts": now_ts - 2,
        },
    )
    _write(
        paths["full"],
        {
            "batch_id": "full-1000",
            "generated_ts": now_ts - 5,
            "quote_count": 5_500,
        },
    )
    token = file_token(paths["full"])
    _write(
        paths["consumer_status"],
        {
            "status": "idle",
            "generated_ts": now_ts - 1,
            "full_sync_receipt": {
                "source_batch_id": "full-1000",
                "source_full_file_token": token,
                "quality_status": "PASS",
            },
        },
    )
    return home


def test_end_to_end_health_requires_all_three_links(tmp_path):
    home = _healthy_files(tmp_path)

    result = evaluate_spool_health(home, now_ts=1_000)

    assert result["healthy"] is True
    assert result["checks"] == {
        "strategy_heartbeat": True,
        "full_market_snapshot": True,
        "sync_receipt": True,
        "level1_callback": True,
    }


def test_stale_heartbeat_blocks_even_with_fresh_files(tmp_path):
    home = _healthy_files(tmp_path)
    paths = bridge_paths(home)
    heartbeat = json.loads(paths["heartbeat"].read_text(encoding="utf-8"))
    heartbeat["updated_ts"] = 960
    _write(paths["heartbeat"], heartbeat)

    result = evaluate_spool_health(home, now_ts=1_000)

    assert result["healthy"] is False
    assert result["failed_checks"] == ["strategy_heartbeat"]


def test_receipt_for_an_older_file_cannot_attest_current_snapshot(tmp_path):
    home = _healthy_files(tmp_path)
    paths = bridge_paths(home)
    consumer = json.loads(
        paths["consumer_status"].read_text(encoding="utf-8")
    )
    consumer["full_sync_receipt"][
        "source_full_file_token"
    ] = "old-file-token"
    _write(paths["consumer_status"], consumer)

    result = evaluate_spool_health(home, now_ts=1_000)

    assert result["healthy"] is False
    assert result["failed_checks"] == ["sync_receipt"]


def test_active_session_requires_a_fresh_genuine_level1_callback(tmp_path):
    home = _healthy_files(tmp_path)

    result = evaluate_spool_health(
        home,
        now_ts=1_000,
        require_level1_callback=True,
    )

    assert result["healthy"] is False
    assert result["failed_checks"] == ["level1_callback"]
    assert result["level1_required"] is True


def test_fresh_level1_callback_completes_the_health_chain(tmp_path):
    home = _healthy_files(tmp_path)
    paths = bridge_paths(home)
    heartbeat = json.loads(
        paths["heartbeat"].read_text(encoding="utf-8")
    )
    heartbeat.update({
        "subscription_id": 7,
        "last_callback_ts": 995,
    })
    _write(paths["heartbeat"], heartbeat)
    _write(
        paths["tracked"],
        {
            "generated_ts": 996,
            "last_callback_ts": 995,
            "quotes": {},
        },
    )

    result = evaluate_spool_health(
        home,
        now_ts=1_000,
        require_level1_callback=True,
    )

    assert result["healthy"] is True
    assert result["checks"]["level1_callback"] is True
    assert result["level1_callback_age_seconds"] == 5


def test_health_cli_imports_real_runtime_dependencies():
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools" / "check_big_qmt_end_to_end_health.py"),
            "--help",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--heartbeat-max-age" in result.stdout
