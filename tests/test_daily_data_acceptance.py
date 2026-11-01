from __future__ import annotations

import importlib.util
import json
from datetime import datetime
from pathlib import Path
import sys
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tools" / "daily_data_acceptance.py"
SPEC = importlib.util.spec_from_file_location("daily_data_acceptance", TOOL)
acceptance = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = acceptance
SPEC.loader.exec_module(acceptance)


class _ConnectionContext:
    def __enter__(self):
        return object()

    def __exit__(self, exc_type, exc, traceback):
        return False


class _Engine:
    def connect(self):
        return _ConnectionContext()


def _passing_check(key: str, label: str):
    return acceptance.AcceptanceCheck(
        key,
        label,
        acceptance.PASS,
        "ready",
        "",
        {},
    )


def test_acceptance_always_emits_exact_seven_contract_checks(monkeypatch):
    replacements = {
        "_check_daily_truth": _passing_check("daily_receipt", "daily"),
        "_check_daily_coverage": _passing_check("daily_coverage", "coverage"),
        "_check_qmt_truth_freshness": _passing_check(
            "qmt_truth_freshness", "qmt freshness"
        ),
        "_check_membership": _passing_check("membership", "membership"),
        "_check_etf": _passing_check("etf_daily", "etf"),
        "_check_concepts": _passing_check("concept_freshness", "concept"),
        "_check_v3_tasks": _passing_check("v3_tasks", "v3"),
    }
    for name, result in replacements.items():
        monkeypatch.setattr(
            acceptance,
            name,
            lambda *args, _result=result, **kwargs: _result,
        )

    report = acceptance.collect_acceptance(
        _Engine(),
        target_date="2026-10-30",
        now=datetime(2026, 10, 30, 23, 20),
    )

    assert report["status"] == acceptance.PASS
    assert report["acceptance_status"] == "DATA_ACCEPTANCE_OK"
    assert report["failed_count"] == 0
    assert [item["key"] for item in report["checks"]] == [
        "daily_receipt",
        "daily_coverage",
        "qmt_truth_freshness",
        "membership",
        "etf_daily",
        "concept_freshness",
        "v3_tasks",
    ]


def test_failure_message_contains_only_failed_remediation():
    report = {
        "target_date": "2026-10-30",
        "status": acceptance.FAIL,
        "acceptance_status": "DATA_ACCEPTANCE_FAILED",
        "checks": [
            _passing_check("daily_receipt", "daily").as_dict(),
            acceptance.AcceptanceCheck(
                "membership",
                "membership",
                acceptance.FAIL,
                "snapshot is stale",
                "rerun task 65",
                {},
            ).as_dict(),
        ],
    }

    message = acceptance.render_message(report)

    assert "每日数据验收｜2026-10-30" in message
    assert "❌ membership：snapshot is stale" in message
    assert "membership：rerun task 65" in message
    assert "daily：ready" not in message.split("**补救指引**")[1]


def test_acceptance_receipt_is_atomic_and_machine_readable(tmp_path):
    report = {
        "schema": acceptance.ACCEPTANCE_SCHEMA,
        "status": acceptance.FAIL,
        "acceptance_status": "DATA_ACCEPTANCE_FAILED",
        "target_date": "2026-10-30",
        "observed_at": "2026-10-30T23:20:00",
        "failed_count": 1,
        "checks": [
            acceptance.AcceptanceCheck(
                "membership",
                "membership",
                acceptance.FAIL,
                "stale",
                "rerun",
                {},
            ).as_dict(),
        ],
    }

    path = acceptance.persist_report(report, output_root=tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert path == tmp_path / "2026-10-30.json"
    assert payload["schema"] == acceptance.ACCEPTANCE_SCHEMA
    assert payload["status"] == acceptance.FAIL
    assert payload["acceptance_status"] == "DATA_ACCEPTANCE_FAILED"
    assert not list(tmp_path.glob(".*.tmp"))


def test_scheduler_final_verification_delegates_to_acceptance_contract():
    from tools.ensure_quality_gate import TASKS

    task = next(
        item
        for item in TASKS
        if item["task_type"] == "daily_data_closeout_verify"
    )
    assert task["task_name"] == "每日数据闭环最终复验"
    assert task["script_path"] == "tools/daily_data_closeout.py"
    assert task["script_args"] == "--mode verify --json"
    assert task["cron_time"] == "23:20"
    assert task["enabled"] == 1
    closeout = (ROOT / "tools" / "daily_data_closeout.py").read_text(
        encoding="utf-8"
    )
    assert "from tools.daily_data_acceptance import main as acceptance_main" in closeout
    assert 'if args.mode == "verify"' in closeout


def test_qmt_daily_task_cannot_regress_to_missing_latest_session_selector():
    from tools.qmt_host_ownership_contract import WINDOWS_QMT_EDGE_TASKS_BY_TYPE

    task = WINDOWS_QMT_EDGE_TASKS_BY_TYPE["qmt_stock_daily_canonical"]
    arguments = task["script_args"].split()
    assert "--latest-session" in arguments
    assert "--replace-existing" in arguments
    assert "--apply" in arguments


def test_closeout_verify_entrypoint_forwards_to_acceptance(monkeypatch):
    from tools import daily_data_closeout as closeout
    from tools import daily_data_acceptance as implementation

    observed = []
    monkeypatch.setattr(
        implementation,
        "main",
        lambda arguments: observed.append(list(arguments)) or 0,
    )

    assert closeout.main(["--mode", "verify", "--json", "--no-notify"]) == 0
    assert observed == [["--json", "--no-notify"]]


def test_deploy_provisions_service_writable_acceptance_receipt_root():
    script = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    assert "DATA_ACCEPTANCE_ROOT=/var/lib/probiga/data-acceptance" in script
    body = script[
        script.index("prepare_data_acceptance_root() {"):
        script.index("migrate_probiga_job_log_legacy_modes() {")
    ]
    assert 'test "$(stat -c \'%a\' -- "$DATA_ACCEPTANCE_ROOT")" = 700' in body
    assert 'sudo -u "$SERVICE_USER" test -w "$DATA_ACCEPTANCE_ROOT"' in body
    assert "\nprepare_data_acceptance_root\n" in script


def _run_main_with_report(monkeypatch, status):
    engine = SimpleNamespace(dispose=lambda: None)
    delivered = []
    failed = status == acceptance.FAIL
    report = {
        "schema": acceptance.ACCEPTANCE_SCHEMA,
        "status": status,
        "acceptance_status": (
            "DATA_ACCEPTANCE_FAILED"
            if failed
            else "DATA_ACCEPTANCE_OK"
        ),
        "target_date": "2026-10-30",
        "observed_at": "2026-10-30T23:20:00",
        "failed_count": 1 if failed else 0,
        "checks": [
            acceptance.AcceptanceCheck(
                "membership",
                "membership",
                acceptance.FAIL if failed else acceptance.PASS,
                "stale" if failed else "ready",
                "rerun task 65" if failed else "",
                {},
            ).as_dict(),
        ],
    }
    monkeypatch.setattr(
        "server.common.batch_db.create_batch_engine",
        lambda: engine,
    )
    monkeypatch.setattr(
        acceptance,
        "collect_acceptance",
        lambda *args, **kwargs: report,
    )
    monkeypatch.setattr(
        acceptance,
        "persist_report",
        lambda report, **kwargs: Path("/tmp/2026-10-30.json"),
    )
    monkeypatch.setattr(
        "server.common.config.get_wecom_webhook",
        lambda kind, required=False: "https://example.invalid/hook",
    )

    def deliver(*args, **kwargs):
        delivered.append((args, kwargs))
        return SimpleNamespace(
            delivery_id="delivery-id",
            delivered_count=1,
            segment_count=1,
        )

    monkeypatch.setattr(
        "integrations.wecom.delivery.deliver_markdown",
        deliver,
    )
    code = acceptance.main(
        ["--target-date", "2026-10-30", "--json"]
    )
    return code, delivered


def test_failed_acceptance_sends_one_actionable_alert(monkeypatch):
    code, delivered = _run_main_with_report(monkeypatch, acceptance.FAIL)

    assert code == 2
    assert len(delivered) == 1
    args, kwargs = delivered[0]
    assert "membership" in args[1]
    assert "rerun task 65" in args[1]
    assert kwargs["delivery_kind"] == "daily_data_acceptance"


def test_successful_acceptance_stays_silent(monkeypatch):
    code, delivered = _run_main_with_report(monkeypatch, acceptance.PASS)

    assert code == 0
    assert delivered == []
