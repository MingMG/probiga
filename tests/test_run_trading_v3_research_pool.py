from datetime import date, datetime
import gzip
import hashlib
import json
import sys
from zoneinfo import ZoneInfo

import pytest

from tools import run_trading_v3_research_pool as runner


def test_packaged_seed_is_the_verified_0904_research_artifact():
    target = date(2026, 9, 4)
    payload, source_bytes = runner._load_packaged_seed(target)
    verified = runner.validate_research_payload(
        payload,
        expected_date=target,
        now=datetime(2026, 9, 7, 7, 30),
    )
    assert len(source_bytes) < runner.MAX_RESEARCH_PAYLOAD_BYTES
    assert verified["artifact_sha256"] == (
        "5d40cb3cffeb64cf0aba4f4945f12a588efead8ccb2068d8f35088f060ca3b48"
    )
    assert verified["forecast_count"] == 2400
    observation_statuses = {
        "VALIDATED_POSITIVE",
        "PAPER_DISCOVERY_CANDIDATE",
        "LEFT_SIDE_PREPARE",
        "RESEARCH_ONLY_UNCALIBRATED",
    }
    observation_rows = [
        row for row in verified["forecasts"]
        if row.get("status") in observation_statuses
    ]
    assert len(observation_rows) == 524
    assert len({row["stock_code"] for row in observation_rows}) == 466


def test_packaged_seed_publishes_without_recomputing(monkeypatch, tmp_path):
    target = date(2026, 9, 4)
    source_bytes = json.dumps({"fixed": "payload"}).encode("utf-8")
    seed_root = tmp_path / "research-pools"
    seed_root.mkdir()
    with gzip.GzipFile(
        filename=str(seed_root / "2026-09-04.json.gz"),
        mode="wb",
        mtime=0,
    ) as stream:
        stream.write(source_bytes)
    payload = json.loads(source_bytes)
    compressed_bytes = (seed_root / "2026-09-04.json.gz").read_bytes()
    current = datetime(2026, 9, 7, 7, 35)
    artifact_hash = "5" * 64
    seen = []

    monkeypatch.setattr(runner, "PACKAGED_RESEARCH_POOL_ROOT", seed_root)
    monkeypatch.setattr(
        runner,
        "PACKAGED_RESEARCH_POOL_SEEDS",
        {
            target: {
                "filename": "2026-09-04.json.gz",
                "gzip_sha256": hashlib.sha256(compressed_bytes).hexdigest(),
                "payload_file_sha256": hashlib.sha256(source_bytes).hexdigest(),
            }
        },
    )
    monkeypatch.setattr(
        runner,
        "authoritative_closed_trade_date",
        lambda engine, *, now: target.isoformat(),
    )

    def validate(result, *, expected_date, now):
        assert result == payload
        assert expected_date == target
        assert now == current
        seen.append("validate")
        return {
            "artifact_sha256": artifact_hash,
            "research_known_at": datetime(2026, 9, 7, 0, 2, 41),
        }

    def publish(result, **kwargs):
        assert result == payload
        assert kwargs["publisher_build_sha"] == "a" * 40
        assert kwargs["published_at"] == current
        assert kwargs["source_bytes"] == source_bytes
        seen.append("publish")
        return {
            "artifact_sha256": artifact_hash,
            "payload_file_sha256": "e" * 64,
            "publisher_build_sha": "a" * 40,
        }

    def readback(day, *, now):
        assert day == target
        assert now == current
        seen.append("readback")
        return {
            "pool_readable": True,
            "status": "READY",
            "trade_date": target.isoformat(),
            "artifact_sha256": artifact_hash,
            "payload_file_sha256": "e" * 64,
            "publisher_build_sha": "a" * 40,
            "summary": {"observation_stock_count": 466},
        }

    monkeypatch.setattr(runner, "validate_research_payload", validate)
    monkeypatch.setattr(runner, "publish_research_pool", publish)
    monkeypatch.setattr(runner, "read_research_pool", readback)
    monkeypatch.setattr(runner, "code_version", lambda: ("a" * 40, "test"))
    monkeypatch.setattr(
        runner,
        "run_retrospective_research_v3",
        lambda *a, **k: pytest.fail("packaged seed must not recompute"),
    )
    result = runner.publish_packaged_research_pool(
        object(),
        target=target,
        now=current,
    )
    assert seen == ["validate", "publish", "readback"]
    assert result["source"] == "PACKAGED_VERIFIED_RESEARCH_SEED"
    assert result["readback"]["summary"]["observation_stock_count"] == 466


def test_packaged_seed_rejects_a_non_authoritative_date_before_loading(monkeypatch):
    monkeypatch.setattr(
        runner,
        "authoritative_closed_trade_date",
        lambda *a, **k: "2026-09-07",
    )
    monkeypatch.setattr(
        runner,
        "_load_packaged_seed",
        lambda *a, **k: pytest.fail("must not load a seed for a different date"),
    )
    with pytest.raises(ValueError, match="authoritative closed session"):
        runner.publish_packaged_research_pool(
            object(),
            target=date(2026, 9, 4),
            now=datetime(2026, 9, 7, 18, 1),
        )


def test_packaged_seed_cli_does_not_create_a_kline_engine(monkeypatch, capsys):
    primary = type("Primary", (), {"dispose": lambda self: None})()
    monkeypatch.setattr(sys, "argv", ["runner", "--from-packaged-seed", "2026-09-04"])
    monkeypatch.setattr(runner, "load_project_env", lambda: None)
    monkeypatch.setattr(runner, "create_tool_engine", lambda: primary)
    monkeypatch.setattr(
        runner,
        "get_kline_engine",
        lambda: pytest.fail("packaged seed must not open the kline database"),
    )
    monkeypatch.setattr(
        runner,
        "publish_packaged_research_pool",
        lambda engine, *, target: {
            "status": "completed",
            "trade_date": target.isoformat(),
        },
    )
    assert runner.main() == 0
    assert json.loads(capsys.readouterr().out)["trade_date"] == "2026-09-04"


def test_monday_research_uses_friday_facts_and_actual_monday_knowledge(monkeypatch):
    primary, kline = object(), object()
    captured = {}
    payload = {"test": "retrospective artifact"}

    def resolve(engine, *, now):
        assert engine is primary
        assert now.date() == date(2026, 9, 7)
        return "2026-09-04"

    def research(engine, **kwargs):
        assert engine is primary
        captured.update(kwargs)
        return payload

    def publish(result, *, publisher_build_sha):
        assert result is payload
        assert result["notification"] == {"status": "suppressed", "reason": "RETROSPECTIVE_RESEARCH"}
        assert publisher_build_sha == "a" * 40
        return {
            "status": "ok",
            "trade_date": "2026-09-04",
            "artifact_sha256": "b" * 64,
            "payload_file_sha256": "c" * 64,
        }

    def readback(target):
        assert target == date(2026, 9, 4)
        return {
            "status": "READY",
            "pool_readable": True,
            "trade_date": "2026-09-04",
            "artifact_sha256": "b" * 64,
            "payload_file_sha256": "c" * 64,
            "summary": {"observation_stock_count": 3},
        }

    monkeypatch.setattr(runner, "authoritative_closed_trade_date", resolve)
    monkeypatch.setattr(runner, "run_retrospective_research_v3", research)
    monkeypatch.setattr(runner, "publish_research_pool", publish)
    monkeypatch.setattr(runner, "read_research_pool", readback)
    monkeypatch.setattr(runner, "code_version", lambda: ("a" * 40, "test"))
    result = runner.generate_research_pool(
        primary,
        kline_engine=kline,
        now=datetime(2026, 9, 6, 16, 10, tzinfo=ZoneInfo("UTC")),
    )

    assert captured["as_of"] == date(2026, 9, 4)
    assert captured["decision_at"] == datetime(2026, 9, 4, 23, 59, 59, 999999)
    assert captured["research_known_at"] == datetime(2026, 9, 7, 0, 10)
    assert captured["kline_engine"] is kline
    assert captured["resolve_fact_cutoff_from_evidence"] is True
    assert result["database_writes"] is False
    assert result["order_authority"] is False
    assert result["notification_eligible"] is False


def test_research_job_rejects_unclosed_session_before_calculation(monkeypatch):
    monkeypatch.setattr(runner, "authoritative_closed_trade_date", lambda *a, **k: "2026-09-07")
    monkeypatch.setattr(runner, "run_retrospective_research_v3", lambda *a, **k: pytest.fail("must not calculate current session"))
    with pytest.raises(ValueError, match="completed session"):
        runner.generate_research_pool(object(), kline_engine=object(), now=datetime(2026, 9, 7, 17))


def test_same_day_research_publishes_when_computation_completes(monkeypatch):
    seen = []
    monkeypatch.setattr(runner, "authoritative_closed_trade_date", lambda *a, **k: "2026-09-07")

    def compute(*a, **kwargs):
        assert kwargs["decision_at"] == datetime(2026, 9, 7, 18)
        assert kwargs["research_known_at"] == datetime(2026, 9, 7, 22, 10)
        assert kwargs["resolve_fact_cutoff_from_evidence"] is True
        seen.append("compute")
        return {}

    def publish(*a, **kwargs):
        seen.append("publish")
        return {
            "status": "ok",
            "trade_date": "2026-09-07",
            "artifact_sha256": "b" * 64,
            "payload_file_sha256": "c" * 64,
        }

    def readback(target):
        seen.append("readback")
        assert target == date(2026, 9, 7)
        return {
            "status": "READY",
            "pool_readable": True,
            "trade_date": "2026-09-07",
            "artifact_sha256": "b" * 64,
            "payload_file_sha256": "c" * 64,
            "summary": {"observation_stock_count": 2},
        }

    monkeypatch.setattr(runner, "run_retrospective_research_v3", compute)
    monkeypatch.setattr(runner, "publish_research_pool", publish)
    monkeypatch.setattr(runner, "read_research_pool", readback)
    monkeypatch.setattr(runner, "code_version", lambda: ("a" * 40, "test"))
    result = runner.generate_research_pool(object(), kline_engine=object(), now=datetime(2026, 9, 7, 22, 10))
    assert seen == ["compute", "publish", "readback"]
    assert result["publication"]["status"] == "ok"
    assert result["readback"]["summary"]["observation_stock_count"] == 2


def test_research_job_fails_if_published_hash_cannot_be_read_back(monkeypatch):
    monkeypatch.setattr(
        runner,
        "authoritative_closed_trade_date",
        lambda *a, **k: "2026-09-04",
    )
    monkeypatch.setattr(
        runner,
        "run_retrospective_research_v3",
        lambda *a, **k: {},
    )
    monkeypatch.setattr(
        runner,
        "publish_research_pool",
        lambda *a, **k: {
            "artifact_sha256": "b" * 64,
            "payload_file_sha256": "c" * 64,
        },
    )
    monkeypatch.setattr(
        runner,
        "read_research_pool",
        lambda *a, **k: {
            "status": "READY",
            "pool_readable": True,
            "trade_date": "2026-09-04",
            "artifact_sha256": "d" * 64,
            "payload_file_sha256": "c" * 64,
        },
    )
    monkeypatch.setattr(runner, "code_version", lambda: ("a" * 40, "test"))
    with pytest.raises(RuntimeError, match="exact readback"):
        runner.generate_research_pool(
            object(),
            kline_engine=object(),
            now=datetime(2026, 9, 7, 0, 10),
        )


def test_research_job_fails_if_readback_contains_no_observation_candidates(
    monkeypatch,
):
    monkeypatch.setattr(
        runner,
        "authoritative_closed_trade_date",
        lambda *a, **k: "2026-09-04",
    )
    monkeypatch.setattr(
        runner,
        "run_retrospective_research_v3",
        lambda *a, **k: {},
    )
    monkeypatch.setattr(
        runner,
        "publish_research_pool",
        lambda *a, **k: {
            "artifact_sha256": "b" * 64,
            "payload_file_sha256": "c" * 64,
        },
    )
    monkeypatch.setattr(
        runner,
        "read_research_pool",
        lambda *a, **k: {
            "status": "EMPTY",
            "pool_readable": True,
            "trade_date": "2026-09-04",
            "artifact_sha256": "b" * 64,
            "payload_file_sha256": "c" * 64,
            "summary": {
                "observation_stock_count": 0,
                "total_forecast_count": 2400,
                "excluded_forecast_count": 2400,
            },
        },
    )
    monkeypatch.setattr(runner, "code_version", lambda: ("a" * 40, "test"))
    with pytest.raises(
        RuntimeError,
        match=(
            "NO_RESEARCH_OBSERVATION_CANDIDATES: "
            "total_forecast_count=2400, excluded_forecast_count=2400"
        ),
    ):
        runner.generate_research_pool(
            object(),
            kline_engine=object(),
            now=datetime(2026, 9, 7, 0, 10),
        )


def test_research_job_recovers_missed_cron_but_does_not_repeat_success():
    from server.api import scheduler_runtime
    from tools.add_trading_v3_tasks import TASKS

    definition = next(row for row in TASKS if row["task_type"] == "trading_v3_research_pool")
    row = dict(definition, last_run_status="pending", last_triggered_at=None)
    assert scheduler_runtime._critical_cron_catchup_allowed(
        row, now=datetime(2026, 9, 7, 23), cron_time=definition["cron_time"]
    )
    row.update(last_run_status="success", last_triggered_at=datetime(2026, 9, 7, 22, 15))
    assert not scheduler_runtime._critical_cron_catchup_allowed(
        row, now=datetime(2026, 9, 7, 23), cron_time=definition["cron_time"]
    )
    assert definition["task_type"] in scheduler_runtime.NON_TRADING_DAY_SKIP_TYPES


def test_scheduler_does_not_pass_a_bare_date_to_the_research_runner():
    from server.api import scheduler_runtime
    from tools.add_trading_v3_tasks import TASKS

    definition = next(
        row for row in TASKS
        if row["task_type"] == "trading_v3_research_pool"
    )
    assert scheduler_runtime._build_task_args(
        dict(definition),
        definition["script_path"],
        "2026-09-07",
    ) == []


def test_research_api_keeps_projection_separate_from_formal_pool(monkeypatch):
    from server.api.routers import trading_v3
    from server.trading_v3 import research_pool

    target = date(2026, 9, 4)
    projection = {"status": "AVAILABLE", "trade_date": target.isoformat(), "canonical_eligible": False, "items": []}
    monkeypatch.setattr(research_pool, "read_research_pool", lambda day: projection if day == target else pytest.fail("date changed"))
    monkeypatch.setattr(trading_v3, "_envelope", lambda data: data)
    assert trading_v3.research_stock_pool(target) is projection
