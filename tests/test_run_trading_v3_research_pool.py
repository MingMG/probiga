from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from tools import run_trading_v3_research_pool as runner


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
