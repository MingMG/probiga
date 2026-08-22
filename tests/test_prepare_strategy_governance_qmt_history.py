from copy import deepcopy
from datetime import date, timedelta

import pytest

from tools import prepare_strategy_governance_qmt_history as preparation


def _sessions(count=120):
    start = date(2026, 1, 1)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _complete_result(sessions):
    daily = {
        day: {"stock_count": 2, "stock_set_hash": str(index % 10) * 64}
        for index, day in enumerate(sessions)
    }
    rows = len(sessions) * 2
    return {
        "run_id": "qmt_attest_test",
        "status": "COMPLETED",
        "apply": True,
        "attestation_protocol": preparation.ATTESTATION_PROTOCOL_VERSION,
        "universe_manifest_schema": preparation.UNIVERSE_MANIFEST_SCHEMA,
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "target_rows": rows,
        "qmt_rows": rows,
        "matched_rows": rows,
        "missing_qmt_rows": 0,
        "mismatched_rows": 0,
        "source_only_rows": 0,
        "daily_universe": daily,
    }


def test_schema_preparation_installs_then_strictly_validates(monkeypatch):
    calls = []
    monkeypatch.setattr(
        preparation,
        "ensure_attestation_tables",
        lambda engine: calls.append(("ensure", engine)),
    )
    monkeypatch.setattr(
        preparation,
        "validate_attestation_schema",
        lambda engine: calls.append(("validate", engine)) or {
            "table_count": 4,
            "trigger_count": 2,
        },
    )

    result = preparation.prepare_attestation_schema("engine")

    assert calls == [("ensure", "engine"), ("validate", "engine")]
    assert result == {
        "status": "ok",
        "mode": "schema-only",
        "attestation_protocol": preparation.ATTESTATION_PROTOCOL_VERSION,
        "table_count": 4,
        "trigger_count": 2,
        "automatic_real_order_submission": False,
    }


def test_prepare_requires_and_attests_exact_120_closed_sessions(monkeypatch):
    sessions = _sessions()
    monkeypatch.setattr(
        preparation,
        "authoritative_closed_trade_date",
        lambda _engine: sessions[-1],
    )
    monkeypatch.setattr(
        preparation,
        "_latest_closed_sessions",
        lambda _engine, *, target_trade_date: sessions,
    )
    calls = []

    def fake_attester(engine, **kwargs):
        calls.append((engine, kwargs))
        return _complete_result(sessions)

    result = preparation.prepare_governance_qmt_history(
        "engine", attester=fake_attester,
    )

    assert result["status"] == "ok"
    assert result["session_count"] == 120
    assert result["target_rows"] == 240
    assert calls == [("engine", {
        "start_date": sessions[0],
        "end_date": sessions[-1],
        "apply": True,
    })]


@pytest.mark.parametrize(
    "mutation",
    [
        lambda payload, sessions: payload.update(run_id=""),
        lambda payload, sessions: payload.update(status="PARTIAL"),
        lambda payload, sessions: payload.update(apply=False),
        lambda payload, sessions: payload.update(
            attestation_protocol="LEGACY_BATCH_V1"
        ),
        lambda payload, sessions: payload.update(
            universe_manifest_schema="unknown"
        ),
        lambda payload, sessions: payload.update(qmt_rows=239),
        lambda payload, sessions: payload.update(matched_rows=239),
        lambda payload, sessions: payload.update(missing_qmt_rows=1),
        lambda payload, sessions: payload.update(mismatched_rows=1),
        lambda payload, sessions: payload.update(source_only_rows=1),
        lambda payload, sessions: payload["daily_universe"].pop(sessions[0]),
        lambda payload, sessions: payload["daily_universe"][sessions[0]].update(
            stock_count=0
        ),
        lambda payload, sessions: payload["daily_universe"][sessions[0]].update(
            stock_count=1
        ),
        lambda payload, sessions: payload["daily_universe"][sessions[0]].update(
            stock_set_hash="A" * 64
        ),
        lambda payload, sessions: payload["daily_universe"][sessions[0]].update(
            extra="not-canonical"
        ),
    ],
)
def test_prepare_fails_closed_for_partial_or_inexact_attestation(
    monkeypatch, mutation,
):
    sessions = _sessions()
    monkeypatch.setattr(
        preparation,
        "authoritative_closed_trade_date",
        lambda _engine: sessions[-1],
    )
    monkeypatch.setattr(
        preparation,
        "_latest_closed_sessions",
        lambda _engine, *, target_trade_date: sessions,
    )
    payload = _complete_result(sessions)
    mutation(payload, sessions)

    with pytest.raises(
        preparation.GovernanceQmtHistoryNotReady,
        match="未完整覆盖120",
    ):
        preparation.prepare_governance_qmt_history(
            "engine", attester=lambda *_args, **_kwargs: deepcopy(payload),
        )


def test_schema_only_cli_fails_closed_and_disposes_engine(monkeypatch, capsys):
    class Engine:
        disposed = False

        def dispose(self):
            self.disposed = True

    engine = Engine()
    monkeypatch.setattr(preparation, "load_project_env", lambda: None)
    monkeypatch.setattr(
        preparation,
        "create_batch_engine",
        lambda **_kwargs: engine,
    )
    monkeypatch.setattr(
        preparation,
        "prepare_attestation_schema",
        lambda _engine: (_ for _ in ()).throw(RuntimeError("schema drift")),
    )

    assert preparation.main(["--schema-only"]) == 2
    payload = __import__("json").loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "schema drift" in payload["reason"]
    assert payload["automatic_real_order_submission"] is False
    assert engine.disposed is True


def test_latest_closed_sessions_requires_exact_count_and_high_watermark():
    class Result:
        def __init__(self, rows):
            self._rows = rows

        def mappings(self):
            return self

        def all(self):
            return self._rows

    class Connection:
        def __init__(self, rows):
            self.rows = rows

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, *_args, **_kwargs):
            return Result(self.rows)

    class Engine:
        def __init__(self, rows):
            self.rows = rows

        def connect(self):
            return Connection(self.rows)

    sessions = _sessions()
    engine = Engine([{"trade_date": day} for day in reversed(sessions)])
    assert preparation._latest_closed_sessions(
        engine, target_trade_date=sessions[-1],
    ) == sessions

    short_engine = Engine([
        {"trade_date": day} for day in reversed(sessions[:-1])
    ])
    with pytest.raises(preparation.GovernanceQmtHistoryNotReady):
        preparation._latest_closed_sessions(
            short_engine, target_trade_date=sessions[-1],
        )
