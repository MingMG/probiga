from __future__ import annotations

import json
from datetime import datetime

import pytest

from biz.intraday_alert.core import (
    DeliveryDispatchError,
    QualityGateError,
    advance_states,
    drain_outbox,
    make_observation_id,
    prepare_benchmark_snapshot,
    validate_qmt_snapshot,
)


NOW = datetime(2026, 8, 13, 10, 15, 30)


def _receipt(**overrides):
    value = {
        "receipt_id": "receipt-a",
        "source_provider": "gj_big_qmt_inner",
        "source_snapshot_token": "1723515330",
        "source_generated_at": datetime(2026, 8, 13, 10, 15, 0),
        "published_at": datetime(2026, 8, 13, 10, 15, 10),
        "capture_mode": "LIVE_FORWARD",
        "quality_status": "PASS",
        "expected_count": 100,
        "observed_count": 100,
        "coverage": 1.0,
        "evidence_json": json.dumps({"full_batch_id": "batch-a"}),
    }
    value.update(overrides)
    return value


def _rows(count=100, batch="batch-a"):
    return [
        {
            "stock_code": f"{index:06d}",
            "short_name": f"股票{index}",
            "price": 10.0,
            "change_pct": (index % 10) / 10,
            "amount": 1_000_000 + index,
            "data_source": "gj_big_qmt_inner",
            "batch_id": batch,
        }
        for index in range(count)
    ]


def _event(state="ENHANCED"):
    return {
        "event_key": "MARKET:breadth:UP",
        "event_type": "MARKET_STATE",
        "subject_code": "breadth",
        "subject_name": "全市场广度",
        "direction": "UP",
        "target_state": state,
        "severity": 2,
        "evidence": {"breadth": 72.0},
    }


def test_quality_gate_accepts_exact_fresh_full_batch():
    result = validate_qmt_snapshot(_receipt(), _rows(), now=NOW)
    assert len(result.rows) == 100
    assert result.warnings == ()


@pytest.mark.parametrize(
    ("receipt", "rows", "code"),
    [
        (_receipt(coverage=0.94), _rows(), "receipt_low_coverage"),
        (_receipt(source_generated_at=datetime(2026, 8, 13, 10, 12)), _rows(), "receipt_stale"),
        (_receipt(), _rows(94), "snapshot_row_shortfall"),
        (_receipt(), _rows(batch="other-batch"), "snapshot_batch_mismatch"),
    ],
)
def test_quality_gate_fails_closed(receipt, rows, code):
    with pytest.raises(QualityGateError) as captured:
        validate_qmt_snapshot(receipt, rows, now=NOW)
    assert captured.value.code == code


def test_unparseable_batch_is_explicit_warning_not_false_verification():
    receipt = _receipt(evidence_json="{}", source_snapshot_token="timestamp-token")
    result = validate_qmt_snapshot(receipt, _rows(), now=NOW)
    assert result.warnings == ("receipt_batch_unverifiable",)


def test_same_source_receipt_is_idempotent_observation_id():
    first = make_observation_id(_receipt())
    second = make_observation_id({**_receipt(), "published_at": NOW})
    assert first == second
    assert first != make_observation_id(_receipt(receipt_id="receipt-b"))


def test_state_emits_transition_once_and_absence_does_not_claim_invalidation():
    states, emissions = advance_states({}, [_event()], now=NOW)
    assert [item["transition_name"] for item in emissions] == ["ENHANCED"]
    previous = {states[0]["event_key"]: states[0]}

    states, emissions = advance_states(previous, [_event()], now=NOW)
    assert emissions == []
    previous = {states[0]["event_key"]: states[0]}

    states, emissions = advance_states(previous, [], now=NOW)
    assert states[0]["state"] == "ENHANCED"
    assert emissions == []


def test_explicit_opposite_evidence_invalidates_and_upgrade_bypasses_cooldown():
    states, _ = advance_states({}, [_event()], now=NOW)
    previous = {states[0]["event_key"]: states[0]}
    confirmed = _event("CONFIRMED")
    states, emissions = advance_states(previous, [confirmed], now=NOW)
    assert [item["transition_name"] for item in emissions] == ["CONFIRMED"]
    previous = {states[0]["event_key"]: states[0]}
    invalidated = {**_event("INVALIDATED"), "facts": ["反向量价证据已出现"]}
    states, emissions = advance_states(previous, [invalidated], now=NOW)
    assert states[0]["state"] == "INVALIDATED"
    assert [item["transition_name"] for item in emissions] == ["INVALIDATED"]


def test_shadow_mode_never_resolves_webhook_or_calls_http():
    called = []

    def forbidden(*args, **kwargs):
        called.append((args, kwargs))
        raise AssertionError("must not be called")

    assert drain_outbox(
        object(),
        mode="shadow",
        now=NOW,
        delivery_fn=forbidden,
        webhook_getter=forbidden,
    ) == 0
    assert called == []


class _Result:
    def __init__(self, row=None):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row


class _Connection:
    def __init__(self):
        self.claimed = False
        self.statements = []

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        self.statements.append((sql, dict(params or {})))
        if "SELECT outbox_id, content_markdown, attempts" in sql:
            if self.claimed:
                return _Result(None)
            self.claimed = True
            return _Result({"outbox_id": "outbox-a", "content_markdown": "消息", "attempts": 0})
        return _Result()


class _Begin:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, tb):
        return False


class _Engine:
    def __init__(self):
        self.connection = _Connection()

    def begin(self):
        return _Begin(self.connection)


class _BenchmarkConnection:
    def __init__(self):
        self.inserts = []

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        if sql.startswith("SELECT amount"):
            return _ScalarResult(None)
        if sql.startswith("SELECT amount_delta"):
            return _RowsResult([])
        if sql.startswith("INSERT INTO"):
            self.inserts.append(dict(params or {}))
            return _Result()
        raise AssertionError(sql)


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _RowsResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


def test_stale_benchmark_quote_is_excluded():
    connection = _BenchmarkConnection()
    result = prepare_benchmark_snapshot(
        connection,
        [
            {
                "instrument_code": "510300",
                "instrument_name": "沪深300ETF",
                "instrument_type": "ETF",
                "price": 4.0,
                "change_pct": 0.2,
                "amount": 10_000,
                "source_provider": "eastmoney_ulist",
                "source_time": datetime(2026, 8, 13, 10, 10),
                "quality_status": "PASS",
            }
        ],
        trade_date=NOW.date(),
        observed_at=NOW,
        minute_number=45,
    )
    assert result["available"] is False
    assert connection.inserts == []


def test_render_adapter_passes_complete_observation(monkeypatch):
    import biz.intraday_alert.core as core
    import biz.intraday_alert.render as renderer

    captured = {}

    def fake_render(event, observation):
        captured.update(observation)
        return "ok"

    monkeypatch.setattr(renderer, "render_event", fake_render)
    content = core._render_emission(_event(), {"coverage": 0.99, "source_provider": "qmt"})
    assert content == "ok"
    assert captured == {"coverage": 0.99, "source_provider": "qmt"}


def test_live_failure_is_durable_and_nonzero():
    engine = _Engine()

    def fail_delivery(*args, **kwargs):
        raise RuntimeError("transport failed")

    with pytest.raises(DeliveryDispatchError):
        drain_outbox(
            engine,
            mode="live",
            now=NOW,
            delivery_fn=fail_delivery,
            webhook_getter=lambda *args, **kwargs: "configured-secret-not-stored",
        )
    failed_updates = [
        params for sql, params in engine.connection.statements
        if "SET status='FAILED', next_retry_at" in sql and params.get("outbox_id") == "outbox-a"
    ]
    assert failed_updates
    assert failed_updates[0]["next_retry_at"] > NOW
