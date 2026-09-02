from __future__ import annotations

import gzip
import json
import time

import pytest

from integrations.bigqmt import spool
from integrations.bigqmt.qmt_strategy import probiga_big_qmt_bridge as producer


def _queue_roots(monkeypatch: pytest.MonkeyPatch, tmp_path):
    roots = {
        name: tmp_path / name
        for name in (
            "requests",
            "responses",
            "inflight",
            "checkpoints",
            "dead_letter",
            "cancelled",
        )
    }
    for path in roots.values():
        path.mkdir()
    monkeypatch.setattr(producer, "_bridge_root", str(tmp_path))
    monkeypatch.setattr(producer, "_requests_root", str(roots["requests"]))
    monkeypatch.setattr(producer, "_responses_root", str(roots["responses"]))
    monkeypatch.setattr(producer, "_model_instance_id", "model-instance-1")
    return roots


def _request_payload(request_id: str, action: str) -> dict:
    return {
        "schema_version": 3,
        "request_id": request_id,
        "action": action,
        "deadline_ts": time.time() + 60,
        "attempt": 1,
        "cursor": 0,
        "params": {},
    }


def test_control_requests_preempt_already_queued_bulk_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    roots = _queue_roots(monkeypatch, tmp_path)
    (roots["requests"] / "090_1_bulk.json").write_text(
        json.dumps(_request_payload("bulk", "sector_members_many")),
        encoding="utf-8",
    )
    (roots["requests"] / "000_2_ping.json").write_text(
        json.dumps(_request_payload("ping", "ping")),
        encoding="utf-8",
    )
    calls: list[str] = []
    monkeypatch.setattr(
        producer,
        "_execute_request",
        lambda _context, action, _params: calls.append(action) or {},
    )
    monkeypatch.setattr(producer, "_write_heartbeat", lambda _status: None)

    assert producer._process_one_request(object()) is True
    assert producer._process_one_request(object()) is True

    assert calls == ["ping", "sector_members_many"]
    assert list(roots["inflight"].iterdir()) == []


def test_restart_requeues_live_claim_and_dead_letters_cancelled_claim(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    roots = _queue_roots(monkeypatch, tmp_path)
    live_name = "090_1_live.json"
    cancelled_name = "090_2_cancelled.json"
    (roots["inflight"] / live_name).write_text(
        json.dumps(_request_payload("live", "instrument_details")),
        encoding="utf-8",
    )
    (roots["inflight"] / cancelled_name).write_text(
        json.dumps(_request_payload("cancelled", "instrument_details")),
        encoding="utf-8",
    )
    (roots["cancelled"] / "cancelled.json").write_text("{}", encoding="utf-8")

    producer._recover_inflight_requests()

    assert (roots["requests"] / live_name).is_file()
    assert not (roots["inflight"] / cancelled_name).exists()
    dead_letters = list(roots["dead_letter"].glob("cancelled_*.json"))
    assert dead_letters
    metadata = json.loads(
        (dead_letters[0].with_name(dead_letters[0].name + ".meta.json"))
        .read_text(encoding="utf-8")
    )
    assert metadata["reason"] == "cancelled_during_restart"


def test_idempotent_request_reuses_completed_response(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    sleeps = 0

    def complete_request(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        paths = spool.bridge_paths(tmp_path)
        request_path = next(paths["requests"].glob("*.json"))
        request_payload = json.loads(request_path.read_text(encoding="utf-8"))
        response_path = paths["responses"] / (
            request_payload["request_id"] + ".json.gz"
        )
        with gzip.open(response_path, "wt", encoding="utf-8") as handle:
            json.dump(
                {
                    "request_id": request_payload["request_id"],
                    "status": "ok",
                    "rows": [{"stock_code": "000001"}],
                },
                handle,
            )

    monkeypatch.setattr(spool.time, "sleep", complete_request)
    first = spool.request(
        "instrument_details",
        qmt_home=tmp_path,
        timeout=2,
        idempotency_key="catalog:2026-09-02:0",
    )
    second = spool.request(
        "instrument_details",
        qmt_home=tmp_path,
        timeout=2,
        idempotency_key="catalog:2026-09-02:0",
    )

    assert first == second
    assert sleeps == 1
    assert len(list(spool.bridge_paths(tmp_path)["responses"].glob("*.json.gz"))) == 1


def test_timed_out_request_is_cancelled_instead_of_left_pending(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    clock = [0.0]

    def monotonic() -> float:
        clock[0] += 0.6
        return clock[0]

    monkeypatch.setattr(spool.time, "monotonic", monotonic)
    monkeypatch.setattr(spool.time, "sleep", lambda _seconds: None)

    with pytest.raises(TimeoutError, match="timed out"):
        spool.request("kline", qmt_home=tmp_path, timeout=1)

    paths = spool.bridge_paths(tmp_path)
    assert list(paths["requests"].glob("*.json")) == []
    assert len(list(paths["cancelled"].glob("*.json"))) == 2
