from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta
import json
import multiprocessing
import os
from pathlib import Path
import stat
from unittest.mock import patch

import pytest

from server.common.trading_day_store import (
    CHINA, JournalConflict, JournalStoreError, TradingDayStore, validate_journal_input,
)


DAY = "2024-07-12"
NOW = datetime(2024, 7, 12, 15, 1, tzinfo=CHINA)


@pytest.fixture(autouse=True)
def _clock(monkeypatch):
    monkeypatch.setattr("server.common.trading_day_store._now", lambda: NOW)


def _payload(revision=0):
    return {"revision": revision, "plans": [{
        "stock_code": "600519", "stock_name": "测试名称", "theme": "用户观察方向",
        "reason": "核对昨日趋势", "trigger": "用户填写的确认条件",
        "invalidation": "用户填写的失效条件", "source_as_of": "2024-07-12 09:08:00",
        "source_run_uid": "frozen-run", "status": "WATCHING", "note": "",
    }], "review": {"text": "待验证的个人判断"}}


def _concurrent_save(root: str, start, label: str):
    store = TradingDayStore(Path(root), lock_timeout=10)
    payload = _payload()
    payload["review"]["text"] = label
    start.wait(10)
    try:
        with patch("server.common.trading_day_store._now", return_value=NOW):
            return "saved", store.save(7, DAY, payload)["revision"]
    except JournalConflict as exc:
        return "conflict", exc.revision


def test_persists_across_instances_and_isolates_user_and_day(tmp_path):
    root = tmp_path / "journals"
    first = TradingDayStore(root)
    assert first.read(7, DAY)["revision"] == 0
    saved = first.save(7, DAY, _payload())
    second = TradingDayStore(root)
    assert second.read(7, DAY) == saved
    assert second.read(8, DAY)["plans"] == []
    assert second.read(7, "2024-07-15")["plans"] == []
    assert all(path.is_file() for path in root.iterdir())
    assert all(path.name.startswith("trading-day-user-") for path in root.iterdir())
    assert not list(root.glob("*.tmp"))
    if os.name != "nt":
        assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in root.iterdir())


def test_changes_preserve_original_conditions_and_creation_time(tmp_path):
    store = TradingDayStore(tmp_path / "journals")
    saved = store.save(7, DAY, _payload())
    changed = _payload(1)
    changed["plans"][0].update(trigger="修订确认条件", status="REVIEWED", note="今天未满足条件")
    changed["review"]["text"] = ""
    revised = store.save(7, DAY, changed)
    old, new = saved["plans"][0], revised["plans"][0]
    assert new["trigger"] == "修订确认条件"
    assert new["original"]["trigger"] == old["trigger"]
    assert new["original"] == old["original"]
    assert new["created_at"] == old["created_at"]
    assert new["updated_at"] >= old["updated_at"]
    assert revised["review"]["text"] == ""
    assert revised["review"]["updated_at"]
    unchanged = store.save(7, DAY, {**changed, "revision": 2})
    assert unchanged["plans"][0]["updated_at"] == new["updated_at"]


def test_conflict_does_not_overwrite_and_removal_requires_current_revision(tmp_path):
    store = TradingDayStore(tmp_path / "journals")
    saved = store.save(7, DAY, _payload())
    with pytest.raises(JournalConflict) as conflict:
        store.save(7, DAY, {"revision": 0, "plans": [], "review": {"text": "stale"}})
    assert conflict.value.revision == 1
    assert store.read(7, DAY) == saved
    removed = store.save(7, DAY, {"revision": 1, "plans": [], "review": {"text": "已移出观察"}})
    assert removed["plans"] == []
    assert removed["revision"] == 2


def test_separate_processes_cannot_both_commit_same_revision(tmp_path):
    root = tmp_path / "journals"
    TradingDayStore(root)
    context = multiprocessing.get_context("spawn")
    with context.Manager() as manager:
        start = manager.Event()
        with ProcessPoolExecutor(max_workers=2, mp_context=context) as pool:
            jobs = [pool.submit(_concurrent_save, str(root), start, str(index)) for index in range(2)]
            start.set()
            outcomes = sorted(job.result(timeout=30) for job in jobs)
    assert outcomes == [("conflict", 1), ("saved", 1)]
    assert TradingDayStore(root).read(7, DAY)["revision"] == 1


def test_failed_atomic_replace_keeps_previous_document(tmp_path, monkeypatch):
    store = TradingDayStore(tmp_path / "journals")
    saved = store.save(7, DAY, _payload())
    def fail_replace(*args):
        raise OSError("simulated disk failure")
    monkeypatch.setattr("server.common.trading_day_store.os.replace", fail_replace)
    with pytest.raises(OSError):
        store.save(7, DAY, _payload(1))
    assert store.read(7, DAY) == saved
    assert not list(store.root.glob("*.tmp"))


def test_corrupt_or_different_identity_fails_closed(tmp_path):
    store = TradingDayStore(tmp_path / "journals")
    store.save(7, DAY, _payload())
    path = store.root / f"trading-day-user-7-{DAY}.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["user_id"] = 8
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(JournalStoreError):
        store.read(7, DAY)
    path.write_text("broken", encoding="utf-8")
    with pytest.raises(JournalStoreError):
        store.save(7, DAY, _payload())
    assert path.read_text(encoding="utf-8") == "broken"


def test_rejects_linked_state_and_unsafe_root(tmp_path):
    store = TradingDayStore(tmp_path / "journals")
    store.save(7, DAY, _payload())
    path = store.root / f"trading-day-user-7-{DAY}.json"
    os.link(path, tmp_path / "linked-state")
    with pytest.raises(JournalStoreError):
        store.read(7, DAY)
    with pytest.raises(JournalStoreError):
        TradingDayStore(Path("relative-root"))
    with pytest.raises(JournalStoreError):
        TradingDayStore(Path(__file__).resolve().parents[1] / "journal-state")


@pytest.mark.parametrize("user_id,day", [(True, DAY), (0, DAY), ("7", DAY), (7, "../../escape"), (7, "2024-02-30")])
def test_identity_and_date_cannot_escape_root(tmp_path, user_id, day):
    store = TradingDayStore(tmp_path / "journals")
    with pytest.raises(ValueError):
        store.read(user_id, day)
    assert list(store.root.iterdir()) == []


@pytest.mark.parametrize("field,value", [
    ("stock_code", "000000"), ("stock_code", "../../"), ("stock_code", "１２３４５６"),
    ("trigger", "x" * 1201), ("note", "\x00"), ("source_as_of", "not a date"),
    ("source_as_of", "2099-01-01"), ("source_as_of", "2024-07-13"),
    ("status", "BUY"), ("status", []), ("user_id", 12),
])
def test_plan_validation_rejects_invalid_or_future_evidence(field, value):
    payload = _payload()
    payload["plans"][0][field] = value
    with pytest.raises(ValueError):
        validate_journal_input(payload, DAY)


def test_rejects_duplicate_and_excess_plans():
    payload = _payload()
    payload["plans"] *= 2
    with pytest.raises(ValueError):
        validate_journal_input(payload, DAY)
    payload["plans"] = [deepcopy(_payload()["plans"][0]) for _ in range(101)]
    with pytest.raises(ValueError):
        validate_journal_input(payload, DAY)


def test_historical_journal_allows_supplemental_notes_but_preserves_observation_conditions(tmp_path, monkeypatch):
    store = TradingDayStore(tmp_path / "journals")
    original = store.save(7, DAY, _payload())
    later = NOW + timedelta(days=1)
    monkeypatch.setattr("server.common.trading_day_store._now", lambda: later)
    changed = _payload(1)
    changed["plans"][0].update(status="REVIEWED", note="次日补记：没有确认")
    changed["review"]["text"] = "次日补复盘"
    saved = store.save(7, DAY, changed)
    assert saved["plans"][0]["created_at"] == original["plans"][0]["created_at"]
    assert saved["plans"][0]["original"] == original["plans"][0]["original"]
    assert saved["plans"][0]["updated_at"] == later.isoformat(timespec="microseconds")
    assert saved["review"]["updated_at"] == later.isoformat(timespec="microseconds")
    changed["revision"] = 2
    changed["plans"][0]["trigger"] = "事后改写条件"
    with pytest.raises(ValueError, match="conditions"):
        store.save(7, DAY, changed)
    with pytest.raises(ValueError, match="added or removed"):
        store.save(7, DAY, {"revision": 2, "plans": [], "review": {"text": ""}})
    assert store.read(7, DAY) == saved


def test_historical_notes_can_be_added_without_backdated_plans_and_future_edits_are_rejected(tmp_path):
    store = TradingDayStore(tmp_path / "journals")
    history_day = "2024-07-11"
    journal = store.save(7, history_day, {"revision": 0, "plans": [], "review": {"text": "补记"}})
    assert journal["updated_at"].startswith(DAY)
    payload = _payload(1)
    payload["plans"][0]["source_as_of"] = history_day
    with pytest.raises(ValueError, match="added or removed"):
        store.save(7, history_day, payload)
    for plans in ([], _payload()["plans"]):
        with pytest.raises(ValueError, match="future journals"):
            store.save(7, "2024-07-13", {"revision": 0, "plans": plans, "review": {"text": "预填"}})
    assert store.read(7, "2024-07-13")["revision"] == 0


@pytest.mark.parametrize("mutate", [
    lambda document: document["plans"][0].update(status="BUY"),
    lambda document: document["plans"][0].update(trigger=[]),
    lambda document: document["plans"][0].pop("source_run_uid"),
    lambda document: document["plans"][0]["original"].update(invalidation=[]),
    lambda document: document["plans"][0].update(updated_at="2024-07-12T08:00:00+08:00"),
    lambda document: document["review"].update(updated_at=None),
    lambda document: document.update(revision=True),
])
def test_damaged_plan_evidence_fails_closed(tmp_path, mutate):
    store = TradingDayStore(tmp_path / "journals")
    store.save(7, DAY, _payload())
    path = store.root / f"trading-day-user-7-{DAY}.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    mutate(document)
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(JournalStoreError):
        store.read(7, DAY)
