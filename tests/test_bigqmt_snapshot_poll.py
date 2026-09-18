from datetime import datetime
from types import SimpleNamespace

import pytest

from test_bigqmt_callback_liveness import load_producer


def configured(monkeypatch, count=2, tracked=1):
    p = load_producer()
    p._all_codes = ["%06d.SZ" % n for n in range(count)]
    p._tracked_codes = p._all_codes[:tracked]
    p._load_config = lambda **kw: False
    p._quote_phase = lambda now: ("live", "2026-09-18")
    clock = [datetime(2026, 9, 18, 10, 0).timestamp()]
    monkeypatch.setattr(p.time, "time", lambda: clock[0])
    writes = []
    p._atomic_write = lambda name, payload: writes.append((name, payload))
    p._refresh_quote_universe(force=True)
    return p, clock, writes


def tick(clock, price=10):
    return {"time": int(clock[0] * 1000), "lastPrice": price,
            "bidPrice": [price - .01], "askPrice": [price + .01]}


def test_bounded_full_market_sweep_keeps_tracked_fresh_within_publication_budget(monkeypatch):
    p, clock, writes = configured(monkeypatch, count=5563, tracked=280)
    calls = []
    def native(codes):
        calls.append(list(codes))
        return {code: tick(clock) for code in codes}
    context = SimpleNamespace(get_full_tick=native)
    start = clock[0]
    for _ in range(20):
        p._refresh_full_snapshot(context)
        published = [payload for name, payload in writes if name == "full_quotes.json"]
        if published:
            # Existing server contract: at least85% of native source times
            # remain within15s, including immediately before the next publish.
            fresh = [row for row in published[-1]["quotes"].values()
                     if clock[0] - row["time"] / 1000 <= 15]
            assert len(fresh) / 5563 >= .85
        clock[0] += 1
    full = [payload for name, payload in writes if name == "full_quotes.json"]
    assert len(full) >= 2
    assert all(len(batch) <= 2000 for batch in calls)
    assert all(set(p._tracked_codes).issubset(batch) for batch in calls)
    assert full[0]["generated_ts"] - start < 4
    assert full[1]["generated_ts"] - full[0]["generated_ts"] <= 4
    assert full[0]["quote_count"] == 5563
    assert full[0]["quote_acquisition_protocol"] == p.QUOTE_ACQUISITION_PROTOCOL
    assert full[0]["quote_acquisition_mode"] == "full_tick_poll"


def test_repeated_native_cache_does_not_refresh_observation_or_event_time(monkeypatch):
    p, clock, writes = configured(monkeypatch)
    source = tick(clock)
    context = SimpleNamespace(get_full_tick=lambda codes: {code: source for code in codes})
    p._refresh_full_snapshot(context)
    first = dict(p._tracked_quotes[p._tracked_codes[0]])
    clock[0] += 20
    p._refresh_full_snapshot(context)
    assert p._tracked_quotes[p._tracked_codes[0]] == first
    assert p._last_poll_ts == clock[0]
    source["askPrice"][0] = 11
    assert first["askPrice"] == [10.01]
    p._refresh_full_snapshot(context)
    changed = p._tracked_quotes[p._tracked_codes[0]]
    assert changed["time"] == first["time"]
    assert changed["_probiga_observed_at"] != first["_probiga_observed_at"]
    assert changed["_probiga_acquisition_method"] == "ContextInfo.get_full_tick"
    assert "_probiga_received_at" not in changed


def test_repeated_refresh_and_tracked_changes_do_not_starve_full_sweep(monkeypatch):
    p, clock, writes = configured(monkeypatch, count=5563, tracked=280)
    native = SimpleNamespace(get_full_tick=lambda codes: {code: tick(clock) for code in codes})
    for step in range(50):
        if step % 2 == 0:
            p._load_config = lambda **kw: True
            # Changing tracked membership also must leave full-sweep progress.
            p._tracked_codes = p._all_codes[step:280 + step]
            p._refresh_quote_universe()
        p._refresh_full_snapshot(native)
        clock[0] += 1
    assert any(payload["quote_count"] == 5563 for _, payload in writes)


def test_out_of_order_and_missing_native_time_cannot_replace_newer_quote(monkeypatch):
    p, clock, writes = configured(monkeypatch)
    code = p._tracked_codes[0]
    p._record_polled_quotes({code: tick(clock)}, [code], clock[0])
    first = p._quote_cache[code]
    older = dict(tick(clock, 11), time=int(clock[0] * 1000) - 1)
    assert p._record_polled_quotes({code: older}, [code], clock[0]) == {}
    assert p._record_polled_quotes({code: {"lastPrice": 12}}, [code], clock[0]) == {}
    assert p._quote_cache[code] == first


def test_invalid_response_does_not_advance_sweep_and_retry_recovers(monkeypatch):
    p, clock, writes = configured(monkeypatch)
    with pytest.raises(ValueError):
        p._refresh_full_snapshot(SimpleNamespace(get_full_tick=lambda codes: None))
    assert p._poll_pending == p._all_codes
    assert writes == []
    p._refresh_full_snapshot(SimpleNamespace(get_full_tick=lambda codes:
        {code: tick(clock) for code in codes}))
    assert writes[-1][1]["quote_count"] == 2


def test_missing_new_cycle_row_is_not_filled_with_prior_cycle_value(monkeypatch):
    p, clock, writes = configured(monkeypatch)
    p._refresh_full_snapshot(SimpleNamespace(get_full_tick=lambda codes:
        {code: tick(clock) for code in codes}))
    clock[0] += 31
    p._refresh_full_snapshot(SimpleNamespace(get_full_tick=lambda codes:
        {p._tracked_codes[0]: tick(clock)}))
    assert writes[-1][1]["quote_count"] == 1


def test_failed_closing_publication_retries_complete_capture_without_new_native_read(monkeypatch):
    p, clock, writes = configured(monkeypatch)
    p._quote_phase = lambda now: ("closed", "2026-09-18")
    p._refresh_quote_universe()
    p._atomic_write = lambda *args: (_ for _ in ()).throw(OSError("sharing violation"))
    with pytest.raises(OSError):
        p._refresh_full_snapshot(SimpleNamespace(get_full_tick=lambda codes:
            {code: tick(clock) for code in codes}))
    assert p._full_snapshot_pending
    original = dict(p._poll_cycle_quotes)
    clock[0] += 10
    p._atomic_write = lambda name, payload: writes.append((name, payload))
    p._refresh_full_snapshot(SimpleNamespace(get_full_tick=lambda codes: 1 / 0))
    assert writes[-1][1]["quotes"] == original
    assert not p._full_snapshot_pending


def test_mtime_refresh_and_phase_change_schedule_reads_without_fake_freshness(monkeypatch):
    p, clock, writes = configured(monkeypatch)
    native = SimpleNamespace(get_full_tick=lambda codes: {code: tick(clock) for code in codes})
    p._refresh_full_snapshot(native)
    first = p._quote_cache[p._tracked_codes[0]]["_probiga_observed_at"]
    p._load_config = lambda **kw: True
    clock[0] += 1
    p._refresh_quote_universe()
    assert p._poll_pending == p._all_codes
    assert p._quote_cache[p._tracked_codes[0]]["_probiga_observed_at"] == first
    p._quote_phase = lambda now: ("closed", "2026-09-18")
    p._refresh_quote_universe()
    assert not p._quote_cache
    p._refresh_full_snapshot(native)
    closed_payload = writes[-1][1]
    p._load_config = lambda **kw: False
    for _ in range(4):
        clock[0] += 100
        p._refresh_quote_universe()
        p._refresh_full_snapshot(SimpleNamespace(get_full_tick=lambda codes: 1 / 0))
    assert writes[-1][1]["quotes"] == closed_payload["quotes"]
    assert writes[-1][1]["last_poll_ts"] == closed_payload["last_poll_ts"]
    assert writes[-1][1]["generated_ts"] > closed_payload["generated_ts"]


def test_cached_context_detaches_rows_and_only_polls_unmanaged_symbols(monkeypatch):
    p, clock, writes = configured(monkeypatch)
    code = p._tracked_codes[0]
    p._quote_cache[code] = tick(clock)
    calls = []
    def native(codes):
        calls.append(codes)
        return {"000300.SH": tick(clock)}
    context = p._QuoteCacheContext(SimpleNamespace(get_full_tick=native))
    result = context.get_full_tick([code, p._all_codes[1], "000300.SH"])
    result[code]["askPrice"][0] = 20
    assert p._quote_cache[code]["askPrice"] == [10.01]
    assert p._all_codes[1] not in result
    assert calls == [["000300.SH"]]


def test_stop_during_native_read_discards_returned_data_and_publication(monkeypatch):
    p, clock, writes = configured(monkeypatch)
    p._write_heartbeat = lambda status: None
    def native(codes):
        p.stop(SimpleNamespace())
        return {code: tick(clock) for code in codes}
    with pytest.raises(RuntimeError, match="QMT_MODEL_STOPPING"):
        p._refresh_full_snapshot(SimpleNamespace(get_full_tick=native))
    assert not p._quote_cache
    assert not writes


def test_closing_acquisition_slot_does_not_repeat_at_midnight_or_weekend():
    p = load_producer()
    def phase(value):
        return p._quote_phase(datetime.fromisoformat(value).timestamp())
    assert phase("2026-09-16 15:10") == ("closed", "2026-09-16")
    assert phase("2026-09-17 00:30") == ("closed", "2026-09-16")
    assert phase("2026-09-17 09:15") == ("live", "2026-09-17")
    assert phase("2026-09-19 10:00") == ("closed", "2026-09-18")
    assert phase("2026-09-21 08:00") == ("closed", "2026-09-18")
