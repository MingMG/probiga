from types import SimpleNamespace

from test_bigqmt_callback_liveness import load_producer


def configured():
    p = load_producer()
    p._all_codes = ["000001.SZ", "600000.SH"]
    p._load_config = lambda **kw: True
    p._atomic_write = lambda *a: None
    return p


def test_one_subscription_covers_market_and_unchanged_watchlist_does_not_resubscribe():
    p = configured()
    subscriptions = []
    c = SimpleNamespace(subscribe_whole_quote=lambda codes, callback:
                        subscriptions.append((codes, callback)) or len(subscriptions),
                        unsubscribe_quote=lambda sid: None)
    p._refresh_subscription(c)
    p._refresh_subscription(c)
    assert len(subscriptions) == 1
    assert subscriptions[0][0] == p._all_codes
    subscriptions[0][1]({"600000.SH": {"time": 100, "lastPrice": 10}})
    assert p._quote_cache["600000.SH"]["lastPrice"] == 10
    assert "600000.SH" not in p._tracked_quotes


def test_initial_read_is_paced_and_never_repeated_or_promoted_to_callback():
    p = configured()
    p._all_codes = ["%06d.SZ" % n for n in range(401)]
    p._seed_pending = list(p._all_codes)
    p._subscribed_codes = frozenset(p._all_codes)
    calls, publications = [], []
    clock = [100.0]
    p.time = SimpleNamespace(time=lambda: clock[0],
                             strftime=lambda *a: "now", localtime=lambda *a: None)
    p._now_text = lambda: "now"
    p._atomic_write = lambda name, payload: publications.append(payload)

    def seed(codes):
        calls.append(codes)
        # A real callback races with an older cold-start cache response.
        p.whole_quote_callback({codes[0]: {"time": 200, "lastPrice": 20}})
        return dict((c, {"time": 100, "lastPrice": 10}) for c in codes)

    c = SimpleNamespace(get_full_tick=seed)
    for _ in range(10):
        p._refresh_full_snapshot(c)
        clock[0] += 5
    assert list(map(len, calls)) == [200, 200, 1]
    assert len(publications[0]["quotes"]) == 401
    assert p._quote_cache[p._all_codes[0]]["lastPrice"] == 20
    assert "_probiga_received_at" not in p._quote_cache[p._all_codes[1]]
    assert p._tracked_quotes == {}


def test_late_old_subscription_and_out_of_order_quotes_cannot_regress_cache():
    p = configured()
    callbacks = []
    c = SimpleNamespace(subscribe_whole_quote=lambda codes, callback:
                        callbacks.append(callback) or len(callbacks),
                        unsubscribe_quote=lambda sid: None)
    p._refresh_subscription(c)
    p._refresh_subscription(c, force=True)
    callbacks[1]({"000001.SZ": {"time": 200, "lastPrice": 20}})
    callbacks[0]({"000001.SZ": {"time": 300, "lastPrice": 30}})
    callbacks[1]({"000001.SZ": {"time": 100, "lastPrice": 10}})
    assert p._tracked_quotes["000001.SZ"]["lastPrice"] == 20


def test_all_consumers_read_detached_cache_without_native_polling():
    p = configured()
    p._subscribed_codes = frozenset(p._all_codes)
    p._quote_cache = {"000001.SZ": {"askPrice": [10], "time": 100}}
    view = p._QuoteCacheContext(SimpleNamespace(get_full_tick=lambda _: 1 / 0))
    result = view.get_full_tick(["000001.SZ", "600000.SH"])
    result["000001.SZ"]["askPrice"][0] = 20
    assert p._quote_cache["000001.SZ"]["askPrice"] == [10]
    assert "600000.SH" not in result


def test_ad_hoc_instruments_remain_available_without_rereading_managed_market():
    p = configured()
    p._subscribed_codes = frozenset(p._all_codes)
    p._quote_cache = {"000001.SZ": {"time": 100}}
    calls = []
    def native(codes):
        calls.append(codes)
        return {"000300.SH": {"time": 200}}
    view = p._QuoteCacheContext(SimpleNamespace(get_full_tick=native))
    assert set(view.get_full_tick(["000001.SZ", "000300.SH"])) == {"000001.SZ", "000300.SH"}
    assert calls == [["000300.SH"]]


def test_failed_subscription_is_retried_without_config_change():
    p = configured()
    attempts = []
    def subscribe(codes, callback):
        attempts.append(codes)
        return -1 if len(attempts) == 1 else 2
    c = SimpleNamespace(subscribe_whole_quote=subscribe)
    import pytest
    with pytest.raises(RuntimeError):
        p._refresh_subscription(c)
    p._load_config = lambda **kw: False
    p._last_subscription_attempt = 0
    p._refresh_subscription(c)
    assert p._subscription_id == 2


def test_silent_market_subscription_recovers_but_not_during_lunch():
    p = configured()
    p._subscription_id = 1
    p._subscribed_codes = frozenset(p._all_codes)
    p._subscription_started_ts = 100
    p._last_market_callback_ts = 100
    p._load_config = lambda **kw: False
    local = SimpleNamespace(tm_wday=3, tm_hour=12, tm_min=0)
    p.time = SimpleNamespace(time=lambda: 1000, localtime=lambda _: local)
    p._write_tracked_snapshot = lambda **kw: None
    calls = []
    c = SimpleNamespace(unsubscribe_quote=lambda sid: calls.append(sid),
                        subscribe_whole_quote=lambda codes, callback: 2)
    p._refresh_subscription(c)
    assert calls == []
    local.tm_hour = 13
    p._refresh_subscription(c)
    assert calls == [1]
    assert p._subscription_id == 2
    assert p._subscription_started_ts == 1000


def test_invalid_initial_read_is_not_published_and_can_recover():
    p = configured()
    p._seed_pending = list(p._all_codes)
    writes = []
    p._atomic_write = lambda *args: writes.append(args)
    import pytest
    with pytest.raises(ValueError):
        p._refresh_full_snapshot(SimpleNamespace(get_full_tick=lambda codes: None))
    assert writes == []
    assert p._seed_pending == p._all_codes


def test_watchlist_change_during_backoff_is_not_lost():
    p = configured()
    p._subscription_id = 1
    p._subscribed_codes = frozenset(["000001.SZ"])
    p._last_subscription_attempt = 100
    clock = [110]
    local = SimpleNamespace(tm_wday=3, tm_hour=0, tm_min=0)
    p.time = SimpleNamespace(time=lambda: clock[0], localtime=lambda _: local)
    p._write_tracked_snapshot = lambda **kw: None
    calls = []
    c = SimpleNamespace(unsubscribe_quote=lambda sid: None,
                        subscribe_whole_quote=lambda codes, callback: calls.append(codes) or 2)
    p._refresh_subscription(c)
    assert calls == []
    clock[0] = 140
    p._load_config = lambda **kw: False
    p._refresh_subscription(c)
    assert calls == [p._all_codes]
