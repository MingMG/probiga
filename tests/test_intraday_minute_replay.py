from datetime import date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from struct import pack
from types import SimpleNamespace

import pytest

from biz.intraday_alert import minute_replay as replay
from server.common.qmt_datadir_minute_coverage import validate_matrix
from server.common.qmt_index_minute_grid import CORE_GRID


DAY = date(2026, 11, 6)


def test_replay_uses_contiguous_prior_bars_and_emits_once_per_threshold_crossing():
    detector = replay._MinuteAnomalies()
    start = datetime(2026, 11, 6, 11, 18)
    for index in range(13):
        detector.observe({"stock_code": "000001", "trade_time": start + timedelta(minutes=index),
                          "price": Decimal("10") if index < 10 else Decimal("10.20"),
                          "amount": Decimal("1250000") if index < 10 else Decimal("5000000")})
    for index in range(4):
        detector.observe({"stock_code": "000001", "trade_time": datetime(2026, 11, 6, 13, 1) + timedelta(minutes=index),
                          "price": Decimal("9") if index < 3 else Decimal("8.82"), "amount": Decimal("0")})
    result = detector.results()
    assert detector.counts == {"RAPID_RISE": 1, "RAPID_DROP": 1, "AMOUNT_SURGE": 1}
    assert result["RAPID_RISE"][0]["bar_at"] == "2026-11-06T11:28:00+08:00"
    assert result["RAPID_DROP"][0]["window_start_at"] == "2026-11-06T13:01:00+08:00"
    assert result["RAPID_DROP"][0]["change_pct"] == -2
    assert result["AMOUNT_SURGE"][0]["amount_ratio"] == 4


class _ReadConnection:
    dialect = SimpleNamespace(name="mysql")

    def __init__(self):
        self.sql = []

    def execution_options(self, **options):
        assert options == {"isolation_level": "REPEATABLE READ"}
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, statement, params=None):
        self.sql.append(str(statement))
        return SimpleNamespace(first=lambda: None)


@pytest.mark.parametrize("problem", [None, "late", "digest"])
def test_replay_releases_only_same_exact_matrix_after_real_availability(monkeypatch, problem):
    day = DAY.isoformat()
    code = "000001"
    scope = {"codes": (code,), "daily_close_by_code": {code: "10"},
             "full_market_manifest_hash": "a" * 64, "expected_traded_code_set_sha256": "b" * 64}
    rows = [{"stock_code": code, "trade_time": stamp, "trade_date": DAY, "source_time": stamp,
             "price": Decimal("10.20") if index == 4 else Decimal("10"),
             "change": Decimal("0"), "change_pct": Decimal("0"), "volume": Decimal("10"),
             "amount": Decimal("100"), "qmt_code": "000001.SZ", "data_source": "qmt_datadir",
             "batch_id": "qmt-minute", "data_version": "qmt_datadir_v1", "quality_status": "VERIFIED",
             "permission_status": "SUPPORTED"}
            for index, moment in enumerate(CORE_GRID) for stamp in (datetime.fromisoformat(f"{day} {moment}"),)]
    proof = validate_matrix(iter(rows), day=day, scope=scope)
    receipt = {"trade_date": DAY, "status": "COMPLETE", "run_id": "sealed-run", "code_count": 1,
               "row_count": len(rows), "finished_at": datetime(2026, 11, 6, 22, 34, 47),
               "content_sha256": "c" * 64,
               "detail": {**proof, "readback_rows": len(rows), "readback_codes": 1}}
    if problem == "digest":
        rows[0] = dict(rows[0], amount=Decimal("101"))
    connection = _ReadConnection()
    calls = []

    def read_matrix(_connection, selected_day, **kwargs):
        assert selected_day == day and kwargs["scope"] is scope
        calls.append(selected_day)
        yield from rows

    monkeypatch.setattr(replay, "load_receipt", lambda *_a, **_k: receipt)
    monkeypatch.setattr(replay, "trade_scope", lambda *_a, **_k: scope)
    monkeypatch.setattr(replay, "_minute_rows", read_matrix)
    engine = SimpleNamespace(connect=lambda: connection)
    cutoff = datetime(2026, 11, 6, 22 if problem == "late" else 23)
    if problem:
        with pytest.raises(replay.MinuteReplayBlocked, match="not yet available" if problem == "late" else "minute_readback"):
            replay.replay_minute_anomalies(engine, DAY, cutoff=cutoff)
    else:
        report = replay.replay_minute_anomalies(engine, DAY, cutoff=cutoff)
        assert report["mode"] == "POST_CLOSE_MINUTE_REPLAY"
        assert report["evidence_available_at"] == "2026-11-06T22:34:47+08:00"
        assert report["source_proof"] == proof
        assert report["candidate_counts"]["RAPID_RISE"] == 1
        assert report["strongest_events"]["RAPID_RISE"][0]["stock_code"] == code
    assert connection.sql[0] == "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"
    assert calls == ([] if problem == "late" else [day])


def test_matrix_batches_cover_both_intermediate_and_trailing_unexpected_securities(monkeypatch):
    monkeypatch.setattr(replay, "_BATCH_CODES", 2)
    codes = ["000001", "000003", "000005"]
    storage = codes[:]
    monkeypatch.setattr(replay, "_decode_matrix_group", lambda item, **_: [item["stock_code"]])

    def query(_connection, sql, params, **_):
        return [{"stock_code": code} for code in sorted(storage)
                if ("stock_code>:lower" not in sql or code > params["lower"])
                and ("stock_code<=:upper" not in sql or code <= params["upper"])]

    monkeypatch.setattr(replay, "_read_matrix_query", query)
    scope = {"codes": codes}
    assert list(replay._minute_rows(None, DAY.isoformat(), scope=scope, deadline=1)) == codes
    for unexpected in ("", "000002", "999999"):
        storage[:] = [*codes, unexpected]
        with pytest.raises(replay.MinuteReplayBlocked, match="security coverage differs"):
            list(replay._minute_rows(None, DAY.isoformat(), scope=scope, deadline=1))


def test_local_slice_uses_only_target_day_and_binds_exact_native_publication(tmp_path, monkeypatch):
    def raw_bar(moment, price=10000):
        words = [0] * replay.DAT_WORD_COUNT
        words[0] = int(moment.replace(tzinfo=replay.CST).timestamp())
        words[4] = price
        words[6] = 1
        words[8] = 10
        words[13] = 10000
        return pack("<16I", *words)

    day = DAY.isoformat()
    code = "000001"
    path = replay.dat_path(tmp_path, "SZ", replay.MINUTE_PERIOD, code)
    path.parent.mkdir(parents=True)
    native = b"".join(raw_bar(datetime.fromisoformat(f"{day} {moment}"), 10200 if index == 4 else 10000)
                      for index, moment in enumerate(CORE_GRID))
    path.write_bytes(replay.DAT_HEADER + raw_bar(datetime(2026, 11, 5, 15)) + native + raw_bar(datetime(2026, 11, 7, 9)))
    assert replay._native_session_records(path, DAY).tobytes() == native

    count = len(CORE_GRID)
    scope = {"codes": (code,), "daily_close_by_code": {code: "10"},
             "full_market_manifest_hash": "a" * 64, "expected_traded_code_set_sha256": "b" * 64}
    receipt = {"trade_date": DAY, "status": "COMPLETE", "run_id": "sealed-run", "code_count": 1,
               "row_count": count, "finished_at": datetime(2026, 11, 6, 22, 34, 47),
               "content_sha256": sha256(b"000001.SZ" + sha256(native).digest()).hexdigest(),
               "detail": {"full_market_manifest_hash": "a" * 64, "expected_traded_code_set_sha256": "b" * 64,
                          "row_hash": "c" * 64, "coverage": 1.0, "eligible_code_count": 1,
                          "readback_rows": count, "readback_codes": 1, "minute_grid_count": count,
                          "minute_grid_sha256": replay.GRID_HASH, "bad_grid_codes": 0}}
    monkeypatch.setattr(replay, "load_receipt", lambda *_a, **_k: receipt)
    monkeypatch.setattr(replay, "trade_scope", lambda *_a, **_k: scope)
    monkeypatch.setattr(replay, "_minute_rows", lambda *_a, **_k: pytest.fail("local mode read remote matrix"))
    engine = SimpleNamespace(connect=_ReadConnection)
    cutoff = datetime(2026, 11, 6, 23)
    result = replay.replay_local_minute_anomalies(engine, DAY, tmp_path, cutoff=cutoff)
    assert result["source"] == "qmt_local_datadir"
    assert result["candidate_counts"]["RAPID_RISE"] == 1
    assert result["receipt_comparison"]["native_slice_digest_matches"]
    assert not result["receipt_comparison"]["reconstructed_matrix_digest_matches"]
    assert result["evidence_available_at"] == "2026-11-06T22:34:47+08:00"
    receipt["content_sha256"] = "d" * 64
    with pytest.raises(replay.MinuteReplayBlocked, match="first verified after"):
        replay.replay_local_minute_anomalies(engine, DAY, tmp_path, cutoff=cutoff)
