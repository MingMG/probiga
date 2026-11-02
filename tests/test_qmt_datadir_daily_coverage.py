from copy import deepcopy
from datetime import datetime
import hashlib
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from server.common import qmt_datadir_daily_coverage as coverage
from server.common import daily_stock_universe
from server.common import turnover_snapshot

DAY = "2026-11-02"
CUTOFF = datetime(2026, 11, 2, 20, 0)
SUSPENDED = ("000831", "002129", "002717", "601326", "688121", "688496")
IPO = "001381"
TRADED = "600000"


def authorities():
    codes = sorted((*SUSPENDED, IPO, TRADED))
    catalog = SimpleNamespace(batch_id="catalog", manifest_hash="a" * 64,
                              member_set_hash="b" * 64, eligible_codes=lambda day: codes)
    calendar = SimpleNamespace(batch_id="calendar", manifest_hash="c" * 64, session_set_hash="d" * 64,
                               sessions_between=lambda start, end: [DAY])
    return catalog, calendar


def bar():
    return {"stock_code": TRADED, "trade_date": DAY, "open": 10, "close": 11,
            "high": 11, "low": 10, "volume": 100, "amount": 1100, "pre_close": 10}


def snapshot():
    tick = {"time": 1793582100000, "_probiga_observed_at": "2026-11-02 15:10:01",
            "_probiga_acquisition_method": "ContextInfo.get_full_tick", "stockStatus": 7,
            "open": 0, "high": 0, "low": 0, "volume": 0, "amount": 0}
    return {"source": "gj_big_qmt_inner", "quote_acquisition_protocol": "probiga.qmt-full-tick-poll.v1",
            "quote_acquisition_mode": "full_tick_poll", "generated_at": "2026-11-02 19:56:00",
            "batch_id": "qmt-native", "quotes": {
                code + (".SH" if code.startswith("6") else ".SZ"): deepcopy(tick)
                for code in SUSPENDED}}


def listing():
    body = "证券代码：001381。首次公开发行股票并在主板上市之上市公告书。本公司股票将于2026年11月3日在深圳证券交易所上市。"
    return {IPO: {"schema": "probiga.cninfo-listing-evidence.v1", "stock_code": IPO,
                  "listing_date": "2026-11-03", "publication_date": DAY,
                  "official_document_sha256": "e" * 64,
                  "source_url": "https://static.cninfo.com.cn/finalpage/2026-11-02/1225646928.PDF",
                  "source_text": body, "source_text_sha256": hashlib.sha256(body.encode()).hexdigest()}}


def manifest(**kwargs):
    catalog, calendar = authorities()
    return coverage.build_manifest(catalog=catalog, calendar=calendar, day=DAY,
                                   rows=[bar()], run_id="qdd-day", captured_at=CUTOFF,
                                   snapshot=kwargs.get("snapshot", snapshot()),
                                   listings=kwargs.get("listings", listing()))


def test_incident_six_suspensions_and_future_listing_are_exact_without_fake_bars():
    proof = manifest()
    catalog, calendar = authorities()
    assert coverage.validate_manifest(proof, catalog=catalog, calendar=calendar) == proof
    assert len(proof["entities"]) == 8
    assert [item["stock_code"] for item in proof["entities"] if item["classification"] == "TRADED"] == [TRADED]
    assert {item["stock_code"] for item in proof["entities"] if item["classification"] == "NO_TRADE"} == set(SUSPENDED)
    assert [item["stock_code"] for item in proof["entities"] if item["classification"] == "NOT_LISTED"] == [IPO]


def test_after_midnight_envelope_reuses_target_day_native_suspensions():
    quotes = snapshot()
    quotes["generated_at"] = "2026-11-03 01:50:26"
    quotes["last_poll_at"] = "2026-11-02 20:54:09"
    for tick in quotes["quotes"].values():
        tick["_probiga_observed_at"] = "2026-11-02 20:54:09"
    catalog, calendar = authorities()
    proof = coverage.build_manifest(
        catalog=catalog, calendar=calendar, day=DAY, rows=[bar()], run_id="qdd-recovery",
        captured_at=datetime(2026, 11, 3, 1, 51), snapshot=quotes, listings=listing(),
    )
    assert coverage.validate_manifest(proof, catalog=catalog, calendar=calendar) == proof
    assert {item["stock_code"] for item in proof["entities"]
            if item["classification"] == "NO_TRADE"} == set(SUSPENDED)
    assert all(item["evidence"]["snapshot_generated_at"] == "2026-11-03 01:50:26"
               for item in proof["entities"] if item["classification"] == "NO_TRADE")


@pytest.mark.parametrize("observed,generated", [
    ("2026-11-03 01:50:00", "2026-11-03 01:50:26"),
    ("2026-11-02 20:54:09", "2026-11-02 20:54:08"),
    ("2026-11-02 20:54:09", "2026-11-03 01:51:01"),
])
def test_rewritten_envelope_rejects_wrong_day_acquisition_or_invalid_time_order(observed, generated):
    quotes = snapshot()
    quotes["generated_at"] = generated
    quotes["quotes"]["000831.SZ"]["_probiga_observed_at"] = observed
    with pytest.raises(ValueError, match="suspension"):
        coverage.suspension_evidence(quotes, "000831", DAY, datetime(2026, 11, 3, 1, 51))


@pytest.mark.parametrize("change", [
    {"stockStatus": 0}, {"volume": 1}, {"amount": 1}, {"open": 1},
    {"time": 1793322900000}, {"_probiga_observed_at": "2026-11-02 14:00:00"},
])
def test_unknown_or_stale_absence_never_passes_like_the_old_95_percent_gate(change):
    quotes = snapshot()
    quotes["quotes"]["000831.SZ"].update(change)
    with pytest.raises(ValueError, match="suspension"):
        manifest(snapshot=quotes)


def test_missing_security_requires_replayable_listing_source_not_ipo_price():
    with pytest.raises((ValueError, KeyError)):
        manifest(listings={})
    bad = listing()
    bad[IPO]["listing_date"] = "2026-11-04"
    with pytest.raises(ValueError, match="listing"):
        manifest(listings=bad)


def database(monkeypatch):
    catalog, calendar = authorities()
    engine = create_engine("sqlite+pysqlite:///:memory:")
    columns = "manifest_hash schema_version dataset period provider trade_date status strategy_eligible run_id catalog_batch_id catalog_manifest_hash calendar_batch_id calendar_manifest_hash source_batch_id grid_profile expected_entity_count entity_count expected_traded_count actual_traded_count no_trade_count bar_count expected_entity_set_hash expected_traded_set_hash actual_traded_set_hash no_trade_set_hash entity_root_hash reason_count captured_at manifest_json created_at".split()
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE qmt_history_coverage_manifest (" + ",".join(column + " TEXT" for column in columns) + ",PRIMARY KEY(manifest_hash))"))
        connection.execute(text("CREATE TABLE qmt_history_coverage_entity (manifest_hash TEXT,stock_code TEXT,expected_state TEXT,classification TEXT,bar_count INTEGER,time_set_hash TEXT,first_time TEXT,last_time TEXT,source_row_hash TEXT,row_hash TEXT,created_at TEXT,PRIMARY KEY(manifest_hash,stock_code))"))
        connection.execute(text("CREATE TABLE sm_stock_kline (stock_code TEXT,trade_date TEXT,open REAL,close REAL,high REAL,low REAL,volume REAL,amount REAL,pre_close REAL,data_source TEXT,k_type INTEGER,adjust_type INTEGER,quality_status TEXT,permission_status TEXT)"))
        connection.execute(text("INSERT INTO sm_stock_kline VALUES (:stock_code,:trade_date,:open,:close,:high,:low,:volume,:amount,:pre_close,'qmt_datadir',1,0,'VERIFIED','SUPPORTED')"), bar())
        coverage.insert_manifest(connection, manifest())
    monkeypatch.setattr(coverage, "load_stock_catalog", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(coverage, "load_trade_calendar_receipt", lambda *args, **kwargs: calendar)
    return engine


def test_real_database_replay_rejects_mutated_bars_and_entity_partitions(monkeypatch):
    engine = database(monkeypatch)
    assert coverage.load_daily_manifest(engine, DAY, decision_known_at=CUTOFF)["status"] == "EXACT"
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_kline SET volume=101"))
    with pytest.raises(ValueError, match="database bars differ"):
        coverage.load_daily_manifest(engine, DAY, decision_known_at=CUTOFF)
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_kline SET volume=100"))
        connection.execute(text("DELETE FROM qmt_history_coverage_entity WHERE stock_code='000831'"))
    with pytest.raises(ValueError, match="entity inventory"):
        coverage.load_daily_manifest(engine, DAY, decision_known_at=CUTOFF)
    engine.dispose()


@pytest.mark.parametrize("case", ["same_day", "old_day", "bad_inventory", "changed_bar"])
def test_turnover_release_catchup_reuses_only_verified_target_daily_content(monkeypatch, case):
    from server.api import scheduler_runtime

    engine = database(monkeypatch)
    target = "2026-11-03" if case == "old_day" else DAY
    now = datetime(2026, 11, 3, 21)
    daily = {"task_type": "qmt_stock_daily_canonical", "enabled": 1,
             "_release_terminal_build_sha": "a" * 40}
    turnover = {"task_type": "target_turnover_snapshot",
                "_scheduler_target_trade_date": target,
                "_release_expected_target_date": target}
    monkeypatch.setattr(scheduler_runtime, "_scheduler_build_commit_sha", lambda: "b" * 40)
    with engine.begin() as connection:
        if case == "bad_inventory":
            connection.execute(text("DELETE FROM qmt_history_coverage_entity WHERE stock_code='000831'"))
        elif case == "changed_bar":
            connection.execute(text("UPDATE sm_stock_kline SET volume=101"))

    result = scheduler_runtime._release_catchup_dependencies_ready(
        turnover, [daily, turnover], engine=engine, now=now,
    )
    assert result == {
        "same_day": (True, "ready"),
        "old_day": (False, "qmt_stock_daily_canonical:exact_target_manifest_unavailable"),
        "bad_inventory": (False, "qmt_stock_daily_canonical:ValueError"),
        "changed_bar": (False, "qmt_stock_daily_canonical:ValueError"),
    }[case]
    engine.dispose()


def test_consumers_share_exact_trade_scope_and_readback_proof(monkeypatch):
    engine = database(monkeypatch)
    monkeypatch.setattr(daily_stock_universe, "validate_stock_catalog_runtime_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_stock_universe, "_load_attested_target_receipt", lambda *args, **kwargs: None)
    universe = daily_stock_universe.load_daily_stock_universe(engine, DAY, decision_known_at=CUTOFF)
    assert universe.expected_codes == (TRADED,)
    assert set(universe.excluded_no_row_codes) == set((*SUSPENDED, IPO))
    audit = daily_stock_universe.validate_daily_stock_coverage(universe, kline_rows=[bar()], flow_rows=[{"stock_code": TRADED}])
    assert audit["kline_coverage"] == audit["traded_flow_coverage"] == 1.0
    from server.common import qmt_daily_market_truth
    monkeypatch.setattr(qmt_daily_market_truth, "load_trade_calendar_receipt", lambda *args, **kwargs: authorities()[1])
    with engine.connect() as connection:
        authority = turnover_snapshot.load_turnover_universe_authority(connection, target_date=DAY, decision_at=CUTOFF, require_triggers=False)
    assert authority.expected_codes == (TRADED,)
    assert authority.truth_sha256 == universe.no_row_exception_proof_sha256
    engine.dispose()


def test_public_minute_flow_replays_suspension_and_prelisting_exemptions(monkeypatch):
    from tools.crawl_minute_kline import verified_no_trade_codes
    engine = database(monkeypatch)
    catalog, _calendar = authorities()
    codes, proof = verified_no_trade_codes(engine, catalog, trade_date=DAY, decision_known_at=CUTOFF)
    assert codes == set((*SUSPENDED, IPO))
    assert proof["proof_provider"] == "qmt_datadir" and proof["no_trade_count"] == 7
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_kline SET volume=101"))
    with pytest.raises(ValueError, match="database bars differ"):
        verified_no_trade_codes(engine, catalog, trade_date=DAY, decision_known_at=CUTOFF)
    engine.dispose()


def test_corrupt_local_publication_cannot_fall_back_to_a_native_attestation(monkeypatch):
    from server.common import qmt_daily_market_truth
    engine = database(monkeypatch)
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_kline SET close=12,high=12"))
    with engine.connect() as connection:
        with pytest.raises(ValueError, match="database bars differ"):
            qmt_daily_market_truth.load_qmt_daily_market_truth(
                connection, start_date=DAY, end_date=DAY, decision_known_at=CUTOFF,
            )
    engine.dispose()


def test_official_listing_snapshot_is_reused_until_its_listing_date(monkeypatch):
    engine = database(monkeypatch)
    retained = coverage.retained_listing_evidence(engine, [IPO], target_date=DAY, decision_known_at=CUTOFF)
    assert retained[IPO]["listing_date"] == "2026-11-03"
    assert coverage.retained_listing_evidence(engine, [IPO], target_date="2026-11-03", decision_known_at=datetime(2026, 11, 3, 20)) == {}
    engine.dispose()


def test_old_95_percent_receipt_without_exact_manifest_is_blocked(monkeypatch):
    from server.common import qmt_daily_market_truth
    from server.common import qmt_datadir_result
    engine = database(monkeypatch)
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM qmt_history_coverage_entity"))
        connection.execute(text("DELETE FROM qmt_history_coverage_manifest"))
    monkeypatch.setattr(qmt_datadir_result, "load_receipt", lambda *args, **kwargs: {
        "status": "COMPLETE", "row_count": 5571, "code_count": 5571,
        "finished_at": "2026-11-02 15:51:00", "detail": {"coverage": 5571/5578},
    })
    with engine.connect() as connection:
        with pytest.raises(RuntimeError, match="no exact full-market manifest"):
            qmt_daily_market_truth.load_qmt_daily_market_truth(
                connection, start_date=DAY, end_date=DAY, decision_known_at=CUTOFF,
            )
    engine.dispose()


def test_native_poll_after_slow_catalog_read_is_bound_to_actual_observation_time(monkeypatch):
    from tools import import_qmt_datadir_daily as importer
    from integrations.bigqmt import spool
    from server.common import official_listing_evidence
    catalog, calendar = authorities()
    class Clock(datetime):
        calls = 0
        @classmethod
        def now(cls, timezone=None):
            cls.calls += 1
            value = datetime(2026, 11, 2, 19, 54) if cls.calls == 1 else CUTOFF
            return value.replace(tzinfo=timezone)
    monkeypatch.setattr(importer.dt, "datetime", Clock)
    monkeypatch.setattr(importer, "load_stock_catalog", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(importer, "load_trade_calendar_receipt", lambda *args, **kwargs: calendar)
    monkeypatch.setattr(spool, "read_snapshot", lambda *args, **kwargs: snapshot())
    monkeypatch.setattr(importer, "retained_listing_evidence", lambda *args, **kwargs: {})
    def official(codes, **kwargs):
        assert codes == [IPO]  # Suspensions must never spill into IPO resolution.
        return listing()
    monkeypatch.setattr(official_listing_evidence, "load_official_listing_evidence", official)
    engine = SimpleNamespace(connect=lambda: __import__('contextlib').nullcontext(object()))
    row = bar()
    harvested = {"codes": {TRADED}, "rows": [(TRADED, DAY, DAY, row['open'], row['close'], row['high'], row['low'], row['volume'], row['amount'], row['pre_close'], 1, 10)]}
    proof = importer._coverage_manifest(engine, session=datetime.fromisoformat(DAY).date(), harvested=harvested, run_id="canary")
    assert sum(entity["classification"] == "NO_TRADE" for entity in proof["entities"]) == 6


@pytest.mark.parametrize("field,value", [("captured_at", "2026-11-02 19:00:00"), ("calendar_manifest_hash", "f" * 64)])
def test_persisted_metadata_cannot_rebind_a_sealed_daily_manifest(monkeypatch, field, value):
    engine = database(monkeypatch)
    with engine.begin() as connection:
        connection.execute(text(f"UPDATE qmt_history_coverage_manifest SET {field}=:value"), {"value": value})
    with pytest.raises(ValueError, match="authority metadata"):
        coverage.load_daily_manifest(engine, DAY, decision_known_at=CUTOFF)
    engine.dispose()


def test_daily_content_reuse_keeps_immutable_run_and_hash_but_rejects_changed_source(monkeypatch):
    from tools import import_qmt_datadir_daily as importer
    engine = database(monkeypatch)
    proof = coverage.load_daily_manifest(engine, DAY, decision_known_at=CUTOFF)
    catalog, calendar = authorities()
    monkeypatch.setattr(importer, "load_stock_catalog", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(importer, "load_trade_calendar_receipt", lambda *args, **kwargs: calendar)
    monkeypatch.setattr(importer, "persisted_fingerprint", lambda *args, **kwargs: {"row_hash": "f" * 64})
    monkeypatch.setattr(importer, "load_receipt", lambda *args, **kwargs: {
        "status": "COMPLETE", "run_id": proof["run_id"], "row_count": 1, "code_count": 1,
        "detail": {"full_market_manifest_hash": proof["manifest_hash"], "coverage": 1.0,
                   "readback_rows": 1, "readback_codes": 1, "row_hash": "f" * 64},
    })
    row = bar()
    native = (TRADED, DAY, DAY, row["open"], row["close"], row["high"], row["low"],
              row["volume"], row["amount"], row["pre_close"], 1, 10)
    harvested = {"codes": {TRADED}, "rows": [native]}
    reused = importer._reusable_partition(engine, session=datetime.fromisoformat(DAY).date(), harvested=harvested)
    assert reused["run_id"] == proof["run_id"] and reused["manifest_hash"] == proof["manifest_hash"]
    changed = list(native)
    changed[4] = changed[5] = 12
    assert importer._reusable_partition(engine, session=datetime.fromisoformat(DAY).date(),
                                        harvested={"codes": {TRADED}, "rows": [tuple(changed)]}) is None
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_kline SET close=12,high=12"))
    with pytest.raises(ValueError, match="database bars differ"):
        importer._reusable_partition(engine, session=datetime.fromisoformat(DAY).date(), harvested=harvested)
    engine.dispose()


def test_legacy_daily_partition_without_exact_proof_cannot_take_content_reuse(monkeypatch):
    from tools import import_qmt_datadir_daily as importer
    monkeypatch.setattr(importer, "load_daily_manifest", lambda *args, **kwargs: None)
    assert importer._reusable_partition(object(), session=datetime.fromisoformat(DAY).date(),
                                       harvested={"codes": {TRADED}, "rows": []}) is None
