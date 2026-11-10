from copy import deepcopy
from datetime import date, datetime, timedelta
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


def historical_document(source_text, *, date="2026-11-02", identifier=1234):
    return {"source_url": f"https://static.cninfo.com.cn/finalpage/{date}/{identifier}.PDF",
            "observed_at": "2026-11-07T10:00:00+08:00", "source_text": source_text,
            "source_text_sha256": hashlib.sha256(source_text.encode()).hexdigest(),
            "official_document_sha256": "a" * 64}


def test_historical_october_daily_append_replays_only_after_durable_completion(monkeypatch):
    from server.common import qmt_daily_market_truth
    from tools.crawl_minute_kline import verified_no_trade_codes
    from tools import import_qmt_datadir_daily as importer

    engine = database(monkeypatch)
    day = "2026-10-28"
    new_code, suspended, delisted = "920285", "002827", "301139"
    catalog, calendar = authorities()
    catalog.eligible_codes = lambda target: [TRADED, new_code, suspended, delisted]
    calendar.sessions_between = lambda start, end: [day]
    monkeypatch.setattr(coverage, "load_stock_catalog", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(coverage, "load_trade_calendar_receipt", lambda *args, **kwargs: calendar)
    monkeypatch.setattr(qmt_daily_market_truth, "load_trade_calendar_receipt", lambda *args, **kwargs: calendar)
    monkeypatch.setattr(importer, "persisted_fingerprint", lambda *args, **kwargs: {"row_hash": "prior"})
    acquired = datetime(2026, 11, 7, 10, 0)
    completed = datetime(2026, 11, 7, 10, 1)
    old = {**bar(), "trade_date": day}
    new = {**old, "stock_code": new_code}
    documents = {
        suspended: [historical_document("证券代码：002827。公司股票停牌情况。公司股票自2026年10月26日开市起开始停牌，"
                                       "公司股票将于2026年11月2日开市起复牌。")],
        delisted: [historical_document("证券代码：301139。关于公司股票终止上市暨摘牌的公告。摘牌日期：2026年10月28日。",
                                       date=day, identifier=2345)],
    }
    proof = coverage.build_manifest(catalog=catalog, calendar=calendar, day=day, rows=[old, new],
                                    run_id="historical", captured_at=acquired, historical_documents=documents)
    assert {item["stock_code"]: item["classification"] for item in proof["entities"]} == {
        TRADED: "TRADED", new_code: "TRADED", suspended: "NO_TRADE", delisted: "DELISTED",
    }
    with pytest.raises(ValueError, match="historical daily lifecycle inventory"):
        coverage.build_manifest(catalog=catalog, calendar=calendar, day=day, rows=[old, new], run_id="bad",
                                captured_at=acquired, historical_documents={})
    with engine.begin() as connection:
        connection.execute(text("""CREATE TABLE qmt_datadir_import_partition (
            dataset TEXT, trade_date TEXT, source_root TEXT, status TEXT, code_count INTEGER, row_count INTEGER,
            content_sha256 TEXT, run_id TEXT, started_at TEXT, finished_at TEXT, detail_json TEXT)"""))
        connection.execute(text("""INSERT INTO qmt_datadir_import_partition VALUES (
            'stock_daily', :day, 'DAT', 'COMPLETE', 1, 1, 'source', 'legacy', :day, :day, :detail)"""),
                           {"day": day, "detail": json.dumps({"row_hash": "prior", "readback_rows": 1, "readback_codes": 1})})
        connection.execute(text("INSERT INTO sm_stock_kline VALUES (:stock_code,:trade_date,:open,:close,:high,:low,:volume,:amount,:pre_close,'qmt_datadir',1,0,'VERIFIED','SUPPORTED')"), old)
        append, corrections, receipt = importer._historical_append_plan(connection, day=day, proof=proof)
        assert append == {new_code} and corrections == [] and receipt["run_id"] == "legacy"
        connection.execute(text("UPDATE sm_stock_kline SET volume=101 WHERE trade_date=:day"), {"day": day})
        with pytest.raises(ValueError, match="retained business row differs"):
            importer._historical_append_plan(connection, day=day, proof=proof)
        connection.execute(text("UPDATE sm_stock_kline SET volume=100 WHERE trade_date=:day"), {"day": day})
        connection.execute(text("INSERT INTO sm_stock_kline VALUES (:stock_code,:trade_date,:open,:close,:high,:low,:volume,:amount,:pre_close,'qmt_datadir',1,0,'VERIFIED','SUPPORTED')"), new)
        coverage.insert_manifest(connection, proof)
        with pytest.raises(ValueError, match="completion receipt differs"):
            coverage.load_daily_manifest(connection, day, decision_known_at=completed)
        connection.execute(text("""UPDATE qmt_datadir_import_partition SET
            code_count=2,row_count=2,run_id='historical',started_at=:acquired,finished_at=:completed,detail_json=:detail"""),
                           {"acquired": acquired, "completed": completed, "detail": json.dumps({
                               "full_market_manifest_hash": proof["manifest_hash"], "coverage": 1.0,
                               "readback_rows": 2, "readback_codes": 2,
                           })})
        assert coverage.load_daily_manifest(connection, day, decision_known_at=acquired) is None
        assert coverage.load_daily_manifest(connection, day, decision_known_at=completed) == proof
        with pytest.raises(RuntimeError, match="no exact full-market manifest"):
            qmt_daily_market_truth.load_qmt_daily_market_truth(connection, start_date=day, end_date=day,
                                                               decision_known_at=acquired)
        truth = qmt_daily_market_truth.load_qmt_daily_market_truth(connection, start_date=day, end_date=day,
                                                                  decision_known_at=completed)
        assert truth.run_finished_at == completed.isoformat(" ") and truth.attested_row_count == 2
        codes, identity = verified_no_trade_codes(connection, catalog, trade_date=day, decision_known_at=completed)
        assert codes == {suspended, delisted} and identity["receipt_id"] == proof["manifest_hash"]
    engine.dispose()


def test_official_retirement_hearing_must_bind_authority_and_not_use_future_capture():
    from server.common.official_historical_daily_evidence import historical_daily_evidence

    day = "2026-10-30"
    work_letter = historical_document(
        "证券代码：688121。关于收到监管工作函的公告。你公司股票自2026年10月29日开市起停牌，"
        "本所将在有关期限届满或者听证程序结束后审议，作出相应的终止上市决定。",
        date="2026-10-29", identifier=1,
    )
    notice = historical_document(
        "证券代码：688121。公司于2026年10月28日收到了上海证券交易所下发的终止上市的事先告知书。"
        "应当在收到本通知后5个交易日内提交书面听证申请。",
        date="2026-10-29", identifier=2,
    )
    cutoff = datetime(2026, 11, 7, 10, 0)
    with pytest.raises(ValueError, match="lifecycle is unproven"):
        historical_daily_evidence([work_letter], stock_code="688121", day=day, cutoff=cutoff)
    with pytest.raises(ValueError, match="identity or time"):
        historical_daily_evidence([work_letter, notice], stock_code="688121", day=day,
                                  cutoff=datetime(2026, 11, 6, 10, 0))
    assert historical_daily_evidence([work_letter, notice], stock_code="688121", day=day, cutoff=cutoff)[0] == "NO_TRADE"
    with pytest.raises(ValueError, match="lifecycle is unproven"):
        historical_daily_evidence([work_letter, notice], stock_code="688121", day="2026-11-05", cutoff=cutoff)


def test_separate_official_halt_and_resumption_close_the_historical_interval():
    from server.common.official_historical_daily_evidence import historical_daily_evidence

    halt = historical_document("证券代码：600621。关于筹划控股股东变更事项的停牌公告。"
                               "公司股票于2026年10月26日（星期一）开市起开始停牌。",
                               date="2026-10-26", identifier=1)
    resume = historical_document("证券代码：600621。关于公司控股股东拟发生变更暨复牌的提示性公告。"
                                 "公司股票自2026年10月28日（星期三）开市起复牌。",
                                 date="2026-10-28", identifier=2)
    assert historical_daily_evidence([halt, resume], stock_code="600621", day="2026-10-27",
                                     cutoff=datetime(2026, 11, 7, 10, 0))[0] == "NO_TRADE"


def test_delisted_stock_last_trading_day_retains_verified_public_provider(monkeypatch):
    from server.common import historical_daily_bar_evidence as public
    from server.common import qmt_daily_market_truth

    day, previous, code = "2026-10-27", "2026-10-26", "301139"
    capture = datetime(2026, 11, 7, 10, 0)
    traded = {"stock_code": code, "trade_date": day, "open": 0.66, "close": 0.73, "high": 0.73,
              "low": 0.64, "volume": 12413976, "amount": 8577375, "pre_close": 0.65}
    tencent = public._tencent_row({**traded, "volume": 12414000}, day)
    tencent_prev = public._tencent_row({**traded, "close": 0.65}, previous)
    evidence = {"schema": public.SCHEMA,
                "sina": public._identity(public.SINA_PROVIDER, capture.isoformat(" "), coverage.canonical_bar(traded)),
                "tencent": public._identity(public.TENCENT_PROVIDER, capture.isoformat(" "), tencent),
                "tencent_previous": public._identity(public.TENCENT_PROVIDER, capture.isoformat(" "), tencent_prev)}
    mismatch = {**evidence, "tencent": public._identity(public.TENCENT_PROVIDER, capture.isoformat(" "),
                                                       {**tencent, "volume": "12414100.000000"})}
    with pytest.raises(ValueError, match="sources disagree"):
        public.verify_public_bar(mismatch, stock_code=code, day=day, previous_day=previous, cutoff=capture)
    wrong_close = {**evidence, "tencent_previous": public._identity(public.TENCENT_PROVIDER, capture.isoformat(" "),
                                                                  {**tencent_prev, "close": "0.660000"})}
    with pytest.raises(ValueError, match="sources disagree"):
        public.verify_public_bar(wrong_close, stock_code=code, day=day, previous_day=previous, cutoff=capture)
    engine = database(monkeypatch)
    catalog, calendar = authorities()
    catalog.eligible_codes = lambda target: [code]
    calendar.sessions_between = lambda start, end: [day] if start == end == day else [previous, day]
    monkeypatch.setattr(coverage, "load_stock_catalog", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(coverage, "load_trade_calendar_receipt", lambda *args, **kwargs: calendar)
    monkeypatch.setattr(qmt_daily_market_truth, "load_trade_calendar_receipt", lambda *args, **kwargs: calendar)
    proof = coverage.build_manifest(catalog=catalog, calendar=calendar, day=day, rows=[traded],
                                    run_id="mixed-historical", captured_at=capture, historical_documents={},
                                    historical_public_bars={code: evidence}, previous_session=previous)
    with engine.begin() as connection:
        connection.execute(text("""CREATE TABLE qmt_datadir_import_partition (
            dataset TEXT, trade_date TEXT, source_root TEXT, status TEXT, code_count INTEGER, row_count INTEGER,
            content_sha256 TEXT, run_id TEXT, started_at TEXT, finished_at TEXT, detail_json TEXT)"""))
        connection.execute(text("""INSERT INTO qmt_datadir_import_partition VALUES (
            'stock_daily', :day, 'DAT', 'COMPLETE', 1, 1, 'source', 'mixed-historical', :captured, :captured, :detail)"""),
                           {"day": day, "captured": capture, "detail": json.dumps({
                               "full_market_manifest_hash": proof["manifest_hash"], "coverage": 1.0,
                               "readback_rows": 1, "readback_codes": 1,
                           })})
        coverage.insert_manifest(connection, proof)
        connection.execute(text("INSERT INTO sm_stock_kline VALUES (:stock_code,:trade_date,:open,:close,:high,:low,:volume,:amount,:pre_close,'qmt_datadir',1,0,'VERIFIED','SUPPORTED')"), traded)
        with pytest.raises(ValueError, match="provider or quality differs"):
            coverage.load_daily_manifest(connection, day, decision_known_at=capture)
        connection.execute(text("UPDATE sm_stock_kline SET data_source='sina_history', quality_status='SOURCE_VERIFIED' WHERE stock_code=:code"), {"code": code})
        result = qmt_daily_market_truth.load_qmt_daily_market_truth(connection, start_date=day, end_date=day,
                                                                   decision_known_at=capture)
        assert result.as_dict()["bar_providers"] == {"qmt_datadir": 0, "sina_history": 1}
        assert result.as_dict()["public_bar_codes"] == [code]
    engine.dispose()


def test_uint64_repair_preserves_original_scope_and_blocks_old_daily_cutoff(monkeypatch):
    from server.common import qmt_datadir_minute_coverage as minute
    from tools import import_qmt_datadir_daily as importer

    engine = database(monkeypatch)
    before = coverage.load_daily_manifest(engine, DAY, decision_known_at=CUTOFF)
    old_amount = 1100
    corrected_amount = coverage.NATIVE_AMOUNT_WORD * 5 + old_amount
    row = (TRADED, DAY, DAY, 10, 11, 11, 10, 100, corrected_amount, 10, 1, 10)
    harvested = {"rows": [row], "codes": {TRADED}}
    corrected = importer._exact_amount_repair_manifest(engine, session=date.fromisoformat(DAY),
                                                       harvested=harvested, run_id="uint64-correction")
    correction = corrected["native_amount_correction"]
    assert correction["prior_manifest_hash"] == before["manifest_hash"]
    assert correction["rows"] == [{"stock_code": TRADED, "previous_amount": "1100.000000",
                                   "amount": f"{corrected_amount}.000000"}]
    assert [entity for entity in corrected["entities"] if entity["stock_code"] != TRADED] == [
        entity for entity in before["entities"] if entity["stock_code"] != TRADED]
    invalid = list(row)
    invalid[7] = 101  # A different volume may never be laundered as an amount-only repair.
    with pytest.raises(ValueError, match="other business fields differ"):
        importer._exact_amount_repair_manifest(engine, session=date.fromisoformat(DAY),
                                              harvested={"rows": [tuple(invalid)]}, run_id="invalid")
    assert importer._exact_amount_repair_manifest(engine, session=date.fromisoformat(DAY),
                                                  harvested={"rows": [tuple(invalid)]}, run_id="invalid", required=False) is None
    with engine.begin() as connection:
        connection.execute(text("""CREATE TABLE qmt_datadir_import_partition (
            dataset TEXT, trade_date TEXT, source_root TEXT, status TEXT, code_count INTEGER, row_count INTEGER,
            content_sha256 TEXT, run_id TEXT, started_at TEXT, finished_at TEXT, detail_json TEXT)"""))
        captured = datetime.fromisoformat(corrected["captured_at"])
        finished = captured + timedelta(seconds=1)
        prior_detail = {"row_hash": "old-fingerprint", "full_market_manifest_hash": before["manifest_hash"],
                        "coverage": 1.0, "readback_rows": 1, "readback_codes": 1}
        connection.execute(text("""INSERT INTO qmt_datadir_import_partition VALUES (
            'stock_daily', :day, 'DAT', 'COMPLETE', 1, 1, 'source', :run_id, :at, :at, :detail)"""),
                           {"day": DAY, "at": CUTOFF, "run_id": before["run_id"], "detail": json.dumps(prior_detail)})
        monkeypatch.setattr(importer, "persisted_fingerprint", lambda *args, **kwargs: {"row_hash": "old-fingerprint"})
        missing, planned, prior = importer._historical_append_plan(connection, day=DAY, proof=corrected)
        assert missing == set() and planned == correction["rows"]
        connection.execute(text("UPDATE sm_stock_kline SET amount=:value"), {"value": corrected_amount})
        coverage.insert_manifest(connection, corrected)
        connection.execute(text("""UPDATE qmt_datadir_import_partition SET
            run_id=:run_id,started_at=:captured,finished_at=:finished,detail_json=:detail"""),
                           {"run_id": corrected["run_id"], "captured": captured, "finished": finished,
                            "detail": json.dumps({**prior_detail, "full_market_manifest_hash": corrected["manifest_hash"],
                                                  "prior_completion": prior}, default=str)})
        assert coverage.load_daily_manifest(connection, DAY, decision_known_at=finished) == corrected
        assert coverage.load_daily_manifest(connection, DAY, decision_known_at=captured) is None
        with pytest.raises(ValueError, match="database bars differ"):
            coverage.load_daily_manifest(connection, DAY, decision_known_at=CUTOFF)
        assert coverage.load_daily_manifest(connection, DAY, decision_known_at=CUTOFF, verify_rows=False) == before

        scope = minute.trade_scope(connection, DAY, manifest_hash=before["manifest_hash"], verify_rows=False)
        count = len(minute.CORE_GRID)
        matrix = {"row_count": count, "code_count": 1, "row_hash": "minute-hash", "coverage": 1.0,
                  "eligible_code_count": 1, "full_market_manifest_hash": before["manifest_hash"],
                  "expected_traded_code_set_sha256": scope["expected_traded_code_set_sha256"],
                  "minute_grid_count": count, "minute_grid_sha256": minute.GRID_HASH, "bad_grid_codes": 0}
        connection.execute(text("""INSERT INTO qmt_datadir_import_partition VALUES (
            'stock_minute', :day, 'DAT', 'COMPLETE', 1, :count, 'source', 'native-minute', :at, :at, :detail)"""),
                           {"day": DAY, "count": count, "at": CUTOFF,
                            "detail": json.dumps({**matrix, "readback_rows": count, "readback_codes": 1})})
        def read_matrix(conn, day, *, scope, **kwargs):
            assert scope["full_market_manifest_hash"] == before["manifest_hash"]
            assert scope["codes"] == (TRADED,) and scope["daily_close_by_code"] == {TRADED: "11.000000"}
            return matrix
        monkeypatch.setattr(minute, "persisted_matrix", read_matrix)
        assert minute.load_verified_minute_publication(connection, DAY, decision_known_at=finished)["run_id"] == "native-minute"

    latest_catalog, latest_calendar = authorities()
    latest_catalog.member_set_hash = "new-unrelated-catalog-set"
    monkeypatch.setattr(importer, "load_stock_catalog", lambda *args, **kwargs: latest_catalog)
    monkeypatch.setattr(importer, "load_trade_calendar_receipt", lambda *args, **kwargs: latest_calendar)
    class CompletedClock(datetime):
        @classmethod
        def now(cls, timezone=None):
            return finished.replace(tzinfo=timezone)
    monkeypatch.setattr(importer.dt, "datetime", CompletedClock)
    assert importer._reusable_partition(engine, session=date.fromisoformat(DAY), harvested=harvested) == corrected
    engine.dispose()


def test_post_close_block_revision_requires_exact_published_delta_and_prior_minute_grid():
    from server.common import qmt_post_close_block_evidence as block

    day, code = "2026-11-03", "920006"
    old = coverage.canonical_bar({**bar(), "stock_code": code, "trade_date": day,
                                  "close": 18.59, "open": 17.59, "low": 17.59, "high": 19.42,
                                  "volume": 5420900, "amount": 100775139, "pre_close": 17.84})
    final = {**old, "volume": "5702200.000000", "amount": "105924680.000000"}
    cells = [day, code, "晟楠科技", "18.31", "281242", "514.95", "buy seat", "sell seat", "A股"]
    rows = [block._minute_row(day + " " + slot, 18.59, int(index < 238), int(index < 238))
            for index, slot in enumerate(block.CORE_GRID)]
    rows[-1].update(volume="56784.000000", amount="105924330.000000")
    prior = "a" * 64
    cutoff = datetime(2026, 11, 7, 10, 0)
    evidence = {"publication": {"source_url": block._url(code, day), "source_row": cells,
                                 "raw_response_sha256": "b" * 64, "source_row_sha256": coverage.canonical_digest(cells),
                                 "observed_at": cutoff.isoformat(" ")},
                "minute_run_id": "existing-native", "minute_partition_row_hash": "c" * 64,
                "minute_partition_source_sha256": "d" * 64, "native_minute_dat_sha256": "e" * 64,
                "minute_finished_at": day + " 19:09:50", "prior_daily_manifest_hash": prior,
                "minute_rows": rows, "minute_rows_sha256": coverage.canonical_digest(rows)}
    assert coverage.native_amount_correction(old, final) is None
    assert block.verify_post_close_revision(old, final, evidence, prior_hash=prior, cutoff=cutoff)["kind"] == block.KIND
    with pytest.raises(ValueError, match="unrelated"):
        block.verify_post_close_revision(old, {**final, "close": "18.600000"}, evidence, prior_hash=prior, cutoff=cutoff)
    with pytest.raises(ValueError, match="source precision"):
        block.verify_post_close_revision(old, {**final, "amount": "105924681.000000"}, evidence,
                                         prior_hash=prior, cutoff=cutoff)
    with pytest.raises(ValueError, match="identity differs"):
        block.verify_post_close_revision(old, final, evidence, prior_hash=prior, cutoff=cutoff - timedelta(seconds=1))
    changed = deepcopy(evidence)
    changed["minute_rows"][-1]["amount"] = "105923930.000000"
    changed["minute_rows_sha256"] = coverage.canonical_digest(changed["minute_rows"])
    with pytest.raises(ValueError, match="source precision"):
        block.verify_post_close_revision(old, final, changed, prior_hash=prior, cutoff=cutoff)


@pytest.mark.parametrize("canonical_source", ["free", "native", "corrupt"])
def test_strategy_daily_window_resolves_retained_free_receipt_against_current_rows(
    monkeypatch, canonical_source,
):
    from server.common import qmt_daily_market_truth, strategy_daily_input_window
    from server.common import verified_daily_market_truth as free

    engine = database(monkeypatch)
    catalog, calendar = authorities()
    catalog.eligible_codes = lambda day: [TRADED]
    monkeypatch.setattr(free, "load_stock_catalog", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(
        qmt_daily_market_truth, "load_trade_calendar_receipt", lambda *args, **kwargs: calendar,
    )
    captured_at = "2026-11-02 18:00:00"
    receipt = {
        "schema": free.VERIFIED_DAILY_RECEIPT_SCHEMA,
        "dataset": free.FREE_DAILY_DATASET,
        "source": "tencent_qt",
        "target_date": DAY,
        "captured_at": captured_at,
        "row_count": 1,
        "code_count": 1,
        "code_set_hash": coverage.canonical_digest([TRADED]),
        "row_hash": coverage.canonical_digest(free.canonical_daily_rows([bar()])),
        "coverage_ratio": 1.0,
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
    }
    receipt["receipt_id"] = coverage.canonical_digest(receipt)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE acquisition_partition_state (
                dataset TEXT, source TEXT, target_date TEXT, partition_key TEXT,
                status TEXT, written_rows INTEGER, last_success_at TEXT, detail_json TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO acquisition_partition_state
            VALUES ('stock_daily', 'tencent_qt', :day, '__daily_partition__',
                    'complete', 1, :captured_at, :detail)
        """), {"day": DAY, "captured_at": captured_at, "detail": json.dumps(receipt)})
        if canonical_source == "free":
            connection.execute(text("""
                UPDATE sm_stock_kline SET data_source='tencent_qt', quality_status='SOURCE_VERIFIED'
            """))
        elif canonical_source == "corrupt":
            connection.execute(text("UPDATE sm_stock_kline SET volume=101"))

    with engine.connect() as connection:
        if canonical_source == "corrupt":
            with pytest.raises(RuntimeError, match="daily database bars differ from full-market proof"):
                strategy_daily_input_window.validate_daily_input_sessions(
                    connection, sessions=[DAY], decision_known_at=CUTOFF,
                )
        else:
            proof = strategy_daily_input_window.validate_daily_input_sessions(
                connection, sessions=[DAY], decision_known_at=CUTOFF,
            )
            provider = "tencent_qt" if canonical_source == "free" else "qmt_datadir"
            assert proof["latest_daily_truth"]["provider"] == provider
            assert proof["daily_attested_row_count"] == 1
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


def _full_day_halt_document(*, body=None, observed="2026-11-10T19:54:00+08:00"):
    body = body or ("证券代码：688053。关于撤销其他风险警示暨停牌的公告。"
                    "公司股票将于2026年11月10日停牌一天，自2026年11月11日起复牌交易。")
    return {
        "source_url": "https://static.cninfo.com.cn/finalpage/2026-11-10/1225663598.PDF",
        "observed_at": observed,
        "source_text": body,
        "source_text_sha256": hashlib.sha256(body.encode()).hexdigest(),
        "official_document_sha256": "d" * 64,
        "announcement_id": "1225663598",
        "announcement_time_ms": 1794240000000,
        "source_row_sha256": "f" * 64,
    }


def test_post_close_daily_import_accepts_official_full_day_halt_when_native_status_is_five(monkeypatch):
    from tools import import_qmt_datadir_daily as importer
    from integrations.bigqmt import spool
    from server.common import official_listing_evidence

    day, code, other = "2026-11-10", "688053", "688798"
    cutoff = datetime(2026, 11, 10, 20)
    catalog, calendar = authorities()
    catalog.eligible_codes = lambda day: [TRADED, code, other]
    calendar.sessions_between = lambda start, end: [day]
    quote = {"time": int(datetime(2026, 11, 10, 15, 0, tzinfo=coverage.CHINA).timestamp() * 1000),
             "_probiga_observed_at": "2026-11-10 15:10:06",
             "_probiga_acquisition_method": "ContextInfo.get_full_tick", "stockStatus": 5,
             "open": 0, "high": 0, "low": 0, "volume": 0, "amount": 0}
    native = {**snapshot(), "generated_at": "2026-11-10 19:56:00",
              "quotes": {code + ".SH": quote, other + ".SH": {**quote, "stockStatus": 7}}}
    with pytest.raises(ValueError, match="native post-close suspension is unproven: 688053"):
        coverage.suspension_evidence(native, code, day, cutoff)

    engine = database(monkeypatch)

    class Clock(datetime):
        @classmethod
        def now(cls, timezone=None):
            return cutoff.replace(tzinfo=timezone)

    monkeypatch.setattr(importer.dt, "datetime", Clock)
    for module in (importer, coverage):
        monkeypatch.setattr(module, "load_stock_catalog", lambda *args, **kwargs: catalog)
        monkeypatch.setattr(module, "load_trade_calendar_receipt", lambda *args, **kwargs: calendar)
    monkeypatch.setattr(spool, "read_snapshot", lambda *args, **kwargs: native)
    monkeypatch.setattr(importer, "retained_listing_evidence", lambda *args, **kwargs: {})
    monkeypatch.setattr(official_listing_evidence, "load_official_listing_evidence", lambda *args, **kwargs: {})

    def official(codes, *, target_date):
        assert codes == [code] and target_date == day
        return {code: [_full_day_halt_document()]}

    monkeypatch.setattr(importer, "load_daily_suspension_documents", official)
    harvested = {"codes": {TRADED}, "rows": [(TRADED, day, day, 10, 11, 11, 10, 100, 1100, 10, 1, 10)]}
    proof = importer._coverage_manifest(engine, session=date.fromisoformat(day), harvested=harvested, run_id="qdd-exact")
    assert [(row["stock_code"], row["classification"]) for row in proof["entities"]] == [
        (TRADED, "TRADED"), (code, "NO_TRADE"), (other, "NO_TRADE"),
    ]
    assert [row["evidence"]["kind"] for row in proof["entities"][1:]] == [
        "OFFICIAL_DAILY_SUSPENSION", "NATIVE_QMT_SUSPENSION",
    ]
    quote["volume"] = 1
    with pytest.raises(ValueError, match="requires target-day native zero turnover"):
        importer._coverage_manifest(engine, session=date.fromisoformat(day), harvested=harvested, run_id="conflict")
    quote["volume"] = 0
    from server.api import scheduler_runtime
    from server.common import component_release

    with monkeypatch.context() as production:
        production.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
        production.setattr(importer, "_daily_transaction", lambda engine: engine.begin())
        production.setattr(component_release, "runtime_component_build_sha", lambda role: "a" * 40)
        production.setattr(
            scheduler_runtime, "_verified_linux_activation_build",
            lambda *args, **kwargs: (None, "linux_active_lease_unavailable"),
        )
        with pytest.raises(RuntimeError, match="official daily suspension Linux reader is unavailable"):
            importer._publish(
                engine, session=date.fromisoformat(day), rows=harvested["rows"], covered_codes={TRADED},
                short_names={}, run_id="qdd-exact", source_root=None, content_sha256="b" * 64,
                eligible_code_count=3, coverage_manifest=proof,
            )
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_kline SET trade_date=:day"), {"day": day})
        coverage.insert_manifest(connection, proof)
    assert coverage.load_daily_manifest(engine, day, decision_known_at=cutoff) == proof
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM sm_stock_kline")).scalar_one() == 1
    from server.common.qmt_attestation_contract import canonical_digest

    tampered = deepcopy(proof)
    entity = tampered["entities"][1]
    entity["evidence"]["native_tick"]["amount"] = 1
    entity["row_hash"] = canonical_digest({k: v for k, v in entity.items() if k != "row_hash"})
    tampered["manifest_hash"] = canonical_digest({k: v for k, v in tampered.items() if k != "manifest_hash"})
    with pytest.raises(ValueError, match="requires target-day native zero turnover"):
        coverage.validate_manifest(tampered, catalog=catalog, calendar=calendar)
    engine.dispose()


@pytest.mark.parametrize("change", ["late", "wrong_issuer", "partial", "resumed", "misbound_notice"])
def test_official_same_day_suspension_rejects_unavailable_or_inexact_issuer_evidence(change):
    from server.common.official_historical_daily_evidence import official_daily_suspension_evidence

    body = {
        "wrong_issuer": "证券代码：688798。公司股票将于2026年11月10日停牌一天。",
        "partial": "证券代码：688053。公司股票将于2026年11月10日停牌半天。",
        "resumed": "证券代码：688053。公司股票将于2026年11月10日停牌一天，另将于2026年11月10日复牌。",
    }.get(change)
    doc = _full_day_halt_document(body=body, observed=(
        "2026-11-10T20:00:01+08:00" if change == "late" else "2026-11-10T19:54:00+08:00"
    ))
    if change == "misbound_notice":
        doc["announcement_id"] = "1225664395"
    with pytest.raises(ValueError, match="official|document identity or time"):
        official_daily_suspension_evidence(
            [doc], stock_code="688053", day="2026-11-10", cutoff=datetime(2026, 11, 10, 20),
        )


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
