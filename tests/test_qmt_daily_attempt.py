from datetime import datetime, timedelta
import json

from sqlalchemy import create_engine, event, text
import pytest

from integrations.bigqmt import spool
from integrations.qmt.datadir_import import Universe, UniverseMember
from server.common import official_listing_evidence
from server.common import qmt_datadir_daily_coverage as coverage
from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from tests.test_qmt_datadir_daily_coverage import CUTOFF, DAY, IPO, SUSPENDED, TRADED, authorities, bar, listing, snapshot
from tests.test_qmt_datadir_import import SESSION, _record, _write
from tools import import_qmt_datadir_daily as daily


def attempt_database():
    engine = create_engine("sqlite://")
    event.listen(engine, "connect", lambda connection, _: connection.create_function("NOW", 0, lambda: "2026-11-28 00:01:00"))
    privileged_migrate_formal_analysis_schema(engine)
    with engine.begin() as c:
        c.execute(text("""CREATE TABLE qmt_datadir_import_partition (
            dataset TEXT,trade_date DATE,source_root TEXT,status TEXT,code_count INT,row_count INT,
            content_sha256 TEXT,run_id TEXT,started_at TEXT,finished_at TEXT,detail_json TEXT,
            PRIMARY KEY(dataset,trade_date))"""))
    return engine


def test_partial_daily_receipt_retains_bars_and_legal_absences_until_missing_issuer_resolves(monkeypatch, tmp_path):
    engine = attempt_database()
    catalog, calendar = authorities()
    monkeypatch.setattr(daily, "_daily_transaction", lambda engine: engine.begin())
    monkeypatch.setattr(daily, "load_stock_catalog", lambda *a, **kw: catalog)
    monkeypatch.setattr(daily, "load_trade_calendar_receipt", lambda *a, **kw: calendar)
    monkeypatch.setattr(daily, "retained_listing_evidence", lambda *a, **kw: {})
    monkeypatch.setattr(daily, "load_daily_suspension_documents", lambda *a, **kw: {})
    monkeypatch.setattr(spool, "read_snapshot", lambda *a, **kw: snapshot())
    monkeypatch.setattr(official_listing_evidence, "load_official_listing_evidence", lambda *a, **kw: {})
    session = datetime.fromisoformat(DAY).date()
    values = ("10", "11", "11", "10", "100", "1100", "10", "1", "10")
    harvested = {"rows": [(TRADED, DAY, session, *values)], "codes": {TRADED}, "digest": "e"*64,
                 "reused_codes": 0, "native_records": {TRADED+".SH": {"record_sha256": "c"*64, "values": list(values)}}}
    def classify(run):
        return daily._coverage_manifest(engine, session=session, harvested=harvested, run_id=run,
                                        attempt_root=tmp_path, save_partial=True)

    partial = classify("attempt")
    assert partial["status"] == "PARTIAL" and partial["strategy_eligible"] is False
    receipt = daily.load_receipt(engine, dataset=daily.ATTEMPT_DATASET, trade_date=DAY)
    detail = receipt["detail"]
    assert set(detail["unresolved_codes"]) == {IPO} and set(detail["no_bar_artifacts"]) == set(SUSPENDED)
    assert receipt["status"] == "BLOCKED" and receipt["row_count"] == 1 and detail["eligible_code_count"] == 8
    key = "bars." + TRADED[:4]
    assert FormalAnalysisStore(engine).load_artifact(detail["bar_group_artifacts"][key])[TRADED]["stock_code"] == TRADED
    assert daily._retained_native_records(engine, day=DAY, source_root=tmp_path) == harvested["native_records"]
    assert daily.load_receipt(engine, dataset=daily.DATASET, trade_date=DAY) is None
    with pytest.raises(ValueError):
        daily.validate_manifest(partial, catalog=catalog, calendar=calendar)
    with engine.connect() as c, pytest.raises(ValueError, match="incomplete daily coverage"):
        daily.insert_manifest(c, partial)

    # The full tick file advanced to another session while the issuer notice became available.
    monkeypatch.setattr(spool, "read_snapshot", lambda *a, **kw: {})
    def resolve(codes, **kwargs):
        assert codes == [IPO]
        return listing()
    monkeypatch.setattr(official_listing_evidence, "load_official_listing_evidence", resolve)
    exact = classify("complete")
    assert daily.validate_manifest(exact, catalog=catalog, calendar=calendar)["status"] == "EXACT"


@pytest.mark.parametrize("legacy", [False, True])
def test_verified_stock_reads_only_its_available_attempt_group(monkeypatch, tmp_path, legacy):
    engine = attempt_database()
    catalog, calendar = authorities()
    other = "000001"
    catalog.eligible_codes = lambda _: sorted((TRADED, other, IPO, *SUSPENDED))
    rows = [bar(), {**bar(), "stock_code": other}]
    proof = coverage.build_manifest(catalog=catalog, calendar=calendar, day=DAY, rows=rows, run_id="attempt",
                                    captured_at=CUTOFF, snapshot=snapshot(), allow_partial=True,
                                    unresolved_codes={IPO: "issuer pending"})
    generation = proof["manifest_hash"]
    values = ["10", "11", "11", "10", "100", "1100", "10", "1", "10"]
    native = {symbol: {"record_sha256": "c"*64, "values": list(values)} for symbol in [TRADED+".SH", other+".SZ"]}
    harvested = {"native_records": native, "digest": "e" * 64, "reused_codes": 0}
    monkeypatch.setattr(daily, "_daily_transaction", lambda engine: engine.begin())
    native[TRADED+".SH"]["values"][4] = "99"
    with pytest.raises(ValueError, match="bar differs from native DAT"):
        daily._save_daily_attempt(engine, proof=proof, source_root=tmp_path, harvested=harvested, previous_run=(None, None))
    native[TRADED+".SH"]["values"][4] = "100"
    daily._save_daily_attempt(engine, proof=proof, source_root=tmp_path, harvested=harvested, previous_run=(None, None))
    receipt = daily.load_receipt(engine, dataset=daily.ATTEMPT_DATASET, trade_date=DAY)
    detail = receipt["detail"]
    if legacy:
        values = sorted((daily.canonical_bar(row) for row in rows), key=lambda r: r["stock_code"])
        detail.pop("bar_group_artifacts")
        detail.update(schema="probiga.qmt-daily-attempt.v1", bars_artifact_sha256=
                      FormalAnalysisStore(engine).put(generation, "daily_attempt", "bars", values))
        with engine.begin() as conn:
            conn.execute(text("UPDATE qmt_datadir_import_partition SET detail_json=:d"), {"d": json.dumps(detail)})

    def read(codes, cutoff="2026-11-28 00:01:00+08:00"):
        return coverage.read_verified_attempt_bars(engine, DAY, codes, decision_known_at=cutoff)

    assert not read([TRADED], "2026-11-27T16:00:59Z")  # observed but not yet durably available
    assert not read([IPO, *SUSPENDED])
    assert read([TRADED, IPO]) == {TRADED: {**daily.canonical_bar(bar()), "data_source": "qmt_datadir",
                                           "known_at": "2026-11-28 00:01:00"}}
    if legacy:
        statements = []
        event.listen(engine, "before_cursor_execute", lambda _, __, sql, *args: statements.append(sql))
        assert set(read([other])) == {other}
        assert not any("st_formal_analysis_artifact" in sql for sql in statements)
        # A different receipt must authenticate against its original immutable publication.
        with engine.begin() as conn:
            conn.execute(text("UPDATE qmt_datadir_import_partition SET run_id='another'"))
        with pytest.raises(ValueError, match="publication seal"):
            read([TRADED])
    else:
        # A corrupted unrelated shard is never downloaded by a one-security request.
        from server.common.formal_analysis_store import FormalAnalysisArtifactIntegrityError
        with engine.begin() as conn:
            conn.execute(text("UPDATE st_formal_analysis_artifact SET payload_zlib=x'00' WHERE artifact_sha256=:sha"),
                         {"sha": detail["bar_group_artifacts"]["bars." + other[:4]]})
        assert set(read([TRADED])) == {TRADED}
        with pytest.raises(FormalAnalysisArtifactIntegrityError):
            read([other])
        with engine.begin() as conn:
            conn.execute(text("UPDATE qmt_datadir_import_partition SET finished_at='2026-11-27 23:59:59'"))
        with pytest.raises(ValueError, match="receipt seal"):
            read([TRADED])
    engine.dispose()


def test_reuse_requires_same_native_record_and_rechecks_duplicate_or_corrected_dat(tmp_path, monkeypatch):
    code = "600000"
    sym = code + ".SH"
    member = UniverseMember(sym, "SH", code)
    universe = Universe({sym: member}, {sym: (SESSION.isoformat(), None)})
    path = tmp_path / "SH" / daily.DAILY_PERIOD / (code + ".DAT")
    first = _record("15:00", 9600, 9700, 9500, 9650, 100, 96000, 9590)
    _write(path, [first])
    source = daily._harvest(tmp_path, universe, SESSION, isolate_invalid=True)
    decoder = daily.iter_daily_rows
    monkeypatch.setattr(daily, "iter_daily_rows", lambda *a: pytest.fail("verified record decoded again"))
    def resume():
        return daily._harvest(tmp_path, universe, SESSION, isolate_invalid=True,
                              retained_loader=lambda: source["native_records"])
    reused = resume()
    assert reused["reused_codes"] == 1 and reused["rows"] == source["rows"] and reused["digest"] == source["digest"]
    _write(path, [first, first])
    assert code in resume()["invalid_codes"]
    monkeypatch.setattr(daily, "iter_daily_rows", decoder)
    first[4] += 1
    _write(path, [first])
    revised = resume()
    assert revised["reused_codes"] == 0 and revised["rows"] != source["rows"] and revised["digest"] != source["digest"]


def test_native_placeholders_need_independent_no_trade_evidence_and_read_failures_are_isolated(tmp_path, monkeypatch):
    symbols = ["600001", "600002", "600003", "600004"]
    members = {code+".SH": UniverseMember(code+".SH", "SH", code) for code in symbols}
    universe = Universe(members, {sym: (SESSION.isoformat(), None) for sym in members})
    for code, volume, amount in zip(symbols, [0, 0, 10, 100], [0, 100, 0, 10000]):
        path = tmp_path / "SH" / daily.DAILY_PERIOD / (code+".DAT")
        _write(path, [_record("15:00", 9600, 9700, 9500, 9650, volume, amount, 9590)])
    result = daily._harvest(tmp_path, universe, SESSION, isolate_invalid=True)
    assert result["codes"] == {symbols[-1]}
    assert set(result["invalid_codes"]) == {symbols[1], symbols[2]}
    assert set(daily._harvest(tmp_path, universe, SESSION)["codes"]) == set(symbols)
    loader = daily.load_dat
    def permission_failure(root, market, period, code):
        if code == symbols[1]:
            raise PermissionError("file temporarily unavailable")
        return loader(root, market, period, code)
    monkeypatch.setattr(daily, "load_dat", permission_failure)
    result = daily._harvest(tmp_path, universe, SESSION, isolate_invalid=True)
    assert result["codes"] == {symbols[-1]}
    assert "temporarily unavailable" in result["invalid_codes"][symbols[1]]

def test_multi_session_daily_import_continues_past_an_unproved_security(tmp_path, monkeypatch, capsys):
    target = SESSION + timedelta(days=1)
    symbols = ["600001", "600002"]
    members = {code+".SH": UniverseMember(code+".SH", "SH", code) for code in symbols}
    universe = Universe(members, {sym: (SESSION.isoformat(), None) for sym in members})
    for code in symbols:
        first = _record("15:00", 9600, 9700, 9500, 9650, 100, 96000, 9590)
        following = first.copy()
        following[0] += 86400
        records = [first, following] if code == symbols[0] else [following]
        _write(tmp_path / "SH" / daily.DAILY_PERIOD / (code+".DAT"), records)
    engine = create_engine("sqlite://")
    catalog, calendar = authorities()
    catalog.eligible_codes = lambda day: symbols
    calendar.sessions_between = lambda start, end: [start]
    monkeypatch.setattr(daily, "create_batch_engine", lambda: engine)
    monkeypatch.setattr(daily, "load_project_env", lambda: None)
    monkeypatch.setattr(daily, "load_universe", lambda *a, **kw: universe)
    monkeypatch.setattr(daily, "_sessions", lambda *a: [SESSION,target])
    monkeypatch.setattr(daily, "_existing_sources", lambda *a: [])
    monkeypatch.setattr(daily, "_short_names", lambda *a: {})
    monkeypatch.setattr(daily, "load_receipt", lambda *a, **kw: None)
    monkeypatch.setattr(daily, "_reusable_partition", lambda *a, **kw: None)
    monkeypatch.setattr(daily, "load_stock_catalog", lambda *a, **kw: catalog)
    monkeypatch.setattr(daily, "load_trade_calendar_receipt", lambda *a, **kw: calendar)
    monkeypatch.setattr(spool, "read_snapshot", lambda *a, **kw: {})
    monkeypatch.setattr(daily, "retained_listing_evidence", lambda *a, **kw: {})
    monkeypatch.setattr(official_listing_evidence, "load_official_listing_evidence", lambda *a, **kw: {})
    monkeypatch.setattr(daily, "load_daily_suspension_documents", lambda *a, **kw: {})
    assert daily.main(["--start", SESSION.isoformat(), "--end", target.isoformat(), "--datadir-root", str(tmp_path)]) == 1
    first, second = json.loads(capsys.readouterr().out)["results"]
    assert first["status"] == "BLOCKED" and set(first["unresolved_codes"]) == {symbols[1]}
    assert second["status"] == "READY" and second["rows"] == 2
