"""Contract tests for the QMT datadir importer.

The expected minute values are the production publisher's rows for 600000.SH
on 2026-10-14, which the decoder was validated against row by row.
"""
from __future__ import annotations

from contextlib import nullcontext
import datetime as dt
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
import json
from types import SimpleNamespace

import numpy as np
import pytest

from integrations.qmt.datadir import (
    CST,
    DAT_HEADER,
    DAILY_PERIOD,
    MINUTE_PERIOD,
    DatadirError,
    iter_daily_rows,
    iter_index_daily_rows,
    iter_index_minute_rows,
    iter_minute_rows,
    load_dat,
    read_dat,
    select_records,
)
from integrations.qmt.datadir_import import (
    SOURCE,
    Universe,
    UniverseMember,
    existing_sources,
    plan,
)
from server.common.qmt_datadir_result import validate_task_result
from server.common.official_historical_daily_evidence import official_daily_delisting_evidence
from tools import import_qmt_datadir_daily as daily_import

SESSION = dt.date(2026, 10, 14)
GRID = frozenset(
    [f"{9 + (30 + i) // 60:02d}:{(30 + i) % 60:02d}:00" for i in range(121)]
    + [f"{13 + (1 + i) // 60:02d}:{(1 + i) % 60:02d}:00" for i in range(120)]
)


def _epoch(moment: str, session: dt.date = SESSION) -> int:
    hour, minute = (int(part) for part in moment.split(":"))
    return int(dt.datetime(session.year, session.month, session.day, hour, minute,
                           tzinfo=CST).timestamp())


def _record(moment, open_, high, low, close, volume, amount, prev, offset=0):
    words = np.zeros(16, dtype="<u4")
    words[0] = _epoch(moment, SESSION) + offset
    words[1], words[2], words[3], words[4] = open_, high, low, close
    words[6] = volume
    words[8] = amount
    words[13] = prev
    return words


def _write(path: Path, records) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = np.array(records, dtype="<u4").tobytes()
    path.write_bytes(DAT_HEADER + body)
    return path


def test_stale_whole_market_dat_blocks_daily_import_before_listing_lookup(tmp_path, monkeypatch, capsys):
    target = SESSION + dt.timedelta(days=1)
    members = {
        f"{code}.SH": UniverseMember(f"{code}.SH", "SH", code)
        for code in (f"{600000 + i:06d}" for i in range(41))
    }
    universe = Universe(members, {member.qmt_code: (SESSION.isoformat(), None)
                                  for member in members.values()})
    for member in members.values():
        _write(tmp_path / "SH" / DAILY_PERIOD / f"{member.code}.DAT",
               [_record("15:00", 9600, 9700, 9500, 9650, 100, 96000, 9590)])

    build = "a" * 40
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", build)
    monkeypatch.setattr(daily_import, "load_project_env", lambda: None)
    monkeypatch.setattr(daily_import, "create_batch_engine", object)
    monkeypatch.setattr(daily_import, "_sessions", lambda *args: [target])
    monkeypatch.setattr(daily_import, "load_universe", lambda *args, **kwargs: universe)
    monkeypatch.setattr(daily_import, "_short_names", lambda engine: {})
    monkeypatch.setattr(daily_import, "_existing_sources", lambda *args: [])
    monkeypatch.setattr(daily_import, "_reusable_partition", lambda *args, **kwargs: None)
    monkeypatch.setattr(daily_import, "_coverage_manifest",
                        lambda *args, **kwargs: pytest.fail("entered per-stock coverage with stale DAT files"))
    monkeypatch.setattr(daily_import, "_publish",
                        lambda *args, **kwargs: pytest.fail("published a missing partition"))

    rc = daily_import.main(["--start", target.isoformat(), "--end", target.isoformat(),
                            "--datadir-root", str(tmp_path), "--apply", "--json"])
    out = capsys.readouterr()
    receipt = json.loads(out.out)
    assert receipt["target_trade_date"] == target.isoformat()
    assert receipt["reason_code"] == (
        f"QMT_NATIVE_DAILY_SOURCE_EMPTY target={target} expected_codes=41 exact_codes=0"
    )
    assert validate_task_result("qmt_stock_daily_canonical", receipt, return_code=rc,
                                expected_build_sha=build) == "blocked"


def test_daily_recovery_routes_expired_missing_security_to_official_notice(monkeypatch):
    target = dt.date(2026, 11, 13)
    day = target.isoformat()
    expired = "002731"
    suspended = "000635"
    traded = "600000"
    catalog = SimpleNamespace(
        members=[{"stock_code": expired, "expire_date": day}],
        eligible_codes=lambda value: [expired, suspended, traded],
    )
    engine = SimpleNamespace(connect=lambda: nullcontext(None))
    monkeypatch.setattr(daily_import, "load_stock_catalog", lambda *args, **kwargs: catalog)
    monkeypatch.setattr(daily_import, "load_trade_calendar_receipt", lambda *args, **kwargs: object())
    from integrations.bigqmt import spool
    snapshot = {"source": "native"}
    monkeypatch.setattr(spool, "read_snapshot", lambda *args, **kwargs: snapshot)

    def native_suspension(snap, code, trade_date, cutoff):
        assert snap is snapshot
        assert trade_date == day
        if code != suspended:
            raise ValueError("no native tick")

    def load_delisting(codes, *, target_date):
        assert codes == [expired]
        assert target_date == day
        return {expired: [{"announcement_id": "1225669888"}]}

    monkeypatch.setattr(daily_import, "suspension_evidence", native_suspension)
    monkeypatch.setattr(daily_import, "load_daily_delisting_documents", load_delisting)
    monkeypatch.setattr(daily_import, "retained_listing_evidence",
                        lambda *args, **kwargs: pytest.fail("delisting was routed to listing lookup"))
    monkeypatch.setattr(daily_import, "build_manifest", lambda **kwargs: kwargs)
    args = daily_import._coverage_manifest(
        engine, session=target, harvested={"rows": [], "codes": {traded}}, run_id="worker",
    )
    assert args["daily_delisting_documents"] == {expired: [{"announcement_id": "1225669888"}]}
    assert args["daily_suspension_documents"] == {}


def test_indexed_daily_delisting_checks_identity_effective_day_and_known_time():
    target = dt.date(2026, 11, 13)
    published = dt.datetime.combine(target, dt.time(), tzinfo=CST)
    observed = published + dt.timedelta(days=1, hours=21)
    body = "证券代码：002731。关于公司股票终止上市暨摘牌的公告。摘牌日期：2026年11月13日。"
    document = {
        "source_url": "https://static.cninfo.com.cn/finalpage/2026-11-13/1225669888.PDF",
        "observed_at": observed.isoformat(),
        "source_text": body,
        "source_text_sha256": sha256(body.encode()).hexdigest(),
        "official_document_sha256": "a" * 64,
        "announcement_id": "1225669888",
        "announcement_time_ms": int(published.timestamp() * 1000),
        "source_row_sha256": "b" * 64,
    }
    proof = official_daily_delisting_evidence(
        [document], stock_code="002731", day=target.isoformat(), cutoff=observed,
    )
    assert proof["documents"] == [document]
    with pytest.raises(ValueError, match="unproven"):
        official_daily_delisting_evidence(
            [document], stock_code="002731", day="2026-11-16", cutoff=observed,
        )
    with pytest.raises(ValueError, match="publication identity"):
        official_daily_delisting_evidence(
            [{**document, "announcement_id": "1225669889"}],
            stock_code="002731", day=target.isoformat(), cutoff=observed,
        )
    with pytest.raises(ValueError, match="identity or time"):
        official_daily_delisting_evidence(
            [document], stock_code="002731", day=target.isoformat(), cutoff=published,
        )


def test_read_dat_rejects_a_foreign_header(tmp_path):
    path = tmp_path / "000001.DAT"
    path.write_bytes(b"\x00" * 72)
    with pytest.raises(DatadirError, match="header"):
        read_dat(path)


def test_read_dat_rejects_a_truncated_record(tmp_path):
    path = tmp_path / "000001.DAT"
    path.write_bytes(DAT_HEADER + b"\x00" * 65)
    with pytest.raises(DatadirError, match="multiple"):
        read_dat(path)


def test_minute_rows_reproduce_the_production_publisher(tmp_path):
    root = tmp_path / "datadir"
    _write(
        root / "SH" / MINUTE_PERIOD / "600000.DAT",
        [
            _record("09:30", 9600, 9600, 9600, 9600, 1604, 1539840, 9590),
            _record("09:31", 9580, 9640, 9570, 9600, 14563, 13995689, 9600),
            _record("09:32", 9600, 9630, 9600, 9610, 7699, 7406616, 9600),
        ],
    )
    dat = load_dat(root, "SH", MINUTE_PERIOD, "600000")
    assert dat is not None
    rows = [row for _, row in iter_minute_rows(dat, select_records(dat, [SESSION]), GRID)]

    assert [row[1].strftime("%H:%M") for row in rows] == ["09:30", "09:31", "09:32"]
    assert rows[0][3] == Decimal("9.600")
    assert rows[0][4] == Decimal("0.010")
    assert rows[0][5] == Decimal("0.010") / Decimal("9.590") * 100
    assert rows[0][6] == Decimal("1604")
    assert rows[0][7] == Decimal("1539840")
    assert rows[0][8] == "600000.SH"
    assert rows[1][4] == Decimal("0.000")
    assert rows[2][5] == Decimal("0.010") / Decimal("9.600") * 100


def test_minute_rows_drop_bars_outside_the_native_grid(tmp_path):
    root = tmp_path / "datadir"
    _write(
        root / "SZ" / MINUTE_PERIOD / "000001.DAT",
        [
            _record("09:30", 1000, 1000, 1000, 1000, 1, 1, 1000),
            _record("13:00", 1000, 1000, 1000, 1000, 1, 1, 1000),
            _record("13:01", 1000, 1000, 1000, 1000, 1, 1, 1000),
        ],
    )
    dat = load_dat(root, "SZ", MINUTE_PERIOD, "000001")
    moments = [row[1].strftime("%H:%M")
               for _, row in iter_minute_rows(dat, select_records(dat, [SESSION]), GRID)]
    assert moments == ["09:30", "13:01"]


def test_minute_rows_ignore_other_sessions(tmp_path):
    root = tmp_path / "datadir"
    _write(
        root / "SZ" / MINUTE_PERIOD / "000001.DAT",
        [
            _record("09:30", 1000, 1000, 1000, 1000, 1, 1, 1000, offset=-86400),
            _record("09:30", 1000, 1000, 1000, 1000, 1, 1, 1000),
        ],
    )
    dat = load_dat(root, "SZ", MINUTE_PERIOD, "000001")
    rows = list(iter_minute_rows(dat, select_records(dat, [SESSION]), GRID))
    assert len(rows) == 1
    assert rows[0][0] == SESSION


def test_daily_rows_convert_lots_to_shares(tmp_path):
    root = tmp_path / "datadir"
    _write(
        root / "SH" / DAILY_PERIOD / "600000.DAT",
        [_record("00:00", 9600, 9790, 9530, 9730, 922942, 896160848, 9640)],
    )
    dat = load_dat(root, "SH", DAILY_PERIOD, "600000")
    rows = list(iter_daily_rows(dat, [SESSION]))
    assert len(rows) == 1
    _, row = rows[0]
    assert row[3] == Decimal("9.600")     # open
    assert row[4] == Decimal("9.730")     # close
    assert row[5] == Decimal("9.790")     # high
    assert row[6] == Decimal("9.530")     # low
    assert row[7] == Decimal("92294200")  # shares, not lots
    assert row[9] == Decimal("9.640")     # pre-close
    assert row[10] == Decimal("0.090")
    assert row[11] == Decimal("0.090") / Decimal("9.640") * 100


@pytest.mark.parametrize("iterator, period, market, code, low, high, amount_column, expected", [
    (iter_daily_rows, DAILY_PERIOD, "SZ", "300308", 2498621527, 5, 8, 23973458007),
    (iter_index_daily_rows, DAILY_PERIOD, "SH", "000001", 3835420247, 179, 8, 772634566231),
    (iter_minute_rows, MINUTE_PERIOD, "SZ", "300308", 0, 1, 7, 4294967296),
    (iter_index_minute_rows, MINUTE_PERIOD, "SH", "000001", 1406993009, 2, 5, 9996927601),
])
def test_turnover_decodes_both_unsigned_words_without_changing_units(
    tmp_path, iterator, period, market, code, low, high, amount_column, expected,
):
    raw = _record("00:00" if period == DAILY_PERIOD else "15:00", 1000, 1000, 1000, 1000, 1, low, 1000)
    raw[9] = high
    _write(tmp_path / market / period / f"{code}.DAT", [raw])
    dat = load_dat(tmp_path, market, period, code)
    if period == DAILY_PERIOD:
        (_, row), = iterator(dat, [SESSION])
    else:
        (_, row), = iterator(dat, select_records(dat, [SESSION]), GRID)
    assert row[amount_column] == Decimal(expected)


def test_load_dat_returns_none_for_a_missing_instrument(tmp_path):
    assert load_dat(tmp_path, "SH", MINUTE_PERIOD, "999999") is None


def test_plan_marks_empty_sessions_as_import():
    plans = plan({}, [SESSION], replace_existing=False)
    assert [item.action for item in plans] == ["IMPORT"]


def test_plan_skips_a_session_held_by_another_source():
    existing = {SESSION: ["gj_big_qmt_inner"]}
    plans = plan(existing, [SESSION], replace_existing=False)
    assert plans[0].action == "SKIP_EXISTING"


def test_plan_replaces_a_foreign_session_only_when_asked():
    existing = {SESSION: ["gj_big_qmt_inner"]}
    plans = plan(existing, [SESSION], replace_existing=True)
    assert plans[0].action == "REPLACE"


def test_plan_reimports_its_own_partial_session():
    existing = {SESSION: [SOURCE]}
    plans = plan(existing, [SESSION], replace_existing=False)
    assert plans[0].action == "IMPORT"


def test_plan_skips_a_session_with_a_complete_receipt():
    existing = {SESSION: [SOURCE]}
    plans = plan(existing, [SESSION], replace_existing=True, already_complete={SESSION})
    assert plans[0].action == "SKIP_COMPLETE"


def test_existing_sources_is_empty_without_sessions():
    assert existing_sources(object(), []) == {}


def _universe(listed: dict[str, tuple[str, str | None]]):
    from integrations.qmt.datadir_import import Universe, UniverseMember

    members = {}
    for qmt_code in listed:
        code, _, market = qmt_code.partition(".")
        members[qmt_code] = UniverseMember(qmt_code=qmt_code, market=market, code=code)
    return Universe(members=members, listed_on=listed)


def test_eligible_codes_follow_the_catalog_listing_window():
    universe = _universe({
        "600000.SH": ("1999-11-10", None),
        "301234.SZ": ("2026-10-01", None),
        "600001.SH": ("1999-01-01", "2026-10-02"),
    })
    assert universe.eligible_codes(dt.date(2026, 9, 30)) == 2   # before the new listing
    assert universe.eligible_codes(dt.date(2026, 10, 1)) == 3    # new one listed, old one not yet expired
    assert universe.eligible_codes(SESSION) == 2                 # old one expired


def test_exact_minute_scope_rejects_even_one_missing_traded_security():
    from server.common.qmt_datadir_minute_coverage import validate_code_scope

    expected = [f"{600000 + i}" for i in range(100)]
    entry = dict(codes=expected[:-1], code_rows={code: 241 for code in expected[:-1]}, rows=99*241)
    with pytest.raises(ValueError, match="exact minute trade scope"):
        validate_code_scope(entry, {"codes": expected})


def test_exact_minute_scope_uses_proven_traded_set_instead_of_catalog_floor():
    from server.common.qmt_datadir_minute_coverage import validate_code_scope

    traded = [f"{600000 + i}" for i in range(5563)]
    validate_code_scope(dict(codes=traded, code_rows={code: 241 for code in traded}, rows=len(traded)*241),
                        {"codes": traded})


def test_harvest_filters_each_dates_exact_traded_set_without_padding(tmp_path, monkeypatch):
    from integrations.qmt import datadir_import as importer
    from server.common.qmt_index_minute_grid import CORE_GRID

    root = tmp_path / "native"
    tomorrow = SESSION + dt.timedelta(days=1)
    universe = _universe({"600000.SH": ("1999-01-01", None), "600001.SH": ("1999-01-01", None)})
    for code in ("600000", "600001"):
        _write(root / "SH" / MINUTE_PERIOD / (code + ".DAT"), [
            _record(moment[:5], 10000, 10000, 10000, 10000, 1, 1000, 10000, offset=offset)
            for offset in (0, 86400) for moment in CORE_GRID])
    staged = []
    monkeypatch.setattr(importer, "_insert_stage", lambda conn, chunk: staged.extend(chunk))
    stats = importer.harvest(object(), root=root, universe=universe.ordered(),
        sessions=[SESSION, tomorrow], run_id="scope-run",
        codes_by_session={SESSION: {"600000"}, tomorrow: {"600001"}})
    assert stats[SESSION]["codes"] == ["600000"]
    assert stats[tomorrow]["codes"] == ["600001"]
    assert stats[SESSION]["rows"] == stats[tomorrow]["rows"] == 241
    assert len(staged) == 482
    assert {(row[1], row[3]) for row in staged} == {("600000", SESSION), ("600001", tomorrow)}


def _publication_database():
    from sqlalchemy import create_engine, text
    from integrations.qmt.datadir_import import MINUTE_COLUMNS, MINUTE_STAGE_COLUMNS
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.connection.driver_connection.create_function("NOW", 0, lambda: "2026-11-03 04:40:00")
        connection.execute(text("CREATE TABLE sm_stock_minute (id INTEGER PRIMARY KEY AUTOINCREMENT," + ",".join(name + " TEXT COLLATE NOCASE" for name in MINUTE_COLUMNS) + ")"))
        connection.execute(text("CREATE TABLE qmt_datadir_minute_stage (" + ",".join(name + " TEXT" for name in MINUTE_STAGE_COLUMNS) + ")"))
        for code in ("600000", "600001"):
            connection.execute(text("INSERT INTO sm_stock_minute (stock_code,trade_date,trade_time,price,data_source) VALUES (:code,:day,:stamp,'1','qmt_datadir')"),
                               {"code": code, "day": SESSION, "stamp": str(SESSION) + " 09:15:00"})
            connection.execute(text("INSERT INTO qmt_datadir_minute_stage VALUES ('run-1',:code,:stamp,:day,'10','0','0','100','1000',:symbol)"),
                               {"code": code, "day": SESSION, "stamp": str(SESSION) + " 09:15:00", "symbol": code + ".SH"})
        connection.execute(text("INSERT INTO sm_stock_minute (stock_code,trade_date,trade_time,price,data_source) VALUES ('600002',:day,:stamp,'2','old')"),
                           {"day": SESSION, "stamp": str(SESSION) + " 09:15:00"})
        connection.execute(text("INSERT INTO sm_stock_minute (stock_code,trade_date,trade_time,price,data_source) VALUES ('600000','2026-10-30','2026-10-30 09:15:00','3','qmt_datadir')"))
    return engine


def test_publish_replaces_its_own_rows_and_superseded_source_in_stock_batches():
    from integrations.qmt.datadir_import import publish_session
    from sqlalchemy import text
    engine = _publication_database()
    with engine.begin() as connection:
        publish_session(connection, session=SESSION, run_id="run-1", batch_id="batch-1",
                        replace_sources=["old"], batch_codes=1)
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT stock_code,price,data_source,batch_id,source_time FROM sm_stock_minute WHERE trade_date=:day ORDER BY stock_code"), {"day": SESSION}).fetchall()
        assert [tuple(row[:4]) for row in rows] == [("600000", "10", "qmt_datadir", "batch-1"), ("600001", "10", "qmt_datadir", "batch-1")]
        assert all(row[4] == str(SESSION) + " 09:15:00" for row in rows)
        assert connection.execute(text("SELECT price FROM sm_stock_minute WHERE trade_date='2026-10-30'")).scalar() == "3"
    engine.dispose()


def test_publish_never_touches_a_foreign_source_by_default():
    from integrations.qmt.datadir_import import publish_session
    from sqlalchemy import text
    engine = _publication_database()
    with engine.begin() as connection:
        publish_session(connection, session=SESSION, run_id="run-1", batch_id="batch-1", replace_sources=(), batch_codes=1)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT price FROM sm_stock_minute WHERE data_source='old'")).scalar() == "2"
        assert connection.execute(text("SELECT COUNT(*) FROM sm_stock_minute WHERE batch_id='batch-1'")).scalar() == 2
    engine.dispose()


def test_publish_keeps_an_exact_partition_without_any_row_writes():
    from integrations.qmt.datadir_import import publish_session
    from sqlalchemy import text

    engine = _publication_database()
    params = dict(session=SESSION, run_id="run-1", replace_sources=(), batch_codes=1)
    with engine.begin() as connection:
        publish_session(connection, batch_id="original", **params)
        before = connection.execute(text("SELECT * FROM sm_stock_minute ORDER BY id")).all()
        for event in ("INSERT", "DELETE", "UPDATE"):
            connection.execute(text(
                f"CREATE TRIGGER no_{event.lower()} BEFORE {event} ON sm_stock_minute "
                "BEGIN SELECT RAISE(ABORT,'identical minute rows must be retained'); END"
            ))
        publish_session(connection, batch_id="later", **params)
        assert connection.execute(text("SELECT * FROM sm_stock_minute ORDER BY id")).all() == before
    engine.dispose()


def test_publish_changes_only_missing_stale_duplicate_or_authorized_provider_rows():
    from integrations.qmt.datadir_import import MINUTE_COLUMNS, publish_session
    from sqlalchemy import text

    engine = _publication_database()
    params = dict(session=SESSION, run_id="run-1", batch_codes=1)
    moment = str(SESSION) + " 09:15:00"
    next_minute = str(SESSION) + " 09:16:00"
    with engine.begin() as connection:
        publish_session(connection, batch_id="original", replace_sources=(), **params)
        kept = connection.execute(text(
            "SELECT * FROM sm_stock_minute WHERE stock_code='600000' AND trade_time=:stamp"
        ), {"stamp": moment}).one()
        columns = ",".join(MINUTE_COLUMNS)
        connection.execute(text(
            f"INSERT INTO sm_stock_minute ({columns}) SELECT {columns} FROM sm_stock_minute WHERE id=:id"
        ), {"id": kept.id})
        connection.execute(text(
            "UPDATE sm_stock_minute SET quality_status='verified' WHERE stock_code='600001'"
        ))
        connection.execute(text(
            "INSERT INTO sm_stock_minute(stock_code,trade_date,trade_time,data_source) "
            "VALUES ('600003',:day,:stamp,'qmt_datadir'),('600002',:day,:stamp,'OLD')"
        ), {"day": SESSION, "stamp": moment})
        connection.execute(text(
            "INSERT INTO qmt_datadir_minute_stage VALUES "
            "('run-1','600000',:stamp,:day,'11','1','10','100','1100','600000.SH')"
        ), {"day": SESSION, "stamp": next_minute})
        connection.execute(text("CREATE TABLE writes (operation, id, code, stamp, source)"))
        for event, alias in (("INSERT", "NEW"), ("DELETE", "OLD")):
            connection.execute(text(
                f"CREATE TRIGGER audit_{event.lower()} AFTER {event} ON sm_stock_minute "
                f"BEGIN INSERT INTO writes VALUES ('{event}',{alias}.id,{alias}.stock_code,"
                f"{alias}.trade_time,{alias}.data_source); END"
            ))
        publish_session(connection, batch_id="later", replace_sources=("old",), **params)
        writes = connection.execute(text(
            "SELECT operation,code,stamp,source FROM writes ORDER BY operation,code,stamp"
        )).all()
        assert writes == [
            ("DELETE", "600000", moment, "qmt_datadir"),  # Duplicate
            ("DELETE", "600001", moment, "qmt_datadir"),  # Wrong quality case
            ("DELETE", "600002", moment, "old"),         # Explicitly superseded
            ("DELETE", "600003", moment, "qmt_datadir"),  # No corresponding stage
            ("INSERT", "600000", next_minute, "qmt_datadir"),
            ("INSERT", "600001", moment, "qmt_datadir"),
        ]
        assert connection.execute(text(
            "SELECT * FROM sm_stock_minute WHERE stock_code='600000' AND trade_time=:stamp"
        ), {"stamp": moment}).one() == kept
        assert connection.execute(text(
            "SELECT quality_status FROM sm_stock_minute WHERE stock_code='600001'"
        )).scalar_one() == "VERIFIED"
        assert connection.execute(text(
            "SELECT data_source FROM sm_stock_minute WHERE stock_code='600002'"
        )).scalar_one() == "OLD"
    engine.dispose()


def test_later_stock_batch_failure_rolls_back_all_prior_batches():
    from integrations.qmt.datadir_import import publish_session
    from sqlalchemy import text
    engine = _publication_database()
    with engine.begin() as connection:
        connection.execute(text("CREATE TRIGGER fail_second_batch BEFORE INSERT ON sm_stock_minute WHEN NEW.stock_code='600001' BEGIN SELECT RAISE(ABORT,'source batch rejected'); END"))
    with pytest.raises(Exception, match="source batch rejected"):
        with engine.begin() as connection:
            publish_session(connection, session=SESSION, run_id="run-1", batch_id="batch-1", replace_sources=["old"], batch_codes=1)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM sm_stock_minute WHERE batch_id='batch-1'")).scalar() == 0
        assert connection.execute(text("SELECT COUNT(*) FROM sm_stock_minute WHERE trade_date=:day AND price='1'"), {"day": SESSION}).scalar() == 2
        assert connection.execute(text("SELECT COUNT(*) FROM qmt_datadir_minute_stage WHERE run_id='run-1'")).scalar() == 2
    engine.dispose()


def test_new_day_publication_uses_stage_stock_ranges_without_full_run_rescans():
    from integrations.qmt.datadir_import import publish_session
    from server.common.auxiliary_runtime_schema import QMT_DATADIR_STAGE_INDEX_NAME
    from sqlalchemy import event, text

    engine = _publication_database()
    with engine.begin() as connection:
        connection.execute(text("DELETE FROM sm_stock_minute WHERE trade_date=:day AND data_source='qmt_datadir'"), {"day": SESSION})
        connection.execute(text(f"CREATE INDEX {QMT_DATADIR_STAGE_INDEX_NAME} ON qmt_datadir_minute_stage(run_id,trade_date,stock_code,trade_time)"))
    statements = []

    @event.listens_for(engine, "before_cursor_execute")
    def capture(_conn, _cursor, statement, parameters, _context, _many):
        if statement.lstrip().startswith("INSERT INTO `sm_stock_minute`"):
            statements.append((statement, parameters))

    with engine.begin() as connection:
        publish_session(connection, session=SESSION, run_id="run-1", batch_id="new-date", replace_sources=(), batch_codes=1)
    assert len(statements) == 2
    with engine.connect() as connection:
        for statement, parameters in statements:
            plan = connection.exec_driver_sql("EXPLAIN QUERY PLAN " + statement, parameters).fetchall()
            descriptions = " ".join(str(row[-1]) for row in plan)
            assert f"USING INDEX {QMT_DATADIR_STAGE_INDEX_NAME}" in descriptions
            assert "run_id=? AND trade_date=? AND stock_code=?" in descriptions
            assert "SCAN qmt_datadir_minute_stage" not in descriptions
        assert connection.execute(text("SELECT COUNT(*) FROM sm_stock_minute WHERE batch_id='new-date'")).scalar() == 2
        assert connection.execute(text("SELECT price FROM sm_stock_minute WHERE trade_date='2026-10-30'")).scalar() == "3"
        assert connection.execute(text("SELECT price FROM sm_stock_minute WHERE data_source='old'")).scalar() == "2"
    engine.dispose()
