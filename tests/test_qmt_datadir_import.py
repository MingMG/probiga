"""Contract tests for the QMT datadir importer.

The expected minute values are the production publisher's rows for 600000.SH
on 2026-10-14, which the decoder was validated against row by row.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import numpy as np
import pytest

from integrations.qmt.datadir import (
    CST,
    DAT_HEADER,
    DAILY_PERIOD,
    MINUTE_PERIOD,
    DatadirError,
    iter_daily_rows,
    iter_minute_rows,
    load_dat,
    read_dat,
    select_records,
)
from integrations.qmt.datadir_import import (
    SOURCE,
    existing_sources,
    plan,
)

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


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict | None]] = []

    def execute(self, statement, params=None):
        self.calls.append((" ".join(str(statement).split()), params))
        return None


def test_publish_replaces_its_own_rows_before_dropping_the_old_source():
    from integrations.qmt.datadir_import import publish_session

    recorder = _Recorder()
    publish_session(
        recorder,
        session=SESSION,
        run_id="run-1",
        batch_id="batch-1",
        replace_sources=["gj_big_qmt_inner"],
    )

    assert len(recorder.calls) == 3
    first, second, third = recorder.calls
    assert first[0].startswith("DELETE FROM `sm_stock_minute`")
    assert "data_source=:s" in first[0]
    assert "INSERT INTO `sm_stock_minute`" in second[0]
    assert "FROM `qmt_datadir_minute_stage`" in second[0]
    assert "trade_time, NOW()" in second[0]  # source_time mirrors the bar start
    assert third[0].startswith("DELETE FROM `sm_stock_minute`")
    assert "data_source IN" in third[0]
    assert third[1] == {"d": SESSION, "sources": ["gj_big_qmt_inner"]}


def test_publish_never_touches_a_foreign_source_by_default():
    from integrations.qmt.datadir_import import publish_session

    recorder = _Recorder()
    publish_session(
        recorder, session=SESSION, run_id="run-1", batch_id="batch-1", replace_sources=()
    )
    assert len(recorder.calls) == 2
