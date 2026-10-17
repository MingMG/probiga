"""Contracts for the native QMT .DAT reader (no QMT process required)."""
import struct
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from acquisition.sources.local_dat import (
    HEADER, DatFormatError, bars_to_rows, census, core_grid, iter_bar_files,
    read_bars, read_records, session_coverage,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _ts(year, month, day, hour, minute=0):
    return int(pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute,
                            tz=SHANGHAI).timestamp())


def _record(ts, open_, high, low, close, volume_lots, amount, pre_close,
            *, reserved_zero=True):
    """Build one 64-byte native record with the verified vendor field layout."""
    body = struct.pack("<5I", ts, open_, high, low, close)
    body += struct.pack("<I", 0)                 # offset 20, always zero
    body += struct.pack("<I", volume_lots)       # offset 24
    body += struct.pack("<I", 678)               # offset 28, constant
    body += struct.pack("<Q", amount)            # offset 32, u64 yuan
    body += struct.pack("<I", 13)                # offset 40, constant
    body += struct.pack("<I", 0x3F800000)        # offset 44, float 1.0
    body += struct.pack("<I", 0x3F800000)        # offset 48, float 1.0
    body += struct.pack("<I", pre_close)         # offset 52
    body += struct.pack("<II", 0, 32761)         # offsets 56/60, constants
    assert len(body) == 64
    return body


def _write(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(HEADER + b"".join(records))
    return path


def test_minute_bar_matches_the_verified_database_row(tmp_path):
    path = _write(tmp_path / "SZ" / "60" / "000001.DAT", [
        _record(_ts(2026, 7, 1, 9, 30), 10050, 10050, 10050, 10050,
                7036, 7_071_180, 10050),
    ])
    frame = read_bars(path, "1m")
    assert list(frame.columns) == [
        "trade_time", "trade_date", "open", "high", "low", "close",
        "pre_close", "volume_lots", "volume", "amount",
    ]
    assert frame.loc[0, "trade_time"] == pd.Timestamp("2026-07-01 09:30:00")
    assert frame.loc[0, "trade_date"] == date(2026, 7, 1)
    assert frame.loc[0, "close"] == pytest.approx(10.05)
    assert frame.loc[0, "volume_lots"] == 7036
    assert frame.loc[0, "volume"] == 7036          # minute volume stays in 手
    assert frame.loc[0, "amount"] == 7_071_180

    rows = bars_to_rows(frame, "1m", "000001.SZ")
    assert rows == [{
        "stock_code": "000001", "trade_time": pd.Timestamp("2026-07-01 09:30:00").to_pydatetime(),
        "trade_date": date(2026, 7, 1), "volume": 7036, "amount": 7_071_180,
        "change": 0.0, "change_pct": 0.0, "price": 10.05, "avg_price": 10.05,
    }]


def test_daily_bar_uses_the_whole_uint64_amount_and_scales_volume(tmp_path):
    amount = (1 << 32) + 761_722_262           # high word 1, low word 761722262
    path = _write(tmp_path / "SH" / "86400" / "600519.DAT", [
        _record(_ts(2026, 7, 13, 0), 1197_120, 1200_000, 1190_000, 1198_500,
                41_983, amount, 1197_120),
    ])
    frame = read_bars(path, "1d")
    assert frame.loc[0, "amount"] == 5_056_689_558
    assert frame.loc[0, "volume_lots"] == 41_983
    assert frame.loc[0, "volume"] == 4_198_300   # daily volume is shares
    assert frame.loc[0, "trade_time"] == pd.Timestamp("2026-07-13 15:00:00")

    row = bars_to_rows(frame, "1d", "600519.SH")[0]
    assert row["k_type"] == 1
    assert row["open"] == pytest.approx(1197.12)
    assert row["close"] == pytest.approx(1198.5)
    assert row["volume"] == 4_198_300
    assert row["amount"] == 5_056_689_558
    assert row["pre_close"] == pytest.approx(1197.12)
    assert row["change"] == pytest.approx(1.38)
    assert row["change_pct"] == pytest.approx(1.38 / 1197.12 * 100)
    assert "price" not in row and "avg_price" not in row


def test_rejects_files_that_do_not_match_the_native_format(tmp_path):
    bad_header = tmp_path / "bad.DAT"
    bad_header.write_bytes(b"\x00" * 72)
    with pytest.raises(DatFormatError):
        read_records(bad_header)

    truncated = tmp_path / "truncated.DAT"
    truncated.write_bytes(HEADER + b"\x00" * 63)
    with pytest.raises(DatFormatError):
        read_records(truncated)


def test_placeholder_records_are_padding_and_duplicates_are_rejected(tmp_path):
    times = [_ts(2026, 7, 1, 9, 30)]
    path = _write(tmp_path / "SZ" / "60" / "000001.DAT", [
        _record(times[0], 10050, 10050, 10050, 10050, 7036, 7_071_180, 10050),
        _record(_ts(2026, 7, 1, 9, 31), 0, 0, 0, 0, 0, 0, 0),
    ])
    rows = bars_to_rows(read_bars(path, "1m"), "1m", "000001")
    assert len(rows) == 1

    duplicate = _write(tmp_path / "SZ" / "60" / "000002.DAT", [
        _record(times[0], 10050, 10050, 10050, 10050, 7036, 7_071_180, 10050),
        _record(times[0], 10060, 10060, 10060, 10060, 1, 1_000_000, 10050),
    ])
    with pytest.raises(DatFormatError):
        bars_to_rows(read_bars(duplicate, "1m"), "1m", "000002")


def test_missing_previous_close_leaves_change_unset(tmp_path):
    path = _write(tmp_path / "SZ" / "60" / "000001.DAT", [
        _record(_ts(2026, 7, 1, 9, 30), 10050, 10050, 10050, 10050,
                7036, 7_071_180, 0),
    ])
    row = bars_to_rows(read_bars(path, "1m"), "1m", "000001")[0]
    assert row["change"] is None and row["change_pct"] is None


def test_session_coverage_uses_the_canonical_241_bar_clock(tmp_path):
    grid = core_grid()
    assert len(grid) == 241 and grid[0] == "09:30:00" and grid[-1] == "15:00:00"
    records = []
    for value in grid:
        hour, minute, _ = (int(part) for part in value.split(":"))
        records.append(_record(_ts(2026, 7, 1, hour, minute),
                               10050, 10050, 10050, 10050, 1, 100_500, 10050))
    frame = read_bars(_write(tmp_path / "SZ" / "60" / "000001.DAT", records), "1m")
    coverage = session_coverage(frame)
    assert [item.status for item in coverage] == ["full"]
    assert coverage[0].bars == 241

    partial = frame[frame["trade_time"].dt.strftime("%H:%M:%S") != "11:30:00"]
    coverage = session_coverage(partial)
    assert coverage[0].status == "partial"
    assert coverage[0].missing == ("11:30:00",)


def test_census_and_file_iteration_walk_the_datadir(tmp_path):
    _write(tmp_path / "SH" / "60" / "600519.DAT", [
        _record(_ts(2026, 7, 1, 9, 30), 1, 1, 1, 1, 1, 1, 1),
    ])
    _write(tmp_path / "SZ" / "86400" / "000001.DAT", [
        _record(_ts(2026, 7, 1, 0), 1, 1, 1, 1, 1, 1, 1),
        _record(_ts(2026, 7, 2, 0), 1, 1, 1, 1, 1, 1, 1),
    ])
    assert [(m, c) for m, c, _ in iter_bar_files(tmp_path, "1m")] == [("SH", "600519")]
    minute = census(tmp_path, "1m")
    assert len(minute) == 1 and minute[0].sessions == 1 and minute[0].full_sessions == 0
    daily = census(tmp_path, "1d")
    assert len(daily) == 1 and daily[0].sessions == 2 and daily[0].full_sessions == 2
    assert (daily[0].first_date, daily[0].last_date) == ("2026-07-01", "2026-07-02")
