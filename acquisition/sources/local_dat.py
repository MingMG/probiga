"""Read Guojin QMT's local market-data files without the QMT runtime.

The QMT client keeps native bars under ``<datadir>/<market>/<period>/<code>.DAT``::

    SH/60/<code>.DAT        one-minute bars
    SH/86400/<code>.DAT     daily bars

Each file is an 8-byte header (``feffffffffffff7f``) followed by fixed 64-byte
little-endian records. The layout below was verified against the production
database for ``SZ.000001`` and ``SH.600519`` on 2026-07-01/2026-07-13:

    offset  width  meaning
    0       u32    UTC epoch seconds (minute: bar-start; daily: local midnight)
    4       u32    open  x 1000
    8       u32    high  x 1000
    12      u32    low   x 1000
    16      u32    close x 1000
    24      u32    volume in lots (手)
    32      u64    amount in yuan
    52      u32    previous bar close x 1000

Volume units differ per product and are part of the database contract:
``sm_stock_minute.volume`` stores lots, ``sm_stock_kline.volume`` stores shares,
so :func:`read_bars` exposes the raw ``volume_lots`` and the product-scaled
``volume``. Nothing here writes to a database; :func:`bars_to_rows` only maps a
frame to the columns the existing direct writer accepts.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterator, Literal
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

HEADER = b"\xfe\xff\xff\xff\xff\xff\xff\x7f"
RECORD_SIZE = 64
SHANGHAI = ZoneInfo("Asia/Shanghai")
MARKETS = ("SH", "SZ", "BJ")
TIMEFRAME_DIR = {"1m": "60", "1d": "86400"}
VOLUME_FACTOR = {"1m": 1, "1d": 100}

# Offsets, not a packed format: bytes 20-23, 28-31 and 40-51 are vendor fields
# that are not part of the bar (20 is always zero; 28-31 is a constant).
RECORD_DTYPE = np.dtype({
    "names": ["ts", "open", "high", "low", "close", "volume", "amount", "pre_close"],
    "formats": ["<u4", "<u4", "<u4", "<u4", "<u4", "<u4", "<u8", "<u4"],
    "offsets": [0, 4, 8, 12, 16, 24, 32, 52],
    "itemsize": RECORD_SIZE,
})

Timeframe = Literal["1m", "1d"]


class DatFormatError(ValueError):
    """The file is not a native QMT bar file with the verified layout."""


@dataclass(frozen=True)
class FileCensus:
    market: str
    code: str
    timeframe: str
    path: str
    records: int
    sessions: int
    full_sessions: int
    partial_sessions: int
    first_date: str
    last_date: str


@dataclass(frozen=True)
class SessionCoverage:
    session: str
    bars: int
    status: str
    missing: tuple[str, ...]
    extra: tuple[str, ...]


def _check_timeframe(timeframe: str) -> None:
    if timeframe not in TIMEFRAME_DIR:
        raise ValueError(f"unsupported timeframe: {timeframe!r}")


def read_records(path: str | Path) -> np.ndarray:
    """Return the raw fixed-width records, rejecting anything not in the format."""
    raw = Path(path).read_bytes()
    if len(raw) < len(HEADER) or raw[:len(HEADER)] != HEADER:
        raise DatFormatError(f"{path}: not a native QMT bar file")
    payload = len(raw) - len(HEADER)
    if payload % RECORD_SIZE:
        raise DatFormatError(f"{path}: payload is not a multiple of {RECORD_SIZE} bytes")
    return np.frombuffer(raw, dtype=RECORD_DTYPE, count=payload // RECORD_SIZE,
                         offset=len(HEADER))


def _local_times(records: np.ndarray) -> pd.DatetimeIndex:
    times = pd.to_datetime(records["ts"].astype("int64"), unit="s", utc=True)
    return times.tz_convert(SHANGHAI).tz_localize(None)


def read_bars(path: str | Path, timeframe: Timeframe) -> pd.DataFrame:
    """Parse one file into a frame ordered as stored.

    Columns: ``trade_time`` (naive Asia/Shanghai), ``trade_date`` (``date``),
    ``open``/``high``/``low``/``close``/``pre_close`` (yuan), ``volume_lots``
    (raw 手), ``volume`` (canonical for the product) and ``amount`` (yuan).
    """
    _check_timeframe(timeframe)
    records = read_records(path)
    times = _local_times(records)
    if timeframe == "1d":
        # The daily source timestamp is local midnight; the database contract
        # for a daily bar's trade_time is the session close.
        times = times.normalize() + pd.Timedelta(hours=15)
    frame = pd.DataFrame({
        "trade_time": times,
        "trade_date": pd.Series(times.date, dtype="object"),
        "open": records["open"].astype("float64") / 1000.0,
        "high": records["high"].astype("float64") / 1000.0,
        "low": records["low"].astype("float64") / 1000.0,
        "close": records["close"].astype("float64") / 1000.0,
        "pre_close": records["pre_close"].astype("float64") / 1000.0,
        "volume_lots": records["volume"].astype("int64"),
        "volume": records["volume"].astype("int64") * VOLUME_FACTOR[timeframe],
        "amount": records["amount"].astype("int64"),
    })
    return frame


def iter_bar_files(datadir: str | Path, timeframe: Timeframe) -> Iterator[tuple[str, str, Path]]:
    """Yield ``(market, code, path)`` for every bar file in a QMT datadir."""
    _check_timeframe(timeframe)
    root = Path(datadir)
    for market in MARKETS:
        folder = root / market / TIMEFRAME_DIR[timeframe]
        if not folder.is_dir():
            continue
        for path in sorted(folder.iterdir()):
            if path.suffix.lower() == ".dat" and path.is_file():
                yield market, path.stem, path


def core_grid() -> tuple[str, ...]:
    """The canonical 241-bar A-share clock, owned by the server contract."""
    from server.common.qmt_index_minute_grid import CORE_GRID
    return tuple(CORE_GRID)


def session_coverage(frame: pd.DataFrame) -> list[SessionCoverage]:
    """Classify each session in a minute frame against the canonical grid."""
    if frame.empty:
        return []
    required = set(core_grid())
    result = []
    for session, group in frame.groupby("trade_date", sort=True):
        observed = {value.strftime("%H:%M:%S") for value in group["trade_time"]}
        if len(observed) != len(group):
            status = "duplicate"
        elif observed == required:
            status = "full"
        elif required <= observed:
            status = "extended"
        elif observed:
            status = "partial"
        else:
            status = "empty"
        result.append(SessionCoverage(
            session=str(session), bars=len(group), status=status,
            missing=tuple(sorted(required - observed)),
            extra=tuple(sorted(observed - required)),
        ))
    return result


def census_file(path: str | Path, timeframe: Timeframe) -> tuple[int, Counter, str, str]:
    """Return ``(records, bars_per_session, first_date, last_date)`` cheaply."""
    _check_timeframe(timeframe)
    records = read_records(path)
    if not len(records):
        return 0, Counter(), "", ""
    dates = _local_times(records).date
    counts = Counter(str(value) for value in dates)
    return len(records), counts, min(counts), max(counts)


def census(datadir: str | Path, timeframe: Timeframe) -> list[FileCensus]:
    """Inventory every bar file, including how many sessions are complete."""
    _check_timeframe(timeframe)
    required = len(core_grid()) if timeframe == "1m" else 1
    result = []
    for market, code, path in iter_bar_files(datadir, timeframe):
        records, counts, first_date, last_date = census_file(path, timeframe)
        full = sum(1 for value in counts.values() if value == required)
        result.append(FileCensus(
            market=market, code=code, timeframe=timeframe, path=str(path),
            records=records, sessions=len(counts), full_sessions=full,
            partial_sessions=len(counts) - full,
            first_date=first_date, last_date=last_date,
        ))
    return result


def _is_placeholder(frame: pd.DataFrame) -> pd.Series:
    prices = frame[["open", "high", "low", "close"]]
    return (prices.eq(0).all(axis=1) & frame["volume_lots"].eq(0)
            & frame["amount"].eq(0))


def bars_to_rows(frame: pd.DataFrame, timeframe: Timeframe, code: str,
                 *, drop_placeholders: bool = True) -> list[dict]:
    """Map a frame to the existing direct-writer row shape for one security.

    Placeholder records (all prices, volume and amount zero) are native padding,
    not observations. Duplicate timestamps are rejected rather than silently
    overwriting one observation with another.
    """
    _check_timeframe(timeframe)
    stock_code = str(code).split(".")[0]
    data = frame
    if drop_placeholders:
        data = data[~_is_placeholder(data)]
    if data["trade_time"].duplicated().any():
        raise DatFormatError(f"{code}: duplicate bar timestamps")
    rows = []
    for row in data.itertuples(index=False):
        pre_close = float(row.pre_close)
        change = float(row.close) - pre_close if pre_close > 0 else None
        change_pct = change / pre_close * 100.0 if change is not None else None
        trade_time = row.trade_time.to_pydatetime()
        common = {
            "stock_code": stock_code,
            "trade_time": trade_time,
            "trade_date": row.trade_date,
            "volume": int(row.volume),
            "amount": int(row.amount),
            "change": change,
            "change_pct": change_pct,
        }
        if timeframe == "1m":
            lots = int(row.volume_lots)
            common.update(
                price=float(row.close),
                avg_price=(int(row.amount) / (lots * 100.0)) if lots > 0 else None,
            )
        else:
            common.update(
                k_type=1,
                adjust_type=0,  # native QMT bars are unadjusted
                open=float(row.open), high=float(row.high),
                low=float(row.low), close=float(row.close),
                pre_close=pre_close if pre_close > 0 else None,
            )
        rows.append(common)
    return rows


def _write_json(value, stream) -> None:
    json.dump(value, stream, ensure_ascii=False, indent=2, default=str)
    stream.write("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read native QMT .DAT bar files")
    sub = parser.add_subparsers(dest="command", required=True)
    inventory = sub.add_parser("census", help="inventory every bar file in a datadir")
    inventory.add_argument("--datadir", required=True)
    inventory.add_argument("--timeframe", choices=sorted(TIMEFRAME_DIR), default="1m")
    inventory.add_argument("--out")
    show = sub.add_parser("show", help="print the first parsed bars of one file")
    show.add_argument("--path", required=True)
    show.add_argument("--timeframe", choices=sorted(TIMEFRAME_DIR), default="1m")
    show.add_argument("--limit", type=int, default=5)
    args = parser.parse_args(argv)

    if args.command == "census":
        rows = census(args.datadir, args.timeframe)
        payload = {
            "datadir": args.datadir, "timeframe": args.timeframe,
            "files": len(rows),
            "records": sum(item.records for item in rows),
            "sessions": sum(item.sessions for item in rows),
            "full_session_files": sum(1 for item in rows if item.partial_sessions == 0),
            "partial_session_files": sum(1 for item in rows if item.partial_sessions),
            "first_date": min((item.first_date for item in rows if item.first_date), default=""),
            "last_date": max((item.last_date for item in rows if item.last_date), default=""),
            "members": [asdict(item) for item in rows],
        }
        if args.out:
            Path(args.out).write_text(json.dumps(payload, ensure_ascii=False, indent=2,
                                                 default=str), encoding="utf-8")
            print(f"wrote {args.out}")
        else:
            _write_json(payload, sys.stdout)
        return 0

    frame = read_bars(args.path, args.timeframe)
    preview = frame.head(max(0, args.limit)).copy()
    preview["trade_time"] = preview["trade_time"].astype(str)
    preview["trade_date"] = preview["trade_date"].astype(str)
    _write_json(preview.to_dict(orient="records"), sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
