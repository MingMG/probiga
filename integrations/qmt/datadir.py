"""Read-only decoding of the Guojin QMT local bar cache (``datadir``).

QMT keeps a per-instrument binary file for every bar period under
``<datadir>/<MARKET>/<period>/<code>.DAT``.  The format is stable across the
trading client and miniQMT and does not need QMT's Python runtime, so it stays
readable after the 2026-10-23 restriction on the built-in interpreter.

File layout, verified against the production publisher row by row
(600000.SH and 000001.SZ, 2026-10-14, 241/241 minutes, zero difference):

* 8 byte header ``feffffff ffffff7f``
* N fixed 64 byte little-endian records, 16 ``uint32`` words each

  =====  ==============================================================
  word   meaning
  =====  ==============================================================
  0      bar start, Unix seconds in China standard time
  1..4   open, high, low, close, scaled by 1000
  6      minute bars: volume in lots (手); daily bars: volume in lots too
  8      turnover amount in CNY
  13     previous bar close (for the first bar of a session, pre-close),
         scaled by 1000
  =====  ==============================================================

Volume must be scaled at the call site: ``sm_stock_minute.volume`` is stored in
lots, ``sm_stock_kline.volume`` in shares.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Sequence

import numpy as np

CST = dt.timezone(dt.timedelta(hours=8), name="Asia/Shanghai")

DAT_HEADER = b"\xfe\xff\xff\xff\xff\xff\xff\x7f"
DAT_RECORD_BYTES = 64
DAT_WORD_COUNT = 16

WORD_TIME = 0
WORD_OPEN = 1
WORD_HIGH = 2
WORD_LOW = 3
WORD_CLOSE = 4
WORD_VOLUME = 6
WORD_AMOUNT = 8
WORD_PREV_CLOSE = 13

PRICE_SCALE = 1000
PRICE_SCALE_DIGITS = 3
LOT_TO_SHARES = 100

MINUTE_PERIOD = "60"
DAILY_PERIOD = "86400"

MARKETS = ("SH", "SZ", "BJ")


class DatadirError(RuntimeError):
    """The local QMT bar cache is missing or does not match the known format."""


@dataclass(frozen=True)
class DatFile:
    """One decoded ``.DAT`` file."""

    market: str
    code: str
    period: str
    path: Path
    records: np.ndarray

    @property
    def qmt_code(self) -> str:
        return f"{self.code}.{self.market}"

    def local_times(self) -> np.ndarray:
        """Return bar-start timestamps as China-local epoch seconds."""
        # Fixed +08:00; China has no daylight saving since 1991.
        return self.records[:, WORD_TIME].astype("int64") + 8 * 3600

    def session_dates(self) -> np.ndarray:
        """Return the sorted ``datetime.date`` values present in the file."""
        days = np.unique(self.local_times() // 86400)
        return np.array([dt.date(1970, 1, 1) + dt.timedelta(days=int(d)) for d in days])


def dat_path(root: Path | str, market: str, period: str, code: str) -> Path:
    """Return the canonical file for ``code``.

    QMT also writes ``<code>_<id>.DAT`` duplicates when two instruments share a
    numeric code.  Every such duplicate has a plain counterpart, so the plain
    name is the only one this loader reads.
    """
    return Path(root) / market / period / f"{code}.DAT"


def read_dat(path: Path | str) -> np.ndarray:
    """Decode one ``.DAT`` file into an ``(N, 16)`` ``uint32`` array."""
    target = Path(path)
    try:
        raw = target.read_bytes()
    except OSError as exc:
        raise DatadirError(f"cannot read {target}: {exc}") from exc
    if len(raw) < len(DAT_HEADER) or raw[:8] != DAT_HEADER:
        raise DatadirError(f"{target} does not start with the QMT bar header")
    body = len(raw) - len(DAT_HEADER)
    if body % DAT_RECORD_BYTES:
        raise DatadirError(
            f"{target} has {body} payload bytes, not a multiple of {DAT_RECORD_BYTES}"
        )
    return np.frombuffer(raw, dtype="<u4", offset=len(DAT_HEADER)).reshape(
        -1, DAT_WORD_COUNT
    )


def load_dat(root: Path | str, market: str, period: str, code: str) -> DatFile | None:
    """Load ``code`` for ``market``; return ``None`` when QMT never cached it."""
    path = dat_path(root, market, period, code)
    if not path.is_file():
        return None
    return DatFile(market, code, period, path, read_dat(path))


def _day_number(session: dt.date) -> int:
    """Return the China-local Unix day index of ``session``."""
    return session.toordinal() - dt.date(1970, 1, 1).toordinal()


def session_slice(records: np.ndarray, session: dt.date) -> np.ndarray:
    """Return the records whose bar start falls on ``session`` (China time)."""
    local = records[:, WORD_TIME].astype("int64") + 8 * 3600
    return records[local // 86400 == _day_number(session)]


def _price(word: int) -> Decimal:
    return Decimal(int(word)).scaleb(-PRICE_SCALE_DIGITS)


def _change(close: int, previous: int) -> tuple[Decimal, Decimal]:
    current = _price(close)
    base = _price(previous)
    if base == 0:
        return Decimal(0), Decimal(0)
    delta = current - base
    return delta, delta / base * 100


def select_records(dat: DatFile, sessions: Sequence[dt.date]) -> np.ndarray:
    """Return the records belonging to any of ``sessions`` in one pass.

    One decode per file: a 167 session cache is ~40k records and this runs for
    every instrument, so a per-session rescan would dominate the import.
    """
    if not sessions:
        return dat.records[:0]
    wanted = np.fromiter({_day_number(session) for session in sessions}, dtype="int64")
    local = dat.records[:, WORD_TIME].astype("int64") + 8 * 3600
    return dat.records[np.isin(local // 86400, wanted)]


def iter_minute_rows(
    dat: DatFile,
    selected: np.ndarray,
    grid: frozenset[str],
) -> Iterator[tuple[dt.date, tuple]]:
    """Yield ``(session, row)`` for minute bars on the canonical grid.

    ``row`` is ``(stock_code, trade_time, trade_date, price, change,
    change_pct, volume, amount, qmt_code)`` with volume in lots.  ``trade_time``
    doubles as the source time: QMT labels a bar by its start.
    Bars outside ``grid`` are dropped: QMT has shipped both 240 and 241 bar
    layouts, and only the native 241-bar clock is canonical here.
    """
    for record in selected:
        moment = dt.datetime.fromtimestamp(int(record[WORD_TIME]), CST)
        if moment.strftime("%H:%M:%S") not in grid:
            continue
        session = moment.date()
        close = int(record[WORD_CLOSE])
        change, change_pct = _change(close, int(record[WORD_PREV_CLOSE]))
        yield session, (
            dat.code,
            moment.replace(tzinfo=None),
            session,
            _price(close),
            change,
            change_pct,
            Decimal(int(record[WORD_VOLUME])),
            Decimal(int(record[WORD_AMOUNT])),
            dat.qmt_code,
        )


def iter_daily_rows(
    dat: DatFile,
    sessions: Sequence[dt.date],
) -> Iterator[tuple[dt.date, tuple]]:
    """Yield ``(session, row)`` for daily bars.

    ``row`` is ``(stock_code, trade_time, trade_date, open, close, high, low,
    volume, amount, pre_close, change, change_pct)`` with volume in shares.
    """
    wanted = set(sessions)
    for record in dat.records:
        moment = dt.datetime.fromtimestamp(int(record[WORD_TIME]), CST)
        session = moment.date()
        if session not in wanted:
            continue
        change, change_pct = _change(int(record[WORD_CLOSE]), int(record[WORD_PREV_CLOSE]))
        yield session, (
            dat.code,
            dt.datetime.combine(session, dt.time()),
            session,
            _price(record[WORD_OPEN]),
            _price(record[WORD_CLOSE]),
            _price(record[WORD_HIGH]),
            _price(record[WORD_LOW]),
            Decimal(int(record[WORD_VOLUME]) * LOT_TO_SHARES),
            Decimal(int(record[WORD_AMOUNT])),
            _price(record[WORD_PREV_CLOSE]),
            change,
            change_pct,
        )
