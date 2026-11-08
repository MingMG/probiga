"""Linux-owned exact-session board evidence shared by the API and collectors."""
from __future__ import annotations

from contextlib import closing
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
from zoneinfo import ZoneInfo

from server.common.qmt_attestation_contract import canonical_digest
from server.common.trading_day_store import journal_runtime_root


_TABLE = "sector_observation_v1"
_FILE_NAME = "sector-heatmap-v1.sqlite3"
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_ROWS = 1_500
_MAX_PAYLOAD_BYTES = 1_000_000
_FLOW_FIELDS = ("main_net_inflow", "max_net_inflow", "lg_net_inflow", "mid_net_inflow", "sm_net_inflow")


def _path(*, create: bool = False) -> Path:
    root = journal_runtime_root()
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise OSError("sector observation runtime root unavailable")
    if os.name != "nt":
        info = root.stat()
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise OSError("sector observation runtime root is not private")
    path = root / _FILE_NAME
    if create:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.close(fd)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise OSError("sector observation database must be a regular single-link file")
    if os.name != "nt" and (info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
        raise OSError("sector observation database is not private")
    return path


def _clock(value) -> datetime:
    stamp = datetime.fromisoformat(str(value))
    return stamp.astimezone(_SHANGHAI).replace(tzinfo=None) if stamp.tzinfo else stamp


def _normalize(kind: str, selected: str, rows: list[dict]) -> list[dict]:
    day = date.fromisoformat(selected)
    if day.isoformat() != selected or not 0 < len(rows) <= _MAX_ROWS:
        raise ValueError("sector observation date or row count differs")
    normalized = {}
    for row in rows:
        code, name = row["index_code"], row["index_name"]
        if not isinstance(code, str) or not isinstance(name, str):
            raise ValueError("sector observation name or code is invalid")
        name = name.strip()
        source, observed = _clock(row["source_snapshot_at"]), _clock(row["etl_sync_at"])
        if (str(row.get("trade_date", selected)) != selected or not re.fullmatch(r"BK[0-9]{4,8}", code)
                or code in normalized or not 0 < len(name) <= 128 or not name.isprintable()
                or source.date() != day or source.time() < time(15) or observed < source):
            raise ValueError("sector observation identity, date or source time differs")
        item = {"index_code": code, "index_name": name, "source_snapshot_at": source.isoformat(),
                "etl_sync_at": observed.isoformat()}
        if kind == "industry_flow":
            try:
                amounts = {field: Decimal(str(row[field])) for field in _FLOW_FIELDS}
            except InvalidOperation as exc:
                raise ValueError("invalid industry flow amount") from exc
            if (any(not amount.is_finite() or abs(amount) >= 10**18 for amount in amounts.values())
                    or abs(amounts["main_net_inflow"] - amounts["max_net_inflow"] - amounts["lg_net_inflow"]) > Decimal("0.03")):
                raise ValueError("industry flow components differ")
            item.update({field: format(amount, "f") for field, amount in amounts.items()})
        normalized[code] = item
    return [normalized[code] for code in sorted(normalized)]


def _content_hash(rows: list[dict]) -> str:
    return canonical_digest([{key: value for key, value in row.items() if key != "etl_sync_at"} for row in rows])


def _publish(kind: str, selected: str, rows: list[dict]) -> dict:
    rows = _normalize(kind, selected, rows)
    row_hash = _content_hash(rows)
    stamp = max(row["source_snapshot_at"] for row in rows)
    observed = max(row["etl_sync_at"] for row in rows)
    payload = json.dumps(rows, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    if len(payload.encode("utf-8")) > _MAX_PAYLOAD_BYTES:
        raise ValueError("sector observation payload exceeds limit")
    with closing(sqlite3.connect(_path(create=True), timeout=2)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(f"""
            CREATE TABLE IF NOT EXISTS {_TABLE} (
                kind TEXT NOT NULL, trade_date TEXT NOT NULL, source_at TEXT NOT NULL,
                observed_at TEXT NOT NULL, row_hash TEXT NOT NULL, payload TEXT NOT NULL,
                PRIMARY KEY (kind, trade_date)
            )
        """)
        count = 0
        if _content_hash(_load(connection, kind, selected, None)) != row_hash:
            connection.execute(f"""
                INSERT INTO {_TABLE} VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(kind, trade_date) DO UPDATE SET source_at=excluded.source_at,
                    observed_at=excluded.observed_at, row_hash=excluded.row_hash, payload=excluded.payload
                WHERE excluded.source_at > source_at
                    OR (excluded.source_at = source_at AND excluded.observed_at >= observed_at)
            """, (kind, selected, stamp, observed, row_hash, payload))
            count = connection.execute("SELECT changes()").fetchone()[0]
            if _load(connection, kind, selected, None) != rows:
                raise RuntimeError("sector observation transaction readback differs")
    return {"row_count": len(rows), "code_count": len(rows), "database_writes": count,
            "row_sha256": row_hash, "code_set_sha256": canonical_digest([row["index_code"] for row in rows])}


def _load(connection, kind: str, selected: str, now: datetime | None) -> list[dict]:
    stored = connection.execute(f"""
        SELECT row_hash, payload FROM {_TABLE} WHERE kind=? AND trade_date=?
          AND length(CAST(payload AS BLOB))<=?
    """, (kind, selected, _MAX_PAYLOAD_BYTES)).fetchone()
    if stored is None:
        return []
    digest, payload = stored
    rows = json.loads(payload)
    if rows != _normalize(kind, selected, rows) or digest != _content_hash(rows):
        raise ValueError("sector observation persisted digest differs")
    if now is not None and any(_clock(row["etl_sync_at"]) > now for row in rows):
        return []
    return rows


def _read(kind: str, selected: str, now: datetime) -> list[dict]:
    try:
        path = _path()
    except FileNotFoundError:
        return []
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2)) as connection:
        return _load(connection, kind, selected, now)


def publish_industry_flow(trade_date: str, rows: list[dict]) -> dict:
    return _publish("industry_flow", trade_date, rows)


def publish_concept_names(trade_date: str, rows: list[dict]) -> dict:
    return _publish("concept_names", trade_date, rows)


def read_industry_flow(trade_date: str, *, now: datetime) -> list[dict]:
    return _read("industry_flow", trade_date, now)


def read_concept_names(trade_date: str, *, now: datetime) -> list[dict]:
    return _read("concept_names", trade_date, now)
