"""Durable dashboard observations. HTTP readers never acquire market data."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, time, timedelta
import json
import os
import sqlite3
from zoneinfo import ZoneInfo

from server.common.qmt_attestation_contract import canonical_digest
from server.common.sector_heatmap_store import _path

ZONE = ZoneInfo("Asia/Shanghai")
FRESH_SECONDS = 20
EXPIRED_SECONDS = 60
MAX_BYTES = 2_000_000


def clock(value=None):
    value = value or datetime.now(ZONE)
    return value.astimezone(ZONE).replace(tzinfo=None) if value.tzinfo else value


def stamp(value):
    return clock(datetime.fromisoformat(str(value)))


def iso(value):
    return clock(value).replace(tzinfo=ZONE).isoformat(timespec="seconds")


@contextmanager
def database(*, write=False, root=None):
    path = _path(create=write, root=root)
    db = sqlite3.connect(path if write else path.as_uri()+"?mode=ro", uri=not write, timeout=1)
    db.row_factory = sqlite3.Row
    try:
        if write:
            db.execute("PRAGMA synchronous=FULL")
        yield db
    finally:
        db.close()


def initialize():
    with database(write=True) as db, db:
        db.execute("""CREATE TABLE IF NOT EXISTS dashboard_observation (
            trade_date TEXT NOT NULL, component TEXT NOT NULL, version TEXT NOT NULL,
            source_at TEXT, captured_at TEXT NOT NULL, basis TEXT NOT NULL, payload TEXT NOT NULL,
            run_id TEXT,
            PRIMARY KEY(trade_date,component))""")
        db.execute("""CREATE TABLE IF NOT EXISTS dashboard_capture (
            trade_date TEXT NOT NULL, component TEXT NOT NULL, run_id TEXT NOT NULL,
            status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT,
            error TEXT, published INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(trade_date,component))""")
        db.execute("""CREATE TABLE IF NOT EXISTS dashboard_owner (
            slot INTEGER PRIMARY KEY CHECK(slot=1), token TEXT, pid INTEGER, process_identity TEXT,
            child_pid INTEGER, build_sha TEXT, renewed_at TEXT, lease_until TEXT,
            run_id TEXT, state TEXT, total_units INTEGER, completed_units INTEGER,
            last_progress_at TEXT, next_capture_at TEXT)""")


def runtime(*, root=None):
    try:
        with database(root=root) as db:
            row = db.execute("SELECT * FROM dashboard_owner WHERE slot=1").fetchone()
            if not row:
                return {}
            result = dict(row)
            result["current_units"] = [dict(item) for item in db.execute(
                "SELECT component,status,published,error,started_at,finished_at FROM dashboard_capture WHERE run_id=? LIMIT 16",
                (row["run_id"],)).fetchall()]
            return result
    except (FileNotFoundError, sqlite3.OperationalError) as exc:
        if isinstance(exc, FileNotFoundError) or "no such table" in str(exc):
            return {}
        raise


def _authorized(db, token, now):
    row = db.execute("SELECT token,child_pid,lease_until FROM dashboard_owner WHERE slot=1").fetchone()
    if (not row or row["token"] != token or row["child_pid"] != os.getpid()
            or stamp(row["lease_until"]) < now):
        raise RuntimeError("dashboard producer lease lost")


def start_component(day, component, run_id, token):
    now = clock()
    with database(write=True) as db, db:
        _authorized(db, token, now)
        db.execute("""INSERT INTO dashboard_capture VALUES (?,?,?,'RUNNING',?,NULL,NULL,0)
            ON CONFLICT(trade_date,component) DO UPDATE SET run_id=excluded.run_id,
            status='RUNNING',started_at=excluded.started_at,finished_at=NULL,error=NULL,published=0""",
                   (day, component, run_id, iso(now)))


def finish_component(day, component, run_id, token, *, published, error=None):
    now = clock()
    with database(write=True) as db, db:
        _authorized(db, token, now)
        updated = db.execute("""UPDATE dashboard_capture SET status=?,finished_at=?,error=?,published=?
            WHERE trade_date=? AND component=? AND run_id=? AND status='RUNNING'""",
            ("FAILED" if error else "COMPLETE", iso(now), error, published, day, component, run_id)).rowcount
        if updated != 1:
            raise RuntimeError("dashboard capture identity changed")
        db.execute("""UPDATE dashboard_owner SET completed_units=completed_units+1,last_progress_at=?
            WHERE slot=1 AND token=?""", (iso(now), token))


def publish(day, component, payload, *, source_at, basis, token):
    now = clock()
    source = stamp(source_at) if source_at else None
    if basis not in ("source", "received", "close", "observation") or (basis != "received" and source is None):
        raise ValueError("dashboard source time basis missing")
    if source and (source.date().isoformat() != day or source > now):
        raise ValueError("dashboard source date or future timestamp differs")
    if any(payload.get(key) not in (None, day) for key in ("trade_date", "requested_date")):
        raise ValueError("dashboard payload session differs")
    observed = stamp(payload["captured_at"]) if basis == "observation" and payload.get("captured_at") else None
    if basis == "observation" and (observed is None or observed.date().isoformat() != day
                                    or observed > now or source > observed):
        raise ValueError("dashboard observation capture time differs")
    encoded = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > MAX_BYTES:
        raise ValueError("dashboard observation exceeds bound")
    version = canonical_digest(payload)
    with database(write=True) as db, db:
        _authorized(db, token, now)
        previous = db.execute("SELECT source_at,basis,payload FROM dashboard_observation WHERE trade_date=? AND component=?",
                              (day, component)).fetchone()
        if previous and basis == previous["basis"] == "observation" and stamp(json.loads(previous["payload"])["captured_at"]) > observed:
            raise ValueError("dashboard capture observation moved backwards")
        if previous and basis != "observation" and previous[0] and source and stamp(previous[0]) > source:
            raise ValueError("dashboard source observation moved backwards")
        run_id = db.execute("SELECT run_id FROM dashboard_owner WHERE slot=1").fetchone()[0]
        db.execute("""INSERT INTO dashboard_observation VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(trade_date,component) DO UPDATE SET version=excluded.version,
            source_at=excluded.source_at,captured_at=excluded.captured_at,basis=excluded.basis,payload=excluded.payload,run_id=excluded.run_id""",
                   (day, component, version, iso(source) if source else None, iso(now), basis, encoded, run_id))
        stored = db.execute("SELECT version,payload FROM dashboard_observation WHERE trade_date=? AND component=?",
                            (day, component)).fetchone()
        if tuple(stored) != (version, encoded):
            raise RuntimeError("dashboard durable observation readback differs")


def freshness(row, day, *, now, payload=None):
    now = clock(now)
    source = stamp(row["source_at"]) if row["source_at"] else None
    received = stamp(row["captured_at"])
    observed = None
    if row["basis"] == "observation":
        observed = stamp((payload if payload is not None else json.loads(row["payload"]))["captured_at"])
    age_from = observed or source or received
    anchor = now
    paused = day == now.date().isoformat() and time(11, 30) < now.time() < time(13)
    if paused:
        anchor = now.replace(hour=11, minute=30, second=0, microsecond=0)
    age = max(0, int((anchor-age_from).total_seconds()))
    if row["basis"] == "close" and (day < now.date().isoformat() or now.time() >= time(15)):
        state = "closed"
    elif paused and age <= EXPIRED_SECONDS:
        state = "paused"
    else:
        state = "expired" if age > EXPIRED_SECONDS else "delayed" if age > FRESH_SECONDS else "fresh"
    return {"version": row["version"], "capture_run_id": row["run_id"], "source_at": row["source_at"], "received_at": row["captured_at"],
            "observation_at": iso(observed) if observed else None,
            "age_seconds": age, "age_basis": "observation" if observed else "source" if source else "received", "state": state,
            "fresh_seconds": FRESH_SECONDS, "expired_seconds": EXPIRED_SECONDS}


def read(day, *, now=None, root=None):
    """One bounded, read-only SQLite read, independent of all source clients."""
    now = clock(now)
    try:
        with database(root=root) as db:
            rows = db.execute("""SELECT * FROM dashboard_observation WHERE trade_date=?
                AND length(CAST(payload AS BLOB))<=? LIMIT 32""", (day, MAX_BYTES)).fetchall()
            attempts = db.execute("SELECT * FROM dashboard_capture WHERE trade_date=? LIMIT 16", (day,)).fetchall()
    except (FileNotFoundError, sqlite3.OperationalError) as exc:
        if isinstance(exc, FileNotFoundError) or "no such table" in str(exc):
            return {}, {}
        raise
    result = {}
    attempts = {row["component"]: dict(row) for row in attempts}
    for row in rows:
        payload = json.loads(row["payload"])
        if canonical_digest(payload) != row["version"]:
            raise ValueError("dashboard persisted observation digest differs")
        meta = freshness(row, day, now=now, payload=payload)
        name = row["component"]
        owner = "indices" if name.startswith("index:") else "heatmap" if name.startswith("heat:") else name
        if owner not in attempts and owner in ("quotes", "indices", "turnover", "ladder"):
            owner = "board"
        attempt = attempts.get(owner)
        if attempt and attempt["status"] == "FAILED" and attempt["run_id"] != row["run_id"]:
            meta["last_error"] = attempt["error"]
        result[name] = (payload, meta)
    return result, attempts


def _errors(attempts):
    return {name: {"run_id": row["run_id"], "status": row["status"], "error": row["error"],
                   "last_attempt_at": row["finished_at"] or row["started_at"]}
            for name, row in attempts.items() if row["status"] == "FAILED"}


def board(day, *, now=None):
    parts, attempts = read(day, now=now)
    if not parts:
        return None
    quote, observation = parts.get("quotes", ({}, {}))
    result = {"status": "unavailable", "requested_date": day, "trade_date": day,
              "sample_size": 0, "price_sample_size": 0, "rankings": {}, "distribution": [],
              "strong": {}, "market_overview": None, **quote}
    result["rankings"] = dict(result["rankings"])
    result["observation"] = observation
    result["source_errors"] = _errors(attempts)
    result["indices"] = [{**payload, "observation": meta}
                         for key, (payload, meta) in sorted(parts.items()) if key.startswith("index:")]
    result["observations"] = {"quotes": observation}
    for name in ("turnover", "ladder"):
        if name not in parts:
            continue
        payload, meta = parts[name]
        result["observations"][name] = meta
        if name == "ladder":
            result["limit_ladder"] = {**{key: value for key, value in payload.items() if key != "_evidence"}, "observation": meta}
        else:
            result["rankings"]["turnover"] = payload["rows"]
            result["ranking_observations"] = {"turnover": {**payload["observation"], "observation": meta}}
            result["turnover_coverage"] = payload["coverage"]
            result["turnover_evidence"] = payload.get("evidence")
    result["events"] = parts.get("events", ({"events": []}, {}))[0]["events"][:20]
    missing = list(quote.get("unavailable_fields", []))
    if not quote or observation.get("state") == "expired":
        missing += ["distribution", "strong", "rankings.gainers", "rankings.losers", "rankings.amount"]
    if len(result["indices"]) != 7 or any(row["observation"]["state"] == "expired" for row in result["indices"]):
        missing.append("indices")
    for component, field in (("turnover", "rankings.turnover"), ("ladder", "limit_ladder")):
        if component not in parts or parts[component][1]["state"] == "expired":
            missing.append(field)
    ladder = result.get("limit_ladder") or {}
    for field in ("limit_up_count", "max_streak", "limit_down_count", "opened_limit_up_count"):
        if ladder and ladder.get(field) is None:
            missing.append("limit_ladder."+field)
    result["unavailable_fields"] = missing
    result["status"] = "stale" if quote and observation.get("state") == "expired" else "partial" if missing else "available"
    if result.get("market_overview"):
        result["market_overview"] = {**result["market_overview"], "observation": observation}
        if observation.get("state") == "expired":
            result["market_overview"].update(is_realtime=False, freshness_status="stale")
    result["version"] = canonical_digest({key: meta["version"] for key, (_, meta) in parts.items()})
    return result


def heatmap(day, *, now=None):
    parts, attempts = read(day, now=now)
    selected = {key: value for key, value in parts.items() if key.startswith("heat:")}
    if not selected:
        return None
    result = {"requested_date": day, "trade_date": day, "concept": {}, "industry": {}, "source_errors": _errors(attempts)}
    for kind in ("concept", "industry"):
        for mode in ("change", "net_inflow", "five_day_return"):
            payload, meta = selected.get(f"heat:{kind}:{mode}", ({"status": "unavailable", "rows": []}, {}))
            result[kind][mode] = {**payload, "observation": meta}
    result["version"] = canonical_digest({key: meta["version"] for key, (_, meta) in selected.items()})
    return result


def component(day, name, *, now=None, root=None):
    parts, attempts = read(day, now=now, root=root)
    if name not in parts:
        return None
    payload, meta = parts[name]
    return {**payload, "observation": meta, "version": meta["version"], "source_errors": _errors(attempts)}
