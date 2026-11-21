"""One supervised Linux producer, independent of browser connections."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timedelta
import json
import logging
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import threading
from time import monotonic
import uuid

from sqlalchemy import text

from server.common import market_dashboard_store as store

LOGGER = logging.getLogger(__name__)
LEASE_SECONDS = 30
CYCLE_SECONDS = 20
CAPTURE_DEADLINE_SECONDS = 45
_thread = None
_stop = threading.Event()
_start_lock = threading.Lock()


def process_identity(pid):
    try:
        # A reused PID is not the old owner.
        stat = Path(f"/proc/{pid}/stat").read_text()
        return stat[stat.rfind(")")+2:].split()[19]
    except (OSError, IndexError):
        return None


def _parent(db, token):
    row = db.execute("SELECT token,pid FROM dashboard_owner WHERE slot=1").fetchone()
    if not row or row["token"] != token or row["pid"] != os.getpid():
        raise RuntimeError("dashboard supervisor ownership changed")


def claim(token):
    now = store.clock()
    with store.database(write=True) as db, db:
        previous = db.execute("SELECT * FROM dashboard_owner WHERE slot=1").fetchone()
        if previous and previous["token"]:
            if store.stamp(previous["lease_until"]) >= now:
                return False
            if process_identity(previous["pid"]) == previous["process_identity"]:
                raise RuntimeError("expired dashboard owner process is still present")
            if previous["child_pid"] and process_identity(previous["child_pid"]) is not None:
                raise RuntimeError("previous dashboard capture process is still present")
            db.execute("""UPDATE dashboard_capture SET status='FAILED',finished_at=?,error='producer_owner_lost'
                WHERE status='RUNNING'""", (store.iso(now),))
        db.execute("""INSERT OR REPLACE INTO dashboard_owner VALUES
            (1,?,?,?,?,?,?,?,NULL,'RUNNING',0,0,NULL,NULL)""",
            (token, os.getpid(), process_identity(os.getpid()), None,
             os.environ.get("PROBIGA_EXPECTED_GIT_SHA", ""), store.iso(now), store.iso(now+timedelta(seconds=LEASE_SECONDS))))
    return True


def renew(token, *, child_pid=None):
    now = store.clock()
    with store.database(write=True) as db, db:
        _parent(db, token)
        db.execute("UPDATE dashboard_owner SET renewed_at=?,lease_until=?,child_pid=? WHERE slot=1",
                   (store.iso(now), store.iso(now+timedelta(seconds=LEASE_SECONDS)), child_pid))


def _terminate(child):
    if child.poll() is not None:
        return
    if os.name == "posix":
        if os.getpgid(child.pid) != child.pid:
            raise RuntimeError("dashboard capture process group identity changed")
        os.killpg(child.pid, signal.SIGTERM)
    else:
        child.terminate()
    try:
        child.wait(timeout=3)
    except subprocess.TimeoutExpired:
        if os.name == "posix":
            os.killpg(child.pid, signal.SIGKILL)
        else:
            child.kill()
        child.wait(timeout=3)


def _terminal(token, reason=None):
    now = store.clock()
    with store.database(write=True) as db, db:
        _parent(db, token)
        state = db.execute("SELECT run_id FROM dashboard_owner WHERE slot=1").fetchone()
        db.execute("""UPDATE dashboard_capture SET status='FAILED',finished_at=?,error=?
            WHERE run_id=? AND status='RUNNING'""", (store.iso(now), reason or "capture_child_ended", state["run_id"]))
        failed = db.execute("SELECT COUNT(*) FROM dashboard_capture WHERE run_id=? AND status='FAILED'", (state["run_id"],)).fetchone()[0]
        db.execute("UPDATE dashboard_owner SET state=?,child_pid=NULL WHERE slot=1",
                   ("FAILED" if failed or reason else "COMPLETE",))


def _supervise():
    import fcntl
    token = uuid.uuid4().hex
    child = None
    fd = None
    try:
        store.initialize()
        lock_path = store._path().parent / "market-dashboard-owner.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while not _stop.is_set() and not claim(token):
            _stop.wait(2)
        if _stop.is_set():
            return
        while not _stop.is_set():
            renew(token)
            state = store.runtime()
            due = state.get("next_capture_at")
            if due and store.stamp(due) > store.clock():
                _stop.wait(min(5, (store.stamp(due)-store.clock()).total_seconds()))
                continue
            started = monotonic()
            child = subprocess.Popen([sys.executable, "-m", __name__, "--capture", token],
                cwd=str(Path(__file__).resolve().parents[2]), stdin=subprocess.DEVNULL,
                start_new_session=True)
            renew(token, child_pid=child.pid)
            timed_out = False
            while child.poll() is None and not _stop.wait(2):
                renew(token, child_pid=child.pid)
                if monotonic()-started >= CAPTURE_DEADLINE_SECONDS:
                    timed_out = True
                    break
            if child.poll() is None:
                _terminate(child)
            result = child.wait(timeout=3)
            _terminal(token, "capture_deadline_exceeded" if timed_out else "capture_child_failed" if result else None)
            child = None
            _stop.wait(max(1, CYCLE_SECONDS-(monotonic()-started)))
    except BlockingIOError:
        LOGGER.info("dashboard producer already owned by another API process")
    except Exception as exc:
        LOGGER.error("dashboard producer stopped: %s", type(exc).__name__)
    finally:
        if child is not None:
            _terminate(child)
        try:
            with store.database(write=True) as db, db:
                db.execute("""UPDATE dashboard_capture SET status='FAILED',finished_at=?,error='producer_supervisor_stopped'
                    WHERE status='RUNNING' AND run_id=(SELECT run_id FROM dashboard_owner WHERE token=? AND pid=?)""",
                    (store.iso(store.clock()), token, os.getpid()))
                db.execute("""UPDATE dashboard_owner SET token=NULL,child_pid=NULL,state=?,lease_until=?
                    WHERE slot=1 AND token=? AND pid=?""", ("NOT_STARTED" if _stop.is_set() else "FAILED", store.iso(store.clock()), token, os.getpid()))
        except (OSError, sqlite3.Error, RuntimeError):
            LOGGER.error("dashboard producer release could not be recorded")
        if fd is not None:
            os.close(fd)


def start_dashboard_producer():
    global _thread
    if os.name != "posix" or os.environ.get("PROBIGA_DEPLOYMENT_MODE") != "production":
        return
    with _start_lock:
        if _thread and _thread.is_alive():
            return
        _stop.clear()
        _thread = threading.Thread(target=_supervise, name="market-dashboard-producer", daemon=True)
        _thread.start()


def stop_dashboard_producer():
    _stop.set()
    if _thread:
        _thread.join(timeout=10)
        if _thread.is_alive():
            raise RuntimeError("dashboard producer shutdown deadline exceeded")


def _target():
    from server.api.routers._engine import get_engine
    now = store.clock()
    before_open = now.time() < time(9, 25)
    with get_engine().connect() as connection:
        dates = connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(1200) */ trade_date
            FROM si_trade_calendar WHERE trade_status=1 AND trade_date<=:today
            ORDER BY trade_date DESC LIMIT 2"""), {"today": now.date()}).scalars().all()
        if not dates:
            raise RuntimeError("dashboard trading calendar unavailable")
        day = str(dates[0])[:10]
        if before_open and day == now.date().isoformat():
            if len(dates) < 2:
                raise RuntimeError("dashboard previous session unavailable")
            day = str(dates[1])[:10]
        future = connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(1000) */ MIN(trade_date)
            FROM si_trade_calendar WHERE trade_status=1 AND trade_date>:day"""), {"day": day}).scalar()
    live = day == now.date().isoformat() and time(9, 25) <= now.time() < time(15)
    next_open = datetime.combine(store.stamp(str(future)+"T00:00:00").date(), time(9, 25)) if future else now+timedelta(minutes=1)
    return day, now, live, next_open


def _publish_board(day, value, token, *, closed):
    count = 0
    if value.get("market_overview") and value.get("sample_size", 0) > 0:
        fields = ("as_of", "source", "as_of_basis", "sample_size", "price_sample_size", "sample_scope",
                  "sample_sources", "market_overview", "distribution", "strong", "close_drawdowns", "liquidity", "quote_identity")
        payload = {key: value.get(key) for key in fields}
        payload["unavailable_fields"] = [field for field in value.get("unavailable_fields", [])
            if field in ("distribution", "strong", "rankings.gainers", "rankings.losers", "rankings.amount", "liquidity.volume_ratio5")]
        payload["rankings"] = {key: value["rankings"][key] for key in ("gainers", "losers", "amount")}
        store.publish(day, "quotes", payload, source_at=value["as_of"], basis="close" if closed else "source", token=token)
        count += 1
    for row in value.get("indices", []):
        store.publish(day, "index:"+row["index_code"], row, source_at=row.get("as_of") or value.get("as_of"),
                      basis="close" if closed else "source", token=token)
        count += 1
    turnover = value.get("rankings", {}).get("turnover", [])
    if turnover:
        observation = value.get("ranking_observations", {}).get("turnover") or {
            "trade_date": day, "as_of": value["as_of"], "source": value.get("source"), "scope": "已核验换手样本"}
        store.publish(day, "turnover", {"rows": turnover, "observation": observation,
            "coverage": value["turnover_coverage"], "evidence": value.get("turnover_evidence")},
            source_at=observation["as_of"], basis="close" if closed else "source", token=token)
        count += 1
    if value.get("limit_ladder"):
        ladder = value["limit_ladder"]
        if not ladder.get("source_errors") or "ladder" not in store.read(day)[0]:
            store.publish(day, "ladder", ladder, source_at=day+"T15:00:00+08:00" if closed else None,
                          basis="close" if closed else "received", token=token)
            count += 1
    if not count:
        raise RuntimeError("dashboard board has no verified observations")
    return count


def _publish_heatmap(day, value, token, *, closed):
    count = 0
    for kind in ("concept", "industry"):
        for mode in ("change", "net_inflow", "five_day_return"):
            metric = value.get(kind, {}).get(mode) or {}
            if metric.get("status") != "available":
                continue
            store.publish(day, f"heat:{kind}:{mode}", metric,
                source_at=metric.get("as_of") or day+"T15:00:00+08:00",
                basis="close" if closed and value.get("observation_mode") != "intraday" else "source", token=token)
            count += 1
    if not count:
        raise RuntimeError("dashboard heatmap has no verified observations")
    return count


def capture_once(token):
    from server.api.routers import market_home
    day, now, live, next_open = _target()
    closed = not live
    parts, attempts = store.read(day, now=now)
    jobs = {
        "board": lambda: market_home.build_market_board(datetime.fromisoformat(day).date(), now=store.clock()),
        "heatmap": lambda: market_home.build_sector_heatmap(datetime.fromisoformat(day).date(), now=store.clock()),
        "events": lambda: market_home._read_event_page(day),
    }
    if live:
        jobs.pop("board")
        jobs.update({
            "quotes": lambda: market_home.build_market_board(datetime.fromisoformat(day).date(), now=store.clock()),
            "indices": lambda: market_home.read_live_indices(market_home.INDEX_NAMES, day, now=store.clock(), deadline=monotonic()+12),
            "turnover": lambda: market_home.read_live_turnover(day, now=store.clock(), deadline=monotonic()+14),
            "ladder": lambda: market_home.capture_live_ladder(day, now=store.clock(), deadline=monotonic()+12),
            "liquidity": lambda: market_home._capture_liquidity(day),
        })
        if "breadth_history" not in parts:
            jobs["breadth_history"] = lambda: {"rows": [{**row, "trade_date": str(row["trade_date"])[:10]}
                for row in market_home._breadth_history(day, monotonic()+3)]}
        if time(11, 30) < now.time() < time(13):
            def paused(name):
                prefix = "heat:" if name == "heatmap" else "index:" if name == "indices" else None
                keys = [key for key in parts if key.startswith(prefix)] if prefix else [name]
                expected = 6 if name == "heatmap" else 7 if name == "indices" else 1
                return len(keys) == expected and all(key in parts and parts[key][1]["state"] == "paused" for key in keys)
            jobs = {name: load for name, load in jobs.items() if not paused(name)}
    else:
        complete = {
            "board": all(key in parts and parts[key][1]["state"] == "closed"
                         for key in ("quotes", "ladder", "turnover", *["index:"+code for code in market_home.INDEX_NAMES]))
                     and attempts.get("board", {}).get("status") == "COMPLETE",
            "heatmap": len([key for key, (_, meta) in parts.items() if key.startswith("heat:") and meta["state"] == "closed"]) == 6
                       and attempts.get("heatmap", {}).get("status") == "COMPLETE",
            "events": "events" in parts and parts["events"][1]["state"] == "closed",
        }
        jobs = {key: value for key, value in jobs.items() if not complete[key]}
    run_id = uuid.uuid4().hex
    with store.database(write=True) as db, db:
        store._authorized(db, token, store.clock())
        db.execute("""UPDATE dashboard_owner SET run_id=?,state='RUNNING',total_units=?,completed_units=0,
            last_progress_at=?,next_capture_at=? WHERE slot=1 AND token=?""",
            (run_id, len(jobs), store.iso(store.clock()), store.iso(now+timedelta(seconds=20)), token))
    def capture(name, load):
        store.start_component(day, name, run_id, token)
        published, error = 0, None
        try:
            value = load()
            if name in ("board", "quotes"):
                if value.get("source_errors"):
                    error = json.dumps(value["source_errors"], ensure_ascii=True)[:1000]
                published = _publish_board(day, value, token, closed=closed)
                if not error and name == "board" and any(field != "events" for field in value.get("unavailable_fields", [])):
                    error = "partial_board_coverage"
            elif name == "indices":
                published = _publish_board(day, {"indices": value}, token, closed=False)
                if published != 7:
                    error = "partial_index_coverage"
            elif name == "turnover":
                if not value or not value.get("rows"):
                    raise RuntimeError("turnover ranking unavailable")
                published = _publish_board(day, {"as_of": value["as_of"], "rankings": {"turnover": value["rows"]},
                    "ranking_observations": {"turnover": {key: item for key, item in value.items() if key != "rows"}},
                    "turnover_coverage": {"verified_count": len(value["rows"]), "sample_size": value["observed_count"],
                        "excluded_count": value["excluded_count"]}}, token, closed=False)
            elif name == "ladder":
                if value.get("source_errors"):
                    error = json.dumps(value["source_errors"], ensure_ascii=True)[:1000]
                published = _publish_board(day, {"limit_ladder": value}, token, closed=False)
            elif name == "heatmap":
                published = _publish_heatmap(day, value, token, closed=closed)
                if published != 6:
                    error = "partial_sector_metric_coverage"
            else:
                if name == "liquidity" and value.get("status") != "available":
                    raise RuntimeError("intraday liquidity not available")
                store.publish(day, name, value, source_at=value.get("as_of") if name == "liquidity" else
                    day+"T15:00:00+08:00" if closed else None,
                    basis="close" if closed else "source" if name == "liquidity" else "received", token=token)
                published = 1
        except Exception as exc:
            error = error or getattr(exc, "reason_code", None) or getattr(exc, "code", None) or type(exc).__name__
            LOGGER.warning("dashboard %s failed: %s", name, error)
        store.finish_component(day, name, run_id, token, published=published, error=error)
    with ThreadPoolExecutor(max_workers=len(jobs) or 1) as pool:
        futures = [pool.submit(capture, name, load) for name, load in jobs.items()]
        for future in futures:
            future.result()
    if live and time(11, 30) < now.time() < time(13) and not jobs:
        with store.database(write=True) as db, db:
            store._authorized(db, token, store.clock())
            db.execute("UPDATE dashboard_owner SET next_capture_at=? WHERE slot=1 AND token=?",
                       (store.iso(now.replace(hour=13, minute=0, second=0, microsecond=0)), token))
    if closed:
        written, receipts = store.read(day)
        retry = any(receipts.get(name, {}).get("status") != "COMPLETE" for name in ("board", "heatmap", "events"))
        retry = retry or any(meta["state"] != "closed" for key, (_, meta) in written.items() if key.startswith("heat:"))
        with store.database(write=True) as db, db:
            store._authorized(db, token, store.clock())
            db.execute("UPDATE dashboard_owner SET next_capture_at=? WHERE slot=1 AND token=?",
                       (store.iso(store.clock()+timedelta(seconds=60) if retry else next_open), token))


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--capture":
        raise SystemExit("dashboard capture requires its supervisor identity")
    capture_once(sys.argv[2])
