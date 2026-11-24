"""One supervised Linux producer, independent of browser connections."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timedelta
import json
import logging
import os
from pathlib import Path
import signal
import stat
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
_external_thread = None
_external_role = False
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


def _retry_control(operation):
    deadline = monotonic()+5
    for attempt in range(3):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            code = getattr(exc, "sqlite_errorcode", 0) & 255
            if code not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED) or attempt == 2 or monotonic() >= deadline:
                raise
            _stop.wait(min(.25*(attempt+1), max(0, deadline-monotonic())))


def _control_error(stage, exc):
    return json.dumps({"stage": stage, "type": type(exc).__name__,
        "errno": getattr(exc, "sqlite_errorcode", None) or getattr(exc, "errno", None)}, separators=(",", ":"))


def _terminate(child):
    if child.poll() is not None:
        return
    try:
        if os.name == "posix":
            if os.getpgid(child.pid) != child.pid:
                raise RuntimeError("dashboard capture process group identity changed")
            os.killpg(child.pid, signal.SIGTERM)
        else:
            child.terminate()
    except ProcessLookupError:
        child.wait(timeout=3)
        return
    try:
        child.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            if os.name == "posix":
                os.killpg(child.pid, signal.SIGKILL)
            else:
                child.kill()
        except ProcessLookupError:
            pass
        child.wait(timeout=3)


def _terminal(token, reason=None):
    now = store.clock()
    with store.database(write=True) as db, db:
        _parent(db, token)
        state = db.execute("SELECT run_id FROM dashboard_owner WHERE slot=1").fetchone()
        db.execute("""UPDATE dashboard_capture SET status='FAILED',finished_at=?,error=?
            WHERE run_id=? AND status='RUNNING'""", (store.iso(now), reason or "capture_child_ended", state["run_id"]))
        failed = db.execute("SELECT COUNT(*) FROM dashboard_capture WHERE run_id=? AND status='FAILED'", (state["run_id"],)).fetchone()[0]
        completed = db.execute("SELECT COUNT(*) FROM dashboard_capture WHERE run_id=? AND status IN ('FAILED','COMPLETE')", (state["run_id"],)).fetchone()[0]
        db.execute("UPDATE dashboard_owner SET state=?,child_pid=NULL,completed_units=?,last_progress_at=? WHERE slot=1",
                   ("FAILED" if failed or reason else "COMPLETE", completed, store.iso(now)))
        if not failed and not reason:
            previous = db.execute("""SELECT trade_date,error FROM dashboard_capture
                WHERE trade_date=(SELECT trade_date FROM dashboard_capture
                    WHERE run_id=? AND component<>'supervisor' LIMIT 1)
                  AND component='supervisor' AND status='FAILED'""", (state["run_id"],)).fetchone()
            if previous:
                db.execute("UPDATE dashboard_capture SET status='COMPLETE',finished_at=?,error=? WHERE trade_date=? AND component='supervisor'",
                    (store.iso(now), json.dumps({"failure": previous["error"], "recovered_by_run_id": state["run_id"]}), previous["trade_date"]))


def _record_supervisor_failure(token, reason, *, child_terminal=True):
    now = store.clock()
    with store.database(write=True) as db, db:
        _parent(db, token)
        run_id = db.execute("SELECT run_id FROM dashboard_owner WHERE slot=1").fetchone()[0]
        if not run_id:
            LOGGER.error("dashboard control failure cannot be persisted without a run: %s", reason)
            return
        if child_terminal:
            db.execute("""UPDATE dashboard_capture SET status='FAILED',finished_at=?,error=?
                WHERE run_id=? AND status='RUNNING'""", (store.iso(now), reason, run_id))
        day = db.execute("SELECT trade_date FROM dashboard_capture WHERE run_id=? LIMIT 1", (run_id,)).fetchone()
        day = day[0] if day else now.date().isoformat()
        exists = db.execute("SELECT 1 FROM dashboard_capture WHERE trade_date=? AND component='supervisor' AND run_id=?", (day, run_id)).fetchone()
        db.execute("""INSERT INTO dashboard_capture VALUES (?,'supervisor',?,'FAILED',?,?,?,0)
            ON CONFLICT(trade_date,component) DO UPDATE SET run_id=excluded.run_id,status='FAILED',
            started_at=excluded.started_at,finished_at=excluded.finished_at,error=excluded.error,published=0""",
            (day, run_id, store.iso(now), store.iso(now), reason))
        completed = db.execute("SELECT COUNT(*) FROM dashboard_capture WHERE run_id=? AND status IN ('FAILED','COMPLETE')", (run_id,)).fetchone()[0]
        db.execute("""UPDATE dashboard_owner SET state='FAILED',total_units=COALESCE(total_units,0)+?,
            completed_units=?,last_progress_at=? WHERE slot=1""",
            (0 if exists else 1, completed, store.iso(now)))


def _release_owner(token, state):
    with store.database(write=True) as db, db:
        _parent(db, token)
        db.execute("""UPDATE dashboard_owner SET token=NULL,child_pid=NULL,state=?,lease_until=?
            WHERE slot=1 AND token=? AND pid=?""", (state, store.iso(store.clock()), token, os.getpid()))


def _supervise():
    import fcntl
    token = uuid.uuid4().hex
    child = None
    fd = None
    owned = False
    failure = None
    stage = "initialize"
    try:
        _retry_control(store.initialize)
        lock_path = store._path().parent / "market-dashboard-owner.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        stage = "owner_lock"
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        stage = "claim"
        while not _stop.is_set():
            if _retry_control(lambda: claim(token)):
                owned = True
                break
            _stop.wait(2)
        if _stop.is_set():
            return
        while not _stop.is_set():
            stage = "renew"
            _retry_control(lambda: renew(token))
            stage = "runtime"
            state = _retry_control(store.runtime)
            due = state.get("next_capture_at")
            if due and store.stamp(due) > store.clock():
                _stop.wait(min(5, (store.stamp(due)-store.clock()).total_seconds()))
                continue
            started = monotonic()
            command = [sys.executable, "-m", "server.common.market_dashboard_worker", "--capture", token]
            if _external_role:
                command.append("--external")
            stage = "spawn"
            child = subprocess.Popen(command,
                cwd=str(Path(__file__).resolve().parents[2]), stdin=subprocess.DEVNULL,
                start_new_session=True)
            stage = "renew_child"
            _retry_control(lambda: renew(token, child_pid=child.pid))
            timed_out = False
            stage = "wait"
            while child.poll() is None and not _stop.wait(2):
                stage = "renew_child"
                _retry_control(lambda: renew(token, child_pid=child.pid))
                if monotonic()-started >= CAPTURE_DEADLINE_SECONDS:
                    timed_out = True
                    break
                stage = "wait"
            if child.poll() is None:
                stage = "terminate"
                _terminate(child)
            stage = "wait"
            result = child.wait(timeout=3)
            child = None
            stage = "terminal"
            _retry_control(lambda: _terminal(token, "capture_deadline_exceeded" if timed_out else "capture_child_failed" if result else None))
            _stop.wait(max(1, CYCLE_SECONDS-(monotonic()-started)))
    except Exception as exc:
        if stage == "owner_lock" and isinstance(exc, BlockingIOError):
            LOGGER.info("dashboard producer already owned by another API process")
        else:
            failure = _control_error(stage, exc)
            LOGGER.error("dashboard producer failed: %s", failure)
    finally:
        if child is not None:
            try:
                _terminate(child)
                child.wait(timeout=3)
                child = None
            except Exception as exc:
                cleanup = _control_error("cleanup_terminate", exc)
                failure = json.dumps({"failure": json.loads(failure), "cleanup": json.loads(cleanup)}) if failure else cleanup
                LOGGER.error("dashboard child terminal state unproven: %s", cleanup)
        if owned:
            try:
                if failure:
                    _retry_control(lambda: _record_supervisor_failure(token, failure, child_terminal=child is None))
                elif child is None:
                    _retry_control(lambda: _terminal(token, "producer_shutdown"))
            except Exception as exc:
                LOGGER.error("dashboard control failure could not be persisted: %s; original=%s",
                    _control_error("persist_failure", exc), failure)
            if child is None:
                try:
                    _retry_control(lambda: _release_owner(token, "NOT_STARTED" if _stop.is_set() and not failure else "FAILED"))
                except Exception as exc:
                    LOGGER.error("dashboard owner release failed: %s", _control_error("release_owner", exc))
        if fd is not None:
            os.close(fd)


def start_dashboard_producer():
    global _thread, _external_thread
    if os.name != "posix" or os.environ.get("PROBIGA_DEPLOYMENT_MODE") != "production":
        return
    with _start_lock:
        if _thread and _thread.is_alive():
            return
        _stop.clear()
        _thread = threading.Thread(target=_supervise, name="market-dashboard-producer", daemon=True)
        _thread.start()
        _external_thread = threading.Thread(target=_supervise_external_process, name="external-market-producer", daemon=True)
        _external_thread.start()


def stop_dashboard_producer():
    _stop.set()
    if _thread:
        _thread.join(timeout=10)
        if _thread.is_alive():
            raise RuntimeError("dashboard producer shutdown deadline exceeded")
    if _external_thread:
        _external_thread.join(timeout=15)
        if _external_thread.is_alive():
            raise RuntimeError("external observation supervisor shutdown deadline exceeded")


def _supervise_external_process():
    from server.common.trading_day_store import journal_runtime_root
    base = journal_runtime_root()
    root = base / "external-market"
    try:
        base_info = base.lstat()
        if (not base.is_absolute() or not stat.S_ISDIR(base_info.st_mode) or base_info.st_uid != os.geteuid()
                or stat.S_IMODE(base_info.st_mode) != 0o700):
            raise RuntimeError("external observation parent root is not private")
        root.mkdir(mode=0o700, exist_ok=True)
        info = root.lstat()
        if (not root.is_absolute() or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o700):
            raise RuntimeError("external observation runtime root is not private")
        environment = {**os.environ, "PROBIGA_JOB_LOG_ROOT": str(root)}
        for attempt in range(3):
            child = subprocess.Popen([sys.executable, "-m", "server.common.market_dashboard_worker", "--external-supervisor"],
                cwd=str(Path(__file__).resolve().parents[2]), env=environment,
                stdin=subprocess.DEVNULL, start_new_session=True)
            while child.poll() is None and not _stop.wait(2):
                pass
            if _stop.is_set():
                if child.poll() is None:
                    child.send_signal(signal.SIGTERM)
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        _terminate(child)
                return
            LOGGER.error("external observation supervisor exited: attempt=%s exit=%s", attempt+1, child.returncode)
            if _stop.wait(5*(attempt+1)):
                return
    except Exception as exc:
        LOGGER.error("external observation supervisor FAILED: %s", type(exc).__name__)


def capture_external(token):
    from tools.env_config import create_tool_engine, load_project_env
    from tools.refresh_external_market_context import capture_current_observation
    now = store.clock()
    day, run_id = now.date().isoformat(), uuid.uuid4().hex
    with store.database(write=True) as db, db:
        store._authorized(db, token, now)
        db.execute("""UPDATE dashboard_owner SET run_id=?,state='RUNNING',total_units=1,completed_units=0,
            last_progress_at=?,next_capture_at=? WHERE slot=1 AND token=?""",
            (run_id, store.iso(now), store.iso(now+timedelta(seconds=120)), token))
    store.start_component(day, "external", run_id, token)
    load_project_env()
    engine = create_tool_engine(connect_args={"connect_timeout": 8, "read_timeout": 10, "write_timeout": 10})
    published, error = 0, None
    try:
        report = capture_current_observation(engine)
        store.publish(report["context_date"], "external", report, source_at=None, basis="received", token=token)
        published = report["available_count"]
        if report["status"] != "COMPLETE":
            error = "partial_external_instrument_coverage"
    except Exception as exc:
        error = getattr(exc, "reason_code", None) or getattr(exc, "code", None) or type(exc).__name__
        LOGGER.warning("external observation acquisition FAILED: %s", error)
    finally:
        engine.dispose()
    store.finish_component(day, "external", run_id, token, published=published, error=error)


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
            failure = market_home._source_failure(exc)
            detail = getattr(exc, "detail", None)
            if isinstance(detail, dict):
                # Liquidity already supplies structured, bounded source diagnostics.
                for key in ("error", "stage", "type", "code", "database_errno"):
                    item = detail.get(key)
                    if isinstance(item, (str, int)) and len(str(item)) <= 120:
                        failure[key] = item
                reason = detail.get("reason")
                if isinstance(reason, str) and reason.startswith("liquidity_live:"):
                    failure["reason"] = reason[:200]
            error = error or json.dumps(failure, ensure_ascii=True)[:1000]
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
    if sys.argv[1:] == ["--external-supervisor"]:
        _external_role = True
        CAPTURE_DEADLINE_SECONDS = 180
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, lambda *_: _stop.set())
        _supervise()
    elif len(sys.argv) == 4 and sys.argv[1] == "--capture" and sys.argv[3] == "--external":
        capture_external(sys.argv[2])
    elif len(sys.argv) == 3 and sys.argv[1] == "--capture":
        capture_once(sys.argv[2])
    else:
        raise SystemExit("dashboard capture requires its supervisor identity")
