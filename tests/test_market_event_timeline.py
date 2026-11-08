"""Read persisted event-ledger shapes with stable, exact-date paging."""
from datetime import date, datetime
import json
from pathlib import Path
import shutil
import subprocess

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from server.api.routers import market_event_timeline as timeline
from server.common.limit_up_events import LIMIT_STATUS_BASIS


DAY = date(2026, 11, 6)
NOW = datetime(2026, 11, 7, 9)


@pytest.fixture
def ledger():
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE sm_limit_up_event (
                event_id INTEGER PRIMARY KEY, event_key TEXT, trade_date DATE, stock_code TEXT,
                short_name TEXT, event_type TEXT, event_at DATETIME, source_snapshot_at DATETIME,
                created_at DATETIME, source_provider TEXT, limit_status_basis TEXT, evidence_json TEXT
            )
        """))
        connection.execute(text("CREATE INDEX limit_day_time ON sm_limit_up_event (trade_date,event_at)"))
        connection.execute(text("""
            CREATE TABLE sm_market_radar_event (
                event_id INTEGER PRIMARY KEY, stock_code TEXT, sector_name TEXT, event_type TEXT,
                snapshot_at DATETIME, created_at DATETIME, data_source TEXT, direction TEXT, detail_json TEXT
            )
        """))
    yield engine
    engine.dispose()


def insert_limit(engine, event_id, stamp, kind="TOUCH", *, provider="gj_big_qmt_inner",
                 created_at=None, proven=True):
    session = stamp[:10]
    with engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO sm_limit_up_event VALUES
                (:id,:key,:day,'600001','示例股票',:kind,:at,:at,:created,:provider,:basis,:evidence)
        """), {
            "id": event_id, "key": f"{event_id:064x}", "day": session,
            "kind": kind, "at": stamp, "created": created_at or f"{session} 15:00:10",
            "provider": provider, "basis": LIMIT_STATUS_BASIS,
            "evidence": json.dumps({
                "limit_status_basis": LIMIT_STATUS_BASIS,
                "at_limit": (kind != "BREAK") if proven else None,
                "sealed": kind in {"SEAL", "RESEAL", "QUEUE_SNAPSHOT"},
            }),
        })


def read(engine, *, day=DAY, stream="limit_up", limit=2, cursor="", now=NOW):
    return timeline.read_timeline(engine, day, stream, limit=limit, cursor=cursor, now=now)


def test_same_time_events_and_higher_ids_respect_the_first_read_bounds(ledger):
    insert_limit(ledger, 1, "2026-11-06 09:30:00.123456")
    insert_limit(ledger, 2, "2026-11-06 09:30:00.123456", "SEAL")
    insert_limit(ledger, 3, "2026-11-06 13:21:00", "BREAK")
    insert_limit(ledger, 4, "2026-11-06 15:00:02", "RESEAL")
    insert_limit(ledger, 5, "2026-11-05 15:00:00", "SEAL")
    insert_limit(ledger, 6, "2026-11-06 15:00:00", "SEAL", provider="unverified")
    insert_limit(ledger, 7, "2026-11-06 10:00:00", created_at="2026-11-07 09:01:00")
    insert_limit(ledger, 8, "2026-11-06 12:30:00", "BREAK")
    first = read(ledger)
    assert first["trade_date"] == "2026-11-06"
    assert first["status"] == "AVAILABLE"
    assert first["coverage"] == {"status": "UNVERIFIED", "reason_code": "NO_SESSION_COVERAGE_RECEIPT"}
    assert [r["event_id"] for r in first["rows"]] == ["1", "2"]
    assert first["rows"][0]["event_at"] == "2026-11-06T09:30:00.123456+08:00"
    assert first["rows"][0]["available_at"] == "2026-11-06T15:00:10+08:00"
    one = read(ledger, limit=1)
    assert read(ledger, cursor=one["next_cursor"], limit=1)["rows"][0]["event_id"] == "2"

    insert_limit(ledger, 9, "2026-11-06 14:00:00", "QUEUE_SNAPSHOT")
    later = datetime(2026, 11, 7, 9, 2)
    second = read(ledger, cursor=first["next_cursor"], now=later)
    assert second["known_through"] == first["known_through"]
    assert [r["event_type"] for r in second["rows"]] == ["BREAK", "RESEAL"]
    assert second["next_cursor"] is None
    assert read(ledger, cursor=first["page_cursor"], now=later) == first

    refreshed = read(ledger, limit=20, now=later)
    assert {r["event_id"] for r in refreshed["rows"]} == {"1", "2", "3", "4", "7", "9"}


def test_invalid_evidence_advances_raw_keyset_and_does_not_become_a_zero_session(ledger):
    insert_limit(ledger, 1, "2026-11-06 10:00:00", "SEAL", proven=False)
    insert_limit(ledger, 2, "2026-11-06 10:00:01", "SEAL")
    first = read(ledger, limit=1)
    assert first["status"] == "NO_VERIFIED_RECORDS"
    assert first["discarded_count"] == 1
    assert first["coverage"]["status"] == "UNVERIFIED"
    assert read(ledger, cursor=first["next_cursor"], limit=1)["rows"][0]["event_id"] == "2"


def test_radar_stream_keeps_exact_day_identity_and_provenance_separate(ledger):
    with ledger.begin() as connection:
        connection.execute(text("""
            INSERT INTO sm_market_radar_event VALUES
                (1,'600002',NULL,'stock_anomaly','2026-11-06 09:45:00','2026-11-06 09:45:04',
                 'qmt_full_tick_5level','UP','{"stock":{"short_name":"示例个股"}}'),
                (2,NULL,'示例板块','sector_anomaly','2026-11-06 14:25:00','2026-11-06 14:25:09',
                 'qmt_full_tick_5level','DOWN','{}'),
                (3,'600003',NULL,'stock_anomaly','2026-11-05 09:45:00','2026-11-05 09:45:04',
                 'qmt_full_tick_5level','UP','{}')
        """))
    radar = read(ledger, stream="radar")
    assert radar["source"] == "sm_market_radar_event"
    assert [row["subject"] for row in radar["rows"]] == ["示例个股", "示例板块"]
    assert radar["rows"][1]["available_at"] == "2026-11-06T14:25:09+08:00"
    assert read(ledger)["status"] == "NO_VERIFIED_RECORDS"


def test_public_contract_rejects_cross_date_cursor_invalid_dates_and_excessive_limits(ledger, monkeypatch):
    insert_limit(ledger, 1, "2026-11-06 10:00:00", "SEAL")
    insert_limit(ledger, 2, "2026-11-06 10:00:01", "BREAK")
    monkeypatch.setattr(timeline, "get_current_mysql_url", lambda: "test")
    monkeypatch.setattr(timeline, "get_mysql_url", lambda: "test")
    monkeypatch.setattr(timeline, "_engine", lambda _: ledger)
    app = FastAPI()
    app.include_router(timeline.router, prefix="/api")
    client = TestClient(app)
    path = "/api/monitor/event-timeline"
    first = client.get(path, params={"date": "2026-11-06", "limit": 1})
    assert first.status_code == 200
    cursor = first.json()["next_cursor"]
    assert client.get(path, params={"date": "2026-11-05", "cursor": cursor}).status_code == 422
    assert client.get(path, params={"date": "2026-11-06", "stream": "radar", "cursor": cursor}).status_code == 422
    assert client.get(path, params={"date": "2026-02-30"}).status_code == 422
    assert client.get(path, params={"date": "2026-11-06", "limit": 101}).status_code == 422


def test_missing_ledger_is_a_data_blocker(ledger):
    with ledger.begin() as connection:
        connection.execute(text("DROP TABLE sm_limit_up_event"))
    result = read(ledger)
    assert result["status"] == "DATA_BLOCKED"
    assert result["reason_code"] == "EVENT_STORE_UNAVAILABLE"
    assert result["next_cursor"] is None

def test_page_keeps_date_and_cursor_while_switching_streams():
    executable = shutil.which("node")
    if executable is None:
        pytest.skip("Node.js unavailable")
    script = Path(__file__).resolve().parents[1] / "server/static/js/market-event-timeline.js"
    result = subprocess.run([executable, "-e", r"""
const assert = require('node:assert/strict');
function element() {
    return {value:'', children:[], listeners:{}, textContent:'',
        addEventListener(name, handler) {this.listeners[name]=handler},
        append(child) {this.children.push(child)},
        replaceChildren() {this.children=[]},
    };
}
const nodes = Object.fromEntries(['trade-date','stream','events','status','previous','next','page-number','board','filters']
    .map(id => [id, element()]));
nodes.stream.value = 'limit_up';
global.document = {getElementById: id => nodes[id], createElement: element};
global.location = {search:'?date=2026-11-06', pathname:'/static/market-event-timeline.html'};
global.history = {replaceState(_, __, url) {location.search = url;}};
const calls = [];
let releaseOld;
global.fetch = async url => {
    const params = new URL(url, 'http://local.test').searchParams;
    const call = Object.fromEntries(params);
    calls.push(call);
    if (call.date === '2026-11-05') await new Promise(resolve => {releaseOld = resolve});
    const event = {event_type:'SEAL', subject:'<img src=x>', stock_code:'600001',
        event_at:'2026-11-06T10:05:00+08:00', available_at:'2026-11-06T10:05:09+08:00'};
    return {ok:true, json: async () => ({trade_date:call.date, stream:call.stream,
        rows:call.stream === 'limit_up' ? [event] : [], page_cursor:call.cursor || 'frozen-first',
        next_cursor:call.cursor === 'frozen-next' ? null : 'frozen-next'})};
};
require(process.argv[1]);
const ready = () => new Promise(resolve => setImmediate(resolve));
(async () => {
    await ready();
    assert.deepEqual(calls[0], {date:'2026-11-06',stream:'limit_up'});
    assert.match(nodes.status.textContent, /2026-11-06.*全天采集覆盖未核验/);
    const cells = nodes.events.children[0].children;
    assert.equal(cells[2].textContent, '2026-11-06 10:05:09');
    assert.equal(cells[4].textContent, '<img src=x> 600001');
    assert.match(nodes.board.href, /trade_date=2026-11-06/);
    nodes.next.listeners.click(); await ready();
    assert.equal(calls[1].cursor, 'frozen-next');
    assert.equal(nodes['page-number'].textContent, '第 2 页');
    nodes.previous.listeners.click(); await ready();
    assert.equal(calls[2].cursor, 'frozen-first');
    nodes.stream.value='radar';
    nodes.stream.listeners.change(); await ready();
    assert.deepEqual(calls[3], {date:'2026-11-06',stream:'radar'});
    assert.match(nodes.status.textContent, /暂无可核验记录/);
    nodes['trade-date'].value = '2026-11-05';
    nodes['trade-date'].listeners.change();
    assert.equal(typeof releaseOld, 'function');
    nodes['trade-date'].value = '2026-11-04';
    nodes['trade-date'].listeners.change(); await ready();
    assert.match(nodes.status.textContent, /2026-11-04/);
    releaseOld(); await ready();
    assert.match(nodes.status.textContent, /2026-11-04/);
    assert.doesNotMatch(nodes.status.textContent, /2026-11-05/);
    nodes['trade-date'].value = '';
    nodes['trade-date'].listeners.change(); await ready();
    assert.equal(nodes.status.textContent, '请选择交易日。');
    assert.equal(nodes.events.children.length, 0);
    assert.equal(nodes.next.disabled, true);
    assert.doesNotMatch(location.search, /date=/);
})().catch(error => {console.error(error); process.exitCode=1;});
""", str(script)], capture_output=True, text=True, encoding="utf-8", timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
