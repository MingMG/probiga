import hashlib
import json

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

from server.api.routers import trading_v3
from server.engine import strategy_governance
from server.trading_v3.decision_truth import canonical_hash


_PRIOR = "1" * 32
_NEXT = "2" * 32
_URL = "/api/v3/stock-pool/revision-diff"


def _row(*, later=False, empty=False, **changes):
    trade_date = changes.get("trade_date", "2026-11-10")
    run_uid = _NEXT if later else _PRIOR
    supersedes = _PRIOR if later else ""
    pools = {"observation": [], "confirmation": [], "tradable": []}
    targets = []
    if not empty:
        retained = {"stock_code": "000001", "strategies": ["trend"], "reason": "观察"}
        pools["observation"].append(retained)
        if later:
            targets.append({"stock_code": "600410", "strategy_key": "event"})
        else:
            pools["confirmation"].extend([
                {**retained, "strategies": ["value"]},
                {"stock_code": "600900", "strategies": ["trend"], "reason": "前批确认"},
            ])
    payload = {
        "run_uid": run_uid, "trade_date": trade_date, "run_revision": 2 if later else 1,
        "supersedes_run_uid": supersedes, "input_hash": "a" * 64, "decision_hash": "b" * 64,
        "is_canonical": True, "input_ready": True, "result_mode": "CANONICAL_PERSISTED",
        "source_status": changes.get("source_status", "fresh"),
        "candidate_source": {
            "status": "COMPLETED", "query_completed": True,
            "trade_date": trade_date, "data_date": trade_date,
        },
        "pools": pools, "paper_execution_plan": {"targets": targets},
    }
    raw = json.dumps(payload, ensure_ascii=False)
    return {
        "run_uid": run_uid, "trade_date": trade_date, "run_revision": payload["run_revision"],
        "supersedes_run_uid": supersedes, "status": "COMPLETED", "is_canonical": int(later),
        "input_ready": 1, "source_status": "fresh",
        "finished_at": "2026-11-11T08:30:00" if later else "2026-11-10T22:35:00",
        "input_hash": payload["input_hash"], "decision_hash": payload["decision_hash"],
        "result_json": raw, "result_hash": hashlib.sha256(raw.encode()).hexdigest(),
        **{f"{level}_count": len(pool) for level, pool in pools.items()},
        **changes,
    }


@pytest.fixture
def revision_api(monkeypatch):
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool,
    )
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_strategy_governance_run (
                run_uid TEXT PRIMARY KEY, trade_date TEXT NOT NULL,
                run_revision INTEGER NOT NULL, supersedes_run_uid TEXT NOT NULL,
                status TEXT NOT NULL, is_canonical INTEGER NOT NULL, input_ready INTEGER NOT NULL,
                source_status TEXT NOT NULL, finished_at TEXT,
                input_hash TEXT NOT NULL, decision_hash TEXT NOT NULL,
                result_json TEXT NOT NULL, result_hash TEXT NOT NULL,
                observation_count INTEGER NOT NULL, confirmation_count INTEGER NOT NULL,
                tradable_count INTEGER NOT NULL
            )
        """))
    monkeypatch.setattr(strategy_governance, "get_engine", lambda: engine)
    monkeypatch.setattr(trading_v3, "strategy_governance_database_deferred", lambda: False)
    # The separate canonical replay suite exercises the semantic funding
    # validator. Here the exact stored bytes, hash, status, lineage and V3
    # membership projection all run through the production read path.
    replayed = []
    monkeypatch.setattr(
        strategy_governance, "_validate_canonical_governance_result",
        lambda result: replayed.append(result["run_uid"]),
    )
    app = FastAPI()
    app.include_router(trading_v3.router, prefix="/api")
    with TestClient(app) as client:
        yield engine, client, replayed
    engine.dispose()


def _seed(engine, *rows):
    fields = ", ".join(rows[0])
    values = ", ".join(f":{field}" for field in rows[0])
    with engine.begin() as connection:
        connection.execute(text(
            f"INSERT INTO st_strategy_governance_run ({fields}) VALUES ({values})"
        ), list(rows))


def _compare(client, **params):
    return client.get(_URL, params={"prior_run_uid": _PRIOR, "next_run_uid": _NEXT, **params})


def test_governance_pool_revision_api_reads_exact_sealed_successors(revision_api):
    engine, client, replayed = revision_api
    _seed(engine, _row(), _row(later=True, is_canonical=0))
    statements = []

    @event.listens_for(engine, "before_cursor_execute")
    def record(_conn, _cursor, sql, params, _context, _executemany):
        statements.append((sql, params))

    response = _compare(client)
    assert response.status_code == 200
    payload = response.json()
    assert payload["comparison_time_source"] == "GOVERNANCE_RUN_FINISHED_AT"
    assert payload["read_only"] is True and payload["real_order_authority"] is False
    diff = payload["data"]
    assert diff["prior"]["finished_at"] == "2026-11-10T22:35:00+08:00"
    assert "cutoff" not in diff["prior"]
    assert diff["next"]["run_uid"] == _NEXT
    assert [row["stock_code"] for row in diff["added"]] == ["600410"]
    assert [row["stock_code"] for row in diff["removed"]] == ["600900"]
    assert diff["retained"][0]["prior"]["strategy_keys"] == ["trend", "value"]
    assert diff["diff_sha256"] == canonical_hash({
        key: value for key, value in diff.items() if key != "diff_sha256"
    })
    assert replayed == [_PRIOR, _NEXT]
    assert len(statements) == 1
    assert statements[0][0].startswith("SELECT ")
    assert statements[0][1] == (_PRIOR, _NEXT)


@pytest.mark.parametrize(("changes", "status"), [
    ({"status": "FAILED"}, 404),
    ({"finished_at": None}, 404),
    ({"source_status": "stale"}, 409),
    ({"result_hash": "f" * 64}, 409),
    ({"supersedes_run_uid": "3" * 32}, 409),
    ({"trade_date": "2026-11-11"}, 409),
    ({"observation_count": 1}, 409),
])
def test_unverified_governance_revision_never_becomes_empty(revision_api, changes, status):
    engine, client, _replayed = revision_api
    _seed(engine, _row(), _row(later=True, empty=True, **changes))
    response = _compare(client)
    assert response.status_code == status
    assert "data" not in response.json()


def test_governance_pool_revision_api_accepts_verified_empty_only_with_exact_runs(revision_api):
    engine, client, _replayed = revision_api
    _seed(engine, _row(), _row(later=True, empty=True))
    response = _compare(client)
    assert response.status_code == 200
    assert response.json()["data"]["next"]["stock_count"] == 0
    assert response.json()["data"]["counts"] == {"added": 0, "removed": 2, "retained": 0}
    assert _compare(client, next_run_uid="3" * 32).status_code == 404
    assert _compare(client, next_run_uid=_PRIOR).status_code == 422
    assert client.get(_URL, params={"next_run_uid": _NEXT}).status_code == 422
