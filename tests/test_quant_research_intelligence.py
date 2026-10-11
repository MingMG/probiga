from __future__ import annotations

import json
from datetime import datetime

import pytest
from sqlalchemy import create_engine, event, text

from biz.intelligence import quant_research


def _payload(**overrides):
    candidates = []
    for index in range(1, 4):
        candidates.append({
            "title": f"Study {index}",
            "source_url": f"https://example.com/paper-{index}",
            "source_published_date": "2026-10-01",
            "strategy_family": "cross-sectional factor",
            "market_scope": "A-share daily equities",
            "strategy_logic": "Rank securities by a documented signal and rebalance weekly.",
            "evidence_stage": "HISTORICAL_BACKTEST",
            "reproducibility_status": "NOT_REPRODUCED",
            "qmt_feasibility": "ADAPTATION_REQUIRED",
            "qmt_feasibility_note": "Requires a PIT feature adapter and cost model.",
            "performance_metrics": {"annualized_return": "reported by source"},
            "transaction_costs": "Source assumes 10bp each side.",
            "biases_and_limitations": "Independent reproduction is still required.",
            "public_code_url": f"https://github.com/example/study-{index}",
            "priority": index,
        })
    payload = {
        "source_task_id": "A股量化研究精选",
        "published_at": "2026-10-09T19:00:00+08:00",
        "candidates": candidates,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    payload.update(overrides)
    return payload


def _sqlite_ledger():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text(f"""
            CREATE TABLE {quant_research.BATCH_TABLE} (
                batch_id TEXT PRIMARY KEY, batch_schema TEXT NOT NULL,
                source_task_id TEXT NOT NULL, published_at TEXT NOT NULL,
                candidate_count INTEGER NOT NULL, status TEXT NOT NULL,
                automatic_real_order_submission INTEGER NOT NULL,
                real_order_authority INTEGER NOT NULL, payload_json TEXT NOT NULL,
                payload_hash TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL
            )
        """))
        connection.execute(text(f"""
            CREATE TABLE {quant_research.CANDIDATE_TABLE} (
                candidate_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL,
                rank_no INTEGER NOT NULL, title TEXT NOT NULL,
                source_url TEXT NOT NULL, source_published_date TEXT NOT NULL,
                strategy_family TEXT NOT NULL, market_scope TEXT NOT NULL,
                evidence_stage TEXT NOT NULL, reproducibility_status TEXT NOT NULL,
                qmt_feasibility TEXT NOT NULL, priority INTEGER NOT NULL,
                status TEXT NOT NULL, automatic_real_order_submission INTEGER NOT NULL,
                real_order_authority INTEGER NOT NULL, candidate_json TEXT NOT NULL,
                candidate_hash TEXT NOT NULL, created_at TEXT NOT NULL,
                UNIQUE(batch_id, rank_no)
            )
        """))
    return engine


def test_quant_research_batch_is_hash_bound_and_research_only():
    batch = quant_research.normalize_quant_research_batch(_payload())

    assert batch["candidate_count"] == 3
    assert batch["status"] == "RESEARCH_ONLY"
    assert batch["automatic_real_order_submission"] is False
    assert batch["real_order_authority"] is False
    assert all(item["status"] == "RESEARCH_ONLY" for item in batch["candidates"])
    assert len(batch["batch_id"]) == len(batch["payload_hash"]) == 64


@pytest.mark.parametrize(
    "patch",
    (
        {"automatic_real_order_submission": True},
        {"real_order_authority": True},
        {"candidates": []},
    ),
)
def test_quant_research_rejects_order_authority_and_incomplete_batches(patch):
    with pytest.raises(ValueError):
        quant_research.normalize_quant_research_batch(_payload(**patch))


def test_quant_research_persistence_is_idempotent_and_readable(monkeypatch):
    engine = _sqlite_ledger()
    monkeypatch.setattr(
        quant_research, "validate_quant_research_runtime", lambda *_args, **_kwargs: None
    )

    first = quant_research.persist_quant_research_batch(
        engine, _payload(), created_at=datetime(2026, 10, 9, 19, 1)
    )
    second = quant_research.persist_quant_research_batch(
        engine, _payload(), created_at=datetime(2026, 10, 9, 19, 2)
    )
    loaded = quant_research.load_quant_research_candidates(
        engine, as_of="2026-10-11"
    )

    assert first["status"] == "COMPLETE"
    assert first["readback_verified"] is True
    assert first["idempotent"] is False
    assert second["readback_verified"] is True
    assert second["idempotent"] is True
    assert second["batch_id"] == first["batch_id"]
    assert loaded["candidate_count"] == 3
    assert loaded["research_only"] is True
    assert all(item["real_order_authority"] is False for item in loaded["candidates"])


def test_quant_research_same_candidates_can_appear_in_independent_weekly_batches(
    monkeypatch,
):
    engine = _sqlite_ledger()
    monkeypatch.setattr(
        quant_research, "validate_quant_research_runtime", lambda *_args, **_kwargs: None
    )
    first_payload = _payload(published_at="2026-10-09T04:00:00+08:00")
    second_payload = _payload(published_at="2026-10-16T04:00:00+08:00")

    first = quant_research.persist_quant_research_batch(engine, first_payload)
    second = quant_research.persist_quant_research_batch(engine, second_payload)
    first_retry = quant_research.persist_quant_research_batch(engine, first_payload)
    second_retry = quant_research.persist_quant_research_batch(engine, second_payload)

    assert first["status"] == second["status"] == "COMPLETE"
    assert first["batch_id"] != second["batch_id"]
    assert first_retry["idempotent"] is second_retry["idempotent"] is True
    with engine.connect() as connection:
        rows = connection.execute(text(f"""
            SELECT batch_id, COUNT(*) AS count, COUNT(DISTINCT candidate_id) AS ids
            FROM {quant_research.CANDIDATE_TABLE}
            GROUP BY batch_id ORDER BY batch_id
        """)).mappings().all()
        assert {row["batch_id"]: (row["count"], row["ids"]) for row in rows} == {
            first["batch_id"]: (3, 3),
            second["batch_id"]: (3, 3),
        }
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {quant_research.BATCH_TABLE}"
        )).scalar_one() == 2
    loaded = quant_research.load_quant_research_candidates(
        engine, as_of="2026-10-16"
    )
    assert loaded["candidate_count"] == 6
    assert {row["batch_id"] for row in loaded["candidates"]} == {
        first["batch_id"], second["batch_id"]
    }


def test_quant_research_distinct_batches_at_same_time_can_share_candidates(
    monkeypatch,
):
    engine = _sqlite_ledger()
    monkeypatch.setattr(
        quant_research, "validate_quant_research_runtime", lambda *_args, **_kwargs: None
    )
    first_payload = _payload()
    second_payload = _payload()
    second_payload["candidates"][2]["strategy_logic"] += " Compare a sector-neutral variant."

    first = quant_research.persist_quant_research_batch(engine, first_payload)
    second = quant_research.persist_quant_research_batch(engine, second_payload)

    assert first["status"] == second["status"] == "COMPLETE"
    assert first["readback_verified"] is second["readback_verified"] is True
    assert first["batch_id"] != second["batch_id"]
    with engine.connect() as connection:
        rows = connection.execute(text(f"""
            SELECT batch_id, rank_no, candidate_id, candidate_hash
            FROM {quant_research.CANDIDATE_TABLE}
            ORDER BY batch_id, rank_no
        """)).mappings().all()
    assert len(rows) == len({row["candidate_id"] for row in rows}) == 6
    by_batch = {
        batch_id: {row["rank_no"]: row for row in rows if row["batch_id"] == batch_id}
        for batch_id in (first["batch_id"], second["batch_id"])
    }
    for rank_no in (1, 2):
        before = by_batch[first["batch_id"]][rank_no]
        after = by_batch[second["batch_id"]][rank_no]
        assert before["candidate_hash"] == after["candidate_hash"]
        assert before["candidate_id"] != after["candidate_id"]
    assert quant_research.load_quant_research_candidates(engine)["candidate_count"] == 6


def test_quant_research_post_commit_readback_rejects_commit_event_tampering(
    monkeypatch,
):
    engine = _sqlite_ledger()
    monkeypatch.setattr(
        quant_research, "validate_quant_research_runtime", lambda *_args, **_kwargs: None
    )
    payload = _payload()
    batch = quant_research.normalize_quant_research_batch(payload)
    changed_candidate = dict(batch["candidates"][0])
    changed_candidate["strategy_logic"] = "Changed after the transaction readback."
    observations = {"commit_events": 0, "transaction_candidate_readback": False}

    def observe_sql(_connection, _cursor, statement, _parameters, _context, _many):
        if (
            statement.lstrip().startswith("SELECT *")
            and f"FROM {quant_research.CANDIDATE_TABLE}" in statement
        ):
            observations["transaction_candidate_readback"] = True

    def tamper_before_dbapi_commit(connection):
        observations["commit_events"] += 1
        assert observations["transaction_candidate_readback"] is True
        assert connection.in_transaction() is True
        stored = connection.execute(text(
            f"SELECT candidate_json FROM {quant_research.CANDIDATE_TABLE} "
            "WHERE batch_id=:batch_id AND rank_no=1"
        ), {"batch_id": batch["batch_id"]}).scalar_one()
        assert json.loads(stored) == batch["candidates"][0]
        connection.execute(text(
            f"UPDATE {quant_research.CANDIDATE_TABLE} SET candidate_json=:value "
            "WHERE batch_id=:batch_id AND rank_no=1"
        ), {"value": json.dumps(changed_candidate), "batch_id": batch["batch_id"]})

    event.listen(engine, "after_cursor_execute", observe_sql)
    event.listen(engine, "commit", tamper_before_dbapi_commit)
    receipt = None
    try:
        with pytest.raises(RuntimeError, match="readback differs"):
            receipt = quant_research.persist_quant_research_batch(engine, payload)
    finally:
        event.remove(engine, "commit", tamper_before_dbapi_commit)
        event.remove(engine, "after_cursor_execute", observe_sql)

    assert receipt is None
    assert observations["commit_events"] == 1
    # The mutation reached the real SQLite commit: only a post-commit readback
    # can now reject this durable content before issuing COMPLETE.
    with engine.connect() as connection:
        stored = connection.execute(text(
            f"SELECT candidate_json FROM {quant_research.CANDIDATE_TABLE} "
            "WHERE batch_id=:batch_id AND rank_no=1"
        ), {"batch_id": batch["batch_id"]}).scalar_one()
    assert json.loads(stored) == changed_candidate


@pytest.mark.parametrize(
    ("column", "replacement"),
    (
        ("candidate_json", "{}"),
        ("candidate_hash", "0" * 64),
        ("candidate_id", "0" * 64),
        ("rank_no", 4),
        ("automatic_real_order_submission", 1),
        ("real_order_authority", 1),
        ("status", "EXECUTABLE"),
        ("title", "Different title in indexed metadata"),
        ("source_url", "https://example.com/different-source"),
        ("reproducibility_status", "INDEPENDENTLY_REPRODUCED"),
    ),
)
def test_quant_research_retry_rejects_changed_candidate_readback(
    monkeypatch, column, replacement,
):
    engine = _sqlite_ledger()
    monkeypatch.setattr(
        quant_research, "validate_quant_research_runtime", lambda *_args, **_kwargs: None
    )
    payload = _payload()
    receipt = quant_research.persist_quant_research_batch(engine, payload)
    with engine.begin() as connection:
        connection.execute(text(
            f"UPDATE {quant_research.CANDIDATE_TABLE} SET {column}=:replacement "
            "WHERE batch_id=:batch_id AND rank_no=1"
        ), {"replacement": replacement, "batch_id": receipt["batch_id"]})

    with pytest.raises(RuntimeError):
        quant_research.persist_quant_research_batch(engine, payload)


def test_quant_research_retry_rejects_changed_candidate_json_with_original_hash(
    monkeypatch,
):
    engine = _sqlite_ledger()
    monkeypatch.setattr(
        quant_research, "validate_quant_research_runtime", lambda *_args, **_kwargs: None
    )
    payload = _payload()
    receipt = quant_research.persist_quant_research_batch(engine, payload)
    with engine.begin() as connection:
        stored = connection.execute(text(
            f"SELECT candidate_json FROM {quant_research.CANDIDATE_TABLE} "
            "WHERE batch_id=:batch_id AND rank_no=1"
        ), {"batch_id": receipt["batch_id"]}).scalar_one()
        changed = json.loads(stored)
        changed["strategy_logic"] = "A different method without changing stored hashes."
        connection.execute(text(
            f"UPDATE {quant_research.CANDIDATE_TABLE} SET candidate_json=:value "
            "WHERE batch_id=:batch_id AND rank_no=1"
        ), {"value": json.dumps(changed), "batch_id": receipt["batch_id"]})

    with pytest.raises(RuntimeError):
        quant_research.persist_quant_research_batch(engine, payload)


@pytest.mark.parametrize(
    ("column", "replacement"),
    (
        ("payload_json", "{}"),
        ("payload_hash", "0" * 64),
        ("batch_schema", "probiga.quant-research-batch.invalid"),
        ("source_task_id", "Different research owner"),
        ("published_at", "2026-10-16T04:00:00"),
        ("candidate_count", 4),
        ("status", "EXECUTABLE"),
        ("automatic_real_order_submission", 1),
        ("real_order_authority", 1),
    ),
)
def test_quant_research_retry_rejects_changed_batch_readback(
    monkeypatch, column, replacement,
):
    engine = _sqlite_ledger()
    monkeypatch.setattr(
        quant_research, "validate_quant_research_runtime", lambda *_args, **_kwargs: None
    )
    payload = _payload()
    receipt = quant_research.persist_quant_research_batch(engine, payload)
    with engine.begin() as connection:
        connection.execute(text(
            f"UPDATE {quant_research.BATCH_TABLE} SET {column}=:replacement "
            "WHERE batch_id=:batch_id"
        ), {"replacement": replacement, "batch_id": receipt["batch_id"]})

    with pytest.raises(RuntimeError):
        quant_research.persist_quant_research_batch(engine, payload)


@pytest.mark.parametrize(
    ("table", "column", "replacement"),
    (
        (quant_research.CANDIDATE_TABLE, "candidate_json", "{}"),
        (quant_research.CANDIDATE_TABLE, "candidate_hash", "0" * 64),
        (quant_research.CANDIDATE_TABLE, "candidate_id", "0" * 64),
        (quant_research.CANDIDATE_TABLE, "rank_no", 4),
        (quant_research.CANDIDATE_TABLE, "automatic_real_order_submission", 1),
        (quant_research.BATCH_TABLE, "payload_json", "{}"),
        (quant_research.BATCH_TABLE, "payload_hash", "0" * 64),
        (quant_research.BATCH_TABLE, "source_task_id", "Different research owner"),
    ),
)
def test_quant_research_loader_rejects_tampered_ledger(
    monkeypatch, table, column, replacement,
):
    engine = _sqlite_ledger()
    monkeypatch.setattr(
        quant_research, "validate_quant_research_runtime", lambda *_args, **_kwargs: None
    )
    receipt = quant_research.persist_quant_research_batch(engine, _payload())
    condition = " AND rank_no=1" if table == quant_research.CANDIDATE_TABLE else ""
    with engine.begin() as connection:
        connection.execute(text(
            f"UPDATE {table} SET {column}=:replacement "
            f"WHERE batch_id=:batch_id{condition}"
        ), {"replacement": replacement, "batch_id": receipt["batch_id"]})

    with pytest.raises(RuntimeError):
        quant_research.load_quant_research_candidates(engine)


def test_runtime_bundle_includes_quant_research_contract():
    from server.common import production_runtime_schema_bundle as bundle

    assert "quant_research" in {name for name, _ in bundle._MIGRATIONS}
    assert "quant_research" in {name for name, _ in bundle._VALIDATORS}


def test_research_radar_frontend_renders_research_only_candidates():
    source = open("server/static/js/app.js", encoding="utf-8").read()

    assert "量化研究候选 · 仅研究" in source
    assert "quant_research_candidates" in source
    assert "不会注册为生产策略" in source
