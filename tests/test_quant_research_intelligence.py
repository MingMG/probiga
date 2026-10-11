from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import create_engine, text

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
    assert first["idempotent"] is False
    assert second["idempotent"] is True
    assert second["batch_id"] == first["batch_id"]
    assert loaded["candidate_count"] == 3
    assert loaded["research_only"] is True
    assert all(item["real_order_authority"] is False for item in loaded["candidates"])


def test_runtime_bundle_includes_quant_research_contract():
    from server.common import production_runtime_schema_bundle as bundle

    assert "quant_research" in {name for name, _ in bundle._MIGRATIONS}
    assert "quant_research" in {name for name, _ in bundle._VALIDATORS}


def test_research_radar_frontend_renders_research_only_candidates():
    source = open("server/static/js/app.js", encoding="utf-8").read()

    assert "量化研究候选 · 仅研究" in source
    assert "quant_research_candidates" in source
    assert "不会注册为生产策略" in source
