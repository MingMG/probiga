"""Daily score shard durability and final-receipt readback."""
import json
import os
from time import perf_counter

import pytest
from sqlalchemy import create_engine, text

from server.common import analysis_pool_receipt as receipts
from server.common import analysis_snapshot_codec as legacy_codec
from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import ARTIFACT_TABLE, MEMBER_TABLE, privileged_migrate_formal_analysis_schema
from tests.test_score_shared_reconstruction_storage import _logical_size, _score_inputs


GENERATION = "9" * 64


@pytest.fixture
def authority(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'formal-score.db'}")
    privileged_migrate_formal_analysis_schema(engine)
    try:
        yield engine, FormalAnalysisStore(engine)
    finally:
        engine.dispose()


def _mixed_inputs(count):
    inputs = _score_inputs(count)
    # A valid score row need not mirror display JSON byte for byte. This
    # defeats the old all-stock common-proof special case.
    inputs["scored_rows"][-1]["event_risk_detail"] += " "
    return inputs


def test_durable_score_shards_preserve_a_mixed_full_score_and_small_receipt(authority, monkeypatch):
    engine, store = authority
    inputs = _mixed_inputs(257)
    logical = receipts._complete_score_snapshot_payload(**inputs)
    monkeypatch.setattr(legacy_codec, "SNAPSHOT_EXPANDED_MAX_BYTES", 2 * 1024 * 1024)
    with pytest.raises(ValueError, match="expanded size exceeds contract"):
        receipts.build_score_snapshot(**inputs)

    envelope = receipts.build_durable_score_snapshot(store=store, generation_sha256=GENERATION, **inputs)
    assert envelope["encoding"] == receipts.DURABLE_SCORE_SNAPSHOT_ENCODING
    assert "payload_base64" not in envelope
    assert len(json.dumps(envelope).encode()) < 4096
    members = store.list_members(GENERATION, "score")
    assert len([key for key in members if key.startswith("rows:")]) == 3
    manifest = store.load_artifact(envelope["payload_sha256"])
    assert [shard["row_count"] for shard in manifest["shards"]] == [128, 128, 1]
    assert "pit_reconstruction_provenance" in manifest["shared_scored_fields"]
    assert receipts.decode_score_snapshot(envelope, engine=engine) == logical

    publication = receipts.build_publication_receipt(
        manifest={
            "schema": receipts.ANALYSIS_POOL_RECEIPT_SCHEMA, "trade_date": inputs["trade_date"],
            "score_snapshot_sha256": envelope["payload_sha256"],
        },
        run_uid=inputs["run_uid"], publisher_task_type="analysis_fast", build_sha=inputs["build_sha"],
        published_at=inputs["decision_at"], score_snapshot=envelope, store=store,
    )
    with monkeypatch.context() as ctx:
        ctx.setattr(store, "load_artifacts", lambda *_: pytest.fail("receipt validity reloaded scored rows"))
        assert receipts.publication_receipt_is_valid(publication, store=store)
        assert receipts.read_score_snapshot_header(envelope, store=store)["stock_count"] == 257

    assert receipts.build_durable_score_snapshot(store=store, generation_sha256=GENERATION, **inputs) == envelope


def test_durable_score_rebind_keeps_exact_verified_members_and_original_origin(authority, monkeypatch):
    _, store = authority
    inputs = _mixed_inputs(129)
    prior = receipts.build_durable_score_snapshot(store=store, generation_sha256=GENERATION, **inputs)
    before = store.list_members(GENERATION, "score")
    with monkeypatch.context() as ctx:
        ctx.setattr(store, "load_artifacts", lambda *_: pytest.fail("rebind loaded score rows"))
        assert receipts.rebind_durable_score_snapshot(
            prior, store=store, run_uid=prior["run_uid"], build_sha=prior["build_sha"],
        ) == prior
        rebound = receipts.rebind_durable_score_snapshot(
            prior, store=store, run_uid="1" * 32, build_sha="b" * 40,
        )
    after = store.list_members(GENERATION, "score")
    assert all(after[key] == sha for key, sha in before.items())
    assert len(after) == len(before) + 1
    assert rebound["payload_sha256"] != prior["payload_sha256"]
    assert rebound["decision_at"] == prior["decision_at"]
    new_manifest = store.load_artifact(rebound["payload_sha256"])
    assert new_manifest["origin"] == {"run_uid": inputs["run_uid"], "build_sha": inputs["build_sha"]}
    assert receipts.decode_score_snapshot(rebound, store=store) == {
        **receipts.decode_score_snapshot(prior, store=store), "run_uid": "1" * 32, "build_sha": "b" * 40,
    }


def test_durable_score_fails_closed_on_tampered_or_missing_storage(authority):
    engine, store = authority
    envelope = receipts.build_durable_score_snapshot(store=store, generation_sha256=GENERATION, **_mixed_inputs(129))
    for tampered in (
        {**envelope, "stock_count": 128},
        {**envelope, "decision_at": "2026-11-15T15:58:44"},
        {**envelope, "generation_sha256": "a" * 64},
        {**envelope, "payload_sha256": "2" * 64},
    ):
        with pytest.raises(ValueError, match="ANALYSIS_SCORE_DURABLE"):
            receipts.decode_score_snapshot(tampered, store=store)

    members = store.list_members(GENERATION, "score")
    with engine.begin() as conn:
        conn.execute(text(f"UPDATE {ARTIFACT_TABLE} SET payload_zlib=:bad WHERE artifact_sha256=:sha"),
                     {"bad": b"truncated", "sha": members["rows:000001"]})
    with pytest.raises(ValueError, match="ANALYSIS_SCORE_DURABLE"):
        receipts.decode_score_snapshot(envelope, store=store)
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {MEMBER_TABLE} WHERE stage='score' AND unit_key='rows:000001'"))
    with pytest.raises(ValueError, match="SHARD_MEMBER_MISMATCH"):
        receipts.decode_score_snapshot(envelope, store=store)


def test_signed_materialization_reuse_requires_the_exact_authority_and_envelope(authority, monkeypatch):
    engine, store = authority
    inputs = _mixed_inputs(2)
    envelope = receipts.build_durable_score_snapshot(store=store, generation_sha256=GENERATION, **inputs)
    with engine.connect() as connection, engine.connect() as other:
        decoded = receipts.decode_score_snapshot(envelope, engine=connection)
        rebound = receipts.rebind_durable_score_snapshot(
            envelope, store=store, run_uid="4" * 32, build_sha=inputs["build_sha"],
        )
        monkeypatch.setattr(receipts, "decode_score_snapshot", lambda *_a, **_kw: pytest.fail("readback re-decoded"))
        args = {
            "trade_date": inputs["trade_date"], "analysis_rows": inputs["analysis_rows"], "recommendation_rows": [],
        }
        tiny_manifest = receipts.build_pool_manifest(
            **args, score_snapshot=envelope, decoded_score_snapshot=decoded, engine=connection,
        )
        assert tiny_manifest["score_snapshot_sha256"] == envelope["payload_sha256"]
        for reference, materialized, bind in (
            (rebound, decoded, connection), (envelope, decoded, other), (envelope, dict(decoded), connection),
        ):
            with pytest.raises(ValueError, match="READBACK_IDENTITY_MISMATCH"):
                receipts.build_pool_manifest(
                    **args, score_snapshot=reference, decoded_score_snapshot=materialized, engine=bind,
                )
        decoded["analysis_rows"][0]["event_risk_detail"] = "changed after authentication"
        with pytest.raises(ValueError, match="PUBLICATION_ROWS_MISMATCH"):
            receipts.build_pool_manifest(
                **{**args, "analysis_rows": decoded["analysis_rows"]},
                score_snapshot=envelope, decoded_score_snapshot=decoded, engine=connection,
            )


@pytest.mark.skipif(
    os.environ.get("PROBIGA_RUN_FULL_DURABLE_SCORE_TEST") != "1",
    reason="explicit full-market durable storage acceptance",
)
def test_durable_score_publishes_over_256_mib_without_special_case(authority):
    engine, store = authority
    inputs = _mixed_inputs(5_571)
    logical = receipts._complete_score_snapshot_payload(**inputs)
    logical_size = _logical_size(logical)
    assert logical_size > 256 * (1 << 20)
    start = perf_counter()
    envelope = receipts.build_durable_score_snapshot(store=store, generation_sha256=GENERATION, **inputs)
    write_seconds = perf_counter() - start
    start = perf_counter()
    assert receipts.decode_score_snapshot(envelope, engine=engine) == logical
    read_seconds = perf_counter() - start
    with engine.connect() as connection:
        artifacts = connection.execute(text(
            f"SELECT COUNT(*),SUM(payload_bytes),SUM(LENGTH(payload_zlib)) FROM {ARTIFACT_TABLE}"
        )).one()
    print(json.dumps({
        "row_count": 5571, "logical_bytes": logical_size,
        "artifacts": artifacts[0], "raw_shard_bytes": artifacts[1], "compressed_storage_bytes": artifacts[2],
        "envelope_bytes": len(json.dumps(envelope).encode()), "write_seconds": round(write_seconds, 3),
        "read_seconds": round(read_seconds, 3),
    }, sort_keys=True))
