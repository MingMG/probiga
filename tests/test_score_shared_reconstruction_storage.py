"""Prove full-market score storage keeps the original reconstruction evidence."""
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from random import Random
from time import perf_counter
import zlib

import pytest

from server.common import analysis_pool_receipt as receipts
from server.common import analysis_snapshot_codec as codec
from server.common.daily_delivery_control import (
    analysis_publication_reference,
    bind_analysis_publication_checkpoint,
)
from server.engine.strategy_center import _candidate_reconstruction_contract


TARGET = "2026-11-13"
DECISION = "2026-11-15T15:58:41"
RECONSTRUCTED_AT = "2026-11-15T13:47:45.256897"
MODE = "HISTORICAL_RECONSTRUCTION"
OLD_ENCODING = "zlib-base64-dictionary-columnar-json-v1"
MARKET_SIZE = 5_571


def _canonical(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def _logical_size(payload):
    """Measure expanded JSON without allocating a full-market byte string."""
    metadata = {key: value for key, value in payload.items() if key not in {"analysis_rows", "scored_rows"}}
    size = len(_canonical(metadata)) + 2  # Each additional table follows a metadata member.
    for name in ("analysis_rows", "scored_rows"):
        rows = payload[name]
        size += len(_canonical(name)) + 3 + max(0, len(rows) - 1)
        size += sum(len(_canonical(row)) for row in rows)
    return size


def _reconstruction_proof():
    entropy = Random(20261113)
    core = {
        "schema": "probiga.qmt-announcement-historical-reconstruction.v2",
        "mode": MODE,
        "target_trade_date": TARGET,
        "source_query_cutoff_at": TARGET + "T23:59:59.999999",
        "reconstructed_at": RECONSTRUCTED_AT,
        "known_at": RECONSTRUCTED_AT,
        "provider": "cninfo.announcement",
        "source": "cninfo.announcement",
        "scheduler_run_uid": "1" * 32,
        "build_sha": "2" * 40,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "authority": {
            "schema": "probiga.qmt-announcement-reconstruction-authority.v3",
            "reconciliation": {
                "prior_members": 5_579,
                "target_members": 5_580,
                "proof_chain": [
                    {"batch_sha256": entropy.randbytes(32).hex(), "source_position": item}
                    for item in range(150)
                ],
            },
            "localized_example": '留存引号"、反斜杠\\与花括号{}',
        },
    }
    return {**core, "reconstruction_sha256": hashlib.sha256(_canonical(core)).hexdigest()}


def _score_inputs(count):
    proof = _reconstruction_proof()
    entropy = Random(334)
    analysis, scores = [], []
    for index in range(count):
        code = f"{600000 + index:06}"
        stock_hash = entropy.randbytes(32).hex()
        event_text = json.dumps(
            {
                "notice_count_14d": index % 16,
                "risk_titles": [f'{code} "pit_reconstruction_provenance": {{}} \\ 交易公告'],
                "event_revision_ids": [stock_hash[:32]],
                "event_content_hashes": [stock_hash],
                "pit_reconstruction_mode": MODE,
                "pit_reconstruction_sha256": proof["reconstruction_sha256"],
                "pit_reconstructed_at": RECONSTRUCTED_AT,
                "pit_reconstruction_provenance": proof,
                "latest_notice_time": TARGET + "T19:01:33",
            },
            ensure_ascii=False,
        )
        analysis.append({
            "stock_code": code, "analysis_date": TARGET, "event_risk_detail": event_text,
            "event_risk_score": float(index % 54), "recommend_status": "SUSPENDED",
        })
        scores.append({
            "stock_code": code, "event_pit_status": "AVAILABLE", "finance_pit_status": "AVAILABLE",
            "flow_input_decision_at": DECISION, "flow_input_root_sha256": "3" * 64,
            "industry_snapshot_date": TARGET, "industry_snapshot_source": "gj_big_qmt_inner",
            "industry_source_snapshot_date": "2026-11-12", "industry_previous_session_fallback": True,
            "membership_proof_sha256": "4" * 64, "pit_common_receipt_root_hash": "5" * 64,
            "turnover_full_market_count": count, "turnover_full_market_proof_root_sha256": "6" * 64,
            "pit_reconstruction_mode": MODE, "pit_reconstructed_at": RECONSTRUCTED_AT,
            "pit_reconstruction_sha256": proof["reconstruction_sha256"],
            "pit_reconstruction_provenance": proof, "event_risk_detail": event_text,
            "event_coverage_id": stock_hash, "final_trade_score": float(index % 80 + 10),
            "close": round(entropy.uniform(1, 380), 2), "kline_window_ratio": entropy.random(),
            "market_cap": entropy.randrange(100_000_000, 100_000_000_000),
        })
    return {
        "trade_date": TARGET, "decision_at": DECISION, "run_uid": "7" * 32,
        "build_sha": "8" * 40, "analysis_rows": analysis, "scored_rows": scores,
    }


@pytest.fixture
def score_inputs():
    return _score_inputs(40)


def test_repeated_reconstruction_preserves_exact_rows_and_candidate_proof(score_inputs, monkeypatch):
    logical = receipts._score_snapshot_payload(**score_inputs)
    logical_size = _logical_size(logical)
    assert logical_size == len(_canonical(logical))
    monkeypatch.setattr(codec, "SNAPSHOT_EXPANDED_MAX_BYTES", 1 << 20)
    assert logical_size > codec.SNAPSHOT_EXPANDED_MAX_BYTES

    with pytest.raises(ValueError, match="expanded size exceeds contract"):
        codec.encode_snapshot_payload(logical)
    envelope = receipts.build_score_snapshot(**score_inputs)
    assert envelope["encoding"] != OLD_ENCODING
    restored = receipts.decode_score_snapshot(envelope, trade_date=TARGET)
    assert restored == logical
    assert hashlib.sha256(_canonical(restored)).digest() == hashlib.sha256(_canonical(logical)).digest()
    assert restored["input_proof"]["membership_proof_sha256"] == "4" * 64
    assert {row["industry_source_snapshot_date"] for row in restored["scored_rows"]} == {"2026-11-12"}
    publication = receipts.build_publication_receipt(
        manifest={
            "schema": receipts.ANALYSIS_POOL_RECEIPT_SCHEMA,
            "trade_date": TARGET,
            "score_snapshot_sha256": envelope["payload_sha256"],
        },
        run_uid=score_inputs["run_uid"], build_sha=score_inputs["build_sha"],
        publisher_task_type="analysis_fast", published_at=DECISION, score_snapshot=envelope,
    )
    reference = analysis_publication_reference(publication)
    checkpoint = bind_analysis_publication_checkpoint({"replay_output": json.dumps(reference)}, publication)
    assert checkpoint["publication_receipt"]["score_snapshot"] == envelope

    proof = logical["scored_rows"][0]["pit_reconstruction_provenance"]
    candidate = _candidate_reconstruction_contract(TARGET, restored["scored_rows"][:2])
    assert candidate["mode"] == MODE
    assert candidate["provenance"] == proof
    assert candidate["reconstruction_sha256"] == proof["reconstruction_sha256"]
    restored["scored_rows"][0]["pit_reconstruction_provenance"]["authority"]["schema"] = "changed"
    assert restored["scored_rows"][1]["pit_reconstruction_provenance"] == proof
    assert json.loads(restored["analysis_rows"][0]["event_risk_detail"])["pit_reconstruction_provenance"] == proof
    assert receipts.decode_score_snapshot(envelope, trade_date=TARGET) == logical


def test_score_storage_binds_compressed_and_payload_checksums(score_inputs):
    envelope = receipts.build_score_snapshot(**score_inputs)
    compressed = base64.b64decode(envelope["payload_base64"], validate=True)
    assert envelope["encoding"] != OLD_ENCODING
    stale_hash = {**envelope, "payload_sha256": "0" * 64}
    relabeled = {**envelope, "encoding": OLD_ENCODING}
    stale_compression_hash = {**envelope, "compressed_sha256": "0" * 64}
    excess_stream = compressed + zlib.compress(b"unused")
    trailing_data = {
        **envelope,
        "payload_base64": base64.b64encode(excess_stream).decode("ascii"),
        "compressed_sha256": hashlib.sha256(excess_stream).hexdigest(),
    }
    for tampered in (stale_hash, stale_compression_hash, trailing_data, relabeled):
        with pytest.raises(ValueError, match="ANALYSIS_SCORE_SNAPSHOT"):
            receipts.decode_score_snapshot(tampered, trade_date=TARGET)

    compact = codec.decode_snapshot_payload(zlib.decompress(compressed))
    assert hashlib.sha256(_canonical(compact)).hexdigest() == envelope["payload_sha256"]
    malformed = deepcopy(compact)
    malformed["analysis_rows"][0]["event_risk_detail"] = ["same prefix"]
    forged = deepcopy(compact)
    shared = forged[codec.SCORE_RECONSTRUCTION_STORAGE_KEY]
    provenance = json.loads(shared["provenance_json"])
    provenance["provider"] = "unverified.provider"
    shared["provenance_json"] = json.dumps(provenance, ensure_ascii=False)
    for invalid in (malformed, forged):
        invalid_compressed = zlib.compress(codec.encode_snapshot_payload(invalid), level=9)
        resealed_invalid = {
            **envelope,
            "payload_base64": base64.b64encode(invalid_compressed).decode("ascii"),
            "compressed_sha256": hashlib.sha256(invalid_compressed).hexdigest(),
            "payload_sha256": hashlib.sha256(_canonical(invalid)).hexdigest(),
        }
        with pytest.raises(ValueError, match="ANALYSIS_SCORE_SNAPSHOT_INVALID"):
            receipts.decode_score_snapshot(resealed_invalid, trade_date=TARGET)



def test_compact_storage_checks_rehydration_budget_before_returning_rows(score_inputs, monkeypatch):
    envelope = receipts.build_score_snapshot(**score_inputs)
    assert envelope["encoding"] != OLD_ENCODING

    monkeypatch.setattr(codec, "_SCORE_REHYDRATION_TEXT_MAX_BYTES", 64 * 1024)
    with pytest.raises(ValueError, match="ANALYSIS_SCORE_SNAPSHOT_INVALID"):
        receipts.decode_score_snapshot(envelope, trade_date=TARGET)


def test_generic_preliminary_and_archived_score_storage_are_independent(score_inputs):
    path = Path(__file__).parent / "fixtures" / "analysis_snapshots_v1.json"
    archived = json.loads(path.read_text(encoding="utf-8"))
    historic_score = receipts.decode_score_snapshot(archived["score"])
    assert hashlib.sha256(_canonical(historic_score)).hexdigest() == archived["score"]["payload_sha256"]

    preliminary_inputs = _score_inputs(81)
    preliminary = receipts.build_preliminary_analysis_snapshot(
        analysis_rows=preliminary_inputs["analysis_rows"],
        scored_rows=preliminary_inputs["scored_rows"],
        candidate_rows=[{"stock_code": row["stock_code"]} for row in preliminary_inputs["scored_rows"][:80]],
        flow_date=TARGET, hot_date="", market_mood_score=52.5,
    )
    assert preliminary["encoding"] == OLD_ENCODING
    assert receipts.decode_preliminary_analysis_snapshot(preliminary)["scored_rows"] == preliminary_inputs["scored_rows"]

    nonhistorical = deepcopy(score_inputs)
    for table in ("analysis_rows", "scored_rows"):
        for row in nonhistorical[table]:
            row["event_risk_detail"] = '{"event_pit_status":"AVAILABLE"}'
            for key in ("pit_reconstruction_mode", "pit_reconstructed_at", "pit_reconstruction_sha256",
                        "pit_reconstruction_provenance"):
                row.pop(key, None)
    ordinary = receipts.build_score_snapshot(**nonhistorical)
    assert ordinary["encoding"] == OLD_ENCODING
    assert receipts.decode_score_snapshot(ordinary, trade_date=TARGET) == receipts._score_snapshot_payload(**nonhistorical)


def _market_scale_round_trip():
    inputs = _score_inputs(MARKET_SIZE)
    logical = receipts._score_snapshot_payload(**inputs)
    proof_size = len(_canonical(logical["scored_rows"][0]["pit_reconstruction_provenance"]))
    assert 10_000 <= proof_size <= 17_000
    logical_size = _logical_size(logical)
    assert logical_size > 256 * (1 << 20)

    start = perf_counter()
    envelope = receipts.build_score_snapshot(**inputs)
    encode_seconds = perf_counter() - start
    assert envelope["encoding"] != OLD_ENCODING
    start = perf_counter()
    restored = receipts.decode_score_snapshot(envelope, trade_date=TARGET)
    decode_seconds = perf_counter() - start
    assert restored == logical
    assert _candidate_reconstruction_contract(TARGET, restored["scored_rows"][:2])["mode"] == MODE

    compressed = base64.b64decode(envelope["payload_base64"], validate=True)
    wire_size = len(zlib.decompress(compressed))
    base64_size = len(envelope["payload_base64"])
    assert wire_size < 128 * (1 << 20)
    assert base64_size < receipts.SCORE_SNAPSHOT_MAX_BYTES
    return {
        "stocks": MARKET_SIZE, "shared_proof_bytes": proof_size, "logical_bytes": logical_size,
        "wire_bytes": wire_size, "base64_bytes": base64_size,
        "encode_seconds": round(encode_seconds, 3), "decode_seconds": round(decode_seconds, 3),
    }


@pytest.mark.skipif(
    os.environ.get("PROBIGA_RUN_FULL_SCORE_STORAGE_TEST") != "1",
    reason="explicit full-market performance acceptance",
)
def test_market_scale_shared_reconstruction_round_trip():
    print(json.dumps(_market_scale_round_trip(), sort_keys=True))
