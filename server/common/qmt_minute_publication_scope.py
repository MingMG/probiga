"""Immutable per-security openings from the native QMT minute readback."""

from __future__ import annotations

from datetime import date, datetime, time
import json
import re
from typing import Any, Mapping, Sequence

from sqlalchemy import text

from server.common.pit_facts import normalize_decision_at
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_result import SHA64, load_receipt, persisted_rows, row_hash
from server.common.qmt_history_coverage import COVERAGE_TABLE

SCHEMA = "probiga.qmt-minute-security-publication.v1"
_DATASETS = {
    "stock_minute": "stock_minute_security_publication",
    "index_minute": "index_minute_security_publication",
}
_MAX_PROOF_BYTES = 2_000_000
_MAX_SELECTED_CODES = 120
_READ_CODE_BATCH_SIZE = 40
_MAX_BARS_PER_CODE = 400
_CODE = re.compile(r"[0-9]{6}")


def _inventory_valid(value: Any) -> bool:
    return bool(
        isinstance(value, dict) and value
        and all(
            isinstance(code, str) and _CODE.fullmatch(code)
            and isinstance(item, dict) and set(item) == {"row_count", "row_hash"}
            and type(item["row_count"]) is int
            and 1 <= item["row_count"] <= _MAX_BARS_PER_CODE
            and isinstance(item["row_hash"], str) and SHA64.fullmatch(item["row_hash"])
            for code, item in value.items()
        )
    )


def seal_minute_security_publication(
    connection, *, dataset: str, trade_date: str, fingerprints: Mapping[str, Any],
) -> str:
    """Bind the existing completed partition and its per-code readback on commit."""
    receipt = load_receipt(connection, dataset=dataset, trade_date=trade_date)
    inventory = fingerprints.get("security_fingerprints")
    if (
        not receipt or receipt["status"] != "COMPLETE" or not receipt["finished_at"]
        or not receipt["run_id"]
        or not SHA64.fullmatch(str(receipt.get("content_sha256") or ""))
        or not SHA64.fullmatch(str(fingerprints.get("row_hash") or ""))
        or fingerprints["row_hash"] != receipt["detail"].get("row_hash")
        or fingerprints.get("row_count") != receipt["row_count"]
        or fingerprints.get("code_count") != receipt["code_count"]
        or receipt["detail"].get("readback_rows") != receipt["row_count"]
        or receipt["detail"].get("readback_codes") != receipt["code_count"]
        or not _inventory_valid(inventory)
        or len(inventory) != receipt["code_count"]
        or sum(item["row_count"] for item in inventory.values()) != receipt["row_count"]
    ):
        raise ValueError("minute security publication differs from its completed partition")
    known_at = normalize_decision_at(receipt["finished_at"])
    if known_at < datetime.combine(date.fromisoformat(trade_date), time(15)):
        raise ValueError("minute security publication predates the session close")
    manifest = {
        "schema": SCHEMA,
        "dataset": dataset,
        "trade_date": trade_date,
        "known_at": known_at.isoformat(sep=" ", timespec="seconds"),
        "run_id": receipt["run_id"],
        "source_content_sha256": receipt["content_sha256"],
        "partition_row_hash": fingerprints["row_hash"],
        "security_fingerprints": inventory,
    }
    payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if len(payload.encode("utf-8")) > _MAX_PROOF_BYTES:
        raise ValueError("minute security publication exceeds the proof byte limit")
    digest = canonical_digest(manifest)
    code_set_hash = canonical_digest(sorted(inventory))
    connection.execute(text(f"""
        INSERT INTO {COVERAGE_TABLE} (
            manifest_hash,schema_version,dataset,period,provider,trade_date,
            status,strategy_eligible,run_id,grid_profile,expected_entity_count,entity_count,
            expected_traded_count,actual_traded_count,bar_count,expected_entity_set_hash,
            expected_traded_set_hash,actual_traded_set_hash,no_trade_set_hash,entity_root_hash,
            captured_at,manifest_json,created_at
        )
        SELECT :digest,:schema,:dataset,'1m','qmt_datadir',:day,'EXACT',1,:run_id,:schema,
            :codes,:codes,:codes,:codes,:rows,:code_set,:code_set,:code_set,:empty_set,:root,
            :known_at,:payload,CURRENT_TIMESTAMP
        WHERE NOT EXISTS (SELECT 1 FROM {COVERAGE_TABLE} WHERE manifest_hash=:digest)
    """), {
        "digest": digest, "schema": SCHEMA, "dataset": _DATASETS[dataset], "day": trade_date,
        "run_id": receipt["run_id"], "codes": len(inventory), "rows": receipt["row_count"],
        "code_set": code_set_hash, "empty_set": canonical_digest([]), "root": canonical_digest(inventory),
        "known_at": manifest["known_at"], "payload": payload,
    })
    return digest


def load_verified_minute_security_rows(
    receipt_connection,
    data_connection,
    *,
    dataset: str,
    trade_date: str,
    decision_known_at: datetime | str,
    codes: Sequence[str],
) -> tuple[dict[str, Any], dict[str, list[dict]]]:
    """Open only requested securities from the last available immutable receipt."""
    selected = sorted(set(codes))
    if not 1 <= len(selected) <= _MAX_SELECTED_CODES or any(not _CODE.fullmatch(code) for code in selected):
        raise ValueError("minute security request scope is invalid")
    cutoff = normalize_decision_at(decision_known_at)
    day = date.fromisoformat(trade_date)
    if day > cutoff.date():
        raise ValueError("minute trade date is after the decision cutoff")
    row = receipt_connection.execute(text(f"""
        SELECT /*+ MAX_EXECUTION_TIME(3000) */ manifest_hash,captured_at,
               CASE WHEN LENGTH(manifest_json) <= :proof_bytes THEN manifest_json END AS manifest_json,
               run_id,bar_count,actual_traded_count,entity_root_hash
        FROM {COVERAGE_TABLE}
        WHERE dataset=:dataset AND trade_date=:day AND provider='qmt_datadir'
          AND status='EXACT' AND strategy_eligible=1 AND schema_version=:schema
          AND captured_at<=:cutoff
        ORDER BY captured_at DESC,created_at DESC,manifest_hash DESC LIMIT 1
    """), {
        "dataset": _DATASETS[dataset], "day": trade_date, "schema": SCHEMA,
        "cutoff": cutoff.isoformat(sep=" ", timespec="microseconds"),
        "proof_bytes": _MAX_PROOF_BYTES,
    }).mappings().first()
    if not row:
        return {}, {}
    raw = row["manifest_json"]
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > _MAX_PROOF_BYTES:
        raise ValueError("minute security proof exceeds the byte limit")
    proof = json.loads(raw)
    if not isinstance(proof, dict) or set(proof) != {
        "schema", "dataset", "trade_date", "known_at", "run_id",
        "source_content_sha256", "partition_row_hash", "security_fingerprints",
    }:
        raise ValueError("minute security proof shape differs")
    inventory = proof["security_fingerprints"]
    known_at = normalize_decision_at(proof["known_at"])
    if (
        proof["schema"] != SCHEMA or proof["dataset"] != dataset or proof["trade_date"] != trade_date
        or not proof["run_id"] or proof["run_id"] != row["run_id"]
        or known_at != normalize_decision_at(row["captured_at"])
        or not datetime.combine(day, time(15)) <= known_at <= cutoff
        or not SHA64.fullmatch(str(proof["source_content_sha256"]))
        or not SHA64.fullmatch(str(proof["partition_row_hash"]))
        or not _inventory_valid(inventory)
        or len(inventory) != row["actual_traded_count"]
        or sum(item["row_count"] for item in inventory.values()) != row["bar_count"]
        or canonical_digest(inventory) != row["entity_root_hash"]
        or canonical_digest(proof) != row["manifest_hash"]
    ):
        raise ValueError("minute security publication proof differs")

    requested = [code for code in selected if code in inventory]
    rows_by_code: dict[str, list[dict]] = {}
    for offset in range(0, len(requested), _READ_CODE_BATCH_SIZE):
        batch = requested[offset:offset + _READ_CODE_BATCH_SIZE]
        _, _, rows = persisted_rows(
            data_connection, dataset=dataset, trade_date=trade_date, codes=batch,
            max_rows=sum(inventory[code]["row_count"] for code in batch),
        )
        code_key = "stock_code" if dataset == "stock_minute" else "index_code"
        for item in rows:
            rows_by_code.setdefault(item[code_key], []).append(item)
    return {**proof, "manifest_sha256": row["manifest_hash"]}, {
        code: bars for code, bars in rows_by_code.items()
        if len(bars) == inventory[code]["row_count"] and row_hash(bars) == inventory[code]["row_hash"]
    }
