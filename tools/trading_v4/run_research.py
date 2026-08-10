#!/usr/bin/env python3
"""Run the independent, forward-only Trading V4 research observation.

The input contains explicit timestamps and source records.  The command never
reads the business database, the wall clock, V2/V3 strategy output or an order
gateway.  When ``--output`` is used, it is restricted to the V4 release
artifact namespace and created exclusively so an earlier run cannot be
overwritten.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
import json
from pathlib import Path
import sys
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from server.trading_v4.application import run_forward_research_observation
from server.trading_v4.domain import (
    AsOfDataset,
    AsOfRecord,
    DecisionClock,
    QualityStatus,
    canonical_json,
    deterministic_hash,
)
from tools.trading_v4.release_integrity import (
    V4ReleaseIntegrityError,
    validate_v4_release,
)


INPUT_SCHEMA_VERSION = "probiga.trading-v4.forward-research-input.v1"
RELEASE_ID = "trading_v4.1.0-research"
ARTIFACT_ROOT = (
    REPOSITORY_ROOT
    / "artifacts"
    / "trading_v4"
    / "releases"
    / RELEASE_ID
)


class ResearchInputError(ValueError):
    """Raised when the explicit input document is incomplete or ambiguous."""


def load_request(path: Path) -> Mapping[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ResearchInputError(f"cannot read input: {path}") from exc
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ResearchInputError(f"duplicate JSON key: {key}")
            result[key] = item
        return result

    def reject_constant(value: str) -> None:
        raise ResearchInputError(f"input contains non-finite JSON value: {value}")

    try:
        value = json.loads(
            raw,
            parse_float=Decimal,
            parse_constant=reject_constant,
            object_pairs_hook=no_duplicates,
        )
    except ResearchInputError:
        raise
    except (json.JSONDecodeError, ValueError) as exc:
        raise ResearchInputError("input is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise ResearchInputError("input root must be a JSON object")
    if value.get("schema_version") != INPUT_SCHEMA_VERSION:
        raise ResearchInputError(
            f"schema_version must be {INPUT_SCHEMA_VERSION!r}"
        )
    allowed = {
        "schema_version",
        "dataset_name",
        "knowledge_cutoff",
        "decision_time",
        "valid_until",
        "decision_clock",
        "instruments",
        "records",
        "dataset_quality_status",
    }
    if set(value) - allowed:
        raise ResearchInputError(
            f"input contains unsupported fields: {tuple(sorted(set(value) - allowed))}"
        )
    _reject_nonfinite_tree(value, "input")
    return value


def build_dataset(document: Mapping[str, Any]) -> AsOfDataset:
    records_value = document.get("records")
    if not isinstance(records_value, list) or not records_value:
        raise ResearchInputError("records must be a non-empty JSON array")
    records = tuple(_record(item, index) for index, item in enumerate(records_value))
    return AsOfDataset(
        dataset_name=_required_text(document, "dataset_name"),
        as_of=_timestamp(document.get("knowledge_cutoff"), "knowledge_cutoff"),
        records=records,
        quality_status=QualityStatus(document.get("dataset_quality_status", "PASS")),
    )


def run_document(document: Mapping[str, Any]) -> Mapping[str, Any]:
    release = _verified_release_identity()
    dataset = build_dataset(document)
    instruments_value = document.get("instruments")
    if not isinstance(instruments_value, list) or not instruments_value:
        raise ResearchInputError("instruments must be a non-empty JSON array")
    instruments = tuple(
        _required_list_text(item, "instruments item")
        for item in instruments_value
    )
    decision_clock = DecisionClock(document.get("decision_clock"))
    if decision_clock != DecisionClock.AFTER_CLOSE:
        raise ResearchInputError("V4.1 daily-bar research requires AFTER_CLOSE")
    bundle = run_forward_research_observation(
        dataset,
        instruments=instruments,
        decision_time=_timestamp(document.get("decision_time"), "decision_time"),
        valid_until=_timestamp(document.get("valid_until"), "valid_until"),
        decision_clock=decision_clock,
        code_commit_sha=release["code_commit"],
        config_hash=release["config_sha256"],
    )
    request_hash = deterministic_hash(document)
    return {
        "schema_version": "probiga.trading-v4.forward-research-result.v1",
        "system_version": "4.1.0-research",
        "release_id": RELEASE_ID,
        "lifecycle_status": "RESEARCH_ONLY",
        "request_hash": request_hash,
        "source_tree_sha256": release["source_tree_sha256"],
        "manifest_sha256": release["manifest_sha256"],
        "manifest_integrity_status": release["manifest_integrity_status"],
        "config_sha256": release["config_sha256"],
        "code_commit": release["code_commit"],
        "code_commit_clean": release["code_commit_clean"],
        "dataset_id": dataset.dataset_id,
        "data_manifest_hash": bundle.decision_input.context.raw_data_manifest_hash,
        "result_hash": bundle.result_hash,
        "execution_boundary": {
            "forecasts_emitted": False,
            "actions_emitted": False,
            "execution_intents_emitted": False,
            "paper_orders_allowed": False,
            "real_orders_allowed": False,
        },
        "bundle": bundle.as_dict(),
    }


def _verified_release_identity() -> Mapping[str, Any]:
    """Run the complete release validator before claiming release identity."""
    try:
        integrity = validate_v4_release()
    except (OSError, V4ReleaseIntegrityError) as exc:
        raise ResearchInputError(f"V4 release integrity failed: {exc}") from exc
    document = integrity.document
    return {
        "source_tree_sha256": integrity.source_tree_sha256,
        "manifest_sha256": integrity.manifest_sha256,
        "manifest_integrity_status": integrity.status,
        "config_sha256": document["config_sha256"],
        "code_commit": document["code_commit"],
        "code_commit_clean": document["code_commit_clean"],
    }


def write_result(document: Mapping[str, Any], output: Path | None) -> None:
    rendered = canonical_json(document) + "\n"
    if output is None:
        sys.stdout.write(rendered)
        return
    resolved = output.resolve()
    root = ARTIFACT_ROOT.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ResearchInputError(
            f"output must stay under the V4 artifact namespace: {root}"
        ) from exc
    if resolved.suffix.lower() != ".json":
        raise ResearchInputError("output must use a .json suffix")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    try:
        with resolved.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(rendered)
    except FileExistsError as exc:
        raise ResearchInputError(
            f"refusing to overwrite existing V4 artifact: {resolved}"
        ) from exc


def _record(value: Any, index: int) -> AsOfRecord:
    if not isinstance(value, Mapping):
        raise ResearchInputError(f"records[{index}] must be a JSON object")
    allowed = {
        "record_id",
        "source",
        "knowledge_time",
        "ingested_at",
        "payload",
        "event_time",
        "source_published_at",
        "first_seen_at",
        "received_at",
        "revised_at",
        "revision_id",
        "quality_status",
    }
    if set(value) - allowed:
        raise ResearchInputError(
            f"records[{index}] contains unsupported fields: "
            f"{tuple(sorted(set(value) - allowed))}"
        )
    payload = value.get("payload")
    if not isinstance(payload, Mapping):
        raise ResearchInputError(f"records[{index}].payload must be an object")
    return AsOfRecord(
        record_id=_required_text(value, "record_id"),
        source=_required_text(value, "source"),
        knowledge_time=_timestamp(value.get("knowledge_time"), "knowledge_time"),
        ingested_at=_timestamp(value.get("ingested_at"), "ingested_at"),
        payload=payload,
        event_time=_optional_timestamp(value.get("event_time"), "event_time"),
        source_published_at=_optional_timestamp(
            value.get("source_published_at"),
            "source_published_at",
        ),
        first_seen_at=_optional_timestamp(
            value.get("first_seen_at"),
            "first_seen_at",
        ),
        received_at=_optional_timestamp(value.get("received_at"), "received_at"),
        revised_at=_optional_timestamp(value.get("revised_at"), "revised_at"),
        revision_id=str(value.get("revision_id") or ""),
        quality_status=QualityStatus(value.get("quality_status", "PASS")),
    )


def _required_text(value: Mapping[str, Any], name: str) -> str:
    return _required_list_text(value.get(name), name)


def _required_list_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ResearchInputError(f"{name} must be non-empty text")
    if value != value.strip():
        raise ResearchInputError(f"{name} must be canonical text")
    return value


def _reject_nonfinite_tree(value: Any, path: str) -> None:
    if isinstance(value, Decimal) and not value.is_finite():
        raise ResearchInputError(f"{path} contains a non-finite decimal")
    if isinstance(value, Mapping):
        for key, item in value.items():
            _reject_nonfinite_tree(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _reject_nonfinite_tree(item, f"{path}[{index}]")


def _timestamp(value: Any, name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ResearchInputError(f"{name} must be an ISO-8601 timestamp")
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ResearchInputError(f"{name} is not a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ResearchInputError(f"{name} must include a timezone offset")
    return parsed


def _optional_timestamp(value: Any, name: str) -> datetime | None:
    if value is None:
        return None
    return _timestamp(value, name)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the non-actionable Trading V4 forward research kernel."
    )
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Optional exclusive-create JSON path below "
            f"{ARTIFACT_ROOT}"
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        request = load_request(args.input)
        result = run_document(request)
        write_result(result, args.output)
    except (ResearchInputError, TypeError, ValueError) as exc:
        print(f"trading_v4_research=BLOCKED error={exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
