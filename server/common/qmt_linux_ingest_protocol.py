"""Authenticated envelopes for the Windows-QMT to Linux ingestion boundary.

The existing Windows worker credential is used only as key material.  The
credential itself is never sent over the network; an HMAC key is derived for
this protocol and cannot be used as the AI worker bearer token.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import time
from typing import Any, Mapping


PROTOCOL = "probiga.qmt-linux-ingest.v1"
PLAN_SCHEMA = "probiga.qmt-linux-ingest-plan.v1"
COMMIT_SCHEMA = "probiga.qmt-linux-ingest-commit.v1"
RESPONSE_SCHEMA = "probiga.qmt-linux-ingest-response.v1"
MAX_PLAN_BYTES = 64 * 1024
MAX_COMMIT_BYTES = 32 * 1024 * 1024
MAX_CLOCK_SKEW_SECONDS = 300

_NONCE_RE = re.compile(r"[0-9a-f]{32}\Z")
_SIGNATURE_RE = re.compile(r"[0-9a-f]{64}\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


class QmtLinuxIngestProtocolError(ValueError):
    """The authenticated ingestion envelope does not match the contract."""


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise QmtLinuxIngestProtocolError("duplicate JSON key")
        value[key] = item
    return value


def decode_object(raw: bytes, *, limit: int) -> dict[str, Any]:
    if not raw or len(raw) > int(limit):
        raise QmtLinuxIngestProtocolError("ingestion body size differs")
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=lambda token: (_ for _ in ()).throw(
                QmtLinuxIngestProtocolError(f"nonfinite JSON token: {token}")
            ),
        )
    except QmtLinuxIngestProtocolError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QmtLinuxIngestProtocolError("ingestion body is not canonical JSON") from exc
    if not isinstance(value, dict):
        raise QmtLinuxIngestProtocolError("ingestion body must be an object")
    return value


def validate_sha256(value: object, *, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if _SHA256_RE.fullmatch(normalized) is None:
        raise QmtLinuxIngestProtocolError(f"{field} must be SHA-256")
    return normalized


def _derived_key(secret: str) -> bytes:
    value = str(secret or "").encode("utf-8")
    if len(value) < 24:
        raise QmtLinuxIngestProtocolError("QMT ingestion machine credential is unavailable")
    return hmac.new(value, (PROTOCOL + ":key").encode("ascii"), hashlib.sha256).digest()


def request_signature(
    secret: str,
    payload: Mapping[str, Any],
    *,
    timestamp: int,
    nonce: str,
) -> str:
    nonce_value = str(nonce or "").strip().lower()
    if _NONCE_RE.fullmatch(nonce_value) is None:
        raise QmtLinuxIngestProtocolError("QMT ingestion nonce differs")
    message = (
        f"{PROTOCOL}:request\n{int(timestamp)}\n{nonce_value}\n"
        f"{canonical_sha256(dict(payload))}"
    ).encode("ascii")
    return hmac.new(_derived_key(secret), message, hashlib.sha256).hexdigest()


def new_request_headers(
    secret: str,
    payload: Mapping[str, Any],
    *,
    now: int | None = None,
    nonce: str | None = None,
) -> dict[str, str]:
    timestamp = int(time.time() if now is None else now)
    nonce_value = str(nonce or secrets.token_hex(16)).lower()
    return {
        "X-ProBigA-QMT-Ingest-Time": str(timestamp),
        "X-ProBigA-QMT-Ingest-Nonce": nonce_value,
        "X-ProBigA-QMT-Ingest-Signature": request_signature(
            secret,
            payload,
            timestamp=timestamp,
            nonce=nonce_value,
        ),
    }


def verify_request_headers(
    secret: str,
    payload: Mapping[str, Any],
    *,
    timestamp: object,
    nonce: object,
    signature: object,
    now: int | None = None,
) -> None:
    try:
        timestamp_value = int(str(timestamp or ""))
    except ValueError as exc:
        raise QmtLinuxIngestProtocolError("QMT ingestion timestamp differs") from exc
    current = int(time.time() if now is None else now)
    if abs(current - timestamp_value) > MAX_CLOCK_SKEW_SECONDS:
        raise QmtLinuxIngestProtocolError("QMT ingestion timestamp expired")
    nonce_value = str(nonce or "").strip().lower()
    supplied = str(signature or "").strip().lower()
    if _NONCE_RE.fullmatch(nonce_value) is None or _SIGNATURE_RE.fullmatch(supplied) is None:
        raise QmtLinuxIngestProtocolError("QMT ingestion authentication differs")
    expected = request_signature(
        secret,
        payload,
        timestamp=timestamp_value,
        nonce=nonce_value,
    )
    if not hmac.compare_digest(supplied, expected):
        raise QmtLinuxIngestProtocolError("QMT ingestion authentication differs")


def signed_response(secret: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    core = dict(payload)
    core["schema"] = RESPONSE_SCHEMA
    proof = hmac.new(
        _derived_key(secret),
        (PROTOCOL + ":response\n" + canonical_sha256(core)).encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    return {**core, "proof": proof}


def verify_signed_response(secret: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(payload)
    supplied = str(value.pop("proof", "")).strip().lower()
    if value.get("schema") != RESPONSE_SCHEMA or _SIGNATURE_RE.fullmatch(supplied) is None:
        raise QmtLinuxIngestProtocolError("QMT ingestion response proof differs")
    expected = hmac.new(
        _derived_key(secret),
        (PROTOCOL + ":response\n" + canonical_sha256(value)).encode("ascii"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(supplied, expected):
        raise QmtLinuxIngestProtocolError("QMT ingestion response proof differs")
    return value
