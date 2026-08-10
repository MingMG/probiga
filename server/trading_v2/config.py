"""Versioned configuration helpers for the V2 trading core."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def canonical_json_hash(payload: Any) -> str:
    raw = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def load_frozen_json(relative_path: str) -> tuple[dict[str, Any], str]:
    path = PROJECT_ROOT / relative_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload, canonical_json_hash(payload)
