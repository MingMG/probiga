"""Canonical serialization and deterministic hashing for V4 contracts.

The implementation deliberately uses only the Python standard library.  It
normalizes timestamps to UTC, mappings by key, enums by value, and decimals to
a non-exponent string representation before hashing.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any


def require_aware(value: datetime, field_name: str) -> datetime:
    """Reject ambiguous wall-clock timestamps at a decision boundary."""

    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def canonical_datetime(value: datetime) -> str:
    require_aware(value, "datetime")
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decimal_text(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("non-finite Decimal values are not hashable")
    if value == 0:
        return "0"
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def canonical_primitive(value: Any) -> Any:
    """Convert supported contract values into canonical JSON primitives."""

    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: canonical_primitive(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, Enum):
        return canonical_primitive(value.value)
    if isinstance(value, datetime):
        return canonical_datetime(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return _decimal_text(value)
    if isinstance(value, Mapping):
        converted: dict[str, Any] = {}
        for key, item_value in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical mappings require string keys")
            converted[key] = canonical_primitive(item_value)
        return {key: converted[key] for key in sorted(converted)}
    if isinstance(value, (tuple, list)):
        return [canonical_primitive(item) for item in value]
    if isinstance(value, (set, frozenset)):
        raise TypeError("unordered collections are not valid contract values")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float values are not hashable")
        return 0.0 if value == 0 else value
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"unsupported contract value: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        canonical_primitive(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def deterministic_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def deterministic_id(prefix: str, value: Any, *, length: int = 24) -> str:
    if not prefix or not prefix.replace("_", "").isalnum():
        raise ValueError("prefix must be a non-empty alphanumeric identifier")
    if length < 12 or length > 64:
        raise ValueError("length must be between 12 and 64")
    return f"{prefix}_{deterministic_hash(value)[:length]}"


def freeze(value: Any) -> Any:
    """Recursively freeze JSON-like values without changing their meaning."""

    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for key, item_value in value.items():
            if not isinstance(key, str):
                raise TypeError("contract mappings require string keys")
            frozen[key] = freeze(item_value)
        return MappingProxyType({key: frozen[key] for key in sorted(frozen)})
    if isinstance(value, (tuple, list)):
        return tuple(freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        raise TypeError("unordered collections are not valid contract values")
    if is_dataclass(value) and not isinstance(value, type):
        parameters = getattr(type(value), "__dataclass_params__", None)
        is_owned_frozen_contract = (
            parameters is not None
            and parameters.frozen
            and type(value).__module__
            == "server.trading_v4.domain.contracts"
        )
        if not is_owned_frozen_contract:
            raise TypeError(
                "nested dataclasses must be frozen V4 domain contracts"
            )
    canonical_primitive(value)
    return value


class ContractMixin:
    """JSON-safe projection shared by immutable domain contracts."""

    def as_dict(self) -> dict[str, Any]:
        payload = canonical_primitive(self)
        if not isinstance(payload, dict):
            raise TypeError("contract projection must be a mapping")
        return payload

    def canonical_json(self) -> str:
        return canonical_json(self)
