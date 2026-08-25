"""Strict canonical-JSON validation for hash- and database-bound inputs."""
from __future__ import annotations

import json
import math
from typing import Any


CANONICAL_JSON_MAX_BYTES = 1 * 1024 * 1024
CANONICAL_JSON_MAX_DEPTH = 32


def validate_canonical_json(
    value: Any,
    *,
    label: str = "治理JSON",
    max_bytes: int = CANONICAL_JSON_MAX_BYTES,
    max_depth: int = CANONICAL_JSON_MAX_DEPTH,
) -> Any:
    """Return a detached JSON value or reject noncanonical/oversized input."""

    ancestors: set[int] = set()

    def normalize(item: Any, depth: int) -> Any:
        if depth > int(max_depth):
            raise ValueError(f"{label}嵌套深度不能超过{max_depth}层")
        if item is None or isinstance(item, (bool, str)):
            return item
        if type(item) is int:
            return item
        if type(item) is float:
            if not math.isfinite(item):
                raise ValueError(f"{label}数值必须是有限数")
            return item
        if isinstance(item, list):
            identity = id(item)
            if identity in ancestors:
                raise ValueError(f"{label}不能包含循环引用")
            ancestors.add(identity)
            try:
                return [normalize(member, depth + 1) for member in item]
            finally:
                ancestors.remove(identity)
        if isinstance(item, dict):
            if any(type(key) is not str for key in item):
                raise ValueError(f"{label}对象键必须是字符串")
            identity = id(item)
            if identity in ancestors:
                raise ValueError(f"{label}不能包含循环引用")
            ancestors.add(identity)
            try:
                return {
                    key: normalize(member, depth + 1)
                    for key, member in item.items()
                }
            finally:
                ancestors.remove(identity)
        raise ValueError(f"{label}包含不可序列化类型{type(item).__name__}")

    normalized = normalize(value, 0)
    try:
        serialized = json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (UnicodeEncodeError, ValueError) as exc:
        raise ValueError(f"{label}不是有效UTF-8规范JSON") from exc
    if len(serialized) > int(max_bytes):
        raise ValueError(f"{label}规范序列化不得超过{max_bytes}字节")
    return normalized


__all__ = [
    "CANONICAL_JSON_MAX_BYTES",
    "CANONICAL_JSON_MAX_DEPTH",
    "validate_canonical_json",
]
