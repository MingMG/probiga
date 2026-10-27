# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


TOOL_SCHEMA_VERSION = "probiga.a-share-briefing-tools.v1"
TOOL_DATA_STATUSES = frozenset({"AVAILABLE", "PARTIAL", "NO_DATA"})


@dataclass(frozen=True)
class ToolResult:
    """One bounded tool response before request metadata is attached."""

    data: list[dict[str, Any]]
    as_of: str | None
    data_status: str
    missing_fields: tuple[str, ...] = ()
    meta: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.data_status not in TOOL_DATA_STATUSES:
            raise ValueError(f"unsupported tool data status: {self.data_status}")


__all__ = [
    "TOOL_DATA_STATUSES",
    "TOOL_SCHEMA_VERSION",
    "ToolResult",
]
