"""Early, streaming request-size guard for external strategy evidence."""
from __future__ import annotations

import re
from typing import Any, Awaitable, Callable

from starlette.responses import JSONResponse


STRATEGY_EVIDENCE_REQUEST_MAX_BYTES = 52 * 1024 * 1024
STRATEGY_GOVERNANCE_REQUEST_MAX_BYTES = 1 * 1024 * 1024
_CHALLENGER_EVIDENCE_PATH = re.compile(
    r"^/api/strategy-center/challengers/[0-9a-fA-F]{32}/evidence$"
)


class StrategyEvidenceRequestTooLarge(ValueError):
    """Raised while streaming a protected request beyond its hard limit."""


class StrategyGovernanceRequestTooLarge(ValueError):
    """Raised before parsing an oversized ordinary governance write."""


def is_strategy_evidence_submission(scope: dict[str, Any]) -> bool:
    if scope.get("type") != "http" or scope.get("method") != "POST":
        return False
    path = str(scope.get("path") or "")
    return bool(
        path == "/api/strategy-center/metrics"
        or _CHALLENGER_EVIDENCE_PATH.fullmatch(path)
    )


def is_strategy_governance_write(scope: dict[str, Any]) -> bool:
    if scope.get("type") != "http" or scope.get("method") not in {
        "POST", "PUT", "PATCH", "DELETE",
    }:
        return False
    path = str(scope.get("path") or "")
    return bool(
        path.startswith("/api/strategy-center/")
        and not is_strategy_evidence_submission(scope)
    )


def strategy_evidence_too_large_response() -> JSONResponse:
    return JSONResponse(
        status_code=413,
        content={
            "status": "error",
            "error": "strategy_evidence_request_too_large",
            "message": "外部策略证据请求体不得超过52 MiB",
            "maximum_request_bytes": STRATEGY_EVIDENCE_REQUEST_MAX_BYTES,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )


def strategy_governance_too_large_response() -> JSONResponse:
    return JSONResponse(
        status_code=413,
        content={
            "status": "error",
            "error": "strategy_governance_request_too_large",
            "message": "普通策略治理请求体不得超过1 MiB",
            "maximum_request_bytes": STRATEGY_GOVERNANCE_REQUEST_MAX_BYTES,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )


class StrategyEvidenceRequestSizeMiddleware:
    """Bound evidence bodies before FastAPI/Pydantic materializes their JSON."""

    def __init__(
        self, app: Callable[..., Awaitable[None]], *,
        max_bytes: int = STRATEGY_EVIDENCE_REQUEST_MAX_BYTES,
        governance_max_bytes: int = STRATEGY_GOVERNANCE_REQUEST_MAX_BYTES,
    ) -> None:
        self.app = app
        self.max_bytes = int(max_bytes)
        self.governance_max_bytes = int(governance_max_bytes)
        if self.max_bytes < 1 or self.governance_max_bytes < 1:
            raise ValueError("策略治理请求体上限必须大于0")

    async def __call__(self, scope, receive, send) -> None:
        evidence_submission = is_strategy_evidence_submission(scope)
        governance_write = is_strategy_governance_write(scope)
        if not evidence_submission and not governance_write:
            await self.app(scope, receive, send)
            return
        request_max_bytes = (
            self.max_bytes if evidence_submission
            else self.governance_max_bytes
        )
        too_large_response = (
            strategy_evidence_too_large_response
            if evidence_submission
            else strategy_governance_too_large_response
        )
        header_map = {
            bytes(key).lower(): bytes(value)
            for key, value in scope.get("headers", [])
        }
        raw_length = header_map.get(b"content-length")
        if raw_length is not None:
            try:
                declared_length = int(raw_length.decode("ascii"))
            except (UnicodeError, ValueError):
                declared_length = -1
            if declared_length < 0:
                response = JSONResponse(
                    status_code=400,
                    content={
                        "status": "error",
                        "error": "invalid_content_length",
                        "message": "证据请求Content-Length无效",
                        "automatic_real_order_submission": False,
                        "real_order_authority": False,
                    },
                )
                await response(scope, receive, send)
                return
            if declared_length > request_max_bytes:
                response = too_large_response()
                await response(scope, receive, send)
                return

        consumed = 0

        async def bounded_receive():
            nonlocal consumed
            message = await receive()
            if message.get("type") == "http.request":
                body = message.get("body", b"")
                consumed += len(body) if isinstance(body, bytes) else 0
                if consumed > request_max_bytes:
                    exception_type = (
                        StrategyEvidenceRequestTooLarge
                        if evidence_submission
                        else StrategyGovernanceRequestTooLarge
                    )
                    raise exception_type("策略治理请求体超过硬上限")
            return message

        await self.app(scope, bounded_receive, send)


__all__ = [
    "STRATEGY_EVIDENCE_REQUEST_MAX_BYTES",
    "STRATEGY_GOVERNANCE_REQUEST_MAX_BYTES",
    "StrategyEvidenceRequestSizeMiddleware",
    "StrategyEvidenceRequestTooLarge",
    "StrategyGovernanceRequestTooLarge",
    "is_strategy_evidence_submission",
    "is_strategy_governance_write",
    "strategy_evidence_too_large_response",
    "strategy_governance_too_large_response",
]
