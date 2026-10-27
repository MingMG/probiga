# -*- coding: utf-8 -*-
"""Single-operator request identity.

The project has one operator.  When authentication is disabled the request is
still given a stable operator identity so governance and personal records do not
need a separate account system.  When a token is configured, the same identity
is issued after a constant-time token check.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
import secrets

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from server.common.config import get_admin_auth_config


ADMIN_TOKEN_HEADER = "X-ProBigA-Admin-Token"
ADMIN_BEARER_SCHEME = "Bearer"
OPERATOR_NAME = "operator"
OPERATOR_ROLE = "ADMIN"

PUBLIC_API_PREFIXES = (
    "/api/auth",
    "/api/health",
    "/api/ai-bridge/worker",
    "/api/qmt-ingest",
    "/api/briefing-tools",
)
PUBLIC_PATHS = {
    "/login",
    "/favicon.ico",
}
PROTECTED_PAGE_PATHS = {
    "/",
    "/intraday-battle",
    "/battle",
    "/deploy",
    "/market-radar",
    "/ai-stock",
    "/ai-general",
    "/trading-v2",
    "/trading-v3",
    "/docs",
    "/redoc",
    "/openapi.json",
}


@dataclass(frozen=True)
class OperatorIdentity:
    id: int = 1
    username: str = OPERATOR_NAME
    role: str = OPERATOR_ROLE
    is_active: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "username": self.username,
            "role": self.role,
            "is_active": self.is_active,
        }


def _prefix_match(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix + "/")


def is_admin_protected_path(path: str, method: str) -> bool:
    del method
    normalized_path = path or "/"
    if any(_prefix_match(normalized_path, prefix) for prefix in PUBLIC_API_PREFIXES):
        return False
    if normalized_path in PUBLIC_PATHS:
        return False
    if normalized_path.startswith("/static/"):
        return normalized_path.lower().endswith((".html", ".htm"))
    if normalized_path.startswith("/api/"):
        return True
    return normalized_path in PROTECTED_PAGE_PATHS


def _production_mode() -> bool:
    return os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production"


def admin_auth_status() -> dict[str, object]:
    config = get_admin_auth_config()
    enabled = bool(config.get("enabled"))
    token_configured = bool(config.get("token"))
    return {
        "enabled": enabled,
        "required": enabled,
        "token_configured": token_configured,
        "ready": (not enabled) or token_configured,
        "production_mode": _production_mode(),
        "operator": OperatorIdentity().as_dict(),
        "accepted_credentials": [
            ADMIN_TOKEN_HEADER,
            f"Authorization: {ADMIN_BEARER_SCHEME} <token>",
        ]
        if enabled
        else [],
        "protected_scope": "all application pages and /api/* except /api/auth and /api/health",
    }


def _request_header_token(request: Request) -> str:
    header_token = request.headers.get(ADMIN_TOKEN_HEADER, "").strip()
    if header_token:
        return header_token
    authorization = request.headers.get("Authorization", "").strip()
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() == ADMIN_BEARER_SCHEME.lower() and token.strip():
        return token.strip()
    return ""


def _set_operator(request: Request, auth_kind: str) -> None:
    request.state.auth_kind = auth_kind
    request.state.auth_user = OperatorIdentity()


def validate_admin_request(request: Request) -> Response | None:
    if not is_admin_protected_path(request.url.path, request.method):
        return None

    config = get_admin_auth_config()
    enabled = bool(config.get("enabled"))
    if not enabled:
        _set_operator(request, "disabled")
        return None

    expected = str(config.get("token") or "")
    if not expected:
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "error": "admin_token_missing",
                "message": "PROBIGA_ADMIN_TOKEN is required when authentication is enabled.",
            },
            headers={"Cache-Control": "no-store"},
        )

    supplied = _request_header_token(request)
    if supplied and secrets.compare_digest(supplied, expected):
        _set_operator(request, "operator_token")
        return None

    return JSONResponse(
        status_code=401,
        content={
            "status": "error",
            "error": "admin_token_required",
            "message": "A valid operator token is required.",
        },
        headers={
            "WWW-Authenticate": "Bearer",
            "Cache-Control": "no-store",
        },
    )


__all__ = [
    "ADMIN_BEARER_SCHEME",
    "ADMIN_TOKEN_HEADER",
    "OperatorIdentity",
    "admin_auth_status",
    "is_admin_protected_path",
    "validate_admin_request",
]
