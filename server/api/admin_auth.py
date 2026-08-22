# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import re
import secrets
from urllib.parse import urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from server.api.routers._engine import get_engine
from server.auth.service import registration_state, registration_window_open, resolve_session
from server.common.config import get_account_auth_config, get_admin_auth_config

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
ADMIN_TOKEN_HEADER = "X-ProBigA-Admin-Token"
ADMIN_BEARER_SCHEME = "Bearer"
SESSION_COOKIE = "probiga_session"

ADMIN_ACCOUNT_ROLE = "ADMIN"
EVIDENCE_REVIEWER_ROLE = "EVIDENCE_REVIEWER"
REVIEWER_READ_PREFIXES = ("/api/strategy-center",)
REVIEWER_PAGE_PATHS = {"/"}
REVIEWER_REVIEW_PATH = re.compile(
    r"^/api/strategy-center/metrics/[0-9a-f]{32}/review$"
)

PUBLIC_API_PREFIXES = (
    "/api/auth",
    "/api/health",
    "/api/ai-bridge/worker",
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

# Kept in the security status payload for compatibility with earlier acceptance
# tooling. Account login now protects every non-health API, not only this subset.
ADMIN_READ_PREFIXES = (
    "/api/deploy",
    "/api/scheduler",
    "/api/datasource",
    "/api/commentary",
    "/api/jq/minute",
    "/api/notify",
)


def _prefix_match(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix + "/")


def is_admin_protected_path(path: str, method: str) -> bool:
    normalized_path = path or "/"
    if _prefix_match(normalized_path, "/api/auth/users"):
        return True
    if any(_prefix_match(normalized_path, prefix) for prefix in PUBLIC_API_PREFIXES):
        return False
    if normalized_path in PUBLIC_PATHS:
        return False
    if normalized_path.startswith("/static/"):
        return normalized_path.lower().endswith((".html", ".htm"))
    if normalized_path.startswith("/api/"):
        return True
    if normalized_path in PROTECTED_PAGE_PATHS:
        return True
    return False


def _production_mode() -> bool:
    return os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production"


def admin_auth_status() -> dict[str, object]:
    config = get_admin_auth_config()
    account_config = get_account_auth_config()
    enabled = bool(config.get("enabled"))
    token_configured = bool(config.get("token"))
    production_mode = _production_mode()
    deadline = str(account_config.get("registration_deadline") or "").strip()
    account_state: dict[str, object]
    try:
        account_state = registration_state(get_engine())
        account_state["registration_open"] = bool(
            account_state["registration_open"]
        ) and registration_window_open(
            deadline,
            require_explicit=production_mode,
        )
        account_backend_ready = True
    except Exception as exc:
        account_state = {
            "registration_open": False,
            "user_initialized": False,
            "user_count": 0,
            "error": type(exc).__name__,
        }
        account_backend_ready = False
    account_state["registration_deadline"] = deadline or None
    user_initialized = bool(account_state.get("user_initialized"))
    credential_ready = token_configured or (
        account_backend_ready and user_initialized
    )
    registration_open = bool(account_state.get("registration_open"))
    registration_window_bounded = not registration_open or bool(deadline)
    bootstrap_safe = not production_mode or registration_window_bounded
    return {
        "enabled": enabled,
        "token_configured": token_configured,
        "account_backend_ready": account_backend_ready,
        **account_state,
        # Before the first user is registered the system is still fail-closed;
        # the legacy token remains available for non-browser operations.
        "credential_ready": credential_ready,
        "registration_deadline_configured": bool(deadline),
        "registration_window_bounded": registration_window_bounded,
        "production_bootstrap_safe": bootstrap_safe,
        "ready": enabled and credential_ready and bootstrap_safe,
        "session_hours": int(account_config["session_hours"]),
        "refresh_after_hours": int(account_config["refresh_after_hours"]),
        "safe_methods": sorted(SAFE_METHODS),
        "protected_read_prefixes": list(ADMIN_READ_PREFIXES),
        "protected_scope": "all application pages and /api/* except /api/auth and /api/health",
        "protected_mutation_scope": "/api/* non-safe methods",
        "accepted_credentials": [
            f"HttpOnly cookie: {SESSION_COOKIE}",
            ADMIN_TOKEN_HEADER,
            f"Authorization: {ADMIN_BEARER_SCHEME} <token>",
        ],
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


def _wants_html(request: Request) -> bool:
    if request.method.upper() not in {"GET", "HEAD"}:
        return False
    path = request.url.path
    if path in PROTECTED_PAGE_PATHS or path.lower().endswith((".html", ".htm")):
        return True
    return "text/html" in request.headers.get("Accept", "").lower()


def _login_redirect(request: Request) -> RedirectResponse:
    next_path = request.url.path
    if request.url.query:
        next_path += "?" + request.url.query
    from urllib.parse import quote

    return RedirectResponse(
        url="/login?next=" + quote(next_path, safe="/"),
        status_code=303,
        headers={"Cache-Control": "no-store"},
    )
def _admin_auth_response(status_code: int, error: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"status": "error", "error": error, "message": message, "login_url": "/login"},
        headers={
            "WWW-Authenticate": "Bearer",
            "X-ProBigA-Admin-Auth": "required",
            "Cache-Control": "no-store",
        },
    )


def _origin_allowed(request: Request) -> bool:
    """Reject cross-site cookie mutations in addition to SameSite=Strict."""
    if request.method.upper() in SAFE_METHODS:
        return True
    origin = request.headers.get("Origin", "").strip()
    if not origin:
        return True
    try:
        parsed = urlsplit(origin)
        origin_host = (parsed.hostname or "").lower()
        origin_port = parsed.port
        request_host = (request.url.hostname or "").lower()
        request_port = request.url.port
        return (
            parsed.scheme.lower() == request.url.scheme.lower()
            and origin_host == request_host
            and origin_port == request_port
        )
    except ValueError:
        return False


def _reviewer_request_allowed(path: str, method: str) -> bool:
    """Limit independent reviewers to governance evidence inspection/review."""

    normalized_path = path or "/"
    normalized_method = method.upper()
    if normalized_method in SAFE_METHODS:
        return normalized_path in REVIEWER_PAGE_PATHS or any(
            _prefix_match(normalized_path, prefix)
            for prefix in REVIEWER_READ_PREFIXES
        )
    return normalized_method == "POST" and bool(
        REVIEWER_REVIEW_PATH.fullmatch(normalized_path)
    )


def validate_admin_request(request: Request) -> Response | None:
    if not is_admin_protected_path(request.url.path, request.method):
        return None

    config = get_admin_auth_config()
    enabled = bool(config.get("enabled"))
    if not enabled:
        if _production_mode():
            return _admin_auth_response(
                503,
                "admin_auth_not_ready",
                "Production administrative authentication is not ready.",
            )
        request.state.auth_kind = "disabled"
        return None
    if _production_mode() and admin_auth_status().get("ready") is not True:
        return _admin_auth_response(
            503,
            "admin_auth_not_ready",
            "Production administrative authentication is not ready.",
        )

    expected = str(config.get("token") or "")
    supplied = _request_header_token(request)
    if supplied and expected and secrets.compare_digest(supplied, expected):
        request.state.auth_kind = "legacy_token"
        return None

    session_token = request.cookies.get(SESSION_COOKIE, "").strip()
    if session_token:
        try:
            identity = resolve_session(get_engine(), session_token)
        except Exception:
            return _admin_auth_response(
                503,
                "auth_backend_unavailable",
                "登录服务暂时不可用，请稍后重试。",
            )
        if identity is not None:
            if not _origin_allowed(request):
                return _admin_auth_response(
                    403,
                    "cross_site_request_blocked",
                    "已拒绝跨站操作请求。",
                )
            user = identity.user
            role = str(getattr(user, "role", "") or "").strip().upper()
            if getattr(user, "is_active", False) is not True:
                return _admin_auth_response(
                    403,
                    "account_inactive",
                    "当前账户已停用。",
                )
            if role != ADMIN_ACCOUNT_ROLE and not (
                role == EVIDENCE_REVIEWER_ROLE
                and _reviewer_request_allowed(request.url.path, request.method)
            ):
                return _admin_auth_response(
                    403,
                    "account_role_forbidden",
                    "当前账户角色无权访问此接口。",
                )
            request.state.auth_kind = "account_session"
            request.state.auth_user = user
            request.state.auth_session = identity
            return None

    if _wants_html(request):
        return _login_redirect(request)
    return _admin_auth_response(
        401,
        "admin_auth_required",
        "请先登录后再访问 ProBigA。",
    )
