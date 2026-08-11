# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from server.api.routers._engine import get_engine
from server.auth.service import (
    AuthError,
    IssuedSession,
    authenticate,
    registration_state,
    registration_window_open,
    register_first_admin,
    resolve_session,
    revoke_session,
    rotate_session,
)
from server.common.config import get_account_auth_config, get_admin_auth_config

router = APIRouter(prefix="/auth", tags=["auth"])
logger = logging.getLogger(__name__)

SESSION_COOKIE = "probiga_session"


def _production_mode() -> bool:
    return os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == "production"


def _authentication_required() -> bool:
    """Mirror the middleware's fail-closed authentication boundary."""
    return _production_mode() or bool(get_admin_auth_config().get("enabled"))


class Credentials(BaseModel):
    username: str
    password: str


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()[:64]
    return str(request.client.host if request.client else "")[:64]


def _user_agent(request: Request) -> str:
    return request.headers.get("User-Agent", "")[:255]


def _session_token(request: Request) -> str:
    return request.cookies.get(SESSION_COOKIE, "").strip()


def _cookie_secure(request: Request) -> bool:
    config = get_account_auth_config()
    configured = config.get("cookie_secure")
    if configured is not None:
        return bool(configured)
    forwarded = request.headers.get("X-Forwarded-Proto", "").split(",", 1)[0].strip().lower()
    return forwarded == "https" or request.url.scheme == "https"


def _set_session_cookie(response: JSONResponse, request: Request, issued: IssuedSession) -> None:
    config = get_account_auth_config()
    response.set_cookie(
        key=SESSION_COOKIE,
        value=issued.token,
        max_age=int(config["session_hours"]) * 3600,
        httponly=True,
        secure=_cookie_secure(request),
        samesite="strict",
        path="/",
    )
    response.headers["Cache-Control"] = "no-store"


def _clear_session_cookie(response: JSONResponse, request: Request) -> None:
    response.delete_cookie(
        key=SESSION_COOKIE,
        path="/",
        httponly=True,
        secure=_cookie_secure(request),
        samesite="strict",
    )
    response.headers["Cache-Control"] = "no-store"


def _error_response(exc: AuthError, request: Request | None = None) -> JSONResponse:
    response = JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "error": exc.code, "message": exc.message},
    )
    if exc.retry_after:
        response.headers["Retry-After"] = str(exc.retry_after)
    if exc.status_code == 401:
        response.headers["WWW-Authenticate"] = "Bearer"
    response.headers["Cache-Control"] = "no-store"
    if request is not None and exc.code == "session_expired":
        _clear_session_cookie(response, request)
    return response
def _success_payload(issued: IssuedSession) -> dict:
    return {"status": "ok", "authenticated": True, **issued.identity.as_dict()}


@router.get("/status")
def auth_status(request: Request):
    required = _authentication_required()
    try:
        state = registration_state(get_engine())
        identity = resolve_session(get_engine(), _session_token(request))
    except Exception:
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "error": "auth_backend_unavailable",
                "message": "登录服务暂时不可用，请稍后重试。",
                "required": required,
                "authenticated": False,
            },
            headers={"Cache-Control": "no-store"},
        )
    config = get_account_auth_config()
    deadline = config.get("registration_deadline")
    window_open = registration_window_open(
        str(deadline or ""),
        require_explicit=_production_mode(),
    )
    state["registration_open"] = bool(state["registration_open"]) and window_open
    payload = {
        "status": "ok",
        **state,
        "required": required,
        "authenticated": identity is not None,
        "session_hours": int(config["session_hours"]),
        "registration_deadline": deadline,
    }
    if identity is not None:
        payload.update(identity.as_dict())
    response = JSONResponse(payload)
    response.headers["Cache-Control"] = "no-store"
    return response


@router.post("/register")
def register(payload: Credentials, request: Request):
    config = get_account_auth_config()
    if not registration_window_open(
        str(config.get("registration_deadline") or ""),
        require_explicit=_production_mode(),
    ):
        return _error_response(
            AuthError(
                "registration_window_closed",
                "首个管理员注册窗口已关闭，请联系系统维护者重新开启。",
                status_code=403,
            )
        )
    try:
        issued = register_first_admin(
            get_engine(),
            username=payload.username,
            password=payload.password,
            session_hours=int(config["session_hours"]),
            refresh_after_hours=int(config["refresh_after_hours"]),
            client_ip=_client_ip(request),
            user_agent=_user_agent(request),
        )
    except AuthError as exc:
        return _error_response(exc)
    response = JSONResponse(status_code=201, content=_success_payload(issued))
    _set_session_cookie(response, request, issued)
    return response


@router.post("/login")
def login(payload: Credentials, request: Request):
    config = get_account_auth_config()
    try:
        issued = authenticate(
            get_engine(),
            username=payload.username,
            password=payload.password,
            session_hours=int(config["session_hours"]),
            refresh_after_hours=int(config["refresh_after_hours"]),
            max_failures=int(config["max_failures"]),
            lock_minutes=int(config["lock_minutes"]),
            client_ip=_client_ip(request),
            user_agent=_user_agent(request),
        )
    except AuthError as exc:
        return _error_response(exc)
    response = JSONResponse(_success_payload(issued))
    _set_session_cookie(response, request, issued)
    return response


@router.post("/refresh")
def refresh(request: Request):
    config = get_account_auth_config()
    try:
        issued = rotate_session(
            get_engine(),
            token=_session_token(request),
            session_hours=int(config["session_hours"]),
            refresh_after_hours=int(config["refresh_after_hours"]),
            client_ip=_client_ip(request),
            user_agent=_user_agent(request),
        )
    except AuthError as exc:
        return _error_response(exc, request)
    response = JSONResponse(_success_payload(issued))
    _set_session_cookie(response, request, issued)
    return response


@router.post("/logout")
def logout(request: Request):
    token = _session_token(request)
    if token:
        try:
            revoke_session(get_engine(), token, client_ip=_client_ip(request))
        except Exception as exc:
            # Cookie removal must still work if the database is temporarily down.
            logger.warning("Failed to revoke account session during logout: %s", exc)
    response = JSONResponse({"status": "ok", "authenticated": False})
    _clear_session_cookie(response, request)
    return response


@router.get("/me")
def me(request: Request):
    try:
        identity = resolve_session(get_engine(), _session_token(request))
    except Exception:
        identity = None
    if identity is None:
        return _error_response(
            AuthError("session_expired", "登录已过期，请重新登录。", status_code=401),
            request,
        )
    response = JSONResponse({"status": "ok", "authenticated": True, **identity.as_dict()})
    response.headers["Cache-Control"] = "no-store"
    return response
