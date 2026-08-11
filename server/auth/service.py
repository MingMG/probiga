# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, delete, func, insert, or_, select, update
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from server.auth.schema import (
    auth_audit,
    auth_bootstrap,
    auth_session,
    auth_user,
    ensure_auth_schema,
)

PASSWORD_SCHEME = "pbkdf2_sha256"
PASSWORD_ITERATIONS = 600_000
SESSION_TOKEN_BYTES = 32
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_.\-\u4e00-\u9fff]+$")


class AuthError(RuntimeError):
    def __init__(self, code: str, message: str, *, status_code: int = 400, retry_after: int | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.retry_after = retry_after


@dataclass(frozen=True)
class AuthUser:
    id: int
    username: str
    role: str
    is_active: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "username": self.username,
            "role": self.role,
            "is_active": self.is_active,
        }


@dataclass(frozen=True)
class SessionIdentity:
    session_id: int
    user: AuthUser
    issued_at: datetime
    refresh_after: datetime
    expires_at: datetime

    def as_dict(self) -> dict[str, Any]:
        return {
            "user": self.user.as_dict(),
            "issued_at": _iso(self.issued_at),
            "refresh_after": _iso(self.refresh_after),
            "expires_at": _iso(self.expires_at),
        }


@dataclass(frozen=True)
class IssuedSession:
    token: str
    identity: SessionIdentity


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _iso(value: datetime) -> str:
    return value.replace(microsecond=0).isoformat() + "Z"


def normalize_username(username: str) -> tuple[str, str]:
    display = str(username or "").strip()
    if not 3 <= len(display) <= 32:
        raise AuthError("invalid_username", "账号长度需为 3–32 个字符。")
    if not USERNAME_PATTERN.fullmatch(display):
        raise AuthError("invalid_username", "账号只能包含中文、字母、数字、点、横线或下划线。")
    return display, display.casefold()


def validate_password(password: str, *, username: str = "") -> str:
    value = str(password or "")
    if not 10 <= len(value) <= 128:
        raise AuthError("weak_password", "密码长度需为 10–128 个字符。")
    if username and value.casefold() == username.casefold():
        raise AuthError("weak_password", "密码不能与账号相同。")
    return value


def hash_password(password: str, *, iterations: int = PASSWORD_ITERATIONS) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"{PASSWORD_SCHEME}${iterations}${salt.hex()}${digest.hex()}"


def verify_password(password: str, encoded: str) -> bool:
    try:
        scheme, iterations_text, salt_hex, digest_hex = str(encoded).split("$", 3)
        if scheme != PASSWORD_SCHEME:
            return False
        iterations = int(iterations_text)
        if iterations < 100_000 or iterations > 2_000_000:
            return False
        expected = bytes.fromhex(digest_hex)
        actual = hashlib.pbkdf2_hmac(
            "sha256",
            str(password or "").encode("utf-8"),
            bytes.fromhex(salt_hex),
            iterations,
        )
        return hmac.compare_digest(actual, expected)
    except (TypeError, ValueError):
        return False


def hash_session_token(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def registration_state(engine: Engine) -> dict[str, Any]:
    ensure_auth_schema(engine)
    with engine.connect() as conn:
        user_count = int(conn.execute(select(func.count()).select_from(auth_user)).scalar_one())
        row = conn.execute(
            select(auth_bootstrap.c.registration_open, auth_bootstrap.c.claimed_user_id)
            .where(auth_bootstrap.c.id == 1)
        ).mappings().one()
    registration_open = bool(row["registration_open"]) and user_count == 0
    return {
        "registration_open": registration_open,
        "user_initialized": user_count > 0,
        "user_count": user_count,
    }


def registration_window_open(
    deadline: str | None,
    *,
    now: datetime | None = None,
    require_explicit: bool = False,
) -> bool:
    """Return whether the first-account registration window is open.

    Development keeps the historical no-deadline bootstrap behavior.  A
    production caller can require an explicit deadline so an omitted setting
    fails closed instead of exposing an unlimited first-admin claim window.
    """
    raw = str(deadline or "").strip()
    if not raw:
        return not require_explicit
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc) <= parsed.astimezone(timezone.utc)


def _audit(
    conn: Connection,
    *,
    event_type: str,
    success: bool,
    username: str = "",
    user_id: int | None = None,
    client_ip: str = "",
    detail: str = "",
) -> None:
    conn.execute(
        insert(auth_audit).values(
            user_id=user_id,
            username=str(username or "")[:64],
            event_type=event_type[:40],
            success=bool(success),
            client_ip=str(client_ip or "")[:64],
            detail=str(detail or "")[:1000],
            created_at=utcnow(),
        )
    )


def _session_values(
    *,
    user_id: int,
    token: str,
    now: datetime,
    session_hours: int,
    refresh_after_hours: int,
    client_ip: str,
    user_agent: str,
) -> dict[str, Any]:
    expires_at = now + timedelta(hours=session_hours)
    refresh_after = now + timedelta(hours=min(refresh_after_hours, session_hours))
    return {
        "user_id": user_id,
        "token_hash": hash_session_token(token),
        "issued_at": now,
        "refresh_after": refresh_after,
        "expires_at": expires_at,
        "last_seen_at": now,
        "revoked_at": None,
        "client_ip": str(client_ip or "")[:64],
        "user_agent": str(user_agent or "")[:255],
    }


def _issue_session_in_connection(
    conn: Connection,
    *,
    user: AuthUser,
    session_hours: int,
    refresh_after_hours: int,
    client_ip: str,
    user_agent: str,
) -> IssuedSession:
    now = utcnow()
    token = secrets.token_urlsafe(SESSION_TOKEN_BYTES)
    values = _session_values(
        user_id=user.id,
        token=token,
        now=now,
        session_hours=session_hours,
        refresh_after_hours=refresh_after_hours,
        client_ip=client_ip,
        user_agent=user_agent,
    )
    result = conn.execute(insert(auth_session).values(**values))
    session_id = int(result.inserted_primary_key[0])
    identity = SessionIdentity(
        session_id=session_id,
        user=user,
        issued_at=values["issued_at"],
        refresh_after=values["refresh_after"],
        expires_at=values["expires_at"],
    )
    return IssuedSession(token=token, identity=identity)


def register_first_admin(
    engine: Engine,
    *,
    username: str,
    password: str,
    session_hours: int,
    refresh_after_hours: int,
    client_ip: str = "",
    user_agent: str = "",
) -> IssuedSession:
    ensure_auth_schema(engine)
    display, normalized = normalize_username(username)
    secret = validate_password(password, username=display)
    password_hash = hash_password(secret)
    now = utcnow()
    try:
        with engine.begin() as conn:
            bootstrap = conn.execute(
                select(auth_bootstrap)
                .where(auth_bootstrap.c.id == 1)
                .with_for_update()
            ).mappings().one()
            user_count = int(conn.execute(select(func.count()).select_from(auth_user)).scalar_one())
            if not bool(bootstrap["registration_open"]) or user_count > 0:
                _audit(
                    conn,
                    event_type="REGISTER_REJECTED",
                    success=False,
                    username=display,
                    client_ip=client_ip,
                    detail="initial registration already closed",
                )
                raise AuthError("registration_closed", "首个管理员账号已创建，注册入口已经关闭。", status_code=409)
            result = conn.execute(
                insert(auth_user).values(
                    username=display,
                    username_norm=normalized,
                    password_hash=password_hash,
                    role="ADMIN",
                    is_active=True,
                    failed_login_count=0,
                    locked_until=None,
                    password_changed_at=now,
                    last_login_at=now,
                    created_at=now,
                    updated_at=now,
                )
            )
            user_id = int(result.inserted_primary_key[0])
            conn.execute(
                update(auth_bootstrap)
                .where(auth_bootstrap.c.id == 1)
                .values(registration_open=False, claimed_user_id=user_id, updated_at=now)
            )
            user = AuthUser(id=user_id, username=display, role="ADMIN", is_active=True)
            issued = _issue_session_in_connection(
                conn,
                user=user,
                session_hours=session_hours,
                refresh_after_hours=refresh_after_hours,
                client_ip=client_ip,
                user_agent=user_agent,
            )
            _audit(
                conn,
                event_type="REGISTER",
                success=True,
                username=display,
                user_id=user_id,
                client_ip=client_ip,
                detail="first admin account created",
            )
            return issued
    except IntegrityError as exc:
        raise AuthError("registration_conflict", "账号初始化发生并发冲突，请刷新页面后重试。", status_code=409) from exc


def authenticate(
    engine: Engine,
    *,
    username: str,
    password: str,
    session_hours: int,
    refresh_after_hours: int,
    max_failures: int,
    lock_minutes: int,
    client_ip: str = "",
    user_agent: str = "",
) -> IssuedSession:
    ensure_auth_schema(engine)
    try:
        display, normalized = normalize_username(username)
    except AuthError:
        display, normalized = str(username or "").strip()[:64], "\0"
    now = utcnow()
    with engine.begin() as conn:
        row = conn.execute(
            select(auth_user)
            .where(auth_user.c.username_norm == normalized)
            .with_for_update()
        ).mappings().first()
        valid = bool(
            row
            and row["is_active"]
            and verify_password(password, str(row["password_hash"]))
        )
        if not valid:
            if row:
                failures = int(row["failed_login_count"] or 0) + 1
                locked_until = row["locked_until"]
                if failures >= max_failures:
                    failures = 0
                    locked_until = now + timedelta(minutes=lock_minutes)
                conn.execute(
                    update(auth_user)
                    .where(auth_user.c.id == row["id"])
                    .values(failed_login_count=failures, locked_until=locked_until, updated_at=now)
                )
            _audit(
                conn,
                event_type="LOGIN",
                success=False,
                username=display,
                user_id=int(row["id"]) if row else None,
                client_ip=client_ip,
                detail="invalid credentials",
            )
            raise AuthError("invalid_credentials", "账号或密码不正确。", status_code=401)

        locked_until = row["locked_until"]
        if isinstance(locked_until, datetime) and locked_until > now:
            retry_after = max(1, int((locked_until - now).total_seconds()))
            _audit(
                conn,
                event_type="LOGIN_LOCKED",
                success=False,
                username=str(row["username"]),
                user_id=int(row["id"]),
                client_ip=client_ip,
                detail=f"retry_after={retry_after}",
            )
            raise AuthError(
                "account_temporarily_locked",
                "登录失败次数过多，请稍后再试。",
                status_code=429,
                retry_after=retry_after,
            )

        user = AuthUser(
            id=int(row["id"]),
            username=str(row["username"]),
            role=str(row["role"]),
            is_active=bool(row["is_active"]),
        )
        conn.execute(
            update(auth_user)
            .where(auth_user.c.id == user.id)
            .values(failed_login_count=0, locked_until=None, last_login_at=now, updated_at=now)
        )
        conn.execute(
            update(auth_session)
            .where(
                and_(
                    auth_session.c.user_id == user.id,
                    auth_session.c.revoked_at.is_(None),
                    auth_session.c.expires_at <= now,
                )
            )
            .values(revoked_at=now)
        )
        issued = _issue_session_in_connection(
            conn,
            user=user,
            session_hours=session_hours,
            refresh_after_hours=refresh_after_hours,
            client_ip=client_ip,
            user_agent=user_agent,
        )
        _audit(
            conn,
            event_type="LOGIN",
            success=True,
            username=user.username,
            user_id=user.id,
            client_ip=client_ip,
            detail="password login",
        )
        return issued


def resolve_session(engine: Engine, token: str, *, touch: bool = True) -> SessionIdentity | None:
    raw = str(token or "").strip()
    if not raw:
        return None
    ensure_auth_schema(engine)
    now = utcnow()
    token_hash = hash_session_token(raw)
    with engine.begin() as conn:
        row = conn.execute(
            select(
                auth_session.c.id.label("session_id"),
                auth_session.c.issued_at,
                auth_session.c.refresh_after,
                auth_session.c.expires_at,
                auth_session.c.last_seen_at,
                auth_session.c.revoked_at,
                auth_user.c.id.label("user_id"),
                auth_user.c.username,
                auth_user.c.role,
                auth_user.c.is_active,
            )
            .select_from(auth_session.join(auth_user, auth_session.c.user_id == auth_user.c.id))
            .where(auth_session.c.token_hash == token_hash)
        ).mappings().first()
        if not row or row["revoked_at"] is not None or not bool(row["is_active"]):
            return None
        if row["expires_at"] <= now:
            conn.execute(
                update(auth_session)
                .where(auth_session.c.id == row["session_id"])
                .values(revoked_at=now)
            )
            return None
        if touch and (not row["last_seen_at"] or row["last_seen_at"] <= now - timedelta(minutes=5)):
            conn.execute(
                update(auth_session)
                .where(auth_session.c.id == row["session_id"])
                .values(last_seen_at=now)
            )
        return SessionIdentity(
            session_id=int(row["session_id"]),
            user=AuthUser(
                id=int(row["user_id"]),
                username=str(row["username"]),
                role=str(row["role"]),
                is_active=bool(row["is_active"]),
            ),
            issued_at=row["issued_at"],
            refresh_after=row["refresh_after"],
            expires_at=row["expires_at"],
        )


def rotate_session(
    engine: Engine,
    *,
    token: str,
    session_hours: int,
    refresh_after_hours: int,
    client_ip: str = "",
    user_agent: str = "",
) -> IssuedSession:
    identity = resolve_session(engine, token, touch=False)
    if identity is None:
        raise AuthError("session_expired", "登录已过期，请重新登录。", status_code=401)
    now = utcnow()
    with engine.begin() as conn:
        updated = conn.execute(
            update(auth_session)
            .where(
                and_(
                    auth_session.c.id == identity.session_id,
                    auth_session.c.revoked_at.is_(None),
                    auth_session.c.expires_at > now,
                )
            )
            .values(revoked_at=now)
        )
        if updated.rowcount != 1:
            raise AuthError("session_expired", "登录已过期，请重新登录。", status_code=401)
        issued = _issue_session_in_connection(
            conn,
            user=identity.user,
            session_hours=session_hours,
            refresh_after_hours=refresh_after_hours,
            client_ip=client_ip,
            user_agent=user_agent,
        )
        _audit(
            conn,
            event_type="SESSION_REFRESH",
            success=True,
            username=identity.user.username,
            user_id=identity.user.id,
            client_ip=client_ip,
            detail="session token rotated",
        )
        return issued


def revoke_session(engine: Engine, token: str, *, client_ip: str = "") -> bool:
    raw = str(token or "").strip()
    if not raw:
        return False
    ensure_auth_schema(engine)
    now = utcnow()
    with engine.begin() as conn:
        row = conn.execute(
            select(auth_session.c.id, auth_session.c.user_id, auth_user.c.username)
            .select_from(auth_session.join(auth_user, auth_session.c.user_id == auth_user.c.id))
            .where(auth_session.c.token_hash == hash_session_token(raw))
        ).mappings().first()
        if not row:
            return False
        updated = conn.execute(
            update(auth_session)
            .where(and_(auth_session.c.id == row["id"], auth_session.c.revoked_at.is_(None)))
            .values(revoked_at=now)
        )
        if updated.rowcount:
            _audit(
                conn,
                event_type="LOGOUT",
                success=True,
                username=str(row["username"]),
                user_id=int(row["user_id"]),
                client_ip=client_ip,
                detail="session revoked",
            )
        return bool(updated.rowcount)


def prune_auth_data(engine: Engine, *, session_retention_days: int = 30, audit_retention_days: int = 180) -> dict[str, int]:
    ensure_auth_schema(engine)
    now = utcnow()
    with engine.begin() as conn:
        sessions = conn.execute(
            delete(auth_session).where(
                or_(
                    auth_session.c.expires_at < now - timedelta(days=session_retention_days),
                    auth_session.c.revoked_at < now - timedelta(days=session_retention_days),
                )
            )
        ).rowcount
        audits = conn.execute(
            delete(auth_audit).where(auth_audit.c.created_at < now - timedelta(days=audit_retention_days))
        ).rowcount
    return {"sessions_deleted": int(sessions or 0), "audits_deleted": int(audits or 0)}

