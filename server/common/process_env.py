# -*- coding: utf-8 -*-
"""Helpers for child-process environment setup.

Keep sensitive runtime values in one place so task runners do not each format
database URLs by hand.
"""
from __future__ import annotations

from contextlib import contextmanager
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Iterable

from sqlalchemy.engine import Engine, make_url

from server.common.adata_release import resolve_adata_source

_PROXY_ENV_NAMES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)


def _looks_like_local_proxy(value: str) -> bool:
    normalized = str(value or "").lower()
    return "127.0.0.1" in normalized or "localhost" in normalized or "[::1]" in normalized


def _strip_dead_local_proxy_env(env: dict[str, str]) -> None:
    if str(env.get("PROBIGA_KEEP_PROXY_ENV") or "").strip() == "1":
        return
    for name in _PROXY_ENV_NAMES:
        value = env.get(name)
        if value and _looks_like_local_proxy(value):
            env.pop(name, None)


def child_process_timeout(default_seconds: int, *, env_name: str = "PROBIGA_CHILD_TIMEOUT_SECONDS") -> int:
    """Resolve a positive subprocess timeout from an environment variable."""
    raw = os.environ.get(env_name) or os.environ.get("PROBIGA_CHILD_TIMEOUT_SECONDS", "")
    try:
        value = int(float(str(raw).strip())) if raw else int(default_seconds)
    except (TypeError, ValueError):
        value = int(default_seconds)
    return max(1, value)


def mask_url(value: str | None) -> str:
    """Return a log-safe database URL."""
    if not value:
        return ""
    try:
        return make_url(value).render_as_string(hide_password=True)
    except Exception:
        return "***"


def engine_url(engine: Engine) -> str:
    """Render an engine URL for child processes.

    The returned value may contain credentials and should only be passed through
    an environment variable, never logged or returned from an API response.
    """
    return engine.url.render_as_string(hide_password=False)


def build_child_env(
    root: str | Path,
    *,
    engine: Engine | None = None,
    mysql_url: str | None = None,
    override_mysql_url: bool = True,
    extra_python_paths: Iterable[str | Path] = (),
) -> dict[str, str]:
    """Build a subprocess environment for ProBigA jobs."""
    root_path = Path(root)
    env = os.environ.copy()
    _strip_dead_local_proxy_env(env)

    resolved_mysql_url = mysql_url or (engine_url(engine) if engine is not None else "")
    if resolved_mysql_url and (override_mysql_url or not env.get("MYSQL_URL")):
        env["MYSQL_URL"] = resolved_mysql_url

    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")

    adata_source = resolve_adata_source(root_path)
    python_paths = [str(adata_source), str(root_path)]
    python_paths.extend(str(Path(p)) for p in extra_python_paths)
    existing = env.get("PYTHONPATH")
    if existing:
        python_paths.extend(part for part in existing.split(os.pathsep) if part)
    mutable_adata = (root_path / "adata").resolve()
    safe_python_paths = []
    for value in python_paths:
        try:
            resolved = Path(value or os.curdir).resolve()
        except OSError:
            continue
        if resolved == mutable_adata and resolved != adata_source:
            continue
        safe_python_paths.append(value)
    env["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(safe_python_paths))
    return env


_MISSING = object()


@contextmanager
def temporary_env(
    overrides: Mapping[str, object | None],
    *,
    overwrite: bool = True,
):
    """Temporarily set environment variables and restore them afterwards."""
    previous: dict[str, str | object] = {}
    changed: list[str] = []
    for key, value in overrides.items():
        if value is None:
            continue
        if not overwrite and key in os.environ:
            continue
        previous[key] = os.environ.get(key, _MISSING)
        os.environ[key] = str(value)
        changed.append(key)
    try:
        yield
    finally:
        for key in reversed(changed):
            old_value = previous[key]
            if old_value is _MISSING:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(old_value)
