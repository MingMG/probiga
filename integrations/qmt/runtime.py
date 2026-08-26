from __future__ import annotations

import os
import sys
from pathlib import Path
from types import ModuleType


DEFAULT_QMT_CONNECTION_PORTS = (
    58610,
    58670,
    58671,
    58672,
    58673,
    58680,
)


def _dedupe(items: list[Path]) -> list[Path]:
    result: list[Path] = []
    seen: set[str] = set()
    for item in items:
        key = str(item).lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def iter_xtquant_import_paths() -> list[Path]:
    paths: list[Path] = []

    raw_site = os.environ.get("QMT_SITE_PACKAGES", "").strip()
    if raw_site:
        paths.append(Path(raw_site))

    raw_xtquant = os.environ.get("XTQUANT_PATH", "").strip()
    if raw_xtquant:
        xt_path = Path(raw_xtquant)
        paths.append(xt_path)
        if xt_path.name.lower() == "xtquant":
            paths.append(xt_path.parent)

    raw_python = os.environ.get("QMT_PYTHON", "").strip()
    if raw_python:
        py_path = Path(raw_python)
        base_dir = py_path.parent if py_path.suffix.lower() in {".exe", ".bat", ".cmd"} else py_path
        paths.extend(
            [
                base_dir,
                base_dir / "Lib" / "site-packages",
                base_dir / "lib" / "site-packages",
                base_dir.parent,
                base_dir.parent / "Lib" / "site-packages",
                base_dir.parent / "lib" / "site-packages",
            ]
        )

    return _dedupe([p for p in paths if str(p).strip()])


def ensure_xtquant_on_path() -> str | None:
    for candidate in iter_xtquant_import_paths():
        site_path = candidate
        if candidate.is_dir() and candidate.name.lower() == "xtquant":
            site_path = candidate.parent

        xt_dir = site_path / "xtquant"
        if xt_dir.is_dir():
            site_str = str(site_path)
            if site_str not in sys.path:
                sys.path.insert(0, site_str)
            return site_str
    return None


def import_xtdata() -> ModuleType:
    try:
        from xtquant import xtdata  # type: ignore[import-untyped]

        return xtdata
    except ImportError:
        ensure_xtquant_on_path()
        try:
            from xtquant import xtdata  # type: ignore[import-untyped]

            return xtdata
        except ImportError as inner_exc:
            searched = [str(p) for p in iter_xtquant_import_paths()]
            raise RuntimeError(
                "xtquant is not importable. Set QMT_SITE_PACKAGES, XTQUANT_PATH, "
                "or QMT_PYTHON to your QMT installation."
                f" searched={searched}"
            ) from inner_exc


def qmt_connection_port_candidates(
    configured_port: str | int | None = None,
) -> list[int]:
    raw = (
        os.environ.get("QMT_PORT", "")
        if configured_port is None
        else str(configured_port)
    ).strip()
    ports: list[int] = []
    if raw:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 0
        if 1 <= value <= 65535:
            ports.append(value)
    for value in DEFAULT_QMT_CONNECTION_PORTS:
        if value not in ports:
            ports.append(value)
    return ports


def connect_xtdata(
    xtdata: ModuleType | object,
    *,
    configured_port: str | int | None = None,
) -> int:
    """Connect to one known desktop service without ambient auto-discovery."""

    connector = getattr(xtdata, "connect", None)
    if not callable(connector):
        raise RuntimeError("xtdata.connect is unavailable")
    last_error: Exception | None = None
    for port in qmt_connection_port_candidates(configured_port):
        try:
            connector(port=port, remember_if_success=False)
            return port
        except Exception as exc:  # native SDK exposes several exception types
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RuntimeError("QMT desktop connection candidates are unavailable")
