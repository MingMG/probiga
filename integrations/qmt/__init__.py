"""Guojin QMT integration package.

The dedicated QMT SDK runtime intentionally contains fewer dependencies than
the main application runtime.  Keep package imports lazy so the persistent
gateway can load the native SDK without importing SQLAlchemy or API modules.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "QmtBackend",
    "ensure_xtquant_on_path",
    "from_qmt_symbol",
    "from_qmt_index_symbol",
    "import_xtdata",
    "is_configured",
    "iter_xtquant_import_paths",
    "to_qmt_index_symbol",
    "to_qmt_symbol",
]


def __getattr__(name: str) -> Any:
    if name in {"QmtBackend", "from_qmt_symbol", "is_configured", "to_qmt_symbol"}:
        from integrations.qmt import backend

        return getattr(backend, name)
    if name in {"from_qmt_index_symbol", "to_qmt_index_symbol"}:
        from integrations.qmt import info

        return getattr(info, name)
    if name in {"ensure_xtquant_on_path", "import_xtdata", "iter_xtquant_import_paths"}:
        from integrations.qmt import runtime

        return getattr(runtime, name)
    raise AttributeError(name)
