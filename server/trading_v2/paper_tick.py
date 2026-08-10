"""V2 paper tick compatibility entrypoint."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.engine import Engine

from .execution import run_execution_tick


def run_paper_tick(
    engine: Engine,
    *,
    now: datetime | None = None,
    account_id: str = "paper-main-v2",
) -> dict[str, Any]:
    return run_execution_tick(
        engine,
        now=now,
        account_id=account_id,
    )
