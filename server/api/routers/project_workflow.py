"""Read-only engineering acceptance backed by the project workflow ledger."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path as FilePath
from typing import Annotated

from fastapi import APIRouter, HTTPException, Path, Query, Response

from server.project_workflow import store

router = APIRouter(prefix="/project-workflow", tags=["engineering-acceptance"])

_NO_CACHE = {"Cache-Control": "no-store"}


def _ledger_unavailable() -> HTTPException:
    return HTTPException(
        status_code=503,
        detail="编排证据库暂不可用",
        headers=_NO_CACHE,
    )


def _ledger_path() -> FilePath:
    value = os.environ.get("PROBIGA_WORKFLOW_DB", "").strip()
    if not value:
        raise _ledger_unavailable()
    path = FilePath(value)
    try:
        if path.is_absolute() and path.is_file():
            return path
    except OSError:
        pass
    raise _ledger_unavailable()


@router.get("/tasks")
def workflow_tasks(
    response: Response,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> dict:
    try:
        tasks = store.list_tasks(_ledger_path(), limit=limit)
    except (OSError, sqlite3.Error):
        raise _ledger_unavailable() from None
    response.headers.update(_NO_CACHE)
    return {"tasks": tasks}


@router.get("/tasks/{task_id}")
def workflow_task(
    task_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_.:-]{1,96}$")],
    response: Response,
) -> dict:
    try:
        task = store.task_view(_ledger_path(), task_id)
    except (OSError, sqlite3.Error):
        raise _ledger_unavailable() from None
    if task is None:
        raise HTTPException(status_code=404, detail="未找到该任务", headers=_NO_CACHE)
    response.headers.update(_NO_CACHE)
    return task
