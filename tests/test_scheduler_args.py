from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from server.api import scheduler_runtime
from server.common.scheduler_args import build_scheduler_task_args


def test_concept_scheduler_args_bind_the_authoritative_closed_session():
    row = {
        "task_type": "eastmoney_concept_current",
        "script_args": "--dataset current --json",
        "date_param": "",
        "_trigger_source": "scheduled",
    }
    assert build_scheduler_task_args(
        row,
        "tools/sync_eastmoney_concept_market.py",
        "2026-10-26",
    ) == [
        "--dataset",
        "current",
        "--json",
        "--trade-date",
        "2026-10-26",
    ]

    with pytest.raises(ValueError, match="provider date differs"):
        build_scheduler_task_args(
            {
                **row,
                "script_args": "--dataset current --json --trade-date 2026-10-27",
            },
            "tools/sync_eastmoney_concept_market.py",
            "2026-10-26",
        )


def test_ordinary_concept_dispatch_uses_the_closed_session_before_market_open(
    monkeypatch,
):
    observed = {}

    def closed_target(engine, *, now, task_type):
        observed.update(
            {
                "engine": engine,
                "now": now,
                "task_type": task_type,
            }
        )
        return "2026-10-26"

    monkeypatch.setattr(
        scheduler_runtime,
        "_release_catchup_closed_target_date",
        closed_target,
    )
    engine = object()
    now = datetime(
        2026,
        10,
        27,
        7,
        30,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    assert scheduler_runtime._task_dispatch_date(
        {
            "task_type": "eastmoney_concept_current",
            "_trigger_source": "scheduled",
        },
        engine,
        now=now,
    ) == "2026-10-26"
    assert observed == {
        "engine": engine,
        "now": now,
        "task_type": "eastmoney_concept_current",
    }
