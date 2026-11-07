from __future__ import annotations

from datetime import datetime
from functools import partial
import json

from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from server.api import scheduler_runtime
from server.common.authoritative_market_clock import authoritative_closed_trade_date
from server.common.scheduler_validation import scheduler_output_status
from tools import sync_north_flow_daily


def test_north_flow_records_non_disclosure_without_publishing_repeated_ths_values(monkeypatch, capsys):
    engine = create_engine("sqlite+pysqlite://", poolclass=StaticPool)
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES ('2026-11-05',1),('2026-11-06',1)"))
        connection.execute(text("CREATE TABLE st_north_flow_daily (trade_date TEXT, net_tgt INTEGER)"))
        connection.execute(text("INSERT INTO st_north_flow_daily VALUES ('2026-11-05',37047000000)"))
    monkeypatch.setattr(sync_north_flow_daily, "load_project_env", lambda: None)
    monkeypatch.setattr(sync_north_flow_daily, "create_tool_engine", lambda **kwargs: engine)
    monkeypatch.setattr(engine, "dispose", lambda: None)
    now = datetime(2026, 11, 7, 15, 35)
    monkeypatch.setattr(
        sync_north_flow_daily, "authoritative_closed_trade_date",
        partial(authoritative_closed_trade_date, now=now),
    )
    task = {
        "task_type": "north_flow_daily",
        "script_path": "tools/sync_north_flow_daily.py",
        "script_args": "--date latest --apply --json",
    }
    dispatch_date = scheduler_runtime._task_dispatch_date(task, engine, now=now)
    argument_row = scheduler_runtime._task_argument_row(task, now=now, target_date=dispatch_date)
    args = scheduler_runtime._build_task_args(argument_row, task["script_path"], dispatch_date)

    assert dispatch_date == "2026-11-06"
    assert args == ["--date", "2026-11-06", "--apply", "--json"]
    assert sync_north_flow_daily.main(args) == 0
    output = capsys.readouterr().out
    assert json.loads(output) == {
        "database_writes": False,
        "reason_code": "NORTHBOUND_NET_FLOW_NOT_DISCLOSED",
        "schema": "probiga.north-flow-daily-result.v1",
        "status": "NOT_DISCLOSED",
        "target_date": "2026-11-06",
    }
    with engine.connect() as connection:
        assert connection.execute(text("SELECT * FROM st_north_flow_daily")).all() == [
            ("2026-11-05", 37_047_000_000),
        ]
    assert scheduler_output_status(argument_row, output, return_code=0) == "degraded"
    assert scheduler_output_status(argument_row, output.replace("false", "true"), return_code=0) == "failed"
    assert scheduler_output_status(argument_row, output.replace("2026-11-06", "2026-11-05"), return_code=0) == "failed"
    assert scheduler_output_status(argument_row, output, return_code=2) == "failed"
