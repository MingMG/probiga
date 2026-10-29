#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Verify the daily data close and send an actionable WeCom receipt."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.wecom.delivery import deliver_markdown
from server.api.data_monitor import BY_KEY, Observer
from server.common.batch_db import create_batch_engine
from server.common.config import get_wecom_webhook
from server.common.kline_data import get_kline_engine
from server.common.minute_data import get_minute_engine
from tools.data_quality_check import expected_completed_trade_date

SHANGHAI = ZoneInfo("Asia/Shanghai")
CLOSEOUT_SCHEMA = "probiga.daily-data-closeout.v1"
MAX_REPAIR_LAUNCHES = 4
BAD_DATASET_STATUSES = frozenset({"missing", "partial", "unknown"})

TASK_LABELS = {
    "news_sync": "多源财经快讯",
    "notice_eastmoney": "东财公告",
    "stock_finance": "股票财务 PIT",
    "north_flow_daily": "北向资金",
    "sector_heat_east": "东财板块热度",
    "stock_dividend_eastmoney": "分红事件",
    "qmt_index_current": "指数实时快照",
    "qmt_index_minute": "指数分钟线",
    "eastmoney_concept_current": "概念实时快照",
    "eastmoney_concept_minute": "概念分钟线",
    "eastmoney_concept_flow_snapshot": "概念资金流",
}

RETRY_PRIORITY = {
    "qmt_stock_daily_canonical": 0,
    "qmt_stock_minute_canonical": 1,
    "capital_flow_batch_fast": 2,
    "qmt_index_kline": 3,
    "qmt_index_minute": 3,
    "eastmoney_concept_current": 4,
    "eastmoney_concept_kline": 4,
    "eastmoney_concept_minute": 4,
    "eastmoney_concept_flow_snapshot": 4,
    "news_sync": 5,
    "notice_eastmoney": 6,
    "stock_finance": 7,
    "north_flow_daily": 8,
    "sector_heat_east": 9,
    "alist_daily": 10,
    "alist_info": 11,
    "hot_pop_east": 12,
    "hot_rank_ths": 12,
}


@dataclass(frozen=True)
class Problem:
    name: str
    status: str
    detail: str
    task_type: str = ""

    def as_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "status": self.status,
            "detail": self.detail,
            "task_type": self.task_type,
        }


def _now() -> datetime:
    return datetime.now(SHANGHAI).replace(tzinfo=None, microsecond=0)


def _task_rows(engine: Engine, task_types: set[str]) -> dict[str, dict[str, Any]]:
    if not task_types:
        return {}
    from sqlalchemy import bindparam

    statement = text(
        "SELECT * FROM st_scheduled_tasks "
        "WHERE task_type IN :task_types ORDER BY id"
    ).bindparams(bindparam("task_types", expanding=True))
    with engine.connect() as connection:
        rows = [
            dict(row)
            for row in connection.execute(
                statement,
                {"task_types": sorted(task_types)},
            ).mappings()
        ]
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        task_type = str(row.get("task_type") or "")
        if task_type in result:
            raise RuntimeError(f"duplicate scheduler task type: {task_type}")
        result[task_type] = row
    return result


def _task_problem(
    task_type: str,
    row: Mapping[str, Any],
    *,
    target_date: str,
) -> Problem | None:
    if int(row.get("enabled") or 0) != 1:
        return None
    status = str(row.get("last_run_status") or "").strip().lower()
    last_run_at = row.get("last_run_at")
    run_date = (
        last_run_at.date().isoformat()
        if isinstance(last_run_at, datetime)
        else str(last_run_at or "")[:10]
    )
    label = TASK_LABELS.get(
        task_type,
        str(row.get("task_name") or task_type),
    )
    if status == "running" and run_date >= target_date:
        return Problem(label, "waiting", f"{run_date or '-'} 运行中", task_type)
    if status in {"success", "degraded"} and run_date >= target_date:
        return None
    if not run_date:
        detail = "今日任务尚未运行"
    elif run_date < target_date:
        detail = f"最新运行日期 {run_date}，目标 {target_date}"
    else:
        detail = f"{run_date} 运行状态 {status or 'unknown'}"
    return Problem(label, "missing", detail, task_type)


def _dataset_problem(key: str, cell: Mapping[str, Any]) -> Problem | None:
    status = str(cell.get("status") or "unknown").lower()
    if status not in BAD_DATASET_STATUSES:
        return None
    spec = BY_KEY[key]
    expected = cell.get("expected_count")
    actual = cell.get("actual_count")
    coverage = cell.get("coverage_ratio")
    parts = [str(cell.get("reason") or status)]
    if expected is not None or actual is not None:
        parts.append(f"覆盖 {actual if actual is not None else '-'}/{expected if expected is not None else '-'}")
    if coverage is not None:
        parts.append(f"{float(coverage) * 100:.2f}%")
    return Problem(
        spec.name,
        status,
        "；".join(parts),
        spec.task_type,
    )


def collect_closeout(
    engine: Engine,
    *,
    target_date: str,
    now: datetime,
    observer: Observer | None = None,
) -> dict[str, Any]:
    monitor = observer or Observer(
        business=engine,
        kline=get_kline_engine(),
        minute=get_minute_engine(),
    )
    cells = monitor.inspect_day(
        target_date,
        now,
        missing_limit=5,
    )
    problems = [
        problem
        for key, cell in cells.items()
        if (problem := _dataset_problem(key, cell)) is not None
    ]
    task_types = set(TASK_LABELS)
    task_rows = _task_rows(engine, task_types)
    for task_type in sorted(task_types):
        row = task_rows.get(task_type)
        if row is None:
            problems.append(
                Problem(
                    TASK_LABELS[task_type],
                    "missing",
                    "调度任务缺失",
                    task_type,
                )
            )
            continue
        problem = _task_problem(
            task_type,
            row,
            target_date=target_date,
        )
        if problem is not None:
            problems.append(problem)
    deduplicated: dict[tuple[str, str], Problem] = {}
    for problem in problems:
        deduplicated[(problem.task_type, problem.name)] = problem
    ordered = sorted(
        deduplicated.values(),
        key=lambda item: (
            RETRY_PRIORITY.get(item.task_type, 99),
            item.name,
        ),
    )
    failed = [item for item in ordered if item.status != "waiting"]
    waiting = [item for item in ordered if item.status == "waiting"]
    return {
        "schema": CLOSEOUT_SCHEMA,
        "status": "PASS" if not failed and not waiting else "WARN",
        "target_date": target_date,
        "observed_at": now.isoformat(timespec="seconds"),
        "datasets": cells,
        "problems": [item.as_dict() for item in ordered],
        "failed_count": len(failed),
        "waiting_count": len(waiting),
    }


def launch_repairs(
    engine: Engine,
    report: Mapping[str, Any],
    *,
    max_launches: int = MAX_REPAIR_LAUNCHES,
) -> list[dict[str, Any]]:
    from server.api.scheduler_runtime import launch_scheduler_task

    task_types = {
        item["task_type"]
        for item in report.get("problems", [])
        if item.get("status") != "waiting" and item.get("task_type")
    }
    rows = _task_rows(engine, task_types)
    attempts: list[dict[str, Any]] = []
    for task_type in sorted(
        task_types,
        key=lambda value: (RETRY_PRIORITY.get(value, 99), value),
    ):
        if len(attempts) >= max(0, int(max_launches)):
            break
        row = rows.get(task_type)
        if row is None or int(row.get("enabled") or 0) != 1:
            attempts.append(
                {
                    "task_type": task_type,
                    "accepted": False,
                    "status": "disabled_or_missing",
                }
            )
            continue
        result = launch_scheduler_task(
            row,
            root=ROOT,
            engine=engine,
        )
        attempts.append(
            {
                "task_type": task_type,
                "task_name": str(row.get("task_name") or task_type),
                "accepted": bool(result.get("accepted")),
                "status": str(result.get("status") or ""),
            }
        )
    return attempts


def render_message(
    report: Mapping[str, Any],
    *,
    mode: str,
    attempts: list[dict[str, Any]],
) -> str:
    target = str(report["target_date"])
    status = "数据闭环完成" if int(report["failed_count"]) == 0 and int(report["waiting_count"]) == 0 else "数据闭环仍有缺口"
    mode_label = "自动补采检查" if mode == "repair" else "最终复验"
    lines = [
        f"## A股数据闭环｜{target}",
        f"**{status}**（{mode_label}）",
        f"检查时间：{report['observed_at']}",
    ]
    problems = list(report.get("problems") or [])
    failures = [item for item in problems if item.get("status") != "waiting"]
    waiting = [item for item in problems if item.get("status") == "waiting"]
    if failures:
        lines.append("\n**未完成**")
        for item in failures[:10]:
            lines.append(f"- {item['name']}：{item['detail']}")
        if len(failures) > 10:
            lines.append(f"- 另有 {len(failures) - 10} 项未完成")
    if waiting:
        lines.append("\n**仍在运行，暂不重复启动**")
        for item in waiting[:8]:
            lines.append(f"- {item['name']}：{item['detail']}")
    if attempts:
        accepted = [item for item in attempts if item.get("accepted")]
        rejected = [item for item in attempts if not item.get("accepted")]
        if accepted:
            lines.append("\n**已自动触发重试**")
            lines.extend(
                f"- {item.get('task_name') or item['task_type']}"
                for item in accepted
            )
        if rejected:
            lines.append("\n**重试未受理**")
            lines.extend(
                f"- {item['task_type']}：{item.get('status') or 'unknown'}"
                for item in rejected
            )
    if not problems:
        lines.append("\n- 日线、分钟线、资金流、指数、概念、热点及关键任务回执均通过。")
    elif mode == "repair":
        lines.append("\n23:20 将自动复验并再次发送最终结果。")
    lines.append("\n以上为系统数据状态通知，不构成投资建议。")
    return "\n".join(lines)


def _target_date(engine: Engine, now: datetime) -> str:
    with engine.connect() as connection:
        is_trade_day = int(
            connection.execute(
                text(
                    "SELECT COUNT(*) FROM si_trade_calendar "
                    "WHERE trade_date=:day AND trade_status=1"
                ),
                {"day": now.date().isoformat()},
            ).scalar()
            or 0
        )
    if not is_trade_day:
        return ""
    target = expected_completed_trade_date(
        engine,
        now=now,
        ready_time="15:20",
    )
    if not target:
        raise RuntimeError("completed trade date is unavailable")
    date.fromisoformat(target)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("repair", "verify"), required=True)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-notify", action="store_true")
    args = parser.parse_args(argv)

    now = _now()
    engine = create_batch_engine()
    target = _target_date(engine, now)
    if not target:
        report = {
            "schema": CLOSEOUT_SCHEMA,
            "status": "SKIPPED",
            "reason": "non_trading_day",
            "observed_at": now.isoformat(timespec="seconds"),
        }
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0

    report = collect_closeout(
        engine,
        target_date=target,
        now=now,
    )
    attempts = (
        launch_repairs(engine, report)
        if args.mode == "repair" and report["status"] != "PASS"
        else []
    )
    report["repair_attempts"] = attempts
    report["mode"] = args.mode
    message = render_message(report, mode=args.mode, attempts=attempts)
    if not args.no_notify:
        result = deliver_markdown(
            get_wecom_webhook("briefing", required=False),
            message,
            engine=engine,
            delivery_kind=f"daily_data_closeout_{args.mode}",
            webhook_kind="briefing",
            title=f"## A股数据闭环｜{target}",
            idempotency_key=f"daily-data-closeout:{args.mode}:{target}",
        )
        report["wecom_delivery_id"] = result.delivery_id
        report["wecom_delivered_count"] = result.delivered_count
        report["wecom_segment_count"] = result.segment_count
    report["message"] = message

    if args.json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True, default=str))
    else:
        print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
