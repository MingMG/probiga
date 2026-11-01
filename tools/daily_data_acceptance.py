#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only daily data acceptance with durable receipts and failure alerts."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
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


ACCEPTANCE_SCHEMA = "probiga.daily-data-acceptance.v1"
DEFAULT_OUTPUT_ROOT = Path(
    os.environ.get(
        "PROBIGA_DATA_ACCEPTANCE_ROOT",
        "/var/lib/probiga/data-acceptance",
    )
)
SHANGHAI = ZoneInfo("Asia/Shanghai")
PASS = "PASS"
FAIL = "FAIL"
SKIPPED = "SKIPPED"


@dataclass(frozen=True, slots=True)
class AcceptanceCheck:
    key: str
    label: str
    status: str
    reason: str
    remediation: str
    evidence: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "status": self.status,
            "reason": self.reason,
            "remediation": self.remediation,
            "evidence": dict(self.evidence),
        }


def _now() -> datetime:
    return datetime.now(SHANGHAI).replace(tzinfo=None, microsecond=0)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None, microsecond=0)
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(SHANGHAI).replace(tzinfo=None)
    return parsed.replace(microsecond=0)


def _short_error(exc: BaseException, *, limit: int = 300) -> str:
    value = re.sub(r"\s+", " ", str(exc or type(exc).__name__)).strip()
    return value[:limit]


def _latest_task(
    connection: Any,
    *,
    task_type: str,
) -> Mapping[str, Any] | None:
    return connection.execute(
        text(
            """
            SELECT id, task_name, task_type, enabled, cron_time,
                   script_path, script_args
              FROM st_scheduled_tasks
             WHERE task_type=:task_type
             ORDER BY id
             LIMIT 1
            """
        ),
        {"task_type": task_type},
    ).mappings().one_or_none()


def _latest_task_history(
    connection: Any,
    *,
    task_type: str,
) -> Mapping[str, Any] | None:
    return connection.execute(
        text(
            """
            SELECT run_uid, task_name, task_type, run_at, finished_at,
                   status, exit_code, host_name, scheduler_instance_id,
                   build_sha, trigger_source, output
              FROM st_scheduled_task_history
             WHERE task_type=:task_type
             ORDER BY run_at DESC, id DESC
             LIMIT 1
            """
        ),
        {"task_type": task_type},
    ).mappings().one_or_none()


def _status_accepts_completed_run(status: object) -> bool:
    return str(status or "").strip().lower() in {"success", "degraded"}


def _blocked_reason(output: object) -> str:
    value = str(output or "")
    matched = re.search(r"DATA_BLOCKED:[^\\\"\n]{1,300}", value)
    if matched is not None:
        return matched.group(0)
    matched = re.search(r"DATA_BLOCKED[^\\\"\n]{0,300}", value)
    return matched.group(0) if matched is not None else ""


def _daily_truth(
    connection: Any,
    *,
    target_date: str,
    now: datetime,
) -> tuple[Any, dict[str, Any]]:
    from server.common.qmt_daily_market_truth import (
        load_qmt_daily_market_truth,
    )
    from server.common.verified_daily_market_truth import (
        load_verified_daily_market_truth,
    )

    qmt_error: BaseException | None = None
    try:
        truth = load_qmt_daily_market_truth(
            connection,
            start_date=target_date,
            end_date=target_date,
            decision_known_at=now,
            verify_rows=False,
        )
        return truth, {"selected_provider": "qmt", "qmt_error": ""}
    except Exception as exc:
        qmt_error = exc
    free_truth = load_verified_daily_market_truth(
        connection,
        start_date=target_date,
        end_date=target_date,
        decision_known_at=now,
    )
    return free_truth, {
        "selected_provider": free_truth.provider,
        "qmt_error": _short_error(qmt_error),
    }


def _check_daily_truth(
    connection: Any,
    *,
    target_date: str,
    now: datetime,
) -> AcceptanceCheck:
    remediation = (
        "先重跑 qmt_stock_daily_canonical（任务111）；若QMT源不可用，"
        "再临时启用 free_stock_daily_canonical（任务138）并重新验收。"
    )
    try:
        truth, source_detail = _daily_truth(
            connection,
            target_date=target_date,
            now=now,
        )
        detail = truth.as_dict()
        finished = _parse_datetime(
            detail.get("run_finished_at")
            or detail.get("captured_at")
        )
        if finished is None:
            raise RuntimeError("daily truth has no completion timestamp")
        age_seconds = max(0, int((now - finished).total_seconds()))
        rows = int(
            detail.get("attested_row_count")
            or detail.get("row_count")
            or 0
        )
        if rows <= 0:
            raise RuntimeError("daily truth contains no rows")
        if age_seconds > 24 * 60 * 60:
            raise RuntimeError(
                f"daily truth is stale by {age_seconds // 3600} hours"
            )
    except Exception as exc:
        return AcceptanceCheck(
            "daily_receipt",
            "当日标准日线回执",
            FAIL,
            _short_error(exc),
            remediation,
            {"target_date": target_date},
        )
    return AcceptanceCheck(
        "daily_receipt",
        "当日标准日线回执",
        PASS,
        f"{detail.get('schema')} rows={rows}",
        "",
        {
            "target_date": target_date,
            "schema": detail.get("schema"),
            "provider": source_detail["selected_provider"],
            "receipt_id": detail.get("receipt_id"),
            "run_id": detail.get("run_id"),
            "row_count": rows,
            "finished_at": finished.isoformat(timespec="seconds"),
            "age_seconds": age_seconds,
            "qmt_fallback_error": source_detail["qmt_error"],
        },
    )


def _check_daily_coverage(
    engine: Engine,
    connection: Any,
    *,
    target_date: str,
    now: datetime,
) -> AcceptanceCheck:
    from server.common.strategy_daily_input_window import (
        resolve_daily_input_sessions,
    )

    remediation = (
        "检查 qmt_stock_daily_canonical 的目标日和前一交易日覆盖；"
        "不要用空行或旧行补数。"
    )
    try:
        sessions = resolve_daily_input_sessions(
            engine,
            target_trade_date=target_date,
            session_count=2,
            decision_known_at=now,
        )
        previous = sessions[-2]
        rows = connection.execute(
            text(
                """
                SELECT trade_date, COUNT(DISTINCT stock_code) AS code_count
                  FROM sm_stock_kline
                 WHERE trade_date IN (:target_date, :previous_date)
                   AND k_type=1 AND adjust_type=0
                 GROUP BY trade_date
                """
            ),
            {
                "target_date": target_date,
                "previous_date": previous,
            },
        ).mappings().all()
        counts = {
            str(row["trade_date"])[:10]: int(row["code_count"] or 0)
            for row in rows
        }
        target_count = counts.get(target_date, 0)
        previous_count = counts.get(previous, 0)
        if target_count <= 0 or previous_count <= 0:
            raise RuntimeError(
                f"daily row coverage is missing: target={target_count}, "
                f"previous={previous_count}"
            )
        if target_count < previous_count * 0.90:
            raise RuntimeError(
                f"daily code coverage dropped below 90%: "
                f"{target_count}/{previous_count}"
            )
    except Exception as exc:
        return AcceptanceCheck(
            "daily_coverage",
            "当日日线覆盖量级",
            FAIL,
            _short_error(exc),
            remediation,
            {"target_date": target_date},
        )
    return AcceptanceCheck(
        "daily_coverage",
        "当日日线覆盖量级",
        PASS,
        f"{target_count}/{previous_count} codes",
        "",
        {
            "target_date": target_date,
            "previous_session": previous,
            "target_code_count": target_count,
            "previous_code_count": previous_count,
            "ratio": round(target_count / previous_count, 6),
        },
    )


def _check_qmt_truth_freshness(
    connection: Any,
    *,
    target_date: str,
    now: datetime,
) -> AcceptanceCheck:
    remediation = (
        "确认 Windows QMT datadir 已完成目标交易日文件写入，并重跑 "
        "qmt_stock_daily_canonical（任务111）；完成回执必须早于验收时点且不超过24小时。"
    )
    try:
        receipt = connection.execute(
            text(
                """
                SELECT run_id, status, started_at, finished_at,
                       row_count, code_count, content_sha256
                  FROM qmt_datadir_import_partition
                 WHERE dataset='stock_daily'
                   AND trade_date=:target_date
                 ORDER BY finished_at DESC, started_at DESC, run_id DESC
                 LIMIT 1
                """
            ),
            {"target_date": target_date},
        ).mappings().one_or_none()
        if receipt is None:
            raise RuntimeError("QMT daily import receipt is missing")
        if str(receipt.get("status") or "") != "COMPLETE":
            raise RuntimeError(
                f"QMT daily import status is {receipt.get('status') or 'unknown'}"
            )
        finished = _parse_datetime(receipt.get("finished_at"))
        if finished is None:
            raise RuntimeError("QMT daily import has no finished_at")
        if finished > now:
            raise RuntimeError("QMT daily import finished after acceptance cutoff")
        age_seconds = max(0, int((now - finished).total_seconds()))
        if age_seconds > 24 * 60 * 60:
            raise RuntimeError(
                f"QMT daily import is stale by {age_seconds // 3600} hours"
            )
        if int(receipt.get("row_count") or 0) <= 0:
            raise RuntimeError("QMT daily import contains no rows")
    except Exception as exc:
        return AcceptanceCheck(
            "qmt_truth_freshness",
            "QMT日线真值新鲜度",
            FAIL,
            _short_error(exc),
            remediation,
            {"target_date": target_date},
        )
    return AcceptanceCheck(
        "qmt_truth_freshness",
        "QMT日线真值新鲜度",
        PASS,
        f"finished={finished.isoformat(timespec='seconds')}",
        "",
        {
            "target_date": target_date,
            "run_id": str(receipt.get("run_id") or ""),
            "finished_at": finished.isoformat(timespec="seconds"),
            "age_seconds": age_seconds,
            "row_count": int(receipt.get("row_count") or 0),
            "code_count": int(receipt.get("code_count") or 0),
            "content_sha256": str(receipt.get("content_sha256") or ""),
        },
    )


def _check_membership(
    engine: Engine,
    connection: Any,
    *,
    target_date: str,
    now: datetime,
) -> AcceptanceCheck:
    from server.common.strategy_daily_input_window import (
        resolve_daily_input_sessions,
    )

    remediation = (
        "检查 Windows QMT 登录与 membership capture；修复后重跑 "
        "qmt_membership_snapshot（任务65），再运行本验收。"
    )
    try:
        sessions = resolve_daily_input_sessions(
            engine,
            target_trade_date=target_date,
            session_count=3,
            decision_known_at=now,
        )
        allowed_dates = set(sessions[-2:])
        snapshot = connection.execute(
            text(
                """
                SELECT snapshot_date, captured_at, quality_status,
                       concept_count, industry_count
                  FROM qmt_membership_snapshot_run
                 ORDER BY snapshot_date DESC, captured_at DESC
                 LIMIT 1
                """
            )
        ).mappings().one_or_none()
        history = _latest_task_history(
            connection,
            task_type="qmt_membership_snapshot",
        )
        blocked = _blocked_reason(history.get("output") if history else "")
        if snapshot is None:
            raise RuntimeError("membership snapshot is unavailable")
        snapshot_date = str(snapshot["snapshot_date"])[:10]
        if snapshot_date not in allowed_dates:
            raise RuntimeError(
                f"membership snapshot {snapshot_date} is older than "
                f"allowed sessions {sorted(allowed_dates)}"
            )
        if blocked:
            raise RuntimeError(blocked)
    except Exception as exc:
        return AcceptanceCheck(
            "membership",
            "QMT membership 状态",
            FAIL,
            _short_error(exc),
            remediation,
            {
                "target_date": target_date,
                "latest_snapshot_date": (
                    str(snapshot["snapshot_date"])[:10]
                    if snapshot is not None
                    else None
                ),
                "latest_task_status": (
                    str(history.get("status") or "")
                    if history is not None
                    else None
                ),
            },
        )
    return AcceptanceCheck(
        "membership",
        "QMT membership 状态",
        PASS,
        f"snapshot={snapshot_date} quality={snapshot['quality_status']}",
        "",
        {
            "target_date": target_date,
            "snapshot_date": snapshot_date,
            "captured_at": str(snapshot["captured_at"]),
            "quality_status": str(snapshot["quality_status"]),
            "concept_count": int(snapshot["concept_count"] or 0),
            "industry_count": int(snapshot["industry_count"] or 0),
        },
    )


def _check_etf(
    connection: Any,
    *,
    target_date: str,
) -> AcceptanceCheck:
    remediation = (
        "确认 ETF 任务是否应启用；启用后重跑 etf_forward_daily（任务64），"
        "并检查 BigQMT no-order capability。"
    )
    task = _latest_task(connection, task_type="etf_forward_daily")
    history = _latest_task_history(
        connection,
        task_type="etf_forward_daily",
    )
    try:
        if task is None:
            raise RuntimeError("etf_forward_daily scheduler task is missing")
        if int(task.get("enabled") or 0) != 1:
            raise RuntimeError("etf_forward_daily scheduler task is disabled")
        if history is None:
            raise RuntimeError("etf_forward_daily has no run history")
        run_at = _parse_datetime(history.get("run_at"))
        if run_at is None or run_at.date() < date.fromisoformat(target_date):
            raise RuntimeError("etf_forward_daily did not run for target date")
        if not _status_accepts_completed_run(history.get("status")):
            detail = _short_error(history.get("output"), limit=240)
            raise RuntimeError(f"etf_forward_daily status={history.get('status')} {detail}")
    except Exception as exc:
        return AcceptanceCheck(
            "etf_daily",
            "ETF 日线任务",
            FAIL,
            _short_error(exc),
            remediation,
            {
                "target_date": target_date,
                "task_id": int(task["id"]) if task is not None else None,
                "enabled": bool(task.get("enabled")) if task is not None else None,
                "last_status": str(history.get("status") or "") if history else None,
            },
        )
    return AcceptanceCheck(
        "etf_daily",
        "ETF 日线任务",
        PASS,
        f"{history.get('status')} at {history.get('finished_at')}",
        "",
        {
            "target_date": target_date,
            "task_id": int(task["id"]),
            "run_uid": str(history.get("run_uid") or ""),
            "run_at": str(history.get("run_at")),
            "finished_at": str(history.get("finished_at")),
            "status": str(history.get("status") or ""),
        },
    )


def _check_concepts(
    engine: Engine,
    connection: Any,
    *,
    target_date: str,
    now: datetime,
) -> AcceptanceCheck:
    from server.common.strategy_daily_input_window import (
        resolve_daily_input_sessions,
    )

    remediation = (
        "重跑 eastmoney_concept_current（任务由 ensure_quality_gate 安装），"
        "验收要求最新概念快照不早于最近两个交易日。"
    )
    try:
        sessions = resolve_daily_input_sessions(
            engine,
            target_trade_date=target_date,
            session_count=3,
            decision_known_at=now,
        )
        allowed_dates = set(sessions[-2:])
        latest = connection.execute(
            text(
                """
                SELECT MAX(trade_date) AS latest_date, COUNT(*) AS row_count
                  FROM sm_concept_east_current
                """
            )
        ).mappings().one()
        latest_date = (
            str(latest["latest_date"])[:10]
            if latest["latest_date"] is not None
            else ""
        )
        row_count = int(latest["row_count"] or 0)
        if row_count <= 0 or latest_date not in allowed_dates:
            raise RuntimeError(
                f"latest concept snapshot {latest_date or '-'} is outside "
                f"{sorted(allowed_dates)}"
            )
    except Exception as exc:
        return AcceptanceCheck(
            "concept_freshness",
            "概念快照新鲜度",
            FAIL,
            _short_error(exc),
            remediation,
            {"target_date": target_date},
        )
    return AcceptanceCheck(
        "concept_freshness",
        "概念快照新鲜度",
        PASS,
        f"latest={latest_date} rows={row_count}",
        "",
        {
            "target_date": target_date,
            "latest_snapshot_date": latest_date,
            "row_count": row_count,
            "allowed_sessions": sorted(allowed_dates),
        },
    )


def _check_v3_tasks(
    connection: Any,
    *,
    target_date: str,
) -> AcceptanceCheck:
    remediation = (
        "先修复其上游数据，再按任务ID重跑失败的 V3 任务；"
        "V3任务不做自动重跑，避免掩盖真实数据问题。"
    )
    try:
        tasks = connection.execute(
            text(
                """
                SELECT id, task_name, task_type, enabled, cron_time
                  FROM st_scheduled_tasks
                 WHERE task_type LIKE 'trading_v3_%'
                 ORDER BY id
                """
            )
        ).mappings().all()
        failures: list[dict[str, Any]] = []
        observed: list[dict[str, Any]] = []
        for task in tasks:
            if int(task.get("enabled") or 0) != 1:
                continue
            history = connection.execute(
                text(
                    """
                    SELECT run_at, finished_at, status, exit_code, output
                      FROM st_scheduled_task_history
                     WHERE task_type=:task_type
                       AND run_at >= :target_start
                     ORDER BY run_at DESC, id DESC
                     LIMIT 1
                    """
                ),
                {
                    "task_type": str(task["task_type"]),
                    "target_start": f"{target_date} 00:00:00",
                },
            ).mappings().one_or_none()
            item = {
                "task_id": int(task["id"]),
                "task_type": str(task["task_type"]),
                "task_name": str(task["task_name"]),
                "status": str(history.get("status") or "") if history else "",
            }
            observed.append(item)
            if history is None or not _status_accepts_completed_run(
                history.get("status")
            ):
                failures.append(item)
        if failures:
            labels = ", ".join(
                f"{item['task_id']}:{item['status'] or 'not-run'}"
                for item in failures
            )
            raise RuntimeError(f"enabled V3 tasks failed: {labels}")
    except Exception as exc:
        return AcceptanceCheck(
            "v3_tasks",
            "V3 收盘任务",
            FAIL,
            _short_error(exc),
            remediation,
            {"target_date": target_date},
        )
    return AcceptanceCheck(
        "v3_tasks",
        "V3 收盘任务",
        PASS,
        f"{len(observed)} enabled tasks succeeded",
        "",
        {"target_date": target_date, "tasks": observed},
    )


def collect_acceptance(
    engine: Engine,
    *,
    target_date: str,
    now: datetime,
) -> dict[str, Any]:
    checks: list[AcceptanceCheck] = []
    with engine.connect() as connection:
        checks.append(
            _check_daily_truth(
                connection,
                target_date=target_date,
                now=now,
            )
        )
        checks.append(
            _check_daily_coverage(
                engine,
                connection,
                target_date=target_date,
                now=now,
            )
        )
        checks.append(
            _check_qmt_truth_freshness(
                connection,
                target_date=target_date,
                now=now,
            )
        )
        checks.append(
            _check_membership(
                engine,
                connection,
                target_date=target_date,
                now=now,
            )
        )
        checks.append(
            _check_etf(
                connection,
                target_date=target_date,
            )
        )
        checks.append(
            _check_concepts(
                engine,
                connection,
                target_date=target_date,
                now=now,
            )
        )
        checks.append(
            _check_v3_tasks(
                connection,
                target_date=target_date,
            )
        )
    failed = [check for check in checks if check.status == FAIL]
    return {
        "schema": ACCEPTANCE_SCHEMA,
        "status": PASS if not failed else FAIL,
        "acceptance_status": (
            "DATA_ACCEPTANCE_OK" if not failed else "DATA_ACCEPTANCE_FAILED"
        ),
        "target_date": target_date,
        "observed_at": now.isoformat(timespec="seconds"),
        "checks": [check.as_dict() for check in checks],
        "failed_count": len(failed),
    }


def render_message(report: Mapping[str, Any]) -> str:
    target = str(report["target_date"])
    lines = [
        f"## 每日数据验收｜{target}",
        f"**{report['status']}**",
    ]
    for check in report.get("checks") or []:
        icon = "✅" if check.get("status") == PASS else "❌"
        lines.append(
            f"- {icon} {check.get('label')}：{check.get('reason')}"
        )
    failures = [
        check for check in report.get("checks") or []
        if check.get("status") == FAIL
    ]
    if failures:
        lines.append("\n**补救指引**")
        lines.extend(
            f"- {check.get('label')}：{check.get('remediation')}"
            for check in failures
        )
    return "\n".join(lines)


def persist_report(
    report: Mapping[str, Any],
    *,
    output_root: Path,
    message: str | None = None,
) -> Path:
    output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    target = date.fromisoformat(str(report["target_date"])).isoformat()
    path = output_root / f"{target}.json"
    payload = {**report, "message": message or render_message(report)}
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    ) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        dir=output_root,
        prefix=f".{target}.",
        suffix=".tmp",
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def _target_date(engine: Engine, now: datetime) -> str:
    from tools.daily_data_closeout import _target_date as resolve_target_date

    return resolve_target_date(engine, now)


def _alert_identity(report: Mapping[str, Any]) -> str:
    failures = [
        {
            "key": item.get("key"),
            "reason": item.get("reason"),
        }
        for item in report.get("checks") or []
        if item.get("status") == FAIL
    ]
    digest = hashlib.sha256(
        json.dumps(
            failures,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return f"daily-data-acceptance:{report['target_date']}:{digest}"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-date")
    parser.add_argument("--now")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--no-notify", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    from server.common.batch_db import create_batch_engine

    now = _parse_datetime(args.now) if args.now else _now()
    if now is None:
        raise SystemExit("--now must be an ISO timestamp")
    engine = create_batch_engine()
    try:
        target_date = args.target_date or _target_date(engine, now)
        if not target_date:
            report: dict[str, Any] = {
                "schema": ACCEPTANCE_SCHEMA,
                "status": SKIPPED,
                "acceptance_status": "DATA_ACCEPTANCE_SKIPPED",
                "reason": "non_trading_day",
                "observed_at": now.isoformat(timespec="seconds"),
                "checks": [],
                "failed_count": 0,
            }
        else:
            date.fromisoformat(target_date)
            report = collect_acceptance(
                engine,
                target_date=target_date,
                now=now,
            )

        message = render_message(report) if report["status"] != SKIPPED else ""
        receipt_path = None
        if report["status"] != SKIPPED:
            receipt_path = persist_report(
                report,
                output_root=args.output_root,
                message=message,
            )
            report["receipt_path"] = str(receipt_path)

        if report["status"] == FAIL and not args.no_notify:
            from integrations.wecom.delivery import deliver_markdown
            from server.common.config import get_wecom_webhook

            webhook = get_wecom_webhook("briefing", required=False)
            if webhook:
                try:
                    delivery = deliver_markdown(
                        webhook,
                        message,
                        engine=engine,
                        delivery_kind="daily_data_acceptance",
                        webhook_kind="briefing",
                        title=f"## 每日数据验收｜{report['target_date']}",
                        idempotency_key=_alert_identity(report),
                    )
                except Exception as exc:
                    report["notification"] = {
                        "status": "FAILED",
                        "error_type": type(exc).__name__,
                    }
                else:
                    report["notification"] = {
                        "status": "DELIVERED",
                        "delivery_id": delivery.delivery_id,
                        "delivered_count": delivery.delivered_count,
                        "segment_count": delivery.segment_count,
                    }
            else:
                report["notification"] = {
                    "status": "NOT_CONFIGURED",
                }
            if receipt_path is not None:
                persist_report(
                    report,
                    output_root=args.output_root,
                    message=message,
                )

        if args.json:
            print(json.dumps(report, ensure_ascii=False, sort_keys=True, default=str))
        elif message:
            print(message)
        return 0 if report["status"] in {PASS, SKIPPED} else 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
