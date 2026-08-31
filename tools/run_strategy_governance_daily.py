#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run the daily dynamic strategy governance close cycle."""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.authoritative_market_clock import (
    authoritative_closed_trade_date,
)
from server.engine.strategy_governance_orchestrator import (
    INTEGRITY_ERROR,
    NOT_DUE,
    NOT_READY,
    PROGRAM_ERROR,
    orchestrate_strategy_governance,
)
from server.common.governance_safety import real_order_authority_is_closed
from server.common.strategy_governance_mode import (
    strategy_governance_database_deferred,
)


def _load_project_env() -> None:
    from tools.env_config import load_project_env

    load_project_env()


def _create_tool_engine():
    from tools.env_config import create_tool_engine

    return create_tool_engine()


def _capture_industry_history(target_trade_date: str) -> dict:
    """Copy the exact-date immutable QMT facts before governance."""

    from tools.sync_strategy_industry_history import capture_industry_history

    engine = _create_tool_engine()
    try:
        return capture_industry_history(engine, trade_date=target_trade_date)
    finally:
        engine.dispose()


def _bootstrap_execution_adapters() -> dict:
    from server.engine.strategy_execution_adapters import (
        bootstrap_strategy_execution_adapter_registry,
    )

    return bootstrap_strategy_execution_adapter_registry()


def _blocked(
    reason: str,
    target_trade_date: str = "",
    input_trade_date: str = "",
) -> int:
    print(
        json.dumps(
            {
                "status": "blocked",
                "reason": reason,
                "target_trade_date": target_trade_date,
                "input_trade_date": input_trade_date,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            },
            ensure_ascii=False,
            default=str,
        )
    )
    return 2


def _deferred_database_blocked_output() -> dict:
    """Return the fixed cash-only result for the explicit deferred DB mode."""

    return {
        "status": "blocked",
        "orchestration_status": NOT_READY,
        "reason_code": "GOVERNANCE_DATABASE_DEFERRED",
        "error_class": "NOT_READY",
        "retryable": True,
        "input_ready": False,
        "reason": "策略治理数据库迁移尚未完成，治理任务已失败关闭",
        "blocking_stage": "governance_database",
        "target_trade_date": "",
        "requested_trade_date": "",
        "input_trade_date": "",
        "allocations": [{
            "target_type": "CASH",
            "simulated_weight_pct": 100.0,
            "real_order_authority": False,
        }],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _input_block_reason(
    snapshot: dict, target_trade_date: str, input_ready: bool, input_reason: str
) -> str:
    """Reject an internally consistent snapshot when it is for an older day."""

    if not input_ready:
        return str(input_reason or "治理输入未就绪")
    snapshot_trade_date = str(snapshot.get("trade_date") or "")[:10]
    snapshot_data_date = str(snapshot.get("data_date") or "")[:10]
    if (
        snapshot_trade_date != target_trade_date
        or snapshot_data_date != target_trade_date
    ):
        return (
            "底层票池尚未产出权威已收盘交易日数据"
            f"（要求{target_trade_date}，实际交易日"
            f"{snapshot_trade_date or 'missing'}、数据日"
            f"{snapshot_data_date or 'missing'}）"
        )
    return ""


def _no_real_order_authority(value) -> bool:
    return real_order_authority_is_closed(value)


def _valid_iso_date(value: object, *, allow_empty: bool = True) -> bool:
    if value == "" and allow_empty:
        return True
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def validate_cli_result(
    payload: object,
    process_exit: int,
    *,
    expected_build_sha: str = "",
) -> str:
    """Validate the exact orchestration status/exit contract for deploy."""

    if (
        not isinstance(payload, dict)
        or payload.get("automatic_real_order_submission") is not False
        or payload.get("real_order_authority") is not False
        or not _no_real_order_authority(payload)
    ):
        raise ValueError("治理输出的安全字段无效")
    status = payload.get("status")
    orchestration = payload.get("orchestration_status")
    allocations = payload.get("allocations")
    if not isinstance(allocations, list):
        raise ValueError("治理输出缺少资金分配")
    weights = []
    for item in allocations:
        if not isinstance(item, dict):
            raise ValueError("治理资金分配行无效")
        if item.get("real_order_authority") is not False:
            raise ValueError("治理资金分配未显式关闭真实下单权限")
        value = item.get("simulated_weight_pct")
        if isinstance(value, bool):
            raise ValueError("治理资金权重无效")
        try:
            weight = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("治理资金权重无效") from exc
        if not math.isfinite(weight) or weight < 0:
            raise ValueError("治理资金权重无效")
        weights.append(weight)
    if abs(sum(weights) - 100.0) > 0.000001:
        raise ValueError("治理资金权重不守恒")

    if status == "ok" and orchestration == "COMPLETED":
        if (
            process_exit != 0
            or not re.fullmatch(r"[0-9a-f]{32}", str(
                payload.get("run_uid") or ""
            ))
            or not _valid_iso_date(payload.get("trade_date"), allow_empty=False)
            or not isinstance(payload.get("summary"), dict)
            or payload.get("reason_code") != "GOVERNANCE_COMPLETED"
            or (
                expected_build_sha
                and payload.get("build_commit_sha") != expected_build_sha
            )
        ):
            raise ValueError("治理完成输出与退出码不一致")
        return "completed"

    if status == "not_due" and orchestration == NOT_DUE:
        current_run = payload.get("current_run")
        if (
            process_exit != 0
            or payload.get("error_class") != "NONE"
            or payload.get("retryable") is not False
            or payload.get("input_ready") is not False
            or not isinstance(payload.get("reason"), str)
            or not payload["reason"].strip()
            or not isinstance(payload.get("reason_code"), str)
            or not payload["reason_code"]
            or not _valid_iso_date(
                payload.get("target_trade_date"), allow_empty=False
            )
            or not _valid_iso_date(payload.get("requested_trade_date", ""))
            or len(allocations) != 1
            or allocations[0].get("target_type") != "CASH"
            or (
                expected_build_sha
                and (
                    not isinstance(current_run, dict)
                    or current_run.get("build_commit_sha")
                    != expected_build_sha
                )
            )
        ):
            raise ValueError("治理未到期输出与退出码不一致")
        return "not_due"

    expected_block = {
        NOT_READY: (2, "NOT_READY", True, "not_ready"),
        INTEGRITY_ERROR: (3, "INTEGRITY", False, "integrity_error"),
        PROGRAM_ERROR: (4, "PROGRAM", False, "program_error"),
    }.get(str(orchestration or ""))
    if status != "blocked" or expected_block is None:
        raise ValueError("治理输出状态未知")
    expected_exit, expected_class, retryable, disposition = expected_block
    if (
        process_exit != expected_exit
        or payload.get("error_class") != expected_class
        or payload.get("retryable") is not retryable
        or payload.get("input_ready") is not False
        or not isinstance(payload.get("reason"), str)
        or not payload["reason"].strip()
        or not isinstance(payload.get("reason_code"), str)
        or not payload["reason_code"]
        or not isinstance(payload.get("blocking_stage"), str)
        or not payload["blocking_stage"]
        or not _valid_iso_date(payload.get("target_trade_date", ""))
        or not _valid_iso_date(payload.get("requested_trade_date", ""))
        or not _valid_iso_date(payload.get("input_trade_date", ""))
        or len(allocations) != 1
        or allocations[0].get("target_type") != "CASH"
    ):
        raise ValueError("治理阻断输出与退出码不一致")
    return disposition


def _process_exit_for_result(result: object) -> int:
    if not isinstance(result, dict):
        return 4
    orchestration_status = result.get("orchestration_status")
    if result.get("status") == "ok" or orchestration_status == NOT_DUE:
        return 0
    if orchestration_status == NOT_READY:
        return 2
    if orchestration_status == INTEGRITY_ERROR:
        return 3
    if orchestration_status == PROGRAM_ERROR:
        return 4
    return 1


def _safe_contract_failure_output(
    result: object,
    *,
    requested_trade_date: str,
) -> dict:
    """Return one fail-closed machine result without reflecting unsafe data."""

    source = result if isinstance(result, dict) else {}
    target_trade_date = str(
        source.get("target_trade_date")
        or source.get("trade_date")
        or requested_trade_date
        or ""
    )[:10]
    if not _valid_iso_date(target_trade_date):
        target_trade_date = ""
    clean_requested = str(requested_trade_date or "")[:10]
    if not _valid_iso_date(clean_requested):
        clean_requested = ""
    input_trade_date = str(source.get("input_trade_date") or "")[:10]
    if not _valid_iso_date(input_trade_date):
        input_trade_date = ""
    return {
        "status": "blocked",
        "orchestration_status": PROGRAM_ERROR,
        "reason_code": "INVALID_ORCHESTRATION_OUTPUT_CONTRACT",
        "error_class": "PROGRAM",
        "retryable": False,
        "input_ready": False,
        "reason": "治理输出契约校验失败，已失败关闭",
        "blocking_stage": "result_contract",
        "target_trade_date": target_trade_date,
        "requested_trade_date": clean_requested,
        "input_trade_date": input_trade_date,
        "allocations": [{
            "target_type": "CASH",
            "simulated_weight_pct": 100.0,
            "real_order_authority": False,
        }],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def run_daily_governance(
    *,
    requested_trade_date: str = "",
    strategy_limit: int = 500,
    expected_build_sha: str = "",
    external_market_context: dict | None = None,
) -> tuple[dict, int]:
    """Execute one canonical governance run and return its validated payload."""

    if strategy_governance_database_deferred():
        output = _deferred_database_blocked_output()
        process_exit = 2
        validate_cli_result(output, process_exit)
        return output, process_exit

    requested_trade_date = str(requested_trade_date or "").strip()
    engine = _create_tool_engine()
    try:
        from server.engine import strategy_governance as governance_engine
        from server.engine.strategy_center import bind_external_market_overlay

        with bind_external_market_overlay(external_market_context):
            result = orchestrate_strategy_governance(
                requested_trade_date=requested_trade_date,
                strategy_limit=max(1, min(500, int(strategy_limit))),
                operator=(
                    "scheduled_external_market_overlay"
                    if external_market_context is not None
                    else "scheduled_daily_governance"
                ),
                # An explicit authoritative date is an operator-requested
                # revision; the ordinary scheduler is a no-op when a canonical
                # run already exists.
                allow_revision=bool(requested_trade_date),
                engine=engine,
                industry_capture=(
                    lambda _engine, *, trade_date: _capture_industry_history(
                        trade_date
                    )
                ),
                governance_runner=governance_engine.governance_snapshot,
                # Standalone scheduler processes own one adapter bootstrap
                # before any calendar read or governance write.
                process_preflight=_bootstrap_execution_adapters,
                ensure_build_commit_sha=expected_build_sha,
            )
    finally:
        engine.dispose()
    output = result
    if result.get("status") == "ok":
        output = {
            "status": result.get("status"),
            "orchestration_status": result.get("orchestration_status"),
            "reason_code": result.get("reason_code"),
            "run_uid": result.get("run_uid"),
            "trade_date": result.get("trade_date"),
            "summary": result.get("summary"),
            "build_commit_sha": result.get("build_commit_sha"),
            "industry_snapshot": result.get("industry_snapshot"),
            "lifecycle_transitions": result.get("lifecycle_transitions"),
            "allocations": result.get("allocations"),
            "automatic_real_order_submission": result.get(
                "automatic_real_order_submission"
            ),
            "real_order_authority": result.get("real_order_authority"),
        }
    process_exit = _process_exit_for_result(result)
    try:
        validate_cli_result(
            output,
            process_exit,
            expected_build_sha=expected_build_sha,
        )
    except (TypeError, ValueError):
        output = _safe_contract_failure_output(
            result,
            requested_trade_date=requested_trade_date,
        )
        process_exit = 4
        validate_cli_result(output, process_exit)
    return output, process_exit


def main() -> int:
    parser = argparse.ArgumentParser(description="更新策略治理、竞技榜、票池和模拟权重")
    parser.add_argument("--trade-date", default="")
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help=(
            "候选票源单次读取上限（1-500）；不限制动态策略发现、"
            "健康计算或竞技排名数量"
        ),
    )
    parser.add_argument(
        "--expected-build-sha",
        default="",
        help=(
            "发布验收要求的40位构建SHA；若当前规范运行属于旧构建，"
            "会为权威交易日生成受审计的新修订"
        ),
    )
    parser.add_argument(
        "--validate-result-exit", type=int, default=-1,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()
    expected_build_sha = str(args.expected_build_sha or "").strip()
    if expected_build_sha and re.fullmatch(r"[0-9a-f]{40}", expected_build_sha) is None:
        parser.error("--expected-build-sha必须是40位小写十六进制Git SHA")
    if args.validate_result_exit >= 0:
        try:
            payload = json.load(sys.stdin)
            print(validate_cli_result(
                payload,
                args.validate_result_exit,
                expected_build_sha=expected_build_sha,
            ))
            return 0
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            print(f"invalid:{type(exc).__name__}", file=sys.stderr)
            return 2
    _load_project_env()
    output, process_exit = run_daily_governance(
        requested_trade_date=str(args.trade_date or "").strip(),
        strategy_limit=args.limit,
        expected_build_sha=expected_build_sha,
    )
    print(json.dumps(output, ensure_ascii=False, default=str))
    return process_exit


if __name__ == "__main__":
    raise SystemExit(main())
