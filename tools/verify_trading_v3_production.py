#!/usr/bin/env python3
"""Collect read-only production evidence for the Trading V3 chain."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.kline_data import get_kline_engine
from server.common.adata_release import validate_adata_release_source
from server.trading_v3.config import config_hash, load_v3_config
from server.trading_v3.counterfactual_worker import (
    counterfactual_queue_stats,
)
from server.trading_v3.repository import TradingV3Repository
from tools.env_config import create_tool_engine, load_project_env
from tools.remote_support import (
    PRODUCTION_CODE_RELEASE_ROOT,
    PRODUCTION_CURRENT_RELEASE_LINK,
    production_ssh_client,
    production_ssh_connect_kwargs,
    PRODUCTION_ADATA_RELEASE_ROOT,
    PRODUCTION_RELEASE_VENV_ROOT,
    production_release_command,
)
from tools.trading_v3_fourth_layer_readiness import (
    collect_fourth_layer_readiness,
)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date,)):
        return value.isoformat()
    return str(value)


def _one(engine: Engine, statement: str) -> dict[str, Any]:
    with engine.connect() as connection:
        row = connection.execute(text(statement)).mappings().first()
    return dict(row) if row else {}


def _all(engine: Engine, statement: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(text(statement)).mappings().all()
    return [dict(row) for row in rows]


def _safe_one(
    engine: Engine,
    statement: str,
) -> dict[str, Any]:
    try:
        return _one(engine, statement)
    except Exception as exc:
        return {"query_error": f"{type(exc).__name__}: {exc}"}


def _safe_all(
    engine: Engine,
    statement: str,
) -> list[dict[str, Any]]:
    try:
        return _all(engine, statement)
    except Exception as exc:
        return [{"query_error": f"{type(exc).__name__}: {exc}"}]


def _safe_call(function, *args, **kwargs) -> Any:
    try:
        return function(*args, **kwargs)
    except Exception as exc:
        return {"query_error": f"{type(exc).__name__}: {exc}"}


def _query_ok(value: Any) -> bool:
    if isinstance(value, dict):
        return "query_error" not in value and all(
            _query_ok(item) for item in value.values()
        )
    if isinstance(value, list):
        return all(_query_ok(item) for item in value)
    return True


_FORWARD_EXIT_ALLOCATION_HEALTH_SQL = """
SELECT
    (SELECT COUNT(*)
     FROM st_forward_exit_allocation_v3)
        AS allocation_row_count,
    (SELECT COUNT(*)
     FROM st_fill_v2 raw_sell
     WHERE raw_sell.account_id = 'paper-main-v2'
       AND raw_sell.side = 'SELL')
        AS paper_sell_fill_count,
    (SELECT COUNT(*)
     FROM st_forward_exit_allocation_v3 allocation
     LEFT JOIN st_fill_v2 raw_sell
       ON raw_sell.fill_id = allocation.exit_fill_id
     LEFT JOIN st_fill_v2 raw_entry
       ON raw_entry.fill_id = allocation.entry_fill_id
     LEFT JOIN st_forward_trade_evidence_v3 parent
       ON parent.evidence_id = allocation.evidence_id
     WHERE allocation.allocation_protocol_version <>
               'PAPER_FIFO_EXIT_ALLOCATION_V1'
        OR BINARY allocation.allocation_id <> BINARY SHA2(CONCAT(
               allocation.exit_fill_id, '|',
               allocation.allocation_sequence, '|',
               allocation.entry_fill_id, '|',
               'PAPER_FIFO_EXIT_ALLOCATION_V1'
           ), 256)
        OR allocation.allocation_sequence < 0
        OR allocation.allocated_quantity <= 0
        OR allocation.allocated_gross_cny < 0
        OR allocation.allocated_fee_cny < 0
        OR raw_sell.fill_id IS NULL
        OR raw_sell.side <> 'SELL'
        OR raw_sell.account_id <> allocation.account_id
        OR raw_sell.stock_code <> allocation.stock_code
        OR raw_sell.order_id <> allocation.exit_order_id
        OR raw_sell.filled_at <> allocation.exit_filled_at
        OR raw_sell.quantity <= 0
        OR allocation.allocated_quantity > raw_sell.quantity
        OR raw_entry.fill_id IS NULL
        OR raw_entry.side <> 'BUY'
        OR raw_entry.account_id <> allocation.account_id
        OR raw_entry.stock_code <> allocation.stock_code
        OR raw_entry.filled_at > allocation.exit_filled_at
        OR NOT (
             (
                 allocation.attribution_status = 'ATTRIBUTED'
                 AND allocation.evidence_id IS NOT NULL
                 AND parent.evidence_id IS NOT NULL
                 AND parent.account_id = allocation.account_id
                 AND parent.stock_code = allocation.stock_code
                 AND parent.entry_fill_id = allocation.entry_fill_id
                 AND parent.closed_quantity > 0
                 AND parent.evidence_status IN (
                     'PARTIALLY_CLOSED', 'MATURED'
                 )
                 AND parent.exit_at IS NOT NULL
                 AND allocation.exit_filled_at <= parent.exit_at
                 AND JSON_CONTAINS(
                     IF(
                         JSON_VALID(parent.exit_fill_ids_json),
                         parent.exit_fill_ids_json,
                         JSON_ARRAY()
                     ),
                     JSON_QUOTE(allocation.exit_fill_id)
                 ) = 1
                 AND JSON_CONTAINS(
                     IF(
                         JSON_VALID(parent.exit_order_ids_json),
                         parent.exit_order_ids_json,
                         JSON_ARRAY()
                     ),
                     JSON_QUOTE(allocation.exit_order_id)
                 ) = 1
             )
             OR (
                 allocation.attribution_status = 'UNATTRIBUTED'
                 AND allocation.evidence_id IS NULL
             )
        )) AS invalid_allocation_row_count,
    (SELECT COUNT(*)
     FROM st_forward_exit_allocation_v3 allocation
     JOIN (
         SELECT exit_fill_id, MAX(allocation_sequence) AS maximum_sequence
         FROM st_forward_exit_allocation_v3
         GROUP BY exit_fill_id
     ) sequence_tail
       ON sequence_tail.exit_fill_id = allocation.exit_fill_id
     JOIN st_fill_v2 raw_sell
       ON raw_sell.fill_id = allocation.exit_fill_id
      AND raw_sell.side = 'SELL'
     WHERE allocation.allocation_sequence < sequence_tail.maximum_sequence
       AND (
           allocation.allocated_gross_cny <> ROUND(
               raw_sell.gross_amount * allocation.allocated_quantity /
               raw_sell.quantity,
               6
           )
           OR allocation.allocated_fee_cny <> ROUND(
               raw_sell.fee_amount * allocation.allocated_quantity /
               raw_sell.quantity,
               6
           )
       )) AS invalid_non_tail_rounding_count,
    (SELECT COUNT(*)
     FROM st_fill_v2 raw_sell
     LEFT JOIN (
         SELECT exit_fill_id,
                COUNT(*) AS allocation_row_count,
                COUNT(DISTINCT allocation_sequence)
                    AS distinct_sequence_count,
                MIN(allocation_sequence) AS minimum_sequence,
                MAX(allocation_sequence) AS maximum_sequence,
                SUM(allocated_quantity) AS allocated_quantity,
                SUM(allocated_gross_cny) AS allocated_gross_cny,
                SUM(allocated_fee_cny) AS allocated_fee_cny
         FROM st_forward_exit_allocation_v3
         GROUP BY exit_fill_id
     ) coverage
       ON coverage.exit_fill_id = raw_sell.fill_id
     WHERE raw_sell.side = 'SELL'
       AND (
           raw_sell.account_id = 'paper-main-v2'
           OR coverage.exit_fill_id IS NOT NULL
       )
       AND (
           coverage.exit_fill_id IS NULL
           OR coverage.allocation_row_count <= 0
           OR coverage.distinct_sequence_count <>
               coverage.allocation_row_count
           OR coverage.minimum_sequence <> 0
           OR coverage.maximum_sequence <>
               coverage.allocation_row_count - 1
           OR coverage.allocated_quantity <> raw_sell.quantity
           OR coverage.allocated_gross_cny <>
               CAST(raw_sell.gross_amount AS DECIMAL(20,6))
           OR coverage.allocated_fee_cny <>
               CAST(raw_sell.fee_amount AS DECIMAL(20,6))
       )) AS invalid_sell_coverage_count
"""


def _forward_exit_allocation_health_valid(summary: Any) -> bool:
    if not isinstance(summary, dict) or not _query_ok(summary):
        return False
    count_fields = (
        "allocation_row_count",
        "paper_sell_fill_count",
        "invalid_allocation_row_count",
        "invalid_non_tail_rounding_count",
        "invalid_sell_coverage_count",
    )
    if not set(count_fields).issubset(summary):
        return False
    try:
        counts = {field: int(summary[field]) for field in count_fields}
    except (TypeError, ValueError):
        return False
    return (
        all(value >= 0 for value in counts.values())
        and counts["invalid_allocation_row_count"] == 0
        and counts["invalid_non_tail_rounding_count"] == 0
        and counts["invalid_sell_coverage_count"] == 0
    )


_EXPECTED_REAL_TRADING_GUARDS = {
    "trg_trade_account_v2_real_disabled_bi": (
        "BEFORE", "INSERT", "st_trade_account_v2", "real_trading_enabled"
    ),
    "trg_trade_account_v2_real_disabled_bu": (
        "BEFORE", "UPDATE", "st_trade_account_v2", "real_trading_enabled"
    ),
    "trg_execution_plan_v3_real_disabled_bi": (
        "BEFORE", "INSERT", "st_execution_plan_v3", "real_order_allowed"
    ),
    "trg_execution_plan_v3_real_disabled_bu": (
        "BEFORE", "UPDATE", "st_execution_plan_v3", "real_order_allowed"
    ),
}


def _real_trading_guard_rows_valid(rows: Any) -> bool:
    """Require the exact trigger surface and a fail-closed SIGNAL body."""

    if not isinstance(rows, list) or not _query_ok(rows):
        return False
    by_name: dict[str, dict[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, dict):
            return False
        name = str(raw.get("TRIGGER_NAME") or "")
        if not name or name in by_name:
            return False
        by_name[name] = raw
    if set(by_name) != set(_EXPECTED_REAL_TRADING_GUARDS):
        return False
    for name, (timing, event, table_name, guarded_column) in (
        _EXPECTED_REAL_TRADING_GUARDS.items()
    ):
        row = by_name[name]
        body = "".join(
            str(row.get("ACTION_STATEMENT") or "")
            .replace("`", "")
            .casefold()
            .split()
        )
        if (
            str(row.get("ACTION_TIMING") or "").upper() != timing
            or str(row.get("EVENT_MANIPULATION") or "").upper() != event
            or str(row.get("EVENT_OBJECT_TABLE") or "") != table_name
            or f"coalesce(new.{guarded_column},0)<>0" not in body
            or "signalsqlstate'45000'" not in body
        ):
            return False
    return True


def _is_production_runtime() -> bool:
    configured = os.environ.get("PROBIGA_CODE_ROOT", "").strip()
    if not configured:
        return False
    try:
        return ROOT.resolve(strict=True) == Path(configured).resolve(strict=True)
    except (OSError, ValueError):
        return False


def _local_production_runtime_identity() -> tuple[bool, str]:
    """Prove this process is inside the SHA-pinned production runtime."""

    if not _is_production_runtime():
        return False, "repository path is not the configured production root"
    if os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip() != "production":
        return False, "PROBIGA_DEPLOYMENT_MODE is not production"
    expected_sha = os.environ.get("PROBIGA_EXPECTED_GIT_SHA", "").strip()
    build_sha = os.environ.get("PROBIGA_BUILD_COMMIT_SHA", "").strip()
    adata_sha = os.environ.get("PROBIGA_EXPECTED_ADATA_SHA", "").strip()
    adata_tree_sha = os.environ.get(
        "PROBIGA_EXPECTED_ADATA_TREE_SHA256", ""
    ).strip()
    adata_source_value = os.environ.get("PROBIGA_ADATA_SOURCE_DIR", "").strip()
    if re.fullmatch(r"[0-9a-f]{40}", expected_sha) is None:
        return False, "production Git SHA pin is absent or invalid"
    if build_sha != expected_sha:
        return False, "production build SHA differs from its Git SHA pin"
    if re.fullmatch(r"[0-9a-f]{40}", adata_sha) is None:
        return False, "production adata Git SHA pin is absent or invalid"
    if re.fullmatch(r"[0-9a-f]{64}", adata_tree_sha) is None:
        return False, "production adata tree SHA pin is absent or invalid"
    expected_code_root = Path(PRODUCTION_CODE_RELEASE_ROOT) / expected_sha
    configured_code_root = os.environ.get("PROBIGA_CODE_ROOT", "").strip()
    if configured_code_root != str(expected_code_root):
        return False, "production code root is not the SHA-addressed release"
    try:
        canonical_code_root = expected_code_root.resolve(strict=True)
        if expected_code_root.is_symlink() or canonical_code_root != expected_code_root:
            return False, "production code release is not a canonical directory"
        if ROOT.resolve(strict=True) != canonical_code_root:
            return False, "verifier code is not the active SHA-addressed release"
        current_link = Path(PRODUCTION_CURRENT_RELEASE_LINK)
        if (
            not current_link.is_symlink()
            or current_link.resolve(strict=True) != canonical_code_root
        ):
            return False, "production current link does not select this release"
    except (OSError, ValueError) as exc:
        return False, f"active production code identity cannot be proven: {exc}"

    try:
        actual_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=10,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError, UnicodeError) as exc:
        return False, f"cannot inspect production Git identity: {exc}"
    if actual_sha != expected_sha:
        return False, "production checkout does not match its Git SHA pin"

    venv_link = Path(PRODUCTION_RELEASE_VENV_ROOT) / expected_sha
    try:
        if not venv_link.is_symlink():
            return False, "production release venv is not a SHA-addressed symlink"
        venv_target = venv_link.resolve(strict=True)
        release_venv_root = Path(PRODUCTION_RELEASE_VENV_ROOT).resolve(
            strict=True
        )
        venv_target.relative_to(release_venv_root)
        if not venv_target.name.startswith(f"build-{expected_sha}-"):
            return False, "production release venv target is not bound to Git SHA"
        if Path(sys.prefix).resolve(strict=True) != venv_target:
            return False, "verifier is not running from the active release venv"
        if (
            (venv_link / ".probiga.gitsha").read_text(encoding="ascii").strip()
            != expected_sha
        ):
            return False, "release venv Git marker differs"
        if (
            (venv_link / ".adata.gitsha").read_text(encoding="ascii").strip()
            != adata_sha
            or (venv_link / ".adata.tree.sha256")
            .read_text(encoding="ascii")
            .strip()
            != adata_tree_sha
        ):
            return False, "release venv adata markers differ"
    except (OSError, UnicodeError, ValueError) as exc:
        return False, f"release venv identity cannot be proven: {exc}"

    expected_adata_source = Path(PRODUCTION_ADATA_RELEASE_ROOT) / (
        f"{adata_sha}-{adata_tree_sha}"
    )
    try:
        adata_source = Path(adata_source_value).resolve(strict=True)
        if adata_source != expected_adata_source.resolve(strict=True):
            return False, "adata source is not the content-addressed release path"
        validate_adata_release_source(
            adata_source,
            expected_git_sha=adata_sha,
            expected_tree_sha256=adata_tree_sha,
            repository_root=ROOT,
            require_read_only=True,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        return False, f"sealed adata identity cannot be proven: {exc}"

    python_paths = tuple(
        Path(value).resolve()
        for value in os.environ.get("PYTHONPATH", "").split(os.pathsep)
        if value
    )
    if python_paths != (adata_source, ROOT.resolve()):
        return False, "PYTHONPATH is not exactly sealed adata plus pinned release"
    return True, "active pinned release and sealed adata identity verified"


def _print_runtime_identity_block(reason: str) -> None:
    print(json.dumps({
        "acceptance_status": "BLOCKED",
        "checklist": {"production_runtime_identity": False},
        "reason": reason,
    }, ensure_ascii=False, indent=2))


def _run_on_production_host() -> int:
    """Execute the verifier against the server's own runtime database.

    A workstation may point ``MYSQL_URL`` at the QMT-side evidence database.
    Calling a script named "production verifier" against that URL previously
    produced a false pass even when the public server lacked snapshot tables.
    """

    load_project_env()
    command = production_release_command(
        "tools/verify_trading_v3_production.py",
        ("--local-runtime",),
    )
    import paramiko

    client = production_ssh_client(paramiko)
    client.connect(**production_ssh_connect_kwargs(timeout=30))
    try:
        _stdin, stdout, stderr = client.exec_command(
            command,
            timeout=240,
        )
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        status = stdout.channel.recv_exit_status()
        if out:
            print(out, end="" if out.endswith("\n") else "\n")
        if err:
            print(err, file=sys.stderr, end="" if err.endswith("\n") else "\n")
        return status
    finally:
        client.close()


def main() -> int:
    load_project_env()
    primary = create_tool_engine()
    kline = get_kline_engine()
    try:
        v3_config = load_v3_config()
        current_config_hash = config_hash()
        repository = TradingV3Repository(primary)
        schema = repository.table_readiness()
        columns = (
            repository.production_column_readiness()
            if schema and all(schema.values())
            else {}
        )
        guard_status = (
            repository.real_trading_guard_readiness()
            if schema and all(schema.values())
            else {
                "account_insert": False,
                "account_update": False,
                "execution_plan_insert": False,
                "execution_plan_update": False,
            }
        )
        active_calibration_status = (
            repository.active_calibration_status()
            if schema and all(schema.values())
            else {"calibrations": {}, "rejections": {}}
        )
        accepted_calibrations = dict(
            active_calibration_status.get("calibrations") or {}
        )
        fourth_layer = collect_fourth_layer_readiness(
            primary,
            kline,
            config=v3_config,
            current_config_hash=current_config_hash,
        )
        evidence = {
            "checked_on": date.today(),
            "schema": schema,
            "schema_ready": bool(schema) and all(schema.values()),
            "production_columns": columns,
            "real_trading_guard_status": guard_status,
            "active_calibration_status": {
                "accepted": {
                    key: table.model_version
                    for key, table in accepted_calibrations.items()
                },
                "rejections": active_calibration_status.get(
                    "rejections"
                ) or {},
            },
            "fourth_layer": fourth_layer,
            "account": _safe_one(
                primary,
                """
                SELECT account_id, account_name, status, initial_cash,
                       cash_balance, peak_equity, fee_profile_version,
                       instrument_rule_version, real_trading_enabled,
                       updated_at
                FROM st_trade_account_v2
                WHERE account_id = 'paper-main-v2'
                """,
            ),
            "real_trading_database_guards": _safe_all(
                primary,
                """
                SELECT TRIGGER_NAME, EVENT_MANIPULATION,
                       ACTION_TIMING, EVENT_OBJECT_TABLE,
                       ACTION_STATEMENT
                FROM information_schema.TRIGGERS
                WHERE TRIGGER_SCHEMA = DATABASE()
                  AND TRIGGER_NAME IN (
                      'trg_trade_account_v2_real_disabled_bi',
                      'trg_trade_account_v2_real_disabled_bu',
                      'trg_execution_plan_v3_real_disabled_bi',
                      'trg_execution_plan_v3_real_disabled_bu'
                  )
                ORDER BY TRIGGER_NAME
                """,
            ),
            "fee_profiles": _safe_all(
                primary,
                """
                SELECT fee_profile_version, security_type,
                       buy_commission_rate, sell_commission_rate,
                       minimum_commission, stamp_tax_sell_rate,
                       transfer_fee_buy_rate, transfer_fee_sell_rate,
                       confirmation_status, effective_from
                FROM st_fee_profile_v2
                WHERE effective_to IS NULL
                ORDER BY security_type
                """,
            ),
            "active_models": _safe_all(
                primary,
                """
                SELECT model_id, strategy_key, model_version,
                       lifecycle_status, training_start, training_end,
                       validation_start, validation_end, dataset_hash,
                       activated_at
                FROM st_model_registry_v3
                WHERE lifecycle_status = 'PAPER_ACTIVE'
                ORDER BY strategy_key, model_version
                """,
            ),
            "latest_validation": repository.latest_validation(),
            "level1_capability": _safe_one(
                primary,
                """
                SELECT capability_code, status, protocol_version,
                       consecutive_trade_days, checked_at, passed_at
                FROM st_execution_capability_v2
                WHERE capability_code =
                      'B-003_RELIABLE_LEVEL1_BID_ASK'
                """,
            ),
            "canonical_decision_runs": _safe_all(
                primary,
                """
                SELECT d.run_uid, d.trade_date, d.decision_at, d.mode,
                       d.model_version, d.lifecycle_status, d.status,
                       d.dominant_regime, d.risk_asset_cap,
                       d.forecast_count, d.validated_count, d.target_count,
                       d.data_snapshot_hash, d.result_hash,
                       d.config_hash, d.code_commit_sha,
                       d.calibration_set_hash
                FROM st_decision_run_v3 d
                WHERE d.status = 'COMPLETED'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM st_decision_run_v3 d2
                      WHERE d2.trade_date = d.trade_date
                        AND d2.status = 'COMPLETED'
                        AND (
                            CASE d2.mode
                                WHEN 'close' THEN 0
                                WHEN 'premarket' THEN 1
                                ELSE 2
                            END
                            <
                            CASE d.mode
                                WHEN 'close' THEN 0
                                WHEN 'premarket' THEN 1
                                ELSE 2
                            END
                            OR (
                                CASE d2.mode
                                    WHEN 'close' THEN 0
                                    WHEN 'premarket' THEN 1
                                    ELSE 2
                                END
                                =
                                CASE d.mode
                                    WHEN 'close' THEN 0
                                    WHEN 'premarket' THEN 1
                                    ELSE 2
                                END
                                AND (
                                    d2.decision_at > d.decision_at
                                    OR (
                                        d2.decision_at = d.decision_at
                                        AND d2.run_uid > d.run_uid
                                    )
                                )
                            )
                        )
                  )
                ORDER BY d.trade_date DESC
                LIMIT 5
                """,
            ),
            "latest_runtime_decision": _safe_one(
                primary,
                """
                SELECT run_uid, trade_date, decision_at, mode,
                       model_version, lifecycle_status, status,
                       dominant_regime, risk_asset_cap, forecast_count,
                       validated_count, target_count, data_snapshot_hash,
                       result_hash, config_hash, code_commit_sha,
                       calibration_set_hash
                FROM st_decision_run_v3
                ORDER BY decision_at DESC, created_at DESC
                LIMIT 1
                """,
            ),
            "latest_forecast_breakdown": _safe_all(
                primary,
                """
                SELECT f.trade_date, f.strategy_key, f.forecast_status,
                       COUNT(*) AS row_count,
                       MIN(f.valid_until) AS first_valid_until,
                       MAX(f.valid_until) AS last_valid_until
                FROM st_alpha_forecast_v3 f
                JOIN (
                    SELECT d.run_uid
                    FROM st_decision_run_v3 d
                    ORDER BY d.decision_at DESC, d.created_at DESC
                    LIMIT 1
                ) latest ON latest.run_uid = f.run_uid
                GROUP BY f.trade_date, f.strategy_key, f.forecast_status
                ORDER BY f.strategy_key, f.forecast_status
                """,
            ),
            "latest_theme_signal_coverage": _safe_one(
                primary,
                """
                SELECT COUNT(*) AS retained_signal_count,
                       COUNT(DISTINCT theme_code)
                           AS exact_theme_count,
                       COUNT(DISTINCT CONCAT(
                           strategy_key, '|', theme_code
                       )) AS exact_theme_strategy_group_count,
                       COUNT(DISTINCT stock_code)
                           AS covered_stock_count,
                       COUNT(DISTINCT strategy_key)
                           AS covered_strategy_count,
                       SUM(selected_as_primary)
                           AS selected_primary_count,
                       SUM(CASE
                           WHEN theme_name REGEXP
                               '人工智能|AI|机器人'
                           THEN 1 ELSE 0
                       END) AS ai_robot_example_signal_count,
                       SUM(CASE
                           WHEN theme_name NOT REGEXP
                               '人工智能|AI|机器人'
                           THEN 1 ELSE 0
                       END) AS non_ai_robot_signal_count
                FROM st_theme_signal_v3 s
                JOIN (
                    SELECT d.run_uid
                    FROM st_decision_run_v3 d
                    ORDER BY d.decision_at DESC, d.created_at DESC
                    LIMIT 1
                ) latest ON latest.run_uid = s.run_uid
                """,
            ),
            "latest_non_ai_robot_theme_examples": _safe_all(
                primary,
                """
                SELECT theme_code, theme_name,
                       COUNT(*) AS retained_signal_count,
                       COUNT(DISTINCT strategy_key)
                           AS strategy_count,
                       COUNT(DISTINCT stock_code) AS stock_count,
                       SUM(selected_as_primary)
                           AS selected_primary_count,
                       MAX(raw_score) AS maximum_raw_score
                FROM st_theme_signal_v3 s
                JOIN (
                    SELECT d.run_uid
                    FROM st_decision_run_v3 d
                    ORDER BY d.decision_at DESC, d.created_at DESC
                    LIMIT 1
                ) latest ON latest.run_uid = s.run_uid
                WHERE theme_name NOT REGEXP '人工智能|AI|机器人'
                GROUP BY theme_code, theme_name
                ORDER BY selected_primary_count DESC,
                         maximum_raw_score DESC,
                         theme_code
                LIMIT 30
                """,
            ),
            "forward_exit_allocation": _safe_one(
                primary,
                _FORWARD_EXIT_ALLOCATION_HEALTH_SQL,
            ),
            "portfolio_and_execution": _safe_one(
                primary,
                """
                SELECT
                    (SELECT COUNT(*) FROM st_target_portfolio_v3)
                        AS v3_target_count,
                    (SELECT COUNT(*) FROM st_position_state_v3
                     WHERE account_id = 'paper-main-v2'
                       AND quantity > 0) AS v3_open_position_count,
                    (SELECT COUNT(*) FROM st_execution_plan_v3)
                        AS v3_execution_plan_count,
                    (SELECT COUNT(*) FROM st_execution_plan_v3
                     WHERE real_order_allowed <> 0)
                        AS real_order_allowed_count,
                    (SELECT COUNT(*)
                     FROM st_forward_trade_evidence_v3)
                        AS forward_evidence_count,
                    (SELECT COUNT(*)
                     FROM st_forward_trade_evidence_v3
                     WHERE evidence_status = 'MATURED')
                        AS matured_forward_trade_count,
                    (SELECT COUNT(*)
                     FROM st_forward_trade_evidence_v3 e
                     LEFT JOIN st_fill_v2 f
                       ON f.fill_id = e.entry_fill_id
                     LEFT JOIN st_order_v2 o
                       ON o.order_id = e.entry_order_id
                     LEFT JOIN st_trade_intent_v2 i
                       ON i.intent_id = e.source_intent_id
                     LEFT JOIN st_alpha_forecast_v3 a
                       ON a.forecast_id = e.source_forecast_id
                     WHERE e.evidence_kind <> 'EXECUTED_PAPER'
                        OR e.protocol_version <>
                           'PAPER_EXECUTED_LEDGER_V1'
                        OR e.sample_owner_role <> 'PRIMARY'
                        OR e.attribution_status NOT IN (
                           'VERIFIED_SNAPSHOT',
                           'LEGACY_VERSION_DERIVED',
                           'LEGACY_SINGLE_STRATEGY_RESOLVED'
                        )
                        OR CHAR_LENGTH(e.ownership_hash) <> 64
                        OR f.fill_id IS NULL
                        OR o.order_id IS NULL
                        OR i.intent_id IS NULL
                        OR i.decision_run_uid <> e.source_run_uid
                        OR a.forecast_id IS NULL
                        OR a.run_uid <> e.source_run_uid
                        OR a.stock_code <> e.stock_code
                        OR a.strategy_key <> e.strategy_key
                        OR e.ownership_hash <> SHA2(CONCAT(
                           e.source_run_uid, '|',
                           e.source_forecast_id, '|',
                           e.stock_code, '|', e.strategy_key
                        ), 256))
                        AS invalid_forward_evidence_count,
                    (SELECT COUNT(*)
                     FROM st_fill_v2 f
                     JOIN st_order_v2 o ON o.order_id = f.order_id
                     JOIN st_trade_intent_v2 i
                       ON i.intent_id = o.intent_id
                     LEFT JOIN st_forward_trade_evidence_v3 e
                       ON e.entry_fill_id = f.fill_id
                     WHERE f.account_id = 'paper-main-v2'
                       AND f.side = 'BUY'
                       AND i.reason_code IN (
                           'V3_PAPER_DISCOVERY',
                           'V3_VALIDATED_POSITIVE'
                       )
                       AND e.evidence_id IS NULL)
                        AS unattributed_v3_buy_fill_count,
                    (SELECT COUNT(*)
                     FROM st_target_portfolio_v3 t
                     JOIN (
                         SELECT run_uid
                         FROM st_decision_run_v3
                         ORDER BY decision_at DESC, created_at DESC
                         LIMIT 1
                     ) latest ON latest.run_uid = t.run_uid
                     LEFT JOIN st_alpha_forecast_v3 a
                       ON a.forecast_id = t.primary_forecast_id
                     WHERE t.primary_strategy_key = ''
                        OR t.primary_forecast_id = ''
                        OR CHAR_LENGTH(
                           t.attribution_snapshot_hash
                        ) <> 64
                        OR a.forecast_id IS NULL
                        OR a.run_uid <> t.run_uid
                        OR a.stock_code <> t.stock_code
                        OR a.strategy_key <>
                           t.primary_strategy_key)
                        AS invalid_latest_target_owner_count,
                    (SELECT COUNT(*)
                     FROM st_counterfactual_v3
                     WHERE evidence_kind <> 'SHADOW'
                        OR execution_status <> 'NOT_APPLICABLE')
                        AS invalid_shadow_evidence_count,
                    (SELECT COUNT(*)
                     FROM st_opportunity_recall_v3
                     WHERE evidence_kind <> 'SHADOW'
                        OR protocol_version NOT IN (
                           'COUNTERFACTUAL_TECHNICAL_PROXY_V1',
                           'COUNTERFACTUAL_TECHNICAL_PROXY_V2'
                        )) AS invalid_recall_evidence_count,
                    (SELECT COUNT(*)
                     FROM (
                         SELECT source_forecast_id
                         FROM st_counterfactual_v3
                         GROUP BY source_forecast_id
                         HAVING COUNT(*) > 1
                     ) duplicate_counterfactual)
                        AS duplicate_counterfactual_source_count,
                    (SELECT COUNT(*)
                     FROM st_shadow_portfolio_v3)
                        AS shadow_portfolio_row_count,
                    (SELECT COUNT(*)
                     FROM st_theme_signal_v3)
                        AS theme_signal_row_count,
                    (SELECT COUNT(*)
                     FROM st_theme_signal_v3
                     WHERE selected_as_primary = 1)
                        AS selected_theme_signal_row_count,
                    (SELECT COUNT(*)
                     FROM st_shadow_portfolio_v3
                     WHERE result_status = 'MATURED')
                        AS matured_shadow_portfolio_row_count,
                    (SELECT COUNT(DISTINCT strategy_key)
                     FROM st_shadow_portfolio_v3
                     WHERE portfolio_kind = 'STRATEGY')
                        AS shadow_strategy_count,
                    (SELECT COUNT(DISTINCT group_key)
                     FROM st_shadow_portfolio_v3
                     WHERE portfolio_kind = 'THEME')
                        AS shadow_theme_group_count,
                    (SELECT COUNT(*)
                     FROM st_shadow_portfolio_v3
                     WHERE strategy_key =
                           'weak_market_structural_mainline')
                        AS weak_market_structural_shadow_count,
                    (SELECT COUNT(*)
                     FROM st_shadow_portfolio_v3 s
                     LEFT JOIN st_alpha_forecast_v3 f
                       ON f.forecast_id = s.source_forecast_id
                     LEFT JOIN st_theme_signal_v3 ts
                       ON ts.theme_signal_id =
                          s.source_theme_signal_id
                     WHERE s.evidence_kind <> 'SHADOW'
                        OR s.order_allowed <> 0
                        OR s.can_activate_model <> 0
                        OR f.forecast_id IS NULL
                        OR f.run_uid <> s.run_uid
                        OR f.stock_code <> s.stock_code
                        OR f.strategy_key <> s.strategy_key
                        OR (
                           s.portfolio_kind = 'STRATEGY'
                           AND (
                               s.source_theme_signal_id <> ''
                               OR s.strategy_result_key <> SHA2(CONCAT(
                                  s.run_uid, '|', s.stock_code, '|',
                                  s.strategy_key, '|', s.horizon_days
                               ), 256)
                           )
                        )
                        OR (
                           s.portfolio_kind = 'THEME'
                           AND s.protocol_version =
                               'V3_THEME_SIGNAL_LEDGER_V2'
                           AND (
                               ts.theme_signal_id IS NULL
                               OR ts.run_uid <> s.run_uid
                               OR ts.stock_code <> s.stock_code
                               OR ts.strategy_key <> s.strategy_key
                               OR JSON_CONTAINS(
                                  ts.theme_cluster_keys_json,
                                  JSON_QUOTE(s.group_key)
                               ) = 0
                               OR s.strategy_result_key <> SHA2(CONCAT(
                                  s.run_uid, '|', s.stock_code, '|',
                                  s.strategy_key, '|', s.horizon_days, '|',
                                  ts.theme_feature_key, '|', s.group_key
                               ), 256)
                           )
                        ))
                        AS invalid_shadow_portfolio_count,
                    (SELECT COUNT(*)
                     FROM (
                         SELECT stock_code
                         FROM st_position_lot_v2
                         WHERE account_id = 'paper-main-v2'
                           AND remaining_quantity > 0
                         GROUP BY stock_code
                         UNION
                         SELECT stock_code
                         FROM st_order_v2
                         WHERE account_id = 'paper-main-v2'
                           AND side = 'BUY'
                           AND status IN (
                               'CREATED', 'RISK_APPROVED', 'QUEUED',
                               'PARTIALLY_FILLED'
                           )
                         GROUP BY stock_code
                     ) actual_paper_stocks)
                        AS actual_paper_live_stock_count,
                    (SELECT COUNT(*) FROM st_order_v2
                     WHERE account_id = 'paper-main-v2'
                       AND status IN (
                           'CREATED', 'RISK_APPROVED', 'QUEUED',
                           'PARTIALLY_FILLED'
                       )) AS active_v2_order_count,
                    (
                        SELECT COUNT(*)
                        FROM st_execution_plan_v3 p
                        JOIN st_trade_intent_v2 i
                          ON i.decision_run_uid = p.run_uid
                         AND i.stock_code = p.stock_code
                         AND i.action = p.side
                        JOIN st_order_v2 o
                          ON o.intent_id = i.intent_id
                         AND o.side = p.side
                        WHERE p.state IN (
                            'PAPER_QUEUED',
                            'PAPER_PARTIALLY_FILLED'
                        )
                          AND o.status IN (
                              'FILLED', 'CANCELLED',
                              'EXPIRED', 'REJECTED'
                          )
                    ) AS stale_execution_plan_state_count,
                    (
                        SELECT COUNT(DISTINCT f.stock_code)
                        FROM st_alpha_forecast_v3 f
                        JOIN (
                            SELECT run_uid
                            FROM st_decision_run_v3
                            WHERE status = 'COMPLETED'
                            ORDER BY decision_at DESC, created_at DESC
                            LIMIT 1
                        ) latest_forecast
                          ON latest_forecast.run_uid = f.run_uid
                        WHERE f.stock_code LIKE '92%'
                    ) AS latest_bse_forecast_stock_count,
                    (SELECT COUNT(*) FROM st_position_lot_v2
                     WHERE account_id = 'paper-main-v2'
                       AND remaining_quantity > 0) AS v2_open_lot_count,
                    (
                        SELECT COUNT(*)
                        FROM st_strategy_version_v2
                        WHERE strategy_id IN (
                            'sector_preheat',
                            'intraday_dynamic_activation'
                        )
                          AND lifecycle_status IN (
                              'PAPER_TRIAL',
                              'PAPER_ACTIVE'
                          )
                    ) AS active_legacy_entry_strategy_count,
                    (
                        SELECT COUNT(*)
                        FROM st_order_v2 old_order
                        JOIN st_trade_intent_v2 old_intent
                          ON old_intent.intent_id =
                             old_order.intent_id
                        WHERE old_order.account_id =
                              'paper-main-v2'
                          AND old_order.side = 'BUY'
                          AND old_order.filled_quantity = 0
                          AND old_order.status IN (
                              'CREATED',
                              'RISK_APPROVED',
                              'QUEUED'
                          )
                          AND (
                              old_intent.strategy_version
                                  LIKE 'stock_strategy_v2.%%'
                              OR old_intent.strategy_version
                                  LIKE 'sector_preheat_%%'
                              OR old_intent.strategy_version
                                  LIKE
                                  'intraday_dynamic_activation_%%'
                          )
                    ) AS active_legacy_buy_order_count
                """,
            ),
            "scheduler_tasks": _safe_all(
                primary,
                """
                SELECT id, task_name, task_type, cron_time,
                       interval_minutes, enabled,
                       script_path, script_args, date_param,
                       last_run_status, last_run_at, last_triggered_at,
                       last_run_duration,
                       RIGHT(COALESCE(last_run_output, ''), 1200)
                           AS output_tail
                FROM st_scheduled_tasks
                WHERE task_type IN (
                    'stock_kline',
                    'trading_v3_close_decision',
                    'trading_v3_premarket_review',
                    'trading_v3_counterfactual_audit',
                    'trading_v3_continuous_calibration',
                    'trading_v2_premarket_decision',
                    'trading_v2_intraday_activation',
                    'trading_v2_close_decision',
                    'etf_forward_daily',
                    'trading_v2_level1_validation',
                    'intraday_realtime',
                    'qmt_membership_snapshot'
                )
                ORDER BY id
                """,
            ),
            "forward_evidence": {
                "etf": _safe_one(
                    primary,
                    """
                    SELECT COUNT(*) AS observation_count,
                           MIN(data_date) AS first_data_date,
                           MAX(data_date) AS latest_data_date
                    FROM st_etf_forward_observation
                    """,
                ),
                "level1": _safe_one(
                    kline,
                    """
                    SELECT COUNT(*) AS receipt_count,
                           COUNT(DISTINCT trade_date) AS trade_day_count,
                           MIN(trade_date) AS first_trade_date,
                           MAX(trade_date) AS latest_trade_date,
                           SUM(quality_status = 'PASS') AS pass_count
                    FROM st_qmt_minute_sync_receipt_v2
                    """,
                ),
                "membership": _safe_one(
                    kline,
                    """
                    SELECT COUNT(*) AS run_count,
                           MIN(snapshot_date) AS first_snapshot_date,
                           MAX(snapshot_date) AS latest_snapshot_date,
                           SUM(concept_relation_count)
                               AS concept_relation_count,
                           SUM(industry_relation_count)
                               AS industry_relation_count
                    FROM qmt_membership_snapshot_run
                    WHERE quality_status = 'QMT_VALIDATED'
                    """,
                ),
                "production_membership": _safe_one(
                    primary,
                    """
                    SELECT COUNT(*) AS run_count,
                           MIN(snapshot_date) AS first_snapshot_date,
                           MAX(snapshot_date) AS latest_snapshot_date,
                           SUM(concept_relation_count)
                               AS concept_relation_count,
                           SUM(industry_relation_count)
                               AS industry_relation_count
                    FROM qmt_membership_snapshot_run
                    WHERE quality_status = 'QMT_VALIDATED'
                    """,
                ),
            },
            "data_evidence": {
                "trade_calendar": _safe_one(
                    primary,
                    """
                    SELECT MAX(trade_date) AS expected_trade_date
                    FROM si_trade_calendar
                    WHERE trade_status = 1
                      AND trade_date <= CASE
                          WHEN CURRENT_TIME() >= '15:10:00'
                          THEN CURRENT_DATE()
                          ELSE DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
                      END
                    """,
                ),
                "daily_kline": _safe_one(
                    kline,
                    """
                    SELECT MAX(trade_date) AS latest_trade_date,
                           COUNT(DISTINCT CASE
                               WHEN trade_date = (
                                   SELECT MAX(trade_date)
                                   FROM sm_stock_kline
                                   WHERE k_type = 1
                               )
                               THEN stock_code END
                           ) AS latest_code_count
                    FROM sm_stock_kline
                    WHERE k_type = 1
                    """,
                ),
                "qmt_attestation": _safe_one(
                    kline,
                    """
                    SELECT run_id, start_date, end_date, status,
                           target_rows, qmt_rows, matched_rows,
                           missing_qmt_rows, mismatched_rows,
                           already_attested_rows, updated_rows,
                           started_at, finished_at
                    FROM qmt_kline_attestation_run
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                ),
            },
            "counterfactual": {
                "queue": _safe_call(
                    counterfactual_queue_stats,
                    primary,
                ),
                "ledger": _safe_one(
                    primary,
                    """
                    SELECT COUNT(*) AS row_count,
                           MAX(outcome_date) AS latest_outcome_date,
                           SUM(missed_opportunity) AS missed_count,
                           SUM(false_positive) AS false_positive_count
                    FROM st_counterfactual_v3
                    """,
                ),
                "misattributed_acceptance": _safe_one(
                    primary,
                    """
                    SELECT COUNT(*) AS row_count
                    FROM st_counterfactual_v3 c
                    JOIN st_alpha_forecast_v3 f
                      ON f.forecast_id = c.source_forecast_id
                    JOIN st_target_portfolio_v3 t
                      ON t.run_uid = f.run_uid
                     AND t.stock_code = f.stock_code
                    WHERE c.accepted = 1
                      AND JSON_CONTAINS(
                          COALESCE(t.strategy_keys_json, '[]'),
                          JSON_QUOTE(c.strategy_key)
                      ) = 0
                    """,
                ),
                "latest_recall": repository.latest_opportunity_recall(),
            },
        }

        account = evidence["account"]
        guards = evidence["real_trading_database_guards"]
        portfolio = evidence["portfolio_and_execution"]
        forward_exit_allocation = evidence["forward_exit_allocation"]
        validation = evidence["latest_validation"] or {}
        active_models = evidence["active_models"]
        decision_runs = evidence["canonical_decision_runs"]
        latest_runtime = evidence["latest_runtime_decision"]
        data = evidence["data_evidence"]
        source_membership = evidence["forward_evidence"]["membership"]
        production_membership = evidence["forward_evidence"][
            "production_membership"
        ]
        current_version = str(v3_config["strategy_version"])
        quality_policy = v3_config.get("data_quality", {})
        expected_trade_date = data["trade_calendar"].get(
            "expected_trade_date"
        )
        latest_concept_snapshot = production_membership.get(
            "latest_snapshot_date"
        )
        concept_snapshot_age = (
            (expected_trade_date - latest_concept_snapshot).days
            if isinstance(expected_trade_date, date)
            and isinstance(latest_concept_snapshot, date)
            else None
        )
        evidence["data_evidence"]["concept_snapshot_age_days"] = (
            concept_snapshot_age
        )
        accepted_model_versions = {
            table.model_version
            for table in accepted_calibrations.values()
        }
        scheduler_tasks = evidence["scheduler_tasks"]
        counterfactual_tasks = [
            item
            for item in scheduler_tasks
            if item.get("task_type")
            == "trading_v3_counterfactual_audit"
        ]
        continuous_calibration_tasks = [
            item
            for item in scheduler_tasks
            if item.get("task_type")
            == "trading_v3_continuous_calibration"
        ]
        level1_capture_tasks = [
            item
            for item in scheduler_tasks
            if item.get("task_type") == "intraday_realtime"
        ]
        level1_validation_tasks = [
            item
            for item in scheduler_tasks
            if item.get("task_type")
            == "trading_v2_level1_validation"
        ]
        level1_capability = evidence["level1_capability"]
        scheduler_evaluations = dict(
            dict(fourth_layer.get("scheduler") or {}).get(
                "task_evaluations"
            ) or {}
        )
        account_query_ok = bool(account) and _query_ok(account)
        portfolio_query_ok = bool(portfolio) and _query_ok(portfolio)
        forward_exit_allocation_ok = (
            _forward_exit_allocation_health_valid(
                forward_exit_allocation
            )
        )
        legacy_task_types = {
            "trading_v2_premarket_decision",
            "trading_v2_intraday_activation",
            "trading_v2_close_decision",
        }
        checklist = {
            **dict(fourth_layer.get("checklist") or {}),
            "schema_ready": evidence["schema_ready"],
            "production_columns_ready": (
                bool(columns) and all(columns.values())
            ),
            "paper_account_active": account.get("status") == "ACTIVE",
            "real_trading_off": int(
                account.get("real_trading_enabled") or 0
            ) == 0 and account_query_ok,
            "database_real_trade_guards_present": (
                all(guard_status.values())
                and _real_trading_guard_rows_valid(guards)
            ),
            "active_oos_model_present": bool(
                accepted_calibrations
            ),
            "latest_validation_pass": (
                validation.get("result_status") == "PASS"
                and validation.get("model_version")
                in accepted_model_versions
            ),
            "latest_decision_completed": (
                bool(decision_runs)
                and latest_runtime.get("status") == "COMPLETED"
                and latest_runtime.get("model_version") == current_version
                and _query_ok(decision_runs)
            ),
            "latest_decision_provenance_complete": (
                latest_runtime.get("config_hash")
                == current_config_hash
                and str(
                    latest_runtime.get("code_commit_sha") or ""
                )
                not in {"", "UNKNOWN", "WORKTREE"}
                and len(
                    str(
                        latest_runtime.get(
                            "calibration_set_hash"
                        )
                        or ""
                    )
                )
                == 64
            ),
            "legacy_v2_entry_routes_disabled": (
                _query_ok(scheduler_tasks)
                and all(
                    int(item.get("enabled") or 0) == 0
                    for item in scheduler_tasks
                    if item.get("task_type") in legacy_task_types
                )
            ),
            "legacy_v2_entry_state_retired": (
                portfolio_query_ok
                and int(
                    portfolio.get(
                        "active_legacy_entry_strategy_count"
                    )
                    or 0
                )
                == 0
                and int(
                    portfolio.get(
                        "active_legacy_buy_order_count"
                    )
                    or 0
                )
                == 0
            ),
            "no_real_orders": int(
                portfolio.get("real_order_allowed_count") or 0
            ) == 0 and _query_ok(portfolio),
            "actual_paper_positions_within_configured_risk_cap": int(
                portfolio.get("actual_paper_live_stock_count") or 0
            ) <= int(
                load_v3_config().get("paper_execution", {}).get(
                    "maximum_live_positions",
                    12,
                )
            ) and portfolio_query_ok,
            "kline_current": (
                _query_ok(data["trade_calendar"])
                and _query_ok(data["daily_kline"])
                and data["trade_calendar"].get("expected_trade_date")
                is not None
                and data["daily_kline"].get("latest_trade_date")
                is not None
                and data["trade_calendar"].get("expected_trade_date")
                == data["daily_kline"].get("latest_trade_date")
            ),
            "qmt_attestation_clean": (
                data["qmt_attestation"].get("status") == "COMPLETED"
                and data["qmt_attestation"].get("start_date") is not None
                and data["qmt_attestation"].get("end_date") is not None
                and data["trade_calendar"].get("expected_trade_date")
                is not None
                and data["qmt_attestation"].get("start_date")
                <= data["trade_calendar"].get("expected_trade_date")
                and data["qmt_attestation"].get("end_date")
                >= data["trade_calendar"].get("expected_trade_date")
                and int(
                    data["qmt_attestation"].get("target_rows") or 0
                ) > 0
                and int(
                    data["qmt_attestation"].get("matched_rows") or 0
                )
                == int(
                    data["qmt_attestation"].get("target_rows") or 0
                )
                and int(
                    data["qmt_attestation"].get("missing_qmt_rows") or 0
                ) == 0
                and int(
                    data["qmt_attestation"].get("mismatched_rows") or 0
                ) == 0
            ),
            "qmt_membership_promoted_to_production": (
                source_membership.get("latest_snapshot_date")
                == production_membership.get("latest_snapshot_date")
                and int(
                    production_membership.get("run_count") or 0
                ) > 0
            ),
            "concept_snapshot_fresh": (
                concept_snapshot_age is not None
                and 0 <= concept_snapshot_age <= int(
                    quality_policy.get(
                        "maximum_concept_snapshot_age_days",
                        5,
                    )
                )
            ),
            "bse_in_latest_forecast_universe": int(
                portfolio.get("latest_bse_forecast_stock_count") or 0
            ) > 0,
            "execution_plan_state_consistent": int(
                portfolio.get("stale_execution_plan_state_count") or 0
            ) == 0 and portfolio_query_ok,
            "forward_exit_allocation_ledger_conserved": (
                forward_exit_allocation_ok
            ),
            "forward_learning_uses_executed_fills_only": (
                portfolio_query_ok
                and forward_exit_allocation_ok
                and int(
                    portfolio.get("invalid_forward_evidence_count") or 0
                ) == 0
                and int(
                    portfolio.get("unattributed_v3_buy_fill_count") or 0
                ) == 0
                and int(
                    portfolio.get("invalid_latest_target_owner_count") or 0
                ) == 0
                and int(
                    portfolio.get("invalid_shadow_evidence_count") or 0
                ) == 0
                and int(
                    portfolio.get("invalid_recall_evidence_count") or 0
                ) == 0
                and bool(
                    load_v3_config().get("forward_learning", {}).get(
                        "executed_evidence_only"
                    )
                )
                and not bool(
                    load_v3_config().get("forward_learning", {}).get(
                        "shadow_evidence_can_activate_model"
                    )
                )
                and bool(
                    load_v3_config().get("forward_learning", {}).get(
                        "one_primary_owner_per_entry_fill"
                    )
                )
                and bool(
                    load_v3_config().get("forward_learning", {}).get(
                        "ambiguous_ownership_is_quarantined"
                    )
                )
            ),
            "strategy_result_keys_unique": int(
                portfolio.get(
                    "duplicate_counterfactual_source_count"
                )
                or 0
            ) == 0 and portfolio_query_ok,
            "shadow_portfolios_isolated": (
                int(portfolio.get("shadow_portfolio_row_count") or 0) > 0
                and int(
                    portfolio.get("theme_signal_row_count") or 0
                ) > 0
                and int(
                    portfolio.get("selected_theme_signal_row_count") or 0
                ) > 0
                and int(
                    portfolio.get("shadow_strategy_count") or 0
                ) > 0
                and int(
                    portfolio.get("shadow_theme_group_count") or 0
                ) > 0
                and int(
                    portfolio.get("invalid_shadow_portfolio_count") or 0
                ) == 0
                and not bool(
                    load_v3_config().get("shadow_portfolios", {}).get(
                        "order_allowed"
                    )
                )
                and not bool(
                    load_v3_config().get("shadow_portfolios", {}).get(
                        "can_activate_model"
                    )
                )
            ),
            "weak_market_structural_shadow_sampling_active": int(
                portfolio.get("weak_market_structural_shadow_count") or 0
            ) > 0,
            "oos_profit_gate_not_lowered": (
                int(
                    load_v3_config().get("profit_gate", {}).get(
                        "minimum_oos_samples", 0
                    )
                ) >= 80
                and float(
                    load_v3_config().get("profit_gate", {}).get(
                        "minimum_profit_factor", 0
                    )
                ) >= 1.3
                and float(
                    load_v3_config().get("profit_gate", {}).get(
                        "minimum_payoff_ratio", 0
                    )
                ) >= 1.0
                and int(
                    load_v3_config().get("profit_gate", {}).get(
                        "minimum_portfolio_trades", 0
                    )
                ) >= 80
                and float(
                    load_v3_config().get("profit_gate", {}).get(
                        "minimum_portfolio_profit_factor", 0
                    )
                ) >= 1.3
            ),
            "counterfactual_strategy_attribution_clean": int(
                evidence["counterfactual"][
                    "misattributed_acceptance"
                ].get("row_count")
                or 0
            ) == 0 and _query_ok(
                evidence["counterfactual"]["misattributed_acceptance"]
            ),
            "counterfactual_backlog_drained": int(
                evidence["counterfactual"]["queue"].get(
                    "eligible_due_count"
                )
                or 0
            ) == 0 and _query_ok(evidence["counterfactual"]["queue"]),
            "counterfactual_drain_scheduler_ready": (
                _query_ok(counterfactual_tasks)
                and bool(
                    dict(scheduler_evaluations.get(
                        "trading_v3_counterfactual_audit"
                    ) or {}).get("ready")
                )
            ),
            "continuous_calibration_scheduler_ready": (
                _query_ok(continuous_calibration_tasks)
                and bool(
                    dict(scheduler_evaluations.get(
                        "trading_v3_continuous_calibration"
                    ) or {}).get("ready")
                )
            ),
            "level1_continuous_collection_route_ready": (
                _query_ok(level1_capture_tasks)
                and _query_ok(level1_validation_tasks)
                and any(
                    int(item.get("enabled") or 0) == 1
                    and int(item.get("interval_minutes") or 0) == 1
                    and str(item.get("script_path") or "")
                    == "tools/sync_qmt_primary.py"
                    and "realtime" in str(
                        item.get("script_args") or ""
                    )
                    for item in level1_capture_tasks
                )
                and any(
                    int(item.get("enabled") or 0) == 1
                    and str(item.get("script_path") or "")
                    == "tools/validate_trading_v2_level1.py"
                    for item in level1_validation_tasks
                )
            ),
            "level1_five_day_continuity_pass": (
                level1_capability.get("status") == "PASS"
                and level1_capability.get("protocol_version")
                == "level1_continuity_v2.0.0"
                and int(
                    level1_capability.get(
                        "consecutive_trade_days"
                    )
                    or 0
                ) >= 5
            ),
            "all_acceptance_queries_succeeded": _query_ok(evidence),
        }
        evidence["checklist"] = checklist
        evidence["acceptance_status"] = (
            "PASS" if all(checklist.values()) else "BLOCKED"
        )
        print(json.dumps(
            evidence,
            ensure_ascii=False,
            indent=2,
            default=_json_default,
        ))
        return 0 if evidence["acceptance_status"] == "PASS" else 2
    finally:
        primary.dispose()
        kline.dispose()


if __name__ == "__main__":
    if "--local-runtime" in sys.argv:
        sys.argv.remove("--local-runtime")
        identity_ready, identity_reason = _local_production_runtime_identity()
        if not identity_ready:
            _print_runtime_identity_block(identity_reason)
            raise SystemExit(2)
        raise SystemExit(main())
    if _is_production_runtime():
        identity_ready, identity_reason = _local_production_runtime_identity()
        if not identity_ready:
            _print_runtime_identity_block(identity_reason)
            raise SystemExit(2)
        raise SystemExit(main())
    raise SystemExit(_run_on_production_host())
