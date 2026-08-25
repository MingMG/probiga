# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import importlib.util
import logging
import os
from pathlib import Path
import re
import secrets
import subprocess
from time import monotonic

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from sqlalchemy.engine import make_url

from server.api.admin_auth import admin_auth_status
from server.api.scheduler_runtime import (
    _detached_job_log_root,
    _open_detached_job_log,
    scheduler_runtime_info,
)
from server.common.scheduler_authority import (
    PRODUCTION_SCHEDULER_SERVICE,
    scheduler_authority_contract,
)
from server.api.routers._engine import get_engine
from server.common.scheduler_script_policy import (
    SchedulerScriptPolicyError,
    resolve_scheduler_script,
)
from server.common.scheduler_runtime_health import (
    check_linux_standalone_scheduler_heartbeat,
)
from server.common.batch_db import quote_identifier
from server.common.adata_release import (
    ADATA_GIT_MARKER,
    ADATA_GIT_SHA_ENV,
    ADATA_SOURCE_ENV,
    ADATA_TREE_MARKER,
    ADATA_TREE_SHA_ENV,
    AdataReleaseError,
    ensure_adata_import_path,
    validate_adata_release_source,
)
from server.common.config import (
    get_current_mysql_url,
    get_gj_qmt_config,
    get_minute_mysql_pool_config,
    get_mysql_url,
)
from server.common.current_data import get_current_engine
from server.engine.strategy_funding_checkpoint import (
    FUNDING_CHECKPOINT_AUDIT_MAX_BYTES,
    FUNDING_CHECKPOINT_BATCH_MAX_BYTES,
    FUNDING_CHECKPOINT_BATCH_MAX_ROWS,
    FUNDING_CHECKPOINT_MANIFEST_MAX_BYTES,
    FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH,
    FUNDING_CHECKPOINT_TARGET_AVG_BYTES,
    FUNDING_CHECKPOINT_TOTAL_HARD_BYTES,
    FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES,
    validate_strategy_funding_checkpoint_schema,
)

try:
    from server.common.config import get_qmt_live_runtime_config as _get_qmt_live_runtime_config
except ImportError:
    def _get_qmt_live_runtime_config() -> dict[str, int | bool]:
        return {
            "enabled": False,
            "poll_seconds": 5,
            "idle_sleep_seconds": 30,
            "trading_hours_only": True,
            "candidate_limit": 60,
        }

router = APIRouter(tags=["health"])
_LOGGER = logging.getLogger(__name__)
_EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH = (
    "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde"
)
_EXPECTED_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH = (
    "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943"
)
_EXPECTED_METRIC_REVIEW_CONTRACT_HASH = (
    "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84"
)
_EXPECTED_METRIC_REVIEW_TRIGGER_NAMES = frozenset({
    "trg_strategy_metric_input_immutable_bd",
    "trg_strategy_metric_input_review_bu",
})
_EXPECTED_FUNDING_TABLE_COUNTS = {
    "st_strategy_funding_daily_fact": {
        "column_count": 29,
        "index_count": 9,
        "foreign_key_count": 3,
        "check_count": 7,
    },
    "st_strategy_funding_checkpoint": {
        "column_count": 46,
        "index_count": 12,
        "foreign_key_count": 7,
        "check_count": 13,
    },
}
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
_ROOT_SHADOW_SUFFIXES = (".py", ".pyw", ".pyc", ".pyd", ".so")
_BYTECODE_SCAN_ROOTS = (
    "server",
    "biz",
    "integrations",
    "tools",
    "scripts",
    "strategies",
    "versions",
)
_RELEASE_GIT_TIMEOUT_SECONDS = 15
_GIT_INDEX_ENTRY_RE = re.compile(
    r"^(?P<mode>[0-7]{6}) (?P<object>[0-9a-f]{40,64}) (?P<stage>[0-3])\t(?P<path>.*)$"
)


def _release_git_command(*args: str) -> list[str]:
    """Inspect the root-owned immutable checkout without global Git state."""
    return [
        "git",
        "-c",
        f"safe.directory={REPOSITORY_ROOT}",
        *args,
    ]


def _untracked_root_shadow_files(tracked: set[str]) -> tuple[str, ...]:
    """Find root-level import shadows, including files ignored by Git."""
    candidates: set[str] = set()
    for child in REPOSITORY_ROOT.iterdir():
        if child.is_file() and child.suffix.lower() in _ROOT_SHADOW_SUFFIXES:
            candidates.add(child.name)
        elif child.is_dir():
            init_file = child / "__init__.py"
            if init_file.is_file():
                candidates.add(init_file.relative_to(REPOSITORY_ROOT).as_posix())
            for pattern in ("*.pyc", "__init__*.pyd", "__init__*.so"):
                for compiled in child.glob(pattern):
                    candidates.add(
                        compiled.relative_to(REPOSITORY_ROOT).as_posix()
                    )
    for root_name in _BYTECODE_SCAN_ROOTS:
        code_root = REPOSITORY_ROOT / root_name
        if not code_root.is_dir():
            continue
        for pattern in ("*.pyc", "*.pyo"):
            candidates.update(
                item.relative_to(REPOSITORY_ROOT).as_posix()
                for item in code_root.rglob(pattern)
            )
    return tuple(sorted(candidates - tracked))


def _standalone_scheduler_status() -> dict[str, str | bool | int | None]:
    """Prove that the production standalone scheduler is active and enabled."""
    try:
        active_result = subprocess.run(
            ["systemctl", "is-active", PRODUCTION_SCHEDULER_SERVICE],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=5,
        )
        enabled_result = subprocess.run(
            ["systemctl", "is-enabled", PRODUCTION_SCHEDULER_SERVICE],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=5,
        )
        pid_result = subprocess.run(
            [
                "systemctl",
                "show",
                "--property=MainPID",
                "--value",
                PRODUCTION_SCHEDULER_SERVICE,
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=5,
        )
        load_state = ""
        if not active_result.stdout.strip() or not enabled_result.stdout.strip():
            load_result = subprocess.run(
                [
                    "systemctl",
                    "show",
                    "--property=LoadState",
                    "--value",
                    PRODUCTION_SCHEDULER_SERVICE,
                ],
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="strict",
                timeout=5,
            )
            load_state = load_result.stdout.strip().lower()
    except (OSError, subprocess.SubprocessError, UnicodeError) as exc:
        _LOGGER.exception("standalone scheduler status probe failed")
        return {
            "verified": False,
            "active": None,
            "state": None,
            "enabled": None,
            "enablement_state": None,
            "pid": None,
            "error": "standalone scheduler status probe failed",
            "error_code": "scheduler_status_probe_failed",
        }

    state = active_result.stdout.strip().lower()
    active_returncode = active_result.returncode
    enablement_state = enabled_result.stdout.strip().lower()
    enablement_returncode = enabled_result.returncode
    pid_text = pid_result.stdout.strip()
    try:
        pid = int(pid_text)
    except (TypeError, ValueError):
        pid = None
    if load_state == "not-found":
        if not state:
            state = "unknown"
            active_returncode = 4
        if not enablement_state:
            enablement_state = "not-found"
            enablement_returncode = 1
    non_active_states = {
        "activating",
        "reloading",
        "deactivating",
        "inactive",
        "failed",
        "unknown",
    }
    if state == "active" and active_returncode == 0:
        active: bool | None = True
        active_verified = True
    elif state in non_active_states and active_returncode in {0, 3, 4}:
        active = False
        active_verified = True
    else:
        active = None
        active_verified = False

    non_persistent_enablement_states = {
        "enabled-runtime",
        "linked",
        "linked-runtime",
        "alias",
        "indirect",
        "generated",
        "transient",
        "disabled",
        "masked",
        "masked-runtime",
        "static",
        "not-found",
    }
    if enablement_state == "enabled" and enablement_returncode == 0:
        enabled = True
        enablement_verified = True
    elif (
        enablement_state in non_persistent_enablement_states
        and enablement_returncode in {0, 1, 3, 4}
    ):
        enabled = False
        enablement_verified = True
    else:
        enabled = None
        enablement_verified = False

    pid_verified = pid is not None and pid > 0 and pid_result.returncode == 0
    verified = active_verified and enablement_verified and pid_verified
    error = None
    if not active_verified:
        error = f"systemctl_is_active_exit_{active_returncode}"
    elif not enablement_verified:
        error = f"systemctl_is_enabled_exit_{enablement_returncode}"
    elif active is not True:
        error = "standalone_scheduler_inactive"
    elif enabled is not True:
        error = "standalone_scheduler_disabled"
    elif not pid_verified:
        error = "standalone_scheduler_pid_unverified"
    return {
        "verified": verified,
        "active": active,
        "state": state or None,
        "enabled": enabled,
        "enablement_state": enablement_state or None,
        "pid": pid,
        "error": error,
    }


def _detached_job_log_readiness() -> dict[str, object]:
    """Prove that detached workers can securely create and fsync a log."""

    probe_path: Path | None = None
    try:
        log_root = _detached_job_log_root(
            root=REPOSITORY_ROOT,
            env=dict(os.environ),
        )
        probe_path = log_root / (
            f".health-{os.getpid()}-{secrets.token_hex(16)}.probe"
        )
        with _open_detached_job_log(probe_path) as handle:
            handle.write("health\n")
            handle.flush()
            os.fsync(handle.fileno())
        probe_path.unlink()
        probe_path = None
        return {"status": "ok", "ready": True}
    except Exception as exc:
        _log_public_health_probe_failure("detached_job_log_readiness", exc)
        return {
            "status": "error",
            "ready": False,
            "error": "detached job log readiness probe failed",
            "error_code": "detached_job_log_readiness_failed",
        }
    finally:
        if probe_path is not None:
            try:
                probe_path.unlink(missing_ok=True)
            except OSError:
                pass


def _standalone_scheduler_heartbeat_readiness(
    expected_pid: object,
) -> dict[str, object]:
    """Bind the fresh DB heartbeat to the live systemd scheduler process."""

    try:
        pid = int(expected_pid)
        expected_sha = str(os.environ.get("PROBIGA_BUILD_COMMIT_SHA") or "")
        with get_engine().connect() as connection:
            passed, detail = check_linux_standalone_scheduler_heartbeat(
                connection,
                expected_build_sha=expected_sha,
                expected_pid=pid,
            )
    except Exception as exc:
        _log_public_health_probe_failure(
            "standalone_scheduler_heartbeat_readiness", exc
        )
        return {
            "status": "error",
            "ready": False,
            "error": "standalone scheduler heartbeat probe failed",
            "error_code": "scheduler_heartbeat_probe_failed",
        }
    return {
        "status": "ok" if passed else "error",
        "ready": bool(passed),
        "detail": detail,
        **(
            {}
            if passed
            else {
                "error": "standalone scheduler heartbeat is not current",
                "error_code": "scheduler_heartbeat_not_current",
            }
        ),
    }


def _deployed_git_revision() -> dict[str, object]:
    expected = os.environ.get("PROBIGA_EXPECTED_GIT_SHA", "").strip() or None
    production_mode = (
        os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower()
        == "production"
    )
    actual: str | None = None
    tracked_worktree_clean = False
    tracked_change_count: int | None = None
    untracked_executables: tuple[str, ...] = ("GIT_INSPECTION_FAILED",)
    root_shadow_files: tuple[str, ...] = ("GIT_INSPECTION_FAILED",)
    inspection_error_code: str | None = None
    inspection_error_stage: str | None = None
    inspection_durations_ms: dict[str, int] = {}
    current_stage = "head_revision"

    def _run_probe(stage: str, *args: str) -> str:
        nonlocal current_stage
        current_stage = stage
        started = monotonic()
        try:
            return subprocess.run(
                _release_git_command(*args),
                cwd=REPOSITORY_ROOT,
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="strict",
                timeout=_RELEASE_GIT_TIMEOUT_SECONDS,
            ).stdout
        finally:
            inspection_durations_ms[stage] = round((monotonic() - started) * 1000)

    protected_paths = (
        "server",
        "biz",
        "integrations",
        "tools",
        "scripts",
        "strategies",
        "versions",
        "artifacts/trading_v4",
        "artifacts/trading_v5",
        "artifacts/trading_v6",
        ".github",
        "deploy",
        "requirements-platform.txt",
        ".gitattributes",
        ".gitignore",
        "sitecustomize.py",
        "usercustomize.py",
        ":(top,glob)*.py",
        ":(top,glob)*.pyw",
        ":(top,glob)*.pyd",
        ":(top,glob)*.so",
        ":(top,glob)*/__init__.py",
        ":(top,glob)*/__init__*.pyc",
        ":(top,glob)*/__init__*.pyd",
        ":(top,glob)*/__init__*.so",
    )
    try:
        actual = _run_probe("head_revision", "rev-parse", "HEAD").strip()
        tracked_status = _run_probe(
            "tracked_status",
            "status",
            "--porcelain",
            "--untracked-files=no",
            "--",
            *protected_paths,
        ).splitlines()
        tracked_change_count = len(tracked_status)
        tracked_worktree_clean = not tracked_status
        index_inventory = _run_probe(
            "index_inventory",
            "ls-files",
            "--stage",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
            *protected_paths,
        ).split("\0")
        tracked: set[str] = set()
        untracked_output: list[str] = []
        for entry in index_inventory:
            if not entry:
                continue
            matched = _GIT_INDEX_ENTRY_RE.fullmatch(entry)
            if matched is None:
                untracked_output.append(entry)
            else:
                tracked.add(matched.group("path"))
        executable_suffixes = (".py", ".pyw", ".pyc", ".pyd", ".so")
        untracked_executables = tuple(
            sorted(
                path.strip().replace("\\", "/")
                for path in untracked_output
                if path.strip().lower().endswith(executable_suffixes)
            )
        )
        current_stage = "filesystem_shadow_scan"
        started = monotonic()
        try:
            root_shadow_files = _untracked_root_shadow_files(tracked)
        finally:
            inspection_durations_ms[current_stage] = round(
                (monotonic() - started) * 1000
            )
    except subprocess.TimeoutExpired:
        inspection_error_code = "probe_timeout"
        inspection_error_stage = current_stage
    except subprocess.CalledProcessError:
        inspection_error_code = "command_failed"
        inspection_error_stage = current_stage
    except OSError:
        inspection_error_code = "process_or_filesystem_error"
        inspection_error_stage = current_stage
    except UnicodeError:
        inspection_error_code = "invalid_command_output"
        inspection_error_stage = current_stage
    matches = bool(expected and actual and expected == actual)
    return {
        "expected_git_sha": expected,
        "actual_git_sha": actual,
        "deployment_mode": "production" if production_mode else "development",
        "expected_sha_configured": expected is not None,
        "matches_expected": matches if expected is not None else None,
        "inspection_status": "error" if inspection_error_code else "ok",
        "inspection_error_code": inspection_error_code,
        "inspection_error_stage": inspection_error_stage,
        "inspection_durations_ms": inspection_durations_ms,
        "tracked_worktree_clean": tracked_worktree_clean,
        "tracked_change_count": tracked_change_count,
        "untracked_executable_paths": untracked_executables[:20],
        "untracked_executable_count": (
            None if inspection_error_code else len(untracked_executables)
        ),
        "untracked_root_shadow_paths": root_shadow_files[:20],
        "untracked_root_shadow_count": (
            None if inspection_error_code else len(root_shadow_files)
        ),
        "code_worktree_clean": (
            tracked_worktree_clean
            and not untracked_executables
            and not root_shadow_files
        ),
    }


def _deployed_adata_revision() -> dict[str, str | bool | None]:
    source = os.environ.get(ADATA_SOURCE_ENV, "").strip() or None
    expected_git_sha = os.environ.get(ADATA_GIT_SHA_ENV, "").strip() or None
    expected_tree_sha = os.environ.get(ADATA_TREE_SHA_ENV, "").strip() or None
    configured = bool(source and expected_git_sha and expected_tree_sha)
    result: dict[str, str | bool | None] = {
        "source_configured": bool(source),
        "expected_git_sha": expected_git_sha,
        "expected_tree_sha256": expected_tree_sha,
        "configured": configured,
        "verified": False,
        "read_only": False,
        "error": None,
        "error_code": None,
    }
    if not configured:
        result["error"] = "adata release configuration is incomplete"
        result["error_code"] = "configuration_incomplete"
        return result
    try:
        validated = validate_adata_release_source(
            source,
            expected_git_sha=expected_git_sha,
            expected_tree_sha256=expected_tree_sha,
            repository_root=REPOSITORY_ROOT,
            require_read_only=True,
        )
        verified_source = ensure_adata_import_path(REPOSITORY_ROOT)
        spec = importlib.util.find_spec("adata")
        if spec is None or not spec.origin:
            raise AdataReleaseError("adata import origin is unavailable")
        origin = Path(spec.origin).resolve(strict=True)
        try:
            origin.relative_to(verified_source)
        except ValueError as exc:
            raise AdataReleaseError(
                "adata import origin is outside the verified release source"
            ) from exc
    except AdataReleaseError:
        result["error"] = "adata release validation failed"
        result["error_code"] = "release_validation_failed"
        return result
    except ImportError:
        result["error"] = "adata runtime import validation failed"
        result["error_code"] = "runtime_import_failed"
        return result
    except OSError:
        result["error"] = "adata filesystem validation failed"
        result["error_code"] = "filesystem_validation_failed"
        return result
    except ValueError:
        result["error"] = "adata release configuration is invalid"
        result["error_code"] = "configuration_invalid"
        return result
    result.update(
        {
            "actual_git_sha": validated["git_sha"],
            "actual_tree_sha256": validated["tree_sha256"],
            "git_marker": ADATA_GIT_MARKER,
            "tree_marker": ADATA_TREE_MARKER,
            "verified": True,
            "read_only": validated["read_only"],
            "import_within_pinned_source": True,
        }
    )
    return result


def _format_mysql_target(url_value: str | None = None) -> dict[str, str | int | None]:
    url = make_url(url_value or get_mysql_url(required=True))
    return {
        "drivername": url.drivername,
        "host": url.host,
        "port": url.port,
        "database": url.database,
    }


def _serialize_ts(value) -> str | None:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return None


def _is_trading_time(now: datetime | None = None) -> bool:
    current = now or datetime.now()
    if current.weekday() >= 5:
        return False
    hhmm = current.hour * 100 + current.minute
    return (925 <= hhmm <= 1135) or (1255 <= hhmm <= 1505)


def _table_freshness(table_name: str, code_column: str, *, fresh_window_seconds: int) -> dict[str, object]:
    engine = get_current_engine() if table_name in {"sm_stock_current", "sm_rt_quote_snapshot"} else get_engine()
    quoted_table = quote_identifier(table_name)
    quoted_code_column = quote_identifier(code_column)
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start = today_start + timedelta(days=1)
    today_sql = text(
        f"""
        SELECT
            MAX(snapshot_at) AS latest_snapshot_at,
            COUNT(*) AS today_rows,
            COUNT(DISTINCT {quoted_code_column}) AS today_symbols
        FROM {quoted_table}
        WHERE snapshot_at >= :today_start
          AND snapshot_at < :tomorrow_start
        """
    )
    total_sql = text(
        """
        SELECT COALESCE(TABLE_ROWS, 0) AS total_rows
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :table_name
        """
    )
    now = datetime.now()
    try:
        with engine.connect() as conn:
            row = conn.execute(
                today_sql,
                {"today_start": today_start, "tomorrow_start": tomorrow_start},
            ).mappings().first() or {}
            total_rows = conn.execute(total_sql, {"table_name": table_name}).scalar() or 0
    except Exception:
        return {
            "table": table_name,
            "status": "error",
            "error": "database freshness probe failed",
            "error_code": "database_probe_failed",
        }

    latest_snapshot_at = row.get("latest_snapshot_at")
    age_seconds = None
    if isinstance(latest_snapshot_at, datetime):
        age_seconds = max(0, int((now - latest_snapshot_at).total_seconds()))

    trading_now = _is_trading_time(now)
    intraday_fresh = None
    if trading_now:
        intraday_fresh = bool(age_seconds is not None and age_seconds <= fresh_window_seconds)

    status = "ok"
    if trading_now and not intraday_fresh:
        status = "warn"

    return {
        "table": table_name,
        "status": status,
        "latest_snapshot_at": _serialize_ts(latest_snapshot_at),
        "age_seconds": age_seconds,
        "today_rows": int(row.get("today_rows") or 0),
        "today_symbols": int(row.get("today_symbols") or 0),
        "total_rows": int(total_rows),
        "intraday_fresh": intraday_fresh,
        "fresh_window_seconds": int(fresh_window_seconds),
    }


def _combine_qmt_table_status(*items: dict[str, object]) -> str:
    if any(item.get("status") == "error" for item in items):
        return "error"
    if any(int(item.get("total_rows") or 0) <= 0 for item in items):
        return "warn"
    if any(item.get("status") == "warn" for item in items):
        return "warn"
    return "ok"


def _log_public_health_probe_failure(
    check_name: str,
    exc: BaseException,
) -> None:
    """Log only non-sensitive correlation data for a public health probe."""

    _LOGGER.error(
        "public_health_probe_failed check=%s incident_id=%s exception_type=%s",
        check_name,
        secrets.token_hex(8),
        type(exc).__name__,
    )


def _primary_database_readiness() -> dict[str, str | bool]:
    """Prove that the primary database accepts a minimal round trip."""

    try:
        engine = get_engine()
        with engine.connect() as conn:
            ready = conn.execute(text("SELECT 1")).scalar_one() == 1
    except Exception as exc:
        _log_public_health_probe_failure("primary_database_readiness", exc)
        return {
            "status": "error",
            "ready": False,
            "error": "primary database readiness probe failed",
            "error_code": "database_readiness_probe_failed",
        }
    if not ready:
        return {
            "status": "error",
            "ready": False,
            "error": "primary database readiness probe returned an invalid result",
            "error_code": "database_readiness_result_invalid",
        }
    return {"status": "ok", "ready": True}


def _strategy_funding_schema_readiness() -> dict[str, object]:
    """Prove the frozen funding fact/checkpoint contract without exposing metadata."""

    try:
        from server.engine.strategy_governance import (
            EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES,
            GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACT_HASH,
            METRIC_INPUT_REVIEW_TRIGGER_CONTRACT_HASH,
            validate_governance_append_only_triggers,
            validate_metric_input_review_triggers,
        )

        engine = get_engine()
        with engine.connect() as conn:
            detail = validate_strategy_funding_checkpoint_schema(conn)
            metric_triggers = validate_metric_input_review_triggers(conn)
            append_only_triggers = validate_governance_append_only_triggers(
                conn
            )
    except Exception:
        return {
            "status": "error",
            "ready": False,
            "error": "strategy funding schema validation failed",
            "error_code": "funding_schema_validation_failed",
        }

    expected_budgets = {
        "checkpoint_target_average_bytes": FUNDING_CHECKPOINT_TARGET_AVG_BYTES,
        "checkpoint_total_target_bytes": FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES,
        "checkpoint_total_hard_bytes": FUNDING_CHECKPOINT_TOTAL_HARD_BYTES,
        "batch_max_rows": FUNDING_CHECKPOINT_BATCH_MAX_ROWS,
        "batch_max_bytes": FUNDING_CHECKPOINT_BATCH_MAX_BYTES,
        "manifest_max_bytes": FUNDING_CHECKPOINT_MANIFEST_MAX_BYTES,
        "audit_max_bytes": FUNDING_CHECKPOINT_AUDIT_MAX_BYTES,
    }
    observed_budgets = {
        name: detail.get(name) for name in expected_budgets
    }
    ready = (
        FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH
        == _EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH
        and detail.get("table_count") == 2
        and detail.get("tables") == _EXPECTED_FUNDING_TABLE_COUNTS
        and detail.get("trigger_count") == 4
        and detail.get("contract_hash")
        == _EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH
        and detail.get("rolling_history_storage")
        == "ADDRESSABLE_APPEND_ONLY_DAILY_FACT_CHAIN"
        and detail.get("automatic_real_order_submission") is False
        and detail.get("real_order_authority") is False
        and observed_budgets == expected_budgets
        and GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACT_HASH
        == _EXPECTED_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
        and append_only_triggers.get("contract_hash")
        == _EXPECTED_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
        and append_only_triggers.get("trigger_count") == 38
        and set(append_only_triggers.get("trigger_names") or ())
        == EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
        and metric_triggers.get("trigger_count") == 2
        and METRIC_INPUT_REVIEW_TRIGGER_CONTRACT_HASH
        == _EXPECTED_METRIC_REVIEW_CONTRACT_HASH
        and metric_triggers.get("contract_hash")
        == _EXPECTED_METRIC_REVIEW_CONTRACT_HASH
        and set(metric_triggers.get("trigger_names") or ())
        == _EXPECTED_METRIC_REVIEW_TRIGGER_NAMES
    )
    if not ready:
        return {
            "status": "error",
            "ready": False,
            "error": "strategy funding schema contract is incomplete",
            "error_code": "funding_schema_contract_incomplete",
        }
    return {
        "status": "ok",
        "ready": True,
        "contract_hash": _EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH,
        "table_count": 2,
        "trigger_count": 4,
        "funding_trigger_count": 4,
        "governance_append_only_trigger_count": 38,
        "governance_metric_review_trigger_count": 2,
        "governance_trigger_count": 40,
        "governance_append_only_contract_hash": (
            _EXPECTED_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
        ),
        "governance_metric_review_contract_hash": (
            _EXPECTED_METRIC_REVIEW_CONTRACT_HASH
        ),
        "rolling_history_storage": "ADDRESSABLE_APPEND_ONLY_DAILY_FACT_CHAIN",
        "budgets": expected_budgets,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _scheduler_script_policy_readiness() -> dict[str, str | bool]:
    """Exercise the same immutable-script gate used by scheduled tasks."""

    try:
        resolve_scheduler_script(
            REPOSITORY_ROOT,
            "tools/run_scheduler_daemon.py",
        )
    except (OSError, SchedulerScriptPolicyError) as exc:
        _log_public_health_probe_failure(
            "scheduler_script_policy_readiness", exc
        )
        return {
            "status": "error",
            "ready": False,
            "error": "scheduler script policy readiness probe failed",
            "error_code": "scheduler_script_policy_probe_failed",
        }
    return {"status": "ok", "ready": True}


@router.get("/health")
def health():
    revision = _deployed_git_revision()
    adata_revision = _deployed_adata_revision()
    auth = admin_auth_status()
    database = _primary_database_readiness()
    funding_schema = _strategy_funding_schema_readiness()
    scheduler_script_policy = _scheduler_script_policy_readiness()
    production_mode = revision["deployment_mode"] == "production"
    scheduler_authority = scheduler_authority_contract()
    scheduler = scheduler_runtime_info()
    standalone_scheduler = (
        _standalone_scheduler_status()
        if production_mode
        else {
            "verified": False,
            "active": None,
            "state": None,
            "enabled": None,
            "enablement_state": None,
            "pid": None,
            "error": "not_checked_outside_production",
        }
    )
    detached_job_logs = (
        _detached_job_log_readiness()
        if production_mode
        else {
            "status": "not_checked",
            "ready": None,
            "error": "not_checked_outside_production",
        }
    )
    standalone_scheduler_heartbeat = (
        _standalone_scheduler_heartbeat_readiness(
            standalone_scheduler.get("pid")
        )
        if production_mode
        else {
            "status": "not_checked",
            "ready": None,
            "error": "not_checked_outside_production",
        }
    )
    if production_mode and not revision["expected_sha_configured"]:
        raise HTTPException(
            status_code=503,
            detail="production release revision is not configured",
        )
    if (
        (production_mode or revision["expected_sha_configured"])
        and (
            not revision["matches_expected"]
            or not revision["code_worktree_clean"]
        )
    ):
        if revision.get("inspection_status") == "error":
            reason = "inspection_failed"
        elif revision.get("matches_expected") is not True:
            reason = "revision_mismatch"
        elif revision.get("tracked_worktree_clean") is not True:
            reason = "tracked_changes"
        elif revision.get("untracked_executable_count"):
            reason = "untracked_executable"
        else:
            reason = "untracked_import_shadow"
        raise HTTPException(
            status_code=503,
            detail={
                "code": "release_identity_check_failed",
                "message": (
                    "deployed checkout differs from the pinned clean release revision"
                ),
                "reason": reason,
                "head_matches_expected": revision.get("matches_expected"),
                "worktree_clean": revision.get("code_worktree_clean"),
                "probe_stage": revision.get("inspection_error_stage"),
                "probe_error": revision.get("inspection_error_code"),
                "probe_durations_ms": revision.get("inspection_durations_ms", {}),
                "tracked_change_count": revision.get("tracked_change_count"),
                "untracked_executable_count": revision.get(
                    "untracked_executable_count"
                ),
                "untracked_import_shadow_count": revision.get(
                    "untracked_root_shadow_count"
                ),
            },
        )
    if production_mode and not adata_revision["verified"]:
        raise HTTPException(
            status_code=503,
            detail="separately versioned adata runtime is not pinned and immutable",
        )
    if production_mode and auth.get("ready") is not True:
        raise HTTPException(
            status_code=503,
            detail="production administrative authentication is not ready",
        )
    if production_mode and database.get("ready") is not True:
        raise HTTPException(
            status_code=503,
            detail="primary database readiness check failed",
        )
    if production_mode and funding_schema.get("ready") is not True:
        raise HTTPException(
            status_code=503,
            detail="strategy funding schema readiness check failed",
        )
    if production_mode and scheduler_script_policy.get("ready") is not True:
        raise HTTPException(
            status_code=503,
            detail="scheduler script policy readiness check failed",
        )
    if production_mode and detached_job_logs.get("ready") is not True:
        raise HTTPException(
            status_code=503,
            detail="detached job log readiness check failed",
        )
    if production_mode and (
        scheduler.get("embedded_scheduler_enabled") is not False
        or scheduler.get("embedded_scheduler_running") is not False
    ):
        raise HTTPException(
            status_code=503,
            detail="production embedded scheduler is not disabled and inactive",
        )
    if production_mode and (
        standalone_scheduler.get("verified") is not True
        or standalone_scheduler.get("active") is not True
        or standalone_scheduler.get("enabled") is not True
    ):
        raise HTTPException(
            status_code=503,
            detail="standalone scheduler activity and enablement could not be proven",
        )
    if (
        production_mode
        and standalone_scheduler_heartbeat.get("ready") is not True
    ):
        raise HTTPException(
            status_code=503,
            detail="standalone scheduler heartbeat could not be proven",
        )
    return {
        "status": "ok",
        "release_revision": revision,
        "adata_release_revision": adata_revision,
        "admin_auth_ready": bool(auth.get("ready")),
        "database": database,
        "strategy_funding_schema": funding_schema,
        "scheduler_runtime": scheduler,
        "scheduler_authority": scheduler_authority,
        "scheduler_script_policy": scheduler_script_policy,
        "standalone_scheduler": standalone_scheduler,
        "standalone_scheduler_heartbeat": standalone_scheduler_heartbeat,
        "detached_job_logs": detached_job_logs,
        "in_app_deploy_enabled": (
            os.environ.get("PROBIGA_IN_APP_DEPLOY_ENABLED", "").strip() == "1"
        ),
    }


@router.get("/health/runtime")
def health_runtime():
    return {
        "status": "ok",
        **scheduler_runtime_info(),
        "scheduler_authority": scheduler_authority_contract(),
        "mysql_target": _format_mysql_target(),
        "current_mysql_target": _format_mysql_target(get_current_mysql_url()),
        "minute_mysql_pool": get_minute_mysql_pool_config(),
        "gj_qmt": get_gj_qmt_config(),
        "qmt_live_runtime": _get_qmt_live_runtime_config(),
    }


@router.get("/health/schema")
def health_schema():
    from server.db.migrations import run_migrations, summarize_results

    results = run_migrations(get_engine(), dry_run=True)
    pending = [item for item in results if item.status == "would_add"]
    missing_tables = [item for item in results if item.status == "missing_table"]
    return {
        "status": "warn" if pending or missing_tables else "ok",
        "summary": summarize_results(results),
        "pending_columns": [item.as_dict() for item in pending],
        "missing_tables": [item.as_dict() for item in missing_tables],
        "results": [item.as_dict() for item in results],
    }


@router.get("/health/security")
def health_security():
    admin_auth = admin_auth_status()
    auth_ready = bool(admin_auth["ready"])
    return {
        "status": "ok" if auth_ready else "warn",
        "admin_auth": admin_auth,
        "expected_edge_controls": {
            "nginx_rate_limits": {
                "api": "10r/s burst 60",
                "admin": "1r/s burst 20",
            },
            "note": "Edge rate limits are configured outside the app and should be verified by ops checks.",
        },
    }


@router.get("/health/intraday-readiness")
def health_intraday_readiness():
    from tools.data_quality_check import intraday_readiness

    return intraday_readiness(get_engine())


@router.get("/health/qmt-bridge")
def health_qmt_bridge():
    from integrations.qmt import bridge
    from integrations.qmt.diagnostics import diagnostics

    runtime = _get_qmt_live_runtime_config()
    runtime_enabled = bool(runtime.get("enabled"))
    local_bridge_configured = bridge.is_configured()
    if local_bridge_configured:
        qmt = diagnostics(timeout=int(get_gj_qmt_config()["ping_timeout"] or 8))
    else:
        qmt = {
            "status": "disabled",
            "provider": "gj_qmt",
            "sdk": {"configured": False, "error_code": "SDK_RUNTIME_MISSING"},
        }
    stock_current = _table_freshness(
        "sm_stock_current",
        "stock_code",
        fresh_window_seconds=max(20, int(runtime["poll_seconds"]) * 4),
    )
    index_current = _table_freshness(
        "sm_index_current",
        "index_code",
        fresh_window_seconds=max(30, int(runtime["poll_seconds"]) * 6),
    )
    table_status = _combine_qmt_table_status(stock_current, index_current)
    collector_mode = (
        "embedded_runtime"
        if runtime_enabled and local_bridge_configured
        else "local_sdk_probe"
        if local_bridge_configured
        else "external_windows_collector"
    )
    overall_status = str(qmt.get("status") or "error")
    status_reason = ""
    if collector_mode == "external_windows_collector":
        overall_status = table_status
        status_reason = "Production API does not run the QMT SDK directly; freshness is judged from the local Windows QMT collector writing the current-data database through the reverse tunnel."
        if runtime_enabled:
            status_reason += " The embedded runtime is only a stale-data Sina fallback and does not overwrite fresh Big QMT rows."
        if table_status == "warn":
            status_reason += " During trading hours, stale snapshots usually mean the local QMT client, gateway, live runtime, or MySQL reverse tunnel needs attention."
        elif table_status == "error":
            status_reason += " Realtime snapshot tables could not be read from production DB."
    for item in (stock_current, index_current):
        if item.get("status") == "error":
            overall_status = "error"
            break
        if item.get("status") == "warn" and overall_status == "ok":
            overall_status = item["status"]
    return {
        "status": overall_status,
        "collector_mode": collector_mode,
        "trading_now": _is_trading_time(),
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status_reason": status_reason,
        "mysql_target": _format_mysql_target(),
        "current_mysql_target": _format_mysql_target(get_current_mysql_url()),
        "gj_qmt": qmt,
        "qmt_live_runtime": runtime,
        "stock_current": stock_current,
        "index_current": index_current,
    }


@router.get("/health/qmt-capabilities")
def health_qmt_capabilities(force: bool = False):
    from integrations.qmt import bridge
    from integrations.qmt.diagnostics import capabilities

    runtime = _get_qmt_live_runtime_config()
    # The production API runs on Linux while the licensed QMT SDK and client
    # run on the Windows collector.  Enabling the server-side freshness loop
    # must not make this endpoint probe a non-existent local Windows runtime.
    # ``force=true`` remains available for hosts that intentionally configure
    # a local SDK probe.
    if not force and not bridge.is_configured():
        return {
            "ok": False,
            "provider": "gj_qmt",
            "status": "external_windows_collector",
            "reason": "生产服务器通过 Windows QMT 采集器接收数据，本机不直接加载 QMT SDK",
            "qmt_live_runtime": runtime,
            "rows": [],
        }
    timeout = max(2, int(get_gj_qmt_config()["ping_timeout"] or 8) + 4)
    return capabilities(timeout=timeout, force=force)


@router.get("/health/qmt-core-probe")
def health_qmt_core_probe(force: bool = False):
    from integrations.qmt.diagnostics import core_probe

    timeout = max(15, int(get_gj_qmt_config()["ping_timeout"] or 8) + 22)
    return core_probe(timeout=timeout, force=force)
