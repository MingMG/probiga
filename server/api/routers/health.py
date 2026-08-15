# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import importlib.util
import os
from pathlib import Path
import subprocess

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from sqlalchemy.engine import make_url

from server.api.admin_auth import admin_auth_status
from server.api.scheduler_runtime import scheduler_runtime_info
from server.api.routers._engine import get_engine
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


def _untracked_root_shadow_files() -> tuple[str, ...]:
    """Find root-level import shadows, including files ignored by Git."""
    tracked = set(
        subprocess.run(
            [
                "git",
                "ls-files",
                "--",
                ":(top,glob)*.py",
                ":(top,glob)*.pyw",
                ":(top,glob)*.pyc",
                ":(top,glob)*.pyd",
                ":(top,glob)*.so",
                ":(top,glob)*/__init__.py",
                ":(top,glob)*/__init__*.pyc",
                ":(top,glob)*/__init__*.pyd",
                ":(top,glob)*/__init__*.so",
            ],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=5,
        ).stdout.splitlines()
    )
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


def _standalone_scheduler_status() -> dict[str, str | bool | None]:
    """Prove that the legacy standalone scheduler is inactive and not enabled."""
    try:
        active_result = subprocess.run(
            ["systemctl", "is-active", "probiga-scheduler.service"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=5,
        )
        enabled_result = subprocess.run(
            ["systemctl", "is-enabled", "probiga-scheduler.service"],
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
                    "probiga-scheduler.service",
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
        return {
            "verified": False,
            "active": None,
            "state": None,
            "enabled": None,
            "enablement_state": None,
            "error": type(exc).__name__,
        }

    state = active_result.stdout.strip().lower()
    active_returncode = active_result.returncode
    enablement_state = enabled_result.stdout.strip().lower()
    enablement_returncode = enabled_result.returncode
    if load_state == "not-found":
        if not state:
            state = "unknown"
            active_returncode = 4
        if not enablement_state:
            enablement_state = "not-found"
            enablement_returncode = 1
    active_states = {"active", "activating", "reloading", "deactivating"}
    inactive_states = {"inactive", "failed", "unknown"}
    if state in active_states:
        active: bool | None = True
        active_verified = True
    elif state in inactive_states and active_returncode in {3, 4}:
        active = False
        active_verified = True
    else:
        active = None
        active_verified = False

    safe_enablement_states = {
        "disabled",
        "masked",
        "masked-runtime",
        "static",
        "not-found",
    }
    unsafe_enablement_states = {
        "enabled",
        "enabled-runtime",
        "linked",
        "linked-runtime",
        "alias",
        "indirect",
        "generated",
        "transient",
    }
    if (
        enablement_state in safe_enablement_states
        and enablement_returncode in {0, 1, 3, 4}
    ):
        enabled: bool | None = False
        enablement_verified = True
    elif enablement_state in unsafe_enablement_states:
        enabled = True
        enablement_verified = True
    else:
        enabled = None
        enablement_verified = False

    verified = active_verified and enablement_verified
    error = None
    if not active_verified:
        error = f"systemctl_is_active_exit_{active_returncode}"
    elif not enablement_verified:
        error = f"systemctl_is_enabled_exit_{enablement_returncode}"
    elif enabled:
        error = "standalone_scheduler_enabled"
    return {
        "verified": verified,
        "active": active,
        "state": state or None,
        "enabled": enabled,
        "enablement_state": enablement_state or None,
        "error": error,
    }


def _deployed_git_revision() -> dict[str, str | bool | None]:
    expected = os.environ.get("PROBIGA_EXPECTED_GIT_SHA", "").strip() or None
    production_mode = (
        os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower()
        == "production"
    )
    try:
        actual = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=5,
        ).stdout.strip()
        tracked_status = subprocess.run(
            [
                "git",
                "status",
                "--porcelain",
                "--untracked-files=all",
                "--",
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
            ],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=5,
        ).stdout.strip()
        tracked_worktree_clean = tracked_status == ""
        untracked_output = subprocess.run(
            [
                "git",
                "ls-files",
                "--others",
                "--exclude-standard",
                "--",
                "sitecustomize.py",
                "usercustomize.py",
                "server",
                "biz",
                "integrations",
                "tools",
                "scripts",
            ],
            cwd=REPOSITORY_ROOT,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=5,
        ).stdout.splitlines()
        executable_suffixes = (".py", ".pyw", ".pyc", ".pyd", ".so")
        untracked_executables = tuple(
            sorted(
                path.strip().replace("\\", "/")
                for path in untracked_output
                if path.strip().lower().endswith(executable_suffixes)
            )
        )
        root_shadow_files = _untracked_root_shadow_files()
    except (OSError, subprocess.SubprocessError, UnicodeError):
        actual = None
        tracked_worktree_clean = False
        untracked_executables = ("GIT_INSPECTION_FAILED",)
        root_shadow_files = ("GIT_INSPECTION_FAILED",)
    matches = bool(expected and actual and expected == actual)
    return {
        "expected_git_sha": expected,
        "actual_git_sha": actual,
        "deployment_mode": "production" if production_mode else "development",
        "expected_sha_configured": expected is not None,
        "matches_expected": matches if expected is not None else None,
        "tracked_worktree_clean": tracked_worktree_clean,
        "untracked_executable_paths": untracked_executables[:20],
        "untracked_root_shadow_paths": root_shadow_files[:20],
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
        "source_dir": source,
        "expected_git_sha": expected_git_sha,
        "expected_tree_sha256": expected_tree_sha,
        "configured": configured,
        "verified": False,
        "read_only": False,
        "error": None,
    }
    if not configured:
        result["error"] = "adata release source and hashes are not fully configured"
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
    except (AdataReleaseError, ImportError, OSError, ValueError) as exc:
        result["error"] = str(exc)
        return result
    result.update(
        {
            "actual_git_sha": validated["git_sha"],
            "actual_tree_sha256": validated["tree_sha256"],
            "git_marker": ADATA_GIT_MARKER,
            "tree_marker": ADATA_TREE_MARKER,
            "verified": True,
            "read_only": validated["read_only"],
            "import_origin": str(origin),
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
    except Exception as exc:
        return {
            "table": table_name,
            "status": "error",
            "error": str(exc),
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


@router.get("/health")
def health():
    revision = _deployed_git_revision()
    adata_revision = _deployed_adata_revision()
    auth = admin_auth_status()
    production_mode = revision["deployment_mode"] == "production"
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
        raise HTTPException(
            status_code=503,
            detail=(
                "deployed checkout differs from the pinned clean release revision"
            ),
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
    if production_mode and (
        scheduler.get("embedded_scheduler_enabled") is not True
        or scheduler.get("embedded_scheduler_running") is not True
    ):
        raise HTTPException(
            status_code=503,
            detail="production embedded scheduler is not enabled and running",
        )
    if production_mode and (
        standalone_scheduler.get("verified") is not True
        or standalone_scheduler.get("active") is not False
        or standalone_scheduler.get("enabled") is not False
    ):
        raise HTTPException(
            status_code=503,
            detail="standalone scheduler inactivity and disablement could not be proven",
        )
    return {
        "status": "ok",
        "release_revision": revision,
        "adata_release_revision": adata_revision,
        "admin_auth_ready": bool(auth.get("ready")),
        "scheduler_runtime": scheduler,
        "standalone_scheduler": standalone_scheduler,
        "in_app_deploy_enabled": (
            os.environ.get("PROBIGA_IN_APP_DEPLOY_ENABLED", "").strip() == "1"
        ),
    }


@router.get("/health/runtime")
def health_runtime():
    return {
        "status": "ok",
        **scheduler_runtime_info(),
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
