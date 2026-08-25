#!/usr/bin/env python3
"""Synchronize one full-market official QMT announcement PIT batch."""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from datetime import datetime
import json
import os
from pathlib import Path
import re
import stat
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.qmt_announcement_pit import (
    QMT_ANNOUNCEMENT_TASK_SCHEMA,
    QMTAnnouncementBlocked,
    synchronize_qmt_announcements,
    validate_task_result,
)
from tools.qmt_announcement_task_contract import (
    QMT_ANNOUNCEMENT_CHECKPOINT_DIR,
)


def _is_production() -> bool:
    return os.environ.get("PROBIGA_DEPLOYMENT_MODE", "").strip().lower() == (
        "production"
    )


def _checkpoint_root(value: str) -> Path:
    """Return one writable, non-link checkpoint root without code-tree escape."""

    raw = str(value or "").strip()
    requested = (
        raw
        or os.environ.get("QMT_ANNOUNCEMENT_CHECKPOINT_DIR", "").strip()
        or QMT_ANNOUNCEMENT_CHECKPOINT_DIR
    )
    if os.name == "nt" and requested == QMT_ANNOUNCEMENT_CHECKPOINT_DIR:
        program_data = os.environ.get("PROGRAMDATA", r"C:\ProgramData").strip()
        candidate = (
            Path(program_data)
            / "ProBigA"
            / "qmt-announcement-checkpoints"
        )
    else:
        candidate = Path(requested)
    if not candidate.is_absolute():
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", "not-absolute"
        )
    absolute = Path(os.path.abspath(str(candidate)))
    if _is_production() and requested != QMT_ANNOUNCEMENT_CHECKPOINT_DIR:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
            "production-root-differs",
        )

    try:
        if absolute.exists() or absolute.is_symlink():
            if absolute.is_symlink() or not absolute.is_dir():
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
                    "root-not-directory-or-is-symlink",
                )
        elif _is_production() and os.name != "nt":
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
                "production-root-missing",
            )
        else:
            absolute.mkdir(parents=True, mode=0o700)
            if os.name == "posix":
                absolute.chmod(0o700)

        resolved = absolute.resolve(strict=True)
    except QMTAnnouncementBlocked:
        raise
    except OSError as exc:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", type(exc).__name__
        ) from exc
    if resolved != absolute:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", "resolved-root-differs"
        )

    root_stat = absolute.stat(follow_symlinks=False)
    if not stat.S_ISDIR(root_stat.st_mode):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", "root-is-not-directory"
        )
    if _is_production() and os.name == "posix":
        if root_stat.st_uid != os.geteuid():
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
                "service-user-does-not-own-root",
            )
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", "root-mode-not-0700"
            )

    # State is service-owned and therefore untrusted input on the next run.
    # Reject every link before any checkpoint manifest/result is read.  With a
    # resolved exact root and no descendant links, all reads/writes stay under
    # the persistent state directory while the release tree remains sealed.
    def walk_error(exc: OSError) -> None:
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", type(exc).__name__
        ) from exc

    for current, directories, files in os.walk(
        absolute, followlinks=False, onerror=walk_error
    ):
        current_path = Path(current)
        try:
            current_path.resolve(strict=True).relative_to(absolute)
        except (OSError, ValueError) as exc:
            raise QMTAnnouncementBlocked(
                "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
                "checkpoint-tree-resolve-escape",
            ) from exc
        for name in [*directories, *files]:
            entry = current_path / name
            if entry.is_symlink():
                raise QMTAnnouncementBlocked(
                    "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID",
                    "checkpoint-tree-contains-symlink",
                )
    if not os.access(absolute, os.R_OK | os.W_OK | os.X_OK):
        raise QMTAnnouncementBlocked(
            "QMT_ANNOUNCEMENT_CHECKPOINT_ROOT_INVALID", "root-not-rwx"
        )
    return absolute


def _blocked(reason_code: str, detail: str = "") -> dict:
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    return {
        "schema": QMT_ANNOUNCEMENT_TASK_SCHEMA,
        "status": "DATA_BLOCKED",
        "reason_code": str(reason_code or "QMT_ANNOUNCEMENT_DATA_BLOCKED"),
        "detail": str(detail or "")[:1000],
        "batch_id": "",
        "batch_root_hash": "",
        "catalog_batch_id": "",
        "catalog_manifest_hash": "",
        "catalog_member_set_hash": "",
        "stock_count": 0,
        "coverage_count": 0,
        "event_count": 0,
        "empty_stock_count": 0,
        "fact_cutoff_at": now,
        "decision_at": now,
        "received_at": now,
        "capture_seconds": 0,
        "window_start": "",
        "window_end": "",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument(
        "--checkpoint-dir",
        default="",
    )
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument(
        "--validate-result-exit", type=int, default=-1,
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)
    if args.validate_result_exit >= 0:
        try:
            payload = json.load(sys.stdin)
            print(validate_task_result(payload, args.validate_result_exit))
            return 0
        except Exception as exc:
            print(f"invalid:{type(exc).__name__}", file=sys.stderr)
            return 2

    from tools.env_config import create_tool_engine, load_project_env

    load_project_env()
    engine = None
    try:
        checkpoint_dir = _checkpoint_root(args.checkpoint_dir)
        engine = create_tool_engine()
        from integrations.qmt.runtime import import_xtdata

        # Some QMT builds print connection diagnostics.  Keep stdout a single
        # machine JSON record so scheduler validation cannot accept ambiguity.
        with redirect_stdout(sys.stderr):
            xtdata = import_xtdata()
            payload = synchronize_qmt_announcements(
                engine,
                xtdata=xtdata,
                checkpoint_root=checkpoint_dir,
                window_days=args.window_days,
                batch_size=args.batch_size,
                resume=not args.no_resume,
            )
    except Exception as exc:
        reason = str(getattr(exc, "reason_code", "") or "")
        if not reason:
            message = str(exc).lower()
            reason = (
                "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE"
                if "xtquant" in message or "qmt" in message and "import" in message
                else "QMT_ANNOUNCEMENT_RUNTIME_DATA_BLOCKED"
            )
        payload = _blocked(reason, type(exc).__name__)
    finally:
        if engine is not None:
            engine.dispose()
    process_exit = 0 if payload.get("status") == "COMPLETE" else 2
    try:
        validate_task_result(payload, process_exit)
    except Exception as exc:
        payload = _blocked(
            "QMT_ANNOUNCEMENT_INVALID_RESULT_CONTRACT", type(exc).__name__
        )
        process_exit = 2
        validate_task_result(payload, process_exit)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return process_exit


if __name__ == "__main__":
    raise SystemExit(main())
