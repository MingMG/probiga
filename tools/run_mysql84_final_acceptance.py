#!/usr/bin/env python3
"""Run the frozen-source MySQL 8.4 pre-cutover acceptance as one workflow.

The workflow is resumable but never infers success from an existing artifact:
only a checkpointed, plan-bound successful step can be skipped.  It performs
the final binlog catch-up, provisions a schema-scoped migration account,
audits schema semantics, repairs the reviewed MySQL 5.5 fractional-DATETIME
compatibility drift, materializes safe MySQL 8.4 defaults, compares business
data, runs V2/V3/V4 inside the guarded trigger window, and finishes with a
read-only business smoke.  It does not stop services, move data, promote
``.env``, or activate production trading.
"""

from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.mysql55_to_mysql84_data_manifest import (  # noqa: E402
    KNOWN_MYSQL84_ZERO_DATE_COLUMNS,
    AuditConfig,
    load_config,
)
from tools.provision_mysql84_migration_account import (  # noqa: E402
    APPLY_ACK as MIGRATION_ACCOUNT_ACK,
)
from tools.run_mysql55_consistent_dump import (  # noqa: E402
    assert_protected_client_option_file,
)
from tools.run_mysql55_to_mysql84_binlog_catchup import (  # noqa: E402
    FINAL_FROZEN_ACK,
)


EXECUTE_ACK = "I_CONFIRM_SOURCE_WRITES_ARE_FROZEN_FOR_FINAL_ACCEPTANCE"
REACQUIRED_FREEZE_ACK = (
    "I_CONFIRM_SOURCE_COORDINATE_IS_UNCHANGED_AND_REACQUIRED_FREEZE_MAY_RESUME"
)
REVIEWED_MIGRATION_PREFLIGHT_RESUME_ACK = (
    "I_CONFIRM_FAILED_MIGRATION_WAS_PLAN_ONLY_AND_REVIEWED_PREFLIGHT_PASSED"
)
SCHEMAS = ("biga", "probiga", "probiga_qmt_history")
_SHA256_RE = __import__("re").compile(r"^[0-9a-f]{64}$")
_UUID_RE = __import__("re").compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


class AcceptanceError(RuntimeError):
    """A final acceptance safety or validation gate failed."""


@dataclass(frozen=True, slots=True)
class Step:
    name: str
    command: tuple[str, ...]
    outputs: tuple[Path, ...]
    validator: Callable[[], dict[str, Any]]
    environment: Mapping[str, str] | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _json_sha256(value: object) -> str:
    payload = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _absolute_file(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise AcceptanceError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise AcceptanceError(f"{label} does not exist") from exc
    if not resolved.is_file():
        raise AcceptanceError(f"{label} must be a file")
    return resolved


def _absolute_dir(path: Path, *, label: str) -> Path:
    if not path.is_absolute():
        raise AcceptanceError(f"{label} must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise AcceptanceError(f"{label} does not exist") from exc
    if not resolved.is_dir():
        raise AcceptanceError(f"{label} must be a directory")
    return resolved


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AcceptanceError(f"{label} is invalid JSON") from exc
    if not isinstance(value, dict):
        raise AcceptanceError(f"{label} must be a JSON object")
    return value


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AcceptanceError(message)


def _validate_data_config(
    config: AuditConfig, *, target_uuid: str, target_port: int
) -> None:
    _require(config.schemas == SCHEMAS, "data manifest config must use the three canonical schemas")
    _require(config.source.version == "5.5.20-log", "data manifest source version must be 5.5.20-log")
    _require(config.source.port == 3306, "data manifest source port must be 3306")
    _require(not config.source.require_tls, "legacy source data manifest must not claim TLS")
    _require(config.target.version == "8.4.11", "data manifest target version must be 8.4.11")
    _require(config.target.port == target_port, "data manifest target port mismatch")
    _require(config.target.server_uuid == target_uuid, "data manifest target UUID mismatch")
    _require(config.target.require_tls, "data manifest target must require TLS")
    _require(config.counts.mode == "all", "final data manifest must count all base tables")
    catalog_comparison = config.raw.get("catalog_comparison")
    _require(
        isinstance(catalog_comparison, dict),
        "final data manifest must pin its source/target catalogue relationship",
    )
    _require(
        catalog_comparison.get("mode")
        in {"exact", "reviewed_v2_v3_v4_source_projection"},
        "final data manifest catalogue relationship is unsupported",
    )
    _require(
        _SHA256_RE.fullmatch(
            str(catalog_comparison.get("source_catalog_sha256", "")).lower()
        )
        is not None
        and _SHA256_RE.fullmatch(
            str(catalog_comparison.get("target_catalog_sha256", "")).lower()
        )
        is not None,
        "final data manifest must pin both live catalogue digests",
    )
    _require(
        set(config.legacy_zero_date_columns) == set(KNOWN_MYSQL84_ZERO_DATE_COLUMNS),
        "data manifest lacks the complete zero-date risk list",
    )


def validate_freeze_guard(ready_path: Path, heartbeat_path: Path) -> dict[str, Any]:
    ready = _read_json(ready_path, label="source freeze ready evidence")
    heartbeat = _read_json(heartbeat_path, label="source freeze heartbeat")
    pid = int(ready.get("pid") or 0)
    if (
        ready.get("tool") != "hold_mysql55_cutover_lock"
        or ready.get("status") != "locked"
        or ready.get("global_read_lock_held") is not True
        or ready.get("named_lock_held") is not True
        or heartbeat.get("tool") != "hold_mysql55_cutover_lock"
        or heartbeat.get("status") != "locked"
        or heartbeat.get("pid") != pid
        or heartbeat.get("global_read_lock_held") is not True
        or heartbeat.get("blocked_writer_detected") is not False
    ):
        raise AcceptanceError("source freeze guardian is not safely holding its locks")
    source = heartbeat.get("source", {})
    if (
        source.get("version") != "5.5.20-log"
        or source.get("port") != 3306
        or source.get("server_id") != 55
        or source.get("log_bin") != 1
        or source.get("binlog_format") != "STATEMENT"
    ):
        raise AcceptanceError("source freeze guardian identity drifted")
    try:
        observed = datetime.fromisoformat(
            str(heartbeat.get("heartbeat_at_utc") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise AcceptanceError("source freeze heartbeat timestamp is invalid") from exc
    age = (datetime.now(timezone.utc) - observed.astimezone(timezone.utc)).total_seconds()
    if age < -5 or age > 30:
        raise AcceptanceError("source freeze heartbeat is stale")
    if pid <= 0:
        raise AcceptanceError("source freeze guardian PID is invalid")
    if not _pid_is_alive(pid):
        raise AcceptanceError("source freeze guardian process is not alive")
    return {
        "pid": pid,
        "heartbeat_at_utc": heartbeat["heartbeat_at_utc"],
        "master_file": source.get("master_file"),
        "master_position": source.get("master_position"),
    }


def validate_freeze_guard_with_retry(
    ready_path: Path,
    heartbeat_path: Path,
    *,
    attempts: int = 4,
) -> dict[str, Any]:
    """Retry brief Windows process/file visibility races without weakening the gate."""

    if attempts < 1:
        raise AcceptanceError("freeze guard validation attempts must be positive")
    last_error: AcceptanceError | None = None
    for attempt in range(attempts):
        try:
            return validate_freeze_guard(ready_path, heartbeat_path)
        except AcceptanceError as exc:
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(0.25)
    assert last_error is not None
    raise last_error


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        process_query_limited_information = 0x1000
        still_active = 259
        handle = ctypes.windll.kernel32.OpenProcess(
            process_query_limited_information, False, pid
        )
        if not handle:
            return False
        try:
            exit_code = wintypes.DWORD()
            if not ctypes.windll.kernel32.GetExitCodeProcess(
                handle, ctypes.byref(exit_code)
            ):
                return False
            return int(exit_code.value) == still_active
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _dump_sha256(manifest_path: Path) -> str:
    manifest = _read_json(manifest_path, label="dump manifest")
    if manifest.get("mode") not in {"online-rehearsal", "final-frozen"}:
        raise AcceptanceError("dump manifest mode is unsupported")
    if manifest.get("mysqldump", {}).get("return_code") != 0:
        raise AcceptanceError("dump manifest has no successful mysqldump result")
    value = str(manifest.get("artifacts", {}).get("dump", {}).get("sha256") or "").lower()
    if _SHA256_RE.fullmatch(value) is None:
        raise AcceptanceError("dump manifest has no valid dump SHA-256")
    coordinates = manifest.get("snapshot_binlog_coordinates")
    if (
        manifest.get("binlog_coordinates_captured") is not True
        or not isinstance(coordinates, Mapping)
        or not coordinates.get("file")
        or not isinstance(coordinates.get("position"), int)
    ):
        raise AcceptanceError("dump manifest has no captured binlog snapshot coordinate")
    return value


def _read_option(path: Path, *, expected_port: int) -> dict[str, str]:
    protected = assert_protected_client_option_file(path)
    parser = configparser.RawConfigParser(interpolation=None, strict=True)
    parser.read(protected, encoding="utf-8-sig")
    if parser.sections() != ["client"]:
        raise AcceptanceError("migration option file must contain exactly [client]")
    values = {key: value.strip() for key, value in parser.items("client", raw=True)}
    expected = {"protocol": "tcp", "host": "127.0.0.1", "port": str(expected_port)}
    for name, value in expected.items():
        if values.get(name, "").casefold() != value.casefold():
            raise AcceptanceError(f"migration option file {name} mismatch")
    if not values.get("user") or not values.get("password"):
        raise AcceptanceError("migration option file lacks credentials")
    return values


def _migration_environment(option_path: Path, *, port: int, ca: Path) -> dict[str, str]:
    options = _read_option(option_path, expected_port=port)
    url = (
        "mysql+pymysql://"
        f"{quote(options['user'], safe='')}:{quote(options['password'], safe='')}"
        f"@127.0.0.1:{port}/probiga"
    )
    environment = dict(os.environ)
    environment["MYSQL84_MIGRATION_URL"] = url
    environment["MYSQL84_MIGRATION_SSL_CA"] = str(ca)
    return environment


def _validate_json(path: Path, predicate: Callable[[dict[str, Any]], bool], message: str) -> dict[str, Any]:
    value = _read_json(path, label=path.name)
    if not predicate(value):
        raise AcceptanceError(message)
    return value


def _new_output(path: Path, *, resume: bool, completed: bool) -> None:
    if not path.exists():
        return
    if resume and completed:
        return
    if resume and not completed and path.name.endswith(".checkpoint.json"):
        # The manifest reader revalidates role, config hash, endpoint identity,
        # catalogue and snapshot context before trusting checkpointed tables.
        return
    raise AcceptanceError(f"untrusted pre-existing step artifact: {path}")


def _atomic_json(path: Path, payload: Mapping[str, Any], *, replace: bool) -> None:
    if not path.is_absolute():
        raise AcceptanceError("state/evidence path must be absolute")
    if path.exists() and not replace:
        raise AcceptanceError(f"refusing to overwrite evidence: {path}")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _run_step(step: Step, *, stdout: Path, stderr: Path) -> int:
    environment = dict(step.environment) if step.environment is not None else None
    try:
        with stdout.open("xb") as out_stream, stderr.open("xb") as err_stream:
            completed = subprocess.run(
                list(step.command),
                stdin=subprocess.DEVNULL,
                stdout=out_stream,
                stderr=err_stream,
                shell=False,
                env=environment,
                check=False,
            )
    finally:
        if environment is not None:
            environment.pop("MYSQL84_MIGRATION_URL", None)
            environment.clear()
    return int(completed.returncode)


def _plan(args: argparse.Namespace, paths: Mapping[str, Path]) -> dict[str, Any]:
    return {
        "tool": "run_mysql84_final_acceptance",
        "source_option_file_sha256": _sha256(paths["source_option"]),
        "dump_manifest_sha256": _sha256(paths["dump_manifest"]),
        "data_manifest_config_sha256": _sha256(paths["data_config"]),
        "manifest_python_sha256": _sha256(paths["manifest_python"]),
        "manifest_pymysql_sha256": (
            _sha256(paths["manifest_site"] / "pymysql" / "__init__.py")
            if "manifest_site" in paths
            else None
        ),
        "target_admin_option_file_sha256": _sha256(paths["target_admin"]),
        "target_ca_sha256": _sha256(paths["target_ca"]),
        "freeze_ready_evidence_sha256": _sha256(paths["freeze_ready"]),
        "expected_target_uuid": args.expected_target_uuid,
        "expected_target_port": args.expected_target_port,
        "expected_target_datadir": str(paths["target_datadir"]),
        "snapshot_id": args.snapshot_id,
        "writes_frozen_at": args.writes_frozen_at,
        "restored_artifact_sha256": args.restored_artifact_sha256,
        "workers": args.workers,
    }


def _validate_reacquired_freeze_resume(
    *,
    state: Mapping[str, Any],
    current_plan: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    failed_step = str(state.get("failed_step") or "")
    if state.get("status") != "failed" or failed_step not in {
        "compare_business_data",
        "v2_v3_v4_migrations",
    }:
        raise AcceptanceError(
            "reacquired-freeze resume is limited to a reviewed resumable failure"
        )
    migration_review: dict[str, Any] | None = None
    if failed_step == "compare_business_data":
        if state.get("target_may_be_tainted") is not False:
            raise AcceptanceError("comparison failure is not sealed as untainted")
    else:
        if state.get("target_may_be_tainted") is not True:
            raise AcceptanceError("migration failure taint marker is invalid")
        migration_review = _validate_reviewed_migration_preflight_resume(
            state=state,
            current_plan=current_plan,
            paths=paths,
        )
    stored_plan = state.get("plan")
    if not isinstance(stored_plan, Mapping):
        raise AcceptanceError("failed acceptance state has no sealed plan")
    stored_invariants = dict(stored_plan)
    current_invariants = dict(current_plan)
    prior_freeze_sha = str(
        stored_invariants.pop("freeze_ready_evidence_sha256", "")
    ).lower()
    current_freeze_sha = str(
        current_invariants.pop("freeze_ready_evidence_sha256", "")
    ).lower()
    if (
        _SHA256_RE.fullmatch(prior_freeze_sha) is None
        or _SHA256_RE.fullmatch(current_freeze_sha) is None
        or stored_invariants != current_invariants
    ):
        raise AcceptanceError(
            "reacquired-freeze resume changed an invariant other than freeze evidence"
        )

    live_freeze = validate_freeze_guard_with_retry(
        paths["freeze_ready"], paths["freeze_heartbeat"]
    )
    catchup = _read_json(
        paths["out"] / "01-binlog-final.json", label="final binlog catch-up"
    )
    requested_stop = catchup.get("requested_stop")
    cursor_after = catchup.get("cursor_after")
    expected_coordinate = {
        "file": str(live_freeze.get("master_file") or ""),
        "position": int(live_freeze.get("master_position") or 0),
    }
    if (
        catchup.get("status") != "success"
        or catchup.get("mode") != "final-frozen"
        or requested_stop != cursor_after
        or requested_stop != expected_coordinate
        or catchup.get("target_may_be_tainted") is not False
    ):
        raise AcceptanceError(
            "source coordinate changed after the completed frozen data captures"
        )
    result = {
        "mode": "reacquired_global_read_lock",
        "validated_at_utc": _utc_now(),
        "failed_step": failed_step,
        "prior_freeze_ready_sha256": prior_freeze_sha,
        "current_freeze_ready_sha256": current_freeze_sha,
        "source_coordinate": expected_coordinate,
        "current_guardian_pid": int(live_freeze["pid"]),
        "current_heartbeat_at_utc": live_freeze["heartbeat_at_utc"],
        "unchanged_plan_invariants": True,
    }
    if migration_review is not None:
        result["reviewed_migration_preflight"] = migration_review
    return result


def _validate_reviewed_migration_preflight_resume(
    *,
    state: Mapping[str, Any],
    current_plan: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> dict[str, Any]:
    preflight_path = paths.get("reviewed_migration_preflight")
    failure_dir = paths.get("reviewed_migration_failure_dir")
    if preflight_path is None or failure_dir is None:
        raise AcceptanceError(
            "reviewed migration resume requires preflight and preserved failure evidence"
        )
    failure = _read_json(
        failure_dir / "failed-10-restored-migrations.json",
        label="preserved failed migration evidence",
    )
    failure_text = str(failure.get("failure") or "")
    if (
        failure.get("status") != "failed"
        or not failure_text.startswith(
            "V4 table column drift detected for st_factor_definition_v4:"
        )
    ):
        raise AcceptanceError("preserved migration failure is not the reviewed dry-run gate")
    trigger = _read_json(
        failure_dir / "failed-10-trigger-window.json",
        label="preserved failed trigger window",
    )
    trust = trigger.get("trust_transition")
    child = trigger.get("child")
    if (
        trigger.get("outcome") != "child_failed"
        or trigger.get("named_lock_acquired") is not True
        or trigger.get("named_lock_released") is not True
        or trigger.get("production_activation_allowed") is not False
        or not isinstance(child, Mapping)
        or child.get("started") is not True
        or int(child.get("return_code", 0)) != 2
        or not isinstance(trust, Mapping)
        or any(
            trust.get(name) is not True
            for name in (
                "enable_attempted",
                "enabled_verified",
                "restore_attempted",
                "restore_primary_verified",
                "restore_secondary_verified",
            )
        )
    ):
        raise AcceptanceError("failed trigger window did not close safely")

    preflight = _read_json(preflight_path, label="reviewed migration preflight")
    target = preflight.get("target")
    identity = preflight.get("schema_identity")
    if (
        preflight.get("status") != "plan_only"
        or preflight.get("mode") != "final-frozen"
        or preflight.get("ledger_before") != preflight.get("ledger_after")
        or not isinstance(target, Mapping)
        or not isinstance(identity, Mapping)
        or str(target.get("server_uuid") or "").lower()
        != str(current_plan.get("expected_target_uuid") or "").lower()
        or int(target.get("port", 0))
        != int(current_plan.get("expected_target_port", 0))
        or str(target.get("datadir") or "").casefold()
        != str(current_plan.get("expected_target_datadir") or "").casefold()
        or str(identity.get("server_uuid") or "").lower()
        != str(current_plan.get("expected_target_uuid") or "").lower()
        or int(identity.get("port", 0))
        != int(current_plan.get("expected_target_port", 0))
        or not str(identity.get("tls_cipher") or "").strip()
    ):
        raise AcceptanceError("reviewed migration preflight target or ledger is invalid")

    plan = preflight.get("plan")
    ledger = preflight.get("ledger_after")
    if not isinstance(plan, Mapping) or not isinstance(ledger, Mapping):
        raise AcceptanceError("reviewed migration preflight plan is invalid")
    ledger_names = {
        "v2": "schema_migration_v2",
        "v3": "schema_migration_v3",
        "v4": "schema_migration_v4",
    }
    version_counts: dict[str, int] = {}
    for family, ledger_name in ledger_names.items():
        planned = plan.get(family)
        ledger_rows = ledger.get(ledger_name)
        if not isinstance(planned, list) or not isinstance(ledger_rows, list):
            raise AcceptanceError(f"reviewed {family} migration plan is invalid")
        planned_versions = {
            str(item.get("version") or "")
            for item in planned
            if isinstance(item, Mapping) and item.get("status") == "exists"
        }
        ledger_versions = {
            str(item.get("version") or "")
            for item in ledger_rows
            if isinstance(item, Mapping)
        }
        if (
            not planned_versions
            or len(planned_versions) != len(planned)
            or planned_versions != ledger_versions
        ):
            raise AcceptanceError(f"reviewed {family} migrations are not all sealed as existing")
        version_counts[family] = len(planned_versions)

    failed_at = datetime.fromisoformat(
        str(state.get("failed_at_utc") or "").replace("Z", "+00:00")
    )
    preflight_finished = datetime.fromisoformat(
        str(preflight.get("finished_at_utc") or "").replace("Z", "+00:00")
    )
    if preflight_finished <= failed_at:
        raise AcceptanceError("reviewed migration preflight predates the failed step")
    return {
        "status": "passed",
        "preflight_sha256": _sha256(preflight_path),
        "failure_evidence_sha256": _sha256(
            failure_dir / "failed-10-restored-migrations.json"
        ),
        "trigger_window_sha256": _sha256(
            failure_dir / "failed-10-trigger-window.json"
        ),
        "version_counts": version_counts,
        "target_uuid": str(target["server_uuid"]).lower(),
        "target_port": int(target["port"]),
    }


def _build_steps(args: argparse.Namespace, p: Mapping[str, Path]) -> list[Step]:
    py = str(p["python"])
    manifest_py = str(p.get("manifest_python", p["python"]))
    manifest_environment = None
    if "manifest_site" in p:
        manifest_environment = dict(os.environ)
        manifest_environment["PYTHONPATH"] = str(p["manifest_site"])
    tools = ROOT / "tools"
    common_target = (
        "--expected-target-uuid", args.expected_target_uuid,
        "--expected-target-port", str(args.expected_target_port),
        "--expected-target-datadir", str(p["target_datadir"]),
    )
    schema_comparison_mode = str(p.get("schema_comparison_mode", "exact"))
    catchup = p["out"] / "01-binlog-final.json"
    migration_account = p["out"] / "02-migration-account.json"
    schema_audit = p["out"] / "03-schema-audit.json"
    fractional_repair = p["out"] / "04-fractional-datetime-repair.json"
    datetime_json = p["out"] / "05-datetime-defaults.json"
    source_data = p["out"] / "06-source-data.json"
    source_checkpoint = p["out"] / "06-source-data.checkpoint.json"
    target_data = p["out"] / "07-target-data.json"
    target_checkpoint = p["out"] / "07-target-data.checkpoint.json"
    comparison = p["out"] / "08-data-compare.json"
    checks_json = p["out"] / "09-check-constraints.json"
    migration_evidence = p["out"] / "10-restored-migrations.json"
    trigger_evidence = p["out"] / "10-trigger-window.json"
    smoke = p["out"] / "11-business-smoke.json"

    def j(path: Path, predicate, message: str):
        return lambda: _validate_json(path, predicate, message)

    steps: list[Step] = [
        Step(
            "final_binlog_catchup",
            (
                py, str(tools / "run_mysql55_to_mysql84_binlog_catchup.py"),
                "--mode", "final-frozen",
                "--source-option-file", str(p["source_option"]),
                "--dump-manifest", str(p["dump_manifest"]),
                "--binlog-dir", str(p["binlog_dir"]),
                "--mysqlbinlog", str(p["mysqlbinlog"]),
                "--mysql", str(p["mysql"]),
                "--target-option-file", str(p["target_admin"]),
                "--target-ssl-ca", str(p["target_ca"]),
                *common_target,
                "--checkpoint", str(p["out"] / "binlog-catchup.checkpoint.json"),
                "--segment-dir", str(p["out"] / "binlog-segments"),
                "--evidence", str(catchup),
                "--writes-frozen-ack", FINAL_FROZEN_ACK,
            ),
            (catchup,),
            j(
                catchup,
                lambda v: v.get("status") == "success"
                and v.get("mode") == "final-frozen"
                and v.get("target_may_be_tainted") is False
                and v.get("cursor_after") == v.get("requested_stop"),
                "final binlog catch-up evidence is not complete",
            ),
        ),
        Step(
            "provision_migration_account",
            (
                py, str(tools / "provision_mysql84_migration_account.py"),
                "--target-admin-option-file", str(p["target_admin"]),
                "--target-ssl-ca", str(p["target_ca"]),
                *common_target,
                "--migration-option-file", str(p["migration_option"]),
                "--evidence", str(migration_account),
                "--apply-ack", MIGRATION_ACCOUNT_ACK,
            ),
            (migration_account, p["migration_option"]),
            j(
                migration_account,
                lambda v: v.get("status") == "success"
                and v.get("secrets_in_evidence") is False
                and v.get("target", {}).get("server_uuid") == args.expected_target_uuid,
                "migration account evidence is invalid",
            ),
        ),
        Step(
            "schema_semantic_audit",
            (
                py, str(tools / "audit_mysql55_to_mysql84_schema.py"),
                "--source-option-file", str(p["source_option"]),
                "--target-option-file", str(p["target_admin"]),
                "--target-ssl-ca", str(p["target_ca"]),
                "--expected-source-version", "5.5.20-log",
                "--expected-target-version", "8.4.11",
                "--expected-target-uuid", args.expected_target_uuid,
                "--comparison-mode", schema_comparison_mode,
                "--output", str(schema_audit),
            ),
            (schema_audit,),
            j(
                schema_audit,
                lambda v: v.get("semantic_match") is True
                and all(item.get("difference_count") == 0 for item in v.get("comparisons", [])),
                "source/target schema semantics differ",
            ),
        ),
        Step(
            "repair_fractional_datetime_compatibility",
            (
                py, str(tools / "repair_mysql84_fractional_datetime_compat.py"),
                "--source-option-file", str(p["source_option"]),
                "--target-option-file", str(p["target_admin"]),
                "--target-ssl-ca", str(p["target_ca"]),
                *common_target,
                "--evidence", str(fractional_repair),
                "--apply-ack",
                "I_CONFIRM_SOURCE_WRITES_FROZEN_AND_REPAIR_ISOLATED_MYSQL84_TARGET",
            ),
            (fractional_repair,),
            j(
                fractional_repair,
                lambda v: v.get("status") == "success"
                and v.get("transaction_committed") is True
                and v.get("all_tables_match_frozen_source") is True
                and v.get("secrets_in_evidence") is False
                and v.get("target", {}).get("server_uuid") == args.expected_target_uuid,
                "fractional-DATETIME compatibility repair is incomplete",
            ),
        ),
        Step(
            "materialize_datetime_defaults",
            (
                py, str(tools / "materialize_mysql84_datetime_defaults.py"),
                "--schema", "probiga", "--apply",
                "--confirm-restored-target-offline",
                "--expected-server-uuid", args.expected_target_uuid,
                "--expected-server-port", str(args.expected_target_port),
                "--ssl-ca", str(p["target_ca"]), "--json",
            ),
            (datetime_json,),
            j(
                datetime_json,
                lambda v: v.get("status") == "ok" and v.get("complete") is True,
                "DATETIME default materialization is incomplete",
            ),
            None,
        ),
        Step(
            "capture_frozen_source_data",
            (
                manifest_py, str(tools / "mysql55_to_mysql84_data_manifest.py"),
                "capture-source",
                "--config", str(p["data_config"]),
                "--option-file", str(p["source_option"]),
                "--output", str(source_data),
                "--checkpoint", str(source_checkpoint),
                "--workers", str(args.workers),
                "--snapshot-mode", "cutover_writes_frozen",
                "--snapshot-id", args.snapshot_id,
                "--assert-ddl-frozen",
                "--assert-writes-frozen",
                "--writes-frozen-at", args.writes_frozen_at,
                "--restore-artifact-sha256", args.restored_artifact_sha256,
                *(["--resume"] if args.resume and source_checkpoint.exists() else []),
            ),
            (source_data, source_checkpoint),
            j(
                source_data,
                lambda v: v.get("role") == "source"
                and v.get("snapshot", {}).get("eligible_for_final_cutover_comparison") is True
                and v.get("coverage", {}).get("exact_counts_cover_all_base_tables") is True,
                "frozen source data manifest is not cutover-eligible",
            ),
            manifest_environment,
        ),
        Step(
            "capture_quiescent_target_data",
            (
                manifest_py, str(tools / "mysql55_to_mysql84_data_manifest.py"),
                "capture-target",
                "--config", str(p["data_config"]),
                "--option-file", str(p["target_admin"]),
                "--ssl-ca", str(p["target_ca"]),
                "--source-manifest", str(source_data),
                "--output", str(target_data),
                "--checkpoint", str(target_checkpoint),
                "--workers", str(args.workers),
                "--assert-target-quiescent",
                "--restored-artifact-sha256", args.restored_artifact_sha256,
                *(["--resume"] if args.resume and target_checkpoint.exists() else []),
            ),
            (target_data, target_checkpoint),
            j(
                target_data,
                lambda v: v.get("role") == "target"
                and v.get("endpoint", {}).get("server_uuid") == args.expected_target_uuid
                and v.get("coverage", {}).get("exact_counts_cover_all_base_tables") is True,
                "target data manifest is incomplete",
            ),
            manifest_environment,
        ),
        Step(
            "compare_business_data",
            (
                manifest_py, str(tools / "mysql55_to_mysql84_data_manifest.py"),
                "compare",
                "--source-manifest", str(source_data),
                "--target-manifest", str(target_data),
                "--output", str(comparison),
                "--allow-reviewed-post-migration-transitions",
                "--source-option-file", str(p["source_option"]),
                "--target-option-file", str(p["target_admin"]),
                "--target-ssl-ca", str(p["target_ca"]),
            ),
            (comparison,),
            j(
                comparison,
                lambda v: v.get("result", {}).get("configured_checks_match") is True
                and v.get("result", {}).get("exact_counts_cover_all_base_tables") is True
                and v.get("result", {}).get("risk_based_cutover_checks_passed") is True
                and v.get("result", {}).get(
                    "reviewed_post_migration_transitions_applied"
                )
                is True
                and not v.get("mismatches"),
                "business data comparison did not pass the cutover gate",
            ),
            manifest_environment,
        ),
    ]

    migration_environment = None
    if p["migration_option"].exists():
        migration_environment = _migration_environment(
            p["migration_option"], port=args.expected_target_port, ca=p["target_ca"]
        )
    steps.extend(
        [
            Step(
                "materialize_check_constraints",
                (
                    py, str(tools / "materialize_mysql84_check_constraints.py"),
                    "--schema", "probiga", "--apply",
                    "--confirm-restored-target-offline",
                    "--expected-server-uuid", args.expected_target_uuid,
                    "--expected-server-port", str(args.expected_target_port),
                    "--ssl-ca", str(p["target_ca"]), "--json",
                ),
                (checks_json,),
                j(
                    checks_json,
                    lambda v: v.get("status") == "ok" and v.get("complete") is True,
                    "CHECK constraint materialization is incomplete",
                ),
                migration_environment,
            ),
            Step(
                "v2_v3_v4_migrations",
                (
                    py, str(tools / "run_mysql84_trigger_migration_window.py"),
                    "--admin-option-file", str(p["target_admin"]),
                    "--target-ssl-ca", str(p["target_ca"]),
                    "--expected-server-uuid", args.expected_target_uuid,
                    "--expected-server-port", str(args.expected_target_port),
                    "--business-offline-ack", "BUSINESS_WRITES_STOPPED",
                    "--change-id", args.change_id,
                    "--evidence", str(trigger_evidence),
                    "--",
                    py, str(tools / "run_mysql84_restored_migrations.py"),
                    "--mode", "final-frozen",
                    "--schema", "probiga",
                    "--admin-option-file", str(p["migration_option"]),
                    "--ssl-ca", str(p["target_ca"]),
                    "--expected-server-uuid", args.expected_target_uuid,
                    "--expected-server-port", str(args.expected_target_port),
                    "--expected-datadir", str(p["target_datadir"]),
                    "--offline-ack", "BUSINESS_WRITES_STOPPED",
                    "--evidence", str(migration_evidence),
                    "--allow-execution-evidence",
                ),
                (trigger_evidence, migration_evidence),
                lambda: {
                    "trigger": _validate_json(
                        trigger_evidence,
                        lambda v: v.get("outcome") == "success"
                        and v.get("child", {}).get("return_code") == 0
                        and v.get("trust_transition", {}).get("restore_primary_verified") is True
                        and v.get("trust_transition", {}).get("restore_secondary_verified") is True,
                        "guarded trigger migration window did not close safely",
                    ),
                    "migration": _validate_json(
                        migration_evidence,
                        lambda v: v.get("status") == "ok"
                        and v.get("replay_ledger_after") == v.get("ledger_after"),
                        "V2/V3/V4 migration or replay did not pass",
                    ),
                },
            ),
            Step(
                "read_only_business_smoke",
                (
                    py, str(tools / "mysql84_restored_business_smoke.py"),
                    "--admin-option-file", str(p["target_admin"]),
                    "--ssl-ca", str(p["target_ca"]),
                    "--expected-server-uuid", args.expected_target_uuid,
                    "--expected-server-port", str(args.expected_target_port),
                    "--expected-datadir", str(p["target_datadir"]),
                    "--evidence", str(smoke),
                ),
                (smoke,),
                j(
                    smoke,
                    lambda v: v.get("status") == "ok"
                    and v.get("read_only_transaction") is True
                    and v.get("target", {}).get("server_uuid") == args.expected_target_uuid,
                    "read-only business smoke failed",
                ),
            ),
        ]
    )
    return steps


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--manifest-python", type=Path)
    parser.add_argument("--manifest-site-packages", type=Path)
    parser.add_argument("--source-option-file", type=Path, required=True)
    parser.add_argument("--dump-manifest", type=Path, required=True)
    parser.add_argument("--binlog-dir", type=Path, required=True)
    parser.add_argument("--mysqlbinlog", type=Path, required=True)
    parser.add_argument("--mysql", type=Path, required=True)
    parser.add_argument("--target-admin-option-file", type=Path, required=True)
    parser.add_argument("--target-ssl-ca", type=Path, required=True)
    parser.add_argument("--target-migration-option-file", type=Path, required=True)
    parser.add_argument("--freeze-ready-evidence", type=Path, required=True)
    parser.add_argument("--freeze-heartbeat", type=Path, required=True)
    parser.add_argument("--expected-target-uuid", required=True)
    parser.add_argument("--expected-target-port", type=int, required=True)
    parser.add_argument("--expected-target-datadir", type=Path, required=True)
    parser.add_argument("--data-manifest-config", type=Path, required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--writes-frozen-at", required=True)
    parser.add_argument("--restored-artifact-sha256", required=True)
    parser.add_argument("--change-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--resume-reacquired-freeze-ack")
    parser.add_argument("--resume-reviewed-migration-preflight-ack")
    parser.add_argument("--reviewed-migration-preflight-evidence", type=Path)
    parser.add_argument("--reviewed-migration-failure-evidence-dir", type=Path)
    parser.add_argument("--execute-ack", required=True)
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.execute_ack != EXECUTE_ACK:
        raise AcceptanceError("exact frozen-source final-acceptance acknowledgement is required")
    target_uuid = str(args.expected_target_uuid).strip().lower()
    if _UUID_RE.fullmatch(target_uuid) is None:
        raise AcceptanceError("expected target UUID is invalid")
    args.expected_target_uuid = target_uuid
    if args.expected_target_port == 3306 or not 1 <= args.expected_target_port <= 65535:
        raise AcceptanceError("final acceptance requires an isolated non-3306 target")
    if not 1 <= args.workers <= 8:
        raise AcceptanceError("workers must be in 1..8")
    restored_sha = str(args.restored_artifact_sha256).strip().lower()
    if _SHA256_RE.fullmatch(restored_sha) is None:
        raise AcceptanceError("restored artifact SHA-256 is invalid")
    args.restored_artifact_sha256 = restored_sha
    try:
        frozen_at = datetime.fromisoformat(args.writes_frozen_at)
    except ValueError as exc:
        raise AcceptanceError("writes-frozen-at must be ISO-8601") from exc
    if frozen_at.tzinfo is None:
        raise AcceptanceError("writes-frozen-at must include a timezone")

    output = args.output_dir.expanduser().resolve(strict=False)
    output.mkdir(parents=True, exist_ok=True)
    paths = {
        "out": output,
        "python": _absolute_file(args.python, label="Python executable"),
        "source_option": _absolute_file(args.source_option_file, label="source option file"),
        "dump_manifest": _absolute_file(args.dump_manifest, label="dump manifest"),
        "binlog_dir": _absolute_dir(args.binlog_dir, label="binlog directory"),
        "mysqlbinlog": _absolute_file(args.mysqlbinlog, label="mysqlbinlog"),
        "mysql": _absolute_file(args.mysql, label="mysql client"),
        "target_admin": _absolute_file(args.target_admin_option_file, label="target admin option file"),
        "target_ca": _absolute_file(args.target_ssl_ca, label="target CA"),
        "migration_option": args.target_migration_option_file.expanduser().resolve(strict=False),
        "freeze_ready": _absolute_file(args.freeze_ready_evidence, label="freeze ready evidence"),
        "freeze_heartbeat": _absolute_file(args.freeze_heartbeat, label="freeze heartbeat"),
        "target_datadir": _absolute_dir(args.expected_target_datadir, label="target datadir"),
        "data_config": _absolute_file(args.data_manifest_config, label="data manifest config"),
    }
    if args.reviewed_migration_preflight_evidence is not None:
        paths["reviewed_migration_preflight"] = _absolute_file(
            args.reviewed_migration_preflight_evidence,
            label="reviewed migration preflight evidence",
        )
    if args.reviewed_migration_failure_evidence_dir is not None:
        paths["reviewed_migration_failure_dir"] = _absolute_dir(
            args.reviewed_migration_failure_evidence_dir,
            label="reviewed migration failure evidence directory",
        )
    paths["manifest_python"] = (
        _absolute_file(args.manifest_python, label="manifest Python executable")
        if args.manifest_python is not None
        else paths["python"]
    )
    if args.manifest_site_packages is not None:
        paths["manifest_site"] = _absolute_dir(
            args.manifest_site_packages,
            label="manifest Python site-packages",
        )
        _absolute_file(
            paths["manifest_site"] / "pymysql" / "__init__.py",
            label="manifest PyMySQL package",
        )
    if not paths["migration_option"].is_absolute() or not paths["migration_option"].parent.is_dir():
        raise AcceptanceError("migration option output requires an existing absolute parent")
    if paths["migration_option"] == paths["target_admin"]:
        raise AcceptanceError("migration and administrator option files must differ")
    data_config = load_config(paths["data_config"])
    _validate_data_config(
        data_config, target_uuid=target_uuid, target_port=args.expected_target_port
    )
    paths["schema_comparison_mode"] = str(
        data_config.raw["catalog_comparison"]["mode"]
    )
    dump_sha = _dump_sha256(paths["dump_manifest"])
    if dump_sha == restored_sha:
        # Allowed: the raw dump can be directly restorable.  Most 5.5 dumps use
        # a sanitized artifact, so inequality is also expected and supported.
        pass

    validate_freeze_guard_with_retry(paths["freeze_ready"], paths["freeze_heartbeat"])
    plan = _plan(args, paths)
    plan_sha = _json_sha256(plan)
    state_path = output / "final-acceptance.state.json"
    final_path = output / "final-acceptance.json"
    completed: list[dict[str, Any]] = []
    resume_guard: dict[str, Any] | None = None
    if args.resume:
        state = _read_json(state_path, label="final acceptance state")
        if state.get("plan_sha256") != plan_sha:
            if state.get("failed_step") == "v2_v3_v4_migrations":
                if (
                    args.resume_reviewed_migration_preflight_ack
                    != REVIEWED_MIGRATION_PREFLIGHT_RESUME_ACK
                ):
                    raise AcceptanceError(
                        "reviewed migration-preflight resume acknowledgement is required"
                    )
            elif args.resume_reacquired_freeze_ack != REACQUIRED_FREEZE_ACK:
                raise AcceptanceError(
                    "resume plan differs from checkpointed final acceptance"
                )
            resume_guard = _validate_reacquired_freeze_resume(
                state=state,
                current_plan=plan,
                paths=paths,
            )
            stored_plan = state.get("plan")
            if not isinstance(stored_plan, dict):
                raise AcceptanceError("failed acceptance state plan is invalid")
            plan = dict(stored_plan)
            plan_sha = str(state.get("plan_sha256") or "")
            if _SHA256_RE.fullmatch(plan_sha) is None or _json_sha256(plan) != plan_sha:
                raise AcceptanceError("failed acceptance state plan hash is invalid")
        raw_completed = state.get("steps", [])
        if not isinstance(raw_completed, list):
            raise AcceptanceError("final acceptance state steps are invalid")
        completed = list(raw_completed)
    elif state_path.exists() or final_path.exists():
        raise AcceptanceError("output directory already contains acceptance state/evidence")

    steps = _build_steps(args, paths)
    completed_names = [str(item.get("name")) for item in completed]
    if completed_names != [step.name for step in steps[: len(completed_names)]]:
        raise AcceptanceError("checkpointed step order differs from the current workflow")
    for index, recorded in enumerate(completed):
        step = steps[index]
        recorded_outputs = recorded.get("outputs")
        if not isinstance(recorded_outputs, list) or len(recorded_outputs) != len(
            step.outputs
        ):
            raise AcceptanceError(
                f"checkpointed output list differs for step {step.name}"
            )
        for expected_path, recorded_output in zip(step.outputs, recorded_outputs):
            if not isinstance(recorded_output, Mapping):
                raise AcceptanceError(
                    f"checkpointed output entry is invalid for step {step.name}"
                )
            actual_path = Path(str(recorded_output.get("path") or "")).resolve(
                strict=True
            )
            if actual_path != expected_path.resolve(strict=True):
                raise AcceptanceError(
                    f"checkpointed output path differs for step {step.name}"
                )
            recorded_sha = str(recorded_output.get("sha256") or "").lower()
            if _SHA256_RE.fullmatch(recorded_sha) is None or _sha256(
                actual_path
            ) != recorded_sha:
                raise AcceptanceError(
                    f"checkpointed output hash changed for step {step.name}"
                )

    started_at = _utc_now()
    for index, step in enumerate(steps, start=1):
        validate_freeze_guard_with_retry(paths["freeze_ready"], paths["freeze_heartbeat"])
        already_completed = step.name in completed_names
        for artifact in step.outputs:
            _new_output(artifact, resume=args.resume, completed=already_completed)
        if already_completed:
            step.validator()
            continue
        if step.name in {"materialize_datetime_defaults", "materialize_check_constraints"}:
            if not paths["migration_option"].exists():
                raise AcceptanceError("migration account step did not create its option file")
            step = Step(
                step.name,
                step.command,
                step.outputs,
                step.validator,
                _migration_environment(
                    paths["migration_option"],
                    port=args.expected_target_port,
                    ca=paths["target_ca"],
                ),
            )
        stdout = step.outputs[0] if step.name in {
            "materialize_datetime_defaults", "materialize_check_constraints"
        } else output / f"{index:02d}-{step.name}.stdout.log"
        stderr = output / f"{index:02d}-{step.name}.stderr.log"
        if args.resume and (stdout.exists() or stderr.exists()):
            attempt = 1
            while True:
                resumed_stdout = output / f"{index:02d}-{step.name}.resume-{attempt}.stdout.log"
                resumed_stderr = output / f"{index:02d}-{step.name}.resume-{attempt}.stderr.log"
                if not resumed_stdout.exists() and not resumed_stderr.exists():
                    stdout, stderr = resumed_stdout, resumed_stderr
                    break
                attempt += 1
        if stdout.exists() or stderr.exists():
            raise AcceptanceError(f"untrusted pre-existing log for step {step.name}")
        step_started = _utc_now()
        return_code = _run_step(step, stdout=stdout, stderr=stderr)
        validate_freeze_guard_with_retry(paths["freeze_ready"], paths["freeze_heartbeat"])
        if return_code != 0:
            failure_state = {
                "schema_version": 1,
                "tool": "run_mysql84_final_acceptance",
                "status": "failed",
                "plan_sha256": plan_sha,
                "plan": plan,
                "steps": completed,
                "failed_step": step.name,
                "failed_step_return_code": return_code,
                "failed_at_utc": _utc_now(),
                "target_may_be_tainted": step.name in {
                    "repair_fractional_datetime_compatibility",
                    "materialize_datetime_defaults",
                    "materialize_check_constraints",
                    "v2_v3_v4_migrations",
                },
            }
            _atomic_json(state_path, failure_state, replace=state_path.exists())
            raise AcceptanceError(f"step failed: {step.name}")
        validated = step.validator()
        completed.append(
            {
                "name": step.name,
                "status": "passed",
                "started_at_utc": step_started,
                "finished_at_utc": _utc_now(),
                "command_sha256": _json_sha256(list(step.command)),
                "outputs": [
                    {"path": str(path), "sha256": _sha256(path)}
                    for path in step.outputs
                ],
                "validator_summary_sha256": _json_sha256(validated),
            }
        )
        completed_names.append(step.name)
        state = {
            "schema_version": 1,
            "tool": "run_mysql84_final_acceptance",
            "status": "in_progress",
            "plan_sha256": plan_sha,
            "plan": plan,
            "steps": completed,
            "updated_at_utc": _utc_now(),
            "target_may_be_tainted": False,
            "resume_guard": resume_guard,
        }
        _atomic_json(state_path, state, replace=state_path.exists())

    smoke = _read_json(output / "11-business-smoke.json", label="business smoke")
    comparison = _read_json(output / "08-data-compare.json", label="data comparison")
    result = {
        "schema_version": 1,
        "tool": "run_mysql84_final_acceptance",
        "status": "passed",
        "cutover_ready": True,
        "started_at_utc": started_at,
        "finished_at_utc": _utc_now(),
        "plan_sha256": plan_sha,
        "target": smoke["target"],
        "snapshot_id": args.snapshot_id,
        "dump_manifest_sha256": _sha256(paths["dump_manifest"]),
        "restored_artifact_sha256": restored_sha,
        "data_comparison": comparison["result"],
        "steps": completed,
        "source_writes_remain_frozen": True,
        "source_global_read_lock_verified": True,
        "production_service_switched": False,
        "production_env_promoted": False,
        "production_trading_activation_changed": False,
        "resume_guard": resume_guard,
    }
    _atomic_json(final_path, result, replace=False)
    state = dict(result)
    state["status"] = "complete"
    _atomic_json(state_path, state, replace=True)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run(args)
    except (AcceptanceError, OSError, ValueError) as exc:
        try:
            output = args.output_dir.expanduser().resolve(strict=False)
            output.mkdir(parents=True, exist_ok=True)
            error_path = output / "final-acceptance.driver-error.json"
            _atomic_json(
                error_path,
                {
                    "schema_version": 1,
                    "tool": "run_mysql84_final_acceptance",
                    "status": "failed",
                    "failed_at_utc": _utc_now(),
                    "exception_type": type(exc).__name__,
                    "message": str(exc),
                    "secrets_in_evidence": False,
                },
                replace=error_path.exists(),
            )
        except Exception:
            pass
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
