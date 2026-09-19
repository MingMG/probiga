#!/usr/bin/env python3
"""Publish signed component identities in a permanent MySQL ledger.

The root signing key is anchored by privileged table metadata. initialize is a
coordinated operation; publish and verify never execute DDL or change grants.
"""
from __future__ import annotations

import argparse
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text  # noqa: E402

from server.common.component_release import (  # noqa: E402
    load_runtime_component_release,
    validate_component_release,
)
from server.common.component_release_attestation import load_component_attestation  # noqa: E402
from server.common.component_release_ledger import (  # noqa: E402
    TABLE_NAME, build_component_ledger_metadata, load_component_ledger_public_key,
    load_signing_key, sign_component_manifest, signing_public_key,
)
from tools import prepare_strategy_governance_schema as boundary_policy  # noqa: E402

PUBLICATION_LOCK_NAME = "probiga:component-release:publish"
_RUNTIME_CONFIG_PATH = Path("/opt/ProBigA/.env")


class ComponentPublicationError(RuntimeError):
    """A fixed error category; database messages and credentials never escape."""

    def __init__(self, category: str):
        self.category = category
        super().__init__(category)


def _load_protected_runtime_env() -> None:
    """Load the existing protected runtime credentials, never release files."""
    try:
        boundary_policy._require_root_execution()
        path = _RUNTIME_CONFIG_PATH
        parent = path.parent
        file_info = path.lstat()
        parent_info = parent.lstat()
        if (
            not stat.S_ISREG(file_info.st_mode)
            or file_info.st_uid != 0
            or file_info.st_nlink != 1
            or stat.S_IMODE(file_info.st_mode) != 0o640
            or not stat.S_ISDIR(parent_info.st_mode)
            or parent_info.st_uid != 0
            or stat.S_IMODE(parent_info.st_mode) != 0o755
            or parent.resolve(strict=True) != parent
        ):
            raise ValueError("runtime configuration is not protected")
        boundary_policy.load_project_env(path)
    except Exception:
        raise ComponentPublicationError("RUNTIME_CONFIGURATION_UNSAFE") from None


def _safe_manifest(manifest_path: str) -> dict[str, str]:
    previous = os.environ.get("PROBIGA_COMPONENT_RELEASE_PATH")
    if previous is not None and previous != manifest_path:
        raise ComponentPublicationError("MANIFEST_INVALID")
    os.environ["PROBIGA_COMPONENT_RELEASE_PATH"] = manifest_path
    try:
        return load_runtime_component_release()
    except Exception:
        raise ComponentPublicationError("MANIFEST_INVALID") from None
    finally:
        if previous is None:
            os.environ.pop("PROBIGA_COMPONENT_RELEASE_PATH", None)
        else:
            os.environ["PROBIGA_COMPONENT_RELEASE_PATH"] = previous


def _require_runtime_metadata_authority(connection: Any) -> None:
    try:
        grants = boundary_policy._sa_grants(connection)
        boundary_policy._validate_runtime_grants(grants)
        summary = boundary_policy._runtime_grant_summary(grants)
        if (
            summary["observed_contract"] != boundary_policy.TARGET_RUNTIME_PRIVILEGE_CONTRACT
            or summary["persistent_ddl_privileges"]
            or summary["grant_option"]
        ):
            raise ValueError("runtime can mutate protected metadata")
    except Exception:
        raise ComponentPublicationError("RUNTIME_METADATA_AUTHORITY_UNSAFE") from None


def _has_ledger(connection: Any) -> bool:
    count = connection.execute(text(
        "SELECT COUNT(*) FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA='probiga' AND TABLE_NAME=:table_name"
    ), {"table_name": TABLE_NAME}).scalar_one()
    if count not in (0, 1):
        raise ComponentPublicationError("COMPONENT_METADATA_UNAVAILABLE")
    return count == 1


def _has_record(connection: Any, manifest: dict[str, str]) -> bool:
    count = connection.execute(text(
        "SELECT COUNT(*) FROM probiga.st_component_release_manifest "
        "WHERE linux_build_sha=:linux_build_sha"
    ), {"linux_build_sha": manifest["linux_build_sha"]}).scalar_one()
    if count not in (0, 1):
        raise ComponentPublicationError("COMPONENT_METADATA_UNAVAILABLE")
    return count == 1


def _require_exact_metadata(connection: Any, manifest: dict[str, str]) -> None:
    try:
        observed = load_component_attestation(connection, manifest["linux_build_sha"])
        if observed != manifest:
            raise ValueError("component identity differs")
    except Exception:
        raise ComponentPublicationError("COMPONENT_IDENTITY_CONFLICT") from None


def _require_lineage(connection: Any, manifest: dict[str, str]) -> None:
    if manifest["scope"] == "COORDINATED":
        return
    try:
        if manifest["parent_linux_build_sha"] == manifest["linux_build_sha"]:
            raise ValueError("Linux release cannot be its own parent")
        parent = load_component_attestation(connection, manifest["parent_linux_build_sha"])
        anchor = load_component_attestation(connection, manifest["contract_build_sha"])
        if anchor["scope"] != "COORDINATED" or anchor["linux_build_sha"] != manifest["contract_build_sha"]:
            raise ValueError("contract anchor is not coordinated")
        for prior in (parent, anchor):
            if any(prior[field] != manifest[field] for field in (
                "windows_build_sha", "contract_build_sha", "contract_sha256",
            )):
                raise ValueError("component contract lineage differs")
    except Exception:
        raise ComponentPublicationError("COMPONENT_LINEAGE_INVALID") from None


def _initialize_locked(connection: Any, manifest: dict[str, str]) -> str:
    if manifest["scope"] != "COORDINATED":
        raise ComponentPublicationError("COORDINATED_INITIALIZATION_REQUIRED")
    if _has_ledger(connection):
        public_key = load_component_ledger_public_key(connection)
        key = load_signing_key(create=False)
        if signing_public_key(key) != public_key:
            raise ComponentPublicationError("COMPONENT_SIGNING_KEY_DIFFERS")
        return "initialized"
    key = load_signing_key(create=True)
    comment = build_component_ledger_metadata(signing_public_key(key))
    connection.execute(text(
        "CREATE TABLE `probiga`.`st_component_release_manifest` ("
        "`linux_build_sha` CHAR(40) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, "
        "`manifest_json` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, "
        "`signature` CHAR(88) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, "
        "PRIMARY KEY (`linux_build_sha`)) ENGINE=InnoDB "
        "DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT=:component_comment"
    ), {"component_comment": comment})
    if load_component_ledger_public_key(connection) != signing_public_key(key):
        raise ComponentPublicationError("COMPONENT_SIGNING_KEY_DIFFERS")
    return "initialized"


def _publish_locked(connection: Any, manifest: dict[str, str]) -> str:
    public_key = load_component_ledger_public_key(connection)
    key = load_signing_key(create=False)
    if signing_public_key(key) != public_key:
        raise ComponentPublicationError("COMPONENT_SIGNING_KEY_DIFFERS")
    if _has_record(connection, manifest):
        _require_exact_metadata(connection, manifest)
        return "existing"
    manifest_json, signature = sign_component_manifest(manifest, key)
    connection.execute(text(
        "INSERT INTO probiga.st_component_release_manifest "
        "(linux_build_sha, manifest_json, signature) VALUES "
        "(:linux_build_sha, :manifest_json, :signature)"
    ), {"linux_build_sha": manifest["linux_build_sha"], "manifest_json": manifest_json, "signature": signature})
    connection.commit()
    _require_exact_metadata(connection, manifest)
    return "created"


def _apply(boundary: Any, manifest: dict[str, str], *, mode: str) -> dict[str, str]:
    with boundary.runtime_engine.connect() as runtime:
        _require_runtime_metadata_authority(runtime)
        if mode != "initialize":
            load_component_ledger_public_key(runtime)
            _require_lineage(runtime, manifest)
        if mode == "verify":
            _require_exact_metadata(runtime, manifest)
            return {"status": "verified", "linux_build_sha": manifest["linux_build_sha"]}
    if boundary.migrator_engine is None:
        raise ComponentPublicationError("DATABASE_BOUNDARY_INVALID")
    with boundary.migrator_engine.connect() as migrator:
        acquired = migrator.execute(text("SELECT GET_LOCK(:lock_name, 0)"), {"lock_name": PUBLICATION_LOCK_NAME}).scalar_one()
        if acquired != 1:
            raise ComponentPublicationError("COMPONENT_LOCK_BUSY")
        try:
            # These immutable parents cannot change, but re-read them in the
            # serialized publication session before issuing any CREATE.
            if mode == "initialize":
                status = _initialize_locked(migrator, manifest)
            else:
                _require_lineage(migrator, manifest)
                status = _publish_locked(migrator, manifest)
        finally:
            try:
                released = migrator.execute(text("SELECT RELEASE_LOCK(:lock_name)"), {"lock_name": PUBLICATION_LOCK_NAME}).scalar_one()
            except Exception:
                raise ComponentPublicationError("COMPONENT_LOCK_RELEASE_FAILED") from None
            if released != 1:
                raise ComponentPublicationError("COMPONENT_LOCK_RELEASE_FAILED")
    with boundary.runtime_engine.connect() as runtime:
        _require_runtime_metadata_authority(runtime)
        if mode == "initialize":
            load_component_ledger_public_key(runtime)
        else:
            _require_exact_metadata(runtime, manifest)
    return {"status": status, "linux_build_sha": manifest["linux_build_sha"]}


def publish_component_release(manifest_path: str, *, mode: str) -> dict[str, str]:
    if mode not in {"initialize", "publish", "verify"}:
        raise ComponentPublicationError("INVALID_ARGUMENT")
    boundary = None
    try:
        _load_protected_runtime_env()
        manifest = _safe_manifest(manifest_path)
        # Credentials, TLS, server UUID, account identity and trust=OFF are
        # validated by the established root-only boundary. No migration runs.
        boundary = boundary_policy._open_boundary(include_migrator=True, expected_trust=0)
        validate_component_release(manifest)
        return _apply(boundary, manifest, mode=mode)
    except ComponentPublicationError:
        raise
    except Exception:
        raise ComponentPublicationError("COMPONENT_PUBLICATION_FAILED") from None
    finally:
        if boundary is not None:
            try:
                boundary.runtime_engine.dispose()
                if boundary.migrator_engine is not None:
                    boundary.migrator_engine.dispose()
            except Exception:
                raise ComponentPublicationError("COMPONENT_BOUNDARY_CLOSE_FAILED") from None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--mode", choices=("initialize", "publish", "verify"), required=True)
    args = parser.parse_args(argv)
    try:
        result = publish_component_release(args.manifest, mode=args.mode)
    except ComponentPublicationError as exc:
        print(json.dumps({"status": "error", "category": exc.category}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
