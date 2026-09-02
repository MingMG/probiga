"""Immutable production release identity that does not require a Git checkout."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import text


MANIFEST_SCHEMA = "probiga.release-manifest.v1"
MANIFEST_FILE_NAME = "probiga.release.json"
REGISTRY_TABLE = "st_release_manifest"
SHA40_RE = re.compile(r"^[0-9a-f]{40}$")
SHA64_RE = re.compile(r"^[0-9a-f]{64}$")
REQUIRED_REGISTRY_COLUMNS = frozenset(
    {
        "id",
        "release_id",
        "source_tree_hash",
        "migration_version",
        "built_at",
        "artifact_hash",
        "manifest_sha256",
        "registered_at",
    }
)


class ReleaseManifestError(RuntimeError):
    pass


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _duplicate_key(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ReleaseManifestError(f"duplicate release manifest key: {key}")
        result[key] = value
    return result


def validate_release_manifest(payload: Mapping[str, object]) -> dict[str, str]:
    normalized = {
        "schema": str(payload.get("schema") or ""),
        "release_id": str(payload.get("release_id") or "").strip().lower(),
        "source_tree_hash": str(payload.get("source_tree_hash") or "").strip().lower(),
        "migration_version": str(payload.get("migration_version") or "").strip(),
        "built_at": str(payload.get("built_at") or "").strip(),
        "artifact_hash": str(payload.get("artifact_hash") or "").strip().lower(),
        "manifest_sha256": str(payload.get("manifest_sha256") or "").strip().lower(),
    }
    if normalized["schema"] != MANIFEST_SCHEMA:
        raise ReleaseManifestError("unsupported release manifest schema")
    if SHA40_RE.fullmatch(normalized["release_id"]) is None:
        raise ReleaseManifestError("release manifest release_id is invalid")
    if SHA64_RE.fullmatch(normalized["source_tree_hash"]) is None:
        raise ReleaseManifestError("release manifest source_tree_hash is invalid")
    if SHA64_RE.fullmatch(normalized["artifact_hash"]) is None:
        raise ReleaseManifestError("release manifest artifact_hash is invalid")
    if not normalized["migration_version"] or len(normalized["migration_version"]) > 128:
        raise ReleaseManifestError("release manifest migration_version is invalid")
    try:
        parsed_built_at = datetime.fromisoformat(
            normalized["built_at"].replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ReleaseManifestError("release manifest built_at is invalid") from exc
    if parsed_built_at.tzinfo is None:
        raise ReleaseManifestError("release manifest built_at must include timezone")
    core = {key: value for key, value in normalized.items() if key != "manifest_sha256"}
    if (
        SHA64_RE.fullmatch(normalized["manifest_sha256"]) is None
        or normalized["manifest_sha256"] != _canonical_sha256(core)
    ):
        raise ReleaseManifestError("release manifest seal is invalid")
    if set(payload) != set(normalized):
        raise ReleaseManifestError("release manifest fields differ")
    return normalized


def build_release_manifest(
    *,
    release_id: str,
    source_tree_hash: str,
    migration_version: str,
    built_at: str,
    artifact_components: Mapping[str, object],
) -> dict[str, str]:
    core = {
        "schema": MANIFEST_SCHEMA,
        "release_id": str(release_id).strip().lower(),
        "source_tree_hash": str(source_tree_hash).strip().lower(),
        "migration_version": str(migration_version).strip(),
        "built_at": str(built_at).strip(),
        "artifact_hash": _canonical_sha256(dict(artifact_components)),
    }
    return validate_release_manifest(
        {**core, "manifest_sha256": _canonical_sha256(core)}
    )


def release_manifest_path(root: Path | str) -> Path:
    configured = str(os.environ.get("PROBIGA_RELEASE_MANIFEST_PATH") or "").strip()
    return Path(configured) if configured else Path(root) / MANIFEST_FILE_NAME


def load_release_manifest(root: Path | str) -> dict[str, str]:
    path = release_manifest_path(root)
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_duplicate_key,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ReleaseManifestError(f"non-finite release manifest value: {value}")
            ),
        )
    except ReleaseManifestError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReleaseManifestError("release manifest is unreadable") from exc
    if not isinstance(payload, dict):
        raise ReleaseManifestError("release manifest root must be an object")
    return validate_release_manifest(payload)


def write_release_manifest(
    root: Path | str,
    payload: Mapping[str, object],
) -> Path:
    """Create one sealed manifest; an existing identical release is reused."""

    normalized = validate_release_manifest(payload)
    path = release_manifest_path(root)
    if path.exists():
        existing = load_release_manifest(root)
        stable_fields = {
            "schema",
            "release_id",
            "source_tree_hash",
            "migration_version",
            "artifact_hash",
        }
        if any(existing[field] != normalized[field] for field in stable_fields):
            raise ReleaseManifestError("existing release manifest identity differs")
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o444)
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    return path


def verify_runtime_release_manifest(root: Path | str) -> dict[str, object]:
    manifest = load_release_manifest(root)
    expected_release = str(
        os.environ.get("PROBIGA_BUILD_COMMIT_SHA")
        or os.environ.get("PROBIGA_EXPECTED_GIT_SHA")
        or ""
    ).strip().lower()
    expected_tree = str(
        os.environ.get("PROBIGA_RELEASE_TREE_SHA256") or ""
    ).strip().lower()
    errors: list[str] = []
    if expected_release and manifest["release_id"] != expected_release:
        errors.append("release_id_mismatch")
    if expected_tree and manifest["source_tree_hash"] != expected_tree:
        errors.append("source_tree_hash_mismatch")
    path = release_manifest_path(root)
    stat_result = path.stat()
    read_only = stat_result.st_mode & 0o222 == 0
    if os.name == "posix":
        # Root-owned and not group/world-writable is the deployment trust seal.
        read_only = stat_result.st_uid == 0 and stat_result.st_mode & 0o022 == 0
    if not read_only:
        errors.append("manifest_is_mutable")
    return {
        "manifest": manifest,
        "manifest_path": str(path),
        "verified": not errors,
        "read_only": read_only,
        "errors": errors,
    }


def _dialect_name(connection) -> str:
    return str(getattr(getattr(connection, "dialect", None), "name", "")).lower()


def _registry_columns(connection) -> set[str]:
    if _dialect_name(connection) == "sqlite":
        return {
            str(row[1])
            for row in connection.execute(
                text(f"PRAGMA table_info({REGISTRY_TABLE})")
            ).fetchall()
        }
    return {
        str(row[0])
        for row in connection.execute(text(f"SHOW COLUMNS FROM {REGISTRY_TABLE}")).fetchall()
    }


def validate_release_manifest_runtime_schema(engine) -> dict[str, object]:
    with engine.connect() as connection:
        columns = _registry_columns(connection)
    missing = sorted(REQUIRED_REGISTRY_COLUMNS - columns)
    if missing:
        raise RuntimeError(f"release manifest registry differs: missing_columns={missing}")
    return {
        "table": REGISTRY_TABLE,
        "physical_contract_verified": True,
        "runtime_ddl_required": False,
        "read_only": True,
    }


def privileged_migrate_release_manifest_schema(engine) -> dict[str, object]:
    with engine.begin() as connection:
        if _dialect_name(connection) == "sqlite":
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    release_id TEXT NOT NULL UNIQUE,
                    source_tree_hash TEXT NOT NULL,
                    migration_version TEXT NOT NULL,
                    built_at TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL,
                    manifest_sha256 TEXT NOT NULL,
                    registered_at DATETIME NOT NULL
                )
            """))
        else:
            connection.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    release_id CHAR(40) NOT NULL,
                    source_tree_hash CHAR(64) NOT NULL,
                    migration_version VARCHAR(128) NOT NULL,
                    built_at VARCHAR(40) NOT NULL,
                    artifact_hash CHAR(64) NOT NULL,
                    manifest_sha256 CHAR(64) NOT NULL,
                    registered_at DATETIME NOT NULL,
                    UNIQUE KEY uk_release_manifest_release_id (release_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """))
    return validate_release_manifest_runtime_schema(engine)


def register_runtime_release_manifest(
    engine,
    manifest: Mapping[str, object],
) -> dict[str, str]:
    normalized = validate_release_manifest(manifest)
    params = {**normalized, "registered_at": datetime.now()}
    with engine.begin() as connection:
        if _dialect_name(connection) == "sqlite":
            statement = text(f"""
                INSERT OR IGNORE INTO {REGISTRY_TABLE}
                    (release_id, source_tree_hash, migration_version, built_at,
                     artifact_hash, manifest_sha256, registered_at)
                VALUES
                    (:release_id, :source_tree_hash, :migration_version, :built_at,
                     :artifact_hash, :manifest_sha256, :registered_at)
            """)
        else:
            statement = text(f"""
                INSERT INTO {REGISTRY_TABLE}
                    (release_id, source_tree_hash, migration_version, built_at,
                     artifact_hash, manifest_sha256, registered_at)
                VALUES
                    (:release_id, :source_tree_hash, :migration_version, :built_at,
                     :artifact_hash, :manifest_sha256, :registered_at)
                ON DUPLICATE KEY UPDATE release_id=release_id
            """)
        connection.execute(statement, params)
        row = connection.execute(
            text(f"""
                SELECT release_id, source_tree_hash, migration_version, built_at,
                       artifact_hash, manifest_sha256
                FROM {REGISTRY_TABLE} WHERE release_id=:release_id
            """),
            {"release_id": normalized["release_id"]},
        ).mappings().first()
    registered = {key: str(value) for key, value in dict(row or {}).items()}
    expected_registered = {
        key: normalized[key]
        for key in (
            "release_id",
            "source_tree_hash",
            "migration_version",
            "built_at",
            "artifact_hash",
            "manifest_sha256",
        )
    }
    if registered != expected_registered:
        raise ReleaseManifestError("database release manifest identity differs")
    return normalized


def _write_from_cli(args: argparse.Namespace) -> int:
    from server.common.production_runtime_schema_bundle import _contract_metadata

    built_at = args.built_at or datetime.now(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    manifest = build_release_manifest(
        release_id=args.release_id,
        source_tree_hash=args.source_tree_hash,
        migration_version=_contract_metadata()["contract_hash"],
        built_at=built_at,
        artifact_components={
            "release_id": args.release_id,
            "source_tree_hash": args.source_tree_hash,
            "input_lock_sha256": args.input_lock_sha256,
            "wheel_manifest_sha256": args.wheel_manifest_sha256,
            "adata_sha": args.adata_sha,
            "adata_tree_sha256": args.adata_tree_sha256,
            "adapter_registry_seal_sha256": args.adapter_registry_seal_sha256,
        },
    )
    path = write_release_manifest(args.root, manifest)
    print(str(path))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    writer = subparsers.add_parser("write")
    writer.add_argument("--root", required=True)
    writer.add_argument("--release-id", required=True)
    writer.add_argument("--source-tree-hash", required=True)
    writer.add_argument("--built-at", default="")
    writer.add_argument("--input-lock-sha256", required=True)
    writer.add_argument("--wheel-manifest-sha256", required=True)
    writer.add_argument("--adata-sha", required=True)
    writer.add_argument("--adata-tree-sha256", required=True)
    writer.add_argument("--adapter-registry-seal-sha256", required=True)
    args = parser.parse_args(argv)
    if args.command == "write":
        return _write_from_cli(args)
    raise AssertionError("unreachable release manifest command")


__all__ = [
    "MANIFEST_FILE_NAME",
    "MANIFEST_SCHEMA",
    "REGISTRY_TABLE",
    "ReleaseManifestError",
    "build_release_manifest",
    "load_release_manifest",
    "privileged_migrate_release_manifest_schema",
    "register_runtime_release_manifest",
    "release_manifest_path",
    "validate_release_manifest",
    "validate_release_manifest_runtime_schema",
    "verify_runtime_release_manifest",
    "write_release_manifest",
]


if __name__ == "__main__":
    raise SystemExit(main())
