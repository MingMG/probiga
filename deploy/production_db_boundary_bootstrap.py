#!/usr/bin/env python3
"""One-time, fail-closed installation of production MySQL boundary secrets.

The deploy engine runs this file as root from the immutable release checkout.
It intentionally never prints credential values or exception text.
"""
from __future__ import annotations

import configparser
import hashlib
import json
import os
import re
import secrets
import ssl
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


STAGE = Path("/home/probiga-deploy/.probiga-db-boundary-stage")
CONFIG_DIR = Path("/etc/probiga")
ADMIN = CONFIG_DIR / "mysql-trigger-admin.ini"
MIGRATOR = CONFIG_DIR / "mysql-migrator.ini"
TRANSACTION = CONFIG_DIR / ".database-boundary-bootstrap.transaction"
TRANSACTION_BUILD = CONFIG_DIR / ".database-boundary-bootstrap.transaction.build"
TRANSACTION_COMMITTED = CONFIG_DIR / ".database-boundary-bootstrap.transaction.committed"
TRANSACTION_ROLLED_BACK = CONFIG_DIR / ".database-boundary-bootstrap.transaction.rolled-back"
CA = CONFIG_DIR / "mysql84-ca.pem"
CA_CONFIG_VALUE = "/etc/probiga/mysql84-ca.pem"
ENV = Path("/opt/ProBigA/.env")
APP_ROOT = ENV.parent
ENV_TEMP = APP_ROOT / ".env.database-boundary-bootstrap"
FILES = {
    "mysql-trigger-admin.ini": "probiga_trigger_admin",
    "mysql-migrator.ini": "probiga_migrator",
}
PASSWORD = re.compile(r"[A-Za-z0-9_-]{48,160}\Z")
ROOT_UID = 0
ROOT_GID = 0
TRANSACTION_SCHEMA = "probiga.database-boundary-bootstrap-transaction.v1"
TRANSACTION_FILES = frozenset({
    "metadata.json",
    "state",
    "env.original",
    "env.prepared",
    "mysql-trigger-admin.ini",
    "mysql-migrator.ini",
    "stage",
})


class BoundaryError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class Identities:
    deploy_uid: int
    deploy_gid: int
    service_gid: int


@dataclass(frozen=True)
class TransactionSnapshot:
    identities: Identities
    metadata: dict[str, object]
    options: dict[str, bytes]
    env_original: bytes
    env_prepared: bytes

    @property
    def stage_identity(self) -> tuple[int, int]:
        stage = self.metadata["stage"]
        assert isinstance(stage, dict)
        return int(stage["dev"]), int(stage["ino"])


def _state(path: Path) -> os.stat_result:
    try:
        return path.lstat()
    except OSError as exc:
        raise BoundaryError("metadata") from exc


def _absent(path: Path) -> bool:
    return not os.path.lexists(path)


def _assert_dir(path: Path, uid: int, gid: int, mode: int) -> os.stat_result:
    value = _state(path)
    if (
        not stat.S_ISDIR(value.st_mode)
        or stat.S_ISLNK(value.st_mode)
        or value.st_uid != uid
        or value.st_gid != gid
        or stat.S_IMODE(value.st_mode) != mode
    ):
        raise BoundaryError("directory-boundary")
    return value


def _directory_identity(value: os.stat_result) -> tuple[int, int]:
    return value.st_dev, value.st_ino


def _set_directory_metadata(
    path: Path,
    uid: int,
    gid: int,
    mode: int,
    *,
    expected_identity: tuple[int, int] | None = None,
) -> os.stat_result:
    """Change directory metadata through a no-follow descriptor.

    A deploy-owned stage can be replaced between a pathname check and rename.
    No pathname chmod/chown is therefore allowed on a claimed object.
    """

    before = _state(path)
    if not stat.S_ISDIR(before.st_mode) or stat.S_ISLNK(before.st_mode):
        raise BoundaryError("directory-boundary")
    if expected_identity is not None and _directory_identity(before) != expected_identity:
        raise BoundaryError("toctou")
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise BoundaryError("directory-boundary") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or _directory_identity(opened) != _directory_identity(before)
            or (
                expected_identity is not None
                and _directory_identity(opened) != expected_identity
            )
        ):
            raise BoundaryError("toctou")
        os.fchown(descriptor, uid, gid)
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
        final = os.fstat(descriptor)
        if (
            final.st_uid != uid
            or final.st_gid != gid
            or stat.S_IMODE(final.st_mode) != mode
            or _directory_identity(final) != _directory_identity(before)
        ):
            raise BoundaryError("directory-boundary")
        return final
    finally:
        os.close(descriptor)


def _read_regular(
    path: Path,
    uid: int,
    gid: int,
    mode: int,
    maximum: int,
    *,
    allowed_links: frozenset[int] = frozenset({1}),
) -> tuple[bytes, os.stat_result]:
    before = _state(path)
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or before.st_nlink not in allowed_links
        or before.st_uid != uid
        or before.st_gid != gid
        or stat.S_IMODE(before.st_mode) != mode
        or before.st_size < 1
        or before.st_size > maximum
    ):
        raise BoundaryError("file-boundary")
    flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise BoundaryError("toctou")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            (after.st_dev, after.st_ino, after.st_size)
            != (before.st_dev, before.st_ino, before.st_size)
            or len(payload) != before.st_size
        ):
            raise BoundaryError("toctou")
        return payload, before
    finally:
        os.close(descriptor)


def _parse_option(payload: bytes, expected_user: str) -> None:
    if b"\x00" in payload:
        raise BoundaryError("option-shape")
    try:
        text = payload.decode("utf-8-sig")
        parser = configparser.RawConfigParser(interpolation=None, strict=True)
        parser.read_string(text)
    except (UnicodeError, configparser.Error) as exc:
        raise BoundaryError("option-shape") from exc
    keys = {"protocol", "host", "port", "user", "password"}
    if parser.sections() != ["client"] or parser.defaults() or set(
        parser.options("client")
    ) != keys:
        raise BoundaryError("option-shape")
    values = {key: parser.get("client", key, raw=True).strip() for key in keys}
    if (
        values["protocol"].casefold() != "tcp"
        or values["host"] != "127.0.0.1"
        or values["port"] != "13306"
        or values["user"] != expected_user
        or PASSWORD.fullmatch(values["password"]) is None
    ):
        raise BoundaryError("option-policy")


def _validate_ca() -> bytes:
    payload, _ = _read_regular(CA, ROOT_UID, ROOT_GID, 0o644, 1024 * 1024)
    for parent in CA.parents:
        state = _state(parent)
        if (
            not stat.S_ISDIR(state.st_mode)
            or stat.S_ISLNK(state.st_mode)
            or state.st_uid != ROOT_UID
            or stat.S_IMODE(state.st_mode) & 0o022
        ):
            raise BoundaryError("ca-parent")
    try:
        ssl.create_default_context(cafile=str(CA))
    except (OSError, ssl.SSLError) as exc:
        raise BoundaryError("ca-content") from exc
    return payload


def _render_env(payload: bytes) -> bytes:
    try:
        text = payload.decode("utf-8")
    except UnicodeError as exc:
        raise BoundaryError("env-shape") from exc
    if "\x00" in text:
        raise BoundaryError("env-shape")
    lines = text.splitlines(keepends=True)
    found = {"MYSQL_SSL_CA": 0, "MYSQL_TLS_REQUIRED": 0}
    rendered: list[str] = []
    for line in lines:
        match = re.match(r"^(MYSQL_SSL_CA|MYSQL_TLS_REQUIRED)=([^\r\n]*)(\r?\n)?$", line)
        if not match:
            rendered.append(line)
            continue
        key = match.group(1)
        found[key] += 1
        value = match.group(2)
        if key == "MYSQL_TLS_REQUIRED" and value != "true":
            raise BoundaryError("env-tls")
        if key == "MYSQL_SSL_CA" and value not in {
            "/etc/probiga/mysql-combined-ca.pem",
            CA_CONFIG_VALUE,
        }:
            raise BoundaryError("env-ca")
        rendered.append(f"{key}={CA_CONFIG_VALUE if key == 'MYSQL_SSL_CA' else 'true'}{match.group(3) or ''}")
    if found != {"MYSQL_SSL_CA": 1, "MYSQL_TLS_REQUIRED": 1}:
        raise BoundaryError("env-shape")
    return "".join(rendered).encode("utf-8")


def _stage_payloads(uid: int, gid: int) -> dict[str, bytes]:
    _assert_dir(STAGE, uid, gid, 0o700)
    try:
        names = set(os.listdir(STAGE))
    except OSError as exc:
        raise BoundaryError("stage-list") from exc
    if names != set(FILES):
        raise BoundaryError("stage-shape")
    result: dict[str, bytes] = {}
    for name, user in FILES.items():
        payload, _ = _read_regular(STAGE / name, uid, gid, 0o600, 4096)
        _parse_option(payload, user)
        result[name] = payload
    return result


def _stage_snapshot(uid: int, gid: int) -> tuple[dict[str, bytes], os.stat_result]:
    state = _assert_dir(STAGE, uid, gid, 0o700)
    payloads = _stage_payloads(uid, gid)
    final = _state(STAGE)
    if _directory_identity(final) != _directory_identity(state):
        raise BoundaryError("toctou")
    return payloads, final


def _validate_stage_parent(identities: Identities) -> None:
    home = STAGE.parent
    home_state = _state(home)
    if (
        not stat.S_ISDIR(home_state.st_mode)
        or stat.S_ISLNK(home_state.st_mode)
        or home_state.st_uid != identities.deploy_uid
        or home_state.st_gid != identities.deploy_gid
        or stat.S_IMODE(home_state.st_mode) & 0o022
    ):
        raise BoundaryError("stage-parent")
    parent = home.parent
    parent_state = _state(parent)
    if (
        not stat.S_ISDIR(parent_state.st_mode)
        or stat.S_ISLNK(parent_state.st_mode)
        or parent_state.st_uid != ROOT_UID
        or parent_state.st_gid != ROOT_GID
        or stat.S_IMODE(parent_state.st_mode) & 0o022
    ):
        raise BoundaryError("stage-parent")


def _sync_dir(path: Path) -> None:
    descriptor = os.open(
        path, os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_DIRECTORY", 0)
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(
    path: Path,
    payload: bytes,
    uid: int,
    gid: int,
    mode: int,
    *,
    replace: bool = True,
) -> None:
    temporary = path.parent / f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
    completed = False
    target_created = False
    descriptor = -1
    try:
        descriptor = os.open(temporary, flags, mode)
        try:
            os.fchown(descriptor, uid, gid)
            os.fchmod(descriptor, mode)
            view = memoryview(payload)
            while view:
                view = view[os.write(descriptor, view) :]
            os.fsync(descriptor)
        finally:
            try:
                os.close(descriptor)
            finally:
                descriptor = -1
        if replace:
            os.replace(temporary, path)
        else:
            os.link(temporary, path, follow_symlinks=False)
            target_created = True
            temporary.unlink()
        _sync_dir(path.parent)
        completed = True
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if not completed:
            if target_created:
                try:
                    path.unlink()
                except OSError:
                    pass
            try:
                temporary.unlink()
            except OSError:
                pass


def _transaction_path(name: str) -> Path:
    return TRANSACTION / name


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _write_exclusive(path: Path, payload: bytes, uid: int, gid: int, mode: int) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, mode)
    try:
        os.fchown(descriptor, uid, gid)
        os.fchmod(descriptor, mode)
        view = memoryview(payload)
        while view:
            view = view[os.write(descriptor, view) :]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_state(path: Path, value: str) -> None:
    if value not in {"snapshot", "claimed", "prepared", "committing", "rolling-back"}:
        raise BoundaryError("transaction-state")
    payload = f"{value}\n".encode()
    pending = path / "state.next"
    if os.path.lexists(pending):
        observed, _ = _read_regular(pending, ROOT_UID, ROOT_GID, 0o600, 64)
        if observed != payload:
            raise BoundaryError("transaction-state")
    else:
        _write_exclusive(pending, payload, ROOT_UID, ROOT_GID, 0o600)
    os.replace(pending, path / "state")
    _sync_dir(path)


def _recover_pending_state(path: Path) -> None:
    pending = path / "state.next"
    if _absent(pending):
        return
    pending_state = _state(pending)
    if (
        not stat.S_ISREG(pending_state.st_mode)
        or stat.S_ISLNK(pending_state.st_mode)
        or pending_state.st_nlink != 1
        or pending_state.st_uid != ROOT_UID
        or pending_state.st_gid != ROOT_GID
        or stat.S_IMODE(pending_state.st_mode) != 0o600
        or pending_state.st_size > 64
    ):
        raise BoundaryError("transaction-state")
    if pending_state.st_size == 0:
        pending.unlink()
        _sync_dir(path)
        return
    payload, _ = _read_regular(pending, ROOT_UID, ROOT_GID, 0o600, 64)
    try:
        value = payload.decode("ascii").strip()
    except UnicodeError as exc:
        raise BoundaryError("transaction-state") from exc
    if value not in {"snapshot", "claimed", "prepared", "committing", "rolling-back"}:
        pending.unlink()
        _sync_dir(path)
        return
    os.replace(pending, path / "state")
    _sync_dir(path)


def _read_state(path: Path) -> str:
    payload, _ = _read_regular(path / "state", ROOT_UID, ROOT_GID, 0o600, 64)
    try:
        value = payload.decode("ascii").strip()
    except UnicodeError as exc:
        raise BoundaryError("transaction-state") from exc
    if value not in {"snapshot", "claimed", "prepared", "committing", "rolling-back"}:
        raise BoundaryError("transaction-state")
    return value


def _purge_tree(path: Path) -> None:
    """Delete a root-controlled transaction tree without following links."""

    if _absent(path):
        return
    state = _state(path)
    if (
        not stat.S_ISDIR(state.st_mode)
        or stat.S_ISLNK(state.st_mode)
        or state.st_uid != ROOT_UID
        or state.st_gid != ROOT_GID
        or stat.S_IMODE(state.st_mode) & 0o077
    ):
        raise BoundaryError("transaction-boundary")
    try:
        entries = list(os.scandir(path))
    except OSError as exc:
        raise BoundaryError("transaction-boundary") from exc
    for entry in entries:
        candidate = path / entry.name
        try:
            entry_state = entry.stat(follow_symlinks=False)
        except OSError as exc:
            raise BoundaryError("transaction-boundary") from exc
        if stat.S_ISDIR(entry_state.st_mode) and not stat.S_ISLNK(entry_state.st_mode):
            _purge_tree(candidate)
        else:
            try:
                candidate.unlink()
            except OSError as exc:
                raise BoundaryError("transaction-boundary") from exc
    try:
        path.rmdir()
    except OSError as exc:
        raise BoundaryError("transaction-boundary") from exc
    _sync_dir(path.parent)


def _cleanup_interrupted_containers() -> None:
    if not _absent(TRANSACTION_BUILD):
        _purge_tree(TRANSACTION_BUILD)
    if not _absent(TRANSACTION_ROLLED_BACK):
        _purge_tree(TRANSACTION_ROLLED_BACK)


def _metadata_payload(
    identities: Identities,
    stage_state: os.stat_result,
    env_state: os.stat_result,
    env_original: bytes,
    env_prepared: bytes,
    app_state: os.stat_result,
    options: dict[str, bytes],
) -> bytes:
    value = {
        "schema_version": TRANSACTION_SCHEMA,
        "identities": {
            "deploy_uid": identities.deploy_uid,
            "deploy_gid": identities.deploy_gid,
            "service_gid": identities.service_gid,
        },
        "stage": {
            "dev": stage_state.st_dev,
            "ino": stage_state.st_ino,
            "uid": stage_state.st_uid,
            "gid": stage_state.st_gid,
            "mode": stat.S_IMODE(stage_state.st_mode),
        },
        "env": {
            "uid": env_state.st_uid,
            "gid": env_state.st_gid,
            "mode": stat.S_IMODE(env_state.st_mode),
            "atime_ns": env_state.st_atime_ns,
            "mtime_ns": env_state.st_mtime_ns,
            "original_sha256": _sha(env_original),
            "prepared_sha256": _sha(env_prepared),
        },
        "app": {
            "dev": app_state.st_dev,
            "ino": app_state.st_ino,
            "uid": app_state.st_uid,
            "gid": app_state.st_gid,
            "mode": stat.S_IMODE(app_state.st_mode),
        },
        "options": {name: _sha(options[name]) for name in FILES},
    }
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _create_transaction(
    identities: Identities,
    stage_state: os.stat_result,
    env_state: os.stat_result,
    env_original: bytes,
    env_prepared: bytes,
    app_state: os.stat_result,
    options: dict[str, bytes],
) -> None:
    if not _absent(TRANSACTION) or not _absent(TRANSACTION_COMMITTED):
        raise BoundaryError("transaction-present")
    if not _absent(TRANSACTION_BUILD):
        _purge_tree(TRANSACTION_BUILD)
    os.mkdir(TRANSACTION_BUILD, 0o700)
    _set_directory_metadata(TRANSACTION_BUILD, ROOT_UID, ROOT_GID, 0o700)
    try:
        _atomic_write(
            TRANSACTION_BUILD / "env.original",
            env_original,
            ROOT_UID,
            ROOT_GID,
            0o600,
            replace=False,
        )
        _atomic_write(
            TRANSACTION_BUILD / "env.prepared",
            env_prepared,
            ROOT_UID,
            ROOT_GID,
            0o600,
            replace=False,
        )
        for name in FILES:
            _atomic_write(
                TRANSACTION_BUILD / name,
                options[name],
                ROOT_UID,
                ROOT_GID,
                0o600,
                replace=False,
            )
        _atomic_write(
            TRANSACTION_BUILD / "metadata.json",
            _metadata_payload(
                identities,
                stage_state,
                env_state,
                env_original,
                env_prepared,
                app_state,
                options,
            ),
            ROOT_UID,
            ROOT_GID,
            0o600,
            replace=False,
        )
        _write_state(TRANSACTION_BUILD, "snapshot")
        _sync_dir(TRANSACTION_BUILD)
        os.rename(TRANSACTION_BUILD, TRANSACTION)
        _sync_dir(CONFIG_DIR)
    except BaseException:
        if not _absent(TRANSACTION_BUILD):
            _purge_tree(TRANSACTION_BUILD)
        raise


def _required_int(mapping: object, key: str) -> int:
    if not isinstance(mapping, dict) or type(mapping.get(key)) is not int:
        raise BoundaryError("transaction-metadata")
    return int(mapping[key])


def _load_transaction(identities: Identities) -> tuple[TransactionSnapshot, str]:
    _assert_dir(TRANSACTION, ROOT_UID, ROOT_GID, 0o700)
    _recover_pending_state(TRANSACTION)
    try:
        names = set(os.listdir(TRANSACTION))
    except OSError as exc:
        raise BoundaryError("transaction-boundary") from exc
    required = TRANSACTION_FILES - {"stage"}
    if not required.issubset(names) or not names.issubset(TRANSACTION_FILES):
        raise BoundaryError("transaction-shape")
    raw_meta, _ = _read_regular(
        _transaction_path("metadata.json"), ROOT_UID, ROOT_GID, 0o600, 16384
    )
    try:
        metadata = json.loads(raw_meta.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise BoundaryError("transaction-metadata") from exc
    if not isinstance(metadata, dict) or metadata.get("schema_version") != TRANSACTION_SCHEMA:
        raise BoundaryError("transaction-metadata")
    identity_meta = metadata.get("identities")
    if (
        _required_int(identity_meta, "deploy_uid") != identities.deploy_uid
        or _required_int(identity_meta, "deploy_gid") != identities.deploy_gid
        or _required_int(identity_meta, "service_gid") != identities.service_gid
    ):
        raise BoundaryError("transaction-identity")
    stage_meta = metadata.get("stage")
    env_meta = metadata.get("env")
    app_meta = metadata.get("app")
    for key in ("dev", "ino", "uid", "gid", "mode"):
        _required_int(stage_meta, key)
        _required_int(app_meta, key)
    for key in ("uid", "gid", "mode", "atime_ns", "mtime_ns"):
        _required_int(env_meta, key)
    options: dict[str, bytes] = {}
    option_hashes = metadata.get("options")
    if not isinstance(option_hashes, dict) or set(option_hashes) != set(FILES):
        raise BoundaryError("transaction-metadata")
    for name, user in FILES.items():
        payload, _ = _read_regular(
            _transaction_path(name),
            ROOT_UID,
            ROOT_GID,
            0o600,
            4096,
            allowed_links=frozenset({1, 2}),
        )
        _parse_option(payload, user)
        if option_hashes.get(name) != _sha(payload):
            raise BoundaryError("transaction-hash")
        options[name] = payload
    env_original, _ = _read_regular(
        _transaction_path("env.original"), ROOT_UID, ROOT_GID, 0o600, 1024 * 1024
    )
    env_prepared, _ = _read_regular(
        _transaction_path("env.prepared"), ROOT_UID, ROOT_GID, 0o600, 1024 * 1024
    )
    if (
        not isinstance(env_meta, dict)
        or env_meta.get("original_sha256") != _sha(env_original)
        or env_meta.get("prepared_sha256") != _sha(env_prepared)
        or _render_env(env_original) != env_prepared
    ):
        raise BoundaryError("transaction-hash")
    return (
        TransactionSnapshot(
            identities=identities,
            metadata=metadata,
            options=options,
            env_original=env_original,
            env_prepared=env_prepared,
        ),
        _read_state(TRANSACTION),
    )


def _meta_section(snapshot: TransactionSnapshot, name: str) -> dict[str, object]:
    value = snapshot.metadata.get(name)
    if not isinstance(value, dict):
        raise BoundaryError("transaction-metadata")
    return value


def _claim_path() -> Path:
    return _transaction_path("stage")


def _validate_claim(snapshot: TransactionSnapshot, *, allow_deploy_owner: bool) -> os.stat_result:
    state = _state(_claim_path())
    allowed_owner = {(ROOT_UID, ROOT_GID)}
    if allow_deploy_owner:
        allowed_owner.add(
            (snapshot.identities.deploy_uid, snapshot.identities.deploy_gid)
        )
    if (
        not stat.S_ISDIR(state.st_mode)
        or stat.S_ISLNK(state.st_mode)
        or _directory_identity(state) != snapshot.stage_identity
        or (state.st_uid, state.st_gid) not in allowed_owner
        or stat.S_IMODE(state.st_mode) != 0o700
    ):
        raise BoundaryError("claim-boundary")
    return state


def _validate_claim_payloads(snapshot: TransactionSnapshot) -> None:
    try:
        names = set(os.listdir(_claim_path()))
    except OSError as exc:
        raise BoundaryError("claim-boundary") from exc
    if names != set(FILES):
        raise BoundaryError("toctou")
    for name, user in FILES.items():
        payload, _ = _read_regular(
            _claim_path() / name,
            snapshot.identities.deploy_uid,
            snapshot.identities.deploy_gid,
            0o600,
            4096,
        )
        _parse_option(payload, user)
        if payload != snapshot.options[name]:
            raise BoundaryError("toctou")


def _claim_stage(
    snapshot: TransactionSnapshot,
    *,
    fault_hook: Callable[[str], None] | None,
) -> None:
    if os.path.lexists(_claim_path()):
        state = _validate_claim(snapshot, allow_deploy_owner=True)
        _set_directory_metadata(
            _claim_path(),
            ROOT_UID,
            ROOT_GID,
            0o700,
            expected_identity=_directory_identity(state),
        )
        _validate_claim_payloads(snapshot)
        _write_state(TRANSACTION, "claimed")
        return
    if _absent(STAGE):
        raise BoundaryError("stage-missing")
    stage_meta = _meta_section(snapshot, "stage")
    before = _state(STAGE)
    if (
        not stat.S_ISDIR(before.st_mode)
        or stat.S_ISLNK(before.st_mode)
        or _directory_identity(before) != snapshot.stage_identity
        or before.st_uid != _required_int(stage_meta, "uid")
        or before.st_gid != _required_int(stage_meta, "gid")
        or stat.S_IMODE(before.st_mode) != _required_int(stage_meta, "mode")
    ):
        raise BoundaryError("toctou")
    if fault_hook:
        fault_hook("before-claim-rename")
    os.rename(STAGE, _claim_path())
    _sync_dir(STAGE.parent)
    _sync_dir(TRANSACTION)
    # This post-rename lstat happens before every metadata operation.  A
    # pathname swapped to a symlink is never chmod/chown'ed as root.
    claimed = _validate_claim(snapshot, allow_deploy_owner=True)
    _set_directory_metadata(
        _claim_path(),
        ROOT_UID,
        ROOT_GID,
        0o700,
        expected_identity=_directory_identity(claimed),
    )
    _validate_claim_payloads(snapshot)
    _write_state(TRANSACTION, "claimed")


def _app_metadata(snapshot: TransactionSnapshot) -> tuple[int, int, int, tuple[int, int]]:
    app = _meta_section(snapshot, "app")
    return (
        _required_int(app, "uid"),
        _required_int(app, "gid"),
        _required_int(app, "mode"),
        (_required_int(app, "dev"), _required_int(app, "ino")),
    )


def _ensure_app_prepared(snapshot: TransactionSnapshot) -> None:
    original_uid, original_gid, original_mode, identity = _app_metadata(snapshot)
    current = _state(APP_ROOT)
    if (
        not stat.S_ISDIR(current.st_mode)
        or stat.S_ISLNK(current.st_mode)
        or _directory_identity(current) != identity
    ):
        raise BoundaryError("app-boundary")
    observed = (current.st_uid, current.st_gid, stat.S_IMODE(current.st_mode))
    if observed == (ROOT_UID, ROOT_GID, 0o755):
        return
    if observed != (original_uid, original_gid, original_mode):
        raise BoundaryError("app-boundary")
    _set_directory_metadata(
        APP_ROOT,
        ROOT_UID,
        ROOT_GID,
        0o755,
        expected_identity=identity,
    )
    _sync_dir(APP_ROOT.parent)


def _snapshot_option_path(name: str) -> Path:
    return _transaction_path(name)


def _ensure_option_targets(snapshot: TransactionSnapshot) -> None:
    for name, user in FILES.items():
        source = _snapshot_option_path(name)
        source_state = _state(source)
        if (
            not stat.S_ISREG(source_state.st_mode)
            or stat.S_ISLNK(source_state.st_mode)
            or source_state.st_uid != ROOT_UID
            or source_state.st_gid != ROOT_GID
            or stat.S_IMODE(source_state.st_mode) != 0o600
            or source_state.st_nlink not in {1, 2}
        ):
            raise BoundaryError("transaction-boundary")
        target = CONFIG_DIR / name
        if _absent(target):
            if source_state.st_nlink != 1:
                raise BoundaryError("target-state")
            os.link(source, target, follow_symlinks=False)
            _sync_dir(CONFIG_DIR)
        payload, target_state = _read_regular(
            target,
            ROOT_UID,
            ROOT_GID,
            0o600,
            4096,
            allowed_links=frozenset({2}),
        )
        final_source = _state(source)
        if (
            (target_state.st_dev, target_state.st_ino)
            != (final_source.st_dev, final_source.st_ino)
            or payload != snapshot.options[name]
        ):
            raise BoundaryError("target-state")
        _parse_option(payload, user)


def _env_metadata(snapshot: TransactionSnapshot) -> tuple[int, int, int, int, int]:
    env = _meta_section(snapshot, "env")
    return (
        _required_int(env, "uid"),
        _required_int(env, "gid"),
        _required_int(env, "mode"),
        _required_int(env, "atime_ns"),
        _required_int(env, "mtime_ns"),
    )


def _env_disposition(snapshot: TransactionSnapshot) -> str:
    state = _state(ENV)
    observed = (state.st_uid, state.st_gid, stat.S_IMODE(state.st_mode))
    original_uid, original_gid, original_mode, _, _ = _env_metadata(snapshot)
    prepared_match = False
    original_match = False
    if observed == (ROOT_UID, snapshot.identities.service_gid, 0o640):
        payload, _ = _read_regular(
            ENV,
            ROOT_UID,
            snapshot.identities.service_gid,
            0o640,
            1024 * 1024,
        )
        if payload == snapshot.env_prepared:
            prepared_match = True
    if observed == (original_uid, original_gid, original_mode):
        payload, _ = _read_regular(
            ENV, original_uid, original_gid, original_mode, 1024 * 1024
        )
        if payload == snapshot.env_original:
            original_match = True
    if prepared_match and original_match:
        return "both"
    if prepared_match:
        return "prepared"
    if original_match:
        return "original"
    raise BoundaryError("env-state")


def _replace_env(
    payload: bytes,
    uid: int,
    gid: int,
    mode: int,
    *,
    timestamps: tuple[int, int] | None = None,
) -> None:
    if os.path.lexists(ENV_TEMP):
        temporary_state = _state(ENV_TEMP)
        if (
            not stat.S_ISREG(temporary_state.st_mode)
            or stat.S_ISLNK(temporary_state.st_mode)
            or temporary_state.st_nlink != 1
            or (temporary_state.st_uid, temporary_state.st_gid)
            not in {(uid, gid), (ROOT_UID, ROOT_GID)}
            or stat.S_IMODE(temporary_state.st_mode) != mode
            or temporary_state.st_size > 1024 * 1024
        ):
            raise BoundaryError("env-temp")
        ENV_TEMP.unlink()
        _sync_dir(APP_ROOT)
    _write_exclusive(ENV_TEMP, payload, uid, gid, mode)
    os.replace(ENV_TEMP, ENV)
    _sync_dir(APP_ROOT)
    if timestamps is not None:
        os.utime(ENV, ns=timestamps, follow_symlinks=False)
        flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(ENV, flags)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _sync_dir(APP_ROOT)


def _ensure_env_prepared(snapshot: TransactionSnapshot) -> None:
    disposition = _env_disposition(snapshot)
    if disposition == "original":
        _replace_env(
            snapshot.env_prepared,
            ROOT_UID,
            snapshot.identities.service_gid,
            0o640,
        )
    if _env_disposition(snapshot) not in {"prepared", "both"}:
        raise BoundaryError("env-state")


def _remove_env_temp(snapshot: TransactionSnapshot) -> None:
    if _absent(ENV_TEMP):
        return
    state = _state(ENV_TEMP)
    observed = (state.st_uid, state.st_gid, stat.S_IMODE(state.st_mode))
    original_uid, original_gid, original_mode, _, _ = _env_metadata(snapshot)
    allowed_metadata = {
        (ROOT_UID, snapshot.identities.service_gid, 0o640),
        (original_uid, original_gid, original_mode),
        (ROOT_UID, ROOT_GID, 0o640),
        (ROOT_UID, ROOT_GID, original_mode),
    }
    if (
        not stat.S_ISREG(state.st_mode)
        or stat.S_ISLNK(state.st_mode)
        or state.st_nlink != 1
        or observed not in allowed_metadata
        or state.st_size > 1024 * 1024
    ):
        raise BoundaryError("env-temp")
    ENV_TEMP.unlink()
    _sync_dir(APP_ROOT)


def _validate_prepared(snapshot: TransactionSnapshot) -> None:
    _validate_claim(snapshot, allow_deploy_owner=False)
    _ensure_app_prepared(snapshot)
    _ensure_option_targets(snapshot)
    if _env_disposition(snapshot) not in {"prepared", "both"}:
        raise BoundaryError("env-state")


def _resume_prepare(
    snapshot: TransactionSnapshot,
    state: str,
    *,
    fault_hook: Callable[[str], None] | None,
) -> None:
    if state == "committing":
        raise BoundaryError("transaction-committing")
    if state == "rolling-back":
        raise BoundaryError("transaction-rolling-back")
    _claim_stage(snapshot, fault_hook=fault_hook)
    if fault_hook:
        fault_hook("after-claim")
    _ensure_app_prepared(snapshot)
    _ensure_option_targets(snapshot)
    if fault_hook:
        fault_hook("after-options")
    _ensure_env_prepared(snapshot)
    if fault_hook:
        fault_hook("after-env")
    _validate_prepared(snapshot)
    _write_state(TRANSACTION, "prepared")


def _validate_installed(
    identities: Identities,
    *,
    allowed_links: frozenset[int] = frozenset({1}),
) -> tuple[dict[str, bytes], bytes]:
    if not _absent(STAGE):
        raise BoundaryError("installed-stage-present")
    _assert_dir(APP_ROOT, ROOT_UID, ROOT_GID, 0o755)
    options: dict[str, bytes] = {}
    for name, user in FILES.items():
        payload, _ = _read_regular(
            CONFIG_DIR / name,
            ROOT_UID,
            ROOT_GID,
            0o600,
            4096,
            allowed_links=allowed_links,
        )
        _parse_option(payload, user)
        options[name] = payload
    env, _ = _read_regular(
        ENV, ROOT_UID, identities.service_gid, 0o640, 1024 * 1024
    )
    if _render_env(env) != env:
        raise BoundaryError("installed-env-drift")
    return options, env


def _recover_committed(
    identities: Identities,
) -> tuple[dict[str, bytes], bytes]:
    # A committing transaction has crossed its durable decision point.  The
    # live boundary must be preserved; only root-owned duplicate snapshots are
    # removed.  Target link counts become one after cleanup.
    if not _absent(TRANSACTION_COMMITTED):
        _validate_installed(
            identities, allowed_links=frozenset({1, 2})
        )
        _purge_tree(TRANSACTION_COMMITTED)
    return _validate_installed(identities)


def _finish_commit(
    snapshot: TransactionSnapshot,
    identities: Identities,
) -> tuple[dict[str, bytes], bytes]:
    _validate_prepared(snapshot)
    _write_state(TRANSACTION, "committing")
    _sync_dir(TRANSACTION)
    if not _absent(TRANSACTION_COMMITTED):
        raise BoundaryError("transaction-committed-present")
    os.rename(TRANSACTION, TRANSACTION_COMMITTED)
    _sync_dir(CONFIG_DIR)
    return _recover_committed(identities)


def _remove_transaction_targets(snapshot: TransactionSnapshot) -> None:
    for name in FILES:
        target = CONFIG_DIR / name
        if _absent(target):
            continue
        source = _snapshot_option_path(name)
        source_state = _state(source)
        payload, target_state = _read_regular(
            target,
            ROOT_UID,
            ROOT_GID,
            0o600,
            4096,
            allowed_links=frozenset({2}),
        )
        if (
            payload != snapshot.options[name]
            or (source_state.st_dev, source_state.st_ino)
            != (target_state.st_dev, target_state.st_ino)
        ):
            raise BoundaryError("rollback-target")
        target.unlink()
        _sync_dir(CONFIG_DIR)


def _rebuild_claim(snapshot: TransactionSnapshot) -> None:
    claim = _claim_path()
    if os.path.lexists(claim):
        claim_state = _validate_claim(snapshot, allow_deploy_owner=True)
        if (claim_state.st_uid, claim_state.st_gid) != (ROOT_UID, ROOT_GID):
            claim_state = _set_directory_metadata(
                claim,
                ROOT_UID,
                ROOT_GID,
                0o700,
                expected_identity=_directory_identity(claim_state),
            )
        try:
            names = os.listdir(claim)
        except OSError as exc:
            raise BoundaryError("rollback-claim") from exc
        for name in names:
            candidate = claim / name
            value = _state(candidate)
            if stat.S_ISDIR(value.st_mode) and not stat.S_ISLNK(value.st_mode):
                raise BoundaryError("rollback-claim")
            candidate.unlink()
        for name in FILES:
            _atomic_write(
                claim / name,
                snapshot.options[name],
                snapshot.identities.deploy_uid,
                snapshot.identities.deploy_gid,
                0o600,
                replace=False,
            )
        _set_directory_metadata(
            claim,
            snapshot.identities.deploy_uid,
            snapshot.identities.deploy_gid,
            0o700,
            expected_identity=_directory_identity(claim_state),
        )
        if not _absent(STAGE):
            raise BoundaryError("rollback-stage-present")
        os.rename(claim, STAGE)
        _sync_dir(STAGE.parent)
        _sync_dir(TRANSACTION)
    restored, restored_state = _stage_snapshot(
        snapshot.identities.deploy_uid, snapshot.identities.deploy_gid
    )
    if (
        _directory_identity(restored_state) != snapshot.stage_identity
        or {name: _sha(restored[name]) for name in FILES}
        != {name: _sha(snapshot.options[name]) for name in FILES}
    ):
        raise BoundaryError("rollback-stage")


def _finish_rollback(snapshot: TransactionSnapshot) -> tuple[dict[str, bytes], bytes]:
    _write_state(TRANSACTION, "rolling-back")
    _remove_transaction_targets(snapshot)
    disposition = _env_disposition(snapshot)
    if disposition in {"prepared", "both"}:
        uid, gid, mode, atime_ns, mtime_ns = _env_metadata(snapshot)
        _replace_env(
            snapshot.env_original,
            uid,
            gid,
            mode,
            timestamps=(atime_ns, mtime_ns),
        )
    if _env_disposition(snapshot) not in {"original", "both"}:
        raise BoundaryError("rollback-env")
    _remove_env_temp(snapshot)
    _rebuild_claim(snapshot)
    app_uid, app_gid, app_mode, app_identity = _app_metadata(snapshot)
    current = _state(APP_ROOT)
    if _directory_identity(current) != app_identity:
        raise BoundaryError("rollback-app")
    observed = (current.st_uid, current.st_gid, stat.S_IMODE(current.st_mode))
    if observed != (app_uid, app_gid, app_mode):
        if observed != (ROOT_UID, ROOT_GID, 0o755):
            raise BoundaryError("rollback-app")
        _set_directory_metadata(
            APP_ROOT,
            app_uid,
            app_gid,
            app_mode,
            expected_identity=app_identity,
        )
        _sync_dir(APP_ROOT.parent)
    if not _absent(TRANSACTION_ROLLED_BACK):
        raise BoundaryError("transaction-rollback-present")
    os.rename(TRANSACTION, TRANSACTION_ROLLED_BACK)
    _sync_dir(CONFIG_DIR)
    _purge_tree(TRANSACTION_ROLLED_BACK)
    return snapshot.options, snapshot.env_original


def _receipt(
    mode: str,
    options: dict[str, bytes] | None,
    env: bytes,
    ca: bytes,
) -> dict[str, object]:
    hashes = {
        "env": hashlib.sha256(env).hexdigest(),
        "ca": hashlib.sha256(ca).hexdigest(),
    }
    if options is not None:
        hashes.update({
            "admin": hashlib.sha256(
                options["mysql-trigger-admin.ini"]
            ).hexdigest(),
            "migrator": hashlib.sha256(
                options["mysql-migrator.ini"]
            ).hexdigest(),
        })
    return {
        "schema_version": "probiga.database-boundary-bootstrap.v1",
        "status": "ok",
        "mode": mode,
        "hashes": hashes,
        "secrets_emitted": False,
    }


def _system_identities() -> Identities:
    import grp
    import pwd

    deploy = pwd.getpwnam("probiga-deploy")
    service = grp.getgrnam("probiga")
    return Identities(deploy.pw_uid, deploy.pw_gid, service.gr_gid)


def run(
    action: str = "prepare",
    *,
    fault_hook: Callable[[str], None] | None = None,
) -> dict[str, object]:
    if action not in {"prepare", "commit", "rollback", "verify"}:
        raise BoundaryError("action")
    if os.geteuid() != ROOT_UID:
        raise BoundaryError("root-required")
    identities = _system_identities()
    _assert_dir(CONFIG_DIR, ROOT_UID, ROOT_GID, 0o755)
    ca = _validate_ca()
    _cleanup_interrupted_containers()
    if not _absent(TRANSACTION) and not _absent(TRANSACTION_COMMITTED):
        raise BoundaryError("transaction-conflict")
    if not _absent(TRANSACTION_COMMITTED):
        options, env = _recover_committed(identities)
        return _receipt("recovered-commit", options, env, ca)

    present = [not _absent(ADMIN), not _absent(MIGRATOR)]
    if any(present) and not all(present) and _absent(TRANSACTION):
        raise BoundaryError("partial-install")
    if action == "verify":
        if not _absent(TRANSACTION):
            snapshot, state = _load_transaction(identities)
            if state == "committing":
                options, env = _finish_commit(snapshot, identities)
                return _receipt("recovered-commit", options, env, ca)
            raise BoundaryError("transaction-pending")
        if not all(present):
            raise BoundaryError("not-installed")
        options, env = _validate_installed(identities)
        return _receipt("verified", options, env, ca)

    if action == "rollback":
        if not _absent(TRANSACTION):
            snapshot, state = _load_transaction(identities)
            if state == "committing":
                options, env = _finish_commit(snapshot, identities)
                return _receipt("recovered-commit", options, env, ca)
            options, env = _finish_rollback(snapshot)
            return _receipt("rolled-back", options, env, ca)
        if all(present):
            options, env = _validate_installed(identities)
            return _receipt("already-committed", options, env, ca)
        env_state = _state(ENV)
        env, _ = _read_regular(
            ENV,
            env_state.st_uid,
            env_state.st_gid,
            stat.S_IMODE(env_state.st_mode),
            1024 * 1024,
        )
        return _receipt("not-started", None, env, ca)

    if action == "commit":
        if _absent(TRANSACTION):
            if not all(present):
                raise BoundaryError("transaction-missing")
            options, env = _validate_installed(identities)
            return _receipt("already-committed", options, env, ca)
        snapshot, state = _load_transaction(identities)
        if state == "rolling-back":
            raise BoundaryError("transaction-rolling-back")
        options, env = _finish_commit(snapshot, identities)
        return _receipt("committed", options, env, ca)

    # prepare
    if not _absent(TRANSACTION):
        snapshot, state = _load_transaction(identities)
        if state == "committing":
            options, env = _finish_commit(snapshot, identities)
            return _receipt("recovered-commit", options, env, ca)
        if state == "rolling-back":
            _finish_rollback(snapshot)
        else:
            try:
                _resume_prepare(snapshot, state, fault_hook=fault_hook)
            except BaseException as original:
                try:
                    latest, latest_state = _load_transaction(identities)
                    if latest_state != "committing":
                        _finish_rollback(latest)
                except BaseException as rollback_error:
                    raise BoundaryError("rollback-incomplete") from rollback_error
                raise original
            return _receipt(
                "prepared",
                snapshot.options,
                snapshot.env_prepared,
                ca,
            )

    present = [not _absent(ADMIN), not _absent(MIGRATOR)]
    if all(present):
        options, env = _validate_installed(identities)
        return _receipt("already-installed", options, env, ca)
    if any(present):
        raise BoundaryError("partial-install")
    env_state = _state(ENV)
    if env_state.st_uid not in {ROOT_UID, identities.deploy_uid}:
        raise BoundaryError("env-owner")
    env, env_state = _read_regular(
        ENV,
        env_state.st_uid,
        identities.service_gid,
        0o640,
        1024 * 1024,
    )
    rendered_env = _render_env(env)
    if not _absent(ENV_TEMP):
        raise BoundaryError("env-temp-present")
    app_state = _state(APP_ROOT)
    if (app_state.st_uid, app_state.st_gid) not in {
        (identities.deploy_uid, identities.deploy_gid),
        (ROOT_UID, ROOT_GID),
    }:
        raise BoundaryError("app-owner")
    _assert_dir(APP_ROOT, app_state.st_uid, app_state.st_gid, 0o755)
    _validate_stage_parent(identities)
    options, stage_state = _stage_snapshot(
        identities.deploy_uid, identities.deploy_gid
    )
    if stage_state.st_dev != _state(CONFIG_DIR).st_dev:
        raise BoundaryError("cross-filesystem")
    if fault_hook:
        fault_hook("after-preflight")
    _create_transaction(
        identities,
        stage_state,
        env_state,
        env,
        rendered_env,
        app_state,
        options,
    )
    snapshot, state = _load_transaction(identities)
    try:
        _resume_prepare(snapshot, state, fault_hook=fault_hook)
    except BaseException as original:
        try:
            latest, latest_state = _load_transaction(identities)
            if latest_state != "committing":
                _finish_rollback(latest)
        except BaseException as rollback_error:
            raise BoundaryError("rollback-incomplete") from rollback_error
        raise original
    return _receipt("prepared", snapshot.options, snapshot.env_prepared, ca)


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if len(arguments) != 1 or arguments[0] not in {
        "prepare",
        "commit",
        "rollback",
        "verify",
    }:
        print("database_boundary_bootstrap=FAILED code=action", file=sys.stderr)
        return 2

    def interrupted(signum: int, _frame: object) -> None:
        raise BoundaryError(f"signal-{signum}")

    import signal

    for name in ("SIGTERM", "SIGINT", "SIGHUP"):
        signum = getattr(signal, name, None)
        if signum is not None:
            signal.signal(signum, interrupted)
    try:
        result = run(arguments[0])
    except BoundaryError as exc:
        print(f"database_boundary_bootstrap=FAILED code={exc.code}", file=sys.stderr)
        return 2
    except BaseException:
        print("database_boundary_bootstrap=FAILED code=internal", file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
