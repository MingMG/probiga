"""Immutable formal analysis artifacts and generation membership."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import contextmanager, nullcontext
import hashlib
import json
import re
import sqlite3
import time
from typing import Any
import zlib

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError, IntegrityError

from server.db.formal_analysis_schema import ARTIFACT_TABLE, FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES, MEMBER_TABLE


FORMAL_ANALYSIS_BATCH_MAX_MEMBERS = 256
FORMAL_ANALYSIS_BATCH_MAX_BYTES = 32 * 1024 * 1024
_BATCH_MAX_BYTES = FORMAL_ANALYSIS_BATCH_MAX_BYTES
_LOAD_MAX_ARTIFACTS = 8
_LIST_MAX_MEMBERS = 20_000
_SHA256 = re.compile(r"[a-f0-9]{64}")
_STAGE = re.compile(r"[a-z][a-z0-9_:-]{0,47}")
_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}")
_MYSQL_LOCK_RETRY = frozenset({1205, 1213})
_SQLITE_LOCK_RETRY = frozenset({sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})
_COMPRESSED_ARTIFACT_MAX_BYTES = FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES + 8192
_LOAD_MAX_COMPRESSED_BYTES = _BATCH_MAX_BYTES + _LOAD_MAX_ARTIFACTS * 8192
_ARTIFACT_PAYLOAD_READ = (
    f"CASE WHEN payload_bytes BETWEEN 1 AND {FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES} "
    f"AND LENGTH(payload_zlib) <= {_COMPRESSED_ARTIFACT_MAX_BYTES} THEN payload_zlib END AS payload_zlib"
)
_JSON_ENCODER = json.JSONEncoder(ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


class FormalAnalysisStoreError(RuntimeError):
    """The formal analysis artifact boundary cannot satisfy the operation."""


class FormalAnalysisConflict(FormalAnalysisStoreError):
    """An immutable generation key already points to a different artifact."""


class FormalAnalysisArtifactIntegrityError(FormalAnalysisStoreError):
    """A persisted checkpoint or artifact fails authentication."""


class FormalAnalysisArtifactMissing(FormalAnalysisStoreError):
    """An exact artifact requested by digest is unavailable."""


def _identity(value: str, pattern: re.Pattern[str], field: str) -> str:
    if type(value) is not str or not pattern.fullmatch(value):
        raise ValueError(f"formal analysis {field} is invalid")
    return value


def _json_types(value: Any) -> None:
    kind = type(value)
    if kind is dict:
        if any(type(key) is not str for key in value):
            raise ValueError("formal analysis JSON object keys must be strings")
        for child in value.values():
            _json_types(child)
    elif kind is list:
        for child in value:
            _json_types(child)
    elif kind not in (str, int, bool, float, type(None)):
        raise ValueError("formal analysis checkpoint contains a non-JSON value")


def _canonical_payload(payload: Any) -> bytes:
    if payload is None:
        raise ValueError("formal analysis checkpoint payload cannot be null")
    raw = bytearray()
    try:
        _json_types(payload)
        for chunk in _JSON_ENCODER.iterencode(payload):
            encoded = chunk.encode("utf-8")
            if len(raw) + len(encoded) > FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES:
                raise ValueError("formal analysis artifact exceeds the byte limit")
            raw.extend(encoded)
    except (TypeError, OverflowError, RecursionError, UnicodeError) as exc:
        raise ValueError("formal analysis checkpoint contains invalid JSON") from exc
    return bytes(raw)


def _decode_artifact(row: Mapping[str, Any], *, expected: str) -> Any:
    size = row.get("payload_bytes")
    payload = row.get("payload_zlib")
    if (
        row.get("artifact_sha256") != expected or type(size) is not int
        or not 0 < size <= FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES
        or not isinstance(payload, (bytes, bytearray, memoryview))
        or len(payload) > _COMPRESSED_ARTIFACT_MAX_BYTES
    ):
        raise FormalAnalysisArtifactIntegrityError("formal analysis artifact header differs")
    try:
        inflater = zlib.decompressobj()
        raw = inflater.decompress(payload, size + 1)
        if not inflater.eof or inflater.unconsumed_tail or inflater.unused_data or len(raw) != size:
            raise ValueError("formal analysis artifact compressed framing differs")
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError("formal analysis artifact digest differs")
        decoded = json.loads(raw)
        if _canonical_payload(decoded) != raw:
            raise ValueError("formal analysis artifact JSON is not canonical")
        return decoded
    except (ValueError, zlib.error, RecursionError, UnicodeError) as exc:
        raise FormalAnalysisArtifactIntegrityError("formal analysis artifact content differs") from exc


def _lookup(connection: Connection, statement: str, parameter: str, keys: list[str], **other: str):
    if not keys:
        return []
    query = text(statement).bindparams(bindparam(parameter, expanding=True))
    return connection.execute(query, {parameter: keys, **other}).mappings().all()


def _insert_missing(connection: Connection, statement: str, pending: dict, read_existing) -> None:
    for _attempt in range(3):
        if not pending:
            return
        try:
            with connection.begin_nested():
                connection.execute(text(statement), list(pending.values()))
        except IntegrityError:
            # MySQL's default repeatable-read snapshot can predate a competing
            # insert. The collision check uses a current locking read.
            duplicates = read_existing(list(pending), current=True)
            if not duplicates:
                raise
            pending = {key: row for key, row in pending.items() if key not in duplicates}
        else:
            return
    if pending:
        raise FormalAnalysisStoreError("formal analysis concurrent insert retry budget exceeded")


class FormalAnalysisStore:
    """Use a caller transaction or a bounded engine-owned write transaction."""

    def __init__(self, engine_or_connection: Engine | Connection) -> None:
        self._bind = engine_or_connection

    @contextmanager
    def _connection(self):
        with (self._bind.connect() if isinstance(self._bind, Engine) else nullcontext(self._bind)) as connection:
            yield connection

    def put(self, generation_sha256: str, stage: str, key: str, payload: Any) -> str:
        return self.put_many(generation_sha256, stage, {key: payload})[key]

    def _transaction(self, operation):
        if isinstance(self._bind, Connection):
            if not self._bind.in_transaction():
                raise FormalAnalysisStoreError("formal analysis checkpoint requires the caller transaction")
            return operation(self._bind)
        for attempt in range(3):
            try:
                with self._bind.begin() as connection:
                    return operation(connection)
            except DBAPIError as exc:
                # A failed savepoint rollback can mask the original server
                # deadlock if MySQL already rolled back the whole transaction.
                error: BaseException | None = exc
                while isinstance(error, DBAPIError):
                    args = getattr(error.orig, "args", ())
                    if args and args[0] in _MYSQL_LOCK_RETRY:
                        break
                    sqlite_code = getattr(error.orig, "sqlite_errorcode", None)
                    if isinstance(sqlite_code, int) and (sqlite_code & 255) in _SQLITE_LOCK_RETRY:
                        break
                    error = error.__context__
                else:
                    raise
                if attempt == 2:
                    raise
                time.sleep(0.05 * (attempt + 1))
        raise FormalAnalysisStoreError("formal analysis checkpoint write retry budget exceeded")

    def get_or_put(self, generation_sha256: str, stage: str, key: str, payload: Any) -> Any:
        """Insert the first value or return the independently committed winner."""
        generation = _identity(generation_sha256, _SHA256, "generation digest")
        stage = _identity(stage, _STAGE, "stage")
        key = _identity(key, _KEY, "checkpoint key")

        def select_or_append(connection: Connection):
            existing = self._get_member(connection, generation, stage, key)
            if existing is not None:
                return existing
            try:
                FormalAnalysisStore(connection).put(generation, stage, key, payload)
            except FormalAnalysisConflict:
                pass
            winner = self._get_member(connection, generation, stage, key, current=True)
            if winner is None:
                raise FormalAnalysisArtifactIntegrityError("formal analysis checkpoint winner is unavailable")
            return winner

        return self._transaction(select_or_append)

    def put_many(self, generation_sha256: str, stage: str, payloads: Mapping[str, Any]) -> dict[str, str]:
        generation = _identity(generation_sha256, _SHA256, "generation digest")
        stage = _identity(stage, _STAGE, "stage")
        if not isinstance(payloads, Mapping) or len(payloads) > FORMAL_ANALYSIS_BATCH_MAX_MEMBERS:
            raise ValueError("formal analysis checkpoint write batch exceeds the member limit")
        prepared: dict[str, dict[str, Any]] = {}
        membership = {}
        total_bytes = 0
        for key, payload in payloads.items():
            key = _identity(key, _KEY, "checkpoint key")
            canonical = _canonical_payload(payload)
            digest = hashlib.sha256(canonical).hexdigest()
            if digest not in prepared:
                total_bytes += len(canonical)
                if total_bytes > _BATCH_MAX_BYTES:
                    raise ValueError("formal analysis checkpoint write batch exceeds the byte limit")
                prepared[digest] = {
                    "artifact_sha256": digest, "payload_bytes": len(canonical),
                    "payload_zlib": zlib.compress(canonical, level=3),
                }
            membership[key] = digest
        if not membership:
            return {}
        ordered = dict(sorted(membership.items()))
        self._transaction(lambda connection: self._write(connection, generation, stage, prepared, ordered))
        return ordered

    @staticmethod
    def _write(connection: Connection, generation: str, stage: str, artifacts: dict, membership: dict) -> None:
        mysql = connection.dialect.name == "mysql"
        if connection.dialect.name == "sqlite" and not connection.connection.driver_connection.in_transaction:
            # SQLite's legacy driver can otherwise commit a first SAVEPOINT
            # independently of the caller's still-implicit transaction.
            connection.exec_driver_sql("BEGIN")

        def existing_artifacts(keys, *, current=False):
            suffix = " FOR SHARE" if current and mysql else ""
            records = _lookup(
                connection, f"SELECT artifact_sha256,payload_bytes,{_ARTIFACT_PAYLOAD_READ} FROM {ARTIFACT_TABLE} "
                f"WHERE artifact_sha256 IN :ids{suffix}", "ids", keys,
            )
            for row in records:
                _decode_artifact(row, expected=row["artifact_sha256"])
            return {row["artifact_sha256"] for row in records}

        def existing_members(keys, *, current=False):
            suffix = " FOR SHARE" if current and mysql else ""
            records = _lookup(
                connection, f"SELECT unit_key,artifact_sha256 FROM {MEMBER_TABLE} "
                "WHERE generation_sha256=:generation AND stage=:stage "
                f"AND unit_key IN :keys{suffix}", "keys", keys, generation=generation, stage=stage,
            )
            for row in records:
                if row["artifact_sha256"] != membership[row["unit_key"]]:
                    raise FormalAnalysisConflict("formal analysis immutable checkpoint member differs")
            return {row["unit_key"] for row in records}

        # The same operation either inserts every requested member or leaves
        # the caller's enclosing transaction unchanged on a rejected batch.
        with connection.begin_nested():
            existing = existing_members(list(membership))
            existing_objects = existing_artifacts(list(artifacts))
            _insert_missing(connection, (
                f"INSERT INTO {ARTIFACT_TABLE} (artifact_sha256,payload_bytes,payload_zlib) "
                "VALUES (:artifact_sha256,:payload_bytes,:payload_zlib)"
            ), {key: artifacts[key] for key in sorted(set(artifacts) - existing_objects)}, existing_artifacts)
            _insert_missing(connection, (
                f"INSERT INTO {MEMBER_TABLE} (generation_sha256,stage,unit_key,artifact_sha256) "
                "VALUES (:generation,:stage,:key,:artifact_sha256)"
            ), {
                key: {"generation": generation, "stage": stage, "key": key, "artifact_sha256": membership[key]}
                for key in sorted(membership.keys() - existing)
            }, existing_members)

    def get(self, generation_sha256: str, stage: str, key: str) -> Any | None:
        generation = _identity(generation_sha256, _SHA256, "generation digest")
        stage = _identity(stage, _STAGE, "stage")
        key = _identity(key, _KEY, "checkpoint key")
        with self._connection() as connection:
            return self._get_member(connection, generation, stage, key)

    def get_many(self, generation_sha256: str, stage: str, keys: Sequence[str]) -> dict[str, Any]:
        generation = _identity(generation_sha256, _SHA256, "generation digest")
        stage = _identity(stage, _STAGE, "stage")
        if isinstance(keys, (str, bytes)) or not isinstance(keys, Sequence) or len(keys) > FORMAL_ANALYSIS_BATCH_MAX_MEMBERS:
            raise ValueError("formal analysis checkpoint read batch exceeds the member limit")
        keys = list(dict.fromkeys(_identity(key, _KEY, "checkpoint key") for key in keys))
        if not keys:
            return {}
        with self._connection() as connection:
            members = _lookup(connection, f"""SELECT m.unit_key,m.artifact_sha256,a.payload_bytes
                FROM {MEMBER_TABLE} m LEFT JOIN {ARTIFACT_TABLE} a ON a.artifact_sha256=m.artifact_sha256
                WHERE m.generation_sha256=:generation AND m.stage=:stage AND m.unit_key IN :keys""",
                "keys", keys, generation=generation, stage=stage)
        sizes = {}
        for row in members:
            try:
                digest = _identity(row['artifact_sha256'], _SHA256, 'stored artifact digest')
            except ValueError as exc:
                raise FormalAnalysisArtifactIntegrityError('formal analysis member digest is invalid') from exc
            if row['payload_bytes'] is None:
                raise FormalAnalysisArtifactMissing('formal analysis member artifact is unavailable')
            if type(row['payload_bytes']) is not int or not 0 < row['payload_bytes'] <= FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES:
                raise FormalAnalysisArtifactIntegrityError('formal analysis artifact header differs')
            sizes[digest] = row['payload_bytes']
        if sum(sizes.values()) > _BATCH_MAX_BYTES:
            raise FormalAnalysisArtifactIntegrityError('formal analysis checkpoint read batch exceeds the byte limit')
        artifacts = {}
        digests = list(sizes)
        for start in range(0, len(digests), _LOAD_MAX_ARTIFACTS):
            artifacts.update(self.load_artifacts(digests[start:start + _LOAD_MAX_ARTIFACTS]))
        return {row['unit_key']:artifacts[row['artifact_sha256']] for row in members}

    @staticmethod
    def _get_member(connection: Connection, generation: str, stage: str, key: str, *, current: bool = False):
        params = {"generation": generation, "stage": stage, "key": key}
        if current and connection.dialect.name == "mysql":
            # Both sides must bypass the consistent snapshot when a competing
            # transaction inserted a new link and its content together.
            expected = connection.execute(text(f"""
                SELECT artifact_sha256 FROM {MEMBER_TABLE}
                WHERE generation_sha256=:generation AND stage=:stage AND unit_key=:key FOR SHARE
            """), params).scalar_one_or_none()
            if expected is None:
                return None
            row = connection.execute(text(f"""
                SELECT artifact_sha256,payload_bytes,{_ARTIFACT_PAYLOAD_READ}
                FROM {ARTIFACT_TABLE} WHERE artifact_sha256=:digest FOR SHARE
            """), {"digest": expected}).mappings().one_or_none()
            return FormalAnalysisStore._decode_member(row, expected)
        row = connection.execute(text(f"""
            SELECT m.artifact_sha256 AS expected,a.artifact_sha256,a.payload_bytes,{_ARTIFACT_PAYLOAD_READ}
            FROM {MEMBER_TABLE} m LEFT JOIN {ARTIFACT_TABLE} a ON a.artifact_sha256=m.artifact_sha256
            WHERE m.generation_sha256=:generation AND m.stage=:stage AND m.unit_key=:key
        """), params).mappings().one_or_none()
        return None if row is None else FormalAnalysisStore._decode_member(row, row["expected"])

    @staticmethod
    def _decode_member(row: Mapping[str, Any] | None, expected: str) -> Any:
        try:
            _identity(expected, _SHA256, "stored member digest")
        except ValueError as exc:
            raise FormalAnalysisArtifactIntegrityError("formal analysis member digest is invalid") from exc
        if row is None:
            raise FormalAnalysisArtifactIntegrityError("formal analysis member artifact is unavailable")
        return _decode_artifact(row, expected=expected)

    def load_artifact(self, artifact_sha256: str) -> Any:
        return self.load_artifacts([artifact_sha256])[artifact_sha256]

    def load_artifacts(self, artifact_sha256s: Sequence[str]) -> dict[str, Any]:
        if (
            not isinstance(artifact_sha256s, Sequence) or isinstance(artifact_sha256s, (str, bytes))
            or len(artifact_sha256s) > _LOAD_MAX_ARTIFACTS
        ):
            raise ValueError("formal analysis batch exceeds the artifact read count limit")
        digests = dict.fromkeys(_identity(value, _SHA256, "artifact digest") for value in artifact_sha256s)
        with self._connection() as connection:
            rows = _lookup(connection, f"""
                SELECT a.artifact_sha256,a.payload_bytes,
                    CASE WHEN total_bytes <= {_BATCH_MAX_BYTES} AND total_wire_bytes <= {_LOAD_MAX_COMPRESSED_BYTES}
                        AND a.payload_bytes BETWEEN 1 AND {FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES}
                        AND LENGTH(a.payload_zlib) <= {_COMPRESSED_ARTIFACT_MAX_BYTES}
                    THEN a.payload_zlib END AS payload_zlib, total_bytes, total_wire_bytes
                FROM {ARTIFACT_TABLE} a CROSS JOIN (
                    SELECT SUM(payload_bytes) AS total_bytes,
                        SUM(LENGTH(payload_zlib)) AS total_wire_bytes
                    FROM {ARTIFACT_TABLE} WHERE artifact_sha256 IN :ids
                ) AS batch WHERE a.artifact_sha256 IN :ids
            """, "ids", list(digests))
        by_digest = {row["artifact_sha256"]: row for row in rows}
        if by_digest.keys() != digests.keys():
            raise FormalAnalysisArtifactMissing("formal analysis artifact is unavailable")
        if any(row["total_bytes"] > _BATCH_MAX_BYTES or row["total_wire_bytes"] > _LOAD_MAX_COMPRESSED_BYTES
               for row in rows):
            raise FormalAnalysisArtifactIntegrityError("formal analysis batch exceeds the artifact read byte limit")
        return {digest: _decode_artifact(by_digest[digest], expected=digest) for digest in digests}

    def list_members(self, generation_sha256: str, stage: str) -> dict[str, str]:
        generation = _identity(generation_sha256, _SHA256, "generation digest")
        stage = _identity(stage, _STAGE, "stage")
        with self._connection() as connection:
            rows = connection.execute(text(f"""
                SELECT unit_key,artifact_sha256 FROM {MEMBER_TABLE}
                WHERE generation_sha256=:generation AND stage=:stage
                ORDER BY unit_key LIMIT :limit
            """), {"generation": generation, "stage": stage, "limit": _LIST_MAX_MEMBERS + 1}).all()
        if len(rows) > _LIST_MAX_MEMBERS:
            raise FormalAnalysisArtifactIntegrityError("formal analysis checkpoint exceeds the member read limit")
        try:
            return {
                _identity(key, _KEY, "stored checkpoint key"): _identity(digest, _SHA256, "stored artifact digest")
                for key, digest in rows
            }
        except ValueError as exc:
            raise FormalAnalysisArtifactIntegrityError("formal analysis stored checkpoint identity differs") from exc
