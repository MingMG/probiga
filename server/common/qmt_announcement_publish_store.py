"""Private durable shards for atomic announcement publication."""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
import hashlib
import json
import re
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Connection, Engine

from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import (
    ARTIFACT_TABLE,
    FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES,
    MEMBER_TABLE,
)


_STAGE = "qmt-announcement-publish"
_SHARD_SCHEMA = "probiga.qmt-announcement-publication-shard.v1"
_SEAL_SCHEMA = "probiga.qmt-announcement-publication-seal.v1"
_CNINFO = "cninfo.announcement"
_SOURCES = frozenset({_CNINFO, "qmt.announcement", "eastmoney.notice"})
_DIGEST = re.compile(r"[a-f0-9]{64}")
_KEY = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,159}")
_CODE = re.compile(r"[0-9]{6}")
_MAX_SHARDS = 512
_MAX_COVERAGE = 20_000
_MAX_FRONTIERS = 20_000
_MAX_EVENT_FRONTIERS = 250_000

_EVENT_TABLE = "st_pit_event_revision"
_COVERAGE_TABLE = "st_pit_source_coverage"
_EVENT_TYPES = {
    "revision_id": "CHAR(64)", "identity_hash": "CHAR(64)", "event_key": "VARCHAR(160)",
    "stock_code": "VARCHAR(16)", "event_date": "DATE", "published_at": "DATETIME(6)",
    "source_published_text": "VARCHAR(128)", "publication_time_status": "VARCHAR(32)",
    "known_at": "DATETIME(6)", "received_at": "DATETIME(6)", "revision_no": "INT UNSIGNED",
    "supersedes_revision_id": "CHAR(64)", "source": "VARCHAR(64)", "batch_id": "VARCHAR(128)",
    "content_hash": "CHAR(64)", "revision_fingerprint_hash": "CHAR(64)",
    "payload_json": "LONGTEXT", "created_at": "DATETIME(6)",
}
_COVERAGE_TYPES = {
    "coverage_id": "CHAR(64)", "scope_hash": "CHAR(64)", "fact_kind": "VARCHAR(16)",
    "stock_code": "VARCHAR(16)", "window_start": "DATE", "window_end": "DATE",
    "known_at": "DATETIME(6)", "received_at": "DATETIME(6)", "covered_through_at": "DATETIME(6)",
    "watermark_kind": "VARCHAR(32)", "watermark_hash": "CHAR(64)", "coverage_status": "VARCHAR(32)",
    "result_count": "INT UNSIGNED", "source_response_hash": "CHAR(64)", "fact_set_hash": "CHAR(64)",
    "revision_no": "INT UNSIGNED", "supersedes_coverage_id": "CHAR(64)", "source": "VARCHAR(64)",
    "batch_id": "VARCHAR(128)", "coverage_fingerprint_hash": "CHAR(64)",
    "payload_json": "LONGTEXT", "created_at": "DATETIME(6)",
}
_RECEIPT_FIELDS = frozenset({
    "coverage_id", "source_response_hash", "fact_set_hash", "revision_no",
    "supersedes_coverage_id", "covered_through_at", "idempotent",
})
_PROOF_FIELDS = frozenset({
    "event_predecessors", "coverage_predecessors", "coverage_receipts",
    "event_count", "coverage_count", "batch_ids", "batch_roots",
})

# The store records a raw zlib stream. MySQL UNCOMPRESS needs the four-byte
# little-endian uncompressed size preceding that stream.
_ARTIFACT_SELECT = f"""
    SELECT a.payload_bytes, m.artifact_sha256,
           CONVERT(UNCOMPRESS(CONCAT(
               REVERSE(UNHEX(LPAD(HEX(a.payload_bytes),8,'0'))), a.payload_zlib
           )) USING utf8mb4) AS payload
      FROM {MEMBER_TABLE} m JOIN {ARTIFACT_TABLE} a ON a.artifact_sha256=m.artifact_sha256
     WHERE m.generation_sha256=:generation AND m.stage=:stage AND m.unit_key=:key
       AND m.artifact_sha256=:digest
       AND a.payload_bytes BETWEEN 1 AND {FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES}
       AND LENGTH(a.payload_zlib)<={FORMAL_ANALYSIS_ARTIFACT_MAX_BYTES + 8192}
     LIMIT 1
"""


class AnnouncementPublishError(RuntimeError):
    """The private publication proof cannot authorize canonical rows."""


class AnnouncementPublishConflict(AnnouncementPublishError):
    """A canonical PIT predecessor advanced after the shard was planned."""


def _digest(value: Any) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise AnnouncementPublishError("announcement publication digest is invalid")
    return value


def _shard_key(value: Any) -> str:
    if not isinstance(value, str) or value == "seal" or _KEY.fullmatch(value) is None:
        raise AnnouncementPublishError("announcement publication shard key is invalid")
    return value


def _sql_value(value: Any) -> str | int | None:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            raise AnnouncementPublishError("announcement publication timestamp must be naive")
        return value.isoformat(sep=" ", timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if value is None or type(value) in (str, int):
        return value
    raise AnnouncementPublishError("announcement publication row value is invalid")


def _predecessors(values: Mapping[str, Sequence[Any] | None]) -> dict[str, list[Any] | None]:
    if not isinstance(values, Mapping) or len(values) > _MAX_FRONTIERS:
        raise AnnouncementPublishError("announcement publication frontier exceeds its bound")
    result = {}
    for key, head in values.items():
        _digest(key)
        if head is None:
            result[key] = None
        elif (
            isinstance(head, (list, tuple)) and len(head) == 2
            and type(head[0]) is int and 0 < head[0] < 2**32
        ):
            result[key] = [head[0], _digest(head[1])]
        else:
            raise AnnouncementPublishError("announcement publication predecessor is invalid")
    return result


def _coverage_receipt(receipt: Mapping[str, Any]) -> dict[str, Any]:
    if set(receipt) != _RECEIPT_FIELDS:
        raise AnnouncementPublishError("announcement publication coverage receipt fields differ")
    result = dict(receipt)
    for key in ("coverage_id", "source_response_hash", "fact_set_hash"):
        _digest(result[key])
    if result["supersedes_coverage_id"] is not None:
        _digest(result["supersedes_coverage_id"])
    if (
        type(result["revision_no"]) is not int or not 0 < result["revision_no"] < 2**32
        or type(result["idempotent"]) is not bool
    ):
        raise AnnouncementPublishError("announcement publication coverage revision differs")
    result["covered_through_at"] = _timestamp_text(result["covered_through_at"])
    return result


def _timestamp_text(value: datetime | str) -> str:
    return str(_sql_value(datetime.fromisoformat(value) if isinstance(value, str) else value))


def _stock_codes_digest(codes: Sequence[str]) -> str:
    encoded = json.dumps(sorted(codes), separators=(",", ":")).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _prepare_rows(rows, columns, heads, *, source, scope: str, identifier: str, previous: str):
    normalized = []
    scopes = set()
    for row in rows:
        if set(row) != columns.keys():
            raise AnnouncementPublishError("announcement publication canonical row fields differ")
        item = {key: _sql_value(row[key]) for key in columns}
        identity = _digest(item[scope])
        _digest(item[identifier])
        if identity in scopes or identity not in heads:
            raise AnnouncementPublishError("announcement publication row frontier differs")
        scopes.add(identity)
        expected = heads[identity] or [0, None]
        if (
            type(item["revision_no"]) is not int
            or item["revision_no"] != expected[0] + 1 or item[previous] != expected[1]
            or item["source"] != source or not isinstance(item["batch_id"], str)
            or not isinstance(item["stock_code"], str) or _CODE.fullmatch(item["stock_code"]) is None
            or not isinstance(item["payload_json"], str)
        ):
            raise AnnouncementPublishError("announcement publication canonical row identity differs")
        normalized.append(item)
    return normalized


def _check_heads(connection, *, table, index, scope, identifier, expected):
    if not expected:
        return
    mysql = connection.dialect.name == "mysql"
    found = {}
    ordered = sorted(expected)
    for offset in range(0, len(ordered), 500):
        keys = ordered[offset:offset + 500]
        if mysql:
            query = text(f"""
                SELECT latest.* FROM JSON_TABLE(:keys, '$[*]'
                    COLUMNS (scope_hash CHAR(64) PATH '$')) AS requested
                JOIN LATERAL (
                    SELECT {scope}, revision_no, {identifier} FROM {table} FORCE INDEX ({index})
                    WHERE {scope}=requested.scope_hash COLLATE utf8mb4_bin
                    ORDER BY {scope} DESC, revision_no DESC LIMIT 1 FOR UPDATE
                ) AS latest ON TRUE
            """)
            params = {"keys": json.dumps(keys)}
        else:
            query = text(f"""
                SELECT {scope}, revision_no, {identifier} FROM {table}
                WHERE {scope} IN :keys ORDER BY {scope}, revision_no DESC
            """).bindparams(bindparam("keys", expanding=True))
            params = {"keys": keys}
        for key, revision, row_id in connection.execute(query, params):
            found.setdefault(key, [revision, row_id])
    if any(found.get(key) != head for key, head in expected.items()):
        raise AnnouncementPublishConflict("announcement publication canonical predecessor changed")


def _mysql_insert(connection, *, table, columns, field, parameters):
    projection = ", ".join(
        f"{column} {kind} PATH '$.{column}' ERROR ON EMPTY ERROR ON ERROR"
        for column, kind in columns.items()
    )
    names = ", ".join(f"`{column}`" for column in columns)
    row_names = ", ".join(f"planned.`{column}`" for column in columns)
    return connection.execute(text(f"""
        INSERT INTO {table} ({names})
        SELECT {row_names}
          FROM ({_ARTIFACT_SELECT}) AS artifact
          JOIN JSON_TABLE(artifact.payload, '$.{field}[*]' COLUMNS ({projection})) AS planned
            ON TRUE
         WHERE OCTET_LENGTH(artifact.payload)=artifact.payload_bytes
           AND SHA2(artifact.payload,256)=artifact.artifact_sha256
    """), parameters).rowcount


def _sqlite_insert(connection, *, table, columns, rows):
    names = ", ".join(columns)
    values = ", ".join(f":{column}" for column in columns)
    deserializers = {
        column: date.fromisoformat if kind == "DATE" else datetime.fromisoformat
        for column, kind in columns.items() if kind in {"DATE", "DATETIME(6)"}
    }
    prepared = []
    for row in rows:
        item = dict(row)
        for column, deserialize in deserializers.items():
            if item[column] is not None:
                item[column] = deserialize(item[column])
        prepared.append(item)
    return connection.execute(
        text(f"INSERT INTO {table} ({names}) VALUES ({values})"), prepared,
    ).rowcount


class AnnouncementPublishStore:
    """Append immutable private shards, then publish in the caller's transaction."""

    def __init__(
        self, engine_or_connection: Engine | Connection, generation_sha256: str, *,
        source: str = _CNINFO, watermark_kind: str = "HISTORICAL_RECONSTRUCTION",
    ):
        self._bind = engine_or_connection
        self.generation = _digest(generation_sha256)
        if (
            source not in _SOURCES or watermark_kind not in {"HISTORICAL_RECONSTRUCTION", "QUERY_CUTOFF"}
            or (watermark_kind == "HISTORICAL_RECONSTRUCTION" and source != _CNINFO)
        ):
            raise AnnouncementPublishError("announcement publication source authority differs")
        self._source = source
        self._watermark_kind = watermark_kind
        self._store = FormalAnalysisStore(engine_or_connection)

    def put_shard(
        self, key: str, *, event_rows, coverage_rows, event_predecessors,
        coverage_predecessors, coverage_receipts,
    ) -> str:
        key = _shard_key(key)
        events = _predecessors(event_predecessors)
        coverages = _predecessors(coverage_predecessors)
        prepared_events = _prepare_rows(
            event_rows, _EVENT_TYPES, events, source=self._source, scope="identity_hash",
            identifier="revision_id", previous="supersedes_revision_id",
        )
        prepared_coverage = _prepare_rows(
            coverage_rows, _COVERAGE_TYPES, coverages, source=self._source, scope="scope_hash",
            identifier="coverage_id", previous="supersedes_coverage_id",
        )
        receipts = [_coverage_receipt(item) for item in coverage_receipts]
        if not receipts or len(receipts) != len(coverages):
            raise AnnouncementPublishError("announcement publication shard coverage differs")
        ids = {item["coverage_id"] for item in receipts}
        if len(ids) != len(receipts) or not {r["coverage_id"] for r in prepared_coverage} <= ids:
            raise AnnouncementPublishError("announcement publication shard coverage identities differ")
        batch_roots = set()
        for row in prepared_coverage:
            if (
                row["fact_kind"] != "event" or row["coverage_status"] != "COMPLETE"
                or row["watermark_kind"] != self._watermark_kind
            ):
                raise AnnouncementPublishError("announcement publication coverage authority differs")
            try:
                evidence = json.loads(row["payload_json"])["watermark"]["evidence"]
                batch_roots.add(_digest(evidence["global_batch_root_hash"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise AnnouncementPublishError("announcement publication coverage root is missing") from exc
        return self._store.put(self.generation, _STAGE, key, {
            "schema": _SHARD_SCHEMA, "generation": self.generation, "key": key,
            "source": self._source, "watermark_kind": self._watermark_kind,
            "event_rows": prepared_events, "coverage_rows": prepared_coverage,
            "proof": {
                "event_predecessors": events, "coverage_predecessors": coverages,
                "coverage_receipts": receipts, "event_count": len(prepared_events),
                "coverage_count": len(prepared_coverage),
                "batch_ids": sorted({r["batch_id"] for r in prepared_events + prepared_coverage}),
                "batch_roots": sorted(batch_roots),
            },
        })

    def get_shard(self, key: str) -> dict | None:
        key = _shard_key(key)
        shard = self._store.get(self.generation, _STAGE, key)
        if shard is not None and (
            not isinstance(shard, dict) or shard.get("schema") != _SHARD_SCHEMA
            or shard.get("generation") != self.generation or shard.get("key") != key
            or shard.get("source") != self._source or shard.get("watermark_kind") != self._watermark_kind
        ):
            raise AnnouncementPublishError("announcement publication shard identity differs")
        return shard

    def _artifact_parameters(self, key: str, expected_digest: str) -> dict[str, str]:
        return {
            "generation": self.generation, "stage": _STAGE,
            "key": _shard_key(key), "digest": _digest(expected_digest),
        }

    def get_shard_proof(self, key: str, expected_digest: str) -> dict:
        """Verify an exact artifact and return its compact proof without projecting payloads."""
        params = self._artifact_parameters(key, expected_digest)
        if isinstance(self._bind, Engine):
            with self._bind.connect() as connection:
                return AnnouncementPublishStore(
                    connection, self.generation, source=self._source,
                    watermark_kind=self._watermark_kind,
                ).get_shard_proof(key, expected_digest)
        if self._bind.dialect.name == "mysql":
            raw = self._bind.execute(text(f"""
                SELECT JSON_EXTRACT(artifact.payload,'$.proof') AS proof
                  FROM ({_ARTIFACT_SELECT}) AS artifact
                 WHERE OCTET_LENGTH(artifact.payload)=artifact.payload_bytes
                   AND SHA2(artifact.payload,256)=artifact.artifact_sha256
                   AND JSON_UNQUOTE(JSON_EXTRACT(artifact.payload,'$.schema'))=:schema
                   AND JSON_UNQUOTE(JSON_EXTRACT(artifact.payload,'$.generation'))=:generation
                   AND JSON_UNQUOTE(JSON_EXTRACT(artifact.payload,'$.key'))=:key
                   AND JSON_UNQUOTE(JSON_EXTRACT(artifact.payload,'$.source'))=:source
                   AND JSON_UNQUOTE(JSON_EXTRACT(artifact.payload,'$.watermark_kind'))=:watermark
            """), {
                **params, "schema": _SHARD_SCHEMA, "source": self._source,
                "watermark": self._watermark_kind,
            }).scalar_one_or_none()
            if raw is None:
                raise AnnouncementPublishError("announcement publication shard digest differs")
            proof = json.loads(raw)
        else:
            bound = self._bind.execute(text(f"""
                SELECT artifact_sha256 FROM {MEMBER_TABLE}
                 WHERE generation_sha256=:generation AND stage=:stage AND unit_key=:key
            """), params).scalar_one_or_none()
            if bound != expected_digest:
                raise AnnouncementPublishError("announcement publication shard digest differs")
            proof = self.get_shard(key)["proof"]
        if not isinstance(proof, dict) or set(proof) != _PROOF_FIELDS:
            raise AnnouncementPublishError("announcement publication shard proof differs")
        return proof

    def list_shards(self) -> dict[str, str]:
        return {
            _shard_key(key): digest
            for key, digest in self._store.list_members(self.generation, _STAGE).items()
            if key != "seal"
        }

    def get_seal(self) -> dict | None:
        seal = self._store.get(self.generation, _STAGE, "seal")
        if seal is not None and (
            not isinstance(seal, dict) or seal.get("schema") != _SEAL_SCHEMA
            or seal.get("generation") != self.generation
            or seal.get("source") != self._source or seal.get("watermark_kind") != self._watermark_kind
        ):
            raise AnnouncementPublishError("announcement publication seal identity differs")
        return seal

    def promote(
        self, connection: Connection, *, expected_shards: Mapping[str, str], batch_id: str,
        batch_root_hash: str, expected_coverage_count: int,
        check_deadline: Callable[[str], Any] | None = None,
        expected_stock_codes: Sequence[str] | None = None,
    ) -> dict:
        if not connection.in_transaction() or (
            connection is not self._bind and connection.engine is not self._bind
        ):
            raise AnnouncementPublishError("announcement publication requires the caller transaction")
        mysql = connection.dialect.name == "mysql"
        if mysql and connection.get_isolation_level() not in {"REPEATABLE READ", "SERIALIZABLE"}:
            raise AnnouncementPublishError("announcement publication requires locking isolation")
        if (
            not isinstance(expected_shards, Mapping) or not 0 < len(expected_shards) <= _MAX_SHARDS
            or type(expected_coverage_count) is not int or not 0 < expected_coverage_count <= _MAX_COVERAGE
            or not isinstance(batch_id, str) or not batch_id or len(batch_id) > 128
        ):
            raise AnnouncementPublishError("announcement publication manifest scope is invalid")
        root = _digest(batch_root_hash)
        expected = dict(sorted((_shard_key(key), _digest(sha)) for key, sha in expected_shards.items()))
        expected_codes = None
        if expected_stock_codes is not None:
            if (
                len(expected_stock_codes) != expected_coverage_count
                or any(not isinstance(code, str) or _CODE.fullmatch(code) is None for code in expected_stock_codes)
            ):
                raise AnnouncementPublishError("announcement publication expected issuer set differs")
            expected_codes = set(expected_stock_codes)
            if len(expected_codes) != expected_coverage_count:
                raise AnnouncementPublishError("announcement publication expected issuer set differs")
        session = AnnouncementPublishStore(
            connection, self.generation, source=self._source, watermark_kind=self._watermark_kind,
        )
        hint = " FOR UPDATE" if mysql else ""
        members = dict(connection.execute(text(f"""
            SELECT unit_key,artifact_sha256 FROM {MEMBER_TABLE}
             WHERE generation_sha256=:generation AND stage=:stage
             ORDER BY unit_key LIMIT :limit{hint}
        """), {"generation": self.generation, "stage": _STAGE, "limit": _MAX_SHARDS + 2}).all())
        seal_digest = members.pop("seal", None)
        if members != expected:
            raise AnnouncementPublishError("announcement publication shard membership differs")
        seal = session.get_seal()
        if seal_digest is not None and seal is None:
            raise AnnouncementPublishError("announcement publication seal is outside the caller snapshot")
        if seal is not None:
            if (
                seal.get("shards") != expected or seal.get("batch_id") != batch_id
                or seal.get("batch_root_hash") != root or seal.get("coverage_count") != expected_coverage_count
                or (expected_codes is not None and seal.get("stock_codes_sha256") != _stock_codes_digest(expected_stock_codes))
            ):
                raise AnnouncementPublishError("announcement publication sealed identity differs")
            return seal

        expected_receipts = {}
        event_heads, coverage_heads = {}, {}
        plans = []
        for key, digest in expected.items():
            if check_deadline is not None:
                check_deadline(f"announcement-shard-proof:{key}")
            parameters = self._artifact_parameters(key, digest)
            proof = session.get_shard_proof(key, digest)
            if (
                proof["batch_ids"] not in ([], [batch_id])
                or proof["batch_roots"] not in ([], [root])
            ):
                raise AnnouncementPublishError("announcement publication shard proof differs")
            for heads, name in ((event_heads, "event_predecessors"), (coverage_heads, "coverage_predecessors")):
                observed = _predecessors(proof[name])
                if heads.keys() & observed.keys():
                    raise AnnouncementPublishError("announcement publication shard frontiers overlap")
                heads.update(observed)
            receipts = [_coverage_receipt(item) for item in proof["coverage_receipts"]]
            if (
                len(receipts) != len(proof["coverage_predecessors"])
                or any(type(proof[n]) is not int or proof[n] < 0 for n in ("event_count", "coverage_count"))
                or proof["event_count"] > len(proof["event_predecessors"])
                or proof["coverage_count"] > len(receipts)
            ):
                raise AnnouncementPublishError("announcement publication shard counters differ")
            for receipt in receipts:
                coverage_id = receipt.pop("coverage_id")
                receipt.pop("idempotent")
                if coverage_id in expected_receipts:
                    raise AnnouncementPublishError("announcement publication coverage receipts overlap")
                expected_receipts[coverage_id] = receipt
            plans.append((parameters, proof["event_count"], proof["coverage_count"]))
            if len(event_heads) > _MAX_EVENT_FRONTIERS or len(coverage_heads) > expected_coverage_count:
                raise AnnouncementPublishError("announcement publication frontier exceeds its manifest bound")
        if len(expected_receipts) != expected_coverage_count:
            raise AnnouncementPublishError("announcement publication has incomplete expected coverage")

        for table, index, scope, identifier, heads in (
            (_EVENT_TABLE, "uk_pit_event_identity_revision", "identity_hash", "revision_id", event_heads),
            (_COVERAGE_TABLE, "uk_pit_coverage_scope_revision", "scope_hash", "coverage_id", coverage_heads),
        ):
            if check_deadline is not None:
                check_deadline(f"announcement-predecessors:{table}")
            _check_heads(connection, table=table, index=index, scope=scope, identifier=identifier, expected=heads)
        for offset, (table, columns, field) in enumerate((
            (_EVENT_TABLE, _EVENT_TYPES, "event_rows"), (_COVERAGE_TABLE, _COVERAGE_TYPES, "coverage_rows"),
        ), start=1):
            for plan in plans:
                count = plan[offset]
                if not count:
                    continue
                params = plan[0]
                if check_deadline is not None:
                    check_deadline(f"announcement-insert:{field}:{params['key']}")
                inserted = (
                    _mysql_insert(connection, table=table, columns=columns, field=field, parameters=params)
                    if mysql else _sqlite_insert(
                        connection, table=table, columns=columns,
                        rows=session.get_shard(params["key"])[field],
                    )
                )
                if inserted != count:
                    raise AnnouncementPublishError("announcement publication inserted row count differs")

        index = " FORCE INDEX (idx_pit_coverage_batch_stock)" if mysql else ""
        coverage_ids = {}
        with connection.execute(text(f"""
            SELECT stock_code, coverage_id, source_response_hash, fact_set_hash,
                   revision_no, supersedes_coverage_id, covered_through_at
              FROM {_COVERAGE_TABLE}{index}
             WHERE fact_kind='event' AND source=:source AND batch_id=:batch
               AND coverage_status='COMPLETE' AND watermark_kind=:watermark
             ORDER BY stock_code LIMIT :limit
        """), {
            "source": self._source, "watermark": self._watermark_kind,
            "batch": batch_id, "limit": expected_coverage_count + 1,
        }) as rows:
            for row in rows.mappings():
                receipt = dict(row)
                code, row_id = receipt.pop("stock_code"), receipt.pop("coverage_id")
                receipt["covered_through_at"] = _timestamp_text(receipt["covered_through_at"])
                if code in coverage_ids or expected_receipts.get(row_id) != receipt:
                    raise AnnouncementPublishError("announcement canonical coverage readback differs")
                coverage_ids[code] = row_id
        if (
            len(coverage_ids) != expected_coverage_count or set(coverage_ids.values()) != expected_receipts.keys()
            or (expected_codes is not None and coverage_ids.keys() != expected_codes)
        ):
            raise AnnouncementPublishError("announcement canonical coverage is incomplete")
        if check_deadline is not None:
            check_deadline("announcement-publication-seal")
        seal = {
            "schema": _SEAL_SCHEMA, "generation": self.generation, "batch_id": batch_id,
            "source": self._source, "watermark_kind": self._watermark_kind,
            "batch_root_hash": root, "coverage_count": expected_coverage_count,
            "stock_codes_sha256": _stock_codes_digest(list(coverage_ids)),
            "coverage_ids": list(coverage_ids.values()), "shards": expected,
        }
        session._store.put(self.generation, _STAGE, "seal", seal)
        return seal
