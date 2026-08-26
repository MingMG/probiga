from __future__ import annotations

import inspect
from datetime import datetime
from unittest.mock import patch

import pytest

from server.common import schema_recovery_evidence as evidence


def _record(*, action: str = evidence.PLAN_ACTION, payload=None):
    plan_payload = payload or {"table": "legacy_table", "rewrite": ["collation"]}
    return evidence.make_evidence_record(
        recovery_version="legacy-physical.v1",
        source_table="legacy_table",
        source_row_id=0,
        action=action,
        business_key={"table": "legacy_table"},
        source_row={
            "before_fingerprint": {
                "row_count": 2,
                "content_sha256": "a" * 64,
            },
            "fingerprint_columns": ["id", "value"],
        },
        plan_payload=plan_payload,
    )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("business_key_json", '{"table":"other"}'),
        (
            "source_row_json",
            '{"before_fingerprint":{"content_sha256":"b","row_count":2},'
            '"fingerprint_columns":["id","value"]}',
        ),
        ("plan_payload_json", '{"rewrite":[],"table":"legacy_table"}'),
        ("source_row_sha256", "b" * 64),
        ("plan_sha256", "b" * 64),
        ("recovery_key", "b" * 64),
    ],
)
def test_strict_record_recomputes_every_cryptographic_field(field, replacement):
    tampered = _record()
    tampered[field] = replacement

    with pytest.raises(RuntimeError, match="differ"):
        evidence._strict_evidence_record(tampered)


def test_record_requires_canonical_json():
    tampered = _record()
    tampered["business_key_json"] = '{ "table": "legacy_table" }'

    with pytest.raises(RuntimeError, match="not canonical JSON"):
        evidence._strict_evidence_record(tampered)


class _Result:
    def __init__(self, *, one=None, rows=()):
        self._one = one
        self._rows = list(rows)

    def mappings(self):
        return self

    def one_or_none(self):
        return self._one

    def all(self):
        return list(self._rows)


class _PersistConnection:
    def __init__(self, stored):
        self.stored = stored
        self.insert_count = 0

    def execute(self, statement, params=None):
        sql = str(statement).upper()
        if sql.startswith("INSERT IGNORE"):
            self.insert_count += 1
            return _Result()
        if "SELECT EVENT_ID" in sql:
            return _Result(one=self.stored[str(params["recovery_key"])])
        raise AssertionError(sql)


def _stored(record):
    return {
        "event_id": 7,
        "created_at": datetime(2026, 8, 26, 12, 0, 0),
        **record,
    }


def test_insert_ignore_exact_duplicate_is_fully_reverified_and_collapsed():
    record = _record()
    connection = _PersistConnection({record["recovery_key"]: _stored(record)})
    with patch.object(evidence, "ensure_evidence_table"), patch.object(
        evidence, "validate_recovery_evidence_schema"
    ):
        detail = evidence.persist_and_verify_evidence(
            connection, [record, dict(record)]
        )

    assert connection.insert_count == 1
    assert detail["evidence_row_count"] == 1
    assert detail["evidence_verified"] is True


def test_insert_ignore_tampered_existing_row_fails_closed():
    record = _record()
    stale = _stored(record)
    stale["source_row_json"] = stale["source_row_json"].replace("a" * 64, "b" * 64)
    connection = _PersistConnection({record["recovery_key"]: stale})
    with patch.object(evidence, "ensure_evidence_table"), patch.object(
        evidence, "validate_recovery_evidence_schema"
    ), pytest.raises(RuntimeError, match="cryptographic fields differ"):
        evidence.persist_and_verify_evidence(connection, [record])


def _trigger_rows():
    rows = []
    for name, statement in evidence.TRIGGER_STATEMENTS.items():
        rows.append({
            "TRIGGER_NAME": name,
            "DEFINER": evidence.EXPECTED_TRIGGER_DEFINER,
            "ACTION_TIMING": "BEFORE",
            "EVENT_MANIPULATION": "UPDATE" if name.endswith("_bu") else "DELETE",
            "EVENT_OBJECT_TABLE": evidence.EVIDENCE_TABLE,
            "ACTION_ORIENTATION": "ROW",
            "ACTION_STATEMENT": evidence._expected_trigger_body(statement),
            "SQL_MODE": evidence.EXPECTED_TRIGGER_SQL_MODE,
            "CHARACTER_SET_CLIENT": evidence.EXPECTED_CHARACTER_SET_CLIENT,
            "COLLATION_CONNECTION": evidence.EXPECTED_COLLATION_CONNECTION,
            "DATABASE_COLLATION": evidence.EXPECTED_COLLATION,
        })
    return rows


def _index_rows():
    rows = []
    for name, (unique, columns) in evidence._EXPECTED_INDEXES.items():
        rows.extend({
            "INDEX_NAME": name,
            "NON_UNIQUE": 0 if unique else 1,
            "SEQ_IN_INDEX": sequence,
            "COLUMN_NAME": column,
        } for sequence, column in enumerate(columns, 1))
    return rows


class _ContractConnection:
    def __init__(self, trigger_rows):
        self.trigger_rows = trigger_rows

    def execute(self, statement, _params=None):
        sql = str(statement).upper()
        if "INFORMATION_SCHEMA.TABLES" in sql:
            return _Result(one={
                "ENGINE": evidence.EXPECTED_ENGINE,
                "TABLE_COLLATION": evidence.EXPECTED_COLLATION,
            })
        if "INFORMATION_SCHEMA.STATISTICS" in sql:
            return _Result(rows=_index_rows())
        if "INFORMATION_SCHEMA.TRIGGERS" in sql:
            return _Result(rows=self.trigger_rows)
        raise AssertionError(sql)


def test_append_only_physical_contract_requires_exact_trigger_definitions():
    connection = _ContractConnection(_trigger_rows())
    with patch.object(
        evidence,
        "_evidence_column_inventory",
        return_value=dict(evidence._EXPECTED_COLUMNS),
    ):
        detail = evidence.validate_recovery_evidence_schema(
            None, connection=connection
        )
    assert detail["append_only_verified"] is True
    assert detail["trigger_count"] == 2

    wrong = _trigger_rows()
    wrong[0] = {**wrong[0], "DEFINER": "probiga_runtime@127.0.0.1"}
    with patch.object(
        evidence,
        "_evidence_column_inventory",
        return_value=dict(evidence._EXPECTED_COLUMNS),
    ), pytest.raises(RuntimeError, match="trigger definition differs"):
        evidence.validate_recovery_evidence_schema(
            None, connection=_ContractConnection(wrong)
        )

    with patch.object(
        evidence,
        "_evidence_column_inventory",
        return_value=dict(evidence._EXPECTED_COLUMNS),
    ), pytest.raises(RuntimeError, match="triggers differ"):
        evidence.validate_recovery_evidence_schema(
            None, connection=_ContractConnection(_trigger_rows()[:1])
        )


def test_evidence_table_creator_never_creates_triggers_with_migrator_session():
    source = inspect.getsource(evidence.ensure_evidence_table).upper()
    assert "CREATE TRIGGER" not in source
    assert set(evidence.TRIGGER_STATEMENTS) == {
        "trg_privileged_schema_recovery_evidence_immutable_bu",
        "trg_privileged_schema_recovery_evidence_immutable_bd",
    }


def test_pending_plan_journal_pairs_plan_and_verified_by_plan_hash():
    plan = _record()
    verified = _record(action=evidence.VERIFIED_ACTION)
    rows = [_stored(plan), _stored(verified)]
    rows[1]["event_id"] = 8

    class Connection:
        def execute(self, *_args, **_kwargs):
            return _Result(rows=rows)

    assert evidence.load_pending_physical_rewrite_plan(
        Connection(),
        recovery_version="legacy-physical.v1",
        source_table="legacy_table",
    ) is None

    rows.pop()
    pending = evidence.load_pending_physical_rewrite_plan(
        Connection(),
        recovery_version="legacy-physical.v1",
        source_table="legacy_table",
    )
    assert pending["plan_sha256"] == plan["plan_sha256"]
    assert pending["business_key"] == {"table": "legacy_table"}


def test_pending_plan_journal_rejects_multiple_unverified_plans():
    first = _record(payload={"table": "legacy_table", "rewrite": ["collation"]})
    second = _record(payload={"table": "legacy_table", "rewrite": ["engine"]})
    rows = [_stored(first), _stored(second)]
    rows[1]["event_id"] = 8

    class Connection:
        def execute(self, *_args, **_kwargs):
            return _Result(rows=rows)

    with pytest.raises(RuntimeError, match="multiple unverified"):
        evidence.load_pending_physical_rewrite_plan(
            Connection(),
            recovery_version="legacy-physical.v1",
            source_table="legacy_table",
        )
