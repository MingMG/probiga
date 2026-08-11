from __future__ import annotations

from copy import deepcopy
import hashlib
import re
from types import SimpleNamespace

import pytest

from server.integrations.v2_execution_evidence_writer import writer
from tools import trading_v2_evidence_negative_probes as probes


class _Result:
    def __init__(self, *, mapping=None, scalar=None) -> None:
        self._mapping = mapping
        self._scalar = scalar

    def mappings(self):
        return self

    def first(self):
        return self._mapping

    def scalar(self):
        return self._scalar


class _DbGuardError(Exception):
    pass


class _WrappedGuardError(Exception):
    def __init__(self, inner: Exception) -> None:
        super().__init__("wrapped DBAPI error")
        self.orig = inner


def _baseline(metadata: probes.EvidenceTableProbeMetadata) -> dict[str, object]:
    row: dict[str, object] = {
        column: f"value:{metadata.evidence_type}:{column}"
        for column in metadata.columns
    }
    primary = hashlib.sha256(metadata.evidence_type.encode("ascii")).hexdigest()
    row[metadata.primary_column] = primary
    for ordinal, group in enumerate(metadata.invalid_identity_groups):
        if len(group) > 1:
            value = (
                primary
                if metadata.primary_column in group
                else hashlib.sha256(
                    f"{metadata.evidence_type}:{ordinal}".encode("ascii")
                ).hexdigest()
            )
            for column in group:
                row[column] = value
    row["history_origin"] = "START_AFTER_UNKNOWN"
    return row


class _ConnectionContext:
    def __init__(self, engine: "_IdentityBoundFakeEngine") -> None:
        self.engine = engine
        self.connection = _FakeConnection(engine)

    def __enter__(self):
        self.engine.connection_count += 1
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class _FakeTransaction:
    def __init__(self, connection: "_FakeConnection") -> None:
        self.connection = connection
        self.engine = connection.engine
        self.snapshot = deepcopy(self.engine.tables)
        self.rolled_back = False
        self.had_dml = False

    def rollback(self) -> None:
        if self.rolled_back:
            raise AssertionError("transaction rolled back twice")
        self.engine.tables = deepcopy(self.snapshot)
        self.rolled_back = True
        self.engine.events.append(("ROLLBACK", self.had_dml))
        if self.had_dml and self.engine.after_dml_rollback is not None:
            callback = self.engine.after_dml_rollback
            self.engine.after_dml_rollback = None
            callback(self.engine)


class _FakeConnection:
    def __init__(self, engine: "_IdentityBoundFakeEngine") -> None:
        self.engine = engine
        self.transaction: _FakeTransaction | None = None

    def begin(self) -> _FakeTransaction:
        if self.transaction is not None:
            raise AssertionError("nested fake transaction")
        self.transaction = _FakeTransaction(self)
        self.engine.events.append(("BEGIN", False))
        return self.transaction

    def execute(self, statement, params=None):
        sql = str(statement)
        values = {} if params is None else dict(params)
        self.engine.sql.append((sql, values))

        match = re.search(r"v2e-negative:count:([a-z0-9_]+)", sql)
        if match:
            return _Result(scalar=len(self.engine.tables[match.group(1)]))

        match = re.search(r"v2e-negative:baseline:([a-z0-9_]+)", sql)
        if match:
            metadata = self.engine.by_table[match.group(1)]
            found = next(
                (
                    row
                    for row in self.engine.tables[metadata.table]
                    if row[metadata.primary_column] == values["primary_value"]
                ),
                None,
            )
            return _Result(mapping=None if found is None else dict(found))

        match = re.search(
            r"v2e-negative:unique:([a-z0-9_]+):(\d+)", sql
        )
        if match:
            metadata = self.engine.by_table[match.group(1)]
            ordinal = int(match.group(2))
            unique_key = metadata.unique_keys[ordinal]
            count = sum(
                all(row[column] == values[f"uk_{ordinal}_{index}"]
                    for index, column in enumerate(unique_key))
                for row in self.engine.tables[metadata.table]
            )
            return _Result(scalar=count)

        match = re.search(r"v2e-negative:candidate:([a-z0-9_]+)", sql)
        if match:
            metadata = self.engine.by_table[match.group(1)]
            count = sum(
                row[metadata.primary_column] == values["primary_value"]
                for row in self.engine.tables[metadata.table]
            )
            return _Result(scalar=count)

        match = re.search(
            r"v2e-negative:(INVALID_INSERT|REPLACE|ON_DUPLICATE_KEY_UPDATE):"
            r"([a-z0-9_]+)",
            sql,
        )
        if not match:
            raise AssertionError(f"unexpected SQL: {sql}")
        if self.transaction is None:
            raise AssertionError("DML was executed without an explicit transaction")
        self.transaction.had_dml = True
        operation = probes.NegativeProbeOperation(match.group(1))
        metadata = self.engine.by_table[match.group(2)]
        self.engine.events.append(("DML", operation.value))

        if self.engine.guard_mode == "expected":
            message = metadata.expected_message(operation)
            raise _WrappedGuardError(_DbGuardError(1644, message))
        if self.engine.guard_mode == "wrong_errno":
            raise _WrappedGuardError(_DbGuardError(1062, "duplicate key"))
        if self.engine.guard_mode == "wrong_message":
            raise _WrappedGuardError(_DbGuardError(1644, "wrong guard"))
        if self.engine.guard_mode != "success":
            raise AssertionError(f"unknown guard mode {self.engine.guard_mode}")

        row = dict(values)
        if operation is probes.NegativeProbeOperation.INVALID_INSERT:
            self.engine.tables[metadata.table].append(row)
        elif operation is probes.NegativeProbeOperation.REPLACE:
            self.engine.tables[metadata.table] = [
                row
                if item[metadata.primary_column] == row[metadata.primary_column]
                else item
                for item in self.engine.tables[metadata.table]
            ]
        return _Result(scalar=1)


class _IdentityBoundFakeEngine:
    def __init__(
        self,
        baselines: tuple[tuple[probes.EvidenceTableProbeMetadata, dict], ...],
        *,
        guard_mode: str = "expected",
    ) -> None:
        self.by_table = {
            metadata.table: metadata
            for metadata in probes.EVIDENCE_TABLE_PROBE_METADATA.values()
        }
        self.tables = {
            metadata.table: [deepcopy(row)] for metadata, row in baselines
        }
        self.guard_mode = guard_mode
        self.after_dml_rollback = None
        self.events: list[tuple[str, object]] = []
        self.sql: list[tuple[str, dict]] = []
        self.connection_count = 0

    def connect(self):
        return _ConnectionContext(self)


def _case(metadata: probes.EvidenceTableProbeMetadata, row: dict) -> probes.EvidenceNegativeProbeCase:
    return probes.EvidenceNegativeProbeCase(
        metadata.evidence_type,
        str(row[metadata.primary_column]),
    )


def test_allowlist_covers_exactly_five_tables_and_matches_writer_columns():
    assert tuple(probes.EVIDENCE_TABLE_PROBE_METADATA) == (
        "MARKET_CALENDAR",
        "QUOTE_RECEIPT",
        "FILL_EXECUTION",
        "CASH_EVENT",
        "ORDER_TRANSITION",
    )
    assert probes.CALENDAR_STORAGE_COLUMNS == writer.CALENDAR_STORAGE_COLUMNS
    assert probes.QUOTE_STORAGE_COLUMNS == writer.QUOTE_STORAGE_COLUMNS
    assert probes.FILL_STORAGE_COLUMNS == writer.FILL_STORAGE_COLUMNS
    assert probes.CASH_STORAGE_COLUMNS == writer.CASH_STORAGE_COLUMNS
    assert probes.ORDER_STORAGE_COLUMNS == writer.ORDER_STORAGE_COLUMNS
    for metadata in probes.EVIDENCE_TABLE_PROBE_METADATA.values():
        mutated = {
            column
            for group in metadata.invalid_identity_groups
            for column in group
        }
        assert all(set(unique_key) & mutated for unique_key in metadata.unique_keys)


@pytest.mark.parametrize(
    "metadata",
    tuple(probes.EVIDENCE_TABLE_PROBE_METADATA.values()),
    ids=lambda item: item.evidence_type,
)
def test_invalid_builder_is_fresh_parameterized_and_does_not_mutate_baseline(metadata):
    baseline = _baseline(metadata)
    original = deepcopy(baseline)

    statement = probes.build_negative_probe_statement(
        metadata,
        baseline,
        probes.NegativeProbeOperation.INVALID_INSERT,
    )

    assert baseline == original
    assert statement.sql.startswith(
        f"/* v2e-negative:INVALID_INSERT:{metadata.table} */\nINSERT INTO"
    )
    assert "INVALID_PROBE" not in statement.sql
    assert statement.parameters["history_origin"] == "INVALID_PROBE"
    assert set(statement.parameters) == set(metadata.columns)
    assert re.fullmatch(r"[0-9a-f]{64}", statement.candidate_primary_value)
    assert statement.candidate_primary_value != baseline[metadata.primary_column]
    for column in metadata.columns:
        assert f":{column}" in statement.sql
    for unique_key in metadata.unique_keys:
        assert any(
            statement.parameters[column] != baseline[column]
            for column in unique_key
        )


def test_quote_invalid_candidate_does_not_assume_event_and_payload_hash_equal():
    metadata = probes.metadata_for_evidence_type("QUOTE_RECEIPT")
    baseline = _baseline(metadata)
    baseline["quote_event_id"] = "a" * 64
    baseline["source_payload_hash"] = "b" * 64

    statement = probes.build_negative_probe_statement(
        metadata,
        baseline,
        probes.NegativeProbeOperation.INVALID_INSERT,
    )

    assert metadata.invalid_identity_groups == (
        ("quote_evidence_id", "evidence_hash"),
        ("quote_event_id",),
    )
    assert statement.parameters["quote_event_id"] != baseline["quote_event_id"]
    assert (
        statement.parameters["source_payload_hash"]
        == baseline["source_payload_hash"]
    )


def test_unique_preflight_execute_has_exact_statement_and_params_arguments():
    metadata = probes.metadata_for_evidence_type("MARKET_CALENDAR")
    candidate = probes.build_invalid_candidate(metadata, _baseline(metadata))

    class _StrictTwoArgumentConnection:
        def __init__(self) -> None:
            self.calls = []

        def execute(self, statement, parameters):
            self.calls.append((statement, parameters))
            return _Result(scalar=0)

    connection = _StrictTwoArgumentConnection()
    observed = probes._count_unique_candidate(
        connection,
        metadata,
        metadata.unique_keys[0],
        candidate,
        0,
    )

    assert observed == 0
    assert len(connection.calls) == 1
    statement, parameters = connection.calls[0]
    assert "v2e-negative:unique:st_market_calendar_evidence_v2:0" in str(
        statement
    )
    assert parameters == {"uk_0_0": candidate["calendar_evidence_id"]}


@pytest.mark.parametrize(
    ("operation", "prefix", "tail"),
    (
        (probes.NegativeProbeOperation.REPLACE, "REPLACE INTO", ""),
        (
            probes.NegativeProbeOperation.ON_DUPLICATE_KEY_UPDATE,
            "INSERT INTO",
            "ON DUPLICATE KEY UPDATE",
        ),
    ),
)
def test_existing_row_builders_use_exact_baseline(operation, prefix, tail):
    metadata = probes.metadata_for_evidence_type("CASH_EVENT")
    baseline = _baseline(metadata)
    baseline["cash_event_payload_json"] = '{"private":"never-inline"}'

    statement = probes.build_negative_probe_statement(
        metadata,
        baseline,
        operation,
    )

    assert f"\n{prefix} `st_cash_event_binding_v2`" in statement.sql
    assert statement.parameters == baseline
    assert "never-inline" not in statement.sql
    assert tail in statement.sql
    if operation is probes.NegativeProbeOperation.ON_DUPLICATE_KEY_UPDATE:
        assert (
            "`cash_binding_id` = VALUES(`cash_binding_id`)" in statement.sql
        )


def test_builder_rejects_inexact_baseline_and_unregistered_metadata():
    metadata = probes.metadata_for_evidence_type("MARKET_CALENDAR")
    baseline = _baseline(metadata)
    baseline.pop("created_at")
    with pytest.raises(probes.EvidenceNegativeProbeContractError, match="missing"):
        probes.build_negative_probe_statement(
            metadata,
            baseline,
            probes.NegativeProbeOperation.REPLACE,
        )

    copied = probes.EvidenceTableProbeMetadata(
        **{
            field: getattr(metadata, field)
            for field in metadata.__dataclass_fields__
        }
    )
    with pytest.raises(probes.EvidenceNegativeProbeContractError, match="registered"):
        probes.build_negative_probe_statement(
            copied,
            _baseline(metadata),
            probes.NegativeProbeOperation.REPLACE,
        )


def test_mysql_guard_extraction_walks_wrapped_orig_and_checks_message():
    wrapped = _WrappedGuardError(
        _DbGuardError(1644, "Calendar Evidence Is Append Only")
    )
    assert probes.mysql_error_code_message(wrapped) == (
        1644,
        "Calendar Evidence Is Append Only",
    )
    probes.require_1644_guard(wrapped, "calendar evidence is append only")

    with pytest.raises(probes.EvidenceNegativeProbeGuardError, match="code=1062"):
        probes.require_1644_guard(
            _WrappedGuardError(_DbGuardError(1062, "duplicate")),
            "calendar evidence is append only",
        )
    with pytest.raises(
        probes.EvidenceNegativeProbeGuardError,
        match="expected_message",
    ):
        probes.require_1644_guard(
            _WrappedGuardError(_DbGuardError(1644, "different guard")),
            "calendar evidence is append only",
        )


def test_full_five_table_three_operation_matrix_rolls_back_and_retains_rows():
    items = tuple(
        (metadata, _baseline(metadata))
        for metadata in probes.EVIDENCE_TABLE_PROBE_METADATA.values()
    )
    engine = _IdentityBoundFakeEngine(items)
    cases = tuple(_case(metadata, row) for metadata, row in items)
    original = deepcopy(engine.tables)

    results = probes.run_negative_probes(engine, cases)

    assert len(results) == 15
    assert engine.tables == original
    assert all(result.mysql_errno == 1644 for result in results)
    assert all(result.baseline_retained for result in results)
    assert [result.operation for result in results[:3]] == list(
        probes.ALL_NEGATIVE_PROBE_OPERATIONS
    )
    dml_events = [event for event in engine.events if event[0] == "DML"]
    assert len(dml_events) == 15
    assert all(event[0] != "COMMIT" for event in engine.events)


@pytest.mark.parametrize(
    "runner,operation",
    (
        (
            probes.run_invalid_insert_probes,
            probes.NegativeProbeOperation.INVALID_INSERT,
        ),
        (probes.run_replace_probes, probes.NegativeProbeOperation.REPLACE),
        (
            probes.run_on_duplicate_key_update_probes,
            probes.NegativeProbeOperation.ON_DUPLICATE_KEY_UPDATE,
        ),
    ),
)
def test_operation_specific_public_runners(runner, operation):
    metadata = probes.metadata_for_evidence_type("ORDER_TRANSITION")
    baseline = _baseline(metadata)
    engine = _IdentityBoundFakeEngine(((metadata, baseline),))

    results = runner(engine, (_case(metadata, baseline),))

    assert len(results) == 1
    assert results[0].operation is operation


@pytest.mark.parametrize("guard_mode", ("wrong_errno", "wrong_message"))
def test_wrong_guard_fails_only_after_explicit_rollback_and_retention(guard_mode):
    metadata = probes.metadata_for_evidence_type("MARKET_CALENDAR")
    baseline = _baseline(metadata)
    engine = _IdentityBoundFakeEngine(
        ((metadata, baseline),),
        guard_mode=guard_mode,
    )

    with pytest.raises(probes.EvidenceNegativeProbeGuardError):
        probes.run_replace_probes(engine, (_case(metadata, baseline),))

    assert engine.tables[metadata.table] == [baseline]
    assert ("ROLLBACK", True) in engine.events


@pytest.mark.parametrize(
    "operation",
    tuple(probes.NegativeProbeOperation),
)
def test_unexpected_success_is_rolled_back_before_error(operation):
    metadata = probes.metadata_for_evidence_type("MARKET_CALENDAR")
    baseline = _baseline(metadata)
    engine = _IdentityBoundFakeEngine(
        ((metadata, baseline),),
        guard_mode="success",
    )

    with pytest.raises(
        probes.EvidenceNegativeProbeUnexpectedSuccess,
        match="explicit transaction was rolled back",
    ):
        probes.run_negative_probe(engine, _case(metadata, baseline), operation)

    assert engine.tables[metadata.table] == [baseline]
    dml_index = engine.events.index(("DML", operation.value))
    rollback_index = engine.events.index(("ROLLBACK", True))
    assert dml_index < rollback_index


def test_retention_rejects_baseline_mutation_after_guard_rollback():
    metadata = probes.metadata_for_evidence_type("CASH_EVENT")
    baseline = _baseline(metadata)
    engine = _IdentityBoundFakeEngine(((metadata, baseline),))

    def mutate(current: _IdentityBoundFakeEngine) -> None:
        current.tables[metadata.table][0]["history_origin"] = "UNKNOWN"

    engine.after_dml_rollback = mutate
    with pytest.raises(
        probes.EvidenceNegativeProbeRetentionError,
        match="baseline changed",
    ):
        probes.run_replace_probes(engine, (_case(metadata, baseline),))


def test_retention_rejects_row_count_change_after_guard_rollback():
    metadata = probes.metadata_for_evidence_type("ORDER_TRANSITION")
    baseline = _baseline(metadata)
    engine = _IdentityBoundFakeEngine(((metadata, baseline),))

    def append_row(current: _IdentityBoundFakeEngine) -> None:
        extra = deepcopy(baseline)
        extra[metadata.primary_column] = "f" * 64
        extra[metadata.content_hash_column] = "f" * 64
        current.tables[metadata.table].append(extra)

    engine.after_dml_rollback = append_row
    with pytest.raises(
        probes.EvidenceNegativeProbeRetentionError,
        match="row count changed",
    ):
        probes.run_replace_probes(engine, (_case(metadata, baseline),))


def test_invalid_candidate_unique_collision_fails_before_dml_and_rolls_back():
    metadata = probes.metadata_for_evidence_type("MARKET_CALENDAR")
    baseline = _baseline(metadata)
    statement = probes.build_negative_probe_statement(
        metadata,
        baseline,
        probes.NegativeProbeOperation.INVALID_INSERT,
    )
    collision = deepcopy(baseline)
    collision[metadata.primary_column] = statement.candidate_primary_value
    collision[metadata.content_hash_column] = statement.candidate_primary_value
    engine = _IdentityBoundFakeEngine(((metadata, baseline),))
    engine.tables[metadata.table].append(collision)

    with pytest.raises(
        probes.EvidenceNegativeProbeContractError,
        match="collides with unique key",
    ):
        probes.run_invalid_insert_probes(engine, (_case(metadata, baseline),))

    assert not any(event[0] == "DML" for event in engine.events)
    assert ("ROLLBACK", False) in engine.events


@pytest.mark.parametrize(
    "cases",
    (
        (),
        (SimpleNamespace(evidence_type="MARKET_CALENDAR", primary_value="0" * 64),),
        (probes.EvidenceNegativeProbeCase("UNKNOWN", "0" * 64),),
        (probes.EvidenceNegativeProbeCase("MARKET_CALENDAR", "not-a-hash"),),
    ),
)
def test_case_validation_fails_before_any_checkout(cases):
    metadata = probes.metadata_for_evidence_type("MARKET_CALENDAR")
    baseline = _baseline(metadata)
    engine = _IdentityBoundFakeEngine(((metadata, baseline),))

    with pytest.raises(probes.EvidenceNegativeProbeContractError):
        probes.run_negative_probes(engine, cases)

    assert engine.connection_count == 0


def test_duplicate_cases_and_operations_are_rejected():
    metadata = probes.metadata_for_evidence_type("MARKET_CALENDAR")
    baseline = _baseline(metadata)
    engine = _IdentityBoundFakeEngine(((metadata, baseline),))
    case = _case(metadata, baseline)

    with pytest.raises(probes.EvidenceNegativeProbeContractError, match="duplicate"):
        probes.run_negative_probes(engine, (case, case))
    with pytest.raises(probes.EvidenceNegativeProbeContractError, match="duplicated"):
        probes.run_negative_probes(
            engine,
            (case,),
            operations=(
                probes.NegativeProbeOperation.REPLACE,
                probes.NegativeProbeOperation.REPLACE,
            ),
        )
    assert engine.connection_count == 0
