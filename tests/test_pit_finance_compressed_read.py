from contextlib import nullcontext
from datetime import date, datetime
from types import SimpleNamespace
import zlib

import pytest
from sqlalchemy import event, text

from server.common import pit_facts as pit
from tests.test_pit_finance_event_facts import (
    _engine, _finance_revision_chunk_case, _nonfiling_evidence,
)


def _mysql_transport(engine, *, corrupt_code=None, fail_code=None, invalid_reply=None):
    with engine.connect() as connection:
        driver = connection.connection.driver_connection
        code_by_id = dict(connection.execute(text(
            f"SELECT revision_id,stock_code FROM {pit.FINANCE_REVISION_TABLE}"
        )).all())
        driver.create_function("OCTET_LENGTH", 1, lambda raw: len(raw.encode("utf-8")))

        def compress(raw):
            raw = raw.encode("utf-8")
            packed = zlib.compress(raw)
            return len(raw).to_bytes(4, "little") + packed + (b"." if packed[-1] == 32 else b"")

        driver.create_function("COMPRESS", 1, compress)

    class MysqlConnection:
        dialect = SimpleNamespace(name="mysql")

        def __init__(self):
            self.connection = engine.connect()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.connection.__exit__(*args)

        def connect(self):
            return nullcontext(self)

        def get_isolation_level(self):
            return "REPEATABLE READ"

        def begin(self):
            return self.connection.begin()

        def execution_options(self, *, isolation_level):
            assert isolation_level == "REPEATABLE READ"
            return self

        def execute(self, statement, parameters):
            sql = statement.text
            original_sql = sql
            if pit.FINANCE_REVISION_TABLE in sql and "COMPRESS(payload_json)" in sql:
                codes = [code_by_id[key] for key in parameters["row_ids"]]
                if fail_code in codes:
                    from sqlalchemy.exc import OperationalError
                    raise OperationalError("finance revision transport", {}, Exception(2013, "lost connection"))
                if corrupt_code in codes:
                    sql = sql.replace(
                        "COMPRESS(payload_json) AS payload_json_compressed",
                        "SUBSTR(COMPRESS(payload_json),1,LENGTH(COMPRESS(payload_json))-1) AS payload_json_compressed",
                    )
            sql = sql.replace(" FORCE INDEX (idx_pit_coverage_decision)", "")
            sql = sql.replace(" FORCE INDEX (PRIMARY)", "")
            if sql != original_sql:
                options = statement.get_execution_options()
                statement = text(sql).bindparams(*statement._bindparams.values()).execution_options(**options)
            result = self.connection.execute(statement, parameters)
            phase = "payload" if "COMPRESS(payload_json)" in sql else "size"
            if invalid_reply and "row_ids" in parameters and phase == invalid_reply[0]:
                with result:
                    rows = [dict(row) for row in result.mappings()]
                if invalid_reply[1] == "missing":
                    rows.pop()
                elif invalid_reply[1] == "duplicate":
                    rows[-1] = dict(rows[0])

                class Reply:
                    def __enter__(self):
                        return self

                    def __exit__(self, *args):
                        return False

                    def mappings(self):
                        return iter(rows)

                return Reply()
            return result

    return SimpleNamespace(connect=MysqlConnection)


def test_finance_revision_compression_preserves_every_field_and_complete_chain():
    engine, kwargs = _finance_revision_chunk_case()
    query = dict(
        table_name=pit.FINANCE_REVISION_TABLE, codes=sorted(kwargs["codes"]),
        decision_at=datetime.fromisoformat(kwargs["decision_at"]),
        start_date=None, end_date=date.fromisoformat(kwargs["as_of_date"]),
    )
    expected = pit._query_revisions(engine, **query)
    assert len(expected) == 6
    assert sorted(row["revision_no"] for row in expected) == [1, 1, 1, 1, 2, 2]
    actual = pit._query_revisions(_mysql_transport(engine), **query)
    assert actual == expected
    assert all(set(row) == pit._REQUIRED_COLUMNS[pit.FINANCE_REVISION_TABLE] for row in actual)
    engine.dispose()


def test_finance_revision_mysql_batches_keep_rr_and_exact_manifest(monkeypatch):
    engine, kwargs = _finance_revision_chunk_case()
    pit.append_finance_expected_unavailable(
        engine, stock_code="002731", expected_report_date="2026-03-31",
        known_at="2026-08-30 01:06:35",
        official_evidence=_nonfiling_evidence(), batch_id="nonfiling-transport",
    )
    codes = [f"{index:06d}" for index in range(1, 42)] + ["002731"]
    kwargs = {**kwargs, "codes": list(reversed(codes)) + ["000001", ""],
              "decision_at": "2026-08-30 01:08:00", "fact_cutoff_at": "2026-08-30 01:07:00",
              "as_of_date": "2026-08-30"}
    expected = pit.load_finance_facts(engine, **kwargs)
    queries = []
    payload_queries = []

    def record(connection, _cursor, sql, _params, context, _many):
        if f"FROM {pit.FINANCE_REVISION_TABLE} WHERE stock_code IN" in sql:
            params = context.compiled_parameters[0]
            chunk = [params[key] for key in params if key.startswith("codes_")]
            queries.append((id(connection), connection.get_transaction(), sql, chunk,
                            context.execution_options.get("stream_results")))
        elif f"FROM {pit.FINANCE_REVISION_TABLE}" in sql and "COMPRESS(payload_json)" in sql:
            params = context.compiled_parameters[0]
            keys = [params[key] for key in params if key.startswith("row_ids_")]
            payload_queries.append((sql, keys, context.execution_options.get("stream_results")))

    event.listen(engine, "before_cursor_execute", record)
    try:
        progress = []
        actual = pit.load_finance_facts(
            _mysql_transport(engine), **kwargs, read_progress=lambda done, total: progress.append((done, total)),
        )
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert actual == expected
    assert [chunk for _, _, _, chunk, _ in queries] == [codes[:40], codes[40:]]
    assert len({identity for identity, _, _, _, _ in queries}) == 1
    assert len({id(transaction) for _, transaction, _, _, _ in queries}) == 1
    assert all(transaction is not None and streaming is True for _, transaction, _, _, streaming in queries)
    assert all(sql.split(" FROM ", 1)[0].rsplit("*/", 1)[-1].strip() == "revision_id"
               for _, _, sql, _, _ in queries)
    assert all("COMPRESS(payload_json)" in sql and "OCTET_LENGTH(payload_json)" in sql
               and len(keys) <= 480 and streaming is True for sql, keys, streaming in payload_queries)
    assert all(all(column in sql.split(" FROM ", 1)[0]
                   for column in pit._REQUIRED_COLUMNS[pit.FINANCE_REVISION_TABLE] - {"payload_json"})
               for sql, _, _ in payload_queries)
    assert progress == [(40, 42), (42, 42)]
    assert pit.COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT == 500
    engine.dispose()


def test_finance_id_inventory_restores_full_rows_across_480_primary_row_boundary():
    engine, kwargs = _finance_revision_chunk_case()
    for index in range(481):
        pit.append_finance_revision(
            engine, {"stock_code": "000001", "report_date": f"{1900 + index // 4}-{('03-31', '06-30', '09-30', '12-31')[index % 4]}",
                     "report_type": f"Q{index % 4 + 1}", "notice_date": "2026-08-20 18:30:00", "roe_wtd": index},
            known_at="2026-08-20 18:31:00", batch_id=f"finance-transport-{index}",
        )
    query = dict(table_name=pit.FINANCE_REVISION_TABLE, codes=sorted(kwargs["codes"]),
                 decision_at=datetime.fromisoformat(kwargs["decision_at"]),
                 start_date=None, end_date=date.fromisoformat(kwargs["as_of_date"]))
    expected = pit._query_revisions(engine, **query)
    counts = []

    def record(_connection, _cursor, sql, _params, context, _many):
        if f"FROM {pit.FINANCE_REVISION_TABLE}" in sql and "COMPRESS(payload_json)" in sql:
            counts.append(sum(key.startswith("row_ids_") for key in context.compiled_parameters[0]))

    event.listen(engine, "before_cursor_execute", record)
    try:
        actual = pit._query_revisions(_mysql_transport(engine), **query)
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert len(actual) == 487
    assert counts == [480, 7]
    assert actual == expected
    engine.dispose()


@pytest.mark.parametrize("failure", ["truncated", "transport"])
def test_finance_revision_later_compressed_failure_never_returns_partial_batch(failure):
    engine, kwargs = _finance_revision_chunk_case()
    kwargs = {**kwargs, "codes": [f"{index:06d}" for index in range(1, 42)]}
    # The second chunk must carry a real payload for a truncated-frame failure.
    pit.append_finance_revision(
        engine, {"stock_code": "000041", "report_date": "2026-06-30",
                 "report_type": "Q2", "notice_date": "2026-08-20 18:30:00", "roe_wtd": 12.5},
        known_at="2026-08-20 18:31:00", batch_id="later-finance-transport",
    )
    reader = _mysql_transport(
        engine, **({"corrupt_code": "000041"} if failure == "truncated" else {"fail_code": "000041"}),
    )
    result = pit.load_finance_facts(reader, **kwargs)
    assert result.facts == {}
    assert set(result.status_by_code.values()) == {pit.PIT_SCHEMA_UNAVAILABLE}
    assert all(reason.startswith("PIT_FINANCE_SCHEMA_UNAVAILABLE:") for reason in result.reason_by_code.values())
    engine.dispose()


def test_finance_compressed_read_still_checks_old_report_payload():
    engine, kwargs = _finance_revision_chunk_case()
    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_finance_revision_immutable_bu"))
        connection.execute(text(
            f"UPDATE {pit.FINANCE_REVISION_TABLE} SET payload_json='{{}}' "
            "WHERE stock_code='000002' AND report_date='2026-03-31' AND revision_no=1"
        ))
    result = pit.load_finance_facts(_mysql_transport(engine), **kwargs)
    assert result.status_by_code["000001"] == pit.PIT_AVAILABLE
    assert result.status_by_code["000002"] == pit.PIT_DATA_BLOCKED
    assert result.reason_by_code["000002"].startswith("PIT_FINANCE_BAD_CHAIN:")
    engine.dispose()


@pytest.mark.parametrize("phase", ["size", "payload"])
@pytest.mark.parametrize("failure", ["missing", "duplicate"])
def test_finance_primary_reply_membership_failure_never_returns_partial_facts(phase, failure):
    engine, kwargs = _finance_revision_chunk_case()
    result = pit.load_finance_facts(_mysql_transport(engine, invalid_reply=(phase, failure)), **kwargs)
    assert result.facts == {}
    assert set(result.status_by_code.values()) == {pit.PIT_SCHEMA_UNAVAILABLE}
    assert all(reason.startswith("PIT_FINANCE_SCHEMA_UNAVAILABLE:ValueError")
               for reason in result.reason_by_code.values())
    engine.dispose()


@pytest.mark.parametrize("reader", ["empty-coverage", "nonfiling"])
@pytest.mark.parametrize("phase", ["size", "payload"])
@pytest.mark.parametrize("failure", ["missing", "duplicate"])
def test_finance_coverage_primary_reply_membership_failure_is_not_valid_empty(reader, phase, failure):
    engine = _engine()
    for code in ("000001", "002731"):
        for day in (30, 31):
            if reader == "nonfiling":
                pit.append_finance_expected_unavailable(
                    engine, stock_code=code, expected_report_date="2026-03-31",
                    known_at=f"2026-08-{day} 09:00:00",
                    official_evidence=_nonfiling_evidence(stock_code=code),
                    batch_id=f"nonfiling-{code}-{day}",
                )
            else:
                known = f"2026-08-{day} 09:00:00"
                pit.append_source_coverage(
                    engine, fact_kind="finance", stock_code=code,
                    window_start="1900-01-01", window_end="2026-08-31",
                    known_at=known, covered_through_at=known, watermark_kind="CAPTURED_AT",
                    watermark_evidence={"source_call": "success"}, source_rows=[], fact_bindings=[],
                    source="test.finance", batch_id=f"empty-{code}-{day}",
                )
    mysql = _mysql_transport(engine, invalid_reply=(phase, failure))
    with pytest.raises(ValueError) as caught:
        if reader == "nonfiling":
            pit.load_finance_expected_unavailable(
                mysql, codes=["000001", "002731"], decision_at="2026-08-31 10:00:00",
                expected_report_date="2026-03-31",
            )
        else:
            pit._authoritative_empty_coverage(
                mysql, fact_kind="finance", codes=["000001", "002731"],
                fact_cutoff_at=datetime(2026, 8, 31, 9), decision_at=datetime(2026, 8, 31, 10),
                start_date=date(1900, 1, 1), end_date=date(2026, 8, 31),
            )
    assert getattr(caught.value, "_pit_read_stage") == f"finance-{reader}-{phase}:0"
    engine.dispose()
