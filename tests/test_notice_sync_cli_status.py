from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from biz.notice import sync_notice_em


class _Client:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _write_complete_history_ledger(
    path,
    codes: list[str],
    *,
    created_at: datetime,
    completed_at: datetime,
):
    ledger = sync_notice_em._new_history_ledger(codes, now=created_at)
    entries = []
    for code in codes:
        row_hash = sync_notice_em._notice_row_hash([])
        entries.append(
            {
                "stock_code": code,
                "captured_at": completed_at.isoformat(
                    sep=" ", timespec="microseconds"
                ),
                "total_hits": 0,
                "page_count": 1,
                "written_count": 0,
                "deleted_count": 0,
                "persisted_count": 0,
                "source_row_hash": row_hash,
                "persisted_row_hash": row_hash,
            }
        )
    return sync_notice_em._atomic_write_history_ledger(
        path,
        {
            **ledger,
            "status": "COMPLETE",
            "updated_at": completed_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "completed_at": completed_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "next_offset": len(codes),
            "completed_code_count": len(codes),
            "completed_code_set_hash": sync_notice_em._code_set_hash(codes),
            "completed_entries": entries,
            "evidence_chain_sha256": sync_notice_em._history_entry_chain(entries),
            "last_failure": None,
        },
    )


def _install_cli_fakes(monkeypatch, *, codes: list[str], outcomes: dict[str, object]):
    engine = object()
    observed: dict[str, object] = {}
    monkeypatch.setattr(sync_notice_em, "create_batch_engine", lambda: engine)
    monkeypatch.setattr(sync_notice_em, "run_ddl", lambda value: observed.setdefault("ddl", value))

    def read_codes(value, offset, limit):
        observed["read"] = (value, offset, limit)
        return codes

    monkeypatch.setattr(sync_notice_em, "read_codes_from_db", read_codes)
    monkeypatch.setattr(
        sync_notice_em.httpx,
        "Client",
        lambda **_kwargs: _Client(),
    )

    def fetch(_client, stock_code, **_kwargs):
        observed.setdefault("fetch_kwargs", {})[stock_code] = dict(_kwargs)
        outcome = outcomes[stock_code]
        if isinstance(outcome, Exception):
            raise outcome
        rows = list(outcome)
        return sync_notice_em.NoticeFetchResult(
            rows=rows,
            captured_at=datetime(2026, 8, 26, 20, 15),
            window_start=_kwargs["begin_date"],
            exhausted=True,
            page_count=1,
            total_hits=len(rows),
            expected_pages=1 if rows else 0,
            window_end=_kwargs["end_date"],
            bounded=True,
        )

    monkeypatch.setattr(sync_notice_em, "fetch_pages", fetch)
    monkeypatch.setattr(
        sync_notice_em,
        "_parse_item",
        lambda stock_code, item, _captured_at, **_kwargs: {
            "stock_code": stock_code,
            "art_code": item["art_code"],
        },
    )
    monkeypatch.setattr(
        sync_notice_em,
        "reconcile_rows",
        lambda _engine, rows, **_kwargs: sync_notice_em.NoticePersistResult(
            written_count=len(rows),
            deleted_count=0,
            persisted_count=len(rows),
            persisted_row_hash=sync_notice_em._notice_row_hash(rows),
        ),
    )
    monkeypatch.setattr(sync_notice_em.time, "sleep", lambda _seconds: None)
    return observed


def test_full_market_limit_zero_is_preserved_and_complete_run_succeeds(
    monkeypatch, capsys
):
    observed = _install_cli_fakes(
        monkeypatch,
        codes=["000001", "000002"],
        outcomes={
            "000001": [{"art_code": "a"}],
            "000002": [],
        },
    )

    status = sync_notice_em.main(
        [
            "--from-si-all-code",
            "--limit",
            "0",
            "--sleep",
            "0",
            "--as-of-date",
            "2026-08-26",
            "--min-coverage",
            "1",
            "--min-row-coverage",
            "0.5",
        ]
    )

    assert status == 0
    assert observed["read"] == (observed["ddl"], 0, 0)
    receipts = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("{")
    ]
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt["schema"] == "probiga.notice-sync-result.v1"
    assert receipt["status"] == "PASS"
    assert receipt["requested_code_count"] == 2
    assert receipt["succeeded_code_count"] == 2
    assert receipt["nonempty_code_count"] == 1
    assert receipt["authoritative_empty_code_count"] == 1
    assert receipt["failed_code_count"] == 0
    assert receipt["pagination_exhausted_code_count"] == 2
    assert receipt["pagination_exhausted_code_set_hash"] == receipt[
        "requested_code_set_hash"
    ]
    assert receipt["pagination_evidence"] == (
        "eastmoney_exact_stock_total_hits_v1"
    )
    assert receipt["sync_mode"] == "incremental"
    assert receipt["request_window_start"] == "2026-07-12"
    assert receipt["request_window_end"] == "2026-08-27"
    assert len(receipt["batch_id"]) == 64
    assert receipt["association_validated"] == 1
    assert receipt["data_source"] == sync_notice_em.NOTICE_PROVIDER_ID
    assert receipt["data_version"] == sync_notice_em.NOTICE_DATA_VERSION
    assert receipt["quality_status"] == sync_notice_em.NOTICE_QUALITY_STATUS
    assert receipt["permission_status"] == "PUBLIC"
    expected_persisted_manifest = [
        {
            "stock_code": "000001",
            "row_count": 1,
            "row_hash": sync_notice_em._notice_row_hash(
                [{"stock_code": "000001", "art_code": "a"}]
            ),
        },
        {
            "stock_code": "000002",
            "row_count": 0,
            "row_hash": sync_notice_em._notice_row_hash([]),
        },
    ]
    assert receipt["persisted_manifest_sha256"] == sync_notice_em._sha256(
        expected_persisted_manifest
    )
    assert observed["fetch_kwargs"]["000001"]["begin_date"] == date(
        2026, 7, 12
    )
    assert observed["fetch_kwargs"]["000001"]["end_date"] == date(
        2026, 8, 27
    )
    assert receipt["written_notice_count"] == 1
    assert len(receipt["requested_code_set_hash"]) == 64


def test_partial_provider_failure_is_nonzero_below_coverage_gate(
    monkeypatch, capsys
):
    _install_cli_fakes(
        monkeypatch,
        codes=["000001", "000002"],
        outcomes={
            "000001": [{"art_code": "a"}],
            "000002": RuntimeError("provider failed"),
        },
    )

    status = sync_notice_em.main(
        [
            "--from-si-all-code",
            "--limit",
            "0",
            "--sleep",
            "0",
            "--min-coverage",
            "0.9",
            "--min-row-coverage",
            "0.5",
        ]
    )

    assert status == 1
    receipts = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("{")
    ]
    assert len(receipts) == 1
    receipt = receipts[0]
    assert receipt["status"] == "DATA_BLOCKED"
    assert receipt["succeeded_code_count"] == 1
    assert receipt["failed_code_count"] == 1
    assert receipt["failure_sample"] == [
        {"stock_code": "000002", "error_type": "RuntimeError"}
    ]


def test_empty_stock_universe_is_data_blocked(monkeypatch):
    _install_cli_fakes(monkeypatch, codes=[], outcomes={})

    assert sync_notice_em.main(["--from-si-all-code", "--limit", "0"]) == 2


@pytest.mark.parametrize(
    "option,value",
    (("--min-coverage", "1.1"), ("--min-row-coverage", "-0.1")),
)
def test_invalid_coverage_threshold_is_rejected(option, value):
    with pytest.raises(SystemExit) as exc:
        sync_notice_em.main(["--stock", "000001", option, value])

    assert exc.value.code == 2


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _PagingClient:
    def __init__(self, pages):
        self.pages = pages
        self.params = []

    def get(self, _url, *, params, timeout):
        assert timeout == 20.0
        self.params.append(dict(params))
        return _Response(self.pages[int(params["page_index"])])


def _source_notice(code: str, art_code: str, notice_date: str) -> dict:
    return {
        "art_code": art_code,
        "notice_date": notice_date,
        "codes": [{"stock_code": code}],
    }


def _page(page: int, size: int, total: int, rows: list[dict]) -> dict:
    return {
        "success": 1,
        "data": {
            "page_index": page,
            "page_size": size,
            "total_hits": total,
            "list": rows,
        },
    }


def test_fetch_pages_binds_stock_identity_and_exhausts_total_hits() -> None:
    client = _PagingClient({
        1: _page(
            1,
            2,
            3,
            [
                _source_notice("600519", "A3", "2026-08-26"),
                _source_notice("600519", "A2", "2026-08-25"),
            ],
        ),
        2: _page(
            2,
            2,
            3,
            [_source_notice("600519", "A1", "2026-08-24")],
        ),
    })

    result = sync_notice_em.fetch_pages(
        client,
        "600519",
        page_size=2,
        max_pages=2,
    )

    assert result.exhausted is True
    assert result.total_hits == 3
    assert result.expected_pages == 2
    assert [row["art_code"] for row in result.rows] == ["A3", "A2", "A1"]
    assert all(params["ann_type"] == "A" for params in client.params)
    assert all(params["f_node"] == 0 for params in client.params)
    assert all(params["s_node"] == 0 for params in client.params)
    assert all(params["stock_list"] == "600519" for params in client.params)


def test_fetch_pages_binds_and_enforces_exact_date_window() -> None:
    client = _PagingClient({
        1: _page(
            1,
            100,
            1,
            [_source_notice("600519", "A1", "2026-08-15")],
        )
    })

    result = sync_notice_em.fetch_pages(
        client,
        "600519",
        page_size=100,
        max_pages=2,
        begin_date=date(2026, 8, 1),
        end_date=date(2026, 8, 16),
    )

    assert result.bounded is True
    assert result.window_start == date(2026, 8, 1)
    assert result.window_end == date(2026, 8, 16)
    assert client.params == [
        {
            "sr": -1,
            "page_size": 100,
            "page_index": 1,
            "ann_type": "A",
            "client_source": "web",
            "f_node": 0,
            "s_node": 0,
            "stock_list": "600519",
            "begin_time": "2026-08-01",
            "end_time": "2026-08-16",
        }
    ]

    outside = _PagingClient({
        1: _page(
            1,
            100,
            1,
            [_source_notice("600519", "A0", "2026-07-31")],
        )
    })
    with pytest.raises(RuntimeError, match="outside the requested date scope"):
        sync_notice_em.fetch_pages(
            outside,
            "600519",
            page_size=100,
            max_pages=2,
            begin_date=date(2026, 8, 1),
            end_date=date(2026, 8, 16),
        )


def test_validated_parse_writes_complete_visible_provenance() -> None:
    batch_id = "a" * 64
    row = sync_notice_em._parse_item(
        "600519",
        {
            **_source_notice("600519", "AN-1", "2026-08-15"),
            "title": "贵州茅台公告",
            "display_time": "2026-08-14 20:41:29:380",
        },
        datetime(2026, 8, 14, 20, 42),
        validated_stock_identity=True,
        batch_id=batch_id,
    )

    assert row["association_validated"] == 1
    assert row["qmt_code"] == "600519.SH"
    assert row["data_source"] == sync_notice_em.NOTICE_PROVIDER_ID
    assert row["source_time"] == datetime(2026, 8, 14, 20, 41, 29)
    assert row["received_at"] == datetime(2026, 8, 14, 20, 42)
    assert row["batch_id"] == batch_id
    assert row["data_version"] == sync_notice_em.NOTICE_DATA_VERSION
    assert row["quality_status"] == sync_notice_em.NOTICE_QUALITY_STATUS
    assert row["permission_status"] == "PUBLIC"

    with pytest.raises(ValueError, match="stock identity differs"):
        sync_notice_em._parse_item(
            "600519",
            _source_notice("000001", "AN-2", "2026-08-15"),
            datetime(2026, 8, 15, 10),
            validated_stock_identity=True,
            batch_id=batch_id,
        )


def test_fetch_pages_rejects_wrong_stock_or_insufficient_ceiling() -> None:
    wrong_stock = _PagingClient({
        1: _page(
            1,
            2,
            1,
            [_source_notice("000001", "A1", "2026-08-26")],
        )
    })
    with pytest.raises(RuntimeError, match="stock identity differs"):
        sync_notice_em.fetch_pages(
            wrong_stock,
            "600519",
            page_size=2,
            max_pages=2,
        )

    ceiling = _PagingClient({
        1: _page(
            1,
            2,
            5,
            [
                _source_notice("600519", "A2", "2026-08-26"),
                _source_notice("600519", "A1", "2026-08-25"),
            ],
        )
    })
    with pytest.raises(RuntimeError, match="ceiling is insufficient"):
        sync_notice_em.fetch_pages(
            ceiling,
            "600519",
            page_size=2,
            max_pages=2,
        )


class _RowsResult:
    def __init__(self, rows=None, *, rowcount=0):
        self._rows = list(rows or [])
        self.rowcount = rowcount

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _ReconcileConnection:
    def __init__(self, existing_rows):
        self.rows = list(existing_rows)
        self.statements = []

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        self.statements.append((sql, params))
        if sql.startswith("DELETE FROM si_notice_eastmoney"):
            deleted = len(self.rows)
            self.rows = []
            return _RowsResult(rowcount=deleted)
        if sql.startswith("INSERT INTO si_notice_eastmoney"):
            self.rows = [dict(row) for row in params]
            return _RowsResult(rowcount=len(self.rows))
        if sql.startswith("SELECT stock_code, art_code"):
            return _RowsResult(self.rows)
        raise AssertionError(sql)


class _ReconcileEngine:
    def __init__(self, rows):
        self.connection = _ReconcileConnection(rows)

    def begin(self):
        connection = self.connection

        class _Context:
            def __enter__(self):
                return connection

            def __exit__(self, *_args):
                return False

        return _Context()


def test_reconcile_atomically_replaces_only_validated_rows_and_reads_them_back():
    batch_id = "b" * 64
    captured = datetime(2026, 8, 26, 20, 15)
    row = sync_notice_em._parse_item(
        "000001",
        {
            **_source_notice("000001", "NEW", "2026-08-26"),
            "title": "新公告",
            "display_time": "2026-08-26 20:00:00",
        },
        captured,
        validated_stock_identity=True,
        batch_id=batch_id,
    )
    engine = _ReconcileEngine([{"stock_code": "000001", "art_code": "OLD"}])

    result = sync_notice_em.reconcile_rows(
        engine,
        [row],
        stock_code="000001",
        captured_at=captured,
        window_start=date(2026, 8, 1),
        window_end=date(2026, 8, 27),
    )

    assert result.written_count == 1
    assert result.deleted_count == 1
    assert result.persisted_count == 1
    assert result.persisted_row_hash == sync_notice_em._notice_row_hash([row])
    insert_sql = engine.connection.statements[1][0]
    assert "association_validated" in insert_sql
    assert "permission_status" in insert_sql
    readback_sql = engine.connection.statements[2][0]
    assert "association_validated=1" in readback_sql


def _notice_contract_table(engine, *, unique: bool) -> None:
    unique_sql = ", UNIQUE(stock_code, art_code)" if unique else ""
    with engine.begin() as connection:
        connection.execute(text(f"""
            CREATE TABLE si_notice_eastmoney (
                stock_code TEXT NOT NULL, art_code TEXT NOT NULL,
                notice_date DATE, title TEXT, column_name TEXT,
                display_time TEXT, detail_url TEXT,
                association_validated INTEGER NOT NULL DEFAULT 0,
                etl_sync_at DATETIME NOT NULL, qmt_code TEXT,
                data_source TEXT, source_time DATETIME,
                received_at DATETIME, batch_id TEXT, data_version TEXT,
                quality_status TEXT, permission_status TEXT
                {unique_sql}
            )
        """))


def test_prepared_schema_guard_requires_formal_columns_and_composite_unique_key():
    passing = create_engine("sqlite+pysqlite:///:memory:")
    _notice_contract_table(passing, unique=True)
    sync_notice_em.run_ddl(passing)

    missing_unique = create_engine("sqlite+pysqlite:///:memory:")
    _notice_contract_table(missing_unique, unique=False)
    with pytest.raises(RuntimeError, match="unique key"):
        sync_notice_em.run_ddl(missing_unique)


def test_history_repair_universe_includes_legacy_notice_associations():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_all_code(stock_code TEXT)"))
        connection.execute(
            text("CREATE TABLE si_notice_eastmoney(stock_code TEXT)")
        )
        connection.execute(
            text("INSERT INTO si_all_code VALUES ('000001'),('000002')")
        )
        connection.execute(
            text(
                "INSERT INTO si_notice_eastmoney VALUES "
                "('000002'),('600999')"
            )
        )

    assert sync_notice_em.read_history_repair_codes(engine) == [
        "000001",
        "000002",
        "600999",
    ]


def test_historical_repair_is_sharded_resumable_and_whole_batch_ledgered(
    monkeypatch, tmp_path, capsys
):
    monkeypatch.delenv("PROBIGA_JOB_LOG_ROOT", raising=False)
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    codes = ["000001", "000002"]
    fetch_count = 0
    monkeypatch.setattr(
        sync_notice_em.httpx,
        "Client",
        lambda **_kwargs: _Client(),
    )

    def fetch(_client, code, **kwargs):
        nonlocal fetch_count
        fetch_count += 1
        assert "begin_date" not in kwargs
        return sync_notice_em.NoticeFetchResult(
            rows=[{"art_code": f"AN-{code}"}],
            captured_at=datetime(2026, 8, 26, 20, 15),
            window_start=date(1900, 1, 1),
            exhausted=True,
            page_count=1,
            total_hits=1,
            expected_pages=1,
        )

    monkeypatch.setattr(sync_notice_em, "fetch_pages", fetch)
    monkeypatch.setattr(
        sync_notice_em,
        "_parse_item",
        lambda code, item, _captured, **_kwargs: {
            "stock_code": code,
            "art_code": item["art_code"],
        },
    )
    monkeypatch.setattr(
        sync_notice_em,
        "reconcile_rows",
        lambda _engine, rows, **_kwargs: sync_notice_em.NoticePersistResult(
            written_count=len(rows),
            deleted_count=1,
            persisted_count=len(rows),
            persisted_row_hash=sync_notice_em._notice_row_hash(rows),
        ),
    )
    monkeypatch.setattr(sync_notice_em.time, "sleep", lambda _seconds: None)
    ledger_path = tmp_path / "notice-history-ledger.json"

    first = sync_notice_em._run_history_repair(
        engine=object(),
        codes=codes,
        started_at=datetime(2026, 8, 26, 20),
        ledger_path=ledger_path,
        shard_size=1,
        page_size=100,
        max_pages=1000,
        sleep_seconds=0,
    )
    second = sync_notice_em._run_history_repair(
        engine=object(),
        codes=codes,
        started_at=datetime(2026, 8, 26, 20, 1),
        ledger_path=ledger_path,
        shard_size=1,
        page_size=100,
        max_pages=1000,
        sleep_seconds=0,
    )
    third = sync_notice_em._run_history_repair(
        engine=object(),
        codes=codes,
        started_at=datetime(2026, 8, 26, 20, 2),
        ledger_path=ledger_path,
        shard_size=1,
        page_size=100,
        max_pages=1000,
        sleep_seconds=0,
    )

    assert first == 2
    assert second == third == 0
    assert fetch_count == 2
    receipts = [
        json.loads(line)
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("{")
    ]
    assert [receipt["status"] for receipt in receipts] == [
        "PROGRESS",
        "PASS",
        "PASS",
    ]
    assert [receipt["retryable"] for receipt in receipts] == [True, False, False]
    assert all(
        receipt["task_type"]
        == sync_notice_em.HISTORY_TASK_TYPE
        and receipt["dataset"] == sync_notice_em.HISTORY_DATASET
        and receipt["executor_owner"] == "linux_provider"
        and receipt["provider"] == sync_notice_em.NOTICE_PROVIDER_ID
        for receipt in receipts
    )
    assert receipts[0]["remaining_code_count"] == 1
    assert receipts[1]["remaining_code_count"] == 0
    assert receipts[2]["processed_code_count_this_run"] == 0
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    assert ledger["status"] == "COMPLETE"
    assert ledger["completed_code_count"] == 2
    assert ledger["requested_codes"] == codes
    assert [entry["stock_code"] for entry in ledger["completed_entries"]] == codes
    assert len(ledger["ledger_sha256"]) == 64
    assert len(ledger["evidence_chain_sha256"]) == 64
    frozen = sync_notice_em._load_or_create_history_ledger(
        ledger_path,
        codes=None,
        now=datetime(2026, 8, 26, 20, 3),
    )
    assert frozen["requested_codes"] == codes

    ledger["next_offset"] = 1
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum differs"):
        sync_notice_em._load_or_create_history_ledger(
            ledger_path,
            codes=codes,
            now=datetime(2026, 8, 26, 20, 2),
        )


def test_history_generation_reuses_verified_parent_and_fetches_only_new_code(
    monkeypatch, tmp_path, capsys
) -> None:
    monkeypatch.delenv("PROBIGA_JOB_LOG_ROOT", raising=False)
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    base_path = tmp_path / "notice-history-ledger.json"
    parent = _write_complete_history_ledger(
        base_path,
        ["000001", "000002"],
        created_at=datetime(2026, 8, 26, 20),
        completed_at=datetime(2026, 8, 26, 20, 1),
    )
    frozen_bytes = base_path.read_bytes()
    generation_path, generation = (
        sync_notice_em._select_or_create_history_generation(
            base_path,
            current_codes=["000000", "000001", "000002"],
            now=datetime(2026, 8, 26, 20, 2),
        )
    )
    assert generation_path != base_path
    assert generation["generation"] == 2
    assert generation["parent_ledger_sha256"] == parent["ledger_sha256"]
    assert generation["requested_codes"] == ["000001", "000002", "000000"]
    assert generation["next_offset"] == generation["inherited_entry_count"] == 2

    fetched: list[str] = []
    monkeypatch.setattr(
        sync_notice_em.httpx,
        "Client",
        lambda **_kwargs: _Client(),
    )

    def fetch(_client, code, **_kwargs):
        fetched.append(code)
        return sync_notice_em.NoticeFetchResult(
            rows=[{"art_code": f"AN-{code}"}],
            captured_at=datetime(2026, 8, 26, 20, 3),
            window_start=date(1900, 1, 1),
            exhausted=True,
            page_count=1,
            total_hits=1,
            expected_pages=1,
        )

    monkeypatch.setattr(sync_notice_em, "fetch_pages", fetch)
    monkeypatch.setattr(
        sync_notice_em,
        "_parse_item",
        lambda code, item, _captured, **_kwargs: {
            "stock_code": code,
            "art_code": item["art_code"],
        },
    )
    monkeypatch.setattr(
        sync_notice_em,
        "reconcile_rows",
        lambda _engine, rows, **_kwargs: sync_notice_em.NoticePersistResult(
            written_count=len(rows),
            deleted_count=0,
            persisted_count=len(rows),
            persisted_row_hash=sync_notice_em._notice_row_hash(rows),
        ),
    )

    assert sync_notice_em._run_history_repair(
        engine=object(),
        codes=generation["requested_codes"],
        started_at=datetime(2026, 8, 26, 20, 2),
        ledger_path=generation_path,
        shard_size=250,
        page_size=100,
        max_pages=1000,
        sleep_seconds=0,
    ) == 0
    assert fetched == ["000000"]
    assert base_path.read_bytes() == frozen_bytes
    completed = sync_notice_em._read_history_ledger(
        generation_path,
        codes=generation["requested_codes"],
    )
    assert completed["status"] == "COMPLETE"
    assert completed["completed_code_count"] == 3
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["status"] == "PASS"

    with pytest.raises(ValueError, match="COMPLETE ledger is immutable"):
        sync_notice_em._atomic_write_history_ledger(
            base_path,
            {**parent, "last_failure": {"error_type": "tamper"}},
        )


def test_history_generation_creation_is_concurrent_idempotent(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.delenv("PROBIGA_JOB_LOG_ROOT", raising=False)
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    base_path = tmp_path / "notice-history-ledger.json"
    _write_complete_history_ledger(
        base_path,
        ["000001"],
        created_at=datetime(2026, 8, 26, 20),
        completed_at=datetime(2026, 8, 26, 20, 1),
    )
    frozen_bytes = base_path.read_bytes()

    def select_generation(_index: int):
        path, ledger = sync_notice_em._select_or_create_history_generation(
            base_path,
            current_codes=["000001", "000002"],
            now=datetime(2026, 8, 26, 20, 2),
        )
        return path, ledger["ledger_sha256"]

    with ThreadPoolExecutor(max_workers=2) as executor:
        selected = list(executor.map(select_generation, range(2)))
    assert selected[0] == selected[1]
    assert base_path.read_bytes() == frozen_bytes
    assert len(sync_notice_em._load_history_generations(base_path)) == 2


def test_tampered_child_generation_blocks_the_whole_chain(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.delenv("PROBIGA_JOB_LOG_ROOT", raising=False)
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    base_path = tmp_path / "notice-history-ledger.json"
    _write_complete_history_ledger(
        base_path,
        ["000001"],
        created_at=datetime(2026, 8, 26, 20),
        completed_at=datetime(2026, 8, 26, 20, 1),
    )
    generation_path, _ledger = (
        sync_notice_em._select_or_create_history_generation(
            base_path,
            current_codes=["000001", "000002"],
            now=datetime(2026, 8, 26, 20, 2),
        )
    )
    raw = json.loads(generation_path.read_text(encoding="utf-8"))
    raw["next_offset"] = 0
    generation_path.write_text(json.dumps(raw), encoding="utf-8")

    with pytest.raises(ValueError, match="checksum differs"):
        sync_notice_em._load_history_generations(base_path)


def test_history_machine_failures_distinguish_retryable_source_and_terminal_ledger():
    started = datetime(2026, 8, 26, 20)
    transient = sync_notice_em._history_failure_result(
        started_at=started,
        finished_at=datetime(2026, 8, 26, 20, 1),
        error=RuntimeError("Eastmoney request failed after 3 attempts"),
        codes=["000001"],
    )
    terminal = sync_notice_em._history_failure_result(
        started_at=started,
        finished_at=datetime(2026, 8, 26, 20, 1),
        error=ValueError("history ledger checksum differs"),
        codes=["000001"],
    )

    assert transient["status"] == terminal["status"] == "DATA_BLOCKED"
    assert transient["retryable"] is True
    assert terminal["retryable"] is False
    for receipt in (transient, terminal):
        supplied = receipt["result_sha256"]
        assert supplied == sync_notice_em._sha256(
            {key: value for key, value in receipt.items() if key != "result_sha256"}
        )


def test_history_ledger_must_stay_inside_configured_protected_root(
    monkeypatch, tmp_path
):
    protected = tmp_path / "jobs"
    protected.mkdir()
    if sync_notice_em.os.name != "nt":
        sync_notice_em.os.chmod(protected, 0o700)
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(protected))
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")

    inside_path = protected / "notice-history.json"
    ledger = sync_notice_em._load_or_create_history_ledger(
        inside_path,
        codes=["000001"],
        now=datetime(2026, 8, 26, 20),
    )
    assert ledger["requested_codes"] == ["000001"]
    with pytest.raises(ValueError, match="directly inside the protected root"):
        sync_notice_em._load_or_create_history_ledger(
            outside / "notice-history.json",
            codes=["000001"],
            now=datetime(2026, 8, 26, 20),
        )
