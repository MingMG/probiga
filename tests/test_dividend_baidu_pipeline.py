from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biz.stock_market import sync_dividend_baidu as dividend


NOW = datetime(2026, 8, 26, 21, 0)


class _Client:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame

    def get_dividend(self, *, stock_code: str) -> pd.DataFrame:
        return self.frame.copy()


def _row(code: str, report_date: str = "2026-06-01") -> dict[str, object]:
    return {
        "stock_code": code,
        "report_date": report_date,
        "dividend_plan": "10派1.00元",
        "ex_dividend_date": "2026-06-10",
        "etl_sync_at": NOW,
    }


def _collection(
    *,
    requested: tuple[str, ...] = ("000001", "000002"),
    rows: tuple[dict[str, object], ...] | None = None,
    nonempty: tuple[str, ...] = ("000001",),
    empty: tuple[str, ...] = ("000002",),
    failures: tuple[dict[str, str], ...] = (),
) -> dividend.DividendCollection:
    return dividend.DividendCollection(
        requested_codes=requested,
        responded_codes=tuple(sorted(set(nonempty) | set(empty))),
        nonempty_codes=nonempty,
        empty_codes=empty,
        failures=failures,
        rows=rows if rows is not None else (_row("000001"),),
        response_status_manifest_hash="f" * 64,
    )


def _dividend_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE sm_dividend (
                stock_code TEXT NOT NULL,
                report_date DATE NOT NULL,
                dividend_plan TEXT NOT NULL,
                ex_dividend_date DATE,
                etl_sync_at DATETIME NOT NULL,
                UNIQUE(stock_code,report_date)
            )
            """
        )
    return engine


def test_adata_dividend_nonempty_response_is_identity_and_date_validated() -> None:
    frame = pd.DataFrame([_row("000001")]).drop(columns=["etl_sync_at"])
    provider = dividend.AdataBaiduDividendProvider(
        client_factory=lambda: _Client(frame),
        empty_probe=lambda code: pytest.fail(f"unexpected empty probe {code}"),
    )
    result = provider.fetch("000001", as_of="2026-08-26", observed_at=NOW)
    assert result.status == "NONEMPTY"
    assert result.stock_code == "000001"
    assert result.rows[0]["report_date"] == "2026-06-01"
    assert result.rows[0]["etl_sync_at"] == NOW


def test_empty_dividend_requires_explicit_authoritative_probe() -> None:
    empty = pd.DataFrame(columns=["report_date", "dividend_plan", "ex_dividend_date"])
    provider = dividend.AdataBaiduDividendProvider(
        client_factory=lambda: _Client(empty),
        empty_probe=lambda code: "baidu_result_code_0_bonus_body_empty",
    )
    result = provider.fetch("000002", as_of="2026-08-26", observed_at=NOW)
    assert result.status == "AUTHORITATIVE_EMPTY"
    assert result.rows == ()

    blocked = dividend.AdataBaiduDividendProvider(
        client_factory=lambda: _Client(empty),
        empty_probe=lambda code: "ambiguous_empty",
    )
    with pytest.raises(RuntimeError, match="not authoritative"):
        blocked.fetch("000002", as_of="2026-08-26", observed_at=NOW)


def test_authoritative_empty_payload_rejects_provider_errors_and_hidden_rows() -> None:
    assert dividend.authoritative_empty_reason(
        {"ResultCode": "0", "Result": []}, code="000002"
    ) == "baidu_result_code_0_empty_result"
    with pytest.raises(RuntimeError, match="not authoritative"):
        dividend.authoritative_empty_reason(
            {"ResultCode": "100", "Result": []}, code="000002"
        )
    payload = {
        "ResultCode": "0",
        "Result": [
            {
                "DisplayData": {
                    "resultData": {
                        "tplData": {
                            "result": {
                                "tabs": [
                                    {
                                        "content": {
                                            "newCompany": {
                                            "bonusTransfer": {
                                                "body": [
                                                    [
                                                        "2026-06-01",
                                                        "10派1.00元",
                                                        "2026-06-10",
                                                        "",
                                                    ]
                                                ]
                                            }
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        ],
    }
    with pytest.raises(RuntimeError, match="contains dividend rows"):
        dividend.authoritative_empty_reason(payload, code="000002")
    payload["Result"][0]["DisplayData"]["resultData"]["tplData"]["result"][
        "tabs"
    ][0]["content"]["newCompany"]["bonusTransfer"]["body"] = [
        ["2026-06-01", "利润不分配", "--", ""]
    ]
    assert dividend.authoritative_empty_reason(
        payload, code="000002"
    ) == "baidu_result_code_0_all_non_distribution_plans"


def test_collection_attempts_every_code_and_blocks_any_failure() -> None:
    attempted: list[str] = []

    class Provider:
        def fetch(self, code: str, **kwargs):
            attempted.append(code)
            if code == "000002":
                raise ConnectionError("network unavailable")
            return dividend.DividendFetchResult(
                stock_code=code,
                status="NONEMPTY",
                rows=(_row(code),),
                evidence="rows",
            )

    collection = dividend.collect_snapshot(
        ("000001", "000002", "000003"),
        provider=Provider(),
        as_of="2026-08-26",
        observed_at=NOW,
        workers=2,
        sleep_seconds=0,
    )
    assert sorted(attempted) == ["000001", "000002", "000003"]
    assert len(collection.failures) == 1
    with pytest.raises(RuntimeError, match="failures block publication"):
        dividend.validate_collection(
            collection, min_nonempty_code_ratio=0.2
        )


def test_collection_requires_reasonable_nonempty_evidence() -> None:
    collection = _collection(
        requested=("000001", "000002", "000003", "000004", "000005"),
        nonempty=("000001",),
        empty=("000002", "000003", "000004", "000005"),
    )
    with pytest.raises(RuntimeError, match="nonempty evidence is unreasonable"):
        dividend.validate_collection(
            collection, min_nonempty_code_ratio=0.21
        )
    evidence = dividend.validate_collection(
        collection, min_nonempty_code_ratio=0.2
    )
    assert evidence["requested_code_count"] == 5
    assert evidence["responded_code_count"] == 5
    assert evidence["failure_count"] == 0
    assert evidence["authoritative_empty_code_count"] == 4


def test_dividend_snapshot_replaces_requested_scope_atomically() -> None:
    engine = _dividend_engine()
    collection = _collection()
    evidence = dividend.validate_collection(
        collection, min_nonempty_code_ratio=0.2
    )
    proof = dividend.replace_snapshot(
        engine, collection=collection, evidence=evidence
    )
    assert proof["row_count"] == 1
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT stock_code,report_date,dividend_plan "
                "FROM sm_dividend ORDER BY stock_code"
            )
        ).all()
    assert rows == [("000001", "2026-06-01", "10派1.00元")]


def test_dividend_insert_failure_rolls_back_all_scope_deletes() -> None:
    engine = _dividend_engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO sm_dividend VALUES "
                "('000001','2025-06-01','old','2025-06-10',:now)"
            ),
            {"now": NOW},
        )
        connection.exec_driver_sql(
            """
            CREATE TRIGGER fail_dividend_insert BEFORE INSERT ON sm_dividend
            WHEN NEW.stock_code='000001'
            BEGIN SELECT RAISE(ABORT, 'forced dividend failure'); END
            """
        )
    collection = _collection()
    evidence = dividend.validate_collection(
        collection, min_nonempty_code_ratio=0.2
    )
    with pytest.raises(Exception, match="forced dividend failure"):
        dividend.replace_snapshot(
            engine, collection=collection, evidence=evidence
        )
    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT stock_code,report_date,dividend_plan FROM sm_dividend")
        ).all()
    assert rows == [("000001", "2025-06-01", "old")]


def test_dividend_formal_path_has_no_runtime_ddl_or_table_replace() -> None:
    source = Path("biz/stock_market/sync_dividend_baidu.py").read_text(
        encoding="utf-8"
    ).upper()
    assert "CREATE TABLE" not in source
    assert "ALTER TABLE" not in source
    assert "DROP TABLE" not in source
    assert "TO_SQL" not in source
    assert "IF_EXISTS=\"REPLACE\"" not in source
