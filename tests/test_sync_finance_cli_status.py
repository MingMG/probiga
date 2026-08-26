from __future__ import annotations

import pandas as pd
import pytest
from datetime import date

from biz.stock_finance import sync_finance


def _finance_frame(code: str) -> pd.DataFrame:
    minimum = sync_finance.minimum_expected_report_date(date.today())
    return pd.DataFrame(
        [{"stock_code": code, "report_date": minimum.isoformat()}]
    )


def test_finance_cli_fails_when_stock_universe_is_empty(monkeypatch):
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(sync_finance, "get_finance_stock_universe", lambda engine: {})

    assert sync_finance.main(["--sleep", "0"]) == 2


def test_finance_cli_fails_batch_when_any_stock_fails_but_keeps_processing(
    monkeypatch,
):
    attempted: list[str] = []
    committed: list[str] = []
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"000001": None, "000002": None},
    )

    def fake_fetch(code: str) -> pd.DataFrame:
        attempted.append(code)
        if code == "000002":
            raise RuntimeError("provider unavailable")
        return _finance_frame(code)

    def fake_upsert(engine, frame, *, stock_code: str) -> int:
        committed.append(stock_code)
        return 1

    monkeypatch.setattr(sync_finance, "fetch_finance", fake_fetch)
    monkeypatch.setattr(sync_finance, "upsert_finance", fake_upsert)

    assert sync_finance.main(["--sleep", "0"]) == 1
    assert attempted == ["000001", "000002"]
    assert committed == ["000001"]


def test_finance_cli_succeeds_only_when_every_stock_finishes(monkeypatch):
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"000001": None, "000002": None},
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_finance",
        _finance_frame,
    )
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda engine, frame, *, stock_code: 1,
    )

    assert sync_finance.main(["--sleep", "0"]) == 0


def test_finance_cli_rejects_empty_provider_frame_and_does_not_commit(monkeypatch):
    committed: list[str] = []
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"000001": None},
    )
    monkeypatch.setattr(sync_finance, "fetch_finance", lambda code: pd.DataFrame())
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda engine, frame, *, stock_code: committed.append(stock_code) or 1,
    )

    assert sync_finance.main(["--sleep", "0"]) == 1
    assert committed == []


def test_empty_finance_frame_cannot_publish_successful_empty_coverage():
    with pytest.raises(ValueError, match="at least one row"):
        sync_finance.upsert_finance(
            object(),
            pd.DataFrame(),
            stock_code="000001",
        )


def test_stale_latest_report_period_is_data_blocked() -> None:
    with pytest.raises(RuntimeError, match="最新报告期过旧"):
        sync_finance.validate_finance_response(
            "000001",
            pd.DataFrame(
                [{"stock_code": "000001", "report_date": "2025-09-30"}]
            ),
            as_of=date(2026, 8, 26),
            minimum_report_date=date(2026, 3, 31),
        )


def test_post_deadline_new_listing_is_exempt_from_prior_period_floor() -> None:
    latest = sync_finance.validate_finance_response(
        "000001",
        pd.DataFrame(
            [{"stock_code": "000001", "report_date": "2025-12-31"}]
        ),
        as_of=date(2026, 8, 26),
        minimum_report_date=date(2026, 3, 31),
        disclosure_deadline=date(2026, 4, 30),
        listing_date=date(2026, 5, 8),
    )

    assert latest == date(2025, 12, 31)


def test_new_listing_exemption_never_turns_empty_response_into_success() -> None:
    with pytest.raises(RuntimeError, match="返回空结果"):
        sync_finance.validate_finance_response(
            "000001",
            pd.DataFrame(),
            as_of=date(2026, 8, 26),
            minimum_report_date=date(2026, 3, 31),
            disclosure_deadline=date(2026, 4, 30),
            listing_date=date(2026, 5, 8),
        )


def test_finance_coverage_threshold_cannot_be_lowered(monkeypatch):
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())

    assert sync_finance.main(["--min-code-coverage", "0.99"]) == 2


def test_finance_limit_zero_means_the_complete_loaded_universe(monkeypatch):
    attempted: list[str] = []
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"000001": None, "000002": None, "600000": None},
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_finance",
        lambda code: attempted.append(code) or _finance_frame(code),
    )
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda engine, frame, *, stock_code: 1,
    )

    assert sync_finance.main(["--limit", "0", "--sleep", "0"]) == 0
    assert attempted == ["000001", "000002", "600000"]
