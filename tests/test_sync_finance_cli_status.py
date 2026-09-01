from __future__ import annotations

import pandas as pd
import pytest
import json
from contextlib import nullcontext
from datetime import date, timedelta
from types import SimpleNamespace
import threading
import time

from biz.stock_finance import sync_finance


def _finance_frame(code: str) -> pd.DataFrame:
    minimum = sync_finance.minimum_expected_report_date(date.today())
    return pd.DataFrame(
        [{"stock_code": code, "report_date": minimum.isoformat()}]
    )


def _stub_atomic_batch_seal(monkeypatch) -> None:
    monkeypatch.setattr(
        sync_finance,
        "append_finance_atomic_batch_seal",
        lambda *args, **kwargs: {
            "schema": "probiga.finance-atomic-batch-seal.v1",
            "status": "COMPLETE",
            "eligible_member_count": 1,
        },
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
    _stub_atomic_batch_seal(monkeypatch)
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


def test_002731_stale_primary_uses_only_official_nonfiling_disposition(
    monkeypatch,
):
    calls: list[tuple[str, str]] = []
    today = date.today()

    class FakeEngine:
        def begin(self):
            return nullcontext(object())

        def dispose(self):
            return None

    evidence = {
        "source": "cninfo.finance.nonfiling",
        "reason_code": "CNINFO_REGULATORY_PERIODIC_REPORT_NOT_FILED",
        "stock_code": "002731",
        "expected_report_date": sync_finance.minimum_expected_report_date(
            today
        ).isoformat(),
        "announcement_id": "1225497518",
        "announcement_title": "未在规定期限内披露定期报告",
        "valid_until": (today + timedelta(days=7)).isoformat(),
        "next_retry_date": (today + timedelta(days=1)).isoformat(),
    }
    monkeypatch.setattr(sync_finance, "get_engine", FakeEngine)
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"002731": date(2015, 1, 1)},
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_finance",
        lambda code: pd.DataFrame(
            [{"stock_code": code, "report_date": "2025-09-30"}]
        ),
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_cninfo_nonfiling_evidence",
        lambda code, **kwargs: evidence,
    )
    monkeypatch.setattr(
        sync_finance,
        "append_finance_expected_unavailable",
        lambda connection, **kwargs: (
            calls.append((kwargs["stock_code"], kwargs["expected_report_date"].isoformat()))
            or SimpleNamespace(coverage_id="c" * 64)
        ),
    )
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda *args, **kwargs: pytest.fail("old finance rows must not be re-upserted"),
    )

    assert sync_finance.main(["--code", "002731", "--sleep", "0"]) == 0
    assert calls == [
        ("002731", sync_finance.minimum_expected_report_date(today).isoformat())
    ]


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
    _stub_atomic_batch_seal(monkeypatch)
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


def test_finance_seal_existing_emits_explicit_machine_result(monkeypatch, capsys):
    class FakeEngine:
        def dispose(self):
            return None

    monkeypatch.setattr(sync_finance, "get_engine", FakeEngine)
    monkeypatch.setattr(
        sync_finance,
        "append_finance_atomic_batch_seal",
        lambda *args, **kwargs: {
            "schema": "probiga.pit-finance-atomic-batch.v1",
            "eligible_code_count": 5200,
            "catalog_member_count": 5200,
            "expected_unavailable_count": 1,
            "eligible_code_set_hash": "a" * 64,
            "coverage_root_sha256": "b" * 64,
            "batch_root_sha256": "c" * 64,
            "seal_coverage_id": "d" * 64,
        },
    )

    assert sync_finance.main(["--seal-existing"]) == 0
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["schema"] == "probiga.finance-atomic-batch-result.v1"
    assert payload["seal_schema"] == "probiga.pit-finance-atomic-batch.v1"
    assert payload["status"] == "PASS"


def test_finance_offset_selects_one_ordered_non_overlapping_shard(monkeypatch):
    attempted: list[str] = []
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {
            "000001": None,
            "000002": None,
            "000003": None,
            "000004": None,
        },
    )
    monkeypatch.setattr(
        sync_finance,
        "fetch_finance",
        lambda code: attempted.append(code) or _finance_frame(code),
    )
    monkeypatch.setattr(sync_finance, "upsert_finance", lambda *a, **k: 1)

    assert sync_finance.main([
        "--offset", "2", "--limit", "2", "--sleep", "0",
    ]) == 0
    assert attempted == ["000003", "000004"]


@pytest.mark.parametrize(
    "argv",
    [
        ["--offset", "-1"],
        ["--code", "000001", "--offset", "1"],
    ],
)
def test_finance_offset_rejects_ambiguous_or_negative_scope(monkeypatch, argv):
    engine_requested = False

    def fail_if_engine_requested():
        nonlocal engine_requested
        engine_requested = True
        return object()

    monkeypatch.setattr(sync_finance, "get_engine", fail_if_engine_requested)
    assert sync_finance.main(argv) == 2
    assert engine_requested is False


def test_finance_fetch_workers_are_concurrent_but_database_writes_are_serial(
    monkeypatch,
):
    codes = ["000001", "000002", "000003", "000004"]
    active_fetches = 0
    maximum_active_fetches = 0
    fetch_lock = threading.Lock()
    release_fetches = threading.Event()
    committed: list[str] = []
    upsert_threads: list[int] = []
    main_thread = threading.get_ident()

    _stub_atomic_batch_seal(monkeypatch)
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {code: None for code in codes},
    )

    def fake_fetch(code: str) -> pd.DataFrame:
        nonlocal active_fetches, maximum_active_fetches
        with fetch_lock:
            active_fetches += 1
            maximum_active_fetches = max(maximum_active_fetches, active_fetches)
            if maximum_active_fetches >= 2:
                release_fetches.set()
        assert release_fetches.wait(timeout=2)
        time.sleep(0.01)
        with fetch_lock:
            active_fetches -= 1
        return _finance_frame(code)

    def fake_upsert(engine, frame, *, stock_code: str) -> int:
        upsert_threads.append(threading.get_ident())
        committed.append(stock_code)
        return 1

    monkeypatch.setattr(sync_finance, "fetch_finance", fake_fetch)
    monkeypatch.setattr(sync_finance, "upsert_finance", fake_upsert)

    assert sync_finance.main(["--workers", "2", "--sleep", "0"]) == 0
    assert maximum_active_fetches == 2
    assert committed == codes
    assert upsert_threads == [main_thread] * len(codes)


def test_finance_fetch_workers_keep_processing_after_provider_failure(monkeypatch):
    attempted: list[str] = []
    committed: list[str] = []
    monkeypatch.setattr(sync_finance, "get_engine", lambda: object())
    monkeypatch.setattr(
        sync_finance,
        "get_finance_stock_universe",
        lambda engine: {"000001": None, "000002": None, "000003": None},
    )

    def fake_fetch(code: str) -> pd.DataFrame:
        attempted.append(code)
        if code == "000002":
            raise RuntimeError("provider unavailable")
        return _finance_frame(code)

    monkeypatch.setattr(sync_finance, "fetch_finance", fake_fetch)
    monkeypatch.setattr(
        sync_finance,
        "upsert_finance",
        lambda engine, frame, *, stock_code: committed.append(stock_code) or 1,
    )

    assert sync_finance.main(["--workers", "3", "--sleep", "0"]) == 1
    assert set(attempted) == {"000001", "000002", "000003"}
    assert committed == ["000001", "000003"]


@pytest.mark.parametrize("workers", [0, sync_finance.MAX_FETCH_WORKERS + 1])
def test_finance_fetch_workers_are_safely_bounded(monkeypatch, workers):
    engine_requested = False

    def fail_if_engine_requested():
        nonlocal engine_requested
        engine_requested = True
        return object()

    monkeypatch.setattr(sync_finance, "get_engine", fail_if_engine_requested)

    assert sync_finance.main(["--workers", str(workers)]) == 2
    assert engine_requested is False
