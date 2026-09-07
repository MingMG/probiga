from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
import json
import threading
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import create_engine, text

from server.common.announcement_provider import (
    AnnouncementProviderError,
    CNINFO_PROVIDER_RECEIPT_SCHEMA,
    CNINFO_STEADY_REQUEST_INTERVAL_SECONDS,
    CninfoMarketAnnouncementProvider,
    ProviderBackedAnnouncementAdapter,
)
from server.common.qmt_announcement_pit import (
    ANNOUNCEMENT_FALLBACK_REASON_CODES,
    AnnouncementCatalog,
    CNINFO_ANNOUNCEMENT_SOURCE,
    HistoricalReconstructionContext,
    QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V3_SCHEMA,
    QMT_ANNOUNCEMENT_RECONSTRUCTION_STOCK_SCOPE,
    QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN,
    QMTAnnouncementBlocked,
    _validate_reconstruction_context,
    _explicit_qmt_unavailability_reason,
    _fallback_receipt_valid,
    synchronize_historical_cninfo_announcements,
    synchronize_qmt_announcements,
    validate_complete_historical_reconstruction_batch,
    validate_task_result,
)
from server.common.pit_facts import (
    _fallback_event_receipt_valid,
    append_source_coverage,
    canonical_hash as pit_canonical_hash,
    ensure_pit_fact_schema,
    load_event_facts,
    resolve_common_fact_cutoff,
)


def _epoch_ms(second: int) -> int:
    value = datetime(
        2026, 8, 28, 20, 44, second,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    )
    return int(value.timestamp() * 1000)


def _row(code: str, index: int, *, event_id: str | None = None) -> dict:
    return {
        "secCode": code,
        "announcementId": event_id or f"event-{code}-{index:03d}",
        "announcementTime": _epoch_ms(59 - index),
        "announcementTitle": f"announcement {index}",
        "adjunctUrl": f"finalpage/{code}-{index}.PDF",
    }


def _row_on_date(code: str, index: int, day: date) -> dict:
    published = datetime(
        day.year, day.month, day.day, 20, 44, 59,
        tzinfo=ZoneInfo("Asia/Shanghai"),
    ) - timedelta(seconds=index)
    return {
        "secCode": code,
        "announcementId": f"event-{code}-{day:%Y%m%d}-{index:03d}",
        "announcementTime": int(published.timestamp() * 1000),
        "announcementTitle": f"announcement {day:%Y%m%d}-{index}",
        "adjunctUrl": f"finalpage/{code}-{day:%Y%m%d}-{index}.PDF",
    }


class _Response:
    def __init__(self, body: dict) -> None:
        self._body = body
        self.encoding = ""

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._body


class _HttpStatusError(RuntimeError):
    def __init__(self, response) -> None:
        self.response = response
        super().__init__(f"HTTP {response.status_code}")


class _HttpFailureResponse:
    def __init__(self, *, status_code: int, retry_after: str = "") -> None:
        self.status_code = int(status_code)
        self.headers = {"Retry-After": retry_after}

    def raise_for_status(self) -> None:
        raise _HttpStatusError(self)


class _FakeTime:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(float(seconds))
        self.value += float(seconds)


class _Client:
    def __init__(
        self,
        *,
        masters: list[list[dict]],
        rows_by_code: dict[str, list[dict]],
        confirmation_drift_code: str = "",
        last_page_drift_code: str = "",
        wrong_echo_code: str = "",
    ) -> None:
        self.masters = masters
        self.rows_by_code = rows_by_code
        self.confirmation_drift_code = confirmation_drift_code
        self.last_page_drift_code = last_page_drift_code
        self.wrong_echo_code = wrong_echo_code
        self.get_count = 0
        self.post_count = defaultdict(int)
        self.requested_stock_identities: list[str] = []
        self.lock = threading.Lock()

    def get(self, _url: str) -> _Response:
        with self.lock:
            index = min(self.get_count, len(self.masters) - 1)
            self.get_count += 1
            rows = [dict(item) for item in self.masters[index]]
        return _Response({"stockList": rows})

    def post(self, _url: str, *, data: dict) -> _Response:
        stock_identity = str(data["stock"])
        code, _org_id = stock_identity.split(",", 1)
        page = int(data["pageNum"])
        with self.lock:
            self.requested_stock_identities.append(stock_identity)
            self.post_count[(code, page)] += 1
            call_count = self.post_count[(code, page)]
        window_start_text, window_end_text = str(data["seDate"]).split(
            "~", 1
        )
        window_start = date.fromisoformat(window_start_text)
        window_end = date.fromisoformat(window_end_text)
        shanghai = ZoneInfo("Asia/Shanghai")
        rows = [
            dict(item) for item in self.rows_by_code.get(code, [])
            if window_start
            <= datetime.fromtimestamp(
                int(item["announcementTime"]) / 1000.0,
                tz=ZoneInfo("UTC"),
            ).astimezone(shanghai).date()
            <= window_end
        ]
        total = len(rows)
        start = (page - 1) * 30
        page_rows = rows[start:start + 30]
        if (
            code == self.confirmation_drift_code
            and page == 1
            and call_count == 2
            and page_rows
        ):
            page_rows[0]["announcementTitle"] = "changed during capture"
        if (
            code == self.last_page_drift_code
            and page > 1
            and call_count == 2
            and page_rows
        ):
            page_rows[-1]["announcementTitle"] = "changed last page"
        expected_pages = max(1, (total + 29) // 30)
        return _Response({
            "pageNum": (
                page + 1 if code == self.wrong_echo_code else page
            ),
            "totalRecordNum": total,
            "totalpages": total // 30,
            "hasMore": page < expected_pages,
            "announcements": page_rows if total else None,
        })

    def close(self) -> None:
        return None


class _DenseSweepClient(_Client):
    """Inject bounded same-day pagination drift without changing totals."""

    def __init__(
        self,
        *args,
        dense_day: date,
        first_sweep_duplicate: bool = False,
        drift_every_sweep: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.dense_day = dense_day
        self.first_sweep_duplicate = first_sweep_duplicate
        self.drift_every_sweep = drift_every_sweep
        self.dense_sweep = 0

    def post(self, _url: str, *, data: dict) -> _Response:
        page = int(data["pageNum"])
        exact_dense_day = str(data["seDate"]) == (
            f"{self.dense_day.isoformat()}~{self.dense_day.isoformat()}"
        )
        if exact_dense_day and page == 1:
            self.dense_sweep += 1
        response = super().post(_url, data=data)
        if not exact_dense_day:
            return response
        body = response.json()
        page_rows = body.get("announcements") or []
        if (
            self.first_sweep_duplicate
            and self.dense_sweep == 1
            and page == 2
            and page_rows
        ):
            code = str(data["stock"]).split(",", 1)[0]
            dense_rows = [
                dict(item) for item in self.rows_by_code[code]
                if datetime.fromtimestamp(
                    int(item["announcementTime"]) / 1000.0,
                    tz=ZoneInfo("UTC"),
                ).astimezone(ZoneInfo("Asia/Shanghai")).date()
                == self.dense_day
            ]
            page_rows[0] = dense_rows[0]
        if self.drift_every_sweep and page == 1 and page_rows:
            page_rows[0]["announcementTitle"] = (
                f"drifting snapshot {self.dense_sweep}"
            )
        return response


class _RateLimitedClient(_Client):
    def __init__(
        self, *args, failures: int, status_code: int = 403, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.failures = int(failures)
        self.status_code = int(status_code)
        self.attempts = 0

    def post(self, _url: str, *, data: dict):
        self.attempts += 1
        if self.attempts <= self.failures:
            return _HttpFailureResponse(
                status_code=self.status_code,
                retry_after="3" if self.status_code in {403, 429, 567} else "",
            )
        return super().post(_url, data=data)


class _TooManyPagesClient(_Client):
    def post(self, _url: str, *, data: dict) -> _Response:
        return _Response({
            "pageNum": 1,
            "totalRecordNum": 6_001,
            "totalpages": 200,
            "hasMore": True,
            "announcements": [],
        })


class _FailOnCodeProvider(CninfoMarketAnnouncementProvider):
    def __init__(self, *args, fail_code: str, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.fail_code = fail_code

    def fetch(self, **kwargs):
        if kwargs.get("stock_code") == self.fail_code:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_SOURCE_RATE_LIMITED",
                f"stock={self.fail_code},http=403",
            )
        return super().fetch(**kwargs)


def _master(*codes: str) -> list[dict]:
    return [
        {"code": code, "orgId": f"org-{code}", "category": "A"}
        for code in codes
    ]


def _provider(client: _Client) -> CninfoMarketAnnouncementProvider:
    return CninfoMarketAnnouncementProvider(
        client=client,
        minimum_request_interval=0,
        now_fn=lambda: datetime(2026, 8, 29, 9, 0),
    )


def _historical_authority(
    catalog: AnnouncementCatalog, target: date,
) -> dict:
    no_trade_codes = [catalog.codes[-1]]
    truth = {
        "schema": "probiga.qmt-daily-market-consumer-truth.v1",
        "requested_sessions": [target.isoformat()],
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "attested_row_count": len(catalog.codes) - len(no_trade_codes),
    }
    membership = {
        "snapshot_date": target.isoformat(),
        "quality_status": "QMT_VALIDATED",
        "source": "qmt.reference",
    }
    reconciliation = {
        "schema": "probiga.qmt-announcement-catalog-reconciliation.v2",
        "target_trade_date": target.isoformat(),
        "authority_catalog_batch_id": catalog.batch_id,
        "authority_catalog_manifest_hash": catalog.manifest_hash,
        "authority_eligible_count": len(catalog.codes),
        "prior_eligible_count": len(catalog.codes),
        "attested_daily_count": len(catalog.codes) - len(no_trade_codes),
        "native_no_trade_count": len(no_trade_codes),
        "native_no_trade_codes": no_trade_codes,
        "native_no_trade_codes_sha256": pit_canonical_hash(no_trade_codes),
        "excluded_from_prior": [],
        "added_to_prior": [],
    }
    return {
        "schema": "probiga.qmt-announcement-reconstruction-authority.v2",
        "target_trade_date": target.isoformat(),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "catalog_member_count": len(catalog.codes),
        "catalog_codes_sha256": pit_canonical_hash(list(catalog.codes)),
        "qmt_daily_truth": truth,
        "qmt_daily_truth_sha256": pit_canonical_hash(truth),
        "membership_snapshot": membership,
        "membership_snapshot_sha256": pit_canonical_hash(membership),
        "reconciliation": reconciliation,
        "reconciliation_sha256": pit_canonical_hash(reconciliation),
    }


def _historical_authority_v3(
    catalog: AnnouncementCatalog, target: date,
) -> dict:
    authority = _historical_authority(catalog, target)
    authority["schema"] = QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V3_SCHEMA
    authority["execution_origin"] = (
        QMT_ANNOUNCEMENT_SCHEDULER_RECOVERY_ORIGIN
    )
    authority["stock_scope_authority"] = (
        QMT_ANNOUNCEMENT_RECONSTRUCTION_STOCK_SCOPE
    )
    authority.pop("membership_snapshot")
    authority.pop("membership_snapshot_sha256")
    return authority


def test_historical_authority_v3_requires_exact_stock_scope_contract() -> None:
    target = date(2026, 9, 3)
    catalog = AnnouncementCatalog(
        batch_id="catalog-historical-20260903",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        codes=("000001", "600519"),
        qmt_by_code={"000001": "000001.SZ", "600519": "600519.SH"},
    )

    def validate(authority: dict) -> None:
        _validate_reconstruction_context(
            HistoricalReconstructionContext(
                target_trade_date=target,
                source_query_cutoff_at=datetime.combine(
                    target, datetime.max.time()
                ),
                reconstruction_started_at=datetime(2026, 9, 5, 13, 0),
                scheduler_run_uid="1" * 32,
                build_sha="2" * 40,
                authority=authority,
            ),
            catalog=catalog,
        )

    validate(_historical_authority_v3(catalog, target))

    v2_without_membership = _historical_authority(catalog, target)
    v2_without_membership.pop("membership_snapshot")
    v2_without_membership.pop("membership_snapshot_sha256")
    with pytest.raises(QMTAnnouncementBlocked):
        validate(v2_without_membership)

    for field, value in (
        ("stock_scope_authority", ""),
        ("membership_snapshot", {"snapshot_date": target.isoformat()}),
        ("membership_snapshot_sha256", "a" * 64),
    ):
        invalid_v3 = _historical_authority_v3(catalog, target)
        invalid_v3[field] = value
        with pytest.raises(QMTAnnouncementBlocked):
            validate(invalid_v3)


def _fetch(provider: CninfoMarketAnnouncementProvider, code: str):
    suffix = "SH" if code.startswith("6") else "SZ"
    return provider.fetch(
        stock_code=code,
        qmt_code=f"{code}.{suffix}",
        requested_start_time="20260729000000",
        requested_end_time="20260828235959",
    )


def test_cninfo_exact_stock_nonempty_and_empty_receipts_are_sealed() -> None:
    master = _master("000001", "600519", "999999")
    client = _Client(
        masters=[master, master],
        rows_by_code={"000001": [_row("000001", 0)], "600519": []},
    )
    provider = _provider(client)
    adapter = ProviderBackedAnnouncementAdapter(provider, workers=2)
    adapter.bind_capture_deadline(
        fact_cutoff_at=datetime.now(), max_capture_delay=timedelta(minutes=30)
    )
    for qmt_code in ("000001.SZ", "600519.SH"):
        adapter.download_history_data(
            qmt_code,
            period="announcement",
            start_time="20260729000000",
            end_time="20260828235959",
        )
    frames = adapter.get_market_data_ex(
        field_list=[],
        stock_list=["000001.SZ", "600519.SH"],
        period="announcement",
        start_time="20260729000000",
        end_time="20260828235959",
        count=-1,
        dividend_type="none",
        fill_data=False,
    )
    receipts = adapter.capture_receipts()

    assert len(frames["000001.SZ"]) == 1
    assert frames["600519.SH"].empty
    assert client.get_count == 2
    assert set(client.requested_stock_identities) == {
        "000001,org-000001", "600519,org-600519"
    }
    assert set(receipts) == {"000001", "600519"}
    assert {item["schema"] for item in receipts.values()} == {
        CNINFO_PROVIDER_RECEIPT_SCHEMA
    }
    assert {item["result_count"] for item in receipts.values()} == {0, 1}
    assert all(
        item["security_master_sha256"]
        == item["security_master_end_sha256"]
        for item in receipts.values()
    )
    assert all(item["directory_member_count"] == 3 for item in receipts.values())
    assert all(
        item["directory_catalog_coverage_count"] == 2
        and item["directory_catalog_missing_count"] == 0
        and item["directory_catalog_extra_count"] == 1
        for item in receipts.values()
    )


def test_cninfo_security_master_is_loaded_once_under_concurrency() -> None:
    codes = ("000001", "000002", "000003", "000004")
    master = _master(*codes)
    client = _Client(
        masters=[master, master], rows_by_code={code: [] for code in codes}
    )
    provider = _provider(client)
    adapter = ProviderBackedAnnouncementAdapter(provider, workers=4)
    adapter.bind_capture_deadline(
        fact_cutoff_at=datetime.now(), max_capture_delay=timedelta(minutes=30)
    )
    qmt_codes = [f"{code}.SZ" for code in codes]
    for qmt_code in qmt_codes:
        adapter.download_history_data(
            qmt_code,
            period="announcement",
            start_time="20260729000000",
            end_time="20260828235959",
        )
    adapter.get_market_data_ex(
        field_list=[], stock_list=qmt_codes, period="announcement",
        start_time="20260729000000", end_time="20260828235959",
        count=-1, dividend_type="none", fill_data=False,
    )
    adapter.capture_receipts()
    assert client.get_count == 2


def test_cninfo_missing_catalog_member_never_becomes_empty_complete() -> None:
    master = _master("000001")
    provider = _provider(_Client(masters=[master], rows_by_code={}))
    with pytest.raises(AnnouncementProviderError) as exc:
        _fetch(provider, "600519")
    assert exc.value.reason_code == (
        "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_INCOMPLETE"
    )


def test_cninfo_duplicate_across_pages_blocks_complete() -> None:
    rows = [_row("000001", index) for index in range(31)]
    rows[30]["announcementId"] = rows[0]["announcementId"]
    master = _master("000001")
    provider = _provider(
        _Client(masters=[master], rows_by_code={"000001": rows})
    )
    with pytest.raises(AnnouncementProviderError) as exc:
        _fetch(provider, "000001")
    assert exc.value.reason_code == "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED"


def test_cninfo_single_page_is_not_replayed() -> None:
    master = _master("000001")
    provider = _provider(_Client(
        masters=[master],
        rows_by_code={"000001": [_row("000001", 0)]},
        confirmation_drift_code="000001",
    ))
    result = _fetch(provider, "000001")
    assert len(result.rows) == 1
    assert provider._client.post_count[("000001", 1)] == 1
    assert result.receipt["pagination_mode"] == "EXACT_STOCK_SINGLE_PAGE"


def test_cninfo_dense_day_requires_two_complete_identical_sweeps() -> None:
    master = _master("000001")
    provider = _provider(_Client(
        masters=[master],
        rows_by_code={
            "000001": [_row("000001", index) for index in range(31)]
        },
    ))
    result = _fetch(provider, "000001")
    assert len(result.rows) == 31
    assert result.receipt["pagination_mode"] == (
        "EXACT_STOCK_DATE_SHARDS_DOUBLE_ATTESTED"
    )
    assert result.receipt["pagination_round_count"] == 2
    assert result.receipt["pagination_attempt_count"] == 2
    assert result.receipt["pagination_invalid_round_count"] == 0
    assert result.receipt["pagination_complete_round_attempts"] == [1, 2]
    assert len(set(
        result.receipt["pagination_complete_round_sha256"][-2:]
    )) == 1
    dense_leaves = [
        item for item in result.receipt["date_shard_manifest"]
        if item["capture_mode"] == "DENSE_DAY_COMPLETE_SWEEP"
    ]
    assert len(dense_leaves) == 1
    assert dense_leaves[0]["result_count"] == 31
    assert dense_leaves[0]["provider_page_count"] == 2


def test_cninfo_dense_day_raw_drift_never_becomes_complete() -> None:
    master = _master("000001")
    provider = _provider(_DenseSweepClient(
        masters=[master],
        rows_by_code={
            "000001": [_row("000001", index) for index in range(31)]
        },
        dense_day=date(2026, 8, 28),
        drift_every_sweep=True,
    ))

    with pytest.raises(AnnouncementProviderError) as exc:
        _fetch(provider, "000001")
    assert exc.value.reason_code == "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED"


def test_cninfo_300936_shape_recovers_only_after_two_stable_sweeps() -> None:
    code = "300936"
    dense_day = date(2026, 8, 20)
    rows = (
        [_row_on_date(code, index, dense_day) for index in range(58)]
        + [_row_on_date(code, index, date(2026, 8, 24)) for index in range(8)]
        + [_row_on_date(code, 0, date(2026, 8, 27))]
    )
    master = _master(code)
    client = _DenseSweepClient(
        masters=[master, master],
        rows_by_code={code: rows},
        dense_day=dense_day,
        first_sweep_duplicate=True,
    )
    provider = _provider(client)

    result = _fetch(provider, code)
    finalized = provider.finalize_receipts({code: result.receipt})[code]

    assert len(result.rows) == 67
    assert len({item["announcement_id"] for item in result.rows}) == 67
    assert result.receipt["pagination_invalid_round_count"] == 1
    assert result.receipt["pagination_attempt_count"] == 3
    assert result.receipt["pagination_complete_round_attempts"] == [2, 3]
    assert len(set(
        result.receipt["pagination_complete_round_sha256"][-2:]
    )) == 1
    assert _fallback_receipt_valid(
        finalized,
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        stock_code=code,
        qmt_code=f"{code}.SZ",
        requested_start_time="20260729000000",
        requested_end_time="20260828235959",
        result_count=67,
        catalog_codes=(code,),
    ) is True

    evidence = {
        "window_start": "2026-07-29",
        "query_end_time": "20260828235959",
        "catalog_member_count": 1,
        "catalog_member_set_hash": finalized[
            "requested_catalog_member_set_sha256"
        ],
        "provider_receipt": finalized,
        "provider_receipt_hash": pit_canonical_hash(finalized),
    }
    assert _fallback_event_receipt_valid(
        evidence,
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        stock_code=code,
        result_count=67,
    ) is True
    changed_receipt = dict(finalized)
    changed_receipt["pagination_complete_round_sha256"] = ["0" * 64] * 2
    changed = dict(evidence)
    changed["provider_receipt"] = changed_receipt
    changed["provider_receipt_hash"] = pit_canonical_hash(changed_receipt)
    assert _fallback_event_receipt_valid(
        changed,
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        stock_code=code,
        result_count=67,
    ) is False


def test_cninfo_default_steady_rate_fits_5555_single_page_capture_budget() -> None:
    master = _master("000001")
    provider = CninfoMarketAnnouncementProvider(
        client=_Client(masters=[master], rows_by_code={})
    )

    assert provider._minimum_interval == (
        CNINFO_STEADY_REQUEST_INTERVAL_SECONDS
    )
    assert 5_555 * CNINFO_STEADY_REQUEST_INTERVAL_SECONDS < 30 * 60


def test_cninfo_403_respects_retry_after_then_recovers() -> None:
    master = _master("000001")
    client = _RateLimitedClient(
        masters=[master], rows_by_code={"000001": []}, failures=1
    )
    clock = _FakeTime()
    provider = CninfoMarketAnnouncementProvider(
        client=client,
        minimum_request_interval=0.25,
        now_fn=lambda: datetime(2026, 8, 29, 9, 0),
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
        jitter_fn=lambda _start, _end: 0.0,
    )

    result = _fetch(provider, "000001")

    assert result.receipt["result_count"] == 0
    assert client.attempts == 2
    assert sum(clock.sleeps) >= 3.0


def test_cninfo_504_retries_same_session_then_recovers() -> None:
    master = _master("000001")
    client = _RateLimitedClient(
        masters=[master], rows_by_code={"000001": []},
        failures=1, status_code=504,
    )
    clock = _FakeTime()
    provider = CninfoMarketAnnouncementProvider(
        client=client,
        minimum_request_interval=0.25,
        now_fn=lambda: datetime(2026, 8, 29, 9, 0),
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
        jitter_fn=lambda _start, _end: 0.0,
    )

    assert _fetch(provider, "000001").receipt["result_count"] == 0
    assert client.attempts == 2
    assert sum(clock.sleeps) >= 1.0


@pytest.mark.parametrize("status_code,expected_attempts", ((504, 4), (400, 1)))
def test_cninfo_gateway_retry_is_finite_and_400_is_not_retried(
    status_code, expected_attempts
) -> None:
    master = _master("000001")
    client = _RateLimitedClient(
        masters=[master], rows_by_code={}, failures=10,
        status_code=status_code,
    )
    clock = _FakeTime()
    provider = CninfoMarketAnnouncementProvider(
        client=client,
        minimum_request_interval=0,
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
        jitter_fn=lambda _start, _end: 0.0,
    )

    with pytest.raises(AnnouncementProviderError) as exc:
        _fetch(provider, "000001")
    assert exc.value.reason_code == "ANNOUNCEMENT_FALLBACK_SOURCE_QUERY_FAILED"
    assert client.attempts == expected_attempts


def test_cninfo_retry_backoff_cannot_cross_capture_deadline() -> None:
    master = _master("000001")
    client = _RateLimitedClient(
        masters=[master], rows_by_code={}, failures=10, status_code=504
    )
    clock = _FakeTime()
    provider = CninfoMarketAnnouncementProvider(
        client=client,
        minimum_request_interval=0.25,
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
        jitter_fn=lambda _start, _end: 0.0,
    )
    provider.bind_capture_deadline(remaining_seconds=0.5)

    with pytest.raises(AnnouncementProviderError) as exc:
        _fetch(provider, "000001")
    assert exc.value.reason_code == (
        "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"
    )
    assert client.attempts == 1


def test_cninfo_page_loop_cannot_cross_capture_deadline() -> None:
    master = _master("000001")
    client = _Client(
        masters=[master],
        rows_by_code={
            "000001": [_row("000001", index) for index in range(31)]
        },
    )
    clock = _FakeTime()
    provider = CninfoMarketAnnouncementProvider(
        client=client,
        minimum_request_interval=0.25,
        sleep_fn=clock.sleep,
        monotonic_fn=clock.monotonic,
    )
    provider.bind_capture_deadline(remaining_seconds=0.6)

    with pytest.raises(AnnouncementProviderError) as exc:
        _fetch(provider, "000001")
    assert exc.value.reason_code == (
        "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"
    )
    # The root page plus one deterministic shard query complete; the next
    # request is rejected before it can cross the monotonic deadline.
    assert sum(client.post_count.values()) == 2


def test_cninfo_per_stock_page_limit_is_fail_closed() -> None:
    master = _master("000001")
    provider = _provider(_TooManyPagesClient(
        masters=[master], rows_by_code={}
    ))

    with pytest.raises(AnnouncementProviderError) as exc:
        _fetch(provider, "000001")
    assert exc.value.reason_code == (
        "ANNOUNCEMENT_FALLBACK_PAGINATION_LIMIT_EXCEEDED"
    )


def test_cninfo_page_echo_and_security_master_drift_block_complete() -> None:
    master = _master("000001")
    provider = _provider(_Client(
        masters=[master], rows_by_code={"000001": []},
        wrong_echo_code="000001",
    ))
    with pytest.raises(AnnouncementProviderError) as exc:
        _fetch(provider, "000001")
    assert exc.value.reason_code == "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED"

    changed = _master("000001")
    changed[0]["orgId"] = "changed-org"
    provider = _provider(_Client(
        masters=[master, changed], rows_by_code={"000001": []}
    ))
    result = _fetch(provider, "000001")
    with pytest.raises(AnnouncementProviderError) as exc:
        provider.finalize_receipts({"000001": result.receipt})
    assert exc.value.reason_code == (
        "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_CHANGED"
    )


def test_publisher_revalidates_cninfo_v2_directory_and_pagination_proof() -> None:
    master = _master("000001", "600519")
    provider = _provider(_Client(
        masters=[master, master],
        rows_by_code={"000001": [_row("000001", 0)]},
    ))
    result = _fetch(provider, "000001")
    receipt = provider.finalize_receipts({"000001": result.receipt})["000001"]
    arguments = {
        "source": CNINFO_ANNOUNCEMENT_SOURCE,
        "stock_code": "000001",
        "qmt_code": "000001.SZ",
        "requested_start_time": "20260729000000",
        "requested_end_time": "20260828235959",
        "result_count": 1,
        "catalog_codes": ("000001",),
    }
    assert _fallback_receipt_valid(receipt, **arguments) is True
    for field, replacement in (
        ("directory_raw_sha256", "0" * 64),
        ("query_stock_identity", "000001,wrong"),
        ("last_page_has_more", True),
        ("pagination_sha256", "0" * 64),
        ("security_master_end_sha256", "0" * 64),
        ("requested_catalog_member_set_sha256", "0" * 64),
    ):
        changed = dict(receipt)
        changed[field] = replacement
        assert _fallback_receipt_valid(changed, **arguments) is False


def test_only_frozen_qmt_unavailability_is_fallback_eligible() -> None:
    assert _explicit_qmt_unavailability_reason(
        RuntimeError("NO_PERMISSION")
    ) == "QMT_ANNOUNCEMENT_NO_PERMISSION_OR_QUERY_FAILED"
    assert _explicit_qmt_unavailability_reason(
        ModuleNotFoundError("No module named 'xtquant'", name="xtquant")
    ) == "QMT_ANNOUNCEMENT_SDK_UNAVAILABLE"
    assert _explicit_qmt_unavailability_reason(TypeError("local bug")) == ""
    assert _explicit_qmt_unavailability_reason(
        RuntimeError("database unavailable")
    ) == ""


def test_signed_big_qmt_terminal_pandas_failure_is_fallback_eligible() -> None:
    error = RuntimeError(
        "Big QMT announcement failed: Traceback (most recent call last):\n"
        '  File "D:/broker/python/_PyContextInfo.py", line 701, '
        'in get_market_data_ex\n'
        "    import pandas as pd\n"
        "ModuleNotFoundError: No module named 'pandas'"
    )

    assert _explicit_qmt_unavailability_reason(error) == (
        "QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE"
    )


@pytest.mark.parametrize(
    "message",
    (
        "ModuleNotFoundError: No module named 'pandas'",
        (
            "Big QMT announcement failed: ModuleNotFoundError: "
            "No module named 'pandas'"
        ),
        (
            "Big QMT announcement failed: _PyContextInfo.py: "
            "ModuleNotFoundError: No module named 'pandas'"
        ),
        (
            "Big QMT announcement failed: _PyContextInfo.py: "
            "get_market_data_ex: ModuleNotFoundError: No module named 'numpy'"
        ),
        (
            "local get_market_data_ex _PyContextInfo.py failed: "
            "ModuleNotFoundError: No module named 'pandas'"
        ),
    ),
)
def test_partial_terminal_dependency_fingerprints_cannot_authorize_fallback(
    message,
) -> None:
    assert _explicit_qmt_unavailability_reason(RuntimeError(message)) == ""


def test_cninfo_exact_receipts_publish_one_atomic_catalog_batch(
    monkeypatch, tmp_path
) -> None:
    cutoff = datetime.now().replace(microsecond=0)
    master = _master("000001", "600519", "999999")
    client = _Client(
        masters=[master, master],
        rows_by_code={"000001": [_row("000001", 0)], "600519": []},
    )
    provider = CninfoMarketAnnouncementProvider(
        client=client,
        minimum_request_interval=0,
        now_fn=lambda: cutoff,
    )
    adapter = ProviderBackedAnnouncementAdapter(provider, workers=2)
    catalog = AnnouncementCatalog(
        batch_id="catalog-cninfo",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        codes=("000001", "600519"),
        qmt_by_code={"000001": "000001.SZ", "600519": "600519.SH"},
    )
    monkeypatch.setattr(
        "server.common.qmt_announcement_pit._load_catalog",
        lambda _engine, _cutoff: catalog,
    )
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_pit_fact_schema(engine)
    times = iter((
        cutoff,
        cutoff + timedelta(seconds=1),
    ))
    result = synchronize_qmt_announcements(
        engine,
        xtdata=adapter,
        checkpoint_root=tmp_path,
        now_fn=lambda: next(times, cutoff + timedelta(seconds=1)),
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        fallback_reason=next(iter(ANNOUNCEMENT_FALLBACK_REASON_CODES)),
        resume=False,
        batch_size=2,
    )

    assert result["status"] == "COMPLETE"
    assert result["source"] == CNINFO_ANNOUNCEMENT_SOURCE
    assert result["stock_count"] == result["coverage_count"] == 2
    assert result["event_count"] == 1
    assert result["empty_stock_count"] == 1
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_pit_source_coverage "
            "WHERE source='cninfo.announcement' AND batch_id=:batch_id"
        ), {"batch_id": result["batch_id"]}).scalar_one() == 2
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_pit_event_revision "
            "WHERE source='cninfo.announcement' AND batch_id=:batch_id"
        ), {"batch_id": result["batch_id"]}).scalar_one() == 1


def test_historical_reconstruction_uses_target_cutoff_and_actual_known_time(
    monkeypatch, tmp_path,
) -> None:
    target = date(2026, 9, 1)
    started = datetime(2026, 9, 3, 13, 0, 0)
    reconstructed = datetime(2026, 9, 3, 13, 0, 20)
    catalog = AnnouncementCatalog(
        batch_id="catalog-historical-20260901",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        codes=("000001", "600519"),
        qmt_by_code={"000001": "000001.SZ", "600519": "600519.SH"},
    )
    master = _master("000001", "600519", "301688")
    client = _Client(
        masters=[master, master],
        rows_by_code={
            "000001": [_row_on_date("000001", 0, target)],
            "600519": [],
        },
    )
    provider = CninfoMarketAnnouncementProvider(
        client=client,
        minimum_request_interval=0,
        now_fn=lambda: started + timedelta(seconds=10),
    )
    monkeypatch.setattr(
        ProviderBackedAnnouncementAdapter,
        "bind_capture_deadline",
        lambda adapter, **_kwargs: setattr(
            adapter, "_deadline_monotonic", float("inf")
        ),
    )
    truth = {
        "schema": "probiga.qmt-daily-market-consumer-truth.v1",
        "requested_sessions": [target.isoformat()],
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "attested_row_count": 1,
    }
    reconciliation = {
        "schema": "probiga.qmt-announcement-catalog-reconciliation.v2",
        "target_trade_date": target.isoformat(),
        "authority_catalog_batch_id": catalog.batch_id,
        "authority_catalog_manifest_hash": catalog.manifest_hash,
        "authority_eligible_count": len(catalog.codes),
        "prior_eligible_count": len(catalog.codes),
        "attested_daily_count": 1,
        "native_no_trade_count": 1,
        "native_no_trade_codes": ["600519"],
        "native_no_trade_codes_sha256": pit_canonical_hash(["600519"]),
        "excluded_from_prior": [],
        "added_to_prior": [],
    }
    authority = _historical_authority_v3(catalog, target)
    authority["qmt_daily_truth"] = truth
    authority["qmt_daily_truth_sha256"] = pit_canonical_hash(truth)
    authority["reconciliation"] = reconciliation
    authority["reconciliation_sha256"] = pit_canonical_hash(reconciliation)
    values = iter((started, reconstructed))
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_pit_fact_schema(engine)

    result = synchronize_historical_cninfo_announcements(
        engine,
        adapter=ProviderBackedAnnouncementAdapter(provider, workers=1),
        checkpoint_root=tmp_path,
        catalog=catalog,
        context=HistoricalReconstructionContext(
            target_trade_date=target,
            source_query_cutoff_at=datetime.combine(
                target, datetime.max.time()
            ),
            reconstruction_started_at=started,
            scheduler_run_uid="1" * 32,
            build_sha="2" * 40,
            authority=authority,
        ),
        now_fn=lambda: next(values, reconstructed + timedelta(seconds=1)),
        batch_size=2,
    )

    assert result["status"] == "COMPLETE"
    assert result["mode"] == "HISTORICAL_RECONSTRUCTION"
    assert result["database_writes"] is True
    result_authority = result["reconstruction_provenance"]["authority"]
    assert result_authority["schema"] == (
        QMT_ANNOUNCEMENT_RECONSTRUCTION_AUTHORITY_V3_SCHEMA
    )
    assert "membership_snapshot" not in result_authority
    assert "membership_snapshot_sha256" not in result_authority
    assert result["fact_cutoff_at"].startswith("2026-09-01T23:59:59")
    assert result["received_at"] == "2026-09-03T13:00:20.000000"
    assert result["coverage_count"] == 2
    assert result["event_count"] == 1
    assert result["empty_stock_count"] == 1
    assert client.requested_stock_identities == [
        "000001,org-000001", "600519,org-600519",
    ]

    class _CatalogProof:
        batch_id = catalog.batch_id
        manifest_hash = catalog.manifest_hash
        member_set_hash = catalog.member_set_hash
        members = (
            {"stock_code": "000001", "qmt_code": "000001.SZ"},
            {"stock_code": "600519", "qmt_code": "600519.SH"},
        )

        @staticmethod
        def eligible_codes(_target: str):
            return list(catalog.codes)

    monkeypatch.setattr(
        "server.common.qmt_stock_catalog.load_stock_catalog",
        lambda *_args, **_kwargs: _CatalogProof(),
    )
    # Exercise both candidate-chain discovery and immutable payload validation
    # across multiple bounded chunks while preserving the same proof result.
    monkeypatch.setattr(
        "server.common.qmt_announcement_pit.DEFAULT_BATCH_SIZE", 1
    )
    with pytest.raises(Exception, match="COMPLETE_BATCH_NOT_FOUND"):
        validate_complete_historical_reconstruction_batch(
            engine,
            codes=catalog.codes,
            decision_at=reconstructed - timedelta(seconds=1),
            window_start=target - timedelta(days=30),
            window_end=target,
            expected_trade_date=target,
        )
    proof = validate_complete_historical_reconstruction_batch(
        engine,
        codes=catalog.codes,
        decision_at=reconstructed + timedelta(seconds=1),
        window_start=target - timedelta(days=30),
        window_end=target,
        expected_trade_date=target,
    )
    assert proof["batch_id"] == result["batch_id"]
    assert proof["reconstruction_sha256"] == result[
        "reconstruction_sha256"
    ]
    subset_proof = validate_complete_historical_reconstruction_batch(
        engine,
        codes=catalog.codes,
        decision_at=reconstructed + timedelta(seconds=1),
        window_start=target - timedelta(days=20),
        window_end=target,
        expected_trade_date=target,
    )
    assert subset_proof["batch_id"] == result["batch_id"]
    assert subset_proof["window_start"] == (
        target - timedelta(days=30)
    ).isoformat()
    with pytest.raises(QMTAnnouncementBlocked) as uncovered:
        validate_complete_historical_reconstruction_batch(
            engine,
            codes=catalog.codes,
            decision_at=reconstructed + timedelta(seconds=1),
            window_start=target - timedelta(days=31),
            window_end=target,
            expected_trade_date=target,
        )
    assert uncovered.value.reason_code == (
        "QMT_ANNOUNCEMENT_COMPLETE_BATCH_NOT_FOUND"
    )
    # A later live QUERY_CUTOFF revision in the same per-stock chain must not
    # hide the immutable historical batch or trigger another provider run.
    with engine.connect() as connection:
        persisted = connection.execute(text(
            "SELECT stock_code,payload_json FROM st_pit_source_coverage "
            "WHERE batch_id=:batch_id ORDER BY stock_code"
        ), {"batch_id": result["batch_id"]}).mappings().all()
    later_results = {}
    later_receipts = {}
    for persisted_row in persisted:
        payload = json.loads(persisted_row["payload_json"])
        code = str(persisted_row["stock_code"])
        later_results[code] = payload["source_rows"]
        later_receipts[code] = payload["watermark"]["evidence"][
            "provider_receipt"
        ]
    from server.common.qmt_announcement_pit import _publish_batch, build_batch_root
    live_received = reconstructed + timedelta(minutes=1)
    live_end_time = live_received.strftime("%Y%m%d%H%M%S")
    for receipt in later_receipts.values():
        receipt["requested_end_time"] = live_end_time
        receipt["captured_at"] = live_received.isoformat(
            sep=" ", timespec="microseconds"
        )
        receipt["security_master_started_at"] = receipt["captured_at"]
        receipt["security_master_ended_at"] = receipt["captured_at"]
    live_batch_id = "cninfo-ann-20260903T130120-live"
    live_root, live_entries = build_batch_root(
        batch_id=live_batch_id,
        fact_cutoff_at=live_received,
        received_at=live_received,
        window_start=target - timedelta(days=30),
        window_end=target,
        catalog=catalog,
        results=later_results,
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        provider_receipts=later_receipts,
    )
    _publish_batch(
        engine,
        batch_id=live_batch_id,
        batch_root_hash=live_root,
        entries=live_entries,
        fact_cutoff_at=live_received,
        received_at=live_received,
        window_start=target - timedelta(days=30),
        window_end=target,
        catalog=catalog,
        results=later_results,
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        fallback_reason=next(iter(ANNOUNCEMENT_FALLBACK_REASON_CODES)),
        provider_receipts=later_receipts,
        now_fn=lambda: live_received,
    )
    reused = validate_complete_historical_reconstruction_batch(
        engine,
        codes=catalog.codes,
        decision_at=live_received + timedelta(seconds=1),
        window_start=target - timedelta(days=30),
        window_end=target,
        expected_trade_date=target,
    )
    assert reused["batch_id"] == result["batch_id"]
    for code in catalog.codes:
        append_source_coverage(
            engine,
            fact_kind="finance",
            stock_code=code,
            window_start="1900-01-01",
            window_end=target,
            known_at=live_received,
            covered_through_at=live_received,
            watermark_kind="CAPTURED_AT",
            watermark_evidence={"provider": "test.finance"},
            source_rows=[],
            fact_bindings=[],
            source="test.finance",
            batch_id="finance-20260903-live",
        )
    with monkeypatch.context() as cutoff_patch:
        cutoff_patch.setattr(
            "server.common.qmt_announcement_pit.validate_complete_announcement_batch",
            lambda *_args, **_kwargs: proof,
        )
        cutoff_patch.setattr(
            "server.common.pit_facts._live_capture_allowed",
            lambda **_kwargs: True,
        )
        common = resolve_common_fact_cutoff(
            engine,
            codes=catalog.codes,
            decision_at=live_received + timedelta(seconds=1),
            finance_start_date="1900-01-01",
            finance_end_date=target,
            event_start_date=target - timedelta(days=30),
            event_end_date=target,
            require_qmt_event_batch=True,
        )
    assert common["status"] == "AVAILABLE", common.get("reason")
    assert {
        receipt["batch_id"]
        for receipt in common["receipts"]
        if receipt["fact_kind"] == "event"
    } == {result["batch_id"]}

    from tools.sync_qmt_announcement_pit import validate_existing_task_result

    fresh = json.loads(json.dumps(result))
    fresh["validation_run_uid"] = "1" * 32
    fresh["validation_build_sha"] = "2" * 40
    assert validate_existing_task_result(
        fresh,
        0,
        expected_trade_date=target.isoformat(),
        expected_scheduler_run_uid="1" * 32,
        expected_build_sha="2" * 40,
    ) == "complete"
    drifted_fresh = json.loads(json.dumps(fresh))
    drifted_provenance = drifted_fresh["reconstruction_provenance"]
    drifted_provenance["scheduler_run_uid"] = "3" * 32
    drifted_core = {
        key: value for key, value in drifted_provenance.items()
        if key != "reconstruction_sha256"
    }
    drifted_provenance["reconstruction_sha256"] = pit_canonical_hash(
        drifted_core
    )
    drifted_fresh["reconstruction_sha256"] = drifted_provenance[
        "reconstruction_sha256"
    ]
    with pytest.raises(ValueError, match="historical recovery result differs"):
        validate_existing_task_result(
            drifted_fresh,
            0,
            expected_trade_date=target.isoformat(),
            expected_scheduler_run_uid="1" * 32,
            expected_build_sha="2" * 40,
        )
    assert validate_task_result(result, 0) == "complete"
    tampered = json.loads(json.dumps(result))
    tampered["reconstruction_provenance"]["provider"] = "other"
    with pytest.raises(ValueError, match="provenance"):
        validate_task_result(tampered, 0)
    nested = json.loads(json.dumps(result))
    nested_authority = nested["reconstruction_provenance"]["authority"]
    nested_authority["qmt_daily_truth"]["attested_row_count"] = 2
    nested_authority["qmt_daily_truth_sha256"] = pit_canonical_hash(
        nested_authority["qmt_daily_truth"]
    )
    nested_core = {
        key: value
        for key, value in nested["reconstruction_provenance"].items()
        if key != "reconstruction_sha256"
    }
    nested["reconstruction_provenance"]["reconstruction_sha256"] = (
        pit_canonical_hash(nested_core)
    )
    nested["reconstruction_sha256"] = nested[
        "reconstruction_provenance"
    ]["reconstruction_sha256"]
    with pytest.raises(ValueError, match="provenance"):
        validate_task_result(nested, 0)
    polluted = json.loads(json.dumps(result))
    polluted_authority = polluted["reconstruction_provenance"]["authority"]
    polluted_authority["membership_snapshot"] = {
        "snapshot_date": target.isoformat(),
        "quality_status": "QMT_VALIDATED",
    }
    polluted_authority["membership_snapshot_sha256"] = pit_canonical_hash(
        polluted_authority["membership_snapshot"]
    )
    polluted_core = {
        key: value
        for key, value in polluted["reconstruction_provenance"].items()
        if key != "reconstruction_sha256"
    }
    polluted["reconstruction_provenance"]["reconstruction_sha256"] = (
        pit_canonical_hash(polluted_core)
    )
    polluted["reconstruction_sha256"] = polluted[
        "reconstruction_provenance"
    ]["reconstruction_sha256"]
    with pytest.raises(ValueError, match="provenance"):
        validate_task_result(polluted, 0)
    mistimed = json.loads(json.dumps(result))
    mistimed_provenance = mistimed["reconstruction_provenance"]
    mistimed_provenance["security_master"]["ended_at"] = (
        "2026-09-03 12:59:59.000000"
    )
    mistimed_core = {
        key: value for key, value in mistimed_provenance.items()
        if key != "reconstruction_sha256"
    }
    mistimed_provenance["reconstruction_sha256"] = pit_canonical_hash(
        mistimed_core
    )
    mistimed["reconstruction_sha256"] = mistimed_provenance[
        "reconstruction_sha256"
    ]
    with pytest.raises(ValueError, match="provenance"):
        validate_task_result(mistimed, 0)

    before = load_event_facts(
        engine,
        codes=catalog.codes,
        decision_at=reconstructed - timedelta(seconds=1),
        fact_cutoff_at=datetime.combine(target, datetime.max.time()),
        start_date=target - timedelta(days=30),
        end_date=target,
        require_qmt_complete_batch=True,
    )
    after = load_event_facts(
        engine,
        codes=catalog.codes,
        decision_at=reconstructed + timedelta(seconds=1),
        fact_cutoff_at=datetime.combine(target, datetime.max.time()),
        start_date=target - timedelta(days=30),
        end_date=target,
        require_qmt_complete_batch=True,
    )
    before_statuses = {
        code: before.status_for(code) for code in catalog.codes
    }
    after_statuses = {
        code: after.status_for(code) for code in catalog.codes
    }
    assert all(
        status not in {"AVAILABLE", "NO_ROWS"}
        for status in before_statuses.values()
    ), before_statuses
    assert all(
        status in {"AVAILABLE", "NO_ROWS"}
        for status in after_statuses.values()
    ), after_statuses

    with engine.connect() as connection:
        rows = connection.execute(text(
            "SELECT watermark_kind,known_at,covered_through_at,payload_json "
            "FROM st_pit_source_coverage WHERE fact_kind='event' "
            "ORDER BY stock_code"
        )).mappings().all()
    assert {row["watermark_kind"] for row in rows} == {
        "HISTORICAL_RECONSTRUCTION", "QUERY_CUTOFF",
    }
    historical_rows = [
        row for row in rows
        if row["watermark_kind"] == "HISTORICAL_RECONSTRUCTION"
    ]
    assert {str(row["known_at"]) for row in historical_rows} == {
        "2026-09-03 13:00:20"
    }
    assert {str(row["covered_through_at"]) for row in historical_rows} == {
        "2026-09-01 23:59:59.999999"
    }
    payload = json.loads(historical_rows[0]["payload_json"])
    provenance = payload["watermark"]["evidence"][
        "reconstruction_provenance"
    ]
    assert provenance["known_at"] == "2026-09-03T13:00:20.000000"
    assert provenance["source_query_cutoff_at"].startswith(
        "2026-09-01T23:59:59"
    )
    assert provenance["automatic_real_order_submission"] is False


def test_cninfo_rejects_provider_row_after_historical_target_cutoff() -> None:
    class _LeakyClient(_Client):
        def post(self, _url: str, *, data: dict) -> _Response:
            code = str(data["stock"]).split(",", 1)[0]
            return _Response({
                "pageNum": int(data["pageNum"]),
                "totalRecordNum": 1,
                "totalpages": 0,
                "hasMore": False,
                "announcements": [
                    _row_on_date(code, 0, date(2026, 9, 2))
                ],
            })

    captured = datetime(2026, 9, 3, 13, 0, 10)
    client = _LeakyClient(
        masters=[_master("000001")], rows_by_code={}
    )
    provider = CninfoMarketAnnouncementProvider(
        client=client,
        minimum_request_interval=0,
        now_fn=lambda: captured,
    )
    provider.bind_capture_deadline(
        remaining_seconds=3600,
    )

    with pytest.raises(AnnouncementProviderError) as exc:
        provider.fetch(
            stock_code="000001",
            qmt_code="000001.SZ",
            requested_start_time="20260802000000",
            requested_end_time="20260901235959",
        )

    assert exc.value.reason_code == "ANNOUNCEMENT_FALLBACK_ROW_INVALID"


def test_cninfo_failed_stock_resumes_same_cutoff_with_staged_receipt(
    monkeypatch, tmp_path
) -> None:
    cutoff = datetime.now().replace(microsecond=0)
    master = _master("000001", "600519")
    catalog = AnnouncementCatalog(
        batch_id="catalog-cninfo-resume",
        manifest_hash="c" * 64,
        member_set_hash="d" * 64,
        codes=("000001", "600519"),
        qmt_by_code={"000001": "000001.SZ", "600519": "600519.SH"},
    )
    monkeypatch.setattr(
        "server.common.qmt_announcement_pit._load_catalog",
        lambda _engine, _cutoff: catalog,
    )
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_pit_fact_schema(engine)
    fallback_reason = "QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE"
    first_client = _Client(
        masters=[master], rows_by_code={"000001": [_row("000001", 0)]}
    )
    first_provider = _FailOnCodeProvider(
        client=first_client,
        fail_code="600519",
        minimum_request_interval=0,
        now_fn=lambda: cutoff + timedelta(seconds=1),
    )
    first = synchronize_qmt_announcements(
        engine,
        xtdata=ProviderBackedAnnouncementAdapter(first_provider, workers=1),
        checkpoint_root=tmp_path,
        now_fn=lambda: cutoff + timedelta(seconds=2),
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        fallback_reason=fallback_reason,
        capture_fact_cutoff_at=cutoff,
        resume=True,
        batch_size=100,
    )

    assert first["status"] == "DATA_BLOCKED"
    assert first["reason_code"] == "ANNOUNCEMENT_FALLBACK_SOURCE_RATE_LIMITED"
    assert first_client.requested_stock_identities == ["000001,org-000001"]
    staged = list(tmp_path.glob("cninfo-ann-*/results/*.json"))
    assert [path.stem for path in staged] == ["000001"]
    unrelated = tmp_path / "zz-unrelated-checkpoint"
    unrelated.mkdir()
    (unrelated / "manifest.json").write_text(
        '{"schema":"unrelated"}', encoding="utf-8"
    )

    second_client = _Client(
        masters=[master, master], rows_by_code={"600519": []}
    )
    second_provider = CninfoMarketAnnouncementProvider(
        client=second_client,
        minimum_request_interval=0,
        now_fn=lambda: cutoff + timedelta(seconds=3),
    )
    second = synchronize_qmt_announcements(
        engine,
        xtdata=ProviderBackedAnnouncementAdapter(second_provider, workers=1),
        checkpoint_root=tmp_path,
        now_fn=lambda: cutoff + timedelta(seconds=4),
        source=CNINFO_ANNOUNCEMENT_SOURCE,
        fallback_reason=fallback_reason,
        capture_fact_cutoff_at=cutoff + timedelta(seconds=2),
        resume=True,
        batch_size=100,
    )

    assert second["status"] == "COMPLETE"
    assert second["fact_cutoff_at"] == cutoff.isoformat(
        timespec="microseconds"
    )
    assert second["coverage_count"] == 2
    assert second_client.requested_stock_identities == ["600519,org-600519"]


def test_historical_reconstruction_retry_resumes_same_sealed_checkpoint(
    tmp_path,
) -> None:
    target = date(2026, 9, 1)
    first_started = datetime.now().replace(microsecond=0)
    second_started = first_started + timedelta(minutes=1)
    catalog = AnnouncementCatalog(
        batch_id="catalog-historical-resume",
        manifest_hash="a" * 64,
        member_set_hash="b" * 64,
        codes=("000001", "600519"),
        qmt_by_code={"000001": "000001.SZ", "600519": "600519.SH"},
    )
    authority = _historical_authority(catalog, target)
    master = _master(*catalog.codes)
    first_client = _Client(
        masters=[master],
        rows_by_code={"000001": [_row_on_date("000001", 0, target)]},
    )
    first_provider = _FailOnCodeProvider(
        client=first_client,
        fail_code="600519",
        minimum_request_interval=0,
        now_fn=lambda: first_started + timedelta(seconds=10),
    )
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_pit_fact_schema(engine)
    with pytest.raises(QMTAnnouncementBlocked) as exc:
        synchronize_historical_cninfo_announcements(
            engine,
            adapter=ProviderBackedAnnouncementAdapter(first_provider, workers=1),
            checkpoint_root=tmp_path,
            catalog=catalog,
            context=HistoricalReconstructionContext(
                target_trade_date=target,
                source_query_cutoff_at=datetime.combine(target, datetime.max.time()),
                reconstruction_started_at=first_started,
                scheduler_run_uid="1" * 32,
                build_sha="2" * 40,
                authority=authority,
            ),
            now_fn=lambda: first_started,
            batch_size=2,
        )
    assert exc.value.reason_code == "ANNOUNCEMENT_FALLBACK_SOURCE_RATE_LIMITED"
    assert first_client.requested_stock_identities == ["000001,org-000001"]

    second_client = _Client(
        masters=[master, master],
        rows_by_code={"600519": []},
    )
    second_provider = CninfoMarketAnnouncementProvider(
        client=second_client,
        minimum_request_interval=0,
        now_fn=lambda: second_started + timedelta(seconds=10),
    )
    result = synchronize_historical_cninfo_announcements(
        engine,
        adapter=ProviderBackedAnnouncementAdapter(second_provider, workers=1),
        checkpoint_root=tmp_path,
        catalog=catalog,
        context=HistoricalReconstructionContext(
            target_trade_date=target,
            source_query_cutoff_at=datetime.combine(target, datetime.max.time()),
            reconstruction_started_at=second_started,
            scheduler_run_uid="3" * 32,
            build_sha="2" * 40,
            authority=authority,
        ),
        now_fn=lambda: second_started + timedelta(seconds=20),
        batch_size=2,
    )

    assert result["status"] == "COMPLETE"
    assert result["reconstruction_provenance"]["scheduler_run_uid"] == "3" * 32
    assert result["reconstruction_provenance"]["reconstruction_started_at"] == (
        first_started + timedelta(seconds=10)
    ).isoformat(timespec="microseconds")
    assert second_client.requested_stock_identities == ["600519,org-600519"]
    assert len(list(tmp_path.glob("cninfo-ann-reconstruction-*"))) == 1
