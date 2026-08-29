"""Auditable non-QMT announcement provider contract.

Providers in this module never publish PIT rows themselves.  They must return
one exhaustive, source-identified receipt per requested catalog member.  The
atomic publisher in :mod:`server.common.qmt_announcement_pit` verifies and
hash-binds every receipt before any database write becomes visible.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import random
import re
import threading
import time
from typing import Any, Callable, Mapping, Protocol

import pandas as pd


EASTMONEY_SOURCE = "eastmoney.notice"
CNINFO_SOURCE = "cninfo.announcement"
PROVIDER_RECEIPT_SCHEMA = "probiga.announcement-provider-receipt.v1"
CNINFO_PROVIDER_RECEIPT_SCHEMA = (
    "probiga.cninfo-announcement-provider-receipt.v3"
)
CNINFO_STEADY_REQUEST_INTERVAL_SECONDS = 0.08
CNINFO_MAX_PAGES_PER_STOCK = 200
CNINFO_DATE_SHARD_MIN_STABLE_ROUNDS = 2
CNINFO_DATE_SHARD_MAX_CAPTURE_ROUNDS = 8
CNINFO_DATE_SHARD_SPLIT_VERSION = "MIDPOINT_INCLUSIVE_V1"
ANNOUNCEMENT_DB_PUBLISH_RESERVE_SECONDS = 60.0
_QMT_CODE_RE = re.compile(r"^(?:0|3|4|6|8|9)\d{5}\.(?:SH|SZ|BJ)$")


class AnnouncementProviderError(RuntimeError):
    """One source failure with a stable machine-readable disposition."""

    def __init__(self, reason_code: str, detail: str = "") -> None:
        self.reason_code = str(reason_code or "ANNOUNCEMENT_PROVIDER_FAILED")
        self.detail = str(detail or "")[:1000]
        super().__init__(
            self.reason_code
            if not self.detail else f"{self.reason_code}:{self.detail}"
        )


@dataclass(frozen=True)
class ProviderResult:
    rows: tuple[dict[str, Any], ...]
    receipt: dict[str, Any]


class AnnouncementProvider(Protocol):
    source: str

    def fetch(
        self,
        *,
        stock_code: str,
        qmt_code: str,
        requested_start_time: str,
        requested_end_time: str,
    ) -> ProviderResult: ...

    def close(self) -> None: ...


def _canonical_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _compact_datetime(value: str) -> datetime:
    raw = str(value or "").strip()
    if re.fullmatch(r"\d{14}", raw) is None:
        raise ValueError("provider request time must be YYYYmmddHHMMSS")
    return datetime.strptime(raw, "%Y%m%d%H%M%S")


class EastmoneyAnnouncementProvider:
    """Exact per-stock/date-window provider backed by the existing API client."""

    source = EASTMONEY_SOURCE

    def __init__(
        self,
        *,
        page_size: int = 100,
        max_pages: int = 1000,
        minimum_request_interval: float = 0.35,
        client: Any | None = None,
        fetch_pages_fn: Callable[..., Any] | None = None,
        parse_item_fn: Callable[..., dict[str, Any]] | None = None,
    ) -> None:
        if not 1 <= int(page_size) <= 100:
            raise ValueError("Eastmoney notice page_size must be 1..100")
        if not 1 <= int(max_pages) <= 1000:
            raise ValueError("Eastmoney notice max_pages must be 1..1000")
        if not 0.0 <= float(minimum_request_interval) <= 10.0:
            raise ValueError("Eastmoney notice request interval is invalid")
        from biz.notice import sync_notice_em

        if sync_notice_em.httpx is None:
            raise RuntimeError("httpx is required for Eastmoney notices")
        self._page_size = int(page_size)
        self._max_pages = int(max_pages)
        self._minimum_interval = float(minimum_request_interval)
        self._fetch_pages = fetch_pages_fn or sync_notice_em.fetch_pages
        self._parse_item = parse_item_fn or sync_notice_em._parse_item
        self._data_version = sync_notice_em.NOTICE_DATA_VERSION
        self._client = client or sync_notice_em.httpx.Client(
            headers={"User-Agent": "Mozilla/5.0 ProBigA-announcement-pit"},
            trust_env=False,
        )
        self._owns_client = client is None
        self._rate_lock = threading.Lock()
        self._last_request_monotonic = 0.0

    def _rate_limit(self) -> None:
        with self._rate_lock:
            remaining = (
                self._last_request_monotonic
                + self._minimum_interval
                - time.monotonic()
            )
            if remaining > 0:
                time.sleep(remaining)
            self._last_request_monotonic = time.monotonic()

    def fetch(
        self,
        *,
        stock_code: str,
        qmt_code: str,
        requested_start_time: str,
        requested_end_time: str,
    ) -> ProviderResult:
        code = str(stock_code or "").strip().zfill(6)
        instrument = str(qmt_code or "").strip().upper()
        start_at = _compact_datetime(requested_start_time)
        cutoff = _compact_datetime(requested_end_time)
        if (
            re.fullmatch(r"\d{6}", code) is None
            or _QMT_CODE_RE.fullmatch(instrument) is None
            or instrument[:6] != code
            or start_at > cutoff
        ):
            raise ValueError("Eastmoney announcement request identity differs")
        self._rate_limit()
        try:
            fetched = self._fetch_pages(
                self._client,
                code,
                page_size=self._page_size,
                max_pages=self._max_pages,
                begin_date=start_at.date(),
                end_date=cutoff.date(),
            )
        except Exception as exc:
            response = getattr(exc, "response", None)
            status_code = int(getattr(response, "status_code", 0) or 0)
            if status_code in {429, 567}:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_SOURCE_RATE_LIMITED",
                    f"http-{status_code}",
                ) from exc
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_SOURCE_QUERY_FAILED",
                type(exc).__name__,
            ) from exc
        if (
            fetched.bounded is not True
            or fetched.exhausted is not True
            or fetched.window_start != start_at.date()
            or fetched.window_end != cutoff.date()
            or fetched.total_hits != len(fetched.rows)
            or fetched.expected_pages < 0
            or fetched.page_count < 1
        ):
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE", code
            )
        provider_batch_id = _canonical_hash({
            "schema": "probiga.eastmoney-announcement-query.v1",
            "stock_code": code,
            "qmt_code": instrument,
            "requested_start_time": requested_start_time,
            "requested_end_time": requested_end_time,
            "captured_at": fetched.captured_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "total_hits": fetched.total_hits,
            "rows": fetched.rows,
        })
        rows: list[dict[str, Any]] = []
        for raw in fetched.rows:
            parsed = self._parse_item(
                code,
                raw,
                fetched.captured_at,
                validated_stock_identity=True,
                batch_id=provider_batch_id,
            )
            published_at = str(parsed.get("published_at") or "").strip()
            if not published_at:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_PUBLICATION_TIME_UNVERIFIED", code
                )
            published = datetime.fromisoformat(published_at)
            if published > cutoff:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_FUTURE_PUBLICATION", code
                )
            rows.append({
                **dict(raw),
                "stock_code": code,
                "qmt_code": instrument,
                "art_code": parsed["art_code"],
                "title": parsed["title"],
                "notice_date": str(parsed["notice_date"] or ""),
                "published_at": published_at,
                "detail_url": parsed["detail_url"],
                "provider": self.source,
                "provider_data_version": self._data_version,
                "source_row_hash": _canonical_hash(raw),
                "provider_batch_id": provider_batch_id,
                "provider_captured_at": fetched.captured_at.isoformat(
                    sep=" ", timespec="microseconds"
                ),
            })
        payload_hash = _canonical_hash({
            "schema": "probiga.eastmoney-announcement-response.v1",
            "stock_code": code,
            "requested_start_time": requested_start_time,
            "requested_end_time": requested_end_time,
            "total_hits": fetched.total_hits,
            "page_count": fetched.page_count,
            "expected_pages": fetched.expected_pages,
            "rows": rows,
        })
        receipt = {
            "schema": PROVIDER_RECEIPT_SCHEMA,
            "status": "COMPLETE",
            "source": self.source,
            "stock_code": code,
            "qmt_code": instrument,
            "requested_start_time": requested_start_time,
            "requested_end_time": requested_end_time,
            "captured_at": fetched.captured_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "exhausted": True,
            "page_count": int(fetched.page_count),
            "expected_pages": int(fetched.expected_pages),
            "result_count": len(rows),
            "provider_payload_sha256": payload_hash,
            "permission_status": "PUBLIC",
            "quality_status": "SOURCE_IDENTITY_VALIDATED",
            "data_version": self._data_version,
        }
        return ProviderResult(tuple(rows), receipt)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


class _UnsupportedCninfoWideMarketProvider:
    """Official market-wide CNInfo pagination, fetched once then partitioned.

    CNInfo exposes millisecond publication timestamps.  Exact values are
    retained; a source value exactly at midnight is conservatively downgraded
    by the PIT parser to a date marker instead of being promoted to exact
    intraday evidence.
    """

    source = CNINFO_SOURCE
    endpoint = "https://www.cninfo.com.cn/new/hisAnnouncement/query"

    def __init__(
        self,
        *,
        page_size: int = 30,
        minimum_request_interval: float = 0.05,
        workers: int = 4,
        client: Any | None = None,
        now_fn: Callable[[], datetime] = datetime.now,
    ) -> None:
        raise RuntimeError(
            "CNInfo broad-market pagination is forbidden; use exact stock identity"
        )
        if int(page_size) != 30:
            raise ValueError("CNInfo full-market page size must be 30")
        if not 0.0 <= float(minimum_request_interval) <= 2.0:
            raise ValueError("CNInfo request interval is invalid")
        if not 1 <= int(workers) <= 4:
            raise ValueError("CNInfo pagination workers must be 1..4")
        from biz.notice import sync_notice_em

        if sync_notice_em.httpx is None:
            raise RuntimeError("httpx is required for CNInfo announcements")
        self._page_size = int(page_size)
        self._minimum_interval = float(minimum_request_interval)
        self._workers = int(workers)
        self._client = client or sync_notice_em.httpx.Client(
            headers={
                "User-Agent": "Mozilla/5.0 ProBigA-announcement-pit",
                "Referer": "https://www.cninfo.com.cn/",
            },
            trust_env=False,
            timeout=30.0,
        )
        self._owns_client = client is None
        self._now_fn = now_fn
        self._capture_key: tuple[str, str] | None = None
        self._capture_lock = threading.Lock()
        self._rows_by_code: dict[str, tuple[dict[str, Any], ...]] = {}
        self._market_receipt: dict[str, Any] = {}

    @staticmethod
    def _publication_datetime(value: Any) -> datetime:
        try:
            numeric = int(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PUBLICATION_DATE_INVALID"
            ) from exc
        if not 1_000_000_000_000 <= numeric < 10_000_000_000_000:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PUBLICATION_DATE_INVALID"
            )
        from zoneinfo import ZoneInfo

        return datetime.fromtimestamp(
            numeric / 1000.0, tz=ZoneInfo("UTC")
        ).astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)

    def _post_page(self, page: int, start: date, end: date) -> Mapping[str, Any]:
        payload = {
            "pageNum": str(page),
            "pageSize": str(self._page_size),
            "column": "szse",
            "tabName": "fulltext",
            "plate": "",
            "stock": "__UNSUPPORTED_WIDE_QUERY__",
            "searchkey": "",
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": f"{start.isoformat()}~{end.isoformat()}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }
        last_error: Exception | None = None
        for attempt in range(3):
            if self._minimum_interval:
                time.sleep(self._minimum_interval)
            try:
                response = self._client.post(self.endpoint, data=payload)
                response.raise_for_status()
                try:
                    response.encoding = "utf-8"
                except Exception:
                    pass
                body = response.json()
                if not isinstance(body, Mapping):
                    raise RuntimeError("CNInfo response is not an object")
                return body
            except Exception as exc:
                last_error = exc
                status_code = int(
                    getattr(getattr(exc, "response", None), "status_code", 0)
                    or 0
                )
                if status_code in {429, 567}:
                    reason = "ANNOUNCEMENT_FALLBACK_SOURCE_RATE_LIMITED"
                else:
                    reason = "ANNOUNCEMENT_FALLBACK_SOURCE_QUERY_FAILED"
                if attempt == 2:
                    raise AnnouncementProviderError(
                        reason,
                        f"page={page},error={type(exc).__name__}",
                    ) from exc
                time.sleep(float(attempt + 1))
        raise AnnouncementProviderError(
            "ANNOUNCEMENT_FALLBACK_SOURCE_QUERY_FAILED",
            type(last_error).__name__ if last_error else "unknown",
        )

    def _capture_market(self, start_at: datetime, cutoff: datetime) -> None:
        key = (
            start_at.strftime("%Y%m%d%H%M%S"),
            cutoff.strftime("%Y%m%d%H%M%S"),
        )
        if self._capture_key == key:
            return
        first = self._post_page(1, start_at.date(), cutoff.date())
        try:
            total_records = int(first.get("totalRecordNum"))
            total_pages = int(first.get("totalpages"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE", "metadata"
            ) from exc
        if total_records < 0 or total_pages < 0:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE", "negative"
            )
        expected_pages = (
            (total_records + self._page_size - 1) // self._page_size
            if total_records else 0
        )
        if total_pages != expected_pages:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE",
                "page-count",
            )
        pages = [first] if total_pages else []
        def fetch_page(page: int) -> tuple[int, Mapping[str, Any]]:
            return page, self._post_page(
                page, start_at.date(), cutoff.date()
            )

        if total_pages > 1:
            with ThreadPoolExecutor(max_workers=self._workers) as pool:
                for offset in range(2, total_pages + 1, 32):
                    page_numbers = list(
                        range(offset, min(total_pages + 1, offset + 32))
                    )
                    for page, current in pool.map(fetch_page, page_numbers):
                        if (
                            int(current.get("totalRecordNum") or -1)
                            != total_records
                            or int(current.get("totalpages") or -1)
                            != total_pages
                        ):
                            raise AnnouncementProviderError(
                                "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED",
                                str(page),
                            )
                        pages.append(current)
        raw_rows: list[dict[str, Any]] = []
        for page_index, page in enumerate(pages, 1):
            rows = page.get("announcements")
            if not isinstance(rows, list):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE",
                    f"page={page_index}",
                )
            expected_rows = (
                self._page_size
                if page_index < total_pages
                else total_records - self._page_size * (total_pages - 1)
            )
            if len(rows) != expected_rows or any(
                not isinstance(item, Mapping) for item in rows
            ):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE",
                    f"page={page_index},rows={len(rows)}",
                )
            raw_rows.extend(dict(item) for item in rows)
        if len(raw_rows) != total_records:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE", "total"
            )
        captured_at = self._now_fn().replace(tzinfo=None)
        by_code: dict[str, list[dict[str, Any]]] = {}
        identities: dict[tuple[str, str], dict[str, Any]] = {}
        for raw in raw_rows:
            code = str(raw.get("secCode") or "").strip().zfill(6)
            event_id = str(raw.get("announcementId") or "").strip()
            title = str(raw.get("announcementTitle") or "").strip()
            published_at = self._publication_datetime(
                raw.get("announcementTime")
            )
            published_date = published_at.date()
            if (
                re.fullmatch(r"\d{6}", code) is None
                or not event_id
                or not title
                or not start_at.date() <= published_date <= cutoff.date()
                or published_at > cutoff
            ):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_ROW_INVALID", code
                )
            identity = (code, event_id)
            normalized = {
                "stock_code": code,
                "announcement_id": event_id,
                "announcementTime": raw.get("announcementTime"),
                "notice_date": published_date.isoformat(),
                "published_at": published_at.isoformat(
                    sep=" ", timespec="microseconds"
                ),
                "source_published_text": published_date.isoformat(),
                "title": title,
                "detail_url": (
                    "https://static.cninfo.com.cn/"
                    + str(raw.get("adjunctUrl") or "").lstrip("/")
                ),
                "provider": self.source,
                "provider_captured_at": captured_at.isoformat(
                    sep=" ", timespec="microseconds"
                ),
                "source_row_hash": _canonical_hash(raw),
            }
            previous = identities.get(identity)
            if previous is not None:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED",
                    f"duplicate={code}:{event_id}",
                )
            identities[identity] = normalized
            by_code.setdefault(code, []).append(normalized)
        for code, rows in by_code.items():
            rows.sort(key=lambda item: (
                str(item["notice_date"]), str(item["announcement_id"])
            ))
        market_payload_hash = _canonical_hash({
            "schema": "probiga.cninfo-market-announcement-response.v1",
            "requested_start_time": key[0],
            "requested_end_time": key[1],
            "total_record_count": total_records,
            "total_page_count": total_pages,
            "rows": raw_rows,
        })
        self._capture_key = key
        self._rows_by_code = {
            code: tuple(rows) for code, rows in by_code.items()
        }
        self._market_receipt = {
            "schema": "probiga.cninfo-market-announcement-receipt.v1",
            "status": "COMPLETE",
            "source": self.source,
            "requested_start_time": key[0],
            "requested_end_time": key[1],
            "captured_at": captured_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "exhausted": True,
            "page_size": self._page_size,
            "page_count": total_pages,
            "result_count": total_records,
            "provider_payload_sha256": market_payload_hash,
        }

    def _ensure_market_capture(self, start_at: datetime, cutoff: datetime) -> None:
        key = (
            start_at.strftime("%Y%m%d%H%M%S"),
            cutoff.strftime("%Y%m%d%H%M%S"),
        )
        if self._capture_key == key:
            return
        with self._capture_lock:
            if self._capture_key == key:
                return
            self._capture_market(start_at, cutoff)

    def fetch(
        self,
        *,
        stock_code: str,
        qmt_code: str,
        requested_start_time: str,
        requested_end_time: str,
    ) -> ProviderResult:
        code = str(stock_code or "").strip().zfill(6)
        instrument = str(qmt_code or "").strip().upper()
        start_at = _compact_datetime(requested_start_time)
        cutoff = _compact_datetime(requested_end_time)
        if (
            re.fullmatch(r"\d{6}", code) is None
            or _QMT_CODE_RE.fullmatch(instrument) is None
            or instrument[:6] != code
            or start_at > cutoff
        ):
            raise ValueError("CNInfo announcement request identity differs")
        self._ensure_market_capture(start_at, cutoff)
        rows = tuple(dict(item) for item in self._rows_by_code.get(code, ()))
        market_hash = str(
            self._market_receipt.get("provider_payload_sha256") or ""
        )
        receipt = {
            "schema": PROVIDER_RECEIPT_SCHEMA,
            "status": "COMPLETE",
            "source": self.source,
            "stock_code": code,
            "qmt_code": instrument,
            "requested_start_time": requested_start_time,
            "requested_end_time": requested_end_time,
            "captured_at": self._market_receipt["captured_at"],
            "exhausted": True,
            "page_count": int(self._market_receipt["page_count"]),
            "expected_pages": int(self._market_receipt["page_count"]),
            "result_count": len(rows),
            "provider_payload_sha256": _canonical_hash({
                "schema": "probiga.cninfo-stock-announcement-partition.v1",
                "market_payload_sha256": market_hash,
                "stock_code": code,
                "rows": rows,
            }),
            "market_payload_sha256": market_hash,
            "market_result_count": int(self._market_receipt["result_count"]),
            "permission_status": "PUBLIC",
            "quality_status": "MARKET_PAGINATION_EXHAUSTED",
            "data_version": _canonical_hash(
                "probiga.cninfo-market-announcement.v1"
            ),
        }
        return ProviderResult(rows, receipt)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


class CninfoMarketAnnouncementProvider:
    """Official CNInfo master plus exact ``code,orgId`` stock queries.

    Broad-market pagination is intentionally forbidden because that endpoint
    can drift or wrap while paging.  Every catalog member is queried alone,
    including members with zero rows.  The public security master is fetched
    before capture and again while receipts are sealed; drift blocks the whole
    batch.
    """

    source = CNINFO_SOURCE
    endpoint = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
    security_master_endpoint = (
        "https://www.cninfo.com.cn/new/data/szse_stock.json"
    )

    def __init__(
        self,
        *,
        page_size: int = 30,
        minimum_request_interval: float = (
            CNINFO_STEADY_REQUEST_INTERVAL_SECONDS
        ),
        client: Any | None = None,
        now_fn: Callable[[], datetime] = datetime.now,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
        jitter_fn: Callable[[float, float], float] = random.uniform,
    ) -> None:
        if int(page_size) != 30:
            raise ValueError("CNInfo per-stock page size must be 30")
        if not 0.0 <= float(minimum_request_interval) <= 2.0:
            raise ValueError("CNInfo request interval is invalid")
        from biz.notice import sync_notice_em

        if sync_notice_em.httpx is None:
            raise RuntimeError("httpx is required for CNInfo announcements")
        self._page_size = int(page_size)
        self._minimum_interval = float(minimum_request_interval)
        self._httpx = sync_notice_em.httpx
        self._client_headers = {
            "User-Agent": "Mozilla/5.0 ProBigA-announcement-pit",
            "Referer": "https://www.cninfo.com.cn/",
        }
        self._client = client or self._new_client()
        self._owns_client = client is None
        self._now_fn = now_fn
        self._sleep = sleep_fn
        self._monotonic = monotonic_fn
        self._jitter = jitter_fn
        self._rate_lock = threading.Lock()
        self._next_request_monotonic = 0.0
        self._deadline_monotonic: float | None = None
        self._client_lock = threading.Lock()
        self._master_lock = threading.Lock()
        self._master_by_code: dict[str, dict[str, str]] | None = None
        self._master_start_hash = ""
        self._master_raw_hash = ""
        self._master_member_set_hash = ""
        self._master_start_captured_at = ""
        self._fetched_codes: set[str] = set()
        self._fetched_lock = threading.Lock()

    def bind_capture_deadline(self, *, remaining_seconds: float) -> None:
        remaining = float(remaining_seconds)
        if remaining <= 0:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"
            )
        self._deadline_monotonic = self._monotonic() + remaining

    def _check_deadline(self) -> None:
        if (
            self._deadline_monotonic is not None
            and self._monotonic() >= self._deadline_monotonic
        ):
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"
            )

    def _new_client(self) -> Any:
        return self._httpx.Client(
            headers=dict(self._client_headers),
            trust_env=False,
            timeout=30.0,
        )

    def _rate_limit(self) -> None:
        with self._rate_lock:
            now = self._monotonic()
            remaining = self._next_request_monotonic - now
            if (
                self._deadline_monotonic is not None
                and now + max(0.0, remaining) >= self._deadline_monotonic
            ):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"
                )
            if remaining > 0:
                self._sleep(remaining)
                now = self._monotonic()
            if (
                self._deadline_monotonic is not None
                and now >= self._deadline_monotonic
            ):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"
                )
            self._next_request_monotonic = max(
                self._next_request_monotonic, now
            ) + self._minimum_interval

    def _defer_all_requests(self, seconds: float) -> None:
        with self._rate_lock:
            deferred = max(
                self._next_request_monotonic,
                self._monotonic() + max(0.0, float(seconds)),
            )
            if (
                self._deadline_monotonic is not None
                and deferred >= self._deadline_monotonic
            ):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"
                )
            self._next_request_monotonic = deferred

    @staticmethod
    def _retry_after_seconds(response: Any) -> float:
        headers = getattr(response, "headers", None)
        raw = str(headers.get("Retry-After") or "").strip() if headers else ""
        if not raw:
            return 0.0
        try:
            return max(0.0, min(120.0, float(raw)))
        except ValueError:
            try:
                parsed = parsedate_to_datetime(raw)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return max(0.0, min(
                    120.0,
                    (parsed - datetime.now(timezone.utc)).total_seconds(),
                ))
            except (TypeError, ValueError, OverflowError):
                return 0.0

    def _reset_owned_session(self) -> bool:
        if not self._owns_client:
            return False
        with self._client_lock:
            previous = self._client
            self._client = self._new_client()
            try:
                previous.close()
            except Exception:
                pass
        return True

    def _prime_owned_session(self) -> None:
        """Refresh CNInfo cookies and prove the directory did not drift."""

        self._rate_limit()
        with self._client_lock:
            response = self._client.get(self.security_master_endpoint)
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, Mapping) or not isinstance(
            body.get("stockList"), list
        ):
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_INCOMPLETE",
                "session-refresh",
            )
        if self._master_raw_hash and _canonical_hash(body) != self._master_raw_hash:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_CHANGED",
                "session-refresh",
            )

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        detail: str,
        data: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any]:
        refresh_before_request = False
        for attempt in range(4):
            try:
                if refresh_before_request:
                    self._prime_owned_session()
                    refresh_before_request = False
                self._rate_limit()
                with self._client_lock:
                    response = (
                        self._client.get(url)
                        if method == "GET"
                        else self._client.post(url, data=dict(data or {}))
                    )
                response.raise_for_status()
                try:
                    response.encoding = "utf-8"
                except Exception:
                    pass
                body = response.json()
                if not isinstance(body, Mapping):
                    raise RuntimeError("CNInfo response is not an object")
                self._check_deadline()
                return body
            except AnnouncementProviderError:
                raise
            except Exception as exc:
                error_response = getattr(exc, "response", None)
                status_code = int(getattr(error_response, "status_code", 0) or 0)
                rate_limited = status_code in {403, 429, 567}
                transient_gateway = status_code in {
                    408, 500, 502, 503, 504
                }
                reason = (
                    "ANNOUNCEMENT_FALLBACK_SOURCE_RATE_LIMITED"
                    if rate_limited
                    else "ANNOUNCEMENT_FALLBACK_SOURCE_QUERY_FAILED"
                )
                if not (rate_limited or transient_gateway) or attempt == 3:
                    raise AnnouncementProviderError(
                        reason,
                        f"{detail},error={type(exc).__name__}"
                        + (f",http={status_code}" if status_code else ""),
                    ) from exc
                retry_after = self._retry_after_seconds(error_response)
                exponential = min(
                    30.0,
                    2.0 ** (attempt + 1)
                    if rate_limited
                    else 2.0 ** attempt,
                )
                delay = max(retry_after, exponential) + max(
                    0.0, float(self._jitter(0.0, 0.5))
                )
                self._defer_all_requests(delay)
                # Gateway failures keep the proven session/cookies.  Only an
                # explicit source-side throttle rebuilds and re-primes it.
                refresh_before_request = (
                    self._reset_owned_session() if rate_limited else False
                )
        raise AssertionError("CNInfo retry loop did not terminate")

    @staticmethod
    def _publication_datetime(value: Any) -> datetime:
        try:
            numeric = int(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PUBLICATION_DATE_INVALID"
            ) from exc
        if not 1_000_000_000_000 <= numeric < 10_000_000_000_000:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PUBLICATION_DATE_INVALID"
            )
        from zoneinfo import ZoneInfo

        return datetime.fromtimestamp(
            numeric / 1000.0, tz=ZoneInfo("UTC")
        ).astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)

    def _security_master_snapshot(
        self,
    ) -> tuple[dict[str, dict[str, str]], str, str, str, str]:
        body = self._request_json(
            "GET", self.security_master_endpoint, detail="security-master"
        )
        raw_rows = body.get("stockList")
        if not isinstance(raw_rows, list) or not raw_rows:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_INCOMPLETE",
                "stockList",
            )
        master: dict[str, dict[str, str]] = {}
        for raw in raw_rows:
            if not isinstance(raw, Mapping):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_INCOMPLETE", "row"
                )
            code = str(raw.get("code") or "").strip()
            org_id = str(raw.get("orgId") or "").strip()
            category = str(raw.get("category") or "").strip()
            if re.fullmatch(r"\d{6}", code) is None or not org_id:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_INCOMPLETE",
                    code or "identity",
                )
            if code in master:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_DUPLICATE", code
                )
            master[code] = {
                "code": code,
                "org_id": org_id,
                "category": category,
            }
        manifest_hash = _canonical_hash({
            "schema": "probiga.cninfo-security-master.v1",
            "members": [master[code] for code in sorted(master)],
        })
        raw_hash = _canonical_hash(body)
        member_set_hash = _canonical_hash(sorted(master))
        captured_at = self._now_fn().replace(tzinfo=None).isoformat(
            sep=" ", timespec="microseconds"
        )
        return master, manifest_hash, raw_hash, member_set_hash, captured_at

    def _ensure_security_master(self) -> dict[str, dict[str, str]]:
        current = self._master_by_code
        if current is not None:
            return current
        with self._master_lock:
            if self._master_by_code is None:
                master, manifest_hash, raw_hash, member_set_hash, captured_at = (
                    self._security_master_snapshot()
                )
                self._master_by_code = master
                self._master_start_hash = manifest_hash
                self._master_raw_hash = raw_hash
                self._master_member_set_hash = member_set_hash
                self._master_start_captured_at = captured_at
            return self._master_by_code

    def _post_page(
        self,
        *,
        code: str,
        org_id: str,
        page: int,
        start: date,
        end: date,
    ) -> Mapping[str, Any]:
        return self._request_json(
            "POST",
            self.endpoint,
            detail=f"stock={code},page={page}",
            data={
                "pageNum": str(page),
                "pageSize": str(self._page_size),
                "column": "szse",
                "tabName": "fulltext",
                "plate": "",
                "stock": f"{code},{org_id}",
                "searchkey": "",
                "secid": "",
                "category": "",
                "trade": "",
                "seDate": f"{start.isoformat()}~{end.isoformat()}",
                "sortName": "",
                "sortType": "",
                "isHLtitle": "true",
            },
        )

    def _page_rows(
        self,
        body: Mapping[str, Any],
        *,
        code: str,
        page: int,
        expected_pages: int,
        total_records: int,
        reported_totalpages: int,
    ) -> list[dict[str, Any]]:
        try:
            current_total = int(body.get("totalRecordNum"))
            current_reported_pages = int(body.get("totalpages"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE",
                f"stock={code},page={page},metadata",
            ) from exc
        echoed_page = body.get("pageNum")
        if echoed_page not in {None, ""}:
            try:
                echoed_page = int(echoed_page)
            except (TypeError, ValueError, OverflowError) as exc:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED",
                    f"stock={code},page={page},echo",
                ) from exc
        rows_value = body.get("announcements")
        rows = [] if rows_value is None and total_records == 0 else rows_value
        expected_rows = (
            0
            if total_records == 0
            else (
                self._page_size
                if page < expected_pages
                else total_records - self._page_size * (expected_pages - 1)
            )
        )
        if (
            current_total != total_records
            or current_reported_pages != reported_totalpages
            or echoed_page not in {None, "", page}
            or not isinstance(rows, list)
            or len(rows) != expected_rows
            or any(not isinstance(item, Mapping) for item in rows)
            or body.get("hasMore") is not (page < expected_pages)
        ):
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED",
                f"stock={code},page={page}",
            )
        return [dict(item) for item in rows]

    def _page_metadata(
        self,
        body: Mapping[str, Any],
        *,
        code: str,
        window_start: date,
        window_end: date,
    ) -> tuple[int, int, int]:
        """Validate CNInfo's zero-based page counter for one exact query."""

        try:
            total_records = int(body.get("totalRecordNum"))
            reported_totalpages = int(body.get("totalpages"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE",
                f"stock={code},window={window_start}~{window_end},metadata",
            ) from exc
        expected_pages = max(
            1, (total_records + self._page_size - 1) // self._page_size
        )
        if (
            total_records < 0
            or reported_totalpages < 0
            # CNInfo reports the zero-based last page index, rather than the
            # number of pages.  For example, 67 rows reports ``2``.
            or reported_totalpages != total_records // self._page_size
        ):
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE",
                f"stock={code},window={window_start}~{window_end},page-count",
            )
        if expected_pages > CNINFO_MAX_PAGES_PER_STOCK:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_LIMIT_EXCEEDED",
                f"stock={code},pages={expected_pages}",
            )
        return total_records, reported_totalpages, expected_pages

    def _capture_stable_date_shards(
        self,
        *,
        code: str,
        org_id: str,
        start: date,
        end: date,
        root_body: Mapping[str, Any] | None = None,
        total_query_counter: list[int] | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int, int]:
        """Exhaust one window as a deterministic, identity-complete sweep.

        CNInfo's page boundaries can move while a multi-page query is being
        read.  A duplicate across two pages therefore proves neither the full
        result set nor a harmless duplicate.  Split the date interval until
        every leaf is one exhausted page where possible.  A single dense day
        that still spans pages is accepted only as one internally complete
        sweep; the caller independently repeats the whole tree until two
        consecutive raw-by-identity snapshots are exactly equal.
        """

        query_count = 0
        leaves: list[dict[str, Any]] = []

        def visit(
            window_start: date,
            window_end: date,
            body: Mapping[str, Any] | None = None,
        ) -> tuple[list[dict[str, Any]], int, int]:
            nonlocal query_count
            shared_counter = total_query_counter if total_query_counter is not None else [0]
            if shared_counter[0] >= CNINFO_MAX_PAGES_PER_STOCK:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_PAGINATION_LIMIT_EXCEEDED",
                    f"stock={code},date-shard-queries={shared_counter[0]}",
                )
            if body is None:
                current = self._post_page(
                    code=code,
                    org_id=org_id,
                    page=1,
                    start=window_start,
                    end=window_end,
                )
                shared_counter[0] += 1
            else:
                current = body
            query_count += 1
            total, reported_pages, expected_pages = self._page_metadata(
                current,
                code=code,
                window_start=window_start,
                window_end=window_end,
            )
            first_page_rows = self._page_rows(
                current,
                code=code,
                page=1,
                expected_pages=expected_pages,
                total_records=total,
                reported_totalpages=reported_pages,
            )
            if expected_pages == 1:
                page_hash = _canonical_hash(first_page_rows)
                leaves.append({
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "capture_mode": "SINGLE_PAGE",
                    "result_count": total,
                    "provider_reported_totalpages": reported_pages,
                    "provider_page_count": 1,
                    "page_sha256": page_hash,
                    "last_page_has_more": False,
                })
                return first_page_rows, total, reported_pages
            if window_start >= window_end:
                dense_pages = [first_page_rows]
                for page in range(2, expected_pages + 1):
                    if shared_counter[0] >= CNINFO_MAX_PAGES_PER_STOCK:
                        raise AnnouncementProviderError(
                            "ANNOUNCEMENT_FALLBACK_PAGINATION_LIMIT_EXCEEDED",
                            f"stock={code},date-shard-queries={shared_counter[0]}",
                        )
                    dense_body = self._post_page(
                        code=code,
                        org_id=org_id,
                        page=page,
                        start=window_start,
                        end=window_end,
                    )
                    shared_counter[0] += 1
                    query_count += 1
                    dense_pages.append(self._page_rows(
                        dense_body,
                        code=code,
                        page=page,
                        expected_pages=expected_pages,
                        total_records=total,
                        reported_totalpages=reported_pages,
                    ))
                dense_rows = [
                    item for page_rows in dense_pages for item in page_rows
                ]
                dense_identities = [
                    (
                        str(item.get("secCode") or "").strip().zfill(6),
                        str(item.get("announcementId") or "").strip(),
                    )
                    for item in dense_rows
                ]
                if (
                    len(dense_rows) != total
                    or any(
                        row_code != code or not event_id
                        for row_code, event_id in dense_identities
                    )
                    or len(set(dense_identities)) != total
                ):
                    raise AnnouncementProviderError(
                        "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED",
                        f"stock={code},dense-day-incomplete={window_start}",
                    )
                canonical_dense_rows = sorted(
                    dense_rows,
                    key=lambda item: (
                        str(item.get("secCode") or ""),
                        str(item.get("announcementId") or ""),
                    ),
                )
                page_hash = _canonical_hash({
                    "page_sha256": [
                        _canonical_hash(page_rows)
                        for page_rows in dense_pages
                    ],
                    "rows_by_identity": canonical_dense_rows,
                })
                leaves.append({
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "capture_mode": "DENSE_DAY_COMPLETE_SWEEP",
                    "result_count": total,
                    "provider_reported_totalpages": reported_pages,
                    "provider_page_count": expected_pages,
                    "page_sha256": page_hash,
                    "last_page_has_more": False,
                })
                return dense_rows, total, reported_pages
            midpoint = window_start + timedelta(
                days=(window_end - window_start).days // 2
            )
            left_rows, left_total, _ = visit(window_start, midpoint)
            right_rows, right_total, _ = visit(
                midpoint + timedelta(days=1), window_end
            )
            if left_total + right_total != total:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED",
                    f"stock={code},date-shard-total={window_start}~{window_end}",
                )
            return left_rows + right_rows, total, reported_pages

        rows, root_total, root_reported_pages = visit(start, end, root_body)
        identities = [
            (
                str(item.get("secCode") or "").strip().zfill(6),
                str(item.get("announcementId") or "").strip(),
            )
            for item in rows
        ]
        if (
            sum(int(item["result_count"]) for item in leaves) != root_total
            or len(rows) != root_total
            or any(
                row_code != code or not event_id
                for row_code, event_id in identities
            )
            or len(set(identities)) != root_total
        ):
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED",
                f"stock={code},date-shard-root-identity",
            )
        return rows, leaves, query_count, root_total, root_reported_pages

    def fetch(
        self,
        *,
        stock_code: str,
        qmt_code: str,
        requested_start_time: str,
        requested_end_time: str,
    ) -> ProviderResult:
        code = str(stock_code or "").strip().zfill(6)
        instrument = str(qmt_code or "").strip().upper()
        start_at = _compact_datetime(requested_start_time)
        cutoff = _compact_datetime(requested_end_time)
        if (
            re.fullmatch(r"\d{6}", code) is None
            or _QMT_CODE_RE.fullmatch(instrument) is None
            or instrument[:6] != code
            or start_at > cutoff
        ):
            raise ValueError("CNInfo announcement request identity differs")
        master = self._ensure_security_master()
        identity = master.get(code)
        if identity is None or not identity.get("org_id"):
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_INCOMPLETE", code
            )
        org_id = identity["org_id"]
        first = self._post_page(
            code=code, org_id=org_id, page=1,
            start=start_at.date(), end=cutoff.date(),
        )
        total_records, reported_totalpages, provider_pages = (
            self._page_metadata(
                first,
                code=code,
                window_start=start_at.date(),
                window_end=cutoff.date(),
            )
        )
        date_shard_manifest: list[dict[str, Any]] = []
        date_shard_manifest_sha256 = ""
        date_shard_recheck_sha256 = ""
        pagination_round_count = 1
        pagination_query_count = 1
        pagination_attempt_count = 1
        pagination_invalid_round_count = 0
        pagination_complete_round_sha256: list[str] = []
        pagination_complete_round_attempts: list[int] = []
        pagination_mode = "EXACT_STOCK_SINGLE_PAGE"
        quality_status = "EXACT_STOCK_PAGINATION_EXHAUSTED"
        if provider_pages == 1:
            pages = [self._page_rows(
                first, code=code, page=1, expected_pages=provider_pages,
                total_records=total_records,
                reported_totalpages=reported_totalpages,
            )]
            raw_rows = list(pages[0])
        else:
            total_query_counter = [1]
            previous_round_hash = ""
            previous_complete_attempt = 0
            accepted: tuple[
                list[dict[str, Any]], list[dict[str, Any]], int, int, int
            ] | None = None
            root_body: Mapping[str, Any] | None = first
            for attempt in range(1, CNINFO_DATE_SHARD_MAX_CAPTURE_ROUNDS + 1):
                try:
                    current = self._capture_stable_date_shards(
                        code=code,
                        org_id=org_id,
                        start=start_at.date(),
                        end=cutoff.date(),
                        root_body=root_body,
                        total_query_counter=total_query_counter,
                    )
                except AnnouncementProviderError as exc:
                    if exc.reason_code != (
                        "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED"
                    ):
                        raise
                    pagination_invalid_round_count += 1
                    # An incomplete sweep breaks consecutiveness.  A later
                    # snapshot must establish a new two-round stable pair.
                    previous_round_hash = ""
                    previous_complete_attempt = 0
                    root_body = None
                    continue
                current_rows, current_manifest, _, current_total, current_pages = (
                    current
                )
                canonical_rows = sorted(
                    current_rows,
                    key=lambda item: (
                        str(item.get("secCode") or ""),
                        str(item.get("announcementId") or ""),
                    ),
                )
                current_round_hash = _canonical_hash({
                    "schema": "probiga.cninfo-date-shard-capture-round.v1",
                    "split_version": CNINFO_DATE_SHARD_SPLIT_VERSION,
                    "root_total_record_count": current_total,
                    "root_reported_totalpages": current_pages,
                    "manifest": current_manifest,
                    "rows_by_identity": canonical_rows,
                })
                pagination_complete_round_sha256.append(current_round_hash)
                pagination_complete_round_attempts.append(attempt)
                if (
                    previous_round_hash
                    and current_round_hash == previous_round_hash
                    and previous_complete_attempt == attempt - 1
                ):
                    accepted = current
                    pagination_attempt_count = attempt
                    break
                previous_round_hash = current_round_hash
                previous_complete_attempt = attempt
                root_body = None
            if accepted is None:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED",
                    f"stock={code},stable-date-shard-sweeps-unavailable",
                )
            (
                raw_rows,
                date_shard_manifest,
                _accepted_query_count,
                total_records,
                reported_totalpages,
            ) = accepted
            pagination_query_count = total_query_counter[0]
            pagination_round_count = len(pagination_complete_round_sha256)
            date_shard_manifest_sha256 = _canonical_hash({
                "schema": "probiga.cninfo-date-shard-manifest.v1",
                "stock_code": code,
                "requested_start_time": requested_start_time,
                "requested_end_time": requested_end_time,
                "shards": date_shard_manifest,
            })
            date_shard_recheck_sha256 = _canonical_hash({
                "schema": "probiga.cninfo-date-shard-manifest.v1",
                "stock_code": code,
                "requested_start_time": requested_start_time,
                "requested_end_time": requested_end_time,
                "shards": date_shard_manifest,
            })
            pages = []
            pagination_mode = "EXACT_STOCK_DATE_SHARDS_DOUBLE_ATTESTED"
            quality_status = "EXACT_STOCK_DATE_SHARDS_DOUBLE_ATTESTED"
        if len(raw_rows) != total_records:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_PAGINATION_INCOMPLETE",
                f"stock={code},total",
            )
        raw_rows.sort(
            key=lambda item: (
                int(item.get("announcementTime") or 0),
                str(item.get("announcementId") or ""),
            ),
            reverse=True,
        )
        captured_at = self._now_fn().replace(tzinfo=None)
        rows: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        previous_time: datetime | None = None
        for raw in raw_rows:
            row_code = str(raw.get("secCode") or "").strip().zfill(6)
            event_id = str(raw.get("announcementId") or "").strip()
            title = str(raw.get("announcementTitle") or "").strip()
            published_at = self._publication_datetime(
                raw.get("announcementTime")
            )
            if (
                row_code != code
                or not event_id
                or not title
                or not start_at.date() <= published_at.date() <= cutoff.date()
                or published_at > cutoff
            ):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_ROW_INVALID", code
                )
            row_identity = (row_code, event_id)
            if row_identity in seen:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED",
                    f"duplicate={code}:{event_id}",
                )
            if previous_time is not None and published_at > previous_time:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_PAGINATION_CHANGED",
                    f"stock={code},order",
                )
            seen.add(row_identity)
            previous_time = published_at
            rows.append({
                "stock_code": code,
                "qmt_code": instrument,
                "announcement_id": event_id,
                "announcementTime": raw.get("announcementTime"),
                "notice_date": published_at.date().isoformat(),
                "published_at": published_at.isoformat(
                    sep=" ", timespec="microseconds"
                ),
                "source_published_text": str(raw.get("announcementTime")),
                "title": title,
                "detail_url": (
                    "https://static.cninfo.com.cn/"
                    + str(raw.get("adjunctUrl") or "").lstrip("/")
                ),
                "provider": self.source,
                "provider_captured_at": captured_at.isoformat(
                    sep=" ", timespec="microseconds"
                ),
                "source_row_hash": _canonical_hash(raw),
            })
        page_hashes = (
            [str(item["page_sha256"]) for item in date_shard_manifest]
            if date_shard_manifest
            else [_canonical_hash(page_rows) for page_rows in pages]
        )
        first_anchor = page_hashes[0]
        last_anchor = page_hashes[-1]
        first_page_recheck = ""
        # ``page_count`` represents independently attested leaf partitions in
        # shard mode, and physical pages for the legacy single-page mode.
        expected_pages = (
            len(date_shard_manifest) if date_shard_manifest else len(pages)
        )
        pagination_hash = _canonical_hash({
            "pagination_mode": pagination_mode,
            "total_record_count": total_records,
            "reported_totalpages": reported_totalpages,
            "page_count": expected_pages,
            "page_sha256": page_hashes,
            "last_page_has_more": False,
            "first_page_sha256": first_anchor,
            "last_page_sha256": last_anchor,
            "first_page_recheck_sha256": first_page_recheck,
            **({
                "date_shard_manifest_sha256": date_shard_manifest_sha256,
                "date_shard_recheck_sha256": date_shard_recheck_sha256,
                "pagination_round_count": pagination_round_count,
                "pagination_query_count": pagination_query_count,
                "pagination_attempt_count": pagination_attempt_count,
                "pagination_invalid_round_count": (
                    pagination_invalid_round_count
                ),
                "pagination_complete_round_sha256": (
                    pagination_complete_round_sha256
                ),
                "pagination_complete_round_attempts": (
                    pagination_complete_round_attempts
                ),
                "date_shard_split_version": CNINFO_DATE_SHARD_SPLIT_VERSION,
            } if date_shard_manifest else {}),
        })
        payload_hash = _canonical_hash({
            "schema": "probiga.cninfo-per-stock-announcement.v1",
            "security_master_sha256": self._master_start_hash,
            "stock_code": code,
            "qmt_code": instrument,
            "org_id": org_id,
            "requested_start_time": requested_start_time,
            "requested_end_time": requested_end_time,
            "total_records": total_records,
            "reported_totalpages": reported_totalpages,
            "page_hashes": page_hashes,
            "first_page_sha256": first_anchor,
            "last_page_anchor_sha256": last_anchor,
            "first_page_recheck_sha256": first_page_recheck,
            "date_shard_manifest": date_shard_manifest,
            "date_shard_manifest_sha256": date_shard_manifest_sha256,
            "date_shard_recheck_sha256": date_shard_recheck_sha256,
            "pagination_round_count": pagination_round_count,
            "pagination_query_count": pagination_query_count,
            "pagination_attempt_count": pagination_attempt_count,
            "pagination_invalid_round_count": pagination_invalid_round_count,
            "pagination_complete_round_sha256": (
                pagination_complete_round_sha256
            ),
            "pagination_complete_round_attempts": (
                pagination_complete_round_attempts
            ),
            "date_shard_split_version": (
                CNINFO_DATE_SHARD_SPLIT_VERSION
                if date_shard_manifest else ""
            ),
            "rows": rows,
        })
        with self._fetched_lock:
            if code in self._fetched_codes:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_DUPLICATE_MEMBER", code
                )
            self._fetched_codes.add(code)
        receipt = {
            "schema": CNINFO_PROVIDER_RECEIPT_SCHEMA,
            "status": "COMPLETE",
            "source": self.source,
            "stock_code": code,
            "qmt_code": instrument,
            "org_id": org_id,
            "query_stock_identity": f"{code},{org_id}",
            "requested_start_time": requested_start_time,
            "requested_end_time": requested_end_time,
            "captured_at": captured_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "exhausted": True,
            "page_count": expected_pages,
            "expected_pages": expected_pages,
            "max_pages_per_stock": CNINFO_MAX_PAGES_PER_STOCK,
            "result_count": len(rows),
            "provider_total_record_count": total_records,
            "provider_reported_totalpages": reported_totalpages,
            "page_sha256": page_hashes,
            "first_page_anchor_sha256": first_anchor,
            "last_page_anchor_sha256": last_anchor,
            "first_page_recheck_sha256": first_page_recheck,
            "pagination_mode": pagination_mode,
            "last_page_has_more": False,
            "pagination_sha256": pagination_hash,
            "date_shard_manifest": date_shard_manifest,
            "date_shard_count": len(date_shard_manifest),
            "date_shard_manifest_sha256": date_shard_manifest_sha256,
            "date_shard_recheck_sha256": date_shard_recheck_sha256,
            "pagination_round_count": pagination_round_count,
            "pagination_query_count": pagination_query_count,
            "pagination_attempt_count": pagination_attempt_count,
            "pagination_invalid_round_count": pagination_invalid_round_count,
            "pagination_complete_round_sha256": (
                pagination_complete_round_sha256
            ),
            "pagination_complete_round_attempts": (
                pagination_complete_round_attempts
            ),
            "date_shard_split_version": (
                CNINFO_DATE_SHARD_SPLIT_VERSION
                if date_shard_manifest else ""
            ),
            "security_master_sha256": self._master_start_hash,
            "directory_raw_sha256": self._master_raw_hash,
            "directory_manifest_hash": self._master_start_hash,
            "directory_member_set_hash": self._master_member_set_hash,
            "security_master_started_at": self._master_start_captured_at,
            "provider_payload_sha256": payload_hash,
            "permission_status": "PUBLIC",
            "quality_status": quality_status,
            "data_version": _canonical_hash(
                "probiga.cninfo-per-stock-announcement.v1"
            ),
        }
        return ProviderResult(tuple(rows), receipt)

    def restore_receipts(
        self, receipts: Mapping[str, Mapping[str, Any]]
    ) -> None:
        """Restore hash-bound per-stock staging from the same directory."""

        master = self._ensure_security_master()
        restored: set[str] = set()
        for raw_code, raw_receipt in receipts.items():
            code = str(raw_code or "").strip().zfill(6)
            if not isinstance(raw_receipt, Mapping) or code not in master:
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID", code
                )
            receipt = dict(raw_receipt)
            qmt_code = str(receipt.get("qmt_code") or "").upper()
            org_id = master[code]["org_id"]
            if (
                receipt.get("schema") != CNINFO_PROVIDER_RECEIPT_SCHEMA
                or receipt.get("status") != "COMPLETE"
                or receipt.get("source") != self.source
                or receipt.get("stock_code") != code
                or not _QMT_CODE_RE.fullmatch(qmt_code)
                or qmt_code[:6] != code
                or receipt.get("org_id") != org_id
                or receipt.get("query_stock_identity") != f"{code},{org_id}"
                or receipt.get("security_master_sha256")
                != self._master_start_hash
                or receipt.get("directory_raw_sha256") != self._master_raw_hash
                or receipt.get("directory_manifest_hash")
                != self._master_start_hash
                or receipt.get("directory_member_set_hash")
                != self._master_member_set_hash
                or receipt.get("exhausted") is not True
                or receipt.get("pagination_mode") not in {
                    "EXACT_STOCK_SINGLE_PAGE",
                    "EXACT_STOCK_DATE_SHARDS_DOUBLE_ATTESTED",
                }
            ):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID", code
                )
            restored.add(code)
        with self._fetched_lock:
            if self._fetched_codes.intersection(restored):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_DUPLICATE_MEMBER"
                )
            self._fetched_codes.update(restored)

    def finalize_receipts(
        self, receipts: Mapping[str, Mapping[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        """Seal receipts only after a stable second security-master anchor."""

        master = self._ensure_security_master()
        with self._master_lock:
            end_master, end_hash, end_raw_hash, end_set_hash, end_captured_at = (
                self._security_master_snapshot()
            )
            if (
                end_hash != self._master_start_hash
                or end_raw_hash != self._master_raw_hash
                or end_set_hash != self._master_member_set_hash
                or end_master != master
            ):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_CHANGED"
                )
        requested = sorted(str(code).zfill(6) for code in receipts)
        with self._fetched_lock:
            fetched = sorted(self._fetched_codes)
        missing = [code for code in requested if code not in master]
        if requested != fetched or missing:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_SECURITY_MASTER_INCOMPLETE",
                f"requested={len(requested)},fetched={len(fetched)},missing={len(missing)}",
            )
        requested_hash = _canonical_hash(requested)
        finalized: dict[str, dict[str, Any]] = {}
        for code in requested:
            raw = receipts.get(code)
            if not isinstance(raw, Mapping):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID", code
                )
            receipt = dict(raw)
            if (
                receipt.get("security_master_sha256")
                != self._master_start_hash
                or receipt.get("org_id") != master[code]["org_id"]
            ):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID", code
                )
            receipt.update({
                "security_master_end_sha256": end_hash,
                "security_master_ended_at": end_captured_at,
                "security_master_member_count": len(master),
                "directory_member_count": len(master),
                "directory_catalog_coverage_count": len(requested),
                "directory_catalog_missing_count": 0,
                "directory_catalog_extra_count": len(master) - len(requested),
                "requested_catalog_member_count": len(requested),
                "requested_catalog_member_set_sha256": requested_hash,
                "security_master_unrequested_count": len(master) - len(requested),
                "security_master_missing_requested_count": 0,
            })
            receipt["directory_attestation_sha256"] = _canonical_hash({
                "directory_raw_sha256": receipt["directory_raw_sha256"],
                "directory_manifest_hash": receipt[
                    "directory_manifest_hash"
                ],
                "directory_member_set_hash": receipt[
                    "directory_member_set_hash"
                ],
                "directory_member_count": receipt[
                    "directory_member_count"
                ],
                "security_master_start_sha256": receipt[
                    "security_master_sha256"
                ],
                "security_master_end_sha256": receipt[
                    "security_master_end_sha256"
                ],
                "requested_catalog_member_count": receipt[
                    "requested_catalog_member_count"
                ],
                "requested_catalog_member_set_sha256": receipt[
                    "requested_catalog_member_set_sha256"
                ],
                "directory_catalog_missing_count": 0,
            })
            finalized[code] = receipt
        return finalized

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


class ProviderBackedAnnouncementAdapter:
    """Expose any audited provider through the existing full-market reader API."""

    force_fresh_capture = True
    resumable_capture = True
    checkpoint_batch_size = 1

    def __init__(
        self,
        provider: AnnouncementProvider,
        *,
        workers: int = 1,
    ) -> None:
        if not 1 <= int(workers) <= 4:
            raise ValueError("announcement fallback workers must be 1..4")
        self.source = str(provider.source or "").strip()
        self._provider = provider
        self._workers = int(workers)
        self._pending: dict[str, tuple[str, str]] = {}
        self._receipts: dict[str, dict[str, Any]] = {}
        self._deadline_monotonic: float | None = None

    def bind_capture_deadline(
        self, *, fact_cutoff_at: datetime, max_capture_delay: timedelta
    ) -> None:
        from zoneinfo import ZoneInfo

        remaining = (
            fact_cutoff_at
            + max_capture_delay
            - datetime.now(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
        ).total_seconds()
        if remaining <= 0:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"
            )
        capture_remaining = remaining - ANNOUNCEMENT_DB_PUBLISH_RESERVE_SECONDS
        if capture_remaining <= 0:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"
            )
        self._deadline_monotonic = time.monotonic() + capture_remaining
        provider_binder = getattr(
            self._provider, "bind_capture_deadline", None
        )
        if callable(provider_binder):
            provider_binder(remaining_seconds=capture_remaining)

    def connect(self, *, port: int, remember_if_success: bool) -> None:
        if remember_if_success is not False or not 1 <= int(port) <= 65535:
            raise RuntimeError("announcement fallback connection contract differs")

    def download_history_data(
        self,
        stock_code: str,
        *,
        period: str,
        start_time: str,
        end_time: str,
        **_kwargs: Any,
    ) -> None:
        code = str(stock_code or "").strip().upper()
        _compact_datetime(start_time)
        _compact_datetime(end_time)
        if period != "announcement" or _QMT_CODE_RE.fullmatch(code) is None:
            raise RuntimeError("announcement fallback download scope differs")
        self._pending[code] = (start_time, end_time)

    def _fetch_one(self, qmt_code: str, start: str, end: str) -> ProviderResult:
        if (
            self._deadline_monotonic is None
            or time.monotonic() >= self._deadline_monotonic
        ):
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_CAPTURE_DEADLINE_EXPIRED"
            )
        return self._provider.fetch(
            stock_code=qmt_code[:6],
            qmt_code=qmt_code,
            requested_start_time=start,
            requested_end_time=end,
        )

    def get_market_data_ex(
        self,
        *,
        field_list: list[Any],
        stock_list: list[str],
        period: str,
        start_time: str,
        end_time: str,
        count: int,
        dividend_type: str,
        fill_data: bool,
    ) -> dict[str, pd.DataFrame]:
        codes = [str(code or "").strip().upper() for code in stock_list]
        if (
            field_list != []
            or not codes
            or len(codes) != len(set(codes))
            or period != "announcement"
            or count != -1
            or dividend_type != "none"
            or fill_data is not False
            or any(self._pending.get(code) != (start_time, end_time) for code in codes)
        ):
            raise RuntimeError("announcement fallback read contract differs")
        if self._workers == 1:
            fetched = [self._fetch_one(code, start_time, end_time) for code in codes]
        else:
            with ThreadPoolExecutor(max_workers=self._workers) as pool:
                fetched = list(pool.map(
                    lambda code: self._fetch_one(code, start_time, end_time),
                    codes,
                ))
        frames: dict[str, pd.DataFrame] = {}
        for code, result in zip(codes, fetched):
            frames[code] = pd.DataFrame(list(result.rows))
            self._receipts[code[:6]] = dict(result.receipt)
            self._pending.pop(code, None)
        return frames

    def capture_receipts(self) -> dict[str, dict[str, Any]]:
        receipts = {
            code: dict(receipt) for code, receipt in self._receipts.items()
        }
        finalizer = getattr(self._provider, "finalize_receipts", None)
        if callable(finalizer):
            finalized = finalizer(receipts)
            if not isinstance(finalized, Mapping) or set(finalized) != set(
                receipts
            ):
                raise AnnouncementProviderError(
                    "ANNOUNCEMENT_FALLBACK_RECEIPTS_INCOMPLETE"
                )
            receipts = {
                str(code): dict(receipt)
                for code, receipt in finalized.items()
            }
            self._receipts = {
                code: dict(receipt) for code, receipt in receipts.items()
            }
        return receipts

    def staged_capture_receipts(self) -> dict[str, dict[str, Any]]:
        """Return unsealed receipts for tamper-evident checkpoint staging."""

        return {
            code: dict(receipt) for code, receipt in self._receipts.items()
        }

    def restore_capture_receipts(
        self, receipts: Mapping[str, Mapping[str, Any]]
    ) -> None:
        if self._receipts:
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_DUPLICATE_MEMBER", "restore"
            )
        normalized = {
            str(code).zfill(6): dict(receipt)
            for code, receipt in receipts.items()
            if isinstance(receipt, Mapping)
        }
        if len(normalized) != len(receipts):
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_RECEIPT_INVALID", "restore"
            )
        restorer = getattr(self._provider, "restore_receipts", None)
        if not callable(restorer):
            raise AnnouncementProviderError(
                "ANNOUNCEMENT_FALLBACK_RESUME_UNSUPPORTED"
            )
        restorer(normalized)
        self._receipts = normalized

    def close(self) -> None:
        self._provider.close()


__all__ = [
    "AnnouncementProvider",
    "AnnouncementProviderError",
    "CNINFO_PROVIDER_RECEIPT_SCHEMA",
    "CNINFO_SOURCE",
    "CninfoMarketAnnouncementProvider",
    "EASTMONEY_SOURCE",
    "EastmoneyAnnouncementProvider",
    "PROVIDER_RECEIPT_SCHEMA",
    "ProviderBackedAnnouncementAdapter",
    "ProviderResult",
]
