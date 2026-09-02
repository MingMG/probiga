"""
股票财务核心指标同步脚本

调用 adata stock.finance.get_core_index() 获取东方财富财务数据，
写入 si_stock_finance 表。

用法：
    python biz/stock_finance/sync_finance.py                 # 全量同步（增量：只拉新报告期）
    python biz/stock_finance/sync_finance.py --code 600396   # 同步单只股票
    python biz/stock_finance/sync_finance.py --limit 100      # 只同步前100只
"""

import argparse
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import json
import re
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from sqlalchemy import text

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from server.common.adata_release import ensure_adata_import_path

ensure_adata_import_path(ROOT)

from server.common.batch_db import create_batch_engine, read_frame
from server.common.finance_coverage import (
    FinanceDisclosureGate,
    coerce_optional_date,
    finance_disclosure_gate,
    report_period_gate_applies,
)
from server.common.pit_facts import (
    append_finance_atomic_batch_seal,
    append_finance_expected_unavailable,
    append_finance_revision,
    append_source_coverage,
    canonical_hash,
    load_latest_finance_atomic_batch_baseline,
)


MAX_FETCH_WORKERS = 16
FETCH_PREFETCH_MULTIPLIER = 2
PRIMARY_FINANCE_SOURCE = "adata.finance.core_index"
PRIMARY_FINANCE_CONTRACT_VERSION = "adata-core-index-pit-v2"
FINANCE_NOTICE_OVERLAP_DAYS = 3
FINANCE_NOTICE_KEYWORDS = re.compile(
    r"(年度报告|半年度报告|季度报告|业绩预告|业绩快报|审计报告|"
    r"财务报告|财务报表|更正公告|复牌|上市)"
)
DEFAULT_FINANCE_CHECKPOINT_FILE = (
    "/var/lib/probiga/jobs/stock-finance-daily-v2.json"
)
CNINFO_FINANCE_NONFILING_SOURCE = "cninfo.finance.nonfiling"
CNINFO_ANNOUNCEMENT_ENDPOINT = (
    "https://www.cninfo.com.cn/new/hisAnnouncement/query"
)
CNINFO_STATIC_ROOT = "https://static.cninfo.com.cn/"
FINANCE_NONFILING_REASON = "CNINFO_REGULATORY_PERIODIC_REPORT_NOT_FILED"
FINANCE_NONFILING_MAX_AGE_DAYS = 7
FINANCE_NONFILING_QUERY_DAYS = 45
CNINFO_NONFILING_ISSUERS: dict[str, str] = {
    # ST Cuìhuá did not file its 2025 annual report or 2026 Q1 report by the
    # statutory deadline.  CNInfo org ids are issuer identities, not secrets.
    "002731": "9900022974",
}


class FinanceStaleResponse(RuntimeError):
    """A non-empty, identity-valid provider response with an old period."""


def get_engine():
    return create_batch_engine(pool_size=5, max_overflow=10)


def get_finance_stock_universe(engine) -> dict[str, date | None]:
    """Load the current authoritative A-share universe and listing dates."""

    df = read_frame(
        text(
            "SELECT stock_code, list_date FROM si_all_code "
            "WHERE stock_code REGEXP '^(0|3|4|6|8|9)[0-9]{5}$' "
            "AND (list_date IS NULL OR list_date <= CURRENT_DATE) "
            "ORDER BY stock_code"
        ),
        engine,
    )
    universe: dict[str, date | None] = {}
    for row in df.to_dict("records"):
        raw = str(row.get("stock_code") or "").strip()
        if not raw:
            raise RuntimeError("DATA_BLOCKED: finance universe contains empty code")
        code = raw.zfill(6)
        if code in universe:
            raise RuntimeError(
                f"DATA_BLOCKED: finance universe contains duplicate code {code}"
            )
        universe[code] = coerce_optional_date(row.get("list_date"))
    return universe


def get_all_stock_codes(engine) -> list[str]:
    """Backward-compatible ordered code list from the finance universe."""

    return list(get_finance_stock_universe(engine))


@dataclass(frozen=True)
class FinanceCandidatePlan:
    codes: list[str]
    reasons: dict[str, list[str]]
    parent_seal_coverage_id: str
    parent_batch_root_sha256: str
    input_root_sha256: str


def _checkpoint_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            "DATA_BLOCKED: finance checkpoint is unreadable"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("DATA_BLOCKED: finance checkpoint is malformed")
    observed_hash = str(payload.pop("checkpoint_sha256", "") or "")
    if observed_hash != canonical_hash(payload):
        raise RuntimeError("DATA_BLOCKED: finance checkpoint hash differs")
    return payload


def _write_checkpoint(path: Path, payload: Mapping[str, Any]) -> None:
    normalized = dict(payload)
    normalized["checkpoint_sha256"] = canonical_hash(normalized)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(normalized, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def select_daily_finance_candidates(
    engine,
    universe: Mapping[str, date | None],
    *,
    as_of: date,
    checkpoint: Mapping[str, Any] | None = None,
) -> FinanceCandidatePlan:
    """Build the bounded union of stocks whose finance disposition changed."""

    decision_at = datetime.now().replace(microsecond=0)
    baseline = load_latest_finance_atomic_batch_baseline(
        engine,
        decision_at=decision_at,
    )
    if not baseline:
        raise RuntimeError(
            "DATA_BLOCKED: finance immutable baseline is unavailable; "
            "run the independent --full-baseline repair first"
        )
    members = baseline.get("members")
    if not isinstance(members, Mapping):
        raise RuntimeError("DATA_BLOCKED: finance baseline members are malformed")

    reasons: dict[str, set[str]] = {}

    def add(code: str, reason: str) -> None:
        normalized = str(code or "").strip().zfill(6)
        if normalized in universe:
            reasons.setdefault(normalized, set()).add(reason)

    gate = finance_disclosure_gate(as_of)
    for code, listing_date in universe.items():
        member = members.get(code)
        if not isinstance(member, Mapping):
            add(code, "NEW_OR_MISSING_CATALOG_MEMBER")
            continue
        if str(member.get("coverage_status") or "") == "EXPECTED_UNAVAILABLE":
            try:
                next_retry = date.fromisoformat(
                    str(member.get("next_retry_date") or "")
                )
                valid_until = date.fromisoformat(
                    str(member.get("valid_until") or "")
                )
            except ValueError:
                add(code, "UNAVAILABLE_REVIEW_PROOF_MISSING")
            else:
                if (
                    str(member.get("expected_report_date") or "")
                    != gate.minimum_report_date.isoformat()
                    or next_retry <= as_of
                    or valid_until < as_of
                ):
                    add(code, "UNAVAILABLE_REVIEW_DUE")
            continue
        prefix = member.get("strategy_prefix_binding")
        try:
            latest_report = date.fromisoformat(
                str(
                    prefix.get("latest_report_date")
                    if isinstance(prefix, Mapping)
                    else ""
                )
            )
        except ValueError:
            add(code, "MISSING_OR_FAILED_DISPOSITION")
        else:
            if (
                report_period_gate_applies(listing_date, gate)
                and latest_report < gate.minimum_report_date
            ):
                add(code, "DISCLOSURE_GATE_ADVANCED")

    prior_contract = str(baseline.get("provider_contract_version") or "")
    if prior_contract and prior_contract != PRIMARY_FINANCE_CONTRACT_VERSION:
        for code in universe:
            add(code, "PRIMARY_PROVIDER_VERSION_CHANGED")

    overlap_start = as_of - timedelta(days=FINANCE_NOTICE_OVERLAP_DAYS)
    try:
        notice_frame = read_frame(
            text(
                "SELECT stock_code, title, notice_date, received_at "
                "FROM si_notice_eastmoney "
                "WHERE association_validated=1 "
                "AND notice_date>=:window_start AND notice_date<=:window_end"
            ),
            engine,
            params={
                "window_start": overlap_start,
                "window_end": as_of,
            },
        )
    except Exception as exc:
        raise RuntimeError(
            "DATA_BLOCKED: finance announcement candidate source unavailable"
        ) from exc
    for row in notice_frame.to_dict("records"):
        if FINANCE_NOTICE_KEYWORDS.search(str(row.get("title") or "")):
            add(str(row.get("stock_code") or ""), "NEW_OR_CORRECTED_ANNOUNCEMENT")

    checkpoint = dict(checkpoint or {})
    for code in checkpoint.get("unresolved_codes") or ():
        add(str(code), "PREVIOUS_FAILED_SHARD")

    normalized_reasons = {
        code: sorted(values)
        for code, values in sorted(reasons.items())
    }
    input_root = canonical_hash({
        "schema": "probiga.finance-daily-candidate-input.v1",
        "as_of": as_of.isoformat(),
        "catalog_codes": sorted(universe),
        "catalog_member_set_hash": str(
            baseline.get("catalog_member_set_hash") or ""
        ),
        "parent_batch_root_sha256": str(
            baseline.get("batch_root_sha256") or ""
        ),
        "provider_contract_version": PRIMARY_FINANCE_CONTRACT_VERSION,
        "reasons": normalized_reasons,
    })
    return FinanceCandidatePlan(
        codes=sorted(normalized_reasons),
        reasons=normalized_reasons,
        parent_seal_coverage_id=str(
            baseline.get("seal_coverage_id") or ""
        ),
        parent_batch_root_sha256=str(
            baseline.get("batch_root_sha256") or ""
        ),
        input_root_sha256=input_root,
    )


def fetch_finance(stock_code: str) -> pd.DataFrame:
    """调用 adata 获取单只股票的财务核心指标"""
    try:
        from adata.stock.finance import finance
        df = finance.get_core_index(stock_code)
        if df is None or df.empty:
            raise RuntimeError(
                f"DATA_BLOCKED: {stock_code} 财务源返回空结果"
            )
        return df
    except Exception as exc:
        raise RuntimeError(f"{stock_code} 财务源请求失败") from exc


def enrich_finance_publication_evidence(
    engine,
    frame: pd.DataFrame,
    *,
    stock_code: str,
    observed_at: datetime,
) -> pd.DataFrame:
    """Bind date-only finance rows to exact upstream announcement evidence."""

    if frame is None or frame.empty or "notice_date" not in frame.columns:
        return frame
    result = frame.copy()
    notice_dates = pd.to_datetime(
        result["notice_date"], errors="coerce"
    ).dt.date.dropna()
    if notice_dates.empty:
        return result
    announcements = read_frame(
        text(
            "SELECT stock_code, art_code, notice_date, title, display_time, "
            "source_time, received_at, detail_url, data_source, data_version "
            "FROM si_notice_eastmoney "
            "WHERE stock_code=:stock_code AND association_validated=1 "
            "AND notice_date>=:window_start AND notice_date<=:window_end"
        ),
        engine,
        params={
            "stock_code": str(stock_code).zfill(6),
            "window_start": min(notice_dates),
            "window_end": max(notice_dates),
        },
    )
    by_date: dict[date, dict[str, Any]] = {}
    for raw in announcements.to_dict("records"):
        if not FINANCE_NOTICE_KEYWORDS.search(str(raw.get("title") or "")):
            continue
        notice_date = pd.to_datetime(
            raw.get("notice_date"), errors="coerce"
        )
        if pd.isna(notice_date):
            continue
        display_time = pd.to_datetime(
            raw.get("display_time"), errors="coerce"
        )
        source_time = pd.to_datetime(
            raw.get("source_time"), errors="coerce"
        )
        published = display_time if not pd.isna(display_time) else source_time
        received = pd.to_datetime(raw.get("received_at"), errors="coerce")
        if (
            pd.isna(published)
            or published.to_pydatetime().replace(tzinfo=None) > observed_at
            or (not pd.isna(received) and received.to_pydatetime().replace(
                tzinfo=None
            ) > observed_at)
        ):
            continue
        item = {
            "published_at": published.to_pydatetime().replace(
                tzinfo=None, microsecond=0
            ).isoformat(),
            "publication_source": str(raw.get("data_source") or ""),
            "publication_event_key": str(raw.get("art_code") or ""),
            "publication_url": str(raw.get("detail_url") or ""),
            "publication_received_at": (
                received.to_pydatetime().replace(
                    tzinfo=None, microsecond=0
                ).isoformat()
                if not pd.isna(received)
                else ""
            ),
            "publication_data_version": str(raw.get("data_version") or ""),
        }
        item["publication_content_sha256"] = canonical_hash({
            "schema": "probiga.finance-publication-binding.v1",
            **item,
            "title": str(raw.get("title") or ""),
        })
        key = notice_date.date()
        if (
            key not in by_date
            or item["published_at"] > by_date[key]["published_at"]
        ):
            by_date[key] = item
    for index, raw_date in result["notice_date"].items():
        parsed = pd.to_datetime(raw_date, errors="coerce")
        if pd.isna(parsed) or parsed.date() not in by_date:
            continue
        for key, value in by_date[parsed.date()].items():
            result.at[index, key] = value
    return result


def fetch_cninfo_nonfiling_evidence(
    stock_code: str,
    *,
    as_of: date,
    expected_report_date: date,
) -> dict[str, Any]:
    """Return recent official proof that a required periodic report is absent."""

    code = str(stock_code or "").strip().zfill(6)
    org_id = CNINFO_NONFILING_ISSUERS.get(code)
    if not org_id:
        raise RuntimeError(
            f"DATA_BLOCKED: {code} has no reviewed CNInfo non-filing identity"
        )
    start = as_of - timedelta(days=FINANCE_NONFILING_QUERY_DAYS)
    response = requests.post(
        CNINFO_ANNOUNCEMENT_ENDPOINT,
        data={
            "pageNum": "1",
            "pageSize": "30",
            "column": "szse",
            "tabName": "fulltext",
            "plate": "",
            "stock": f"{code},{org_id}",
            "searchkey": "",
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": f"{start.isoformat()}~{as_of.isoformat()}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "false",
        },
        headers={
            "Accept": "application/json",
            "Referer": "https://www.cninfo.com.cn/",
            "User-Agent": "Mozilla/5.0 ProBigAFinance/1.0",
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=30,
    )
    response.raise_for_status()
    api_raw = bytes(response.content)
    try:
        payload = response.json()
    except (TypeError, ValueError) as exc:
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing response is not JSON") from exc
    if not isinstance(payload, Mapping):
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing response is malformed")
    announcements = payload.get("announcements")
    if not isinstance(announcements, list):
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing catalogue is malformed")
    candidates: list[tuple[datetime, Mapping[str, Any]]] = []
    for item in announcements:
        if not isinstance(item, Mapping):
            raise RuntimeError("DATA_BLOCKED: CNInfo announcement row is malformed")
        if (
            str(item.get("secCode") or "").zfill(6) != code
            or str(item.get("orgId") or "") != org_id
        ):
            raise RuntimeError("DATA_BLOCKED: CNInfo announcement identity differs")
        title = str(item.get("announcementTitle") or "")
        if not (
            "未在规定期限内披露定期报告" in title
            or "无法在法定期限内披露定期报告" in title
            or (
                "无法在规定期限内披露" in title
                and "年度报告" in title
            )
        ):
            continue
        try:
            published = datetime.fromtimestamp(
                int(item.get("announcementTime")) / 1000,
                tz=ZoneInfo("Asia/Shanghai"),
            ).replace(tzinfo=None)
        except (TypeError, ValueError, OSError, OverflowError) as exc:
            raise RuntimeError("DATA_BLOCKED: CNInfo announcement time is invalid") from exc
        candidates.append((published, item))
    if not candidates:
        raise RuntimeError("DATA_BLOCKED: no official recent non-filing proof")
    published, selected = max(candidates, key=lambda pair: pair[0])
    announcement_date = published.date()
    valid_until = announcement_date + timedelta(days=FINANCE_NONFILING_MAX_AGE_DAYS)
    if announcement_date > as_of or as_of > valid_until:
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing proof is outside validity")
    announcement_id = str(selected.get("announcementId") or "")
    adjunct = str(selected.get("adjunctUrl") or "")
    if (
        not re.fullmatch(r"\d+", announcement_id)
        or not re.fullmatch(
            rf"finalpage/\d{{4}}-\d{{2}}-\d{{2}}/{re.escape(announcement_id)}\.PDF",
            adjunct,
        )
    ):
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing document identity differs")
    document_url = CNINFO_STATIC_ROOT + adjunct
    document = requests.get(
        document_url,
        headers={"User-Agent": "Mozilla/5.0 ProBigAFinance/1.0"},
        timeout=30,
    )
    document.raise_for_status()
    document_raw = bytes(document.content)
    if len(document_raw) < 1024 or not document_raw.startswith(b"%PDF"):
        raise RuntimeError("DATA_BLOCKED: CNInfo non-filing document is not a PDF")
    next_retry = min(as_of + timedelta(days=1), valid_until)
    return {
        "source": CNINFO_FINANCE_NONFILING_SOURCE,
        "reason_code": FINANCE_NONFILING_REASON,
        "stock_code": code,
        "expected_report_date": expected_report_date.isoformat(),
        "announcement_id": announcement_id,
        "announcement_title": str(selected.get("announcementTitle") or ""),
        "announcement_published_at": published.replace(microsecond=0).isoformat(),
        "announcement_url": document_url,
        "announcement_document_sha256": hashlib.sha256(document_raw).hexdigest(),
        "catalog_response_sha256": hashlib.sha256(api_raw).hexdigest(),
        "valid_from": announcement_date.isoformat(),
        "valid_until": valid_until.isoformat(),
        "next_retry_date": next_retry.isoformat(),
    }


def _fetch_finance_with_cooldown(
    stock_code: str,
    *,
    sleep_seconds: float,
) -> pd.DataFrame:
    """Fetch one provider response and apply one worker-local cooldown."""

    try:
        return fetch_finance(stock_code)
    finally:
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def iter_finance_fetches(
    codes: list[str],
    *,
    workers: int,
    sleep_seconds: float,
):
    """Yield provider responses in input order with bounded fetch concurrency.

    Only the external provider call runs in worker threads.  The caller keeps
    response validation and every database transaction on its main thread.
    Keeping at most ``workers * 2`` futures in flight bounds memory while still
    overlapping provider latency with the previous response's database write.
    """

    if workers <= 1 or len(codes) <= 1:
        for index, code in enumerate(codes):
            try:
                yield code, fetch_finance(code), None
            except Exception as exc:
                yield code, None, exc
            if sleep_seconds > 0 and index < len(codes) - 1:
                time.sleep(sleep_seconds)
        return

    max_inflight = min(
        len(codes),
        max(workers, workers * FETCH_PREFETCH_MULTIPLIER),
    )
    code_iterator = iter(codes)
    pending: deque[tuple[str, Future[pd.DataFrame]]] = deque()
    with ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="finance-fetch",
    ) as executor:
        for _ in range(max_inflight):
            code = next(code_iterator, None)
            if code is None:
                break
            pending.append((
                code,
                executor.submit(
                    _fetch_finance_with_cooldown,
                    code,
                    sleep_seconds=sleep_seconds,
                ),
            ))

        while pending:
            code, future = pending.popleft()
            try:
                frame = future.result()
                error = None
            except Exception as exc:
                frame = None
                error = exc
            yield code, frame, error

            next_code = next(code_iterator, None)
            if next_code is not None:
                pending.append((
                    next_code,
                    executor.submit(
                        _fetch_finance_with_cooldown,
                        next_code,
                        sleep_seconds=sleep_seconds,
                    ),
                ))


def upsert_finance(
    engine,
    df: pd.DataFrame,
    *,
    stock_code: str | None = None,
    observed_at: datetime | None = None,
) -> int:
    """Append immutable PIT revisions before refreshing the legacy cache."""
    df = df if df is not None else pd.DataFrame()
    now_dt = (observed_at or datetime.now()).replace(microsecond=0)
    now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    batch_id = f"adata-finance-{now_dt.strftime('%Y%m%dT%H%M%S')}"
    cols = [
        "stock_code", "short_name", "report_date", "report_type", "notice_date",
        "basic_eps", "diluted_eps", "non_gaap_eps", "net_asset_ps",
        "cap_reserve_ps", "undist_profit_ps", "oper_cf_ps",
        "total_rev", "gross_profit", "net_profit_attr_sh", "non_gaap_net_profit",
        "total_rev_yoy_gr", "net_profit_yoy_gr", "non_gaap_net_profit_yoy_gr",
        "total_rev_qoq_gr", "net_profit_qoq_gr",
        "roe_wtd", "roe_non_gaap_wtd", "roa_wtd", "gross_margin", "net_margin",
        "curr_ratio", "quick_ratio", "cash_flow_ratio", "asset_liab_ratio",
    ]

    # 只保留存在的列
    available = [c for c in cols if c in df.columns]
    evidence_columns = [
        column
        for column in (
            "published_at",
            "publication_source",
            "publication_event_key",
            "publication_url",
            "publication_received_at",
            "publication_data_version",
            "publication_content_sha256",
        )
        if column in df.columns
    ]
    frame = (
        df[[*available, *evidence_columns]].copy()
        if available
        else pd.DataFrame()
    )

    # 数值列转为 float（防止 pandas object 类型）
    non_numeric_columns = {
        "stock_code", "short_name", "report_date", "report_type",
        "notice_date", *evidence_columns,
    }
    for c in frame.columns:
        if c not in non_numeric_columns:
            frame[c] = pd.to_numeric(frame[c], errors="coerce")

    codes = sorted({
        str(value).strip().zfill(6)
        for value in (frame.get("stock_code", pd.Series(dtype=str)).tolist())
        if str(value).strip()
    })
    requested_code = str(stock_code or "").strip().zfill(6)
    if requested_code and requested_code != "000000":
        if codes and codes != [requested_code]:
            raise ValueError("finance response stock identity differs from request")
        codes = [requested_code]
    if not codes:
        raise ValueError("finance coverage requires the requested stock code")
    if len(codes) != 1:
        raise ValueError("finance coverage transaction must contain one stock")

    if frame.empty:
        raise ValueError("finance provider response must contain at least one row")
    missing_identity = {"stock_code", "report_date"} - set(frame.columns)
    if missing_identity:
        raise ValueError(
            "finance response is missing required identity columns: "
            + ", ".join(sorted(missing_identity))
        )

    # 构造 INSERT SQL
    placeholders = ", ".join([f":{c}" for c in available])
    col_names = ", ".join(available)
    update_clause = ", ".join([f"{c} = VALUES({c})" for c in available if c not in ("stock_code", "report_date")])
    update_clause += ", etl_sync_at = VALUES(etl_sync_at)"

    sql = text(f"""
        INSERT INTO si_stock_finance ({col_names}, etl_sync_at)
        VALUES ({placeholders}, :etl_sync_at)
        ON DUPLICATE KEY UPDATE {update_clause}
    """)

    count = 0
    with engine.begin() as conn:
        source_rows: list[dict] = []
        fact_bindings: list[dict] = []
        for _, row in frame.iterrows():
            params = {c: (None if pd.isna(row[c]) else row[c]) for c in available}
            fact_params = {
                **params,
                **{
                    column: (
                        None if pd.isna(row[column]) else row[column]
                    )
                    for column in evidence_columns
                },
            }
            # The append-only fact is the strategy source of truth.  A missing
            # PIT schema or an invalid identity aborts the transaction before
            # the mutable display cache can advance on its own.
            receipt = append_finance_revision(
                conn,
                fact_params,
                known_at=now_dt,
                received_at=now_dt,
                source=PRIMARY_FINANCE_SOURCE,
                batch_id=batch_id,
            )
            source_rows.append(dict(fact_params))
            fact_bindings.append({
                "revision_id": receipt.revision_id,
                "content_hash": receipt.content_hash,
            })
            params["etl_sync_at"] = now
            conn.execute(sql, params)
            count += 1
        append_source_coverage(
            conn,
            fact_kind="finance",
            stock_code=codes[0],
            window_start="1900-01-01",
            window_end=now_dt.date(),
            known_at=now_dt,
            received_at=now_dt,
            covered_through_at=now_dt,
            watermark_kind="CAPTURED_AT",
            watermark_evidence={
                "provider": PRIMARY_FINANCE_SOURCE,
                "capture": "successful_function_return",
                "adapter_contract_version": PRIMARY_FINANCE_CONTRACT_VERSION,
            },
            source_rows=source_rows,
            fact_bindings=fact_bindings,
            source=PRIMARY_FINANCE_SOURCE,
            batch_id=batch_id,
        )

    return count


def minimum_expected_report_date(as_of: date) -> date:
    """Backward-compatible accessor for the current disclosure-period floor."""

    return finance_disclosure_gate(as_of).minimum_report_date


def validate_finance_response(
    stock_code: str,
    frame: pd.DataFrame,
    *,
    as_of: date,
    minimum_report_date: date,
    listing_date: date | None = None,
    disclosure_deadline: date | None = None,
) -> date:
    """Validate non-empty provider identity and a reasonable latest period."""

    if frame is None or frame.empty:
        raise RuntimeError(
            f"DATA_BLOCKED: {stock_code} 财务源返回空结果，禁止记录完整覆盖"
        )
    required = {"stock_code", "report_date"}
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(
            f"DATA_BLOCKED: {stock_code} 财务响应缺少字段: "
            + ", ".join(sorted(missing))
        )
    requested = str(stock_code).strip().zfill(6)
    observed_codes = {
        str(value or "").strip().zfill(6)
        for value in frame["stock_code"].tolist()
    }
    if observed_codes != {requested}:
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 财务响应代码集合不一致: "
            f"{sorted(observed_codes)}"
        )
    report_dates = pd.to_datetime(frame["report_date"], errors="coerce").dt.date
    if report_dates.isna().any():
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 财务响应含无效报告期"
        )
    invalid_periods = [
        value for value in report_dates
        if (value.month, value.day) not in {(3, 31), (6, 30), (9, 30), (12, 31)}
    ]
    if invalid_periods:
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 财务响应含非标准报告期: "
            f"{sorted(set(invalid_periods))[:5]}"
        )
    latest = max(report_dates)
    if latest > as_of:
        raise RuntimeError(
            f"DATA_BLOCKED: {requested} 最新报告期 {latest} 晚于采集日 {as_of}"
        )
    gate = FinanceDisclosureGate(
        minimum_report_date=minimum_report_date,
        disclosure_deadline=disclosure_deadline or minimum_report_date,
    )
    if report_period_gate_applies(listing_date, gate) and latest < minimum_report_date:
        raise FinanceStaleResponse(
            f"DATA_BLOCKED: {requested} 最新报告期过旧: latest={latest}, "
            f"minimum={minimum_report_date}"
        )
    return latest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="同步股票财务核心指标")
    parser.add_argument("--code", type=str, default=None, help="同步单只股票代码")
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="全市场同步起始偏移；用于可验证的互不重叠人工分片",
    )
    parser.add_argument("--limit", type=int, default=None, help="只同步前N只")
    parser.add_argument("--sleep", type=float, default=0.3, help="每只股票间隔秒数（防限流）")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help=(
            "财务源并发抓取线程数；默认1保持兼容，正式并发建议4，"
            f"允许1..{MAX_FETCH_WORKERS}"
        ),
    )
    parser.add_argument(
        "--min-code-coverage",
        type=float,
        default=1.0,
        help="非空股票覆盖率；正式任务固定要求 1.0，不能降低",
    )
    parser.add_argument(
        "--min-report-date",
        default="",
        help="最新报告期下限 YYYY-MM-DD；默认按法定披露窗口推导",
    )
    parser.add_argument(
        "--seal-existing",
        action="store_true",
        help=(
            "不请求外部源；严格复核当前全目录PIT coverage并追加原子批次完成水位"
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--daily-incremental",
        action="store_true",
        help="仅刷新公告/目录/失败/缺口/到期复核/版本变化候选",
    )
    mode.add_argument(
        "--full-baseline",
        action="store_true",
        help="独立历史修复：重抓并重验当前完整股票目录",
    )
    parser.add_argument(
        "--as-of-date",
        default="",
        help="调度器绑定的目标交易日 YYYY-MM-DD；默认当前日期",
    )
    parser.add_argument(
        "--checkpoint-file",
        default=DEFAULT_FINANCE_CHECKPOINT_FILE,
        help="按目标日、股票和候选输入根保存的可恢复断点",
    )
    args = parser.parse_args(argv)

    if args.seal_existing and (args.daily_incremental or args.full_baseline):
        print(
            "[ERROR] DATA_BLOCKED: --seal-existing cannot be combined with a run mode",
            file=sys.stderr,
        )
        return 2
    if args.daily_incremental and (args.code or args.offset or args.limit):
        print(
            "[ERROR] DATA_BLOCKED: daily finance candidates cannot be manually sliced",
            file=sys.stderr,
        )
        return 2
    if args.min_code_coverage != 1.0:
        print(
            "[ERROR] DATA_BLOCKED: finance production code coverage is fixed at 1.0",
            file=sys.stderr,
        )
        return 2
    if not 1 <= args.workers <= MAX_FETCH_WORKERS:
        print(
            "[ERROR] DATA_BLOCKED: finance fetch workers must be between "
            f"1 and {MAX_FETCH_WORKERS}",
            file=sys.stderr,
        )
        return 2
    if args.offset < 0 or (args.code and args.offset != 0):
        print(
            "[ERROR] DATA_BLOCKED: finance offset must be non-negative and "
            "cannot be combined with --code",
            file=sys.stderr,
        )
        return 2
    try:
        run_as_of = (
            datetime.strptime(args.as_of_date, "%Y-%m-%d").date()
            if args.as_of_date
            else datetime.now().date()
        )
    except ValueError:
        print("[ERROR] --as-of-date 必须为 YYYY-MM-DD", file=sys.stderr)
        return 2
    try:
        disclosure_gate = finance_disclosure_gate(run_as_of)
        if args.min_report_date:
            explicit_minimum = datetime.strptime(
                args.min_report_date, "%Y-%m-%d"
            ).date()
            disclosure_gate = FinanceDisclosureGate(
                minimum_report_date=explicit_minimum,
                disclosure_deadline=run_as_of,
            )
        min_report_date = disclosure_gate.minimum_report_date
    except ValueError:
        print("[ERROR] --min-report-date 必须为 YYYY-MM-DD", file=sys.stderr)
        return 2
    if min_report_date > run_as_of:
        print("[ERROR] --min-report-date 不能晚于当前日期", file=sys.stderr)
        return 2

    engine = get_engine()
    try:
        if args.seal_existing:
            if args.code or args.offset or args.limit:
                print(
                    "[ERROR] DATA_BLOCKED: --seal-existing requires the full catalog",
                    file=sys.stderr,
                )
                return 2
            try:
                seal = append_finance_atomic_batch_seal(
                    engine,
                    as_of_date=run_as_of,
                    completed_known_at=datetime.now().replace(microsecond=0),
                    changed_codes=[],
                    provider_contract_version=(
                        PRIMARY_FINANCE_CONTRACT_VERSION
                    ),
                )
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "schema": "probiga.finance-atomic-batch-result.v1",
                            "status": "DATA_BLOCKED",
                            "as_of": run_as_of.isoformat(),
                            "reason": f"{type(exc).__name__}:{exc}",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
                return 1
            print(
                json.dumps(
                    {
                        **seal,
                        "seal_schema": seal.get("schema"),
                        "schema": "probiga.finance-atomic-batch-result.v1",
                        "status": "PASS",
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
            return 0
        universe = get_finance_stock_universe(engine)
        if not universe:
            print("[ERROR] DATA_BLOCKED: finance stock universe is empty")
            return 2

        candidate_plan: FinanceCandidatePlan | None = None
        checkpoint_path: Path | None = None
        checkpoint_state: dict[str, Any] = {}
        resumed_codes: set[str] = set()
        if args.daily_incremental:
            checkpoint_path = Path(args.checkpoint_file)
            try:
                previous_checkpoint = _checkpoint_payload(checkpoint_path)
                candidate_plan = select_daily_finance_candidates(
                    engine,
                    universe,
                    as_of=run_as_of,
                    checkpoint=previous_checkpoint,
                )
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "schema": "probiga.finance-sync-result.v2",
                            "status": "DATA_BLOCKED",
                            "as_of": run_as_of.isoformat(),
                            "reason": f"{type(exc).__name__}:{exc}",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                )
                return 1
            planned_codes = list(candidate_plan.codes)
            if (
                str(previous_checkpoint.get("as_of") or "")
                == run_as_of.isoformat()
                and str(previous_checkpoint.get("input_root_sha256") or "")
                == candidate_plan.input_root_sha256
            ):
                resumed_codes = {
                    str(code).zfill(6)
                    for code in previous_checkpoint.get("completed_codes") or ()
                    if str(code).zfill(6) in planned_codes
                }
            codes = [code for code in planned_codes if code not in resumed_codes]
            checkpoint_state = {
                "schema": "probiga.finance-daily-checkpoint.v2",
                "status": "IN_PROGRESS",
                "as_of": run_as_of.isoformat(),
                "input_root_sha256": candidate_plan.input_root_sha256,
                "parent_batch_root_sha256": (
                    candidate_plan.parent_batch_root_sha256
                ),
                "planned_codes": planned_codes,
                "completed_codes": sorted(resumed_codes),
                "unresolved_codes": sorted(set(planned_codes) - resumed_codes),
                "updated_at": datetime.now().replace(microsecond=0).isoformat(),
            }
            _write_checkpoint(checkpoint_path, checkpoint_state)
        elif args.code:
            requested_code = args.code.strip().zfill(6)
            if requested_code not in universe:
                print(
                    "[ERROR] DATA_BLOCKED: requested finance stock is absent "
                    f"from si_all_code: {requested_code}"
                )
                return 2
            codes = [requested_code]
            planned_codes = list(codes)
        else:
            codes = list(universe)
            if args.offset:
                codes = codes[args.offset:]
            if args.limit and args.limit > 0:
                codes = codes[: args.limit]
            planned_codes = list(codes)

        codes = list(dict.fromkeys(str(code).strip().zfill(6) for code in codes))
        planned_codes = list(dict.fromkeys(
            str(code).strip().zfill(6) for code in planned_codes
        ))
        print(
            f"[INFO] 财务模式={'DAILY_INCREMENTAL' if args.daily_incremental else 'FULL_OR_MANUAL'}, "
            f"候选 {len(planned_codes)} 只，待请求 {len(codes)} 只，"
            f"断点已完成 {len(resumed_codes)} 只"
        )
        if not planned_codes and not args.daily_incremental:
            print("[ERROR] DATA_BLOCKED: finance requested scope is empty")
            return 2

        total_rows = 0
        failures: list[dict[str, str]] = []
        completed_codes: list[str] = []
        latest_periods: dict[str, str] = {}
        applicable_latest_periods: dict[str, str] = {}
        exempt_new_listing_codes: list[str] = []
        expected_unavailable_codes: list[str] = []
        expected_unavailable_details: dict[str, dict[str, str]] = {}
        fetches = iter_finance_fetches(
            codes,
            workers=args.workers,
            sleep_seconds=args.sleep,
        )
        for i, (code, df, fetch_error) in enumerate(fetches):
            resolved_this_code = False
            try:
                if fetch_error is not None:
                    raise fetch_error
                if df is None:
                    raise RuntimeError(
                        f"DATA_BLOCKED: {code} 财务源未返回响应"
                    )
                try:
                    latest = validate_finance_response(
                        code,
                        df,
                        as_of=run_as_of,
                        minimum_report_date=min_report_date,
                        listing_date=universe[code],
                        disclosure_deadline=disclosure_gate.disclosure_deadline,
                    )
                except FinanceStaleResponse:
                    if code not in CNINFO_NONFILING_ISSUERS:
                        raise
                    evidence = fetch_cninfo_nonfiling_evidence(
                        code,
                        as_of=run_as_of,
                        expected_report_date=min_report_date,
                    )
                    observed_at = datetime.now().replace(microsecond=0)
                    with engine.begin() as connection:
                        receipt = append_finance_expected_unavailable(
                            connection,
                            stock_code=code,
                            expected_report_date=min_report_date,
                            known_at=observed_at,
                            received_at=observed_at,
                            official_evidence=evidence,
                            batch_id=(
                                "cninfo-finance-nonfiling-"
                                + observed_at.strftime("%Y%m%dT%H%M%S")
                            ),
                        )
                    expected_unavailable_codes.append(code)
                    expected_unavailable_details[code] = {
                        "reason_code": str(evidence["reason_code"]),
                        "announcement_id": str(evidence["announcement_id"]),
                        "valid_until": str(evidence["valid_until"]),
                        "next_retry_date": str(evidence["next_retry_date"]),
                        "disposition_id": receipt.coverage_id,
                    }
                    resolved_this_code = True
                else:
                    if args.daily_incremental:
                        df = enrich_finance_publication_evidence(
                            engine,
                            df,
                            stock_code=code,
                            observed_at=datetime.now().replace(microsecond=0),
                        )
                    rows = upsert_finance(engine, df, stock_code=code)
                    if rows <= 0:
                        raise RuntimeError(
                            f"DATA_BLOCKED: {code} 财务源未提交任何报告期"
                        )
                    total_rows += rows
                    completed_codes.append(code)
                    latest_periods[code] = latest.isoformat()
                    if report_period_gate_applies(universe[code], disclosure_gate):
                        applicable_latest_periods[code] = latest.isoformat()
                    else:
                        exempt_new_listing_codes.append(code)
                    resolved_this_code = True
            except Exception as exc:
                print(f"  [WARN] {code} 获取/写入失败: {exc}")
                failures.append({"stock_code": code, "error": str(exc)})

            if checkpoint_path is not None:
                completed_checkpoint_codes = {
                    str(item).zfill(6)
                    for item in checkpoint_state.get("completed_codes") or ()
                }
                if resolved_this_code:
                    completed_checkpoint_codes.add(code)
                checkpoint_state.update({
                    "completed_codes": sorted(completed_checkpoint_codes),
                    "unresolved_codes": sorted(
                        set(planned_codes) - completed_checkpoint_codes
                    ),
                    "updated_at": datetime.now().replace(
                        microsecond=0
                    ).isoformat(),
                })
                _write_checkpoint(checkpoint_path, checkpoint_state)

            if (i + 1) % 50 == 0:
                print(
                    f"[PROGRESS] {i + 1}/{len(codes)}, 已写入 {total_rows} 条, "
                    f"失败 {len(failures)}"
                )

        resolved_codes = (
            set(resumed_codes)
            | set(completed_codes)
            | set(expected_unavailable_codes)
        )
        nonempty_count = len(set(resumed_codes) | set(completed_codes))
        denominator = len(planned_codes)
        coverage = nonempty_count / denominator if denominator else 1.0
        resolved_count = len(resolved_codes)
        resolution_coverage = (
            resolved_count / denominator if denominator else 1.0
        )
        atomic_batch: dict[str, Any] = {}
        full_catalog_run = bool(
            args.daily_incremental
            or (not args.code and args.offset == 0 and not args.limit)
        )
        if (
            full_catalog_run
            and not failures
            and resolution_coverage == 1.0
        ):
            try:
                atomic_batch = append_finance_atomic_batch_seal(
                    engine,
                    as_of_date=run_as_of,
                    completed_known_at=datetime.now().replace(microsecond=0),
                    changed_codes=(
                        planned_codes if args.daily_incremental else None
                    ),
                    provider_contract_version=(
                        PRIMARY_FINANCE_CONTRACT_VERSION
                    ),
                )
            except Exception as exc:
                failures.append({
                    "stock_code": "ATOMIC_BATCH_SEAL",
                    "error": f"{type(exc).__name__}:{exc}",
                })
        if checkpoint_path is not None:
            checkpoint_state.update({
                "status": "COMPLETE" if not failures else "DATA_BLOCKED",
                "unresolved_codes": (
                    []
                    if not failures
                    else sorted(set(planned_codes) - resolved_codes)
                ),
                "atomic_batch_root_sha256": str(
                    atomic_batch.get("batch_root_sha256") or ""
                ),
                "updated_at": datetime.now().replace(microsecond=0).isoformat(),
            })
            _write_checkpoint(checkpoint_path, checkpoint_state)
        report = {
            "schema": "probiga.finance-sync-result.v2",
            "status": (
                "PASS"
                if not failures and resolution_coverage == 1.0
                else "DATA_BLOCKED"
            ),
            "as_of": run_as_of.isoformat(),
            "minimum_report_date": min_report_date.isoformat(),
            "minimum_report_disclosure_deadline": (
                disclosure_gate.disclosure_deadline.isoformat()
            ),
            "mode": (
                "DAILY_INCREMENTAL"
                if args.daily_incremental
                else "FULL_BASELINE_OR_MANUAL"
            ),
            "requested_code_count": len(planned_codes),
            "provider_request_code_count": len(codes),
            "checkpoint_resumed_code_count": len(resumed_codes),
            "nonempty_code_count": nonempty_count,
            "nonempty_code_coverage": coverage,
            "expected_unavailable_code_count": len(expected_unavailable_codes),
            "expected_unavailable_code_sample": expected_unavailable_details,
            "resolved_code_count": resolved_count,
            "resolution_coverage": resolution_coverage,
            "written_report_count": total_rows,
            "failure_count": len(failures),
            "failure_sample": failures[:20],
            "atomic_batch": atomic_batch,
            "candidate_input_root_sha256": (
                candidate_plan.input_root_sha256 if candidate_plan else ""
            ),
            "candidate_reasons": (
                candidate_plan.reasons if candidate_plan else {}
            ),
            "parent_batch_root_sha256": (
                candidate_plan.parent_batch_root_sha256
                if candidate_plan
                else ""
            ),
            "report_period_applicable_code_count": len(applicable_latest_periods),
            "new_listing_period_exempt_code_count": len(exempt_new_listing_codes),
            "oldest_latest_report_date": (
                min(latest_periods.values()) if latest_periods else None
            ),
            "oldest_latest_applicable_report_date": (
                min(applicable_latest_periods.values())
                if applicable_latest_periods else None
            ),
        }
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        if failures or resolution_coverage != 1.0:
            print(
                f"[FAILED] DATA_BLOCKED: 财务同步未完整: {denominator} 只股票, "
                f"非空覆盖 {nonempty_count}/{denominator} ({coverage:.2%}), "
                f"依法暂不可得 {len(expected_unavailable_codes)}, "
                f"写入 {total_rows} 条报告期, 失败 {len(failures)}"
            )
            return 1
        print(
            f"[OK] 同步完成: {denominator} 只股票, "
            f"已解析 {resolved_count}/{denominator}, "
            f"依法暂不可得 {len(expected_unavailable_codes)}, "
            f"写入 {total_rows} 条报告期, 失败 0"
        )
        return 0
    finally:
        dispose = getattr(engine, "dispose", None)
        if callable(dispose):
            dispose()


if __name__ == "__main__":
    raise SystemExit(main())
