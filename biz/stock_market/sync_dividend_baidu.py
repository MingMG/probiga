#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Synchronize the full authoritative stock universe from Baidu dividends.

This is the provider-specific replacement for the legacy generic
``run_single_table.py sm_dividend`` path.  Every immutable-QMT/``si_all_code``
target must return either validated rows or independently confirmed successful
empty evidence before the existing table slice is replaced in one transaction.
No runtime DDL is permitted.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.adata_release import ensure_adata_import_path, production_mode
from server.common.qmt_stock_catalog import (
    load_target_stock_catalog,
    validate_stock_catalog_runtime_schema,
)
from tools.env_config import create_tool_engine, load_project_env


SHANGHAI = ZoneInfo("Asia/Shanghai")
PROVIDER_ID = "adata_stock_dividend_baidu"
PROVIDER_METHOD = "adata.stock.market.StockDividend.get_dividend"
RECEIPT_SCHEMA = "probiga.stock-dividend-baidu-receipt.v1"
BAIDU_URL = (
    "https://gushitong.baidu.com/opendata?openapi=1&dspName=iphone&"
    "tn=tangram&client=app&query={code}&code={code}&word={code}&"
    "resource_id=5429&ma_ver=4&finClientType=pc"
)
REQUIRED_DIVIDEND_COLUMNS = frozenset(
    {"stock_code", "report_date", "dividend_plan", "ex_dividend_date"}
)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _code(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise RuntimeError("dividend universe contains an empty stock code")
    code = raw.zfill(6)
    if len(code) != 6 or not code.isdigit():
        raise RuntimeError(f"invalid dividend stock code: {value!r}")
    return code


def code_set_hash(codes: Iterable[str]) -> str:
    normalized = sorted({_code(code) for code in codes})
    return hashlib.sha256("\n".join(normalized).encode("ascii")).hexdigest()


def _receipt(payload: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["receipt_id"] = _digest(result)
    return result


def _release_hash(value: Any, length: int) -> str | None:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    if len(normalized) != length or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise RuntimeError("adata release identity is malformed")
    return normalized


def adata_release_identity() -> dict[str, Any]:
    git_sha = _release_hash(os.environ.get("PROBIGA_EXPECTED_ADATA_SHA"), 40)
    tree_sha = _release_hash(
        os.environ.get("PROBIGA_EXPECTED_ADATA_TREE_SHA256"), 64
    )
    if production_mode() and (git_sha is None or tree_sha is None):
        raise RuntimeError("production dividend task lacks sealed adata identity")
    return {
        "git_sha": git_sha or "development_unsealed",
        "tree_sha256": tree_sha or "development_unsealed",
        "method": PROVIDER_METHOD,
        "empty_evidence": "baidu_result_code_0_structural_probe",
    }


def validate_runtime_schema(engine: Any) -> dict[str, Any]:
    """Require the existing legacy cache shape; never migrate as runtime."""

    required = {
        "sm_dividend": {
            "stock_code",
            "report_date",
            "dividend_plan",
            "ex_dividend_date",
            "etl_sync_at",
        },
        "si_all_code": {"stock_code", "list_date"},
    }
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT TABLE_NAME, COLUMN_NAME
                  FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA=DATABASE()
                   AND TABLE_NAME IN ('sm_dividend','si_all_code')
                """
            )
        ).fetchall()
    observed: dict[str, set[str]] = {table: set() for table in required}
    for table_name, column_name in rows:
        observed.setdefault(str(table_name), set()).add(str(column_name))
    missing = {
        table: sorted(columns - observed.get(table, set()))
        for table, columns in required.items()
        if columns - observed.get(table, set())
    }
    if missing:
        raise RuntimeError(
            "dividend runtime schema migration is missing: "
            + _canonical_json(missing)
        )
    return {
        "status": "PASS",
        "schema_hash": _digest(
            {table: sorted(observed[table]) for table in sorted(observed)}
        ),
    }


@dataclass(frozen=True)
class DividendUniverse:
    as_of: str
    codes: tuple[str, ...]
    code_set_hash: str
    catalog_batch_id: str
    catalog_manifest_hash: str
    catalog_member_set_hash: str
    catalog_captured_at: str


def load_authoritative_universe(
    engine: Any,
    *,
    as_of: str,
    known_at: datetime,
) -> DividendUniverse:
    """Require exact equality between immutable QMT and business catalog."""

    validate_stock_catalog_runtime_schema(engine)
    catalog, catalog_codes = load_target_stock_catalog(
        engine,
        target_date=as_of,
        decision_known_at=known_at.replace(tzinfo=None),
    )
    qmt_codes = tuple(sorted(_code(code) for code in catalog_codes))
    if len(set(qmt_codes)) != len(qmt_codes):
        raise RuntimeError("immutable QMT dividend universe contains duplicates")
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT stock_code
                  FROM si_all_code
                 WHERE stock_code REGEXP '^(0|3|4|6|8|9)[0-9]{5}$'
                   AND (list_date IS NULL OR list_date <= :as_of)
                 ORDER BY stock_code
                """
            ),
            {"as_of": as_of},
        ).fetchall()
    business_codes = tuple(_code(row[0]) for row in rows)
    if len(set(business_codes)) != len(business_codes):
        raise RuntimeError("si_all_code dividend universe contains duplicates")
    if business_codes != qmt_codes:
        qmt = set(qmt_codes)
        business = set(business_codes)
        raise RuntimeError(
            "dividend QMT/si_all_code universe differs: "
            f"qmt={len(qmt)}, si_all_code={len(business)}, "
            f"qmt_only={sorted(qmt - business)[:20]}, "
            f"si_only={sorted(business - qmt)[:20]}"
        )
    if not qmt_codes:
        raise RuntimeError("authoritative dividend universe is empty")
    return DividendUniverse(
        as_of=as_of,
        codes=qmt_codes,
        code_set_hash=code_set_hash(qmt_codes),
        catalog_batch_id=catalog.batch_id,
        catalog_manifest_hash=catalog.manifest_hash,
        catalog_member_set_hash=catalog.member_set_hash,
        catalog_captured_at=catalog.captured_at,
    )


@dataclass(frozen=True)
class DividendFetchResult:
    stock_code: str
    status: str
    rows: tuple[dict[str, Any], ...]
    evidence: str


def authoritative_empty_reason(payload: Any, *, code: str) -> str:
    """Return explicit successful-empty evidence or fail on ambiguity."""

    if not isinstance(payload, dict) or str(payload.get("ResultCode")) != "0":
        raise RuntimeError(
            f"Baidu dividend empty probe was not authoritative for {code}"
        )
    result = payload.get("Result")
    if result is None or result == []:
        return "baidu_result_code_0_empty_result"
    if not isinstance(result, list) or not isinstance(result[-1], dict):
        raise RuntimeError(
            f"Baidu dividend empty probe structure differs for {code}"
        )
    try:
        result_node = result[-1]["DisplayData"]["resultData"]["tplData"][
            "result"
        ]
        tabs = result_node["tabs"]
        if not isinstance(tabs, list) or not tabs:
            raise KeyError("tabs")
        new_company = tabs[-1]["content"]["newCompany"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(
            f"Baidu dividend empty probe structure differs for {code}"
        ) from exc
    if not isinstance(new_company, dict):
        raise RuntimeError(
            f"Baidu dividend empty probe company node differs for {code}"
        )
    if "bonusTransfer" not in new_company:
        return "baidu_result_code_0_bonus_section_absent"
    bonus_transfer = new_company.get("bonusTransfer")
    if not isinstance(bonus_transfer, dict) or "body" not in bonus_transfer:
        raise RuntimeError(
            f"Baidu dividend empty probe bonus node differs for {code}"
        )
    body = bonus_transfer.get("body")
    if body is None or body == []:
        return "baidu_result_code_0_bonus_body_empty"
    if not isinstance(body, list):
        raise RuntimeError(
            f"Baidu dividend empty probe body differs for {code}"
        )
    plans: list[str] = []
    for item in body:
        if isinstance(item, Mapping):
            plan = item.get("分红方案") or item.get("dividend_plan")
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            plan = item[1]
        else:
            raise RuntimeError(
                f"Baidu dividend empty probe row differs for {code}"
            )
        plans.append(str(plan or "").strip())
    if plans and all(plan == "利润不分配" for plan in plans):
        return "baidu_result_code_0_all_non_distribution_plans"
    raise RuntimeError(
        f"adata returned empty but Baidu contains dividend rows for {code}"
    )


class AdataBaiduDividendProvider:
    """Wrap StockDividend and prove empty returns against the raw response."""

    def __init__(
        self,
        *,
        client_factory: Callable[[], Any] | None = None,
        empty_probe: Callable[[str], str] | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._client_factory = client_factory
        self._empty_probe = empty_probe
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    @staticmethod
    def _default_client_factory() -> Any:
        from adata.stock.market.stock_dividend import StockDividend

        return StockDividend()

    def _probe_successful_empty(self, code: str) -> str:
        """Distinguish provider-confirmed no rows from adata's error empty."""

        from adata.common.headers import baidu_headers

        response = requests.get(
            BAIDU_URL.format(code=code),
            headers=baidu_headers.text_headers,
            timeout=self._timeout_seconds,
        )
        if response.status_code != 200 or not response.text:
            raise RuntimeError(
                f"Baidu dividend empty probe HTTP failure for {code}: "
                f"status={response.status_code}"
            )
        try:
            payload = response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"Baidu dividend empty probe returned invalid JSON for {code}"
            ) from exc
        return authoritative_empty_reason(payload, code=code)

    @staticmethod
    def _normalize_nonempty(
        code: str,
        frame: pd.DataFrame,
        *,
        as_of: str,
        observed_at: datetime,
    ) -> tuple[dict[str, Any], ...]:
        missing = REQUIRED_DIVIDEND_COLUMNS - set(frame.columns)
        if missing:
            raise RuntimeError(
                f"Baidu dividend response lacks columns for {code}: {sorted(missing)}"
            )
        out = frame.loc[:, sorted(REQUIRED_DIVIDEND_COLUMNS)].copy()
        out["stock_code"] = out["stock_code"].map(_code)
        if set(out["stock_code"]) != {code}:
            raise RuntimeError(f"Baidu dividend response identity differs for {code}")
        report_dates = pd.to_datetime(out["report_date"], errors="coerce")
        if report_dates.isna().any():
            raise RuntimeError(f"Baidu dividend response has invalid dates for {code}")
        if bool((report_dates.dt.date > date.fromisoformat(as_of)).any()):
            raise RuntimeError(
                f"Baidu dividend response has future announcement date for {code}"
            )
        out["report_date"] = report_dates.dt.date.map(date.isoformat)
        ex_dates = pd.to_datetime(out["ex_dividend_date"], errors="coerce")
        invalid_ex = (
            out["ex_dividend_date"].notna()
            & out["ex_dividend_date"].astype(str).str.strip().ne("")
            & ex_dates.isna()
        )
        if bool(invalid_ex.any()):
            raise RuntimeError(
                f"Baidu dividend response has invalid ex date for {code}"
            )
        out["ex_dividend_date"] = [
            None if pd.isna(value) else value.date().isoformat()
            for value in ex_dates
        ]
        out["dividend_plan"] = out["dividend_plan"].fillna("").astype(str).str.strip()
        if bool(
            out["dividend_plan"].eq("").any()
            or out["dividend_plan"].eq("利润不分配").any()
        ):
            raise RuntimeError(f"Baidu dividend response has invalid plan for {code}")
        key_columns = ["stock_code", "report_date"]
        if bool(out.duplicated(key_columns, keep=False).any()):
            raise RuntimeError(
                f"Baidu dividend response duplicates business identity for {code}"
            )
        out["etl_sync_at"] = observed_at.replace(tzinfo=None, microsecond=0)
        return tuple(
            out.sort_values(key_columns).to_dict(orient="records")
        )

    def fetch(
        self,
        code: str,
        *,
        as_of: str,
        observed_at: datetime,
    ) -> DividendFetchResult:
        client_factory = self._client_factory or self._default_client_factory
        frame = client_factory().get_dividend(stock_code=code)
        if not isinstance(frame, pd.DataFrame):
            raise RuntimeError(f"adata StockDividend returned non-frame for {code}")
        if frame.empty:
            probe = self._empty_probe or self._probe_successful_empty
            evidence = probe(code)
            if not str(evidence or "").startswith("baidu_result_code_0_"):
                raise RuntimeError(
                    f"Baidu dividend empty evidence is not authoritative for {code}"
                )
            return DividendFetchResult(
                stock_code=code,
                status="AUTHORITATIVE_EMPTY",
                rows=(),
                evidence=str(evidence),
            )
        rows = self._normalize_nonempty(
            code,
            frame,
            as_of=as_of,
            observed_at=observed_at,
        )
        return DividendFetchResult(
            stock_code=code,
            status="NONEMPTY",
            rows=rows,
            evidence="adata_stock_dividend_validated_rows",
        )


@dataclass(frozen=True)
class DividendCollection:
    requested_codes: tuple[str, ...]
    responded_codes: tuple[str, ...]
    nonempty_codes: tuple[str, ...]
    empty_codes: tuple[str, ...]
    failures: tuple[dict[str, str], ...]
    rows: tuple[dict[str, Any], ...]
    response_status_manifest_hash: str = ""


def collect_snapshot(
    codes: Iterable[str],
    *,
    provider: AdataBaiduDividendProvider,
    as_of: str,
    observed_at: datetime,
    workers: int,
    sleep_seconds: float,
) -> DividendCollection:
    """Attempt every request even when earlier codes fail."""

    requested = tuple(sorted(_code(code) for code in codes))
    if not requested or len(set(requested)) != len(requested):
        raise RuntimeError("dividend requested universe is empty or duplicated")

    def _one(code: str) -> DividendFetchResult:
        try:
            return provider.fetch(code, as_of=as_of, observed_at=observed_at)
        finally:
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

    results: dict[str, DividendFetchResult] = {}
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        futures = {pool.submit(_one, code): code for code in requested}
        for future in as_completed(futures):
            code = futures[future]
            try:
                result = future.result()
                if result.stock_code != code:
                    raise RuntimeError("provider response code differs")
                results[code] = result
            except Exception as exc:
                failures.append(
                    {
                        "stock_code": code,
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:500],
                    }
                )
    responded = tuple(sorted(results))
    nonempty = tuple(
        sorted(code for code, result in results.items() if result.status == "NONEMPTY")
    )
    empty = tuple(
        sorted(
            code
            for code, result in results.items()
            if result.status == "AUTHORITATIVE_EMPTY"
        )
    )
    unknown_status = sorted(set(results) - set(nonempty) - set(empty))
    if unknown_status:
        failures.extend(
            {
                "stock_code": code,
                "error_type": "ProviderStatusError",
                "error": "provider returned an unsupported status",
            }
            for code in unknown_status
        )
    rows = tuple(
        row
        for code in nonempty
        for row in results[code].rows
    )
    failure_by_code = {item["stock_code"]: item for item in failures}
    response_manifest: list[dict[str, Any]] = []
    for code in requested:
        result = results.get(code)
        if result is not None:
            canonical_rows = canonical_dividend_rows(result.rows)
            response_manifest.append(
                {
                    "stock_code": code,
                    "status": result.status,
                    "evidence": result.evidence,
                    "row_count": len(canonical_rows),
                    "row_hash": _digest(canonical_rows),
                }
            )
        else:
            failure = failure_by_code[code]
            response_manifest.append(
                {
                    "stock_code": code,
                    "status": "FAILURE",
                    "error_type": failure["error_type"],
                    "error_hash": _digest(failure["error"]),
                }
            )
    return DividendCollection(
        requested_codes=requested,
        responded_codes=responded,
        nonempty_codes=nonempty,
        empty_codes=empty,
        failures=tuple(sorted(failures, key=lambda item: item["stock_code"])),
        rows=rows,
        response_status_manifest_hash=_digest(response_manifest),
    )


def validate_collection(
    collection: DividendCollection,
    *,
    min_nonempty_code_ratio: float,
) -> dict[str, Any]:
    requested = set(collection.requested_codes)
    responded = set(collection.responded_codes)
    failures = {item["stock_code"] for item in collection.failures}
    if responded | failures != requested or responded & failures:
        raise RuntimeError("dividend collection did not account for every request")
    if collection.failures:
        raise RuntimeError(
            "dividend provider failures block publication: "
            f"failure_count={len(collection.failures)}, "
            f"sample={list(collection.failures[:5])}"
        )
    if responded != requested:
        raise RuntimeError("dividend responded code set differs from requested set")
    if set(collection.nonempty_codes) | set(collection.empty_codes) != requested:
        raise RuntimeError("dividend result statuses do not cover the universe")
    if set(collection.nonempty_codes) & set(collection.empty_codes):
        raise RuntimeError("dividend result statuses overlap")
    if (
        len(collection.response_status_manifest_hash) != 64
        or any(
            character not in "0123456789abcdef"
            for character in collection.response_status_manifest_hash
        )
    ):
        raise RuntimeError("dividend per-code response status manifest is missing")
    minimum = float(min_nonempty_code_ratio)
    if not math.isfinite(minimum) or not 0 < minimum <= 1:
        raise RuntimeError("dividend nonempty evidence threshold is invalid")
    ratio = len(collection.nonempty_codes) / len(collection.requested_codes)
    if not collection.rows or ratio < minimum:
        raise RuntimeError(
            "dividend nonempty evidence is unreasonable: "
            f"nonempty={len(collection.nonempty_codes)}/"
            f"{len(collection.requested_codes)} ({ratio:.2%}), "
            f"minimum={minimum:.2%}, rows={len(collection.rows)}"
        )
    canonical = canonical_dividend_rows(collection.rows)
    return {
        "requested_code_count": len(collection.requested_codes),
        "requested_code_set_hash": code_set_hash(collection.requested_codes),
        "responded_code_count": len(collection.responded_codes),
        "responded_code_set_hash": code_set_hash(collection.responded_codes),
        "nonempty_code_count": len(collection.nonempty_codes),
        "nonempty_code_set_hash": code_set_hash(collection.nonempty_codes),
        "authoritative_empty_code_count": len(collection.empty_codes),
        "authoritative_empty_code_set_hash": code_set_hash(collection.empty_codes),
        "failure_count": 0,
        "response_status_manifest_hash": (
            collection.response_status_manifest_hash
        ),
        "nonempty_code_ratio": ratio,
        "row_count": len(canonical),
        "row_hash": _digest(canonical),
    }


def canonical_dividend_rows(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    canonical: list[dict[str, Any]] = []
    identities: set[tuple[str, str]] = set()
    for row in rows:
        code = _code(row.get("stock_code"))
        report_date = date.fromisoformat(str(row.get("report_date"))[:10]).isoformat()
        identity = (code, report_date)
        if identity in identities:
            raise RuntimeError(f"duplicate dividend business identity: {identity}")
        identities.add(identity)
        plan = str(row.get("dividend_plan") or "").strip()
        if not plan:
            raise RuntimeError(f"empty dividend plan for {identity}")
        raw_ex_date = row.get("ex_dividend_date")
        ex_date = (
            None
            if raw_ex_date in (None, "", "NaT")
            else date.fromisoformat(str(raw_ex_date)[:10]).isoformat()
        )
        canonical.append(
            {
                "stock_code": code,
                "report_date": report_date,
                "dividend_plan": plan,
                "ex_dividend_date": ex_date,
            }
        )
    return sorted(
        canonical,
        key=lambda row: (row["stock_code"], row["report_date"]),
    )


_INSERT_SQL = text(
    """
    INSERT INTO sm_dividend
      (stock_code,report_date,dividend_plan,ex_dividend_date,etl_sync_at)
    VALUES
      (:stock_code,:report_date,:dividend_plan,:ex_dividend_date,:etl_sync_at)
    """
)


def _scope_statement(prefix: str, codes: list[str]) -> tuple[str, dict[str, str]]:
    params = {f"{prefix}_{index}": code for index, code in enumerate(codes)}
    placeholders = ",".join(f":{name}" for name in params)
    return placeholders, params


def _read_scope(connection: Any, codes: tuple[str, ...]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for offset in range(0, len(codes), 500):
        chunk = list(codes[offset : offset + 500])
        placeholders, params = _scope_statement("read_code", chunk)
        result = connection.execute(
            text(
                "SELECT stock_code,report_date,dividend_plan,ex_dividend_date "
                f"FROM sm_dividend WHERE stock_code IN ({placeholders}) "
                "ORDER BY stock_code,report_date"
            ),
            params,
        ).mappings().all()
        rows.extend(dict(row) for row in result)
    return rows


def replace_snapshot(
    engine: Any,
    *,
    collection: DividendCollection,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    """Replace only the proven full current universe in one transaction."""

    expected_rows = canonical_dividend_rows(collection.rows)
    if int(evidence.get("row_count") or 0) != len(expected_rows):
        raise RuntimeError("dividend collection evidence row count differs")
    write_rows = [
        {
            **row,
            "etl_sync_at": collection.rows[0]["etl_sync_at"],
        }
        for row in expected_rows
    ]
    if not write_rows:
        raise RuntimeError("dividend replacement may not publish an empty snapshot")
    with engine.begin() as connection:
        for offset in range(0, len(collection.requested_codes), 500):
            chunk = list(collection.requested_codes[offset : offset + 500])
            placeholders, params = _scope_statement("delete_code", chunk)
            connection.execute(
                text(
                    "DELETE FROM sm_dividend "
                    f"WHERE stock_code IN ({placeholders})"
                ),
                params,
            )
        for offset in range(0, len(write_rows), 1000):
            connection.execute(_INSERT_SQL, write_rows[offset : offset + 1000])
        transaction_rows = canonical_dividend_rows(
            _read_scope(connection, collection.requested_codes)
        )
        if transaction_rows != expected_rows:
            raise RuntimeError("dividend transaction readback differs before commit")
    with engine.connect() as connection:
        committed_rows = canonical_dividend_rows(
            _read_scope(connection, collection.requested_codes)
        )
    if committed_rows != expected_rows:
        raise RuntimeError("dividend committed readback differs")
    return {
        "row_count": len(committed_rows),
        "row_hash": _digest(committed_rows),
        "scope_code_count": len(collection.requested_codes),
        "scope_code_set_hash": code_set_hash(collection.requested_codes),
    }


def run_sync(
    engine: Any,
    *,
    now: datetime | None = None,
    workers: int = 4,
    sleep_seconds: float = 0.1,
    min_nonempty_code_ratio: float = 0.2,
    provider: AdataBaiduDividendProvider | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    current = current.replace(microsecond=0)
    as_of = current.date().isoformat()
    schema = validate_runtime_schema(engine)
    universe = load_authoritative_universe(
        engine,
        as_of=as_of,
        known_at=current,
    )
    collection = collect_snapshot(
        universe.codes,
        provider=provider or AdataBaiduDividendProvider(),
        as_of=as_of,
        observed_at=current,
        workers=workers,
        sleep_seconds=sleep_seconds,
    )
    collection_evidence = validate_collection(
        collection,
        min_nonempty_code_ratio=min_nonempty_code_ratio,
    )
    database = replace_snapshot(
        engine,
        collection=collection,
        evidence=collection_evidence,
    )
    if (
        database["row_count"] != collection_evidence["row_count"]
        or database["row_hash"] != collection_evidence["row_hash"]
    ):
        raise RuntimeError("dividend database receipt differs from source snapshot")
    return _receipt(
        {
            "schema": RECEIPT_SCHEMA,
            "status": "PASS",
            "sync_date": as_of,
            "provider": PROVIDER_ID,
            "executor_owner": "linux_provider",
            "catalog": {
                "batch_id": universe.catalog_batch_id,
                "manifest_hash": universe.catalog_manifest_hash,
                "member_set_hash": universe.catalog_member_set_hash,
                "captured_at": universe.catalog_captured_at,
                "target_code_set_hash": universe.code_set_hash,
            },
            "collection": collection_evidence,
            "database": database,
            "schema_hash": schema["schema_hash"],
            "source_identity": adata_release_identity(),
        }
    )


def _failure_receipt(*, sync_date: str, error: BaseException) -> dict[str, Any]:
    return _receipt(
        {
            "schema": RECEIPT_SCHEMA,
            "status": "DATA_BLOCKED",
            "sync_date": sync_date,
            "provider": PROVIDER_ID,
            "executor_owner": "linux_provider",
            "error_type": type(error).__name__,
            "error": str(error)[:1000],
        }
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--sleep", type=float, default=0.1)
    parser.add_argument("--min-nonempty-code-ratio", type=float, default=0.2)
    args = parser.parse_args(argv)
    now = datetime.now(SHANGHAI).replace(microsecond=0)
    if not args.execute:
        result = _receipt(
            {
                "schema": RECEIPT_SCHEMA,
                "status": "DRY_RUN",
                "sync_date": now.date().isoformat(),
                "provider": PROVIDER_ID,
                "executor_owner": "linux_provider",
                "minimum_nonempty_code_ratio": args.min_nonempty_code_ratio,
            }
        )
        print(_canonical_json(result), flush=True)
        return 0
    try:
        if args.workers != 4 or args.sleep != 0.1:
            raise RuntimeError(
                "production dividend concurrency contract is fixed at workers=4,sleep=0.1"
            )
        if args.min_nonempty_code_ratio != 0.2:
            raise RuntimeError(
                "production dividend nonempty evidence threshold is fixed at 0.2"
            )
        load_project_env()
        # Resolve/verify the sealed adata tree before any request or DML.
        ensure_adata_import_path(ROOT)
        source_identity = adata_release_identity()
        if production_mode() and source_identity["git_sha"] == "development_unsealed":
            raise RuntimeError("production dividend source identity is unavailable")
        engine = create_tool_engine()
        try:
            result = run_sync(
                engine,
                now=now,
                workers=args.workers,
                sleep_seconds=args.sleep,
                min_nonempty_code_ratio=args.min_nonempty_code_ratio,
            )
        finally:
            engine.dispose()
    except Exception as exc:  # exactly one machine-readable outcome
        result = _failure_receipt(sync_date=now.date().isoformat(), error=exc)
        print(_canonical_json(result), flush=True)
        return 1
    print(_canonical_json(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
