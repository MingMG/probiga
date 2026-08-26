#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Fail-closed multi-source news synchronization for the formal scheduler.

This entry point deliberately does not deliver WeCom messages.  A successful
delivery is not evidence that news rows were collected and persisted; delivery
remains a separate task in ``biz/news/sync_news.py``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine


RESULT_SCHEMA = "probiga.news-sync-result.v1"


def _fetch_cls(client: Any, pages: int) -> list[dict[str, Any]]:
    from biz.news.sync_news import fetch_cls

    return fetch_cls(client, pages)


def _fetch_eastmoney(client: Any, pages: int) -> list[dict[str, Any]]:
    from biz.news.sync_news import fetch_eastmoney

    return fetch_eastmoney(client, pages)


def _fetch_sina(client: Any, pages: int) -> list[dict[str, Any]]:
    from biz.news.sync_news import fetch_sina

    return fetch_sina(client, pages)


SOURCE_FETCHERS: dict[str, Callable[[Any, int], list[dict[str, Any]]]] = {
    "cls": _fetch_cls,
    "eastmoney": _fetch_eastmoney,
    "sina": _fetch_sina,
}


class NewsSyncContractError(RuntimeError):
    """Fail the formal run while retaining per-source diagnostic evidence."""

    def __init__(
        self,
        message: str,
        *,
        source_results: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        super().__init__(message)
        self.source_results = {
            str(source): dict(result)
            for source, result in (source_results or {}).items()
        }


def _hash_payload(value: object) -> str:
    serialized = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _with_receipt_id(payload: Mapping[str, Any]) -> dict[str, Any]:
    receipt = dict(payload)
    receipt["receipt_id"] = _hash_payload(receipt)
    return receipt


def _canonical_datetime(value: object, *, field: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    else:
        raw = str(value or "").strip()
        if not raw:
            raise NewsSyncContractError(f"news {field} is empty")
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise NewsSyncContractError(
                f"news {field} is not an ISO datetime: {raw!r}"
            ) from exc
    return parsed.replace(microsecond=0).isoformat(timespec="seconds")


def _canonical_json_list(value: object, *, field: str) -> list[Any]:
    if value in (None, ""):
        return []
    parsed = value
    if isinstance(value, (str, bytes, bytearray)):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise NewsSyncContractError(f"news {field} is not valid JSON") from exc
    if not isinstance(parsed, list):
        raise NewsSyncContractError(f"news {field} must be a list")
    # JSON round-tripping rejects unserializable provider objects and gives the
    # hash the same representation that the database stores.
    try:
        return json.loads(json.dumps(parsed, ensure_ascii=False, sort_keys=True))
    except (TypeError, ValueError) as exc:
        raise NewsSyncContractError(f"news {field} is not JSON serializable") from exc


def _canonical_int(value: object, *, field: str) -> int:
    if value in (None, ""):
        return 0
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise NewsSyncContractError(f"news {field} is not numeric: {value!r}") from exc
    if not math.isfinite(number) or number < 0 or not number.is_integer():
        raise NewsSyncContractError(f"news {field} is invalid: {value!r}")
    return int(number)


def canonical_news_item(item: Mapping[str, Any]) -> dict[str, Any]:
    source = str(item.get("source") or "").strip()
    source_id = str(item.get("source_id") or "").strip()
    if not source or source not in SOURCE_FETCHERS:
        raise NewsSyncContractError(f"news source is unsupported: {source!r}")
    if not source_id:
        raise NewsSyncContractError(f"news source_id is empty for source={source}")
    publish_time = _canonical_datetime(item.get("publish_time"), field="publish_time")
    return {
        "source": source,
        "source_id": source_id,
        "title": str(item.get("title") or "")[:512],
        "content": str(item.get("content") or ""),
        "publish_time": publish_time,
        "level": str(item.get("level") or "C").strip() or "C",
        "stocks": _canonical_json_list(item.get("stocks"), field="stocks"),
        "subjects": _canonical_json_list(item.get("subjects"), field="subjects"),
        "reading_num": _canonical_int(item.get("reading_num"), field="reading_num"),
        "is_top": bool(item.get("is_top")),
        "jpush": bool(item.get("jpush")),
    }


def canonical_news_items(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Normalize and deterministically de-duplicate one fetched batch."""

    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        normalized = canonical_news_item(item)
        key = (normalized["source"], normalized["source_id"])
        existing = by_key.get(key)
        if existing is not None and existing != normalized:
            raise NewsSyncContractError(
                "provider returned conflicting duplicate news identity: "
                f"source={key[0]} source_id={key[1]}"
            )
        by_key[key] = normalized
    return [by_key[key] for key in sorted(by_key)]


def news_row_hash(items: Sequence[Mapping[str, Any]]) -> str:
    return _hash_payload(canonical_news_items(items))


def _source_pages(source: str, pages: int) -> int:
    return pages if source == "cls" else max(1, pages // 2)


def collect_sources(
    client: Any,
    *,
    pages: int,
    fetchers: Mapping[str, Callable[[Any, int], list[dict[str, Any]]]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    if pages <= 0:
        raise NewsSyncContractError("news pages must be positive")
    selected = dict(fetchers or SOURCE_FETCHERS)
    if set(selected) != set(SOURCE_FETCHERS):
        raise NewsSyncContractError(
            "formal news sources must be exactly: " + ",".join(sorted(SOURCE_FETCHERS))
        )

    all_items: list[dict[str, Any]] = []
    results: dict[str, dict[str, Any]] = {}
    for source in sorted(selected):
        requested_pages = _source_pages(source, pages)
        try:
            items = selected[source](client, requested_pages)
            if not isinstance(items, list):
                raise TypeError(f"fetcher returned {type(items).__name__}, expected list")
            normalized_items = canonical_news_items(items)
            wrong_sources = sorted(
                {item["source"] for item in normalized_items if item["source"] != source}
            )
            if wrong_sources:
                raise NewsSyncContractError(
                    f"source={source} returned rows owned by {wrong_sources}"
                )
            results[source] = {
                "status": "SUCCESS",
                "outcome": "NONEMPTY" if normalized_items else "EMPTY",
                "requested_pages": requested_pages,
                "fetched_count": len(normalized_items),
            }
            all_items.extend(normalized_items)
        except Exception as exc:
            results[source] = {
                "status": "FAILED",
                "requested_pages": requested_pages,
                "fetched_count": 0,
                "error_type": type(exc).__name__,
                "error": str(exc)[:500],
            }
    return all_items, results


def _db_params(item: Mapping[str, Any], *, etl_sync_at: datetime) -> dict[str, Any]:
    return {
        "source": item["source"],
        "source_id": item["source_id"],
        "title": item["title"],
        "content": item["content"],
        "publish_time": datetime.fromisoformat(str(item["publish_time"])),
        "level": item["level"],
        "stocks": json.dumps(item["stocks"], ensure_ascii=False, sort_keys=True),
        "subjects": json.dumps(item["subjects"], ensure_ascii=False, sort_keys=True),
        "reading_num": item["reading_num"],
        "is_top": 1 if item["is_top"] else 0,
        "jpush": 1 if item["jpush"] else 0,
        "extra": None,
        "etl_sync_at": etl_sync_at,
    }


def _readback_batch(connection: Any, items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    ids_by_source: dict[str, list[str]] = {}
    for item in items:
        ids_by_source.setdefault(str(item["source"]), []).append(str(item["source_id"]))

    for source, source_ids in sorted(ids_by_source.items()):
        params: dict[str, Any] = {"source": source}
        placeholders: list[str] = []
        for index, source_id in enumerate(sorted(set(source_ids))):
            name = f"source_id_{index}"
            placeholders.append(f":{name}")
            params[name] = source_id
        query = text(
            "SELECT source, source_id, title, content, publish_time, level, "
            "stocks, subjects, reading_num, is_top, jpush "
            "FROM st_news_flash WHERE source=:source AND source_id IN ("
            + ",".join(placeholders)
            + ")"
        )
        rows.extend(dict(row) for row in connection.execute(query, params).mappings())
    return rows


def persist_and_verify(
    engine: Any,
    items: Sequence[Mapping[str, Any]],
    *,
    etl_sync_at: datetime,
) -> dict[str, Any]:
    canonical = canonical_news_items(items)
    if not canonical:
        raise NewsSyncContractError("formal news batch is empty")

    upsert = text(
        "INSERT INTO st_news_flash "
        "(source, source_id, title, content, publish_time, level, stocks, subjects, "
        "reading_num, is_top, jpush, extra, pushed, etl_sync_at) "
        "VALUES (:source, :source_id, :title, :content, :publish_time, :level, :stocks, "
        ":subjects, :reading_num, :is_top, :jpush, :extra, 0, :etl_sync_at) "
        "ON DUPLICATE KEY UPDATE title=VALUES(title), content=VALUES(content), "
        "publish_time=VALUES(publish_time), level=VALUES(level), stocks=VALUES(stocks), "
        "subjects=VALUES(subjects), reading_num=VALUES(reading_num), "
        "is_top=VALUES(is_top), jpush=VALUES(jpush), extra=VALUES(extra), "
        "etl_sync_at=VALUES(etl_sync_at)"
    )
    params = [_db_params(item, etl_sync_at=etl_sync_at) for item in canonical]
    with engine.begin() as connection:
        connection.execute(upsert, params)
        persisted = canonical_news_items(_readback_batch(connection, canonical))
        if persisted != canonical:
            raise NewsSyncContractError(
                "news DB readback differs from the collected formal batch"
            )

    return {
        "persisted_count": len(canonical),
        "latest_publish_time": max(item["publish_time"] for item in canonical),
        "row_hash": _hash_payload(canonical),
    }


def _source_sets(source_results: Mapping[str, Mapping[str, Any]]) -> dict[str, list[str]]:
    return {
        "successful_sources": sorted(
            source for source, result in source_results.items()
            if result.get("status") == "SUCCESS"
        ),
        "nonempty_sources": sorted(
            source for source, result in source_results.items()
            if result.get("status") == "SUCCESS" and result.get("outcome") == "NONEMPTY"
        ),
        "empty_sources": sorted(
            source for source, result in source_results.items()
            if result.get("status") == "SUCCESS" and result.get("outcome") == "EMPTY"
        ),
        "failed_sources": sorted(
            source for source, result in source_results.items()
            if result.get("status") == "FAILED"
        ),
    }


def _receipt(
    *,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    source_results: Mapping[str, Mapping[str, Any]],
    pages: int,
    evidence: Mapping[str, Any] | None = None,
    error: BaseException | None = None,
) -> dict[str, Any]:
    source_sets = _source_sets(source_results)
    payload: dict[str, Any] = {
        "schema": RESULT_SCHEMA,
        "status": status,
        "attempted_sources": sorted(source_results),
        **source_sets,
        "source_results": {
            source: dict(source_results[source]) for source in sorted(source_results)
        },
        "requested_pages": pages,
        "batch_started_at": started_at.isoformat(timespec="seconds"),
        "batch_finished_at": finished_at.isoformat(timespec="seconds"),
        "delivery_attempted": False,
        "evidence": dict(evidence or {}),
    }
    if error is not None:
        payload["error_type"] = type(error).__name__
        payload["error"] = str(error)[:1000]
    return _with_receipt_id(payload)


def sync_news_formal(
    engine: Any,
    *,
    pages: int = 2,
    client: Any | None = None,
    fetchers: Mapping[str, Callable[[Any, int], list[dict[str, Any]]]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    started_at = (now or datetime.now()).replace(microsecond=0)
    owns_client = client is None
    source_results: dict[str, dict[str, Any]] = {}
    try:
        if owns_client:
            try:
                import httpx
            except ImportError as exc:  # pragma: no cover - deployment dependency guard
                raise NewsSyncContractError("httpx is required for formal news sync") from exc
            client = httpx.Client(
                headers={"User-Agent": "Mozilla/5.0 ProBigA-formal-news-sync"},
                timeout=15,
            )
        items, source_results = collect_sources(
            client,
            pages=pages,
            fetchers=fetchers,
        )
        if not _source_sets(source_results)["successful_sources"]:
            raise NewsSyncContractError(
                "all formal news sources failed",
                source_results=source_results,
            )
        canonical = canonical_news_items(items)
        if not canonical:
            raise NewsSyncContractError(
                "formal news result is empty",
                source_results=source_results,
            )
        try:
            evidence = persist_and_verify(
                engine,
                canonical,
                etl_sync_at=started_at,
            )
        except Exception as exc:
            raise NewsSyncContractError(
                f"formal news publication failed: {exc}",
                source_results=source_results,
            ) from exc
    finally:
        if owns_client and client is not None:
            client.close()

    return _receipt(
        status="PASS",
        started_at=started_at,
        finished_at=(now or datetime.now()).replace(microsecond=0),
        source_results=source_results,
        pages=pages,
        evidence=evidence,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="正式多源快讯同步（仅同步，不投递）")
    parser.add_argument("--pages", type=int, default=2, help="财联社页数；其他源取一半且至少一页")
    parser.add_argument("--json", action="store_true", help="兼容统一调度参数；始终输出唯一 JSON receipt")
    args = parser.parse_args(argv)

    started_at = datetime.now().replace(microsecond=0)
    try:
        receipt = sync_news_formal(
            create_batch_engine(pool_size=2, max_overflow=2),
            pages=args.pages,
            now=started_at,
        )
    except Exception as exc:
        source_results = getattr(exc, "source_results", {})
        receipt = _receipt(
            status="FAILED",
            started_at=started_at,
            finished_at=datetime.now().replace(microsecond=0),
            source_results=source_results,
            pages=args.pages,
            error=exc,
        )
        print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
        return 1

    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
