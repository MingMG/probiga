from __future__ import annotations

"""Transform standard-QMT reference data into the existing ProBigA tables."""

import hashlib
import gzip
import json
import os
import re
import time
from collections.abc import Iterable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from integrations.bigqmt import bridge
from integrations.bigqmt.spool import bridge_dir
from integrations.qmt.backend import to_qmt_symbol
from integrations.qmt.info import CORE_INDEXES, to_qmt_index_symbol


PROVIDER_ID = "gj_big_qmt_inner"
STOCK_SECTORS = ("沪深京A股", "上证A股", "深证A股", "京市A股")
INDEX_SECTORS = ("沪深指数", "沪市指数", "深市指数", "中证指数", "主要指数")
CONCEPT_PREFIXES = ("TDGN", "TGN", "GN")
INDUSTRY_PREFIXES = {"1000SW1": "申万一级", "1000SW2": "申万二级"}
SECTOR_DATASET_NAMES = (
    "concept_catalog",
    "concept_constituents",
    "stock_concepts",
    "stock_plates",
    "industry_sw",
)
CACHE_SCHEMA_VERSION = 1


def _sector_cache_path():
    configured = str(os.environ.get("BIG_QMT_REFERENCE_CACHE_PATH") or "").strip()
    return (
        os.path.abspath(os.path.expandvars(configured))
        if configured
        else str(bridge_dir() / "sector_reference_cache.json.gz")
    )


def _read_sector_cache() -> dict[str, pd.DataFrame] | None:
    ttl = max(0, int(os.environ.get("BIG_QMT_REFERENCE_CACHE_SECONDS", "3600")))
    if ttl <= 0:
        return None
    path = _sector_cache_path()
    try:
        if time.time() - os.path.getmtime(path) > ttl:
            return None
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        if int(payload.get("schema_version") or 0) != CACHE_SCHEMA_VERSION:
            return None
        datasets = payload.get("datasets") or {}
        result = {
            name: pd.DataFrame(datasets.get(name) or []) for name in SECTOR_DATASET_NAMES
        }
        if result["concept_catalog"].empty or result["industry_sw"].empty:
            return None
        return result
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None


def _write_sector_cache(datasets: dict[str, pd.DataFrame]) -> None:
    path = _sector_cache_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temporary = f"{path}.{os.getpid()}.tmp"
    payload = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "source": PROVIDER_ID,
        "generated_at": time.time(),
        "datasets": {
            name: datasets.get(name, pd.DataFrame()).to_dict("records")
            for name in SECTOR_DATASET_NAMES
        },
    }
    try:
        with gzip.open(temporary, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"), default=str)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            try:
                os.remove(temporary)
            except OSError:
                pass


def _dedupe(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value or "").strip()
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _fmt_date(value: object) -> str | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) < 8:
        return None
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def _source_code(prefix: str, sector_name: str, limit: int = 32) -> str:
    raw = str(sector_name or "").strip()
    if raw and len(raw) <= limit:
        return raw
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
    return f"{prefix}{digest}"[:limit]


def _stock_symbols(values: Iterable[str]) -> list[str]:
    return [
        symbol
        for symbol in _dedupe(str(value or "").strip().upper() for value in values)
        if to_qmt_symbol(symbol.split(".", 1)[0]) == symbol
    ]


def fetch_all_stock_codes() -> pd.DataFrame:
    symbols: list[str] = []
    for sector_name in STOCK_SECTORS:
        frame = bridge.sector_members(sector_name, timeout=240)
        if frame is not None and not frame.empty:
            symbols.extend(frame["qmt_code"].astype(str).tolist())
    valid = _stock_symbols(symbols)
    if not valid:
        return pd.DataFrame(columns=["stock_code", "short_name", "exchange", "list_date"])
    details = bridge.instrument_details(valid, batch_size=400, timeout=600)
    if details is None or details.empty:
        return pd.DataFrame(columns=["stock_code", "short_name", "exchange", "list_date"])
    out = pd.DataFrame()
    out["stock_code"] = details["stock_code"].astype(str).str.zfill(6)
    out["short_name"] = details.get("short_name", "").fillna("").astype(str).str.strip().str[:128]
    out["exchange"] = details.get("exchange", "").fillna("").astype(str).str.upper().str[:16]
    out["list_date"] = details.get("list_date", "").map(_fmt_date)
    out["qmt_code"] = details["qmt_code"].fillna("").astype(str).str.upper()
    out["data_source"] = PROVIDER_ID
    received_at = pd.Timestamp.now().to_pydatetime()
    out["source_time"] = None
    out["received_at"] = received_at
    out["batch_id"] = f"bigqmt_stock_list_{received_at.strftime('%Y%m%d%H%M%S%f')}"
    out["data_version"] = "bigqmt_inner_v2"
    out["quality_status"] = "VERIFIED"
    out["permission_status"] = "SUPPORTED"
    out = out[out["stock_code"].map(to_qmt_symbol).notna()]
    return out.drop_duplicates(subset=["stock_code"], keep="last").reset_index(drop=True)


def _seed_index_symbols(engine: Engine | None) -> list[str]:
    result = list(CORE_INDEXES)
    if engine is None:
        return _dedupe(result)
    with engine.connect() as conn:
        for sql in (
            "SELECT index_code FROM si_all_index_code",
            "SELECT DISTINCT index_code FROM si_index_constituent",
        ):
            try:
                rows = conn.execute(text(sql)).fetchall()
            except Exception:
                continue
            for row in rows:
                symbol = to_qmt_index_symbol(str(row[0] or ""))
                if symbol:
                    result.append(symbol)
    return _dedupe(result)


def fetch_all_index_codes(*, engine: Engine | None = None) -> pd.DataFrame:
    symbols = _seed_index_symbols(engine)
    for sector_name in INDEX_SECTORS:
        try:
            members = bridge.sector_members(sector_name, timeout=240)
        except Exception:
            continue
        if members is not None and not members.empty:
            symbols.extend(members["qmt_code"].astype(str).tolist())
    symbols = _dedupe(symbols)
    details = bridge.instrument_details(symbols, batch_size=300, timeout=600)
    if details is None or details.empty:
        return pd.DataFrame(columns=["index_code", "concept_code", "name", "source"])
    out = pd.DataFrame()
    out["index_code"] = details["stock_code"].astype(str).str.zfill(6)
    out["concept_code"] = ""
    out["name"] = details.get("short_name", "").fillna("").astype(str).str.strip().str[:256]
    out["source"] = PROVIDER_ID
    # Every symbol here came from an index-sector membership or the existing
    # canonical index table.  Prefix-only stock mapping cannot distinguish
    # 000001.SH (an index) from 000001.SZ (a stock), so do not apply it here.
    out = out[out["name"] != ""]
    return out.drop_duplicates(subset=["index_code"], keep="last").reset_index(drop=True)


def fetch_index_constituents(index_codes: Iterable[str]) -> pd.DataFrame:
    symbols = _dedupe(
        symbol for symbol in (to_qmt_index_symbol(code) for code in index_codes) if symbol
    )
    parts: list[pd.DataFrame] = []
    batch_size = max(5, int(os.environ.get("BIG_QMT_INDEX_MEMBER_BATCH_SIZE", "20")))
    for offset in range(0, len(symbols), batch_size):
        frame = bridge.index_weight_many(symbols[offset : offset + batch_size], timeout=600)
        if frame is not None and not frame.empty:
            parts.append(frame)
    if not parts:
        return pd.DataFrame(columns=["index_code", "stock_code", "short_name"])
    members = pd.concat(parts, ignore_index=True)
    members["index_code"] = members["index_code"].astype(str).str.zfill(6)
    members["stock_code"] = members["stock_code"].astype(str).str.zfill(6)
    members["qmt_code"] = members["qmt_code"].astype(str).str.upper()
    members = members[members["qmt_code"].eq(members["stock_code"].map(to_qmt_symbol))]
    members = members.drop_duplicates(subset=["index_code", "stock_code"], keep="last")
    details = bridge.instrument_details(
        members["qmt_code"].drop_duplicates().tolist(), batch_size=400, timeout=600
    )
    name_map: dict[str, str] = {}
    if details is not None and not details.empty:
        name_map = (
            details.assign(stock_code=details["stock_code"].astype(str).str.zfill(6))
            .drop_duplicates(subset=["stock_code"], keep="last")
            .set_index("stock_code")["short_name"]
            .fillna("")
            .astype(str)
            .to_dict()
        )
    members["short_name"] = members["stock_code"].map(name_map).fillna("")
    return members[["index_code", "stock_code", "short_name"]].reset_index(drop=True)


def _strip_prefix(name: str) -> str:
    for prefix in CONCEPT_PREFIXES + tuple(INDUSTRY_PREFIXES) + ("SW1", "SW2"):
        if name.startswith(prefix):
            return name[len(prefix) :].strip()
    return name.strip()


def _industry_type(name: str, path: str) -> str:
    for prefix, value in INDUSTRY_PREFIXES.items():
        if name.startswith(prefix):
            return value
    text_value = f"{path}/{name}".replace(" ", "")
    if "港股" in text_value:
        return ""
    if re.search(r"申万(一级|1级)", text_value, re.I):
        return "申万一级"
    if re.search(r"申万(二级|2级)", text_value, re.I):
        return "申万二级"
    return ""


def _is_concept(name: str, path: str) -> bool:
    if name.startswith(CONCEPT_PREFIXES):
        return True
    text_value = f"{path}/{name}".replace(" ", "")
    return "概念" in text_value and "行业" not in text_value


def fetch_sector_datasets(*, force_refresh: bool = False) -> dict[str, pd.DataFrame]:
    if not force_refresh:
        cached = _read_sector_cache()
        if cached is not None:
            return cached
    sector_frame = bridge.sector_list(timeout=300)
    empty = pd.DataFrame()
    if sector_frame is None or sector_frame.empty:
        return {
            "concept_catalog": empty,
            "concept_constituents": empty,
            "stock_concepts": empty,
            "stock_plates": empty,
            "industry_sw": empty,
        }

    concepts: list[dict[str, str]] = []
    industries: list[dict[str, str]] = []
    for row in sector_frame.to_dict("records"):
        sector_name = str(row.get("sector_name") or "").strip()
        parent_path = str(row.get("parent_path") or row.get("parent_name") or "").strip()
        if not sector_name:
            continue
        display_name = _strip_prefix(sector_name) or sector_name
        industry_type = _industry_type(sector_name, parent_path)
        if industry_type:
            industries.append({
                "sector_name": sector_name,
                "sw_code": _source_code("QSW", sector_name),
                "industry_name": display_name[:256],
                "industry_type": industry_type,
                "source": PROVIDER_ID,
            })
        elif _is_concept(sector_name, parent_path):
            code = _source_code("QGN", sector_name)
            concepts.append({
                "sector_name": sector_name,
                "concept_code": code,
                "index_code": code,
                "name": display_name[:256],
                "source": PROVIDER_ID,
            })

    concept_catalog = pd.DataFrame(
        concepts, columns=["sector_name", "concept_code", "index_code", "name", "source"]
    ).drop_duplicates(subset=["concept_code"], keep="last")
    industry_catalog = pd.DataFrame(
        industries, columns=["sector_name", "sw_code", "industry_name", "industry_type", "source"]
    ).drop_duplicates(subset=["sw_code"], keep="last")
    selected = _dedupe(
        concept_catalog.get("sector_name", pd.Series(dtype=str)).tolist()
        + industry_catalog.get("sector_name", pd.Series(dtype=str)).tolist()
    )
    member_parts: list[pd.DataFrame] = []
    batch_size = max(5, int(os.environ.get("BIG_QMT_SECTOR_MEMBER_BATCH_SIZE", "30")))
    for offset in range(0, len(selected), batch_size):
        frame = bridge.sector_members_many(selected[offset : offset + batch_size], timeout=600)
        if frame is not None and not frame.empty:
            member_parts.append(frame)
    membership = pd.concat(member_parts, ignore_index=True) if member_parts else pd.DataFrame()
    if not membership.empty:
        membership["stock_code"] = membership["stock_code"].astype(str).str.zfill(6)
        membership["qmt_code"] = membership["qmt_code"].astype(str).str.upper()
        membership = membership[membership["qmt_code"].eq(membership["stock_code"].map(to_qmt_symbol))]
        membership = membership.drop_duplicates(subset=["sector_name", "stock_code"], keep="last")

    name_map: dict[str, str] = {}
    if not membership.empty:
        details = bridge.instrument_details(
            membership["qmt_code"].drop_duplicates().tolist(), batch_size=400, timeout=600
        )
        if details is not None and not details.empty:
            name_map = (
                details.assign(stock_code=details["stock_code"].astype(str).str.zfill(6))
                .drop_duplicates(subset=["stock_code"], keep="last")
                .set_index("stock_code")["short_name"]
                .fillna("")
                .astype(str)
                .to_dict()
            )

    concept_members = membership.merge(
        concept_catalog[["sector_name", "concept_code", "name"]], on="sector_name", how="inner"
    ) if not membership.empty else pd.DataFrame()
    if concept_members.empty:
        concept_constituents = pd.DataFrame(columns=["concept_code", "stock_code", "short_name"])
        stock_concepts = pd.DataFrame(columns=["stock_code", "concept_code", "name", "source", "reason"])
    else:
        concept_members["short_name"] = concept_members["stock_code"].map(name_map).fillna("")
        concept_constituents = concept_members[["concept_code", "stock_code", "short_name"]].drop_duplicates()
        stock_concepts = concept_members[["stock_code", "concept_code", "name"]].drop_duplicates()
        stock_concepts["source"] = PROVIDER_ID
        stock_concepts["reason"] = ""

    industry_members = membership.merge(
        industry_catalog[["sector_name", "sw_code", "industry_name", "industry_type"]],
        on="sector_name", how="inner"
    ) if not membership.empty else pd.DataFrame()
    if industry_members.empty:
        industry_sw = pd.DataFrame(columns=["stock_code", "sw_code", "industry_name", "industry_type", "source"])
    else:
        industry_sw = industry_members[["stock_code", "sw_code", "industry_name", "industry_type"]].drop_duplicates()
        industry_sw["source"] = PROVIDER_ID

    industry_plates = industry_sw.rename(
        columns={"sw_code": "plate_code", "industry_name": "plate_name"}
    )[["stock_code", "plate_code", "plate_name"]].copy()
    industry_plates["plate_type"] = "行业"
    industry_plates["source"] = PROVIDER_ID
    concept_plates = stock_concepts.rename(
        columns={"concept_code": "plate_code", "name": "plate_name"}
    )[["stock_code", "plate_code", "plate_name"]].copy()
    concept_plates["plate_type"] = "概念"
    concept_plates["source"] = PROVIDER_ID
    stock_plates = pd.concat([industry_plates, concept_plates], ignore_index=True).drop_duplicates(
        subset=["stock_code", "plate_code"], keep="last"
    )

    result = {
        "concept_catalog": concept_catalog.drop(columns=["sector_name"], errors="ignore").reset_index(drop=True),
        "concept_constituents": concept_constituents.reset_index(drop=True),
        "stock_concepts": stock_concepts.reset_index(drop=True),
        "stock_plates": stock_plates.reset_index(drop=True),
        "industry_sw": industry_sw.reset_index(drop=True),
    }
    if not result["concept_catalog"].empty and not result["industry_sw"].empty:
        _write_sector_cache(result)
    return result
