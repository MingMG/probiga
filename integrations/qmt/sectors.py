from __future__ import annotations

import re
from collections.abc import Iterable

import pandas as pd

from integrations.qmt import bridge

INDUSTRY_PREFIXES: dict[str, str] = {
    "1000SW1": "申万一级",
    "1000SW2": "申万二级",
}

CONCEPT_PREFIXES: tuple[str, ...] = ("TDGN", "TGN", "GN")
CONCEPT_PRIORITY: dict[str, int] = {prefix: idx for idx, prefix in enumerate(CONCEPT_PREFIXES)}


def _dedupe_strings(items: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _dedupe_qmt_codes(items: Iterable[str]) -> list[str]:
    return _dedupe_strings(str(item or "").strip().upper() for item in items)


def _strip_prefix(value: str, prefixes: Iterable[str]) -> str:
    text = str(value or "").strip()
    for prefix in prefixes:
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return text


def _normalize_concept_name(value: str) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[()（）\[\]\s]+", "", text)
    if text.endswith("概念"):
        text = text[:-2]
    return text.lower()


def _concept_priority(name: str) -> int:
    for prefix, priority in CONCEPT_PRIORITY.items():
        if str(name or "").startswith(prefix):
            return priority
    return len(CONCEPT_PRIORITY) + 9


def build_concept_catalog(sector_names: Iterable[str]) -> pd.DataFrame:
    rows: list[dict[str, str | int]] = []
    for sector_name in _dedupe_strings(sector_names):
        if not sector_name.startswith(CONCEPT_PREFIXES):
            continue
        display_name = _strip_prefix(sector_name, CONCEPT_PREFIXES)
        if not display_name:
            continue
        rows.append(
            {
                "sector_name": sector_name,
                "concept_code": sector_name,
                "index_code": sector_name,
                "name": display_name[:256],
                "name_key": _normalize_concept_name(display_name),
                "priority": _concept_priority(sector_name),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["concept_code", "index_code", "name", "source", "sector_name"])

    out = pd.DataFrame(rows).sort_values(["name_key", "priority", "sector_name"]).drop_duplicates(
        subset=["name_key"],
        keep="first",
    )
    out["source"] = "qmt"
    return out[["concept_code", "index_code", "name", "source", "sector_name"]].reset_index(drop=True)


def build_industry_catalog(sector_names: Iterable[str]) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for sector_name in _dedupe_strings(sector_names):
        for prefix, industry_type in INDUSTRY_PREFIXES.items():
            if not sector_name.startswith(prefix):
                continue
            industry_name = _strip_prefix(sector_name, (prefix,))
            if not industry_name:
                continue
            rows.append(
                {
                    "sector_name": sector_name,
                    "sw_code": sector_name,
                    "industry_name": industry_name[:256],
                    "industry_type": industry_type,
                    "source": "qmt",
                }
            )
            break
    return pd.DataFrame(rows, columns=["sector_name", "sw_code", "industry_name", "industry_type", "source"])


def _fetch_memberships(sector_names: Iterable[str]) -> pd.DataFrame:
    names = _dedupe_strings(sector_names)
    if not names:
        return pd.DataFrame(columns=["sector_name", "stock_code", "qmt_code"])
    df = bridge.sector_members_many(names, timeout=900)
    if df is None or df.empty:
        return pd.DataFrame(columns=["sector_name", "stock_code", "qmt_code"])
    out = df[["sector_name", "stock_code", "qmt_code"]].copy()
    out["stock_code"] = out["stock_code"].astype(str).str.zfill(6)
    out["qmt_code"] = out["qmt_code"].astype(str).str.upper()
    return out.drop_duplicates(subset=["sector_name", "stock_code"], keep="first").reset_index(drop=True)


def _instrument_name_map(qmt_codes: Iterable[str]) -> dict[str, str]:
    codes = _dedupe_qmt_codes(qmt_codes)
    if not codes:
        return {}
    details = bridge.instrument_details(codes, batch_size=400, timeout=300)
    if details is None or details.empty:
        return {}
    return (
        details.assign(stock_code=details["stock_code"].astype(str).str.zfill(6))
        .drop_duplicates(subset=["stock_code"], keep="first")
        .set_index("stock_code")["short_name"]
        .fillna("")
        .astype(str)
        .str.strip()
        .to_dict()
    )


def fetch_sector_datasets() -> dict[str, pd.DataFrame]:
    sector_df = bridge.sector_list(timeout=180)
    if sector_df is None or sector_df.empty:
        empty = pd.DataFrame()
        return {
            "concept_catalog": empty,
            "concept_constituents": empty,
            "stock_concepts": empty,
            "stock_plates": empty,
            "industry_sw": empty,
        }

    sector_names = sector_df["sector_name"].astype(str).tolist()
    concept_catalog = build_concept_catalog(sector_names)
    industry_catalog = build_industry_catalog(sector_names)
    selected_sector_names = _dedupe_strings(
        list(concept_catalog.get("sector_name", pd.Series(dtype=str)).astype(str).tolist())
        + list(industry_catalog.get("sector_name", pd.Series(dtype=str)).astype(str).tolist())
    )
    membership = _fetch_memberships(selected_sector_names)
    name_map = _instrument_name_map(membership.get("qmt_code", pd.Series(dtype=str)).astype(str).tolist())

    if membership.empty:
        concept_constituents = pd.DataFrame(columns=["concept_code", "stock_code", "short_name"])
        stock_concepts = pd.DataFrame(columns=["stock_code", "concept_code", "name", "source", "reason"])
        stock_plates = pd.DataFrame(columns=["stock_code", "plate_code", "plate_name", "plate_type", "source"])
        industry_sw = pd.DataFrame(columns=["stock_code", "sw_code", "industry_name", "industry_type", "source"])
        return {
            "concept_catalog": concept_catalog.drop(columns=["sector_name"], errors="ignore"),
            "concept_constituents": concept_constituents,
            "stock_concepts": stock_concepts,
            "stock_plates": stock_plates,
            "industry_sw": industry_sw,
        }

    concept_members = membership.merge(
        concept_catalog[["sector_name", "concept_code", "name"]],
        on="sector_name",
        how="inner",
    )
    concept_members["short_name"] = concept_members["stock_code"].map(name_map).fillna("")
    concept_constituents = (
        concept_members[["concept_code", "stock_code", "short_name"]]
        .drop_duplicates(subset=["concept_code", "stock_code"], keep="first")
        .reset_index(drop=True)
    )
    stock_concepts = concept_members[["stock_code", "concept_code", "name"]].copy()
    stock_concepts["source"] = "qmt"
    stock_concepts["reason"] = ""
    stock_concepts = stock_concepts.drop_duplicates(subset=["stock_code", "concept_code"], keep="first").reset_index(drop=True)

    industry_members = membership.merge(
        industry_catalog[["sector_name", "sw_code", "industry_name", "industry_type"]],
        on="sector_name",
        how="inner",
    )
    industry_sw = industry_members[["stock_code", "sw_code", "industry_name", "industry_type"]].copy()
    industry_sw["source"] = "qmt"
    industry_sw = industry_sw.drop_duplicates(subset=["stock_code", "sw_code"], keep="first").reset_index(drop=True)

    industry_plates = industry_sw.rename(
        columns={
            "sw_code": "plate_code",
            "industry_name": "plate_name",
        }
    )[["stock_code", "plate_code", "plate_name"]].copy()
    industry_plates["plate_type"] = "行业"
    industry_plates["source"] = "qmt"

    concept_plates = stock_concepts.rename(
        columns={
            "concept_code": "plate_code",
            "name": "plate_name",
        }
    )[["stock_code", "plate_code", "plate_name"]].copy()
    concept_plates["plate_type"] = "概念"
    concept_plates["source"] = "qmt"

    stock_plates = pd.concat([industry_plates, concept_plates], ignore_index=True).drop_duplicates(
        subset=["stock_code", "plate_code"],
        keep="first",
    )

    return {
        "concept_catalog": concept_catalog.drop(columns=["sector_name"], errors="ignore").reset_index(drop=True),
        "concept_constituents": concept_constituents,
        "stock_concepts": stock_concepts,
        "stock_plates": stock_plates.reset_index(drop=True),
        "industry_sw": industry_sw,
    }
