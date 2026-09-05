from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from integrations.qmt import bridge, from_qmt_symbol, to_qmt_symbol

DEFAULT_STOCK_SECTORS: dict[str, str] = {
    "SH": "上证A股",
    "SZ": "深证A股",
    "BJ": "京市A股",
}

CORE_INDEXES: dict[str, str] = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000016.SH": "上证50",
    "000300.SH": "沪深300",
    "000852.SH": "中证1000",
    "000905.SH": "中证500",
    "000906.SH": "中证800",
    "000688.SH": "科创50",
}


def to_qmt_index_symbol(code: str) -> str | None:
    text_value = str(code or "").strip().upper()
    if not text_value:
        return None
    # SZSE 395xxx records are turnover statistics, not price indexes.
    # https://www.szse.cn/www/marketServices/technicalservice/guide/P020230904549038313109.pdf
    if text_value.split(".", 1)[0].startswith("395"):
        return None
    if "." in text_value:
        return text_value
    digits = "".join(ch for ch in text_value if ch.isdigit())
    if len(digits) != 6:
        return None
    if digits.startswith(("399", "98")):
        return f"{digits}.SZ"
    return f"{digits}.SH"


def _fmt_date(value: str) -> str | None:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) != 8:
        return None
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def _dedupe_qmt_codes(symbols: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        text_value = str(symbol or "").strip().upper()
        if not text_value or text_value in seen:
            continue
        seen.add(text_value)
        out.append(text_value)
    return out


def fetch_all_stock_codes() -> pd.DataFrame:
    symbols: list[str] = []
    for sector_name in DEFAULT_STOCK_SECTORS.values():
        members = bridge.sector_members(sector_name, timeout=180)
        if members is not None and not members.empty:
            symbols.extend(members["qmt_code"].astype(str).tolist())

    qmt_codes = _dedupe_qmt_codes(symbols)
    if not qmt_codes:
        return pd.DataFrame(columns=["stock_code", "short_name", "exchange", "list_date", "etl_sync_at"])

    details = bridge.instrument_details(qmt_codes, batch_size=400, timeout=300)
    if details is None or details.empty:
        return pd.DataFrame(columns=["stock_code", "short_name", "exchange", "list_date", "etl_sync_at"])

    out = pd.DataFrame()
    out["stock_code"] = details["stock_code"].astype(str).str.zfill(6)
    out["short_name"] = details.get("short_name", "").fillna("").astype(str).str.strip().str[:128]
    out["exchange"] = details.get("exchange", "").fillna("").astype(str).str.upper().str[:16]
    out["list_date"] = details.get("list_date", "").map(_fmt_date)
    out = out.dropna(subset=["stock_code"]).drop_duplicates(subset=["stock_code"], keep="first")
    return out


def _seed_index_symbols(engine: Engine | None = None) -> list[str]:
    symbols = list(CORE_INDEXES)
    if engine is not None:
        queries = [
            "SELECT index_code FROM si_all_index_code",
            "SELECT DISTINCT index_code FROM si_index_constituent",
        ]
        with engine.connect() as conn:
            for sql in queries:
                try:
                    rows = conn.execute(text(sql)).fetchall()
                except Exception:
                    continue
                for row in rows:
                    symbol = to_qmt_index_symbol(str(row[0] or ""))
                    if symbol:
                        symbols.append(symbol)
    return _dedupe_qmt_codes(symbols)


def fetch_all_index_codes(*, engine: Engine | None = None) -> pd.DataFrame:
    qmt_codes = _seed_index_symbols(engine)
    if not qmt_codes:
        return pd.DataFrame(columns=["index_code", "concept_code", "name", "source", "etl_sync_at"])

    details = bridge.instrument_details(qmt_codes, batch_size=300, timeout=240)
    if details is None or details.empty:
        return pd.DataFrame(columns=["index_code", "concept_code", "name", "source", "etl_sync_at"])

    out = pd.DataFrame()
    out["index_code"] = details["stock_code"].astype(str).str.zfill(6)
    out["concept_code"] = ""
    out["name"] = details.get("short_name", "").fillna("").astype(str).str.strip()
    out["source"] = "qmt"
    out = out[(out["index_code"] != "") & (out["name"] != "")]
    out = out.drop_duplicates(subset=["index_code"], keep="first")
    return out


def fetch_index_constituents(index_codes: Iterable[str]) -> pd.DataFrame:
    symbols = [symbol for symbol in (to_qmt_index_symbol(code) for code in index_codes) if symbol]
    if not symbols:
        return pd.DataFrame(columns=["index_code", "stock_code", "short_name", "etl_sync_at"])

    weight_df = bridge.index_weight_many(_dedupe_qmt_codes(symbols), timeout=600)
    if weight_df is None or weight_df.empty:
        return pd.DataFrame(columns=["index_code", "stock_code", "short_name", "etl_sync_at"])

    weight_df = weight_df.copy()
    weight_df["index_code"] = weight_df["index_code"].astype(str).str.zfill(6)
    members = weight_df[["index_code", "stock_code", "qmt_code"]].drop_duplicates(
        subset=["index_code", "stock_code"],
        keep="first",
    )
    member_symbols = members["qmt_code"].astype(str).tolist()
    details = bridge.instrument_details(_dedupe_qmt_codes(member_symbols), batch_size=400, timeout=300)
    if details is None or details.empty:
        members["short_name"] = ""
        return members[["index_code", "stock_code", "short_name"]]

    name_map = (
        details.assign(stock_code=details["stock_code"].astype(str).str.zfill(6))
        .set_index("stock_code")["short_name"]
        .fillna("")
        .astype(str)
        .to_dict()
    )
    members["short_name"] = members["stock_code"].astype(str).str.zfill(6).map(name_map).fillna("")
    return members[["index_code", "stock_code", "short_name"]]


def to_qmt_stock_symbols(stock_codes: Iterable[str]) -> list[str]:
    return [symbol for symbol in (to_qmt_symbol(code) for code in stock_codes) if symbol]


def to_qmt_index_symbols(index_codes: Iterable[str]) -> list[str]:
    return [symbol for symbol in (to_qmt_index_symbol(code) for code in index_codes) if symbol]


def from_qmt_index_symbol(symbol: str) -> str:
    return from_qmt_symbol(symbol)
