"""Exact native daily revisions corroborated by a published BSE block trade."""
from __future__ import annotations

from datetime import datetime, time
from decimal import Decimal, ROUND_HALF_UP
from hashlib import sha256
from html import unescape
import re
import time as clock
from urllib.parse import urlencode

import requests
from sqlalchemy import text

from integrations.qmt.datadir import (
    CST, LOT_TO_SHARES, MINUTE_PERIOD, iter_minute_rows, load_dat, session_slice,
)
from server.common.official_listing_evidence import _fetch
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_datadir_result import load_receipt
from server.common.qmt_index_minute_grid import CORE_GRID

KIND = "BSE_POST_CLOSE_BLOCK_TRADE"
SOURCE = "https://vip.stock.finance.sina.com.cn/q/go.php/vInvestConsult/kind/dzjy/index.phtml"
_HASH = re.compile(r"[0-9a-f]{64}")


def _url(code, day):
    return SOURCE + "?" + urlencode({"bdate": day, "edate": day, "symbol": code})


def _published_block(code, day):
    url = _url(code, day)
    with requests.Session() as session:
        session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"})
        raw = _fetch(session, "GET", url, deadline=clock.monotonic() + 12,
                     stage="bse_post_close_public_block", cap=256 * 1024)
    page = raw.decode("gb18030", errors="replace")
    candidates = []
    for line in re.findall(r"<tr\b[^>]*>(.*?)</tr>", page, re.I | re.S):
        if code not in line or day not in line:
            continue
        cells = [unescape(re.sub(r"<[^>]+>", "", cell)).strip()
                 for cell in re.findall(r"<td\b[^>]*>(.*?)</td>", line, re.I | re.S)]
        if len(cells) == 9 and cells[:2] == [day, code] and cells[8] == "A股":
            candidates.append(cells)
    if len(candidates) != 1:
        raise ValueError("exact published BSE block trade row differs")
    return {"source_url": url, "raw_response_sha256": sha256(raw).hexdigest(),
            "observed_at": datetime.now(CST).replace(tzinfo=None, microsecond=0).isoformat(" "),
            "source_row": candidates[0], "source_row_sha256": canonical_digest(candidates[0])}


def _stamp(value):
    at = datetime.fromisoformat(str(value))
    return at.astimezone(CST).replace(tzinfo=None) if at.tzinfo else at


def _minute_row(moment, price, volume, amount):
    return {"trade_time": _stamp(moment).isoformat(" "), **{
        key: format(Decimal(str(value)).quantize(Decimal("0.000001")), "f")
        for key, value in (("price", price), ("volume", volume), ("amount", amount))
    }}


def load_post_close_evidence(engine, *, source_root, session, code, prior):
    """Reuse the prior full receipt and freshly compare the exact native/DB symbol grid."""
    if not re.fullmatch(r"[489][0-9]{5}", code):
        raise ValueError("BSE post-close block trade requires one Beijing security")
    day = session.isoformat()
    market = load_dat(source_root, "BJ", MINUTE_PERIOD, code)
    if market is None:
        raise ValueError("BSE post-close native minute source unavailable")
    selected = session_slice(market.records, session)
    native = [_minute_row(row[1], row[3], row[6], row[7])
              for _, row in iter_minute_rows(market, selected, frozenset(CORE_GRID))]
    expected = [item["stock_code"] for item in prior["entities"] if item["classification"] == "TRADED"
                and (Decimal(item["evidence"]["volume"]) > 0 or Decimal(item["evidence"]["amount"]) > 0)]
    with engine.connect() as connection:
        receipt = load_receipt(connection, dataset="stock_minute", trade_date=day)
        detail = (receipt or {}).get("detail") or {}
        if (not receipt or receipt["status"] != "COMPLETE" or code not in expected
                or detail.get("full_market_manifest_hash") != prior["manifest_hash"]
                or detail.get("coverage") != 1.0 or detail.get("bad_grid_codes") != 0
                or detail.get("expected_traded_code_set_sha256") != canonical_digest(expected)
                or any(value != len(expected) for value in (receipt["code_count"], detail.get("readback_codes")))
                or any(value != len(expected) * len(CORE_GRID) for value in (receipt["row_count"], detail.get("readback_rows")))
                or detail.get("minute_grid_count") != len(CORE_GRID)
                or detail.get("minute_grid_sha256") != canonical_digest(list(CORE_GRID))
                or not _HASH.fullmatch(str(detail.get("row_hash") or ""))
                or not _HASH.fullmatch(str(receipt.get("content_sha256") or ""))):
            raise ValueError("BSE post-close prior exact minute receipt differs")
        rows = connection.execute(text("""SELECT /*+ MAX_EXECUTION_TIME(3500) */
            trade_date,trade_time,price,volume,amount,source_time,received_at,qmt_code,data_source,quality_status,permission_status
            FROM sm_stock_minute WHERE stock_code=:code AND trade_time BETWEEN :first AND :last
            ORDER BY trade_time LIMIT :limit"""),
            {"code": code, "first": day + " 09:30:00", "last": day + " 15:30:00",
             "limit": len(CORE_GRID) + 1}).mappings().all()
    finished = _stamp(receipt["finished_at"])
    finality = datetime.combine(session, time(15, 30))
    if (len(rows) != len(CORE_GRID) or any(str(row["trade_date"])[:10] != day or row["qmt_code"] != code + ".BJ"
            or (row["data_source"], row["quality_status"], row["permission_status"]) != ("qmt_datadir", "VERIFIED", "SUPPORTED")
            or row["source_time"] != row["trade_time"] or not finality <= _stamp(row["received_at"]) <= finished
            for row in rows)):
        raise ValueError("BSE post-close persisted minute identity or finality differs")
    stored = [_minute_row(row["trade_time"], row["price"], row["volume"], row["amount"]) for row in rows]
    if native != stored:
        raise ValueError("BSE post-close native and durable minute rows differ")
    published = _published_block(code, day)
    return {"publication": published, "minute_run_id": receipt["run_id"],
            "minute_finished_at": finished.isoformat(" "), "minute_partition_row_hash": detail["row_hash"],
            "minute_partition_source_sha256": receipt["content_sha256"],
            "prior_daily_manifest_hash": prior["manifest_hash"], "minute_rows": stored,
            "minute_rows_sha256": canonical_digest(stored),
            "native_minute_dat_sha256": sha256(selected.tobytes()).hexdigest()}


def verify_post_close_revision(old, new, evidence, *, prior_hash, cutoff):
    """Reproduce only this attested scope correction with explicit native precision."""
    code, day = old["stock_code"], old["trade_date"]
    if (not re.fullmatch(r"[489][0-9]{5}", code) or not _HASH.fullmatch(prior_hash)
            or evidence["prior_daily_manifest_hash"] != prior_hash
            or any(old[key] != new[key] for key in old if key not in {"amount", "volume"})):
        raise ValueError("BSE post-close revision changes unrelated native fields")
    publication, rows = evidence["publication"], evidence["minute_rows"]
    cells = publication["source_row"]
    observed, finished = _stamp(publication["observed_at"]), _stamp(evidence["minute_finished_at"])
    finality = datetime.fromisoformat(day + " 15:30:00")
    if (publication["source_url"] != _url(code, day) or len(cells) != 9 or cells[:2] != [day, code]
            or cells[8] != "A股" or not finality <= finished <= observed <= _stamp(cutoff)
            or publication["source_row_sha256"] != canonical_digest(cells)
            or not _HASH.fullmatch(publication["raw_response_sha256"])
            or any(not _HASH.fullmatch(evidence[key]) for key in (
                "minute_partition_row_hash", "minute_partition_source_sha256", "native_minute_dat_sha256",
            )) or not evidence["minute_run_id"]
            or evidence["minute_rows_sha256"] != canonical_digest(rows)
            or [row["trade_time"] for row in rows] != [day + " " + slot for slot in CORE_GRID]):
        raise ValueError("BSE post-close revision evidence identity differs")
    shares, price, disclosed = Decimal(cells[4]), Decimal(cells[3]), Decimal(cells[5]) * 10_000
    amount = shares * price
    whole_amount = amount.quantize(Decimal(1), rounding=ROUND_HALF_UP)
    old_amount, new_amount = Decimal(old["amount"]), Decimal(new["amount"])
    old_volume, new_volume = Decimal(old["volume"]), Decimal(new["volume"])
    minute_amounts = [Decimal(row["amount"]) for row in rows]
    minute_volumes = [Decimal(row["volume"]) for row in rows]
    residual = new_amount - sum(minute_amounts)
    if (not (shares == int(shares) > 0 and price > 0) or abs(amount - disclosed) >= 100
            or not 0 <= old_amount < new_amount < 2 ** 32
            or whole_amount != int(amount) or new_amount - old_amount != whole_amount
            or not 0 < old_volume < new_volume or old_volume % LOT_TO_SHARES or new_volume % LOT_TO_SHARES
            or abs(new_volume - old_volume - shares) >= LOT_TO_SHARES
            or any(value < 0 or value != int(value) for value in (*minute_amounts, *minute_volumes))
            or sum(minute_volumes) * LOT_TO_SHARES != new_volume
            or not 0 <= residual <= sum(value > 0 for value in minute_amounts)
            or minute_amounts[-1] + 1 < amount or minute_volumes[-1] * LOT_TO_SHARES < shares
            or rows[-1]["price"] != new["close"]):
        raise ValueError("BSE post-close revision is outside exact source precision")
    return {"kind": KIND, "stock_code": code, "previous_amount": old["amount"], "amount": new["amount"],
            "previous_volume": old["volume"], "volume": new["volume"], "evidence": evidence}
