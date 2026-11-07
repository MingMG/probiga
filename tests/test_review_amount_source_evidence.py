from datetime import datetime
from decimal import Decimal
import json
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pandas as pd

from biz.review import amount_source_evidence as evidence
from biz.review.amount_integrity import assess_amount_integrity
from biz.review.quant_digest import generate_quant_digest_from_frames
from biz.review.research_flow import prepare_research_facts


DAY = "2026-11-06"


def _row(code, **fields):
    return {
        "id": 41, "stock_code": code, "trade_date": DAY, "adjust_type": 0,
        "open": 6.35, "close": 6.29, "low": 6.18, "high": 6.43,
        "amount": 12_690_811, "volume": 2_058_000,
        "data_source": "qmt_datadir", "quality_status": "VERIFIED",
        "permission_status": "SUPPORTED", "etl_sync_at": datetime(2026, 11, 7, 3, 18, 15),
        **fields,
    }


def _capture(row, **overrides):
    values = {f"source_{key}": Decimal(str(row[key])) for key in ("open", "close", "low", "high", "amount")}
    values["source_volume_shares"] = Decimal(row["volume"])
    return SimpleNamespace(**{
        **values, "target": SimpleNamespace(target_row_id=row["id"]), "raw_payload_sha256": "a" * 64,
        **overrides,
    })


def _disclosure(*, omit_second=False):
    rows = [
        {"hqbcjhth": str(i), "hqscjhth": str(i + 10), "hqzqdm": "920717", "hqjsrq": "20261106",
         "hqfcbj": "2", "xxzrlx": "B", "hqcjsj": stamp, "hqcjsl": shares, "hqcjjg": 6.09}
        for i, (stamp, shares) in enumerate((("150422", 716368), ("150425", 698772)), start=1)
    ]
    if omit_second:
        rows.pop()
    result = {"content": rows, "number": 0, "size": 20, "numberOfElements": len(rows),
              "totalElements": 2, "lastPage": True}
    return ("bseRead([" + json.dumps([result]) + ", ''])").encode()


def test_review_uses_complete_official_block_scope_without_rewriting_full_day_amount(monkeypatch):
    bar = _row("920717")
    capture = _capture(bar, source_amount=Decimal("12690811.41"))
    monkeypatch.setattr(evidence, "_turnover_rows", lambda *args: (
        {"920717": capture}, {"turnover_known_at": "2026-11-07T06:58:54"},
    ))
    calls = []

    def fetch(session, method, url, **options):
        calls.append(parse_qs(urlsplit(url).query))
        assert method == "GET" and options["cap"] == 128 * 1024
        return _disclosure()

    monkeypatch.setattr(evidence, "_fetch", fetch)
    bars = pd.DataFrame([bar])
    assert assess_amount_integrity(bars)["inconsistent_count"] == 1
    corrected = evidence.attach_review_amount_evidence(None, bars)
    result = assess_amount_integrity(corrected)
    proof, = result["corroborated_rows"]
    assert result["status"] == "OBSERVED"
    assert proof["kind"] == "BSE_AUCTION_SCOPE"
    assert sum(row["shares"] for row in proof["blocks"]["trades"]) == 1_415_140
    assert proof["amount"] == corrected.iloc[0]["amount"] == bar["amount"]
    assert proof["available_at"] == proof["blocks"]["observed_at"]
    previous = pd.DataFrame([{**bar, "trade_date": "2026-11-05", "pre_close": 6.42,
                             "amount": 12_900_000}])
    report = generate_quant_digest_from_frames(
        DAY, target_bars=corrected.assign(pre_close=6.42), previous_bars=previous,
        universe=["920717"], industries=None,
    )
    assert datetime.fromisoformat(report["data_cutoff_at"]).replace(tzinfo=None) == datetime.fromisoformat(
        proof["available_at"]
    )
    assert len(calls) == 1 and calls[0]["startDate"] == calls[0]["endDate"] == [DAY]
    assert "btzqdm" not in calls[0]  # Bulk, including all securities in this session.

    asof = datetime(2026, 11, 7, 20)
    historical = evidence.attach_review_amount_evidence(None, bars, cutoff=asof)
    assert assess_amount_integrity(historical)["status"] == "UNKNOWN"
    from_cached_bars = generate_quant_digest_from_frames(
        DAY, target_bars=corrected.assign(pre_close=6.42), previous_bars=previous,
        universe=["920717"], industries=None, now=asof,
    )
    assert from_cached_bars["market_structure_json"]["amount_quality"]["status"] == "UNKNOWN"
    assert len(calls) == 1

    monkeypatch.setattr(evidence, "_fetch", lambda *args, **kwargs: _disclosure(omit_second=True))
    incomplete = evidence.attach_review_amount_evidence(None, bars)
    assert assess_amount_integrity(incomplete)["status"] == "UNKNOWN"


def test_one_price_exception_needs_exact_source_and_stays_within_native_resolution(monkeypatch):
    bar = _row("603381", open=20.74, close=20.74, low=20.74, high=20.74,
               amount=155_858_466, volume=7_514_900)
    bars = pd.DataFrame([bar])
    captures = {"603381": _capture(bar)}
    monkeypatch.setattr(evidence, "_turnover_rows", lambda *args: (captures, {"turnover_known_at": "2026-11-07T06:58:54"}))
    assert assess_amount_integrity(bars)["status"] == "UNKNOWN"
    corroborated = evidence.attach_review_amount_evidence(None, bars)
    assert assess_amount_integrity(corroborated)["corroborated_rows"][0]["kind"] == "QMT_DAT_LOT_RESOLUTION"

    too_early = evidence.attach_review_amount_evidence(None, bars, cutoff=datetime(2026, 11, 6, 23))
    assert assess_amount_integrity(too_early)["status"] == "UNKNOWN"
    captures["603381"] = _capture(bar, source_amount=Decimal("155858460"))
    assert assess_amount_integrity(evidence.attach_review_amount_evidence(None, bars))["status"] == "UNKNOWN"

    bad_bar = {**bar, "amount": 155_850_000}
    captures["603381"] = _capture(bad_bar)
    assert assess_amount_integrity(evidence.attach_review_amount_evidence(None, pd.DataFrame([bad_bar])))["status"] == "UNKNOWN"

    checked = assess_amount_integrity(corroborated)
    digest = {
        "review_date": DAY, "quality_json": {"source_dates": {"previous_bars": DAY}},
        "market_structure_json": {"sample_count": 1, "previous_sample_count": 1,
                                  "total_amount": bar["amount"], "amount_quality": checked,
                                  "previous_amount_quality": checked},
    }
    sampled = {"theme_evidence": {"amount_quality": {"target": assess_amount_integrity(bars)}}}
    assert prepare_research_facts(digest, sampled)["market_structure_json"]["total_amount"] == bar["amount"]
    sampled["theme_evidence"]["amount_quality"]["target"] = assess_amount_integrity([bad_bar])
    assert prepare_research_facts(digest, sampled)["market_structure_json"]["total_amount"] is None
