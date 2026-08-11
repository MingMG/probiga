from __future__ import annotations

from datetime import datetime

from biz.notice.sync_notice_em import _parse_item


def test_notice_item_requires_explicit_requested_stock_code() -> None:
    item = {
        "art_code": "AN202607270001",
        "title": "美盈森：关于回购股份的公告",
        "display_time": "2026-07-27 18:00:00",
        "codes": [{"stock_code": "002303", "short_name": "美盈森"}],
    }
    row = _parse_item("002303", item, datetime(2026, 7, 27, 20, 0))
    assert row is not None
    assert row["stock_code"] == "002303"
    assert row["association_validated"] == 1


def test_notice_item_rejects_all_market_false_association() -> None:
    item = {
        "art_code": "AN202607270002",
        "title": "某基金产品资料概要更新",
        "display_time": "2026-07-27 18:00:00",
        "codes": [],
    }
    assert _parse_item(
        "002303",
        item,
        datetime(2026, 7, 27, 20, 0),
    ) is None
