from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import create_engine

from biz.notice.sync_notice_em import _parse_item
from server.common.pit_facts import (
    PIT_AVAILABLE,
    append_event_revision,
    ensure_pit_fact_schema,
    load_event_facts,
)


def test_future_business_date_uses_exact_display_time_for_visibility():
    received = datetime(2026, 8, 24, 22, 9, 0)
    payload = _parse_item(
        "000001",
        {
            "art_code": "AN-FUTURE-BUSINESS-DATE",
            "title": "晚间公告",
            "notice_date": "2026-08-25",
            "display_time": "2026-08-24 22:08:57:197",
        },
        received,
    )
    assert str(payload["event_date"]) == "2026-08-25"
    assert payload["published_at"] == "2026-08-24 22:08:57.197000"

    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_pit_fact_schema(engine)
    append_event_revision(engine, payload, known_at=received, received_at=received)

    before = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-24 20:00:00",
        fact_cutoff_at="2026-08-24 20:00:00",
        start_date="2026-08-24",
        end_date="2026-08-24",
    )
    assert not before.facts.get("000001")

    after = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-24 22:10:00",
        fact_cutoff_at="2026-08-24 22:10:00",
        start_date="2026-08-24",
        end_date="2026-08-24",
    )
    assert after.status_for("000001") == PIT_AVAILABLE
    assert after.facts["000001"][0]["event_date"] == "2026-08-25"
    assert after.facts["000001"][0]["event_published_at"].replace(
        "T", " "
    ).startswith("2026-08-24 22:08:57")


def test_source_publication_time_cannot_be_later_than_receipt():
    with pytest.raises(ValueError, match="later than local receipt"):
        _parse_item(
            "000001",
            {
                "art_code": "AN-CLOCK-FUTURE",
                "notice_date": "2026-08-25",
                "display_time": "2026-08-24 22:10:01:001",
            },
            datetime(2026, 8, 24, 22, 10, 0),
        )
