from __future__ import annotations

import pytest

from tools.verify_mysql55_restart_only_binlog_tail import (
    Coordinate,
    Event,
    RestartTailError,
    validate_restart_only,
)


def _accepted() -> Coordinate:
    return Coordinate("mysql-bin.000002", 428_870_702)


def _current() -> Coordinate:
    return Coordinate("mysql-bin.000003", 107)


def _inventory():
    return (
        ("mysql-bin.000001", 536_930_974),
        ("mysql-bin.000002", 428_870_721),
        ("mysql-bin.000003", 107),
    )


def _events():
    return {
        "mysql-bin.000002": (
            Event("mysql-bin.000002", 428_870_702, "Stop", 55, 428_870_721),
        ),
        "mysql-bin.000003": (
            Event("mysql-bin.000003", 4, "Format_desc", 55, 107),
        ),
    }


def test_accepts_only_clean_shutdown_and_startup_events() -> None:
    selected = validate_restart_only(
        accepted=_accepted(),
        current=_current(),
        inventory=_inventory(),
        events_by_file=_events(),
        source_server_id=55,
    )

    assert selected == ("mysql-bin.000002", "mysql-bin.000003")


def test_rejects_any_business_or_unknown_event() -> None:
    events = _events()
    events["mysql-bin.000003"] = (
        Event("mysql-bin.000003", 4, "Format_desc", 55, 107),
        Event("mysql-bin.000003", 107, "Query", 55, 250),
    )

    with pytest.raises(RestartTailError, match="non-restart"):
        validate_restart_only(
            accepted=_accepted(),
            current=Coordinate("mysql-bin.000003", 250),
            inventory=(
                ("mysql-bin.000001", 536_930_974),
                ("mysql-bin.000002", 428_870_721),
                ("mysql-bin.000003", 250),
            ),
            events_by_file=events,
            source_server_id=55,
        )


def test_rejects_a_skipped_binlog_number() -> None:
    with pytest.raises(RestartTailError, match="not contiguous"):
        validate_restart_only(
            accepted=_accepted(),
            current=Coordinate("mysql-bin.000004", 107),
            inventory=(
                ("mysql-bin.000002", 428_870_721),
                ("mysql-bin.000004", 107),
            ),
            events_by_file={
                "mysql-bin.000002": _events()["mysql-bin.000002"],
                "mysql-bin.000004": (
                    Event("mysql-bin.000004", 4, "Format_desc", 55, 107),
                ),
            },
            source_server_id=55,
        )
