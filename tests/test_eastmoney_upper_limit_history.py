from datetime import date

import pytest

from server.common.eastmoney_upper_limit_history import (
    EastmoneyUpperLimitHistoryBlocked,
    _pool_codes_from_rows,
)


def test_identical_provider_pool_duplicates_are_collapsed() -> None:
    row = {
        "c": "600501",
        "n": "test",
        "p": 15330,
        "zbc": 2,
        "zttj": {"days": 1, "ct": 1},
    }

    assert _pool_codes_from_rows(
        [row, dict(row)],
        date(2026, 10, 16),
    ) == ["600501"]


def test_conflicting_provider_pool_duplicates_are_blocked() -> None:
    first = {"c": "600501", "p": 15330}
    second = {"c": "600501", "p": 15340}

    with pytest.raises(
        EastmoneyUpperLimitHistoryBlocked,
        match="conflicting duplicates",
    ):
        _pool_codes_from_rows([first, second], date(2026, 10, 16))
