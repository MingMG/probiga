import pytest

from tools.remove_invalid_kline_instruments import _normalize_codes


def test_invalid_kline_code_normalization_is_exact_and_deduplicated():
    assert _normalize_codes(["301677,688825", "810011", "810011"]) == [
        "301677",
        "688825",
        "810011",
    ]
    with pytest.raises(ValueError):
        _normalize_codes(["ABC"])
