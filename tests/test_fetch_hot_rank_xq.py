from types import SimpleNamespace
from unittest.mock import patch

from tools import fetch_hot_rank_xq


def test_xueqiu_hot_rank_filters_non_a_shares_and_reranks():
    response = SimpleNamespace(
        raise_for_status=lambda: None,
        json=lambda: {
            "error_code": 0,
            "data": {
                "items": [
                    {"symbol": "NVDA", "name": "NVIDIA"},
                    {"symbol": "SH600000", "name": "浦发银行", "current": 10.0},
                    {"symbol": "HK00700", "name": "腾讯控股"},
                    {"symbol": "SZ000001", "name": "平安银行", "current": 12.0},
                    {"symbol": "BJ430047", "name": "诺思兰德", "current": 18.0},
                ]
            },
        },
    )

    with patch.object(
        fetch_hot_rank_xq._SESSION,
        "get",
        return_value=response,
    ) as request:
        frame = fetch_hot_rank_xq._fetch_hot_rank_xq()

    assert request.call_args.kwargs["params"] == {
        "size": 100,
        "_type": 12,
        "type": 12,
    }
    assert frame is not None
    assert frame["stock_code"].tolist() == ["600000", "000001", "430047"]
    assert frame["rank"].tolist() == [1, 2, 3]
