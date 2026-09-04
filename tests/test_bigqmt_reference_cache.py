from __future__ import annotations

import pandas as pd

from integrations.bigqmt import reference


def test_sector_cache_failure_does_not_discard_captured_frames(monkeypatch) -> None:
    monkeypatch.setattr(
        reference.bridge,
        "sector_list",
        lambda **_kwargs: pd.DataFrame(
            [
                {"sector_name": "TDGN测试概念", "parent_path": "概念"},
                {"sector_name": "1000SW1测试行业", "parent_path": "申万一级"},
            ]
        ),
    )

    def members(sector_names, **_kwargs):
        return pd.DataFrame(
            [
                {
                    "sector_name": sector_name,
                    "qmt_code": "000001.SZ",
                    "stock_code": "000001",
                }
                for sector_name in sector_names
            ]
        )

    monkeypatch.setattr(reference.bridge, "sector_members_many", members)
    monkeypatch.setattr(
        reference.bridge,
        "instrument_details",
        lambda *_args, **_kwargs: pd.DataFrame(
            [{"stock_code": "000001", "short_name": "平安银行"}]
        ),
    )
    monkeypatch.setattr(
        reference,
        "_write_sector_cache",
        lambda _datasets: (_ for _ in ()).throw(TypeError("cache serialization")),
    )

    result = reference.fetch_sector_datasets(force_refresh=True)

    assert result["concept_catalog"]["name"].tolist() == ["测试概念"]
    assert result["industry_sw"]["industry_name"].tolist() == ["测试行业"]
