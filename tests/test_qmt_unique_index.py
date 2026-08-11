from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from tools import setup_guojin_qmt_unique_indexes


def test_setup_unique_indexes_main_uses_batch_engine_for_dry_run():
    engine = object()
    result = SimpleNamespace(status="DRY_RUN")

    with patch.object(
        setup_guojin_qmt_unique_indexes.sys,
        "argv",
        ["setup_guojin_qmt_unique_indexes.py"],
    ), patch(
        "tools.setup_guojin_qmt_unique_indexes.create_batch_engine",
        return_value=engine,
    ) as create_batch_engine, patch(
        "tools.setup_guojin_qmt_unique_indexes.ensure_qmt_unique_indexes",
        return_value=[result],
    ) as ensure_qmt_unique_indexes, patch(
        "tools.setup_guojin_qmt_unique_indexes.result_dicts",
        return_value=[{"status": "DRY_RUN"}],
    ):
        assert setup_guojin_qmt_unique_indexes.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    ensure_qmt_unique_indexes.assert_called_once_with(engine, dry_run=True)
