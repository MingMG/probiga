from __future__ import annotations

from unittest.mock import patch

from integrations.qmt.audit import AUDIT_TABLE_DDLS
from tools import setup_guojin_qmt_audit


def test_audit_schema_contains_all_control_tables():
    ddl = "\n".join(AUDIT_TABLE_DDLS)

    assert "qmt_raw_manifest" in ddl
    assert "sys_data_sync_run" in ddl
    assert "sys_data_coverage" in ddl
    assert "sys_data_gap" in ddl
    assert "sys_data_quality_result" in ddl


def test_audit_schema_is_guojin_qmt_scoped():
    ddl = "\n".join(AUDIT_TABLE_DDLS)

    assert ddl.count("gj_qmt") == len(AUDIT_TABLE_DDLS)


def test_setup_audit_main_uses_batch_engine():
    engine = object()

    with patch("tools.setup_guojin_qmt_audit.create_batch_engine", return_value=engine) as create_batch_engine, \
         patch("tools.setup_guojin_qmt_audit.ensure_audit_tables", return_value=5) as ensure_audit_tables:
        assert setup_guojin_qmt_audit.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    ensure_audit_tables.assert_called_once_with(engine)
