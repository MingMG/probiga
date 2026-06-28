from __future__ import annotations

from integrations.qmt.audit import AUDIT_TABLE_DDLS


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
