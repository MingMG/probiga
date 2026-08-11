from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from tools.cutover_mysql84_production import (
    CutoverError,
    ServiceState,
    read_datadir_uuid,
    secure_file_priv_is_disabled,
    validate_acceptance_evidence,
    validate_formal_config,
    validate_new_service_registration,
    validate_provision_evidence,
)


TARGET_UUID = "f40c3202-9260-11f1-86ae-74d4dd7f8500"


@pytest.mark.parametrize("value", [None, "NULL", "null", "  NULL  "])
def test_secure_file_priv_disabled_accepts_mysql84_windows_representations(
    value: object,
) -> None:
    assert secure_file_priv_is_disabled(value)


@pytest.mark.parametrize("value", ["", r"D:\\MySQL84\\files", "NONE"])
def test_secure_file_priv_disabled_rejects_unrestricted_or_directory_values(
    value: object,
) -> None:
    assert not secure_file_priv_is_disabled(value)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_validate_acceptance_requires_every_step_passed() -> None:
    valid = {
        "tool": "run_mysql84_final_acceptance",
        "status": "passed",
        "cutover_ready": True,
        "target": {"server_uuid": TARGET_UUID},
        "steps": [{"name": "smoke", "status": "passed"}],
    }
    validate_acceptance_evidence(valid, expected_uuid=TARGET_UUID)
    valid["steps"].append({"name": "manifest", "status": "failed"})
    with pytest.raises(CutoverError, match="incomplete step"):
        validate_acceptance_evidence(valid, expected_uuid=TARGET_UUID)


def test_validate_provision_evidence_binds_files_and_hash(tmp_path: Path) -> None:
    staged = tmp_path / "staged.env"
    option = tmp_path / "runtime.ini"
    ca = tmp_path / "ca.pem"
    staged.write_text("MYSQL_TLS_REQUIRED=true\n", encoding="utf-8")
    option.write_text("secret", encoding="utf-8")
    ca.write_text("ca", encoding="utf-8")
    evidence = {
        "status": "success",
        "secrets_in_evidence": False,
        "target": {"server_uuid": TARGET_UUID},
        "staged_env": str(staged),
        "runtime_option_file": str(option),
        "formal_ca": str(ca),
        "staged_env_sha256": _sha256(staged),
    }
    validate_provision_evidence(
        evidence,
        expected_uuid=TARGET_UUID,
        staged_env=staged,
        runtime_option_file=option,
        formal_ca=ca,
    )
    staged.write_text("MYSQL_TLS_REQUIRED=false\n", encoding="utf-8")
    with pytest.raises(CutoverError, match="hash differs"):
        validate_provision_evidence(
            evidence,
            expected_uuid=TARGET_UUID,
            staged_env=staged,
            runtime_option_file=option,
            formal_ca=ca,
        )


def test_formal_config_rejects_removable_drive(tmp_path: Path) -> None:
    config = tmp_path / "my.ini"
    config.write_text(
        """[mysqld]
port=3306
bind-address=127.0.0.1
datadir=E:/MySQL84/Data
require-secure-transport=ON
binlog-format=ROW
sync-binlog=1
innodb-flush-log-at-trx-commit=1
local-infile=OFF
mysql-native-password=OFF
tmpdir=F:/temporary
""",
        encoding="utf-8",
    )
    with pytest.raises(CutoverError, match="removable drive F"):
        validate_formal_config(config, expected_datadir=Path(r"E:\MySQL84\Data"))


def test_checked_in_formal_config_keeps_runtime_writes_on_internal_e_drive() -> None:
    config = (
        Path(__file__).resolve().parents[1] / "ops" / "mysql84" / "my-production.ini"
    ).read_text(encoding="utf-8")

    expected_runtime_paths = (
        "datadir=E:/MySQL84/Data",
        "log-bin=E:/MySQL84/Logs/mysql-bin",
        "log-bin-index=E:/MySQL84/Logs/mysql-bin.index",
        "tmpdir=E:/MySQL84/Tmp",
        "log-error=E:/MySQL84/Logs/mysql84.err",
        "pid-file=E:/MySQL84/Logs/mysql84.pid",
        "slow-query-log-file=E:/MySQL84/Logs/mysql84-slow.log",
    )
    for setting in expected_runtime_paths:
        assert setting in config

    assert "D:/MySQL84/logs" not in config
    assert "D:/MySQL84/tmp" not in config


def test_read_datadir_uuid_is_exact(tmp_path: Path) -> None:
    (tmp_path / "auto.cnf").write_text(
        f"[auto]\nserver-uuid={TARGET_UUID}\n", encoding="utf-8"
    )
    assert read_datadir_uuid(tmp_path) == TARGET_UUID


def test_existing_service_must_use_formal_binary_and_config() -> None:
    mysqld = Path(r"D:\MySQL84\software\mysql-8.4.11-winx64\bin\mysqld.exe")
    config = Path(r"D:\MySQL84\config\my.ini")
    valid = ServiceState(
        True,
        "STOPPED",
        "AUTO_START",
        f'"{mysqld}" --defaults-file="{config}" ProBigA-MySQL84',
    )
    validate_new_service_registration(valid, mysqld=mysqld, formal_config=config)
    invalid = ServiceState(
        True,
        "STOPPED",
        "AUTO_START",
        r'"C:\other\mysqld.exe" --defaults-file="C:\other\my.ini"',
    )
    with pytest.raises(CutoverError, match="formal binary/config"):
        validate_new_service_registration(invalid, mysqld=mysqld, formal_config=config)
