# -*- coding: utf-8 -*-
import pytest

from tools import deploy_production_files


def test_backup_retention_defaults_to_two():
    args = deploy_production_files.parse_args(["server/api/main.py"])

    assert args.backup_retention == 2


def test_backup_prune_command_only_targets_old_acceptance_directories():
    command = deploy_production_files._backup_prune_command(
        "/opt/ProBigA/.codex_backups",
        keep=2,
    )

    assert "-mindepth 1 -maxdepth 1 -type d" in command
    assert "-name 'acceptance_[0-9]*'" in command
    assert "sort -r" in command
    assert "tail -n +3" in command
    assert "rm -rf -- /opt/ProBigA/.codex_backups/\"$name\"" in command


def test_backup_retention_must_be_positive():
    with pytest.raises(ValueError, match="positive"):
        deploy_production_files._backup_prune_command("/opt/ProBigA/.codex_backups", 0)

    with pytest.raises(SystemExit):
        deploy_production_files.parse_args(["server/api/main.py", "--backup-retention", "0"])


def test_manual_production_upload_defaults_to_disabled(monkeypatch):
    monkeypatch.delenv("PROBIGA_MANUAL_PRODUCTION_DEPLOY_ENABLED", raising=False)

    with pytest.raises(SystemExit, match="disabled"):
        deploy_production_files._require_manual_deploy_authorization()


def test_manual_production_upload_cannot_be_enabled_by_environment(monkeypatch):
    monkeypatch.setenv("PROBIGA_MANUAL_PRODUCTION_DEPLOY_ENABLED", "1")

    with pytest.raises(SystemExit, match="permanently disabled"):
        deploy_production_files._require_manual_deploy_authorization()
