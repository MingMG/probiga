from __future__ import annotations

from pathlib import Path

import pytest

from tools.provision_mysql84_migration_account import (
    MigrationProvisionError,
    build_option_file,
)


PASSWORD = "A" * 64


def test_migration_option_file_pins_isolated_port() -> None:
    payload = build_option_file(
        user="probiga_migrator", password=PASSWORD, port=33090
    ).decode("ascii")
    assert "host=127.0.0.1" in payload
    assert "port=33090" in payload
    assert "user=probiga_migrator" in payload
    assert f"password={PASSWORD}" in payload


def test_migration_option_file_rejects_production_port() -> None:
    with pytest.raises(MigrationProvisionError, match="isolated port"):
        build_option_file(
            user="probiga_migrator", password=PASSWORD, port=3306
        )


def test_migration_option_file_rejects_unsafe_user() -> None:
    with pytest.raises(MigrationProvisionError, match="unsafe"):
        build_option_file(user="bad-user", password=PASSWORD, port=33090)
