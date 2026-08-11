from __future__ import annotations

import pytest

from server.common.mysql_version_policy import (
    ISOLATED_ACCEPTANCE_ORACLE_MYSQL_VERSIONS,
    MYSQL_57_VALIDATED_BASELINE,
    MYSQL_84_ISOLATED_ACCEPTANCE,
    PRODUCTION_DATABASE_ACTIVATION_ALLOWED,
    is_isolated_acceptance_version,
    is_oracle_mysql_distribution,
    isolated_acceptance_version,
    isolated_acceptance_versions_label,
)


def test_exact_isolated_acceptance_versions_are_frozen_and_non_production():
    assert MYSQL_57_VALIDATED_BASELINE == "5.7.38"
    assert MYSQL_84_ISOLATED_ACCEPTANCE == "8.4.11"
    assert ISOLATED_ACCEPTANCE_ORACLE_MYSQL_VERSIONS == (
        "5.7.38",
        "8.4.11",
    )
    assert isolated_acceptance_versions_label() == "5.7.38 or 8.4.11"
    assert PRODUCTION_DATABASE_ACTIVATION_ALLOWED is False


@pytest.mark.parametrize(
    ("server_version", "base_version"),
    (
        ("5.7.38", "5.7.38"),
        ("5.7.38-log", "5.7.38"),
        ("8.4.11", "8.4.11"),
        ("8.4.11-commercial", "8.4.11"),
    ),
)
def test_exact_versions_and_server_suffixes_are_accepted(
    server_version,
    base_version,
):
    assert isolated_acceptance_version(server_version) == base_version
    assert is_isolated_acceptance_version(server_version) is True


@pytest.mark.parametrize(
    "server_version",
    (
        "5.7.37",
        "5.7.39",
        "8.4.10",
        "8.4.12",
        "8.0.39",
        "5.7.38garbage",
        "8.4.11.foo",
        "5.7.38-MariaDB",
        "8.4.11-Percona",
        "",
    ),
)
def test_unvalidated_patch_or_distribution_is_rejected(server_version):
    assert isolated_acceptance_version(server_version) is None
    assert is_isolated_acceptance_version(server_version) is False


@pytest.mark.parametrize(
    "comment",
    (
        "MySQL Community Server (GPL)",
        "MySQL Enterprise Server - Commercial",
    ),
)
def test_only_oracle_mysql_distribution_comments_are_accepted(comment):
    assert is_oracle_mysql_distribution("8.4.11", comment) is True


@pytest.mark.parametrize(
    "comment",
    ("Percona Server (GPL)", "MariaDB Server", "Compatible Server", ""),
)
def test_non_oracle_distribution_comments_are_rejected(comment):
    assert is_oracle_mysql_distribution("8.4.11", comment) is False
