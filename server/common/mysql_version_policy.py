"""Frozen Oracle MySQL version policy for isolated database acceptance.

The versions in this module are exact server builds for which the repository
may run migrations, structural inspection, and isolated TEST/CI acceptance.
They do not authorize production activation or actionable trading output.
"""
from __future__ import annotations

import re


MYSQL_57_VALIDATED_BASELINE = "5.7.38"
MYSQL_84_ISOLATED_ACCEPTANCE = "8.4.11"
ISOLATED_ACCEPTANCE_ORACLE_MYSQL_VERSIONS = (
    MYSQL_57_VALIDATED_BASELINE,
    MYSQL_84_ISOLATED_ACCEPTANCE,
)
PRODUCTION_DATABASE_ACTIVATION_ALLOWED = False

_VERSION_PATTERNS = tuple(
    (
        version,
        re.compile(
            rf"^{re.escape(version)}"
            r"(?:[-+][0-9A-Za-z][0-9A-Za-z._-]*)?$"
        ),
    )
    for version in ISOLATED_ACCEPTANCE_ORACLE_MYSQL_VERSIONS
)
_ORACLE_MYSQL_VERSION_COMMENT_RE = re.compile(
    r"^MySQL (?:Community|Enterprise) Server\b",
    re.IGNORECASE,
)
_FORBIDDEN_DISTRIBUTION_TOKENS = ("mariadb", "percona")


def isolated_acceptance_version(version: object) -> str | None:
    """Return the exact accepted base version, or ``None`` when unvalidated."""

    candidate = str(version or "").strip()
    lowered = candidate.casefold()
    if any(token in lowered for token in _FORBIDDEN_DISTRIBUTION_TOKENS):
        return None
    for accepted, pattern in _VERSION_PATTERNS:
        if pattern.fullmatch(candidate) is not None:
            return accepted
    return None


def is_isolated_acceptance_version(version: object) -> bool:
    return isolated_acceptance_version(version) is not None


def is_oracle_mysql_distribution(
    version: object,
    version_comment: object,
) -> bool:
    combined = f"{version or ''} {version_comment or ''}".casefold()
    return (
        not any(token in combined for token in _FORBIDDEN_DISTRIBUTION_TOKENS)
        and _ORACLE_MYSQL_VERSION_COMMENT_RE.match(
            str(version_comment or "").strip()
        )
        is not None
    )


def isolated_acceptance_versions_label() -> str:
    return " or ".join(ISOLATED_ACCEPTANCE_ORACLE_MYSQL_VERSIONS)


__all__ = [
    "ISOLATED_ACCEPTANCE_ORACLE_MYSQL_VERSIONS",
    "MYSQL_57_VALIDATED_BASELINE",
    "MYSQL_84_ISOLATED_ACCEPTANCE",
    "PRODUCTION_DATABASE_ACTIVATION_ALLOWED",
    "is_isolated_acceptance_version",
    "is_oracle_mysql_distribution",
    "isolated_acceptance_version",
    "isolated_acceptance_versions_label",
]
