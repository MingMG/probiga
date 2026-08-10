"""Semantic normalization for MySQL metadata exposed across releases."""
from __future__ import annotations


def normalize_mysql_referential_rule(value: object) -> str:
    """Return a stable MySQL foreign-key action label.

    MySQL implements ``NO ACTION`` as ``RESTRICT`` and different server
    releases may expose either spelling through INFORMATION_SCHEMA. Schema
    attestation must preserve every real action difference while treating
    those two labels as the same MySQL behavior.
    """

    normalized = str(value or "").strip().upper()
    return "RESTRICT" if normalized == "NO ACTION" else normalized


__all__ = ["normalize_mysql_referential_rule"]
