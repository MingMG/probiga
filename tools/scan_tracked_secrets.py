#!/usr/bin/env python3
"""Fail closed when tracked source contains embedded credentials.

The scanner intentionally reports only a path, line number, and finding type.
Matched text is never included in stdout or stderr.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Sequence


SOURCE_SUFFIXES = frozenset(
    {
        ".bat",
        ".cnf",
        ".cmd",
        ".conf",
        ".html",
        ".ini",
        ".js",
        ".jsx",
        ".json",
        ".md",
        ".ps1",
        ".py",
        ".sh",
        ".txt",
        ".toml",
        ".ts",
        ".tsx",
        ".vue",
        ".yaml",
        ".yml",
    }
)

# Only these reviewed files contain deliberate credential-shaped test values.
# Do not exclude the whole tests tree: a newly added test is scanned by default.
# The public example environment is documentation, not a runnable credential.
EXCLUDED_PATHS = frozenset(
    {
        ".env.example",
        "tests/fixtures/config.py",
        "tests/test_account_auth.py",
        "tests/test_api_error_handling.py",
        "tests/test_prepare_strategy_governance_schema.py",
        "tests/test_qmt_local_history.py",
        "tests/test_qmt_local_history_capture.py",
        "tests/test_remote_support.py",
        "tests/test_scan_tracked_secrets.py",
        "tests/test_scheduler_runtime.py",
        "tests/test_sync_concept_ths_runtime.py",
        "tests/test_trading_v3_mysql_acceptance.py",
    }
)


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    kind: str


_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "DB_URL_PASSWORD",
        re.compile(
            r"(?i)\b(?:mysql(?:\+pymysql)?|mariadb|postgres(?:ql)?)(?:://|\+[a-z0-9_]+://)"
            r"[^\s:/@'\"]+:[^\s@/'\"]+@"
        ),
    ),
    (
        "PASSWORD_LITERAL",
        re.compile(
            r"(?ix)(?<![a-z0-9_])\$?(?:[a-z_][a-z0-9_]*?)?(?:password|passwd|pwd)"
            r"\b\s*(?::=|=|:)\s*(?:[rubf]{0,2})?"
            r"(?P<quote>['\"])"
            r"(?:(?!(?P=quote)).)+(?P=quote)"
        ),
    ),
    (
        "PASSWORD_LITERAL",
        re.compile(
            r"(?ix)['\"](?:[a-z_][a-z0-9_]*?)?"
            r"(?:password|passwd|pwd|(?:db|database|mysql|remote|ssh)_?pass)"
            r"['\"]\s*:\s*(?:[rubf]{0,2})?"
            r"(?P<mapping_quote>['\"])(?:(?!(?P=mapping_quote)).)+"
            r"(?P=mapping_quote)"
        ),
    ),
    (
        "PASSWORD_LITERAL",
        re.compile(
            r"(?ix)(?<![a-z0-9_])\$?"
            r"(?:(?:db|database|mysql|mariadb|postgres|remote|ssh|server)_?)?pass"
            r"\b\s*(?::=|=|:)\s*(?:[rubf]{0,2})?"
            r"(?P<pass_quote>['\"])(?!\s*pass\s*(?P=pass_quote))"
            r"(?:(?!(?P=pass_quote)).)+(?P=pass_quote)"
        ),
    ),
    (
        "PASSWORD_CLI_ARGUMENT",
        re.compile(
            r"(?ix)(?:\bsshpass\s+-p|\bplink(?:\.exe)?\b[^\r\n]*\s-pw|"
            r"\bmysql(?:\.exe)?\b[^\r\n]*\s-p(?!\s|$)|--pass"
            r"word=)"
        ),
    ),
    (
        "REMOTE_IDENTITY_LITERAL",
        re.compile(
            r"(?ix)(?:\broot@|(?<![a-z0-9_])['\"]?"
            r"(?:remote_ssh_host|remote_host|ssh_host|server)['\"]?"
            r"\s*(?::=|=|:)\s*['\"]?)"
            r"(?!127\.|0\.0\.0\.0)(?:[0-9]{1,3}\.){3}[0-9]{1,3}"
        ),
    ),
)

_SAFE_PASSWORD_UI_LINE = re.compile(
    r"(?ix)(?:"
    r"data-toggle-password\s*=\s*['\"]"
    r"(?:password|confirmPassword|login-password|register-password)['\"]"
    r"|\bPASS(?:ED)?\s*:\s*['\"](?:通过|\#[0-9a-f]{3,8})['\"]"
    r"|\.type\s*=[^;\r\n]{0,80}['\"]password['\"]"
    r")"
)


def _is_excluded(relative_path: str) -> bool:
    normalized = relative_path.replace("\\", "/")
    return normalized in EXCLUDED_PATHS


def _is_source(relative_path: str) -> bool:
    path = PurePosixPath(relative_path.replace("\\", "/"))
    return path.suffix.lower() in SOURCE_SUFFIXES


def scan_text(relative_path: str, text: str) -> list[Finding]:
    """Scan source text without retaining or returning matched values."""
    if _is_excluded(relative_path) or not _is_source(relative_path):
        return []
    findings: list[Finding] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        for kind, pattern in _PATTERNS:
            if pattern.search(line):
                if kind == "PASSWORD_LITERAL" and _SAFE_PASSWORD_UI_LINE.search(line):
                    continue
                findings.append(Finding(relative_path, line_number, kind))
    return findings


def tracked_paths(root: Path) -> list[str]:
    completed = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return sorted(
        item.decode("utf-8", errors="surrogateescape")
        for item in completed.stdout.split(b"\0")
        if item
    )


def scan_paths(root: Path, relative_paths: Iterable[str]) -> list[Finding]:
    findings: list[Finding] = []
    for relative_path in relative_paths:
        if _is_excluded(relative_path) or not _is_source(relative_path):
            continue
        path = root / relative_path
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            # A tracked path that cannot be read must fail closed without
            # disclosing filesystem details beyond its repository path.
            findings.append(Finding(relative_path, 0, "UNREADABLE_SOURCE"))
            continue
        findings.extend(scan_text(relative_path, text))
    return sorted(findings, key=lambda item: (item.path, item.line, item.kind))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Git worktree to scan (default: this repository).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = args.root.resolve()
    try:
        findings = scan_paths(root, tracked_paths(root))
    except (OSError, subprocess.CalledProcessError):
        print("SECRET_SCAN_ERROR", file=sys.stderr)
        return 2
    for finding in findings:
        print(f"{finding.path}:{finding.line}:{finding.kind}")
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
