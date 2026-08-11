#!/usr/bin/env python3
"""Stream-sanitize a MySQL 5.5 logical dump for import into MySQL 8.4.

MySQL 8.0 removed the ``NO_AUTO_CREATE_USER`` SQL mode.  MySQL 5.5's
``mysqldump`` records the source session SQL mode around triggers, so an
otherwise valid logical dump can fail while MySQL 8.4 restores those
statements.  This tool removes only that single mode token from complete,
single-line ``SET sql_mode = '...'`` statements.  It never performs a global
text replacement and never edits the source file in place.

The implementation is binary and line-oriented so multi-hundred-gigabyte
dumps are processed with bounded memory while retaining all unrelated bytes.
Large multi-row ``INSERT`` statements are split into smaller statements by
default.  This avoids a MySQL 8.4.11 Windows parser crash seen with the
multi-hundred-kilobyte ``INSERT`` batches emitted by this legacy server,
without changing any tuple payload.  The conservative 64 KiB limit is
intentional: a prior 256 KiB limit still allowed a 257,677-byte parser input
to reach the server and crash it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import BinaryIO, Iterable


REMOVED_SQL_MODE = b"NO_AUTO_CREATE_USER"
# Keep the parser input well below the 256 KiB failure boundary observed on
# Oracle MySQL 8.4.11 for Windows. This is a statement-size limit, not a row
# limit: tuples are never split and their bytes are copied unchanged.
DEFAULT_MAX_INSERT_BYTES = 64 * 1024

# mysqldump 5.5 emits the first form around triggers.  The plain form is
# accepted as well because hand-wrapped dumps sometimes strip versioned
# comments.  Both patterns are anchored to the complete physical line: an
# INSERT value containing the same text can never match.
_VERSIONED_SET_SQL_MODE = re.compile(
    rb"^(?P<prefix>[ \t]*/\*![0-9]{5,6}[ \t]+SET[ \t]+sql_mode[ \t]*=[ \t]*')"
    rb"(?P<modes>[^'\r\n]*)"
    rb"(?P<suffix>'[ \t]*\*/[ \t]*;[ \t]*(?:\r\n|\n|\r)?)$",
    re.IGNORECASE,
)
_PLAIN_SET_SQL_MODE = re.compile(
    rb"^(?P<prefix>[ \t]*SET[ \t]+(?:SESSION[ \t]+)?sql_mode[ \t]*=[ \t]*')"
    rb"(?P<modes>[^'\r\n]*)"
    rb"(?P<suffix>'[ \t]*;[ \t]*(?:\r\n|\n|\r)?)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SanitizeReport:
    source: str
    output: str | None
    source_bytes: int
    output_bytes: int | None
    lines_scanned: int
    changed_statements: int
    removed_tokens: int
    split_insert_statements: int
    split_insert_chunks: int
    source_sha256: str
    output_sha256: str | None


def _remove_obsolete_mode(modes: bytes) -> tuple[bytes, int]:
    """Remove exact obsolete mode tokens, preserving all other mode names."""

    kept: list[bytes] = []
    removed = 0
    for raw_token in modes.split(b","):
        token = raw_token.strip()
        if token.upper() == REMOVED_SQL_MODE:
            removed += 1
        else:
            kept.append(token)
    return b",".join(kept), removed


def sanitize_dump_line(line: bytes) -> tuple[bytes, int]:
    """Return a sanitized line and the number of removed mode tokens."""

    for pattern in (_VERSIONED_SET_SQL_MODE, _PLAIN_SET_SQL_MODE):
        match = pattern.fullmatch(line)
        if match is None:
            continue
        modes, removed = _remove_obsolete_mode(match.group("modes"))
        if not removed:
            return line, 0
        return match.group("prefix") + modes + match.group("suffix"), removed
    return line, 0


def _split_insert_tuples(body: bytes) -> list[bytes] | None:
    """Return top-level value tuples from an INSERT body, or ``None``.

    The scanner understands MySQL's quoted strings/identifiers and backslash
    escapes.  It deliberately only splits at commas outside parentheses; the
    original tuple bytes are otherwise copied verbatim.
    """

    # mysqldump writes ordinary multi-row values as ``),(`` with no
    # whitespace.  ``bytes.split`` runs in C and is several orders of
    # magnitude faster than inspecting every byte in Python for a 100+ GiB
    # dump.  The fallback scanner below handles unusual whitespace safely.
    fast_parts = body.split(b"),(" )
    if len(fast_parts) >= 2 and fast_parts[0].startswith(b"(") and fast_parts[-1].endswith(b")"):
        fast_tuples = [fast_parts[0] + b")"]
        fast_tuples.extend(b"(" + part + b")" for part in fast_parts[1:-1])
        fast_tuples.append(b"(" + fast_parts[-1])
        # A literal ``),(`` inside a quoted value would make the fast path
        # unsafe.  Tuple fragments from mysqldump have balanced single quotes;
        # an odd fragment therefore falls back to the quote-aware scanner.
        if all(
            item.startswith(b"(")
            and item.endswith(b")")
            and item.count(b"'") % 2 == 0
            for item in fast_tuples
        ):
            return fast_tuples

    tuples: list[bytes] = []
    start = 0
    depth = 0
    quote: int | None = None
    escaped = False
    for index, value in enumerate(body):
        if quote is not None:
            if escaped:
                escaped = False
            elif value == 0x5C:  # backslash
                escaped = True
            elif value == quote:
                quote = None
            continue
        if value in (0x27, 0x22, 0x60):  # ', ", `
            quote = value
        elif value == 0x28:  # (
            depth += 1
        elif value == 0x29:  # )
            depth -= 1
            if depth < 0:
                return None
        elif value == 0x2C and depth == 0:  # comma between tuples
            tuples.append(body[start:index])
            start = index + 1
    if quote is not None or depth != 0:
        return None
    tail = body[start:]
    if not tail:
        return None
    tuples.append(tail)
    if len(tuples) < 2 or any(not item.lstrip().startswith(b"(") for item in tuples):
        return None
    return tuples


def split_large_insert_line(line: bytes, max_bytes: int | None) -> tuple[list[bytes], int]:
    """Split one physical multi-row INSERT line when it exceeds *max_bytes*."""

    if max_bytes is None or len(line) <= max_bytes:
        return [line], 0
    newline = b""
    core = line
    if core.endswith(b"\r\n"):
        core, newline = core[:-2], b"\r\n"
    elif core.endswith((b"\n", b"\r")):
        core, newline = core[:-1], core[-1:]
    match = re.match(rb"^(?P<prefix>.*?\bVALUES\s+)(?P<body>.*?)(?P<suffix>;\s*)$", core, re.IGNORECASE)
    if match is None:
        return [line], 0
    tuples = _split_insert_tuples(match.group("body"))
    if tuples is None:
        return [line], 0
    prefix = match.group("prefix")
    suffix = match.group("suffix")
    pieces: list[bytes] = []
    current: list[bytes] = []
    current_size = len(prefix) + len(suffix) + len(newline)
    for item in tuples:
        item_size = len(item) + (1 if current else 0)
        if current and current_size + item_size > max_bytes:
            pieces.append(prefix + b",".join(current) + suffix + newline)
            current = []
            current_size = len(prefix) + len(suffix) + len(newline)
        current.append(item)
        current_size += item_size
    if current:
        pieces.append(prefix + b",".join(current) + suffix + newline)
    if len(pieces) <= 1:
        return [line], 0
    return pieces, len(pieces)


def _iter_binary_lines(stream: BinaryIO) -> Iterable[bytes]:
    while True:
        line = stream.readline()
        if not line:
            return
        yield line


def sanitize_dump(
    source: Path,
    output: Path | None,
    *,
    expected_changed_statements: int | None = None,
    split_insert_bytes: int | None = DEFAULT_MAX_INSERT_BYTES,
    overwrite: bool = False,
) -> SanitizeReport:
    """Scan or sanitize *source* and return a cryptographic audit report.

    If *output* is ``None``, the dump is checked without writing a copy.
    Otherwise a sibling partial file is written and atomically promoted only
    after the full stream and optional replacement-count assertion succeed.
    """

    source = source.expanduser().resolve(strict=True)
    output = output.expanduser().resolve() if output is not None else None
    if not source.is_file():
        raise ValueError(f"source is not a regular file: {source}")
    if output is not None and output == source:
        raise ValueError("source and output must be different files")
    if output is not None and output.exists() and not overwrite:
        raise FileExistsError(f"output already exists: {output}")

    source_hash = hashlib.sha256()
    output_hash = hashlib.sha256() if output is not None else None
    source_bytes = 0
    output_bytes = 0
    lines_scanned = 0
    changed_statements = 0
    removed_tokens = 0
    split_insert_statements = 0
    split_insert_chunks = 0
    partial: Path | None = None
    destination: BinaryIO | None = None

    try:
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            partial = output.with_name(f".{output.name}.partial-{os.getpid()}")
            if partial.exists():
                raise FileExistsError(f"partial output already exists: {partial}")
            destination = partial.open("xb")

        with source.open("rb") as source_stream:
            for line in _iter_binary_lines(source_stream):
                lines_scanned += 1
                source_bytes += len(line)
                source_hash.update(line)
                sanitized, removed = sanitize_dump_line(line)
                if removed:
                    changed_statements += 1
                    removed_tokens += removed
                if destination is not None:
                    pieces, chunks = split_large_insert_line(sanitized, split_insert_bytes)
                    if chunks:
                        split_insert_statements += 1
                        split_insert_chunks += chunks
                    for piece in pieces:
                        destination.write(piece)
                        output_bytes += len(piece)
                        assert output_hash is not None
                        output_hash.update(piece)
                else:
                    _, chunks = split_large_insert_line(sanitized, split_insert_bytes)
                    if chunks:
                        split_insert_statements += 1
                        split_insert_chunks += chunks

        if expected_changed_statements is not None and (
            changed_statements != expected_changed_statements
        ):
            raise ValueError(
                "changed statement count mismatch: "
                f"expected {expected_changed_statements}, got {changed_statements}"
            )

        if destination is not None:
            destination.flush()
            os.fsync(destination.fileno())
            destination.close()
            destination = None
            assert partial is not None and output is not None
            os.replace(partial, output)
            partial = None

        return SanitizeReport(
            source=str(source),
            output=str(output) if output is not None else None,
            source_bytes=source_bytes,
            output_bytes=output_bytes if output is not None else None,
            lines_scanned=lines_scanned,
            changed_statements=changed_statements,
            removed_tokens=removed_tokens,
            split_insert_statements=split_insert_statements,
            split_insert_chunks=split_insert_chunks,
            source_sha256=source_hash.hexdigest(),
            output_sha256=output_hash.hexdigest() if output_hash is not None else None,
        )
    finally:
        if destination is not None:
            destination.close()
        if partial is not None and partial.exists():
            partial.unlink()


def _write_manifest(path: Path, report: SanitizeReport, *, overwrite: bool) -> None:
    path = path.expanduser().resolve()
    if path.exists() and not overwrite:
        raise FileExistsError(f"manifest already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.partial-{os.getpid()}")
    if partial.exists():
        raise FileExistsError(f"partial manifest already exists: {partial}")
    try:
        with partial.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(asdict(report), stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, path)
    finally:
        if partial.exists():
            partial.unlink()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stream-remove obsolete NO_AUTO_CREATE_USER SQL-mode tokens from "
            "complete SET sql_mode statements in a MySQL 5.5 dump."
        )
    )
    parser.add_argument("source", type=Path, help="Source MySQL 5.5 dump (never modified).")
    parser.add_argument(
        "output",
        type=Path,
        nargs="?",
        help="Sanitized output. Omit with --check-only.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Scan and report without writing an output dump.",
    )
    parser.add_argument(
        "--expect-changed-statements",
        type=int,
        help="Fail unless exactly this many SET statements require sanitizing.",
    )
    parser.add_argument(
        "--split-insert-bytes",
        type=int,
        default=DEFAULT_MAX_INSERT_BYTES,
        help=f"Maximum physical INSERT size before tuple splitting (default: {DEFAULT_MAX_INSERT_BYTES}).",
    )
    parser.add_argument("--manifest", type=Path, help="Optional JSON audit report path.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing output/manifest; the source is still never overwritten.",
    )
    args = parser.parse_args(argv)
    if args.check_only and args.output is not None:
        parser.error("output must be omitted with --check-only")
    if not args.check_only and args.output is None:
        parser.error("output is required unless --check-only is used")
    if args.expect_changed_statements is not None and args.expect_changed_statements < 0:
        parser.error("--expect-changed-statements must be non-negative")
    if args.split_insert_bytes is not None and args.split_insert_bytes < 1024:
        parser.error("--split-insert-bytes must be at least 1024 or omitted")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = sanitize_dump(
            args.source,
            None if args.check_only else args.output,
            expected_changed_statements=args.expect_changed_statements,
            split_insert_bytes=args.split_insert_bytes,
            overwrite=args.overwrite,
        )
        if args.manifest is not None:
            _write_manifest(args.manifest, report, overwrite=args.overwrite)
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(asdict(report), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
