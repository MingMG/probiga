"""Durable validation receipts for immutable issuer-event revision prefixes."""
from __future__ import annotations

from contextlib import closing
import logging
import os
from pathlib import Path
import sqlite3
import stat


logger = logging.getLogger(__name__)
MAX_CHECKPOINT_BYTES = 16 * 1024 * 1024


class EventCoverageCheckpoint:
    # Increment when the rules for accepting a coverage prefix change.
    VERSION = "probiga.event-coverage-prefix.v1"

    def __init__(self, path: Path, namespace: str) -> None:
        self.path = path
        self.namespace = namespace
        self.available = True
        try:
            self._prepare_private_path()
            with closing(self._connect()) as connection, connection:
                connection.execute("""
                    CREATE TABLE IF NOT EXISTS verified_event_prefix (
                        namespace TEXT NOT NULL,
                        scope_hash TEXT NOT NULL,
                        revision_count INTEGER NOT NULL,
                        inventory_sha256 TEXT NOT NULL,
                        PRIMARY KEY (namespace, scope_hash)
                    )
                """)
        except (OSError, sqlite3.Error) as exc:
            self._disable(exc)

    def _prepare_private_path(self) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent = self.path.parent.lstat()
        if not stat.S_ISDIR(parent.st_mode) or (
            os.name == "posix" and (
                (parent.st_uid, parent.st_gid) != (os.geteuid(), os.getegid())
                or stat.S_IMODE(parent.st_mode) != 0o700
            )
        ):
            raise OSError("coverage checkpoint directory is not private")
        if self.path.is_symlink():
            raise OSError("coverage checkpoint is a symbolic link")
        journal = self.path.with_name(self.path.name + "-journal")
        try:
            self._require_private_file(journal.lstat())
        except FileNotFoundError:
            pass
        with closing(os.fdopen(os.open(
            self.path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
            0o600,
        ), "r+b")) as handle:
            self._require_private_file(os.fstat(handle.fileno()))

    @staticmethod
    def _require_private_file(info: os.stat_result) -> None:
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or (
            os.name == "posix" and (
                (info.st_uid, info.st_gid) != (os.geteuid(), os.getegid())
                or stat.S_IMODE(info.st_mode) != 0o600
            )
        ) or info.st_size > MAX_CHECKPOINT_BYTES:
            raise OSError("coverage checkpoint file identity differs")

    def _disable(self, error: Exception) -> None:
        self.available = False
        logger.warning("Event coverage checkpoint unavailable (%s); verifying the full input", type(error).__name__)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=2)
        try:
            if connection.execute("PRAGMA journal_mode").fetchone()[0] != "delete":
                raise OSError("coverage checkpoint journal mode differs")
            page_size = connection.execute("PRAGMA page_size").fetchone()[0]
            limit = MAX_CHECKPOINT_BYTES // page_size
            if connection.execute(f"PRAGMA max_page_count={limit}").fetchone()[0] > limit:
                raise OSError("coverage checkpoint exceeds its byte limit")
            return connection
        except (OSError, sqlite3.Error):
            connection.close()
            raise

    def load(self, scopes: list[str]) -> dict[str, tuple[int, str]]:
        if not self.available or not scopes:
            return {}
        placeholders = ",".join("?" for _ in scopes)
        try:
            with closing(self._connect()) as connection:
                return {
                    scope: (count, digest)
                    for scope, count, digest in connection.execute(
                        "SELECT scope_hash,revision_count,inventory_sha256 "
                        "FROM verified_event_prefix WHERE namespace=? "
                        f"AND scope_hash IN ({placeholders})",
                        (self.namespace, *scopes),
                    )
                }
        except (OSError, sqlite3.Error) as exc:
            self._disable(exc)
            return {}

    def save(self, verified: list[tuple[str, int, str]]) -> None:
        if not self.available or not verified:
            return
        try:
            with closing(self._connect()) as connection, connection:
                connection.executemany("""
                    INSERT INTO verified_event_prefix
                        (namespace,scope_hash,revision_count,inventory_sha256)
                    VALUES (?,?,?,?)
                    ON CONFLICT(namespace,scope_hash) DO UPDATE SET
                        revision_count=excluded.revision_count,
                        inventory_sha256=excluded.inventory_sha256
                    WHERE excluded.revision_count>=verified_event_prefix.revision_count
                """, ((self.namespace, *row) for row in verified))
        except (OSError, sqlite3.Error) as exc:
            self._disable(exc)
