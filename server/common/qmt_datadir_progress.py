"""Observable timing for the existing staged QMT minute publication path."""
from __future__ import annotations

from datetime import datetime
import json
import logging
import os
import re
import time
import uuid

from server.common.authoritative_market_clock import PRODUCTION_TIMEZONE
from server.common.trading_day_store import journal_runtime_root


class MinuteImportProgress:
    def __init__(self, *, run_id: str, total_units: int, operation: str, days,
                 dataset: str = "stock_minute", checkpoint: dict | None = None):
        self.run_id = str(run_id)
        self.total = int(total_units)
        self.operation = operation
        self.dataset = dataset
        self.checkpoint = checkpoint if checkpoint is not None else dict(table="qmt_datadir_minute_stage", run_id=self.run_id)
        self.days = [str(day) for day in days]
        self.operation_uid = uuid.uuid4().hex
        self.started = time.monotonic()
        self.completed = 0
        self.last_progress_at = None if dataset == "qmt_announcement_pit" else self._now()
        self.path = None
        datadir = dataset == "stock_minute" and re.fullmatch(r"qdd_[0-9]{14}_[0-9a-f]{8}", self.run_id)
        announcement = dataset == "qmt_announcement_pit" and re.fullmatch(r"[0-9a-f]{32}", self.run_id)
        if datadir or announcement:
            try:
                root = journal_runtime_root() / "qmt-datadir-progress"
                root.mkdir(parents=True, exist_ok=True)
                prefix = "qmt-announcement-" if announcement else ""
                self.path = root / (prefix + self.run_id + ".jsonl")
            except (OSError, RuntimeError) as exc:
                logging.getLogger(__name__).warning("Minute progress log unavailable: %s", exc)

    @staticmethod
    def _now():
        return datetime.now(PRODUCTION_TIMEZONE).isoformat(timespec="milliseconds")

    def emit(self, phase: str, *, completed_units=None, current_unit="", status="RUNNING",
             console=False, **metrics):
        if completed_units is not None and completed_units > self.completed:
            self.completed = int(completed_units)
            self.last_progress_at = self._now()
        event = dict(schema="probiga.qmt-datadir-progress.v1", dataset=self.dataset,
            run_id=self.run_id, operation=self.operation, operation_uid=self.operation_uid,
            phase=phase, status=status, trade_dates=self.days, total_units=self.total,
            completed_units=self.completed, current_unit=str(current_unit),
            last_progress_at=self.last_progress_at, observed_at=self._now(),
            elapsed_seconds=round(time.monotonic() - self.started, 6),
            checkpoint=self.checkpoint, **metrics)
        line = json.dumps(event, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        if self.path is not None:
            try:
                with self.path.open("a", encoding="utf-8", newline="\n") as stream:
                    stream.write(line + "\n")
                    stream.flush()
                    os.fsync(stream.fileno())
            except OSError as exc:
                logging.getLogger(__name__).warning("Minute progress log write failed: %s", exc)
                self.path = None
        if console:
            try:
                print(line, flush=True)
            except (OSError, ValueError) as exc:
                logging.getLogger(__name__).warning("Minute progress console unavailable: %s", exc)
        return event
