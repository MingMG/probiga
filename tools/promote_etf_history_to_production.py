#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Promote validated local Guojin-QMT ETF history into production."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import posixpath
import shlex
import sys
import tempfile
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.batch_db import create_batch_engine
from tools.env_config import load_project_env
from tools.remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_pythonpath,
    remote_root,
)
from tools.sync_etf_history import ETF_KLINE_UPSERT, ETF_META_UPSERT


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def _line(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=_json_default,
        )
        + "\n"
    ).encode("utf-8")


def export_bundle(path: Path) -> dict[str, Any]:
    engine = create_batch_engine()
    digest = hashlib.sha256()
    meta_count = 0
    kline_count = 0
    with gzip.open(path, "wb", compresslevel=6) as handle:
        header = _line(
            {
                "kind": "header",
                "schema": "probiga.etf-qmt-promotion.v1",
                "source_provider": "gj_big_qmt_inner",
                "generated_at": datetime.now(),
            }
        )
        handle.write(header)
        digest.update(header)
        with engine.connect() as connection:
            meta_rows = connection.execute(
                text("SELECT * FROM si_etf_code ORDER BY etf_code")
            ).mappings()
            for row in meta_rows:
                raw = _line({"kind": "meta", "row": dict(row)})
                handle.write(raw)
                digest.update(raw)
                meta_count += 1
            result = connection.execution_options(
                stream_results=True
            ).execute(
                text(
                    """
                    SELECT etf_code, short_name, trade_time, trade_date,
                           k_type, adjust_type, `open`, `close`, high, low,
                           volume, amount, pre_close, `change`, change_pct,
                           data_source, validation_source, validation_status,
                           validation_price_max_delta,
                           validation_volume_delta_pct,
                           validation_checked_at, received_at, batch_id,
                           data_version, quality_status, permission_status
                    FROM sm_etf_kline
                    WHERE data_source = 'gj_big_qmt_inner'
                      AND validation_status = 'passed'
                      AND quality_status = 'validated'
                    ORDER BY etf_code, trade_date, adjust_type
                    """
                )
            ).mappings()
            for row in result:
                raw = _line({"kind": "kline", "row": dict(row)})
                handle.write(raw)
                digest.update(raw)
                kline_count += 1
        footer = _line(
            {
                "kind": "footer",
                "meta_count": meta_count,
                "kline_count": kline_count,
                "content_sha256": digest.hexdigest(),
            }
        )
        handle.write(footer)
    return {
        "status": "ok",
        "path": str(path),
        "meta_count": meta_count,
        "kline_count": kline_count,
        "content_sha256": digest.hexdigest(),
    }


def _chunks(rows: list[dict[str, Any]], size: int = 500) -> Iterable[list[dict[str, Any]]]:
    for offset in range(0, len(rows), size):
        yield rows[offset : offset + size]


def import_bundle(path: Path) -> dict[str, Any]:
    engine = create_batch_engine()
    digest = hashlib.sha256()
    meta_rows: list[dict[str, Any]] = []
    kline_batch: list[dict[str, Any]] = []
    imported_meta = 0
    imported_kline = 0
    footer: dict[str, Any] | None = None
    with engine.begin() as connection:
        existing = {
            str(row[0])
            for row in connection.execute(
                text(
                    """
                    SELECT TABLE_NAME FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME IN ('si_etf_code','sm_etf_kline')
                    """
                )
            ).fetchall()
        }
        if existing != {"si_etf_code", "sm_etf_kline"}:
            raise RuntimeError("ETF schema migration is incomplete")
        with gzip.open(path, "rb") as handle:
            for raw in handle:
                payload = json.loads(raw.decode("utf-8"))
                kind = payload.get("kind")
                if kind == "footer":
                    footer = payload
                    break
                digest.update(raw)
                if kind == "header":
                    if (
                        payload.get("schema")
                        != "probiga.etf-qmt-promotion.v1"
                        or payload.get("source_provider")
                        != "gj_big_qmt_inner"
                    ):
                        raise RuntimeError("ETF bundle header is invalid")
                elif kind == "meta":
                    meta_rows.append(payload["row"])
                elif kind == "kline":
                    row = payload["row"]
                    if (
                        row.get("data_source") != "gj_big_qmt_inner"
                        or row.get("validation_status") != "passed"
                        or row.get("quality_status") != "validated"
                    ):
                        raise RuntimeError(
                            "ETF bundle contains an unvalidated row"
                        )
                    kline_batch.append(row)
                    if len(kline_batch) >= 500:
                        connection.execute(
                            text(ETF_KLINE_UPSERT),
                            kline_batch,
                        )
                        imported_kline += len(kline_batch)
                        kline_batch = []
                else:
                    raise RuntimeError(f"unknown ETF bundle row: {kind}")
        if footer is None:
            raise RuntimeError("ETF bundle footer is missing")
        if footer.get("content_sha256") != digest.hexdigest():
            raise RuntimeError("ETF bundle content hash mismatch")
        if int(footer.get("meta_count") or -1) != len(meta_rows):
            raise RuntimeError("ETF bundle metadata count mismatch")
        if kline_batch:
            connection.execute(text(ETF_KLINE_UPSERT), kline_batch)
            imported_kline += len(kline_batch)
        if int(footer.get("kline_count") or -1) != imported_kline:
            raise RuntimeError("ETF bundle K-line count mismatch")
        for batch in _chunks(meta_rows, 100):
            connection.execute(text(ETF_META_UPSERT), batch)
            imported_meta += len(batch)
    with engine.connect() as connection:
        target = connection.execute(
            text(
                """
                SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT etf_code) AS etf_count,
                       MIN(trade_date) AS min_date,
                       MAX(trade_date) AS max_date
                FROM sm_etf_kline
                WHERE data_source = 'gj_big_qmt_inner'
                  AND validation_status = 'passed'
                  AND quality_status = 'validated'
                """
            )
        ).mappings().first()
    return {
        "status": "ok",
        "meta_rows_imported": imported_meta,
        "kline_rows_imported": imported_kline,
        "content_sha256": digest.hexdigest(),
        "target": dict(target or {}),
    }


def promote_to_production() -> dict[str, Any]:
    root = remote_root()
    pythonpath = remote_pythonpath(root)
    load_project_env()
    descriptor, local_name = tempfile.mkstemp(
        prefix="probiga_etf_qmt_",
        suffix=".jsonl.gz",
    )
    os.close(descriptor)
    local_path = Path(local_name)
    remote_path = posixpath.join(
        "/tmp",
        f"probiga_etf_qmt_{uuid.uuid4().hex}.jsonl.gz",
    )
    export_result: dict[str, Any] = {}
    try:
        export_result = export_bundle(local_path)
        import paramiko

        client = production_ssh_client(paramiko)
        client.connect(**production_ssh_connect_kwargs())
        try:
            sftp = client.open_sftp()
            try:
                sftp.put(str(local_path), remote_path)
            finally:
                sftp.close()
            command = (
                f"env PYTHONPATH={shlex.quote(pythonpath)} "
                f"{shlex.quote(root + '/venv/bin/python')} "
                f"{shlex.quote(root + '/tools/promote_etf_history_to_production.py')} "
                f"--import-gzip {shlex.quote(remote_path)}"
            )
            _stdin, stdout, stderr = client.exec_command(
                command,
                timeout=1800,
            )
            output = stdout.read().decode("utf-8", errors="replace")
            error = stderr.read().decode("utf-8", errors="replace")
            status = stdout.channel.recv_exit_status()
            if status:
                raise RuntimeError(error[-4000:])
            import_result = json.loads(output)
        finally:
            client.exec_command(
                f"rm -f -- {shlex.quote(remote_path)}",
                timeout=30,
            )
            client.close()
        return {
            "status": "ok",
            "export": export_result,
            "production_import": import_result,
        }
    finally:
        local_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--promote-production", action="store_true")
    group.add_argument("--export-gzip")
    group.add_argument("--import-gzip")
    args = parser.parse_args()
    load_project_env()
    if args.promote_production:
        result = promote_to_production()
    elif args.export_gzip:
        result = export_bundle(Path(args.export_gzip))
    else:
        result = import_bundle(Path(args.import_gzip))
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
