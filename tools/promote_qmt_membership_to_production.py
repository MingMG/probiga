#!/usr/bin/env python3
"""Promote immutable QMT concept/industry snapshots to production.

The QMT workstation owns the factual close snapshot.  The public application
uses its own MySQL instance, so a decision must not silently fall back to
current industry data merely because the immutable snapshot was never copied.
This tool exports a hash-verified bundle, transfers it over SSH and imports it
transactionally into production.
"""
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
from pathlib import Path
from typing import Any

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.membership_snapshot import (
    ensure_membership_snapshot_tables,
)
from tools.env_config import create_tool_engine, load_project_env
from tools.remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_pythonpath,
    remote_root,
)


SCHEMA = "probiga.qmt-membership-promotion.v1"


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _content_hash(payload: dict[str, Any]) -> str:
    content = {
        key: value
        for key, value in payload.items()
        if key != "content_sha256"
    }
    return hashlib.sha256(_canonical(content).encode("utf-8")).hexdigest()


def _membership_hash(
    rows: list[dict[str, Any]],
    columns: tuple[str, ...],
) -> str:
    values = [
        tuple(str(row.get(column) or "") for column in columns)
        for row in rows
    ]
    payload = json.dumps(
        sorted(values),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: (
            value.isoformat(sep=" ")
            if isinstance(value, datetime)
            else value.isoformat()
            if isinstance(value, date)
            else value
        )
        for key, value in row.items()
    }


def export_bundle(
    path: Path,
    *,
    snapshot_date: date | None = None,
) -> dict[str, Any]:
    engine = create_tool_engine()
    try:
        with engine.connect() as connection:
            where = (
                "AND snapshot_date = :snapshot_date"
                if snapshot_date is not None
                else ""
            )
            params = (
                {"snapshot_date": snapshot_date}
                if snapshot_date is not None
                else {}
            )
            runs = [
                _serialize_row(dict(row))
                for row in connection.execute(
                    text(
                        f"""
                        SELECT snapshot_date, source, quality_status,
                               capture_mode, concept_count,
                               concept_relation_count, industry_count,
                               industry_relation_count, concept_hash,
                               industry_hash, captured_at
                        FROM qmt_membership_snapshot_run
                        WHERE quality_status = 'QMT_VALIDATED'
                          {where}
                        ORDER BY snapshot_date, source
                        """
                    ),
                    params,
                ).mappings()
            ]
            snapshots = []
            for run in runs:
                row_params = {
                    "snapshot_date": run["snapshot_date"],
                    "source": run["source"],
                }
                concepts = [
                    _serialize_row(dict(row))
                    for row in connection.execute(
                        text(
                            """
                            SELECT snapshot_date, source, concept_code,
                                   concept_name, stock_code, short_name,
                                   quality_status, captured_at
                            FROM qmt_concept_member_snapshot
                            WHERE snapshot_date = :snapshot_date
                              AND source = :source
                            ORDER BY concept_code, stock_code
                            """
                        ),
                        row_params,
                    ).mappings()
                ]
                industries = [
                    _serialize_row(dict(row))
                    for row in connection.execute(
                        text(
                            """
                            SELECT snapshot_date, source, industry_code,
                                   industry_name, industry_type,
                                   stock_code, short_name, quality_status,
                                   captured_at
                            FROM qmt_industry_member_snapshot
                            WHERE snapshot_date = :snapshot_date
                              AND source = :source
                            ORDER BY industry_code, stock_code
                            """
                        ),
                        row_params,
                    ).mappings()
                ]
                snapshots.append({
                    "run": run,
                    "concepts": concepts,
                    "industries": industries,
                })
    finally:
        engine.dispose()
    if not snapshots:
        raise RuntimeError("no QMT_VALIDATED membership snapshot found")
    payload = {
        "schema": SCHEMA,
        "exported_at": datetime.now().replace(microsecond=0).isoformat(
            sep=" "
        ),
        "snapshots": snapshots,
    }
    payload["content_sha256"] = _content_hash(payload)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump(
            payload,
            handle,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    return {
        "snapshot_count": len(snapshots),
        "concept_relation_count": sum(
            len(item["concepts"]) for item in snapshots
        ),
        "industry_relation_count": sum(
            len(item["industries"]) for item in snapshots
        ),
        "content_sha256": payload["content_sha256"],
    }


def _validate_snapshot(item: dict[str, Any]) -> None:
    run = dict(item.get("run") or {})
    concepts = list(item.get("concepts") or [])
    industries = list(item.get("industries") or [])
    if str(run.get("quality_status") or "") != "QMT_VALIDATED":
        raise ValueError("membership snapshot is not QMT validated")
    if date.fromisoformat(str(run["snapshot_date"])) > date.today():
        raise ValueError("future membership snapshot is prohibited")
    if len(concepts) != int(run["concept_relation_count"]):
        raise ValueError("concept relation count mismatch")
    if len(industries) != int(run["industry_relation_count"]):
        raise ValueError("industry relation count mismatch")
    concept_hash = _membership_hash(
        concepts,
        ("concept_code", "concept_name", "stock_code", "short_name"),
    )
    industry_hash = _membership_hash(
        industries,
        (
            "industry_code",
            "industry_name",
            "industry_type",
            "stock_code",
            "short_name",
        ),
    )
    if concept_hash != str(run.get("concept_hash") or ""):
        raise ValueError("concept membership hash mismatch")
    if industry_hash != str(run.get("industry_hash") or ""):
        raise ValueError("industry membership hash mismatch")


def _chunks(rows: list[dict[str, Any]], size: int = 2000):
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def import_bundle(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        payload = json.load(handle)
    if payload.get("schema") != SCHEMA:
        raise ValueError("membership promotion schema is invalid")
    if payload.get("content_sha256") != _content_hash(payload):
        raise ValueError("membership promotion content hash mismatch")
    snapshots = list(payload.get("snapshots") or [])
    if not snapshots:
        raise ValueError("membership promotion bundle is empty")
    for item in snapshots:
        _validate_snapshot(item)

    engine = create_tool_engine()
    try:
        ensure_membership_snapshot_tables(engine)
        imported = []
        for item in snapshots:
            run = dict(item["run"])
            concepts = list(item["concepts"])
            industries = list(item["industries"])
            key = {
                "snapshot_date": run["snapshot_date"],
                "source": run["source"],
            }
            with engine.begin() as connection:
                existing = connection.execute(
                    text(
                        """
                        SELECT concept_hash, industry_hash
                        FROM qmt_membership_snapshot_run
                        WHERE snapshot_date = :snapshot_date
                          AND source = :source
                        FOR UPDATE
                        """
                    ),
                    key,
                ).mappings().first()
                if existing:
                    if (
                        str(existing["concept_hash"]) != run["concept_hash"]
                        or str(existing["industry_hash"])
                        != run["industry_hash"]
                    ):
                        raise RuntimeError(
                            "immutable production membership collision: "
                            f"{run['snapshot_date']}/{run['source']}"
                        )
                    status = "idempotent"
                else:
                    concept_sql = text(
                        """
                        INSERT INTO qmt_concept_member_snapshot
                        (snapshot_date, source, concept_code, concept_name,
                         stock_code, short_name, quality_status, captured_at)
                        VALUES
                        (:snapshot_date, :source, :concept_code,
                         :concept_name, :stock_code, :short_name,
                         :quality_status, :captured_at)
                        """
                    )
                    for batch in _chunks(concepts):
                        connection.execute(concept_sql, batch)
                    industry_sql = text(
                        """
                        INSERT INTO qmt_industry_member_snapshot
                        (snapshot_date, source, industry_code, industry_name,
                         industry_type, stock_code, short_name,
                         quality_status, captured_at)
                        VALUES
                        (:snapshot_date, :source, :industry_code,
                         :industry_name, :industry_type, :stock_code,
                         :short_name, :quality_status, :captured_at)
                        """
                    )
                    for batch in _chunks(industries):
                        connection.execute(industry_sql, batch)
                    connection.execute(
                        text(
                            """
                            INSERT INTO qmt_membership_snapshot_run
                            (snapshot_date, source, quality_status,
                             capture_mode, concept_count,
                             concept_relation_count, industry_count,
                             industry_relation_count, concept_hash,
                             industry_hash, captured_at)
                            VALUES
                            (:snapshot_date, :source, :quality_status,
                             :capture_mode, :concept_count,
                             :concept_relation_count, :industry_count,
                             :industry_relation_count, :concept_hash,
                             :industry_hash, :captured_at)
                            """
                        ),
                        run,
                    )
                    status = "imported"
            imported.append({
                "snapshot_date": run["snapshot_date"],
                "source": run["source"],
                "status": status,
                "concept_relations": len(concepts),
                "industry_relations": len(industries),
            })
    finally:
        engine.dispose()
    return {
        "status": "ok",
        "snapshot_count": len(imported),
        "snapshots": imported,
        "content_sha256": payload["content_sha256"],
    }


def promote_to_production(
    *,
    snapshot_date: date | None = None,
) -> dict[str, Any]:
    root = remote_root()
    pythonpath = remote_pythonpath(root)
    import paramiko

    descriptor, raw_path = tempfile.mkstemp(
        prefix="probiga_qmt_membership_",
        suffix=".json.gz",
    )
    os.close(descriptor)
    local_path = Path(raw_path)
    remote_path = posixpath.join(
        "/tmp",
        f"probiga_qmt_membership_{uuid.uuid4().hex}.json.gz",
    )
    client = production_ssh_client(paramiko)
    try:
        exported = export_bundle(
            local_path,
            snapshot_date=snapshot_date,
        )
        client.connect(**production_ssh_connect_kwargs(timeout=30))
        sftp = client.open_sftp()
        try:
            sftp.put(str(local_path), remote_path)
        finally:
            sftp.close()
        command = (
            f"cd {shlex.quote(root)} && "
            f"PYTHONPATH={shlex.quote(pythonpath)} "
            f"{shlex.quote(posixpath.join(root, 'venv/bin/python'))} "
            "tools/promote_qmt_membership_to_production.py "
            f"--import-bundle {shlex.quote(remote_path)}"
        )
        _stdin, stdout, stderr = client.exec_command(command, timeout=300)
        out = stdout.read().decode("utf-8", errors="replace").strip()
        err = stderr.read().decode("utf-8", errors="replace").strip()
        status = stdout.channel.recv_exit_status()
        if status != 0:
            raise RuntimeError(
                f"production membership import failed ({status}): "
                f"{err[-4000:]}"
            )
        result = json.loads(out.splitlines()[-1])
        result["export"] = exported
        return result
    finally:
        if client.get_transport() is not None:
            try:
                sftp = client.open_sftp()
                try:
                    sftp.remove(remote_path)
                except FileNotFoundError:
                    pass
                finally:
                    sftp.close()
            except Exception as exc:
                print(
                    f"warning: failed to remove remote temporary file: {exc}",
                    file=sys.stderr,
                )
        client.close()
        local_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", default="")
    parser.add_argument("--import-bundle", default="")
    args = parser.parse_args()
    load_project_env()
    if args.import_bundle:
        result = import_bundle(Path(args.import_bundle))
    else:
        result = promote_to_production(
            snapshot_date=(
                date.fromisoformat(args.snapshot_date)
                if args.snapshot_date
                else None
            )
        )
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
