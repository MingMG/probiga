#!/usr/bin/env python3
"""Promote immutable ETF forward observations from QMT to production."""
from __future__ import annotations

import argparse
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
from typing import Any

from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env
from server.common.scheduler_tasks import update_scheduler_tasks
from tools.remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_pythonpath,
    remote_root,
)


SCHEMA = "probiga.etf-forward-promotion.v1"


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    )


def _content_hash(payload: dict[str, Any]) -> str:
    content = {
        key: value
        for key, value in payload.items()
        if key != "content_sha256"
    }
    return hashlib.sha256(_canonical(content).encode("utf-8")).hexdigest()


def _parse_json_object(raw: Any, *, field: str) -> dict[str, Any]:
    value = json.loads(str(raw or "{}"))
    if not isinstance(value, dict):
        raise ValueError(f"{field} must contain one JSON object")
    return value


def validate_bundle(payload: dict[str, Any]) -> None:
    if payload.get("schema") != SCHEMA:
        raise ValueError("ETF forward bundle schema is invalid")
    if payload.get("content_sha256") != _content_hash(payload):
        raise ValueError("ETF forward bundle hash mismatch")
    strategies = payload.get("strategies")
    observations = payload.get("observations")
    if not isinstance(strategies, list) or not strategies:
        raise ValueError("ETF forward bundle has no strategy registry")
    if not isinstance(observations, list):
        raise ValueError("ETF forward observations must be a list")
    registry: dict[str, dict[str, Any]] = {}
    for row in strategies:
        if not isinstance(row, dict):
            raise ValueError("ETF forward strategy row is invalid")
        version = str(row.get("strategy_version") or "")
        config_hash = str(row.get("config_hash") or "")
        config = _parse_json_object(
            row.get("config_json"),
            field="config_json",
        )
        if not version or len(config_hash) != 64:
            raise ValueError("ETF forward strategy identity is invalid")
        if bool(
            config.get("forward_protocol", {}).get(
                "automatic_order_submission"
            )
        ):
            raise ValueError("ETF forward strategy may not submit orders")
        if config.get("forward_protocol", {}).get("backfill") != "prohibited":
            raise ValueError("ETF forward strategy must prohibit backfill")
        registry[version] = row
    for row in observations:
        if not isinstance(row, dict):
            raise ValueError("ETF forward observation row is invalid")
        version = str(row.get("strategy_version") or "")
        strategy = registry.get(version)
        if not strategy:
            raise ValueError("observation strategy is not registered")
        if str(row.get("config_hash") or "") != str(
            strategy["config_hash"]
        ):
            raise ValueError("observation config hash does not match")
        data_date = date.fromisoformat(str(row["data_date"]))
        forward_start = date.fromisoformat(
            str(strategy["forward_start_date"])
        )
        if data_date < forward_start:
            raise ValueError("retrospective ETF observation is prohibited")
        if data_date > date.today():
            raise ValueError("future ETF observation is prohibited")
        if str(row.get("data_source") or "") != "gj_big_qmt_inner":
            raise ValueError("ETF observation is not QMT validated")
        context = _parse_json_object(
            row.get("context_json"),
            field="context_json",
        )
        _parse_json_object(row.get("target_json"), field="target_json")
        if bool(context.get("automatic_order_submission")):
            raise ValueError("ETF observation may not submit orders")


def export_bundle(path: Path) -> dict[str, Any]:
    engine = create_tool_engine()
    try:
        with engine.connect() as connection:
            strategies = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT strategy_version, config_hash, frozen_at,
                               forward_start_date, mode, status,
                               config_json, registered_at
                        FROM st_etf_forward_strategy
                        ORDER BY strategy_version
                        """
                    )
                ).mappings()
            ]
            observations = [
                dict(row)
                for row in connection.execute(
                    text(
                        """
                        SELECT strategy_version, config_hash, data_date,
                               observed_at, data_source, input_hash,
                               signal_type, execution_date, target_json,
                               context_json, created_at
                        FROM st_etf_forward_observation
                        ORDER BY strategy_version, data_date
                        """
                    )
                ).mappings()
            ]
    finally:
        engine.dispose()
    payload: dict[str, Any] = {
        "schema": SCHEMA,
        "generated_at": datetime.now(),
        "source": "windows_qmt_host",
        "strategies": strategies,
        "observations": observations,
    }
    payload["content_sha256"] = _content_hash(payload)
    validate_bundle(
        json.loads(_canonical(payload))
    )
    path.write_text(_canonical(payload), encoding="utf-8")
    return {
        "status": "ok",
        "strategy_count": len(strategies),
        "observation_count": len(observations),
        "content_sha256": payload["content_sha256"],
    }


def _same_observation(
    existing: dict[str, Any],
    incoming: dict[str, Any],
) -> bool:
    keys = (
        "config_hash",
        "data_source",
        "input_hash",
        "signal_type",
        "execution_date",
        "target_json",
        "context_json",
    )
    for key in keys:
        left = existing.get(key)
        right = incoming.get(key)
        if key in {"target_json", "context_json"}:
            left = _canonical(_parse_json_object(left, field=key))
            right = _canonical(_parse_json_object(right, field=key))
        elif key == "execution_date":
            left = str(left or "")
            right = str(right or "")
        else:
            left = str(left or "")
            right = str(right or "")
        if left != right:
            return False
    return True


def import_bundle(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    validate_bundle(payload)
    engine = create_tool_engine()
    inserted_strategies = 0
    inserted_observations = 0
    existing_observations = 0
    latest_observed_at: datetime | None = None
    latest_data_date: date | None = None
    try:
        with engine.begin() as connection:
            for row in payload["strategies"]:
                existing = connection.execute(
                    text(
                        """
                        SELECT config_hash
                        FROM st_etf_forward_strategy
                        WHERE strategy_version = :strategy_version
                        """
                    ),
                    {"strategy_version": row["strategy_version"]},
                ).mappings().first()
                if existing:
                    if str(existing["config_hash"]) != str(
                        row["config_hash"]
                    ):
                        raise ValueError(
                            "production strategy version hash conflict"
                        )
                    continue
                connection.execute(
                    text(
                        """
                        INSERT INTO st_etf_forward_strategy (
                            strategy_version, config_hash, frozen_at,
                            forward_start_date, mode, status, config_json,
                            registered_at
                        ) VALUES (
                            :strategy_version, :config_hash, :frozen_at,
                            :forward_start_date, :mode, :status,
                            :config_json, :registered_at
                        )
                        """
                    ),
                    row,
                )
                inserted_strategies += 1

            for row in payload["observations"]:
                existing = connection.execute(
                    text(
                        """
                        SELECT config_hash, data_source, input_hash,
                               signal_type, execution_date, target_json,
                               context_json
                        FROM st_etf_forward_observation
                        WHERE strategy_version = :strategy_version
                          AND data_date = :data_date
                        """
                    ),
                    row,
                ).mappings().first()
                if existing:
                    if not _same_observation(dict(existing), row):
                        raise ValueError(
                            "production ETF observation hash conflict"
                        )
                    existing_observations += 1
                else:
                    connection.execute(
                        text(
                            """
                            INSERT INTO st_etf_forward_observation (
                                strategy_version, config_hash, data_date,
                                observed_at, data_source, input_hash,
                                signal_type, execution_date, target_json,
                                context_json, created_at
                            ) VALUES (
                                :strategy_version, :config_hash, :data_date,
                                :observed_at, :data_source, :input_hash,
                                :signal_type, :execution_date, :target_json,
                                :context_json, :created_at
                            )
                            """
                        ),
                        row,
                    )
                    inserted_observations += 1
                observed_at = datetime.fromisoformat(
                    str(row["observed_at"])
                )
                data_date = date.fromisoformat(str(row["data_date"]))
                latest_observed_at = max(
                    latest_observed_at or observed_at,
                    observed_at,
                )
                latest_data_date = max(
                    latest_data_date or data_date,
                    data_date,
                )

        if latest_observed_at is not None:
            scheduler_output = _canonical({
                "executor": "windows_qmt_host_promoted",
                "data_date": latest_data_date,
                "content_sha256": payload["content_sha256"],
                "observation_count": len(payload["observations"]),
            })
            update_scheduler_tasks(
                engine,
                {
                    "last_run_status": "success",
                    "last_run_at": latest_observed_at.replace(tzinfo=None),
                    "last_triggered_at": latest_observed_at.replace(
                        tzinfo=None
                    ),
                    "last_run_duration": 0,
                    "last_run_output": scheduler_output[-5000:],
                },
                lookup_where="task_type = :task_type",
                lookup_params={"task_type": "etf_forward_daily"},
            )
    finally:
        engine.dispose()
    return {
        "status": "ok",
        "inserted_strategies": inserted_strategies,
        "inserted_observations": inserted_observations,
        "existing_observations": existing_observations,
        "latest_data_date": latest_data_date,
        "content_sha256": payload["content_sha256"],
        "automatic_order_submission": False,
    }


def promote_to_production() -> dict[str, Any]:
    root = remote_root()
    pythonpath = remote_pythonpath(root)
    import paramiko

    descriptor, temporary_name = tempfile.mkstemp(
        prefix="probiga_etf_forward_",
        suffix=".json",
    )
    os.close(descriptor)
    local_path = Path(temporary_name)
    remote_path = posixpath.join(
        "/tmp",
        f"probiga_etf_forward_{uuid.uuid4().hex}.json",
    )
    client: Any = None
    try:
        exported = export_bundle(local_path)
        client = production_ssh_client(paramiko)
        client.connect(**production_ssh_connect_kwargs())
        sftp = client.open_sftp()
        try:
            sftp.put(str(local_path), remote_path)
        finally:
            sftp.close()
        command = (
            f"env PYTHONPATH={shlex.quote(pythonpath)} "
            f"{shlex.quote(root + '/venv/bin/python')} "
            f"{shlex.quote(root + '/tools/promote_etf_forward_to_production.py')} "
            f"--import-json {shlex.quote(remote_path)}"
        )
        _stdin, stdout, stderr = client.exec_command(
            command,
            timeout=300,
        )
        output = stdout.read().decode("utf-8", errors="replace").strip()
        error = stderr.read().decode("utf-8", errors="replace").strip()
        status = stdout.channel.recv_exit_status()
        if status:
            raise RuntimeError(error[-4000:])
        imported = json.loads(output)
        return {
            "status": "ok",
            "export": exported,
            "production_import": imported,
        }
    finally:
        if client is not None:
            try:
                client.exec_command(
                    f"rm -f -- {shlex.quote(remote_path)}",
                    timeout=30,
                )
            except Exception as exc:
                print(
                    f"warning: failed to remove remote temporary file: {exc}",
                    file=sys.stderr,
                )
            client.close()
        local_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--promote-production", action="store_true")
    group.add_argument("--export-json")
    group.add_argument("--import-json")
    args = parser.parse_args()
    load_project_env()
    if args.promote_production:
        result = promote_to_production()
    elif args.export_json:
        result = export_bundle(Path(args.export_json))
    else:
        result = import_bundle(Path(args.import_json))
    print(json.dumps(
        result,
        ensure_ascii=False,
        indent=2,
        default=_json_default,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
