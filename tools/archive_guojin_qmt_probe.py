#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.audit import ensure_audit_tables
from integrations.qmt.diagnostics import capabilities, core_probe, diagnostics
from integrations.qmt.raw_store import archive_payload, result_dict
from server.common.batch_db import create_batch_engine


def _provenance(diag: dict) -> dict:
    client = diag.get("client") or {}
    sdk = diag.get("sdk") or {}
    return {
        "client_version": client.get("client_version"),
        "sdk_module": sdk.get("sdk_module"),
        "sdk_version": sdk.get("sdk_version"),
        "connection_port": sdk.get("connection_port"),
        "transport": sdk.get("transport"),
    }


def main() -> int:
    engine = create_batch_engine(future=True)
    ensure_audit_tables(engine)
    batch_id = uuid.uuid4().hex
    diag = diagnostics(force=True)
    if diag.get("status") != "ok":
        print(json.dumps({"status": "error", "diagnostics": diag}, ensure_ascii=False))
        return 2

    provenance = _provenance(diag)
    calls = [
        ("qmt_capabilities", "capabilities", capabilities(force=True)),
        ("qmt_core_probe", "probe_core", core_probe(force=True)),
    ]
    archived = []
    for dataset, api_name, response in calls:
        archived.append(
            result_dict(
                archive_payload(
                    engine,
                    dataset=dataset,
                    api_name=api_name,
                    params={},
                    payload=response,
                    batch_id=batch_id,
                    provenance=provenance,
                )
            )
        )
    print(json.dumps({"status": "ok", "batch_id": batch_id, "archives": archived}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
