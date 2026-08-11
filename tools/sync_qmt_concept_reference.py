from __future__ import annotations

"""Synchronize QMT concept catalog and memberships as one validated snapshot."""

import json
import os

from biz.stock_info.sync_stock_info import (
    ExternalConceptSourceUnavailable,
    run_ddl,
    sync_qmt_concept_reference,
)
from server.common.batch_db import create_batch_engine


def main() -> int:
    # The canonical wrapper selects ``bigqmt`` on the signed-in Windows
    # owner.  Keep that explicit provenance instead of overwriting it with
    # the legacy MiniQMT route; direct invocations still default to QMT.
    os.environ.setdefault("SI_CONCEPT_SOURCE", "qmt")
    engine = create_batch_engine(future=True)
    run_ddl(engine)
    try:
        result = sync_qmt_concept_reference(engine)
    except ExternalConceptSourceUnavailable as exc:
        print(
            json.dumps(
                {
                    "status": "BLOCK",
                    "reason": "external_concept_source_unavailable",
                    "message": str(exc),
                    "preserved_previous_snapshot": True,
                },
                ensure_ascii=False,
            )
        )
        return 4
    print(json.dumps({"status": "success", **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
