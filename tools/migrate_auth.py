#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.auth.schema import ensure_auth_schema
from server.auth.service import registration_state
from server.common.batch_db import create_batch_engine


def main() -> int:
    engine = create_batch_engine()
    ensure_auth_schema(engine)
    state = registration_state(engine)
    print(json.dumps({"status": "ok", **state}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
