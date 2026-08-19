#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.bigqmt.login_diagnostics import diagnose_bigqmt_login
from tools.env_config import load_project_env


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--heartbeat-max-age-seconds", type=int, default=60)
    args = parser.parse_args()
    load_project_env()
    result = diagnose_bigqmt_login(
        heartbeat_max_age_seconds=max(1, args.heartbeat_max_age_seconds)
    )
    print(
        json.dumps(result, ensure_ascii=False, default=str)
        if args.json
        else result
    )
    return 0 if result["status"] in {"ready", "logged_in", "starting"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
