#!/usr/bin/env python3
"""Validate and append one research-only quantitative study batch."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from biz.intelligence.quant_research import persist_quant_research_batch  # noqa: E402
from server.common.batch_db import create_batch_engine  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path, help="UTF-8 JSON input path")
    source.add_argument("--stdin", action="store_true", help="read JSON from stdin")
    parser.add_argument("--json", action="store_true", help="emit a JSON receipt")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        raw = sys.stdin.read() if args.stdin else args.input.read_text(encoding="utf-8")
        payload = json.loads(raw)
        engine = create_batch_engine()
        try:
            result = persist_quant_research_batch(engine, payload)
        finally:
            engine.dispose()
    except Exception as exc:
        result = {
            "status": "FAILED",
            "stage": "quant_research_import",
            "error_type": type(exc).__name__,
            "message": str(exc),
        }
        print(json.dumps(result, ensure_ascii=False) if args.json else result)
        return 1
    print(json.dumps(result, ensure_ascii=False) if args.json else result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
