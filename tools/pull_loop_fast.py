#!/usr/bin/env python3
"""Disabled legacy hot-rank history loop.

Current-only providers cannot prove historical dates, and a single dated
Eastmoney source must never be promoted into the fused table by this utility.
Use the formal per-source publishers for one explicit target date instead.
"""

import sys


def main() -> int:
    print(
        "Legacy bulk hot-rank history loop is disabled; no provider or fused "
        "data was written.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
