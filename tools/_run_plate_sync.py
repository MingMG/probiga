#!/usr/bin/env python3
"""Retired mutable-file production maintenance entrypoint.

Production releases are accepted only through the audited GitHub workflow and
the root-owned ``probiga-production-deploy`` broker.  This historical filename
is kept as an explicit safety fence for old operator habits and shortcuts.
"""

from __future__ import annotations

import sys


RETIRED_EXIT_CODE = 2
RETIRED_MESSAGE = (
    "Legacy mutable-file production maintenance is retired; "
    "use the audited production deployment workflow."
)


def main() -> int:
    print(RETIRED_MESSAGE, file=sys.stderr)
    return RETIRED_EXIT_CODE


if __name__ == "__main__":
    raise SystemExit(main())
