#!/usr/bin/env python3
"""Prepare the frozen premarket announcement source on the Windows scheduler."""
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.sync_qmt_announcement_pit import main


if __name__ == "__main__":
    raise SystemExit(main(premarket_source=True))
