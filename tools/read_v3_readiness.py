"""Read-only, process-bounded deep readiness probe used by the API snapshot."""
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main():
    try:
        from server.api.routers.trading_v3 import readiness
        payload = readiness()
        print(json.dumps(payload, ensure_ascii=True, default=str), flush=True)
        return 0
    except Exception as exc:
        print(json.dumps({"error_type": type(exc).__name__}), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
