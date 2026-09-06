"""Explicit boundary until a general issuer/disclosure protocol is verified."""

from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo


class CninfoProvider:
    def __init__(self, client=None, clock=None, sleep=None):
        self.clock = clock or (lambda: datetime.now(ZoneInfo("Asia/Shanghai")))

    def fetch_batch(self, dataset: str, request: dict) -> dict:
        # An issuer-specific document or empty search cannot establish a
        # general non-filing exemption. No network request or date invention.
        return {
            "request": deepcopy(request),
            "received_at": self.clock().astimezone(ZoneInfo("Asia/Shanghai")).isoformat(),
            "source_method": "cninfo.disclosure.unconfigured",
            "outcomes": {
                symbol: {"status": "error", "rows": [],
                         "error_code": "UNSUPPORTED_DISCLOSURE_PROTOCOL",
                         "reason": "General issuer/disclosure evidence protocol is not configured"}
                for symbol in request.get("codes", [])
            },
        }
