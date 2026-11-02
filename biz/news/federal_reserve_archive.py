"""Bounded official annual-archive acquisition for the formal news workload."""
from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import json
import subprocess
import sys
from zoneinfo import ZoneInfo

from server.common.federal_reserve_news_coverage import (
    MAX_DOCUMENT_BYTES, RSS_URL, _project, archive_urls, seal_federal_reserve_news_coverage,
)


def _capture(observed_at):
    import httpx

    urls = (*archive_urls(observed_at), RSS_URL)
    def read(url):
        requested_at = datetime.now(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None, microsecond=0).isoformat()
        with client.stream("GET", url, headers={"User-Agent": "Mozilla/5.0 ProBigA-official-news"}) as response:
            response.raise_for_status()
            if response.status_code != 200:
                raise ValueError("FED_SOURCE_HTTP_STATUS_INVALID")
            body = bytearray()
            for chunk in response.iter_bytes():
                body.extend(chunk)
                if len(body) > MAX_DOCUMENT_BYTES:
                    raise ValueError("FED_SOURCE_RESPONSE_SIZE_EXCEEDED")
        data = bytes(body)
        return {"url": url, "http_status": 200, "source_response_sha256": hashlib.sha256(data).hexdigest(),
                "request_started_at": requested_at,
                "captured_at": datetime.now(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None, microsecond=0).isoformat(),
                "body_base64": base64.b64encode(data).decode("ascii")}
    with httpx.Client(trust_env=False, follow_redirects=False,
                      limits=httpx.Limits(max_connections=5, max_keepalive_connections=5),
                      timeout=httpx.Timeout(6, connect=6)) as client:
        with ThreadPoolExecutor(max_workers=5) as pool:
            documents = list(pool.map(read, urls))
    _project(documents, observed_at)
    return {"documents": documents, "captured_at": max(d["captured_at"] for d in documents)}


def fetch_federal_reserve_news(*, observed_at, deadline_seconds=20, root=None):
    """Hard-bound HTTP and parsing in an isolated child; retain verified originals."""
    if not 1 <= float(deadline_seconds) <= 20:
        raise ValueError("FED_SOURCE_DEADLINE_INVALID")
    observed = observed_at.isoformat() if isinstance(observed_at, datetime) else str(observed_at)
    try:
        result = subprocess.run([sys.executable, "-B", "-m", "biz.news.federal_reserve_archive", "--worker", observed],
                                capture_output=True, timeout=deadline_seconds, check=False, close_fds=True)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("FED_ARCHIVE_ACQUISITION_DEADLINE_EXCEEDED") from exc
    if result.returncode != 0 or not result.stdout or len(result.stdout) > 8 * MAX_DOCUMENT_BYTES:
        detail = result.stderr.decode("utf-8", errors="replace").splitlines()[-1:]
        raise RuntimeError("FED_ARCHIVE_ACQUISITION_FAILED: " + (detail[0][:250] if detail else "worker_output_invalid"))
    captured = json.loads(result.stdout)
    items, proof = seal_federal_reserve_news_coverage(captured["documents"], observed_at=observed,
                                                   captured_at=captured["captured_at"], root=root)
    return {"items": items, "archive_coverage": proof}


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--worker":
        raise SystemExit(2)
    print(json.dumps(_capture(sys.argv[2]), ensure_ascii=False, separators=(",", ":")))
