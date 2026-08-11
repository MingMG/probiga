import json
import subprocess
from unittest.mock import patch

from tools.sync_capital_flow_direct import fetch_flow_east


def test_fetch_flow_uses_platform_curl_when_requests_fails():
    payload = {
        "data": {
            "klines": ["2026-07-17,1,2,3,4,5,6"],
        }
    }
    completed = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout=json.dumps(payload).encode("utf-8"),
        stderr=b"",
    )

    with (
        patch("tools.sync_capital_flow_direct.requests.get", side_effect=RuntimeError("tls reset")),
        patch("tools.sync_capital_flow_direct.shutil.which", side_effect=lambda name: "/usr/bin/curl" if name == "curl" else None),
        patch("tools.sync_capital_flow_direct.subprocess.run", return_value=completed) as run,
    ):
        frame = fetch_flow_east("000001")

    assert frame is not None
    assert frame.iloc[0]["trade_date"] == "2026-07-17"
    assert frame.iloc[0]["main_net_inflow"] == 1
    assert run.call_args.args[0][0] == "/usr/bin/curl"
