import os
from datetime import datetime

from integrations.bigqmt.login_diagnostics import (
    classify_login_log,
    diagnose_bigqmt_login,
)


def test_auth_eof_is_classified_without_returning_sensitive_raw_values():
    result = classify_login_log(
        [
            "2026-07-25 10:00:00 login status: 6 account=SECRET_ACCOUNT ip=1.2.3.4",
            "2026-07-25 10:00:01 login status: 21 onDefaultServerParam End of file account=SECRET_ACCOUNT ip=1.2.3.4",
        ],
        now=datetime(2026, 7, 25, 10, 1),
    )
    serialized = repr(result)
    assert result["status"] == "login_failed"
    assert result["reason_code"] == "AUTH_SERVER_EOF"
    assert "SECRET_ACCOUNT" not in serialized
    assert "1.2.3.4" not in serialized


def test_login_in_progress_older_than_120_seconds_is_stalled():
    result = classify_login_log(
        ["2026-07-25 10:00:00 login status: 6"],
        now=datetime(2026, 7, 25, 10, 3),
    )
    assert result["status"] == "login_failed"
    assert result["reason_code"] == "LOGIN_STALLED"


def test_fresh_bridge_heartbeat_wins_over_old_login_log(tmp_path):
    heartbeat = tmp_path / "userdata" / "probiga_bridge" / "heartbeat.json"
    heartbeat.parent.mkdir(parents=True)
    heartbeat.write_text("{}", encoding="utf-8")
    result = diagnose_bigqmt_login(
        qmt_home=tmp_path,
        heartbeat_max_age_seconds=60,
    )
    assert result["status"] == "ready"
    assert result["reason_code"] == "BIGQMT_HEARTBEAT_FRESH"


def test_stale_heartbeat_uses_sanitized_latest_log(tmp_path):
    heartbeat = tmp_path / "userdata" / "probiga_bridge" / "heartbeat.json"
    heartbeat.parent.mkdir(parents=True)
    heartbeat.write_text("{}", encoding="utf-8")
    old = datetime.now().timestamp() - 600
    os.utime(heartbeat, (old, old))
    log = tmp_path / "userdata" / "log" / "XtClient_20260725.log"
    log.parent.mkdir(parents=True)
    log.write_text(
        "2026-07-25 10:00:01 login status: 21 onDefaultServerParam "
        "End of file account=SECRET_ACCOUNT ip=1.2.3.4\n",
        encoding="utf-8",
    )
    result = diagnose_bigqmt_login(
        qmt_home=tmp_path,
        heartbeat_max_age_seconds=60,
        now=datetime(2026, 7, 25, 10, 1, 30),
    )
    assert result["status"] == "login_failed"
    assert result["reason_code"] == "AUTH_SERVER_EOF"
    assert result["source_log"] == "XtClient_20260725.log"
    assert "SECRET_ACCOUNT" not in repr(result)
    assert "1.2.3.4" not in repr(result)
