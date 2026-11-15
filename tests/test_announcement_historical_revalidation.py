from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from server.common import component_release_attestation, daily_delivery_control, pit_facts
from server.common.qmt_announcement_pit import (
    CNINFO_ANNOUNCEMENT_SOURCE,
    AnnouncementCatalog,
    QMTAnnouncementBlocked,
    canonical_hash,
)
from tools.sync_qmt_announcement_pit import _revalidate_located_historical_reconstruction


@pytest.fixture
def retained(monkeypatch):
    target = date(2026, 11, 13)
    start = target - timedelta(days=30)
    decision = datetime(2026, 11, 15, 18, 34, 45)
    old, current = "a" * 40, "b" * 40
    codes = ["000001", "002731", "688613"]
    catalog = AnnouncementCatalog("catalog", "c" * 64, "d" * 64, tuple(codes), {})
    receipt = {
        "mode": "HISTORICAL_RECONSTRUCTION_EXISTING",
        "source": CNINFO_ANNOUNCEMENT_SOURCE,
        "batch_id": "retained-batch", "batch_root_hash": "e" * 64,
        "window_start": start.isoformat(), "window_end": target.isoformat(),
        "catalog_batch_id": catalog.batch_id,
        "catalog_manifest_hash": catalog.manifest_hash,
        "catalog_member_set_hash": catalog.member_set_hash,
        "stock_count": len(codes),
        "received_at": "2026-11-15T13:47:45.256897",
        "reconstruction_provenance": {"catalog_codes_sha256": canonical_hash(codes)},
    }
    proof = daily_delivery_control.FrozenInitialAnnouncement(receipt, {
        "contract_release_id": old,
        "producer_build_sha": old,
        "batch_id": receipt["batch_id"],
        "batch_root_hash": receipt["batch_root_hash"],
    })
    attestations = {
        sha: {
            "scope": "COORDINATED", "linux_build_sha": sha, "contract_build_sha": sha,
            "windows_build_sha": sha, "contract_sha256": sha[0] * 64,
        }
        for sha in (old, current)
    }
    verified = {
        **receipt,
        "catalog_member_count": len(codes),
        "coverage_count": len(codes),
        "reconstructed_at": receipt["received_at"],
    }
    engine, connection = object(), object()
    called = []

    def read_receipt(bind, **kwargs):
        assert bind is connection
        assert kwargs == {"trade_date": target.isoformat(), "decision_at": decision}
        return proof

    def attested(bind, build):
        assert bind is connection
        called.append(("attested", build))
        return attestations[build]

    def verify(actual_engine, retained_proof, **kwargs):
        assert actual_engine is engine and retained_proof is proof
        assert kwargs["codes"] == codes
        assert kwargs["decision"] == decision
        assert kwargs["window_start"] == start and kwargs["window_end"] == target
        kwargs["event_progress"](0, len(codes))
        kwargs["event_progress"](len(codes), len(codes))
        called.append(("raw_verified", retained_proof.receipt["batch_id"]))
        return SimpleNamespace(batch=verified, members=dict.fromkeys(codes))

    monkeypatch.setattr(component_release_attestation, "load_component_attestation", attested)
    monkeypatch.setattr(daily_delivery_control, "select_frozen_initial_announcement_source", read_receipt)
    monkeypatch.setattr(pit_facts, "_load_certified_initial_event_selection", verify)
    args = {
        "catalog": catalog, "target": target, "window_start": start, "decision_at": decision,
        "consumer_release_id": current, "validation_build_sha": current,
    }
    return {
        "args": args, "bind": (engine, connection), "calls": called,
        "old": old, "current": current, "attestations": attestations,
        "receipt": receipt, "verified": verified,
    }


def test_coordinated_global_digest_change_revalidates_exact_signed_raw_batch(retained):
    verified = _revalidate_located_historical_reconstruction(*retained["bind"], **retained["args"])
    assert retained["attestations"][retained["old"]]["contract_sha256"] != (
        retained["attestations"][retained["current"]]["contract_sha256"]
    )
    assert retained["calls"] == [
        ("attested", retained["old"]), ("attested", retained["current"]),
        ("raw_verified", retained["receipt"]["batch_id"]),
    ]
    assert verified is retained["verified"]
    assert verified["received_at"] == "2026-11-15T13:47:45.256897"
    assert verified["reconstruction_provenance"] == retained["receipt"]["reconstruction_provenance"]


def test_only_absent_retained_historical_proof_permits_discovery(retained, monkeypatch):
    monkeypatch.setattr(daily_delivery_control, "select_frozen_initial_announcement_source", lambda *_a, **_k: None)
    assert _revalidate_located_historical_reconstruction(*retained["bind"], **retained["args"]) is None
    assert retained["calls"] == []


@pytest.mark.parametrize("broken_field,expected", [
    ("producer", "component-identity:ValueError"),
    ("catalog_member_set_hash", "current-scope:ValueError"),
])
def test_unbound_locator_cannot_be_accepted_or_trigger_discovery(retained, broken_field, expected):
    if broken_field == "producer":
        retained["attestations"][retained["old"]]["windows_build_sha"] = "f" * 40
    else:
        retained["receipt"][broken_field] = "f" * 64
    with pytest.raises(QMTAnnouncementBlocked) as blocked:
        _revalidate_located_historical_reconstruction(*retained["bind"], **retained["args"])
    assert blocked.value.reason_code == "QMT_ANNOUNCEMENT_SIGNED_RECONSTRUCTION_REVALIDATION_FAILED"
    assert blocked.value.detail == expected
    assert all(phase != "raw_verified" for phase, _ in retained["calls"])


def test_raw_validation_error_returns_phase_and_errno_without_payload(retained, monkeypatch):
    def fail(*_a, **_k):
        raise OperationalError("private SQL text", {"private": 1}, RuntimeError(2013, "private DSN"))

    monkeypatch.setattr(pit_facts, "_load_certified_initial_event_selection", fail)
    with pytest.raises(QMTAnnouncementBlocked) as blocked:
        _revalidate_located_historical_reconstruction(*retained["bind"], **retained["args"])
    assert blocked.value.reason_code == "QMT_ANNOUNCEMENT_SIGNED_RECONSTRUCTION_REVALIDATION_FAILED"
    assert blocked.value.detail == "exact-batch:OperationalError:errno=2013"


def test_changed_rebuilt_root_does_not_copy_past_success(retained):
    retained["verified"]["batch_root_hash"] = "7" * 64
    with pytest.raises(QMTAnnouncementBlocked) as blocked:
        _revalidate_located_historical_reconstruction(*retained["bind"], **retained["args"])
    assert blocked.value.detail == "exact-batch:ValueError"
