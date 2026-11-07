from pathlib import Path
import time

import pytest

from server.project_workflow import store


REV = "1" * 40
FIX = "2" * 40


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "tasks.sqlite3"
    store.initialize(path)
    return path


def manifest(*steps, id="workflow", **fields):
    return {"id": id, "title": id, "goal": "verify the requested outcome", "acceptance": ["usable result"],
            "expires": time.time() + 240, "candidate": REV, "steps": list(steps), **fields}


def step(id, kind, *, env="isolated", **kw):
    return {"id": id, "kind": kind, "inputs": {"environment": env, **kw.pop("inputs", {})}, **kw}


def take(db, expected):
    store.schedule(db, 4)
    items = store.get_launches(db, owner_absent=lambda _: True, cap=4)
    assert len(items) == len(expected)
    result = {}
    for aid in items:
        op, context = store.claim(db, aid, "owner")
        assert op["kind"] in expected
        result[op["id"]] = (aid, op, context)
    return result


def finish(db, aid, code="ok", status="success", **fields):
    assert store.finish(db, aid, "owner", {"status": status, "code": code, **fields})


def test_retry_rework_keeps_candidate_and_failed_test_evidence(db, tmp_path):
    store.submit(db, manifest(step("tests", "test", rework=True),
                              session={"worktree": str(tmp_path / "repo"), "branch": "codex/fixture"}))
    test, _, _ = take(db, {"test"})["tests"]
    finish(db, test, "test_failed", "failure", evidence={"candidate": REV}, log_tail="specific test: assertion")
    rework, _, ctx = take(db, {"rework"})["tests"]
    assert ctx["failure"]["log_tail"] == "specific test: assertion"
    finish(db, rework, "reworked", evidence={"candidate": FIX},
           checkpoint={"session": {"thread_id": "thread-1", "turn_id": "turn-1"}})
    retest, op, ctx = take(db, {"test"})["tests"]
    assert op["inputs"]["candidate"] == FIX
    assert ctx["session"]["thread_id"] == "thread-1"
    finish(db, retest, "tests_passed", evidence={"candidate": FIX, "environment": "isolated"})
    view = store.task_view(db, "workflow")
    assert view["state"] == "COMPLETE"
    assert {(e["candidate"], e["state"]) for e in view["evidence"]} == {(REV, "FAILED"), (FIX, "COMPLETE")}
    assert "specific test" not in store.pack(view)


def test_same_failure_twice_runs_supervisor_without_another_mutation(db, tmp_path):
    store.submit(db, manifest(step("tests", "test", rework=True),
                              session={"worktree": str(tmp_path / "repo"), "branch": "codex/fixture"}))
    first, _, _ = take(db, {"test"})["tests"]
    finish(db, first, "test_failed", "failure", failure_key="same-test")
    repair, _, _ = take(db, {"rework"})["tests"]
    finish(db, repair, "reworked", evidence={"candidate": FIX})
    second, _, _ = take(db, {"test"})["tests"]
    finish(db, second, "test_failed", "failure", failure_key="same-test")
    supervisor, _, _ = take(db, {"supervise"})["tests"]
    finish(db, supervisor, "diagnosis_ready")
    assert store.task_view(db, "workflow")["state"] == "FAILED"
    take(db, set())


def test_unknown_effect_retains_resource_and_rejects_stale_owner(db):
    for id in ("a", "b"):
        store.submit(db, manifest(step("simulate", "simulation", resource="sim:fixture"), id=id))
    a, _, ctx = take(db, {"simulation"})["simulate"]
    assert ctx["task_id"] == "a"
    with store.db(db, write=True) as con:
        con.execute("UPDATE attempt SET lease=0 WHERE id=?", (a,))
    assert not store.get_launches(db, owner_absent=lambda _: False, cap=4)
    assert not store.finish(db, a, "owner", {"status": "success", "code": "stale"})
    assert store.get_launches(db, owner_absent=lambda _: True, cap=4) == [a]
    _, reconciler = store.claim(db, a, "new-owner")
    assert reconciler["mode"] == "reconcile"
    assert not store.finish(db, a, "owner", {"status": "success", "code": "stale"})
    assert store.finish(db, a, "new-owner", {"status": "success", "code": "verified",
                                             "evidence": {"isolated": True, "real_orders": 0}})
    _, _, ctx_b = take(db, {"simulation"})["simulate"]
    assert ctx_b["task_id"] == "b" and ctx_b["fence"] > ctx["fence"]


def test_exact_production_authorization_and_old_readback(db, tmp_path):
    steps = [step("merge", "merge", resource="repository"), step("test", "test", depends=["merge"]),
             step("scope", "classify", depends=["merge"]),
             step("ship", "deploy", env="production", endpoint="linux", resource="release:linux",
                  depends=["test", "scope"]),
             step("check", "readback", env="production", endpoint="linux", depends=["ship"]),
             step("other", "verify")]
    store.submit(db, manifest(*steps, session={"worktree": str(tmp_path / "repo"), "branch": "codex/fixture"}))
    initial = take(db, {"merge", "verify"})
    finish(db, initial["merge"][0], evidence={"candidate": REV, "merged": True})
    finish(db, initial["other"][0])
    initial = take(db, {"test", "classify"})
    finish(db, initial["test"][0], evidence={"candidate": REV, "environment": "isolated"})
    finish(db, initial["scope"][0], evidence={"candidate": REV, "endpoints": ["linux"], "contract": "scope"})
    args = dict(kind="deploy", environment="production", resource="release:linux", endpoint="linux",
                candidate=FIX, expires=time.time() + 120, decision="allow", reference="approval-1")
    store.grant(db, "workflow", **args)
    take(db, set())
    assert store.task_view(db, "workflow")["steps"][3]["code"] == "authorization_missing"
    args.update(candidate=REV, decision="deny")
    store.grant(db, "workflow", **args)
    take(db, set())
    assert store.task_view(db, "workflow")["steps"][3]["code"] == "authorization_denied"
    args.update(decision="allow")
    store.grant(db, "workflow", **args)
    with store.db(db, write=True) as con:
        con.execute("UPDATE grant_record SET expires=0 WHERE candidate=?", (REV,))
    take(db, set())
    assert store.task_view(db, "workflow")["steps"][3]["code"] == "authorization_expired"
    store.grant(db, "workflow", **args)
    ship, _, ctx = take(db, {"deploy"})["ship"]
    assert ctx["authorization"] == "approval-1"
    finish(db, ship, evidence={"candidate": REV, "endpoint": "linux", "released": True})
    check, _, _ = take(db, {"readback"})["check"]
    finish(db, check, evidence={"candidate": FIX, "endpoint": "linux", "active": True, "business_verified": True})
    evidence = {e["stage"]: e["state"] for e in store.task_view(db, "workflow")["evidence"]}
    assert evidence == {"merged": "COMPLETE", "tested": "COMPLETE", "released": "COMPLETE", "active": "FAILED"}


def test_pausing_drains_no_new_dispatch_and_resumes(db):
    store.submit(db, manifest(step("first", "verify"), step("next", "verify", depends=["first"])))
    first, _, _ = take(db, {"verify"})["first"]
    store.control(db, "workflow", "pause")
    finish(db, first)
    take(db, set())
    assert store.task_view(db, "workflow")["state"] == "BLOCKED"
    store.control(db, "workflow", "resume")
    second, _, _ = take(db, {"verify"})["next"]
    finish(db, second)
    assert store.task_view(db, "workflow")["state"] == "COMPLETE"
