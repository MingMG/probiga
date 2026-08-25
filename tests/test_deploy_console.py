# -*- coding: utf-8 -*-
import unittest
import tempfile
from subprocess import CompletedProcess
from unittest.mock import patch
from pathlib import Path

from fastapi import HTTPException

from server.api.routers import deploy


def _cp(stdout="", stderr="", returncode=0):
    return CompletedProcess(args=["git"], returncode=returncode, stdout=stdout, stderr=stderr)


class DeployConsoleTest(unittest.TestCase):
    def tearDown(self):
        deploy._runs.clear()

    def test_git_status_payload_reports_changed_files(self):
        results = [
            _cp("main\n"),
            _cp("abc123\n"),
            _cp("add deploy console\n"),
            _cp(" M server/api/main.py\n?? server/static/deploy.html\n"),
            _cp("git@github.com:example/probiga.git\n"),
            _cp("origin/main\n"),
        ]

        with patch("server.api.routers.deploy._run_git", side_effect=results):
            payload = deploy._git_status_payload()

        self.assertEqual(payload["branch"], "main")
        self.assertEqual(payload["commit"], "abc123")
        self.assertTrue(payload["dirty"])
        self.assertEqual(payload["changed_count"], 2)
        self.assertEqual(payload["upstream"], "origin/main")

    def test_deploy_status_includes_actions_and_history(self):
        repo = {
            "branch": "main",
            "commit": "abc123",
            "subject": "ok",
            "remote": "origin",
            "upstream": "origin/main",
            "dirty": False,
            "changed_count": 0,
            "changed_files": [],
        }

        with patch.dict(
            "server.api.routers.deploy.os.environ",
            {"PROBIGA_IN_APP_DEPLOY_ENABLED": "1"}, clear=False,
        ), patch("server.api.routers.deploy._git_status_payload", return_value=repo), \
             patch("server.api.routers.deploy._read_history", return_value=[]):
            payload = deploy.deploy_status()

        self.assertEqual(payload["repo"]["branch"], "main")
        self.assertIn("push", payload["actions"])
        self.assertIn("commit_push", payload["actions"])
        self.assertIn("local", payload["actions"])

    def test_deploy_run_rejects_when_another_task_is_running(self):
        deploy._runs["running"] = {"id": "running", "status": "running"}

        with patch.dict(
            "server.api.routers.deploy.os.environ",
            {"PROBIGA_IN_APP_DEPLOY_ENABLED": "1"}, clear=False,
        ), self.assertRaises(HTTPException) as raised:
            deploy.deploy_run(deploy.DeployRunRequest(action="push"))
        self.assertEqual(raised.exception.status_code, 409)

    def test_disabled_backend_rejects_all_endpoints_without_starting_work(self):
        with patch.dict(
            "server.api.routers.deploy.os.environ",
            {"PROBIGA_IN_APP_DEPLOY_ENABLED": "0"}, clear=False,
        ), patch("server.api.routers.deploy.threading.Thread") as thread_cls, \
             patch("server.api.routers.deploy.subprocess.Popen") as popen, \
             patch("server.api.routers.deploy._run_git") as run_git:
            for call in (
                deploy.deploy_status,
                lambda: deploy.deploy_run(
                    deploy.DeployRunRequest(action="push")
                ),
                lambda: deploy.deploy_run_detail("missing"),
            ):
                with self.assertRaises(HTTPException) as raised:
                    call()
                self.assertEqual(raised.exception.status_code, 404)
        thread_cls.assert_not_called()
        popen.assert_not_called()
        run_git.assert_not_called()

    def test_disabled_console_page_is_not_served(self):
        from server.api import main as api_main

        with patch.dict(
            "server.api.routers.deploy.os.environ",
            {"PROBIGA_IN_APP_DEPLOY_ENABLED": "0"}, clear=False,
        ), self.assertRaises(HTTPException) as raised:
            api_main.deploy_console()
        self.assertEqual(raised.exception.status_code, 404)

    def test_enabled_history_uses_external_runtime_root(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_root = Path(temp_dir) / "deploy-state"
            run = {
                "id": "abc123", "action": "push", "status": "success",
                "started_at": "2026-08-25 10:00:00",
                "finished_at": "2026-08-25 10:01:00",
                "branch": "main", "commit": "deadbee",
            }
            with patch.dict(
                "server.api.routers.deploy.os.environ",
                {
                    "PROBIGA_IN_APP_DEPLOY_ENABLED": "1",
                    "PROBIGA_DEPLOY_RUNTIME_ROOT": str(runtime_root),
                },
                clear=False,
            ):
                deploy._save_history(run)
                history = deploy._read_history()
                resolved_runtime, _history_file = deploy._deploy_runtime_paths()
            self.assertEqual(history[0]["id"], "abc123")
            self.assertTrue((runtime_root / "history.json").is_file())
            self.assertFalse(
                resolved_runtime.is_relative_to(deploy.ROOT.resolve())
            )


if __name__ == "__main__":
    unittest.main()
