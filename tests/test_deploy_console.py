# -*- coding: utf-8 -*-
import unittest
from subprocess import CompletedProcess
from unittest.mock import patch

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

        with patch("server.api.routers.deploy._git_status_payload", return_value=repo), \
             patch("server.api.routers.deploy._read_history", return_value=[]):
            payload = deploy.deploy_status()

        self.assertEqual(payload["repo"]["branch"], "main")
        self.assertIn("push", payload["actions"])
        self.assertIn("commit_push", payload["actions"])
        self.assertIn("local", payload["actions"])

    def test_deploy_run_rejects_when_another_task_is_running(self):
        deploy._runs["running"] = {"id": "running", "status": "running"}

        with self.assertRaises(Exception):
            deploy.deploy_run(deploy.DeployRunRequest(action="push"))


if __name__ == "__main__":
    unittest.main()

