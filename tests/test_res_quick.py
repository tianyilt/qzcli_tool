"""Tests for `qzcli res -u --quick`.

Verifies that quick mode skips the unbounded historical-jobs walk and
still gathers compute_groups + projects from cluster_info / task_dimension.
"""

import unittest
from unittest import mock

from qzcli import cli


class _FakeAPI:
    """Captures which collection endpoints get called."""

    def __init__(self):
        self.fetch_jobs_calls = 0
        self.fetch_tasks_calls = 0
        self.cluster_info_calls = 0

    def list_jobs_with_cookie(self, *a, **kw):
        self.fetch_jobs_calls += 1
        return {"jobs": [], "total": 0}

    def list_task_dimension(self, *a, **kw):
        self.fetch_tasks_calls += 1
        return {
            "tasks": [
                {"project": {"id": "project-1", "name": "P1"}},
                {"project": {"id": "project-2", "name": "P2"}},
            ],
            "total": 2,
        }

    def get_cluster_basic_info(self, workspace_id, cookie):
        self.cluster_info_calls += 1
        return {
            "compute_groups": [
                {
                    "compute_group_id": "cg-A",
                    "compute_group_name": "A",
                    "cluster_id": "cluster-A",
                    "logic_compute_groups": [
                        {
                            "logic_compute_group_id": "lcg-A1",
                            "logic_compute_group_name": "A1",
                            "resource_types": ["NVIDIA"],
                            "brand": "H200",
                        }
                    ],
                }
            ]
        }

    def extract_resources_from_jobs(self, jobs):  # pragma: no cover — quick=True 不走
        raise AssertionError("extract_resources_from_jobs must not be called in quick mode")


def _patched_fetch_jobs(api, workspace_id, cookie, *, page_size=200, created_by=None):
    api.fetch_jobs_calls += 1
    return []


def _patched_fetch_tasks(api, workspace_id, cookie, project_id=None, *, page_size=200):
    api.fetch_tasks_calls += 1
    return [
        {"project": {"id": "project-1", "name": "P1"}},
        {"project": {"id": "project-2", "name": "P2"}},
    ]


class QuickModeSkipsJobsWalk(unittest.TestCase):
    def test_quick_true_does_not_call_jobs_pagination(self):
        api = _FakeAPI()
        with mock.patch.object(
            cli, "_fetch_all_jobs_with_cookie", side_effect=_patched_fetch_jobs
        ) as patched_jobs, mock.patch.object(
            cli, "_fetch_all_task_dimensions", side_effect=_patched_fetch_tasks
        ) as patched_tasks:
            resources, jobs_count = cli._collect_workspace_resources_from_live_apis(
                api, "ws-test", "cookie=abc", quick=True
            )

        self.assertEqual(jobs_count, 0)
        self.assertEqual(
            patched_jobs.call_count,
            0,
            "jobs pagination must be skipped in quick mode",
        )
        self.assertGreaterEqual(
            patched_tasks.call_count, 1, "task_dimension must still run"
        )
        self.assertGreaterEqual(api.cluster_info_calls, 1, "cluster_info must still run")
        self.assertEqual(resources.get("specs", []), [])
        project_ids = {p["id"] for p in resources.get("projects", [])}
        self.assertSetEqual(project_ids, {"project-1", "project-2"})
        compute_group_ids = {g["id"] for g in resources.get("compute_groups", [])}
        self.assertIn("lcg-A1", compute_group_ids)

    def test_quick_false_still_calls_jobs_pagination(self):
        api = _FakeAPI()
        with mock.patch.object(
            cli, "_fetch_all_jobs_with_cookie", side_effect=_patched_fetch_jobs
        ) as patched_jobs, mock.patch.object(
            cli, "_fetch_all_task_dimensions", side_effect=_patched_fetch_tasks
        ):
            cli._collect_workspace_resources_from_live_apis(
                api, "ws-test", "cookie=abc", quick=False
            )
        self.assertEqual(
            patched_jobs.call_count, 1, "default mode must still walk jobs"
        )


class CliFlagWiring(unittest.TestCase):
    """Smoke-check the CLI surface for the default-quick flip.

    We don't extract a `build_parser()` factory in this PR, so we go
    through the installed `qzcli` entry point via subprocess instead
    of poking argparse internals.
    """

    def test_help_text_advertises_full_flag(self):
        import subprocess

        result = subprocess.run(
            ["qzcli", "res", "--help"], capture_output=True, text=True, timeout=10
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--full", result.stdout)
        self.assertIn("--quick", result.stdout, "keep --quick for backward compat")
        self.assertIn("已是默认行为", result.stdout)


if __name__ == "__main__":
    unittest.main()
