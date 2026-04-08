import argparse
import sys
import unittest
from unittest.mock import patch

from qzcli import cli
from qzcli.api import QzAPIError

try:
    from prompt_toolkit.application import create_app_session
    from prompt_toolkit.input.defaults import create_pipe_input
    from prompt_toolkit.output import DummyOutput

    PROMPT_TOOLKIT_TEST_AVAILABLE = True
except ImportError:
    create_app_session = None
    create_pipe_input = None
    DummyOutput = None
    PROMPT_TOOLKIT_TEST_AVAILABLE = False

TEST_IMAGE = "registry.example.com/test/train-image:latest"


def build_fixture_value(label: str) -> str:
    return f"fixture-{label}"


class FakeDisplay:
    def __init__(self):
        self.messages = []

    def print(self, *args, **kwargs):
        self.messages.append(" ".join(str(arg) for arg in args))

    def print_error(self, *args, **kwargs):
        self.messages.append(" ".join(str(arg) for arg in args))

    def print_warning(self, *args, **kwargs):
        self.messages.append(" ".join(str(arg) for arg in args))

    def print_success(self, *args, **kwargs):
        self.messages.append(" ".join(str(arg) for arg in args))


class FakeStore:
    def __init__(self):
        self.jobs = []

    def add(self, job):
        self.jobs.append(job)


class ResourceCache:
    def __init__(self):
        self.data = {}

    def save_resources(self, workspace_id, resources, name=""):
        current = self.data.get(workspace_id, {})
        self.data[workspace_id] = {
            "id": workspace_id,
            "name": name or current.get("name", ""),
            "projects": {
                item["id"]: dict(item)
                for item in resources.get("projects", [])
                if item.get("id")
            },
            "compute_groups": {
                item["id"]: dict(item)
                for item in resources.get("compute_groups", [])
                if item.get("id")
            },
            "specs": {
                item["id"]: dict(item)
                for item in resources.get("specs", [])
                if item.get("id")
            },
            "updated_at": 0,
        }

    def get_workspace_resources(self, workspace_id):
        return self.data.get(workspace_id)

    def set_workspace_name(self, workspace_id, name):
        current = self.data.get(workspace_id, {})
        self.data[workspace_id] = {
            "id": workspace_id,
            "name": name,
            "projects": current.get("projects", {}),
            "compute_groups": current.get("compute_groups", {}),
            "specs": current.get("specs", {}),
            "updated_at": 0,
        }
        return True

    def find_workspace_by_name(self, name):
        for workspace_id, data in self.data.items():
            ws_name = data.get("name", "")
            if ws_name == name or (
                name.lower() in ws_name.lower() if ws_name else False
            ):
                return workspace_id
        return None

    def find_resource_by_name(self, workspace_id, resource_type, name):
        resources = self.data.get(workspace_id, {}).get(resource_type, {})
        for item in resources.values():
            item_name = item.get("name", "")
            if item_name == name or (
                name.lower() in item_name.lower() if item_name else False
            ):
                return item
        return None

    def list_cached_workspaces(self):
        return [
            {
                "id": workspace_id,
                "name": data.get("name", ""),
                "updated_at": data.get("updated_at", 0),
                "project_count": len(data.get("projects", {})),
                "compute_group_count": len(data.get("compute_groups", {})),
                "spec_count": len(data.get("specs", {})),
            }
            for workspace_id, data in self.data.items()
        ]


class FakeInteractiveAPI:
    SPEC_ID = "00000000-0000-4000-8000-000000000001"

    def list_workspaces(self, cookie):
        return [{"id": "ws-1", "name": "Alpha Workspace"}]

    def list_jobs_with_cookie(
        self, workspace_id, cookie, page_num=1, page_size=200, created_by=None
    ):
        return {
            "jobs": [
                {
                    "workspace_id": workspace_id,
                    "project_id": "project-1",
                    "project_name": "Vision Project",
                    "logic_compute_group_id": "lcg-1",
                    "logic_compute_group_name": "GPU Group A",
                    "framework_config": [
                        {
                            "instance_spec_price_info": {
                                "quota_id": self.SPEC_ID,
                                "gpu_count": 1,
                                "cpu_count": 12,
                                "memory_size_gib": 80,
                                "gpu_info": {
                                    "gpu_product_simple": "GPU-A",
                                    "gpu_type_display": "Generic GPU A",
                                },
                            }
                        }
                    ],
                }
            ],
            "total": 1,
        }

    def extract_resources_from_jobs(self, jobs):
        return {
            "projects": [
                {
                    "id": "project-1",
                    "name": "Vision Project",
                    "workspace_id": "ws-1",
                }
            ],
            "compute_groups": [
                {
                    "id": "lcg-1",
                    "name": "GPU Group A",
                    "workspace_id": "ws-1",
                    "gpu_type": "GPU-A",
                }
            ],
            "specs": [],
        }

    def get_cluster_basic_info(self, workspace_id, cookie):
        return {
            "compute_groups": [
                {
                    "logic_compute_groups": [
                        {
                            "logic_compute_group_id": "lcg-1",
                            "logic_compute_group_name": "GPU Group A",
                            "brand": "GPU-A",
                            "resource_types": ["GPU-A"],
                        }
                    ]
                }
            ]
        }

    def list_specs(self, compute_group_id):
        return [
            {
                "id": self.SPEC_ID,
                "name": "GPU Spec A x1",
                "gpu_count": 1,
                "cpu_count": 12,
                "memory_size_gib": 80,
                "gpu_info": {
                    "gpu_product_simple": "GPU-A",
                    "gpu_type_display": "Generic GPU A",
                },
            }
        ]

    def list_node_dimension(
        self,
        workspace_id,
        cookie,
        logic_compute_group_id=None,
        compute_group_id=None,
        page_num=1,
        page_size=500,
    ):
        nodes = [
            {
                "name": "node-a",
                "status": "Ready",
                "cordon_type": "",
                "gpu": {"used": 0, "total": 8},
                "logic_compute_group": {"id": "lcg-1", "name": "GPU Group A"},
            },
            {
                "name": "node-b",
                "status": "Ready",
                "cordon_type": "",
                "gpu": {"used": 4, "total": 8},
                "logic_compute_group": {"id": "lcg-1", "name": "GPU Group A"},
            },
        ]
        if logic_compute_group_id:
            nodes = [
                node
                for node in nodes
                if node.get("logic_compute_group", {}).get("id")
                == logic_compute_group_id
            ]
        return {"node_dimensions": nodes}


class BrokenSpecsAPI(FakeInteractiveAPI):
    def list_specs(self, compute_group_id):
        raise ValueError("Extra data: line 1 column 5 (char 4)")


class UnsupportedSpecsAPI(FakeInteractiveAPI):
    def list_specs(self, compute_group_id):
        raise QzAPIError("API 请求失败: 响应不是有效 JSON (HTTP 404)")


class MultiComputeGroupAPI(FakeInteractiveAPI):
    def extract_resources_from_jobs(self, jobs):
        return {
            "projects": [
                {
                    "id": "project-1",
                    "name": "Vision Project",
                    "workspace_id": "ws-1",
                }
            ],
            "compute_groups": [
                {
                    "id": "lcg-1",
                    "name": "Busy Group",
                    "workspace_id": "ws-1",
                    "gpu_type": "GPU-A",
                },
                {
                    "id": "lcg-2",
                    "name": "Free Group",
                    "workspace_id": "ws-1",
                    "gpu_type": "GPU-A",
                },
            ],
            "specs": [],
        }

    def get_cluster_basic_info(self, workspace_id, cookie):
        return {
            "compute_groups": [
                {
                    "logic_compute_groups": [
                        {
                            "logic_compute_group_id": "lcg-1",
                            "logic_compute_group_name": "Busy Group",
                            "brand": "GPU-A",
                            "resource_types": ["GPU-A"],
                        },
                        {
                            "logic_compute_group_id": "lcg-2",
                            "logic_compute_group_name": "Free Group",
                            "brand": "GPU-A",
                            "resource_types": ["GPU-A"],
                        },
                    ]
                }
            ]
        }

    def list_node_dimension(
        self,
        workspace_id,
        cookie,
        logic_compute_group_id=None,
        compute_group_id=None,
        page_num=1,
        page_size=500,
    ):
        nodes = [
            {
                "name": "busy-1",
                "status": "Ready",
                "cordon_type": "",
                "gpu": {"used": 8, "total": 8},
                "logic_compute_group": {"id": "lcg-1", "name": "Busy Group"},
            },
            {
                "name": "free-1",
                "status": "Ready",
                "cordon_type": "",
                "gpu": {"used": 0, "total": 8},
                "logic_compute_group": {"id": "lcg-2", "name": "Free Group"},
            },
            {
                "name": "free-2",
                "status": "Ready",
                "cordon_type": "",
                "gpu": {"used": 4, "total": 8},
                "logic_compute_group": {"id": "lcg-2", "name": "Free Group"},
            },
        ]
        if logic_compute_group_id:
            nodes = [
                node
                for node in nodes
                if node.get("logic_compute_group", {}).get("id")
                == logic_compute_group_id
            ]
        return {"node_dimensions": nodes}


class AutoRefreshAPI(FakeInteractiveAPI):
    def __init__(self):
        self.login_calls = 0

    def login_with_cas(self, username, password):
        self.login_calls += 1
        return "fresh-cookie"

    def list_workspaces(self, cookie):
        if cookie != "fresh-cookie":
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)
        return [{"id": "ws-1", "name": "Alpha Workspace"}]

    def list_node_dimension(
        self,
        workspace_id,
        cookie,
        logic_compute_group_id=None,
        compute_group_id=None,
        page_num=1,
        page_size=500,
    ):
        if cookie != "fresh-cookie":
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)
        return super().list_node_dimension(
            workspace_id,
            cookie,
            logic_compute_group_id=logic_compute_group_id,
            compute_group_id=compute_group_id,
            page_num=page_num,
            page_size=page_size,
        )


class PaginatedResourceAPI(FakeInteractiveAPI):
    def list_jobs_with_cookie(
        self, workspace_id, cookie, page_num=1, page_size=200, created_by=None
    ):
        pages = {
            1: [
                {
                    "workspace_id": workspace_id,
                    "project_id": "project-1",
                    "project_name": "Vision Project",
                    "logic_compute_group_id": "lcg-1",
                    "logic_compute_group_name": "GPU Group A",
                    "framework_config": [
                        {
                            "instance_spec_price_info": {
                                "quota_id": "spec-1",
                                "gpu_count": 1,
                                "cpu_count": 12,
                                "memory_size_gib": 80,
                                "gpu_info": {
                                    "gpu_product_simple": "GPU-A",
                                    "gpu_type_display": "Generic GPU A",
                                },
                            }
                        }
                    ],
                }
            ],
            2: [
                {
                    "workspace_id": workspace_id,
                    "project_id": "project-2",
                    "project_name": "Train Project",
                    "logic_compute_group_id": "lcg-2",
                    "logic_compute_group_name": "GPU Group B",
                    "framework_config": [
                        {
                            "instance_spec_price_info": {
                                "quota_id": "spec-2",
                                "gpu_count": 2,
                                "cpu_count": 24,
                                "memory_size_gib": 160,
                                "gpu_info": {
                                    "gpu_product_simple": "GPU-B",
                                    "gpu_type_display": "Generic GPU B",
                                },
                            }
                        }
                    ],
                }
            ],
        }
        return {"jobs": pages.get(page_num, []), "total": 2}

    def extract_resources_from_jobs(self, jobs):
        projects = []
        compute_groups = []
        specs = []
        for job in jobs:
            projects.append(
                {
                    "id": job["project_id"],
                    "name": job["project_name"],
                    "workspace_id": job["workspace_id"],
                }
            )
            compute_groups.append(
                {
                    "id": job["logic_compute_group_id"],
                    "name": job["logic_compute_group_name"],
                    "workspace_id": job["workspace_id"],
                }
            )
            specs.append(
                {
                    "id": job["framework_config"][0]["instance_spec_price_info"][
                        "quota_id"
                    ],
                    "logic_compute_group_id": job["logic_compute_group_id"],
                    "logic_compute_group_ids": [job["logic_compute_group_id"]],
                }
            )
        return {
            "projects": projects,
            "compute_groups": compute_groups,
            "specs": specs,
        }

    def list_task_dimension(
        self, workspace_id, cookie, project_id=None, page_num=1, page_size=200
    ):
        pages = {
            1: [{"project": {"id": "project-3", "name": "Ops Project"}}],
            2: [],
        }
        return {"task_dimensions": pages.get(page_num, []), "total": 1}

    def get_cluster_basic_info(self, workspace_id, cookie):
        return {
            "compute_groups": [
                {
                    "compute_group_id": "cg-1",
                    "compute_group_name": "Pool-GPU-A",
                    "cluster_id": "cluster-1",
                    "logic_compute_groups": [
                        {
                            "logic_compute_group_id": "lcg-1",
                            "logic_compute_group_name": "GPU Group A",
                            "brand": "GPU-A",
                            "resource_types": ["GPU-A"],
                        }
                    ],
                },
                {
                    "compute_group_id": "cg-2",
                    "compute_group_name": "Pool-GPU-B",
                    "cluster_id": "cluster-1",
                    "logic_compute_groups": [
                        {
                            "logic_compute_group_id": "lcg-2",
                            "logic_compute_group_name": "GPU Group B",
                            "brand": "GPU-B",
                            "resource_types": ["GPU-B"],
                        }
                    ],
                },
            ]
        }


class SpecPrefetchTrackingAPI(MultiComputeGroupAPI):
    def __init__(self):
        self.list_specs_calls = []

    def list_specs(self, compute_group_id):
        self.list_specs_calls.append(compute_group_id)
        return [
            {
                "id": f"spec-{compute_group_id}",
                "name": f"{compute_group_id} Spec",
                "gpu_count": 1,
                "cpu_count": 12,
                "memory_size_gib": 80,
                "gpu_info": {
                    "gpu_product_simple": "GPU-A",
                    "gpu_type_display": "Generic GPU A",
                },
            }
        ]


def build_create_interactive_snapshot(api, cache):
    display = FakeDisplay()

    with patch.object(
        cli, "get_cookie", return_value={"cookie": "cookie"}
    ), patch.object(cli, "save_cookie"), patch.object(
        cli, "get_credentials", return_value=("", "")
    ), patch.object(
        cli, "save_resources", side_effect=cache.save_resources
    ), patch.object(
        cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
    ), patch.object(
        cli, "set_workspace_name", side_effect=cache.set_workspace_name
    ), patch.object(
        cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
    ), patch.object(
        cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
    ):
        return cli._prefetch_create_interactive_snapshot(api, display)


class CreateInteractiveTests(unittest.TestCase):
    def test_main_parses_create_interactive_short_flag(self):
        captured = {}

        def fake_cmd(args):
            captured["interactive"] = args.interactive
            captured["image"] = args.image
            captured["name"] = args.name
            return 0

        with patch.object(cli, "cmd_create", side_effect=fake_cmd):
            with patch.object(
                sys,
                "argv",
                [
                    "qzcli",
                    "create",
                    "-i",
                    "--name",
                    "job",
                    "--command",
                    "echo hi",
                    "--workspace",
                    "ws-1",
                ],
            ):
                ret = cli.main()

        self.assertEqual(0, ret)
        self.assertTrue(captured["interactive"])
        self.assertIsNone(captured["image"])
        self.assertEqual("job", captured["name"])

    def test_main_keeps_legacy_create_image_short_flag_compatible(self):
        captured = {}

        def fake_cmd(args):
            captured["interactive"] = args.interactive
            captured["image"] = args.image
            captured["name"] = args.name
            return 0

        with patch.object(cli, "cmd_create", side_effect=fake_cmd):
            with patch.object(
                sys,
                "argv",
                [
                    "qzcli",
                    "create",
                    "-i",
                    "repo/image:tag",
                    "--name",
                    "job",
                    "--command",
                    "echo hi",
                    "--workspace",
                    "ws-1",
                ],
            ):
                ret = cli.main()

        self.assertEqual(0, ret)
        self.assertFalse(captured["interactive"])
        self.assertEqual("repo/image:tag", captured["image"])
        self.assertEqual("job", captured["name"])

    def test_cmd_create_interactive_fills_missing_fields(self):
        cache = ResourceCache()
        display = FakeDisplay()
        store = FakeStore()
        api = FakeInteractiveAPI()
        snapshot = build_create_interactive_snapshot(api, cache)
        args = argparse.Namespace(
            interactive=True,
            name=None,
            cmd_str=None,
            workspace=None,
            project=None,
            compute_group=None,
            spec=None,
            image=None,
            image_type=None,
            instances=None,
            shm=None,
            priority=None,
            framework=None,
            no_track=True,
            dry_run=True,
            output_json=False,
        )
        user_inputs = iter(
            [
                "1",
                "1",
                "1",
                "1",
                "interactive-job",
                "bash run.sh",
                TEST_IMAGE,
                "",
                "4",
                "",
                "8",
                "",
            ]
        )

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(cli, "get_store", return_value=store), patch.object(
            cli, "get_cookie", return_value={"cookie": "cookie"}
        ), patch.object(
            cli, "save_resources", side_effect=cache.save_resources
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ), patch.object(
            cli, "set_workspace_name", side_effect=cache.set_workspace_name
        ), patch.object(
            cli, "find_workspace_by_name", side_effect=cache.find_workspace_by_name
        ), patch.object(
            cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
        ), patch.object(
            cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
        ), patch.object(
            cli, "load_create_interactive_snapshot", return_value=snapshot
        ), patch(
            "builtins.print"
        ), patch(
            "builtins.input", side_effect=lambda prompt="": next(user_inputs)
        ):
            ret = cli.cmd_create(args)

        self.assertEqual(0, ret)
        self.assertEqual("interactive-job", args.name)
        self.assertEqual("bash run.sh", args.cmd_str)
        self.assertEqual("ws-1", args.workspace)
        self.assertEqual("project-1", args.project)
        self.assertEqual("lcg-1", args.compute_group)
        self.assertEqual(FakeInteractiveAPI.SPEC_ID, args.spec)
        self.assertEqual(TEST_IMAGE, args.image)
        self.assertEqual(cli.DEFAULT_CREATE_IMAGE_TYPE, args.image_type)
        self.assertEqual(4, args.instances)
        self.assertEqual(cli.DEFAULT_CREATE_SHM, args.shm)
        self.assertEqual(8, args.priority)
        self.assertEqual(cli.DEFAULT_CREATE_FRAMEWORK, args.framework)
        self.assertTrue(any("工作空间总览 (1 个)" in msg for msg in display.messages))
        self.assertTrue(
            any("Alpha Workspace" in msg and "12/16" in msg for msg in display.messages)
        )
        self.assertTrue(any("计算组总览 (1 个)" in msg for msg in display.messages))
        self.assertTrue(
            any(
                "GPU Group A" in msg and "逻辑组" in msg and "12/16" in msg
                for msg in display.messages
            )
        )

    def test_format_compute_group_option_marks_shared_pool_usage(self):
        text = cli._format_compute_group_option(
            {
                "id": "lcg-1",
                "name": "GPU资源组",
                "gpu_type": "GPU-C",
                "usage_scope": "shared_physical_pool",
                "total_nodes": 86,
                "free_nodes": 48,
                "total_gpus": 688,
                "free_gpus": 384,
                "gpu_util_ratio": 0.442,
            }
        )
        self.assertIn("共享池占用", text)

    def test_cmd_create_interactive_skips_prompts_for_explicit_args(self):
        display = FakeDisplay()
        store = FakeStore()
        args = argparse.Namespace(
            interactive=True,
            name="explicit-job",
            cmd_str="echo hi",
            workspace="ws-1",
            project="project-1",
            compute_group="lcg-1",
            spec="00000000-0000-4000-8000-000000000001",
            image="repo/image:1",
            image_type="SOURCE_PRIVATE",
            instances=2,
            shm=256,
            priority=7,
            framework="pytorch",
            no_track=True,
            dry_run=True,
            output_json=False,
        )

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=object()
        ), patch.object(cli, "get_store", return_value=store), patch.object(
            cli, "get_cookie", return_value={}
        ), patch.object(
            cli, "get_workspace_resources", return_value={"name": "Alpha Workspace"}
        ), patch(
            "builtins.print"
        ), patch(
            "builtins.input",
            side_effect=AssertionError(
                "interactive mode should not prompt when all args are explicit"
            ),
        ):
            ret = cli.cmd_create(args)

        self.assertEqual(0, ret)

    def test_cmd_create_interactive_rejects_explicit_project_outside_selected_workspace(
        self,
    ):
        cache = ResourceCache()
        display = FakeDisplay()
        store = FakeStore()
        api = FakeInteractiveAPI()
        snapshot = build_create_interactive_snapshot(api, cache)
        args = argparse.Namespace(
            interactive=True,
            name=None,
            cmd_str=None,
            workspace=None,
            project="project-does-not-belong",
            compute_group=None,
            spec=None,
            image=None,
            image_type=None,
            instances=None,
            shm=None,
            priority=None,
            framework=None,
            no_track=True,
            dry_run=True,
            output_json=False,
        )

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(cli, "get_store", return_value=store), patch.object(
            cli, "get_cookie", return_value={"cookie": "cookie"}
        ), patch.object(
            cli, "save_resources", side_effect=cache.save_resources
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ), patch.object(
            cli, "set_workspace_name", side_effect=cache.set_workspace_name
        ), patch.object(
            cli, "find_workspace_by_name", side_effect=cache.find_workspace_by_name
        ), patch.object(
            cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
        ), patch.object(
            cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
        ), patch.object(
            cli, "load_create_interactive_snapshot", return_value=snapshot
        ), patch(
            "builtins.print"
        ), patch(
            "builtins.input", return_value="1"
        ):
            ret = cli.cmd_create(args)

        self.assertEqual(1, ret)
        self.assertTrue(any("不属于当前工作空间" in msg for msg in display.messages))

    def test_list_available_workspaces_silently_falls_back_on_auth_error(self):
        cache = ResourceCache()
        cache.set_workspace_name("ws-1", "Alpha Workspace")
        display = FakeDisplay()

        class ExpiredCookieAPI:
            def list_workspaces(self, cookie):
                raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)

        with patch.object(
            cli, "get_cookie", return_value={"cookie": "expired"}
        ), patch.object(cli, "get_credentials", return_value=("", "")), patch.object(
            cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
        ):
            workspaces = cli._list_available_workspaces(ExpiredCookieAPI(), display)

        self.assertEqual([{"id": "ws-1", "name": "Alpha Workspace"}], workspaces)
        self.assertFalse(
            any(
                "回退到本地缓存" in msg or "使用本地缓存" in msg
                for msg in display.messages
            )
        )

    def test_list_available_workspaces_auto_refreshes_cookie_and_shows_capacity(self):
        display = FakeDisplay()
        api = AutoRefreshAPI()
        cookie_state = {}

        def fake_get_cookie():
            return dict(cookie_state) if cookie_state else {}

        def fake_save_cookie(cookie, workspace_id=""):
            cookie_state["cookie"] = cookie
            cookie_state["workspace_id"] = workspace_id

        with patch.object(cli, "get_cookie", side_effect=fake_get_cookie), patch.object(
            cli, "save_cookie", side_effect=fake_save_cookie
        ), patch.object(
            cli,
            "get_credentials",
            return_value=(
                build_fixture_value("refresh-user"),
                build_fixture_value("refresh-auth"),
            ),
        ), patch.object(
            cli, "list_cached_workspaces", return_value=[]
        ):
            workspaces = cli._list_available_workspaces(api, display)

        self.assertEqual(1, api.login_calls)
        self.assertEqual("fresh-cookie", cookie_state["cookie"])
        self.assertEqual("ws-1", workspaces[0]["id"])
        self.assertEqual(2, workspaces[0]["total_nodes"])
        self.assertEqual(12, workspaces[0]["free_gpus"])

    def test_workspace_selection_table_uses_avail_style_summary_and_headers(self):
        display = FakeDisplay()
        options = [
            {
                "id": "ws-1",
                "name": "Alpha Workspace",
                "total_nodes": 10,
                "free_nodes": 4,
                "total_gpus": 80,
                "free_gpus": 32,
            },
            {
                "id": "ws-2",
                "name": "Beta Workspace",
                "total_nodes": 5,
                "free_nodes": 1,
                "total_gpus": 40,
                "free_gpus": 8,
            },
        ]

        cli._render_workspace_selection_table(display, options)

        text = "\n".join(display.messages)
        self.assertIn("工作空间总览 (2 个)", text)
        self.assertIn("空节点 5/15 | 空GPU 40/120 | GPU利用率 66.7%", text)
        self.assertIn("工作空间", text)
        self.assertIn("空节点", text)
        self.assertIn("GPU利用率", text)
        self.assertIn("Alpha Workspace", text)
        self.assertIn("ws-1", text)

    def test_workspace_selection_sort_prefers_more_free_capacity(self):
        options = [
            {
                "id": "ws-b",
                "name": "Beta",
                "total_nodes": 10,
                "free_nodes": 1,
                "total_gpus": 80,
                "free_gpus": 8,
            },
            {
                "id": "ws-a",
                "name": "Alpha",
                "total_nodes": 10,
                "free_nodes": 4,
                "total_gpus": 80,
                "free_gpus": 32,
            },
            {"id": "ws-c", "name": "CacheOnly"},
        ]

        sorted_options = cli._sort_workspace_options_for_selection(options)

        self.assertEqual(
            ["ws-a", "ws-b", "ws-c"], [item["id"] for item in sorted_options]
        )

    def test_cmd_create_interactive_uses_arrow_selector_when_tty_available(self):
        cache = ResourceCache()
        display = FakeDisplay()
        store = FakeStore()
        api = FakeInteractiveAPI()
        snapshot = build_create_interactive_snapshot(api, cache)
        args = argparse.Namespace(
            interactive=True,
            name=None,
            cmd_str=None,
            workspace=None,
            project=None,
            compute_group=None,
            spec=None,
            image=None,
            image_type=None,
            instances=None,
            shm=None,
            priority=None,
            framework=None,
            no_track=True,
            dry_run=True,
            output_json=False,
        )
        prompts = []
        text_inputs = iter(
            [
                "arrow-job",
                "sleep inf",
                TEST_IMAGE,
                "",
                "",
                "",
                "",
                "",
            ]
        )

        def fake_input(prompt=""):
            prompts.append(prompt)
            return next(text_inputs)

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(cli, "get_store", return_value=store), patch.object(
            cli, "get_cookie", return_value={"cookie": "cookie"}
        ), patch.object(
            cli, "save_resources", side_effect=cache.save_resources
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ), patch.object(
            cli, "set_workspace_name", side_effect=cache.set_workspace_name
        ), patch.object(
            cli, "find_workspace_by_name", side_effect=cache.find_workspace_by_name
        ), patch.object(
            cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
        ), patch.object(
            cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
        ), patch.object(
            cli, "load_create_interactive_snapshot", return_value=snapshot
        ), patch.object(
            cli, "_can_use_arrow_select", return_value=True
        ), patch.object(
            cli,
            "_run_resource_hierarchy_tui",
            return_value={
                "workspace_id": "ws-1",
                "ws_display": "Alpha Workspace",
                "project_id": "project-1",
                "project_display": "Vision Project",
                "compute_group_id": "lcg-1",
                "compute_group_display": "GPU Group A",
                "spec_id": FakeInteractiveAPI.SPEC_ID,
            },
        ) as picker_mock, patch(
            "builtins.print"
        ), patch(
            "builtins.input", side_effect=fake_input
        ):
            ret = cli.cmd_create(args)

        self.assertEqual(0, ret)
        self.assertEqual("arrow-job", args.name)
        self.assertEqual("sleep inf", args.cmd_str)
        self.assertEqual("ws-1", args.workspace)
        self.assertEqual("project-1", args.project)
        self.assertEqual("lcg-1", args.compute_group)
        self.assertEqual(FakeInteractiveAPI.SPEC_ID, args.spec)
        self.assertEqual(1, picker_mock.call_count)
        self.assertTrue(prompts[0].startswith("任务名称"))

    @unittest.skipUnless(
        cli.PROMPT_TOOLKIT_AVAILABLE and PROMPT_TOOLKIT_TEST_AVAILABLE,
        "prompt_toolkit unavailable",
    )
    def test_resource_hierarchy_tui_enter_advances_levels(self):
        snapshot = {
            "workspace_options": [
                {
                    "id": "ws-1",
                    "name": "Alpha Workspace",
                }
            ],
            "workspace_details_by_id": {
                "ws-1": {
                    "id": "ws-1",
                    "name": "Alpha Workspace",
                    "resources": {
                        "projects": {
                            "project-1": {
                                "id": "project-1",
                                "name": "Vision Project",
                            }
                        },
                        "compute_groups": {
                            "lcg-1": {
                                "id": "lcg-1",
                                "name": "GPU Group A",
                                "gpu_type": "GPU-A",
                            }
                        },
                        "specs": {
                            "spec-1": {
                                "id": "spec-1",
                                "name": "GPU Spec A x1",
                                "logic_compute_group_id": "lcg-1",
                                "logic_compute_group_ids": ["lcg-1"],
                                "gpu_count": 1,
                                "cpu_count": 12,
                                "memory_gb": 80,
                                "gpu_type": "GPU-A",
                            }
                        },
                    },
                    "project_options": [
                        {
                            "id": "project-1",
                            "name": "Vision Project",
                        }
                    ],
                    "compute_group_options": [
                        {
                            "id": "lcg-1",
                            "name": "GPU Group A",
                            "gpu_type": "GPU-A",
                            "spec_status": "cache",
                        }
                    ],
                    "spec_result_by_compute_group": {
                        "lcg-1": {
                            "items": [
                                {
                                    "id": "spec-1",
                                    "name": "GPU Spec A x1",
                                    "logic_compute_group_id": "lcg-1",
                                    "logic_compute_group_ids": ["lcg-1"],
                                    "gpu_count": 1,
                                    "cpu_count": 12,
                                    "memory_gb": 80,
                                    "gpu_type": "GPU-A",
                                }
                            ],
                            "status": "cache",
                            "error": None,
                        }
                    },
                }
            },
        }

        with create_pipe_input() as pipe_input:
            with create_app_session(input=pipe_input, output=DummyOutput()):
                pipe_input.send_text("\r\r\r\rq")
                result = cli._run_resource_hierarchy_tui(
                    object(),
                    FakeDisplay(),
                    prefetched_snapshot=snapshot,
                )

        self.assertIsNotNone(result)
        self.assertEqual("ws-1", result["workspace_id"])
        self.assertEqual("project-1", result["project_id"])
        self.assertEqual("lcg-1", result["compute_group_id"])
        self.assertEqual("spec-1", result["spec_id"])

    def test_cmd_create_interactive_with_multiple_compute_groups_uses_capacity_sort(
        self,
    ):
        cache = ResourceCache()
        display = FakeDisplay()
        store = FakeStore()
        api = MultiComputeGroupAPI()
        snapshot = build_create_interactive_snapshot(api, cache)
        args = argparse.Namespace(
            interactive=True,
            name=None,
            cmd_str=None,
            workspace=None,
            project=None,
            compute_group=None,
            spec=None,
            image=None,
            image_type=None,
            instances=None,
            shm=None,
            priority=None,
            framework=None,
            no_track=True,
            dry_run=True,
            output_json=False,
        )
        user_inputs = iter(
            [
                "1",
                "1",
                "1",
                "1",
                "multi-group-job",
                "sleep inf",
                TEST_IMAGE,
                "",
                "",
                "",
                "",
                "",
            ]
        )

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(cli, "get_store", return_value=store), patch.object(
            cli, "get_cookie", return_value={"cookie": "cookie"}
        ), patch.object(
            cli, "save_resources", side_effect=cache.save_resources
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ), patch.object(
            cli, "set_workspace_name", side_effect=cache.set_workspace_name
        ), patch.object(
            cli, "find_workspace_by_name", side_effect=cache.find_workspace_by_name
        ), patch.object(
            cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
        ), patch.object(
            cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
        ), patch.object(
            cli, "load_create_interactive_snapshot", return_value=snapshot
        ), patch(
            "builtins.print"
        ), patch(
            "builtins.input", side_effect=lambda prompt="": next(user_inputs)
        ):
            ret = cli.cmd_create(args)

        self.assertEqual(0, ret)
        self.assertEqual("lcg-2", args.compute_group)
        self.assertTrue(any("计算组总览 (2 个)" in msg for msg in display.messages))
        self.assertTrue(
            any(
                "Free Group" in msg and "12/16" in msg and "逻辑组" in msg
                for msg in display.messages
            )
        )

    def test_compute_group_selection_table_uses_scope_column_and_deduped_summary(self):
        display = FakeDisplay()
        options = [
            {
                "id": "lcg-1",
                "name": "GPU资源组",
                "gpu_type": "GPU-C",
                "usage_scope": "shared_physical_pool",
                "compute_group_id": "cg-1",
                "total_nodes": 86,
                "free_nodes": 48,
                "total_gpus": 688,
                "free_gpus": 384,
            },
            {
                "id": "lcg-2",
                "name": "共享GPU组-B",
                "gpu_type": "GPU-C",
                "usage_scope": "shared_physical_pool",
                "compute_group_id": "cg-1",
                "total_nodes": 86,
                "free_nodes": 48,
                "total_gpus": 688,
                "free_gpus": 384,
            },
            {
                "id": "lcg-cpu",
                "name": "CPU公共区",
                "gpu_type": "CPU",
            },
        ]

        cli._render_compute_group_selection_table(display, options)

        text = "\n".join(display.messages)
        self.assertIn("计算组总览 (3 个)", text)
        self.assertIn(
            "按唯一资源池汇总: 空节点 48/86 | 空GPU 384/688 | GPU利用率 44.2%", text
        )
        self.assertIn("计算组", text)
        self.assertIn("GPU类型", text)
        self.assertIn("占用口径", text)
        self.assertIn("GPU资源组", text)
        self.assertIn("共享池", text)
        self.assertIn("CPU公共区", text)
        self.assertIn("缓存", text)

    def test_compute_group_selection_sort_prefers_more_free_capacity(self):
        options = [
            {
                "id": "lcg-b",
                "name": "Beta",
                "total_nodes": 10,
                "free_nodes": 1,
                "total_gpus": 80,
                "free_gpus": 8,
            },
            {
                "id": "lcg-a",
                "name": "Alpha",
                "total_nodes": 10,
                "free_nodes": 4,
                "total_gpus": 80,
                "free_gpus": 32,
            },
            {"id": "lcg-c", "name": "CacheOnly", "gpu_type": "CPU"},
        ]

        sorted_options = cli._sort_compute_group_options_for_selection(options)

        self.assertEqual(
            ["lcg-a", "lcg-b", "lcg-c"], [item["id"] for item in sorted_options]
        )

    def test_load_specs_for_create_result_marks_error_when_no_cache(self):
        api = BrokenSpecsAPI()

        with patch.object(cli, "get_workspace_resources", return_value=None):
            result = cli._load_specs_for_create_result(
                api, "ws-1", "Alpha Workspace", "lcg-1", emit_messages=False
            )

        self.assertEqual("error", result["status"])
        self.assertEqual([], result["items"])
        self.assertIn("Extra data", result["error"])

    def test_load_specs_for_create_result_silently_ignores_unsupported_spec_endpoint(
        self,
    ):
        api = UnsupportedSpecsAPI()

        with patch.object(cli, "get_workspace_resources", return_value=None):
            result = cli._load_specs_for_create_result(
                api, "ws-1", "Alpha Workspace", "lcg-1", emit_messages=False
            )

        self.assertEqual("empty", result["status"])
        self.assertEqual([], result["items"])
        self.assertIsNone(result["error"])

    def test_load_specs_for_create_result_falls_back_to_cache(self):
        cache = ResourceCache()
        cache.save_resources(
            "ws-1",
            {
                "projects": [],
                "compute_groups": [
                    {
                        "id": "lcg-1",
                        "name": "GPU Group A",
                    }
                ],
                "specs": [
                    {
                        "id": "spec-cached",
                        "name": "Cached Spec",
                        "gpu_count": 1,
                        "cpu_count": 12,
                        "memory_gb": 80,
                        "gpu_type": "GPU-A",
                    }
                ],
            },
            "Alpha Workspace",
        )
        api = BrokenSpecsAPI()

        with patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ):
            result = cli._load_specs_for_create_result(
                api, "ws-1", "Alpha Workspace", "lcg-1", emit_messages=False
            )

        self.assertEqual("cache", result["status"])
        self.assertEqual(["spec-cached"], [item["id"] for item in result["items"]])
        self.assertIn("Extra data", result["error"])

    def test_load_specs_for_create_result_filters_out_specs_from_other_compute_groups(
        self,
    ):
        cache = ResourceCache()
        cache.save_resources(
            "ws-1",
            {
                "projects": [],
                "compute_groups": [
                    {"id": "lcg-1", "name": "GPU Group A"},
                    {"id": "lcg-2", "name": "GPU Group B"},
                ],
                "specs": [
                    {
                        "id": "spec-a100",
                        "name": "GPU Spec A x1",
                        "logic_compute_group_id": "lcg-1",
                        "logic_compute_group_ids": ["lcg-1"],
                        "gpu_count": 1,
                        "cpu_count": 12,
                        "memory_gb": 80,
                        "gpu_type": "GPU-A",
                    },
                    {
                        "id": "spec-h100",
                        "name": "GPU Spec B x1",
                        "logic_compute_group_id": "lcg-2",
                        "logic_compute_group_ids": ["lcg-2"],
                        "gpu_count": 1,
                        "cpu_count": 24,
                        "memory_gb": 160,
                        "gpu_type": "GPU-B",
                    },
                ],
            },
            "Alpha Workspace",
        )
        api = BrokenSpecsAPI()

        with patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ):
            result = cli._load_specs_for_create_result(
                api, "ws-1", "Alpha Workspace", "lcg-1", emit_messages=False
            )

        self.assertEqual("cache", result["status"])
        self.assertEqual(["spec-a100"], [item["id"] for item in result["items"]])
        self.assertIn("Extra data", result["error"])

    def test_compute_group_choice_table_includes_spec_status_column(self):
        header_lines, row_lines = cli._build_compute_group_choice_table(
            [
                {
                    "id": "lcg-1",
                    "name": "Realtime Group",
                    "gpu_type": "GPU-A",
                    "spec_status": "realtime",
                    "total_nodes": 2,
                    "free_nodes": 1,
                    "total_gpus": 16,
                    "free_gpus": 8,
                },
                {
                    "id": "lcg-2",
                    "name": "Broken Group",
                    "gpu_type": "GPU-A",
                    "spec_status": "error",
                },
            ]
        )

        self.assertTrue(any("规格" in line for line in header_lines))
        self.assertTrue(any("实时" in line for line in row_lines))
        self.assertTrue(any("异常" in line for line in row_lines))

    def test_cmd_create_interactive_falls_back_to_manual_spec_input_when_spec_query_breaks(
        self,
    ):
        cache = ResourceCache()
        display = FakeDisplay()
        store = FakeStore()
        api = BrokenSpecsAPI()
        snapshot = build_create_interactive_snapshot(api, cache)
        args = argparse.Namespace(
            interactive=True,
            name=None,
            cmd_str=None,
            workspace=None,
            project=None,
            compute_group=None,
            spec=None,
            image=None,
            image_type=None,
            instances=None,
            shm=None,
            priority=None,
            framework=None,
            no_track=True,
            dry_run=True,
            output_json=False,
        )
        user_inputs = iter(
            [
                "1",
                "1",
                "1",
                "manual-spec-id",
                "broken-spec-job",
                "sleep inf",
                TEST_IMAGE,
                "",
                "",
                "",
                "",
                "",
            ]
        )

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(cli, "get_store", return_value=store), patch.object(
            cli, "get_cookie", return_value={"cookie": "cookie"}
        ), patch.object(
            cli, "save_resources", side_effect=cache.save_resources
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ), patch.object(
            cli, "set_workspace_name", side_effect=cache.set_workspace_name
        ), patch.object(
            cli, "find_workspace_by_name", side_effect=cache.find_workspace_by_name
        ), patch.object(
            cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
        ), patch.object(
            cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
        ), patch.object(
            cli, "load_create_interactive_snapshot", return_value=snapshot
        ), patch(
            "builtins.print"
        ), patch(
            "builtins.input", side_effect=lambda prompt="": next(user_inputs)
        ):
            ret = cli.cmd_create(args)

        self.assertEqual(0, ret)
        self.assertEqual("manual-spec-id", args.spec)
        self.assertTrue(any("获取实时规格列表失败" in msg for msg in display.messages))
        self.assertTrue(any("请手动输入 spec ID" in msg for msg in display.messages))

    def test_prompt_text_input_handles_keyboard_interrupt(self):
        display = FakeDisplay()

        with patch("builtins.input", side_effect=KeyboardInterrupt):
            value = cli._prompt_text_input(display, "任务名称")

        self.assertIsNone(value)
        self.assertTrue(any("交互输入已中断" in msg for msg in display.messages))

    def test_refresh_workspace_resources_for_create_fetches_all_job_pages_and_task_projects(
        self,
    ):
        cache = ResourceCache()
        display = FakeDisplay()
        api = PaginatedResourceAPI()

        with patch.object(
            cli, "get_cookie", return_value={"cookie": "cookie"}
        ), patch.object(cli, "save_cookie"), patch.object(
            cli, "get_credentials", return_value=("", "")
        ), patch.object(
            cli, "save_resources", side_effect=cache.save_resources
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ):
            resources = cli._refresh_workspace_resources_for_create(
                api, display, "ws-1", "Alpha Workspace"
            )

        self.assertIsNotNone(resources)
        self.assertEqual(
            {"project-1", "project-2", "project-3"},
            set(resources["projects"].keys()),
        )
        self.assertEqual({"lcg-1", "lcg-2"}, set(resources["compute_groups"].keys()))
        self.assertEqual({"spec-1", "spec-2"}, set(resources["specs"].keys()))

    def test_prefetch_create_interactive_snapshot_prefetches_specs_for_all_compute_groups(
        self,
    ):
        cache = ResourceCache()
        api = SpecPrefetchTrackingAPI()
        snapshot = build_create_interactive_snapshot(api, cache)

        self.assertEqual({"ws-1"}, set(snapshot["workspace_details_by_id"].keys()))
        self.assertEqual({"lcg-1", "lcg-2"}, set(api.list_specs_calls))
        self.assertEqual(2, len(api.list_specs_calls))

    def test_cmd_create_interactive_uses_prefetched_snapshot_without_followup_live_queries(
        self,
    ):
        cache = ResourceCache()
        display = FakeDisplay()
        store = FakeStore()
        args = argparse.Namespace(
            interactive=True,
            name=None,
            cmd_str=None,
            workspace=None,
            project=None,
            compute_group=None,
            spec=None,
            image=None,
            image_type=None,
            instances=None,
            shm=None,
            priority=None,
            framework=None,
            no_track=True,
            dry_run=True,
            output_json=False,
        )
        snapshot = {
            "workspace_options": [
                {
                    "id": "ws-1",
                    "name": "Alpha Workspace",
                    "total_nodes": 2,
                    "free_nodes": 1,
                    "total_gpus": 16,
                    "free_gpus": 8,
                }
            ],
            "workspace_details_by_id": {
                "ws-1": {
                    "id": "ws-1",
                    "name": "Alpha Workspace",
                    "resources": {
                        "projects": {
                            "project-1": {
                                "id": "project-1",
                                "name": "Vision Project",
                            }
                        },
                        "compute_groups": {
                            "lcg-1": {
                                "id": "lcg-1",
                                "name": "GPU Group A",
                                "gpu_type": "GPU-A",
                            }
                        },
                        "specs": {
                            "spec-1": {
                                "id": "spec-1",
                                "name": "GPU Spec A x1",
                                "logic_compute_group_id": "lcg-1",
                                "logic_compute_group_ids": ["lcg-1"],
                                "gpu_count": 1,
                                "cpu_count": 12,
                                "memory_gb": 80,
                                "gpu_type": "GPU-A",
                            }
                        },
                    },
                    "project_options": [
                        {
                            "id": "project-1",
                            "name": "Vision Project",
                        }
                    ],
                    "compute_group_options": [
                        {
                            "id": "lcg-1",
                            "name": "GPU Group A",
                            "gpu_type": "GPU-A",
                            "spec_status": "cache",
                            "total_nodes": 2,
                            "free_nodes": 1,
                            "total_gpus": 16,
                            "free_gpus": 8,
                        }
                    ],
                    "spec_result_by_compute_group": {
                        "lcg-1": {
                            "items": [
                                {
                                    "id": "spec-1",
                                    "name": "GPU Spec A x1",
                                    "logic_compute_group_id": "lcg-1",
                                    "logic_compute_group_ids": ["lcg-1"],
                                    "gpu_count": 1,
                                    "cpu_count": 12,
                                    "memory_gb": 80,
                                    "gpu_type": "GPU-A",
                                }
                            ],
                            "status": "cache",
                            "error": None,
                        }
                    },
                }
            },
        }
        cache.save_resources(
            "ws-1",
            {
                "projects": [
                    {
                        "id": "project-1",
                        "name": "Vision Project",
                    }
                ],
                "compute_groups": [
                    {
                        "id": "lcg-1",
                        "name": "GPU Group A",
                        "gpu_type": "GPU-A",
                    }
                ],
                "specs": [
                    {
                        "id": "spec-1",
                        "name": "GPU Spec A x1",
                        "logic_compute_group_id": "lcg-1",
                        "logic_compute_group_ids": ["lcg-1"],
                        "gpu_count": 1,
                        "cpu_count": 12,
                        "memory_gb": 80,
                        "gpu_type": "GPU-A",
                    }
                ],
            },
            "Alpha Workspace",
        )
        user_inputs = iter(
            [
                "1",
                "1",
                "1",
                "1",
                "snapshot-job",
                "sleep inf",
                TEST_IMAGE,
                "",
                "",
                "",
                "",
                "",
            ]
        )

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=object()
        ), patch.object(cli, "get_store", return_value=store), patch.object(
            cli, "get_cookie", return_value={"cookie": "cookie"}
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ), patch.object(
            cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
        ), patch.object(
            cli, "load_create_interactive_snapshot", return_value=snapshot
        ) as load_snapshot_mock, patch(
            "builtins.print"
        ), patch(
            "builtins.input", side_effect=lambda prompt="": next(user_inputs)
        ):
            ret = cli.cmd_create(args)

        self.assertEqual(0, ret)
        self.assertEqual(1, load_snapshot_mock.call_count)
        self.assertEqual("ws-1", args.workspace)
        self.assertEqual("project-1", args.project)
        self.assertEqual("lcg-1", args.compute_group)
        self.assertEqual("spec-1", args.spec)
        self.assertFalse(
            any("正在获取工作空间实时资源占用" in msg for msg in display.messages)
        )
        self.assertFalse(
            any("正在获取计算组实时资源占用" in msg for msg in display.messages)
        )

    def test_cmd_create_interactive_prefetches_snapshot_when_missing(self):
        cache = ResourceCache()
        display = FakeDisplay()
        store = FakeStore()
        api = FakeInteractiveAPI()
        args = argparse.Namespace(
            interactive=True,
            name=None,
            cmd_str=None,
            workspace=None,
            project=None,
            compute_group=None,
            spec=None,
            image=None,
            image_type=None,
            instances=None,
            shm=None,
            priority=None,
            framework=None,
            no_track=True,
            dry_run=True,
            output_json=False,
        )
        user_inputs = iter(
            [
                "1",
                "1",
                "1",
                "1",
                "prefetch-job",
                "echo hi",
                TEST_IMAGE,
                "",
                "",
                "",
                "",
                "",
            ]
        )

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(cli, "get_store", return_value=store), patch.object(
            cli, "get_cookie", return_value={"cookie": "cookie"}
        ), patch.object(
            cli, "save_cookie"
        ), patch.object(
            cli, "get_credentials", return_value=("", "")
        ), patch.object(
            cli, "save_resources", side_effect=cache.save_resources
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ), patch.object(
            cli, "set_workspace_name", side_effect=cache.set_workspace_name
        ), patch.object(
            cli, "find_workspace_by_name", side_effect=cache.find_workspace_by_name
        ), patch.object(
            cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
        ), patch.object(
            cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
        ), patch.object(
            cli, "save_create_interactive_snapshot"
        ) as save_snapshot_mock, patch.object(
            cli, "load_create_interactive_snapshot", return_value=None
        ), patch(
            "builtins.input", side_effect=lambda prompt="": next(user_inputs)
        ), patch(
            "builtins.print"
        ):
            ret = cli.cmd_create(args)

        self.assertEqual(0, ret)
        self.assertEqual("ws-1", args.workspace)
        self.assertEqual("project-1", args.project)
        self.assertEqual("lcg-1", args.compute_group)
        self.assertEqual(FakeInteractiveAPI.SPEC_ID, args.spec)
        self.assertTrue(save_snapshot_mock.called)
        self.assertTrue(any("正在按需预加载" in msg for msg in display.messages))

    def test_load_required_create_interactive_snapshot_cleans_unsupported_spec_error(
        self,
    ):
        display = FakeDisplay()
        snapshot = {
            "workspace_options": [{"id": "ws-1", "name": "Alpha Workspace"}],
            "workspace_details_by_id": {
                "ws-1": {
                    "spec_result_by_compute_group": {
                        "lcg-1": {
                            "items": [],
                            "status": "error",
                            "error": "API 请求失败: 响应不是有效 JSON (HTTP 404)",
                        }
                    }
                }
            },
        }

        with patch.object(
            cli, "load_create_interactive_snapshot", return_value=snapshot
        ):
            loaded = cli._load_required_create_interactive_snapshot(display)

        self.assertIsNotNone(loaded)
        spec_result = loaded["workspace_details_by_id"]["ws-1"][
            "spec_result_by_compute_group"
        ]["lcg-1"]
        self.assertEqual("empty", spec_result["status"])
        self.assertIsNone(spec_result["error"])

    def test_cmd_avail_does_not_prefetch_create_snapshot_or_specs(self):
        cache = ResourceCache()
        display = FakeDisplay()
        api = SpecPrefetchTrackingAPI()
        args = argparse.Namespace(
            workspace=None,
            nodes=None,
            group=None,
            low_priority=False,
            export=False,
            verbose=False,
        )

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(
            cli, "get_cookie", return_value={"cookie": "cookie"}
        ), patch.object(
            cli, "save_cookie"
        ), patch.object(
            cli, "get_credentials", return_value=("", "")
        ), patch.object(
            cli, "save_resources", side_effect=cache.save_resources
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ), patch.object(
            cli, "set_workspace_name", side_effect=cache.set_workspace_name
        ), patch.object(
            cli, "find_workspace_by_name", side_effect=cache.find_workspace_by_name
        ), patch.object(
            cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
        ), patch.object(
            cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
        ), patch.object(
            cli, "save_create_interactive_snapshot"
        ) as save_snapshot_mock:
            ret = cli.cmd_avail(args)

        self.assertEqual(0, ret)
        self.assertEqual([], api.list_specs_calls)
        save_snapshot_mock.assert_not_called()
        self.assertFalse(
            any("create -i 资源快照已更新" in msg for msg in display.messages)
        )

    def test_cmd_avail_reuses_refreshed_cookie_for_capacity_queries(self):
        cache = ResourceCache()
        display = FakeDisplay()
        api = AutoRefreshAPI()
        cookie_state = {"cookie": "expired"}
        args = argparse.Namespace(
            workspace=None,
            nodes=None,
            group=None,
            low_priority=False,
            export=False,
            verbose=False,
        )

        def fake_get_cookie():
            return dict(cookie_state)

        def fake_save_cookie(cookie, workspace_id=""):
            cookie_state["cookie"] = cookie
            cookie_state["workspace_id"] = workspace_id

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(cli, "get_cookie", side_effect=fake_get_cookie), patch.object(
            cli, "save_cookie", side_effect=fake_save_cookie
        ), patch.object(
            cli,
            "get_credentials",
            return_value=(
                build_fixture_value("avail-user"),
                build_fixture_value("avail-auth"),
            ),
        ), patch.object(
            cli, "save_resources", side_effect=cache.save_resources
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ), patch.object(
            cli, "set_workspace_name", side_effect=cache.set_workspace_name
        ), patch.object(
            cli, "find_workspace_by_name", side_effect=cache.find_workspace_by_name
        ), patch.object(
            cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
        ), patch.object(
            cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
        ), patch.object(
            cli, "save_create_interactive_snapshot"
        ):
            ret = cli.cmd_avail(args)

        self.assertEqual(0, ret)
        self.assertEqual(1, api.login_calls)
        self.assertEqual("fresh-cookie", cookie_state["cookie"])

    def test_cmd_avail_auto_refreshes_when_cookie_missing(self):
        cache = ResourceCache()
        display = FakeDisplay()
        api = AutoRefreshAPI()
        cookie_state = {}
        args = argparse.Namespace(
            workspace=None,
            nodes=None,
            group=None,
            low_priority=False,
            export=False,
            verbose=False,
        )

        def fake_get_cookie():
            return dict(cookie_state) if cookie_state else {}

        def fake_save_cookie(cookie, workspace_id=""):
            cookie_state["cookie"] = cookie
            cookie_state["workspace_id"] = workspace_id

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(cli, "get_cookie", side_effect=fake_get_cookie), patch.object(
            cli, "save_cookie", side_effect=fake_save_cookie
        ), patch.object(
            cli,
            "get_credentials",
            return_value=(
                build_fixture_value("missing-cookie-user"),
                build_fixture_value("missing-cookie-auth"),
            ),
        ), patch.object(
            cli, "save_resources", side_effect=cache.save_resources
        ), patch.object(
            cli, "get_workspace_resources", side_effect=cache.get_workspace_resources
        ), patch.object(
            cli, "set_workspace_name", side_effect=cache.set_workspace_name
        ), patch.object(
            cli, "find_workspace_by_name", side_effect=cache.find_workspace_by_name
        ), patch.object(
            cli, "find_resource_by_name", side_effect=cache.find_resource_by_name
        ), patch.object(
            cli, "list_cached_workspaces", side_effect=cache.list_cached_workspaces
        ), patch.object(
            cli, "save_create_interactive_snapshot"
        ):
            ret = cli.cmd_avail(args)

        self.assertEqual(0, ret)
        self.assertEqual(1, api.login_calls)
        self.assertEqual("fresh-cookie", cookie_state["cookie"])


if __name__ == "__main__":
    unittest.main()
