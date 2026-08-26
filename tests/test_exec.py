"""Unit tests for `qzcli exec` — name/UUID/URL resolution + user_id detection."""

import argparse
import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

from qzcli.api import QzAPIError
from qzcli.cli import (
    _detect_user_id_from_probe,
    _extract_notebook_id,
    _resolve_notebook_id_by_name,
    cmd_exec,
)

UUID_REAL = "cfe43e55-e7a1-484a-898c-695596b0877b"
WS_ID = "ws-11111111-1111-4111-8111-111111111111"


class ExtractNotebookIdTests(unittest.TestCase):
    """`_extract_notebook_id` must accept UUID + 5 URL forms; reject plain names."""

    def test_uuid_pass_through(self):
        self.assertEqual(_extract_notebook_id(UUID_REAL), UUID_REAL)

    def test_uuid_uppercase(self):
        self.assertEqual(_extract_notebook_id(UUID_REAL.upper()), UUID_REAL.upper())

    def test_ide_query_url(self):
        url = f"https://qz.sii.edu.cn/ide?notebook_id={UUID_REAL}"
        self.assertEqual(_extract_notebook_id(url), UUID_REAL)

    def test_interactive_model_detail_url(self):
        # Note: the URL form WITHOUT "-ing-" (the one user pasted in this convo)
        url = (
            f"https://qz.sii.edu.cn/jobs/interactiveModelDetail/{UUID_REAL}"
            f"?spaceId={WS_ID}"
        )
        self.assertEqual(_extract_notebook_id(url), UUID_REAL)

    def test_interactive_modeling_detail_url(self):
        # URL form WITH "-ing-" (the form shown in `qzcli list -ci` output)
        url = (
            f"https://qz.sii.edu.cn/jobs/interactiveModelingDetail/{UUID_REAL}"
            f"?spaceId={WS_ID}"
        )
        self.assertEqual(_extract_notebook_id(url), UUID_REAL)

    def test_full_jupyter_url(self):
        url = (
            "https://ai-notebook-inspire.sii.edu.cn/ws-11111111/"
            "project-44444444/user-55555555/"
            f"jupyter/{UUID_REAL}/80f78fda/lab?token=80f78fda"
        )
        self.assertEqual(_extract_notebook_id(url), UUID_REAL)

    def test_api_v1_notebook_lab_url(self):
        url = f"https://qz.sii.edu.cn/api/v1/notebook/lab/{UUID_REAL}"
        self.assertEqual(_extract_notebook_id(url), UUID_REAL)

    def test_notebook_lab_short_url(self):
        url = f"https://example.com/notebook/lab/{UUID_REAL}"
        self.assertEqual(_extract_notebook_id(url), UUID_REAL)

    def test_plain_name_returns_none(self):
        self.assertIsNone(_extract_notebook_id("zzy-dc-ae-dev"))

    def test_empty_and_none(self):
        self.assertIsNone(_extract_notebook_id(""))
        self.assertIsNone(_extract_notebook_id(None))

    def test_uuid_inside_path_segment_not_matched_without_known_prefix(self):
        # 一个 UUID 出现在某个不认识的路径里，不该被随便抽出
        url = f"https://random.example/random/path/{UUID_REAL}/things"
        self.assertIsNone(_extract_notebook_id(url))


class ResolveNotebookIdByNameTests(unittest.TestCase):
    """Name lookup must search list_notebooks WITHOUT user_ids filter."""

    def _fake_api(self, notebooks_per_ws):
        api = MagicMock()
        api.list_notebooks_with_cookie.side_effect = lambda ws, *a, **kw: {
            "list": notebooks_per_ws.get(ws, [])
        }
        return api

    def test_finds_notebook_by_name(self):
        api = self._fake_api(
            {WS_ID: [{"name": "zzy-dc-ae-dev", "notebook_id": UUID_REAL}]}
        )
        with patch("qzcli.cli.get_api", return_value=api), patch(
            "qzcli.cli.load_all_resources", return_value={WS_ID: {"name": "ws"}}
        ):
            display = MagicMock()
            result = _resolve_notebook_id_by_name("zzy-dc-ae-dev", "cookie", display)
        self.assertEqual(result, UUID_REAL)

    def test_does_not_pass_user_ids_filter(self):
        """Regression: ensure user_ids=[] so 协作者 notebooks 不被过滤掉."""
        api = self._fake_api(
            {WS_ID: [{"name": "mschen-mova-dev-copy", "notebook_id": "9ced1457-..."}]}
        )
        with patch("qzcli.cli.get_api", return_value=api), patch(
            "qzcli.cli.load_all_resources", return_value={WS_ID: {"name": "ws"}}
        ):
            display = MagicMock()
            _resolve_notebook_id_by_name("mschen-mova-dev-copy", "cookie", display)
        # 必须传 user_ids=[]，不能传 [some-user-id]
        kwargs = api.list_notebooks_with_cookie.call_args.kwargs
        self.assertEqual(kwargs.get("user_ids"), [])

    def test_not_found_prints_error(self):
        api = self._fake_api({WS_ID: [{"name": "other", "notebook_id": "x"}]})
        with patch("qzcli.cli.get_api", return_value=api), patch(
            "qzcli.cli.load_all_resources", return_value={WS_ID: {"name": "ws"}}
        ):
            display = MagicMock()
            result = _resolve_notebook_id_by_name("nope", "cookie", display)
        self.assertIsNone(result)
        display.print_error.assert_called_once()

    def test_no_cached_workspaces_prints_error(self):
        with patch("qzcli.cli.load_all_resources", return_value={}):
            display = MagicMock()
            result = _resolve_notebook_id_by_name("anything", "cookie", display)
        self.assertIsNone(result)
        display.print_error.assert_called_once()

    def test_workspace_api_error_continues_to_next(self):
        api = MagicMock()
        api.list_notebooks_with_cookie.side_effect = [
            QzAPIError("first ws blew up"),
            {"list": [{"name": "found", "notebook_id": UUID_REAL}]},
        ]
        with patch("qzcli.cli.get_api", return_value=api), patch(
            "qzcli.cli.load_all_resources",
            return_value={"ws-a": {}, "ws-b": {}},
        ):
            display = MagicMock()
            result = _resolve_notebook_id_by_name("found", "cookie", display)
        self.assertEqual(result, UUID_REAL)

    def test_finds_by_exact_notebook_id(self):
        """粘贴完整 notebook_id（名字不匹配时）也能解析。"""
        api = self._fake_api({WS_ID: [{"name": "some-dev", "notebook_id": UUID_REAL}]})
        with patch("qzcli.cli.get_api", return_value=api), patch(
            "qzcli.cli.load_all_resources", return_value={WS_ID: {"name": "ws"}}
        ):
            display = MagicMock()
            result = _resolve_notebook_id_by_name(UUID_REAL, "cookie", display)
        self.assertEqual(result, UUID_REAL)

    def test_finds_by_notebook_id_prefix(self):
        """唯一的 notebook_id 前缀命中即返回。"""
        api = self._fake_api(
            {
                WS_ID: [
                    {"name": "dev-a", "notebook_id": UUID_REAL},
                    {
                        "name": "dev-b",
                        "notebook_id": "9ced1457-1111-2222-3333-444455556666",
                    },
                ]
            }
        )
        with patch("qzcli.cli.get_api", return_value=api), patch(
            "qzcli.cli.load_all_resources", return_value={WS_ID: {"name": "ws"}}
        ):
            display = MagicMock()
            result = _resolve_notebook_id_by_name(UUID_REAL[:8], "cookie", display)
        self.assertEqual(result, UUID_REAL)

    def test_ambiguous_prefix_returns_none(self):
        """前缀撞到多个 notebook_id → 报错、不默默取第一个。"""
        shared = "cfe43e55"
        api = self._fake_api(
            {
                WS_ID: [
                    {
                        "name": "dev-a",
                        "notebook_id": f"{shared}-aaaa-484a-898c-695596b0877b",
                    },
                    {
                        "name": "dev-b",
                        "notebook_id": f"{shared}-bbbb-484a-898c-695596b0877b",
                    },
                ]
            }
        )
        with patch("qzcli.cli.get_api", return_value=api), patch(
            "qzcli.cli.load_all_resources", return_value={WS_ID: {"name": "ws"}}
        ):
            display = MagicMock()
            result = _resolve_notebook_id_by_name(shared, "cookie", display)
        self.assertIsNone(result)
        display.print_error.assert_called_once()


class DetectUserIdFromProbeTests(unittest.TestCase):
    """`_detect_user_id_from_probe` must match by login_name, not 'first job'."""

    USERNAME = "253208120278"
    MY_UID = "user-55555555-0231-4485-ba30-34e92bf3ea53"
    OTHER_UID = "user-66666666-6666-4666-8666-666666666666"

    def _job(self, login_name, uid):
        return {
            "created_by": {
                "id": uid,
                "extra_info": {"login_name": login_name},
            }
        }

    def test_matches_by_login_name(self):
        jobs = [
            self._job("253208125555", self.OTHER_UID),
            self._job(self.USERNAME, self.MY_UID),
            self._job("253208120000", "user-other"),
        ]
        self.assertEqual(_detect_user_id_from_probe(jobs, self.USERNAME), self.MY_UID)

    def test_returns_empty_if_no_match(self):
        """Bug regression: not finding self should NOT return another user's id."""
        jobs = [
            self._job("253208125555", self.OTHER_UID),
            self._job("253208123456", "user-yet-another"),
        ]
        self.assertEqual(_detect_user_id_from_probe(jobs, self.USERNAME), "")

    def test_returns_empty_if_no_username(self):
        jobs = [self._job(self.USERNAME, self.MY_UID)]
        self.assertEqual(_detect_user_id_from_probe(jobs, ""), "")
        self.assertEqual(_detect_user_id_from_probe(jobs, None), "")

    def test_returns_empty_if_no_jobs(self):
        self.assertEqual(_detect_user_id_from_probe([], self.USERNAME), "")

    def test_handles_missing_created_by_fields(self):
        jobs = [
            {},
            {"created_by": None},
            {"created_by": {}},
            {"created_by": {"extra_info": None}},
            self._job(self.USERNAME, self.MY_UID),
        ]
        self.assertEqual(_detect_user_id_from_probe(jobs, self.USERNAME), self.MY_UID)


class CmdExecTimeoutTests(unittest.TestCase):
    """`qzcli exec --timeout N` must plumb N into _exec_via_jupyter."""

    def _make_args(self, host="dev", remote_cmd=("nvidia-smi",), timeout=120):
        return argparse.Namespace(
            host=host, remote_cmd=list(remote_cmd), timeout=timeout
        )

    def test_default_timeout_120(self):
        args = self._make_args()
        with patch(
            "qzcli.cli._find_notebook_jupyter_info",
            return_value={"base_url": "u", "token": "t", "notebook_id": "n"},
        ), patch(
            "qzcli.cli._exec_via_jupyter", return_value=(0, "ok")
        ) as mock_exec, patch(
            "qzcli.cli.get_display", return_value=MagicMock()
        ):
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cmd_exec(args)
        self.assertEqual(rc, 0)
        kwargs = mock_exec.call_args.kwargs
        self.assertEqual(kwargs.get("timeout"), 120)

    def test_custom_timeout_propagates(self):
        args = self._make_args(timeout=600)
        with patch(
            "qzcli.cli._find_notebook_jupyter_info",
            return_value={"base_url": "u", "token": "t", "notebook_id": "n"},
        ), patch(
            "qzcli.cli._exec_via_jupyter", return_value=(0, "")
        ) as mock_exec, patch(
            "qzcli.cli.get_display", return_value=MagicMock()
        ):
            buf = io.StringIO()
            with redirect_stdout(buf):
                cmd_exec(args)
        self.assertEqual(mock_exec.call_args.kwargs.get("timeout"), 600)

    def test_empty_remote_cmd_returns_1_without_calling_exec(self):
        args = self._make_args(remote_cmd=())
        with patch("qzcli.cli._exec_via_jupyter") as mock_exec, patch(
            "qzcli.cli.get_display", return_value=MagicMock()
        ):
            rc = cmd_exec(args)
        self.assertEqual(rc, 1)
        mock_exec.assert_not_called()


class FindNotebookJupyterInfoTests(unittest.TestCase):
    """Integration: UUID/URL should skip name lookup entirely."""

    def test_uuid_skips_list_notebooks(self):
        from qzcli.cli import _find_notebook_jupyter_info

        with patch("qzcli.cli.get_cookie", return_value={"cookie": "c"}), patch(
            "qzcli.cli.get_api"
        ) as get_api, patch(
            "qzcli.cli._get_jupyter_info",
            return_value={"base_url": "u", "token": "t", "notebook_id": UUID_REAL},
        ):
            display = MagicMock()
            result = _find_notebook_jupyter_info(UUID_REAL, display)
        self.assertEqual(result["notebook_id"], UUID_REAL)
        # 关键断言：UUID 路径下根本不该调 list_notebooks_with_cookie
        get_api.assert_not_called()

    def test_url_skips_list_notebooks(self):
        from qzcli.cli import _find_notebook_jupyter_info

        url = f"https://qz.sii.edu.cn/ide?notebook_id={UUID_REAL}"
        with patch("qzcli.cli.get_cookie", return_value={"cookie": "c"}), patch(
            "qzcli.cli.get_api"
        ) as get_api, patch(
            "qzcli.cli._get_jupyter_info",
            return_value={"base_url": "u", "token": "t", "notebook_id": UUID_REAL},
        ):
            display = MagicMock()
            result = _find_notebook_jupyter_info(url, display)
        self.assertEqual(result["notebook_id"], UUID_REAL)
        get_api.assert_not_called()

    def test_name_uses_list_notebooks(self):
        from qzcli.cli import _find_notebook_jupyter_info

        api = MagicMock()
        api.list_notebooks_with_cookie.return_value = {
            "list": [{"name": "dev", "notebook_id": UUID_REAL}]
        }
        with patch("qzcli.cli.get_cookie", return_value={"cookie": "c"}), patch(
            "qzcli.cli.get_api", return_value=api
        ), patch("qzcli.cli.load_all_resources", return_value={WS_ID: {}}), patch(
            "qzcli.cli._get_jupyter_info",
            return_value={"base_url": "u", "token": "t", "notebook_id": UUID_REAL},
        ):
            display = MagicMock()
            result = _find_notebook_jupyter_info("dev", display)
        self.assertEqual(result["notebook_id"], UUID_REAL)
        api.list_notebooks_with_cookie.assert_called_once()

    def test_no_cookie_prints_error(self):
        from qzcli.cli import _find_notebook_jupyter_info

        with patch("qzcli.cli.get_cookie", return_value=None):
            display = MagicMock()
            result = _find_notebook_jupyter_info(UUID_REAL, display)
        self.assertIsNone(result)
        display.print_error.assert_called_once()


class ExecConcurrencyTests(unittest.TestCase):
    """多 agent 并发 exec 同一台开发机时的隔离性。

    这两条都是真机 3 路并发实测出来的：修复前第 3 路收到的是第 2 路的输出、
    第 1 路什么都没收到。
    """

    def _launch(self, n=3):
        """并发跑 n 次 _exec_launch，收集 job_id 和实际用到的 terminal。"""
        from qzcli import cli

        created, deleted, sent = [], [], []

        class _Resp:
            status_code = 200

            def __init__(self, payload):
                self._p = payload

            def json(self):
                return self._p

        def fake_post(url, **_):
            name = f"t{len(created) + 1}"
            created.append(name)
            return _Resp({"name": name})

        def fake_get(url, **_):
            # 故意让"已存在的终端"非空 —— 老实现会去抢 terms[0]
            return _Resp([{"name": "human-session"}])

        def fake_delete(url, **_):
            deleted.append(url.rsplit("/", 1)[-1])
            return _Resp({})

        def fake_put(url, **_):
            return _Resp({})

        class _WS:
            def settimeout(self, *_a):
                pass

            def recv(self):
                raise RuntimeError("drain")

            def send(self, payload):
                sent.append(payload)

            def close(self):
                pass

        info = {"base_url": "https://nb.example/x", "token": "tk"}
        job_ids = []
        with patch("requests.post", side_effect=fake_post), patch(
            "requests.get", side_effect=fake_get
        ), patch("requests.delete", side_effect=fake_delete), patch(
            "requests.put", side_effect=fake_put
        ), patch(
            "websocket.create_connection", return_value=_WS()
        ), patch(
            "time.sleep"
        ):
            for _ in range(n):
                job_ids.append(cli._exec_launch(info, "echo hi", MagicMock()))
        return job_ids, created, deleted, sent

    def test_job_ids_are_unique_within_same_second(self):
        """job_id 决定输出文件名 /tmp/.qzcli/<job_id>_out。原来只有秒级时间戳，
        同一秒内并发的 exec 会拿到同一个 id，输出互相覆盖。"""
        job_ids, _, _, _ = self._launch(5)
        self.assertEqual(len(set(job_ids)), 5, f"job_id 有重复: {job_ids}")

    def test_each_exec_creates_its_own_terminal(self):
        """老实现复用 terms[0]，既会和别的 agent 抢，也会往人开着的
        交互式终端里打字。现在必须每次自建。"""
        _, created, _, _ = self._launch(3)
        self.assertEqual(len(created), 3)
        self.assertEqual(len(set(created)), 3)
        self.assertNotIn("human-session", created)

    def test_terminal_is_cleaned_up(self):
        """不删的话每跑一次就在开发机上攒一个终端。"""
        _, created, deleted, _ = self._launch(3)
        self.assertEqual(sorted(deleted), sorted(created))

    def test_command_is_detached_so_it_survives_terminal_delete(self):
        """终端删掉后命令还得继续跑完，靠 setsid/nohup 摘出去。"""
        _, _, _, sent = self._launch(1)
        payload = sent[0]
        self.assertIn("setsid", payload)
        self.assertIn("nohup", payload)


class SessionIdTests(unittest.TestCase):
    """QZCLI_SESSION_ID 的三级阶梯 + 自动兜底。"""

    def setUp(self):
        from qzcli import config

        config._AUTO_SESSION_ID = None
        self.addCleanup(setattr, config, "_AUTO_SESSION_ID", None)

    def _get(self, env=None, env_file=None, cfg=None):
        from qzcli import config

        with patch.dict("os.environ", env or {}, clear=False), patch.object(
            config, "load_env_file", return_value=env_file or {}
        ), patch.object(config, "load_config", return_value=cfg or {}):
            return config.get_session_id()

    def test_env_wins(self):
        got = self._get(
            env={"QZCLI_SESSION_ID": "from-env"},
            env_file={"QZCLI_SESSION_ID": "from-file"},
            cfg={"session_id": "from-cfg"},
        )
        self.assertEqual(got, "from-env")

    def test_env_file_beats_config(self):
        got = self._get(
            env_file={"QZCLI_SESSION_ID": "from-file"}, cfg={"session_id": "from-cfg"}
        )
        self.assertEqual(got, "from-file")

    def test_auto_when_unset_and_stable_within_process(self):
        """同一进程内必须稳定 —— 否则一个 agent 的多次 exec 会落到不同 session，
        attach/list 就串不起来。"""
        import os

        env = {k: v for k, v in os.environ.items() if k != "QZCLI_SESSION_ID"}
        with patch.dict("os.environ", env, clear=True):
            a = self._get()
            b = self._get()
        self.assertEqual(a, b)
        self.assertRegex(a, r"^[0-9a-f]{8}$")

    def test_unsafe_chars_are_sanitized(self):
        """session 会进目录名和 job_id，不能带 / 和空格。"""
        got = self._get(env={"QZCLI_SESSION_ID": "my agent/01"})
        self.assertNotIn("/", got)
        self.assertNotIn(" ", got)

    def test_all_unsafe_input_still_distinct(self):
        """纯中文之类全非法字符的 session 名，不能被清成空串然后静默退回自动值 ——
        那样用户显式设的 session 会被无视，且两个不同的名字会撞成同一个。"""
        a = self._get(env={"QZCLI_SESSION_ID": "我的会话"})
        b = self._get(env={"QZCLI_SESSION_ID": "另一个会话"})
        self.assertTrue(a)
        self.assertNotEqual(a, b)


class SessionPathTests(unittest.TestCase):
    """job_id ↔ session ↔ 远端路径的互推，含老格式回落。"""

    def test_new_format_carries_session(self):
        from qzcli.cli import _session_of

        self.assertEqual(_session_of("qzcli_abc12345_1785400000_deadbeef"), "abc12345")
        self.assertEqual(
            _session_of("qzcli_my-agent-01_1785400000_deadbeef"), "my-agent-01"
        )

    def test_legacy_formats_have_no_session(self):
        """升级前发出去的 job_id 必须还能认出来，否则老任务 attach 不回来。"""
        from qzcli.cli import _session_of

        self.assertEqual(_session_of("qzcli_1785400000"), "")
        self.assertEqual(_session_of("qzcli_1785400000_deadbeef"), "")
        self.assertEqual(_session_of(""), "")

    def test_paths_are_namespaced_by_session(self):
        from qzcli.cli import _exec_paths

        out, exit_ = _exec_paths("qzcli_sess1_1785400000_deadbeef")
        self.assertEqual(out, "_qzcli/sess1/qzcli_sess1_1785400000_deadbeef_out")
        self.assertEqual(exit_, "_qzcli/sess1/qzcli_sess1_1785400000_deadbeef_exit")

    def test_legacy_paths_stay_flat(self):
        from qzcli.cli import _exec_paths

        out, _ = _exec_paths("qzcli_1785400000")
        self.assertEqual(out, "_qzcli/qzcli_1785400000_out")


class ExecShellCommandTests(unittest.TestCase):
    """下发到远端终端的那条 shell 命令的形状。"""

    def _sent(self, session="sess1"):
        from qzcli import cli

        sent = []

        class _R:
            status_code = 200

            def __init__(self, p=None):
                self._p = p or {"name": "t1"}

            def json(self):
                return self._p

        class _WS:
            def settimeout(self, *_a):
                pass

            def recv(self):
                raise RuntimeError("drain")

            def send(self, payload):
                sent.append(payload)

            def close(self):
                pass

        with patch("requests.post", return_value=_R()), patch(
            "requests.get", return_value=_R([])
        ), patch("requests.delete", return_value=_R()), patch(
            "requests.put", return_value=_R()
        ), patch(
            "websocket.create_connection", return_value=_WS()
        ), patch(
            "time.sleep"
        ), patch.object(
            cli, "get_session_id", return_value=session
        ):
            cli._exec_launch(
                {"base_url": "https://nb.example/x", "token": "tk"},
                "echo hi",
                MagicMock(),
            )
        return sent[0]

    def test_output_goes_into_session_dir(self):
        payload = self._sent()
        self.assertIn("/tmp/.qzcli/sess1/", payload)

    def test_symlink_no_longer_uses_rm_rf(self):
        """老实现是 `rm -rf "$PWD/_qzcli" && ln -sf`，并发下可能把别的 exec
        正在读的目录删掉。"""
        payload = self._sent()
        self.assertNotIn('rm -rf "$PWD', payload)
        self.assertIn("ln -sfn", payload)

    def test_ttl_prune_is_included(self):
        """不清理的话 /tmp/.qzcli 是永久泄漏：没 attach 的、崩的、超时的
        输出文件全留着。"""
        payload = self._sent()
        self.assertIn("-mtime +", payload)
        self.assertIn("find /tmp/.qzcli", payload)

    def test_still_detached(self):
        payload = self._sent()
        self.assertIn("setsid", payload)
        self.assertIn("nohup", payload)


if __name__ == "__main__":
    unittest.main()
