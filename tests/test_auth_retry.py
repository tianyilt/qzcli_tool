"""Tests for cookie auto-relogin (P0) and CAS login retry-with-backoff (P1)."""

import argparse
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from support.sandbox import sandbox_home  # noqa: E402

from qzcli import api as qz_api
from qzcli.api import QzAPI, QzAPIError, QzTransientError, with_auth_retry
from qzcli.cli import _exec_via_jupyter, cmd_exec, cmd_exec_attach


class WithAuthRetryDecoratorTests(unittest.TestCase):
    """`with_auth_retry`: on 401, relogin once and retry with the fresh cookie."""

    class FakeAPI:
        def __init__(self, relogin_result="new-cookie", auto_relogin=True):
            self._auto_relogin = auto_relogin
            self._relogin_result = relogin_result
            self.relogin_calls = 0
            self.cookies_seen = []

        def _relogin(self, propagate_errors=False, failing_cookie=None):
            # 签名要跟真 QzAPI._relogin 保持一致：with_auth_retry 会把"刚刚失败的
            # 那个 cookie"传进来当去重基准（见 test_relogin_dedup.py）。
            self.relogin_calls += 1
            self.last_failing_cookie = failing_cookie
            return self._relogin_result

        @with_auth_retry
        def fetch(self, job_id, cookie):
            self.cookies_seen.append(cookie)
            if cookie != "new-cookie":
                raise QzAPIError("Cookie 已过期或无效", 401)
            return {"job": job_id}

        @with_auth_retry
        def no_cookie_arg(self):
            self.cookies_seen.append("call")
            if self.relogin_calls == 0:
                raise QzAPIError("Cookie 已过期", 401)
            return "ok"

    def test_retries_with_fresh_cookie(self):
        api = self.FakeAPI()
        result = api.fetch("j1", "stale-cookie")
        self.assertEqual(result, {"job": "j1"})
        self.assertEqual(api.relogin_calls, 1)
        # 第一次用旧 cookie 401，第二次换上 relogin 的新 cookie
        self.assertEqual(api.cookies_seen, ["stale-cookie", "new-cookie"])

    def test_no_cookie_arg_method_retries_after_relogin(self):
        api = self.FakeAPI()
        self.assertEqual(api.no_cookie_arg(), "ok")
        self.assertEqual(api.relogin_calls, 1)

    def test_reraises_when_relogin_unavailable(self):
        api = self.FakeAPI(relogin_result=None)
        with self.assertRaises(QzAPIError) as ctx:
            api.fetch("j1", "stale-cookie")
        self.assertEqual(ctx.exception.code, 401)
        self.assertEqual(api.relogin_calls, 1)

    def test_disabled_auto_relogin_is_noop(self):
        api = self.FakeAPI(auto_relogin=False)
        with self.assertRaises(QzAPIError):
            api.fetch("j1", "stale-cookie")
        self.assertEqual(api.relogin_calls, 0)

    def test_non_401_error_not_retried(self):
        @with_auth_retry
        def fails(self):
            raise QzAPIError("server exploded", 500)

        fake = self.FakeAPI()
        with self.assertRaises(QzAPIError) as ctx:
            fails(fake)
        self.assertEqual(ctx.exception.code, 500)
        self.assertEqual(fake.relogin_calls, 0)


class ReloginTests(unittest.TestCase):
    """`QzAPI._relogin`: persist a fresh cookie via CAS, or no-op without creds."""

    def setUp(self):
        # _relogin 现在带失败冷却（进程内 + CONFIG_DIR 下的冷却文件）。
        # 沙箱把冷却文件挪进临时目录，清理清掉进程内那份 —— 否则本机上一次真实
        # 登录失败会让这些用例读到残留状态而变红（真踩过）。
        self._sandbox = sandbox_home()
        self._sandbox.__enter__()
        self.addCleanup(self._sandbox.__exit__, None, None, None)
        qz_api._clear_relogin_failure()
        self.addCleanup(qz_api._clear_relogin_failure)

    def test_relogin_logs_in_and_persists(self):
        api = QzAPI(username="u", password="p", auth_mode="legacy")
        with patch.object(
            api, "login_with_cas", return_value="fresh"
        ) as mock_login, patch.object(
            qz_api, "get_cookie", return_value={"cookie": "old", "workspace_id": "ws-1"}
        ), patch.object(
            qz_api, "save_cookie"
        ) as mock_save:
            result = api._relogin()
        self.assertEqual(result, "fresh")
        mock_login.assert_called_once_with("u", "p")
        mock_save.assert_called_once_with("fresh", "ws-1")

    def test_relogin_without_credentials_returns_none(self):
        api = QzAPI(username="", password="", auth_mode="legacy")
        api._username, api._password = "", ""
        with patch.object(api, "login_with_cas") as mock_login:
            self.assertIsNone(api._relogin())
        mock_login.assert_not_called()

    def test_relogin_preserves_login_failure(self):
        api = QzAPI(username="u", password="p", auth_mode="legacy")
        with patch.object(
            api, "login_with_cas", side_effect=QzAPIError("nope")
        ), patch.object(
            qz_api, "get_cookie", return_value={"cookie": "old"}
        ), patch.object(
            qz_api, "save_cookie"
        ):
            with self.assertRaisesRegex(QzAPIError, "nope"):
                api._relogin()


class LoginRetryTests(unittest.TestCase):
    """`login_with_cas`: retry transient failures, propagate permanent ones."""

    def setUp(self):
        self.api = QzAPI(username="u", password="p", auth_mode="legacy")

    def test_retries_transient_then_succeeds(self):
        attempts = [QzTransientError("ssl eof"), QzTransientError("503"), "cookie"]

        def fake_once(username, password):
            item = attempts.pop(0)
            if isinstance(item, Exception):
                raise item
            return item

        with patch.object(
            self.api, "_login_with_cas_once", side_effect=fake_once
        ) as mock_once, patch.object(qz_api._time, "sleep") as mock_sleep:
            result = self.api.login_with_cas("u", "p")
        self.assertEqual(result, "cookie")
        self.assertEqual(mock_once.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_permanent_error_not_retried(self):
        with patch.object(
            self.api, "_login_with_cas_once", side_effect=QzAPIError("用户名或密码错误")
        ) as mock_once, patch.object(qz_api._time, "sleep"):
            with self.assertRaises(QzAPIError):
                self.api.login_with_cas("u", "p")
        mock_once.assert_called_once()

    def test_exhausts_retries_then_raises_transient(self):
        with patch.object(
            self.api, "_login_with_cas_once", side_effect=QzTransientError("ssl eof")
        ) as mock_once, patch.object(qz_api._time, "sleep"):
            with self.assertRaises(QzTransientError):
                self.api.login_with_cas("u", "p")
        self.assertEqual(mock_once.call_count, qz_api._LOGIN_MAX_TRIES)


class ExecDetachAttachTests(unittest.TestCase):
    """`exec --detach` launches without polling; `exec-attach` re-polls a job_id."""

    INFO = {"base_url": "u", "token": "t", "notebook_id": "nb-1"}

    def test_detach_launches_and_returns_job_id(self):
        args = argparse.Namespace(
            host="dev", remote_cmd=["sleep", "100"], timeout=120, detach=True
        )
        with patch(
            "qzcli.cli._find_notebook_jupyter_info", return_value=self.INFO
        ), patch(
            "qzcli.cli._exec_launch", return_value="qzcli_42"
        ) as mock_launch, patch(
            "qzcli.cli._exec_poll"
        ) as mock_poll, patch(
            "qzcli.cli.get_display", return_value=MagicMock()
        ):
            rc = cmd_exec(args)
        self.assertEqual(rc, 0)
        mock_launch.assert_called_once()
        mock_poll.assert_not_called()  # detach 不轮询

    def test_attach_polls_existing_job(self):
        args = argparse.Namespace(host="dev", job_id="qzcli_42", timeout=90)
        with patch(
            "qzcli.cli._find_notebook_jupyter_info", return_value=self.INFO
        ), patch(
            "qzcli.cli._exec_poll", return_value=(0, "done", True)
        ) as mock_poll, patch(
            "qzcli.cli.get_display", return_value=MagicMock()
        ):
            rc = cmd_exec_attach(args)
        self.assertEqual(rc, 0)
        self.assertEqual(mock_poll.call_args.args[1], "qzcli_42")

    def test_via_jupyter_timeout_prints_attach_hint(self):
        display = MagicMock()
        with patch("qzcli.cli._exec_launch", return_value="qzcli_7"), patch(
            "qzcli.cli._exec_poll", return_value=(124, "partial", False)
        ):
            exit_code, output = _exec_via_jupyter(self.INFO, "cmd", display, timeout=5)
        self.assertEqual((exit_code, output), (124, "partial"))
        warned = " ".join(str(c.args[0]) for c in display.print_warning.call_args_list)
        self.assertIn("exec-attach", warned)
        self.assertIn("qzcli_7", warned)


if __name__ == "__main__":
    unittest.main()


class CrossProcessReloginLockTests(unittest.TestCase):
    """跨进程重登锁。

    真实事故：多 agent 场景下每次 qzcli 调用都是独立进程，cookie 一过期，
    N 个进程同一瞬间各自去撞 CAS —— CAS 判为异常登录、要求验证码，
    **所有人一起被锁在外面**，连"自动重登"本身也失效。
    进程内的 threading.Lock 完全挡不住这个。
    """

    def setUp(self):
        # _relogin 现在带失败冷却（进程内 + CONFIG_DIR 下的冷却文件）。
        # 沙箱把冷却文件挪进临时目录，清理清掉进程内那份 —— 否则本机上一次真实
        # 登录失败会让这些用例读到残留状态而变红（真踩过）。
        self._sandbox = sandbox_home()
        self._sandbox.__enter__()
        self.addCleanup(self._sandbox.__exit__, None, None, None)
        qz_api._clear_relogin_failure()
        self.addCleanup(qz_api._clear_relogin_failure)

    def _client(self, login_fn):
        import threading

        from qzcli import api as _api

        a = _api.QzAPI.__new__(_api.QzAPI)
        a.auth_mode = "legacy"
        a.base_url = "https://qz.example"
        a._username, a._password = "u", "p"
        a._token = None
        a._auto_relogin = True
        a._relogin_lock = threading.Lock()
        a.login_with_cas = login_fn
        return a

    def test_file_lock_is_acquired_during_relogin(self):
        """重登必须在跨进程锁的保护下进行。"""
        from qzcli import api as _api

        seen = {}

        def fake_login(u, p):
            seen["locked_while_logging_in"] = True
            return "inspire-session=NEW"

        import contextlib

        @contextlib.contextmanager
        def spy_lock():
            seen["lock_used"] = True
            yield True

        with patch.object(_api, "_relogin_file_lock", spy_lock), patch.object(
            _api, "get_cookie", return_value={"cookie": "STALE"}
        ), patch.object(_api, "save_cookie"):
            self._client(fake_login)._relogin()

        self.assertTrue(seen.get("lock_used"), "重登没有走跨进程锁")
        self.assertTrue(seen.get("locked_while_logging_in"))

    def test_rechecks_cookie_after_acquiring_lock(self):
        """拿到锁后要重读盘上的 cookie —— 别的进程可能刚登好了，
        这时应该直接用它的结果，而不是再撞一次 CAS。"""
        from qzcli import api as _api

        calls = []

        def fake_login(u, p):
            calls.append(1)
            return "inspire-session=MINE"

        # 第 1 次读到 STALE（触发重登），拿到锁后第 2/3 次读到别人刚写的新 cookie
        reads = iter(
            [
                {"cookie": "STALE"},
                {"cookie": "STALE"},
                {"cookie": "inspire-session=FROM-OTHER-PROC"},
            ]
        )

        import contextlib

        @contextlib.contextmanager
        def lock():
            yield True

        with patch.object(_api, "_relogin_file_lock", lock), patch.object(
            _api, "get_cookie", side_effect=lambda: next(reads)
        ), patch.object(_api, "save_cookie"), patch.object(_api.QzAPI, "verify_cookie", return_value=True) as verified:
            got = self._client(fake_login)._relogin()

        self.assertEqual(got, "inspire-session=FROM-OTHER-PROC")
        self.assertEqual(calls, [], "别的进程已经登好了，不该再撞一次 CAS")
        verified.assert_called_once_with("inspire-session=FROM-OTHER-PROC", "u")

    def test_lock_helper_yields_and_releases(self):
        from qzcli.api import _relogin_file_lock

        with _relogin_file_lock() as got:
            self.assertTrue(got)
        # 释放后应能立刻再拿到
        with _relogin_file_lock() as got2:
            self.assertTrue(got2)

    def test_lock_failure_does_not_break_command(self):
        """锁文件建不了（只读 HOME 之类）不能让整条命令失败。"""
        from qzcli import api as _api

        with patch("builtins.open", side_effect=OSError("read-only fs")):
            with _api._relogin_file_lock() as got:
                self.assertFalse(got)
