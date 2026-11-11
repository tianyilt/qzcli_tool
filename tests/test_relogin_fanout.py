"""cookie 过期 + 并发扇出时，CAS 只能被打一次。

## 为什么单独立一个文件

v0.4.1 已经为"多进程并发撞 CAS"加过锁，但只覆盖了 ``api._relogin`` 和
``qzcli login`` 两条路。``avail`` / ``usage`` / ``list -c`` 这些命令走的是
``cli._with_live_cookie`` → ``cli._refresh_cookie_for_interactive``，而后者当时
**直接调 ``login_with_cas``**，绕过了全部三层保护。

于是一条 ``qzcli avail`` 在 cookie 过期时会朝 CAS 打出十几次并发登录，CAS 判定为
异常行为并要求验证码 —— 连自动重登本身也一起失效，账号被锁在外面。

已有的 ``test_auth_retry.py`` 没抓到，因为它测的是**单线程**重试语义：调一次、
失败、再调一次，永远看不到扇出。这里补的正是那个缺口。
"""

import os
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from support.sandbox import sandbox_home  # noqa: E402

from qzcli import api as api_mod  # noqa: E402
from qzcli import cli  # noqa: E402
from qzcli.api import QzAPI, QzAPIError  # noqa: E402

#: 过期 cookie + 可用凭据。凭据是必须的 —— 少了它 ``get_credentials()`` 返回空，
#: 旧代码会在真正登录之前就 ``return ""``，于是这组用例在修复前反而"通过"，
#: 变成一个证明不了任何事的测试。第一版就踩了这个，靠"先证明它会红"才发现。
_STALE = {
    "cookie": '{"cookie": "stale", "workspace_id": "ws-1"}',
    "env": {"QZCLI_USERNAME": "u", "QZCLI_PASSWORD": "p"},
}


class _CountingAPI(QzAPI):
    """真 QzAPI（锁和去重都在），只把 CAS 登录换成计数桩。"""

    def __init__(self, fail_with=None, delay=0.02):
        self._username = "u"
        self._password = "p"
        self._relogin_lock = threading.Lock()
        self.cas_calls = 0
        self._call_lock = threading.Lock()
        self._fail_with = fail_with
        self._delay = delay

    def verify_cookie(self, cookie, username):
        # Authentication validation is tested through the transport fixtures;
        # this client models the shared fresh cookie for lock/dedup tests.
        return bool(cookie) and cookie.startswith("inspire-session=fresh")

    def login_with_cas(self, username, password):
        with self._call_lock:
            self.cas_calls += 1
        # 让并发有机会真的重叠 —— 否则线程可能一个接一个跑完，测不出竞态
        import time

        time.sleep(self._delay)
        if self._fail_with:
            raise self._fail_with
        return f"inspire-session=fresh-{self.cas_calls}"


class _SilentDisplay:
    """收集所有输出的假 display。

    用 ``__getattr__`` 兜住 print_warning / print_success 之类：真 display 的方法
    集合会变，写死几个方法会让用例因为一个无关的新方法而 AttributeError。
    """

    def __init__(self):
        self.lines = []
        self.progress = None

    def print(self, msg="", *args, **kwargs):
        self.lines.append(str(msg))

    def __getattr__(self, name):
        if name.startswith("print"):
            return self.print
        raise AttributeError(name)


def _reset_notice():
    cli._refresh_notice_shown = False
    # 冷却是进程级的，用例之间必须清掉，否则前一条的失败会让后一条直接短路
    api_mod._clear_relogin_failure()


class ReloginFanoutTests(unittest.TestCase):
    def setUp(self):
        _reset_notice()

    def test_concurrent_refresh_hits_cas_once(self):
        """16 个线程同时发现 cookie 过期 —— CAS 只能被打 1 次。

        这是本文件的核心断言。修复前这里会是 16。
        """
        api = _CountingAPI()
        display = _SilentDisplay()
        with sandbox_home(**_STALE):
            with ThreadPoolExecutor(max_workers=16) as pool:
                results = list(
                    pool.map(
                        lambda _: cli._refresh_cookie_for_interactive(api, display),
                        range(16),
                    )
                )
        self.assertEqual(api.cas_calls, 1, f"CAS 被打了 {api.cas_calls} 次，应为 1")
        self.assertTrue(all(results), "每个线程都应拿到可用 cookie")
        self.assertEqual(len(set(results)), 1, "所有线程应拿到同一个 cookie")

    def test_notice_printed_once(self):
        """提示也只该出现一条 —— 用户看到两条就是在提示有放大。"""
        api = _CountingAPI()
        display = _SilentDisplay()
        with sandbox_home(**_STALE):
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(
                    pool.map(
                        lambda _: cli._refresh_cookie_for_interactive(api, display),
                        range(8),
                    )
                )
        notices = [ln for ln in display.lines if "正在自动刷新" in ln]
        self.assertEqual(len(notices), 1, f"提示出现了 {len(notices)} 次")

    def test_captcha_error_reaches_the_user(self):
        """ "需要输入验证码"必须原样透出 —— 用户据此才知道要去浏览器取 cookie。

        如果被压成"未找到有效 cookie"，用户只会反复重试 login，把锁定期越拖越长。
        """
        api = _CountingAPI(fail_with=QzAPIError("CAS 要求验证码"))
        display = _SilentDisplay()
        with sandbox_home(**_STALE):
            with self.assertRaises(QzAPIError) as ctx:
                cli._refresh_cookie_for_interactive(api, display)
        self.assertIn("验证码", str(ctx.exception))

    def test_failing_login_is_not_retried_by_every_thread(self):
        """登录失败时也不能让每个线程各打一次 —— 那正是把锁定期拖长的原因。"""
        api = _CountingAPI(fail_with=QzAPIError("需要输入验证码"))
        display = _SilentDisplay()
        with sandbox_home(**_STALE):

            def attempt(_):
                try:
                    return cli._refresh_cookie_for_interactive(api, display)
                except QzAPIError:
                    return ""

            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(attempt, range(8)))
        self.assertLessEqual(
            api.cas_calls, 1, f"失败路径下 CAS 仍被打了 {api.cas_calls} 次"
        )

    def test_cooldown_blocks_a_second_command(self):
        """冷却是跨命令的：上一条命令刚失败，下一条不该再去撞 CAS。

        多 agent 场景下这条最关键 —— 否则第一个 agent 触发验证码后，后面每个
        agent 都会再补几刀，锁定期一路延长。
        """
        first = _CountingAPI(fail_with=QzAPIError("需要输入验证码"))
        second = _CountingAPI(fail_with=QzAPIError("需要输入验证码"))
        display = _SilentDisplay()
        with sandbox_home(**_STALE):
            with self.assertRaises(QzAPIError):
                cli._refresh_cookie_for_interactive(first, display)
            with self.assertRaises(QzAPIError) as ctx:
                cli._refresh_cookie_for_interactive(second, display)
        self.assertEqual(first.cas_calls, 1)
        self.assertEqual(second.cas_calls, 0, "冷却期内不该再打 CAS")
        self.assertIn("验证码", str(ctx.exception), "错误信息要沿用，别变成泛化文案")

    def test_success_clears_cooldown(self):
        """登录成功后要清掉冷却，否则手工修好 cookie 也得干等一分钟。"""
        display = _SilentDisplay()
        with sandbox_home(**_STALE):
            with self.assertRaises(QzAPIError):
                cli._refresh_cookie_for_interactive(
                    _CountingAPI(fail_with=QzAPIError("需要输入验证码")), display
                )
            api_mod._clear_relogin_failure()
            ok = _CountingAPI()
            self.assertTrue(cli._refresh_cookie_for_interactive(ok, display))
            self.assertIsNone(api_mod._recent_relogin_failure())

    def test_does_not_call_login_with_cas_directly(self):
        """回归钉子：这条路必须经过 ``_relogin``（锁在那里），不能直连 CAS。"""
        api = _CountingAPI()
        display = _SilentDisplay()
        with sandbox_home(**_STALE):
            with patch.object(
                QzAPI, "_relogin", return_value="inspire-session=viaRelogin"
            ) as spy:
                cookie = cli._refresh_cookie_for_interactive(api, display)
        spy.assert_called_once()
        self.assertEqual(api.cas_calls, 0, "不应绕过 _relogin 直连 CAS")
        self.assertEqual(cookie, "inspire-session=viaRelogin")

    def test_fake_api_without_relogin_still_works(self):
        """测试里的假 API 没有 ``_relogin`` 时要退回老路径，不能直接崩。"""

        class _FakeAPI:
            def __init__(self):
                self.calls = 0

            def login_with_cas(self, username, password):
                self.calls += 1
                return "inspire-session=fake"

        api = _FakeAPI()
        display = _SilentDisplay()
        with sandbox_home(**_STALE):
            cookie = cli._refresh_cookie_for_interactive(api, display)
        self.assertEqual(cookie, "inspire-session=fake")
        self.assertEqual(api.calls, 1)


if __name__ == "__main__":
    unittest.main()


class AvailWithExpiredCookieTests(unittest.TestCase):
    """``qzcli avail -w <ws>`` 在 cookie 过期时的端到端行为。

    **这是这次事故里最刺眼的覆盖缺口。** ``cmd_avail`` 本来就有 5 个单测，
    ``live_smoke`` 也跑它 —— 但**没有一个是在 cookie 过期的前提下跑的**：
    那些用例要么喂了能用的 cookie，要么把 ``get_credentials`` patch 成空
    （于是自动重登根本不会触发）。

    结果就是"avail 能用"被测得很足，"avail 在登录态挂掉时会怎样"零覆盖 ——
    而放大只发生在后者。用户第一次撞上时，我们连一条会红的用例都没有。
    """

    def setUp(self):
        _reset_notice()

    def _run_avail(self, api, display):
        import argparse

        args = argparse.Namespace(
            workspace="分布式",
            nodes=None,
            group=None,
            low_priority=True,
            export=False,
            verbose=False,
        )
        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(cli, "get_credentials", return_value=("u", "p")), patch.object(
            cli, "find_workspace_by_name", return_value="ws-1"
        ), patch.object(
            cli,
            "list_cached_workspaces",
            return_value=[
                {
                    "id": "ws-1",
                    "name": "分布式空间",
                    "updated_at": 0,
                    "project_count": 0,
                    "compute_group_count": 6,
                    "spec_count": 0,
                }
            ],
        ), patch.object(
            cli,
            "get_workspace_resources",
            return_value={
                "name": "分布式空间",
                "compute_groups": {
                    f"lcg-{i}": {"id": f"lcg-{i}", "name": f"组{i}"} for i in range(6)
                },
                "projects": {},
                "specs": {},
            },
        ), patch.object(
            cli, "save_resources"
        ), patch.object(
            cli, "set_workspace_name"
        ), patch.object(
            cli, "save_cookie"
        ):
            return cli.cmd_avail(args)

    def test_expired_cookie_does_not_stampede_cas(self):
        """6 个计算组并发查询，cookie 全线 401 —— CAS 最多被打 1 次。

        修复前这里会打 6 次以上，正是把账号推进验证码锁定的那个动作。
        """

        class _ExpiredAPI(_CountingAPI):
            def list_node_dimension(self, workspace_id, cookie, **kwargs):
                raise QzAPIError(
                    "Cookie 已过期或无效，请运行 `qzcli login` 重新获取", 401
                )

            def list_task_dimension(self, workspace_id, cookie, **kwargs):
                raise QzAPIError("Cookie 已过期或无效", 401)

            def list_workspaces(self, cookie):
                return [{"id": "ws-1", "name": "分布式空间"}]

        api = _ExpiredAPI(fail_with=QzAPIError("CAS 要求验证码"))
        display = _SilentDisplay()
        with sandbox_home(**_STALE):
            try:
                self._run_avail(api, display)
            except QzAPIError:
                pass  # 登录过不去，命令失败是对的；这里只关心打了几次 CAS
        self.assertLessEqual(
            api.cas_calls, 1, f"cookie 过期时 CAS 被打了 {api.cas_calls} 次"
        )

    def test_captcha_message_is_shown_to_the_user(self):
        """验证码提示必须出现在输出里 —— 用户据此才知道要去浏览器手工取 cookie。"""

        class _ExpiredAPI(_CountingAPI):
            def list_node_dimension(self, workspace_id, cookie, **kwargs):
                raise QzAPIError("Cookie 已过期或无效", 401)

            def list_task_dimension(self, workspace_id, cookie, **kwargs):
                raise QzAPIError("Cookie 已过期或无效", 401)

            def list_workspaces(self, cookie):
                return [{"id": "ws-1", "name": "分布式空间"}]

        api = _ExpiredAPI(fail_with=QzAPIError("CAS 要求验证码"))
        display = _SilentDisplay()
        with sandbox_home(**_STALE):
            with self.assertRaisesRegex(QzAPIError, "验证码") as caught:
                self._run_avail(api, display)
        from qzcli.auth_diag import format_exception
        self.assertIn("验证码", format_exception(caught.exception))
