"""重登去重：同一次失效只能换来**一次** CAS 登录。

## 病理

``_relogin`` 的去重判据原本是「盘上 cookie **相对我进函数那一刻**有没有变过」::

    stale = (get_cookie() or {}).get("cookie")      # 进函数时读
    ...
    if current and current != stale:
        return current

分页函数把 cookie 闭包进去（``cli.py:_fetch_all_node_dimensions``），整个循环共用
一个字符串。于是：

1. 第 1 页 401 → 重登成功 → 盘上换成新 cookie
2. 第 2 页**仍用闭包里的旧 cookie** → 又 401
3. 进 ``_relogin`` 时 ``stale`` 读到的已经是**新** cookie，``current == stale``
   → 去重判定为"没人刷新过" → **再打一次完整 CAS**

N 页 = N 次登录。而 CAS 正是按登录次数判定异常行为并锁验证码 —— 这就是"用着用着
就说要验证码"的直接来源。

正确判据是「盘上 cookie ≠ **刚刚失败的那个**」，所以 ``_relogin`` 需要知道是哪个
cookie 失败了。

## 参考

inspire-skill 用另一种方式绕开同一个坑：它的凭据是**可变对象**（``WebSession``），
重登后 ``_refresh_session_in_place`` 原地改写调用方手里那个对象，所以任何持有引用
的循环下次自然就用上新 cookie。qzcli 全链路传裸字符串，改造面太大，因此这里用等价
的最小手段：判据认"失败的那个"，且分页每页从磁盘重读。
"""

import os
import sys
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from support.sandbox import sandbox_home  # noqa: E402

from qzcli import api as api_mod  # noqa: E402
from qzcli import cli  # noqa: E402
from qzcli.api import QzAPI, QzAPIError  # noqa: E402

_STALE_COOKIE = "inspire-session=stale"
_SANDBOX = {
    "cookie": '{"cookie": "%s", "workspace_id": "ws-1"}' % _STALE_COOKIE,
    "env": {"QZCLI_USERNAME": "u", "QZCLI_PASSWORD": "p"},
}


class _CountingAPI(QzAPI):
    """真 QzAPI（锁 / 去重 / 冷却都在），只把 CAS 登录换成计数桩。"""

    def __init__(self):
        self._username = "u"
        self._password = "p"
        self._relogin_lock = threading.Lock()
        self._auto_relogin = True
        self.base_url = "https://qz.sii.edu.cn"
        self.cas_calls = 0
        self.unauthorized_calls = 0
        self._lock = threading.Lock()

    def login_with_cas(self, username, password):
        with self._lock:
            self.cas_calls += 1
            n = self.cas_calls
        # 让并发用例里的线程有机会真正重叠。桩若瞬时返回，8 个线程会退化成串行执行，
        # 测到的就不是并发去重而是"碰巧没撞上"。
        time.sleep(0.03)
        return f"inspire-session=fresh-{n}"

    def get_user_detail(self, cookie: str = ""):
        """鉴权探针的桩。**必须覆盖，否则这个测试会自己制造一次多余的登录。**

        ``ensure_authenticated`` 会拿它当探针。不覆盖的话它走真实 ``_request_v2``、
        带着本文件里那些假 cookie 去打真网络，必然 401 —— 于是重登一次、换上
        ``fresh-1``、探针再撞一次 401、再登一次，``cas_calls`` 凭空变成 2，
        看起来像「分页时登录了两次」的去重缺陷。

        实际排查过：三次 ``_relogin`` 全在 MainThread（worker 隔离是好的），
        第三次的 ``failing_cookie`` 是刚换上的 ``fresh-1`` —— 只有真实网络调用
        才会拒绝它。桩掉之后立刻恢复成 1 次。

        所以这是**测试自身的缺陷，不是生产 bug**：生产里探针拿的是真 cookie，
        重登一次就成功了。
        """
        return {"id": "u-1", "name": "test-user"}


def _reset():
    api_mod._clear_relogin_failure()
    cli._refresh_notice_shown = False


class PaginationReloginTests(unittest.TestCase):
    """分页循环：整轮只能有一次 CAS 登录。"""

    def setUp(self):
        _reset()
        self.addCleanup(_reset)

    def _paging_api(self, pages=5):
        """节点分页 API：凡是拿旧 cookie 来的都回 401。"""

        class _API(_CountingAPI):
            outer = self

            @api_mod.with_auth_retry
            def list_node_dimension(
                self,
                workspace_id,
                cookie,
                logic_compute_group_id=None,
                compute_group_id=None,
                page_num=1,
                page_size=500,
            ):
                if cookie == _STALE_COOKIE:
                    with self._lock:
                        self.unauthorized_calls += 1
                    raise QzAPIError("Cookie 已过期或无效", 401)
                return {
                    "node_dimensions": [
                        {"node_name": f"n{page_num}-{i}"} for i in range(2)
                    ],
                    "total": pages * 2,
                }

        return _API()

    def test_five_pages_trigger_one_cas_login(self):
        """5 页分页 + cookie 过期 → CAS 只能被打 1 次。

        修复前是 5 次（每页一次完整 CAS 登录）。
        """
        api = self._paging_api(pages=5)
        with sandbox_home(**_SANDBOX):
            nodes = cli._fetch_all_node_dimensions(
                api, "ws-1", _STALE_COOKIE, page_size=2
            )
        self.assertEqual(
            api.cas_calls, 1, f"5 页分页打了 {api.cas_calls} 次 CAS 登录，应为 1"
        )
        self.assertTrue(nodes, "分页仍应正常取到数据")

    def test_later_pages_do_not_even_hit_401(self):
        """第 2 页开始连 401 都不该撞 —— 说明分页确实用上了刷新后的 cookie。

        只改去重判据能把 CAS 压到 1 次，但每页仍会白撞一次 401；分页改成每页取当前
        cookie 之后，401 也只剩第 1 页那一次。
        """
        api = self._paging_api(pages=5)
        with sandbox_home(**_SANDBOX):
            cli._fetch_all_node_dimensions(api, "ws-1", _STALE_COOKIE, page_size=2)
        self.assertEqual(
            api.unauthorized_calls,
            1,
            f"白撞了 {api.unauthorized_calls} 次 401，只该有第 1 页那一次",
        )

    def test_task_dimension_pagination_too(self):
        """``fetch_all_task_dimensions``（usage 走这条）同样只能一次登录。"""

        class _API(_CountingAPI):
            @api_mod.with_auth_retry
            def list_task_dimension(
                self, workspace_id, cookie, project_id=None, page_num=1, page_size=200
            ):
                if cookie == _STALE_COOKIE:
                    with self._lock:
                        self.unauthorized_calls += 1
                    raise QzAPIError("Cookie 已过期或无效", 401)
                return {
                    "task_dimensions": [{"id": f"t{page_num}-{i}"} for i in range(2)],
                    "total": 8,
                }

        api = _API()
        with sandbox_home(**_SANDBOX):
            cli.fetch_all_task_dimensions(
                api, "ws-1", _STALE_COOKIE, page_size=2, max_workers=1
            )
        self.assertEqual(api.cas_calls, 1, f"打了 {api.cas_calls} 次 CAS，应为 1")


class ReloginBaselineTests(unittest.TestCase):
    """``_relogin`` 的去重基准。"""

    def setUp(self):
        _reset()
        self.addCleanup(_reset)

    def test_failing_cookie_is_the_baseline(self):
        """盘上已是新 cookie、但调用方拿的是旧的 —— 不该再登。

        这是分页那个 bug 的最小复现：``stale``（进函数时读盘）等于新 cookie，
        旧判据据此认为"没人刷新过"，于是又登一次。
        """
        api = _CountingAPI()
        with sandbox_home(**_SANDBOX):
            api_mod.save_cookie("inspire-session=already-fresh", "ws-1")
            got = api._relogin(failing_cookie=_STALE_COOKIE)
        self.assertEqual(api.cas_calls, 0, "盘上已经是新的，不该再打 CAS")
        self.assertEqual(got, "inspire-session=already-fresh")

    def test_still_logs_in_when_disk_cookie_is_the_failing_one(self):
        """盘上就是那个失败的 cookie —— 这时必须真登录。

        对照组。少了它，把 ``_relogin`` 改成"永远不登"也能让上面那条变绿。
        """
        api = _CountingAPI()
        with sandbox_home(**_SANDBOX):
            got = api._relogin(failing_cookie=_STALE_COOKIE)
        self.assertEqual(api.cas_calls, 1)
        self.assertEqual(got, "inspire-session=fresh-1")

    def test_backward_compatible_without_failing_cookie(self):
        """不传 ``failing_cookie`` 时行为与旧版一致。"""
        api = _CountingAPI()
        with sandbox_home(**_SANDBOX):
            got = api._relogin()
        self.assertEqual(api.cas_calls, 1)
        self.assertEqual(got, "inspire-session=fresh-1")


class ProjectListCookieFallbackTests(unittest.TestCase):
    """v1 腿不传 cookie 时必须从磁盘兜底。

    **明确打 ``_project_list_items_v1``**：默认路径已经走 v2 了（上游放开了
    ``GetProjectForPage`` 的普通用户权限），但 v1 腿仍是回落路径，这个兜底不能丢。

    v2 那条路有兜底（``_request_v2``），v1 这条漏了 —— 空 cookie 直接进 header，
    必然 401，于是 ``qzcli create`` **每次**都白登一次。
    """

    def setUp(self):
        _reset()
        self.addCleanup(_reset)

    def test_no_cookie_argument_uses_disk_cookie(self):
        captured = {}

        def _fake_curl_post(url, body=None, headers=None, **kwargs):
            captured["cookie"] = (headers or {}).get("cookie", "")

            class _Resp:
                status_code = 200
                text = '{"code": 0, "data": {"items": []}}'

                def json(self):
                    return {"code": 0, "data": {"items": []}}

            return _Resp()

        api = _CountingAPI()
        with sandbox_home(**_SANDBOX):
            with patch.object(api_mod, "_curl_post", side_effect=_fake_curl_post):
                api._project_list_items_v1()
        self.assertEqual(
            captured.get("cookie"),
            _STALE_COOKIE,
            "不传 cookie 时应从磁盘兜底，而不是把空串塞进 header",
        )
        self.assertEqual(api.cas_calls, 0, "不该因为空 cookie 白登一次")

    def test_explicit_cookie_still_wins(self):
        captured = {}

        def _fake_curl_post(url, body=None, headers=None, **kwargs):
            captured["cookie"] = (headers or {}).get("cookie", "")

            class _Resp:
                status_code = 200
                text = '{"code": 0, "data": {"items": []}}'

                def json(self):
                    return {"code": 0, "data": {"items": []}}

            return _Resp()

        api = _CountingAPI()
        with sandbox_home(**_SANDBOX):
            with patch.object(api_mod, "_curl_post", side_effect=_fake_curl_post):
                api._project_list_items_v1("inspire-session=explicit")
        self.assertEqual(captured.get("cookie"), "inspire-session=explicit")


class DecoratorStackTests(unittest.TestCase):
    """v1 腿上同一组装饰器不能挂两遍（429 重试会变成 4×4=16 次）。"""

    def test_rate_limit_retries_are_not_squared(self):
        attempts = {"n": 0}

        def _always_429(url, body=None, headers=None, **kwargs):
            attempts["n"] += 1
            raise api_mod.QzRateLimitError("Too Many Requests", retry_after=0)

        api = _CountingAPI()
        with sandbox_home(**_SANDBOX):
            with patch.object(
                api_mod, "_curl_post", side_effect=_always_429
            ), patch.object(api_mod._time, "sleep", lambda *_: None):
                with self.assertRaises(api_mod.QzRateLimitError):
                    api._project_list_items_v1()
        self.assertLessEqual(
            attempts["n"],
            api_mod._RATE_LIMIT_MAX_TRIES,
            f"429 重试了 {attempts['n']} 次，上限应是 {api_mod._RATE_LIMIT_MAX_TRIES}",
        )


class McpAuthLoginTests(unittest.TestCase):
    """MCP 的 ``qz_auth_login`` 必须和 CLI 走同一套保护。

    ``cmd_login`` 修过、``_refresh_cookie_for_interactive`` 修过，这是第三处
    直连 ``login_with_cas`` 的地方。多 agent 并发调它是常态。
    """

    def setUp(self):
        _reset()
        self.addCleanup(_reset)

    def test_concurrent_mcp_login_hits_cas_once(self):
        from concurrent.futures import ThreadPoolExecutor

        from qzcli import mcp_server

        api = _CountingAPI()
        with sandbox_home(**_SANDBOX):
            with patch.object(mcp_server, "get_api", return_value=api):
                with ThreadPoolExecutor(max_workers=8) as pool:
                    list(
                        pool.map(lambda _: mcp_server.qz_auth_login("u", "p"), range(8))
                    )
        self.assertEqual(
            api.cas_calls, 1, f"8 个并发 MCP 登录打了 {api.cas_calls} 次 CAS，应为 1"
        )


if __name__ == "__main__":
    unittest.main()
