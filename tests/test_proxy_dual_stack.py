"""代理"双栈"一致性。

qzcli 有**两套完全不同的代理机制**：

- ``_curl_post``（几乎所有 API 调用）走 **urllib3**，代理由 ``get_proxy()``
  显式解析后交给 ``PoolManager`` / ``ProxyManager`` / ``SOCKSProxyManager``
- ``login_with_cas`` 走 **requests.Session**，只在 ``get_proxy()`` 非空时才设
  ``session.proxies`` 并关掉 ``trust_env``；为空时 ``trust_env`` 保持 ``True``，
  于是 requests 会**自行**去读环境变量

现有 ``test_proxy.py`` / ``test_login_proxy.py`` 各自测了半边，但**没有任何一条
用例同时喂两条路径同一份配置再比对结论** —— 而"同一份配置、两条路径行为不同"
正是这类 bug 唯一的表现形式。

## 方法论：先实测，别照推

一份勘察报告曾断言"只设 ``HTTP_PROXY`` 时两栈行为相反"。但按 HTTP 惯例
``HTTP_PROXY`` 只对 ``http://`` 生效，而 qzcli 全部流量都是 https —— 所以这条
很可能不成立。下面用例**先把 requests 的实际行为测出来**，再据此断言，
而不是照着惯例或报告推。
"""

import os
import sys
import unittest
from unittest.mock import patch

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from support.sandbox import sandbox_home  # noqa: E402

from qzcli import api as api_mod  # noqa: E402
from qzcli import config as config_mod  # noqa: E402

_HTTPS_URL = "https://qz.sii.edu.cn/api/v1/anything"


def _requests_would_use(url, env):
    """requests 在给定环境变量下，对 ``url`` 实际会选用的代理。

    直接问 requests 自己，而不是照惯例推断。
    """
    session = requests.Session()
    with patch.dict(os.environ, env, clear=True):
        return (
            session.get_environ_proxies(url)
            if hasattr(session, "get_environ_proxies")
            else requests.utils.get_environ_proxies(url, no_proxy=None)
        )


class EnvVarPrecedenceTests(unittest.TestCase):
    """两栈对同一份环境变量的解读必须一致。"""

    def test_https_proxy_is_honored_by_both(self):
        with sandbox_home(env={"HTTPS_PROXY": "http://proxy:3128"}):
            self.assertEqual(config_mod.get_proxy(), "http://proxy:3128")
        chosen = _requests_would_use(_HTTPS_URL, {"HTTPS_PROXY": "http://proxy:3128"})
        self.assertEqual(
            chosen.get("https"),
            "http://proxy:3128",
            "requests 也该对 https URL 用 HTTPS_PROXY —— 两栈一致",
        )

    def test_all_proxy_is_honored_by_both(self):
        with sandbox_home(env={"ALL_PROXY": "socks5h://127.0.0.1:1080"}):
            self.assertEqual(config_mod.get_proxy(), "socks5h://127.0.0.1:1080")
        chosen = _requests_would_use(
            _HTTPS_URL, {"ALL_PROXY": "socks5h://127.0.0.1:1080"}
        )
        self.assertTrue(
            any("socks5h" in str(v) for v in chosen.values()),
            f"requests 没认 ALL_PROXY：{chosen}",
        )

    def test_http_proxy_only_does_not_diverge_for_https_traffic(self):
        """只设 ``HTTP_PROXY`` 时，两栈都不该对 https 流量用代理。

        ``get_proxy()`` 不读 ``HTTP_PROXY`` 是**对的** —— 按 HTTP 惯例它只管
        ``http://``，而 qzcli 全部流量都是 https。这条用例把"两栈仍然一致"钉住，
        免得日后有人"好心"把 HTTP_PROXY 加进 get_proxy，反而制造出真的分叉。
        """
        with sandbox_home(env={"HTTP_PROXY": "http://only-http:3128"}):
            self.assertEqual(
                config_mod.get_proxy(), "", "get_proxy 不该对 https 流量认 HTTP_PROXY"
            )
        chosen = _requests_would_use(
            _HTTPS_URL, {"HTTP_PROXY": "http://only-http:3128"}
        )
        self.assertIsNone(
            chosen.get("https"),
            f"requests 对 https URL 也不该用 HTTP_PROXY，实测: {chosen}",
        )

    def test_config_beats_environment_on_both_paths(self):
        """config.json 里的 proxy 优先级最高，且两栈都要认它。"""
        with sandbox_home(
            config_json={"proxy": "socks5h://from-config:1080"},
            env={"HTTPS_PROXY": "http://from-env:3128"},
        ):
            resolved = config_mod.get_proxy()
            self.assertEqual(resolved, "socks5h://from-config:1080")
            # login 侧：非空 proxy 一定被显式写进 session.proxies 且关掉 trust_env
            sessions = _capture_login_sessions(resolved)
            self.assertTrue(sessions, "没抓到 session")
            for s in sessions:
                self.assertEqual(s.proxies.get("https"), "socks5h://from-config:1080")
                self.assertFalse(s.trust_env, "显式配了代理就不该再读环境变量")


def _capture_login_sessions(proxy_value):
    """跑一次 login_with_cas 的开头，把它建的 Session 抓出来。

    让第一个请求就抛 RequestException，流程会在设完代理之后立刻中断 ——
    既拿到了想看的状态，又不会真的发出网络请求。
    """
    created = []
    real_session_cls = requests.Session

    class _Probe(real_session_cls):
        def __init__(self):
            super().__init__()
            created.append(self)

        def get(self, *a, **kw):
            raise requests.exceptions.RequestException("stop here")

        def post(self, *a, **kw):
            raise requests.exceptions.RequestException("stop here")

    api = api_mod.QzAPI(username="u", password="p", auth_mode="legacy")
    with patch.object(api_mod.requests, "Session", _Probe), patch.object(
        api_mod, "get_proxy", return_value=proxy_value
    ), patch.object(api_mod._time, "sleep", lambda *_: None):
        try:
            api.login_with_cas("u", "p")
        except api_mod.QzTransientError:
            # _Probe 抛 RequestException 当哨兵提前中断登录流程，而 login_with_cas
            # 会把它包成 QzTransientError —— 所以这里接的是**包装后**的类型。
            # 原来写的是 except Exception，接得住但也接得住一切：哨兵哪天不再被
            # 包装、或者 _Probe 里写错个属性名，测试都照样绿。收窄之后这类
            # 假绿会立刻变红。
            pass
    return created


class SchemePreservationTests(unittest.TestCase):
    """scheme 必须逐字保留 —— ``socks5h`` 被降级成 ``socks5`` 会让 DNS 走本地解析。"""

    def test_socks5h_is_not_downgraded_on_either_stack(self):
        proxy = "socks5h://127.0.0.1:1080"
        # login 侧
        for s in _capture_login_sessions(proxy):
            self.assertEqual(s.proxies.get("https"), proxy, "socks5h 被改写了")
        # _curl_post 侧：manager 类型要选到 SOCKS
        manager = api_mod._get_pool_manager(proxy)
        self.assertIn("SOCKS", type(manager).__name__)

    def test_uppercase_scheme_is_tolerated(self):
        """``SOCKS5://`` 这种大写写法不能把 manager 选错。"""
        manager = api_mod._get_pool_manager("SOCKS5://127.0.0.1:1080")
        self.assertIn("SOCKS", type(manager).__name__)

    def test_trailing_slash_does_not_create_a_second_manager(self):
        """``http://p:3128`` 和 ``http://p:3128/`` 是同一个代理。

        规范化没做对的话，lru_cache 会为同一个代理建两套连接池。
        """
        a = api_mod._get_pool_manager("http://p:3128")
        b = api_mod._get_pool_manager("http://p:3128/")
        self.assertIs(type(a), type(b))

    def test_unknown_scheme_is_rejected_with_a_readable_error(self):
        with self.assertRaises(api_mod.QzAPIError) as ctx:
            api_mod._get_pool_manager("ftp://nope:21")
        self.assertIn("ftp://nope:21", str(ctx.exception), "错误里要带上是哪个地址")


class CurlPostActuallyUsesTheProxyTests(unittest.TestCase):
    """现有测试只验了 ``_get_pool_manager`` 这个纯函数。

    ``_curl_post`` 有没有真的把 ``get_proxy()`` 的结果用上，此前零覆盖。
    """

    def test_curl_post_resolves_proxy_at_call_time(self):
        seen = {}

        def _fake_manager(proxy):
            seen["proxy"] = proxy

            class _M:
                def request(self, *a, **kw):
                    class _R:
                        status = 200
                        data = b"{}"
                        headers = {}

                    return _R()

            return _M()

        with sandbox_home(config_json={"proxy": "http://cfg:3128"}):
            with patch.object(api_mod, "_get_pool_manager", side_effect=_fake_manager):
                api_mod._curl_post("https://example.com", body={}, headers={})
        self.assertEqual(seen.get("proxy"), "http://cfg:3128")

    def test_whitespace_in_configured_proxy_is_stripped(self):
        """配置里手抖多打了空格不该让代理解析失败。"""
        seen = {}

        def _fake_manager(proxy):
            seen["proxy"] = proxy

            class _M:
                def request(self, *a, **kw):
                    class _R:
                        status = 200
                        data = b"{}"
                        headers = {}

                    return _R()

            return _M()

        with sandbox_home(config_json={"proxy": "  http://cfg:3128  "}):
            with patch.object(api_mod, "_get_pool_manager", side_effect=_fake_manager):
                api_mod._curl_post("https://example.com", body={}, headers={})
        self.assertEqual(seen.get("proxy"), "http://cfg:3128")


if __name__ == "__main__":
    unittest.main()


class RateLimitMustNotDoubleQpsTests(unittest.TestCase):
    """限流时**绝不能**回落去打另一条路 —— 那等于平台喊"慢点"时把请求量翻倍。

    这条纪律 ``_v2_then_v1`` 里写死了，但 ``get_job_detail`` 是另一条独立路径，
    用的是裸 ``except QzAPIError: pass``。而 ``QzRateLimitError`` 是
    ``QzAPIError`` 的子类 —— 于是 429 被静默吞掉，转头再打一发 v1 openapi。

    真实后果：``qzcli list -c --all-ws``（每个工作空间 5 线程扇出批量查详情）
    在全量形态下稳定撞 429。这条用例就是照着那次 live_smoke 失败写的。
    """

    def test_rate_limit_is_not_swallowed_into_a_v1_retry(self):
        api = api_mod.QzAPI(username="u", password="p", auth_mode="legacy")
        v1_calls = {"n": 0}

        def _count_v1(*a, **kw):
            v1_calls["n"] += 1
            return {"data": {}}

        with sandbox_home(
            cookie='{"cookie": "inspire-session=ok", "workspace_id": "ws-1"}'
        ):
            with patch.object(
                api,
                "get_job_detail_with_cookie",
                side_effect=api_mod.QzRateLimitError(
                    "Too Many Requests", retry_after=0
                ),
            ), patch.object(api, "_request", side_effect=_count_v1):
                with self.assertRaises(api_mod.QzRateLimitError):
                    api.get_job_detail("job-1")
        self.assertEqual(v1_calls["n"], 0, "撞 429 之后还去打了 v1 —— 把请求量翻倍了")

    def test_other_errors_still_fall_back_to_v1(self):
        """对照组：非限流的错误仍要保留 v1 兜底，别把这条路修死了。"""
        api = api_mod.QzAPI(username="u", password="p", auth_mode="legacy")
        with sandbox_home(
            cookie='{"cookie": "inspire-session=ok", "workspace_id": "ws-1"}'
        ):
            with patch.object(
                api,
                "get_job_detail_with_cookie",
                side_effect=api_mod.QzAPIError("v2 路由不通", 404),
            ), patch.object(
                api, "_request", return_value={"data": {"job_id": "job-1"}}
            ):
                got = api.get_job_detail("job-1")
        self.assertEqual(got.get("job_id"), "job-1", "普通错误的 v1 兜底不该被误伤")
