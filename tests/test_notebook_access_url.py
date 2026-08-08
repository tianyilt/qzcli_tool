"""开发机 Jupyter 访问地址：v2 优先，v1 兜底。

## 背景

上游 2026-08 之前，v2 全域拿不到 Jupyter 访问地址（notebook schema 里只有
``extra_info.ProxyJump``），所以 ``qzcli exec`` 只能直接打 v1 的
``/api/v1/notebook/lab/{id}`` 读 **301 响应头**里的 Location —— 这是 qzcli
最后一个 v1 依赖，文档里一直记着「无任何 v2 对应」。

上游补上 ``notebook GetNotebookAccessUrl`` 之后可以迁了。实测两边返回的是
**同一条 URL**（同 host、同路径、同 token），v2 还多给一个 ``vscode_url``。

## 这组**不覆盖**什么（重要）

这里全是**解析与回落逻辑**的单测：URL 怎么拆成三个键、v2 不通怎么退到 v1、
拿不到地址时给什么错误。它们跑得快、不需要凭据，但**证明不了地址真能连上、
命令真能执行** —— fixture 是编造的，连不上任何东西。

那一半由 ``tools/live_smoke.py`` 的「exec 在真实开发机上执行命令」负责：现场找一台
RUNNING 的开发机、拿真地址、跑一条带随机串的 echo、断言回显匹配。**动态发现，
不存任何凭据。**

两者互补，不能互相替代。上一版只有这组单测，于是「迁到 v2 之后 exec 还能不能用」
其实没人验过 —— 而我为了写这组 fixture，还把真 token 抄了进来。

## 为什么单独立一组

改这段之前，``_get_jupyter_info`` 的函数体**一行测试都没有** ——
``tests/test_exec.py`` 的 ``FindNotebookJupyterInfoTests`` 直接把
``_get_jupyter_info`` 整个 mock 掉了，所以换数据源时全部用例照绿。
"""

import unittest
from unittest.mock import MagicMock, patch

from qzcli import cli
from qzcli.api import QzAPI, QzAPIError

# ⚠️ 全部是**编造的**占位值，不要从真实返回里粘贴。
# Jupyter 的 token 就在 URL 里，粘一条真 URL 进来 = 把开发机的访问凭据提交进仓库
# （我干过一次，被 GitGuardian 拦下）。形状照真实的，值必须是假的。
_NB = "notebook-0000-fake-0000-000000000000"
_TOKEN = "token-0000-fake-0000-000000000000"
_HOST = "https://notebook.example.invalid"
_PREFIX = f"{_HOST}/ws-fake/project-fake/user-fake/jupyter/{_NB}/{_TOKEN}"
# 形状照抄真实返回：结尾带 /lab?token=...
_JUPYTER_URL = f"{_PREFIX}/lab?token={_TOKEN}"


class ApiAccessUrlTests(unittest.TestCase):
    def _api(self):
        api = QzAPI(username="u", password="p")
        return api

    def test_v2_is_used_when_available(self):
        api = self._api()
        with patch.object(
            api,
            "_request_v2",
            return_value={
                "jupyter_url": _JUPYTER_URL,
                "vscode_url": "https://x/vscode",
            },
        ) as v2:
            got = api.get_notebook_access_url(_NB, "cookie")
        v2.assert_called_once()
        self.assertEqual(v2.call_args[0][1], "GetNotebookAccessUrl")
        self.assertEqual(v2.call_args[0][2], {"notebook_id": _NB})
        self.assertEqual(got["jupyter_url"], _JUPYTER_URL)
        self.assertEqual(got["vscode_url"], "https://x/vscode")

    def test_falls_back_to_v1_when_v2_route_is_missing(self):
        """v2 路由不通（老平台）时仍要能工作 —— 回落到 301 那条路。"""
        api = self._api()
        with patch.object(
            api, "_request_v2", side_effect=QzAPIError("404 page not found", 404)
        ), patch.object(api, "_notebook_lab_url_v1", return_value=_JUPYTER_URL) as v1:
            got = api.get_notebook_access_url(_NB, "cookie")
        v1.assert_called_once()
        self.assertEqual(got["jupyter_url"], _JUPYTER_URL)
        self.assertEqual(got["vscode_url"], "", "v1 给不出 vscode 地址，应为空串")

    def test_business_error_does_not_fall_back(self):
        """开发机不存在这类业务错误不该回落 —— 回落只会把同一个错误再撞一遍。"""
        api = self._api()
        with patch.object(
            api,
            "_request_v2",
            side_effect=QzAPIError("ResourceNotFound: notebook not found"),
        ), patch.object(api, "_notebook_lab_url_v1") as v1:
            with self.assertRaises(QzAPIError):
                api.get_notebook_access_url(_NB, "cookie")
        v1.assert_not_called()


class GetJupyterInfoTests(unittest.TestCase):
    """``_get_jupyter_info`` 的函数体 —— 此前零覆盖。"""

    def _call(self, urls=None, side_effect=None):
        api = MagicMock()
        if side_effect is not None:
            api.get_notebook_access_url.side_effect = side_effect
        else:
            api.get_notebook_access_url.return_value = urls
        display = MagicMock()
        with patch.object(cli, "get_api", return_value=api):
            return cli._get_jupyter_info(_NB, "cookie", display), display

    def test_parses_base_url_and_token(self):
        """三键契约：base_url 去掉 /lab 及 query，token 是路径里那一段。

        下游 _exec_launch / _exec_poll / MCP 的 exec 全靠这三个键。
        """
        got, _ = self._call({"jupyter_url": _JUPYTER_URL, "vscode_url": ""})
        self.assertEqual(
            got, {"base_url": _PREFIX, "token": _TOKEN, "notebook_id": _NB}
        )

    def test_empty_url_reports_actionable_error(self):
        """开发机没在跑时平台会给空地址 —— 要说清是这个原因，不能只说"失败"。"""
        got, display = self._call({"jupyter_url": "", "vscode_url": ""})
        self.assertIsNone(got)
        msg = " ".join(str(c) for c in display.print_error.call_args_list)
        self.assertIn("未在运行", msg)

    def test_unparseable_url_is_reported_with_the_url(self):
        got, display = self._call({"jupyter_url": "https://example.com/nope"})
        self.assertIsNone(got)
        msg = " ".join(str(c) for c in display.print_error.call_args_list)
        self.assertIn("解析", msg)

    def test_auth_error_propagates(self):
        """401 要往上抛 —— 上层 _find_notebook_jupyter_info 靠它触发自动重登。"""
        with self.assertRaises(QzAPIError):
            self._call(side_effect=QzAPIError("Cookie 已过期", 401))

    def test_v1_url_shape_parses_identically(self):
        """v1 301 Location 和 v2 jupyter_url 是同一条 URL，用同一个正则解析。

        实测两边同 host、同路径、同 token —— 这条钉住"共用正则"这个前提。
        """
        got, _ = self._call({"jupyter_url": _JUPYTER_URL})
        self.assertEqual(got["token"], _TOKEN)
        self.assertTrue(got["base_url"].endswith(_TOKEN))
        self.assertNotIn("?", got["base_url"], "base_url 不该带 query")
        self.assertNotIn("/lab", got["base_url"], "base_url 不该带 /lab")


if __name__ == "__main__":
    unittest.main()
