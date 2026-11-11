"""``mcp_server.py`` —— 1708 行、17 个 tool，此前**零测试**。

## 为什么这块风险最高

它是与 CLI 并列的**第二个用户面**，但**几乎完全是平行重实现**：模块顶部没有
`from .cli import ...`（只有 exec 那两个 tool 在函数体里延迟导入 CLI 的内部函数）。

也就是说 **CLI 的测试完全不保护 MCP**：改了 `cmd_hpc_usage` 的统计口径，
`qz_get_hpc_usage` 不会跟着变，也不会有任何测试报警。历史上已经踩过一次
——「同一个工具的两个入口行为不一致，v2 才支持的 exclude_nodes 在 MCP 侧静默失效」
（`mcp_server.py` 里那段注释就是当时留下的）。

## 这批用例守三条线

1. **stdio 不被污染** —— MCP 走 stdio 协议，任何裸 `print` 到 stdout 都会破坏它。
   `_CollectingDisplay` 是唯一的防线，而它只实现了 4 个方法，CLI 侧一旦用了别的
   display 方法就会 `AttributeError`。
2. **入参校验真的拦得住** —— 缺 cookie、工作空间不存在，必须抛出**可执行**的错误，
   而不是返回一个看着正常的空结果。
3. **和 CLI 的行为一致性** —— 至少把「两边都实现了同一件事」的地方钉住。
"""

import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from support.sandbox import sandbox_home  # noqa: E402

from qzcli import mcp_server  # noqa: E402


@patch.dict(os.environ, {"QZCLI_AUTH_MODE": "legacy"})
class RequireCookieTests(unittest.TestCase):
    def test_missing_cookie_raises_actionable_error(self):
        """没有 cookie 时要说清楚**怎么办**，而不是只说"失败了"。"""
        with sandbox_home():
            with self.assertRaises(RuntimeError) as ctx:
                mcp_server._require_cookie()
        message = str(ctx.exception)
        self.assertIn("qzcli login", message)
        self.assertIn("qz_auth_login", message, "MCP 用户手边没有 CLI，要给 tool 名")

    def test_empty_cookie_value_is_also_rejected(self):
        """cookie 文件在但内容是空串 —— 同样不能放行。"""
        with sandbox_home(cookie='{"cookie": "", "workspace_id": "ws-1"}'):
            with self.assertRaises(RuntimeError):
                mcp_server._require_cookie()

    def test_valid_cookie_is_returned_with_its_data(self):
        with sandbox_home(
            cookie='{"cookie": "inspire-session=ok", "workspace_id": "ws-1"}'
        ):
            cookie, data = mcp_server._require_cookie()
        self.assertEqual(cookie, "inspire-session=ok")
        self.assertEqual(data.get("workspace_id"), "ws-1")


@patch.dict(os.environ, {"QZCLI_AUTH_MODE": "legacy"})
class WorkspaceResolutionTests(unittest.TestCase):
    """`_resolve_workspace_refs` —— 所有需要工作空间的 tool 都从这里进。"""

    def test_ws_prefixed_id_is_taken_as_is(self):
        with sandbox_home():
            refs = mcp_server._resolve_workspace_refs("ws-1234")
        self.assertEqual(refs[0]["id"], "ws-1234")

    def test_unknown_name_raises_and_names_it(self):
        """本地缓存和远端都找不到时，错误里要带上用户输入的那个名字。

        返回空列表会让上层 tool 静默产出"这个工作空间什么都没有"——
        用户完全无从判断是名字写错了还是真的空。
        """
        with sandbox_home(
            cookie='{"cookie": "inspire-session=ok", "workspace_id": "ws-1"}'
        ):
            with patch.object(mcp_server, "find_workspace_by_name", return_value=None):
                with patch.object(
                    mcp_server, "_match_workspace_from_remote", return_value=None
                ):
                    with self.assertRaises(RuntimeError) as ctx:
                        mcp_server._resolve_workspace_refs("不存在的空间")
        self.assertIn("不存在的空间", str(ctx.exception), "错误里要带上用户输入的名字")

    def test_unknown_name_without_cookie_says_cookie_is_missing(self):
        """没 cookie 时连远端都查不了，这时报"未设置 cookie"是对的。

        钉住它是为了区分两种失败：名字错 vs 没法查。二者的下一步动作完全不同。
        """
        with sandbox_home():
            with patch.object(mcp_server, "find_workspace_by_name", return_value=None):
                with self.assertRaises(RuntimeError) as ctx:
                    mcp_server._resolve_workspace_refs("不存在的空间")
        self.assertIn("cookie", str(ctx.exception).lower())

    def test_all_workspaces_prefers_cache(self):
        cached = [{"id": "ws-1", "name": "空间一"}, {"id": "ws-2", "name": "空间二"}]
        with sandbox_home():
            with patch.object(
                mcp_server, "list_cached_workspaces", return_value=cached
            ):
                refs = mcp_server._resolve_workspace_refs(all_workspaces=True)
        self.assertEqual([r["id"] for r in refs], ["ws-1", "ws-2"])

    def test_all_workspaces_without_cache_or_cookie_raises(self):
        """缓存空 + 没 cookie —— 不能假装成"一个空间都没有"。"""
        with sandbox_home():
            with patch.object(mcp_server, "list_cached_workspaces", return_value=[]):
                with self.assertRaises(RuntimeError):
                    mcp_server._resolve_workspace_refs(all_workspaces=True)


class CollectingDisplayTests(unittest.TestCase):
    """`_CollectingDisplay` 是 MCP stdio 协议的唯一防线。"""

    def test_nothing_reaches_stdout(self):
        display = mcp_server._CollectingDisplay()
        buf = io.StringIO()
        with redirect_stdout(buf):
            display.print("普通输出")
            display.print_error("错误")
            display.print_warning("警告")
            display.print_success("成功")
        self.assertEqual(buf.getvalue(), "", "任何写到 stdout 的东西都会破坏 MCP 协议")
        self.assertEqual(len(display.messages), 4, "四条都要被收集下来")

    def test_covers_every_display_method_the_cli_actually_calls(self):
        """CLI 侧 exec 链路用到的 display 方法，这里必须都有。

        `_CollectingDisplay` 只实现了 4 个方法。CLI 那边哪天多用一个
        `display.print_panel()` 之类，MCP 的 exec 就会当场 AttributeError ——
        而这个约定完全是隐式的，只能靠这条用例守着。
        """
        display = mcp_server._CollectingDisplay()
        for name in ("print", "print_error", "print_warning", "print_success"):
            self.assertTrue(callable(getattr(display, name, None)), f"缺少 {name}")


@patch.dict(os.environ, {"QZCLI_AUTH_MODE": "legacy"})
class AuthLoginTests(unittest.TestCase):
    """`qz_auth_login` 必须和 CLI 走同一套并发保护（曾是第三处裸奔）。"""

    def test_goes_through_relogin_not_raw_login(self):
        api = MagicMock()
        api._relogin.return_value = "inspire-session=via-relogin"
        with sandbox_home(env={"QZCLI_USERNAME": "u", "QZCLI_PASSWORD": "p"}):
            with patch.object(mcp_server, "get_api", return_value=api):
                mcp_server.qz_auth_login("u", "p")
        api._relogin.assert_called_once()
        api.login_with_cas.assert_not_called()

    def test_missing_credentials_raise_with_all_config_paths(self):
        """没凭据时要把**所有**可用的配置途径列出来。

        MCP 用户看不到 CLI 的帮助文本，这条错误信息就是他们唯一的线索。
        """
        with sandbox_home():
            with patch.object(mcp_server, "get_credentials", return_value=("", "")):
                with self.assertRaises(Exception) as ctx:
                    mcp_server.qz_auth_login()
        message = str(ctx.exception)
        for hint in ("QZCLI_USERNAME", ".env", "config.json"):
            self.assertIn(hint, message)


class ToolRegistrationTests(unittest.TestCase):
    def test_every_tool_has_a_description(self):
        """没有 description 的 tool，模型侧根本不知道什么时候该用它。"""
        import inspect
        import re

        source = inspect.getsource(mcp_server)
        # `@server.tool()` 不带 description 的写法
        bare = re.findall(r"@server\.tool\(\s*\)", source)
        self.assertEqual(
            bare, [], f"有 {len(bare)} 个 tool 没写 description，模型无从判断何时调用"
        )

    def test_module_does_not_print_to_stdout_at_import(self):
        """import 期间打到 stdout 会在握手阶段就破坏 MCP 协议。"""
        import importlib

        buf = io.StringIO()
        with redirect_stdout(buf):
            importlib.reload(mcp_server)
        self.assertEqual(buf.getvalue(), "")


if __name__ == "__main__":
    unittest.main()
