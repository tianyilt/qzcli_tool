"""检测到密码冲突时，**一个认证请求都不许发出去**。

## 为什么这条比"打印提示"重要得多

v0.4.16 做到了「检测到冲突就把指纹摆出来」，但摆完**照样拿优先级最高的那个去打
认证**。认证服务按失败次数锁账号（5 次），所以冲突时发请求等于拿账号的锁定额度
去赌哪个密码是对的 —— 而且每条命令赌一次。

两条路径都得挡，其中第二条更危险：

1. `qzcli login`：用户至少知道自己在登录。
2. **自动重登**（`api._relogin`）：用户只敲了一句 `qzcli status`，撞上过期
   cookie 就静默去打一次认证。改密码之前 `export` 过的 `QZCLI_PASSWORD` 留在
   长期运行的进程里（编辑器、agent、tmux），于是每一条命令都在悄悄扣额度。
   2026-09-12 实测本机还有 113 个这样的进程。

所以下面每条用例的核心断言都是同一句：**`login_with_cas` 的调用次数是 0**。
只断言"报了错"是不够的 —— 报错之后照样发请求，正是这次要修掉的形状。
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qzcli import api as api_mod  # noqa: E402
from qzcli import config as cfg  # noqa: E402
from qzcli import opslog  # noqa: E402

#: 一眼看得出是编造的。真值绝不进 tracked 文件。
_PW_A = "fake-password-alpha"
_PW_B = "fake-password-beta"


def _sources(env="", envfile="", config=""):
    """把三个来源的密码摆成 patch 用的三件套。"""
    return dict(
        load_config=lambda: ({"username": "fake-user", "password": config} if config else {"username": "fake-user"}),
        load_env_file=lambda: ({"QZCLI_PASSWORD": envfile} if envfile else {}),
    )


class ConflictPredicateTests(unittest.TestCase):
    """先把判据本身钉住 —— 后面所有拦截都建在它上面。"""

    def test_conflict_when_two_sources_differ(self):
        with mock.patch.multiple(cfg, **_sources(config=_PW_B)), mock.patch.dict(
            os.environ, {"QZCLI_PASSWORD": _PW_A}, clear=False
        ):
            rows = cfg.credential_conflict()
        self.assertEqual(2, len(rows), "两处不同的密码必须判为冲突")
        self.assertNotEqual(rows[0][1], rows[1][1])

    def test_no_conflict_when_values_agree(self):
        with mock.patch.multiple(cfg, **_sources(config=_PW_A)), mock.patch.dict(
            os.environ, {"QZCLI_PASSWORD": _PW_A}, clear=False
        ):
            self.assertEqual([], cfg.credential_conflict(), "值相同不算冲突")

    def test_no_conflict_with_single_source(self):
        env = {k: v for k, v in os.environ.items() if k != "QZCLI_PASSWORD"}
        with mock.patch.multiple(cfg, **_sources(config=_PW_A)), mock.patch.dict(
            os.environ, env, clear=True
        ):
            self.assertEqual([], cfg.credential_conflict())

    def test_fingerprints_only_never_plaintext(self):
        with mock.patch.multiple(cfg, **_sources(config=_PW_B)), mock.patch.dict(
            os.environ, {"QZCLI_PASSWORD": _PW_A}, clear=False
        ):
            msg = cfg.describe_credential_conflict()
        for secret in (_PW_A, _PW_B):
            self.assertNotIn(secret, msg, "冲突提示回显了密码明文")
        self.assertIn(cfg.password_fingerprint(_PW_A)[:8], msg)


class ReloginMustNotSendOnConflictTests(unittest.TestCase):
    """自动重登：冲突时 0 次认证请求，且必须留痕。"""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.ops = Path(tmp.name) / "ops.log"
        patcher = mock.patch.dict(os.environ, {"QZCLI_OPS_LOG": str(self.ops)})
        patcher.start()
        self.addCleanup(patcher.stop)

    def _ops_rows(self):
        if not self.ops.exists():
            return []
        import json

        return [
            json.loads(x)
            for x in self.ops.read_text(encoding="utf-8").splitlines()
            if x.strip()
        ]

    def _api_in_conflict(self):
        """造一个「两处密码不一致」的环境，并把所有落盘/网络副作用掐掉。"""
        api = api_mod.QzAPI.__new__(api_mod.QzAPI)
        api._username = "fake-user"
        api._password = _PW_A
        import threading

        api._relogin_lock = threading.Lock()
        return api

    def test_relogin_sends_zero_requests_on_conflict(self):
        api = self._api_in_conflict()
        called = []

        with mock.patch.multiple(cfg, **_sources(config=_PW_B)), mock.patch.dict(
            os.environ, {"QZCLI_PASSWORD": _PW_A}, clear=False
        ), mock.patch.object(
            api_mod.QzAPI, "login_with_cas", side_effect=lambda *a, **k: called.append(a) or "c"
        ), mock.patch.object(
            api_mod, "get_cookie", return_value={"cookie": "stale-fake-cookie"}
        ), mock.patch.object(
            api_mod, "_record_relogin_failure"
        ), mock.patch.object(
            api_mod, "_recent_relogin_failure", return_value=None
        ), mock.patch.object(
            api_mod, "save_cookie"
        ), mock.patch.object(
            api_mod, "_relogin_file_lock"
        ):
            out = api._relogin(failing_cookie="stale-fake-cookie")

        self.assertEqual(
            [], called,
            "冲突时自动重登仍然发了认证请求 —— 这正是把账号越锁越死的那条路径",
        )
        self.assertIsNone(out, "挡下来时不该返回 cookie")

    def test_relogin_conflict_is_written_to_ops_log(self):
        api = self._api_in_conflict()
        with mock.patch.multiple(cfg, **_sources(config=_PW_B)), mock.patch.dict(
            os.environ, {"QZCLI_PASSWORD": _PW_A}, clear=False
        ), mock.patch.object(
            api_mod.QzAPI, "login_with_cas", return_value="c"
        ), mock.patch.object(
            api_mod, "get_cookie", return_value={"cookie": "stale-fake-cookie"}
        ), mock.patch.object(
            api_mod, "_record_relogin_failure"
        ), mock.patch.object(
            api_mod, "_recent_relogin_failure", return_value=None
        ), mock.patch.object(
            api_mod, "save_cookie"
        ), mock.patch.object(
            api_mod, "_relogin_file_lock"
        ):
            api._relogin(failing_cookie="stale-fake-cookie")

        rows = [r for r in self._ops_rows() if r.get("op") == "relogin"]
        self.assertTrue(rows, "自动重登被挡下来却没留痕 —— 排查时会看成「本机没登录过」")
        self.assertEqual("error", rows[0]["outcome"])
        self.assertEqual("credential-conflict", rows[0]["err_class"])

    def test_relogin_failure_is_written_to_ops_log(self):
        """不是冲突、而是真登录失败时，也必须留痕。"""
        api = self._api_in_conflict()
        with mock.patch.multiple(cfg, **_sources(config=_PW_A)), mock.patch.dict(
            os.environ, {"QZCLI_PASSWORD": _PW_A}, clear=False
        ), mock.patch.object(
            api_mod.QzAPI,
            "login_with_cas",
            side_effect=api_mod.QzAPIError("fake 登录失败"),
        ), mock.patch.object(
            api_mod, "get_cookie", return_value={"cookie": "stale-fake-cookie"}
        ), mock.patch.object(
            api_mod, "_record_relogin_failure"
        ), mock.patch.object(
            api_mod, "_recent_relogin_failure", return_value=None
        ), mock.patch.object(
            api_mod, "save_cookie"
        ), mock.patch.object(
            api_mod, "_relogin_file_lock"
        ):
            api._relogin(failing_cookie="stale-fake-cookie")

        rows = [r for r in self._ops_rows() if r.get("op") == "relogin"]
        self.assertTrue(rows, "自动重登失败没留痕")
        self.assertEqual("error", rows[0]["outcome"])
        self.assertEqual("QzAPIError", rows[0]["err_class"])

    def test_relogin_proceeds_when_sources_agree(self):
        """没冲突就必须照常工作 —— 守卫不许把正常路径也挡了。"""
        api = self._api_in_conflict()
        called = []
        with mock.patch.multiple(cfg, **_sources(config=_PW_A)), mock.patch.dict(
            os.environ, {"QZCLI_PASSWORD": _PW_A}, clear=False
        ), mock.patch.object(
            api_mod.QzAPI,
            "login_with_cas",
            side_effect=lambda *a, **k: (called.append(a), "fresh-fake-cookie")[1],
        ), mock.patch.object(
            api_mod, "get_cookie", return_value={"cookie": "stale-fake-cookie"}
        ), mock.patch.object(
            api_mod, "_recent_relogin_failure", return_value=None
        ), mock.patch.object(
            api_mod, "_clear_relogin_failure"
        ), mock.patch.object(
            api_mod, "save_cookie"
        ), mock.patch.object(
            api_mod, "_relogin_file_lock"
        ):
            out = api._relogin(failing_cookie="stale-fake-cookie")
        self.assertEqual(1, len(called), "无冲突时自动重登必须照常发一次登录")
        self.assertEqual("fresh-fake-cookie", out)


class TokenPathMustNotSendOnConflictTests(unittest.TestCase):
    """/auth/token 那条路径把密码**明文**发出去，同样不许在冲突时发。"""

    def test_get_token_raises_before_posting(self):
        api = api_mod.QzAPI.__new__(api_mod.QzAPI)
        api._username = "fake-user"
        api._password = _PW_A
        api._token = ""
        api.base_url = "https://example.invalid"

        posted = []
        with mock.patch.multiple(cfg, **_sources(config=_PW_B)), mock.patch.dict(
            os.environ, {"QZCLI_PASSWORD": _PW_A}, clear=False
        ), mock.patch.object(
            api_mod, "get_token_cache", return_value=None
        ), mock.patch.object(
            api_mod, "_curl_post", side_effect=lambda *a, **k: posted.append(a)
        ):
            with self.assertRaises(api_mod.QzAPIError) as ctx:
                api._get_token()

        self.assertEqual([], posted, "冲突时仍把明文密码 POST 给了 /auth/token")
        self.assertIn("已阻止", str(ctx.exception))
        for secret in (_PW_A, _PW_B):
            self.assertNotIn(secret, str(ctx.exception), "错误信息里回显了密码明文")


class ExplicitSourceTests(unittest.TestCase):
    """被挡下来之后，用户得有一个不含歧义的前进方式。"""

    def test_source_config_picks_config_even_when_env_wins_by_priority(self):
        with mock.patch.multiple(cfg, **_sources(config=_PW_B)), mock.patch.dict(
            os.environ, {"QZCLI_PASSWORD": _PW_A}, clear=False
        ):
            _, pw = cfg.get_credentials_from_source("config")
        self.assertEqual(_PW_B, pw, "--source config 必须无视环境变量的优先级")

    def test_source_env_picks_env(self):
        with mock.patch.multiple(cfg, **_sources(config=_PW_B)), mock.patch.dict(
            os.environ, {"QZCLI_PASSWORD": _PW_A}, clear=False
        ):
            _, pw = cfg.get_credentials_from_source("env")
        self.assertEqual(_PW_A, pw)

    def test_empty_source_is_an_error_not_a_silent_fallback(self):
        """指定的那处没有密码时必须报错。

        静默回退回优先级 = 又一个「说用 A、实际用了 B」，正是本轮要消灭的形状。
        """
        env = {k: v for k, v in os.environ.items() if k != "QZCLI_PASSWORD"}
        with mock.patch.multiple(cfg, **_sources(config=_PW_B)), mock.patch.dict(
            os.environ, env, clear=True
        ):
            with self.assertRaises(ValueError):
                cfg.get_credentials_from_source("env")

    def test_unknown_source_is_rejected(self):
        with self.assertRaises(ValueError):
            cfg.get_credentials_from_source("nope")


if __name__ == "__main__":
    unittest.main()


class _FakeDisplay:
    def __init__(self):
        self.messages = []

    def print(self, *a, **k):
        self.messages.append(" ".join(str(x) for x in a))

    print_error = print
    print_success = print


class _CountingAPI:
    """只数调用次数。这个类的存在就是为了让"发了几次请求"变成可断言的。"""

    def __init__(self):
        self.calls = []

    def login_with_cas(self, username, password):
        self.calls.append((username, password))
        return "fresh-fake-cookie"


class CmdLoginBlocksOnConflictTests(unittest.TestCase):
    """`qzcli login`：冲突时 0 次认证请求、退出码非 0。"""

    def _args(self, **over):
        import argparse

        base = dict(
            username=None,
            password=None,
            password_stdin=False,
            source=None,
            force=False,
            workspace=None,
        )
        base.update(over)
        return argparse.Namespace(**base)

    def _run(self, args, env_pw, cfg_pw, stored_pw):
        from qzcli import cli

        display, api = _FakeDisplay(), _CountingAPI()
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        with mock.patch.multiple(cfg, **_sources(config=cfg_pw)), mock.patch.dict(
            os.environ,
            {"QZCLI_PASSWORD": env_pw, "QZCLI_OPS_LOG": str(Path(tmp.name) / "o.log")},
            clear=False,
        ), mock.patch.object(
            cli, "get_display", return_value=display
        ), mock.patch.object(
            cli, "get_api", return_value=api
        ), mock.patch.object(
            cli, "get_credentials", return_value=("fake-user", stored_pw)
        ), mock.patch.object(
            cli, "save_cookie"
        ), mock.patch.object(
            cli, "_relogin_file_lock"
        ), mock.patch.object(
            cli, "get_cookie", return_value={}
        ), mock.patch.object(
            cli, "_clear_relogin_failure"
        ), mock.patch(
            "builtins.input", side_effect=AssertionError("不该提示输入")
        ), mock.patch(
            "getpass.getpass", side_effect=AssertionError("不该提示输入")
        ):
            rc = cli.cmd_login(args)
        return rc, api, display

    def test_conflict_blocks_and_sends_nothing(self):
        rc, api, display = self._run(
            self._args(), env_pw=_PW_A, cfg_pw=_PW_B, stored_pw=_PW_A
        )
        self.assertEqual([], api.calls, "冲突时 login 仍然发了认证请求")
        self.assertEqual(1, rc, "被挡下来必须是非 0 退出码")
        blob = "\n".join(display.messages)
        self.assertIn("已阻止本次登录", blob)
        self.assertIn("--source", blob, "挡住之后必须给出前进方式")
        for secret in (_PW_A, _PW_B):
            self.assertNotIn(secret, blob, "输出里回显了密码明文")

    def test_force_lets_it_through(self):
        rc, api, _ = self._run(
            self._args(force=True), env_pw=_PW_A, cfg_pw=_PW_B, stored_pw=_PW_A
        )
        self.assertEqual(1, len(api.calls), "--force 应当照发")
        self.assertEqual(0, rc)

    def test_explicit_source_lets_it_through_and_uses_that_source(self):
        rc, api, _ = self._run(
            self._args(source="config"), env_pw=_PW_A, cfg_pw=_PW_B, stored_pw=_PW_A
        )
        self.assertEqual(1, len(api.calls), "--source 指定后应当照发")
        self.assertEqual(
            _PW_B, api.calls[0][1],
            "--source config 说了用 config.json，实际却发了别处的密码",
        )
        self.assertEqual(0, rc)

    def test_explicit_password_is_never_blocked(self):
        """用户当场给的密码没有歧义，不该被挡。"""
        rc, api, _ = self._run(
            self._args(password="fake-typed-password"),
            env_pw=_PW_A,
            cfg_pw=_PW_B,
            stored_pw=_PW_A,
        )
        self.assertEqual(1, len(api.calls))
        self.assertEqual("fake-typed-password", api.calls[0][1])
        self.assertEqual(0, rc)

    def test_no_conflict_still_logs_in_normally(self):
        rc, api, _ = self._run(
            self._args(), env_pw=_PW_A, cfg_pw=_PW_A, stored_pw=_PW_A
        )
        self.assertEqual(1, len(api.calls), "无冲突时必须照常登录")
        self.assertEqual(0, rc)

    def test_password_from_elsewhere_is_not_blocked(self):
        """密码另有来路时，来源冲突与本次请求无关，不该误报。

        误报的代价很具体：用户学会一律加 --force，这道闸门就白装了。
        """
        rc, api, _ = self._run(
            self._args(),
            env_pw=_PW_A,
            cfg_pw=_PW_B,
            stored_pw="fake-password-from-somewhere-else",
        )
        self.assertEqual(1, len(api.calls))
        self.assertEqual(0, rc)
