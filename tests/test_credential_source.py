"""凭据来源可见性 + 失败必须记成失败。

这两件事都是 2026-09-11「账号频繁锁定」那次排查的直接产物：

1. **改完 config.json 却还在用旧密码** —— shell 里一条过期的 `export QZCLI_PASSWORD`
   优先级更高，静默压过配置文件。错误信息只有「账号或密码错误」，指不到那条 export。
2. **日志在撒谎** —— `cmd_login` 捕获异常后 `return 1`，而 `opslog.timed` 只看有没有抛异常，
   于是失败被写成 `outcome: ok`。当时日志里 152 次登录全是「成功」，平台侧审计同期却是
   一串「密码错误」，两边对不上，白白多花时间才想到是日志的问题。

守的就是这两条不许退回去。
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qzcli import config as cfg  # noqa: E402
from qzcli import opslog  # noqa: E402


class CredentialSourceTests(unittest.TestCase):
    """取到的密码必须能说清楚「从哪来的」。"""

    def _patch(self, env, env_file, config):
        return mock.patch.multiple(
            cfg,
            load_config=lambda: config,
            load_env_file=lambda: env_file,
        ), mock.patch.dict(os.environ, env, clear=False)

    def test_env_wins_and_says_so(self):
        """环境变量优先级最高 —— 而且必须**明说**是环境变量。"""
        p1, p2 = self._patch(
            {"QZCLI_PASSWORD": "from-env"},
            {"QZCLI_PASSWORD": "from-envfile"},
            {"password": "from-config"},
        )
        with p1, p2:
            _, pw, _, src = cfg.get_credentials_with_source()
        self.assertEqual(pw, "from-env")
        self.assertIn("环境变量", src)

    def test_config_source_is_named_when_it_is_the_one_used(self):
        """没有环境变量时落到 config.json，来源描述里要出现文件路径。"""
        with mock.patch.multiple(
            cfg, load_config=lambda: {"password": "from-config"}, load_env_file=lambda: {}
        ), mock.patch.dict(os.environ, {}, clear=True):
            _, pw, _, src = cfg.get_credentials_with_source()
        self.assertEqual(pw, "from-config")
        self.assertIn("config.json", src)

    def test_get_credentials_keeps_old_two_tuple_shape(self):
        """老接口不能变形 —— 仓库里别处还在用它。"""
        with mock.patch.multiple(
            cfg, load_config=lambda: {"username": "u", "password": "p"}, load_env_file=lambda: {}
        ), mock.patch.dict(os.environ, {}, clear=True):
            got = cfg.get_credentials()
        self.assertEqual(got, ("u", "p"))


class CredentialConflictTests(unittest.TestCase):
    """多处密码不一致时必须报出来，而且不许回显明文。"""

    def test_conflict_is_reported_with_fingerprints_only(self):
        with mock.patch.multiple(
            cfg, load_config=lambda: {"password": "NEW-secret"}, load_env_file=lambda: {}
        ), mock.patch.dict(os.environ, {"QZCLI_PASSWORD": "OLD-secret"}, clear=False):
            msg = cfg.describe_credential_conflict()
        self.assertTrue(msg, "两处密码不同却没有给出任何提示")
        self.assertIn("环境变量", msg)
        # 明文一个字都不许出现
        self.assertNotIn("NEW-secret", msg)
        self.assertNotIn("OLD-secret", msg)

    def test_no_noise_when_sources_agree(self):
        """值一样就别吵 —— 误报会让人学会忽略这条提示。"""
        with mock.patch.multiple(
            cfg, load_config=lambda: {"password": "same"}, load_env_file=lambda: {}
        ), mock.patch.dict(os.environ, {"QZCLI_PASSWORD": "same"}, clear=False):
            self.assertEqual(cfg.describe_credential_conflict(), "")

    def test_single_source_is_not_a_conflict(self):
        with mock.patch.multiple(
            cfg, load_config=lambda: {"password": "only"}, load_env_file=lambda: {}
        ), mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(cfg.describe_credential_conflict(), "")


class OpsLogFailureTests(unittest.TestCase):
    """返回非 0 = 失败。日志不许把它记成 ok。"""

    def _read_back(self, fn):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "ops.log"
            with mock.patch.dict(os.environ, {"QZCLI_OPS_LOG": str(path)}):
                fn()
            if not path.exists():
                return []
            return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]

    def test_mark_failed_records_error(self):
        def run():
            with opslog.timed("login", target="t") as span:
                span.mark_failed("exit=1")

        rows = self._read_back(run)
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["outcome"], "error",
            "业务失败被记成了 ok —— 这正是当年让排查跑偏的那个 bug",
        )
        self.assertEqual(rows[0]["err_class"], "exit=1")

    def test_exception_still_records_error(self):
        def run():
            try:
                with opslog.timed("login"):
                    raise ValueError("boom")
            except ValueError:
                pass

        rows = self._read_back(run)
        self.assertEqual(rows[0]["outcome"], "error")
        self.assertEqual(rows[0]["err_class"], "ValueError")

    def test_success_path_unchanged(self):
        def run():
            with opslog.timed("login"):
                pass

        rows = self._read_back(run)
        self.assertEqual(rows[0]["outcome"], "ok")
        # record() 会省掉空字段，日志因此更紧凑；这里按「没有或为空」断言
        self.assertEqual(rows[0].get("err_class", ""), "")

    def test_timed_never_swallows_exceptions(self):
        """诊断设施不许改变控制流。"""
        with tempfile.TemporaryDirectory() as d:
            # 必须把日志重定向到临时目录：不隔离的话这条测试会往用户真实的
            # ~/.qzcli/qzcli_ops.log 里写一条假的「login 失败」，而那份日志正是
            # 排查账号锁定时的证据来源 —— 测试污染证据比测试没写更糟。
            with mock.patch.dict(
                os.environ, {"QZCLI_OPS_LOG": str(Path(d) / "ops.log")}
            ):
                with self.assertRaises(ValueError):
                    with opslog.timed("login"):
                        raise ValueError("must propagate")


class OpsLogIsolationTests(unittest.TestCase):
    """测试跑完不许在用户真实的操作日志里留下痕迹。

    2026-09-12 实测:``tests/test_create_interactive.py`` 里两条走真 ``cli.main()``
    的用例，和本文件原先一条没隔离的用例，一起往 ``~/.qzcli/qzcli_ops.log``
    写进了假记录（``argv=["python3 -m unittest", ...]`` 和
    ``argv=["qzcli","create","-i"]``、``duration_ms=0``）。当时正在用这份日志
    给「账号为什么反复被锁」取证，测试写的假条目直接污染了证据。
    """

    def test_default_log_path_is_the_real_state_dir(self):
        """没有 override 时日志落在状态目录里 —— 这就是被测试写脏的那个真实路径。

        注意 ``CONFIG_DIR`` 在 import 时就绑定了，所以运行期改 ``QZCLI_HOME``
        不会改变它;能在测试里隔离日志的唯一手段是 ``QZCLI_OPS_LOG``。
        """
        env = {k: v for k, v in os.environ.items() if k != "QZCLI_OPS_LOG"}
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertEqual(
                (Path(cfg.CONFIG_DIR) / opslog.LOG_NAME).resolve(),
                opslog.log_path().resolve(),
            )

    def test_env_override_is_honoured(self):
        """隔离手段本身必须有效，否则上面那条守门形同虚设。"""
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "nested" / "ops.log"
            with mock.patch.dict(os.environ, {"QZCLI_OPS_LOG": str(target)}):
                opslog.record("login", outcome="ok")
            self.assertTrue(target.exists(), "QZCLI_OPS_LOG 没被 opslog 采纳")

    def test_no_test_invokes_cli_main_without_isolating_the_log(self):
        """走真 ``cli.main()`` 的用例，所在类必须自己把日志重定向掉。

        判据刻意做得粗:凡是出现 ``cli.main()`` 的测试文件,同一个文件里就必须
        出现 ``QZCLI_OPS_LOG``。以后谁再加一条裸的 ``cli.main()`` 用例,这条会红。
        """
        tests_dir = Path(__file__).resolve().parent
        offenders = []
        for f in sorted(tests_dir.glob("test_*.py")):
            text = f.read_text(encoding="utf-8")
            if "cli.main()" in text and "QZCLI_OPS_LOG" not in text:
                offenders.append(f.name)
        self.assertEqual(
            [], offenders,
            "这些测试会往用户真实的 ~/.qzcli/qzcli_ops.log 里写记录，"
            "请在所在类的 setUp 里 patch QZCLI_OPS_LOG 到临时目录: "
            + ", ".join(offenders),
        )


if __name__ == "__main__":
    unittest.main()
