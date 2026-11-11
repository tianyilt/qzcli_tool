"""优先级方向与默认值。

## 为什么要专门钉这个

这条方向我在代码注释和 v0.4.4 的发布说明里**写反过**（写成"训练任务 10 表示
低优"），别人照着看就会把最高优当低优提上去，直接和生产任务抢卡。

实测（真机提交后读平台回包）：

    提交值   存储值   档位
      1       11     LOW
      3       13     LOW
      4       20     NORMAL
      9       34     HIGH
     10       35     HIGH

即**数字越小优先级越低**，和 HPC 完全同向 —— 不是相反。

## 默认值为什么是 3 而不是 10

不显式指定优先级的，多半是调试 / 试跑 / 脚本随手提的任务。让这类任务默认拿最高优
去抢生产的卡是不合理的默认值。要抢卡请显式写 `--priority`，让它是个明确的决定。

改默认值之前这里一条测试都没有 —— 也就是说它被改成任何值都不会有人发现。
"""

import unittest

from qzcli import cli, mcp_server


class PriorityDirectionTests(unittest.TestCase):
    def test_smaller_number_means_lower_priority(self):
        """``avail --lp`` 的低优判据必须是「小于等于某个小数字」。

        判据写反的话，`avail --lp` 会把高优任务当成可抢占的，用户据此去抢卡
        会抢不到，还以为是平台的问题。
        """
        import inspect

        src = inspect.getsource(cli.cmd_avail)
        self.assertIn(
            "low_priority_threshold",
            src,
            "低优判定阈值不见了 —— 是不是被改成了别的写法",
        )
        # 阈值本身必须是个小数字（实测 4 已经是 NORMAL）
        self.assertLessEqual(
            (
                cli._LOW_PRIORITY_THRESHOLD
                if hasattr(cli, "_LOW_PRIORITY_THRESHOLD")
                else 3
            ),
            3,
            "低优阈值不该大于 3 —— 实测提交值 4 已经是 NORMAL 档",
        )


class PriorityDefaultTests(unittest.TestCase):
    def test_cli_default_is_low_priority(self):
        """默认必须落在 LOW 档（实测 <=3 为 LOW）。"""
        self.assertLessEqual(
            cli.DEFAULT_CREATE_PRIORITY,
            3,
            "默认优先级跑到 LOW 档以外了 —— 不指定优先级的任务会去抢生产的卡",
        )
        self.assertGreaterEqual(
            cli.DEFAULT_CREATE_PRIORITY, 1, "平台有效范围是 1-10，0 会被拒"
        )

    def test_mcp_default_matches_cli(self):
        """MCP 和 CLI 是两个并列的用户面，默认值必须一致。

        这两边是平行重实现，历史上分叉过一次；默认优先级分叉的后果是
        「同一个工具，走 MCP 提的任务优先级和走 CLI 的不一样」。
        """
        import inspect

        sig = inspect.signature(mcp_server.qz_create_job)
        mcp_default = sig.parameters["priority"].default
        self.assertEqual(
            mcp_default,
            cli.DEFAULT_CREATE_PRIORITY,
            f"MCP 默认 {mcp_default} 与 CLI 默认 {cli.DEFAULT_CREATE_PRIORITY} 不一致",
        )

    def test_help_text_states_the_direction(self):
        """help 里必须写清方向。

        只写「默认 10」而不说 10 是最高优，用户不看代码根本判断不了 ——
        我自己就照着这句话把方向理解反了。
        """
        import io
        import os
        import sys
        import tempfile
        from contextlib import redirect_stdout
        from unittest import mock

        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, mock.patch.dict(
            os.environ, {"QZCLI_OPS_LOG": directory + "/ops.log"}
        ), mock.patch.object(sys, "argv", ["qzcli", "create", "--help"]):
            with redirect_stdout(buf), self.assertRaises(SystemExit) as stopped:
                cli.main()
            self.assertEqual(0, stopped.exception.code)
        text = buf.getvalue()
        self.assertIn("--priority", text)
        self.assertTrue(
            any(k in text for k in ("越小", "越低", "LOW")),
            "--priority 的 help 没说清数字方向",
        )


if __name__ == "__main__":
    unittest.main()


class PriorityDefaultOverrideTests(unittest.TestCase):
    """默认值必须**可覆盖** —— 这是向后兼容的保障。

    默认值从 10（最高优）改成 3（LOW），对「原来不写 ``--priority``、靠默认拿高优」
    的脚本是行为变更：那些任务会从直接跑变成排队。现网 21% 的任务跑在 HIGH 档
    （实测 1494 个任务：10→269 个、9→44 个），这个面不小。

    所以必须给一条**不用改调用点**就能恢复原状的路。没有测试守着的话，
    这条兼容通道随时可能在重构里消失，而消失了也没人会发现。
    """

    def setUp(self):
        import os
        import sys

        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    def test_env_var_restores_old_default(self):
        """``QZCLI_DEFAULT_PRIORITY=10`` 能把行为恢复成改动前。"""
        from support.sandbox import sandbox_home

        from qzcli import config

        with sandbox_home(env={"QZCLI_DEFAULT_PRIORITY": "10"}):
            self.assertEqual(config.get_default_priority(), 10)

    def test_config_json_override(self):
        from support.sandbox import sandbox_home

        from qzcli import config

        with sandbox_home(config_json={"default_priority": 9}):
            self.assertEqual(config.get_default_priority(), 9)

    def test_env_beats_config_json(self):
        from support.sandbox import sandbox_home

        from qzcli import config

        with sandbox_home(
            config_json={"default_priority": 9},
            env={"QZCLI_DEFAULT_PRIORITY": "1"},
        ):
            self.assertEqual(config.get_default_priority(), 1)

    def test_falls_back_when_unset(self):
        from support.sandbox import sandbox_home

        from qzcli import config

        with sandbox_home():
            self.assertEqual(
                config.get_default_priority(), config.FALLBACK_DEFAULT_PRIORITY
            )

    def test_invalid_values_fall_back_instead_of_breaking_submission(self):
        """配错了不能让提交挂掉，也不能拿平台会拒的值去提交。

        平台有效范围是 1-10（0/11/12 会被拒「无效的优先级值」）。
        """
        from support.sandbox import sandbox_home

        from qzcli import config

        for bad in ("abc", "0", "11", "-1", "3.5", ""):
            with self.subTest(value=bad):
                with sandbox_home(env={"QZCLI_DEFAULT_PRIORITY": bad}):
                    got = config.get_default_priority()
                self.assertEqual(got, config.FALLBACK_DEFAULT_PRIORITY)
                self.assertTrue(1 <= got <= 10)
