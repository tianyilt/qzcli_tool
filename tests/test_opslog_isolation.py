"""把整个测试进程的操作日志重定向到临时目录，并守住这一点。

## 为什么这个机制长这样（踩过两个坑）

**坑一:逐个文件打补丁不够。** 2026-09-12 先是三条用例往用户真实的
``~/.qzcli/qzcli_ops.log`` 里写了假记录，逐个修好之后，``api._relogin`` 新增了
一处 ``opslog.record()``，**四个一行没改的既有测试文件**立刻又开始漏
（test_auth_retry / test_auth_before_fanout / test_job_v2 / test_relogin_dedup）。
漏点会随生产代码长出来，而补丁的人不会同时想起那四个文件。

**坑二:``tests/__init__.py`` 在默认命令下不会被导入。** 本仓文档写的是
``python3 -m unittest discover -s tests``；``-s`` 指向目录时 unittest 把该目录
当 top-level dir、按顶层模块导入，**包的 ``__init__`` 整个被跳过**。实测
``discover -s tests`` 不执行、``python3 -m unittest tests.test_x`` 才执行。
把隔离只放在 ``__init__`` 里 = 看起来做了、实际没做。

**所以放在这里。** 依据是 unittest discover 的一个确定行为：**它先导入全部
匹配 ``test*.py`` 的模块，再开始执行任何一条用例**。于是任一模块在 import 期
设好环境变量，对全部用例都生效。本文件名匹配默认模式，必被导入。
``tests/__init__.py`` 则覆盖 ``python3 -m unittest tests.test_x`` 那条路径。

这份日志是排查「账号为什么反复被锁」的取证来源。被测试写脏比没记更糟 ——
会看到一堆本机从未发生过的失败，而排查的人正指望它说真话。
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# —— 模块 import 期就生效，不能等到 setUp ——
if not os.environ.get("QZCLI_OPS_LOG"):
    _TMP = tempfile.mkdtemp(prefix="qzcli-test-opslog-")
    os.environ["QZCLI_OPS_LOG"] = os.path.join(_TMP, "ops.log")

from qzcli import opslog  # noqa: E402
from qzcli.config import CONFIG_DIR  # noqa: E402


class OpsLogIsolationTests(unittest.TestCase):
    def test_log_path_is_not_the_users_real_audit_log(self):
        """整个测试进程都不许把日志指向用户真实的状态目录。

        这条是**运行期**判据，跟隔离是用什么手段做的无关 —— 以后换机制、
        或者某条用例自己 patch 环境变量忘了收回，都会在这里变红。
        """
        real = (Path(CONFIG_DIR) / opslog.LOG_NAME).resolve()
        actual = opslog.log_path().resolve()
        self.assertNotEqual(
            real, actual,
            "测试进程正在往用户真实的操作日志里写 —— 那是排查账号锁定的取证来源。"
            f"\n  实际路径: {actual}",
        )

    def test_env_override_is_the_mechanism_and_it_works(self):
        """隔离手段本身必须有效，否则上面那条守门形同虚设。"""
        with tempfile.TemporaryDirectory() as d:
            target = Path(d) / "nested" / "ops.log"
            old = os.environ.get("QZCLI_OPS_LOG")
            os.environ["QZCLI_OPS_LOG"] = str(target)
            try:
                opslog.record("login", outcome="ok")
            finally:
                if old is None:
                    os.environ.pop("QZCLI_OPS_LOG", None)
                else:
                    os.environ["QZCLI_OPS_LOG"] = old
            self.assertTrue(target.exists(), "QZCLI_OPS_LOG 没被 opslog 采纳")

    def test_recorded_ops_covers_relogin(self):
        """自动重登必须在册。

        它不经过 ``main()`` 的分发点，不在册的话操作日志里一个字都没有 ——
        而它恰恰是最容易悄悄打认证、把账号锁掉的那条路径。
        """
        self.assertIn("relogin", opslog.RECORDED_OPS)


if __name__ == "__main__":
    unittest.main()
