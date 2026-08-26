"""两个真实事故的回归测试。

两条都不是假想缺陷，是 2026-08 真的发生过、且**都以静默或误导的方式发生**：

1. **`res -u` 把缓存清空**：账号锁定期间跑了一次
   `qzcli res -w 分布式训练空间 -u`，鉴权失败被吞成「拉到 0 个计算组」，
   然后这份空结果被写回缓存，把好数据冲掉。之后所有命令报「未找到计算组」，
   看起来像平台改了名字，实际是本地缓存被自己清了。

2. **封锁提示不说怎么恢复**：原话只有「请先解决登录问题再重试」，
   于是所有人（包括我）都以为要「等一会儿」。**等是没用的** —— 凭据类封锁
   不看时间，只有一次成功的 `qzcli login` 能清掉它。这条误导直接让人白等。
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qzcli import api as api_mod  # noqa: E402
from qzcli import cli as cli_mod  # noqa: E402


class ResourcesEmptyGuardTests(unittest.TestCase):
    """拉到空就不许覆盖非空缓存。"""

    def test_empty_result_detected(self):
        self.assertTrue(cli_mod._resources_look_empty(None))
        self.assertTrue(cli_mod._resources_look_empty({}))
        self.assertTrue(
            cli_mod._resources_look_empty(
                {"compute_groups": [], "projects": [], "specs": []}
            )
        )

    def test_nonempty_result_detected(self):
        self.assertFalse(
            cli_mod._resources_look_empty({"compute_groups": [{"id": "lcg-1"}]})
        )
        self.assertFalse(cli_mod._resources_look_empty({"projects": [{"id": "p-1"}]}))

    def test_quick_mode_empty_specs_is_not_treated_as_failure(self):
        """quick 模式下 specs 本来就是空的 —— 拿它当判据会把正常刷新误判成失败。

        实测：冻结版 home 的 resources.json 里 16 个工作空间 specs 全为 0，
        但 compute_groups 都有值，那是完全正常的 quick 刷新结果。
        """
        self.assertFalse(
            cli_mod._resources_look_empty(
                {"compute_groups": [{"id": "lcg-1"}], "projects": [], "specs": []}
            )
        )


class CredentialBlockMessageTests(unittest.TestCase):
    """封锁提示必须给出可执行的恢复路径。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.cd = Path(self._tmp.name) / ".relogin.cooldown"
        patcher = mock.patch.object(api_mod, "_cooldown_path", lambda: self.cd)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_message_says_waiting_is_useless(self):
        """最关键的一条：必须说清「等待无效」，否则用户会白等。"""
        text = api_mod.describe_credential_block()
        self.assertIn("等待无效", text)

    def test_message_gives_the_exact_recovery_command(self):
        self.assertIn("qzcli login", api_mod.describe_credential_block())

    def test_message_points_at_the_record_file(self):
        text = api_mod.describe_credential_block()
        self.assertIn(str(self.cd), text)

    def test_includes_when_it_was_recorded(self):
        """带上记录时间，用户才能判断「这是刚才的还是上周的」。"""
        self.cd.write_text("1787000000.0\n账号被锁定", encoding="utf-8")
        self.assertIn("记于", api_mod.describe_credential_block())

    def test_no_record_file_still_produces_usable_message(self):
        """记录文件读不到时也不能崩，恢复指引照样要给全。"""
        text = api_mod.describe_credential_block()
        self.assertIn("qzcli login", text)
        self.assertIn("等待无效", text)


if __name__ == "__main__":
    unittest.main()
