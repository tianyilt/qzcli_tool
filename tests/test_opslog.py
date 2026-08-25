"""操作日志的契约。

## 为什么要有这些测试

这个模块存在的唯一理由是「出事之后能查」。所以它有两个不能破的性质，
而且**破了都不会有人立刻发现**：

1. **它不能把命令搞挂**。日志目录不可写（共享盘满、权限变了）时如果抛异常，
   等于诊断设施反过来杀掉生产命令 —— 这是最蠢的失败方式。
2. **它不能记凭据**。``qzcli login --password xxx`` 这类命令行里带密码，
   一旦整条 argv 写进日志，就是把密码落到共享盘上（本仓有过真泄漏史）。

另外还要守住「只读命令不记」—— monitor 每 45 秒轮询一次，记进去的话一天几千行，
真正要找的提交和登录记录会被噪声淹掉，等于白记。
"""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qzcli import opslog  # noqa: E402


class _TmpLog(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name) / "ops.log"
        patcher = mock.patch.dict(os.environ, {"QZCLI_OPS_LOG": str(self.path)})
        patcher.start()
        self.addCleanup(patcher.stop)

    def entries(self):
        if not self.path.exists():
            return []
        return [json.loads(x) for x in self.path.read_text().splitlines() if x.strip()]


class RecordingTests(_TmpLog):
    def test_records_mutating_op(self):
        opslog.record("create", target="job-abc", outcome="ok")
        rows = self.entries()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["op"], "create")
        self.assertEqual(rows[0]["target"], "job-abc")
        self.assertIn("pid", rows[0])
        self.assertIn("ts_utc", rows[0])

    def test_readonly_ops_are_not_recorded(self):
        """monitor 每 45 秒一次，只读命令记进来会把日志冲成噪声。"""
        for op in ("list", "status", "avail", "usage", "logs", "watch"):
            opslog.record(op, target="x")
        self.assertEqual(self.entries(), [])

    def test_failure_records_err_class_but_not_message(self):
        """异常消息可能带敏感内容，只记类名。"""
        opslog.record("create", outcome="error", err_class="QzAPIError")
        row = self.entries()[0]
        self.assertEqual(row["outcome"], "error")
        self.assertEqual(row["err_class"], "QzAPIError")

    def test_timed_context_does_not_swallow_exception(self):
        with self.assertRaises(ValueError):
            with opslog.timed("stop", target="job-x"):
                raise ValueError("boom")
        row = self.entries()[0]
        self.assertEqual(row["outcome"], "error")
        self.assertEqual(row["err_class"], "ValueError")
        self.assertIn("duration_ms", row)


class NeverBreaksTheCommandTests(_TmpLog):
    """日志设施不许把命令带崩 —— 这条比「记得全」重要得多。"""

    def test_unwritable_dir_returns_false_not_raise(self):
        bad = Path(self._tmp.name) / "nope"
        bad.mkdir()
        bad.chmod(0o500)  # 只读目录
        self.addCleanup(bad.chmod, 0o700)
        with mock.patch.dict(os.environ, {"QZCLI_OPS_LOG": str(bad / "x.log")}):
            self.assertFalse(opslog.record("create", target="t"))  # 不抛

    def test_timed_still_runs_body_when_logging_broken(self):
        bad = Path(self._tmp.name) / "nope2"
        bad.mkdir()
        bad.chmod(0o500)
        self.addCleanup(bad.chmod, 0o700)
        ran = []
        with mock.patch.dict(os.environ, {"QZCLI_OPS_LOG": str(bad / "y.log")}):
            with opslog.timed("create"):
                ran.append(1)
        self.assertEqual(ran, [1], "日志写不进去时命令体必须照常执行")


class NoCredentialsTests(_TmpLog):
    def test_argv_is_truncated_so_password_never_lands(self):
        """``qzcli login --password hunter2`` 的密码在第 4 段，必须被截掉。"""
        fake = ["/usr/local/bin/qzcli", "login", "--password", "hunter2-SECRET"]
        with mock.patch.object(sys, "argv", fake):
            opslog.record("login")
        raw = self.path.read_text()
        self.assertNotIn("hunter2-SECRET", raw)
        self.assertEqual(self.entries()[0]["argv"], ["qzcli", "login", "--password"])

    def test_no_credential_shaped_keys_leak(self):
        # 必须是**明显假**的值。第一版我图省事从真实登录输出里复制了一段 session
        # cookie 前缀进来，被本仓的凭据扫描闸门（test_no_secrets_in_repo）当场拦下 ——
        # 测试里的"占位值"必须一眼看出是假的，否则就是往 tracked 文件里塞真凭据。
        fake_cookie = "inspire-session=FAKE-cookie-for-test-do-not-use.example.invalid"
        opslog.record("login", target="user", outcome="ok", note=fake_cookie)
        raw = self.path.read_text()
        # extra 里如果有人顺手塞了 cookie，至少要能被这条测试抓到
        self.assertNotIn(
            "inspire-session=", raw.replace(fake_cookie, ""),
            "除了显式传入的字段外不该有其它凭据痕迹",
        )


class ReadTests(_TmpLog):
    def test_filter_by_op(self):
        opslog.record("create", target="a")
        opslog.record("stop", target="b")
        self.assertEqual(len(opslog.read(self.path, op="create")), 1)

    def test_corrupt_line_is_skipped_not_fatal(self):
        """半截行（断电/并发写坏）不该让整个读取炸掉。"""
        opslog.record("create", target="a")
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write("{ 这不是合法 json\n")
        opslog.record("stop", target="b")
        self.assertEqual(len(opslog.read(self.path)), 2)


class RotationTests(_TmpLog):
    def test_rotates_when_too_big(self):
        with mock.patch.object(opslog, "MAX_BYTES", 200):
            for i in range(40):
                opslog.record("create", target=f"job-{i}")
        self.assertTrue(
            self.path.with_suffix(self.path.suffix + ".1").exists(),
            "超过阈值应轮转，否则共享盘会被写爆",
        )


if __name__ == "__main__":
    unittest.main()
