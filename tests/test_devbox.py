"""``qzcli devbox`` 的契约：持久性判据 + 重启后的合并语义。

## 为什么要有这些测试

这个功能的失败方式**全是静默的**：

1. 把 home 挪到一个「看起来像持久盘、其实是容器 overlay 层」的目录 —— 不会报错，
   重启才发现全丢了。守它的是 ``is_persistent`` 的 ``st_dev`` 判据。
2. 重启后重跑时，用「搬迁」而不是「合并」的语义 —— 会把某一边的 session 直接
   覆盖掉。守它的是本文件下半段那组合并测试，核心断言是**跑前跑后文件一个不少**。

两类错误都不会抛异常、不会有任何输出异常，只会在某天让人发现「我上周的记录呢」。
所以这里的断言都盯着**数据还在不在**，而不是「函数返回没报错」。
"""

import os
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qzcli import devbox  # noqa: E402


def _touch(p: Path, text: str = "x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    return p


def _count_files(root: Path):
    return sum(len(f) for _, _, f in os.walk(str(root)))


class PersistenceGuardTests(unittest.TestCase):
    """判据本身：同一个文件系统 = 临时层，必须拒绝。"""

    def test_same_filesystem_is_not_persistent(self):
        with tempfile.TemporaryDirectory() as tmp:
            # tmp 与 / 在不在同一 fs 取决于机器，所以直接拿 root 自比，判据恒成立
            self.assertFalse(devbox.is_persistent("/", root="/"))
            # 用 tmp 自己当 root，则 tmp 与自己同 fs -> 不持久
            self.assertFalse(devbox.is_persistent(tmp, root=tmp))

    def test_nonexistent_path_falls_back_to_nearest_parent(self):
        """要能判断「还没建出来的目标目录」会落在哪个 fs 上。"""
        with tempfile.TemporaryDirectory() as tmp:
            deep = os.path.join(tmp, "a", "b", "c")
            self.assertFalse(devbox.is_persistent(deep, root=tmp))

    def test_run_refuses_non_persistent_root(self):
        """核心守卫：目标与 / 同盘就拒绝。

        断言只看**环境无关**的部分 —— 文案分容器/非容器两种，这个测试在 Mac 上
        和在开发机上都要能跑。两种文案各自由下面两条钉死。
        """
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "persist"
            root.mkdir()
            home = Path(tmp) / "home"
            home.mkdir()
            with self.assertRaises(devbox.DevboxError) as ctx:
                devbox.run(str(root), home=str(home))
            self.assertIn("拒绝", str(ctx.exception))

    def test_message_says_ephemeral_inside_container(self):
        """在容器里（/ 是 overlay）必须点明「重启即失」，否则用户不知道严重性。"""
        orig = devbox.root_is_ephemeral
        devbox.root_is_ephemeral = lambda: True
        self.addCleanup(setattr, devbox, "root_is_ephemeral", orig)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "p"
            root.mkdir()
            with self.assertRaises(devbox.DevboxError) as ctx:
                devbox.run(str(root), home=tmp)
            self.assertIn("重启即失", str(ctx.exception))

    def test_label_not_scary_outside_container(self):
        """非容器机器上不能把「与 / 同盘」标成「临时」—— 会白吓用户一跳。"""
        orig = devbox.root_is_ephemeral
        devbox.root_is_ephemeral = lambda: False
        self.addCleanup(setattr, devbox, "root_is_ephemeral", orig)
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(devbox.fs_label(tmp, root=tmp), "与 / 同盘")

    def test_prefers_account_level_dir_over_project_level(self):
        """每台开发机都同时挂账号级和项目级个人目录，必须能自己挑，不能一律拒绝。

        第一版「命中多个就报错」实测等于开箱即用是坏的：真机上永远命中两个
        （/inspire/hdd/global_user/<我> 和 /inspire/ssd/project/<项目>/<我>），
        于是 devbox init 永远跑不起来。dotfile 属于「人」不属于「项目」，
        所以优先账号级那个。
        """
        cands = [
            "/inspire/ssd/project/video-generation/liangtianyi-x",
            "/inspire/hdd/global_user/liangtianyi-x",
        ]
        with mock.patch.object(devbox, "is_persistent", lambda p, root="/": True), \
             mock.patch("glob.glob", lambda pat: cands), \
             mock.patch("os.path.isdir", lambda p: True), \
             mock.patch("os.access", lambda p, m: True):
            self.assertEqual(
                devbox.detect_persist_root(), "/inspire/hdd/global_user/liangtianyi-x"
            )

    def test_detect_refuses_explicit_non_persistent_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(devbox.DevboxError) as ctx:
                devbox.detect_persist_root(tmp)
            # 要点明「global_user 那一层本身也是临时的」这个真实陷阱
            self.assertIn("global_user", str(ctx.exception))


class HistoryMergeTests(unittest.TestCase):
    def test_union_without_timestamps(self):
        merged, counts = devbox.merge_history("a\nb\n", "b\nc\n")
        self.assertEqual(merged.splitlines(), ["a", "b", "c"])
        self.assertEqual(counts["merged"], 3)

    def test_zsh_extended_history_sorted_by_time(self):
        old = ": 100:0;first\n"
        new = ": 50:0;earlier\n"
        merged, _ = devbox.merge_history(old, new)
        self.assertEqual(
            merged.splitlines(), [": 50:0;earlier", ": 100:0;first"]
        )

    def test_counts_report_both_sides(self):
        """必须能打印「两边各多少条、合并后多少条」，否则用户无法确认没被吞。"""
        _, counts = devbox.merge_history("a\nb\n", "b\nc\nd\n")
        self.assertEqual((counts["persist"], counts["local"]), (2, 3))


class DirMergeTests(unittest.TestCase):
    """重启场景：持久盘有旧 session、/root 侧有新 session，两边都要留下。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        base = Path(self._tmp.name)
        self.persist = base / "persist" / ".claude"
        self.local = base / "home" / ".claude"
        self.conflict = base / "persist" / devbox.CONFLICT_DIRNAME / "t"

    def test_disjoint_sessions_all_survive(self):
        _touch(self.persist / "projects" / "old.jsonl", "old")
        _touch(self.local / "projects" / "new.jsonl", "new")
        devbox.merge_dir(self.persist, self.local, self.conflict)
        self.assertTrue((self.persist / "projects" / "old.jsonl").exists())
        self.assertTrue((self.persist / "projects" / "new.jsonl").exists())

    def test_same_name_keeps_larger_and_backs_up_loser(self):
        """session 文件只增不减，所以保留更大的；输的那份必须留备份，不许丢。"""
        _touch(self.persist / "s.jsonl", "short")
        _touch(self.local / "s.jsonl", "much longer content here")
        stats = devbox.merge_dir(self.persist, self.local, self.conflict)
        self.assertEqual(
            (self.persist / "s.jsonl").read_text(), "much longer content here"
        )
        self.assertIn("s.jsonl", stats["conflicts"])
        self.assertTrue((self.conflict / "s.jsonl").exists())
        self.assertEqual((self.conflict / "s.jsonl").read_text(), "short")

    def test_persist_wins_when_bigger_but_local_still_backed_up(self):
        _touch(self.persist / "s.jsonl", "much longer content here")
        _touch(self.local / "s.jsonl", "short")
        devbox.merge_dir(self.persist, self.local, self.conflict)
        self.assertEqual(
            (self.persist / "s.jsonl").read_text(), "much longer content here"
        )
        self.assertEqual((self.conflict / "s.jsonl").read_text(), "short")

    def test_dry_run_changes_nothing(self):
        _touch(self.persist / "a.jsonl", "a")
        _touch(self.local / "b.jsonl", "b")
        before = _count_files(self.persist)
        devbox.merge_dir(self.persist, self.local, self.conflict, dry_run=True)
        self.assertEqual(_count_files(self.persist), before)


class EndToEndRestartTests(unittest.TestCase):
    """完整跑一遍 run()，模拟「首次持久化 -> 重启 -> 再跑一次」。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.base = Path(self._tmp.name)
        self.root = self.base / "persist"
        self.root.mkdir()
        self.home = self.base / "home"
        self.home.mkdir()
        # 让 is_persistent 认为 root 是持久的：把 "/" 判据换成 home 所在 fs
        self._orig = devbox.is_persistent
        devbox.is_persistent = lambda p, root="/": str(p).startswith(str(self.root))
        self.addCleanup(setattr, devbox, "is_persistent", self._orig)

    def test_first_run_then_restart_merges_without_loss(self):
        _touch(self.home / ".claude" / "projects" / "s1.jsonl", "session one")
        _touch(self.home / ".bash_history", "cmd-a\ncmd-b\n")

        devbox.run(str(self.root), home=str(self.home))
        self.assertTrue((self.home / ".claude").is_symlink())
        self.assertTrue((self.root / ".claude" / "projects" / "s1.jsonl").exists())

        # —— 模拟重启：软链没了，agent 在 /root 侧重新写了一份新数据 ——
        (self.home / ".claude").unlink()
        _touch(self.home / ".claude" / "projects" / "s2.jsonl", "session two")
        _touch(self.home / ".bash_history", "cmd-c\n")

        devbox.run(str(self.root), home=str(self.home))

        # 新旧 session 都必须在 —— 这是整个功能的核心承诺
        self.assertTrue((self.root / ".claude" / "projects" / "s1.jsonl").exists())
        self.assertTrue((self.root / ".claude" / "projects" / "s2.jsonl").exists())
        hist = (self.root / ".bash_history").read_text().splitlines()
        self.assertEqual(sorted(hist), ["cmd-a", "cmd-b", "cmd-c"])

    def test_history_is_not_a_symlink(self):
        """历史文件被软链的话，shell 的 rename 保存会把软链换成真文件，静默失效。"""
        _touch(self.home / ".zsh_history", ": 1:0;hi\n")
        devbox.run(str(self.root), home=str(self.home))
        self.assertFalse((self.home / ".zsh_history").is_symlink())

    def test_histfile_written_into_rc(self):
        devbox.run(str(self.root), home=str(self.home))
        self.assertIn("HISTFILE", (self.home / ".zshrc").read_text())

    def test_idempotent(self):
        _touch(self.home / ".claude" / "a.jsonl", "a")
        devbox.run(str(self.root), home=str(self.home))
        n1 = _count_files(self.root)
        devbox.run(str(self.root), home=str(self.home))
        self.assertEqual(_count_files(self.root), n1)

    def test_dry_run_makes_no_changes(self):
        _touch(self.home / ".claude" / "a.jsonl", "a")
        before_home = _count_files(self.home)
        before_root = _count_files(self.root)
        devbox.run(str(self.root), home=str(self.home), dry_run=True)
        self.assertEqual(_count_files(self.home), before_home)
        self.assertEqual(_count_files(self.root), before_root)

    def test_ssh_excluded_by_default(self):
        _touch(self.home / ".ssh" / "id_rsa", "PRIVATE")
        devbox.run(str(self.root), home=str(self.home))
        self.assertFalse((self.home / ".ssh").is_symlink())
        self.assertFalse((self.root / ".ssh").exists())

    def test_config_conflict_backed_up_not_overwritten(self):
        _touch(self.root / ".gitconfig", "persisted")
        _touch(self.home / ".gitconfig", "local-edit")
        report = devbox.run(str(self.root), home=str(self.home))
        self.assertEqual((self.root / ".gitconfig").read_text(), "persisted")
        conflicts = [i for i in report["items"] if i.get("conflict")]
        self.assertTrue(conflicts, "配置冲突必须上报，不能静默丢掉本地那份")
        backup = Path(report["conflict_dir"]) / ".gitconfig"
        self.assertEqual(backup.read_text(), "local-edit")


if __name__ == "__main__":
    unittest.main()
