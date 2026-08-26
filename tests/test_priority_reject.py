"""优先级被规格拒绝：翻译成下一步，并记住这次拒绝。

## 真实行为（2026-08-27 A/B 实测）

优先级不只有「空间级两套刻度」这一层。平台还**逐个资源规格**限制可用优先级，
提交阶段直接拒：``InvalidParameter: 任务优先级不在该资源规格允许的范围内``。

同一个 1 卡规格、同一个计算组、同一个项目，**只改优先级**：

- ``--priority 4`` → 被拒
- ``--priority 1`` → 建出来了

## 为什么是「记住」而不是「预检」

InspireSkill 的同类功能是提交前预检，因为它的 quota 列表有一列 Priority。
**我们拿不到那一列**：``task_priority`` 只是 CreateJob 的入参；
``GetWorkspaceTaskQuota`` / ``GetUserTaskQuota`` / ``GetAllQuota`` /
``GetWorkspaceNodeSpecs`` / ``GetScheduleConfig`` 的返回里都没有；
连平台的拒绝消息本身都不说允许范围是什么。

所以只做两件基于已观测事实的事：**翻译** + **记住**。

## 这里守的三条

1. 只拦**已知被拒**的组合，没见过的放行 —— 白名单式预检会把平台后来放开的
   组合永久挡死。
2. 判据不整句硬匹配平台文案 —— 改个标点就失配的判据不如不做。
3. 记录写不进去时**不许影响提交** —— 诊断设施不能反过来搞挂生产。
"""

import json
import os
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REAL_MESSAGE = (
    "任务创建失败: API 请求失败: InvalidParameter: 任务优先级不在该资源规格允许的范围内"
)


class _TmpStore:
    """把学习记录指到临时目录，别污染真实 ~/.qzcli。"""

    def __enter__(self):
        from qzcli import priority

        self._dir = tempfile.TemporaryDirectory()
        self._old = priority.STORE
        priority.STORE = __import__("pathlib").Path(self._dir.name) / "pr.json"
        self.mod = priority
        return priority

    def __exit__(self, *a):
        self.mod.STORE = self._old
        self._dir.cleanup()


class RecognitionTests(unittest.TestCase):
    def test_recognizes_the_real_platform_message(self):
        """喂**真机原话**，不是我想象中的措辞。"""
        from qzcli import priority

        self.assertTrue(priority.is_priority_rejection(REAL_MESSAGE))

    def test_tolerates_wording_drift(self):
        """判据是「优先级 + 规格 + 范围」三要素，不是整句硬匹配。"""
        from qzcli import priority

        self.assertTrue(
            priority.is_priority_rejection("该资源规格允许的优先级范围不包含 4")
        )

    def test_does_not_swallow_unrelated_errors(self):
        """对照：别的错误不能被误判成优先级问题 —— 那会把真正的原因盖掉。"""
        from qzcli import priority

        for other in (
            "AccessForbidden: 您已离开所选项目，无法创建",
            "父项目配额不足：总提交配额限制为 8 GPU",
            "exclude_nodes not enable in workspace",
            "",
        ):
            self.assertFalse(priority.is_priority_rejection(other), other)


class MemoryTests(unittest.TestCase):
    def test_remembers_and_recalls(self):
        with _TmpStore() as p:
            self.assertIsNone(p.known_rejection("ws-1", "spec-1", 4))
            p.remember_rejection("ws-1", "spec-1", 4, gpu_count=1)
            hit = p.known_rejection("ws-1", "spec-1", 4)
            self.assertIsNotNone(hit)
            self.assertEqual(hit["priority"], 4)

    def test_unseen_combinations_are_let_through(self):
        """**核心**：只拦见过的。别的优先级、别的规格、别的空间一律放行。

        反过来（白名单）会把平台后来放开的组合永久挡在门外，而允许范围我们
        根本读不到 —— 白名单从第一天起就是错的。
        """
        with _TmpStore() as p:
            p.remember_rejection("ws-1", "spec-1", 4)
            self.assertIsNone(p.known_rejection("ws-1", "spec-1", 1))  # 换优先级
            self.assertIsNone(p.known_rejection("ws-1", "spec-2", 4))  # 换规格
            self.assertIsNone(p.known_rejection("ws-2", "spec-1", 4))  # 换空间

    def test_write_failure_never_raises(self):
        """写不进去也不能炸 —— 这条挂了会直接让 create 失败。"""
        with _TmpStore() as p:
            p.STORE = __import__("pathlib").Path("/nonexistent-dir-xyz/pr.json")
            with mock.patch("qzcli.priority.CONFIG_DIR", "/nonexistent-dir-xyz"):
                p.remember_rejection("ws-1", "spec-1", 4)  # 不抛异常即通过

    def test_corrupt_store_is_treated_as_empty(self):
        """文件被写坏时当没有，而不是让每次 create 都崩。"""
        with _TmpStore() as p:
            p.STORE.parent.mkdir(parents=True, exist_ok=True)
            p.STORE.write_text("{ 这不是 json", encoding="utf-8")
            self.assertIsNone(p.known_rejection("ws-1", "spec-1", 4))
            p.remember_rejection("ws-1", "spec-1", 4)
            self.assertIsNotNone(p.known_rejection("ws-1", "spec-1", 4))

    def test_store_is_capped(self):
        with _TmpStore() as p:
            p.MAX_ENTRIES = 5
            old = p.MAX_ENTRIES
            try:
                for i in range(12):
                    p.remember_rejection("ws", f"spec-{i}", 4)
                data = json.loads(p.STORE.read_text(encoding="utf-8"))
                self.assertLessEqual(len(data), 12)
            finally:
                p.MAX_ENTRIES = old


class AdviceTests(unittest.TestCase):
    def test_advice_names_a_concrete_next_command(self):
        """不能只说「优先级不对」—— 必须给出照着敲就能跑的下一步。"""
        from qzcli import priority

        text = priority.explain("ws-1", "spec-1", 4, gpu_count=1, compute_group_name="训练区")
        self.assertIn("--priority 1", text)
        self.assertIn("训练区", text)

    def test_advice_only_suggests_lower_priorities(self):
        """被规格拒了往**高**调没有意义，建议里不许出现更高的值。"""
        from qzcli import priority

        self.assertEqual(priority._alternatives(4), [1])
        self.assertEqual(priority._alternatives(10, fair_scale=False), list(range(1, 10)))

    def test_advice_admits_the_range_is_unreadable(self):
        """必须说清「允许范围读不到」，否则用户会以为 qzcli 知道而没告诉他。"""
        from qzcli import priority

        self.assertIn("读", priority.explain("ws", "spec", 4))

    def test_known_rejection_message_says_when_and_how_to_override(self):
        from qzcli import priority

        msg = priority.explain_known(
            {"workspace_id": "ws", "spec_id": "s", "priority": 4, "at": 1770000000}
        )
        self.assertIn("--force-priority", msg)
        self.assertIn("被平台拒过", msg)


if __name__ == "__main__":
    unittest.main()
