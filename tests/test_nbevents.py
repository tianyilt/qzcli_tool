"""开发机事件诊断的契约。

## 为什么要有这些测试

qzcli 的卖点之一是「任务排队时直接给出平台侧真实原因」，但这个能力原来**只覆盖
训练任务**：`qzcli events` 走的是 train ListJobEvents，开发机是另一类对象、另一个
Action。结果是开发机排队时这条命令什么都给不出来 —— 2026-08 有人对着一台 PENDING
的开发机猜了 55 分钟，而平台其实一直存着答案。

这里守两件事：

1. **把 K8s 原文翻译成能行动的结论**。平台只给一段 content，没有 reason/type 字段，
   原文是 "0/1100 nodes are unavailable: 177 Insufficient memory, 260 Insufficient
   cpu, 599 node(s) didn't match Pod's node affinity/selector"。三个原因里，
   "599 台不匹配亲和性" 才是根因（计算组挑错），另外两个是噪声。**排序错了就会
   把人引向"等等看"而不是"换个组"。**
2. **不跨轮次翻旧账**。开发机停了再启会在同一个事件列表里攒多轮记录。不切轮次的话，
   一台现在跑得好好的机器会被上一轮的失败事件误判 —— 实测就这么把一台 RUNNING
   的机器报成了「配额不够」。
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qzcli import nbevents  # noqa: E402

# 真机抓下来的原文（2026-08-25，一台排了 55 分钟没排上的开发机）
REAL_UNSCHED = (
    "0/1100 nodes are unavailable: 177 Insufficient memory, 265 Insufficient cpu, "
    "594 node(s) didn't match Pod's node affinity/selector, 64 node(s) were unschedulable."
)


def _ev(content, ts="1776590164000"):
    return {"content": content, "created_at": ts}


class ClassifyTests(unittest.TestCase):
    def test_affinity_wins_over_insufficient(self):
        """同一句里既有"亲和性不匹配"又有"资源不足"时，必须报前者。

        因为它们指向**相反的动作**：亲和性不匹配 = 换计算组（等没用）；
        资源不足 = 可以等或降规格。报错了会把人引向白等。
        """
        self.assertEqual(
            nbevents.classify_notebook_event(REAL_UNSCHED)["kind"], "affinity"
        )

    def test_preemption_recognized(self):
        k = nbevents.classify_notebook_event("Pod was preempted by higher priority")
        self.assertEqual(k["kind"], "preempted")
        self.assertIn("优先级", k["advice"])

    def test_image_failure_vs_pulling(self):
        self.assertEqual(
            nbevents.classify_notebook_event('Failed to pull image "x"')["kind"],
            "image",
        )
        self.assertEqual(
            nbevents.classify_notebook_event('Pulling image "x"')["kind"],
            "image_pulling",
        )

    def test_unknown_is_not_silently_dropped(self):
        """认不出来要如实说，不要硬套一个分类 —— 平台文案会变。"""
        self.assertEqual(
            nbevents.classify_notebook_event("some brand new message")["kind"],
            "unknown",
        )


class BreakdownTests(unittest.TestCase):
    def test_sorted_by_node_count_desc(self):
        """按机器数从多到少 —— 最多的那条通常就是根因。"""
        parts = nbevents.summarize_unschedulable(REAL_UNSCHED)
        self.assertTrue(parts[0].startswith("594"), parts[0])
        self.assertIn("不属于你选的计算组", parts[0])

    def test_translates_k8s_reasons_to_chinese(self):
        parts = " / ".join(nbevents.summarize_unschedulable(REAL_UNSCHED))
        for expect in ("内存不足", "CPU 不足", "机器被禁止调度"):
            self.assertIn(expect, parts)

    def test_non_unschedulable_returns_empty(self):
        self.assertEqual(nbevents.summarize_unschedulable("Started container x"), [])


class CurrentRoundTests(unittest.TestCase):
    """开发机停了再启，事件列表里会攒多轮。诊断只能看最近一轮。"""

    def test_only_last_round_kept(self):
        evs = [
            _ev("exceeded quota: too much"),          # 上一轮失败
            _ev("Successfully assigned foo to node1"),  # ← 新一轮从这里开始
            _ev("Pulling image \"x\""),
            _ev("Started container foo"),
        ]
        cur = nbevents.current_round(evs)
        self.assertEqual(len(cur), 3)
        self.assertNotIn("exceeded quota", " ".join(e["content"] for e in cur))

    def test_no_start_marker_keeps_everything(self):
        evs = [_ev(REAL_UNSCHED), _ev(REAL_UNSCHED)]
        self.assertEqual(len(nbevents.current_round(evs)), 2)


class DiagnoseTests(unittest.TestCase):
    def test_queued_notebook_gets_actionable_conclusion(self):
        d = nbevents.diagnose([_ev(REAL_UNSCHED)])
        self.assertIsNotNone(d)
        self.assertEqual(d["kind"], "affinity")
        self.assertIn("换一个组", d["advice"])
        self.assertTrue(d["breakdown"])

    def test_healthy_notebook_reports_no_problem(self):
        """跑得好好的机器不能被报成有问题 —— 实测踩过：一台 RUNNING 的机器
        因为翻到上一轮的旧事件，被报成「配额不够」。"""
        evs = [
            _ev("exceeded quota: from a previous round"),
            _ev("Successfully assigned foo to node1"),
            _ev("Pulling image \"x\""),
            _ev("Successfully pulled image \"x\""),
            _ev("Started container foo"),
        ]
        self.assertIsNone(nbevents.diagnose(evs))

    def test_empty_events(self):
        self.assertIsNone(nbevents.diagnose([]))


if __name__ == "__main__":
    unittest.main()
