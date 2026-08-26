"""机器自身的健康诊断：两条会把结论**搞反**的坑。

## 这个功能补的是什么

`qzcli events <job>` 回答「我这个任务为什么没起来」。它答不了
**「是不是这台机器本身有病」**。qzcli 早就有 `--exclude-node`，
但**没有任何东西告诉你该排除谁**。

## 坑一：健康事件是成对的，只 grep "Unhealthy" 会全员误报

平台把 condition 的两个方向都记成事件：``XIDIsUnhealthy`` 之后往往跟着
``XIDIsHealthy``。**实测那 6 台生产机每一台都出现过 XID / GPFS / NotReady，
但全部已恢复。** 只匹配"坏"的那一半，会把 6 台全报成有病 ——
照着去 `--exclude-node`，可用机器会被排到没有。

判据必须是「**每类问题最后一次状态翻转停在哪边**」。

## 坑二：平台按时间**升序**返回，只取第一页 = 拿最旧的数据下"现在"的结论

实测 ``total=222`` / ``page_size=200`` 时，第 1 页给的是**最旧的 200 条**
（第一条是 1 月的），**丢掉最新的 22 条**。而我们要判断的恰恰是"现在健不健康"。
一台昨天刚坏的机器会被判成健康 —— 这是这个功能最坏的失败方式。

所以必须取**最后一页**。这条钉在 ``ApiPaginationTests`` 里。
"""

import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qzcli import nodeevents as ne  # noqa: E402

# 真机抓下来的 reason（2026-08-27，分布式训练空间的生产 GPU 节点）
REAL_REASONS = [
    "GPFSMountUnhealthy",
    "XIDIsUnhealthy",
    "XIDIsHealthy",
    "NodeHasNoDiskPressure",
    "NodeNotReady",
    "NodeReady",
]


#: 固定的"现在"，避免测试跟真实时钟耦合。
NOW = 1_780_000_000  # 秒
DAY_MS = 86_400_000


def _ev(reason, ts, node="n1", msg=""):
    """``ts`` 是**天数偏移**（负数 = 多少天前），转成平台用的毫秒串。

    以前这里直接写 1000 / 2000 当时间戳 —— 那按毫秒算是 1970 年，
    加上时效判断后全部变成"陈旧记录"。**假数据不像真数据，测出来的就是假结论。**
    """
    ms = NOW * 1000 + int(ts) * DAY_MS
    return {
        "node_name": node,
        "reason": reason,
        "message": msg or f"Node condition is now: {reason}",
        "last_timestamp": str(ms),
    }


class PairedConditionTests(unittest.TestCase):
    def test_recovered_node_is_not_reported_as_broken(self):
        """**核心**：坏了又好了 = 现在正常。

        实测 6 台生产机全是这个形状；只 grep Unhealthy 会把 6 台全报成有病。
        """
        evs = [_ev("XIDIsUnhealthy", -3), _ev("XIDIsHealthy", -2)]
        d = ne.diagnose_node(evs)
        self.assertEqual(d["problems"], [])
        self.assertEqual([r["kind"] for r in d["recovered"]], ["gpu_xid"])
        self.assertFalse(ne.should_exclude(d, NOW))

    def test_still_broken_node_is_reported(self):
        """反过来：好了又坏了 = 现在有病。"""
        evs = [_ev("XIDIsHealthy", -3), _ev("XIDIsUnhealthy", -2)]
        d = ne.diagnose_node(evs)
        self.assertEqual([p["kind"] for p in d["problems"]], ["gpu_xid"])
        self.assertTrue(ne.should_exclude(d, NOW))

    def test_order_is_by_time_not_by_list_position(self):
        """乱序喂进来也要按时间判 —— 不能依赖调用方先排好序。"""
        evs = [_ev("XIDIsUnhealthy", -1), _ev("XIDIsHealthy", -5)]
        d = ne.diagnose_node(evs)
        self.assertTrue(d["problems"], "最新的是 Unhealthy，应该报问题")

    def test_healthy_is_matched_before_unhealthy(self):
        """``XIDIsHealthy`` 里也含 ``XID``：匹配顺序反了会把恢复读成故障。"""
        d = ne.diagnose_node([_ev("XIDIsHealthy", -1)])
        self.assertEqual(d["problems"], [])

    def test_each_condition_tracked_independently(self):
        """GPU 好了不代表盘也好了 —— 每类问题各判各的。"""
        evs = [
            _ev("XIDIsUnhealthy", -3),
            _ev("XIDIsHealthy", -2),
            _ev("GPFSMountUnhealthy", -1),
        ]
        d = ne.diagnose_node(evs)
        self.assertEqual([p["kind"] for p in d["problems"]], ["gpfs"])
        self.assertEqual([r["kind"] for r in d["recovered"]], ["gpu_xid"])


class ExclusionAdviceTests(unittest.TestCase):
    def test_only_hardware_and_storage_trigger_exclusion(self):
        """磁盘/内存压力是暂时的，为它排除节点会把可用机器越排越少。"""
        disk = ne.diagnose_node([_ev("NodeHasDiskPressure", -1)])
        self.assertTrue(disk["problems"])
        self.assertFalse(ne.should_exclude(disk, NOW), "磁盘压力不该建议排除")

        xid = ne.diagnose_node([_ev("XIDIsUnhealthy", -1)])
        self.assertTrue(ne.should_exclude(xid, NOW))

    def test_hint_is_pasteable(self):
        """建议必须能直接粘到 create 后面，不是让人自己拼。"""
        self.assertEqual(
            ne.exclude_hint(["b", "a", "a"]), "--exclude-node a --exclude-node b"
        )
        self.assertIsNone(ne.exclude_hint([]))

    def test_verdict_distinguishes_never_broken_from_recovered(self):
        self.assertIn("没发现异常", ne.verdict(ne.diagnose_node([])))
        self.assertIn(
            "曾经",
            ne.verdict(
                ne.diagnose_node([_ev("NodeNotReady", -2), _ev("NodeReady", -1)])
            ),
        )


class ApiPaginationTests(unittest.TestCase):
    """必须取**最后一页** —— 平台按时间升序返回。"""

    def _api(self, total, page_size=200):
        from qzcli.api import QzAPI

        api = QzAPI.__new__(QzAPI)
        calls = []

        def fake(service, action, body, cookie=""):
            calls.append(body["PageNumber"])
            return {"total": total, "events": [_ev("NodeReady", -1)]}

        api._request_v2 = fake
        return api, calls

    def test_fetches_last_page_when_truncated(self):
        """total=222 / page_size=200 → 必须去要第 2 页。

        只要第 1 页 = 拿最旧的 200 条去判断"现在"，一台昨天刚坏的机器会被
        判成健康。
        """
        api, calls = self._api(222)
        api.get_node_events(["n1"], page_size=200)
        self.assertIn(2, calls, f"没有请求最后一页，实际请求了 {calls}")

    def test_single_page_when_not_truncated(self):
        """没超过一页就别多打一次请求。"""
        api, calls = self._api(50)
        api.get_node_events(["n1"], page_size=200)
        self.assertEqual(calls, [1])

    def test_queries_each_node_separately(self):
        """一次问多台会被总条数分页挤掉后面的机器 —— 实测问 5 台只回了前 2 台，
        后 3 台被诊断成「没发现异常记录」，是静默假阴性。"""
        from qzcli.api import QzAPI

        api = QzAPI.__new__(QzAPI)
        asked = []

        def fake(service, action, body, cookie=""):
            asked.append(list(body["filter"]["node_names"]))
            return {"total": 1, "events": []}

        api._request_v2 = fake
        api.get_node_events(["a", "b", "c"])
        self.assertEqual(asked, [["a"], ["b"], ["c"]])

    def test_one_node_failing_does_not_kill_the_batch(self):
        from qzcli.api import QzAPI, QzAPIError

        api = QzAPI.__new__(QzAPI)

        def fake(service, action, body, cookie=""):
            if body["filter"]["node_names"] == ["b"]:
                raise QzAPIError("boom")
            return {"total": 1, "events": [_ev("NodeReady", -1, node=body["filter"]["node_names"][0])]}

        api._request_v2 = fake
        out = api.get_node_events(["a", "b", "c"])
        self.assertEqual(sorted(e["node_name"] for e in out), ["a", "c"])

    def test_empty_input_makes_no_request(self):
        from qzcli.api import QzAPI

        api = QzAPI.__new__(QzAPI)
        api._request_v2 = mock.Mock(side_effect=AssertionError("不该发请求"))
        self.assertEqual(api.get_node_events([]), [])


class RealReasonsTests(unittest.TestCase):
    def test_every_real_reason_is_recognised(self):
        """喂**真机抓到的 reason**，不是我想象中的。认不出来说明规则表漏了。"""
        unknown = []
        for r in REAL_REASONS:
            d = ne.diagnose_node([_ev(r, -1)])
            if not d["problems"] and not d["recovered"]:
                unknown.append(r)
        self.assertEqual(unknown, [], f"这些真实 reason 没被任何规则认出：{unknown}")


if __name__ == "__main__":
    unittest.main()


class StalenessTests(unittest.TestCase):
    """陈旧的"未恢复"不算现存故障。

    ## 为什么加这条（真机踩到）

    平台**只在状态翻转时记事件**，不保证一定会记恢复。实测一台正常在跑任务的
    生产机（qb-prod-gpu005），最后一条 GPFS 事件是 **3.5 个月前**的
    ``GPFSMountUnhealthy``，之后再没有 Healthy。按"最后一次翻转"判就成了
    「现在有问题」，还建议把它排除掉。

    **照这种报警去 `--exclude-node`，半个机房都得排掉。**

    所以陈旧记录降级：如实说「最后一次是 N 天前，之后没有恢复记录」，
    让人自己判断，而不是替他下结论。
    """

    def test_recent_hardware_problem_still_triggers_exclusion(self):
        d = ne.diagnose_node([_ev("XIDIsUnhealthy", -1)])
        self.assertTrue(ne.should_exclude(d, NOW))

    def test_months_old_unrecovered_does_not_trigger_exclusion(self):
        """真机形状：3.5 个月前报过、之后没有恢复记录。"""
        d = ne.diagnose_node([_ev("GPFSMountUnhealthy", -105)])
        self.assertTrue(d["problems"], "仍然要列出来，只是不建议排除")
        self.assertFalse(
            ne.should_exclude(d, NOW),
            "3.5 个月前的未恢复记录不该建议排除节点",
        )

    def test_boundary_is_the_documented_threshold(self):
        """阈值就是 STALE_AFTER_DAYS，别写死一个和常量对不上的数。"""
        fresh = ne.diagnose_node([_ev("XIDIsUnhealthy", -(ne.STALE_AFTER_DAYS - 1))])
        stale = ne.diagnose_node([_ev("XIDIsUnhealthy", -(ne.STALE_AFTER_DAYS + 1))])
        self.assertTrue(ne.should_exclude(fresh, NOW))
        self.assertFalse(ne.should_exclude(stale, NOW))

    def test_age_days_reads_milliseconds(self):
        """时间戳是**毫秒**。当成秒会算出 1970 年，显示和判断一起错
        —— 第一版渲染时多乘了一次 1000，日期显示成了 2 月。"""
        item = {"at": (NOW - 3 * 86400) * 1000}
        self.assertAlmostEqual(ne.age_days(item, NOW), 3.0, places=1)
