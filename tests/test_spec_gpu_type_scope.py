"""补 ``gpu_type`` 时必须按计算组划定范围。

## 病理

``predef_train_spec`` 里的 ``gpu_type`` 常为空，``_fill_missing_gpu_type``
负责从历史任务里补。但它**只按 quota_id 匹配，不看任务跑在哪个计算组**。

而规格是**工作空间级**的（同一个 quota_id 对该空间任一计算组都可用），所以
同一个 spec 在 H100 组跑过、也能在 H200 组用。于是给 H200 组解析规格时，
会把历史上 H100 组的型号填进去。

真实后果：提交到「训练区-H200-1号机房」（实测 180 个节点全是
``NVIDIA_H200_SXM_141G``）的 payload 里，``gpu_type`` 写成
``NVIDIA_H100_SXM_80G``。这比直接报错更糟 —— 任务可能一直排队等一种该组里
根本不存在的卡，**看起来"成功进入排队"，实际永远起不来**。

紧挨着的 ``_specs_from_job_history`` 是有 ``lcg_id != compute_group_id`` 过滤的，
同一个纪律隔二十行没被执行。

原函数的 docstring 自己写着「补不到就留空 —— 让平台去报错，好过我们瞎猜一个
型号」。跨计算组去猜，正是它说的那种瞎猜。
"""

import unittest
from unittest.mock import patch

from qzcli.api import QzAPI

_SPEC_ID = "spec-8card"
_TARGET_LCG = "lcg-h200-target"
_OTHER_LCG = "lcg-h100-other"


def _job(lcg_id, quota_id, gpu_type):
    return {
        "logic_compute_group_id": lcg_id,
        "framework_config": [
            {
                "instance_spec_price_info": {
                    "quota_id": quota_id,
                    "gpu_info": {"gpu_type": gpu_type},
                }
            }
        ],
    }


class GpuTypeScopeTests(unittest.TestCase):
    def _fill(self, jobs, compute_group_id):
        api = QzAPI(username="u", password="p", auth_mode="legacy")
        specs = [{"id": _SPEC_ID, "gpu_type": ""}]
        with patch.object(api, "list_jobs_with_cookie", return_value={"jobs": jobs}):
            api._fill_missing_gpu_type(specs, "ws-1", compute_group_id)
        return specs[0]["gpu_type"]

    def test_does_not_borrow_gpu_type_from_another_compute_group(self):
        """别的计算组用过同一个 spec —— 不能把那边的卡型抄过来。

        这条在修复前必红：会填成 NVIDIA_H100_SXM_80G。
        """
        jobs = [_job(_OTHER_LCG, _SPEC_ID, "NVIDIA_H100_SXM_80G")]
        self.assertEqual(
            self._fill(jobs, _TARGET_LCG),
            "",
            "把别的计算组的卡型抄过来了 —— 宁可留空让平台报错",
        )

    def test_uses_gpu_type_from_the_same_compute_group(self):
        """本组历史里有，就该用它。对照组，否则「永远留空」也能让上面那条变绿。"""
        jobs = [_job(_TARGET_LCG, _SPEC_ID, "NVIDIA_H200_SXM_141G")]
        self.assertEqual(self._fill(jobs, _TARGET_LCG), "NVIDIA_H200_SXM_141G")

    def test_prefers_same_group_when_both_exist(self):
        """两组都跑过同一个 spec —— 必须挑目标组那个，且不受先后顺序影响。"""
        jobs = [
            _job(_OTHER_LCG, _SPEC_ID, "NVIDIA_H100_SXM_80G"),
            _job(_TARGET_LCG, _SPEC_ID, "NVIDIA_H200_SXM_141G"),
        ]
        self.assertEqual(self._fill(jobs, _TARGET_LCG), "NVIDIA_H200_SXM_141G")

    def test_no_compute_group_keeps_old_behavior(self):
        """不指定计算组时维持原行为（任何历史都可用），向后兼容。"""
        jobs = [_job(_OTHER_LCG, _SPEC_ID, "NVIDIA_H100_SXM_80G")]
        self.assertEqual(self._fill(jobs, ""), "NVIDIA_H100_SXM_80G")


if __name__ == "__main__":
    unittest.main()


class GpuTypeFromComputeGroupNodesTests(unittest.TestCase):
    """本组历史里查不到时，用**该计算组节点上的真实卡型**兜底。

    只按计算组过滤历史是不够的：新组、或者该 spec 在本组还没跑过时，历史里
    根本没有对应记录，于是 gpu_type 留空。而平台校验 payload 时要完整型号串 ——
    留空可能被直接拒。

    实测这就是「训练区-H200-1号机房」+「8卡160核」的处境：本组历史里没有这个
    quota_id，但**该组 180 个节点全是 NVIDIA_H200_SXM_141G**。节点信息是权威
    来源，比历史任务可靠得多 —— 卡型是机器的属性，不是任务的属性。
    """

    def _fill(self, jobs, nodes, compute_group_id=_TARGET_LCG):
        api = QzAPI(username="u", password="p", auth_mode="legacy")
        specs = [{"id": _SPEC_ID, "gpu_type": ""}]
        with patch.object(
            api, "list_jobs_with_cookie", return_value={"jobs": jobs}
        ), patch.object(
            api, "list_node_dimension", return_value={"node_dimensions": nodes}
        ):
            api._fill_missing_gpu_type(specs, "ws-1", compute_group_id)
        return specs[0]["gpu_type"]

    def _node(self, gpu_type):
        return {"gpu_info": {"gpu_type": gpu_type}}

    def test_falls_back_to_node_gpu_type(self):
        """历史查不到 → 用本组节点的卡型。修复前这里会留空。"""
        self.assertEqual(
            self._fill([], [self._node("NVIDIA_H200_SXM_141G")] * 3),
            "NVIDIA_H200_SXM_141G",
        )

    def test_history_wins_over_nodes(self):
        """本组历史里有就用历史 —— 那是这个 spec 实际用过的型号，更精确。"""
        jobs = [_job(_TARGET_LCG, _SPEC_ID, "NVIDIA_H100_SXM_80G")]
        self.assertEqual(
            self._fill(jobs, [self._node("NVIDIA_H200_SXM_141G")]),
            "NVIDIA_H100_SXM_80G",
        )

    def test_picks_the_dominant_type_in_a_mixed_group(self):
        """混合卡型的组取多数派，不被个别异类带偏。"""
        nodes = [self._node("NVIDIA_H200_SXM_141G")] * 5 + [
            self._node("NVIDIA_H100_SXM_80G")
        ]
        self.assertEqual(self._fill([], nodes), "NVIDIA_H200_SXM_141G")

    def test_leaves_empty_when_nodes_give_nothing(self):
        """节点也问不出来就留空 —— 让平台报错，好过瞎猜。"""
        self.assertEqual(self._fill([], []), "")

    def test_node_query_failure_is_not_fatal(self):
        """查节点失败不能让整个规格解析崩掉。"""
        api = QzAPI(username="u", password="p", auth_mode="legacy")
        specs = [{"id": _SPEC_ID, "gpu_type": ""}]
        from qzcli.api import QzAPIError

        with patch.object(
            api, "list_jobs_with_cookie", return_value={"jobs": []}
        ), patch.object(api, "list_node_dimension", side_effect=QzAPIError("boom")):
            api._fill_missing_gpu_type(specs, "ws-1", _TARGET_LCG)
        self.assertEqual(specs[0]["gpu_type"], "")
