"""规格归属：用平台给的字段，别自己假设。

## 病理

``_specs_from_schedule_config`` 读平台的 ``predef_train_spec``（工作空间级的一整张
规格表），然后给**每一条**都盖上「属于目标计算组」的戳：

    "logic_compute_group_ids": [compute_group_id] if compute_group_id else []

配套注释写着「规格是工作空间级的，对该空间任一计算组都可用」。**平台不认这个假设** ——
拿别的分区的规格去提交会被直接拒：

    InvalidParameter: framework_config[0]: quota_id "..." does not belong to
    logic_compute_group "..."

而 ``predef_train_spec`` 每条记录**本来就带 ``logic_compute_group_ids``**，明确说了
它属于哪些计算组。实测 5/5 与平台的接受/拒绝完全吻合。

## 后果不只是"列表不准"

自动选规格挑的是 GPU 数最小的那个（v0.4.3 加的，避免默认占大机器）。而小规格恰恰
常常属于开发分区 —— 于是在训练分区上 ``qzcli create`` **不带 ``--spec`` 会选中一个
必被拒的规格**。实测「训练区-H200-1号机房」自动选中 ``1卡10核``，而那条属于
开发区，提交必失败。

也就是说 v0.4.3 修好的「不带 --spec 能提交」，在训练分区上仍然是坏的，只是报错文案
换了一个。

## 领域规则

开发分区支持散卡；训练分区上的散卡规格 ``allowed_priority_levels`` 是 ``['low']``
（只能跑低优）。这条信息平台也在同一份数据里给了，一并带出来。
"""

import unittest
from unittest.mock import patch

from qzcli.api import QzAPI

_TRAIN = "lcg-train"
_DEV = "lcg-dev"

_PREDEF = [
    {
        "id": "spec-dev-1card",
        "name": "1卡10核",
        "cpu_count": 10,
        "memory_size": 200,
        "gpu_count": 1,
        "gpu_type": "",
        "logic_compute_group_ids": [_DEV],
        "allowed_priority_levels": [],
    },
    {
        "id": "spec-train-8card",
        "name": "8卡160核",
        "cpu_count": 160,
        "memory_size": 1800,
        "gpu_count": 8,
        "gpu_type": "",
        "logic_compute_group_ids": [_TRAIN],
        "allowed_priority_levels": [],
    },
    {
        "id": "spec-train-1card-lowonly",
        "name": "1卡20核",
        "cpu_count": 20,
        "memory_size": 400,
        "gpu_count": 1,
        "gpu_type": "",
        "logic_compute_group_ids": [_TRAIN],
        "allowed_priority_levels": ["low"],
    },
    {
        "id": "spec-legacy-no-owner",
        "name": "老数据无归属",
        "cpu_count": 8,
        "memory_size": 100,
        "gpu_count": 1,
        "gpu_type": "",
        "allowed_priority_levels": [],
    },
]


def _client(predef=_PREDEF):
    import json

    api = QzAPI(username="u", password="p", auth_mode="legacy")
    api._request_v2 = lambda service, action, body, **kw: (
        {"schedule_config": {"predef_train_spec": json.dumps(predef)}}
        if action == "GetScheduleConfig"
        else {}
    )
    api.list_jobs_with_cookie = lambda *a, **k: {"jobs": []}
    api.list_node_dimension = lambda *a, **k: {"node_dimensions": []}
    return api


class SpecOwnershipTests(unittest.TestCase):
    def test_other_partitions_specs_are_filtered_out(self):
        """别的分区的规格不能出现在列表里 —— 选了必被平台拒。

        修复前这里会返回全部 4 条。
        """
        ids = {s["id"] for s in _client()._specs_from_schedule_config("ws-1", _TRAIN)}
        self.assertNotIn("spec-dev-1card", ids, "开发分区的规格混进了训练分区列表")
        self.assertIn("spec-train-8card", ids)

    def test_platform_ownership_is_preserved_not_overwritten(self):
        """归属字段要原样保留，不能被改写成"目标计算组"。

        改写掉之后，下游任何按归属做的校验都会失效 —— 它们看到的永远是"属于"。
        """
        specs = _client()._specs_from_schedule_config("ws-1", _TRAIN)
        s = next(x for x in specs if x["id"] == "spec-train-8card")
        self.assertEqual(s["logic_compute_group_ids"], [_TRAIN])

    def test_spec_without_ownership_falls_back(self):
        """平台没给归属的（老数据 / 新分区）仍然可用，向后兼容。"""
        ids = {s["id"] for s in _client()._specs_from_schedule_config("ws-1", _TRAIN)}
        self.assertIn("spec-legacy-no-owner", ids)

    def test_no_compute_group_returns_everything(self):
        """不指定计算组时不过滤 —— 否则"列出这个空间所有规格"就没法用了。"""
        ids = {s["id"] for s in _client()._specs_from_schedule_config("ws-1", "")}
        self.assertEqual(len(ids), 4)

    def test_allowed_priority_levels_is_carried_through(self):
        """训练分区上的散卡只能跑低优 —— 这个限制平台给了，别丢。"""
        specs = _client()._specs_from_schedule_config("ws-1", _TRAIN)
        low = next(x for x in specs if x["id"] == "spec-train-1card-lowonly")
        self.assertEqual(low["allowed_priority_levels"], ["low"])
        whole = next(x for x in specs if x["id"] == "spec-train-8card")
        self.assertEqual(whole["allowed_priority_levels"], [])


class AutoSelectPicksValidSpecTests(unittest.TestCase):
    """自动选规格必须选一个**该计算组真能用**的。"""

    def test_auto_select_does_not_pick_another_partitions_spec(self):
        """训练分区上不能选中开发分区那条 1 卡规格。

        修复前：自动选"GPU 数最小"→ 选中开发分区的 1卡10核 → 提交必被拒。
        """
        from qzcli import cli

        api = _client()
        with patch.object(cli, "get_workspace_resources", return_value={"specs": {}}):
            spec_id, _ = cli._auto_select_spec_for_compute_group(
                "ws-1", _TRAIN, api=api
            )
        self.assertNotEqual(
            spec_id, "spec-dev-1card", "自动选中了别的分区的规格，提交会被拒"
        )
        self.assertIn(
            spec_id,
            {"spec-train-8card", "spec-train-1card-lowonly", "spec-legacy-no-owner"},
        )


if __name__ == "__main__":
    unittest.main()
