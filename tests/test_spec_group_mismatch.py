"""规格和计算组对不上时，报错必须说清楚该改成哪个 spec。

现场（2026-09-21）：提交脚本里 ``SPEC`` 默认值是 infra-debug 分区的规格，
用户换到新建的 MOVA-2.0 计算组提交，qzcli 报
「无法解析规格 '...' 的 cpu/gpu/memory 信息，请运行 qzcli res -w <ws> -u 刷新缓存」。
照做刷了三次缓存，一样报错 —— 因为这条规格根本不属于目标计算组，
缓存里永远不会出现它。用户要的答案是「这个组该用哪条 spec」。
"""

import unittest
from unittest import mock

from qzcli import cli
from qzcli.api import QzAPIError

WS = "ws-1"
INFRA = "lcg-infra"
MOVA = "lcg-mova"
INFRA_SPEC = "spec-infra"
MOVA_SPEC = "spec-mova"


def _cache(with_infra_spec: bool):
    specs = {}
    if with_infra_spec:
        specs[INFRA_SPEC] = {
            "id": INFRA_SPEC,
            "logic_compute_group_ids": [INFRA],
            "gpu_count": 8,
            "cpu_count": 64,
            "memory_gb": 960,
            "gpu_type": "NVIDIA_H200_SXM_141G",
        }
    return {
        "id": WS,
        "name": "CI",
        "projects": {},
        "compute_groups": {
            INFRA: {"id": INFRA, "name": "infra-debug"},
            MOVA: {"id": MOVA, "name": "MOVA-2.0"},
        },
        "specs": specs,
    }


def _api_listing_mova_spec():
    api = mock.MagicMock()
    api.list_specs.return_value = [
        {
            "id": MOVA_SPEC,
            "quota_id": MOVA_SPEC,
            "gpu_count": 8,
            "cpu_count": 150,
            "memory_size_gib": 1500,
            "gpu_type": "NVIDIA_H200_SXM_141G",
            "logic_compute_group_ids": [MOVA],
        }
    ]
    return api


class ExplainSpecGroupMismatchTest(unittest.TestCase):
    def test_spec_owned_by_other_group_names_owner_and_alternative(self):
        cache = _cache(with_infra_spec=True)
        with mock.patch.object(cli, "get_workspace_resources", return_value=cache), mock.patch.object(
            cli, "save_resources"
        ):
            msg = cli._explain_spec_group_mismatch(
                _api_listing_mova_spec(), WS, "CI", MOVA, INFRA_SPEC
            )
        self.assertIn("不属于计算组 MOVA-2.0 (lcg-mova)", msg)
        self.assertIn("归属: infra-debug (lcg-infra)", msg)
        self.assertIn(f"--spec {MOVA_SPEC}", msg)
        self.assertIn("150 CPU", msg)
        # 明确告诉用户刷缓存没用，别再绕
        self.assertIn("res -u", msg)
        self.assertIn("解决不了", msg)

    def test_spec_unknown_to_workspace_still_lists_group_specs(self):
        cache = _cache(with_infra_spec=False)
        with mock.patch.object(cli, "get_workspace_resources", return_value=cache), mock.patch.object(
            cli, "save_resources"
        ):
            msg = cli._explain_spec_group_mismatch(
                _api_listing_mova_spec(), WS, "CI", MOVA, INFRA_SPEC
            )
        self.assertIn("规格表里没有规格 'spec-infra'", msg)
        self.assertIn(f"--spec {MOVA_SPEC}", msg)

    def test_platform_returns_nothing_points_to_interactive(self):
        api = mock.MagicMock()
        api.list_specs.return_value = []
        with mock.patch.object(
            cli, "get_workspace_resources", return_value=_cache(False)
        ), mock.patch.object(cli, "save_resources"):
            msg = cli._explain_spec_group_mismatch(api, WS, "CI", MOVA, INFRA_SPEC)
        self.assertIn("create -i", msg)
        self.assertNotIn("--spec ", msg)


class LookupSpecForPayloadTest(unittest.TestCase):
    def test_cached_spec_of_other_group_is_rejected_not_passed_through(self):
        """以前：缓存里有字段但归属别组 → 刷新后直接放行，把别组 quota_id 塞进 payload。"""
        cache = _cache(with_infra_spec=True)
        api = _api_listing_mova_spec()
        with mock.patch.object(cli, "get_workspace_resources", return_value=cache), mock.patch.object(
            cli, "save_resources"
        ):
            with self.assertRaises(QzAPIError) as ctx:
                cli._lookup_spec_for_payload(api, WS, "CI", MOVA, INFRA_SPEC)
        self.assertIn(f"--spec {MOVA_SPEC}", str(ctx.exception))
        self.assertNotIn("刷新缓存后再试", str(ctx.exception))

    def test_matching_spec_still_resolves(self):
        cache = _cache(with_infra_spec=True)
        with mock.patch.object(cli, "get_workspace_resources", return_value=cache):
            spec = cli._lookup_spec_for_payload(mock.MagicMock(), WS, "CI", INFRA, INFRA_SPEC)
        self.assertEqual(spec["cpu_count"], 64)


if __name__ == "__main__":
    unittest.main()
