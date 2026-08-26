"""构造 payload 时，缓存里的 spec 必须属于**目标计算组**。

## 病理

``_lookup_spec_for_payload`` 只按 spec id 读缓存，不看这条记录是给哪个计算组缓存的。
只要 cpu/gpu/mem 齐全就直接采用 —— 而规格是**工作空间级**的，同一个 id 在别的
计算组缓存过，就会把那边的 ``gpu_type`` 一起带进 payload。

实测：向「训练区-H200-1号机房」提交，缓存里那条 8卡160核 记录的
``logic_compute_group_ids`` 是 ``['lcg-22222222-…']``（某 H100 计算组），
于是 payload 里的 ``gpu_type`` 成了 ``NVIDIA_H100_SXM_80G``，
而该组 180 个节点全是 ``NVIDIA_H200_SXM_141G``。

**这比报错更糟**：任务会一直排队等一种该组里根本不存在的卡，看起来"成功进入
排队"，实际永远起不来 —— 正好骗过「能排队就算通过」的验收。

修法是复用已有的 ``_scope_specs_to_compute_group``：缓存记录不属于目标组时
当作"缓存不可用"，走那条已经存在的实时刷新分支（刷新会按目标组解析出正确卡型）。
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from support.sandbox import sandbox_home  # noqa: E402

from qzcli import cli  # noqa: E402

_WS = "ws-1"
_TARGET = "lcg-h200-target"
_OTHER = "lcg-h100-other"
_SPEC = "spec-8card"


def _cache(lcg_ids, gpu_type):
    return {
        _WS: {
            "id": _WS,
            "name": "测试空间",
            "projects": {},
            "compute_groups": {_TARGET: {"id": _TARGET}, _OTHER: {"id": _OTHER}},
            "specs": {
                _SPEC: {
                    "id": _SPEC,
                    "gpu_count": 8,
                    "cpu_count": 160,
                    "memory_gb": 1800,
                    "gpu_type": gpu_type,
                    "logic_compute_group_ids": lcg_ids,
                }
            },
            "updated_at": "2026-08-05T00:00:00",
        }
    }


class SpecCacheScopeTests(unittest.TestCase):
    def _lookup(self, cache, refreshed_gpu_type=None):
        api = MagicMock()
        refreshed = {"called": False}

        def _fake_refresh(*a, **kw):
            refreshed["called"] = True
            if refreshed_gpu_type is not None:
                # 模拟实时刷新把按目标组解析出的正确记录写回缓存
                from qzcli import config

                ws = dict(config.get_workspace_resources(_WS) or {})
                # ⚠️ save_resources 收的 specs/projects/compute_groups 是 **list**
                # （内部 {s["id"]: s for s in ...}），而 get_workspace_resources
                # 返回的是 dict —— 读写不对称，喂 dict 会 TypeError，而且会被
                # 调用处那层 except Exception: pass 吞掉，表现为"刷新了但没生效"。
                specs = dict(ws.get("specs") or {})
                specs[_SPEC] = dict(
                    specs.get(_SPEC, {}),
                    gpu_type=refreshed_gpu_type,
                    logic_compute_group_ids=[_TARGET],
                )
                config.save_resources(
                    _WS,
                    {
                        "projects": list((ws.get("projects") or {}).values()),
                        "compute_groups": list(
                            (ws.get("compute_groups") or {}).values()
                        ),
                        "specs": list(specs.values()),
                    },
                    ws.get("name", ""),
                )
            return {"items": []}

        with sandbox_home(resources=cache):
            with patch.object(cli, "_load_specs_for_create_result", _fake_refresh):
                spec = cli._lookup_spec_for_payload(
                    api, _WS, "测试空间", _TARGET, _SPEC
                )
        return spec, refreshed["called"]

    def test_cached_spec_from_another_group_triggers_refresh(self):
        """缓存记录属于别的计算组 —— 必须重新解析，不能直接用。

        修复前：直接返回那条 H100 记录，refreshed 为 False。
        """
        cache = _cache([_OTHER], "NVIDIA_H100_SXM_80G")
        spec, refreshed = self._lookup(cache, refreshed_gpu_type="NVIDIA_H200_SXM_141G")
        self.assertTrue(refreshed, "缓存记录不属于目标组，却没有重新解析")
        self.assertEqual(
            spec.get("gpu_type"),
            "NVIDIA_H200_SXM_141G",
            "payload 里带上了别的计算组的卡型",
        )

    def test_cached_spec_of_target_group_is_used_directly(self):
        """本组的缓存记录直接用，不该多打一次网络。对照组。"""
        cache = _cache([_TARGET], "NVIDIA_H200_SXM_141G")
        spec, refreshed = self._lookup(cache)
        self.assertFalse(refreshed, "本组缓存可用却仍去刷新，白打一次请求")
        self.assertEqual(spec.get("gpu_type"), "NVIDIA_H200_SXM_141G")

    def test_spec_without_group_scope_is_still_usable(self):
        """缓存记录没有归属字段时维持原行为（可用即用），向后兼容。"""
        cache = _cache([], "NVIDIA_H200_SXM_141G")
        spec, _ = self._lookup(cache)
        self.assertEqual(spec.get("gpu_count"), 8)


if __name__ == "__main__":
    unittest.main()
