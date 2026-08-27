"""`avail` 那列显示的是**分配率**，别把它当利用率去做判断。

## 结论先行

- **列名保持「GPU利用率」** —— 它是这个命令最常被看的一列，改名的收益不抵改动成本。
- **但它算的是 `(总卡 − 空闲卡) / 总卡`，是分配率**：有多少卡被**分配出去**了。
  一个占着 8 张卡跑 0% 的任务，在这一列里是 100%。

## 那这条测试守什么

不守列名（那是产品决定，会变），守**两件不能糊的事**：

1. **算式本身不能被人"顺手改成"别的口径。** 它就是分配率，改了下游读数全变。
2. **真实利用率另有来源，不能被误删。** `task_dimension_to_row()` 用的
   `gpu.usage_rate` 是平台直接给的真实利用率（`dashboard` 走这条）。
   哪天要回答「这台机器会不会被空闲回收」（平台判据是 GPU 利用率低于阈值持续数小时），
   **只能用它，不能用 avail 那一列** —— 两者在这个场景下结论相反。
3. **代码里必须留着解释这个区别的说明**，否则下一个人看到 `(total-free)/total`
   配「利用率」的标签，只会以为是笔误然后"修"成别的东西。
"""

import os
import pathlib
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import qzcli.cli as cli  # noqa: E402

_SRC = pathlib.Path(cli.__file__).read_text(encoding="utf-8")


class AllocationRatioMathTests(unittest.TestCase):
    """算式就是分配率：占满但不干活 = 100%。"""

    @staticmethod
    def _ratio(total, free):
        return (max(0, total - free) / total) if total > 0 else None

    def test_fully_allocated_idle_cluster_reads_100_percent(self):
        """8 卡全被占、一张都没在算 —— 这一列是 100%，真实利用率是 0%。"""
        self.assertEqual(self._ratio(8, 0), 1.0)

    def test_empty_cluster(self):
        self.assertEqual(self._ratio(8, 8), 0.0)

    def test_zero_total_is_none_not_zero(self):
        """没有卡 ≠ 0% —— 后者会让"最空闲"排序把它排到最前面。"""
        self.assertIsNone(self._ratio(0, 0))

    def test_cli_still_computes_it_from_free_and_total(self):
        """口径不能被换掉：仍然是 (总卡 − 空闲卡) / 总卡。"""
        self.assertRegex(
            _SRC, r"gpu_alloc_ratio\s*=\s*\(?\s*used_gpus?\s*/\s*total_gpus?"
        )


class RealUtilizationSourceTests(unittest.TestCase):
    def test_platform_usage_rate_path_is_not_deleted(self):
        """真实利用率的来源不能被误删。

        没有这条，把 `usage_rate` 整个删掉、全仓库只剩 avail 那个分配率口径，
        其它测试照样全绿，而我们就**永久失去了唯一能回答"这台机器实际在不在干活"
        的数据**。
        """
        self.assertIn("usage_rate", _SRC)

    def test_dashboard_row_uses_usage_rate_not_allocation(self):
        """dashboard 那一行必须走平台的 usage_rate，不能改成自己算。"""
        block = _SRC[_SRC.index("def task_dimension_to_row") :][:2000]
        self.assertIn("usage_rate", block)
        self.assertNotIn("total_free_gpu", block)


class DocumentedDistinctionTests(unittest.TestCase):
    def test_the_difference_is_explained_next_to_the_formula(self):
        """算式旁边必须留着"这是分配率不是利用率"的说明。

        否则下一个人看到 `(total-free)/total` 配「利用率」的标签，会当成笔误去"修"。
        """
        # 锚在**算式那一处**，不是第一个同名变量（那是渲染时的局部变量）
        m = re.search(r"gpu_alloc_ratio\s*=\s*\(used_gpus\s*/\s*total_gpus\)", _SRC)
        self.assertIsNotNone(m, "找不到分配率的计算点")
        window = _SRC[max(0, m.start() - 1200) : m.start()]
        self.assertIn("分配率", window)
        self.assertIn("利用率", window)
        self.assertTrue(
            re.search(r"#.*不是.*利用率|#.*分配率.*不是", window),
            "算式上方没有解释两者区别的注释",
        )


if __name__ == "__main__":
    unittest.main()
