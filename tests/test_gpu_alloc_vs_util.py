"""「GPU 分配率」和「GPU 利用率」是两件事，不许混。

## 为什么有这条

`qzcli avail` 那一列原来叫「GPU利用率」，但它算的是
``(总卡 − 空闲卡) / 总卡`` —— 那是**分配率**：有多少卡被分配出去了。
一个占着 8 张卡跑 0% 的任务，在这一列里显示 **100%**。

这不是措辞洁癖。平台的空闲回收判的是**真实利用率**（实测口径是
「GPU 低于 15% 持续 3 小时就回收」）。所以拿这一列去回答
**「我这台机器会不会被收走」，得到的是完全相反的答案** ——
你看到 100% 以为很忙，平台看到 3% 把它收了。

## 两个来源必须分清

- **分配率**：`_lcg_availability`-系列自己算的 ``(total - free) / total``。
  用户可见文案一律叫「GPU分配率」。
- **真实利用率**：`task_dimension_to_row()` 里的 ``gpu.usage_rate``，
  平台直接给的。用户可见文案叫「GPU利用率」。

这两个名字**只差一个字**，很容易被后来人"顺手统一"。所以钉在这里：
统一了就变红。
"""

import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import qzcli.cli as cli  # noqa: E402

_SRC = __import__("pathlib").Path(cli.__file__).read_text(encoding="utf-8")


class LabelSeparationTests(unittest.TestCase):
    def test_real_utilization_label_is_backed_by_platform_usage_rate(self):
        """叫「GPU利用率」的地方，数据必须来自平台的 usage_rate。"""
        offenders = []
        for i, line in enumerate(_SRC.splitlines(), 1):
            if "GPU利用率" in line and "usage_rate" not in line:
                offenders.append(f"cli.py:{i}  {line.strip()[:80]}")
        self.assertEqual(
            offenders,
            [],
            "这些地方标着「GPU利用率」，但数据不是平台的 usage_rate：\n  "
            + "\n  ".join(offenders)
            + "\n\n自己用 (总卡−空闲卡)/总卡 算出来的是**分配率**，"
            "请改叫「GPU分配率」。理由见本文件 docstring。",
        )

    def test_allocation_ratio_is_never_labelled_utilization(self):
        """反向：算分配率的那几行附近，**代码**里不许出现「利用率」字样。

        只看代码不看注释 —— 那一段的注释正是在解释「这是分配率不是利用率」，
        把它算成违规就等于不许写解释。
        """
        lines = _SRC.splitlines()
        offenders = []
        for i, line in enumerate(lines):
            if re.search(r"=\s*\(?\s*used_gpus?\s*/\s*total_gpus?", line):
                window = [
                    ln
                    for ln in lines[max(0, i - 2) : i + 3]
                    if not ln.lstrip().startswith("#")
                ]
                if "利用率" in "\n".join(window):
                    offenders.append(f"cli.py:{i + 1}")
        self.assertEqual(offenders, [], f"分配率算式旁边出现了「利用率」：{offenders}")

    def test_usage_rate_path_still_exists(self):
        """对照：真实利用率那条路不能被误删 —— 否则上面两条会**空转变绿**。

        没有这条，把 `usage_rate` 整个删掉、全仓库改叫「分配率」，
        前两条测试照样通过，而我们就永久失去了真实利用率这个来源。
        """
        self.assertIn("usage_rate", _SRC)
        self.assertIn("GPU利用率", _SRC)


class AllocationRatioMathTests(unittest.TestCase):
    """分配率的算式本身：占满但不干活 = 100%，这正是它不能叫利用率的原因。"""

    @staticmethod
    def _ratio(total, free):
        return (max(0, total - free) / total) if total > 0 else None

    def test_fully_allocated_idle_cluster_reads_100_percent(self):
        """8 卡全被占、但一张都没在算 —— 分配率 100%，利用率 0%。"""
        self.assertEqual(self._ratio(8, 0), 1.0)

    def test_empty_cluster(self):
        self.assertEqual(self._ratio(8, 8), 0.0)

    def test_zero_total_is_none_not_zero(self):
        """没有卡 ≠ 分配率 0% —— 后者会让「空闲」排序把它排到前面。"""
        self.assertIsNone(self._ratio(0, 0))


if __name__ == "__main__":
    unittest.main()
