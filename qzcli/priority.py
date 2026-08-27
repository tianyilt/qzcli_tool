"""优先级被规格拒绝时，给出能照着做的下一步，并记住这次拒绝。

## 平台的真实行为（2026-08-27 A/B 实测）

优先级不只有「空间级两套刻度」这一层。**平台还会逐个资源规格限制能用的优先级**，
在提交阶段直接拒绝：

    InvalidParameter: 任务优先级不在该资源规格允许的范围内

实测（分布式训练空间 · 训练区-H200-1号机房 · 同一个 1 卡规格，只改优先级）：

===========  ========================
`--priority` 结果
===========  ========================
4（高优）    ❌ 上面那句拒绝
1（低优）    ✅ 建出来了（job_queuing）
===========  ========================

## 为什么这里做的是「解释 + 记忆」而不是「预检」

InspireSkill 的同类功能是**提交前预检**：它的 quota 列表有一列 Priority，
直接显示这一行允许哪些优先级。

**我们拿不到那一列。** 翻遍了能翻的：

- ``task_priority`` 只出现在 CreateJob 的**请求体**里，是入参不是读出
- ``GetWorkspaceTaskQuota`` / ``GetUserTaskQuota`` / ``GetAllQuota`` /
  ``GetWorkspaceNodeSpecs`` 的返回里没有任何 priority 字段
- ``GetScheduleConfig`` 的完整返回（42 个字段）里也没有，而且它**要 workspace
  admin 权限**，16 个空间里只有 1 个读得到
- 实际拿到的规格字段就这些：``cpu_count / gpu_count / gpu_info / gpu_type /
  id / logic_compute_group_ids / memory_size_gib / quota_id``

**连平台的拒绝消息本身都不说允许范围是什么。**

所以这里不假装能预检。做两件**只基于已观测事实**的事：

1. **翻译**：把那句话变成「换 `--priority 1` 重试」这样的具体动作
2. **记住**：把 (空间, 规格, 优先级) → 被拒 记下来，下次同样组合在**发请求前**
   就拦下并说明理由。这条预检的依据是「上次真的被拒过」，不是猜的规则

第 2 条是有意做成**只拦已知被拒的组合**：没见过的组合一律放行去问平台。
反过来做（白名单）会把平台后来放开的组合永久挡在门外。
"""

import json
import os
import time
from typing import Dict, List, Optional

from .config import CONFIG_DIR

#: 平台拒绝这类请求时的原话。匹配「优先级 + 规格 + 范围」三个要素，
#: 不整句硬匹配 —— 平台文案改个标点就失配的判据不如不做。
_REJECT_MARKERS = ("优先级", "规格", "范围")

#: 学到的拒绝记录。跟随 ``QZCLI_HOME``，与其余状态一致。
STORE = CONFIG_DIR / "priority_rejects.json"

#: 记录上限。超了从最旧的开始丢 —— 这是加速层不是账本，丢了最多回到"问平台"。
MAX_ENTRIES = 500


def is_priority_rejection(message: str) -> bool:
    """这条报错是不是「优先级不在该规格允许范围内」。"""
    text = str(message or "")
    return all(m in text for m in _REJECT_MARKERS)


def _key(workspace_id: str, spec_id: str, priority) -> str:
    return f"{workspace_id}|{spec_id}|{priority}"


def _load() -> Dict[str, dict]:
    try:
        with open(STORE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        # 读不出来就当没有 —— 这是加速层，**绝不能因为它让提交挂掉**
        return {}


def remember_rejection(
    workspace_id: str, spec_id: str, priority, gpu_count=None
) -> None:
    """记下这次拒绝。写失败一律静默 —— 诊断设施不许反过来搞挂生产。"""
    if not spec_id:
        return
    data = _load()
    data[_key(workspace_id, spec_id, priority)] = {
        "workspace_id": workspace_id,
        "spec_id": spec_id,
        "priority": priority,
        "gpu_count": gpu_count,
        "at": int(time.time()),
    }
    if len(data) > MAX_ENTRIES:
        for k in sorted(data, key=lambda k: data[k].get("at", 0))[
            : len(data) - MAX_ENTRIES
        ]:
            data.pop(k, None)
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        tmp = f"{STORE}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
        os.replace(tmp, STORE)
    except OSError:
        pass


def known_rejection(workspace_id: str, spec_id: str, priority) -> Optional[dict]:
    """这个组合上次被拒过吗？没见过返回 ``None``（放行去问平台）。"""
    if not spec_id:
        return None
    return _load().get(_key(workspace_id, spec_id, priority))


def _alternatives(priority, fair_scale: bool = True) -> List[int]:
    """建议改成哪个优先级。

    公平调度空间只认 1 和 4，中间值平台不认；其余空间是 1–10。
    这里只给**比当前低**的值 —— 被规格拒了往高调没有意义。
    """
    try:
        cur = int(priority)
    except (TypeError, ValueError):
        cur = 4
    pool = [1, 4] if fair_scale else list(range(1, 11))
    return [p for p in pool if p < cur] or [1]


def explain(
    workspace_id: str,
    spec_id: str,
    priority,
    gpu_count=None,
    compute_group_name: str = "",
) -> str:
    """把这次拒绝翻成能照着做的下一步。"""
    alts = _alternatives(priority)
    lines = [
        "这个**资源规格**不允许你要的优先级 —— 不是配额不够，也不是没卡，"
        "换个优先级或换个规格就能提上去。",
        "",
        f"  当前：--priority {priority}"
        + (f"，规格 {gpu_count} 卡" if gpu_count else "")
        + (f"，计算组 {compute_group_name}" if compute_group_name else ""),
        f"  先试：--priority {alts[0]}",
        "",
        "**平台的限制是逐个规格行来的**，实测形状是「小规格只给低优（会被抢占），"
        "整节点规格才不受限」。所以：",
        "  - 想要一个**不会被抢占**的小规格任务 → 换一个不受限的计算组（如开发区），"
        "而不是在这里硬提优先级",
        "  - 一定要高优 → 用整节点规格提",
        "",
        "⚠️ 允许范围**平台没有任何接口能读**，连这条报错都不说范围是什么。"
        "qzcli 已经记下这次拒绝，下次同样的（空间, 规格, 优先级）组合会在"
        "发请求前就拦住你。",
    ]
    return "\n".join(lines)


def explain_known(entry: dict, compute_group_name: str = "") -> str:
    """命中已学到的拒绝记录时的提示（还没发请求，先拦下来）。"""
    when = entry.get("at")
    stamp = (
        time.strftime("%Y-%m-%d %H:%M", time.localtime(when)) if when else "之前某次"
    )
    return (
        f"拦住了：这个规格 + `--priority {entry.get('priority')}` 的组合，"
        f"**{stamp} 提交时被平台拒过**（任务优先级不在该资源规格允许的范围内）。\n\n"
        + explain(
            entry.get("workspace_id", ""),
            entry.get("spec_id", ""),
            entry.get("priority"),
            entry.get("gpu_count"),
            compute_group_name,
        )
        + "\n\n如果你确认平台已经放开了这个组合，加 `--force-priority` 跳过这道拦截。"
    )
