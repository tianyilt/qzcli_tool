"""把**机器自己**的健康事件翻成「这台还能不能用」。

## 为什么单独有这一层

`qzcli events <job>` 回答的是「**我这个任务**为什么没起来」。它答不了另一个问题：
**「是不是这台机器本身有病？」**

这个缺口是实的：qzcli 早就有 `--exclude-node`，但**没有任何东西告诉你该排除谁**。
撞上「同一台机器上反复失败」时，此前只能靠感觉。

平台上唯一按**节点**而不是按工作负载组织的事件源是
``cluster?Action=ListNodeEvents``（2026-08-27 实测可读，660 条历史）。
它给的是机器自身的病历：

- ``XIDIsUnhealthy`` —— GPU 报了 XID，**硬件级错误**，重跑多半还是死
- ``GPFSMountUnhealthy`` —— 共享盘挂载不健康，读写 ``/inspire/...`` 会挂
- ``NodeNotReady`` —— 节点掉线
- ``NodeHasDiskPressure`` / ``NodeHasMemoryPressure`` —— 资源压力

## 关键：健康事件是**成对**出现的

平台把 condition 的两个方向都记成事件：``XIDIsUnhealthy`` 之后往往跟着
``XIDIsHealthy``。**只看到前者就报警会天天误报** —— 那台机器可能十分钟后就恢复了。

所以判据是：**看每类问题的最后一次状态翻转**。最后一条是 Unhealthy 才算现在有病；
最后一条是 Healthy 就是已经恢复。
"""

import re
import time
from typing import Dict, List, Optional

#: 问题类别 → (识别不健康的 reason, 对应的恢复 reason, 人话, 该怎么办)
#:
#: **成对登记是这张表的核心。** 只登记"坏"的一半，就会把已经恢复的机器一直报成有病。
#:
#: ⚠️ "好"的那半必须写成 ``(?<!un)healthy`` 而不是 ``healthy``。忽略大小写时
#: ``XID.*Healthy`` **也匹配 ``XIDIsUnhealthy``**（XID…un**healthy**），而我们先判"好"
#: 后判"坏" —— 于是一台正在报 XID 的机器会被读成"已恢复"。``Schedulable`` 与
#: ``Unschedulable`` 是同一个陷阱。这不是理论问题，第一版就是这么写的，测试直接变红。
_CONDITIONS = [
    (
        "gpu_xid",
        re.compile(r"XID.*Unhealthy|Unhealthy.*XID", re.I),
        re.compile(r"XID.*(?<!un)healthy", re.I),
        "GPU 报过 XID 错误",
        "**硬件级故障，重跑大概率还是死在这台。** 提交时 `--exclude-node <这台>`，"
        "或换一个计算组。",
    ),
    (
        "gpfs",
        re.compile(r"GPFSMount.*Unhealthy|GPFS.*Unhealthy", re.I),
        re.compile(r"GPFS.*(?<!un)healthy", re.I),
        "共享盘挂载不健康",
        "读写 `/inspire/...` 会挂或极慢。**日志里看起来像是你的代码卡住了**，"
        "其实是盘。避开这台。",
    ),
    (
        "not_ready",
        re.compile(r"NodeNotReady", re.I),
        re.compile(r"NodeReady", re.I),
        "节点掉过线",
        "任务会被驱逐重调度。偶发可忽略；反复出现就避开这台。",
    ),
    (
        "disk",
        re.compile(r"NodeHasDiskPressure", re.I),
        re.compile(r"NodeHasNoDiskPressure", re.I),
        "磁盘吃紧",
        "本地盘快满，容器可能被驱逐。别往容器本地盘写大文件，写共享盘。",
    ),
    (
        "memory",
        re.compile(r"NodeHasMemoryPressure", re.I),
        re.compile(r"NodeHasNoMemoryPressure", re.I),
        "内存吃紧",
        "可能被 OOM kill。降 batch 或换机器。",
    ),
    (
        "cordon",
        re.compile(r"Unschedulable|Cordon(?!ed off complete)", re.I),
        re.compile(r"(?<!un)schedulable|uncordon", re.I),
        "被禁止调度",
        "这台被管理员摘下来了，**看着空但一张卡都拿不到**。`avail` 的空节点数"
        "包含这类，别信。",
    ),
]


def _ts(ev: Dict) -> int:
    """取事件时间。字段可能是 last_timestamp / first_timestamp，也可能是字符串。"""
    for k in ("last_timestamp", "first_timestamp", "created_at"):
        v = ev.get(k)
        if v in (None, ""):
            continue
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v)
        if s.isdigit():
            return int(s)
        # ISO 串
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
            try:
                return int(time.mktime(time.strptime(s[:19], fmt)))
            except ValueError:
                continue
    return 0


def _text(ev: Dict) -> str:
    return f"{ev.get('reason') or ''} {ev.get('message') or ''}"


def diagnose_node(events: List[Dict]) -> Dict:
    """一台机器现在健不健康。

    返回 ``{"problems": [...], "recovered": [...]}``：

    - ``problems``：**最后一次状态翻转停在"坏"**的类别 —— 现在有病
    - ``recovered``：出现过但**最后停在"好"**的类别 —— 曾经坏过、已恢复

    区分这两者是这个模块存在的理由：平台把 condition 的两个方向都记成事件，
    只匹配"坏"的那一半会把恢复了的机器天天报成有病。
    """
    evs = sorted(events or [], key=_ts)
    # 每个类别记最后一次翻转：True=坏, False=好
    last: Dict[str, tuple] = {}
    for ev in evs:
        text = _text(ev)
        for key, bad, good, title, advice in _CONDITIONS:
            # **先判"好"再判"坏"** —— "XIDIsHealthy" 也含 "XID"，顺序反了会把恢复
            # 事件误读成故障。
            if good.search(text):
                last[key] = (False, ev, title, advice)
            elif bad.search(text):
                last[key] = (True, ev, title, advice)

    problems, recovered = [], []
    for key, (is_bad, ev, title, advice) in last.items():
        item = {
            "kind": key,
            "title": title,
            "advice": advice,
            "reason": ev.get("reason", ""),
            "at": _ts(ev),
            "raw": (ev.get("message") or "")[:200],
        }
        (problems if is_bad else recovered).append(item)
    problems.sort(key=lambda x: -x["at"])
    recovered.sort(key=lambda x: -x["at"])
    return {"problems": problems, "recovered": recovered}


def verdict(diag: Dict) -> str:
    """一句话结论。"""
    if diag["problems"]:
        return "现在有问题：" + "；".join(p["title"] for p in diag["problems"])
    if diag["recovered"]:
        return "现在正常（曾经出过：" + "、".join(r["title"] for r in diag["recovered"]) + "）"
    return "没发现异常记录"


#: 多久以前的"未恢复"还算数。超过这个天数就不再当成现存故障。
#:
#: 为什么需要它：平台**只在状态翻转时记事件**，不保证一定会记恢复。实测
#: 一台正常在跑任务的生产机，最后一条 GPFS 事件是 3.5 个月前的
#: ``GPFSMountUnhealthy``，之后再没有 Healthy —— 按"最后一次翻转"判就成了
#: "现在有问题"，还会建议把它排除掉。**照这个报警去 --exclude-node，
#: 半个机房都得排掉。**
#:
#: 所以陈旧的未恢复记录降级成"存疑"：如实说"最后一次是 N 天前，之后没有恢复
#: 记录"，让人自己判断，而不是替他下结论。
STALE_AFTER_DAYS = 7


def age_days(item: Dict, now: Optional[int] = None) -> Optional[float]:
    """这条记录距今多少天。时间戳是**毫秒**。"""
    at = item.get("at") or 0
    if not at:
        return None
    now_ms = int((now if now is not None else time.time()) * 1000)
    return max(0.0, (now_ms - at) / 86_400_000.0)


def is_stale(item: Dict, now: Optional[int] = None) -> bool:
    d = age_days(item, now)
    return d is not None and d > STALE_AFTER_DAYS


def should_exclude(diag: Dict, now: Optional[int] = None) -> bool:
    """要不要建议 `--exclude-node`。

    两个条件都要满足：

    1. 是**硬件/存储类**问题 —— 磁盘/内存压力是暂时的，为它排除节点会把可用
       机器越排越少
    2. **最近的** —— 陈旧的未恢复记录不算（见 ``STALE_AFTER_DAYS``）
    """
    return any(
        p["kind"] in ("gpu_xid", "gpfs", "cordon") and not is_stale(p, now)
        for p in diag["problems"]
    )


def exclude_hint(bad_nodes: List[str]) -> Optional[str]:
    """给出可以直接粘到 `qzcli create` 后面的排除串。"""
    if not bad_nodes:
        return None
    return " ".join(f"--exclude-node {n}" for n in sorted(set(bad_nodes)))
