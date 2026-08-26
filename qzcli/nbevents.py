"""把开发机的平台事件翻译成「能照着行动」的结论。

## 为什么需要翻译

平台 ``notebook ListNotebookEvents`` 只给一段 ``content``，**没有 reason/type
字段**，原文是 K8s 调度器的话，比如：

    0/1100 nodes are unavailable: 177 Insufficient memory, 260 Insufficient cpu,
    599 node(s) didn't match Pod's node affinity/selector, 64 node(s) were unschedulable

对着这句，新同学看不出该做什么。而它其实同时说了三件事：内存不够、CPU 不够、
**599 台机器压根不匹配你选的计算组**。最后那条才是关键 —— 说明计算组挑错了，
换个组就行，等再久也没用。

2026-08 我对着一台 PENDING 的开发机猜了 55 分钟，而这段话平台一直存着。
所以这里做的事只有一件：**把原文里真正决定你下一步动作的那部分拎出来。**
"""

import re
from typing import Dict, List, Optional

#: 分类规则。**顺序有意义** —— 越靠前的越"根因"，命中即停。
#: 比如同时有「亲和性不匹配」和「内存不足」时，前者才是该说的：
#: 机器根本不属于你选的组，缺多少内存都无所谓。
_RULES = [
    (
        "affinity",
        re.compile(r"didn't match Pod's node affinity|node\(s\) didn't match", re.I),
        "计算组挑错了",
        "有大量机器根本不属于你选的计算组。换一个组，等再久也没用 —— 用 "
        "`qzcli avail` 看哪个组总节点数多、有余量。",
    ),
    (
        "preempted",
        re.compile(r"preempt|evict", re.I),
        "被更高优先级的任务挤掉了",
        "你的优先级太低。要么提高 `--priority`，要么换一个不那么抢手的计算组。",
    ),
    (
        "quota",
        re.compile(r"exceeded quota|insufficient quota|quota.*exceed", re.I),
        "配额不够",
        "申请的资源超过了你在这个空间的配额上限。降规格，或者找管理员加配额。",
    ),
    (
        "insufficient",
        re.compile(r"Insufficient (cpu|memory|nvidia|gpu)", re.I),
        "资源不够",
        "目标计算组当前没有足够的空闲资源。降规格（少要点 CPU/内存/卡），"
        "或换一个有余量的组。",
    ),
    (
        "unschedulable",
        re.compile(r"nodes are unavailable|FailedScheduling|Unschedulable", re.I),
        "排不上",
        "调度器没找到能放下它的机器。先用 `qzcli avail` 确认目标组还有余量。",
    ),
    (
        "image",
        re.compile(r"Failed to pull image|ImagePullBackOff|ErrImagePull", re.I),
        "镜像拉不下来",
        "镜像不存在、或没权限、或太大超时。换一个你有权限的小镜像试试。",
    ),
    (
        "image_pulling",
        re.compile(r"Pulling image", re.I),
        "正在拉镜像",
        "还没起来是正常的 —— 大镜像可能要几分钟到几十分钟。",
    ),
    (
        "scheduled",
        re.compile(r"Successfully assigned|Scheduled", re.I),
        "已调度到机器",
        "",
    ),
    (
        "started",
        re.compile(r"Started container|Created container|Successfully pulled", re.I),
        "容器已启动",
        "",
    ),
]

#: 这些属于"已经在往前走"，不是问题。诊断时不该把它们当卡点报出来。
HEALTHY = {"scheduled", "started", "image_pulling"}


def classify_notebook_event(content: str) -> Dict[str, str]:
    """把一条事件 content 归类。返回 ``{kind, title, advice}``。

    认不出来时 ``kind='unknown'`` 并把原文带出去 —— **不要静默丢掉**，
    平台文案会变，认不出的那条往往正是新情况。
    """
    text = content or ""
    for kind, pat, title, advice in _RULES:
        if pat.search(text):
            return {"kind": kind, "title": title, "advice": advice}
    return {"kind": "unknown", "title": "未识别的事件", "advice": ""}


def summarize_unschedulable(content: str) -> List[str]:
    """从 "0/1100 nodes are unavailable: 177 Insufficient memory, ..." 里
    把各个原因拆成人话，按机器数从多到少排。

    机器数最多的那条通常就是根因 —— 599 台"不匹配亲和性"比 177 台"内存不足"
    更能说明问题出在选组上。
    """
    m = re.search(r"nodes are unavailable:\s*(.+)", content or "", re.I)
    if not m:
        return []
    parts = []
    for chunk in m.group(1).split(","):
        chunk = chunk.strip().rstrip(".")
        if not chunk:
            continue
        num = re.match(r"(\d+)\s+(.*)", chunk)
        if not num:
            parts.append((0, chunk))
            continue
        n, why = int(num.group(1)), num.group(2)
        why_cn = (
            "机器不属于你选的计算组"
            if "affinity" in why.lower() or "didn't match" in why.lower()
            else "内存不足" if "memory" in why.lower()
            else "CPU 不足" if "cpu" in why.lower()
            else "GPU 不足" if ("gpu" in why.lower() or "nvidia" in why.lower())
            else "机器被禁止调度" if "unschedulable" in why.lower()
            else "有污点未容忍" if "taint" in why.lower()
            else why
        )
        parts.append((n, f"{n} 台：{why_cn}"))
    parts.sort(key=lambda x: -x[0])
    return [t for _, t in parts]


#: 一台开发机可以停了再启，事件列表里会攒着**好几轮**的记录。
#: 这些标志着"新的一轮启动开始了"——它们之前的事件属于上一轮，与现在无关。
_ROUND_START = re.compile(r"Successfully assigned|Scheduled", re.I)


def current_round(events: List[Dict]) -> List[Dict]:
    """只取**最近一轮**启动的事件。

    开发机停了再启会在同一个列表里攒多轮记录。不切轮次的话，一台现在跑得好好
    的机器会被上一轮失败的事件误判成有问题 —— 实测就这么把一台 RUNNING 的机器
    报成了「配额不够」，而它最近 8 条全是正常进展。
    """
    evs = events or []
    last_start = None
    for i, ev in enumerate(evs):
        if _ROUND_START.search(ev.get("content") or ""):
            last_start = i
    return evs[last_start:] if last_start is not None else evs


def diagnose(events: List[Dict], status: str = "") -> Optional[Dict]:
    """给出**一个**结论：这台开发机现在卡在哪、该做什么。

    只看**最近一轮**里最后一条非健康事件 —— 排队时调度器会每隔几秒重复同一条
    Unschedulable，全列出来是刷屏；而跨轮次翻旧账会误报。

    返回 ``None`` 表示没发现问题（事件里全是正常进展）。
    """
    for ev in reversed(current_round(events)):
        content = ev.get("content") or ""
        cls = classify_notebook_event(content)
        if cls["kind"] in HEALTHY or cls["kind"] == "unknown":
            continue
        return {
            "kind": cls["kind"],
            "title": cls["title"],
            "advice": cls["advice"],
            "breakdown": summarize_unschedulable(content),
            "raw": content,
            "created_at": ev.get("created_at", ""),
        }
    return None
