#!/usr/bin/env python3
"""v1/v2 全量对齐扫描 —— 趁 v1 还在，把两边的语义差异一次性挖出来。

**为什么必须现在做**：平台即将下线 `/api/v1`。v1 是我们唯一的 ground truth，
它一消失，v2 的任何语义偏差就只能靠用户撞出来再反推。已经这样丢过好几次：
`avail` 全线 429、新计算组被误判、新项目被误判 —— 每次都是「用户报上来才知道」。

**和 `compare_v1_v2.py` 的区别**：那个是 6 个端点 × 单个工作空间 × 手动跑，
只比字段名。这个是**全部已迁端点 × 全部工作空间 × 逐字段逐值**，一次跑完出报告。

判据分三档，混在一起看会淹没真问题：

- ``SCHEMA``  字段名集合不一致 —— **最危险**，v2 换个字段名不会报错，只会静默
              返回空列表，代码照跑
- ``VALUE``   同一条记录同一字段值不同 —— 需要人判断是真差异还是实时波动
- ``VOLUME``  条目数/total 不一致 —— 分页语义或过滤语义不同

**只读**：不提交、不停止、不修改任何任务或缓存。

用法::

    python3 tools/parity_sweep.py                    # 全部工作空间
    python3 tools/parity_sweep.py --workspace 分布式
    python3 tools/parity_sweep.py --only jobs nodes  # 只扫指定端点
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from qzcli.api import get_api  # noqa: E402
from qzcli.config import find_workspace_by_name, get_cookie  # noqa: E402

OUT_DIR = REPO_ROOT / "docs"

# 实时波动允许的相对漂移。集群是活的，任务数/节点占用秒级变化；
# 卡太死这份报告会全红然后被当噪声忽略，那就白扫了。
VOLUME_DRIFT_TOLERANCE = 0.10

# 已核实无害的 v1/v2 差异：写在这里，报告里降级成 BENIGN 不再刷屏。
# **加进来必须附核实结论** —— 否则这个列表会变成掩盖真问题的地毯。
#: **已复核的结构差异**：v1/v2 字段名集合确实不同，但已查明无害。
#: key 是 ``(端点, 字段名)``，value 必须写清**为什么无害** ——
#: 「无人消费」要能指出全仓 grep 的结果，不能只写"应该没事"。
#:
#: 这不是"把红灯调绿"的开关。放进来的前提是**已经查过**：
#: 该字段有没有被代码读、缺了会不会让某个功能静默失效。
#: 没查清的一律留在报告里红着。
REVIEWED_SCHEMA_DIFFS = {
    ("projects", "member_remain_budget"): (
        "v1 独有，v2 无对应。含义是「你个人在该项目下的剩余额度」"
        "（区别于项目池的 remain_budget —— 实测公共科研项目 9.98 亿 vs 673）。"
        "全仓 grep 无任何消费点，迁 v2 无功能损失。"
        "（2026-08-09 勘误：此处原本写着「v2 无替代接口」，是错的 —— "
        "预算数据走 project.GetProjectBudgetUsageOverview / "
        "GetProjectMemberBudgetUsage，只是不在 qz CLI 的 spec 里。）"
    ),
}

KNOWN_BENIGN = {
    # v2 补了中文品牌名，v1 是空串。全量 96 对比对里唯一的实质差异，
    # 系统性出现在所有工作空间；我们代码只读 gpu_type，不读它。
    "brand_name": "v2 补充中文品牌名（v1 为空串），代码不读该字段",
}

# 这些字段天然随时间变化，值不一致不算差异。
# 注意要**递归**看：gpu/memory/cpu 是嵌套 dict，里面的 used/available 才是波动源，
# 只比顶层字段名会把整个 gpu={...} 判成差异。
VOLATILE_FIELDS = {
    "running_time_ms",
    "updated_at",
    "age",
    "last_timestamp",
    "usage_rate",
    "used",
    "available",
    "left_time",
    "live_time",
    "status",
    "sub_status",
    # 节点上"当前跑着哪些任务/哪些用户"。任务起停是分钟级的，v1 和 v2 两次调用
    # 之间就可能变。**已实测定性**：同为 v1、相隔 150 秒的两次调用，100 个共同
    # 节点里 tasks_associated 和 users_associated 各变了 2 个；而同一时刻的
    # v1 vs v2 是 100/100 完全一致。所以差异来自采样时刻不同，不是接口语义不同。
    #
    # 定性方法记在这里，免得下次发版又要从头查一遍：**拿 v1 跟 v1 自己比**。
    # 若同源两次调用就对不上，那就是波动；只有同源稳定、跨源不同，才是真差异。
    "tasks_associated",
    "users_associated",
}


class Finding:
    def __init__(self, kind, endpoint, workspace, detail):
        self.kind = kind  # SCHEMA / VALUE / VOLUME / ERROR
        self.endpoint = endpoint
        self.workspace = workspace
        self.detail = detail

    def as_dict(self):
        return {
            "kind": self.kind,
            "endpoint": self.endpoint,
            "workspace": self.workspace,
            "detail": self.detail,
        }


def _differs_ignoring_volatile(a, b) -> bool:
    """递归比较，跳过 VOLATILE_FIELDS 和 KNOWN_BENIGN。

    必须递归：``gpu`` / ``memory`` / ``cpu`` 都是嵌套 dict，波动的是里面的
    ``used`` / ``available``。只比顶层会把整个嵌套结构判成差异，
    真问题就被这些噪声淹了。
    """
    if isinstance(a, dict) and isinstance(b, dict):
        for k in set(a) | set(b):
            if k in VOLATILE_FIELDS or k in KNOWN_BENIGN:
                continue
            if _differs_ignoring_volatile(a.get(k), b.get(k)):
                return True
        return False
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return True
        return any(_differs_ignoring_volatile(x, y) for x, y in zip(a, b))
    return a != b


def _keys(obj) -> set:
    return set(obj.keys()) if isinstance(obj, dict) else set()


def _rows(payload: Dict[str, Any], candidates: Tuple[str, ...]) -> List[dict]:
    for key in candidates:
        val = payload.get(key)
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]
    return []


def _total(payload: Dict[str, Any], rows: List[dict]) -> int:
    t = payload.get("total")
    return t if isinstance(t, int) else len(rows)


def compare_pair(
    endpoint: str,
    ws_name: str,
    v1: Any,
    v2: Any,
    list_keys: Tuple[str, ...],
    id_field: Optional[str] = None,
) -> List[Finding]:
    """比对一对 v1/v2 响应，返回差异清单。"""
    out: List[Finding] = []

    if isinstance(v1, Exception) or isinstance(v2, Exception):
        # 一边报错一边不报错，本身就是重要信号（比如 v2 权限更严）
        if isinstance(v1, Exception) != isinstance(v2, Exception):
            who = "v1" if isinstance(v1, Exception) else "v2"
            err = v1 if isinstance(v1, Exception) else v2
            out.append(
                Finding(
                    "ERROR",
                    endpoint,
                    ws_name,
                    f"只有 {who} 失败: {type(err).__name__}: {str(err)[:120]}",
                )
            )
        return out

    # ---- 顶层字段名 ----
    k1, k2 = _keys(v1), _keys(v2)
    if k1 != k2:
        out.append(
            Finding(
                "SCHEMA",
                endpoint,
                ws_name,
                f"顶层字段不同：只在 v1={sorted(k1 - k2)} 只在 v2={sorted(k2 - k1)}",
            )
        )

    r1, r2 = _rows(v1, list_keys), _rows(v2, list_keys)

    # ---- 条目数 / total ----
    t1, t2 = _total(v1, r1), _total(v2, r2)
    if t1 or t2:
        drift = abs(t1 - t2) / max(t1, t2, 1)
        if drift > VOLUME_DRIFT_TOLERANCE:
            out.append(
                Finding(
                    "VOLUME",
                    endpoint,
                    ws_name,
                    f"total 差异 {drift:.0%}（v1={t1} v2={t2}），超出实时波动容忍度",
                )
            )

    if not r1 and not r2:
        return out
    if bool(r1) != bool(r2):
        out.append(
            Finding(
                "SCHEMA",
                endpoint,
                ws_name,
                f"一边有条目一边为空：v1={len(r1)} v2={len(r2)} —— "
                f"这正是「字段改名导致静默返回空」的表现",
            )
        )
        return out

    # ---- 元素字段名 ----
    #
    # 以前这里是 `_keys(r1[0])` vs `_keys(r2[0])` —— **只拿第一条记录比**。
    # 可选字段（只有运行中的任务才有 running_time_ms 之类）会让它随机报假差异：
    # 实测 running_time_ms 在 v1 的 15/20 条、v2 的 14/20 条里都出现，两边根本
    # 没有 schema 差异，只是第一条恰好一边有一边没有。假红比不报还糟 ——
    # 它会训练人忽略这个闸门。
    #
    # 改成**所有记录的字段名并集**，再扣掉已声明的波动字段：一个字段在两次调用
    # 之间出现/消失（任务跑完了）不是接口换名，不该占用 SCHEMA 这个最高告警级别。
    e1 = set().union(*(_keys(x) for x in r1)) if r1 else set()
    e2 = set().union(*(_keys(x) for x in r2)) if r2 else set()
    only1 = sorted((e1 - e2) - VOLATILE_FIELDS - set(KNOWN_BENIGN))
    only2 = sorted((e2 - e1) - VOLATILE_FIELDS - set(KNOWN_BENIGN))
    unreviewed1 = [f for f in only1 if (endpoint, f) not in REVIEWED_SCHEMA_DIFFS]
    unreviewed2 = [f for f in only2 if (endpoint, f) not in REVIEWED_SCHEMA_DIFFS]
    if unreviewed1 or unreviewed2:
        out.append(
            Finding(
                "SCHEMA",
                endpoint,
                ws_name,
                f"元素字段不同：只在 v1={unreviewed1} 只在 v2={unreviewed2}",
            )
        )
    reviewed = [f for f in only1 + only2 if (endpoint, f) in REVIEWED_SCHEMA_DIFFS]
    if reviewed:
        out.append(
            Finding(
                "SCHEMA_REVIEWED",
                endpoint,
                ws_name,
                "；".join(
                    f"{f}：{REVIEWED_SCHEMA_DIFFS[(endpoint, f)]}" for f in reviewed
                ),
            )
        )

    # ---- 逐条逐字段比值（只比两边都有的记录）----
    if id_field:
        by1 = {x.get(id_field): x for x in r1 if x.get(id_field)}
        by2 = {x.get(id_field): x for x in r2 if x.get(id_field)}
        both = set(by1) & set(by2)
        mismatched: Dict[str, int] = {}
        for key in both:
            a, b = by1[key], by2[key]
            for f in (e1 & e2) - VOLATILE_FIELDS - set(KNOWN_BENIGN):
                if _differs_ignoring_volatile(a.get(f), b.get(f)):
                    mismatched[f] = mismatched.get(f, 0) + 1
        if mismatched:
            top = sorted(mismatched.items(), key=lambda kv: -kv[1])[:6]
            out.append(
                Finding(
                    "VALUE",
                    endpoint,
                    ws_name,
                    f"{len(both)} 条交集里字段值不一致："
                    + ", ".join(f"{f}×{n}" for f, n in top),
                )
            )
    return out


def build_endpoints(a, cookie) -> List[dict]:
    """已迁端点的 v1/v2 配对表。每项：名字、两个调用、列表键、主键字段。

    ``global_scope=True`` 的端点**不按工作空间分**（比如项目列表，
    ``GetProjectForPage`` 根本不收 workspace 参数），只跑一次，不进
    「工作空间 × 端点」的笛卡尔积。
    """
    now = int(time.time())
    return [
        {
            # 项目列表是工作空间枚举的数据源 —— 它要是悄悄变了形状，
            # `qzcli ws` 会静默列不出空间，而不是报错。所以必须进体检。
            "name": "projects",
            "list_keys": ("items",),
            "id_field": "id",
            "global_scope": True,
            "v1": lambda _ws: {"items": a._project_list_items_v1(cookie)},
            "v2": lambda _ws: {"items": a._project_list_items_v2(cookie)},
        },
        {
            "name": "jobs",
            "list_keys": ("jobs",),
            "id_field": "job_id",
            "v1": lambda ws: a._list_jobs_v1(ws, cookie, page_size=20),
            "v2": lambda ws: a._list_jobs_v2(ws, cookie, page_size=20),
        },
        {
            "name": "notebooks",
            "list_keys": ("list",),
            "id_field": "notebook_id",
            "v1": lambda ws: a._list_notebooks_v1(ws, cookie, page_size=20),
            "v2": lambda ws: a._list_notebooks_v2(ws, cookie, page_size=20),
        },
        {
            "name": "nodes",
            "list_keys": ("node_dimensions",),
            "id_field": "name",
            "v1": lambda ws: a._list_node_dimension_v1(ws, cookie, page_size=100),
            "v2": lambda ws: a._list_node_dimension_v2(ws, cookie, page_size=100),
        },
        {
            "name": "tasks",
            "list_keys": ("task_dimensions",),
            "id_field": "task_id",
            "v1": lambda ws: a._list_task_dimension_v1(ws, cookie, page_size=50),
            "v2": lambda ws: a._list_task_dimension_v2(ws, cookie, page_size=50),
        },
        {
            "name": "basic_info",
            "list_keys": ("compute_groups",),
            "id_field": "compute_group_id",
            "v1": lambda ws: a._cluster_basic_info_v1(ws, cookie),
            "v2": lambda ws: a._cluster_basic_info_v2(ws, cookie),
        },
        {
            "name": "overview",
            "list_keys": ("task_groups",),
            "id_field": None,
            "v1": lambda ws: a._list_workspace_tasks_v1(ws, cookie, now - 86400, now),
            "v2": lambda ws: a._list_workspace_tasks_v2(ws, cookie, now - 86400, now),
        },
    ]


def render(findings: List[Finding], scanned: int, endpoints: List[str]) -> str:
    by_kind: Dict[str, List[Finding]] = {}
    for f in findings:
        by_kind.setdefault(f.kind, []).append(f)

    lines = (
        [
            "# v1 / v2 全量对齐扫描报告",
            "",
            f"- 扫描工作空间：**{scanned}**",
            f"- 扫描端点：{', '.join(endpoints)}",
            f"- 发现差异：**{len(findings)}**"
            f"（SCHEMA {len(by_kind.get('SCHEMA', []))} / "
            f"VALUE {len(by_kind.get('VALUE', []))} / "
            f"VOLUME {len(by_kind.get('VOLUME', []))} / "
            f"ERROR {len(by_kind.get('ERROR', []))} / "
            f"SCHEMA_REVIEWED {len(by_kind.get('SCHEMA_REVIEWED', []))}）",
            "",
            "> **判据分档**（混在一起看会淹没真问题）：",
            "> - `SCHEMA` 字段名不一致 —— **最危险**。v2 换字段名不会报错，只会静默返回空，代码照跑",
            "> - `VALUE`  同记录同字段值不同 —— 要人判断是真差异还是实时波动",
            "> - `VOLUME` 条目数/total 不一致 —— 分页或过滤语义不同",
            "> - `ERROR`  只有一边报错 —— 通常是 v2 权限更严或路由缺失",
            "> - `SCHEMA_REVIEWED` 字段名不一致但**已逐条查明无害** —— 不让闸门变红，"
            "但仍然印在这里，免得进了白名单就从视野里消失",
            "",
            "> 已核实无害、不再计入的差异：",
        ]
        + [f"> - `{k}` —— {v}" for k, v in KNOWN_BENIGN.items()]
        + [
            "",
        ]
    )
    if not findings:
        lines += ["**未发现差异。** v1 下线后 v2 的行为应与现状一致。", ""]
        return "\n".join(lines)

    for kind in ("SCHEMA", "ERROR", "VOLUME", "VALUE", "SCHEMA_REVIEWED"):
        items = by_kind.get(kind)
        if not items:
            continue
        lines += [f"## {kind}（{len(items)}）", ""]
        lines += ["| 端点 | 工作空间 | 详情 |", "|---|---|---|"]
        for f in items:
            d = f.detail.replace("|", "\\|")
            lines.append(f"| `{f.endpoint}` | {f.workspace} | {d} |")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workspace", help="只扫一个工作空间（名字或 ws-id）")
    ap.add_argument("--only", nargs="*", help="只扫指定端点")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    ap.add_argument("--sleep", type=float, default=0.3, help="每对之间间隔秒（防限流）")
    args = ap.parse_args()

    cookie = (get_cookie() or {}).get("cookie")
    if not cookie:
        print("✗ 没有 cookie —— 先 `qzcli login`", file=sys.stderr)
        return 1

    a = get_api()
    if args.workspace:
        wid = args.workspace
        if not wid.startswith("ws-"):
            wid = find_workspace_by_name(wid) or ""
        if not wid:
            print(f"✗ 找不到工作空间 {args.workspace}", file=sys.stderr)
            return 1
        workspaces = [{"id": wid, "name": args.workspace}]
    else:
        # ⚠️ **枚举工作空间必须固定走 v1 腿。**
        # list_workspaces 现在默认走 v2（project GetProjectForPage），而项目列表
        # 本身就是本工具要验的对象之一 —— 拿被测对象来驱动测试，它真坏了的话，
        # 这里可能只扫到部分空间却报「全部一致」，是最难发现的一种假绿。
        workspaces = a._workspaces_from_project_items(a._project_list_items_v1(cookie))

    endpoints = build_endpoints(a, cookie)
    if args.only:
        keep = set(args.only)
        endpoints = [e for e in endpoints if e["name"] in keep]

    print(
        f"→ {len(workspaces)} 个工作空间 × {len(endpoints)} 个端点 "
        f"= {len(workspaces) * len(endpoints)} 对比对\n",
        file=sys.stderr,
    )

    findings: List[Finding] = []
    done_global = set()
    for ws in workspaces:
        wid, wname = ws["id"], (ws.get("name") or ws["id"])[:20]
        for ep in endpoints:
            if ep.get("global_scope"):
                if ep["name"] in done_global:
                    continue
                done_global.add(ep["name"])
                wname = "（全局）"

            def run(fn: Callable):
                try:
                    return fn(wid)
                except Exception as exc:  # 两边任一失败都是有效信号
                    return exc

            # **先 v2 后 v1**：v2 是我们要验的那个，让它拿更新鲜的快照，
            # 差异更可能归因于语义而不是时序
            r2, r1 = run(ep["v2"]), run(ep["v1"])
            got = compare_pair(
                ep["name"], wname, r1, r2, ep["list_keys"], ep.get("id_field")
            )
            findings.extend(got)
            mark = "✓" if not got else f"⚠ {len(got)}"
            print(f"  {wname:22} {ep['name']:12} {mark}", file=sys.stderr)
            time.sleep(args.sleep)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    names = [e["name"] for e in endpoints]
    (args.out_dir / "v1_v2_parity_report.md").write_text(
        render(findings, len(workspaces), names), encoding="utf-8"
    )
    (args.out_dir / "v1_v2_parity_raw.json").write_text(
        json.dumps([f.as_dict() for f in findings], ensure_ascii=False, indent=2)
        + "\n",
        encoding="utf-8",
    )
    schema = sum(1 for f in findings if f.kind == "SCHEMA")
    reviewed = sum(1 for f in findings if f.kind == "SCHEMA_REVIEWED")
    print(
        f"\n✓ 完成：{len(findings)} 处差异"
        f"（未复核 SCHEMA {schema} / 已复核 {reviewed}）"
        f" → {args.out_dir}/v1_v2_parity_report.md"
    )
    # 只有**未复核**的 SCHEMA 才让闸门红。已复核的仍然逐条印在报告里，
    # 不会因为"进了白名单"就从视野里消失。
    return 1 if schema else 0


if __name__ == "__main__":
    sys.exit(main())
