#!/usr/bin/env python3
"""活体冒烟：把 qzcli 每个功能点在**真实平台**上跑一遍，逐项判定通过与否。

为什么不能只靠 `--dry-run` 和单测：单测里 `_curl_post` 是假的，dry-run 根本不发请求。
v1→v2 迁移最典型的翻车是"接口通了但语义变了"（过滤被忽略、字段改名导致列表恒空、
分页参数没被认），**只有拿真实响应才能发现**。

用法::

    python3 tools/live_smoke.py --workspace CI-情境智能            # 只读部分
    python3 tools/live_smoke.py --workspace CI-情境智能 --submit   # 含真实提交+停止

`--submit` 会**真的提交一个任务并在验证完后停掉它**（默认 1 卡、低优先级、
命令是 echo 立即退出）。会消耗少量点券。不加这个 flag 则完全只读。
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, List

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from qzcli import api as qzapi  # noqa: E402
from qzcli.api import QzAPIError, get_api  # noqa: E402
from qzcli.config import (  # noqa: E402
    find_workspace_by_name,
    load_all_resources,
    get_cookie,
    get_credentials,
    get_workspace_resources,
    load_env_file,
)


class _QuietDisplay:
    """收集输出的假 display —— exec 内部会 print 一堆进度，别打乱 smoke 的表格。

    用 ``__getattr__`` 兜住 print_* 家族：真 display 的方法集合会变，
    写死几个会因为一个无关的新方法而 AttributeError。
    """

    def __init__(self):
        self.lines = []

    def print(self, msg="", *a, **kw):
        self.lines.append(str(msg))

    def __getattr__(self, name):
        if name.startswith("print"):
            return self.print
        raise AttributeError(name)


RESULTS: List[Dict[str, Any]] = []


def check(point: str, cmd: str):
    """装饰器：把一个功能点的验证包成一条结果记录。

    判定标准写在每个用例里 —— 关键是**验语义不只验不报错**：
    列表要非空、过滤要真的改变结果、v1/v2 要一致。
    """

    def deco(fn: Callable[[], str]):
        def run():
            t0 = time.time()
            try:
                detail = fn()
                RESULTS.append(
                    {
                        "point": point,
                        "cmd": cmd,
                        "ok": True,
                        "detail": detail,
                        "sec": round(time.time() - t0, 1),
                    }
                )
                print(f"  ✓ {point:24} {detail}")
            except Exception as exc:
                RESULTS.append(
                    {
                        "point": point,
                        "cmd": cmd,
                        "ok": False,
                        "detail": f"{type(exc).__name__}: {exc}",
                        "sec": round(time.time() - t0, 1),
                    }
                )
                print(f"  ✗ {point:24} {type(exc).__name__}: {exc}")
                if VERBOSE:
                    traceback.print_exc()

        return run

    return deco


VERBOSE = False


def assert_true(cond: bool, msg: str):
    if not cond:
        raise AssertionError(msg)


def main() -> int:
    global VERBOSE
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workspace", required=True, help="工作空间名或 ws-<uuid>")
    ap.add_argument(
        "--submit", action="store_true", help="含真实提交+停止（会消耗点券）"
    )
    ap.add_argument("--compute-group", help="提交用的 lcg-<uuid>；不传则自动挑有空卡的")
    ap.add_argument("--spec", help="提交用的 spec_id；不传则自动挑 GPU 数最小的")
    ap.add_argument(
        "--image",
        default="docker.sii.shaipower.online/inspire-studio/dhyu-wan-torch29:0.4",
    )
    ap.add_argument(
        "--queue-compute-group",
        help=(
            "低优排队用例的目标计算组 lcg-<uuid>。挑一个低优空位远少于 "
            "--queue-instances 的分区，这样任务必定停在排队、不会真占资源"
        ),
    )
    ap.add_argument(
        "--exec-notebook",
        help=(
            "exec 用例的目标开发机（notebook_id 或名字）。不传则自动挑："
            "优先 CI 空间里 RUNNING 的 mova-base*，再退回任一 RUNNING 的"
        ),
    )
    ap.add_argument(
        "--queue-instances",
        type=int,
        default=72,
        help="低优排队用例的节点数（默认 72，对齐真实训练规模）",
    )
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()
    VERBOSE = args.verbose

    cookie = (get_cookie() or {}).get("cookie")
    if not cookie:
        print("✗ 无 cookie，先 `qzcli login`", file=sys.stderr)
        return 1

    ws = args.workspace
    if not ws.startswith("ws-"):
        resolved = find_workspace_by_name(ws)
        assert resolved, f"未找到工作空间 {ws}"
        ws = resolved
    a = get_api()
    print(f"→ workspace={ws}\n")

    state: Dict[str, Any] = {}

    # ---------------- 只读功能点 ----------------
    print("【只读】")

    @check("查询训练任务列表", "qzcli list -c")
    def _jobs():
        v2 = a._list_jobs_v2(ws, cookie, page_size=5)
        v1 = a._list_jobs_v1(ws, cookie, page_size=5)
        assert_true(v2.get("jobs"), "v2 返回空列表 —— 典型的静默失败")
        assert_true(
            v1["total"] == v2["total"],
            f"v1/v2 total 不一致 {v1['total']} vs {v2['total']}",
        )
        assert_true(
            len(v2["jobs"]) == 5, f"page_size 未生效，返回 {len(v2['jobs'])} 条"
        )
        state["job_id"] = v2["jobs"][0].get("job_id")
        return f"total={v2['total']}，v1/v2 一致，page_size 生效"

    _jobs()

    @check("过滤参数真的生效", "qzcli list -c --user")
    def _filter():
        # 迁移最阴的翻车：v2 不认识某个过滤参数就**静默忽略**，返回全量。
        # 字段名比对发现不了这个 —— 必须验"过滤前后结果确实变了"。
        cur = a._list_jobs_v2(ws, cookie, page_size=1)["total"]
        by_user = a._list_jobs_v2(
            ws, cookie, page_size=1, created_by="user-不存在的人"
        )["total"]
        assert_true(by_user != cur, f"created_by 过滤未生效：过滤前后都是 {cur}")
        return f"不过滤={cur}，过滤到不存在用户={by_user}（确实收窄）"

    _filter()

    @check("查询任务详情", "qzcli status")
    def _detail():
        jid = state.get("job_id")
        assert_true(jid, "上一步没拿到 job_id")
        v2 = a._get_job_detail_v2(jid, cookie)
        v1 = a._get_job_detail_v1(jid, cookie)
        assert_true(v2.get("name"), "v2 详情缺 name")
        assert_true(v1.get("name") == v2.get("name"), "v1/v2 name 不一致")
        assert_true(v1.get("status") == v2.get("status"), "v1/v2 status 不一致")
        return f"{v2.get('name')[:28]} status={v2.get('status')}，v1/v2 一致"

    _detail()

    # 注意：`qzcli events` 子命令**不在 master**，在未合并的 PR #39 里。
    # 这里验的是平台侧 Action 可用（给 PR #39 合并后直接写 v2 铺路），
    # 不是在验 qzcli 现有代码路径。
    @check("任务调度事件（平台侧）", "train ListJobEvents")
    def _events():
        jid = state["job_id"]
        r = a._request_v2(
            "train",
            "ListJobEvents",
            {
                "filter": {"object_type": "job", "object_ids": [jid]},
                "page_num": 1,
                "page_size": 5,
            },
        )
        assert_true("events" in r, f"响应缺 events 字段: {sorted(r)}")
        return f"events={len(r.get('events') or [])} total={r.get('total')}"

    _events()

    @check("查看任务日志", "qzcli logs")
    def _logs():
        # 找一个跑过的任务（CREATING 的没有日志）
        jobs = a._list_jobs_v2(ws, cookie, page_size=30)["jobs"]
        for j in jobs:
            try:
                r = a.get_job_logs(j["job_id"], page_size=3)
            except QzAPIError:
                continue
            hits = (r.get("hits") or {}).get("hits") or r.get("logs") or []
            if hits:
                state["log_job"] = j["job_id"]
                return f"{j['job_id'][:20]}… 取到 {len(hits)} 条日志"
        raise AssertionError("扫了 30 个任务都没取到日志")

    _logs()

    @check("开发机列表 + 过滤", "qzcli list -c（开发机段）")
    def _nb():
        allnb = a._list_notebooks_v2(ws, cookie, page_size=50)
        run = a._list_notebooks_v2(ws, cookie, page_size=50, status=["RUNNING"])
        v1 = a._list_notebooks_v1(ws, cookie, page_size=50)
        assert_true(v1.get("total") == allnb.get("total"), "v1/v2 开发机总数不一致")
        assert_true(
            run.get("total", 0) <= allnb.get("total", 0),
            "status 过滤后反而变多，过滤未生效",
        )
        if run.get("list"):
            state["notebook"] = run["list"][0]
        return f"全部={allnb.get('total')} RUNNING={run.get('total')}，v1/v2 一致"

    _nb()

    @check("空余节点 / 碎卡", "qzcli avail")
    def _avail():
        # `avail` 是翻完**所有**节点再统计的。只取第一页会踩坑：节点按类型排序，
        # 前几十台全是 cpu/hpc，GPU 数当然是 0（v1 也一样）。所以这里必须翻页，
        # 否则测出来的"没有 GPU"是采样问题不是回归。
        def walk(fetch):
            nodes, page = [], 1
            while True:
                r = fetch(ws, cookie, page_num=page, page_size=100)
                batch = r.get("node_dimensions") or []
                nodes.extend(batch)
                if len(nodes) >= r.get("total", 0) or not batch or page > 10:
                    return nodes, r.get("total", 0)
                page += 1

        n2, t2 = walk(a._list_node_dimension_v2)
        n1, t1 = walk(a._list_node_dimension_v1)
        assert_true(n2, "node_dimensions 为空")
        assert_true(t1 == t2, f"v1/v2 节点总数不一致 {t1} vs {t2}")
        assert_true(set(n2[0]) == set(n1[0]), "v1/v2 节点条目字段名不一致")

        gpu2 = {
            x["name"]: (x.get("gpu") or {})
            for x in n2
            if (x.get("gpu") or {}).get("total")
        }
        gpu1 = {
            x["name"]: (x.get("gpu") or {})
            for x in n1
            if (x.get("gpu") or {}).get("total")
        }
        assert_true(gpu2, f"{len(n2)} 个节点里一台有 GPU 的都没有")
        assert_true(set(gpu1) == set(gpu2), "v1/v2 认定的 GPU 节点集合不同")

        # 真正的回归判据：同一台机器两边算出的卡数要一样。
        # 但集群是活的（这套 97% 利用率、任务不停起落），v2 和 v1 是两次
        # **不同时刻**的快照，个别节点在这几秒内被重新分配是正常的。所以对不上的
        # 节点要**单独复查一次**，只有复查仍然不一致才算真差异 —— 否则这个用例
        # 会随机红，然后被当成噪声忽略，那就白测了。
        def gpu_of(fetch, name):
            page = 1
            while page <= 10:
                r = fetch(ws, cookie, page_num=page, page_size=100)
                for x in r.get("node_dimensions") or []:
                    if x.get("name") == name:
                        return x.get("gpu") or {}
                if page * 100 >= r.get("total", 0):
                    break
                page += 1
            return {}

        def differs(g1, g2):
            return g1.get("total") != g2.get("total") or g1.get("used") != g2.get(
                "used"
            )

        suspects = [k for k in gpu2 if differs(gpu1[k], gpu2[k])]
        real, churn = [], []
        for name in suspects:
            if differs(
                gpu_of(a._list_node_dimension_v1, name),
                gpu_of(a._list_node_dimension_v2, name),
            ):
                real.append(name)
            else:
                churn.append(name)
        assert_true(not real, f"{len(real)} 台节点复查后 v1/v2 仍不一致: {real[:3]}")

        tot = sum(g.get("total", 0) for g in gpu2.values())
        used = sum(g.get("used", 0) for g in gpu2.values())
        note = f"，{len(churn)} 台为集群实时波动（复查一致）" if churn else ""
        return (
            f"节点 total={t2}，{len(gpu2)} 台 GPU 机器，"
            f"共 {used}/{tot} 卡已用，v1/v2 逐台一致{note}"
        )

    _avail()

    @check("GPU 使用分布", "qzcli usage")
    def _usage():
        v2 = a._list_task_dimension_v2(ws, cookie, page_size=20)
        v1 = a._list_task_dimension_v1(ws, cookie, page_size=20)
        d2 = v2.get("task_dimensions") or []
        d1 = v1.get("task_dimensions") or []
        assert_true(d2, "task_dimensions 为空")
        assert_true(set(d2[0]) == set(d1[0]), "v1/v2 任务维度字段名不一致")

        # 这里统计的是**正在跑的任务**，秒级变化（本脚本自己就会提交/停止任务）。
        # 严格比 total 必然随机红，所以只要求量级一致；字段名对齐才是防静默失败的
        # 真判据。总数完全一样反而说明取的是缓存。
        t1, t2 = v1.get("total", 0), v2.get("total", 0)
        drift = abs(t1 - t2) / max(t1, t2, 1)
        assert_true(
            drift < 0.10,
            f"v1/v2 任务维度总数差异 {drift:.1%}（{t1} vs {t2}）超出实时波动范围",
        )
        # 交集里的任务，两边算出的 GPU 数必须一致
        by2 = {x.get("task_id") or x.get("id"): x for x in d2}
        by1 = {x.get("task_id") or x.get("id"): x for x in d1}
        both = set(by1) & set(by2)
        bad = [
            k
            for k in both
            if (by1[k].get("gpu") or {}).get("total")
            != (by2[k].get("gpu") or {}).get("total")
        ]
        assert_true(not bad, f"{len(bad)} 个任务 v1/v2 GPU 数不一致: {list(bad)[:3]}")
        return (
            f"任务维度 total={t2}（v1={t1}，实时波动 {drift:.1%}），"
            f"字段一致，交集 {len(both)} 个任务 GPU 数一致"
        )

    _usage()

    @check("工作空间任务概览", "qzcli ws")
    def _wstasks():
        v2 = a._list_workspace_tasks_v2(
            ws, cookie, int(time.time()) - 86400, int(time.time())
        )
        v1 = a._list_workspace_tasks_v1(
            ws, cookie, int(time.time()) - 86400, int(time.time())
        )
        tg = v2.get("task_groups") or []
        assert_true(tg, "task_groups 为空")
        assert_true(len(v1.get("task_groups") or []) == len(tg), "v1/v2 分组数不一致")
        return f"{len(tg)} 个任务类型分组，v1/v2 一致"

    _wstasks()

    @check("集群/计算组基础信息", "qzcli res")
    def _basic():
        v2 = a._cluster_basic_info_v2(ws, cookie)
        v1 = a._cluster_basic_info_v1(ws, cookie)
        assert_true(v2.get("compute_groups"), "compute_groups 为空")
        assert_true(
            len(v1.get("compute_groups") or []) == len(v2["compute_groups"]),
            "v1/v2 计算组数不一致",
        )
        state["basic"] = v2
        return f"{len(v2['compute_groups'])} 个计算组，v1/v2 一致"

    _basic()

    @check("规格(spec)发现", "qzcli create 选规格")
    def _specs():
        lcgs = a._request_v2(
            "workspace",
            "ListLogicComputeGroups",
            {"filter": {"workspace_id": ws}, "page_num": 1, "page_size": 50},
        )
        groups = lcgs.get("logic_compute_groups") or []
        assert_true(groups, "ListLogicComputeGroups 返回空")
        state["lcgs"] = groups
        found = []
        for g in groups[:12]:
            s = a.list_specs(g["logic_compute_group_id"], ws)
            if s:
                found.extend(s)
        assert_true(found, "所有计算组都反推不出 spec —— create 会不可用")
        state["specs"] = found
        gmin = min(x["gpu_count"] for x in found if x["gpu_count"])
        return f"{len(groups)} 个计算组，反推出 {len(found)} 个 spec（最小 {gmin} 卡）"

    _specs()

    @check("计算组硬件规格", "list_node_specs（新增）")
    def _nodespecs():
        ns = a.list_node_specs(ws)
        assert_true(ns, "node_specs 为空")
        return f"{len(ns)} 个 node_spec"

    _nodespecs()

    @check("工作空间/项目列表", "qzcli workspaces")
    def _wslist():
        wss = a.list_workspaces(cookie)
        assert_true(wss, "工作空间列表为空")
        assert_true(any(w.get("id") == ws for w in wss), "当前工作空间不在返回里")
        return f"{len(wss)} 个工作空间（走 v1，v2 无权限）"

    _wslist()

    @check("HPC 任务列表", "list_hpc_jobs")
    def _hpc():
        r = a.list_hpc_jobs(ws, page_size=5)
        assert_true("jobs" in r and "total" in r, f"响应字段异常: {sorted(r)}")
        return f"total={r['total']}"

    _hpc()

    # ---------------- CLI 默认形态 ----------------
    # **这一段是补测试方法论漏洞加的。**
    # 之前所有用例都在 API 层、且都显式指定单个 workspace，于是完美避开了
    # 「不带 -w 时并发扫全部工作空间」这条默认路径 —— 而那才是用户实际敲的命令。
    # 真实后果：`qzcli avail` 并发放大撞上 APISIX 限流全线 429，我一次都没测出来。
    # 所以这里**跑真命令、用默认参数**，不再走 API 层捷径。
    print("\n【CLI 默认形态 — 不带 -w，扫全部工作空间】")

    def run_cli(*args, timeout=900):
        proc = subprocess.run(
            ["qzcli", *args], capture_output=True, text=True, timeout=timeout
        )
        return proc.returncode, proc.stdout + proc.stderr

    def cooldown(seconds=20):
        """全量扫描类命令之间的节流。

        **为什么需要**：这一段每条用例都是"不带 -w、扫全部工作空间"，单条就是几十到
        上百个请求。背靠背连跑 5 条，累计 QPS 会超出平台配额而撞 429 —— 但那是本
        脚本自己造出来的负载，真实用户不会在两分钟内连着跑 avail + usage +
        hpc-usage + list --all-ws + res -u。

        实测依据（别删这段，否则下次又会有人把 cooldown 当成"掩盖问题"删掉）：
        单独连跑 `qzcli list -c --all-ws` 三次，429 出现 **0 / 0 / 0** 次；
        同一条命令放在本段末尾（前面已跑完 avail×3 和 usage 全量）则稳定 429。

        每条用例要验的是"**这一条命令**的扇出会不会撞限流"，累积负载由
        `_cli_repeat`（同一命令连跑 3 次）单独覆盖。
        """
        time.sleep(seconds)

    @check("avail 默认形态", "qzcli avail")
    def _cli_avail():
        rc, out = run_cli("avail")
        # rc 必须断言。以前只查输出关键词，命令 exit 1 但输出恰好没有 "429"
        # 就会算通过 —— 等于这条用例可以静默失效。
        assert_true(rc == 0, f"命令退出码 {rc}（非 0）：{out[-300:]}")
        assert_true("429" not in out, "撞上限流 429 —— 并发放大没控制住")
        assert_true(
            "AccessForbidden" not in out,
            "已禁用/无权限的工作空间没被跳过，噪声会盖住真问题",
        )
        assert_true("查询完成" in out, f"没有任何工作空间查询成功: {out[:200]}")
        done = out.count("查询完成")
        return f"{done} 个工作空间查询完成，无 429、无权限噪声"

    _cli_avail()

    @check("usage 默认形态", "qzcli usage")
    def _cli_usage():
        rc, out = run_cli("usage")
        assert_true(rc == 0, f"命令退出码 {rc}（非 0）：{out[-300:]}")
        assert_true("429" not in out, "撞上限流 429")
        assert_true("AccessForbidden" not in out, "权限噪声未清理")
        return "无 429、无权限噪声"

    _cli_usage()

    cooldown()

    @check("连续调用不触发限流", "qzcli avail ×3")
    def _cli_repeat():
        """限流是**累积**的：单次跑通不代表连续跑通。
        agent 场景下同一命令会被反复调用。"""
        for i in range(3):
            rc, out = run_cli("avail")
            assert_true(rc == 0, f"第 {i+1} 次退出码 {rc}：{out[-200:]}")
            assert_true("429" not in out, f"第 {i+1} 次就撞上 429")
        return "连跑 3 次无 429"

    _cli_repeat()

    # ---- exec：能不能在真实开发机上执行命令
    #
    # **这条是补测试方法论漏洞加的。** 上一轮把 exec 的 Jupyter 地址迁到 v2
    # (notebook GetNotebookAccessUrl) 时，我给它配的是把真机响应抄成常量的单测 ——
    # 结果把真实 Jupyter token 提交进了仓库（token 就写在那条 URL 里）。
    #
    # 正确做法是**动态发现**：现找一台在跑的开发机，直接 exec。凭据全程不落盘，
    # 而且因为是动态的也不会过期。
    #
    # 而且单测只能证明「URL 能解析成三个字段」，证明不了「这个地址真能连上、
    # 命令真能执行」。那一半只有真机能验。
    @check("exec 在真实开发机上执行命令", "qzcli exec")
    def _exec_real():
        import uuid as _uuid

        display_stub = _QuietDisplay()

        from qzcli.cli import _exec_via_jupyter, _find_notebook_jupyter_info

        # 挑目标：优先用户日常在用的那台，它最能代表真实使用
        candidates = []
        if args.exec_notebook:
            candidates = [(args.exec_notebook, "命令行指定")]
        else:
            preferred, others = [], []
            for wid, wsinfo in load_all_resources().items():
                try:
                    r = a.list_notebooks_with_cookie(wid, cookie, page_size=100)
                except QzAPIError:
                    continue
                for n in r.get("list") or []:
                    if n.get("status") != "RUNNING":
                        continue
                    nid = n.get("notebook_id") or n.get("id") or ""
                    name = str(n.get("name") or "")
                    if not nid:
                        continue
                    if "CI-情境智能" == wsinfo.get("name") and name.startswith(
                        "mova-base"
                    ):
                        preferred.append((nid, name))
                    else:
                        others.append((nid, name))
            candidates = preferred + others
        assert_true(candidates, "所有工作空间里都没有 RUNNING 的开发机")

        marker = f"QZSMOKE_{_uuid.uuid4().hex[:10]}"
        failures = []
        # 逐个试到跑通为止。开发机可能个别不响应（终端起不来 / 负载高），
        # 那是单台的问题，不代表 exec 这条路坏了 —— 所以要扫，不能试一台就下结论。
        for nid, name in candidates[:5]:
            try:
                info = _find_notebook_jupyter_info(nid, display_stub)
                if not info:
                    failures.append(f"{name}: 拿不到 Jupyter 地址")
                    continue
                # 不变量：地址要能解析出 exec 需要的三个键
                for key in ("base_url", "token", "notebook_id"):
                    assert_true(info.get(key), f"{name}: 解析结果缺 {key}")
                # 返回的是 (exit_code, output) 元组 —— 别当字符串用
                exit_code, output = _exec_via_jupyter(
                    info, f"echo {marker}", display_stub, timeout=90
                )
                if exit_code == 0 and marker in (output or ""):
                    # 只报名字和 id 前 8 位 —— 输出会被贴进 PR 和飞书，
                    # 绝不能带 token 或完整 access url
                    return f"{name}（{nid[:8]}…）执行成功，回显匹配"
                failures.append(f"{name}: exit={exit_code}，回显未匹配")
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{name}: {type(exc).__name__}: {str(exc)[:60]}")
            finally:
                display_stub.lines.clear()
        raise AssertionError(
            f"试了 {len(candidates[:5])} 台开发机都没跑通：" + "; ".join(failures)
        )

    _exec_real()

    cooldown()

    @check("hpc-usage 默认形态", "qzcli hpc-usage")
    def _cli_hpc_usage():
        """串行遍历全部工作空间。并发度是 1，但请求**总量**照样能撞限流 ——
        限流看的是累计 QPS，不是并发度。"""
        rc, out = run_cli("hpc-usage")
        assert_true(rc == 0, f"命令退出码 {rc}（非 0）：{out[-300:]}")
        assert_true("429" not in out, "撞上限流 429")
        assert_true("AccessForbidden" not in out, "权限噪声未清理")
        return "无 429、无权限噪声"

    _cli_hpc_usage()

    cooldown()

    @check("list 全工作空间", "qzcli list -c --all-ws")
    def _cli_list_all():
        rc, out = run_cli("list", "-c", "--all-ws")
        assert_true(rc == 0, f"命令退出码 {rc}（非 0）：{out[-300:]}")
        assert_true("429" not in out, "撞上限流 429")
        return "无 429"

    _cli_list_all()

    cooldown()

    @check("res -u 全量刷新", "qzcli res -u")
    def _cli_res_update():
        """**8 线程扇出，此前零覆盖，429 风险最高的一条。**

        注意它会重写本地 resources.json —— 这是正常的缓存刷新，不动平台侧数据，
        但确实有本地副作用，所以放在最后跑。
        """
        rc, out = run_cli("res", "-u", timeout=1800)
        assert_true(rc == 0, f"命令退出码 {rc}（非 0）：{out[-300:]}")
        assert_true("429" not in out, "8 线程扇出撞上限流 429")
        assert_true("AccessForbidden" not in out, "权限噪声未清理")
        return "8 线程扇出无 429"

    _cli_res_update()

    # ---------------- 写操作 ----------------
    if args.submit:
        print("\n【写操作 — 真实提交】")

        @check("提交训练任务", "qzcli create")
        def _create():
            specs = state.get("specs") or []
            assert_true(specs, "没有可用 spec")
            spec = None
            if args.spec:
                spec = next((s for s in specs if s["id"] == args.spec), None)
                assert_true(spec, f"指定的 spec {args.spec} 不在可用列表里")
            else:
                # 挑 GPU 数最小的，别占大机器
                spec = min(specs, key=lambda s: (s["gpu_count"] or 99))
            lcg = (
                args.compute_group or (spec.get("logic_compute_group_ids") or [None])[0]
            )
            assert_true(lcg, "拿不到计算组")
            proj = (get_workspace_resources(ws) or {}).get("projects") or {}
            project_id = next(iter(proj), None)
            assert_true(project_id, "拿不到 project_id")

            payload = {
                "name": f"qzcli-v2-smoke-{int(time.time())}",
                "logic_compute_group_id": lcg,
                "project_id": project_id,
                "workspace_id": ws,
                "framework": "pytorch",
                "command": "echo QZCLI_V2_SMOKE_OK && sleep 5",
                # ⚠️ **1 才是低优，10 是最高优。** 实测提交值→平台档位：
                # 1→存储 11→LOW、4→存储 20→NORMAL。训练任务和 HPC 同向，
                # 数字小 = 低优。这里以前写的是 10 并注释成"低优，不抢生产资源" ——
                # 方向反了，等于冒烟测试一直在提最高优的任务和生产抢卡。
                "task_priority": 1,
                "auto_fault_tolerance": False,
                "framework_config": [
                    {
                        "cpu": spec["cpu_count"],
                        "gpu_count": spec["gpu_count"],
                        "mem_gi": spec["memory_size_gib"],
                        "resource_spec_price": qzapi.build_resource_spec_price(
                            {
                                "cpu_count": spec["cpu_count"],
                                "gpu_count": spec["gpu_count"],
                                "memory_gb": spec["memory_size_gib"],
                                "gpu_type": spec["gpu_type"],
                                "id": spec["id"],
                            },
                            lcg,
                        ),
                        "image": args.image,
                        "image_type": "SOURCE_PRIVATE",
                        "instance_count": 1,
                        "shm_gi": max(64, int(spec["memory_size_gib"] * 0.5)),
                    }
                ],
            }
            r = a.create_job_v2(cookie, payload)
            jid = r.get("job_id")
            assert_true(jid, f"响应里没有 job_id: {r}")
            state["new_job"] = jid
            state["new_job_name"] = payload["name"]
            return f"job_id={jid} spec={spec['gpu_count']}卡 优先级=10(低优)"

        _create()

        if state.get("new_job"):

            @check("新任务能被查到", "qzcli status <new>")
            def _newdetail():
                time.sleep(3)
                d = a._get_job_detail_v2(state["new_job"], cookie)
                assert_true(
                    d.get("name") == state["new_job_name"],
                    f"查回来的 name 不对: {d.get('name')}",
                )
                return f"status={d.get('status')}"

            _newdetail()

            @check("停止任务", "qzcli stop")
            def _stop():
                ok = a.stop_job_with_cookie(state["new_job"], cookie)
                assert_true(ok, "stop 返回 False")
                time.sleep(5)
                d = a._get_job_detail_v2(state["new_job"], cookie)
                return f"已发停止指令，当前 status={d.get('status')}"

            _stop()

        # ---- 低优大规模任务能否进入排队
        #
        # 这条是照**用户真实提交场景**加的：72 节点的 MoVA2 训练，低优，投到
        # 目标计算组只有个位数低优空位的分区。它一定排不上，验的就是"能不能正确
        # 进入排队"——即整条提交链路（spec 解析、卡型、优先级、节点数）是通的。
        #
        # **为什么必须验卡型**：规格是工作空间级的，同一个 spec id 在别的计算组
        # 缓存/跑过时，卡型会被抄错（实测给 H200 组填了 H100）。带着错卡型提交，
        # 任务会一直排队等一种该组里不存在的卡 —— 看起来"成功进入排队"，实际永远
        # 起不来，正好骗过这条用例的验收。所以下面把卡型和该组节点的实际卡型对了。
        #
        # **账号门控**：这条会真的提交一个 72 节点任务，只在明确授权的账号下跑。
        # 在 ~/.qzcli/.env 里设 QZCLI_SMOKE_QUEUE_ACCOUNT=<你的用户名> 才启用，
        # 未设置就跳过（不算失败）——避免别人跑本脚本时莫名其妙提交大任务。
        allowed_account = (
            os.environ.get("QZCLI_SMOKE_QUEUE_ACCOUNT")
            or (load_env_file() or {}).get("QZCLI_SMOKE_QUEUE_ACCOUNT")
            or ""
        ).strip()
        current_account = (get_credentials()[0] or "").strip()

        if not allowed_account:
            print("  ⊘ 低优排队（未设 QZCLI_SMOKE_QUEUE_ACCOUNT，跳过）")
        elif current_account != allowed_account:
            print(f"  ⊘ 低优排队（当前账号 {current_account[:4]}*** 非授权账号，跳过）")
        else:

            @check("低优大任务能进排队", "qzcli create --priority 1")
            def _lowpri_queue():
                lcg = args.queue_compute_group
                assert_true(lcg, "需要 --queue-compute-group 指定目标计算组")

                specs = a.list_specs(lcg, ws)
                cand = [x for x in specs if (x.get("gpu_count") or 0) == 8]
                assert_true(cand, f"计算组 {lcg} 下没有 8 卡规格")
                spec = max(cand, key=lambda x: x.get("cpu_count") or 0)

                # 卡型必须和该计算组节点的实际卡型一致，否则会排一辈子队
                node_gpu = a._compute_group_gpu_type(ws, lcg)
                assert_true(node_gpu, "拿不到该计算组节点的卡型")
                assert_true(
                    spec.get("gpu_type") == node_gpu,
                    f"规格卡型 {spec.get('gpu_type')!r} 与该组节点实际卡型 "
                    f"{node_gpu!r} 不符 —— 这样提交会永远排队等一种不存在的卡",
                )

                proj = (get_workspace_resources(ws) or {}).get("projects") or {}
                project_id = next(iter(proj), None)
                assert_true(project_id, "拿不到 project_id")

                payload = {
                    "name": f"qzcli-lowpri-queue-smoke-{int(time.time())}",
                    "logic_compute_group_id": lcg,
                    "project_id": project_id,
                    "workspace_id": ws,
                    "framework": "pytorch",
                    "command": "echo QZCLI_LOWPRI_QUEUE_SMOKE && sleep 5",
                    "task_priority": 1,  # 1 = LOW，见上面那段注释
                    "auto_fault_tolerance": False,
                    "framework_config": [
                        {
                            "cpu": spec["cpu_count"],
                            "gpu_count": spec["gpu_count"],
                            "mem_gi": spec.get("memory_size_gib") or 0,
                            "resource_spec_price": {
                                "cpu_type": "",
                                "cpu_count": spec["cpu_count"],
                                "gpu_type": spec["gpu_type"],
                                "gpu_count": spec["gpu_count"],
                                "memory_size_gib": spec.get("memory_size_gib") or 0,
                                "logic_compute_group_id": lcg,
                                "quota_id": spec["id"],
                            },
                            "image": args.image,
                            "image_type": "SOURCE_PUBLIC",
                            "instance_count": args.queue_instances,
                            "shm_gi": 64,
                        }
                    ],
                }
                job_id = a.create_job_v2(cookie, payload)
                assert_true(job_id, "创建未返回 job_id")
                state["lowpri_job"] = job_id

                time.sleep(8)
                d = a._get_job_detail_v2(job_id, cookie)
                status = d.get("status")
                assert_true(
                    status == "job_queuing",
                    f"期望 job_queuing，实际 {status}（可能被参数问题拒了）",
                )
                assert_true(
                    d.get("priority_level") == "LOW",
                    f"优先级档位是 {d.get('priority_level')!r}，不是 LOW —— "
                    "提成高优会抢生产资源",
                )
                return (
                    f"{args.queue_instances} 节点已排队，档位 LOW，" f"卡型 {node_gpu}"
                )

            _lowpri_queue()

            @check("排队任务能停掉", "qzcli stop")
            def _lowpri_stop():
                job_id = state.get("lowpri_job")
                assert_true(job_id, "上一步没拿到 job_id")
                assert_true(a.stop_job_with_cookie(job_id, cookie), "stop 返回 False")
                time.sleep(6)
                d = a._get_job_detail_v2(job_id, cookie)
                assert_true(
                    d.get("status") == "job_stopped",
                    f"停止后状态是 {d.get('status')}，未回到 job_stopped",
                )
                return "已停止，未占用资源"

            _lowpri_stop()

        # ---- HPC：这条路**没有**迁 v2，仍走 /api/v1/hpc_jobs。
        # 正因为没迁才更要测：确认 v1 这条腿在本次改动后依然是通的。
        @check("提交 HPC 任务", "qzcli hpc（仍走 v1）")
        def _hpc_create():
            # 规格从历史 HPC 任务反推，不写死 —— 换工作空间也能跑
            hist = a.list_hpc_jobs(ws, page_size=20).get("jobs") or []
            sample = next(
                (
                    j
                    for j in hist
                    if (j.get("slurm_cluster_spec") or {}).get("predef_quota_id")
                ),
                None,
            )
            assert_true(sample, "历史 HPC 任务里找不到可复用的规格")
            sp = sample["slurm_cluster_spec"]
            state["hpc_lcg"] = sample["logic_compute_group_id"]
            r = a.create_hpc_job(
                cookie=cookie,
                job_name=f"qzcli-v2-smoke-hpc-{int(time.time())}",
                workspace_id=ws,
                project_id=sample["project_id"],
                logic_compute_group_id=sample["logic_compute_group_id"],
                entrypoint="echo QZCLI_V2_SMOKE_HPC_OK",
                image=sp["image"],
                predef_quota_id=sp["predef_quota_id"],
                cpu=sp["cpu"],
                mem_gi=sp["mem_gi"],
                instances=1,
                image_type=sp.get("image_type", "SOURCE_PRIVATE"),
                # 兜底：万一 entrypoint 没退出，20 分钟后平台自己收
                max_running_time_minutes=20,
            )
            jid = r.get("job_id")
            assert_true(jid, f"响应里没有 job_id: {r}")
            state["hpc_job"] = jid
            return f"job_id={jid} cpu={sp['cpu']} mem={sp['mem_gi']}Gi（v1 路径）"

        _hpc_create()

        if state.get("hpc_job"):

            @check("停止 HPC 任务", "hpc StopJob（v2）")
            def _hpc_stop():
                # qzcli 目前没有 HPC 停止命令，直接打 v2 Action 收尾，
                # 顺便验证它可用 —— 这也是补 `qzcli hpc-stop` 的前置。
                a._request_v2("hpc", "StopJob", {"job_id": state["hpc_job"]})
                time.sleep(5)
                got = a.list_hpc_jobs(ws, page_size=50).get("jobs") or []
                me = next((j for j in got if j.get("job_id") == state["hpc_job"]), None)
                return f"status={me.get('status') if me else '（列表里暂未刷新）'}"

            _hpc_stop()
    else:
        print("\n【写操作】跳过（加 --submit 才会真实提交）")

    # ---------------- 汇总 ----------------
    print("\n" + "=" * 68)
    ok = sum(1 for r in RESULTS if r["ok"])
    for r in RESULTS:
        print(f"  {'✓' if r['ok'] else '✗'}  {r['point']:24} {r['cmd']:28} {r['sec']}s")
    print(f"\n{ok}/{len(RESULTS)} 通过")
    failed = [r for r in RESULTS if not r["ok"]]
    if failed:
        print("\n失败项：")
        for r in failed:
            print(f"  - {r['point']}: {r['detail']}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
