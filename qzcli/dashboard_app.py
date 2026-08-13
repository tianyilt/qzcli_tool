#!/usr/bin/env python3
"""qzcli GPU 使用「成分下钻」看板（Streamlit）。

由 ``qzcli dashboard`` 子命令通过 ``streamlit run`` 拉起。主视图是 plotly
treemap/sunburst：块面积 = GPU 数，默认自顶向下按
``计算组 → 优先级档 → 项目 → 用户 → 任务`` 逐层下钻——点一块 native 放大、
点中心面包屑退回，一眼看出「各计算组里谁占最多、各自高低优」。

数据全部复用 qzcli 的 API 客户端与数据层纯函数（``qzcli.cli``），
无独立平台请求逻辑。
"""

import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import plotly.express as px
import streamlit as st

from qzcli import cli, fragmentation
from qzcli.api import QzAPIError, get_api
from qzcli.diag import swallowed
from qzcli.config import (
    find_workspace_by_name,
    get_cookie,
    get_workspace_resources,
    load_all_resources,
)

# 可作为下钻层级 / 筛选的维度列（顺序即默认下钻顺序）
# 「节点」= 物理节点(host)，可下钻 计算组→节点→任务 看每个节点摞了谁。
HIERARCHY_DIMS = [
    "计算组",
    "节点",
    "GPU类型",
    "集群",
    "优先级档",
    "任务类型",
    "项目",
    "用户",
]
DEFAULT_HIERARCHY = ["计算组", "优先级档", "项目", "用户"]
COLOR_DIMS = [
    "优先级档",
    "整节点/碎卡",
    "任务类型",
    "计算组",
    "GPU利用率(空占卡预警)",
    "运行时长(越久越红)",
]
FILTER_DIMS = ["计算组", "节点分类", "优先级档", "任务类型", "项目", "用户", "状态"]
LEAF = "任务名"

# 连续配色维度 -> (数据列, 色阶, 固定区间 or None, 中点 or None)
CONTINUOUS_COLOR = {
    "GPU利用率(空占卡预警)": ("GPU利用率", "RdYlGn", [0, 100], 50),
    "运行时长(越久越红)": ("运行时长h", "OrRd", None, None),
}


def _resolve_workspace(ws_input):
    """把「分布式」这类名字解析成 (workspace_id, 显示名)。"""
    if ws_input and ws_input.startswith("ws-"):
        res = get_workspace_resources(ws_input) or {}
        return ws_input, res.get("name", ws_input)
    ws_id = find_workspace_by_name(ws_input) if ws_input else None
    if not ws_id:
        return None, ws_input
    res = get_workspace_resources(ws_id) or {}
    return ws_id, res.get("name", ws_input)


FREE_LABEL = "空闲"  # 灰块：未占用的 GPU 容量
FREE_COLOR = "#d9d9d9"

# 固定优先级档配色，避免叠加灰块后 px 重排调色板（高优蓝/中优红/低优绿/空闲灰）
PRIORITY_COLORS = {
    "高优(≥6)": "#636EFA",
    "中优(4-5)": "#EF553B",
    "低优(≤3)": "#00CC96",
    FREE_LABEL: FREE_COLOR,
}

# 「整节点/碎卡」配色维度 → 数据列 别名 + 固定调色板（碎卡=红最扎眼）
CARDING_COLOR_DIM = "整节点/碎卡"
CARDING_COL = "节点分类"
CARDING_COLORS = {
    "碎卡": "#EF553B",  # 红：治理重点
    "空整节点": "#00CC96",  # 绿：可直接排
    "低优满占": "#F2C200",  # 黄：可整节点抢占
    "高优满占": "#636EFA",  # 蓝：不可回收
    "多节点混合": "#AB63FA",  # 紫：跨节点
    "调度禁用": "#7f7f7f",  # 深灰：SchedulingDisabled/cordon，不可用
    "排队/未分配": "#c8c8c8",
    FREE_LABEL: FREE_COLOR,
}


@st.cache_data(ttl=300, show_spinner="拉取任务与计算组映射中…")
def load_df(ws_id, ws_name):
    """拉取在跑任务 + 计算组映射。

    返回 ``(df, free_by_lcg)``：df 是每个在跑任务一行；free_by_lcg 是
    ``{计算组: 空闲GPU数}``（= 节点容量总和 − 已用），用于叠加灰块。
    """
    api = get_api()
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        raise QzAPIError("未设置 cookie，请先运行: qzcli login")
    cookie = cookie_data["cookie"]
    node_map = cli.build_node_to_lcg_map(api, ws_id, cookie)
    tasks = cli.fetch_all_task_dimensions(api, ws_id, cookie)
    rows = [cli.task_dimension_to_row(t, node_map, ws_id) for t in tasks]

    # 给每个任务标「节点分类」= 它所在物理节点的整节点/碎卡状态（零额外拉数，
    # 复用碎卡聚合的 per-node class）。这样主 treemap 可直接按碎卡配色/下钻，
    # 把"成分"和"碎卡治理"接到同一张图。
    frag = fragmentation.compute_node_fragmentation(node_map, rows)
    node_class = {n["node"]: n["class"] for n in frag["nodes"]}
    for r in rows:
        hosts = [h for h in str(r.get("节点", "") or "").split(",") if h]
        classes = {node_class.get(h) for h in hosts if node_class.get(h)}
        if not classes:
            r["节点分类"] = "排队/未分配"
        elif len(classes) == 1:
            r["节点分类"] = next(iter(classes))
        else:
            r["节点分类"] = "多节点混合"

    # 每个计算组的空闲 GPU（按节点容量 − 已用聚合，去重到节点粒度）
    cap, used = {}, {}
    for info in node_map.values():
        lcg = info["lcg"]
        cap[lcg] = cap.get(lcg, 0) + info.get("gpu_total", 0)
        used[lcg] = used.get(lcg, 0) + info.get("gpu_used", 0)
    free_by_lcg = {lcg: max(cap[lcg] - used.get(lcg, 0), 0) for lcg in cap}
    return pd.DataFrame(rows), free_by_lcg


def _frag_one(ws_id):
    """单个 workspace 的节点碎卡聚合(复用与 load_df 相同的数据源)。无 streamlit,
    可在线程里调。"""
    api = get_api()
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        raise QzAPIError("未设置 cookie，请先运行: qzcli login")
    cookie = cookie_data["cookie"]
    node_map = cli.build_node_to_lcg_map(api, ws_id, cookie)
    tasks = cli.fetch_all_task_dimensions(api, ws_id, cookie)
    rows = [cli.task_dimension_to_row(t, node_map, ws_id) for t in tasks]
    res = fragmentation.compute_node_fragmentation(node_map, rows)
    # 只保留「我的碎片分布」需要的最小行,避免 payload 膨胀
    res["rows"] = [
        {
            "用户": r.get("用户", ""),
            "节点": r.get("节点", ""),
            "GPU": r.get("GPU", 0),
            "优先级": r.get("优先级", 10),
        }
        for r in rows
    ]
    return res


def load_all_frag(ws_items):
    """跨所有 workspace 并行拉取 + 合并。ws_items = ((ws_id, ws_name), ...)(元组以便缓存)。
    返回 (all_lcg, all_nodes, all_rows),每条带 workspace 标签。"""
    all_lcg, all_nodes, all_rows = [], [], []

    def work(item):
        wid, wname = item
        try:
            return wname, _frag_one(wid)
        except Exception:
            return wname, None

    # 扇出前串行确认鉴权状态（不发请求）。看板是长驻轮询进程，cookie 迟早会过期；
    # 少了这步，过期那一刻 4×4=16 个线程会同时去登录，CAS 按失败次数延长锁定。
    # 2026-08-12 账号被锁时，这个看板正在以这种方式轮询。
    try:
        get_api().ensure_authenticated()
    except Exception as exc:  # noqa: BLE001 —— 看板不该因此白屏，但要说清原因
        st.error(f"鉴权不可用，已跳过本次刷新：{exc}")
        return [], [], []

    # 外层 workspace 并发。与 build_node_to_lcg_map 的内层 LCG 并发相乘 = 峰值并发,
    # 故压到 4：4×4=16 并发,削平对启智的读取峰值 QPS(实测 76→~16)。
    with ThreadPoolExecutor(max_workers=4) as ex:
        for wname, fr in ex.map(work, ws_items):
            if not fr:
                continue
            for a in fr["by_lcg"].values():
                r = dict(a)
                r["workspace"] = wname
                all_lcg.append(r)
            for nd in fr["nodes"]:
                d = dict(nd)
                d["workspace"] = wname
                all_nodes.append(d)
            for r in fr["rows"]:
                r2 = dict(r)
                r2["workspace"] = wname
                all_rows.append(r2)
    return all_lcg, all_nodes, all_rows


# 猜测「我」是谁(用于「我的碎片分布」默认选中)
_ME_HINTS = ("梁天一", "liangtianyi", "253208120278")


def _guess_me(users):
    for u in users:
        if any(h in str(u) for h in _ME_HINTS):
            return u
    return users[0] if users else None


def render_fragmentation(ws_options):
    """跨所有 workspace 的「整节点 / 碎卡治理」视图。"""
    st.markdown(
        "整节点 = **空整节点**(可直接排) + **低优满占**(可整节点抢占)；"
        "碎卡 = 散在混合节点上的**空卡** + **可回收低优卡**，凑不成整节点。"
        "跨所有 workspace 聚合。"
    )
    all_lcg, all_nodes, all_rows = load_all_frag(tuple(ws_options))

    if not all_lcg:
        st.info(
            "没拉到节点数据。先在终端 `qzcli login` + `qzcli res -u`，再点上方「🔄 刷新数据」。"
        )
        return

    sdf = pd.DataFrame(all_lcg)
    sdf = sdf[sdf["total_nodes"] > 0]

    # 兼容旧 payload:sched_disabled 字段可能缺(远程/旧缓存)
    for col in ("sched_disabled_nodes", "sched_disabled_free_gpus"):
        if col not in sdf.columns:
            sdf[col] = 0

    k = st.columns(6)
    k[0].metric("空整节点", int(sdf["empty_whole"].sum()))
    k[1].metric("低优满占整节点", int(sdf["lowpri_whole"].sum()))
    k[2].metric(
        "碎片低优卡 🔧",
        int(sdf["frag_lowpri_cards"].sum()),
        help="散在高低优混合节点上的低优卡，可抢占但凑不成整节点——治理重点；"
        "这正是 `qzcli avail --lp` 的「低优空余=0」看不到、但看板里能看到的那部分。",
    )
    k[3].metric("碎片空卡", int(sdf["frag_free_cards"].sum()))
    k[4].metric(
        "可凑整节点潜力",
        int(sdf["whole_node_potential"].sum()),
        help="(空卡 + 碎片低优卡) // 每节点卡数，理想整合后能腾出的整节点数。"
        "已剔除调度禁用节点。",
    )
    k[5].metric(
        "调度禁用 ⛔",
        int(sdf["sched_disabled_nodes"].sum()),
        help=f"SchedulingDisabled / cordon 节点（machine-migration / software-fault 等），"
        f"共 {int(sdf['sched_disabled_free_gpus'].sum())} 张空卡看着可用实则调度不上去，"
        "已从上面所有「可用/可凑」量里剔除。",
    )

    st.subheader("① 全集群 · 各计算组：整节点 vs 碎卡")
    cols = [
        "workspace",
        "lcg",
        "gpu_type",
        "total_nodes",
        "empty_whole",
        "lowpri_whole",
        "frag_free_cards",
        "frag_lowpri_cards",
        "sched_disabled_nodes",
        "whole_node_potential",
        "util_pct",
    ]
    ren = {
        "workspace": "工作空间",
        "lcg": "计算组",
        "gpu_type": "GPU类型",
        "total_nodes": "总节点",
        "empty_whole": "空整节点",
        "lowpri_whole": "低优满占整节点",
        "frag_free_cards": "碎片空卡",
        "frag_lowpri_cards": "碎片低优卡",
        "sched_disabled_nodes": "调度禁用节点",
        "whole_node_potential": "可凑整节点潜力",
        "util_pct": "利用率%",
    }
    view1 = (
        sdf[cols]
        .rename(columns=ren)
        .sort_values(["碎片低优卡", "可凑整节点潜力"], ascending=False)
    )
    st.dataframe(view1, use_container_width=True, hide_index=True)

    # ---- 桥:选一个要提交的计算组 → 一键拿它的碎卡节点排除参数 ----
    st.markdown(
        "**🔧 提交时避开碎卡** — 选你要提交作业的计算组，把它"
        "**有空卡的碎卡节点**排掉，逼调度器用整节点/空节点(避免作业被塞进碎片):"
    )
    frag_all = {"nodes": all_nodes}
    lcg_opts = sorted(
        {
            n["lcg"]
            for n in all_nodes
            if n.get("class") == fragmentation.FRAGMENTED and n.get("lcg")
        }
    )
    if lcg_opts:
        pick = st.selectbox("计算组", lcg_opts, key="exclude_lcg")
        names = fragmentation.fragmented_node_names(frag_all, lcg=pick, only_free=True)
        if names:
            st.caption(
                f"{pick}:{len(names)} 个有空卡的碎卡节点 → 粘到 `qzcli create` 后面"
            )
            st.code(fragmentation.format_exclude_args(names), language="bash")
        else:
            st.success(f"{pick} 没有「有空卡的碎卡节点」，无需排除 👍")
    else:
        st.caption("当前没有可排除的碎卡节点。")

    st.subheader("② 我的碎片分布(我的卡散在哪些节点)")
    users = sorted({r["用户"] for r in all_rows if r.get("用户")})
    if not users:
        st.info("当前无在跑任务。")
        return
    mc1, mc2 = st.columns([3, 1])
    me_default = _guess_me(users)
    with mc1:
        me = st.selectbox(
            "用户", users, index=users.index(me_default) if me_default in users else 0
        )
    with mc2:
        only_frag = st.checkbox("只看碎卡节点", value=True)
    my_by_node = defaultdict(int)
    for r in all_rows:
        if r.get("用户") != me:
            continue
        nodes = [x for x in str(r.get("节点", "") or "").split(",") if x]
        g = int(r.get("GPU", 0) or 0)
        if not nodes or g <= 0:
            continue
        per = g // len(nodes) if len(nodes) > 1 else g
        for x in nodes:
            my_by_node[x] += per
    rec = {d["node"]: d for d in all_nodes}
    myrows = []
    for node, mycards in sorted(my_by_node.items(), key=lambda kv: -kv[1]):
        d = rec.get(node, {})
        myrows.append(
            {
                "节点": node,
                "工作空间": d.get("workspace", ""),
                "计算组": d.get("lcg", ""),
                "总卡": d.get("total", 0),
                "已用": d.get("used", 0),
                "空闲": d.get("free", 0),
                "我的卡": mycards,
                "节点低优卡": d.get("low_pri", 0),
                "分类": d.get("class", ""),
            }
        )
    if not myrows:
        st.info(f"{me} 当前无占卡任务。")
        return
    frag_cnt = sum(1 for r in myrows if r["分类"] == fragmentation.FRAGMENTED)
    st.caption(
        f"**{me}** 的卡分散在 **{len(myrows)}** 个节点，其中 **{frag_cnt}** 个是碎卡节点"
        "(整节点没占满 / 高低优混合)——把这些迁走或凑整能少占节点。"
    )
    shown = (
        [r for r in myrows if r["分类"] == fragmentation.FRAGMENTED]
        if only_frag
        else myrows
    )
    if not shown:
        st.success("我没有碎卡节点 👍(卡都落在整节点上)。取消勾选可看全部。")
        return
    st.dataframe(pd.DataFrame(shown), use_container_width=True, hide_index=True)


def _free_rows(free_by_lcg, keep_lcgs):
    """把 {计算组: 空闲GPU} 变成灰块行（每个非零计算组一行）。"""
    out = []
    for lcg, free in free_by_lcg.items():
        if free <= 0 or (keep_lcgs is not None and lcg not in keep_lcgs):
            continue
        out.append(
            {
                "任务名": f"{FREE_LABEL} {int(free)} GPU",
                "用户": FREE_LABEL,
                "项目": FREE_LABEL,
                "任务类型": FREE_LABEL,
                "优先级": -1,
                "优先级档": FREE_LABEL,
                "计算组": lcg,
                "GPU类型": FREE_LABEL,
                "集群": FREE_LABEL,
                "GPU": int(free),
                "GPU利用率": 0.0,
                "状态": FREE_LABEL,
                "节点数": 0,
                "节点": "",
                "节点分类": FREE_LABEL,
                "运行时长h": 0.0,
                "创建时间": "",
                "job_url": "",
                "id": "",
            }
        )
    return out


def _node_customdata(fig, gdf, path_cols, root):
    """为 treemap 每个节点（含内部节点）算聚合明细。

    px 只能把 `values`(GPU) 沿树求和，其它列在内部节点会变 NaN/(?)；这里按节点
    的完整路径匹配 gdf 叶子行，自己算：
    [任务数, GPU 加权平均利用率, 最长运行h, 类型, 占用节点数(去重 host)]。
    """
    sep = "/"
    fp = gdf[path_cols].astype(str).agg(sep.join, axis=1)
    gpu = gdf["GPU"].to_numpy()
    util = gdf["GPU利用率"].to_numpy()
    rt = gdf["运行时长h"].to_numpy()
    cnt = gdf["任务数"].to_numpy()
    types = gdf["任务类型"].astype(str).to_numpy()
    node_cells = gdf["节点"].astype(str).to_numpy() if "节点" in gdf else None
    out = []
    for nid in fig.data[0].ids:
        if nid == root:
            mask = slice(None)
        else:
            rel = nid[len(root) + 1 :] if nid.startswith(root + sep) else nid
            mask = (fp.values == rel) | fp.str.startswith(rel + sep).values
        g = gpu[mask]
        tot = float(g.sum())
        wutil = float((util[mask] * g).sum() / tot) if tot > 0 else 0.0
        mrt = float(rt[mask].max()) if len(g) else 0.0
        uniq = set(types[mask].tolist())
        tstr = next(iter(uniq)) if len(uniq) == 1 else f"{len(uniq)}类混合"
        # 去重的 host 名 → 该子树实际占用的物理节点数（共享节点只算一次）
        nnodes = 0
        if node_cells is not None:
            hosts = set()
            for cell in node_cells[mask]:
                for h in cell.split(","):
                    if h:
                        hosts.add(h)
            nnodes = len(hosts)
        out.append([int(cnt[mask].sum()), wutil, mrt, tstr, nnodes])
    return out


def _apply_filters(df, selections, search):
    out = df
    for dim, chosen in selections.items():
        if chosen:
            out = out[out[dim].isin(chosen)]
    if search:
        mask = out["任务名"].str.contains(search, case=False, na=False) | out[
            "节点"
        ].str.contains(search, case=False, na=False)
        out = out[mask]
    return out


def main():
    st.set_page_config(page_title="qzcli GPU 看板", layout="wide")

    default_ws = os.environ.get("QZCLI_DASHBOARD_WS", "分布式")

    # 本地缓存的工作空间列表（qzcli res -u 维护），做成下拉框
    resources = load_all_resources() or {}
    ws_options = sorted(
        ((wid, (d.get("name") or wid)) for wid, d in resources.items()),
        key=lambda x: x[1],
    )
    ids = [wid for wid, _ in ws_options]
    names = {wid: nm for wid, nm in ws_options}

    # 把默认值（可能是名称或 ws- ID）解析成 ID
    default_id = (
        default_ws if default_ws in names else find_workspace_by_name(default_ws)
    )

    # ---- 标题行：workspace 下拉框 + 刷新 ----
    head_l, head_r = st.columns([4, 1])
    with head_l:
        if ids:
            idx = ids.index(default_id) if default_id in ids else 0
            ws_id = st.selectbox(
                "工作空间",
                ids,
                index=idx,
                format_func=lambda i: names.get(i, i),
            )
            ws_name = names.get(ws_id, ws_id)
        else:
            # 缓存为空时回退到手输
            ws_input = st.text_input("工作空间（名称或 ws- ID）", value=default_ws)
            ws_id, ws_name = _resolve_workspace(ws_input)
    with head_r:
        st.write("")
        st.write("")
        if st.button("🔄 刷新数据", use_container_width=True):
            load_df.clear()

    if not ws_id:
        st.error("未找到可用工作空间。先在终端跑 `qzcli res -u` 刷新缓存，再重试。")
        st.stop()

    st.title(f"🧩 {ws_name} · GPU 占用成分")

    try:
        df, free_by_lcg = load_df(ws_id, ws_name)
    except QzAPIError as e:
        st.error(f"{e}\n\n请在终端运行 `qzcli login` 后点上方「🔄 刷新数据」。")
        st.stop()

    render_capacity_banner(ws_id, ws_name)
    st.divider()

    tab_comp, tab_frag = st.tabs(["📊 成分下钻（treemap）", "🧱 整节点 / 碎卡治理"])
    with tab_comp:
        if df.empty:
            st.info("当前没有在跑任务。")
        else:
            render_composition(df, free_by_lcg, ws_name)
    with tab_frag:
        render_fragmentation(ws_options)


def render_capacity_banner(ws_id, ws_name):
    """顶部横幅：**现在能起几个整节点**。

    这是提交任务前唯一真正要回答的问题 —— 不知道这个数就不知道 ``--instances``
    该填几。以前这个数存在，但埋在「整节点/碎卡」那张 13 行 × 8 列的宽表里，
    要自己从「空整节点 / 可凑整节点潜力」两列里挑，而且两者含义不同容易高估。

    口径**只算空整节点**（完全空闲、现在提就能起），不含「低优满占、可抢占」的
    节点 —— 那些要你提高优先级去挤才拿得到，算进来会让人提了起不来的任务。
    """
    try:
        res = _frag_one(ws_id)
    except QzAPIError as exc:
        st.warning(f"容量信息暂不可用：{exc}")
        return
    except Exception as exc:  # noqa: BLE001 —— 横幅坏了不该让整个看板打不开
        swallowed("看板/容量横幅", exc)
        return

    by_lcg = (res or {}).get("by_lcg") or {}
    usable = []
    for lcg, v in by_lcg.items():
        whole = int(v.get("empty_whole") or 0)
        if whole > 0:
            # 空整节点对应的卡数 = 节点数 × 每节点卡数（node_size）。
            # 别用 frag_free_cards —— 那是**碎卡节点**上零散的空卡，不是整节点的，
            # 混用会把「能起几个整节点」算歪。实测 70 节点 × 8 = 560 卡，
            # 与 `qzcli avail` 报的空 GPU 数吻合。
            size = int(v.get("node_size") or 0)
            usable.append((str(v.get("lcg") or lcg), whole, whole * size))
    usable.sort(key=lambda x: -x[1])

    st.markdown("#### 🚀 现在能起几个整节点")
    if not usable:
        st.error("**0 个** —— 当前工作空间没有完全空闲的节点，提交会排队等待。")
        st.caption(
            "（口径：只算完全空闲的整节点；被低优任务占着的节点需要抢占，未计入）"
        )
        return

    cols = st.columns(min(len(usable), 4))
    for col, (name, whole, cards) in zip(cols, usable[:4]):
        col.metric(
            label=name[:18],
            value=f"{whole} 节点",
            help=f"计算组 {name}：{whole} 个完全空闲的整节点"
            + (f"，约 {cards} 张空卡" if cards else ""),
        )
    top_name, top_whole, _ = usable[0]
    st.caption(
        f"建议：提交到 **{top_name}**，`--instances` 最多填 **{top_whole}**。"
        f"（口径：只算完全空闲整节点；被低优占用、需抢占的节点未计入）"
    )
    if len(usable) > 4:
        st.caption(
            "其余可用计算组：" + "、".join(f"{n}({w})" for n, w, _ in usable[4:])
        )


def render_composition(df, free_by_lcg, ws_name):
    """原「成分下钻」treemap 视图(单一 workspace)。"""
    # ---- Sidebar 筛选 ----
    st.sidebar.header("筛选")
    selections = {}
    for dim in FILTER_DIMS:
        opts = sorted(x for x in df[dim].dropna().unique().tolist() if x != "")
        if opts:
            selections[dim] = st.sidebar.multiselect(dim, opts, default=[])
    search = st.sidebar.text_input("🔍 任务名 / 节点关键字")

    fdf = _apply_filters(df, selections, search)

    # ---- KPI ----
    total_free = int(sum(free_by_lcg.values()))
    total_gpu = int(fdf["GPU"].sum())
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("运行任务数", f"{len(fdf):,}")
    k2.metric("已占用 GPU", f"{total_gpu:,}")
    k3.metric("空闲 GPU", f"{total_free:,}")
    k4.metric("计算组数", fdf[fdf["计算组"] != "排队/未分配"]["计算组"].nunique())

    # ---- 按任务类型占用（看交互式建模/训练/推理各占多少）----
    type_gpu = fdf.groupby("任务类型")["GPU"].sum().sort_values(ascending=False)
    if total_gpu > 0:
        parts = []
        for i, (t, g) in enumerate(type_gpu.items()):
            seg = f"{t} {int(g)} GPU（{g / total_gpu * 100:.0f}%）"
            parts.append(f"**🔹 {seg}**" if i == 0 else seg)
        st.markdown("**按类型占用：** " + " ・ ".join(parts))

    # ---- 控制条 ----
    c1, c2, c3 = st.columns([1, 2, 1.4])
    with c1:
        view = st.radio("视图", ["Treemap", "Sunburst", "Icicle"], horizontal=False)
    with c2:
        hierarchy = st.multiselect(
            "下钻层级顺序（从外到内）", HIERARCHY_DIMS, default=DEFAULT_HIERARCHY
        )
    with c3:
        color_dim = st.selectbox("配色维度", COLOR_DIMS, index=0)
        show_free = st.checkbox("叠加空闲 GPU（灰块）", value=False)

    # 叠加空闲灰块（只对当前筛选到的计算组补），追加到绘图数据
    pdf = fdf
    if show_free:
        keep = set(fdf["计算组"].unique()) if selections.get("计算组") else None
        free_rows = _free_rows(free_by_lcg, keep)
        if free_rows:
            pdf = pd.concat([fdf, pd.DataFrame(free_rows)], ignore_index=True)

    if not hierarchy:
        st.warning("请至少选择一个下钻层级。")
        return

    if fdf.empty:
        st.warning("当前筛选下没有任务。")
        return

    # ---- 预聚合到叶子粒度 ----
    # 同名 clone 任务若不先聚合，px 会把它们并成一个叶子且逐行数值字段丢成 NaN；
    # 这里按完整路径分组，显式指定聚合方式（GPU 求和、利用率取均值、运行时长取最长），
    # 保证叶子悬停有干净数值，并额外带上「任务数」。
    # 同时剔除 0 GPU 的行：treemap 无面积，且会让 px 连续配色的加权平均除零报错。
    plot_df = pdf[pdf["GPU"] > 0]
    if plot_df.empty:
        st.warning("当前筛选下没有占用 GPU 的任务。")
        return
    cont = CONTINUOUS_COLOR.get(color_dim)
    # 「整节点/碎卡」配色映射到实际数据列「节点分类」
    color_col = (
        cont[0]
        if cont
        else (CARDING_COL if color_dim == CARDING_COLOR_DIM else color_dim)
    )
    path_cols = hierarchy + [LEAF]
    agg_map = {"GPU": ("GPU", "sum"), "任务数": (LEAF, "size")}
    agg_extra = [("GPU利用率", "mean"), ("运行时长h", "max"), ("任务类型", "first")]
    if not cont:  # 分类配色列也要能取到（可能不在下钻层级里）
        agg_extra.append((color_col, "first"))
    for col, fn in agg_extra:
        if col not in path_cols and col not in agg_map:
            agg_map[col] = (col, fn)
    # 把节点名 join 进来（悬停按去重 host 名算「占用节点数」，共享节点不重复计）
    if "节点" in plot_df.columns and "节点" not in path_cols:
        agg_map["节点"] = ("节点", lambda s: ",".join(x for x in s.astype(str) if x))
    gdf = plot_df.groupby(path_cols, as_index=False, sort=False).agg(**agg_map)

    # ---- 主视图：treemap / sunburst / icicle ----
    plot_kwargs = dict(
        path=[px.Constant(ws_name)] + path_cols,
        values="GPU",
        color=color_col,
    )
    if cont:
        _col, scale, rng, mid = cont
        plot_kwargs["color_continuous_scale"] = scale
        if rng is None and color_col in gdf:
            # 无固定区间（如运行时长）：按 95 分位截断，避免个别超长任务把色阶拉平
            hi = float(gdf[color_col].quantile(0.95)) or float(gdf[color_col].max())
            rng = [0, hi] if hi and hi > 0 else None
        if rng is not None:
            plot_kwargs["range_color"] = rng
        if mid is not None:
            plot_kwargs["color_continuous_midpoint"] = mid
    else:
        # 分类配色：优先级档 / 整节点碎卡 用固定调色板；其它维度只固定「空闲」为灰
        if color_dim == "优先级档":
            plot_kwargs["color_discrete_map"] = PRIORITY_COLORS
        elif color_dim == CARDING_COLOR_DIM:
            plot_kwargs["color_discrete_map"] = CARDING_COLORS
        else:
            plot_kwargs["color_discrete_map"] = {FREE_LABEL: FREE_COLOR}
    chart_fn = {"Treemap": px.treemap, "Sunburst": px.sunburst, "Icicle": px.icicle}[
        view
    ]
    fig = chart_fn(gdf, **plot_kwargs)

    # 为每个节点（含父级）计算干净的聚合明细，覆盖 customdata
    # （px 对内部节点的非 values 列无法聚合，直接用会显示 NaN/(?)）
    fig.data[0].customdata = _node_customdata(fig, gdf, path_cols, ws_name)
    fig.update_traces(
        root_color="lightgrey",
        hovertemplate=(
            "<b>%{label}</b><br>"
            "GPU=%{value}（占上层 %{percentParent:.0%}）｜占用节点=%{customdata[4]}<br>"
            "任务数=%{customdata[0]}｜类型=%{customdata[3]}<br>"
            "平均利用率=%{customdata[1]:.0f}%｜最长运行=%{customdata[2]:.1f}h"
            "<extra></extra>"
        ),
    )
    fig.update_layout(margin=dict(t=30, l=0, r=0, b=0), height=680)
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "块面积 = GPU 数；点一块放大、点中心面包屑退回。"
        "「计算组」= 机房级 logic_compute_group（经节点反查）；"
        "「排队/未分配」= 尚未占用节点的任务；"
        "勾选「叠加空闲 GPU」后，各计算组还剩多少卡以灰块显示。"
    )

    # ---- 叶子明细（次要，兜底看原始行 / 跳启智）----
    with st.expander(
        f"▸ 当前筛选下任务明细（{len(fdf)} 条 / {int(fdf['GPU'].sum())} GPU）"
    ):
        show_cols = [
            "任务名",
            "用户",
            "项目",
            "计算组",
            "任务类型",
            "优先级档",
            "GPU",
            "GPU利用率",
            "状态",
            "节点数",
            "运行时长h",
            "创建时间",
            "job_url",
        ]
        st.dataframe(
            fdf[show_cols].sort_values("GPU", ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                "job_url": st.column_config.LinkColumn("详情", display_text="🔗 打开"),
                "GPU利用率": st.column_config.NumberColumn("GPU利用率%", format="%.0f"),
            },
        )
        st.download_button(
            "⬇️ 导出当前筛选 CSV",
            fdf[show_cols].to_csv(index=False).encode("utf-8-sig"),
            file_name=f"{ws_name}_gpu_usage.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
