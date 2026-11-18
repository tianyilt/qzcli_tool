"""``resources.json`` 的各种残缺态 —— qzcli 最高价值的一组测试输入。

## 为什么是这些状态

线上踩到的 bug 里有一大类根因相同：**代码把本地缓存当成事实的全集**。而缓存的默认
状态本来就是残缺的：

- ``res -u`` 走快速模式，**明确不产出 specs** —— 所以 ``specs={}`` 不是异常，
  是**默认稳态**。对着本机真实缓存数过：**16 个工作空间里 15 个没有 specs**
- 新建 / 新加入的计算组、项目不在缓存里，但平台上确实存在
- **3 个**工作空间只有计算组没有项目

因此判据不能是"缓存里有没有"，而应该是"平台上有没有"。这个模块把各种残缺形态固化
下来，让每个入口命令都能被参数化地打一遍。

## 结构以真实缓存为准

下面的形状是**照着本机 ``~/.qzcli/resources.json`` 抄的**，不是照文档推的。几个容易
猜错的点：

- 顶层是 ``{ws_id: workspace}`` **扁平映射**，没有 ``workspaces`` 这层包装
- ``compute_groups`` / ``projects`` / ``specs`` 都是**按 id 索引的 dict**，不是 list
- 工作空间 id 前缀是 ``ws-``（不是 ``lws-``），计算组是 ``lcg-``，项目是 ``project-``
- spec 的 key 是**裸 uuid**（没有 ``spec-`` 前缀），且同时有单数
  ``logic_compute_group_id`` 和复数 ``logic_compute_group_ids`` 两个字段

## 断言口径（重要）

**不要断言 ``return 1``。** 线上那批 bug 全都是"报了个错，但错误内容是假的" ——
返回码是对的，内容是错的。正确口径是：

    要么成功，要么给出**正确且可执行**的错误。

比如缓存里没有某个计算组时，"计算组不属于当前工作空间"是**假错误**（它属于，只是
缓存旧了）；"未找到该计算组，请 ``qzcli res -u`` 刷新"才是真错误。
"""

# 测试里反复用到的固定 id。取成可读的假值，方便断言失败时一眼看出是哪个。
WS_ID = "ws-11111111-1111-1111-1111-111111111111"
WS_NAME = "分布式-测试空间"
LCG_ID = "lcg-22222222-2222-2222-2222-222222222222"
LCG_NAME = "示例计算组"
OTHER_LCG_ID = "lcg-33333333-3333-3333-3333-333333333333"
PROJECT_ID = "project-44444444-4444-4444-4444-444444444444"
PROJECT_NAME = "示例项目"
SPEC_ID = "55555555-5555-5555-5555-555555555555"


def _compute_group(lcg_id=LCG_ID, name=LCG_NAME):
    return {
        "id": lcg_id,
        "name": name,
        "compute_group_id": "cg-" + lcg_id[4:],
        "compute_group_name": name,
        "cluster_id": "cluster-example",
        "workspace_id": WS_ID,
    }


def _project(project_id=PROJECT_ID, name=PROJECT_NAME):
    return {"id": project_id, "name": name, "workspace_id": WS_ID}


def _spec(spec_id=SPEC_ID, lcg_ids=(LCG_ID,), gpu=8, cpu=150, mem=1500):
    spec = {
        "id": spec_id,
        "name": f"{gpu}卡",
        "gpu_count": gpu,
        "cpu_count": cpu,
        "memory_gb": mem,
        "gpu_type": "NVIDIA_H200_SXM_141G",
        "gpu_type_display": "",
    }
    if lcg_ids is not None:
        spec["logic_compute_group_id"] = lcg_ids[0]
        spec["logic_compute_group_ids"] = list(lcg_ids)
    return spec


def _workspace(compute_groups=None, projects=None, specs=None, **extra):
    ws = {
        "id": WS_ID,
        "name": WS_NAME,
        "projects": {PROJECT_ID: _project()} if projects is None else projects,
        "compute_groups": (
            {LCG_ID: _compute_group()} if compute_groups is None else compute_groups
        ),
        "specs": {} if specs is None else specs,
        "updated_at": "2026-07-31T00:00:00",
    }
    ws.update(extra)
    return ws


def healthy():
    """完整缓存 —— 对照组。所有残缺态的断言都该和它比。"""
    return {WS_ID: _workspace(specs={SPEC_ID: _spec()})}


def empty():
    """``{}`` —— 冷启动，文件在但什么都没有。

    注意这和"文件不存在"是**不同**的场景：前者说明 ``res -u`` 跑过但没拿到东西，
    后者说明从没跑过。代码对两者的处理路径可能不同。
    """
    return {}


def no_projects():
    """有计算组、无项目。**本机真实缓存里 3/16 个工作空间就是这样。**"""
    return {WS_ID: _workspace(projects={}, specs={SPEC_ID: _spec()})}


def no_specs():
    """``specs={}`` —— **真实缓存里 15/16 命中，是默认稳态，不是异常。**

    ``res -u`` 快速模式不产出 specs。``create`` 不带 ``--spec`` 时若只读缓存，
    在绝大多数工作空间会直接失败 —— 这正是 v0.4.3 修的那个 bug。
    """
    return {WS_ID: _workspace(specs={})}


def no_compute_groups():
    """有项目、无计算组。

    多工作空间命令（``avail`` 不带 ``-w``）会**静默跳过**整个工作空间 —— 用户看到
    的是某个空间凭空消失，没有任何提示说明为什么。
    """
    return {WS_ID: _workspace(compute_groups={}, specs={})}


def partial_specs_other_lcg():
    """缓存里有规格，但全都属于**别的**计算组。

    比"没有规格"更阴险：代码看到 ``specs`` 非空就以为有得选，实际按计算组筛完是空的。
    """
    return {
        WS_ID: _workspace(
            compute_groups={
                LCG_ID: _compute_group(),
                OTHER_LCG_ID: _compute_group(OTHER_LCG_ID, "另一个计算组"),
            },
            specs={SPEC_ID: _spec(lcg_ids=[OTHER_LCG_ID])},
        )
    }


def spec_without_lcg_ids():
    """规格缺 ``logic_compute_group_ids``。

    此时若缓存里有多个计算组，该规格会被**静默丢弃**，表现为"规格凭空消失"且无任何
    告警 —— 用户看到的是"没有可用规格"，完全无从排查。
    """
    return {
        WS_ID: _workspace(
            compute_groups={
                LCG_ID: _compute_group(),
                OTHER_LCG_ID: _compute_group(OTHER_LCG_ID, "另一个计算组"),
            },
            specs={SPEC_ID: _spec(lcg_ids=None)},
        )
    }


def new_compute_group():
    """目标计算组不在缓存里，但平台上有 —— 用户实际踩到的那个 bug。

    正确行为是向平台复核后放行，而不是断言"不属于当前工作空间"。
    """
    return {WS_ID: _workspace(compute_groups={}, specs={})}


def new_project():
    """目标项目不在缓存里，但平台上有 —— 与 ``new_compute_group`` 同构。

    这两处是同一套逻辑，v0.4.2 只修了计算组那一半，v0.4.3 才补上项目。
    """
    return {WS_ID: _workspace(projects={}, specs={SPEC_ID: _spec()})}


def unavailable_flag():
    """工作空间被标记为不可用（平台侧已禁用）。

    影响多工作空间命令的可见集合：这类空间应当被跳过且**不产生噪声告警**。
    """
    ws = _workspace(specs={SPEC_ID: _spec()})
    ws["unavailable"] = {"reason": "AccessForbidden: 该空间已被禁用"}
    return {WS_ID: ws}


def item_missing_id():
    """计算组条目缺 ``id`` 字段 —— 平台返回不完整时写进来的脏数据。

    直接 ``item["id"]`` 会 ``KeyError``，而这个异常若被宽泛的 except 吞掉，
    表现就是整个工作空间凭空消失。
    """
    return {WS_ID: _workspace(compute_groups={LCG_ID: {"name": LCG_NAME}}, specs={})}


def workspace_not_a_dict():
    """工作空间的值不是 dict —— 老版本格式或手工编辑写坏。

    ``load_all_resources`` 的过滤逻辑里有 ``isinstance(ws, dict)`` 判断，
    这条就是喂给它的反例。
    """
    return {WS_ID: "这不是一个 dict"}


def corrupt_json():
    """非法 JSON —— 写入过程被打断（磁盘满、进程被杀）。

    返回字符串，``sandbox_home`` 会原样写入而不做序列化。
    """
    return '{"ws-11111111-1111-1111-1111-11111'


def missing():
    """文件不存在 —— 全新机器。用 ``None`` 表示，``sandbox_home`` 就不建该文件。"""
    return None


#: 全部状态的注册表，供参数化测试遍历。值是 ``(构造函数, 场景说明)``。
ALL_STATES = {
    "healthy": (healthy, "完整缓存（对照组）"),
    "missing": (missing, "文件不存在 —— 全新机器"),
    "empty": (empty, "{} —— 冷启动"),
    "no-projects": (no_projects, "有计算组无项目 —— 真实缓存 3/16 命中"),
    "no-specs": (no_specs, "specs={} —— 真实缓存 15/16 命中，默认稳态"),
    "no-compute-groups": (no_compute_groups, "无计算组 —— 多空间命令静默跳过"),
    "partial-specs-other-lcg": (partial_specs_other_lcg, "规格全属于别的计算组"),
    "spec-without-lcg-ids": (spec_without_lcg_ids, "规格缺 lcg 归属 —— 会被静默丢弃"),
    "new-compute-group": (new_compute_group, "计算组不在缓存但平台上有"),
    "new-project": (new_project, "项目不在缓存但平台上有"),
    "unavailable-flag": (unavailable_flag, "工作空间已被平台禁用"),
    "item-missing-id": (item_missing_id, "条目缺 id —— KeyError 被吞"),
    "workspace-not-a-dict": (workspace_not_a_dict, "工作空间值不是 dict"),
    "corrupt-json": (corrupt_json, "非法 JSON —— 写入被打断"),
}
