# 启智平台 v2 接口测试报告：CLI spec 与实际接口面的差距

- 报告日期：2026-08-09
- 测试账号：普通用户（非 workspace admin）
- 测试通道：qzcli 的 v2 客户端（`POST /api/v2/{service}?Action={Action}`，cookie 鉴权）
- 触发起因：验证一条磁盘挂载相关的接口 `audit?Action=ApplySecurityAudit`，
  发现它**根本不在 qz CLI 的 spec 里**，遂做了完整比对

---

## 一、结论摘要

1. **`qz` CLI 的 spec 只覆盖了平台 v2 接口面的一部分。** spec 声明 11 个服务，
   而 Web 控制台实际在用 **21 个**；有 **12 个服务 spec 里完全没有**，
   包括 `file`、`audit`、`storage`、`billing`、`sandbox*` 这些日常功能。
2. **单个服务的覆盖也可能极不完整。** `project` 服务 spec 里只有 1 个 action
   （`GetProjectForPage`），Web 控制台在用 **32 个**。
3. **这直接导致外部工具作出错误判断。** 我们据 spec 得出过两个结论——
   「v2 拿不到点券/预算数据」「平台无 `ListWorkspaces`」——**两个都是错的**，
   相应能力在平台上一直有，只是没进 spec。这两条错误结论已随 qzcli v0.4.7
   发布，需要单独勘误。
4. spec 只定义**请求参数**，没有响应体定义。客户端无法据此判断某字段是否应被
   填充，也做不了兼容性校验。

---

## 二、测试方法

两路取证，交叉验证，不靠单一来源下结论：

1. **静态**：抓取 Web 控制台全部前端产物（316 个 JS chunk，11.4 MB），
   正则提取写死的 `/api/v2/{service}?Action={Action}` 调用点。
   这是**下界**——动态拼接的调用没被计入。
2. **动态**：用 qzcli 的 v2 客户端逐个真实调用，以服务端返回区分
   「路由不存在」（`InvalidAction: unknown action`）、
   「参数不对」（`InvalidParameter`）、「权限不足」（`AccessForbidden`）、
   「成功」四种情形。

---

## 三、服务级覆盖对照

| 服务 | spec 声明 | 前端在用 | 说明 |
|---|---:|---:|---|
| `audit` | 0 | 6 | **spec 没有** |
| `billing` | 0 | 3 | **spec 没有** |
| `file` | 0 | 9 | **spec 没有** |
| `job` | 0 | 1 | **spec 没有** |
| `operate-log` | 0 | 1 | **spec 没有** |
| `resource-price` | 0 | 6 | **spec 没有** |
| `sandbox` | 0 | 4 | **spec 没有** |
| `sandbox-api-key` | 0 | 3 | **spec 没有** |
| `sandbox-pool` | 0 | 1 | **spec 没有** |
| `sandbox-template` | 0 | 6 | **spec 没有** |
| `serving` | 0 | 1 | **spec 没有** |
| `storage` | 0 | 10 | **spec 没有** |
| `project` | **1** | **32** | 覆盖率 3% |
| `user` | 3 | 16 | |
| `model-hub` | 13 | 16 | |
| `cluster` | 27 | 7 | |
| `notebook` | 21 | 2 | |
| `train` | 19 | 10 | |
| `ray` | 14 | 6 | |
| `image` | 4 | 1 | |
| `workspace` | 31 | 28 | |
| `hpc` | 13 | 0 | spec 独有（CLI 侧在用） |
| `inference-serving` | 23 | 0 | spec 独有 |
| **合计** | **169** | **169** | 总数巧合相同，集合差异巨大 |

> 前端列是下界。`cluster` / `notebook` 等"前端少于 spec"的行，多半是前端走了
> `workspace.*` 的同名双胞胎，或调用是动态拼的，不代表这些 action 不存在。

---

## 四、因 spec 不全而作出的错误结论（已发布，需勘误）

### 4.1 「v2 拿不到点券 / 预算数据」——错

依据：spec 全文搜 `budget` / `billing` **零命中**；`workspace` 下 10 个 quota
接口返回的是资源配额（`cpu_count` / `gpu_count`，单位是卡和核，不是钱）。

实测推翻：

```
POST /api/v2/project?Action=GetProjectBudgetUsageOverview
  {"project_id": "project-7e0957fb-…", "workspace_id": "ws-8207e9e2-…"}
→ {"total":"10,103,676.00", "used":"7,825,893.97", "remain":"2,277,781.34",
   "train":"7,032,724.92", "inference":"793,169.06", "storage":"0.00"}

POST /api/v2/project?Action=GetProjectMemberBudgetUsage
→ 每个成员的 budget / remain_budget / used_budget
```

平台提供的粒度**比 v1 更细**（v1 只有单个 `remain_budget` 数）。

### 4.2 「平台无 `ListWorkspaces`，只能从项目列表推导工作空间」——错

实测 `POST /api/v2/workspace?Action=ListWorkspaces` 带 `{page_num, page_size}`
直接返回完整 `items`（含 `compute_groups`、`creator` 等）。

---

## 五、`audit` 服务实测（6 个 action，spec 全无）

前端产物里提取到的完整清单：

```
ApplySecurityAudit      CancelSecurityShare     CreateSubmitAudit
GetAuditDetail          GetAuditList            GetSecurityShareList
```

| Action | 实测结果 |
|---|---|
| `GetAuditList` | ✅ 可用，返回申请列表（含 creator、created_at 等） |
| `GetSecurityShareList` | ✅ 可用，返回 `{items, total}` |
| `ApplySecurityAudit` | ✅ 路由与鉴权均正常，业务校验见下 |

### 5.1 `ApplySecurityAudit` 的参数语义（文档缺失，实测推出）

请求体：

```json
{"acceptor_type":"project", "share_type":"mount",
 "acceptor_id":"<接收方项目的 en_name>",
 "file_path":"/inspire/ssd/project/<源项目 en_name>/…",
 "description":"…", "workspace_id":"ws-…"}
```

**`acceptor_id` 要的是项目的 `en_name`（如 `video-generation`），不是
`project-<uuid>`。** 字段名叫 `..._id` 但接收的是 slug，这是最容易踩的一处：

| 传入 | 返回 |
|---|---|
| `project-7e0957fb-…`（真实 uuid） | `InvalidParameter: 项目不存在` |
| `video-generation`（en_name） | 通过 acceptor 校验，进入下一道 |

建议：要么改名为 `acceptor_name`，要么同时接受 uuid。

### 5.2 权限规则（实测确认）

`file_path` 归属的项目决定权限——**调用者必须是源项目的成员**：

| 源项目（由 file_path 推导） | 调用者是否成员 | 返回 |
|---|---|---|
| `exploration-topic` | 否 | `AccessForbidden: user is not share project member` |
| `video-generation` | 是 | 通过，报错推进到 `InvalidParameter: invalid file path format` |

规则本身合理。但错误文案 `user is not share project member` 里的
"share project" 指的是**源**项目，容易被理解成接收方项目，建议明确成
「您不是待共享目录所属项目 `<name>` 的成员」。

---

## 五之补、关于"用 curl 绕过文件路径校验"这个建议

平台侧在群里给出的建议是：Web 端对文件路径的校验有问题（已记录待修），
**可以先用 curl 直接打 `ApplySecurityAudit` 绕过**。

实测这条路对本账号**走不通，但卡点不是路径校验**：

```
POST /api/v2/audit?Action=ApplySecurityAudit
  {"acceptor_type":"project","share_type":"mount","acceptor_id":"test-regression",
   "file_path":"/inspire/ssd/project/exploration-topic/public/yrmou/data-mostar-all",
   "workspace_id":"ws-6e6ba362-…"}
→ AccessForbidden: user is not share project member
```

原因是 `file_path` 落在 `exploration-topic`（某工作空间-探索课题）下，
而本账号对该项目**没有任何权限**。三路独立证据：

| 证据 | 结果 |
|---|---|
| `GetProjectForPage` 返回的 `is_member` | `false`（11 条里唯一一条） |
| `GetProjectMemberList`（探索课题） | `AccessForbidden: user has no project permission` |
| `GetProjectMemberList`（音视频生成，对照组） | ✅ 正常返回成员列表 |

把 `acceptor_id` 换成合法的 en_name（`video-generation`）后**报错不变**——
说明源项目成员校验发生在 acceptor 校验之前，acceptor 传什么都不影响。
反过来把源换成本账号有权限的 `video-generation`，该校验立刻通过，
错误推进到下一道（`invalid file path format`）。

**所以要让这条申请走通，需要的是把账号加进 `exploration-topic` 项目，
或者由平台侧放开"非成员共享"，而不是绕过路径校验。**

关于"需要 cookie session"：**已解决**。本报告全部调用都是用真实登录态
（qzcli 的 cookie 管理，CAS 登录后落盘复用）打的，不需要额外准备。

---

## 六、未能打通的接口（需要贵方补充）

| 接口 | 结论 |
|---|---|
| `file?Action=GetSystemStorageTypeList` | ✅ **已解决**。正确请求体是 `{filter: {workspace_id}}`（要嵌一层 `filter`）。返回 `system_storages`：`hdd`(primary) / `ssd` / `qb-ilm` 等 |
| `project?Action=ListMountProjects` | ❌ **仍不通，且已确认是权限门槛而非参数问题** |

`ListMountProjects` 试过 4 种参数形状 —— `{filter:{workspace_id}}`、
`{workspace_id, file_path}`、`{file_path}`、`{project_name}` ——
**返回完全相同的 `AccessForbidden: Access denied`**。参数形状不同而错误一字不差，
说明校验发生在参数解析之前，是权限拦截。

`file?Action=CheckPermission` 用 `{file_path}` 可调通，但对我们测试的路径返回
全空字段（`file_path:""`、`is_dir:false`），疑似还需要存储上下文参数。

想请教：

1. `ListMountProjects` 需要什么权限？（项目 owner/maintainer？workspace admin？）
2. 完整的挂载链路是否为
   `file CheckPermission` → `audit ApplySecurityAudit` → 审批 →
   `file DirectoryFileMount`？`CreateSubmitAudit` 在这条链上是什么位置？
3. v1 的 `/api/v1/audit/security/apply` 与 v2 的 `audit?Action=ApplySecurityAudit`
   请求体字段完全相同、两条并存。后续以哪条为准？v1 会下线吗？

---

## 六之补、只读 action 穷举探活（205 个）

用 `tools/scan_v2_surface.py` 对全部**只读** action 逐个发一次最小请求
（写操作 103 个一律跳过），按服务端返回分类：

| 分类 | 数量 | 含义 |
|---|---:|---|
| 需参数（`InvalidParameter`） | 78 | **路由存在** |
| 可用 | 51 | 空请求体即返回业务数据 |
| 权限（`AccessForbidden`） | 45 | **路由存在** |
| 网关 404 | 15 | 见下 |
| `InternalError` | 9 | |
| `ResourceNotFound` | 7 | |
| **`unknown action`（真不存在）** | **0** | |

**205 个只读 action，没有一个是真不存在的。** 这把「spec 是子集」从服务级
坐实到了 action 级。

> 说明：第一轮扫描曾报 `project GetProjectListV` 不存在，那是**我们提取正则的
> bug** —— 真实名字是 `GetProjectListV2`，`[A-Za-z]+` 把结尾的 `2` 截掉了。
> 已修正并重跑，此处数字是修正后的。

### `inference-serving` 整个服务在网关上打不通

15 个网关 404 里有 **14 个来自 `inference-serving`**，即被探到的**每一个**
只读 action 都是 404：

```
GetServing            ListServings           GetServingLog
ListServingInstances  ListServingVersions    ListServingEvents
GetTaskMetric         GetTaskMetricBatch     GetServingApiMetric
GetServingApiMetricBatch   GetServingConfigByWorkspaceId
GetInferenceServingTerms   GetInferenceServingUserProjectList
GetLastSuccessInferenceServingInfo
```

而 spec 里这个服务声明了 **23 个 action**。**不是部分不通，是整个服务没接上
网关。** 建议要么接上路由，要么从 spec 里摘掉 —— 现状是 spec 承诺了一整套
外部工具用不了的能力。

剩下 1 个 404 之外的异常是 `billing GetProjectBillingDetail` 读超时（60s），
与本报告测试期间 `qzcli logs` 的超时现象同类。

---

## 七、其他实测到的 spec / 行为不符

1. **`GetProjectForPage` 的描述与行为不符。** spec 写
   *"List projects the current user belongs to"*，实测返回的 11 条里有 1 条
   `is_member=false`（状态 `PASS_MODIFY_RESOURCE`）。是有意包含待审项目，
   还是 bug？依赖这句描述做过滤的客户端会拿到不该出现的项目。
2. **v1 的 `is_member` 恒为 `false`。** 同一账号 12 个项目全是 `false`，
   包括明显是成员的。v2 该字段正常。若 v1 尚未下线，建议同步修掉。
3. **spec 无响应体定义。** 每个 action 只有 `parameters`，没有返回字段说明。
   后果是客户端无法判断「字段值为空」是正常还是异常——例如
   `GetProjectForPage` 的 `remain_budget` 键存在但恒为空字符串 `''`，
   我们无从判断这是设计如此还是数据未填。

---

## 八、建议

1. **spec 生成流程接入全部服务**，至少补齐 `file` / `audit` / `storage` /
   `billing` / `sandbox*` 这 12 个，以及 `project` 缺的 31 个 action。
   外部工具依赖 spec 判断能力边界，缺失会直接转化为错误结论——
   本报告第四节就是两个现成的例子。
2. **spec 增加响应体定义**，哪怕只有字段名和类型。
3. **`acceptor_id` 命名与取值对齐**（收 slug 就叫 `acceptor_name`，或兼容 uuid）。
4. **修正 `GetProjectForPage` 的描述**，或修正其行为。
5. 权限类错误文案里指明「哪个项目」，减少排查成本。

---

## 附：复现方式

本报告全部结论可用 qzcli 的 v2 客户端复现：

```python
from qzcli.api import get_api
from qzcli.config import get_cookie

a = get_api()
ck = (get_cookie() or {}).get("cookie")
a._request_v2("audit", "GetAuditList",
              {"workspace_id": "ws-…", "page": 1, "page_size": 10},
              cookie=ck, referer_path="/jobs/files?spaceId=ws-…", raw=True)
```

服务端对未知 action 会明确返回 `InvalidAction: unknown action: <X>`，
可据此枚举验证任一 action 是否存在。
