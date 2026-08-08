# qzcli v1 → v2 端点映射（**真机实测**，2026-07-30）

qz spec 版本 `cf22fdbe`。每一行都用 `qzcli` 的 CAS cookie 实际打过，不是照 schema 推的。
探针全量结果见 `v2_probe_report.md`，接口全量文档见 `api_spec_v2.md`。

## 结论先行

1. **cookie 在整个 v2 面上通用** —— 109 个只读 Action 探下来，**0 个因认证被挡**。迁移不受认证阻塞。
2. **`cluster.*` 系列对普通用户是 `AccessForbidden`，必须走 `workspace.*` 双胞胎。**
   这是最容易踩的坑：按 `qz spec` 的字面描述，`cluster.ListNodeDimension` 和
   `workspace.ListNodeDimension` 描述几乎一样，但前者是集群管理员权限，后者才是工作空间级。
   qzcli 是工作空间级工具，**一律用 `workspace.*`**。
3. ~~`project.ListProjects` 对当前账号是 `AccessForbidden`~~ —— **2026-08 上游已放开**，
   并改名为 `project GetProjectForPage`。它还顺带修掉一个 bug：v1 会把用户**已退出 /
   已结束**的项目也返回（`is_member=False`），选中就报「您已离开所选项目」。

## 映射表

| # | qzcli 现有端点 | v2 Action（实测可用） | 请求体 | 响应 |
|---|---|---|---|---|
| 1 | `POST /api/v1/train_job/list` | `train ListJobs` | `{workspace_id, page_num, page_size}` | `Result.jobs[]` + `Result.total` |
| 2 | `POST /api/v1/train_job/detail` | `train GetJob` | `{job_id}` | `Result.{job 字段平铺}` |
| 3 | `POST /api/v1/train_job/events/list` | `train ListJobEvents` ✅ 已迁 | `{filter:{object_type, object_ids:[id]}, page_num, page_size}` | `Result.events[]` + `Result.total` |
| 4 | `POST /api/v1/train_job/stop` | `train StopJob` | `{job_id}` | — （写操作，探针未打） |
| 5 | `POST /api/v1/notebook/list` | `notebook ListNotebooks` | `{workspace_id, page, page_size}` ⚠️ 是 `page` 不是 `page_num` | `Result.list[]` + `Result.total` |
| 6 | `POST /api/v1/cluster_metric/cluster_basic_info` | **`workspace GetBasicInfo`** | `{workspace_id}` | `Result.{clusters, compute_groups, resource_types}` |
| 7 | `POST /api/v1/cluster_metric/list_node_dimension` | **`workspace ListNodeDimension`** | `{filter:{workspace_id}, page_num, page_size}` | `Result.node_dimensions[]` + `total` |
| 8 | `POST /api/v1/cluster_metric/list_task_dimension` | **`workspace ListTaskDimension`** | `{filter:{workspace_id}, page_num, page_size}` | `Result.task_dimensions[]` + `total` |
| 9 | `POST /api/v1/cluster_metric/overview_task_metric` | **`workspace GetOverviewTaskMetric`** | `{filter:{workspace_id}, time_range:{start_timestamp, end_timestamp, interval_second}}` | `Result.task_groups` |
| 10 | `POST /openapi/v1/specs/list`（**已 404**） | **`workspace GetLogicComputeGroupNodeSpecs`** | `{workspace_id, logic_compute_group_id}` | `Result.node_specs[]` ⚠️ **无 `spec_id`** |
| 11 | `POST /api/v1/hpc_jobs/list` | `hpc ListJobs` | `{workspace_id, page_num, page_size}` | `Result.jobs[]` + `total` |
| 12 | `POST /api/v1/project/list` | `project GetProjectForPage` ✅ 已迁 | `{page, page_size}` | `Result.items[]` + `Result.total` |
| 13 | `GET /api/v1/notebook/lab/{id}` | `notebook GetNotebookAccessUrl` ✅ 已迁 | `{notebook_id}` | `Result.{jupyter_url, vscode_url}` |
| 14 | `POST /api/v2/train?Action=CreateJobConsole` | 已是 v2，**不动** | — | 已真机验证（commit `0a9902a`） |
| 15 | `POST /api/v2/train?Action=GetJobLog` | 已是 v2，**不动** | — | — |

### 关于第 3 行（任务调度事件）

`qzcli events` 来自 PR #39，在 PR #40 之后合入 master，所以第一轮迁移时它还不存在
（当时接口盘点读的是检出的 PR #39 分支，与开发基线 `master` 不一致，一度被错标成
"已迁 v2"）。#39 合入后已按 `train ListJobEvents` 迁完并真机验证。

### ⚠️ AccessForbidden 一律不回落 v1

`_v2_then_v1` **只在路由不通**（404/405/50x、响应非 JSON）时回落。权限类错误
直接抛，别加回去 —— 唯一见过的 `AccessForbidden` 实例是 `该空间已被禁用`，
那是 **v2 判断正确**，反倒 v1 会给非成员返回已禁用空间的陈旧集群结构。
回落等于用错误答案盖掉正确答案。

禁用空间的正解在源头：`list_workspaces` 按 `usage_status != 0` 滤掉
（`project/list` 会把你不是成员的项目也返回，其 `space_list` 里可能挂着这类空间）。

## 实测踩到的坑

**`GetOverviewTaskMetric` 的 `time_range` 是秒级，不是毫秒。**
传毫秒会报 `InternalError: 查询时间区间不能超过1个月`（因为毫秒数被当成秒解释后跨度巨大）。
区间本身也硬限制 ≤ 1 个月。

**`ListLogicComputeGroups` 返回的 id 字段叫 `logic_compute_group_id`，不是 `id`。**
拿错字段会导致 `GetLogicComputeGroupNodeSpecs` 报
`InternalError: 已选择的计算类型组不存在，请重新选择。`

**`inference-serving` 整个 service 在 `/api/v2/inference-serving` 上没有路由**
—— 10 个只读 Action 全部返回 `404 page not found`（text/plain）。
注意区分：这是路由没挂，不是认证失败。qzcli 目前不用这个 service，不影响。

## 平台侧缺口（给数字部）

| 缺口 | 说明 | 影响 |
|---|---|---|
| **无 action 返回 `spec_id`** | `spec_id` 在 v2 里只作请求字段存在；`node_specs[]` 只有硬件规格没有 id。唯一来源是历史任务的 `framework_config[].predef_id` | `qzcli create` 无法自助发现可用规格 |
| **无 `ListWorkspaces`** | workspace 枚举只能从 `project.ListProjects` 的 `items[].space_list[]` 推导，而该 action 普通用户 `AccessForbidden` | qzcli 只能继续用 v1 `/api/v1/project/list` |
| **notebook 无 lab/proxy URL** | 全量 grep 过 notebook schema，只有 `extra_info.ProxyJump`，拿不到 Jupyter 访问地址 | `qzcli exec` 无法迁 v2 |
| **`qz audit` / `qz file` 零 action** | spec 里注册为 service 但没有任何 action | 安全审计、文件挂载无 v2 入口 |
| **`inference-serving` 路由 404** | spec 声明 18 个 action，网关上打不通 | 推理服务无法通过 v2 管理 |
