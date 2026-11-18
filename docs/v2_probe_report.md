# v2 接口 cookie 认证探针报告

> 版本说明：本文保留历史 Cookie 接口探针及数字；当前开发分支默认采用 Python 原生官方 OAuth，Cookie 为显式 legacy 后备。认证实现和本轮验收见 [native_oauth.md](native_oauth.md)。

- qz spec 版本：`cf22fdbe`
- 探测 Action 数：**109**（只读 `List*`/`Get*`，写操作全部跳过）
- ✅ **cookie 被接受：99**（返回 JSON = 认证通过）
  - 其中直接拿到数据（`Result`）：**15**
  - 业务/参数/权限错，但认证已过：**84**
- ❌ 认证失败（302 到 Keycloak / 401）：**0**
- ⬜ 网关未注册该路由（404 `page not found`）：**10**

> **判读原则**：这个探针问的是「cookie 认证过不过」，不是「业务成不成功」。
> 只要返回 JSON（哪怕是 `AccessForbidden` / `InvalidParameter`），就说明 cookie 已被接受、该 Action 可迁；
> 只有 302 到 Keycloak 返回 HTML、或 401，才是认证失败。
> 404 `page not found` 是路由压根没挂在 `/api/v2/{service}` 上，与认证无关。

**结论：cookie 在整个 v2 只读面上通用**，没有任何一个 Action 因为认证被挡。v1→v2 迁移不受认证阻塞。

## 明细

| Service | Action | 判定 | HTTP | 响应信封 / 片段 |
|---|---|---|---|---|
| `cluster` | `GetClusterBasicInfo` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `GetClusterOverviewOptions` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `GetConsumeTimeSeries` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `cluster` | `GetNodeDistincts` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{clusters, compute_groups, compute_nets, gpu_types, logic_compute_groups, n` |
| `cluster` | `GetNodeEvents` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `GetNodeResourceInfoDictionary` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{cpu_infos, gpu_infos}` |
| `cluster` | `GetOverviewResourceMetric` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `GetOverviewResourceMetricByTime` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `GetOverviewTaskMetric` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `ListClusterRegions` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `ListClusters` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `ListLogicComputeGroups` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `ListNodeDimension` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `ListNodeEvents` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{events, total}` |
| `cluster` | `ListNodes` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{backup, fault, nodes, online, total}` |
| `cluster` | `ListProjectDimension` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `ListTaskDimension` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `cluster` | `ListUserDimension` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `hpc` | `GetJob` | JSON_ERR:ResourceNotFound | 200 | `{ResponseMetadata}` |
| `hpc` | `GetJobLog` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `hpc` | `GetTaskMetric` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `hpc` | `GetTaskMetricBatch` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `hpc` | `ListJobEvents` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `hpc` | `ListJobInstances` | JSON_ERR:ResourceNotFound | 200 | `{ResponseMetadata}` |
| `hpc` | `ListJobs` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `inference-serving` | `GetServing` | NOT_ROUTED | 404 | `404 page not found` |
| `inference-serving` | `GetServingApiMetric` | NOT_ROUTED | 404 | `404 page not found` |
| `inference-serving` | `GetServingApiMetricBatch` | NOT_ROUTED | 404 | `404 page not found` |
| `inference-serving` | `GetServingLog` | NOT_ROUTED | 404 | `404 page not found` |
| `inference-serving` | `GetTaskMetric` | NOT_ROUTED | 404 | `404 page not found` |
| `inference-serving` | `GetTaskMetricBatch` | NOT_ROUTED | 404 | `404 page not found` |
| `inference-serving` | `ListServingEvents` | NOT_ROUTED | 404 | `404 page not found` |
| `inference-serving` | `ListServingInstances` | NOT_ROUTED | 404 | `404 page not found` |
| `inference-serving` | `ListServingVersions` | NOT_ROUTED | 404 | `404 page not found` |
| `inference-serving` | `ListServings` | NOT_ROUTED | 404 | `404 page not found` |
| `notebook` | `GetNotebook` | JSON_ERR:ResourceNotFound | 200 | `{ResponseMetadata}` |
| `notebook` | `GetRealtimeNotebookMetric` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{resource_metric_list}` |
| `notebook` | `GetRealtimeNotebookMetricByTime` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{time_seris_metric_groups}` |
| `notebook` | `GetTaskMetric` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `notebook` | `GetTaskMetricBatch` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `notebook` | `ListNotebookEvents` | JSON_ERR:ResourceNotFound | 200 | `{ResponseMetadata}` |
| `notebook` | `ListNotebooks` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `notebook` | `ListRunIndex` | JSON_ERR:ResourceNotFound | 200 | `{ResponseMetadata}` |
| `project` | `ListProjects` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `ray` | `GetJob` | JSON_ERR:ResourceNotFound | 200 | `{ResponseMetadata}` |
| `ray` | `GetJobLog` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `ray` | `GetTaskMetric` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `ray` | `GetTaskMetricBatch` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `ray` | `ListJobCreators` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{items}` |
| `ray` | `ListJobEvents` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{items, total}` |
| `ray` | `ListJobInstances` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{items, total}` |
| `ray` | `ListJobScalingHistories` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{items, total}` |
| `ray` | `ListJobs` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `train` | `GetJob` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `train` | `GetJobLog` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `train` | `GetJobWorkdir` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{data}` |
| `train` | `GetTaskMetric` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `train` | `GetTaskMetricBatch` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `train` | `ListJobEvents` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{events, total}` |
| `train` | `ListJobInstances` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `train` | `ListJobs` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `train` | `ListPreCheckItems` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `user` | `GetAPIKeyPlaintext` | JSON_ERR:InternalError | 200 | `{ResponseMetadata}` |
| `user` | `GetUserDetail` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{avatar_url, created_at, email, extra_info, global_role, id, name, name_en}` |
| `user` | `ListAPIKeys` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{items}` |
| `workspace` | `GetAllQuota` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetBasicInfo` | OK_RESULT | 200 | `{ResponseMetadata, Result} → Result{clusters, compute_groups, resource_types}` |
| `workspace` | `GetDefaultParentProjectSandboxQuota` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `GetDefaultParentProjectUserSandboxQuota` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `GetDefaultSubProjectUserSandboxQuota` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `GetDefaultUserQuota` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetDefaultUserTaskQuota` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `GetDefaultWorkspaceUserSandboxQuota` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `GetEffectiveParentProjectSandboxQuota` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `GetLogicComputeGroupNodeSpecs` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetLogicComputeGroupResource` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetOverviewOptions` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetOverviewResourceMetric` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetOverviewResourceMetricByTime` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetOverviewTaskMetric` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetProjectUserQuotaProjectList` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetSandboxQuotaOverview` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `GetScheduleConfig` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetUserTaskQuota` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `GetWorkspaceComputeResource` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetWorkspaceNodeSpecs` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetWorkspaceQuota` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `GetWorkspaceTaskQuota` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListConfiguredParentProjectSandboxQuotaProjects` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListConfiguredParentProjectUserSandboxQuotaUsers` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListConfiguredSubProjectSandboxQuotaProjects` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListConfiguredSubProjectUserSandboxQuotaUsers` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListConfiguredWorkspaceUserSandboxQuotaUsers` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListLogicComputeGroups` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `ListNodeDimension` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `ListParentProjectSandboxQuotas` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListParentProjectUserSandboxQuotas` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListProjectDimension` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `ListProjectQuotas` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `ListSandboxMenu` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListSubProjectSandboxQuotas` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListSubProjectUserSandboxQuotas` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |
| `workspace` | `ListTaskDimension` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `ListUserDimension` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `ListUserQuotas` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `ListWorkspaceMembers` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `ListWorkspaceNodes` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `ListWorkspaceParentProjects` | JSON_ERR:AccessForbidden | 200 | `{ResponseMetadata}` |
| `workspace` | `ListWorkspaceUserSandboxQuotas` | JSON_ERR:InvalidParameter | 200 | `{ResponseMetadata}` |

