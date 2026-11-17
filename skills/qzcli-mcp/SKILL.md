---
name: qzcli-mcp
description: Use the qzcli MCP tools to query 启智平台 workspaces, resources, jobs, availability, and raw status values. Use this when the user wants live task status, workspace resource recommendations, or drift-resistant status inspection from qzcli.
---

# qzcli MCP

使用这个 skill 时，优先通过 `qzcli-mcp` 暴露的工具获取实时信息，不要凭空假设启智平台状态枚举稳定不变。

## 何时使用

- 用户要看启智平台任务、工作空间、计算组、GPU 使用情况
- 用户要找“哪里还有空机器 / 哪个计算组适合提任务”
- 用户明确担心状态字段、cookie 名、返回结构可能漂移
- 用户要排查“为什么现在模型识别不到某种状态”

## 工作流

1. 先用 `qz_auth_login` 检查认证；需要人工授权时在终端执行 `qzcli login`。原生 OAuth 无需官方 Go `qz`，MCP 不等待网页授权；`qz_set_cookie` 仅用于显式 legacy。
2. 如果 workspace / 计算组缓存可能过期，先用 `qz_refresh_resources`
3. 查询资源推荐时，优先用 `qz_get_availability`
4. 查询任务时，优先用 `qz_list_jobs` / `qz_get_job_detail`
5. 如果状态看起来异常或出现未知枚举，调用 `qz_inspect_status_catalog`

## 状态处理规则

- 永远优先看 `status_raw`
- `status_family` 只是便于上层决策的弱归一化结果
- 不要把 `status_family` 当成平台真实枚举
- 需要解释差异时，直接引用 `raw` 中的字段名和值

## 推荐模式

- 看整体资源：`qz_list_workspaces` -> `qz_refresh_resources` -> `qz_get_availability`
- 看任务：`qz_list_jobs`，必要时再对单条调用 `qz_get_job_detail`
- 排查漂移：`qz_inspect_status_catalog`，重点关注 `unknown_statuses`

## 输出建议

- 面向用户汇报时，同时给出“归一化判断”和“原始状态”
- 当发现未知状态时，明确说明这是平台新状态或未覆盖状态，而不是直接误判为失败
