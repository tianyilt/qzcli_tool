# qzcli - 启智平台任务管理 CLI

一个类似 kubectl/docker 风格的 CLI 工具，用于管理启智平台任务。

## 特性

- **任务列表**: 美观的卡片式显示，完整 URL 方便点击
- **状态监控**: watch 模式实时跟踪任务进度
- **自动追踪**: 与现有提交脚本无缝集成
- **工作空间视图**: 查看工作空间内所有运行任务（需要浏览器 cookie）
- **状态颜色**: 成功(绿)、失败(红)、运行中(青)、等待(蓝)

## 安装依赖

```bash
pip install rich requests
```

## 快速开始

```bash
# 1. 初始化配置（首次使用）
./bin/qzcli init

# 2. 查看任务列表
./bin/qzcli list

# 3. 只看运行中的任务
./bin/qzcli list -r

# 4. 实时监控
./bin/qzcli watch
```

## 命令参考

### 基本命令

| 命令 | 说明 | 示例 |
|------|------|------|
| `init` | 初始化配置 | `qzcli init` |
| `list` | 列出任务 | `qzcli list -n 20` |
| `status` | 查看任务详情 | `qzcli status job-xxx` |
| `stop` | 停止任务 | `qzcli stop job-xxx` |
| `watch` | 实时监控 | `qzcli watch -i 10` |

### 任务管理

| 命令 | 说明 | 示例 |
|------|------|------|
| `track` | 追踪任务 | `qzcli track job-xxx --name "my-job"` |
| `import` | 导入任务 | `qzcli import jobs.txt -r` |
| `remove` | 删除记录 | `qzcli rm job-xxx` |
| `clear` | 清空所有 | `qzcli clear` |

### 工作空间命令（需要浏览器 cookie）

| 命令 | 说明 | 示例 |
|------|------|------|
| `cookie` | 设置浏览器 cookie | `qzcli cookie -f cookies.txt -w <workspace_id>` |
| `workspace` | 查看工作空间任务 | `qzcli ws` |

## 常用选项

```bash
# 列出最近 30 个任务
qzcli list -n 30

# 只显示运行中/排队中的任务
qzcli list -r

# 只显示失败的任务
qzcli list --status job_failed

# 不刷新状态（更快）
qzcli list --no-refresh

# 紧凑表格格式
qzcli list -c

# 每 5 秒刷新一次
qzcli watch -i 5

# 查看 JSON 格式详情
qzcli status job-xxx --json
```

## 工作空间视图（Mac 上使用）

`qzcli ws` 命令可以查看工作空间内所有运行任务（包括网页创建的），显示详细的 GPU/CPU/内存使用率。

**注意**: 此功能需要浏览器 cookie，只能在能直接访问 qz.sii.edu.cn 的机器上使用。

### 获取 Cookie

1. 在浏览器打开启智平台
2. F12 -> Network
3. 右键点击任意请求 -> Copy -> Copy as cURL
4. 从 cURL 命令中提取 `-b` 后的 cookie 值
5. 保存到 `cookies.txt` 文件

### 使用方法

```bash
# 设置 cookie
qzcli cookie -f cookies.txt -w ws-8207e9e2-e733-4eec-a475-cfa1c36480ba

# 查看工作空间任务（默认过滤"扩散"项目）
qzcli ws

# 查看所有项目
qzcli ws -a

# 过滤指定项目
qzcli ws -p "长视频"

# 同步到本地任务列表
qzcli ws -s
```

### 输出示例

```
工作空间任务概览 [扩散] (显示 15/141 个, 120 GPU, 平均利用率 85.2%)

[ 1] ● sglang-eval-A14B-360p-wsd-105000-universe-chinese-score
     8 GPU (94%) | CPU 1% | MEM 18% | 32m | 梁天一
     CI-扩散音视频生成 | 1 节点: qb-prod-gpu1917
     job-626c1948-4984-47fd-99c2-c4438630ee0f
```

## 与提交脚本集成

`submit_sglang_job_eval.sh` 和 `submit_job_eval_with_score.sh` 已自动集成 qzcli。
提交任务时会自动记录到 qzcli，无需手动操作。

## 任务状态

| 状态 | 图标 | 颜色 |
|------|------|------|
| job_succeeded | ✓ | 绿色 |
| job_failed | ✗ | 红色 |
| job_stopped | ⏹ | 黄色 |
| job_running | ● | 青色 |
| job_pending | ◌ | 蓝色 |

## 配置文件

配置存储在 `~/.qzcli/` 目录：

- `config.json` - 认证信息
- `jobs.json` - 任务历史
- `.token_cache` - Token 缓存
- `.cookie` - 浏览器 Cookie（用于内部 API）

## 环境变量

可通过环境变量覆盖配置：

```bash
export QZCLI_USERNAME="your_username"
export QZCLI_PASSWORD="your_password"
export QZCLI_API_URL="https://qz.sii.edu.cn"
```

## 使用建议

- **服务器上**: 使用 `qzcli list -r` 查看运行中的任务，用 OpenAPI 管理
- **Mac 上**: 使用 `qzcli ws` 查看工作空间全貌，包含 GPU 使用率等详细信息
- 通过 git 同步代码，在两边都可以使用
