# qzcli - 启智平台任务管理 CLI

![Release](https://img.shields.io/github/v/release/tianyilt/qzcli_tool?sort=semver) ![Python](https://img.shields.io/badge/python-%3E%3D3.8-blue) ![Tests](https://img.shields.io/badge/tests-54%20passed%2C%201%20skipped-brightgreen) ![License](https://img.shields.io/badge/license-MIT-green)

一个类似 `kubectl` / `docker` 风格的启智平台命令行工具，把资源查询、任务提交、任务管理、日志查看和 MCP/agent 工作流收敛到 CLI 里完成。

## 项目链接

- Release: <https://github.com/tianyilt/qzcli_tool/releases/tag/v0.3.0>
- Changelog: [CHANGELOG.md](CHANGELOG.md)
- Issues: <https://github.com/tianyilt/qzcli_tool/issues>
- License: [MIT](LICENSE)

## 特性

- **一键登录**: `qzcli login` 通过 CAS 认证自动获取 cookie，无需手动复制
- **资源发现**: `qzcli res -u` 调 `cluster_info` / `task_dimension` 聚合工作空间、计算组、项目并本地缓存（默认 quick 模式秒级返回；需要更新 specs 时加 `--full` 走全量历史任务扫描）
- **节点查询**: `qzcli avail` 查询各计算组空余节点，支持低优任务统计和 cookie 失效自动刷新
- **可视化看板**: `qzcli dashboard` 用 treemap 按「计算组→优先级→项目→用户→任务」逐层下钻 GPU 占用（需 `pip install 'qzcli[dashboard]'`）
- **交互式提交**: `qzcli create -i` 提供层级式选择界面，缺少快照时按需预加载
- **任务列表**: 美观的卡片式显示，完整 URL 方便点击
- **日志查看**: `qzcli logs <job-id>` 直连平台日志接口，支持 tail、follow、raw/json 输出
- **排队诊断**: `qzcli events <id>` 给出平台侧的**真实排队原因**，不用自己猜。训练任务和开发机都支持（两者走不同接口，命令会自动分流）。开发机还会把 K8s 原文翻成能直接行动的结论 —— 比如「计算组挑错了：594 台机器不属于你选的组，换个组，等再久也没用」
- **状态监控**: watch 模式实时跟踪任务进度

快速查看资源：

```bash
qzcli login -u 用户名 -p 密码 && qzcli avail
```
```
分布式
  计算组                          空节点     总节点     空GPU GPU类型     
  -----------------------------------------------------------------
  某gpu2-3号机房-2                    3      xxx  x/xxx 某gpu2      
  某gpu2-3号机房                      0      xxx   x/xxx 某gpu2      
  某gpu2-2号机房                      0      xxx   x/xxx 某gpu2      
  cuda12.8版本某gpu1                 0      xxx  x/xxx 某gpu1   
```

## 安装

```bash
cd qzcli_tool
pip install -r requirements.txt
pip install -e .
```

## 快速开始

```bash
# 1. 登录（自动获取 cookie）
qzcli login

# 2. 更新资源缓存（首次使用强烈建议执行，自动发现所有可访问的工作空间）
#    默认 quick 模式秒级返回；如需更新 specs（资源规格），加 --full 走全量历史任务扫描
qzcli res -u

# 3. 查看空余节点
qzcli avail

# 4. 查看运行中的任务（-c 模式必须指定工作空间，或用 --all-ws 查全部）
qzcli ls -c -r -w <工作空间名称或ID>
qzcli ls -c -r --all-ws
```

如果没有显式设置 `QZCLI_ENV_FILE`，`qzcli` 会默认尝试从 `~/.qzcli/.env` 读取 CAS 凭据；如果你的凭据文件在别处，先导出 `QZCLI_ENV_FILE=/path/to/.env`：

```bash
cat > ~/.qzcli/.env <<'EOF'
QZCLI_USERNAME="your_username"
QZCLI_PASSWORD='your_password'
EOF

qzcli login
```

> **重要**: 
> - 首次使用建议执行 `qzcli res -u`，会发现并缓存所有你有权限访问的工作空间，非交互式 `create` / 按名称解析资源时会更稳定
> - 如果遇到 `未找到名称为 'xxx' 的工作空间` 错误，说明缓存需要更新，请重新执行 `qzcli res -u`
> - 新加入的工作空间/项目需要重新执行 `qzcli res -u` 来更新缓存
> - `qzcli create -i` 在没有本地交互快照时会自动按需预加载，不要求你先手动执行 `qzcli avail`

## MCP Server

如果你想在 Codex 或 Claude 里直接调用启智平台相关能力，可以把 `qzcli` 作为 MCP 工具接进去。

```bash
# 1. 进入项目目录（自行替换 xxxxx）
cd /inspire/xxxxx/qzcli_tool

# 2. 安装
python -m pip install -e .
```

安装完成后，可以先检查命令是否已经可用：

```bash
which qzcli-mcp
```

### 接入 Codex

执行下面两条命令即可：

```bash
codex mcp add qzcli -- qzcli-mcp
codex mcp list
```

如果你想固定使用绝对路径，也可以这样写：

```bash
codex mcp add qzcli -- /root/miniconda3/bin/qzcli-mcp （根据 which qzcli-mcp 的返回地址改)
```

### 接入 Claude Code

执行下面两条命令即可：

```bash
claude mcp add qzcli -- qzcli-mcp
claude mcp list
```

如果你想固定使用绝对路径，也可以这样写：

```bash
claude mcp add qzcli -- /root/miniconda3/bin/qzcli-mcp （根据 which qzcli-mcp 的返回地址改)
```

### 使用说明

正常使用时，**不需要**你手动先运行 `qzcli-mcp`。

把它加到 Codex 或 Claude 后，客户端会自动调用它，你手动运行 `qzcli-mcp`，一般只是为了排查问题，你可以直接这样告诉你的 Codex 或者 Claude Code：

```bash
开工了，我要登陆启智平台！

帮我看下现在有多少张华为Atlas950是空闲的

帮我看下现在有多少台某型号卡是空闲的，我要整台的8卡
```

即便数字部某天又在生产环境修改了返回值字段，模型也能根据原始返回JSON快速判断现在哪个字段代表原来的意图，无需手动再次重装qzcli工具（依赖于模型的上下文理解能力）

#### 常见排障

- 如果提示找不到 `qzcli-mcp`，通常重新执行一次安装即可：

```bash
cd /inspire/xxxxx/qzcli_tool
python -m pip install -e .
```

- 如果已经注册过但客户端里看不到，先执行一次 `codex mcp list` 或 `claude mcp list` 确认是否注册成功
- 如果你手动运行 `qzcli-mcp` 后立刻报错，先修复启动报错，再回到客户端里接入

## 推荐工作流

### 每日使用

```bash
# 登录并查看资源
qzcli login && qzcli avail

# 输出示例：
# 示例工作空间
#   计算组                          空节点    总节点 GPU类型     
#   -----------------------------------------------------
#   训练组-A                            4      xxx GPU-A      
#   训练组-B                            1     xxx GPU-B      
#   ...
# 训练任务
#   计算组-C                            1    xxx GPU-C      
```

### 提交任务前

```bash
# 找有 4 个空闲节点的计算组
qzcli avail -n 4 -e

# 如果需要考虑低优任务占用的节点（较慢，但更准确地反映潜在可用资源）
qzcli avail --lp -n 4

# 如果开启了 --lp (low priority) 模式，建议配合 -w 指定工作空间以加快速度
qzcli avail --lp -w 分布式 -n 4
```

### 查看任务

```bash
# 查看所有工作空间运行中的任务
qzcli ls -c --all-ws -r

# 查看指定工作空间
qzcli ls -c -w 分布式 -r
```

## 命令参考

### 认证命令

| 命令 | 说明 | 示例 |
|------|------|------|
| `login` | CAS 登录获取 cookie | `qzcli login` |
| `cookie` | 手动设置 cookie | `qzcli cookie -f cookies.txt` |

```bash
# 交互式登录
qzcli login

# 带参数登录
qzcli login -u 学工号 -p 密码

# 脚本里从 stdin 读密码
echo 'your_password' | qzcli login -u 学工号 --password-stdin

# 查看当前 cookie
qzcli cookie --show

# 清除 cookie
qzcli cookie --clear
```

### 资源管理

| 命令 | 别名 | 说明 |
|------|------|------|
| `resources` | `res`, `lsws` | 管理工作空间资源缓存 |
| `avail` | `av` | 查询计算组空余节点 |

```bash
# 列出已缓存的工作空间
qzcli res --list

# 更新所有工作空间的资源缓存（默认 quick + 8 路并行：跳过历史任务，秒级；不刷新 specs）
qzcli res -u

# 调整并发：网络抖动时可降到 1 退回串行（默认 --parallel 8）
qzcli res -u --parallel 4

# 完整刷新（包含 specs，扫描全部历史任务；大型共享空间可能耗时数十分钟）
qzcli res -u --full

# 更新指定工作空间（同样默认 quick，加 --full 走完整扫描）
qzcli res -w 分布式 -u

# 给工作空间设置别名
qzcli res -w ws-xxx --name 我的空间

# 查看空余节点（默认不包含低优任务统计，速度较快）
qzcli avail

# 查看空余节点（包含低优任务统计，即：空节点 + 低优任务占用的节点）
qzcli avail --lp

# 只查看指定工作空间
qzcli avail -w 分布式

# 只看某个计算组
qzcli avail -w 分布式 -g lcg-xxx

# 显示空闲节点名称
qzcli avail -w 分布式 -v

# 找满足 N 节点需求的计算组
qzcli avail -n 4

# 导出为脚本可用格式
qzcli avail -n 4 -e
```

如果本地 cookie 已过期，但你已经通过 shell 环境变量、`QZCLI_ENV_FILE` 指向的 `.env`（默认 `~/.qzcli/.env`）或 `~/.qzcli/config.json` 保存了 CAS 凭据，`qzcli avail` 会自动刷新 cookie 后继续查询。

### 可视化看板

`qzcli dashboard` 启动一个 Streamlit + plotly treemap 看板，把工作空间**在跑**的 GPU 占用按「**计算组(机房) → 优先级档 → 项目 → 用户 → 任务**」逐层下钻：块面积 = GPU 数，点一块放大、点中心面包屑退回，一眼看清「各计算组里谁占最多、各自高低优」。配色可切 优先级 / 任务类型 / **GPU 利用率**（红 = 申请卡多却在空转）/ **运行时长**（越久越红）；顶部有「按任务类型占比」行（看交互式建模/训练/推理各占多少）与「已占用/空闲 GPU」；勾选「叠加空闲 GPU（灰块）」可把各计算组的**剩余容量**以灰块叠加。工作空间用下拉框选，悬停任意块给出干净明细（任务数/类型/平均利用率/最长运行）。数据分页并发拉取，分布式空间首屏约 5 秒。

看板依赖为**可选 extra**（不装也不影响其它命令）：

```bash
pip install 'qzcli[dashboard]'      # 安装 streamlit / plotly / pandas

qzcli login                          # 看板走 cookie 认证
qzcli dashboard                      # 默认「分布式」，端口 8520，自动开浏览器
qzcli dashboard -w 分布式 --port 8600  # 指定工作空间和端口
qzcli dashboard --no-browser         # headless（远端/无 GUI 场景）
```

> 计算组归属：平台的任务/节点资源维度接口都不直接带 logic_compute_group，看板改为逐 lcg 用 `list_node_dimension(logic_compute_group_id=…)` 反建「节点 → 计算组」映射（与 `avail` 同法），再经任务占用节点挂回，实测全覆盖。「排队/未分配」表示尚未占到节点的任务。

### 任务列表

| 命令 | 别名 | 说明 |
|------|------|------|
| `list` | `ls` | 列出任务 |

```bash
# Cookie 模式（从 API 获取）
qzcli ls -c -w 分布式       # 指定工作空间
qzcli ls -c --all-ws        # 所有工作空间
qzcli ls -c -w 分布式 -r    # 只看运行中
qzcli ls -c -w 分布式 -n 50 # 显示 50 条

# 本地模式（从本地存储）
qzcli ls                    # 默认列表
qzcli ls -r                 # 运行中
qzcli ls --no-refresh       # 不刷新状态
```

### 创建任务

| 命令 | 别名 | 说明 |
|------|------|------|
| `create` | `create-job` | 创建并提交 GPU 分布式训练任务 |
| `hpc` | | 提交 HPC/CPU 任务（Slurm，需 cookie 认证） |
| `batch` | | 从 JSON 配置文件批量提交任务 |

```bash
# 交互式提交：仅补齐未显式传入的参数
qzcli create -i

# 只针对某个 workspace 按需预加载交互快照
qzcli create -i -w "我的工作空间"

# 使用名称（从 qzcli res 缓存解析）
qzcli create \
  --name "my-training-job" \
  --command "bash /path/to/script.sh" \
  --workspace "我的工作空间" \
  --project "我的项目" \
  --compute-group "我的计算组" \
  --image registry.example.com/team/train-image:latest \
  --instances 4 \
  --priority 10

# 使用 ID
qzcli create \
  --name "my-training-job" \
  --command "bash /path/to/script.sh" \
  --workspace ws-<workspace_id> \
  --project project-<project_id> \
  --compute-group lcg-<compute_group_id> \
  --spec <spec_id> \
  --image registry.example.com/team/train-image:latest \
  --instances 4

# 预览 payload 不提交
qzcli create --name test --command "echo hi" --workspace "我的工作空间" --image registry.example.com/team/train-image:latest --dry-run

# JSON 输出（供脚本集成）
qzcli create --name test --command "echo hi" --workspace "我的工作空间" --image registry.example.com/team/train-image:latest --json
```

**参数说明:**

| 参数 | 短选项 | 默认值 | 说明 |
|------|--------|--------|------|
| `--interactive` | `-i` | | 进入交互式任务提交模式，仅提示缺失参数 |
| `--name` | `-n` | (必填) | 任务名称 |
| `--command` | `-c` | (必填) | 执行命令 |
| `--workspace` | `-w` | | 工作空间 ID 或名称 |
| `--project` | `-p` | (自动选择) | 项目 ID 或名称 |
| `--compute-group` | `-g` | (自动选择) | 计算组 ID 或名称 |
| `--spec` | `-s` | (自动选择) | 资源规格 ID |
| `--image` | `-m` | `docker.sii.shaipower.online/inspire-studio/dhyu-wan-torch29:0.4` | Docker 镜像 |
| `--image-type` | | `SOURCE_PRIVATE` | 镜像类型 |
| `--instances` | | 1 | 实例数量 |
| `--shm` | | 1200 | 共享内存 GiB |
| `--priority` | | 10 | 优先级 1-10 |
| `--framework` | | pytorch | 框架类型 |
| `--no-track` | | | 不自动追踪 |
| `--dry-run` | | | 只预览不提交 |
| `--json` | | | JSON 输出 |

兼容性说明：历史脚本中的 `qzcli create -i <image>` 仍可用，CLI 会自动按旧语义解析为 `--image`。

> **提示**: `qzcli create -i` 在 TTY 终端下会先进入单实例全屏的层级式选择菜单，按 `workspace -> project -> compute_group -> spec` 的顺序逐级选择，`Enter/→` 进入下一层，`←` 返回上一层重新选择，界面会直接覆盖刷新而不是连续堆叠多个表格。`compute_group` 选项里会展示 `GPU类型 / 占用口径 / 规格状态 / 空节点 / 空GPU / GPU分配率`，其中 `共享池` 表示该数值来自底层物理 compute group 的共享资源池实时占用，`规格状态` 会标识该计算组的 spec 列表是否来自实时接口、缓存或异常分支。若某个计算组的实时 spec 查询失败，界面不会退出 TUI，而是在同一屏内给出告警，并支持 `m` 手动输入 spec ID、`r` 重试拉取、`←` 返回上一级更换计算组。完成资源选择后，再按原来的方式输入任务名称、执行命令、Docker 镜像等参数。若当前环境不是 TTY，或缺少 `prompt_toolkit`，CLI 会自动回退到原来的文本交互模式。若本地 cookie 已失效且已配置 CAS 账号密码，CLI 会自动刷新 cookie 后重试；若本地没有可复用的交互快照，`create -i` 会按需预加载当前可访问 workspace 的资源快照，并将结果保存到 `~/.qzcli/create_interactive_snapshot.json` 供后续复用。已经显式传入的参数会直接跳过。非交互模式下，`--project`、`--compute-group`、`--spec` 省略时仍会自动从 `qzcli res` 缓存中选取第一个。首次使用前建议先运行 `qzcli login && qzcli res -u`。

### 提交 HPC/CPU 任务

> HPC 任务使用 Slurm 调度，通过 `/api/v1/hpc_jobs` 接口提交，需要 cookie 认证（先运行 `qzcli login`）。

```bash
qzcli hpc \
  --name "bulk-NH3-check-outcar" \
  --workspace ws-<workspace_id> \
  --compute-group lcg-<compute_group_id> \
  --predef-quota-id <predef_quota_id> \
  --cpu 55 \
  --mem-gi 300 \
  --instances 30 \
  --cpus-per-task 55 \
  --image docker.sii.shaipower.online/inspire-studio/vasp_lmp-wyh:1203 \
  --entrypoint "cd /path/to/dir && bash run.sh"
```

**参数说明:**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--name` | (必填) | 任务名称 |
| `--workspace` | (必填) | 工作空间 ID 或名称 |
| `--compute-group` | (必填) | 计算组 ID（lcg-...） |
| `--predef-quota-id` | (必填) | 预定义配额 ID（资源规格 UUID） |
| `--cpu` | (必填) | 每节点 CPU 核心数 |
| `--mem-gi` | (必填) | 每节点内存 GiB |
| `--image` | (必填) | 容器镜像地址 |
| `--entrypoint` | (必填) | 运行命令（shell 字符串） |
| `--project` | 自动选择 | 项目 ID 或名称 |
| `--instances` | 1 | 节点数 |
| `--cpus-per-task` | 1 | 每任务 CPU 数 |
| `--memory-per-cpu` | `5G` | 每 CPU 内存 |
| `--image-type` | `SOURCE_PRIVATE` | 镜像类型 |
| `--no-track` | | 不追踪任务 |
| `--json` | | JSON 输出 |

> **注意**: HPC 任务在启智平台的「高性能计算任务」页面查看，URL 为 `https://qz.sii.edu.cn/jobs/hpc?spaceId=<workspace_id>`，与 GPU 分布式训练任务页面不同。

### 批量提交任务

```bash
# 从 JSON 配置批量提交
qzcli batch batch_eval.json --delay 3

# 预览所有任务
qzcli batch batch_eval.json --dry-run

# 遇到错误继续提交
qzcli batch batch_eval.json --continue-on-error
```

**批量配置文件格式 (JSON):**

```json
{
  "defaults": {
    "workspace": "ws-<workspace_id>",
    "project": "project-<project_id>",
    "compute_group": "lcg-<compute_group_id>",
    "spec": "<spec_id>",
    "image": "docker.sii.shaipower.online/inspire-studio/dhyu-wan-torch29:0.4",
    "instances": 4,
    "shm": 1200,
    "priority": 10
  },
  "matrix": {
    "checkpoint": ["/path/to/ckpt1", "/path/to/ckpt2"],
    "eval_mode": ["mybench_universe", "video_universe"],
    "step": [105000, 200000]
  },
  "name_template": "eval-{checkpoint_basename}-{eval_mode}-step{step}",
  "command_template": "bash /path/to/eval.sh --checkpoint_dir {checkpoint} --eval_mode {eval_mode} --specific_steps {step}"
}
```

`matrix` 中的所有维度会做笛卡尔积，上面的例子会生成 2 x 2 x 2 = 8 个任务。模板中可用 `{key}` 引用 matrix 变量，路径类变量还可用 `{key_basename}` 获取文件名。

**在 shell 脚本中循环提交（替代旧的 curl 方式）:**

```bash
#!/bin/bash
CHECKPOINTS=("/path/to/ckpt1" "/path/to/ckpt2")
STEPS=(105000 200000)

for ckpt in "${CHECKPOINTS[@]}"; do
  for step in "${STEPS[@]}"; do
    qzcli create \
      --name "eval-$(basename $ckpt)-step${step}" \
      --command "bash /path/to/eval.sh --ckpt $ckpt --step $step" \
      --workspace "分布式训练" \
      --compute-group "xxx-3号机房-2" \
      --instances 4
    sleep 3
  done
done
```

### 任务管理

| 命令 | 说明 | 示例 |
|------|------|------|
| `status` | 查看任务详情 | `qzcli status job-xxx` |
| `stop` | 停止任务 | `qzcli stop job-xxx` |
| `logs` | 查看任务容器日志 | `qzcli logs job-xxx --tail 100` |
| `watch` | 实时监控 | `qzcli watch -i 10` |
| `track` | 追踪任务 | `qzcli track job-xxx` |
| `exec` | 在开发机上执行命令（Jupyter API，无需 SSH） | `qzcli exec blender-rl nvidia-smi` |

```bash
# 查看最近 200 条日志（默认）
qzcli logs job-xxx

# 查看最近 N 条，按时间顺序打印
qzcli logs job-xxx --tail 50

# 类似 tail -f 持续轮询新日志
qzcli logs job-xxx --follow --tail 20 --interval 3

# 只输出 message，便于 grep/管道处理
qzcli logs job-xxx --tail 100 --raw

# 每条日志输出一行原始 JSON
qzcli logs job-xxx --tail 10 --json

# 只看指定 pod，或只看某个时间之后的日志
qzcli logs job-xxx --pod job-xxx-worker-0 --since 10m
```

`logs` 使用平台 `/api/v2/train?Action=GetJobLog` 接口，需要 `qzcli login` 保存的 cookie。若提示 Cookie 过期，重新运行 `qzcli login` 后再试。

### 远程执行 / 开发机命令

`qzcli exec` 通过开发机自带的 Jupyter terminal API 在远端跑一条命令，**不需要起 SSH**。命令以 fire-and-forget + 轮询输出的方式执行，本地连接抖动不会杀掉远端进程；`--timeout` 只决定本地等多久后切回，远端命令继续在开发机上跑。

```bash
# 按 name 解析（最常见，需 list_notebooks 里能查到）
qzcli exec blender-rl nvidia-smi

# 按 notebook_id (UUID) 解析
qzcli exec cfe43e55-e7a1-484a-898c-695596b0877b nvidia-smi

# 也能只粘 notebook_id 的一段前缀（前缀唯一才解析；撞到多个会列出候选让你补全）
qzcli exec cfe43e55 nvidia-smi

# 直接粘贴 IDE / Jupyter URL（支持 /ide?notebook_id=、
# /jobs/interactiveModel(ing)?Detail/、/jupyter/、/api/v1/notebook/lab/、/notebook/lab/ 等多种形式）
qzcli exec 'https://qz.sii.edu.cn/jobs/interactiveModelingDetail/cfe43e55-...?spaceId=ws-...' df -h

# 自定义本地等待超时（默认 120 秒）。
# 注意：因 remote_cmd 用 argparse.REMAINDER 吸收，--timeout/--detach 必须放在 target 之前
qzcli exec --timeout 600 blender-rl bash /workspace/long_running.sh

# 后台启动长命令：立即返回 job_id，不等待结果
qzcli exec --detach blender-rl bash /workspace/train.sh
# → 打印 job_id，例如 qzcli_1700000000

# 之后随时重连，继续拉取输出 / 退出码（未结束可反复 attach）
qzcli exec-attach blender-rl qzcli_1700000000
```

`exec` 走开发机域名下的 Jupyter terminal / contents 接口（与浏览器 IDE 同一套），需要 `qzcli login` 保存的 cookie。**Cookie 过期会用本地凭据自动重登一次**（凭据来自 env / `~/.qzcli/.env` / `config.json`），无凭据时回退提示重新 `qzcli login`。

> 对 agent / 自动化：MCP 工具 `qz_exec`（短命令同步；`detach=True` 用于编译、下载、训练等长命令）和 `qz_exec_attach`（轮询长命令结果）提供与上面等价的能力，无需 shell-out，并共享 cookie 自动重登。

### 工作空间视图

```bash
# 查看工作空间内运行任务（含 GPU 使用率）
qzcli ws

# 查看所有项目
qzcli ws -a

# 过滤指定项目
qzcli ws -p "我的项目"
```

## 输出示例

### qzcli avail -v

```
示例工作空间
  计算组                          空节点    总节点 GPU类型     
  -----------------------------------------------------
  OV3蒸馏训练组                       4      8 某gpu2      
    空闲: qb-prod-gpu1006, qb-prod-gpu1029, qb-prod-gpu1034, qb-prod-gpu1064
  openveo训练组                     1     79 某gpu2      
    空闲: qb-prod-gpu2000
```

### qzcli ls -c -w 分布式 -r

```
工作空间: 示例工作空间

[1] ● 运行中 | 44分钟前 | 44分36秒
    eval-OpenVeo3-I2VA-A14B-1227-8s...
    8×某gpu2 | 4节点 | GPU资源组
    https://qz.sii.edu.cn/jobs/distributedTrainingDetail/job-xxx

[2] ● 运行中 | 58分钟前 | 56分47秒
    sglang-eval-A14B-360p-wsd-105000...
    8×某gpu2 | 2节点 | GPU资源组
```

## 配置文件

配置存储在 `~/.qzcli/` 目录：

| 文件 | 说明 |
|------|------|
| `config.json` | API 认证信息 |
| `jobs.json` | 本地任务历史 |
| `.cookie` | Cookie（login 命令自动管理） |
| `resources.json` | 资源缓存（工作空间、计算组等） |
| `create_interactive_snapshot.json` | `create -i` 的交互资源快照 |

## 环境变量

```bash
export QZCLI_USERNAME="your_username"
export QZCLI_PASSWORD="your_password"
export QZCLI_API_URL="https://qz.sii.edu.cn"
export QZCLI_ENV_FILE="/path/to/.env"   # 可选，自定义凭据文件位置
export QZCLI_SESSION_ID="my-agent-01"   # 可选，见下方「多 agent 并发 exec」
```

`qzcli login` / 自动刷新 cookie 会按下面的优先级读取凭据：

```bash
CLI 参数 > --password-stdin > shell 环境变量 > QZCLI_ENV_FILE 指向的 .env（默认 ~/.qzcli/.env） > ~/.qzcli/config.json > 交互输入
```

## 多 agent 并发 exec（session 隔离）

多个 AI agent 常常同时对**同一台开发机**跑 `qzcli exec`。为此每次 exec 都归属于一个
session，输出隔离在 `/tmp/.qzcli/<session>/` 下，`job_id` 形如
`qzcli_<session>_<时间戳>_<随机>`。

session 来源与凭据同一套优先级：`QZCLI_SESSION_ID` 环境变量 →
`QZCLI_ENV_FILE` 指向的 `.env` → `~/.qzcli/config.json` 的 `session_id` →
**都没有就按进程自动生成**（进程内稳定）。

```bash
# 不设也不会串车：每个进程自动一个 session
qzcli exec my-dev nvidia-smi

# 想让多个进程归到同一个 session（比如一个 agent 起了多个 qzcli 子进程）
export QZCLI_SESSION_ID="my-agent-01"

# --detach 之后忘了记 job_id，可以查回来（默认只列本 session）
qzcli exec --list my-dev
qzcli exec --list --all my-dev     # 看所有 session
qzcli exec-attach my-dev <JOB ID>
```

其他几条相关行为：

- 每次 exec **自建一个终端并在启动后删掉**，不复用已有终端 —— 否则会往别人开着的
  交互式会话里打字（实测见过开发机上躺着 4 个别人的终端）
- 命令用 `setsid`（无则 `nohup`）从终端摘出去，删终端不影响它跑完
- `/tmp/.qzcli/` 下超过 **7 天**的 session 目录会在下次 exec 时自动清理
- 升级前拿到的老格式 `job_id`（无 session 段）仍能 `exec-attach`，会回落到平铺路径

## 平台 API v1 → v2 迁移

启智平台正在把 `/api/v1` 下线（`/openapi/v1/specs/list` **已经 404**），官方 CLI `qz`
是纯 v2 客户端。qzcli 的接口层已迁到 v2，策略是 **v2 优先 + v1 兜底**：
公开方法签名一律不变，内部由 `api._v2_then_v1` 分发，只在 v2 **路由不通**
（404/405/50x、或被网关 302 成 HTML）时回落 v1；业务错误和 401 直接抛出，
不做静默降级。

已迁移：`train ListJobs / GetJob / StopJob`、`notebook ListNotebooks`、
`workspace ListNodeDimension / ListTaskDimension / GetBasicInfo /
GetOverviewTaskMetric`、`hpc ListJobs`。

**两个仍留在 v1 的地方**（v2 没有对应能力，见 `docs/v1_to_v2_mapping.md`）：

- `/api/v1/project/list` —— v2 的 `project ListProjects` 对普通账号是
  `AccessForbidden`；且 v2 全域**没有 `ListWorkspaces`**，工作空间只能从项目推导
- `/api/v1/notebook/lab/{id}` —— v2 拿不到 Jupyter 访问地址，`qzcli exec` 依赖它

### 相关文档与工具

| 文件 | 说明 |
|---|---|
| `docs/v1_to_v2_mapping.md` | 端点映射表（**真机实测**，含踩坑记录和平台侧缺口） |
| `docs/api_spec_v2.json` | 官方 `qz` 全部 11 services / 144 actions 的结构化接口定义，可 diff |
| `docs/v2_probe_report.md` | cookie 在 v2 各 Action 上的可用性探针报告 |
| `tools/gen_api_spec_doc.py` | 扫 `qz spec`/`schema`/`--dry-run` 重新生成上面的接口文档 |
| `tools/probe_v2.py` | 探针：只打只读 Action，产物自动对 UUID 打码 |
| `tools/compare_v1_v2.py` | 同一份数据 v1/v2 各拉一次逐字段 diff，防"静默返回空" |

> ⚠️ 改 v2 端点时注意：`cluster.*` 和 `workspace.*` 有一批同名 Action，描述几乎一样，
> 但 **`cluster.*` 是集群管理员权限，普通账号一律 `AccessForbidden`**。
> qzcli 是工作空间级工具，必须走 `workspace.*`。

## Roadmap

- Cookie API 收敛: 提取统一的 browser headers 和 cookie request wrapper，减少 `api.py` 中重复 headers。
- Release 工程化: 继续完善 changelog、贡献者说明、issue 模板和版本发布流程。
- `qzcli exec` 改走 SSH（`notebook GetNotebook` 的 `extra_info.HostIP/SshPort/ProxyJump`），
  以摘掉最后一个 v1 依赖。

## Known Issues

- 对 CAS 联合认证用户，`/auth/token` / `/openapi/v1/*` token path 可能返回 `invalid_grant`。日常功能优先使用 `qzcli login` 保存的 session cookie。
- Cookie 有过期时间；如果命令提示 Cookie 过期，重新运行 `qzcli login`，或配置 `QZCLI_USERNAME` / `QZCLI_PASSWORD` 让 qzcli 自动刷新。
- 平台接口字段偶尔会变化；如果输出异常，优先附带 `--json` 输出或原始报错开 issue。

## Contributors

感谢所有参与代码、PR、测试和反馈的同学：

- [@tianyilt](https://github.com/tianyilt)
- [@0-693](https://github.com/0-693)
- [@gaoyang07](https://github.com/gaoyang07)
- [@YushunXiang](https://github.com/YushunXiang)
- [@GQH123](https://github.com/GQH123)
- [@Hashmapw](https://github.com/Hashmapw)
- [@SyntaxSmith](https://github.com/SyntaxSmith)
- [@ekonwang](https://github.com/ekonwang)
- [@Stepuuu](https://github.com/Stepuuu)

## 致谢

qzcli 的很多能力来自实际使用中的持续反馈、PR 和接口探索。感谢所有参与试用、贡献和反馈的同学。

特别感谢 [`SII-Holos/holos-inspire`](https://github.com/SII-Holos/holos-inspire) 和 [`EmbodiedForge/Inspire-cli`](https://github.com/EmbodiedForge/Inspire-cli) 在启智平台命令行工作流、认证路径和接口探索上的启发与铺垫。qzcli 在此基础上继续面向任务管理、资源查询、日志查看、批量提交和 MCP/agent workflow 做了扩展，希望能让更多启智平台用户少点重复点击，多点自动化空间。

## 使用建议

- **日常使用**: `qzcli login && qzcli avail` 一键登录并查看资源
- **提交前**: `qzcli avail -n 4 -e` 找合适的计算组并导出配置
- **交互式提交 GPU 任务**: `qzcli create -i`，如只关心单个 workspace 可加 `-w`
- **提交 GPU 任务**: `qzcli create -n "job" -c "bash run.sh" -w "分布式训练" --instances 4`
- **提交 HPC 任务**: `qzcli login && qzcli hpc --name "job" --workspace ws-xxx --compute-group lcg-xxx --predef-quota-id uuid --cpu 55 --mem-gi 300 --instances 30 --image img --entrypoint "bash run.sh"`
- **批量提交**: `qzcli batch config.json` 从配置文件批量提交
- **监控任务**: `qzcli ls -c --all-ws -r` 查看所有工作空间运行中的任务
- **查看日志**: `qzcli logs job-xxx --tail 100` 拉取任务容器日志，`--follow` 可持续轮询
- **详细信息**: `qzcli ws` 查看 GPU/CPU/内存使用率
