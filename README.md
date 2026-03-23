# qzcli - 启智平台任务管理 CLI

一个类似 kubectl/docker 风格的 CLI 工具，用于管理启智平台任务。

## 特性

- **一键登录**: `qzcli login` 通过 CAS 认证自动获取 cookie，无需手动复制
- **资源发现**: `qzcli res -u` 自动发现工作空间、计算组、规格等资源并本地缓存
- **节点查询**: `qzcli avail` 查询各计算组空余节点，支持低优任务统计
- **任务列表**: 美观的卡片式显示，完整 URL 方便点击
- **状态监控**: watch 模式实时跟踪任务进度

开启启智的极致hack
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

## 安装依赖

```bash
pip install rich requests mcp

cd qzcli_tool
pip install -e .
```

## 快速开始

```bash
# 1. 登录（自动获取 cookie）
qzcli login

# 2. 更新资源缓存（首次使用必须执行，自动发现所有可访问的工作空间）
qzcli res -u

# 3. 查看空余节点
qzcli avail

# 4. 查看运行中的任务
qzcli ls -c -r
```

> **重要**: 
> - 首次使用必须执行 `qzcli res -u`，会自动发现并缓存所有你有权限访问的工作空间
> - 如果遇到 `未找到名称为 'xxx' 的工作空间` 错误，说明缓存需要更新，请重新执行 `qzcli res -u`
> - 新加入的工作空间/项目需要重新执行 `qzcli res -u` 来更新缓存

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
# CI-情景智能
#   计算组                          空节点    总节点 GPU类型     
#   -----------------------------------------------------
#   OV3蒸馏训练组                       4      xxx 某gpu2      
#   openveo训练组                     1     xxx 某gpu2      
#   ...
# 分布式
#   某gpu2-2号机房                      1    xxx 某gpu2      
```

### 提交任务前

```bash
# 找有 4 个空闲节点的计算组
qzcli avail -n 4 -e

# 如果需要考虑低优任务占用的节点（较慢，但更准确地反映潜在可用资源）
qzcli avail --lp -n 4

# 如果开启了 --lp (low priority) 模式，建议配合 -w 指定工作空间以加快速度
qzcli avail --lp -w CI -n 4
```

### 查看任务

```bash
# 查看所有工作空间运行中的任务
qzcli ls -c --all-ws -r

# 查看指定工作空间
qzcli ls -c -w CI -r
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

# 更新所有工作空间的资源缓存
qzcli res -u

# 更新指定工作空间
qzcli res -w CI -u

# 给工作空间设置别名
qzcli res -w ws-xxx --name 我的空间

# 查看空余节点（默认不包含低优任务统计，速度较快）
qzcli avail

# 查看空余节点（包含低优任务统计，即：空节点 + 低优任务占用的节点）
qzcli avail --lp

# 只查看 CI 工作空间
qzcli avail -w CI

# 显示空闲节点名称
qzcli avail -w CI -v

# 找满足 N 节点需求的计算组
qzcli avail -n 4

# 导出为脚本可用格式
qzcli avail -n 4 -e
```

### 任务列表

| 命令 | 别名 | 说明 |
|------|------|------|
| `list` | `ls` | 列出任务 |

```bash
# Cookie 模式（从 API 获取）
qzcli ls -c -w CI           # 指定工作空间
qzcli ls -c --all-ws        # 所有工作空间
qzcli ls -c -w CI -r        # 只看运行中
qzcli ls -c -w CI -n 50     # 显示 50 条

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
# 使用名称（从 qzcli res 缓存解析）
qzcli create \
  --name "my-training-job" \
  --command "bash /path/to/script.sh" \
  --workspace "分布式训练" \
  --project "扩散" \
  --compute-group "3号机房-2" \
  --instances 4 \
  --priority 10

# 使用 ID
qzcli create \
  --name "my-training-job" \
  --command "bash /path/to/script.sh" \
  --workspace ws-9dcc0e1f-80a4-4af2-bc2f-0e352e7b17e6 \
  --project project-7e0957fb-eaa7-4ded-8dca-dd508b2ae01d \
  --compute-group lcg-a91ad10b-415d-4abd-8170-828a2feae5d2 \
  --spec b618f5cb-c119-4422-937e-f39131853076 \
  --instances 4

# 预览 payload 不提交
qzcli create --name test --command "echo hi" --workspace "分布式训练" --dry-run

# JSON 输出（供脚本集成）
qzcli create --name test --command "echo hi" --workspace "分布式训练" --json
```

**参数说明:**

| 参数 | 短选项 | 默认值 | 说明 |
|------|--------|--------|------|
| `--name` | `-n` | (必填) | 任务名称 |
| `--command` | `-c` | (必填) | 执行命令 |
| `--workspace` | `-w` | | 工作空间 ID 或名称 |
| `--project` | `-p` | (自动选择) | 项目 ID 或名称 |
| `--compute-group` | `-g` | (自动选择) | 计算组 ID 或名称 |
| `--spec` | `-s` | (自动选择) | 资源规格 ID |
| `--image` | `-i` | `dhyu-wan-torch29:0.4` | Docker 镜像 |
| `--instances` | | 1 | 实例数量 |
| `--shm` | | 1200 | 共享内存 GiB |
| `--priority` | | 10 | 优先级 1-10 |
| `--framework` | | pytorch | 框架类型 |
| `--no-track` | | | 不自动追踪 |
| `--dry-run` | | | 只预览不提交 |
| `--json` | | | JSON 输出 |

> **提示**: `--project`、`--compute-group`、`--spec` 省略时会自动从 `qzcli res` 缓存中选取第一个。首次使用前请先运行 `qzcli res -u` 发现资源。

### 提交 HPC/CPU 任务

> HPC 任务使用 Slurm 调度，通过 `/api/v1/hpc_jobs` 接口提交，需要 cookie 认证（先运行 `qzcli login`）。

```bash
qzcli hpc \
  --name "bulk-NH3-check-outcar" \
  --workspace ws-6e6ba362-e98e-45b2-9c5a-311998e93d65 \
  --compute-group lcg-cb2de75c-40ac-4de1-bbb3-11e62b32f424 \
  --predef-quota-id d4ae9e69-cec3-4dce-85d6-364ac0ec12b3 \
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
    "workspace": "ws-9dcc0e1f-80a4-4af2-bc2f-0e352e7b17e6",
    "project": "project-7e0957fb-eaa7-4ded-8dca-dd508b2ae01d",
    "compute_group": "lcg-a91ad10b-415d-4abd-8170-828a2feae5d2",
    "spec": "b618f5cb-c119-4422-937e-f39131853076",
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
| `watch` | 实时监控 | `qzcli watch -i 10` |
| `track` | 追踪任务 | `qzcli track job-xxx` |

### 工作空间视图

```bash
# 查看工作空间内运行任务（含 GPU 使用率）
qzcli ws

# 查看所有项目
qzcli ws -a

# 过滤指定项目
qzcli ws -p "长视频"
```

## 输出示例

### qzcli avail -v

```
CI-情景智能
  计算组                          空节点    总节点 GPU类型     
  -----------------------------------------------------
  OV3蒸馏训练组                       4      8 某gpu2      
    空闲: qb-prod-gpu1006, qb-prod-gpu1029, qb-prod-gpu1034, qb-prod-gpu1064
  openveo训练组                     1     79 某gpu2      
    空闲: qb-prod-gpu2000
```

### qzcli ls -c -w CI -r

```
工作空间: CI-情景智能

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

## 环境变量

```bash
export QZCLI_USERNAME="your_username"
export QZCLI_PASSWORD="your_password"
export QZCLI_API_URL="https://qz.sii.edu.cn"
```

## 使用建议

- **日常使用**: `qzcli login && qzcli avail` 一键登录并查看资源
- **提交前**: `qzcli avail -n 4 -e` 找合适的计算组并导出配置
- **提交 GPU 任务**: `qzcli create -n "job" -c "bash run.sh" -w "分布式训练" --instances 4`
- **提交 HPC 任务**: `qzcli login && qzcli hpc --name "job" --workspace ws-xxx --compute-group lcg-xxx --predef-quota-id uuid --cpu 55 --mem-gi 300 --instances 30 --image img --entrypoint "bash run.sh"`
- **批量提交**: `qzcli batch config.json` 从配置文件批量提交
- **监控任务**: `qzcli ls -c --all-ws -r` 查看所有工作空间运行中的任务
- **详细信息**: `qzcli ws` 查看 GPU/CPU/内存使用率
