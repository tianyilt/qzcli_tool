# qzcli - 启智平台任务管理 CLI

一个类似 kubectl/docker 风格的 CLI 工具，用于管理启智平台任务。

## 特性

- **一键登录**: `qzcli login` 通过 CAS 认证自动获取 cookie，无需手动复制
- **资源发现**: 自动发现工作空间、计算组、规格等资源并本地缓存
- **节点查询**: 查询各计算组空余节点，帮助决定任务提交位置
- **任务列表**: 美观的卡片式显示，完整 URL 方便点击
- **状态监控**: watch 模式实时跟踪任务进度

开启启智的极致hack
```bash
qzcli login -u 用户名 -p 密码 && qzcli avail
 
```
```
分布式
  计算组                          空节点     低优空余    总节点     空GPU GPU类型     
  ---------------------------------------------------------------------------
  某gpu2-3号机房-2                    3        1    xxx  x/xxx 某gpu2      
  某gpu2-3号机房                      0        0    xxx   x/xxx 某gpu2      
  某gpu2-2号机房                      0        0    xxx   x/xxx 某gpu2      
  cuda12.8版本某gpu1                 0        0    xxx  x/xxx 某gpu1   
```

## 安装依赖

```bash
pip install rich requests
```

## 快速开始

```bash
# 1. 登录（自动获取 cookie）
qzcli login

# 2. 更新资源缓存
qzcli res -u

# 3. 查看空余节点
qzcli avail

# 4. 查看运行中的任务
qzcli ls -c -r
```

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

# 输出：
# ✓ [CI-情景智能] OV3蒸馏训练组  4 空节点 [某gpu2]
# WORKSPACE_ID="ws-xxx"
# LOGIC_COMPUTE_GROUP_ID="lcg-xxx"
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

# 查看空余节点
qzcli avail

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
- **监控任务**: `qzcli ls -c --all-ws -r` 查看所有工作空间运行中的任务
- **详细信息**: `qzcli ws` 查看 GPU/CPU/内存使用率
