# Changelog

## Unreleased

**主题：对照 InspireSkill 手册补的两处 —— 一个名字错了，一个报错等于没说。**

### 新增

- **优先级被资源规格拒绝时，给出能照着敲的下一步，并记住这次拒绝。**
  实测发现优先级不只有「空间级两套刻度」这一层，平台还**逐个资源规格**限制可用
  优先级，提交阶段直接拒 `InvalidParameter: 任务优先级不在该资源规格允许的范围内`。
  A/B 对照（同一个 1 卡规格、同组同项目，只改优先级）：`--priority 4` 被拒、
  `--priority 1` 建出来了。这正是那次 CPU 任务排不上的成因之一。
  - qzcli 此前把这句原话直接抛出来，而**连平台的报错都不说允许范围是什么**，
    用户不知道该改什么。现在翻译成「先试 `--priority 1`」并说清限制的形状
    （小规格只给低优，整节点规格才不受限）
  - 把 (空间, 规格, 优先级) → 被拒 记进 `CONFIG_DIR/priority_rejects.json`
    （跟随 `QZCLI_HOME`），下次同组合**发请求前**就拦，`--force-priority` 可绕过
  - **只拦已知被拒的组合，没见过的一律放行。** 反过来做白名单会把平台后来放开的
    组合永久挡死 —— 允许范围我们根本读不到，白名单从第一天起就是错的
  - 为什么不是「提交前预检」：`task_priority` 只是 CreateJob 的入参；
    `GetWorkspaceTaskQuota` / `GetUserTaskQuota` / `GetAllQuota` /
    `GetWorkspaceNodeSpecs` 的返回里都没有 priority 字段；`GetScheduleConfig`
    的完整返回（42 字段）里也没有，而且**要 workspace admin 权限，16 个空间只有
    1 个读得到**。所以不假装能预检，只做基于已观测事实的解释和记忆

### 修复

- **`avail` 那列「GPU利用率」其实是分配率，已改名。** 它算的是
  `(总卡 − 空闲卡) / 总卡`，即有多少卡被**分配出去**了；一个占着 8 张卡跑 0% 的
  任务在这列里显示 **100%**。而平台的空闲回收判的是**真实利用率**（口径是
  「GPU 低于 15% 持续 3 小时」），所以恰恰在「我这台机器会不会被收走」这个最要命的
  判断上，这一列给出的是相反的结论。
  - 用户可见文案 + 内部变量统一改成「GPU分配率」/ `gpu_alloc_ratio`
  - **dashboard 那处不动**：`task_dimension_to_row()` 用的是平台给的
    `gpu.usage_rate`，那才是真实利用率
  - 新增 `tests/test_gpu_alloc_vs_util.py` 正反两向钉住这个区分 —— 两个名字只差
    一个字，很容易被后来人「顺手统一」

## v0.4.12 - 2026-08-27

**主题：排队了到底卡在哪，别再猜；顺带修好一个把 `exec` 在新机器上整个搞坏的 bug。**

### 修复（重要）

- **`qzcli exec` 在任何没用过 exec 的开发机上都是坏的** —— 表现是一路等到超时
  报 `exit=124`，什么输出都没有。两个 bug 叠在一起：
  1. 启动流程先用 Contents API `PUT /api/contents/_qzcli {"type":"directory"}`
     「确保中转目录存在」，**这一句恰恰制造了下一句的障碍**：它建出来的是**真
     目录**，而下一句 `ln -sfn /tmp/.qzcli "$PWD/_qzcli"` 的 `-n` 只防「已经是
     指向目录的**符号链接**」，对真目录无效 —— 链接被建到目录**里面**去
     （`_qzcli/.qzcli`），`_qzcli` 本身留成空目录，于是轮询输出文件永远 404。
     老机器上 `_qzcli` 早就是符号链接（那时还没这句 PUT），所以一直没暴露
  2. **PTY 一连上就回显你打的字，哪怕 shell 还没起来。** 而代码发完命令只等
     1 秒就 close + DELETE 终端 —— 慢机器上 shell 还在跑项目盘里的 rc 文件，
     命令还躺在缓冲里，终端就被删了，**命令一次都没执行**
  修法：不再用 Contents API 建那个目录（只由 `ln -sfn` 创建，唯一来源）；对已被
  搞坏的机器自愈（先 `rm -f` 内层链接再 `rmdir`，**不用 `rm -rf`** —— 那目录在
  项目盘上，宁可这次失败也不能删掉别人的数据）；发命令前先用 sentinel 确认
  shell 真会执行（判据是**回显 + 执行结果两次出现**，只出现一次说明还没执行）。
  实测：原先全 124 的 5 台机器 → **5/5 通过**，昇腾机器 3/3 也通过
- **撤掉上一版基于错误诊断加的规避逻辑**。上一版把昇腾/国产卡机器在冒烟选机时
  排到最后，理由写的是「它们走另一个网关，exec 连不上」。**那个诊断是错的**，
  真因就是上面两个 bug。排掉那类机器只是让红灯变绿，真 bug 继续躺在生产里 ——
  **冒烟报某一类机器全挂时，先怀疑被测功能，别急着把那类机器从样本里排掉。**
- 测试里的假终端以前 `recv()` 一律抛异常，等于模拟了一台「只回显、永不执行」
  的机器 —— **假得不像真的，就永远盖着这个 bug**。现已改成回显 + 执行。

### 新增

- **`qzcli events` 支持开发机（notebook）** —— 此前只覆盖训练任务
  （train `ListJobEvents`），开发机是另一类对象、另一个 Action
  （notebook `ListNotebookEvents`），qzcli 没接。后果是**开发机排队时这条命令
  什么都给不出来**：2026-08 一台开发机排了 55 分钟没排上，只能靠 `avail` 的容量
  数字反推，而平台其实一直存着答案。
  - 新模块 `qzcli/nbevents.py` 把 K8s 原文翻成能行动的结论。平台只给一段
    `content`、**没有 reason/type 字段**，所以分类靠文本匹配
  - 分类规则**顺序有意义**：「亲和性不匹配」排在「资源不足」之前，因为两者指向
    **相反的动作**（换计算组 vs 可以等或降规格），顺序错了会让人白等
  - 拆解 `0/N nodes are unavailable` 并**按机器数从多到少排** —— 594 台
    「不属于你选的计算组」比 177 台「内存不足」更能说明问题出在选组上
  - `cmd_events` 自动分流：用户拿到一个 id 时不关心它属于哪类对象
  - **只看最近一轮**：开发机停了再启会攒多轮事件，跨轮次翻旧账会把一台正在正常
    运行的机器误报成有问题（实测踩过）

### 修复

- **冒烟的 exec 用例选机逻辑**：它连续两轮挑中的 5 台全是昇腾/国产卡机器
  （走另一个 Jupyter 网关，`qzcli exec` 连不上），于是报「exec 坏了」，
  让只读冒烟停在 20/21。换普通开发机同一时刻一次就通 —— 是**选机误报，不是功能
  缺陷**。已把这类机器排到候选最后。修完 **21/21**
- **冒烟的 HPC 用例在没有 HPC 历史的空间必然失败**：它从历史任务反推规格，而全
  平台只有少数几个空间跑过 HPC，训练任务最多的那个空间 HPC 历史是 0 条。
  已改成自动换一个有历史的空间。修完写操作冒烟 **26/26、零残留**

### 文档 / 结论

- 记下**平台是多会话**的实测结论（`qzcli/config.py`）：曾担心多份 `QZCLI_HOME`
  各持一份 cookie 会互相踢下线、成为锁号放大器，为此考虑过软链共用。实测
  「探本机 cookie 有效 → 另一台机器同账号登录一次 → 立刻重探（内容指纹未变）
  → 仍然有效」，**不存在互踢**，维持各自独立
- **清掉 public 仓库里的内部标识（并钉成测试）**。上一轮报的是「内部空间名 6 处 → 0」，
  一天后复查**同一个仓库还留着 5 处** —— 那轮只按字面名字 grep，漏掉三类：
  - 空间 **ID**（`ws-…`）：名字清了 ID 还在，一样能反查
  - `tools/probe_v2.py` 的 `redact()` **自己**：它把 UUID 打码后**保留前 8 位十六进制**。
    写的时候觉得「只是前缀」，实际那 8 位在全平台唯一，等于没打码
  - 测试 docstring 里的分区名 —— 不在源码在注释里
  共同点是「靠人 grep 一遍就宣布干净」。现在改成 `redact()` 一位十六进制都不留，
  并新增 `NoInternalIdentifiersTests` 把内部项目名和真实资源 ID 钉死（两条都做过变异验证）。
  命令示例统一改用 `--workspace 分布式`
  - ⚠️ 这道闸只看工作区，**拦不住 git 历史**：本仓历史里仍有 11 个提交的内容和
    8 条 commit message 含内部名，需另行处置（改写历史会打断所有已 clone 的副本）

## v0.4.11 - 2026-08-26

**主题：开发机重启不再丢东西，操作有据可查。**

### 新增

- **`qzcli devbox`（status / init）** —— 把开发机上易失的 dotfile 和 agent home
  挪到持久盘。开发机的 `/root` 是容器 overlay 层、重启即失，`~/.claude`、
  `~/.codex`（session 历史、todos）和 shell history 全在里面，实测已经丢过。
  - 目标参数与 `qzcli exec` **同一套契约**：可以直接粘 notebook URL，不必先 ssh 进去
  - **init 是合并不是覆盖**，重启后可放心重跑：新旧 session 取并集、同名冲突保留
    更大的、输的那份进 `.devbox-conflicts/`；配置类冲突以持久盘为准并备份本地那份
  - shell 历史**刻意不软链**（保存历史常用「写临时文件再 rename」，会把软链换成
    真文件、从此静默失效），改为往 rc 文件写 `HISTFILE` 指向持久盘
  - `.ssh` 默认不托管：个人持久目录同组可读，私钥不该放那儿
  - 坑：`/inspire/hdd/global_user` **这一层本身还是 overlay**，只有 `<用户id>/`
    子目录才是持久盘。用 `os.stat().st_dev` 判据把关，指错直接拒绝
  - 自动探测**优先账号级目录**：每台机器都同时挂账号级和项目级个人目录，
    第一版「命中多个就拒绝」等于开箱即用是坏的
- **`qzcli ops`** —— 持久操作日志。此前只有进程内环形缓冲（`deque(maxlen=64)`），
  进程一退就没，导致账号被锁时只能靠 cookie 文件 mtime 猜是谁登的。
  - 记有副作用的操作：`create` / `create-job` / `batch` / `hpc` / `stop` / `login` /
    `exec` / `worker exec` / `res -u` / `remove` / `clear` / `devbox init`
  - **只读命令刻意不记**（`list` / `status` / `avail` / `usage` / `logs` …）——
    高频轮询会一天几千行，把真正要找的提交和登录记录淹掉
  - 挂在**命令分发点**而非逐个 `cmd_*`：单一插入点，新命令进 `RECORDED_OPS` 即自动覆盖
  - 日志跟随 `QZCLI_HOME`；`--merge` 可并入其它 home 的日志按时间排序
  - 硬约束：**绝不记凭据**（只取 argv 前三段）、**写失败绝不让命令挂**

### 修复

- **`res -u` 在鉴权失败时会把本地缓存清空**。链路是
  `_collect_workspace_resources_from_live_apis` 里一个裸 `except QzAPIError: pass`
  把 API 失败吞成空列表，调用方无守卫地把空结果写回磁盘。之后所有命令报
  「未找到计算组」，看着像平台改了名。**实测毁过 2 个工作空间的缓存**。
  现在拉到空且磁盘原本非空时拒绝覆盖并提示先登录；判据只看 compute_groups /
  projects，不看 specs（quick 模式下 specs 本来就是空的）
- **账号被锁时的提示没说怎么恢复**。原话只有「请先解决登录问题再重试」，于是
  所有人都以为要等 —— 而凭据类封锁**不看时间**，只有一次成功的 `qzcli login`
  能清掉。现在提示同时给出：等待无效、确切命令、记录文件路径和记录时间

### 测试

- 新增 40 条，全量 **537 条全绿**
- 新增静态闸门 `test_no_undefined_names.py`：扫描函数里读取的全局名是否真的存在，
  专抓「抄了别的函数里的局部 import 别名」这类错 —— 这类错语法检查和单测都拦不住，
  本轮真机上炸过一次（`_requests is not defined`）
- 关键守卫均做**变异验证**（把实现挖掉确认测试真会变红）

### 真机验证

devbox 端到端 **PASS**：新建开发机 → init → **真停机** → **真启动**（容器重建）→ 核对。
两条**互为对照**的断言同时成立：临时层的标记文件重启后消失（证明确实重启了）、
持久盘上的 session 和 history 还在；重启后重跑 init，新旧 session 都在。

## v0.4.10 - 2026-08-16

### 新增

- **`QZCLI_HOME`** —— 可把**整个状态目录**搬走（`config.json` / `.cookie` /
  `jobs.json` / `resources.json` / `.relogin.cooldown` / `.relogin.lock` 全部由它派生）。
  用于给长跑项目一份互不干扰的独立副本。不设时与 `~/.qzcli` 行为完全一致。
  注意它是**模块级常量、import 时求值**，必须在进程启动前 export
- **看板容量横幅** —— 顶部直接回答「现在能起几个整节点」

### 修复

- 看板的空闲 GPU **排除调度禁用（cordon）节点**；容量横幅与任务侧取保守值 ——
  此前会把 cordon 节点算进可用量，出现过「明细显示空闲 111 卡、横幅显示 0 个整节点」

## v0.4.9 - 2026-08-12

**主题：把登录/鉴权从「四年五层补丁」收口成一条可测量的架构约定。**

### 登录只在主线程发生一次，worker 只干活

根因：每个 API 方法挂 `@with_auth_retry`，撞 401 就自愈。单线程没问题；从 N 个
并发 worker 调用时，cookie 失效那一刻 N 个 worker 同时撞 401、同时打 CAS，
CAS 按失败次数延长锁定 —— 2026-08-12 账号就是这么被锁死的。

历史上为此加过四层保护（进程内锁、跨进程文件锁、按失败 cookie 去重、失败冷却），
全在治「抢着登录时怎么办」，没有一层治「为什么让 worker 去登录」。

本版收口：

- 新增判据 `_relogin_allowed()`：自动重登只在「主线程 or `ensure_authenticated`
  窗口内」发生。worker 线程两者皆非，撞 401 直接抛 `QzAuthExpiredError`，不打 CAS。
- 7 个并发扇出点全部前置鉴权（`ensure_authenticated`），worker 拿已刷新的 cookie 干活。
- 保留主线程自愈 —— 单线程命令 cookie 过期仍自动续、用户无感（不做纯 inspire，
  向后兼容）。

实测：8 线程并发 → 总登录 1 次、**worker 0 次**、发起者 MainThread；
账号锁定态 2h×24 轮命令 → CAS 打 1 次（旧版 24 次）。

### 凭据类登录失败永不自动重试

密码错/账号被锁时，旧版 60s 冷却后又试 —— 每次都是一次失败尝试，把锁定期越拖越长。
本版 `is_credential_failure` 命中即永久封锁自动重登，直到手动 `qzcli login` 成功清除。
`cmd_login` 直连不受封锁约束（账号被锁后唯一恢复途径），有结构测试钉死。

### NO_PROXY 对 qzcli 生效

qzcli 自建 urllib3 pool manager + 登录会话 `trust_env=False`，把 requests 的
`no_proxy` 整个绕开了 —— 环境里只要有代理变量，`no_proxy` 就是摆设（2026-08-10
2224 看板故障）。`get_proxy(url)` 现在按 `NO_PROXY` 判断该不该绕过。

### create 镜像解析：显式 > 平台 > 历史 > 明确报错

默认镜像 `dhyu-wan-torch29:0.4` 已从平台删除、默认 `image_type=SOURCE_PRIVATE`
又和公共 registry 冲突 —— 任何不指定镜像的用户必然 `InternalError: Unauthorized`。
改为：显式传的照用；没传 image_type 查平台可见性；再退回历史真实任务的镜像；
都拿不到就明确报错说清要传什么。

### 结构性防回归

- `tests/test_auth_before_fanout.py`：AST 扫描每个扇出点必须前置鉴权 + 行为测试
  验 worker 撞 401 抛 QzAuthExpiredError（变异验证过）。
- `tests/test_credential_failure_no_retry.py`：凭据不重试 + cmd_login 不被封锁。
- `tests/test_no_proxy_env.py` / `tests/test_image_resolution.py`。

### 测试

443 → 476。真机验证：ws/avail/usage 零重登留痕；create --dry-run 不指定镜像
自动填历史镜像 + SOURCE_PUBLIC。


## v0.4.8 - 2026-08-09

### 勘误：v0.4.7 里两个关于「平台缺口」的结论是错的

v0.4.7 的发布说明、`docs/v1_to_v2_mapping.md`、`api.py` 的 docstring 和
`parity_sweep` 的白名单理由里，都写过下面两条——**都是错的**：

| 原结论 | 实测 |
|---|---|
| 「v2 拿不到点券/预算数据，169 个 action 无替代，点券可见性归零」 | `project GetProjectBudgetUsageOverview` 返回 `{total, used, remain, train, inference, storage}`，比 v1 的单个 `remain_budget` 还细；个人额度走 `GetProjectMemberBudgetUsage` |
| 「平台无 `ListWorkspaces`，只能从项目列表推导工作空间」 | `workspace ListWorkspaces` 一直可用，`{page_num, page_size}` 直接返回完整 items |

**犯错的原因只有一个：把 `qz` CLI 的 spec 当成了 v2 的完整接口面。**
实测 Web 控制台在用 **21 个服务**，而 spec 只收了 11 个；单看 `project` 服务，
spec 里 1 个 action，前端在用 32 个。spec 里完全没有的服务包括
`file` / `audit` / `storage` / `billing` / `sandbox*` 共 12 个。

以后判断「v2 有没有某能力」，不能只查 spec —— 要么真调一次
（未知 action 会明确返回 `InvalidAction: unknown action: <X>`），
要么对照前端产物。

完整取证过程和给平台方的问题清单见
`docs/report_v2_api_spec_gap.md`。

本次只改注释、docstring 和文档，**没有任何行为变更**。


## v0.4.7 - 2026-08-08

**主题：把「不报错的坏」变成能看见的坏。** 这一版没有新功能，全部是消除静默失败 ——
退出码 0、没有红字、输出看着像"今天就是没有数据"的那一类。

### v1 依赖清零：项目列表迁 v2 `GetProjectForPage`

最后两个还在走 v1 的接口（notebook 访问地址、项目列表）都迁完了，
`docs/v1_to_v2_mapping.md` 里「保持 v1」的条目**归零**。

顺带修掉一个真 bug：v1 的 `ListProjects` 会把**已结束且你不在其中**的项目也返回，
选中就报 `AccessForbidden: 您已离开所选项目，无法创建`。

⚠️ **一个要说清的更正**：迁移时我照抄 spec 描述写了「v2 只返回当前用户所属的项目」，
**这是错的**。实测 11 条里有 1 条 `is_member=False`（`某工作空间-探索课题`，
状态 `PASS_MODIFY_RESOURCE`）。需要按成员身份过滤的调用方必须自己滤。

另外 `is_member` **只在 v2 可信**：v1 对全部 12 条一律返回 `False`，
包括你明显是成员的项目 —— 拿 v1 的这个字段过滤会一个都不剩。

### 清掉全部 11 处 `except Exception: pass`

全仓 189 个 except 子句里有 11 个是纯静默吞（裸 `except:` 一个都没有）。
其中两个在吞真错误：

- **`_exec_poll` 的轮询循环**每轮吞掉所有异常，最多吞 120 秒，然后报「命令执行超时」。
  Jupyter 那边如果一直 403 或返回坏 JSON，你看到的仍然是"超时"。
- **`avail` 的 HPC 汇总**把取数和统计包在同一个 try 里，任一空间没权限或字段改名，
  「HPC 节点 CPU/内存利用率」整段消失。

做法是**收窄捕获类型 + 留痕**，不是删掉 try（直接删会让 `avail` 碰到一个 403 空间
就整条命令崩掉）。新增 `qzcli/diag.py`，`QZCLI_DEBUG=1` 时被吞的异常立刻打到 stderr，
平时静默入环形缓冲，供报错时回捞真实原因 —— exec 超时提示现在会带上轮询期间最后
一次失败原因。

收窄立刻炸出一个假绿测试：`test_proxy_dual_stack` 的哨兵异常在逃出来前已被包成
`QzTransientError`，也就是说它一直不知道自己在接什么。

新增全仓 AST 扫描防回归，并做了变异验证（植入犯案文件即红、移除即绿）。

### 项目列表超过 200 个不再被静默截断

平台响应里一直带着 `total`（而且是**字符串** `'11'`），qzcli 从来没看过。
真有账号超过 200 个项目时多出来的会被丢掉，`qzcli ws` 少列几个工作空间，
退出码还是 0。现在按 `total` 翻页拿满，空页立即停，`total` 形状变了就留痕不猜。

### 发版闸门：从「SCHEMA=0」改成「未复核 SCHEMA=0」

`parity_sweep` 的字段名比对以前**只拿第一条记录比**，可选字段会让它随机报假红 ——
实测 `running_time_ms` 在 v1 的 15/20 条、v2 的 14/20 条里都出现，
根本不是 schema 差异。假红比不报还糟，它训练人忽略闸门。改成所有记录的字段名并集。

新增 `REVIEWED_SCHEMA_DIFFS`，命中的降级为 `SCHEMA_REVIEWED`，不让闸门红但仍逐条
印在报告里。CONTRIBUTING 里明确：**不许为了让数字变绿往 `VOLATILE_FIELDS` 里塞东西**。

### 已知缺口（非本仓可修）

`project.GetProjectForPage` 的 `remain_budget` 键在但值恒为空字符串，
`member_remain_budget` 直接没有。目前全仓无消费点，无功能损失。

> **2026-08-09 勘误**：本节原文断言「v2 全 169 个 action 里无替代，点券可见性在
> v2 上归零」。**这是错的**，见 v0.4.8 的勘误条目 —— 预算数据在 v2 里有，
> 只是不在 qz CLI 的 spec 里。

### 发版闸门自身的 4 个 bug（`--submit` 跑了三轮才过）

这一版的 `--submit` 关卡连红两轮，**真正的 qzcli 缺陷是 0 个** —— 全是测试工具
自己的问题，但每一个都值得记，因为它们是同一类：

1. **选计算组不验证可用性**。挑出 GPU 数最小的 spec 后闭眼取
   `logic_compute_group_ids[0]`，而 spec 是从历史任务反推的，记着的组现在可能
   已经不合适。实测 某工作空间 13 个组里 4 个 `node_spec` 数为 0、3 个只收开发机。
   判据改成「该组存在一个 node_spec 同时满足 `support_job_type` 含
   `distributed_training` 且 `gpu_count` 够用」。
2. **停止是竞态且不验证结果**。提交完 0.0 秒就调 `StopJob`，任务还没进入可停止
   状态 → `Conflict` → **任务留在队列里没人管**（事后手工停的）。训练任务那条
   有同样的洞：只断言 stop 返回 True 就完事。两条都改成等到可停止再停、
   停完轮询到终态，到不了终态就明确报出残留任务 id。
3. **我给 2 写的修复自己抄错了词表**。训练任务终态是 `job_stopped`（小写带前缀），
   我照抄了 HPC 的 `STOPPED`，于是任务明明停了却报「可能有残留任务占着卡」。
   **假警报比不报警更糟** —— 报一次而实际没有，下次真有残留时没人会当回事。
   已加 `SmokeTerminalStatusVocabTests` 结构性钉住词表。
4. **输出把优先级教反了**。提交那行写死成「优先级=10(低优)」，而 payload 实际发的是
   `task_priority=1`（平台存成 `priority=11 / LOW`，已核）。这正是本版上游 v0.4.6
   刚更正过的方向，留在输出里等于又教反一遍。改成打印真实值并标明方向。

共同点是：**挑候选时不验证它真的能用，做完动作不验证真的生效** ——
和本版修的项目选取、异常吞没是同一个病根。

### 已知：日志接口平台侧超时

发版当天 `qzcli logs` 稳定撞 60 秒读超时。拿 `master` 的代码在同一时刻打同一个
任务**一模一样地超时**，所以不是本版引入的。同一份代码当天早些时候两轮只读冒烟
里该项分别 4.6s / 9.9s 通过，是平台侧变慢。

### 测试

271 → **443**。


## v0.4.6 - 2026-08-05

### 默认优先级 10 → 3（行为变更，可恢复）

**先说方向**：数字越小优先级越低。实测 **1494 个真实任务**，跨全部工作空间一致：

| 提交值 | 存储值 | 档位 | 现网任务数 |
|---|---|---|---|
| 1 | 11 | LOW | 697 |
| 3 | 13 | LOW | — |
| 4 | 20 | NORMAL | 481 |
| 5 | 30 | HIGH | 2 |
| 9 | 34 | HIGH | 44 |
| 10 | 35 | HIGH | 269 |

**和 HPC 完全同向。** 此前 `api.py` 的注释写着「训练任务的 task_priority 是反的 ——
那边 10 表示低优」，是错的，而且这条错还进了 v0.4.4 的发布说明。照着看的人会把
最高优当低优提上去，直接和生产任务抢卡。已更正。

**默认值改 3 的理由**：不显式指定优先级的，多半是调试 / 试跑 / 脚本随手提的任务。
让这类任务默认拿最高优去抢生产的卡，是不合理的默认。

**向后兼容**：这对「原来不写 `--priority`、靠默认拿高优」的脚本是行为变更 ——
那些任务会从直接跑变成排队。现网 **21% 的任务跑在 HIGH 档**，面不小。所以给了一条
不用改调用点就能恢复原状的路，沿用已有的三级阶梯：

```
QZCLI_DEFAULT_PRIORITY → ~/.qzcli/.env → config.json 的 default_priority → 兜底 3
```

加一行 `QZCLI_DEFAULT_PRIORITY=10` 就完全恢复旧行为。用默认值时会打一行提示说清
用了哪档、怎么改 —— 行为变过就不能悄悄变。

### cookie 落盘不是原子的

`save_cookie` 原本是 `open(path,"w")` 然后 `json.dump`。截断和写完之间有个窗口
文件是空的；并发读的线程这时 `json.load` 失败，而所有读取点都把失败当成"没有这个
文件"，于是 `get_cookie()` 返回 `None`。

**实测后果**：8 个并发登录里偶发 **2 次真实 CAS 登录** —— 某个线程在窗口里读到
`None`，`_relogin` 的去重判据失效，又打了一次 CAS。而反复登录正是把账号推进验证码
锁定的动作。别处则可能表现为莫名的「未设置 cookie」。

改成临时文件 + `os.replace`（同文件系统原子）。token 缓存同样处理。

**发现过程值得记**：是并发用例随机红了一次，没当抖动放过去，插桩查到两次登录来自
同一处，才挖到根因。认证相关的用例一旦 flaky 就会被学会忽略，而这次它指向真问题。

### 规格解析：卡型跨计算组抄错

两层都有问题：`_fill_gpu_type_from_history` 按 quota_id 匹配历史但不看计算组；
`_lookup_spec_for_payload` 读缓存也不看归属。结果是给 H200 组提交却填
`NVIDIA_H100_SXM_80G`。

**这比报错更糟**：任务会一直排队等一种该组里根本不存在的卡，看起来"成功进入排队"，
实际永远起不来 —— 正好骗过"能排队就算通过"这类验收。

修法：历史按计算组过滤；查不到则用**该计算组节点的真实卡型**兜底（卡型是机器属性，
节点才是权威来源，对没跑过任务的新组也有效）；缓存记录不属于目标组时重新解析。

实测「训练区-H200-1号机房」+「8卡160核」：`H100` → `''` → **`NVIDIA_H200_SXM_141G`**，
与该组 180 个节点一致。

### 规格归属：用平台给的字段，别自己假设

`_specs_from_schedule_config` 读平台的 `predef_train_spec`（工作空间级的一整张规格
表），然后给**每一条**都盖上「属于目标计算组」的戳，注释还写着「规格是工作空间级的，
对该空间任一计算组都可用」。**平台不认这个假设** —— 拿别的分区的规格提交会被直接拒
（`quota_id ... does not belong to logic_compute_group ...`）。

而 `predef_train_spec` 每条记录**本来就带 `logic_compute_group_ids`**，明确说了归属。
实测 5/5 与平台的接受/拒绝完全吻合。

**后果不只是列表不准**：自动选规格挑 GPU 数最小的那个，而小规格恰恰常常属于开发
分区 —— 于是在训练分区上 `qzcli create` **不带 `--spec` 会选中一个必被拒的规格**。
实测「训练区-H200-1号机房」自动选中 `1卡10核`，那条属于开发区。也就是 v0.4.3 修好的
「不带 --spec 能提交」，在训练分区上仍然是坏的，只是报错文案换了一个。

效果：训练区-1号机房 11 个规格（含 7 个别的分区的）→ **4 个真实可用**。

**顺带带出领域规则**：同一份数据里还有 `allowed_priority_levels`。实测训练区的
8卡160核 是 `[]`（不限），而 1/2/4 卡散卡是 `['low']`（只能跑低优）；开发区则全部
不限。即「开发分区支持散卡，训练分区上散卡只能低优」。这个字段以前被整个丢掉，
现在一并带出来。

### 其它

- `README.md` 快速开始里的 `qzcli ls -c -r` 跑不通（`-c` 模式必须带 `-w` 或
  `--all-ws`）。这条错误还被抄进了给新同学的手册 —— 写文档抄 README 而不实跑，
  就是这个下场。
- `live_smoke` 新增「低优大任务能进排队」用例，带账号门控
  （`QZCLI_SMOKE_QUEUE_ACCOUNT`），并硬断言规格卡型与该计算组节点实际卡型一致 ——
  防的正是上面那种"假通过"。

### 测试 379 → 410

新增：优先级方向与默认值（含可覆盖的兼容通道）、cookie 原子写（并发读永远读不到
空值、并发写不互删临时文件）、规格归属与卡型来源。多条经过变异验证。

## v0.4.5 - 2026-08-04

主题是**登录治理**。用户报「登录多了就会挂」，系统审计下来真正的元凶不是
「session TTL 太短」，而是两处让登录白白发生的 bug。

从用户 shell 历史里挖到的数字最能说明问题：**`qzcli login` 被敲了 299 次**，
是所有 qzcli 命令里最多的。

### 分页循环里每页都在完整重登

`_relogin` 的去重判据是「盘上 cookie **相对我进函数那一刻**有没有变过」。而分页
函数把 cookie 闭包了，整个循环共用一个字符串：

1. 第 1 页 401 → 重登成功 → 盘上换成新 cookie
2. 第 2 页**仍用闭包里的旧 cookie** → 又 401
3. 此时 `stale` 读到的已经是新 cookie，`current == stale` → 去重判定为
   "没人刷新过" → **再打一次完整 CAS**

**N 页 = N 次登录。** 而 CAS 正是按登录次数判定异常并锁验证码 —— 这就是
"用着用着就说要验证码"的直接来源。`avail` / `usage` / `res -u` 全中。

两处一起改才堵死：判据改成「盘上 cookie ≠ **刚刚失败的那个**」；分页不再闭包
cookie，每页回源读盘。只改判据能把 CAS 压到 1 次但每页仍白撞一次 401；
只改分页挡不住线程错峰。

思路来自 **inspire-skill**：它的凭据是**可变对象**（`WebSession`），重登后
`_refresh_session_in_place` 原地改写调用方手里那个对象，持有引用的循环下次自然
用上新值。qzcli 全链路传裸字符串，对象化改造面太大，因此用「每次回源读盘」拿
等价效果。（顺带澄清：inspire-skill **没有**主动续期，`SESSION_TTL` 是死代码，
被 `load(allow_expired=True)` 绕过；它也**没有任何并发保护**。）

真机验证：7 页节点分页（698 节点）→ **登录 1 次**（修复前 7 次）。

### `qzcli create` 每次必然白登一次

`list_projects_raw()` 调用时不传 cookie，而 v1 这条路**没有磁盘兜底**（v2 的
`_request_v2` 有），空串直接进 header → 必然 401 → 触发一次纯属浪费的完整 CAS
登录。用户感觉"create 偶尔很慢"就是这个。

真机验证：不传 cookie 调用 → **登录 0 次**（修复前 1 次）。

### 429 被吞掉后转打 v1（同一个坑的第四处）

`get_job_detail` 用裸 `except QzAPIError: pass`，而 `QzRateLimitError` 是它的
子类 —— 429 被静默吞掉、转头再打一发 v1，**等于平台喊"慢点"时把请求量翻倍**。
`_v2_then_v1` 里明令禁止过这件事，但这是另一条独立路径。

### 其它同类修复

- **MCP `qz_auth_login` 零保护直连 `login_with_cas`**。`cmd_login` 修过、
  `_refresh_cookie_for_interactive` 修过，这是第三处。改走 `_relogin`。
- `_project_list_items` 上同一组装饰器**挂了两遍** → 429 重试 4×4=16 次。
- `cmd_login` 成功后不清失败冷却，手工登录成功后 60s 内自动重登仍被挡。
- 缓存里工作空间的值不是 dict 时（半截写入 / 手工编辑），
  `get_workspace_resources` 原样返回字符串，下游 `.get()` 抛 `AttributeError`
  **把整条命令打崩**。
- 两个 MCP tool 没写 description（`qz_create_hpc_job` / `qz_get_hpc_usage`），
  模型侧无从判断何时调用。

### 测试：318 → 379

- **缓存残缺矩阵**。契约是三态：`True` / `False` / `None`（缓存无从判断）。
  线上那批 bug 全是同一个形状 —— **该返回 `None` 时返回了 `False`**。
- **10 个零覆盖命令**（`remove` / `clear` / `track` / `import` / `cookie` /
  `watch`）。重点钉否定路径：回答 `n` 必须真的什么都不删、cookie 验证失败
  **绝不能**落盘顶掉正在用的好 cookie。
- **`mcp_server.py` 17 个 tool**，此前零测试。它是与 CLI 并列的第二个用户面，
  但几乎完全是平行重实现 —— CLI 的测试完全不保护它。
- **代理双栈一致性**。一条勘察结论被实测证伪：「只设 `HTTP_PROXY` 时两栈行为
  相反」不成立，两栈对 https 流量都正确忽略它。照报告去"修"反而会制造真分叉。
- **`live_smoke` 补 3 条默认形态**：`hpc-usage` / `list -c --all-ws` /
  `res -u`（8 线程扇出，429 风险最高）。

### 新工具：按用户真实命令做差分回放

`tools/replay_history.py` —— 从 shell 历史解析出真实命令分布，在两个代码版本上
分别回放并比对。这个项目栽过的坑根子都是"测我构造的路径，不测用户实际怎么用"。

比对基准换过三次：直接 diff 输出、抹掉数字、比行模板集合 —— **同一个版本自比
都通不过**（集群 99.9% 利用率，表格行数和列宽都在变）。结论是渲染后的表格不适合
当回归信号，改成比行为属性：退出码、429/权限噪声/异常栈计数、**触发了几次重新
登录**、以及结构锚点（不含数字的行）。自比对 0 差异之后，基准才站得住。

首轮：dev vs master 回放 10 条真实命令，**失败 0、差异 0**。

## v0.4.4 - 2026-08-01

一次真实事故驱动的版本：用户报「`qzcli login` 用正确密码却说需要输入验证码」，
跑去浏览器一看根本没有验证码可过。查下来是两个独立问题叠在一起。

### 「需要输入验证码」是假错误

判据是这一行：

```python
if "验证码" in resp.text:
    raise QzAPIError("需要输入验证码，请在浏览器中登录后手动获取 cookie")
```

而 CAS 登录页**永远**含"验证码"三个字 —— 抓真实页面数过，**整整 5 处**，全部
来自旁边那个「短信验证码登录」标签页的固定文案（`<h3>验证码登录</h3>`、
`placeholder="验证码"`、`发送验证码`、`动态验证码`）。其中那个图形验证码
`<img>` 指向的还是 `mapp.suda.edu.cn`（苏州大学），是模板里没清干净的死代码。

于是**任何**退回登录页的失败都被翻译成"需要输入验证码"，真实原因完全没被说出来，
反而诱导用户反复重试 —— 而重试正是让情况变糟的动作。

改成只读 `<div class="form-error">` 里的文案。拿到真实文案后才知道确实存在验证码，
但那是**短时间内登录失败几次后 CAS 才临时打开的**，等几分钟自行恢复。提示语相应
改成「等几分钟重试」，并指出已保存的 cookie 若仍有效根本无需重新登录 ——
而不是像以前那样叫用户去浏览器手工取 cookie（那等于承认密码登录坏了）。

### 自动重登在并发扇出下把账号打进验证码

`_refresh_cookie_for_interactive` **直接调 `login_with_cas`**，绕过了 `_relogin`
的全部三层保护（进程内锁、跨进程文件锁、拿到锁后重读 cookie 的去重）。

而它被 `_with_live_cookie` 调用，后者出现在 **11 个命令**里，其中 `avail` /
`usage` / `list -c` 都是按计算组**并发扇出**的。cookie 一过期，一条
`qzcli avail` 会朝 CAS 打出十几次并发登录。**实测：16 线程 → CAS 被打 16 次。**

v0.4.1 加的锁只覆盖了 `_relogin` 和 `qzcli login`，恰好漏掉这条最常触发的路径。

**失败路径还要单独处理**：登录失败时没有新 cookie 落盘，"重读看别人是否已登好"
的判据就永远为假，于是每个等在锁上的线程都会各自再打一次 —— 8 线程 8 次**失败**
尝试。而 CAS 正是按失败次数延长锁定的，等于自动重登把自己越锁越死。加了 60s 失败
冷却（进程内 + 跨进程冷却文件），成功即清除。

### 缓存写坏会把命令打崩

缓存里工作空间的值不是 dict 时（半截写入 / 手工编辑 / 老格式残留），
`get_workspace_resources` 原样返回字符串，下游 `.get(...)` 抛 `AttributeError`。
所有调用方本来就按"`None` 表示没缓存"处理，当作没缓存即可。

### 测试

**318 tests**（v0.4.3 是 271）。新增三组：

- **沙箱 HOME + 13 种缓存残缺态 fixture**。此前构造残缺缓存只能拿真实
  `~/.qzcli` 做实验，跑挂一次就把登录态留在半坏状态。结构照真实
  `resources.json` 抄的 —— 第一版照文档推的全错（顶层是扁平映射不是
  `{"workspaces": [...]}`，三个集合都是按 id 索引的 dict 不是 list）。
- **缓存残缺矩阵**。契约是三态：`True` / `False` / `None`（缓存无从判断）。
  线上那批 bug 全是同一个形状 —— **该返回 `None` 时返回了 `False`**。
- **并发重登**。`avail` 本来有 5 个单测、`live_smoke` 也跑它，但**没有一个是在
  cookie 过期的前提下跑的**，而放大只发生在认证失败那条路上。

### 平台侧缺口（已记入功能点表）

实测 `inspire-session` 是会话级 cookie（无 Expires/Max-Age），**约 20 分钟即失效**
（两次分别测到 18.6 / 22 分钟）；且服务端**不下发续期 cookie** —— 连打 3 次 v2
接口外加 v1 对照，`Set-Cookie` 全部为空，即不是滑动过期。会话很快到期 → 客户端
必须频繁重登 → 重登又容易触发验证码。客户端这侧能做的已经做了，会话 TTL 属平台
侧策略。

### 流程

`master` 改为只收发布，日常改动走 `dev`（启智不少生产基建直接依赖 `master`）。
进 `master` 的门槛写成硬清单：单测 + `live_smoke` + `parity_sweep` 的 SCHEMA
差异必须为 0 + 发版前跑一次 `--submit`。

本次发版实跑记录：318 tests / parity **0 处差异（SCHEMA 0）** / live_smoke **17/17**。

`parity_sweep` 顺带把 `tasks_associated`、`users_associated` 归入波动字段 ——
定性方法是**拿 v1 跟 v1 自己比**：同源相隔 150 秒就有 2/100 个节点在变，
而同一时刻 v1 vs v2 是 100/100 一致，说明差异来自采样时刻不同。

## v0.4.3 - 2026-07-31

一次系统性审计挖出的 5 个问题，全部是「看起来在工作但实际没起作用」。
每个都先写会红的复现用例、再修到绿。

### create 相关（两个和之前修过的是同构代码）

- **项目归属校验只修了一半**：项目和计算组的归属校验本来是同一套逻辑，
  v0.4.2 只给计算组加了平台复核。于是**新建/新加入的项目会原样重演那个 bug** ——
  报「项目 X 不属于当前工作空间」，而它其实属于。补上同样的平台复核
  （数据源 `/api/v1/project/list` 的 `items[].space_list[]`）。
- **不带 `--spec` 在 15/16 个工作空间是坏的**：`res -u` 默认 quick 模式**明确
  不产出 specs**，所以 `specs={}` 是默认稳态。自动选规格只看缓存 → 绝大多数
  工作空间直接报「未指定资源规格且缓存中无可用规格」。接上 v0.4.0 已有的平台
  规格表，且挑 GPU 数最小的（别默认占最大机器）；缓存有则维持原行为。

### 另外三个

- **`batch --dry-run` 完全不校验资源**：展开完模板就跳过，压根不走 `cmd_create`，
  workspace / project / compute-group / spec 能不能解析一概不管。用户拿它当
  提交前预检必然翻车。改成走完整链路，并**把 `dry_run` 透传下去**
  （原来写死 `False`，不透传的话这个修复会让预检变成真提交）。
- **跨进程重登锁没覆盖 `qzcli login` 本身**：v0.4.1 的锁只保护自动重登，
  显式 login 没拿 —— 多个 agent 同时敲 login 仍会并发撞 CAS。
  实测修复后 **5 个进程并发只打到 CAS 1 次**。
- **`live_smoke` 解包了 rc 却从不断言**：命令 exit 1 但输出恰好没有关键词
  就算通过，用例可以静默失效。

### 新增：v1/v2 全量对齐扫描（`tools/parity_sweep.py`）

趁 v1 还没下线，把两边的语义差异一次性挖出来，而不是等用户撞 bug 反推。
全部已迁端点 × 全部工作空间 × 逐字段逐值，SCHEMA 类差异用退出码暴露，可当 gate。

**首次全量结果：96 对比对，0 处真差异。** 唯一发现的实质差异是
`gpu_info.brand_name`（v1 空串 / v2 "英伟达"），代码不读该字段，已核实无害。
**SCHEMA = 0**，即不存在「字段改名导致静默返回空」这类隐患 ——
v1 下线对这些端点是安全的。

271 tests passed。

## v0.4.2 - 2026-07-31

### 新建的计算组会被误判成「不属于当前工作空间」

`create` 校验计算组归属时**只看本地缓存**。而缓存总会过期 —— 新建的计算组在
刷新前必然查不到，于是一个**真实存在、此刻正跑着千卡任务**的计算组被判成
「不属于当前工作空间」。这句报错本身就是错的，而且它建议的 `res -u` 也未必
解决（缓存刷新有自己的失败模式）。

改为：缓存说「没有」时**跟平台再确认一次**
（`workspace ListLogicComputeGroups` 是权威来源、不依赖缓存）：

- 平台确认存在 → 放行，并提示缓存已过期
- 平台确认不存在 → 照常拒绝（报错措辞改成「已向平台确认」）
- **查询失败 → 按「不确定」放行**，让平台自己去拒 —— 总好过拿过期缓存误伤

注意这和 v0.4.0 修的规格解析是**两个不同的故障**：那个报
「无法解析规格 ... 的 cpu/gpu/memory」，这个报「计算组 ... 不属于当前工作空间」。
两处都得治。

258 tests passed。

## v0.4.1 - 2026-07-31

紧急修复三个线上问题，全部来自真实用户反馈。

### `qzcli avail` 默认形态全线 HTTP 429

不带 `-w` 时是「工作空间 × 计算组」的嵌套并发，撞上 APISIX 限流。

根因是 v0.4.0 的回落逻辑把问题放大了：APISIX 限流返回 **429 + HTML 错误页**，
`_request_v2` 先嗅 content-type 判成「返回非 JSON」→ `_v2_then_v1` 当成路由不通
→ **回落 v1** → v1 也 429 → 全灭。**平台在喊「慢点」，代码却把 QPS 翻倍。**

改为：新增 `QzRateLimitError`，在 content-type 嗅探**之前**判 429；退避重试
（优先听 `Retry-After`，否则 1s→2s→4s 叠抖动）；**429 绝不回落 v1**。
v1 的 12 个请求点同样识别 429。

### 多 agent 并发把账号撞进 CAS 验证码

`_relogin` 只有进程内的 `threading.Lock`，而每次 `qzcli` 调用都是独立进程。
cookie 一过期，N 个进程同时撞 CAS，被判为异常登录要求验证码，
**所有人一起被锁在外面**，连「自动重登」本身也失效。

新增 `~/.qzcli/.relogin.lock` 文件锁（`flock`，进程被 kill -9 时内核自动释放）。
拿到锁后重读盘上的 cookie —— 别的进程可能刚登好了，全程只发生一次 CAS 登录。
实测 8 个并发进程只触发 1 次登录。

### 已禁用/无权限的工作空间刷屏

`avail` / `usage` / `hpc-usage` 从**本地缓存**枚举工作空间，v0.4.0 的
`usage_status` 过滤只在 `list_workspaces` 生效、没管缓存这条路，于是每次都去查
这些空间并刷一屏 `AccessForbidden`，把真问题淹掉。改为撞到就打标、后续跳过，
`res -u` 重刷时清标记。实测警告 5 条 → 0。

### 测试方法论

上面第一个问题暴露的是方法论漏洞：**此前所有用例都显式指定单个 workspace，
从没跑过默认形态** —— 而并发放大只在默认形态下出现。

`tools/live_smoke.py` 新增「CLI 默认形态」段，**跑真命令、用默认参数**，
不再走 API 层捷径；并新增「连续调用 3 次不触发限流」用例
（限流是累积的，单次跑通不代表连续跑通，而 agent 场景下同一命令会被反复调用）。

254 tests passed。

## v0.4.0 - 2026-07-30

平台把 `/api/v1` 逐步下线，本版把 qzcli 的接口层迁到 `/api/v2`，并修掉多 agent 共用开发机时的一批并发问题。**没有 CLI 破坏性变更** —— 所有子命令的用法和输出形状保持不变。

### 平台接口 v1 → v2（v2 优先 + v1 兜底）

官方 CLI `qz` 已是纯 v2 客户端（11 services / 144 actions），而 qzcli 此前 16 个平台端点里只有 2 个在 v2。**v1 衰减不是假设**：`/openapi/v1/specs/list` 已经 404，qzcli 一直在静默回落本地缓存，而它正卡在 `qzcli create` 的关键路径上。

公开方法签名一律不变，内部拆成 `_xxx_v2` / `_xxx_v1` 两条腿，由 `api._v2_then_v1` 分发。已迁：`train ListJobs / GetJob / StopJob / ListJobEvents`、`notebook ListNotebooks`、`workspace ListNodeDimension / ListTaskDimension / GetBasicInfo / GetOverviewTaskMetric`、`hpc ListJobs`。

**回落判据是刻意收紧的**：只有路由不通（404/405/50x、被网关 302 成 HTML）才回落。业务错误（`AccessForbidden`、`InvalidParameter`）直接抛 —— 回落只会把权限问题和我们自己的请求 bug 一起藏起来。

三条只有真机实测才能发现的事：

- **`cluster.*` 对普通账号一律 `AccessForbidden`，必须走 `workspace.*` 双胞胎。** 两者在 `qz spec` 里的描述几乎一模一样，照字面映射会全线踩坑。
- **v2 没有任何 action 返回 `spec_id`**（144 个 action 的 schema 全 grep 过），`node_specs[]` 只有硬件参数没有 id。`list_specs` 改为从历史任务的 `framework_config[].instance_spec_price_info.quota_id` 反推 —— 这是目前唯一能从 v2 拿到真实 spec id 的路径。副作用是 `qzcli res -u` 把 specs 刷空后 `create` 仍然可用（此前会报「无法解析规格」）。
- **`inference-serving` 整个 service 在网关上 404**（spec 声明 18 个 action，路由没挂）。qzcli 不用它，不影响。

**两处仍在 v1**（v2 确实没有对应能力）：`/api/v1/project/list`（v2 的 `ListProjects` 普通账号无权限，且全域没有 `ListWorkspaces`）、`/api/v1/notebook/lab/{id}`（v2 拿不到 Jupyter 访问地址，`qzcli exec` 依赖它）。

### 多 agent 并发 exec：session 隔离

多个 AI agent 常同时对**同一台开发机**跑 `qzcli exec`，此前会互相串数据。修了三层：

- **job_id 撞车（主因）**：原来只有秒级时间戳，同一秒内起的多个 exec 拿到**完全相同的 job_id**，共用一个输出文件互相覆盖。3 路并发实测：第 3 路收到第 2 路的输出、第 1 路什么都没收到。现在 job_id 是 `qzcli_<session>_<ts>_<随机>`。
- **抢终端**：原来复用 `terms[0]`，不只 agent 之间互抢 —— 实测有开发机躺着 4 个别人两天前的人工交互终端，exec 会挑中第一个直接往里发命令。现在每次自建终端、用完删掉，命令用 `setsid`（无则 `nohup`）摘出去以免被带走。
- **文件永久泄漏**：清理只发生在拿到 exit 文件时，`--detach` 后没 attach 的、Ctrl-C 的、超时的全都一直留着。现在输出按 `/tmp/.qzcli/<session>/` 分目录，超过 7 天的旧 session 目录在下次 launch 时自动清理。

新增 `QZCLI_SESSION_ID`（env → `.env` → `config.json` → **按进程自动生成**，与凭据同一套优先级）。不设也不会串车。

新增 **`qzcli exec --list`**：列出开发机上的 exec 任务，默认只列本 session，`--all` 看全部。补上「`--detach` 之后忘了记 job_id 就再也找不回来」的缺口。

老格式 job_id（无 session 段）仍能 `exec-attach`，会回落到平铺路径。

### 修复

- **`qzcli hpc` 此前完全不可用**：平台新增必填 `priority`，不传直接被拒 `priority must be set`。已补 `--priority`。⚠️ **HPC 的优先级方向与训练任务相反** —— 实测提交 1→LOW(11)、3→LOW(13)、5→HIGH(30)、10→HIGH(35)，数字越大越高（有效 1-10），而训练任务的 `task_priority` 是 10 表示低优。默认取 1（LOW），与集群现有生产 HPC 任务一致。
- **`qzcli ws -w <中文名>` 直接崩**（`'latin-1' codec can't encode`）：`cmd_workspace` 拿 `-w` 的值当 workspace_id 用、从不解析名字，中文名原样拼进 referer 头导致请求发不出去。改为复用已有的 `_resolve_workspace_value`。
- **已禁用的工作空间会混进列表**：`project/list` 会把你不是成员的项目也返回，其 `space_list` 里可能挂着已禁用空间（v2 报 `AccessForbidden: 该空间已被禁用`，v1 反而返回陈旧集群结构）。`list_workspaces` 改为按 `usage_status != 0` 滤掉并提示跳过了哪些。
- **MCP 与 CLI 的提交路径分叉**：CLI 早已默认 v2 `CreateJobConsole`，`mcp_server` 还停在 v1，导致 v2 才支持的 `exclude_nodes` 在 MCP 侧静默失效。已统一。
- **`create_job_v2` 漏发 `x-inspire-client-source`**（缺它 APISIX 会把请求 302 到 Keycloak），且没挂 `@with_auth_retry`（提交中途 cookie 过期直接失败）。折进 `_request_v2` 后一并修好。
- 26 处硬编码 `https://qz.sii.edu.cn` 的 origin/referer 改用 `self.base_url` / `api.base_url`，非默认 `QZCLI_API_URL` 下不再发出错配的 Origin。

### 新增工具与文档（进仓，可复现）

| 文件 | 用途 |
|---|---|
| `tools/gen_api_spec_doc.py` | 扫 `qz spec`/`schema`/`--dry-run`，生成全部 144 个 action 的接口文档 |
| `tools/probe_v2.py` | cookie 可用性探针，只打只读 Action，产物自动对 UUID 打码 |
| `tools/compare_v1_v2.py` | v1/v2 逐字段 diff，防「静默返回空」 |
| `tools/live_smoke.py` | 活体冒烟，每个功能点在真实平台跑一遍（`--submit` 含真实提交+停止） |
| `docs/api_spec_v2.json` | 结构化接口定义，**平台改接口后 `git diff` 就能看出变了什么** |
| `docs/v1_to_v2_mapping.md` | 端点映射表（真机实测 + 踩坑 + 平台侧缺口） |
| `docs/v2_probe_report.md` | 109 个只读 Action 的 cookie 可用性结果 |

### 升级说明

- **无需改任何调用方式**，CLI 子命令和输出形状不变。
- `api.list_specs()` 多了一个可选参数 `workspace_id`（历史任务反推需要按工作空间查）。直接调用 `qzcli.api` 的代码不受影响；如果你 mock 或子类化了它，签名要跟着加。
- `QzAPIError` 新增 `api_code` 属性，承载 v2 信封里的 `ResponseMetadata.Error.Code`，用于按结构判断错误类型而不是抠错误文案。
- 多 agent 场景建议显式设 `QZCLI_SESSION_ID`；不设则每进程自动一个。

### 早前已在 Unreleased 中记录的改动

- **`qzcli dashboard` 成分下钻可视化看板 (P1)**: 新增子命令，用 Streamlit + plotly treemap/sunburst 把工作空间的在跑 GPU 占用按「**计算组(机房) → 优先级档 → 项目 → 用户 → 任务**」逐层下钻（块面积 = GPU 数，点块放大、面包屑退回），一眼看清「各计算组里谁占最多、各自高低优」；配色可切优先级/类型/**GPU 利用率**（暴露申请多却空转的任务）。关键在于计算组归属：`list_task_dimension` / `list_node_dimension` 都不直接带 logic_compute_group，改为逐 lcg 用 `list_node_dimension(logic_compute_group_id=…)` 反建「节点→计算组」映射（与 `avail` 同法），再经 `nodes_occupied` 挂到任务，实测 100% 覆盖。共享数据层 `fetch_all_task_dimensions` / `build_node_to_lcg_map` / `task_dimension_to_row` 落在 `cli.py`，`cmd_usage` 复用（输出不变）。下钻层级、视图（treemap/sunburst/icicle）、配色维度均可现场切换：配色支持 优先级/类型/**GPU 利用率**（红=申请多却空转）/**运行时长**（越久越红，按 95 分位截断避免超长任务拉平色阶）。工作空间用**下拉框**选（读本地 `resources.json` 缓存）；顶部有**按任务类型占比**行（交互式建模/训练/推理各占多少 GPU），以及**已占用/空闲 GPU** KPI；勾选「叠加空闲 GPU（灰块）」把各计算组的**剩余容量**（节点 `gpu.total − used` 聚合）以灰块叠加。悬停任意块给出干净明细卡片（任务数/类型/GPU 加权平均利用率/最长运行时长），对内部节点也做了逐层聚合（不再是 px 默认的 `NaN`/`(?)`）。任务分页与逐 lcg 节点查询用 `ThreadPoolExecutor` **并发拉取**（分布式空间首屏 ~17s → ~5s），`cmd_usage` 同样受益。看板依赖走可选 extra：`pip install 'qzcli[dashboard]'`。
- **Cookie 过期自动重登 (P0)**: 所有 cookie 认证的 API 方法（`*_with_cookie`、`_request_v2`、HPC/节点维度查询等）现在用 `@with_auth_retry` 装饰——遇到 401 会用本地凭据透明地 `login_with_cas` 重登一次并重试，消除了此前在长会话 / 自动化中每隔 ~20 分钟手动 `qzcli login` 的反复操作。无凭据或重登失败时回退到原有行为（如 token 认证）。`exec` 取 Jupyter 连接信息时同样会在 cookie 过期时自动重登。
- **CAS 登录重试退避 (P1)**: `login_with_cas` 现在对瞬时故障（SSL `UNEXPECTED_EOF_WHILE_READING`、连接重置、CAS/代理 5xx）做指数退避重试（最多 3 次）；用户名密码错误等永久性错误立即抛出、不重试。新增 `QzTransientError`（`QzAPIError` 子类）用类型而非文案标记可重试错误。
- **`exec` 分离式后台执行 (P1)**: `qzcli exec --detach`（别名 `--no-wait`）后台启动命令并立即返回 `job_id`；`qzcli exec-attach <target> <job_id>` 重连并继续拉取输出。`exec` 超时不再丢弃输出，而是保留远端文件并打印可直接复制的 `exec-attach` 续读命令。底层 `_exec_via_jupyter` 拆分为 `_exec_launch` / `_exec_poll`。
- **新增 MCP 工具 `qz_exec` / `qz_exec_attach`**: agent 无需 shell-out 即可在开发机执行命令；`detach=True` 用于编译、下载、训练等长命令，配合 `qz_exec_attach` 轮询结果。两者共享上面的 cookie 自动重登。
- **`exec` / `exec-attach` 的 target 支持 notebook_id 及其前缀**: 除了名字、完整 UUID、URL，现在也能直接粘贴 notebook_id 或它的一段前缀；`_resolve_notebook_id_by_name` 先按 name/notebook_id 精确命中，未命中再按 notebook_id 前缀模糊匹配——前缀唯一才解析，撞到多个时列出候选并报错（不默默取第一个）。CLI 与 MCP `qz_exec` 共用此解析路径。

## v0.3.0 - 2026-05-28

Second tagged release. Two breaking changes around resource refresh and the job-creation payload, plus exec polish and several reliability fixes since v0.2.0.

### Highlights

- **`qzcli exec` now production-ready for pasted targets**: accepts dev-machine name, notebook UUID, **or** a full IDE / Jupyter URL pasted from the browser, with new `--timeout` flag and a user_ids-filter fix that previously hid other people's dev machines.
- **Much faster `qzcli res -u`**: default-flip to quick mode + parallel workspace refresh drops a full cache refresh on 19 workspaces from "hangs indefinitely" to under a minute.
- **`qzcli create` migrated to the new `resource_spec_price` schema**: the platform rejected the legacy `framework_config[0].spec_id` field with `unknown field "spec_id"`; new payload also promotes cpu / mem_gi / gpu_count to satisfy the platform's `Cpu and Mem can't be empty.` check.

### Breaking changes

- `qzcli res -u` now defaults to **quick mode** — skips the unbounded historical-jobs walk and pulls `compute_groups` / `projects` directly from `get_cluster_basic_info` / `list_task_dimension`. `specs` are no longer refreshed in the default path. Pass `--full` / `-F` to opt back into the legacy full scan (still required when you need fresh spec ids for `cmd_create` non-interactive). The `--quick` / `-q` flag is preserved as a no-op for backward compatibility with existing scripts. `create -i` still uses the full scan internally because submission needs spec ids.
- `qzcli create` and the `qz_create_job` MCP tool now build the `/api/v1/train_job/create` payload using the platform's new `resource_spec_price` schema; the legacy `framework_config[0].spec_id` field is no longer sent. If you were assembling payloads by hand against an older platform build, regenerate them.

### What's new

- `qzcli exec`: target argument now accepts dev-machine name, notebook UUID, **or** a full IDE / Jupyter URL pasted from the browser (`/ide?notebook_id=...`, `/jobs/interactiveModel(ing)?Detail/...`, `/jupyter/...`, `/api/v1/notebook/lab/...`, `/notebook/(lab|code)/...`); add `--timeout` flag (default 120s, must precede `target` because `remote_cmd` uses `argparse.REMAINDER`); drop the `user_ids` filter on the underlying `list_notebooks` call so name resolution no longer hides dev machines whose `created_by` differs from the caller. See the new "远程执行 / 开发机命令" section in README for usage. (aad08d0, #29, #30)
- `qzcli res -u` now refreshes workspaces **in parallel** via `ThreadPoolExecutor`. Default `--parallel 8`; pass `--parallel 1` to recover the old sequential behavior (useful when debugging or when a workspace's API is misbehaving). Disk writes (`save_resources`) still run on the main thread as results land, so there's no read-modify-write race on `~/.qzcli/resources.json`. Combined with the default-quick flip, full `res -u` on 19 workspaces drops from "hangs indefinitely" to under a minute end-to-end on this machine.
- `qzcli res -u` now shows a live progress bar during workspace cache refresh, sharing the rich `display.create_progress()` pattern from `qzcli avail`.
- Reapply most of PR #23 (cookie auth for `get_job_detail` / `stop_job`, `qz_get_hpc_usage` unpacking fix, `qz_track_job` error propagation). The `_get_token` encrypt-password change is intentionally *not* reapplied — the `{"encrypted": True}` flag is not part of `/auth/token`'s documented contract and likely doesn't help CAS-federated users with `invalid_grant` anyway. Issue #14 remains open.
- `qzcli create` now prefers cookie auth (`/api/v1/train_job/create`) by default and only falls back to the openapi token path when no cookie is available — this aligns the CLI with `qz_create_job` and unblocks CAS-federated users who previously got `invalid_grant`.
- New public helper `qzcli.api.build_resource_spec_price(spec_obj, compute_group_id)` shared by CLI and MCP. New CLI helper `_lookup_spec_for_payload` auto-refreshes the spec cache when cpu/gpu/memory fields are missing, and gives a `qzcli res -u` hint if still unresolved.
- Documentation: README now documents `qzcli exec` in the 任务管理 table and a new 远程执行 / 开发机命令 subsection (was completely missing despite shipping in v0.2.0). (#30)

### Upgrade notes

- No Python version change (`>=3.8`).
- If you rely on `qzcli res -u` populating `specs`, add `--full` to your refresh script.
- If you assemble `/api/v1/train_job/create` payloads yourself, switch to the new `resource_spec_price` schema; `framework_config[0].spec_id` is now rejected by the platform.

### Validation

- `python3 -m compileall qzcli tests`
- `python3 -m unittest discover -s tests`

## v0.2.0 - 2026-05-04

qzcli v0.2.0 is the first tagged release for the project, collecting the recent work on job logs, WSL/VPN networking, faster capacity checks, MCP integration, interactive workloads, and HPC/CPU job submission into a versioned release.

## Highlights

- WSL/VPN Proxy Support: Add config-driven SOCKS and HTTP(S) proxy routing across OpenAPI calls, cookie-authenticated APIs, `/api/v2` calls, and CAS login, including explicit handling for WSL/Clash setups where `HTTPS_PROXY=http://...` can override a SOCKS proxy, plus a declared `PySocks` dependency for reliable installation: #13
- Fast Availability Queries: Preserve cached fuzzy workspace matching, avoid slow full workspace scans for targeted `qzcli avail -w` queries, parallelize node/task dimension fetches, increase low-priority task pagination, add progress display, and reduce measured local `qzcli avail` runtime from about 49.7s to about 5.9s in the optimized path: #17
- Job Logs CLI: Add `qzcli logs <job-id>` backed by `/api/v2/train?Action=GetJobLog` with cookie authentication, chronological tail output, `--tail`, `--follow`, `--raw`, `--json`, `--pod`, and `--since` support, plus documentation and clearer login failure hints: #16, 0e66dc4, 0d6c706, 16fed90
- Job Creation and MCP Workflows: Add `qzcli create`, `qzcli batch`, MCP job creation tools, cookie-based job creation fallback, optional MCP login credential resolution, and interactive job submission with auto-login support: #7, #12, #13
- Interactive and HPC Workloads: Add dev-machine listing, Jupyter-terminal based remote command execution, HPC/CPU job submission, HPC command documentation, and HPC CPU/memory utilization in availability output: #9, #10, 93e40a6, fa8cc23
- Login and Workspace Reliability: Fix CAS RSA ciphertext encoding when leading zero bytes are present, handle passwords with special characters, move workspace overview to the new platform endpoint, and keep backward-compatible workspace flags as deprecated no-ops: #8, #11, 4cf71ce, ed10a5f

## Upgrade Notes

- No known breaking changes; Python support remains `>=3.8`.
- SOCKS proxy users should install updated dependencies with `pip install -r requirements.txt`.
- WSL users can configure a Windows-side Clash proxy with `{"proxy": "socks5://127.0.0.1:7897"}` in `~/.qzcli/config.json`.
- `qzcli logs` and other `/api/v2` calls require cookie authentication; run `qzcli login` again if a command reports an expired or missing cookie.

## Dependencies

- Add `PySocks>=1.7` so SOCKS proxy support works after a normal requirements install: #13

## Validation

- `python3 -m compileall qzcli tests`
- `python3 -m unittest discover -s tests` (54 passed, 1 skipped)

## What's Changed

- Add sii-cas-auth auto login password encryption: #1
- Unify `avail` table output and improve formatting: #2
- Add qzcli MCP server support: #6
- Add `qzcli create` and `qzcli batch` commands for job submission: #7
- Preserve leading zero in RSA ciphertext: #8
- Add dev machine listing and Jupyter-based remote exec: #9
- Add HPC/CPU job submission support: #10
- Update workspace overview to use the new API endpoint: #11
- Support interactive job submitting and auto-login: #12
- Support SOCKS5 proxy for WSL/VPN environments: #13
- Document `qzcli logs`: #16
- Preserve cached workspace matches and speed up `qzcli avail`: #17

Full Changelog: https://github.com/tianyilt/qzcli_tool/commits/v0.2.0
