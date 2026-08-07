# Contributing to qzcli

感谢你愿意参与 qzcli。这个项目主要围绕启智平台的真实任务管理工作流演进，欢迎提交 bug report、接口分析、文档改进和 PR。

## 分支策略（重要）

**启智平台上不少生产基建直接依赖这个仓库的 `master`**，所以 `master` 不是开发分支，
是发布分支。日常改动一律不进 `master`。

```
feature/xxx  ──PR──▶  dev  ──攒够一个版本、验收通过──▶  master ──▶ tag + release
```

| 分支 | 用途 | 谁能进 |
|---|---|---|
| `master` | 发布分支。生产环境和 Gitee 镜像跟这条 | 只接受来自 `dev` 的版本合并 |
| `dev` | 集成分支。所有功能 / 修复先在这里攒 | 各 feature 分支的 PR |
| `feature/*` `fix/*` `test/*` | 单个改动 | — |

**开 PR 时 base 选 `dev`，不要选 `master`。**

进 `master` 之前必须过这几关（缺一不可）：

1. `python3 -m unittest discover -s tests` 全绿
2. `python3 tools/live_smoke.py --workspace <某个真实工作空间>` 全绿（只读）
3. `python3 tools/parity_sweep.py` —— **v1/v2 对齐扫描 SCHEMA 差异必须为 0**。
   SCHEMA 类差异意味着 v2 换了字段名，代码不会报错、只会静默返回空
4. 发版前额外跑一次 `live_smoke.py --submit`（会真提交任务，跑完记得停掉）

**各阶段之间要隔开至少 5 分钟。** 这几个工具都是"扫全部工作空间"的全量形态，
背靠背跑会把平台配额打满，然后 gate 被自己的负载误伤。

实测：`parity_sweep` 跑完立刻接 `live_smoke` → **18/20**（`usage` 和
`list --all-ws` 撞 429）；静置几分钟后单独跑同一份代码 → **20/20**。

所以 gate 红了先问一句「是不是我刚才自己打的」，隔开重跑一次再下结论 ——
但**不要**因为红了就去放宽断言。

**唯一的例外是线上已经坏了、需要热修。** 这种情况下直接从 `master` 拉
`hotfix/*`、修完合回 `master`，然后**必须立刻把 `master` 合回 `dev`**，否则
下次 `dev → master` 会把热修覆盖掉。

## 开发环境

```bash
git clone https://github.com/tianyilt/qzcli_tool.git
cd qzcli_tool
python -m pip install -r requirements.txt
python -m pip install -e .
```

## 本地验证

提交 PR 前至少运行：

```bash
python3 -m compileall qzcli tests
python3 -m unittest discover -s tests
git diff --check
```

如果改动涉及真实平台接口，请在 PR 里说明：

- 运行过的 qzcli 命令
- 是否使用 cookie auth 或 token auth
- 是否涉及 workspace / project / compute group / spec 解析
- 是否补了单元测试或手工验证步骤

## PR 建议

- **base 分支选 `dev`**（见上方分支策略）。
- 小步提交，单个 PR 尽量只解决一个问题。
- 新增命令或参数时同步更新 `README.md`。
- 修改认证、任务提交、资源查询等共享路径时补测试。
- 避免把真实 cookie、用户名、workspace UUID、内部项目名、完整日志贴进仓库；必要时请脱敏。

### 凭据（真踩过，别重蹈）

**从真机响应里粘贴样例时，凭据会跟着一起进来，而写的当下一点都不觉得是在写凭据。**

实际发生过：把开发机的 Jupyter 访问 URL 抄进测试 fixture —— 而 **Jupyter 的 token
就写在那条 URL 里**，等于把该开发机的门钥匙提交进了仓库（拿到它就能在上面执行任意
命令）。被 GitGuardian 拦下。

规矩：

- 测试 fixture 的值**必须是编造的**，形状照真实的即可。用 `fake` /
  `example.invalid` 这类一眼可辨的词，别用真值改两个字符
- 提交前跑 `python3 -m unittest tests.test_no_secrets_in_repo`（它也在全量测试里）
- **万一提交了：不要只删掉再提交一次** —— 历史里还在。要 `--amend` 或 rebase
  重写那个提交，`--force-with-lease` 覆盖，然后**轮换该凭据**
- 凭据同样不许进 commit message 和 PR 正文（那两处扫描器管不到，只能靠人）

## Issue 建议

提 bug 时请尽量包含：

- qzcli 版本或 commit
- 运行命令
- 期望行为和实际行为
- 脱敏后的错误输出
- 是否已经执行过 `qzcli login` / `qzcli res -u`

功能建议可以直接描述具体使用场景，例如“提交任务前想自动找满足 N 节点的 compute group”。
