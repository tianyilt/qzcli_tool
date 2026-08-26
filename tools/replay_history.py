#!/usr/bin/env python3
"""按**用户真实敲过的命令**做差分回放。

## 为什么要有这个

这个项目栽过的每一个坑，根子都是同一件事：**测我构造的路径，不测用户实际怎么用**。

- `avail` 全线 429 —— 因为所有用例都显式带 `-w`，而用户常常不带
- 分页每页重登 —— 因为用例只打一页
- `create` 每次白登 —— 因为没人跑过完整的 create 链路

与其继续凭想象补用例，不如直接把 shell 历史里的真实命令捞出来当测试集。

## 它做什么

1. 从 `~/.zsh_history` 解析出所有 `qzcli` 命令，按频次排序
2. 同一条命令分别在**两个代码版本**上跑（默认 `dev` 工作区 vs `master` worktree）
3. 比对退出码和输出，把差异列出来

只读命令才会被回放；`login`（会打 CAS）和任何写操作默认跳过。

## 用法

    # 先建一个 master 的 worktree
    git worktree add /tmp/qzcli-master master

    python3 tools/replay_history.py --baseline /tmp/qzcli-master
    python3 tools/replay_history.py --baseline /tmp/qzcli-master --top 8
    python3 tools/replay_history.py --list-only        # 只看历史统计，不跑

## 注意

每条命令要跑两遍（两个版本），而这些多半是全量扫描 —— 累计请求量不小。
默认在命令之间留 `--cooldown` 秒，别把账号打进限流。
"""

import argparse
import collections
import os
import pathlib
import re
import shlex
import subprocess
import sys
import time

#: 绝不回放的子命令：会写平台数据、或会打 CAS 登录。
_WRITE_OR_AUTH = {
    "login",
    "init",
    "create",
    "create-job",
    "hpc",
    "batch",
    "stop",
    "exec",
    "exec-attach",
    "cookie",
    "remove",
    "clear",
    "import",
    "track",
    "dashboard",
    "watch",
}

#: 输出里逐次都会变的东西，比对前抹掉，否则满屏假差异。
_VOLATILE = [
    (re.compile(r"\d+\.\d+s"), "<t>"),
    (re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?"), "<ts>"),
    (re.compile(r"\d+:\d+:\d+"), "<dur>"),
    (re.compile(r"job-[0-9a-f-]{8,}"), "<job>"),
    (re.compile(r"\r"), ""),
    # 进度条每次刷新的百分比 / 字符
    (re.compile(r"[━╸]+\s*\d+%"), "<bar>"),
]


def unmetafy(data: bytes) -> bytes:
    """还原 zsh 的 metafy 编码。

    zsh 存历史时会把每个 >= 0x80 的字节写成 ``0x83`` + ``(原字节 ^ 0x20)``。
    不还原的话，中文工作空间名（``分布式`` 这类）读出来全是乱码，
    回放时就会拿着一串垃圾去查工作空间 —— 而这些恰恰是用户最常用的参数。
    """
    out = bytearray()
    i = 0
    while i < len(data):
        if data[i] == 0x83 and i + 1 < len(data):
            out.append(data[i + 1] ^ 0x20)
            i += 2
        else:
            out.append(data[i])
            i += 1
    return bytes(out)


def parse_history(path):
    """从 zsh 历史里抽出 qzcli 命令并计数。

    zsh 的扩展历史每行是 ``: <时间戳>:<耗时>;<命令>``，且中文被 metafy 编码过 ——
    所以按 bytes 读、先 unmetafy 再解码。
    """
    raw = unmetafy(pathlib.Path(path).read_bytes()).decode("utf-8", errors="replace")
    counter = collections.Counter()
    for line in raw.splitlines():
        line = re.sub(r"^: \d+:\d+;", "", line).strip()
        # 一行里可能有 `a && b`、`a; b`、`a | b`
        for part in re.split(r"\s*&&\s*|\s*;\s*|\s*\|\s*", line):
            part = part.strip().rstrip("\\").strip()
            if not part.startswith("qzcli "):
                continue
            try:
                argv = shlex.split(part)
            except ValueError:
                continue  # 引号不配对的历史行，跳过
            if len(argv) < 2:
                continue
            counter[tuple(argv[1:])] += 1
    return counter


def is_replayable(argv):
    return argv and argv[0] not in _WRITE_OR_AUTH and not argv[0].startswith("-")


#: 输出里出现即视为问题的标记。
_ERROR_MARKERS = {
    "429": re.compile(r"\b429\b"),
    "AccessForbidden": re.compile(r"AccessForbidden"),
    "Traceback": re.compile(r"Traceback \(most recent"),
    "错误": re.compile(r"错误[:：]"),
    "重新登录": re.compile(r"检测到登录态失效"),
}


def profile(text):
    """把一次运行提炼成**可比对的属性**，而不是原始文本。

    为什么不直接 diff 文本：这些命令报的是集群实时状态，而集群常年 99%+ 利用率。
    实测同一个版本相隔十几秒跑两次就已经不一样了 —— 抹掉数字不够（表格行数在变），
    取行模板集合也不够（rich 表格列宽随数据自适应，padding 跟着变）。
    结论是**渲染后的表格不适合当回归信号**，别再往归一化上打补丁了。

    真正该比的是行为属性：

    - 退出码
    - 有没有 429 / 权限噪声 / 异常栈
    - **触发了几次重新登录** —— 本轮改动正是冲这个去的
    - 结构锚点：输出里不含数字的行（区块标题、列名、提示语）。数据churn 不影响它们，
      而少了一个区块、改了文案会立刻暴露。
    """
    counts = {name: len(rx.findall(text)) for name, rx in _ERROR_MARKERS.items()}
    anchors = {
        ln.strip()
        for ln in text.splitlines()
        if ln.strip() and not any(ch.isdigit() for ch in ln)
    }
    return counts, anchors


def run_one(argv, tree, timeout):
    """在指定代码树上跑一条命令。

    ``cwd`` 必须是中立目录 —— 在仓库根目录跑的话，cwd 里的 ``qzcli/`` 包会盖过
    ``PYTHONPATH``，两个版本就都跑成同一份代码了（我第一版就踩了这个）。
    """
    env = dict(os.environ, PYTHONPATH=str(tree))
    code = "from qzcli.cli import main; import sys; sys.exit(main())"
    try:
        proc = subprocess.run(
            [sys.executable, "-c", code, *argv],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=tempfile_dir(),
            env=env,
        )
        return proc.returncode, proc.stdout + proc.stderr
    except subprocess.TimeoutExpired:
        return None, "<超时>"


_TMPDIR = None


def tempfile_dir():
    global _TMPDIR
    if _TMPDIR is None:
        import tempfile

        _TMPDIR = tempfile.mkdtemp(prefix="qzcli-replay-cwd-")
    return _TMPDIR


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--history", default=str(pathlib.Path.home() / ".zsh_history"))
    ap.add_argument(
        "--baseline", help="对照代码树（通常是 master 的 worktree）；不给则只跑当前树"
    )
    ap.add_argument("--candidate", default=".", help="待验代码树，默认当前目录")
    ap.add_argument("--top", type=int, default=10, help="回放前 N 个高频命令")
    ap.add_argument("--cooldown", type=int, default=15, help="命令之间的间隔秒数")
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--list-only", action="store_true", help="只打印历史统计")
    args = ap.parse_args()

    counter = parse_history(args.history)
    total = sum(counter.values())
    print(f"历史里的 qzcli 命令：{total} 次，{len(counter)} 种形态\n")

    ranked = [(argv, n) for argv, n in counter.most_common() if is_replayable(argv)]
    skipped = [
        (argv, n) for argv, n in counter.most_common() if not is_replayable(argv)
    ]

    print("=== 可回放（只读）===")
    for argv, n in ranked[: args.top]:
        print(f"  {n:4d}  qzcli {' '.join(argv)}")
    print("\n=== 跳过（写操作 / 会打 CAS）===")
    for argv, n in skipped[:8]:
        print(f"  {n:4d}  qzcli {argv[0]} …")

    if args.list_only:
        return 0

    candidate = pathlib.Path(args.candidate).resolve()
    baseline = pathlib.Path(args.baseline).resolve() if args.baseline else None
    print(f"\n待验: {candidate}")
    if baseline:
        print(f"对照: {baseline}")
    print()

    diffs, failures = [], []
    for i, (argv, n) in enumerate(ranked[: args.top], 1):
        label = "qzcli " + " ".join(argv)
        print(f"[{i}/{min(args.top, len(ranked))}] {label}  (历史 {n} 次)", flush=True)

        rc_c, out_c = run_one(list(argv), candidate, args.timeout)
        if rc_c != 0:
            failures.append((label, rc_c, out_c[-400:]))
            print(f"    ✗ 待验版退出码 {rc_c}")
        else:
            print("    ✓ 待验版 rc=0")

        counts_c, anchors_c = profile(out_c)
        bad = {k: v for k, v in counts_c.items() if v and k != "重新登录"}
        if bad:
            failures.append((label, rc_c, f"输出含: {bad}"))
            print(f"    ✗ 输出含问题标记: {bad}")
        if counts_c["重新登录"]:
            print(f"    · 触发重新登录 {counts_c['重新登录']} 次")

        if baseline:
            time.sleep(args.cooldown)
            rc_b, out_b = run_one(list(argv), baseline, args.timeout)
            counts_b, anchors_b = profile(out_b)
            same_rc = rc_b == rc_c
            only_base = sorted(anchors_b - anchors_c)
            only_cand = sorted(anchors_c - anchors_b)
            if not same_rc:
                diffs.append((label, f"退出码 {rc_b}(对照) vs {rc_c}(待验)"))
                print(f"    ⚠ 退出码不一致: {rc_b} → {rc_c}")
            elif only_base or only_cand:
                detail = "; ".join(
                    filter(
                        None,
                        [
                            f"对照版独有结构行 {len(only_base)}" if only_base else "",
                            f"待验版独有结构行 {len(only_cand)}" if only_cand else "",
                        ],
                    )
                )
                sample = (only_base[:2] + only_cand[:2])[:3]
                diffs.append((label, detail + " | 样例: " + " ⏐ ".join(sample)))
                print(f"    ⚠ 结构差异（{detail}）")
            else:
                base_logins = counts_b["重新登录"]
                extra = ""
                if base_logins != counts_c["重新登录"]:
                    extra = f"（重新登录 {base_logins} → {counts_c['重新登录']}）"
                print(f"    ✓ 与对照版一致 {extra}")

        if i < min(args.top, len(ranked)):
            time.sleep(args.cooldown)

    print("\n" + "=" * 60)
    print(
        f"回放 {min(args.top, len(ranked))} 条：失败 {len(failures)}，差异 {len(diffs)}"
    )
    for label, rc, tail in failures:
        print(f"\n  ✗ {label}  rc={rc}\n    {tail.strip()[:300]}")
    for label, detail in diffs:
        print(f"\n  ⚠ {label}\n    {detail}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
