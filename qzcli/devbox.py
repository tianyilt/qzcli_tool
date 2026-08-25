"""把开发机上易失的 dotfile / agent home 挪到持久盘。

## 为什么需要这个

启智开发机（交互式建模实例）的 ``/root`` 是**容器 overlay 层，重启即失**。
Claude Code / Codex 的 ``~/.claude``、``~/.codex``（session 历史、todos）、
以及 ``.bash_history`` / ``.zsh_history`` 全在里面。实测已经丢过。

## 唯一容易做错的地方

``/inspire/hdd/global_user`` **父目录本身还是 overlayfs**，只有
``/inspire/hdd/global_user/<用户id>/`` 子目录才是 gpfs 真挂载：

    /                                        st_dev 同 /  -> overlayfs（临时）
    /inspire/hdd/global_user                 st_dev 同 /  -> overlayfs（临时）⚠️
    /inspire/hdd/global_user/liangtianyi-…   st_dev 不同  -> gpfs（持久）✅

挪错一层的表现是「看起来持久化了」，重启照样全丢，**不会有任何报错** —— 正是本仓
最忌讳的静默失败。所以 :func:`is_persistent` 用 ``st_dev`` 判据把关，通不过就拒绝
执行，绝不降级成「先凑合」。

## 重启之后怎么办

容器内**没有能活过重启的钩子**（``/etc/profile.d``、``/etc/rc.local`` 和 ``/root``
一样在 overlayfs 上）。所以只能重启后重跑一次 —— 也因此 :func:`plan_actions`
必须**能合并**而不是覆盖：重启后 agent 会在 ``/root`` 侧重新写一份新数据，而持久盘
那份是旧的，**两边都有内容**。合并按类型分，总原则是「绝不销毁，冲突全部上报」。
"""

import json
import os
import re
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

#: 候选持久盘根目录。逐个探测，取 st_dev 与 ``/`` 不同（= 真挂载）且可写的。
PERSIST_ROOT_GLOBS = (
    "/inspire/hdd/global_user/*",
    "/inspire/ssd/project/*/[!p]*",  # 排除 public，取个人目录
)

#: 冲突备份目录名（放在持久盘根下）。
CONFLICT_DIRNAME = ".devbox-conflicts"

#: 托管清单。``kind`` 决定合并策略：
#:
#: - ``dir``    目录，按相对路径取并集（session 文件只增不减 -> 冲突保留更大的）
#: - ``config`` 单个配置文件，无法自动合并 -> 持久盘为准，另一份进冲突目录
#: - ``history`` shell 历史，**不软链**（见 :func:`plan_actions` 里的说明）
MANIFEST: Tuple[Dict[str, str], ...] = (
    {"name": "claude", "path": ".claude", "kind": "dir"},
    {"name": "codex", "path": ".codex", "kind": "dir"},
    {"name": "openclaw", "path": ".config/openclaw", "kind": "dir"},
    {"name": "bashrc", "path": ".bashrc", "kind": "config"},
    {"name": "zshrc", "path": ".zshrc", "kind": "config"},
    {"name": "profile", "path": ".profile", "kind": "config"},
    {"name": "gitconfig", "path": ".gitconfig", "kind": "config"},
    {"name": "vimrc", "path": ".vimrc", "kind": "config"},
    {"name": "tmux", "path": ".tmux.conf", "kind": "config"},
    {"name": "bash_history", "path": ".bash_history", "kind": "history"},
    {"name": "zsh_history", "path": ".zsh_history", "kind": "history"},
)

#: ``.ssh`` **默认不托管**：实测个人持久目录权限是 ``drwxrwx--x``（同组可读写、
#: 其他人可 traverse），私钥放这种目录不合适。要托管得显式 ``--include-ssh``。
SSH_ENTRY = {"name": "ssh", "path": ".ssh", "kind": "dir"}


class DevboxError(Exception):
    """探测不到持久盘、或目标目录根本不持久。"""


# --------------------------------------------------------------------------
# 持久性判据
# --------------------------------------------------------------------------


def is_persistent(path, root: str = "/") -> bool:
    """``path`` 是否落在与 ``root`` 不同的文件系统上（= 真挂载 = 持久）。

    用 ``st_dev`` 而不是解析 ``df`` / ``mount`` 的输出：后者格式随发行版变，
    而且要处理 bind mount、overlay 叠加这些边角。``st_dev`` 是内核给的事实。

    路径不存在时**向上找最近的存在祖先**再判断 —— 因为我们经常要判断一个
    「还没建出来的目标目录」是否会落在持久盘上。
    """
    p = Path(path)
    while not p.exists():
        if p.parent == p:
            return False
        p = p.parent
    try:
        return os.stat(str(p)).st_dev != os.stat(root).st_dev
    except OSError:
        return False


def root_is_ephemeral() -> bool:
    """``/`` 是不是容器 overlay（= 重启即失）。

    只有在开发机这类容器里才成立。在笔记本 / 物理机上 ``/`` 是普通文件系统，
    "与 / 同盘" 完全是持久的 —— 不加这个判据的话，用户在自己 Mac 上跑
    ``devbox status`` 会看到满屏「临时」，白白吓一跳（实测第一版就是这样）。
    """
    try:
        with open("/proc/mounts", "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) >= 3 and parts[1] == "/":
                    return parts[2] in ("overlay", "overlayfs", "aufs")
    except OSError:
        return False  # 没有 /proc（macOS 等）= 不是容器
    return False


def fs_label(path, root: str = "/") -> str:
    """给 ``devbox status`` 用的人话标签。"""
    if not Path(path).exists():
        return "不存在"
    if is_persistent(path, root):
        return "持久"
    return "临时(overlay)" if root_is_ephemeral() else "与 / 同盘"


def detect_persist_root(explicit: Optional[str] = None) -> str:
    """找出该机器上可用的持久盘根目录。

    显式给了 ``explicit`` 就只校验它；否则按 :data:`PERSIST_ROOT_GLOBS` 探测。

    **命中 0 个或多个都直接报错**，不替用户挑 —— 挑错的后果（数据落到临时层、
    或落进别人的目录）远比多问一句严重。
    """
    if explicit:
        target = Path(explicit).expanduser()
        if not is_persistent(target):
            raise DevboxError(
                f"{target} 不在持久盘上（与 / 同一个文件系统 = 容器临时层，重启即失）。\n"
                "  提示：/inspire/hdd/global_user 这一层本身也是临时层，\n"
                "  真正持久的是它下面的 <你的用户id>/ 子目录。"
            )
        return str(target)

    import glob as _glob

    hits = []
    for pattern in PERSIST_ROOT_GLOBS:
        for cand in _glob.glob(pattern):
            if not os.path.isdir(cand):
                continue
            if not is_persistent(cand):
                continue
            if not os.access(cand, os.W_OK):
                continue
            hits.append(cand)

    hits = sorted(set(hits))
    if not hits:
        raise DevboxError(
            "没探测到可写的持久盘目录。请用 --target-dir 显式指定，例如：\n"
            "  qzcli devbox init --target-dir /inspire/hdd/global_user/<你的用户id>/devbox"
        )
    if len(hits) > 1:
        listed = "\n".join(f"    {h}" for h in hits)
        raise DevboxError(
            f"探测到多个候选持久目录，不替你挑。请用 --target-dir 指定其一：\n{listed}"
        )
    return hits[0]


# --------------------------------------------------------------------------
# 合并
# --------------------------------------------------------------------------

#: zsh extended history 行首形如 ``: 1786860482:0;cmd``
_ZSH_HIST_RE = re.compile(r"^: (\d+):\d+;")


def merge_history(persist_text: str, local_text: str) -> Tuple[str, Dict[str, int]]:
    """合并两份 shell 历史，返回 ``(合并后文本, 计数)``。

    zsh extended history 带 epoch，按时间排序合并；bash 无时间戳时保序合并。
    两种情况都**去重**（同一条命令在两边都出现时只留一条）。

    计数里给出 ``persist`` / ``local`` / ``merged`` 三个数，让调用方能打印
    「两边各多少条、合并后多少条」—— 不打印这个的话，用户没法确认自己的历史
    到底有没有被吞掉。
    """
    p_lines = persist_text.splitlines()
    l_lines = local_text.splitlines()

    def stamped(lines):
        out, cur_ts = [], None
        for ln in lines:
            m = _ZSH_HIST_RE.match(ln)
            if m:
                cur_ts = int(m.group(1))
            out.append((cur_ts, ln))
        return out

    tagged = stamped(p_lines) + stamped(l_lines)
    has_ts = any(ts is not None for ts, _ in tagged)
    if has_ts:
        # 稳定排序：有时间戳的按时间，没有的（续行）跟着前一条走，靠 enumerate 保序
        tagged = [(ts if ts is not None else 0, i, ln) for i, (ts, ln) in enumerate(tagged)]
        tagged.sort(key=lambda t: (t[0], t[1]))
        ordered = [ln for _, _, ln in tagged]
    else:
        ordered = p_lines + l_lines

    seen, merged = set(), []
    for ln in ordered:
        if ln in seen:
            continue
        seen.add(ln)
        merged.append(ln)

    text = "\n".join(merged)
    if merged:
        text += "\n"
    return text, {
        "persist": len(p_lines),
        "local": len(l_lines),
        "merged": len(merged),
    }


def _iter_files(root: Path):
    for dirpath, _dirnames, filenames in os.walk(str(root)):
        for fn in filenames:
            full = Path(dirpath) / fn
            yield full, full.relative_to(root)


def merge_dir(
    persist: Path, local: Path, conflict_dir: Path, dry_run: bool = False
) -> Dict[str, object]:
    """把 ``local`` 目录并进 ``persist``，返回统计。

    session 文件（``*.jsonl`` 之类）**只增不减**，所以同名冲突时保留**更大的**，
    另一份原样备份到 ``conflict_dir``。**任何一边的文件都不会被直接删掉** ——
    这是本函数的核心约定，测试会钉死它。
    """
    stats = {"copied": 0, "kept_persist": 0, "replaced": 0, "conflicts": []}
    if not local.exists():
        return stats

    for src, rel in _iter_files(local):
        dst = persist / rel
        if not dst.exists():
            if not dry_run:
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(src), str(dst))
            stats["copied"] += 1
            continue

        s_size, d_size = src.stat().st_size, dst.stat().st_size
        if s_size == d_size:
            stats["kept_persist"] += 1
            continue

        loser, winner_is_local = (dst, True) if s_size > d_size else (src, False)
        backup = conflict_dir / rel
        if not dry_run:
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(loser), str(backup))
            if winner_is_local:
                shutil.copy2(str(src), str(dst))
        stats["conflicts"].append(str(rel))
        if winner_is_local:
            stats["replaced"] += 1
        else:
            stats["kept_persist"] += 1

    return stats


def merge_config(
    persist: Path, local: Path, conflict_dir: Path, rel_name: str, dry_run: bool = False
) -> Optional[str]:
    """配置文件无法自动合并：**持久盘那份为准**，本地那份进冲突目录。

    返回冲突备份的相对名（没冲突时返回 ``None``）。绝不静默覆盖任何一边 ——
    用户手改的东西被覆盖过一次，不能再来第二次。
    """
    if not local.exists():
        return None
    if not persist.exists():
        if not dry_run:
            persist.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(local), str(persist))
        return None
    if persist.read_bytes() == local.read_bytes():
        return None
    backup = conflict_dir / rel_name
    if not dry_run:
        backup.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(local), str(backup))
    return rel_name


# --------------------------------------------------------------------------
# 编排
# --------------------------------------------------------------------------


def _relink(link: Path, target: Path, dry_run: bool) -> str:
    """把 ``link`` 指向 ``target``。返回做了什么的人话描述。"""
    if link.is_symlink():
        if os.path.realpath(str(link)) == os.path.realpath(str(target)):
            return "已就位"
        if not dry_run:
            link.unlink()
    elif link.exists():
        if not dry_run:
            shutil.rmtree(str(link)) if link.is_dir() else link.unlink()
    if not dry_run:
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(str(target))
    return "已软链"


def run(
    persist_root: str,
    home: Optional[str] = None,
    only: Optional[List[str]] = None,
    include_ssh: bool = False,
    dry_run: bool = False,
) -> Dict[str, object]:
    """执行（或演练）持久化。返回结构化报告，供本地渲染或远端 JSON 回传。

    幂等：已经就位的直接跳过，可以在每次重启后放心重跑。
    """
    home_p = Path(home or os.path.expanduser("~"))
    root = Path(persist_root)
    if not is_persistent(root):
        why = (
            "= 容器临时层，重启即失"
            if root_is_ephemeral()
            else "（本机 / 不是容器 overlay，持久化到这里没有意义）"
        )
        raise DevboxError(
            f"{root} 与 / 在同一个文件系统上 {why}，拒绝在此持久化。"
        )

    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    conflict_dir = root / CONFLICT_DIRNAME / stamp

    entries = list(MANIFEST) + ([SSH_ENTRY] if include_ssh else [])
    if only:
        wanted = {s.strip() for s in only if s.strip()}
        entries = [e for e in entries if e["name"] in wanted]

    report: Dict[str, object] = {
        # 显式声明形态。渲染端**不要**靠「某个字段在不在」来猜是 init 还是 status ——
        # 猜过一次就错了（status 的 persist_root 是空串不是 None，被当成 init 渲染）。
        "mode": "init",
        "persist_root": str(root),
        "home": str(home_p),
        "dry_run": dry_run,
        "conflict_dir": str(conflict_dir),
        "items": [],
    }

    for entry in entries:
        rel, kind, name = entry["path"], entry["kind"], entry["name"]
        local = home_p / rel
        persist = root / rel
        item: Dict[str, object] = {"name": name, "path": rel, "kind": kind}

        try:
            if kind == "history":
                # **刻意不软链**：shell 保存历史常用「写临时文件再 rename」，
                # rename 会把软链替换成真文件，从此静默不再持久、且没有任何报错。
                # 正确做法是让 shell 直接写持久盘 —— 由 ensure_histfile() 往
                # .zshrc/.bashrc 里写 HISTFILE。这里只负责把两边历史并起来。
                p_text = persist.read_text("utf-8", "replace") if persist.exists() else ""
                l_text = local.read_text("utf-8", "replace") if local.exists() else ""
                if not p_text and not l_text:
                    item["action"] = "跳过（两边都没有）"
                else:
                    merged, counts = merge_history(p_text, l_text)
                    if not dry_run:
                        persist.parent.mkdir(parents=True, exist_ok=True)
                        persist.write_text(merged, encoding="utf-8")
                    item["action"] = "已合并"
                    item["counts"] = counts

            elif kind == "dir":
                if local.exists() and not local.is_symlink():
                    item["merge"] = merge_dir(persist, local, conflict_dir, dry_run)
                    if not dry_run:
                        shutil.rmtree(str(local))
                elif not persist.exists() and not dry_run:
                    persist.mkdir(parents=True, exist_ok=True)
                item["action"] = _relink(local, persist, dry_run)

            else:  # config
                conflict = merge_config(persist, local, conflict_dir, rel, dry_run)
                if conflict:
                    item["conflict"] = conflict
                if not persist.exists() and not dry_run:
                    persist.parent.mkdir(parents=True, exist_ok=True)
                    persist.touch()
                item["action"] = _relink(local, persist, dry_run)
        except OSError as exc:  # noqa: PERF203 —— 单条失败不该让整轮中止
            item["action"] = "失败"
            item["error"] = f"{type(exc).__name__}: {exc}"

        report["items"].append(item)

    report["histfile"] = ensure_histfile(root, home_p, dry_run)
    return report


#: 写进 rc 文件的标记，用来幂等识别自己写过的段落。
HISTFILE_MARK = "# >>> qzcli devbox: 让 shell 直接把历史写到持久盘 >>>"


def ensure_histfile(root: Path, home: Path, dry_run: bool = False) -> Dict[str, str]:
    """往 ``.zshrc`` / ``.bashrc`` 里写 ``HISTFILE`` 指向持久盘。

    为什么不软链历史文件本身：见 :func:`run` 里 ``kind == "history"`` 分支的注释。
    """
    out: Dict[str, str] = {}
    for rc, hist in ((".zshrc", ".zsh_history"), (".bashrc", ".bash_history")):
        rc_path = home / rc
        target = root / hist
        block = (
            f"{HISTFILE_MARK}\n"
            f"export HISTFILE={target}\n"
            "# <<< qzcli devbox <<<\n"
        )
        try:
            text = rc_path.read_text("utf-8", "replace") if rc_path.exists() else ""
        except OSError as exc:
            out[rc] = f"读不到: {exc}"
            continue
        if HISTFILE_MARK in text:
            out[rc] = "已配置"
            continue
        if not dry_run:
            try:
                rc_path.parent.mkdir(parents=True, exist_ok=True)
                with open(str(rc_path), "a", encoding="utf-8") as fh:
                    fh.write("\n" + block)
            except OSError as exc:
                out[rc] = f"写不进: {exc}"
                continue
        out[rc] = "已写入"
    return out


def build_remote_script(action: str, **opts) -> str:
    """生成一份在**目标开发机**上跑的自包含脚本，结果以 JSON 打到 stdout。

    做法是把**本模块自身的源码**整个内嵌进去，再接一段 driver。这样远端跑的就是
    同一份合并逻辑，不会出现「本地一套、远端一套，改了一边忘了另一边」。
    本模块刻意只 import 标准库，正是为了能这样搬过去 —— 目标开发机上大概率
    没装 qzcli（实测别人的机器就没有）。

    传输时整段 base64，避免 PTY 里的引号/换行把脚本撕坏。
    """
    src = Path(__file__).read_text(encoding="utf-8")
    driver = f"""

if __name__ == "__main__":
    import json as _json
    _action = {action!r}
    _opts = {opts!r}
    try:
        if _action == "status":
            _out = status(_opts.get("persist_root"), _opts.get("home"))
        else:
            _root = _opts.get("persist_root") or detect_persist_root(
                _opts.get("target_dir")
            )
            _out = run(
                _root,
                home=_opts.get("home"),
                only=_opts.get("only"),
                include_ssh=bool(_opts.get("include_ssh")),
                dry_run=bool(_opts.get("dry_run")),
            )
        print("QZDEVBOX_JSON " + _json.dumps(_out, ensure_ascii=False))
    except Exception as _exc:
        print("QZDEVBOX_JSON " + _json.dumps(
            {{"error": type(_exc).__name__ + ": " + str(_exc)}}, ensure_ascii=False))
"""
    return src + driver


def remote_command(action: str, **opts) -> str:
    """把脚本包成一条可以直接丢给 ``qzcli exec`` 的单行命令。"""
    import base64

    payload = base64.b64encode(build_remote_script(action, **opts).encode("utf-8"))
    return (
        "python3 -c \"import base64,sys;"
        f"exec(compile(base64.b64decode('{payload.decode()}'),'qzdevbox','exec'))\""
    )


def parse_remote_output(text: str) -> Dict:
    """从远端 PTY 输出里挑出那行 JSON。

    PTY 会混进 banner / 回显 / 提示符，所以用哨兵前缀定位，而不是「取最后一行」。
    """
    for line in (text or "").splitlines():
        line = line.strip()
        if line.startswith("QZDEVBOX_JSON "):
            try:
                return json.loads(line[len("QZDEVBOX_JSON ") :])
            except ValueError as exc:
                return {"error": f"远端返回的 JSON 解析失败: {exc}"}
    return {"error": "远端没有返回结果（脚本可能没跑起来，或输出被截断）"}


def status(persist_root: Optional[str] = None, home: Optional[str] = None) -> Dict:
    """只读：每个托管路径现在是什么状态。重启后第一件事就是跑它。"""
    home_p = Path(home or os.path.expanduser("~"))
    items = []
    for entry in list(MANIFEST) + [SSH_ENTRY]:
        p = home_p / entry["path"]
        real = os.path.realpath(str(p)) if p.exists() or p.is_symlink() else ""
        items.append(
            {
                "name": entry["name"],
                "path": entry["path"],
                "kind": entry["kind"],
                "exists": p.exists() or p.is_symlink(),
                "is_symlink": p.is_symlink(),
                "resolved": real,
                "fs": fs_label(real or str(p)),
            }
        )
    return {
        "mode": "status",
        "home": str(home_p),
        "persist_root": persist_root or "",
        "items": items,
    }
