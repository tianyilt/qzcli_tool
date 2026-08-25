"""qzcli 的持久操作日志。

## 为什么需要

``qzcli/diag.py`` 只有一个进程内环形缓冲（``deque(maxlen=64)``），进程一退就没。
后果是 2026-08 账号被 CAS 锁定三次，每次都只能靠 ``~/.qzcli/.cookie`` 的 mtime
倒推「是谁在什么时候登的」—— 而 mtime 只保留最后一次，等于没有历史。为此连续
误判了四次原因。

所以这里记的是**有副作用的操作**：提交 / 停止 / 登录 / 远程执行 / 刷新缓存 /
删除本地记录。只读命令（``list`` / ``status`` / ``avail`` / ``usage`` / ``logs``）
**刻意不记** —— monitor 每 45 秒轮询一次，全记会一天几千行，把真正要找的
提交和登录记录淹没在噪声里。

## 硬约束

1. **绝不含凭据**。只记 argv 的前三段（够认出是 ``qzcli create`` 还是
   ``qzcli res -u``），不记完整命令行、不记 cookie / token / 密码。
2. **写日志失败绝不能让命令挂**。诊断设施反过来搞挂生产是最蠢的失败方式，
   所以全程包 ``OSError`` 静默跳过（沿用 ``api._record_relogin_failure`` 的姿势）。
3. **多进程并发安全**。用 ``O_APPEND`` 单次 ``write`` 写完整一行 —— 这是 POSIX
   保证原子的边界内（单次 write 且短于 PIPE_BUF），不会出现两个进程的行交错。
"""

import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Optional

from .config import CONFIG_DIR

#: 跟随 ``QZCLI_HOME``，与其它所有状态一致，不做特例。
LOG_NAME = "qzcli_ops.log"

#: 超过这个大小就轮转，保留一个 ``.1`` 备份。共享盘上别写爆。
MAX_BYTES = 8 * 1024 * 1024

#: 会被记录的操作。**新增有副作用的命令时要往这里补一条**，
#: 否则出事了照样查不到。
RECORDED_OPS = frozenset(
    {
        "create",
        "create-job",
        "batch",
        "hpc",
        "stop",
        "login",
        "exec",
        "worker-exec",
        "res-update",
        "remove",
        "clear",
        "devbox-init",
    }
)


def log_path() -> Path:
    override = os.environ.get("QZCLI_OPS_LOG", "").strip()
    return Path(override) if override else Path(CONFIG_DIR) / LOG_NAME


def _safe_argv(limit: int = 3):
    """只取 argv 前几段。

    **不能记完整命令行** —— ``qzcli login --password xxx`` 这种会把密码写进日志。
    前三段足够认出是哪个子命令，也就够定位问题了。
    """
    return [os.path.basename(a) if i == 0 else a for i, a in enumerate(sys.argv[:limit])]


def _rotate_if_needed(path: Path) -> None:
    try:
        if path.exists() and path.stat().st_size > MAX_BYTES:
            backup = path.with_suffix(path.suffix + ".1")
            if backup.exists():
                backup.unlink()
            path.rename(backup)
    except OSError:
        pass  # 轮转失败不该影响记录本身，更不该影响命令


def record(
    op: str,
    outcome: str = "ok",
    target: str = "",
    err_class: str = "",
    duration_ms: Optional[int] = None,
    **extra,
) -> bool:
    """追加一条操作记录。返回是否真的写成功（调用方**不该**依赖这个返回值）。

    Args:
        op: 操作名，见 :data:`RECORDED_OPS`。不在表里的直接忽略，
            避免有人顺手把只读命令也记进来把日志冲爆。
        outcome: ``ok`` / ``error`` / ``blocked``。
        target: 操作对象（job_id / notebook / workspace），便于回溯。
        err_class: 失败时的异常类名，**不记异常消息**（可能带敏感内容）。
    """
    if op not in RECORDED_OPS:
        return False

    entry = {
        "ts_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "op": op,
        "outcome": outcome,
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "argv": _safe_argv(),
    }
    if target:
        entry["target"] = target
    if err_class:
        entry["err_class"] = err_class
    if duration_ms is not None:
        entry["duration_ms"] = duration_ms
    entry.update({k: v for k, v in extra.items() if v not in (None, "")})

    path = log_path()
    _rotate_if_needed(path)
    line = json.dumps(entry, ensure_ascii=False) + "\n"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # O_APPEND + 单次 write：多进程并发追加时不会互相插进对方的行里。
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, line.encode("utf-8"))
        finally:
            os.close(fd)
        return True
    except OSError:
        # 日志写不进去就算了 —— 诊断设施不许把命令带崩。
        return False


class timed:
    """上下文管理器：自动记 outcome / err_class / duration。

    用法::

        with opslog.timed("create", target=name):
            ...  # 抛异常时会记 outcome=error，然后异常继续往上抛（不吞）
    """

    def __init__(self, op: str, target: str = "", **extra):
        self.op, self.target, self.extra = op, target, extra
        self._t0 = 0.0

    def __enter__(self):
        self._t0 = time.time()
        return self

    def __exit__(self, exc_type, exc, _tb):
        record(
            self.op,
            outcome="ok" if exc_type is None else "error",
            target=self.target,
            err_class=exc_type.__name__ if exc_type else "",
            duration_ms=int((time.time() - self._t0) * 1000),
            **self.extra,
        )
        return False  # 绝不吞异常


def read(path: Optional[Path] = None, op: Optional[str] = None, since_hours: Optional[float] = None):
    """读回日志，供 ``qzcli ops`` 用。坏行跳过而不是整体报错。"""
    p = Path(path) if path else log_path()
    if not p.exists():
        return []
    cutoff = None
    if since_hours:
        cutoff = time.gmtime(time.time() - since_hours * 3600)
        cutoff = time.strftime("%Y-%m-%dT%H:%M:%SZ", cutoff)
    out = []
    try:
        for line in p.read_text("utf-8", "replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue  # 半截行（比如断电写坏的）跳过，不让整个读取失败
            if op and rec.get("op") != op:
                continue
            if cutoff and rec.get("ts_utc", "") < cutoff:
                continue
            out.append(rec)
    except OSError:
        return out
    return out
