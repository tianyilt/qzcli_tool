"""被**故意不往上抛**的异常的去处。

## 为什么要有这个模块

有些异常确实不该打断主流程：收尾时解个文件锁失败、`avail` 里某个工作空间没权限、
exec 结束后删临时文件失败 —— 为这些崩掉整条命令是过度反应。

但「不打断主流程」被写成了 `except Exception: pass`，于是变成了**信息彻底消失**。
这个仓库为此付过账：

- `exec` 在两台开发机上都卡满 120s 然后报「超时」。真实原因每一轮轮询都被 `pass`
  掉了，报出来的「超时」和实际发生的事没关系。
- `avail` 的 HPC 利用率整段不见，因为整块 try 里任何一个异常都会让它 `continue`。

这两种症状的共同点是：**退出码 0、没有红字、输出看着像"今天就是没有"**。

## 用法

    try:
        _requests.delete(url, timeout=5)
    except _requests.RequestException as exc:
        swallowed("exec/清理临时文件", exc)

``QZCLI_DEBUG=1`` 时每条立刻打到 stderr；平时静默入环形缓冲，供**报错时回捞原因**：

    exit_code, output, finished = _exec_poll(...)
    if not finished:
        why = last_reason("exec/轮询")   # → "HTTPError: 403 Forbidden" 或 None

## 边界

这个模块**不是**用来放宽捕获范围的许可证。捕获类型该多窄还是多窄 ——
`swallowed()` 解决的是"没人知道发生了什么"，不解决"不该被捕获的东西被捕获了"。
"""

import collections
import os
import sys
from typing import Optional

#: 最近被吞掉的异常。够查一条命令的现场即可，不做持久化。
_RING: "collections.deque" = collections.deque(maxlen=64)


def debug_enabled() -> bool:
    """``QZCLI_DEBUG`` 是否开启。空串 / ``0`` / ``false`` / ``no`` 都算关。"""
    return os.environ.get("QZCLI_DEBUG", "").strip().lower() not in (
        "",
        "0",
        "false",
        "no",
    )


def swallowed(where: str, exc: BaseException) -> None:
    """记一条「捕获了但不上抛」的异常。

    Args:
        where: 现场标识，形如 ``"exec/轮询"``。前缀用于 :func:`last_reason` 回捞。
        exc: 捕获到的异常。
    """
    from .auth_diag import AuthDiagnostic

    reason = AuthDiagnostic().redact(f"{type(exc).__name__}: {exc}")[:300]
    _RING.append((where, reason))
    if debug_enabled():
        print(f"[qzcli:debug] {where} 忽略了 {reason}", file=sys.stderr)


def last_reason(prefix: str) -> Optional[str]:
    """回捞某个现场最近一次被吞掉的原因；没有则 ``None``。

    给「最终要报错、但真实原因在前面被吞了」的地方用 —— 比如 exec 超时。
    """
    for where, reason in reversed(_RING):
        if where.startswith(prefix):
            return reason
    return None


def recent(limit: int = 10):
    """最近 ``limit`` 条，供调试命令 / 测试查看。"""
    return list(_RING)[-limit:]


def clear() -> None:
    """清空。测试用，避免用例之间互相看见对方的记录。"""
    _RING.clear()
