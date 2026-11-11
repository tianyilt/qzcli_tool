"""在**分布式训练任务的 worker 容器**里执行命令。

## 和 ``qzcli exec`` 的区别

``qzcli exec`` 服务的是 **notebook 开发机**：走 Jupyter Contents API + terminal，
需要 notebook_id 和 proxy 地址。**训练任务的 worker 是另一类对象**，没有 Jupyter
proxy，走的是完全不同的通道，所以单开 ``qzcli worker exec``。

## 通道：WebSocket，不是 REST

平台前端「进入容器」按钮背后是：

    wss://qz.sii.edu.cn/api/v2/train_job/remote_cmd?job_id=<>&instance_name=<>

要点（都是实测踩出来的，别照直觉改）：

- **是 WebSocket 不是 REST。** 一开始按 REST ``POST /api/v2/train_jobs/instances/exec``
  打，一路 404 —— 那个路径根本不存在。前端 JS 里的映射是：

      { hpc: "/api/v2/hpc_jobs/instances/exec",
        ray: "/api/v2/ray_job/instances/exec",
        inference_servings: "/api/v2/inference_servings/instances/exec",
        default: "/api/v2/train_job/remote_cmd" }   ← 分布式训练走 default

  只有 hpc/ray/inference 那三个是 ``instances/exec`` 形态；**训练任务是 remote_cmd**。
- **参数走 query string，不是 body。**
- **平台没有「发一条命令返回 stdout」的非交互接口** —— 只有这条交互式 PTY。
  所以拿结果得自己在流里封装：起 shell → 发命令 + 哨兵 → 读到哨兵为止。

## 连上之后

前端的做法照抄：先发 ``command -v bash ... exec bash || exec sh`` 起 shell，
再 ``stty`` 设终端尺寸。返回的是**带 ANSI 转义的 PTY 流**（含欢迎 banner、
命令回显、提示符），要清洗才能当输出用。
"""

import re
import ssl
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse

from .config import get_cookie, get_proxy
from .diag import swallowed

#: 分布式训练 worker 的 PTY 通道。hpc/ray/inference 各有自己的 ``instances/exec``，
#: 本模块目前只做训练任务（用户的实际场景）。
_TRAIN_REMOTE_CMD = "/api/v2/train_job/remote_cmd"

#: 连上后用来起 shell 的第一条命令，照抄前端。
_START_SHELL = "command -v bash >/dev/null 2>&1 && exec bash || exec sh"

#: 浏览器 UA —— 平台网关会看，用 python-requests 的默认 UA 会被挡。
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36"
)

_ANSI_CSI = re.compile(r"\x1b\[[0-9;?]*[a-zA-Z]")
_ANSI_OSC = re.compile(r"\x1b\][^\x07]*\x07")


class WorkerExecError(Exception):
    """连不上 worker 容器，或命令没能正常收尾。"""


def strip_ansi(text: str) -> str:
    """去掉 PTY 流里的 ANSI 转义（CSI + OSC）。"""
    return _ANSI_OSC.sub("", _ANSI_CSI.sub("", text or ""))


def default_instance_name(job_id: str, index: int = 0) -> str:
    """训练任务的实例名约定：``<job_id>-worker-<N>``（实测形态）。"""
    return f"{job_id}-worker-{index}"


def _connect(job_id: str, instance_name: str, timeout: int = 30):
    try:
        import websocket  # noqa: PLC0415 —— 可选依赖，只有本命令需要
    except ImportError as exc:  # pragma: no cover
        raise WorkerExecError(
            "缺少 websocket-client 依赖：pip install websocket-client"
        ) from exc

    from .api import get_api, QZ_CLIENT_SOURCE

    api = get_api()
    server = api.base_url.rstrip("/")
    headers = [f"Origin: {server}", f"User-Agent: {_UA}"]
    if getattr(api, "auth_mode", None) == "qz":
        headers.extend(
            [
                f"Authorization: Bearer {api._official_token()}",
                f"X-Inspire-Client-Source: {QZ_CLIENT_SOURCE}",
            ]
        )
    else:
        cookie = (get_cookie() or {}).get("cookie")
        if not cookie:
            raise WorkerExecError("没有登录态，请先运行 `qzcli login`")
        headers.append(f"Cookie: {cookie}")

    base = server.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    url = f"{base}{_TRAIN_REMOTE_CMD}?" + urlencode(
        {"job_id": job_id, "instance_name": instance_name}
    )

    # 代理：本机（macOS + clash）只能靠代理解析 qz.sii.edu.cn，直连会 DNS 失败。
    # get_proxy 会按 NO_PROXY 判断，不该走代理时返回空串。
    proxy_kwargs: Dict[str, object] = {}
    proxy = (get_proxy(server) or "").strip()
    if proxy:
        parsed = urlparse(proxy if "://" in proxy else f"http://{proxy}")
        if parsed.hostname and parsed.port:
            proxy_kwargs = {
                "http_proxy_host": parsed.hostname,
                "http_proxy_port": parsed.port,
                "proxy_type": "http",
            }

    try:
        return websocket.create_connection(
            url,
            header=headers,
            timeout=timeout,
            sslopt={"cert_reqs": ssl.CERT_NONE},
            **proxy_kwargs,
        )
    except Exception as exc:  # noqa: BLE001 —— 底层异常类型很杂，统一成本模块的错
        raise WorkerExecError(
            f"连不上 worker 容器（{instance_name}）：{type(exc).__name__}: {exc}\n"
            "  常见原因：任务不在运行中 / 实例名不对 / 登录态过期"
        ) from exc


def _drain(ws, seconds: float) -> str:
    """把当前可读的流读干净。到点或没数据就返回。"""
    import websocket  # noqa: PLC0415

    chunks: List[str] = []
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            msg = ws.recv()
        except websocket.WebSocketTimeoutException:
            break
        except Exception:  # noqa: BLE001 —— 连接关闭等，交给上层按已收内容处理
            break
        chunks.append(msg.decode("utf-8", "replace") if isinstance(msg, bytes) else msg)
    return "".join(chunks)


def run_many_in_worker(
    job_id: str,
    commands: List[Tuple[str, str]],
    instance_name: Optional[str] = None,
    *,
    banner_wait: float = 6.0,
    command_timeout: float = 60.0,
) -> List[Tuple[str, int, str]]:
    """**一条连接**里依次跑多条命令，返回 ``[(标签, exit_code, 输出), ...]``。

    为什么不循环调 :func:`run_in_worker`：那样每条命令都新建一个 WebSocket，
    连续快速建连会被平台拒（实测报 ``socket is already closed``）。体检类场景
    动辄五六条命令，必须复用同一条连接 —— 顺带也快得多（省掉每次 6 秒的
    banner 等待）。
    """
    instance_name = instance_name or default_instance_name(job_id)
    ws = _connect(job_id, instance_name)
    results: List[Tuple[str, int, str]] = []
    try:
        ws.settimeout(2)
        _drain(ws, banner_wait)
        ws.send(_START_SHELL + "\n")
        _drain(ws, 3)

        for label, command in commands:
            mark = f"QZW_{int(time.time() * 1000)}"
            ws.send(f"{command}; echo {mark}_$?\n")
            buf = ""
            deadline = time.time() + command_timeout
            while time.time() < deadline:
                chunk = _drain(ws, 3)
                buf += chunk
                if buf.count(mark) >= 2:
                    break
                if not chunk and mark in buf:
                    break
            results.append((label,) + _parse_output(buf, command, mark))
    finally:
        try:
            ws.close()
        except Exception as exc:  # noqa: BLE001 —— 关闭失败不该丢掉已拿到的结果
            swallowed("worker exec/关闭连接", exc)
    return results


def _parse_output(buf: str, command: str, mark: str) -> Tuple[int, str]:
    """把一段 PTY 流清洗成 ``(exit_code, 命令自身输出)``。"""
    clean = strip_ansi(buf)
    exit_code = -1
    hits = re.findall(rf"{mark}_(\d+)", clean)
    if hits:
        exit_code = int(hits[-1])
    start = clean.find(command)
    body = clean[start + len(command) :] if start >= 0 else clean
    body = re.sub(rf";?\s*echo {mark}_\$\?\r?\n?", "", body, count=1)
    body = re.sub(rf"{mark}_\d+\s*", "", body)
    body = re.sub(r"\r\n", "\n", body)
    body = re.sub(r"\n?\[[^\]\n]+\]\$\s*$", "", body)
    return exit_code, body.strip("\n").strip("\r")


def run_in_worker(
    job_id: str,
    command: str,
    instance_name: Optional[str] = None,
    *,
    banner_wait: float = 6.0,
    command_timeout: float = 60.0,
) -> Tuple[int, str]:
    """在 worker 容器里跑一条命令，返回 ``(exit_code, output)``。

    做法：连上 → 吃掉欢迎 banner → 起 shell → 发 ``<cmd>; echo <哨兵>_$?`` →
    读到哨兵 → 清洗 ANSI → 从哨兵取 exit code。

    哨兵带时间戳，避免和命令自身输出里的字符串撞车；**要等哨兵出现两次**
    （一次是命令回显，一次是真正的执行结果）才算读完。

    Args:
        job_id: 训练任务 id。
        command: 要执行的命令（在容器的 shell 里跑）。
        instance_name: 实例名；缺省用 ``<job_id>-worker-0``。
        banner_wait: 等欢迎 banner 刷完的秒数。太短会把 banner 混进输出。
        command_timeout: 等命令产出的最长秒数。

    Returns:
        ``(exit_code, output)``。取不到 exit code 时返回 ``-1``。
    """
    instance_name = instance_name or default_instance_name(job_id)
    ws = _connect(job_id, instance_name)
    try:
        ws.settimeout(2)
        _drain(ws, banner_wait)  # 欢迎 banner
        ws.send(_START_SHELL + "\n")
        _drain(ws, 3)

        mark = f"QZW_{int(time.time() * 1000)}"
        ws.send(f"{command}; echo {mark}_$?\n")

        buf = ""
        deadline = time.time() + command_timeout
        while time.time() < deadline:
            chunk = _drain(ws, 3)
            buf += chunk
            # 哨兵出现两次 = 回显 + 真结果
            if buf.count(mark) >= 2:
                break
            if not chunk and mark in buf:
                break
    finally:
        try:
            ws.close()
        except Exception as exc:  # noqa: BLE001 —— 关闭失败不该影响已拿到的输出
            swallowed("worker exec/关闭连接", exc)

    clean = strip_ansi(buf)
    exit_code = -1
    hits = re.findall(rf"{mark}_(\d+)", clean)
    if hits:
        exit_code = int(hits[-1])

    # 掐掉命令回显之前的部分和末尾的哨兵/提示符，只留命令自身输出
    start = clean.find(command)
    body = clean[start + len(command) :] if start >= 0 else clean
    body = re.sub(rf";?\s*echo {mark}_\$\?\r?\n?", "", body, count=1)
    body = re.sub(rf"{mark}_\d+\s*", "", body)
    body = re.sub(r"\r\n", "\n", body)
    # 末尾会跟一个 shell 提示符（形如 ``[root:liangtianyi-xxx]$``），那是 PTY 的
    # 产物不是命令输出，去掉；否则每次结果都拖一行噪声。
    body = re.sub(r"\n?\[[^\]\n]+\]\$\s*$", "", body)
    return exit_code, body.strip("\n")
