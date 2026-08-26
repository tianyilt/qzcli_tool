"""
配置管理模块
"""

import hashlib
import json
import os
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

# 默认配置
DEFAULT_CONFIG = {
    "api_base_url": "https://qz.sii.edu.cn",
    "username": "",
    "password": "",
    "token_cache_enabled": True,
    "proxy": "",
}


def should_bypass_proxy(url: str) -> bool:
    """按 ``NO_PROXY`` / ``no_proxy`` 判断这个 URL 该不该绕过代理。

    直接复用 ``requests.utils.should_bypass_proxies`` —— 匹配规则有一堆边角
    （前导点、端口、IP 段、``*``），自己手写一定漏。
    """
    try:
        from requests.utils import should_bypass_proxies
    except ImportError:  # pragma: no cover —— requests 是硬依赖，兜底而已
        return False
    try:
        return bool(should_bypass_proxies(url, no_proxy=None))
    except Exception:  # noqa: BLE001 —— 判不出来就当不绕过，保持旧行为
        return False


def get_proxy(url: str = "") -> str:
    """获取 HTTP 请求所用代理地址。

    精度顺序: ``~/.qzcli/config.json`` 的 ``proxy`` 字段 → 环境变量
    ``ALL_PROXY`` → 环境变量 ``HTTPS_PROXY``。返回的字符串可以是任一受支持
    scheme 的 URL,包含 ``http://``、``https://``、``socks4://``、``socks4a://``、
    ``socks5://``、``socks5h://``;``qzcli.api._get_pool_manager`` 会根据
    scheme 选择合适的 urllib3 manager。空字符串表示不走代理(直连)。

    **传 ``url`` 时会先看 ``NO_PROXY``**，命中就返回空串（直连）。

    为什么加这个参数：qzcli 自建 urllib3 pool manager、CAS 登录还显式设了
    ``trust_env=False``，等于把 requests 自带的 ``no_proxy`` 处理整个绕开了。
    后果是**只要环境里有代理变量，``no_proxy`` 就是摆设**。

    2026-08 真出过事：2224 上的看板进程带着 ``https_proxy=127.0.0.1:7891``
    （clash），而 ``qz.sii.edu.cn`` 走那个代理是 SSL EOF。加 ``no_proxy=.sii.edu.cn``
    没用，最后只能把整个代理环境清空 —— 副作用是那个进程从此完全没法用代理
    访问外网。

    ``url`` 不传时行为和以前**完全一致**，老调用点不受影响。
    """
    cfg = load_config()
    proxy = (
        cfg.get("proxy", "")
        or os.environ.get("ALL_PROXY")
        or os.environ.get("HTTPS_PROXY", "")
    )
    if not proxy:
        return ""
    # NO_PROXY 对「配置文件里配的代理」同样生效 —— 用户写了 no_proxy 就是
    # 表达「这个域名别走代理」，跟代理是从哪读来的无关。
    if url and should_bypass_proxy(url):
        return ""
    return proxy


# 配置目录。``QZCLI_HOME`` 可以把**整个状态目录**搬走（config.json / .cookie /
# jobs.json / resources.json / .relogin.cooldown / .relogin.lock 全部由它派生），
# 用于给某个长跑项目一份互不干扰的独立副本——典型场景是 MoVA2 的冻结版 qzcli：
# 它有自己的凭据和 cookie，我在 ``~/.qzcli`` 这边做什么都影响不到它。
#
# ⚠️ 这是**模块级常量**，import 时就求值完了。所以 ``QZCLI_HOME`` 必须在 python
# 进程启动**之前**设好（wrapper 里 export），运行中改 os.environ 不会生效。
#
# 登录失败封锁（.relogin.cooldown）也跟着一起搬，这是有意的：封锁在每个 home
# 内部依然是永久的，所以账号被锁时另一份 home 至多多试 1 次就自己停住，不会
# 演变成重试风暴——不值得为这 1 次造一个"不跟随 QZCLI_HOME"的特例目录。
#
# **多份 home 各持一份 cookie 是安全的，平台是多会话。** 2026-08-27 实测：
# 本机 cookie 探针为「有效」→ 在另一台机器上用同一账号登录一次 → 立刻重探本机
# cookie（内容指纹未变，只看服务端认不认）→ 仍然「有效」。
# 也就是说另一处登录**不会把已有会话踢下线**，不存在「两份 cookie 互相失效、
# 各自不停重登」这种放大器。
# 曾经担心平台是单会话、多份 home 会互相踢，为此考虑过把 .cookie 做成软链共用 ——
# 实验推翻了这个担心，维持各自独立，隔离性更好也更简单。
CONFIG_DIR = Path(os.environ.get("QZCLI_HOME", "").strip() or Path.home() / ".qzcli")
CONFIG_FILE = CONFIG_DIR / "config.json"
JOBS_FILE = CONFIG_DIR / "jobs.json"
TOKEN_CACHE_FILE = CONFIG_DIR / ".token_cache"
COOKIE_FILE = CONFIG_DIR / ".cookie"
DEFAULT_ENV_FILE = CONFIG_DIR / ".env"
CREATE_INTERACTIVE_SNAPSHOT_FILE = CONFIG_DIR / "create_interactive_snapshot.json"


def ensure_config_dir() -> Path:
    """确保配置目录存在"""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    return CONFIG_DIR


def load_config() -> Dict[str, Any]:
    """加载配置文件"""
    ensure_config_dir()

    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                config = json.load(f)
                # 合并默认配置
                return {**DEFAULT_CONFIG, **config}
        except (json.JSONDecodeError, IOError):
            pass

    return DEFAULT_CONFIG.copy()


def save_config(config: Dict[str, Any]) -> None:
    """保存配置文件"""
    ensure_config_dir()

    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def get_env_file_path() -> Path:
    """返回 qzcli 默认使用的 .env 路径。"""
    env_file = os.environ.get("QZCLI_ENV_FILE", "").strip()
    if env_file:
        return Path(env_file).expanduser()
    return DEFAULT_ENV_FILE


def load_env_file() -> Dict[str, str]:
    """读取 qzcli 默认路径或 QZCLI_ENV_FILE 指定的 .env 文件。"""
    env_file = get_env_file_path()
    if not env_file.exists():
        return {}

    values: Dict[str, str] = {}
    try:
        with open(env_file, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].lstrip()
                if "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue

                if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                    value = value[1:-1]
                values[key] = value
    except IOError:
        return {}

    return values


def get_credentials() -> tuple[str, str]:
    """获取认证信息，优先使用环境变量，其次读取默认路径或指定路径的 .env 文件。"""
    config = load_config()
    env_file_values = load_env_file()

    username = (
        os.environ.get("QZCLI_USERNAME")
        or env_file_values.get("QZCLI_USERNAME")
        or config.get("username")
        or ""
    )
    password = (
        os.environ.get("QZCLI_PASSWORD")
        or env_file_values.get("QZCLI_PASSWORD")
        or config.get("password")
        or ""
    )

    return username, password


def get_api_base_url() -> str:
    """获取 API 基础 URL"""
    config = load_config()
    env_file_values = load_env_file()
    return (
        os.environ.get("QZCLI_API_URL")
        or env_file_values.get("QZCLI_API_URL")
        or config.get("api_base_url", DEFAULT_CONFIG["api_base_url"])
    )


# 自动生成的 session id 缓存：**同一进程内必须稳定**，否则同一个 agent 的多次
# exec 会落到不同 session，attach/list 就串不起来了。
_AUTO_SESSION_ID: Optional[str] = None

# session id 会进远端目录名和 job_id，必须文件名安全。
_SESSION_ID_SAFE_RE = re.compile(r"[^A-Za-z0-9_-]")
_SESSION_ID_MAX_LEN = 24


def _sanitize_session_id(raw: str) -> str:
    """把用户给的 session id 收敛成文件名安全的短串。

    会拼进 ``/tmp/.qzcli/<session>/`` 和 job_id，所以不能有 ``/``、空格、中文。
    非法字符换成 ``-``，超长截断。

    **全是非法字符时（比如纯中文的 session 名）不能返回空串** —— 那样会静默
    退回自动值，用户显式设的 session 被无视，而且两个不同的中文名会撞成同一个。
    这种情况改用原串的哈希，保证"不同输入 → 不同 session"。
    """
    raw = raw.strip()
    if not raw:
        return ""
    cleaned = _SESSION_ID_SAFE_RE.sub("-", raw)[:_SESSION_ID_MAX_LEN].strip("-")
    if cleaned:
        return cleaned
    return "s" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:8]


def get_session_id() -> str:
    """获取本次调用所属的 session id。

    优先级和 ``get_credentials`` 一致（env → .env 文件 → config.json），
    都没有则**按进程自动生成**一个并在进程内复用。

    这是给多 agent 并发用的：多个 agent 同时对同一台开发机跑 ``qzcli exec``
    时，靠它把各自的输出隔离到不同目录、也让 ``exec --list`` 只列自己的任务。

    - 想让多个进程归到同一个 session（比如一个 agent 起了多个 qzcli 子进程），
      显式设 ``QZCLI_SESSION_ID``
    - 不设也不会串车：自动值每进程一个
    """
    global _AUTO_SESSION_ID

    config = load_config()
    env_file_values = load_env_file()
    explicit = (
        os.environ.get("QZCLI_SESSION_ID")
        or env_file_values.get("QZCLI_SESSION_ID")
        or config.get("session_id")
        or ""
    )
    if explicit:
        cleaned = _sanitize_session_id(explicit)
        if cleaned:
            return cleaned

    if _AUTO_SESSION_ID is None:
        _AUTO_SESSION_ID = uuid.uuid4().hex[:8]
    return _AUTO_SESSION_ID


#: 不显式 ``--priority`` 时用的优先级。数字越小越低优（1/3→LOW、4→NORMAL、
#: 9/10→HIGH，实测 1494 个真实任务，跨全部工作空间一致）。
#:
#: 取 3（LOW）而不是 10：不指定优先级的多半是调试 / 试跑 / 脚本随手提的任务，
#: 让它默认拿最高优去抢生产的卡是不合理的默认。
FALLBACK_DEFAULT_PRIORITY = 3


def get_default_priority() -> int:
    """``qzcli create`` 不带 ``--priority`` 时用哪个优先级。

    **这是一个可覆盖的默认值，不是硬编码。** 历史上这里是 10（最高优），改成 3
    对「原来不写 --priority、靠默认拿高优」的脚本是行为变更 —— 那些任务会从直接
    跑变成排队。所以给一条不用改调用点就能恢复原状的路：

    优先级顺序（与 ``get_session_id`` / ``get_credentials`` 同构）：

    1. 环境变量 ``QZCLI_DEFAULT_PRIORITY``
    2. ``~/.qzcli/.env`` 里的 ``QZCLI_DEFAULT_PRIORITY``
    3. ``config.json`` 的 ``default_priority``
    4. 兜底 ``FALLBACK_DEFAULT_PRIORITY``（3）

    要恢复旧行为，加一行 ``QZCLI_DEFAULT_PRIORITY=10`` 即可。

    非法值（非整数、超出 1-10）一律忽略并回落到兜底值 —— 配错了不该让提交失败，
    但也不能拿一个平台会拒的值去提交。
    """
    config = load_config()
    env_file_values = load_env_file()
    raw = (
        os.environ.get("QZCLI_DEFAULT_PRIORITY")
        or env_file_values.get("QZCLI_DEFAULT_PRIORITY")
        or config.get("default_priority")
        or ""
    )
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return FALLBACK_DEFAULT_PRIORITY
    if 1 <= value <= 10:
        return value
    return FALLBACK_DEFAULT_PRIORITY


def init_config(
    username: str, password: str, api_base_url: Optional[str] = None
) -> None:
    """初始化配置"""
    config = load_config()
    config["username"] = username
    config["password"] = password
    if api_base_url:
        config["api_base_url"] = api_base_url
    save_config(config)


def _atomic_write_json(path: Path, data: Any) -> None:
    """原子写 JSON：先写同目录临时文件，再 ``os.replace`` 换上去。

    **不能直接 ``open(path, "w")`` 再 dump。** 那样在截断和写完之间有一个窗口，
    文件是空的或半截的；并发读的进程/线程这时 ``json.load`` 会失败，而各个读取点
    都把失败当成"没有这个文件"处理。

    真实后果（实测复现）：8 个并发登录里偶发 2 次真实 CAS 登录 —— 因为某个线程
    在这个窗口里读 cookie 拿到 ``None``，于是重登去重判据失效、又打了一次 CAS。
    在别处则可能表现为莫名其妙的「未设置 cookie」。

    ``os.replace`` 在同一文件系统上是原子的，读者要么看到旧内容、要么看到新内容，
    不会看到中间态。
    """
    # 临时文件名必须**每次调用唯一**。只用 PID 是不够的 —— 同一进程的多个线程
    # 会撞同一个名字，然后互相把对方还没 replace 的临时文件删掉
    # （我第一版就这么写的，8 线程并发直接 FileNotFoundError）。
    # mkstemp 由内核保证唯一，且建在同目录下以确保 os.replace 是同文件系统的原子操作。
    import tempfile

    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".tmp.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    except BaseException:
        # 只在失败时清理；成功路径上临时文件已经被 replace 掉了
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def get_token_cache() -> Optional[Dict[str, Any]]:
    """获取缓存的 token"""
    if not TOKEN_CACHE_FILE.exists():
        return None

    try:
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
            # 检查是否过期（预留 5 分钟缓冲）
            import time

            if cache.get("expires_at", 0) > time.time() + 300:
                return cache
    except (json.JSONDecodeError, IOError):
        pass

    return None


def save_token_cache(token: str, expires_in: int) -> None:
    """保存 token 缓存"""
    ensure_config_dir()

    import time

    cache = {
        "token": token,
        "expires_at": time.time() + expires_in,
    }

    _atomic_write_json(TOKEN_CACHE_FILE, cache)


def clear_token_cache() -> None:
    """清除 token 缓存"""
    if TOKEN_CACHE_FILE.exists():
        TOKEN_CACHE_FILE.unlink()


def save_cookie(cookie: str, workspace_id: str = "") -> None:
    """保存浏览器 cookie"""
    ensure_config_dir()

    import time

    data = {
        "cookie": cookie,
        "workspace_id": workspace_id,
        "saved_at": time.time(),
    }

    _atomic_write_json(COOKIE_FILE, data)


def get_cookie() -> Optional[Dict[str, Any]]:
    """获取保存的 cookie"""
    if not COOKIE_FILE.exists():
        return None

    try:
        with open(COOKIE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def clear_cookie() -> None:
    """清除 cookie"""
    if COOKIE_FILE.exists():
        COOKIE_FILE.unlink()


# 资源缓存文件
RESOURCES_FILE = CONFIG_DIR / "resources.json"


def save_resources(
    workspace_id: str, resources: Dict[str, Any], name: str = ""
) -> None:
    """
    保存工作空间的资源配置到本地缓存

    Args:
        workspace_id: 工作空间 ID
        resources: 资源配置（projects, compute_groups, specs）
        name: 工作空间名称（可选）
    """
    ensure_config_dir()

    import time

    # 读取现有缓存
    all_resources = load_all_resources()

    # 更新该工作空间的资源
    all_resources[workspace_id] = {
        "id": workspace_id,
        "name": name or all_resources.get(workspace_id, {}).get("name", ""),
        "projects": {p["id"]: p for p in resources.get("projects", [])},
        "compute_groups": {g["id"]: g for g in resources.get("compute_groups", [])},
        "specs": {s["id"]: s for s in resources.get("specs", [])},
        "updated_at": time.time(),
    }

    with open(RESOURCES_FILE, "w", encoding="utf-8") as f:
        json.dump(all_resources, f, indent=2, ensure_ascii=False)


def load_all_resources(include_unavailable: bool = False) -> Dict[str, Any]:
    """加载所有工作空间的资源缓存。

    默认**跳过标记为不可用的工作空间**（已禁用 / 当前账号无权限）。
    ``avail`` / ``usage`` / ``hpc-usage`` 这些命令都从这里枚举工作空间，
    不过滤的话每次都会去查这些空间、然后刷一屏
    ``AccessForbidden: 该空间已被禁用`` 警告 —— 噪声盖住真正的问题。

    标记由 ``mark_workspace_unavailable`` 在实际撞到权限错误时写入，
    ``res -u`` 刷新时会重新验证。
    """
    if not RESOURCES_FILE.exists():
        return {}

    try:
        with open(RESOURCES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}

    if include_unavailable:
        return data
    return {
        ws_id: ws
        for ws_id, ws in data.items()
        if not (isinstance(ws, dict) and ws.get("unavailable"))
    }


def mark_workspace_unavailable(workspace_id: str, reason: str = "") -> None:
    """把某个工作空间标记为不可用，后续多空间命令直接跳过它。

    在实际撞到 ``AccessForbidden``（已禁用 / 无权限）时调用。这样"第一次遇到
    才知道"，不用预先维护一份黑名单；``res -u`` 重刷时会清掉标记重新验证。
    """
    if not RESOURCES_FILE.exists():
        return
    try:
        with open(RESOURCES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return
    ws = data.get(workspace_id)
    if not isinstance(ws, dict) or ws.get("unavailable"):
        return
    ws["unavailable"] = True
    ws["unavailable_reason"] = reason[:200]
    try:
        with open(RESOURCES_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except IOError:
        pass


def save_create_interactive_snapshot(snapshot: Dict[str, Any]) -> None:
    """保存 create -i 使用的交互资源快照。"""
    ensure_config_dir()

    import time

    payload = dict(snapshot or {})
    payload["saved_at"] = time.time()
    with open(CREATE_INTERACTIVE_SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def load_create_interactive_snapshot() -> Optional[Dict[str, Any]]:
    """读取 create -i 使用的交互资源快照。"""
    if not CREATE_INTERACTIVE_SNAPSHOT_FILE.exists():
        return None

    try:
        with open(CREATE_INTERACTIVE_SNAPSHOT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def get_workspace_resources(workspace_id: str) -> Optional[Dict[str, Any]]:
    """
    获取指定工作空间的资源缓存

    Args:
        workspace_id: 工作空间 ID

    Returns:
        资源配置字典，或 None（未缓存 / 缓存内容不可用）

    Note:
        缓存被写坏时（半截写入、手工编辑、老格式残留）这里的值可能不是 dict。
        原样返回会让下游的 ``.get(...)`` 抛 ``AttributeError`` 把整条命令打崩 ——
        而调用方全都按"None 表示没缓存"来处理，所以这种情况当作没缓存即可。
    """
    all_resources = load_all_resources()
    cached = all_resources.get(workspace_id)
    if cached is not None and not isinstance(cached, dict):
        return None
    return cached


def set_workspace_name(workspace_id: str, name: str) -> bool:
    """
    设置工作空间的名称（别名）

    Args:
        workspace_id: 工作空间 ID
        name: 名称

    Returns:
        是否成功
    """
    all_resources = load_all_resources()

    if workspace_id not in all_resources:
        # 创建一个空的工作空间条目
        import time

        all_resources[workspace_id] = {
            "id": workspace_id,
            "name": name,
            "projects": {},
            "compute_groups": {},
            "specs": {},
            "updated_at": time.time(),
        }
    else:
        all_resources[workspace_id]["name"] = name

    ensure_config_dir()
    with open(RESOURCES_FILE, "w", encoding="utf-8") as f:
        json.dump(all_resources, f, indent=2, ensure_ascii=False)

    return True


def find_workspace_by_name(name: str) -> Optional[str]:
    """
    通过名称查找工作空间 ID

    Args:
        name: 工作空间名称（支持模糊匹配）

    Returns:
        工作空间 ID，或 None
    """
    all_resources = load_all_resources()

    # 精确匹配优先
    for ws_id, ws_data in all_resources.items():
        if ws_data.get("name", "") == name:
            return ws_id

    # 模糊匹配
    for ws_id, ws_data in all_resources.items():
        if name.lower() in ws_data.get("name", "").lower():
            return ws_id

    return None


def find_resource_by_name(
    workspace_id: str, resource_type: str, name: str
) -> Optional[Dict[str, Any]]:
    """
    通过名称查找资源（项目、计算组、规格）

    Args:
        workspace_id: 工作空间 ID
        resource_type: 资源类型 (projects, compute_groups, specs)
        name: 资源名称（支持模糊匹配）

    Returns:
        资源配置字典，或 None
    """
    ws_resources = get_workspace_resources(workspace_id)
    if not ws_resources:
        return None

    resources = ws_resources.get(resource_type, {})

    # 精确匹配优先
    for res_id, res_data in resources.items():
        res_name = res_data.get("name", "")
        if res_name == name:
            return res_data

    # 模糊匹配
    for res_id, res_data in resources.items():
        res_name = res_data.get("name", "")
        if name.lower() in res_name.lower():
            return res_data

    return None


def list_cached_workspaces() -> List[Dict[str, Any]]:
    """
    列出所有已缓存的工作空间

    Returns:
        工作空间列表 [{id, name, updated_at, ...}, ...]
    """
    all_resources = load_all_resources()
    result = []

    for ws_id, ws_data in all_resources.items():
        result.append(
            {
                "id": ws_id,
                "name": ws_data.get("name", ""),
                "updated_at": ws_data.get("updated_at", 0),
                "project_count": len(ws_data.get("projects", {})),
                "compute_group_count": len(ws_data.get("compute_groups", {})),
                "spec_count": len(ws_data.get("specs", {})),
            }
        )

    return result


def update_workspace_projects(
    workspace_id: str, projects: List[Dict[str, Any]], name: str = ""
) -> int:
    """
    增量更新工作空间的项目列表

    Args:
        workspace_id: 工作空间 ID
        projects: 项目列表 [{"id": ..., "name": ...}, ...]
        name: 工作空间名称（可选）

    Returns:
        新增的项目数量
    """
    ensure_config_dir()

    import time

    # 读取现有缓存
    all_resources = load_all_resources()

    # 获取或创建该工作空间的条目
    if workspace_id not in all_resources:
        all_resources[workspace_id] = {
            "id": workspace_id,
            "name": name,
            "projects": {},
            "compute_groups": {},
            "specs": {},
            "updated_at": time.time(),
        }

    ws_data = all_resources[workspace_id]
    existing_projects = ws_data.get("projects", {})

    # 更新名称（如果提供）
    if name:
        ws_data["name"] = name

    # 增量更新项目
    new_count = 0
    for proj in projects:
        proj_id = proj.get("id", "")
        if proj_id and proj_id not in existing_projects:
            existing_projects[proj_id] = proj
            new_count += 1
        elif proj_id:
            # 更新已有项目的名称（可能有变化）
            existing_projects[proj_id].update(proj)

    ws_data["projects"] = existing_projects
    ws_data["updated_at"] = time.time()

    with open(RESOURCES_FILE, "w", encoding="utf-8") as f:
        json.dump(all_resources, f, indent=2, ensure_ascii=False)

    return new_count


def update_workspace_compute_groups(
    workspace_id: str, compute_groups: List[Dict[str, Any]], name: str = ""
) -> int:
    """
    增量更新工作空间的计算组列表

    Args:
        workspace_id: 工作空间 ID
        compute_groups: 计算组列表 [{"id": ..., "name": ..., "gpu_type": ...}, ...]
        name: 工作空间名称（可选）

    Returns:
        新增的计算组数量
    """
    ensure_config_dir()

    import time

    # 读取现有缓存
    all_resources = load_all_resources()

    # 获取或创建该工作空间的条目
    if workspace_id not in all_resources:
        all_resources[workspace_id] = {
            "id": workspace_id,
            "name": name,
            "projects": {},
            "compute_groups": {},
            "specs": {},
            "updated_at": time.time(),
        }

    ws_data = all_resources[workspace_id]
    existing_groups = ws_data.get("compute_groups", {})

    # 更新名称（如果提供）
    if name:
        ws_data["name"] = name

    # 增量更新计算组
    new_count = 0
    for group in compute_groups:
        group_id = group.get("id", "")
        if group_id and group_id not in existing_groups:
            existing_groups[group_id] = group
            new_count += 1
        elif group_id:
            # 更新已有计算组的信息（可能有变化）
            existing_groups[group_id].update(group)

    ws_data["compute_groups"] = existing_groups
    ws_data["updated_at"] = time.time()

    with open(RESOURCES_FILE, "w", encoding="utf-8") as f:
        json.dump(all_resources, f, indent=2, ensure_ascii=False)

    return new_count
