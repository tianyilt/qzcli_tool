"""
配置管理模块
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

# 默认配置
DEFAULT_CONFIG = {
    "api_base_url": "https://qz.sii.edu.cn",
    "username": "",
    "password": "",
    "token_cache_enabled": True,
}

# 配置目录
CONFIG_DIR = Path.home() / ".qzcli"
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

    with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f)


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

    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


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


def load_all_resources() -> Dict[str, Any]:
    """加载所有工作空间的资源缓存"""
    if not RESOURCES_FILE.exists():
        return {}

    try:
        with open(RESOURCES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


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
        资源配置字典，或 None（未缓存）
    """
    all_resources = load_all_resources()
    return all_resources.get(workspace_id)


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
