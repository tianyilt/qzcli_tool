"""
配置管理模块
"""

import os
import json
from pathlib import Path
from typing import Optional, Dict, Any

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


def get_credentials() -> tuple[str, str]:
    """获取认证信息，优先使用环境变量"""
    config = load_config()
    
    username = os.environ.get("QZCLI_USERNAME") or config.get("username") or ""
    password = os.environ.get("QZCLI_PASSWORD") or config.get("password") or ""
    
    return username, password


def get_api_base_url() -> str:
    """获取 API 基础 URL"""
    config = load_config()
    return os.environ.get("QZCLI_API_URL") or config.get("api_base_url", DEFAULT_CONFIG["api_base_url"])


def init_config(username: str, password: str, api_base_url: Optional[str] = None) -> None:
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
