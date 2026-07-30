"""
启智平台 API 客户端
"""

import functools
import inspect
import json as _json
import random
import sys
import threading
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from . import __version__
from .config import (
    clear_token_cache,
    get_api_base_url,
    get_cookie,
    get_credentials,
    get_proxy,
    get_token_cache,
    save_cookie,
    save_token_cache,
)
from .crypto import encrypt_password

# /api/v2/* requires this header — without it APISIX gateway redirects to
# Keycloak login (returning HTML) even when the Bearer token is valid.
V2_CLIENT_SOURCE = f"qzcli/{__version__}"

# Match the browser-style headers that /api/v1/ cookie endpoints use. The
# platform's /api/v2/ surface piggybacks on the same CAS session cookie that
# `qzcli login` saves.
V2_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)


class QzAPIError(Exception):
    """API 错误。

    ``code``     —— HTTP 状态码（如 401/404），没有则 None。
    ``api_code`` —— v2 信封里 ``ResponseMetadata.Error.Code`` 的原值
                    （如 ``AccessForbidden`` / ``InvalidParameter``）。
                    有了它，下游就能**按结构**判断错误类型，而不是去抠错误文案 ——
                    平台改一个字的措辞就会让 substring 匹配失效。
    """

    def __init__(
        self,
        message: str,
        code: Optional[int] = None,
        api_code: Optional[str] = None,
    ):
        super().__init__(message)
        self.code = code
        self.api_code = api_code


class QzTransientError(QzAPIError):
    """瞬时故障（SSL EOF、连接重置、5xx/代理抖动），值得重试。

    继承自 ``QzAPIError``，所以现有 ``except QzAPIError`` 仍能捕获；区别只是用类型
    标记“可重试”，避免靠匹配错误文案来判断。
    """


# CAS 登录瞬时失败的重试参数（指数退避 + 抖动）。
_LOGIN_MAX_TRIES = 3


def _backoff_delay(attempt: int, base: float = 0.5, cap: float = 2.0) -> float:
    """第 ``attempt`` 次重试（从 0 起）的退避秒数：base*2^n，封顶 cap，叠加抖动。"""
    delay = min(cap, base * (2**attempt))
    return delay + random.uniform(0, delay * 0.25)


def with_auth_retry(method):
    """装饰 cookie 认证的 ``QzAPI`` 方法：遇到 401（cookie 过期）时，用本地凭据
    透明地 ``login_with_cas`` 重新登录一次并重试原调用。

    - 带 ``cookie`` 形参的方法，重试时会换上刚拿到的新 cookie；
    - 自行从磁盘读 cookie 的方法（如 ``_request_v2``），重试时自然读到刷新后的 cookie。

    当没有凭据、``_auto_relogin`` 关闭、或重新登录失败时为 no-op：原始 401 会被重新
    抛出，从而保留既有回退逻辑（例如 token 认证）。
    """
    sig = inspect.signature(method)
    takes_cookie = "cookie" in sig.parameters

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except QzAPIError as exc:
            if exc.code != 401 or not getattr(self, "_auto_relogin", True):
                raise
            new_cookie = self._relogin()
            if not new_cookie:
                raise
            if takes_cookie:
                bound = sig.bind(self, *args, **kwargs)
                bound.arguments["cookie"] = new_cookie
                return method(*bound.args, **bound.kwargs)
            return method(self, *args, **kwargs)

    return wrapper


@lru_cache(maxsize=8)
def _get_pool_manager(proxy: str):
    """Return a cached urllib3 manager for the configured proxy URL."""
    import urllib3

    if not proxy:
        return urllib3.PoolManager()

    normalized = proxy.rstrip("/") + "/"
    if normalized.lower().startswith(
        ("socks4://", "socks4a://", "socks5://", "socks5h://")
    ):
        try:
            from urllib3.contrib.socks import SOCKSProxyManager
        except ImportError as exc:
            raise QzAPIError(
                "当前代理配置需要 SOCKS 支持，请安装 PySocks 或 urllib3[socks]",
            ) from exc
        return SOCKSProxyManager(normalized)

    if normalized.lower().startswith(("http://", "https://")):
        return urllib3.ProxyManager(normalized)

    raise QzAPIError(f"不支持的代理地址: {proxy}")


class _CurlResponse:
    """Minimal response object mimicking requests.Response."""

    def __init__(
        self,
        status_code: int,
        text: str,
        url: str = "",
        headers: Optional[Dict[str, str]] = None,
    ):
        self.status_code = status_code
        self.text = text
        self.url = url
        self.headers = headers or {}

    def json(self):
        return _json.loads(self.text)


def _curl_post(
    url: str,
    *,
    json: Any = None,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 60,
    **_kw,
) -> _CurlResponse:
    """Drop-in replacement for requests.post with explicit proxy handling."""
    if params:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{urlencode(params)}"

    pm = _get_pool_manager((get_proxy() or "").strip())
    body = _json.dumps(json).encode("utf-8") if json is not None else None
    hdrs = dict(headers) if headers else {}
    header_names = {name.lower() for name in hdrs}
    if json is not None and "content-type" not in header_names:
        hdrs["Content-Type"] = "application/json"
    resp = pm.request(
        "POST",
        url,
        body=body,
        headers=hdrs,
        timeout=float(timeout),
        redirect=False,
    )
    return _CurlResponse(
        status_code=resp.status,
        text=resp.data.decode("utf-8", errors="replace"),
        url=url,
        headers=dict(resp.headers),
    )


def _unwrap_v2_result(data: Dict[str, Any]) -> Dict[str, Any]:
    """解封装 v2 Console API 响应（AWS 风格 RPC）。

    错误：`ResponseMetadata.Error`{Code,Message} 或 legacy `code not in (None,0)`。
    成功：取 `Result`（dict），缺则回退 legacy `data`。与平台 Web UI 一致。
    """
    if not isinstance(data, dict):
        return {}
    meta = data.get("ResponseMetadata")
    if isinstance(meta, dict):
        err = meta.get("Error")
        if isinstance(err, dict):
            code = err.get("Code") or "Error"
            message = err.get("Message") or "未知错误"
            raise QzAPIError(f"API 请求失败: {code}: {message}", api_code=str(code))
    elif data.get("code") not in (None, 0):
        raise QzAPIError(
            f"API 请求失败: {data.get('message', '未知错误')}", data.get("code")
        )
    result = data.get("Result")
    if isinstance(result, dict):
        return result
    legacy = data.get("data")
    if isinstance(legacy, dict):
        return legacy
    return {}


# 已经就某个端点提示过"v2 不可用、已回落 v1"的集合，用来避免刷屏：
# `qzcli avail` 这类命令会对十几个工作空间循环调同一个端点。
_V2_FALLBACK_WARNED: set = set()

# 触发回落 v1 的 HTTP 状态码。这些表示"v2 这条路由不通"，重试没意义：
#   404 网关未注册该 Action  405 方法不允许  501 未实现  502/503/504 网关侧挂了
# 刻意**不含** 401 —— 那由 `with_auth_retry` 重登处理。
_V2_FALLBACK_STATUS = frozenset({404, 405, 501, 502, 503, 504})


def _v2_then_v1(name: str, v2_call, v1_call, *, logger=None):
    """先打 v2，只在"v2 这条路由不通"时回落 v1。

    迁移期的保护：平台正在把 /api/v1 逐步下线（``/openapi/v1/specs/list``
    已经 404），所以两条腿都留着。

    **只有** ``_V2_FALLBACK_STATUS`` 里的状态码、或响应非 JSON（APISIX 把请求
    302 到了 Keycloak）才回落。**一切业务错误都直接抛**，包括
    ``AccessForbidden`` 和 ``InvalidParameter``。

    权限类**刻意不回落**，别再加回去：唯一见过的 ``AccessForbidden`` 实例是
    ``该空间已被禁用`` —— 那是 **v2 判断正确**，反倒是 v1 会给非成员返回一个
    已禁用空间的陈旧集群结构。回落 v1 等于用错误答案盖掉正确答案。禁用空间的
    正解是在 ``list_workspaces`` 源头按 ``usage_status`` 滤掉（已实现），
    而不是在这里绕过。同理 ``InvalidParameter`` 是我们自己请求写错了，
    回落只会让它一直不被发现。
    """
    try:
        return v2_call()
    except QzAPIError as exc:
        if not (exc.code in _V2_FALLBACK_STATUS or "非 JSON" in str(exc)):
            raise
        if name not in _V2_FALLBACK_WARNED:
            _V2_FALLBACK_WARNED.add(name)
            msg = f"[qzcli] v2 接口 {name} 不可用（{exc}），本次回落 v1。"
            if logger:
                logger(msg)
            else:
                print(msg, file=sys.stderr)
        return v1_call()


def build_resource_spec_price(
    spec_obj: Dict[str, Any], compute_group_id: str
) -> Dict[str, Any]:
    """Build the resource_spec_price object the new /api/v1/train_job/create expects.

    Mirrors the slurm_cluster_spec.spec_price shape used by create_hpc_job. Translates
    the cache field name `memory_gb` to the platform field name `memory_size_gib`.
    """
    return {
        "cpu_type": "",
        "cpu_count": int(spec_obj.get("cpu_count") or 0),
        "gpu_type": spec_obj.get("gpu_type") or "",
        "gpu_count": int(spec_obj.get("gpu_count") or 0),
        "memory_size_gib": int(spec_obj.get("memory_gb") or 0),
        "logic_compute_group_id": compute_group_id,
        "quota_id": spec_obj.get("id") or "",
    }


class QzAPI:
    """启智平台 API 客户端"""

    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        self.base_url = get_api_base_url()

        if username and password:
            self._username = username
            self._password = password
        else:
            self._username, self._password = get_credentials()

        self._token: Optional[str] = None
        # 关掉可临时禁用 cookie 过期自动重登（如只读 / 无凭据场景）。
        self._auto_relogin = True
        self._relogin_lock = threading.Lock()

    def _post(self, url: str, **kwargs) -> _CurlResponse:
        return _curl_post(url, **kwargs)

    def _relogin(self) -> Optional[str]:
        """用本地凭据走 CAS 重新登录并持久化新 cookie。

        返回新 cookie 字符串；没有凭据或登录失败时返回 ``None``。线程安全：并发
        调用（如 ``get_jobs_detail`` 扇出）共享一次登录，避免对 CAS 造成登录风暴。
        """
        if not (self._username and self._password):
            return None
        stale = (get_cookie() or {}).get("cookie")
        with self._relogin_lock:
            current = (get_cookie() or {}).get("cookie")
            if current and current != stale:
                return current  # 其他线程已经刷新过了
            try:
                cookie = self.login_with_cas(self._username, self._password)
            except QzAPIError:
                return None
            save_cookie(cookie, (get_cookie() or {}).get("workspace_id", ""))
            return cookie

    def _get_token(self, force_refresh: bool = False) -> str:
        """获取 Access Token（带缓存）"""
        if not force_refresh and self._token:
            return self._token

        # 尝试从缓存获取
        if not force_refresh:
            cache = get_token_cache()
            if cache:
                self._token = cache["token"]
                return self._token

        # 请求新 token
        if not self._username or not self._password:
            raise QzAPIError(
                "未配置认证信息，请运行 qzcli init 或设置环境变量 QZCLI_USERNAME/QZCLI_PASSWORD"
            )

        url = f"{self.base_url}/auth/token"
        response = _curl_post(
            url,
            json={"username": self._username, "password": self._password},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        try:
            data = response.json()
        except ValueError:
            raise QzAPIError(
                f"获取 Token 失败: 响应不是有效 JSON (HTTP {response.status_code})"
            )
        if data.get("code") != 0:
            raise QzAPIError(
                f"获取 Token 失败: {data.get('message', '未知错误')}", data.get("code")
            )

        # Token 可能在顶层或 data 字段中
        token_data = data.get("data", data)
        self._token = token_data.get("access_token")
        if not self._token:
            raise QzAPIError("响应中未包含 access_token")

        expires_in_str = token_data.get("expires_in", "604800")
        expires_in = (
            int(expires_in_str) if isinstance(expires_in_str, str) else expires_in_str
        )
        save_token_cache(self._token, expires_in)

        return self._token

    def _request(
        self, endpoint: str, data: Dict[str, Any], retry_on_auth_error: bool = True
    ) -> Dict[str, Any]:
        """发送 API 请求"""
        token = self._get_token()
        url = f"{self.base_url}{endpoint}"

        response = _curl_post(
            url,
            json=data,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )

        try:
            result = response.json()
        except ValueError:
            raise QzAPIError(
                f"API 请求失败: 响应不是有效 JSON (HTTP {response.status_code})"
            )

        # Token 过期时重试
        if result.get("code") == -1 and retry_on_auth_error:
            clear_token_cache()
            self._token = None
            return self._request(endpoint, data, retry_on_auth_error=False)

        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}", result.get("code")
            )

        return result

    @with_auth_retry
    def _request_v2(
        self,
        service: str,
        action: str,
        body: Dict[str, Any],
        cookie: Optional[str] = None,
        referer_path: str = "/jobs",
        raw: bool = False,
    ) -> Dict[str, Any]:
        """POST 到 /api/v2/{service}?Action={action}。

        与 /openapi/v1 不同：
          - 响应是 AWS 风格信封 ``{"ResponseMetadata": ..., "Result": ...}``
          - APISIX 网关要求 ``x-inspire-client-source`` 头，否则 302 到 Keycloak
          - 认证走 cookie（同 /api/v1/）：Bearer 在这条路径下不被接受

        默认返回**已解封装的** ``Result``（见 ``_unwrap_v2_result``）；调用方不需要
        再自己剥一层。需要看原始信封时传 ``raw=True``。

        ``cookie`` 显式传入时优先于磁盘上的（``create_job_v2`` 这类已经在上层拿好
        cookie 的调用点用得到）；不传则从 ``~/.qzcli/.cookie`` 读，配合
        ``with_auth_retry`` 在 401 时自动重登后读到新 cookie。
        """
        if not cookie:
            cookie_data = get_cookie()
            cookie = cookie_data.get("cookie") if cookie_data else None
        if not cookie:
            raise QzAPIError(
                "v2 API 需要 cookie 认证，但本地没有有效 cookie。"
                "请先运行 `qzcli login -u <学工号> -p <密码>` 获取 CAS 会话。"
            )

        url = f"{self.base_url}/api/v2/{service}"
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "referer": f"{self.base_url}{referer_path}",
            "user-agent": V2_BROWSER_UA,
            "x-inspire-client-source": V2_CLIENT_SOURCE,
        }
        response = _curl_post(
            url,
            params={"Action": action},
            json=body,
            headers=headers,
            timeout=60,
        )

        if response.status_code == 401:
            raise QzAPIError(
                "Cookie 已过期或无效，请运行 `qzcli login` 重新获取",
                401,
            )

        # 网关对未注册路由回的是 `404 page not found`（text/plain），要和
        # 「认证失败被 302 到 Keycloak 的 HTML」区分开 —— 前者该回落 v1，
        # 后者重新登录才有用。
        if response.status_code == 404:
            raise QzAPIError(
                f"v2 网关上没有 /api/v2/{service}?Action={action} 这条路由（404）。",
                404,
            )

        ctype = response.headers.get("Content-Type", "")
        if "application/json" not in ctype:
            snippet = response.text[:200].replace("\n", " ")
            raise QzAPIError(
                f"v2 API 返回非 JSON（{response.status_code}, content-type={ctype}）。"
                f"通常表示认证失败、APISIX 网关拒绝、或当前 cookie 无该工作空间权限。"
                f"试试 `qzcli login`。响应片段: {snippet}",
                response.status_code,
            )

        try:
            result = response.json()
        except ValueError as e:
            raise QzAPIError(f"v2 API 响应不是合法 JSON: {e}", response.status_code)

        if response.status_code >= 400:
            raise QzAPIError(
                f"v2 API 请求失败 ({response.status_code}): {result}",
                response.status_code,
            )
        return result if raw else _unwrap_v2_result(result)

    def get_job_detail(self, job_id: str) -> Dict[str, Any]:
        """查询任务详情（使用 cookie 认证，优先于 token）"""
        cookie_data = get_cookie()
        cookie = cookie_data.get("cookie") if cookie_data else None
        if cookie:
            try:
                return self.get_job_detail_with_cookie(job_id, cookie)
            except QzAPIError:
                pass
        result = self._request("/openapi/v1/train_job/detail", {"job_id": job_id})
        return result.get("data", {})

    def get_job_detail_with_cookie(self, job_id: str, cookie: str) -> Dict[str, Any]:
        """任务详情：优先 v2 ``train GetJob``，v2 路由不通时回落 v1。

        两边都把 job 字段平铺在结果顶层，调用方无需区分。
        """
        return _v2_then_v1(
            "train_job/detail",
            lambda: self._get_job_detail_v2(job_id, cookie),
            lambda: self._get_job_detail_v1(job_id, cookie),
        )

    def _get_job_detail_v2(
        self, job_id: str, cookie: Optional[str] = None
    ) -> Dict[str, Any]:
        """``POST /api/v2/train?Action=GetJob``。"""
        return self._request_v2(
            "train",
            "GetJob",
            {"job_id": job_id},
            cookie=cookie,
            referer_path=f"/jobs/distributedTrainingDetail/{job_id}",
        )

    @with_auth_retry
    def _get_job_detail_v1(self, job_id: str, cookie: str) -> Dict[str, Any]:
        """遗留路径 ``POST /api/v1/train_job/detail``。"""
        url = f"{self.base_url}/api/v1/train_job/detail"
        payload = {"job_id": job_id}
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "pragma": "no-cache",
            "referer": f"{self.base_url}/jobs/distributedTrainingDetail/{job_id}",
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }
        response = _curl_post(url, json=payload, headers=headers, timeout=60)
        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)
        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )
        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON，请检查 cookie 是否正确")
        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}",
                result.get("code"),
            )
        return result.get("data", {})

    def _resolve_pod_names(
        self, job_id: str, n_instances: Optional[int] = None
    ) -> List[str]:
        """推断 job 的所有 worker pod 名。

        平台规则：pod 命名为 ``{job_id}-worker-{i}`` for i in 0..n-1。
        n_instances 没显式给时从 detail 反推（兼容多种字段位置）。
        """
        if n_instances is None:
            try:
                d = self.get_job_detail(job_id)
                fc = d.get("framework_config")
                if isinstance(fc, list) and fc and isinstance(fc[0], dict):
                    n_instances = fc[0].get("instance_count")
                if not n_instances:
                    n_instances = (
                        d.get("instance_count")
                        or d.get("instances")
                        or d.get("replica_count")
                    )
            except Exception:
                n_instances = None
        if not n_instances or n_instances < 1:
            n_instances = 1
        return [f"{job_id}-worker-{i}" for i in range(n_instances)]

    def get_job_logs(
        self,
        job_id: str,
        page_size: int = 200,
        pod_names: Optional[List[str]] = None,
        start_timestamp_ms: Optional[str] = None,
        end_timestamp_ms: Optional[str] = None,
        sort: str = "ascend",
    ) -> Dict[str, Any]:
        """拉取 train job 的容器日志（v2 接口）。

        Returns: ``{"logs": [<entry>, ...], "total": int}``。每条 entry 含
        ``log_id, message, node, pod_name, time, timestamp_ms, timestamp_str``。
        """
        if pod_names is None:
            pod_names = self._resolve_pod_names(job_id)

        body: Dict[str, Any] = {
            "page_size": page_size,
            "filter": {"podNames": pod_names},
            "sorter": [
                {"field": "time", "sort": sort},
                {"field": "log-id.keyword", "sort": sort},
            ],
        }
        if start_timestamp_ms is not None:
            body["filter"]["start_timestamp_ms"] = str(start_timestamp_ms)
        if end_timestamp_ms is not None:
            body["filter"]["end_timestamp_ms"] = str(end_timestamp_ms)

        # `_request_v2` 已经剥掉 ResponseMetadata/Result 信封了
        return self._request_v2("train", "GetJobLog", body)

    def get_jobs_detail(
        self, job_ids: List[str], max_workers: int = 5
    ) -> Dict[str, Dict[str, Any]]:
        """批量查询任务详情（并发）"""
        results = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_job = {
                executor.submit(self.get_job_detail, job_id): job_id
                for job_id in job_ids
            }

            for future in as_completed(future_to_job):
                job_id = future_to_job[future]
                try:
                    results[job_id] = future.result()
                except Exception as e:
                    results[job_id] = {"error": str(e)}

        return results

    def stop_job(self, job_id: str) -> bool:
        """停止任务（优先 cookie 认证，回退 token）"""
        cookie_data = get_cookie()
        cookie = cookie_data.get("cookie") if cookie_data else None
        if cookie:
            try:
                return self.stop_job_with_cookie(job_id, cookie)
            except QzAPIError:
                pass
        try:
            self._request("/openapi/v1/train_job/stop", {"job_id": job_id})
            return True
        except QzAPIError:
            return False

    def stop_job_with_cookie(self, job_id: str, cookie: str) -> bool:
        """停止任务：优先 v2 ``train StopJob``，v2 路由不通时回落 v1。

        唯一迁到 v2 的**写**操作。回落判据依旧只认"路由不通"
        （404/405/50x/非 JSON）—— 业务错误（任务已结束、无权限）会直接抛给用户，
        绝不会因为回落而变成"停了两次"。
        """
        return _v2_then_v1(
            "train_job/stop",
            lambda: self._stop_job_v2(job_id, cookie),
            lambda: self._stop_job_v1(job_id, cookie),
        )

    def _stop_job_v2(self, job_id: str, cookie: Optional[str] = None) -> bool:
        """``POST /api/v2/train?Action=StopJob``。"""
        self._request_v2(
            "train",
            "StopJob",
            {"job_id": job_id},
            cookie=cookie,
            referer_path=f"/jobs/distributedTrainingDetail/{job_id}",
        )
        # 成功即无 Error 信封；_request_v2 已在失败时抛异常
        return True

    @with_auth_retry
    def _stop_job_v1(self, job_id: str, cookie: str) -> bool:
        """遗留路径 ``POST /api/v1/train_job/stop``。"""
        url = f"{self.base_url}/api/v1/train_job/stop"
        payload = {"job_id": job_id}
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "pragma": "no-cache",
            "referer": f"{self.base_url}/jobs/distributedTrainingDetail/{job_id}",
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }
        response = _curl_post(url, json=payload, headers=headers, timeout=60)
        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)
        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )
        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON")
        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}",
                result.get("code"),
            )
        return True

    def create_job(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """创建任务"""
        result = self._request("/openapi/v1/train_job/create", config)
        return result.get("data", result)

    @with_auth_retry
    def create_job_with_cookie(
        self, cookie: str, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """使用 cookie 创建任务（内部 API）"""
        url = f"{self.base_url}/api/v1/train_job/create"
        workspace_id = config.get("workspace_id", "")
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "referer": f"{self.base_url}/jobs/distributedTraining?spaceId={workspace_id}",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }
        response = _curl_post(url, json=config, headers=headers, timeout=60)
        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)
        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )
        result = response.json()
        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}", result.get("code")
            )
        return result.get("data", result)

    def create_job_v2(self, cookie: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """创建任务（当前 Web UI v2 Console API）。

        平台 Web UI 已把作业生命周期迁到 `/api/v2/train?Action=CreateJobConsole`
        （AWS 风格 RPC）。payload 结构与 v1 `create_job_with_cookie` 相同（同顶层
        key + framework_config[0] + 嵌套 resource_spec_price），差别只在 endpoint、
        响应封装（ResponseMetadata/Result）和新增的 `exclude_nodes` 等 v2 选项。
        """
        workspace_id = config.get("workspace_id", "")
        return self._request_v2(
            "train",
            "CreateJobConsole",
            config,
            cookie=cookie,
            referer_path=f"/jobs/distributedTraining?spaceId={workspace_id}",
        )

    @with_auth_retry
    def create_hpc_job(
        self,
        cookie: str,
        job_name: str,
        workspace_id: str,
        project_id: str,
        logic_compute_group_id: str,
        entrypoint: str,
        image: str,
        predef_quota_id: str,
        cpu: int,
        mem_gi: int,
        instances: int = 1,
        cpus_per_task: int = 1,
        memory_per_cpu: str = "5G",
        image_type: str = "SOURCE_PRIVATE",
        max_running_time_days: int = 0,
        max_running_time_hours: int = 0,
        max_running_time_minutes: int = 0,
        priority: int = 1,
    ) -> Dict[str, Any]:
        """
        提交 HPC/CPU 任务（使用 cookie 认证，POST /api/v1/hpc_jobs）

        ``priority`` 是**必填**：平台后来加了这个校验，不传直接被拒
        （``API 请求失败: priority must be set``），导致 `qzcli hpc` 整个不可用。

        ⚠️ **HPC 的优先级方向和训练任务相反，别照抄。** 实测提交值→平台档位：

        ==========  ==========  ======
        提交值       存储值       档位
        ==========  ==========  ======
        1           11          LOW
        3           13          LOW
        5           30          HIGH
        10          35          HIGH
        ==========  ==========  ======

        即**数字越大优先级越高**；有效范围 1–10（0/11/12 会被拒
        ``无效的优先级值``）。而训练任务的 ``task_priority`` 是反的 ——
        那边 10 表示低优。所以这里默认取 **1（LOW）**，与集群上现有生产
        HPC 任务一致（它们存储值都是 11/LOW），不抢资源。

        Returns:
            API 响应 data 字段（含 job_id 等）
        """
        url = f"{self.base_url}/api/v1/hpc_jobs"
        payload = {
            "job_name": job_name,
            "workspace_id": workspace_id,
            "project_id": project_id,
            "logic_compute_group_id": logic_compute_group_id,
            "priority": priority,
            "enable_notification": False,
            "dataset_info": [],
            "sbatch_script": {
                "number_of_tasks": instances,
                "cpus_per_task": cpus_per_task,
                "memory_per_cpu": memory_per_cpu,
                "enable_hyper_threading": False,
                "max_running_time_days": max_running_time_days,
                "max_running_time_hours": max_running_time_hours,
                "max_running_time_minutes": max_running_time_minutes,
                "entrypoint": entrypoint,
            },
            "slurm_cluster_spec": {
                "predef_quota_id": predef_quota_id,
                "cpu": cpu,
                "mem_gi": mem_gi,
                "image": image,
                "image_type": image_type,
                "instance_count": instances,
                "spec_price": {
                    "cpu_type": "",
                    "cpu_count": cpu,
                    "gpu_type": "",
                    "gpu_count": 0,
                    "memory_size_gib": mem_gi,
                    "logic_compute_group_id": logic_compute_group_id,
                    "quota_id": predef_quota_id,
                },
            },
        }
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "referer": f"{self.base_url}/jobs/hpc?spaceId={workspace_id}",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        }
        response = _curl_post(url, json=payload, headers=headers, timeout=60)
        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)
        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )
        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON")
        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}",
                result.get("code"),
            )
        return result.get("data", {})

    def list_hpc_jobs(
        self,
        workspace_id: str,
        cookie: Optional[str] = None,
        status: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        """列出 HPC 任务（v2 ``hpc ListJobs``）。

        原来的实现打 ``/api/v1/hpc_jobs/list``，且仓库里零调用者。这里没有直接删掉，
        而是换成 v2 实现 —— 真机验证过返回 ``Result.jobs[]`` + ``Result.total``，
        和 v1 的 ``data.jobs`` 形状一致，所以调用方感知不到差别。

        ``cookie`` 参数保留只为兼容旧签名，实际由 ``_request_v2`` 从磁盘读。
        """
        body: Dict[str, Any] = {
            "workspace_id": workspace_id,
            "page_num": page_num,
            "page_size": page_size,
        }
        if status:
            body["status"] = status
        return self._request_v2(
            "hpc",
            "ListJobs",
            body,
            cookie=cookie,
            referer_path=f"/jobs/hpc?spaceId={workspace_id}",
        )

    def test_connection(self) -> bool:
        """测试连接"""
        try:
            self._get_token(force_refresh=True)
            return True
        except Exception:
            return False

    def list_workspace_tasks(
        self,
        workspace_id: str,
        cookie: str,
        hours: int = 24,
    ) -> Dict[str, Any]:
        """工作空间任务概览：优先 v2 ``workspace GetOverviewTaskMetric``，不通时回落 v1。

        返回含 ``task_groups``（按任务类型分组的状态统计）。

        ⚠️ ``time_range`` 的时间戳是**秒**不是毫秒，且区间**硬限制 ≤ 1 个月**。
        传毫秒会被当成秒解释，跨度爆表，平台报
        ``InternalError: 查询时间区间不能超过1个月``。
        """
        end_ts = int(_time.time())
        start_ts = end_ts - hours * 3600
        return _v2_then_v1(
            "cluster_metric/overview_task_metric",
            lambda: self._list_workspace_tasks_v2(
                workspace_id, cookie, start_ts, end_ts
            ),
            lambda: self._list_workspace_tasks_v1(
                workspace_id, cookie, start_ts, end_ts
            ),
        )

    def _list_workspace_tasks_v2(
        self,
        workspace_id: str,
        cookie: Optional[str],
        start_ts: int,
        end_ts: int,
    ) -> Dict[str, Any]:
        """``POST /api/v2/workspace?Action=GetOverviewTaskMetric``。"""
        return self._request_v2(
            "workspace",
            "GetOverviewTaskMetric",
            {
                "filter": {"workspace_id": workspace_id},
                "time_range": {
                    "start_timestamp": str(start_ts),
                    "end_timestamp": str(end_ts),
                },
            },
            cookie=cookie,
            referer_path=f"/jobs/spacesOverview?spaceId={workspace_id}",
        )

    @with_auth_retry
    def _list_workspace_tasks_v1(
        self,
        workspace_id: str,
        cookie: str,
        start_ts: int,
        end_ts: int,
    ) -> Dict[str, Any]:
        """遗留路径 ``POST /api/v1/cluster_metric/overview_task_metric``。"""
        url = f"{self.base_url}/api/v1/cluster_metric/overview_task_metric"

        payload = {
            "filter": {"workspace_id": workspace_id},
            "time_range": {
                "start_timestamp": str(start_ts),
                "end_timestamp": str(end_ts),
            },
        }

        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "referer": f"{self.base_url}/jobs/spacesOverview?spaceId={workspace_id}",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }

        response = _curl_post(
            url,
            json=payload,
            headers=headers,
            timeout=60,
        )

        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)

        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )

        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON，请检查 cookie 是否正确")

        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}", result.get("code")
            )

        return result.get("data", {})

    # 注意：分发器本身**不**挂 @with_auth_retry —— v2 分支的重试在 `_request_v2`
    # 上，v1 分支的重试在 `_list_jobs_v1` 上，再包一层会变成重试套重试。
    def list_jobs_with_cookie(
        self,
        workspace_id: str,
        cookie: str,
        page_num: int = 1,
        page_size: int = 100,
        created_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """获取任务列表：优先 v2 ``train ListJobs``，v2 路由不通时回落 v1。

        两边响应形状一致（``{jobs: [...], total: N}``），调用方无需区分。
        """
        return _v2_then_v1(
            "train_job/list",
            lambda: self._list_jobs_v2(
                workspace_id, cookie, page_num, page_size, created_by
            ),
            lambda: self._list_jobs_v1(
                workspace_id, cookie, page_num, page_size, created_by
            ),
        )

    def _list_jobs_v2(
        self,
        workspace_id: str,
        cookie: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 100,
        created_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """``POST /api/v2/train?Action=ListJobs`` → ``Result.{jobs, total}``。"""
        body: Dict[str, Any] = {
            "workspace_id": workspace_id,
            "page_num": page_num,
            "page_size": page_size,
        }
        if created_by:
            body["created_by"] = created_by
        return self._request_v2(
            "train",
            "ListJobs",
            body,
            cookie=cookie,
            referer_path=f"/jobs/distributedTraining?spaceId={workspace_id}",
        )

    @with_auth_retry
    def _list_jobs_v1(
        self,
        workspace_id: str,
        cookie: str,
        page_num: int = 1,
        page_size: int = 100,
        created_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """遗留路径 ``POST /api/v1/train_job/list`` → ``data.{jobs, total}``。"""
        url = f"{self.base_url}/api/v1/train_job/list"

        payload = {
            "page_num": page_num,
            "page_size": page_size,
            "workspace_id": workspace_id,
        }

        if created_by:
            payload["created_by"] = created_by

        # 需要完整的浏览器 headers 才能通过认证
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "pragma": "no-cache",
            "referer": f"{self.base_url}/jobs/distributedTraining?spaceId={workspace_id}",
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }

        response = _curl_post(
            url,
            json=payload,
            headers=headers,
            timeout=60,
        )

        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)

        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )

        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON，请检查 cookie 是否正确")

        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}", result.get("code")
            )

        return result.get("data", {})

    @staticmethod
    def _notebook_list_body(
        workspace_id: str,
        page: int,
        page_size: int,
        user_ids: Optional[List[str]],
        status: Optional[List[str]],
    ) -> Dict[str, Any]:
        """v1 和 v2 的开发机列表请求体**完全一致** —— 分页都是 ``page``（不是
        ``page_num``）、过滤都在 ``filter_by{}``、排序键都是 ``order_by[].order``。
        所以这里共用一份，避免两边漂移。"""
        return {
            "workspace_id": workspace_id,
            "page": page,
            "page_size": page_size,
            "filter_by": {
                "keyword": "",
                "user_id": user_ids or [],
                "logic_compute_group_id": [],
                "status": status or [],
                "mirror_url": [],
            },
            "order_by": [{"field": "created_at", "order": "desc"}],
        }

    def list_notebooks_with_cookie(
        self,
        workspace_id: str,
        cookie: str,
        page: int = 1,
        page_size: int = 50,
        user_ids: Optional[List[str]] = None,
        status: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """获取开发机列表：优先 v2 ``notebook ListNotebooks``，不通时回落 v1。

        两边都返回 ``{list: [...], total: N}``。
        """
        return _v2_then_v1(
            "notebook/list",
            lambda: self._list_notebooks_v2(
                workspace_id, cookie, page, page_size, user_ids, status
            ),
            lambda: self._list_notebooks_v1(
                workspace_id, cookie, page, page_size, user_ids, status
            ),
        )

    def _list_notebooks_v2(
        self,
        workspace_id: str,
        cookie: Optional[str] = None,
        page: int = 1,
        page_size: int = 50,
        user_ids: Optional[List[str]] = None,
        status: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """``POST /api/v2/notebook?Action=ListNotebooks`` → ``Result.{list, total}``。"""
        return self._request_v2(
            "notebook",
            "ListNotebooks",
            self._notebook_list_body(workspace_id, page, page_size, user_ids, status),
            cookie=cookie,
            referer_path=f"/jobs/interactiveModeling?spaceId={workspace_id}",
        )

    @with_auth_retry
    def _list_notebooks_v1(
        self,
        workspace_id: str,
        cookie: str,
        page: int = 1,
        page_size: int = 50,
        user_ids: Optional[List[str]] = None,
        status: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """遗留路径 ``POST /api/v1/notebook/list`` → ``data.{list, total}``。"""
        url = f"{self.base_url}/api/v1/notebook/list"

        payload = self._notebook_list_body(
            workspace_id, page, page_size, user_ids, status
        )

        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "referer": f"{self.base_url}/jobs/interactiveModeling?spaceId={workspace_id}",
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }

        response = _curl_post(url, json=payload, headers=headers, timeout=60)

        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)

        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )

        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON，请检查 cookie 是否正确")

        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}", result.get("code")
            )

        return result.get("data", {})

    def extract_resources_from_jobs(self, jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        从任务列表中提取资源配置信息

        Args:
            jobs: 任务列表

        Returns:
            包含 workspaces, projects, compute_groups, specs 的字典
        """
        workspaces = {}
        projects = {}
        compute_groups = {}
        specs = {}

        for job in jobs:
            # 提取 workspace
            ws_id = job.get("workspace_id", "")
            if ws_id and ws_id not in workspaces:
                workspaces[ws_id] = {"id": ws_id}

            # 提取 project
            proj_id = job.get("project_id", "")
            proj_name = job.get("project_name", "")
            if proj_id and proj_id not in projects:
                projects[proj_id] = {
                    "id": proj_id,
                    "name": proj_name,
                    "en_name": job.get("project_en_name", ""),
                    "workspace_id": ws_id,
                }

            # 提取 compute group
            lcg_id = job.get("logic_compute_group_id", "")
            lcg_name = job.get("logic_compute_group_name", "")
            if lcg_id and lcg_id not in compute_groups:
                # 从 framework_config 中提取 GPU 信息
                gpu_info = {}
                fc = job.get("framework_config", [])
                if fc:
                    spec_info = fc[0].get("instance_spec_price_info", {})
                    gpu_info = spec_info.get("gpu_info", {})

                compute_groups[lcg_id] = {
                    "id": lcg_id,
                    "name": lcg_name,
                    "workspace_id": ws_id,
                    "gpu_type": gpu_info.get("gpu_product_simple", ""),
                    "gpu_type_display": gpu_info.get("gpu_type_display", ""),
                }

            # 提取 spec (quota_id)
            fc = job.get("framework_config", [])
            if fc:
                spec_info = fc[0].get("instance_spec_price_info", {})
                spec_id = spec_info.get("quota_id", "")
                if spec_id:
                    existing_group_ids = list(
                        (specs.get(spec_id) or {}).get("logic_compute_group_ids", [])
                    )
                    if lcg_id and lcg_id not in existing_group_ids:
                        existing_group_ids.append(lcg_id)
                    if spec_id not in specs:
                        specs[spec_id] = {
                            "id": spec_id,
                            "logic_compute_group_id": lcg_id,
                            "logic_compute_group_ids": existing_group_ids,
                            "gpu_count": spec_info.get("gpu_count", 0),
                            "cpu_count": spec_info.get("cpu_count", 0),
                            "memory_gb": spec_info.get("memory_size_gib", 0),
                            "gpu_type": spec_info.get("gpu_info", {}).get(
                                "gpu_product_simple", ""
                            ),
                            "gpu_type_display": spec_info.get("gpu_info", {}).get(
                                "gpu_type_display", ""
                            ),
                        }
                    elif existing_group_ids:
                        specs[spec_id]["logic_compute_group_ids"] = existing_group_ids

        return {
            "workspaces": list(workspaces.values()),
            "projects": list(projects.values()),
            "compute_groups": list(compute_groups.values()),
            "specs": list(specs.values()),
        }

    def list_specs(
        self, compute_group_id: str, workspace_id: str = ""
    ) -> List[Dict[str, Any]]:
        """获取计算组可用的规格列表。

        这是整个迁移里最麻烦的一个：**v2 没有任何 action 返回 spec_id**。
        把 144 个 action 的 schema 全 grep 过 —— ``spec_id`` 只作为**请求**字段
        存在（``train.CreateJob.framework_config[]`` / ``hpc.CreateJob`` /
        ``inference-serving.CreateServing``）。``workspace GetWorkspaceNodeSpecs``
        和 ``GetLogicComputeGroupNodeSpecs`` 返回的 ``node_specs[]`` 只有硬件参数
        （cpu/gpu/内存/型号），**没有 id**，拿来当 spec 用会被
        ``_normalize_spec_item`` 直接丢掉。

        而老的 ``/openapi/v1/specs/list`` **平台上已经 404 了**。

        所以这里走两级：

        1. 老 OpenAPI（唯一的"规格清单"语义来源，还活着就用）
        2. v2 ``train ListJobs`` 的历史任务：
           ``framework_config[].instance_spec_price_info`` 里带 ``quota_id``，
           而 quota_id 就是 spec_id，同时还带全套 cpu/gpu/内存/gpu_type。
           这是目前**唯一能从 v2 拿到真实 spec id 的路径**。

        再往上还有第三级（``~/.qzcli/resources.json`` 本地缓存），由调用方兜。

        Args:
            compute_group_id: 逻辑计算组 ID
            workspace_id: 工作空间 ID。留空则跳过第 2 级 —— 历史任务必须按
                工作空间查。
        """
        try:
            result = self._request(
                "/openapi/v1/specs/list", {"logic_compute_group_id": compute_group_id}
            )
            specs = result.get("data", {}).get("specs", [])
            if specs:
                return specs
        except QzAPIError:
            # 404 / invalid_grant 都算这一级不可用，静默降级到历史任务推断
            pass

        if not workspace_id:
            return []
        return self._specs_from_job_history(compute_group_id, workspace_id)

    def _specs_from_job_history(
        self, compute_group_id: str, workspace_id: str, page_size: int = 200
    ) -> List[Dict[str, Any]]:
        """从历史任务里反推规格（v2 ``train ListJobs``）。

        平台不提供"某计算组有哪些规格"的 v2 查询，但**跑过的任务里带着它用的
        规格**，所以按 quota_id 去重就能还原出一份可用规格表。
        字段名对齐 ``_normalize_spec_item`` 认识的那几个别名。
        """
        try:
            data = self.list_jobs_with_cookie(
                workspace_id, "", page_num=1, page_size=page_size
            )
        except QzAPIError:
            return []

        specs: Dict[str, Dict[str, Any]] = {}
        for job in data.get("jobs") or []:
            lcg_id = job.get("logic_compute_group_id", "")
            if compute_group_id and lcg_id != compute_group_id:
                continue
            for fc in job.get("framework_config") or []:
                info = fc.get("instance_spec_price_info") or {}
                quota_id = info.get("quota_id")
                if not quota_id or quota_id in specs:
                    continue
                gpu_info = info.get("gpu_info") or {}
                specs[quota_id] = {
                    "id": quota_id,
                    "quota_id": quota_id,
                    "gpu_count": info.get("gpu_count") or fc.get("gpu_count") or 0,
                    "cpu_count": info.get("cpu_count") or fc.get("cpu") or 0,
                    "memory_size_gib": info.get("memory_size_gib")
                    or fc.get("mem_gi")
                    or 0,
                    # gpu_type 平台校验时要**完整串**（NVIDIA_H200_SXM_141G），
                    # 不是简称，所以直接用响应里的原值
                    "gpu_type": gpu_info.get("gpu_type") or "",
                    "gpu_info": gpu_info,
                    "logic_compute_group_ids": [lcg_id] if lcg_id else [],
                }
        return list(specs.values())

    def list_node_specs(
        self, workspace_id: str, logic_compute_group_id: str = ""
    ) -> List[Dict[str, Any]]:
        """计算组/工作空间的**硬件规格**（v2）。

        注意这**不是** ``list_specs`` 的替代品 —— 返回的 ``node_specs[]`` 里
        没有 spec_id，不能拿去提任务，只能用于展示"这个组有什么样的机器"。
        ``logic_compute_group_id`` 留空则查整个工作空间。
        """
        if logic_compute_group_id:
            body = {
                "workspace_id": workspace_id,
                "logic_compute_group_id": logic_compute_group_id,
            }
            action = "GetLogicComputeGroupNodeSpecs"
        else:
            body = {"workspace_id": workspace_id}
            action = "GetWorkspaceNodeSpecs"
        result = self._request_v2(
            "workspace",
            action,
            body,
            referer_path=f"/jobs/spacesOverview?spaceId={workspace_id}",
        )
        return result.get("node_specs") or []

    @staticmethod
    def _dimension_body(
        workspace_id: str,
        logic_compute_group_id: Optional[str],
        compute_group_id: Optional[str],
        page_num: int,
        page_size: int,
    ) -> Dict[str, Any]:
        """v1 `cluster_metric/list_*_dimension` 和 v2 `workspace List*Dimension`
        共用同一份请求体（``filter{}`` + ``page_num``/``page_size``）。"""
        filter_params = {"workspace_id": workspace_id}
        if logic_compute_group_id:
            filter_params["logic_compute_group_id"] = logic_compute_group_id
        if compute_group_id:
            filter_params["compute_group_id"] = compute_group_id
        return {
            "page_num": page_num,
            "page_size": page_size,
            "filter": filter_params,
        }

    def list_node_dimension(
        self,
        workspace_id: str,
        cookie: str,
        logic_compute_group_id: Optional[str] = None,
        compute_group_id: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        """节点维度资源使用：优先 v2 ``workspace ListNodeDimension``，不通时回落 v1。

        ⚠️ **必须是 `workspace` 而不是 `cluster`**。`qz spec` 里
        ``cluster.ListNodeDimension`` 和 ``workspace.ListNodeDimension`` 描述几乎
        一样，但前者是集群管理员权限 —— 普通账号实测返回 ``AccessForbidden``。
        qzcli 是工作空间级工具，一律走 ``workspace.*``。
        """
        return _v2_then_v1(
            "cluster_metric/list_node_dimension",
            lambda: self._list_node_dimension_v2(
                workspace_id,
                cookie,
                logic_compute_group_id,
                compute_group_id,
                page_num,
                page_size,
            ),
            lambda: self._list_node_dimension_v1(
                workspace_id,
                cookie,
                logic_compute_group_id,
                compute_group_id,
                page_num,
                page_size,
            ),
        )

    def _list_node_dimension_v2(
        self,
        workspace_id: str,
        cookie: Optional[str] = None,
        logic_compute_group_id: Optional[str] = None,
        compute_group_id: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        """``POST /api/v2/workspace?Action=ListNodeDimension`` → ``Result.{node_dimensions, total}``。"""
        return self._request_v2(
            "workspace",
            "ListNodeDimension",
            self._dimension_body(
                workspace_id,
                logic_compute_group_id,
                compute_group_id,
                page_num,
                page_size,
            ),
            cookie=cookie,
            referer_path=f"/jobs/spacesOverview?spaceId={workspace_id}",
        )

    @with_auth_retry
    def _list_node_dimension_v1(
        self,
        workspace_id: str,
        cookie: str,
        logic_compute_group_id: Optional[str] = None,
        compute_group_id: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        """遗留路径 ``POST /api/v1/cluster_metric/list_node_dimension``。"""
        url = f"{self.base_url}/api/v1/cluster_metric/list_node_dimension"

        payload = self._dimension_body(
            workspace_id,
            logic_compute_group_id,
            compute_group_id,
            page_num,
            page_size,
        )

        # 需要完整的浏览器 headers 才能通过认证
        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "pragma": "no-cache",
            "referer": f"{self.base_url}/jobs/spacesOverview?spaceId={workspace_id}",
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }

        response = self._post(
            url,
            json=payload,
            headers=headers,
            timeout=60,
        )

        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)

        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )

        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON，请检查 cookie 是否正确")

        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}", result.get("code")
            )

        return result.get("data", {})

    def list_task_dimension(
        self,
        workspace_id: str,
        cookie: str,
        project_id: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 200,
    ) -> Dict[str, Any]:
        """任务维度资源使用：优先 v2 ``workspace ListTaskDimension``，不通时回落 v1。

        同 ``list_node_dimension``：**必须走 `workspace` 不是 `cluster`**，
        后者对普通账号是 ``AccessForbidden``。
        """
        return _v2_then_v1(
            "cluster_metric/list_task_dimension",
            lambda: self._list_task_dimension_v2(
                workspace_id, cookie, project_id, page_num, page_size
            ),
            lambda: self._list_task_dimension_v1(
                workspace_id, cookie, project_id, page_num, page_size
            ),
        )

    def _list_task_dimension_v2(
        self,
        workspace_id: str,
        cookie: Optional[str] = None,
        project_id: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 200,
    ) -> Dict[str, Any]:
        """``POST /api/v2/workspace?Action=ListTaskDimension`` → ``Result.{task_dimensions, total}``。"""
        filter_params = {"workspace_id": workspace_id}
        if project_id:
            filter_params["project_id"] = project_id
        return self._request_v2(
            "workspace",
            "ListTaskDimension",
            {"page_num": page_num, "page_size": page_size, "filter": filter_params},
            cookie=cookie,
            referer_path=f"/jobs/spacesOverview?spaceId={workspace_id}",
        )

    @with_auth_retry
    def _list_task_dimension_v1(
        self,
        workspace_id: str,
        cookie: str,
        project_id: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 200,
    ) -> Dict[str, Any]:
        """遗留路径 ``POST /api/v1/cluster_metric/list_task_dimension``。"""
        url = f"{self.base_url}/api/v1/cluster_metric/list_task_dimension"

        filter_params = {"workspace_id": workspace_id}
        if project_id:
            filter_params["project_id"] = project_id

        payload = {
            "page_num": page_num,
            "page_size": page_size,
            "filter": filter_params,
        }

        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "pragma": "no-cache",
            "referer": f"{self.base_url}/jobs/spacesOverview?spaceId={workspace_id}",
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }

        response = self._post(
            url,
            json=payload,
            headers=headers,
            timeout=60,
        )

        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)

        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )

        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON，请检查 cookie 是否正确")

        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}", result.get("code")
            )

        return result.get("data", {})

    def get_cluster_basic_info(self, workspace_id: str, cookie: str) -> Dict[str, Any]:
        """工作空间的集群/计算组信息：优先 v2 ``workspace GetBasicInfo``，不通时回落 v1。

        ⚠️ v2 侧对应的是 ``workspace GetBasicInfo``，**不是** 同名的
        ``cluster GetClusterBasicInfo`` —— 后者普通账号 ``AccessForbidden``。
        两边都返回 ``{clusters, compute_groups, resource_types}``。
        """
        return _v2_then_v1(
            "cluster_metric/cluster_basic_info",
            lambda: self._cluster_basic_info_v2(workspace_id, cookie),
            lambda: self._cluster_basic_info_v1(workspace_id, cookie),
        )

    def _cluster_basic_info_v2(
        self, workspace_id: str, cookie: Optional[str] = None
    ) -> Dict[str, Any]:
        """``POST /api/v2/workspace?Action=GetBasicInfo``。"""
        return self._request_v2(
            "workspace",
            "GetBasicInfo",
            {"workspace_id": workspace_id},
            cookie=cookie,
            referer_path=f"/jobs/spacesOverview?spaceId={workspace_id}",
        )

    @with_auth_retry
    def _cluster_basic_info_v1(self, workspace_id: str, cookie: str) -> Dict[str, Any]:
        """遗留路径 ``POST /api/v1/cluster_metric/cluster_basic_info``。"""
        url = f"{self.base_url}/api/v1/cluster_metric/cluster_basic_info"

        payload = {"workspace_id": workspace_id}

        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "pragma": "no-cache",
            "referer": f"{self.base_url}/jobs/spacesOverview?spaceId={workspace_id}",
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }

        response = _curl_post(
            url,
            json=payload,
            headers=headers,
            timeout=60,
        )

        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)

        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )

        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON，请检查 cookie 是否正确")

        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}", result.get("code")
            )

        return result.get("data", {})

    @with_auth_retry
    def list_workspaces(self, cookie: str) -> List[Dict[str, Any]]:
        """
        获取用户可访问的工作空间列表

        通过 /api/v1/project/list 获取项目列表，从中提取工作空间信息。
        每个项目的 space_list 字段包含该项目关联的工作空间。

        Args:
            cookie: 浏览器 cookie 字符串

        Returns:
            工作空间列表 [{"id": "ws-xxx", "name": "工作空间名称"}, ...]
        """
        url = f"{self.base_url}/api/v1/project/list"

        payload = {"page": 1, "page_size": 100, "filter": {}}

        headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "pragma": "no-cache",
            "referer": f"{self.base_url}/operations/projects",
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }

        response = _curl_post(
            url,
            json=payload,
            headers=headers,
            timeout=60,
        )

        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)

        if response.status_code != 200:
            raise QzAPIError(
                f"请求失败: HTTP {response.status_code}", response.status_code
            )

        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON，请检查 cookie 是否正确")

        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}", result.get("code")
            )

        data = result.get("data", {})
        items = data.get("items", [])

        # 从项目的 space_list 中提取工作空间（去重）
        #
        # 注意 project/list 会把**你不是成员的项目**也返回回来，其 space_list 里
        # 可能挂着已被平台禁用的工作空间。这类空间在 v2 上一律
        # `AccessForbidden: 该空间已被禁用`（v2 是对的），而 v1 反而会给非成员
        # 返回陈旧的集群结构。
        #
        # `usage_status != 0` 就是禁用标记 —— 实测账号可见的 17 个空间里只有
        # 一个是 1，正好是 v2 拒绝的那个。在源头滤掉，比在调用处到处 try/except
        # 或者靠回落 v1 去"绕过"要干净得多。
        workspaces = {}
        skipped_disabled = []
        for proj in items:
            space_list = proj.get("space_list", [])
            for space in space_list:
                ws_id = space.get("id", "")
                ws_name = space.get("name", "")
                if not ws_id or ws_id in workspaces:
                    continue
                if space.get("usage_status", 0):
                    skipped_disabled.append(ws_name or ws_id)
                    continue
                workspaces[ws_id] = {
                    "id": ws_id,
                    "name": ws_name,
                }

        if skipped_disabled:
            print(
                f"[qzcli] 已跳过 {len(skipped_disabled)} 个被禁用的工作空间: "
                f"{', '.join(skipped_disabled)}",
                file=sys.stderr,
            )

        return list(workspaces.values())

    @staticmethod
    def _has_session_cookie(cookies: Dict[str, str]) -> bool:
        """Check if any session-like cookie exists (handles name changes like session -> inspire-session)."""
        return any("session" in name.lower() for name in cookies)

    def login_with_cas(self, username: str, password: str) -> str:
        """通过 CAS 统一认证登录，获取 session cookie。

        瞬时故障（SSL EOF、连接重置、CAS/代理 5xx）会以指数退避重试；用户名密码
        错误等永久性错误立即抛出，不重试。
        """
        last_exc: Optional[QzAPIError] = None
        for attempt in range(_LOGIN_MAX_TRIES):
            try:
                return self._login_with_cas_once(username, password)
            except QzTransientError as exc:
                last_exc = exc
                if attempt < _LOGIN_MAX_TRIES - 1:
                    _time.sleep(_backoff_delay(attempt))
        raise last_exc  # 重试用尽，抛出最后一次瞬时错误

    def _login_with_cas_once(self, username: str, password: str) -> str:
        """单次 CAS 登录流程（不含重试）。

        登录流程：
        1. 访问 qz.sii.edu.cn -> 重定向到 Keycloak
        2. Keycloak 重定向到 CAS 登录页
        3. 在 CAS 提交用户名密码
        4. CAS 验证后重定向回 Keycloak
        5. Keycloak 重定向回 qz.sii.edu.cn，设置 session cookie

        Args:
            username: CAS 用户名（学工号）
            password: CAS 密码

        Returns:
            session cookie 字符串
        """
        import re
        from urllib.parse import urlparse

        session = requests.Session()

        # 配置 SOCKS5 代理（WSL 等环境需要）
        # trust_env=False 避免环境变量 HTTP_PROXY（http://）覆盖 SOCKS5 代理
        proxy = get_proxy()
        if proxy:
            session.trust_env = False
            proxy_url = proxy.replace("socks5h://", "socks5://")
            session.proxies = {"http": proxy_url, "https": proxy_url}

        # 设置浏览器 User-Agent
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        }
        session.headers.update(headers)

        # Step 1: 访问启智平台，触发 OAuth 流程
        try:
            resp = session.get(self.base_url, timeout=30, allow_redirects=True)
        except requests.RequestException as e:
            raise QzTransientError(f"无法连接到启智平台: {e}")
        if resp.status_code >= 500:
            raise QzTransientError(
                f"启智平台/代理暂时不可用 (HTTP {resp.status_code})", resp.status_code
            )

        current_url = resp.url
        current_host = urlparse(current_url).netloc

        # 如果已经在启智平台且有 session cookie，说明已登录
        if current_host == "qz.sii.edu.cn":
            qz_cookies = {}
            for cookie in session.cookies:
                if "qz.sii.edu.cn" in cookie.domain:
                    qz_cookies[cookie.name] = cookie.value
            if self._has_session_cookie(qz_cookies):
                cookie_str = "; ".join([f"{k}={v}" for k, v in qz_cookies.items()])
                return cookie_str

        # Step 2: 如果在 Keycloak，需要继续到 CAS
        if "keycloak" in current_url:
            # Keycloak 页面使用 JavaScript 渲染，CAS URL 在 kcContext 对象中
            # 查找 providers 中的 CAS loginUrl
            cas_url_match = re.search(
                r'"loginUrl":\s*"([^"]*broker/cas/login[^"]*)"', resp.text
            )
            if cas_url_match:
                cas_broker_url = cas_url_match.group(1)
                # 处理转义的斜杠
                cas_broker_url = cas_broker_url.replace("\\/", "/")
                if not cas_broker_url.startswith("http"):
                    # 相对 URL，需要拼接
                    parsed = urlparse(current_url)
                    cas_broker_url = (
                        f"{parsed.scheme}://{parsed.netloc}{cas_broker_url}"
                    )

                try:
                    resp = session.get(cas_broker_url, timeout=30, allow_redirects=True)
                    current_url = resp.url
                except requests.RequestException as e:
                    raise QzTransientError(f"跳转 CAS 失败: {e}")
            else:
                raise QzAPIError("Keycloak 页面中未找到 CAS 登录链接")

        # Step 3: 检查是否在 CAS 登录页
        if "cas.sii.edu.cn" not in current_url:
            raise QzAPIError(f"未能到达 CAS 登录页面，当前 URL: {current_url}")

        cas_login_url = current_url
        login_page_html = resp.text

        encrypted_password = encrypt_password(password)

        lt_match = re.search(r'name="lt"\s+value="([^"]+)"', login_page_html)
        execution_match = re.search(
            r'name="execution"\s+value="([^"]+)"', login_page_html
        )

        login_data = {
            "username": username,
            "password": encrypted_password,
            "_eventId": "submit",
            "submit": "登 录",
            "loginType": "1",
            "encrypted": "true",
        }

        if lt_match:
            login_data["lt"] = lt_match.group(1)
        if execution_match:
            login_data["execution"] = execution_match.group(1)

        # Step 5: 提交登录表单
        login_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://cas.sii.edu.cn",
            "Referer": cas_login_url,
        }

        try:
            resp = session.post(
                cas_login_url,
                data=login_data,
                headers=login_headers,
                timeout=30,
                allow_redirects=True,
            )
        except requests.RequestException as e:
            raise QzTransientError(f"登录请求失败: {e}")

        current_url = resp.url

        # Step 6: 检查登录结果
        if "cas.sii.edu.cn" in current_url and "login" in current_url:
            # 仍然在登录页，可能是密码错误
            if "用户名或密码错误" in resp.text or "账号或密码错误" in resp.text:
                raise QzAPIError("用户名或密码错误")
            if "验证码" in resp.text:
                raise QzAPIError("需要输入验证码，请在浏览器中登录后手动获取 cookie")
            raise QzAPIError("登录失败，请检查用户名和密码")

        # Step 7: 确保完成所有重定向回到启智平台
        current_host = urlparse(current_url).netloc
        if current_host != "qz.sii.edu.cn":
            # 可能还需要额外访问启智平台来完成 session 设置
            try:
                resp = session.get(self.base_url, timeout=30, allow_redirects=True)
            except requests.RequestException as e:
                raise QzTransientError(f"获取 session 失败: {e}")

        # 收集所有 qz.sii.edu.cn 域的 cookies
        all_cookies = {}
        for cookie in session.cookies:
            # 检查是否是 qz.sii.edu.cn 的 cookie
            if "qz.sii.edu.cn" in cookie.domain:
                all_cookies[cookie.name] = cookie.value

        if not all_cookies or not self._has_session_cookie(all_cookies):
            try:
                resp = session.get(self.base_url, timeout=30, allow_redirects=True)
                for cookie in session.cookies:
                    if "qz.sii.edu.cn" in cookie.domain:
                        all_cookies[cookie.name] = cookie.value
            except Exception:
                pass

        if not all_cookies or not self._has_session_cookie(all_cookies):
            raise QzAPIError("登录成功但未获取到 session cookie")

        # 构建 cookie 字符串（确保 session 和 session_2 都包含）
        cookie_str = "; ".join([f"{k}={v}" for k, v in all_cookies.items()])

        return cookie_str


# 全局 API 实例（延迟初始化）
_api_instance: Optional[QzAPI] = None


def get_api() -> QzAPI:
    """获取全局 API 实例"""
    global _api_instance
    if _api_instance is None:
        _api_instance = QzAPI()
    return _api_instance
