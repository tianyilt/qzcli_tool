"""
启智平台 API 客户端
"""

import requests
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import (
    get_api_base_url,
    get_credentials,
    get_token_cache,
    save_token_cache,
    clear_token_cache,
)


class QzAPIError(Exception):
    """API 错误"""
    def __init__(self, message: str, code: Optional[int] = None):
        super().__init__(message)
        self.code = code


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
            raise QzAPIError("未配置认证信息，请运行 qzcli init 或设置环境变量 QZCLI_USERNAME/QZCLI_PASSWORD")
        
        url = f"{self.base_url}/auth/token"
        response = requests.post(
            url,
            json={"username": self._username, "password": self._password},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        
        data = response.json()
        if data.get("code") != 0:
            raise QzAPIError(f"获取 Token 失败: {data.get('message', '未知错误')}", data.get("code"))
        
        # Token 可能在顶层或 data 字段中
        token_data = data.get("data", data)
        self._token = token_data.get("access_token")
        if not self._token:
            raise QzAPIError("响应中未包含 access_token")
        
        expires_in_str = token_data.get("expires_in", "604800")
        expires_in = int(expires_in_str) if isinstance(expires_in_str, str) else expires_in_str
        save_token_cache(self._token, expires_in)
        
        return self._token
    
    def _request(self, endpoint: str, data: Dict[str, Any], retry_on_auth_error: bool = True) -> Dict[str, Any]:
        """发送 API 请求"""
        token = self._get_token()
        url = f"{self.base_url}{endpoint}"
        
        response = requests.post(
            url,
            json=data,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=60,
        )
        
        result = response.json()
        
        # Token 过期时重试
        if result.get("code") == -1 and retry_on_auth_error:
            clear_token_cache()
            self._token = None
            return self._request(endpoint, data, retry_on_auth_error=False)
        
        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}",
                result.get("code")
            )
        
        return result
    
    def get_job_detail(self, job_id: str) -> Dict[str, Any]:
        """查询任务详情"""
        result = self._request("/openapi/v1/train_job/detail", {"job_id": job_id})
        return result.get("data", {})
    
    def get_jobs_detail(self, job_ids: List[str], max_workers: int = 5) -> Dict[str, Dict[str, Any]]:
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
        """停止任务"""
        try:
            self._request("/openapi/v1/train_job/stop", {"job_id": job_id})
            return True
        except QzAPIError:
            return False
    
    def create_job(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """创建任务"""
        result = self._request("/openapi/v1/train_job/create", config)
        return result.get("data", result)
    
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
        page_num: int = 1,
        page_size: int = 100,
        project_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        获取工作空间内所有运行中的任务（使用浏览器 cookie 认证）
        
        Args:
            workspace_id: 工作空间 ID
            cookie: 浏览器 cookie 字符串
            page_num: 页码
            page_size: 每页数量（默认 100）
            project_filter: 项目名称过滤（包含匹配）
            
        Returns:
            API 响应数据，包含 task_dimensions 列表
        """
        url = f"{self.base_url}/api/v1/workspace/list_task_dimension"
        
        payload = {
            "page_num": page_num,
            "page_size": page_size,
            "filter": {"workspace_id": workspace_id}
        }
        
        # 模拟浏览器请求
        headers = {
            "Cookie": cookie,
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://qz.sii.edu.cn",
            "Referer": f"https://qz.sii.edu.cn/jobs/spacesOverview?spaceId={workspace_id}",
        }
        
        response = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=60,
        )
        
        if response.status_code == 401:
            raise QzAPIError("Cookie 已过期或无效，请重新获取", 401)
        
        if response.status_code != 200:
            raise QzAPIError(f"请求失败: HTTP {response.status_code}", response.status_code)
        
        try:
            result = response.json()
        except Exception:
            raise QzAPIError("响应不是有效的 JSON，请检查 cookie 是否正确")
        
        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}",
                result.get("code")
            )
        
        data = result.get("data", {})
        
        # 客户端过滤项目
        if project_filter:
            tasks = data.get("task_dimensions", [])
            filtered = [
                t for t in tasks
                if project_filter in t.get("project", {}).get("name", "")
            ]
            data["task_dimensions"] = filtered
        
        return data


# 全局 API 实例（延迟初始化）
_api_instance: Optional[QzAPI] = None


def get_api() -> QzAPI:
    """获取全局 API 实例"""
    global _api_instance
    if _api_instance is None:
        _api_instance = QzAPI()
    return _api_instance
