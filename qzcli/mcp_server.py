"""
qzcli MCP server
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

from mcp.server.fastmcp import FastMCP

from .api import QzAPIError, get_api
from .config import (
    find_resource_by_name,
    find_workspace_by_name,
    get_cookie,
    get_workspace_resources,
    list_cached_workspaces,
    save_cookie,
    save_resources,
    update_workspace_compute_groups,
    update_workspace_projects,
)
from .store import JobRecord, get_store


TYPE_NAMES = {
    "distributed_training": "分布式训练",
    "interactive_modeling": "交互式建模",
    "inference_serving_customize": "推理服务",
    "inference_serving": "推理服务",
    "training": "训练",
}

TERMINAL_SUCCESS_TOKENS = (
    "success",
    "succeeded",
    "complete",
    "completed",
    "finish",
    "finished",
    "done",
)
TERMINAL_FAILED_TOKENS = (
    "fail",
    "failed",
    "error",
    "exception",
    "killed",
    "crash",
)
STOPPED_TOKENS = (
    "stop",
    "stopped",
    "cancel",
    "cancelled",
    "canceled",
    "terminate",
    "terminated",
)
WAITING_TOKENS = (
    "queue",
    "queued",
    "queuing",
    "pending",
    "waiting",
    "wait",
    "schedule",
    "scheduling",
    "creating",
    "starting",
)
RUNNING_TOKENS = (
    "running",
    "executing",
    "active",
    "serving",
    "training",
    "processing",
)


server = FastMCP(
    name="qzcli-mcp",
    instructions=(
        "启智平台任务与资源查询 MCP。"
        "优先使用返回中的 raw/status_raw，不要假设状态字段名称或枚举恒定不变。"
    ),
    dependencies=["requests>=2.28", "rich>=13.0", "mcp>=1.0"],
)


def _now_iso() -> str:
    return datetime.now().isoformat()


def _result(data: Any, *, message: str = "", warnings: Optional[list[str]] = None) -> dict[str, Any]:
    return {
        "ok": True,
        "message": message,
        "generated_at": _now_iso(),
        "warnings": warnings or [],
        "data": data,
    }


def _cookie_preview(cookie: str) -> str:
    if len(cookie) <= 16:
        return cookie
    return f"{cookie[:8]}...{cookie[-8:]}"


def _require_cookie() -> tuple[str, dict[str, Any]]:
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        raise RuntimeError("未设置 cookie，请先运行 qzcli login / qzcli cookie，或调用 qz_auth_login / qz_set_cookie。")
    return cookie_data["cookie"], cookie_data


def _match_workspace_from_remote(name: str, cookie: str) -> Optional[dict[str, str]]:
    api = get_api()
    workspaces = api.list_workspaces(cookie)

    for workspace in workspaces:
        if workspace.get("name", "") == name:
            return workspace

    lowered = name.lower()
    for workspace in workspaces:
        if lowered in workspace.get("name", "").lower():
            return workspace

    return None


def _resolve_workspace_refs(
    workspace: Optional[str] = None,
    *,
    all_workspaces: bool = False,
    allow_default: bool = True,
) -> list[dict[str, str]]:
    cookie: Optional[str] = None
    cookie_data: dict[str, Any] = {}
    if workspace or all_workspaces:
        try:
            cookie, cookie_data = _require_cookie()
        except Exception:
            cookie = None
            cookie_data = {}

    if all_workspaces:
        cached = list_cached_workspaces()
        if cached:
            return [{"id": item["id"], "name": item.get("name", "")} for item in cached]
        if not cookie:
            raise RuntimeError("没有已缓存的工作空间，且当前没有可用 cookie 用于远端发现。")
        return get_api().list_workspaces(cookie)

    if workspace:
        if workspace.startswith("ws-"):
            ws_resources = get_workspace_resources(workspace)
            return [{"id": workspace, "name": (ws_resources or {}).get("name", "")}]

        workspace_id = find_workspace_by_name(workspace)
        if workspace_id:
            ws_resources = get_workspace_resources(workspace_id)
            return [{"id": workspace_id, "name": (ws_resources or {}).get("name", workspace)}]

        if not cookie:
            cookie, _ = _require_cookie()
        remote_workspace = _match_workspace_from_remote(workspace, cookie)
        if remote_workspace:
            return [remote_workspace]

        raise RuntimeError(f"未找到名称为 '{workspace}' 的工作空间。")

    if allow_default:
        _, cookie_data = _require_cookie()
        default_workspace = cookie_data.get("workspace_id", "")
        if default_workspace:
            ws_resources = get_workspace_resources(default_workspace)
            return [{"id": default_workspace, "name": (ws_resources or {}).get("name", "")}]

    raise RuntimeError("请指定 workspace，或先给 cookie 设置默认 workspace_id。")


def _normalize_status(status: Any) -> dict[str, Any]:
    status_raw = "" if status is None else str(status)
    lowered = status_raw.strip().lower()

    family = "unknown"
    matched_tokens: list[str] = []

    def contains_any(tokens: tuple[str, ...]) -> list[str]:
        return [token for token in tokens if token in lowered]

    for family_name, tokens in (
        ("terminal_success", TERMINAL_SUCCESS_TOKENS),
        ("terminal_failed", TERMINAL_FAILED_TOKENS),
        ("stopped", STOPPED_TOKENS),
        ("waiting", WAITING_TOKENS),
        ("running", RUNNING_TOKENS),
    ):
        matched_tokens = contains_any(tokens)
        if matched_tokens:
            family = family_name
            break

    is_terminal = family in {"terminal_success", "terminal_failed", "stopped"}
    is_active = family in {"running", "waiting"}

    return {
        "status_raw": status_raw,
        "status_normalized": lowered,
        "status_family": family,
        "is_terminal": is_terminal,
        "is_active": is_active,
        "matched_tokens": matched_tokens,
    }


def _job_summary_from_api(job_data: dict[str, Any]) -> dict[str, Any]:
    job = JobRecord.from_api_response(job_data)
    status_info = _normalize_status(job_data.get("status", job.status))

    return {
        "job_id": job.job_id,
        "name": job.name,
        "workspace_id": job.workspace_id,
        "project_id": job.project_id,
        "project_name": job.project_name,
        "compute_group_name": job.compute_group_name,
        "gpu_type": job.gpu_type,
        "gpu_count": job.gpu_count,
        "instance_count": job.instance_count,
        "priority_level": job.priority_level,
        "running_time_ms": job.running_time_ms,
        "created_at": job.created_at,
        "finished_at": job.finished_at,
        "url": job.url,
        **status_info,
        "raw": job_data,
    }


def _job_summary_from_store(job: JobRecord) -> dict[str, Any]:
    status_info = _normalize_status(job.status)
    return {
        "job_id": job.job_id,
        "name": job.name,
        "workspace_id": job.workspace_id,
        "project_id": job.project_id,
        "project_name": job.project_name,
        "compute_group_name": job.compute_group_name,
        "gpu_type": job.gpu_type,
        "gpu_count": job.gpu_count,
        "instance_count": job.instance_count,
        "priority_level": job.priority_level,
        "running_time_ms": job.running_time_ms,
        "created_at": job.created_at,
        "finished_at": job.finished_at,
        "url": job.url,
        "source": job.source,
        "metadata": job.metadata,
        **status_info,
        "raw": job.to_dict(),
    }


def _is_running_like(status_info: dict[str, Any]) -> bool:
    if status_info["is_active"]:
        return True
    normalized = status_info["status_normalized"]
    return "running" in normalized or "queue" in normalized or "pending" in normalized


def _paginate_task_dimensions(workspace_id: str, cookie: str, page_size: int = 200) -> list[dict[str, Any]]:
    api = get_api()
    tasks: list[dict[str, Any]] = []
    page_num = 1

    while True:
        data = api.list_task_dimension(workspace_id, cookie, page_num=page_num, page_size=page_size)
        page_tasks = data.get("task_dimensions", [])
        total_count = data.get("total", 0)
        tasks.extend(page_tasks)
        if len(tasks) >= total_count or not page_tasks:
            break
        page_num += 1

    return tasks


def _refresh_workspace_resources(workspace_id: str, workspace_name: str, cookie: str) -> dict[str, Any]:
    api = get_api()
    result = api.list_jobs_with_cookie(workspace_id, cookie, page_size=200)
    jobs = result.get("jobs", [])
    resources = api.extract_resources_from_jobs(jobs)

    compute_groups_from_api: list[dict[str, Any]] = []
    cluster_info_warning = ""
    try:
        cluster_info = api.get_cluster_basic_info(workspace_id, cookie)
        for compute_group in cluster_info.get("compute_groups", []):
            for logic_group in compute_group.get("logic_compute_groups", []):
                logic_group_id = logic_group.get("logic_compute_group_id", "")
                logic_group_name = logic_group.get("logic_compute_group_name", "")
                brand = logic_group.get("brand", "")
                resource_types = logic_group.get("resource_types", [])
                gpu_type = resource_types[0] if resource_types else ""
                if logic_group_id:
                    compute_groups_from_api.append(
                        {
                            "id": logic_group_id,
                            "name": logic_group_name,
                            "gpu_type": brand or gpu_type,
                            "workspace_id": workspace_id,
                        }
                    )
    except Exception as exc:
        cluster_info_warning = str(exc)

    if compute_groups_from_api:
        resources["compute_groups"] = compute_groups_from_api

    save_resources(workspace_id, resources, workspace_name)

    return {
        "workspace_id": workspace_id,
        "workspace_name": workspace_name,
        "job_sample_count": len(jobs),
        "project_count": len(resources.get("projects", [])),
        "compute_group_count": len(resources.get("compute_groups", [])),
        "spec_count": len(resources.get("specs", [])),
        "cluster_info_warning": cluster_info_warning,
    }


def _availability_result(
    workspace_id: str,
    workspace_name: str,
    group_id: str,
    group_name: str,
    gpu_type: str,
    nodes: list[dict[str, Any]],
    node_low_priority_gpu: dict[str, int],
) -> dict[str, Any]:
    free_nodes = []
    low_priority_free_nodes = []
    gpu_free_distribution: dict[int, int] = {}
    total_free_gpus = 0
    total_gpus = 0

    for node in nodes:
        node_name = node.get("name", "")
        node_status = node.get("status", "")
        cordon_type = node.get("cordon_type", "")
        gpu_info = node.get("gpu", {})
        gpu_used = gpu_info.get("used", 0)
        gpu_total = gpu_info.get("total", 0)

        if gpu_total == 0:
            continue

        is_schedulable = node_status == "Ready" and not cordon_type
        gpu_free = max(0, gpu_total - gpu_used)
        total_gpus += gpu_total

        if not is_schedulable:
            continue

        total_free_gpus += gpu_free

        if gpu_free > 0:
            gpu_free_distribution[gpu_free] = gpu_free_distribution.get(gpu_free, 0) + 1

        if gpu_used == 0:
            free_nodes.append({"name": node_name, "gpu_total": gpu_total})

        low_priority_gpu = node_low_priority_gpu.get(node_name, 0)
        if low_priority_gpu >= gpu_total and gpu_used > 0:
            low_priority_free_nodes.append(
                {
                    "name": node_name,
                    "gpu_total": gpu_total,
                    "low_priority_gpu": low_priority_gpu,
                }
            )

    gpu_utilization_ratio = 0.0
    if total_gpus > 0:
        gpu_utilization_ratio = max(0.0, (total_gpus - total_free_gpus) / total_gpus)

    return {
        "workspace_id": workspace_id,
        "workspace_name": workspace_name,
        "compute_group_id": group_id,
        "compute_group_name": group_name,
        "gpu_type": gpu_type,
        "total_nodes": len(nodes),
        "free_nodes": len(free_nodes),
        "free_node_names": [item["name"] for item in free_nodes],
        "low_priority_free_nodes": len(low_priority_free_nodes),
        "low_priority_free_node_names": [item["name"] for item in low_priority_free_nodes],
        "total_gpus": total_gpus,
        "total_free_gpus": total_free_gpus,
        "gpu_utilization_ratio": gpu_utilization_ratio,
        "gpu_free_distribution": {str(key): value for key, value in sorted(gpu_free_distribution.items(), reverse=True)},
        "raw_node_count": len(nodes),
    }


@server.tool(description="通过 CAS 登录启智平台并保存 cookie。")
def qz_auth_login(username: str, password: str, workspace_id: str = "") -> dict[str, Any]:
    api = get_api()
    cookie = api.login_with_cas(username, password)
    save_cookie(cookie, workspace_id)
    cookie_names = [segment.split("=", 1)[0].strip() for segment in cookie.split(";") if "=" in segment]

    return _result(
        {
            "workspace_id": workspace_id,
            "cookie_saved": True,
            "cookie_preview": _cookie_preview(cookie),
            "cookie_names": cookie_names,
            "session_cookie_detected": any("session" in name.lower() for name in cookie_names),
        },
        message="登录成功并已保存 cookie。",
    )


@server.tool(description="手动设置浏览器 cookie，可选校验默认工作空间是否可访问。")
def qz_set_cookie(cookie: str, workspace_id: str = "", test: bool = True) -> dict[str, Any]:
    total = None
    if test and workspace_id:
        total = get_api().list_jobs_with_cookie(workspace_id, cookie, page_size=1).get("total", 0)

    save_cookie(cookie, workspace_id)
    cookie_names = [segment.split("=", 1)[0].strip() for segment in cookie.split(";") if "=" in segment]

    return _result(
        {
            "workspace_id": workspace_id,
            "cookie_saved": True,
            "cookie_preview": _cookie_preview(cookie),
            "cookie_names": cookie_names,
            "session_cookie_detected": any("session" in name.lower() for name in cookie_names),
            "validated_total_jobs": total,
        },
        message="cookie 已保存。",
    )


@server.tool(description="列出可访问或已缓存的工作空间。")
def qz_list_workspaces(refresh: bool = True) -> dict[str, Any]:
    cached = list_cached_workspaces()

    if not refresh:
        return _result(
            {
                "source": "cache",
                "workspaces": cached,
            },
            message="已返回本地缓存的工作空间。",
        )

    cookie, _ = _require_cookie()
    live = get_api().list_workspaces(cookie)
    cached_map = {item["id"]: item for item in cached}

    merged = []
    for workspace in live:
        cached_item = cached_map.get(workspace["id"], {})
        merged.append(
            {
                "id": workspace["id"],
                "name": workspace.get("name", ""),
                "cached_name": cached_item.get("name", ""),
                "updated_at": cached_item.get("updated_at", 0),
                "project_count": cached_item.get("project_count", 0),
                "compute_group_count": cached_item.get("compute_group_count", 0),
                "spec_count": cached_item.get("spec_count", 0),
            }
        )

    return _result(
        {
            "source": "live+cache",
            "workspaces": merged,
            "cached_only_count": max(0, len(cached) - len(merged)),
        },
        message=f"发现 {len(merged)} 个可访问工作空间。",
    )


@server.tool(description="刷新一个或全部工作空间的资源缓存（项目、计算组、规格）。")
def qz_refresh_resources(workspace: str = "", all_workspaces: bool = False) -> dict[str, Any]:
    cookie, _ = _require_cookie()
    workspace_refs = _resolve_workspace_refs(workspace or None, all_workspaces=all_workspaces or not workspace)

    results = []
    warnings = []
    for workspace_ref in workspace_refs:
        workspace_id = workspace_ref.get("id", "")
        workspace_name = workspace_ref.get("name", "")
        try:
            refreshed = _refresh_workspace_resources(workspace_id, workspace_name, cookie)
            results.append(refreshed)
            if refreshed.get("cluster_info_warning"):
                warnings.append(
                    f"{workspace_name or workspace_id}: cluster_basic_info 失败，已保留基于任务历史提取的资源。"
                )
        except Exception as exc:
            warnings.append(f"{workspace_name or workspace_id}: {exc}")

    return _result(
        {
            "requested_count": len(workspace_refs),
            "refreshed_count": len(results),
            "results": results,
        },
        message="资源刷新完成。",
        warnings=warnings,
    )


@server.tool(description="查询计算组空闲节点、低优空余节点与推荐计算组。")
def qz_get_availability(
    workspace: str = "",
    group: str = "",
    required_nodes: int = 0,
    include_low_priority: bool = False,
    refresh_if_missing: bool = True,
) -> dict[str, Any]:
    cookie, _ = _require_cookie()
    workspace_refs = _resolve_workspace_refs(workspace or None, all_workspaces=not bool(workspace))
    api = get_api()
    warnings = []
    all_results = []

    for workspace_ref in workspace_refs:
        workspace_id = workspace_ref["id"]
        workspace_name = workspace_ref.get("name", "")
        cached_resources = get_workspace_resources(workspace_id)

        if not cached_resources:
            if refresh_if_missing:
                refreshed = _refresh_workspace_resources(workspace_id, workspace_name, cookie)
                cached_resources = get_workspace_resources(workspace_id)
                warnings.append(f"{workspace_name or workspace_id}: 未命中缓存，已自动刷新资源。")
                if refreshed.get("cluster_info_warning"):
                    warnings.append(
                        f"{workspace_name or workspace_id}: cluster_basic_info 失败，结果可能缺少部分计算组。"
                    )
            else:
                warnings.append(f"{workspace_name or workspace_id}: 未命中缓存，已跳过。")
                continue

        if not cached_resources:
            continue

        compute_groups = dict(cached_resources.get("compute_groups", {}))

        if group:
            if group.startswith("lcg-"):
                if group in compute_groups:
                    compute_groups = {group: compute_groups[group]}
                else:
                    continue
            else:
                found = find_resource_by_name(workspace_id, "compute_groups", group)
                if found:
                    compute_groups = {found["id"]: found}
                else:
                    continue

        if not compute_groups:
            continue

        node_low_priority_gpu: dict[str, int] = defaultdict(int)
        if include_low_priority:
            tasks = _paginate_task_dimensions(workspace_id, cookie)
            for task in tasks:
                priority = task.get("priority", 10)
                if priority > 3:
                    continue
                gpu_total = task.get("gpu", {}).get("total", 0)
                node_names = task.get("nodes_occupied", {}).get("nodes", [])
                gpu_per_node = gpu_total // len(node_names) if node_names else 0
                for node_name in node_names:
                    node_low_priority_gpu[node_name] += gpu_per_node if len(node_names) > 1 else gpu_total

        for group_id, group_info in compute_groups.items():
            nodes = api.list_node_dimension(workspace_id, cookie, group_id, page_size=1000).get("node_dimensions", [])
            availability = _availability_result(
                workspace_id,
                cached_resources.get("name", workspace_name) or workspace_id,
                group_id,
                group_info.get("name", group_id),
                group_info.get("gpu_type", ""),
                nodes,
                node_low_priority_gpu,
            )
            if "specs" in cached_resources:
                availability["spec_ids"] = list(cached_resources["specs"].keys())
            all_results.append(availability)

    if include_low_priority:
        sorted_results = sorted(
            all_results,
            key=lambda item: (
                item["free_nodes"] + item["low_priority_free_nodes"],
                item["free_nodes"],
                item["total_free_gpus"],
            ),
            reverse=True,
        )
    else:
        sorted_results = sorted(
            all_results,
            key=lambda item: (item["free_nodes"], item["total_free_gpus"]),
            reverse=True,
        )

    available_results = sorted_results
    if required_nodes > 0:
        if include_low_priority:
            available_results = [
                item for item in sorted_results if item["free_nodes"] + item["low_priority_free_nodes"] >= required_nodes
            ]
        else:
            available_results = [item for item in sorted_results if item["free_nodes"] >= required_nodes]

    recommended = available_results[0] if available_results else None

    return _result(
        {
            "filters": {
                "workspace": workspace,
                "group": group,
                "required_nodes": required_nodes,
                "include_low_priority": include_low_priority,
            },
            "result_count": len(sorted_results),
            "available_count": len(available_results),
            "recommended": recommended,
            "results": sorted_results,
        },
        message="空闲资源查询完成。",
        warnings=warnings,
    )


@server.tool(description="列出任务，返回 raw 状态与归一化状态。")
def qz_list_jobs(
    workspace: str = "",
    all_workspaces: bool = False,
    running_only: bool = False,
    limit: int = 20,
) -> dict[str, Any]:
    cookie, _ = _require_cookie()
    workspace_refs = _resolve_workspace_refs(workspace or None, all_workspaces=all_workspaces)
    api = get_api()

    jobs = []
    warnings = []

    for workspace_ref in workspace_refs:
        workspace_id = workspace_ref["id"]
        workspace_name = workspace_ref.get("name", "")
        try:
            payload = api.list_jobs_with_cookie(
                workspace_id,
                cookie,
                page_size=limit * 2 if running_only else limit,
            )
            for job_data in payload.get("jobs", []):
                summary = _job_summary_from_api(job_data)
                summary["workspace_name"] = workspace_name
                jobs.append(summary)
        except Exception as exc:
            warnings.append(f"{workspace_name or workspace_id}: {exc}")

    jobs.sort(key=lambda item: item.get("created_at") or "", reverse=True)

    if running_only:
        jobs = [job for job in jobs if _is_running_like(job)]

    jobs = jobs[:limit]

    status_counts: dict[str, int] = defaultdict(int)
    status_family_counts: dict[str, int] = defaultdict(int)
    for job in jobs:
        status_counts[job["status_raw"]] += 1
        status_family_counts[job["status_family"]] += 1

    return _result(
        {
            "workspace_count": len(workspace_refs),
            "job_count": len(jobs),
            "status_counts": dict(sorted(status_counts.items(), key=lambda item: (-item[1], item[0]))),
            "status_family_counts": dict(sorted(status_family_counts.items(), key=lambda item: (-item[1], item[0]))),
            "jobs": jobs,
        },
        message="任务列表查询完成。",
        warnings=warnings,
    )


@server.tool(description="查询单个任务详情，返回 raw 详情与归一化状态。")
def qz_get_job_detail(job_id: str) -> dict[str, Any]:
    api_data = get_api().get_job_detail(job_id)
    summary = _job_summary_from_api(api_data)

    extra_keys = sorted(
        key
        for key in api_data.keys()
        if key
        not in {
            "job_id",
            "name",
            "status",
            "workspace_id",
            "project_id",
            "project_name",
            "logic_compute_group_name",
            "framework_config",
            "created_at",
            "finished_at",
            "running_time_ms",
            "priority_level",
            "command",
        }
    )

    return _result(
        {
            **summary,
            "extra_field_names": extra_keys,
        },
        message="任务详情查询完成。",
    )


@server.tool(description="停止任务。")
def qz_stop_job(job_id: str) -> dict[str, Any]:
    success = get_api().stop_job(job_id)
    if not success:
        raise RuntimeError(f"停止任务失败: {job_id}")

    return _result(
        {
            "job_id": job_id,
            "stopped": True,
        },
        message="任务已停止。",
    )


@server.tool(description="统计工作空间 GPU 使用分布。")
def qz_get_usage(workspace: str = "") -> dict[str, Any]:
    cookie, _ = _require_cookie()
    workspace_refs = _resolve_workspace_refs(workspace or None, all_workspaces=not bool(workspace))
    api = get_api()
    all_stats = []
    warnings = []

    for workspace_ref in workspace_refs:
        workspace_id = workspace_ref["id"]
        workspace_name = workspace_ref.get("name", "")
        try:
            tasks = _paginate_task_dimensions(workspace_id, cookie)
            if not tasks:
                continue

            gpu_distribution: dict[int, int] = defaultdict(int)
            user_gpu: dict[str, int] = defaultdict(int)
            project_gpu: dict[str, int] = defaultdict(int)
            type_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"count": 0, "gpu": 0})
            priority_stats: dict[int, dict[str, int]] = defaultdict(lambda: {"count": 0, "gpu": 0})
            total_gpu = 0
            projects_found: dict[str, dict[str, str]] = {}

            for task in tasks:
                gpu_total = task.get("gpu", {}).get("total", 0)
                user_name = task.get("user", {}).get("name", "未知")
                project_info = task.get("project", {})
                project_name = project_info.get("name", "未知")
                project_id = project_info.get("id", "")
                task_type = task.get("type", "unknown")
                priority = task.get("priority", 0)

                if project_id and project_id not in projects_found:
                    projects_found[project_id] = {"id": project_id, "name": project_name}

                gpu_distribution[gpu_total] += 1
                user_gpu[user_name] += gpu_total
                project_gpu[project_name] += gpu_total
                type_stats[task_type]["count"] += 1
                type_stats[task_type]["gpu"] += gpu_total
                priority_stats[priority]["count"] += 1
                priority_stats[priority]["gpu"] += gpu_total
                total_gpu += gpu_total

            if projects_found:
                update_workspace_projects(workspace_id, list(projects_found.values()), workspace_name)

            try:
                node_data = api.list_node_dimension(workspace_id, cookie, page_size=500)
                nodes = node_data.get("node_dimensions", [])
                compute_groups_found: dict[str, dict[str, str]] = {}
                for node in nodes:
                    logic_group = node.get("logic_compute_group", {})
                    logic_group_id = logic_group.get("id", "")
                    logic_group_name = logic_group.get("name", "")
                    if logic_group_id and logic_group_id not in compute_groups_found:
                        compute_groups_found[logic_group_id] = {
                            "id": logic_group_id,
                            "name": logic_group_name,
                            "gpu_type": node.get("gpu", {}).get("type", ""),
                            "workspace_id": workspace_id,
                        }
                if compute_groups_found:
                    update_workspace_compute_groups(workspace_id, list(compute_groups_found.values()), workspace_name)
            except QzAPIError:
                pass

            all_stats.append(
                {
                    "workspace_id": workspace_id,
                    "workspace_name": workspace_name,
                    "total_tasks": len(tasks),
                    "total_gpu": total_gpu,
                    "gpu_distribution": {str(key): value for key, value in sorted(gpu_distribution.items())},
                    "user_gpu": dict(sorted(user_gpu.items(), key=lambda item: (-item[1], item[0]))),
                    "project_gpu": dict(sorted(project_gpu.items(), key=lambda item: (-item[1], item[0]))),
                    "type_stats": {
                        task_type: {
                            **info,
                            "display_name": TYPE_NAMES.get(task_type, task_type),
                        }
                        for task_type, info in sorted(type_stats.items(), key=lambda item: -item[1]["gpu"])
                    },
                    "priority_stats": {
                        str(priority): info
                        for priority, info in sorted(priority_stats.items(), key=lambda item: -item[0])
                    },
                }
            )
        except Exception as exc:
            warnings.append(f"{workspace_name or workspace_id}: {exc}")

    total_tasks = sum(item["total_tasks"] for item in all_stats)
    total_gpu = sum(item["total_gpu"] for item in all_stats)

    return _result(
        {
            "workspace_count": len(all_stats),
            "total_tasks": total_tasks,
            "total_gpu": total_gpu,
            "workspaces": all_stats,
        },
        message="GPU 使用分布统计完成。",
        warnings=warnings,
    )


@server.tool(description="查看当前抓到的原始状态值与归一化结果，便于应对平台字段漂移。")
def qz_inspect_status_catalog(
    workspace: str = "",
    all_workspaces: bool = False,
    limit_per_workspace: int = 200,
    sample_limit: int = 5,
) -> dict[str, Any]:
    cookie, _ = _require_cookie()
    workspace_refs = _resolve_workspace_refs(workspace or None, all_workspaces=all_workspaces)
    api = get_api()

    catalog: dict[str, dict[str, Any]] = {}
    warnings = []

    for workspace_ref in workspace_refs:
        workspace_id = workspace_ref["id"]
        workspace_name = workspace_ref.get("name", "")
        try:
            payload = api.list_jobs_with_cookie(workspace_id, cookie, page_size=limit_per_workspace)
            for job_data in payload.get("jobs", []):
                status_info = _normalize_status(job_data.get("status", ""))
                status_raw = status_info["status_raw"]
                if status_raw not in catalog:
                    catalog[status_raw] = {
                        "status_raw": status_raw,
                        "status_family": status_info["status_family"],
                        "matched_tokens": status_info["matched_tokens"],
                        "count": 0,
                        "sample_job_ids": [],
                        "sample_workspace_ids": [],
                    }
                catalog_item = catalog[status_raw]
                catalog_item["count"] += 1
                if len(catalog_item["sample_job_ids"]) < sample_limit:
                    catalog_item["sample_job_ids"].append(job_data.get("job_id", ""))
                if workspace_id not in catalog_item["sample_workspace_ids"] and len(catalog_item["sample_workspace_ids"]) < sample_limit:
                    catalog_item["sample_workspace_ids"].append(workspace_id)
        except Exception as exc:
            warnings.append(f"{workspace_name or workspace_id}: {exc}")

    sorted_catalog = sorted(catalog.values(), key=lambda item: (-item["count"], item["status_raw"]))
    unknown_entries = [item for item in sorted_catalog if item["status_family"] == "unknown"]

    return _result(
        {
            "workspace_count": len(workspace_refs),
            "catalog_size": len(sorted_catalog),
            "unknown_status_count": len(unknown_entries),
            "statuses": sorted_catalog,
            "unknown_statuses": unknown_entries,
        },
        message="原始状态目录扫描完成。",
        warnings=warnings,
    )


@server.tool(description="将任务 ID 加入本地追踪列表。")
def qz_track_job(job_id: str, name: str = "", source: str = "", workspace_id: str = "") -> dict[str, Any]:
    api = get_api()
    store = get_store()

    try:
        api_data = api.get_job_detail(job_id)
        job = JobRecord.from_api_response(api_data, source=source or "")
    except Exception:
        job = JobRecord(
            job_id=job_id,
            name=name or "",
            source=source or "",
            workspace_id=workspace_id or "",
        )

    if name:
        job.name = name
    if source:
        job.source = source
    if workspace_id:
        job.workspace_id = workspace_id

    store.add(job)

    return _result(
        {
            "job": _job_summary_from_store(job),
        },
        message="任务已加入本地追踪列表。",
    )


@server.tool(description="列出本地追踪任务，可选刷新非终态任务状态。")
def qz_list_tracked_jobs(limit: int = 20, running_only: bool = False, refresh: bool = True) -> dict[str, Any]:
    store = get_store()
    api = get_api()
    fetch_limit = limit * 3 if running_only else limit
    jobs = store.list(limit=fetch_limit)
    warnings = []

    if refresh and jobs:
        job_ids = [job.job_id for job in jobs if not _normalize_status(job.status)["is_terminal"]]
        if job_ids:
            try:
                results = api.get_jobs_detail(job_ids)
                for job_id, api_data in results.items():
                    if "error" not in api_data:
                        store.update_from_api(job_id, api_data)
            except Exception as exc:
                warnings.append(f"刷新远端状态失败，已返回本地缓存: {exc}")
        jobs = store.list(limit=fetch_limit)

    summaries = [_job_summary_from_store(job) for job in jobs]
    if running_only:
        summaries = [summary for summary in summaries if _is_running_like(summary)]

    summaries = summaries[:limit]

    return _result(
        {
            "job_count": len(summaries),
            "jobs": summaries,
        },
        message="本地追踪任务列表查询完成。",
        warnings=warnings,
    )


def _resolve_resource_id_mcp(
    workspace_id: str, resource_type: str, value: str
) -> tuple[Optional[str], Optional[str]]:
    """Resolve a resource name or ID to (id, display_name) for MCP tools."""
    if not value:
        return None, None
    prefixes = {"projects": "project-", "compute_groups": "lcg-"}
    prefix = prefixes.get(resource_type, "")
    if prefix and value.startswith(prefix):
        return value, value
    if resource_type == "specs" and len(value) > 20:
        return value, value
    found = find_resource_by_name(workspace_id, resource_type, value)
    if found:
        return found["id"], found.get("name", value)
    return None, None


def _auto_select_resource_mcp(
    workspace_id: str, resource_type: str
) -> tuple[Optional[str], Optional[str]]:
    """Auto-select the first resource of a given type from cache."""
    ws_resources = get_workspace_resources(workspace_id)
    if not ws_resources:
        return None, None
    resources = ws_resources.get(resource_type, {})
    if not resources:
        return None, None
    first = next(iter(resources.values()))
    return first["id"], first.get("name", first["id"])


@server.tool(
    description=(
        "创建并提交任务到启智平台。workspace/project/compute_group 支持名称或 ID"
        "（名称从 qz_refresh_resources 缓存解析）。"
        "省略 project/spec 时自动从缓存选取第一个。"
    )
)
def qz_create_job(
    name: str,
    command: str,
    workspace: str,
    project: str = "",
    compute_group: str = "",
    spec: str = "",
    image: str = "docker.sii.shaipower.online/inspire-studio/dhyu-wan-torch29:0.4",
    image_type: str = "SOURCE_PRIVATE",
    instances: int = 1,
    shm: int = 1200,
    priority: int = 10,
    framework: str = "pytorch",
    track: bool = True,
) -> dict[str, Any]:
    api = get_api()
    store = get_store()
    warnings: list[str] = []

    # Resolve workspace
    if workspace.startswith("ws-"):
        workspace_id = workspace
    else:
        workspace_id = find_workspace_by_name(workspace)
        if not workspace_id:
            raise RuntimeError(f"未找到名称为 '{workspace}' 的工作空间。请先运行 qz_refresh_resources。")

    # Resolve project
    if project:
        if project.startswith("project-"):
            project_id = project
        else:
            project_id, _ = _resolve_resource_id_mcp(workspace_id, "projects", project)
            if not project_id:
                raise RuntimeError(f"未找到项目 '{project}'。")
    else:
        project_id, proj_name = _auto_select_resource_mcp(workspace_id, "projects")
        if not project_id:
            raise RuntimeError("未指定项目且缓存中无可用项目。请指定 project 或先调用 qz_refresh_resources。")
        warnings.append(f"自动选择项目: {proj_name} ({project_id})")

    # Resolve compute group
    if compute_group:
        if compute_group.startswith("lcg-"):
            compute_group_id = compute_group
        else:
            compute_group_id, _ = _resolve_resource_id_mcp(workspace_id, "compute_groups", compute_group)
            if not compute_group_id:
                raise RuntimeError(f"未找到计算组 '{compute_group}'。")
    else:
        compute_group_id, cg_name = _auto_select_resource_mcp(workspace_id, "compute_groups")
        if not compute_group_id:
            raise RuntimeError("未指定计算组且缓存中无可用计算组。请指定 compute_group 或先调用 qz_refresh_resources。")
        warnings.append(f"自动选择计算组: {cg_name} ({compute_group_id})")

    # Resolve spec
    if spec:
        spec_id = spec
    else:
        spec_id, spec_name = _auto_select_resource_mcp(workspace_id, "specs")
        if not spec_id:
            raise RuntimeError("未指定规格且缓存中无可用规格。请指定 spec 或先调用 qz_refresh_resources。")
        warnings.append(f"自动选择规格: {spec_name} ({spec_id})")

    payload = {
        "name": name,
        "logic_compute_group_id": compute_group_id,
        "project_id": project_id,
        "workspace_id": workspace_id,
        "framework": framework,
        "command": command,
        "task_priority": priority,
        "auto_fault_tolerance": False,
        "framework_config": [
            {
                "spec_id": spec_id,
                "image": image,
                "image_type": image_type,
                "instance_count": instances,
                "shm_gi": shm,
            }
        ],
    }

    result = api.create_job(payload)
    job_id = result.get("job_id", "")
    resp_ws_id = result.get("workspace_id", workspace_id)

    if not job_id:
        raise RuntimeError(f"任务创建失败: 响应中未包含 job_id。raw={result}")

    job_url = f"https://qz.sii.edu.cn/jobs/distributedTrainingDetail/{job_id}?spaceId={resp_ws_id}"

    if track:
        job = JobRecord(
            job_id=job_id,
            name=name,
            status="job_pending",
            workspace_id=resp_ws_id,
            project_id=project_id,
            source="qz_create_job",
            command=command,
            url=job_url,
            instance_count=instances,
            priority_level=str(priority),
        )
        store.add(job)

    return _result(
        {
            "job_id": job_id,
            "workspace_id": resp_ws_id,
            "url": job_url,
            "name": name,
            "tracked": track,
            "payload": payload,
        },
        message="任务创建成功。",
        warnings=warnings,
    )


@server.tool()
def qz_create_hpc_job(
    name: str,
    entrypoint: str,
    workspace: str,
    compute_group: str,
    predef_quota_id: str,
    cpu: int,
    mem_gi: int,
    image: str,
    project: str = "",
    instances: int = 1,
    cpus_per_task: int = 1,
    memory_per_cpu: str = "5G",
    image_type: str = "SOURCE_PRIVATE",
    track: bool = True,
) -> dict[str, Any]:
    """
    提交 HPC/CPU 任务到启智平台（使用 cookie 认证，POST /api/v1/hpc_jobs）。

    Args:
        name: 任务名称
        entrypoint: 运行命令（shell 命令字符串）
        workspace: 工作空间名称或 ID（ws-...）
        compute_group: 计算组 ID（lcg-...）
        predef_quota_id: 预定义配额 ID（UUID）
        cpu: 每节点 CPU 核心数
        mem_gi: 每节点内存 GiB
        image: 容器镜像地址
        project: 项目名称或 ID（省略则自动选择）
        instances: 节点数（默认 1）
        cpus_per_task: 每任务 CPU 数（默认 1）
        memory_per_cpu: 每 CPU 内存字符串（默认 5G）
        image_type: 镜像类型（默认 SOURCE_PRIVATE）
        track: 是否追踪任务（默认 True）
    """
    api = get_api()
    store = get_store()
    warnings: list[str] = []

    cookie_data = get_cookie()
    if not cookie_data:
        raise RuntimeError("未找到 cookie，请先调用 qz_auth_login。")
    cookie = cookie_data.get("cookie", "")
    if not cookie:
        raise RuntimeError("cookie 为空，请先调用 qz_auth_login。")

    # Resolve workspace
    if workspace.startswith("ws-"):
        workspace_id = workspace
    else:
        workspace_id = find_workspace_by_name(workspace)
        if not workspace_id:
            raise RuntimeError(f"未找到名称为 '{workspace}' 的工作空间。请先运行 qz_refresh_resources。")

    # Resolve project
    if project:
        if project.startswith("project-"):
            project_id = project
        else:
            project_id, _ = _resolve_resource_id_mcp(workspace_id, "projects", project)
            if not project_id:
                raise RuntimeError(f"未找到项目 '{project}'。")
    else:
        project_id, proj_name = _auto_select_resource_mcp(workspace_id, "projects")
        if not project_id:
            raise RuntimeError("未指定项目且缓存中无可用项目。请指定 project 或先调用 qz_refresh_resources。")
        warnings.append(f"自动选择项目: {proj_name} ({project_id})")

    result = api.create_hpc_job(
        cookie=cookie,
        job_name=name,
        workspace_id=workspace_id,
        project_id=project_id,
        logic_compute_group_id=compute_group,
        entrypoint=entrypoint,
        image=image,
        predef_quota_id=predef_quota_id,
        cpu=cpu,
        mem_gi=mem_gi,
        instances=instances,
        cpus_per_task=cpus_per_task,
        memory_per_cpu=memory_per_cpu,
        image_type=image_type,
    )

    job_id = result.get("job_id", "")
    if not job_id:
        raise RuntimeError(f"任务创建失败: 响应中未包含 job_id。raw={result}")

    job_url = f"https://qz.sii.edu.cn/jobs/hpc?spaceId={workspace_id}"

    if track:
        job = JobRecord(
            job_id=job_id,
            name=name,
            status="job_pending",
            workspace_id=workspace_id,
            project_id=project_id,
            source="qz_create_hpc_job",
            command=entrypoint,
            url=job_url,
            instance_count=instances,
        )
        store.add(job)

    return _result(
        {
            "job_id": job_id,
            "workspace_id": workspace_id,
            "url": job_url,
            "name": name,
            "tracked": track,
        },
        message="HPC 任务创建成功。",
        warnings=warnings,
    )


@server.tool()
def qz_get_hpc_usage(
    workspace: str = "",
    compute_group: str = "",
    verbose: bool = False,
    top: int = 30,
) -> dict[str, Any]:
    """
    查看 HPC 节点的 CPU/内存利用率。

    通过 /api/v1/cluster_metric/list_node_dimension 接口获取各 HPC 节点实时
    CPU 和内存使用率，并按工作空间汇总统计。

    Args:
        workspace: 工作空间 ID 或名称，空字符串表示查询所有已缓存工作空间
        compute_group: 计算组 ID（lcg-...），空字符串表示查所有 HPC 节点
        verbose: 是否返回每个节点的详细数据（默认 False）
        top: verbose=True 时返回 CPU 利用率最高的前 N 个节点（默认 30）
    """
    cookie, warnings = _require_cookie()
    workspace_refs = _resolve_workspace_refs(workspace or None, all_workspaces=not bool(workspace))
    api = get_api()
    all_stats = []

    for workspace_ref in workspace_refs:
        workspace_id = workspace_ref["id"]
        workspace_name = workspace_ref.get("name", "")
        try:
            nodes: list[dict] = []
            page_num = 1
            page_size = 200
            while True:
                data = api.list_node_dimension(
                    workspace_id, cookie,
                    logic_compute_group_id=compute_group or None,
                    page_num=page_num,
                    page_size=page_size,
                )
                batch = data.get("node_dimensions", [])
                total = data.get("total", 0)
                nodes.extend(batch)
                if len(nodes) >= total or len(batch) < page_size:
                    break
                page_num += 1

            hpc_nodes = [n for n in nodes if n.get("node_type", "") == "hpc"]
            if not hpc_nodes:
                continue

            cpu_rates = [n.get("cpu", {}).get("usage_rate", 0) for n in hpc_nodes]
            mem_rates = [n.get("memory", {}).get("usage_rate", 0) for n in hpc_nodes]
            total_nodes = len(hpc_nodes)
            avg_cpu = sum(cpu_rates) / total_nodes if total_nodes else 0
            avg_mem = sum(mem_rates) / total_nodes if total_nodes else 0
            busy_nodes = sum(1 for r in cpu_rates if r > 0.05)

            stat: dict[str, Any] = {
                "workspace_id": workspace_id,
                "workspace_name": workspace_name,
                "total_hpc_nodes": total_nodes,
                "busy_nodes": busy_nodes,
                "avg_cpu_usage_pct": round(avg_cpu * 100, 2),
                "avg_mem_usage_pct": round(avg_mem * 100, 2),
            }

            if verbose:
                sorted_nodes = sorted(hpc_nodes, key=lambda n: -n.get("cpu", {}).get("usage_rate", 0))
                stat["nodes"] = [
                    {
                        "name": n.get("name", ""),
                        "cpu_usage_pct": round(n.get("cpu", {}).get("usage_rate", 0) * 100, 2),
                        "mem_usage_pct": round(n.get("memory", {}).get("usage_rate", 0) * 100, 2),
                        "cpu_used": n.get("cpu", {}).get("used", 0),
                        "cpu_total": n.get("cpu", {}).get("total", 0),
                        "mem_used_gib": round(n.get("memory", {}).get("used", 0), 2),
                        "mem_total_gib": round(n.get("memory", {}).get("total", 0), 2),
                    }
                    for n in sorted_nodes[:top]
                ]

            all_stats.append(stat)
        except Exception as exc:
            warnings.append(f"{workspace_name or workspace_id}: {exc}")

    return _result(
        {
            "workspace_count": len(all_stats),
            "workspaces": all_stats,
        },
        warnings=warnings,
    )


def main() -> None:
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
