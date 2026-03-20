#!/usr/bin/env python3
"""
qzcli - 启智平台任务管理 CLI
"""

import sys
import time
import argparse
import unicodedata
from pathlib import Path
from typing import Optional, List, Sequence

from . import __version__
from .config import (
    init_config, get_credentials, load_config, save_config, CONFIG_DIR,
    save_cookie, get_cookie, clear_cookie,
    save_resources, get_workspace_resources, load_all_resources,
    set_workspace_name, find_workspace_by_name, find_resource_by_name,
    list_cached_workspaces, update_workspace_projects, update_workspace_compute_groups,
)
from .api import get_api, QzAPIError
from .store import get_store, JobRecord
from .display import get_display, format_duration, format_time_ago

try:
    from rich.table import Table
    from rich import box
    RICH_TABLE_AVAILABLE = True
except ImportError:
    RICH_TABLE_AVAILABLE = False
    Table = None  # type: ignore
    box = None  # type: ignore


def _char_display_width(ch: str) -> int:
    """计算单个字符在终端中的显示宽度（中文等宽字符按 2 计算）。"""
    if not ch:
        return 0
    if unicodedata.combining(ch):
        return 0
    if unicodedata.east_asian_width(ch) in ("F", "W"):
        return 2
    return 1


def _display_width(text: object) -> int:
    """计算字符串在终端中的显示宽度。"""
    return sum(_char_display_width(ch) for ch in str(text))


def _truncate_display_text(text: object, max_width: int) -> str:
    """按显示宽度截断文本。"""
    value = str(text)
    if max_width <= 0:
        return ""
    if _display_width(value) <= max_width:
        return value
    if max_width <= 3:
        return "." * max_width

    keep_width = max_width - 3
    chars = []
    used = 0
    for ch in value:
        ch_width = _char_display_width(ch)
        if used + ch_width > keep_width:
            break
        chars.append(ch)
        used += ch_width
    return "".join(chars) + "..."


def _format_cell(text: object, width: int, align: str = "left") -> str:
    """按显示宽度对齐单元格内容。"""
    value = _truncate_display_text(text, width)
    padding = max(0, width - _display_width(value))
    if align == "right":
        return " " * padding + value
    return value + " " * padding


def _render_plain_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[object]],
    aligns: Sequence[str],
    *,
    min_widths: Optional[Sequence[int]] = None,
    max_widths: Optional[Sequence[int]] = None,
    section_break_after_rows: Optional[Sequence[int]] = None,
    indent: str = "  ",
    col_gap: int = 2,
) -> List[str]:
    """渲染纯文本表格（按显示宽度对齐，兼容中文）。"""
    col_count = len(headers)
    if col_count == 0:
        return []

    min_widths = min_widths or [0] * col_count
    max_widths = max_widths or [0] * col_count
    align_list = list(aligns) if aligns else ["left"] * col_count
    if len(align_list) < col_count:
        align_list.extend(["left"] * (col_count - len(align_list)))

    col_widths: List[int] = []
    for i in range(col_count):
        width = _display_width(headers[i])
        for row in rows:
            if i < len(row):
                width = max(width, _display_width(row[i]))
        if i < len(min_widths):
            width = max(width, min_widths[i])
        if i < len(max_widths) and max_widths[i] > 0:
            width = min(width, max_widths[i])
        col_widths.append(width)

    def build_line(cells: Sequence[object]) -> str:
        rendered = []
        for i in range(col_count):
            value = cells[i] if i < len(cells) else ""
            rendered.append(_format_cell(value, col_widths[i], align_list[i]))
        return indent + (" " * col_gap).join(rendered)

    lines = [build_line(headers)]
    separator = indent + "-" * (sum(col_widths) + col_gap * (col_count - 1))
    lines.append(separator)
    section_breaks = set(section_break_after_rows or [])
    for row_idx, row in enumerate(rows):
        lines.append(build_line(row))
        if row_idx in section_breaks and row_idx < len(rows) - 1:
            lines.append(separator)
    return lines


def _format_percent(numerator: int, denominator: int) -> str:
    """格式化百分比。"""
    if denominator <= 0:
        return "-"
    return f"{(numerator / denominator) * 100:.1f}%"


def cmd_init(args):
    """初始化配置"""
    display = get_display()
    
    username = args.username
    password = args.password
    
    if not username:
        username = input("请输入启智平台用户名: ").strip()
    if not password:
        import getpass
        password = getpass.getpass("请输入密码: ")
    
    if not username or not password:
        display.print_error("用户名和密码不能为空")
        return 1
    
    init_config(username, password)
    
    # 测试连接
    display.print("正在验证连接...")
    api = get_api()
    if api.test_connection():
        display.print_success("配置成功！认证信息已保存")
        display.print(f"配置目录: {CONFIG_DIR}")
        return 0
    else:
        display.print_error("认证失败，请检查用户名和密码")
        return 1


def cmd_list_cookie(args):
    """使用 cookie 从 API 获取任务列表"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未设置 cookie，请先运行: qzcli cookie -f cookies.txt")
        return 1
    
    cookie = cookie_data["cookie"]
    
    # 确定要查询的工作空间列表
    workspace_input = args.workspace
    
    if args.all_ws:
        # 查询所有已缓存的工作空间
        all_resources = load_all_resources()
        if not all_resources:
            display.print_error("没有已缓存的工作空间")
            display.print("[dim]请先运行: qzcli res -w <workspace_id> -u[/dim]")
            return 1
        workspace_ids = [(ws_id, data.get("name", "")) for ws_id, data in all_resources.items()]
    elif workspace_input:
        # 指定的工作空间
        if workspace_input.startswith("ws-"):
            workspace_id = workspace_input
            ws_resources = get_workspace_resources(workspace_id)
            ws_name = ws_resources.get("name", "") if ws_resources else ""
        else:
            workspace_id = find_workspace_by_name(workspace_input)
            if workspace_id:
                ws_resources = get_workspace_resources(workspace_id)
                ws_name = ws_resources.get("name", "") if ws_resources else workspace_input
            else:
                display.print_error(f"未找到名称为 '{workspace_input}' 的工作空间")
                display.print("[dim]使用 qzcli res --list 查看已缓存的工作空间[/dim]")
                return 1
        workspace_ids = [(workspace_id, ws_name)]
    else:
        # 使用默认工作空间
        default_ws = cookie_data.get("workspace_id", "")
        if not default_ws:
            display.print_error("请指定工作空间: qzcli ls -c -w <名称或ID>")
            display.print("[dim]或使用 --all-ws 查询所有已缓存的工作空间[/dim]")
            return 1
        ws_resources = get_workspace_resources(default_ws)
        ws_name = ws_resources.get("name", "") if ws_resources else ""
        workspace_ids = [(default_ws, ws_name)]
    
    all_jobs = []
    only_interactive = getattr(args, 'only_interactive', False)
    include_interactive = getattr(args, 'include_interactive', False) or only_interactive
    all_users = getattr(args, 'all_users', False)

    # 获取当前用户 ID（用于开发机过滤）
    current_user_id = None
    if include_interactive and not all_users:
        config = load_config()
        current_user_id = config.get("user_id", "")
        if not current_user_id:
            # 首次：从 train_job/list 获取用户 ID 并缓存
            try:
                probe = api.list_jobs_with_cookie(workspace_ids[0][0], cookie, page_size=1)
                probe_jobs = probe.get("jobs", [])
                if probe_jobs:
                    created_by = probe_jobs[0].get("created_by") or {}
                    current_user_id = created_by.get("id", "")
                    if current_user_id:
                        config["user_id"] = current_user_id
                        save_config(config)
            except QzAPIError:
                pass

    for workspace_id, ws_name in workspace_ids:
        try:
            if len(workspace_ids) > 1:
                display.print(f"[dim]正在获取 {ws_name or workspace_id} 的任务...[/dim]")
            else:
                display.print(f"[dim]正在从 API 获取任务列表...[/dim]")

            # 获取训练任务（除非 --only-interactive）
            if not only_interactive:
                result = api.list_jobs_with_cookie(
                    workspace_id,
                    cookie,
                    page_size=args.limit * 2 if args.running else args.limit
                )

                jobs_data = result.get("jobs", [])

                # 转换为 JobRecord 格式
                for job_data in jobs_data:
                    job = JobRecord.from_api_response(job_data, source="api_cookie")
                    # 添加工作空间名称
                    if ws_name:
                        job.metadata["workspace_name"] = ws_name
                    all_jobs.append(job)

            # 获取交互式建模实例（开发机）
            if include_interactive:
                try:
                    # 通过 notebook/list API 过滤
                    user_ids = [current_user_id] if current_user_id and not all_users else []
                    status_filter = ["RUNNING"] if args.running else []
                    nb_result = api.list_notebooks_with_cookie(
                        workspace_id, cookie,
                        page_size=args.limit,
                        user_ids=user_ids,
                        status=status_filter,
                    )
                    for nb_data in nb_result.get("list", []):
                        job = JobRecord.from_notebook_response(nb_data, workspace_id=workspace_id, workspace_name=ws_name)
                        all_jobs.append(job)
                except QzAPIError as e:
                    if only_interactive:
                        raise
                    display.print_warning(f"获取 {ws_name or workspace_id} 的开发机列表失败: {e}")

        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>")
                return 1
            display.print_warning(f"获取 {ws_name or workspace_id} 失败: {e}")
            continue
    
    if not all_jobs:
        display.print("[dim]暂无任务[/dim]")
        return 0
    
    # 按创建时间排序
    all_jobs.sort(key=lambda x: x.created_at or "", reverse=True)
    
    # 过滤状态
    if args.status:
        all_jobs = [j for j in all_jobs if args.status.lower() in j.status.lower()]
    
    # 过滤运行中的任务
    if args.running:
        active_statuses = {"job_running", "job_queuing", "job_pending", "running", "queuing", "pending"}
        all_jobs = [
            j for j in all_jobs
            if j.status.lower() in active_statuses or "running" in j.status.lower() or "queue" in j.status.lower()
        ]
    
    # 限制数量
    all_jobs = all_jobs[:args.limit]
    
    if not all_jobs:
        display.print("[dim]暂无符合条件的任务[/dim]")
        return 0
    
    # 显示标题
    if len(workspace_ids) == 1:
        ws_name = workspace_ids[0][1]
        if ws_name:
            display.print(f"\n[bold]工作空间: {ws_name}[/bold]\n")
    
    # 复用现有显示函数
    if args.wide and not args.compact:
        display.print_jobs_wide(all_jobs)
    else:
        display.print_jobs_table(all_jobs, show_command=args.verbose, show_url=args.url)
    
    return 0


def cmd_list(args):
    """列出任务"""
    # Cookie 模式：从 API 获取任务
    if args.cookie:
        return cmd_list_cookie(args)
    
    display = get_display()
    store = get_store()
    api = get_api()
    
    # 获取本地存储的任务
    # 如果使用 --running，先获取更多任务再过滤
    fetch_limit = args.limit * 3 if args.running else args.limit
    jobs = store.list(limit=fetch_limit, status=args.status)
    
    if not jobs:
        display.print("[dim]暂无任务记录，使用 qzcli import 导入或 qzcli track 添加任务[/dim]")
        return 0
    
    # 更新任务状态
    if not args.no_refresh:
        display.print("[dim]正在更新任务状态...[/dim]")
        
        # 只更新非终态任务
        job_ids_to_update = [
            j.job_id for j in jobs
            if j.status not in ("job_succeeded", "job_failed", "job_stopped")
        ]
        
        if job_ids_to_update:
            try:
                results = api.get_jobs_detail(job_ids_to_update)
                for job_id, data in results.items():
                    if "error" not in data:
                        store.update_from_api(job_id, data)
            except QzAPIError as e:
                display.print_warning(f"部分任务状态更新失败: {e}")
        
        # 重新获取更新后的列表
        jobs = store.list(limit=fetch_limit, status=args.status)
    
    # 过滤：只显示运行中/排队中的任务
    if args.running:
        active_statuses = {"job_running", "job_queuing", "job_pending", "running", "queuing", "pending"}
        jobs = [
            j for j in jobs
            if j.status.lower() in active_statuses or "running" in j.status.lower() or "queue" in j.status.lower()
        ]
        # 应用 limit
        jobs = jobs[:args.limit]
        
        if not jobs:
            display.print("[dim]暂无运行中的任务[/dim]")
            return 0
    
    if args.wide and not args.compact:
        display.print_jobs_wide(jobs)
    else:
        display.print_jobs_table(jobs, show_command=args.verbose, show_url=args.url)
    return 0


def cmd_status(args):
    """查看任务状态"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    job_id = args.job_id
    
    # 从 API 获取最新状态
    try:
        api_data = api.get_job_detail(job_id)
        job = store.update_from_api(job_id, api_data)
        display.print_job_detail(job, api_data)
        
        if args.json:
            import json
            print(json.dumps(api_data, indent=2, ensure_ascii=False))
        
        return 0
    except QzAPIError as e:
        display.print_error(f"查询失败: {e}")
        return 1


def cmd_stop(args):
    """停止任务"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    job_id = args.job_id
    
    # 确认
    if not args.yes:
        confirm = input(f"确定要停止任务 {job_id}? [y/N] ").strip().lower()
        if confirm != "y":
            display.print("已取消")
            return 0
    
    try:
        if api.stop_job(job_id):
            display.print_success(f"任务 {job_id} 已停止")
            # 更新本地状态
            store.update(job_id, status="job_stopped")
            return 0
        else:
            display.print_error("停止任务失败")
            return 1
    except QzAPIError as e:
        display.print_error(f"停止任务失败: {e}")
        return 1


def cmd_watch(args):
    """实时监控任务状态"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    interval = args.interval
    
    display.print(f"[bold]实时监控模式[/bold] (每 {interval} 秒刷新，按 Ctrl+C 退出)")
    display.print("")
    
    try:
        while True:
            # 获取所有非终态任务
            jobs = store.list()
            active_jobs = [
                j for j in jobs
                if j.status not in ("job_succeeded", "job_failed", "job_stopped")
            ]
            
            # 更新状态
            if active_jobs:
                job_ids = [j.job_id for j in active_jobs]
                try:
                    results = api.get_jobs_detail(job_ids)
                    for job_id, data in results.items():
                        if "error" not in data:
                            store.update_from_api(job_id, data)
                except QzAPIError:
                    pass
            
            # 清屏并显示
            print("\033[2J\033[H", end="")  # 清屏
            
            jobs = store.list(limit=args.limit)
            display.print_jobs_table(
                jobs,
                title=f"启智平台任务监控 (每 {interval}s 刷新)"
            )
            
            # 检查是否还有活跃任务
            active_count = sum(
                1 for j in jobs
                if j.status not in ("job_succeeded", "job_failed", "job_stopped")
            )
            
            if active_count == 0 and not args.keep_alive:
                display.print("\n[green]所有任务已完成[/green]")
                break
            
            time.sleep(interval)
    
    except KeyboardInterrupt:
        display.print("\n[dim]监控已停止[/dim]")
    
    return 0


def cmd_track(args):
    """追踪任务（供脚本调用）"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    job_id = args.job_id
    
    # 尝试从 API 获取详情
    try:
        api_data = api.get_job_detail(job_id)
        job = JobRecord.from_api_response(api_data, source=args.source or "")
    except QzAPIError:
        # API 失败时创建最小记录
        job = JobRecord(
            job_id=job_id,
            name=args.name or "",
            source=args.source or "",
            workspace_id=args.workspace or "",
        )
    
    # 更新元数据
    if args.name:
        job.name = args.name
    if args.source:
        job.source = args.source
    if args.workspace:
        job.workspace_id = args.workspace
    
    store.add(job)
    
    if not args.quiet:
        display.print_success(f"已追踪任务: {job_id}")
    
    return 0


def cmd_import(args):
    """从文件导入任务"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    filepath = Path(args.file)
    if not filepath.exists():
        display.print_error(f"文件不存在: {filepath}")
        return 1
    
    count = store.import_from_file(filepath, source=args.source or filepath.name)
    display.print_success(f"已导入 {count} 个任务")
    
    # 可选：更新导入任务的状态
    if args.refresh and count > 0:
        display.print("正在更新任务状态...")
        jobs = store.list()
        job_ids = [j.job_id for j in jobs if not j.status or j.status == "unknown"]
        
        if job_ids:
            try:
                results = api.get_jobs_detail(job_ids[:50])  # 最多更新 50 个
                updated = 0
                for job_id, data in results.items():
                    if "error" not in data:
                        store.update_from_api(job_id, data)
                        updated += 1
                display.print_success(f"已更新 {updated} 个任务状态")
            except QzAPIError as e:
                display.print_warning(f"状态更新失败: {e}")
    
    return 0


def cmd_remove(args):
    """删除任务记录"""
    display = get_display()
    store = get_store()
    
    job_id = args.job_id
    
    if not args.yes:
        confirm = input(f"确定要删除任务记录 {job_id}? [y/N] ").strip().lower()
        if confirm != "y":
            display.print("已取消")
            return 0
    
    if store.remove(job_id):
        display.print_success(f"已删除任务记录: {job_id}")
        return 0
    else:
        display.print_error(f"任务不存在: {job_id}")
        return 1


def cmd_clear(args):
    """清空所有任务记录"""
    display = get_display()
    store = get_store()
    
    count = store.count()
    
    if count == 0:
        display.print("暂无任务记录")
        return 0
    
    if not args.yes:
        confirm = input(f"确定要清空所有 {count} 个任务记录? [y/N] ").strip().lower()
        if confirm != "y":
            display.print("已取消")
            return 0
    
    store.clear()
    display.print_success(f"已清空 {count} 个任务记录")
    return 0


def cmd_cookie(args):
    """设置浏览器 cookie"""
    display = get_display()
    
    if args.clear:
        clear_cookie()
        display.print_success("已清除 cookie")
        return 0
    
    if args.show:
        cookie_data = get_cookie()
        if cookie_data:
            display.print(f"Workspace: {cookie_data.get('workspace_id', 'N/A')}")
            display.print(f"Cookie: {cookie_data.get('cookie', '')[:80]}...")
        else:
            display.print("[dim]未设置 cookie[/dim]")
        return 0
    
    cookie = args.cookie
    workspace_id = args.workspace or ""
    
    # 支持从文件读取 cookie
    if args.file:
        filepath = Path(args.file)
        if not filepath.exists():
            display.print_error(f"文件不存在: {filepath}")
            return 1
        with open(filepath, "r") as f:
            lines = f.readlines()
            # 取最后一个非空行作为 cookie
            for line in reversed(lines):
                line = line.strip()
                if line and not line.startswith("#") and line != "cookie":
                    cookie = line
                    break
        if not cookie:
            display.print_error("文件中未找到有效的 cookie")
            return 1
        display.print(f"[dim]从文件读取 cookie: {filepath}[/dim]")
    
    if not cookie:
        display.print("请输入浏览器 cookie（从 F12 Network 中复制）:")
        display.print("[dim]提示: 在 qz.sii.edu.cn 页面按 F12 -> Console -> 输入 document.cookie[/dim]")
        cookie = input().strip()
    
    if not cookie:
        display.print_error("cookie 不能为空")
        return 1
    
    # 测试 cookie 是否有效（使用 /openapi/v1/train_job/list 端点）
    if not args.no_test and workspace_id:
        display.print("正在验证 cookie...")
        api = get_api()
        try:
            result = api.list_jobs_with_cookie(workspace_id, cookie, page_size=1)
            total = result.get("total", 0)
            display.print_success(f"Cookie 有效！工作空间内有 {total} 个任务")
        except QzAPIError as e:
            display.print_error(f"Cookie 无效: {e}")
            return 1
    
    save_cookie(cookie, workspace_id)
    display.print_success("Cookie 已保存")
    return 0


def cmd_workspaces(args):
    """从历史任务中提取工作空间和资源配置（支持本地缓存）"""
    display = get_display()
    api = get_api()
    
    # 如果是列出所有已缓存的工作空间
    if args.list:
        cached = list_cached_workspaces()
        if not cached:
            display.print("[dim]暂无已缓存的工作空间，使用 qzcli res -w <workspace_id> 添加[/dim]")
            return 0
        
        display.print(f"\n[bold]已缓存的工作空间 ({len(cached)} 个)[/bold]\n")
        for ws in cached:
            name = ws.get("name") or "[未命名]"
            import datetime
            updated = datetime.datetime.fromtimestamp(ws.get("updated_at", 0)).strftime("%Y-%m-%d %H:%M")
            display.print(f"  [bold]{name}[/bold]")
            display.print(f"    ID: [cyan]{ws['id']}[/cyan]")
            display.print(f"    资源: {ws['project_count']} 项目, {ws['compute_group_count']} 计算组, {ws['spec_count']} 规格")
            display.print(f"    更新: {updated}")
            display.print("")
        
        display.print("[dim]使用方法:[/dim]")
        display.print("  qzcli res -w <名称或ID>      # 查看资源")
        display.print("  qzcli res -w <ID> -u         # 更新缓存")
        display.print("  qzcli res -w <ID> --name 别名  # 设置名称")
        return 0
    
    # 如果只设置名称（没有 -u 参数）
    if hasattr(args, 'name') and args.name and not args.update:
        workspace_id = args.workspace
        if not workspace_id:
            display.print_error("请指定工作空间 ID: qzcli res -w <workspace_id> --name <名称>")
            return 1
        set_workspace_name(workspace_id, args.name)
        display.print_success(f"已设置工作空间名称: {args.name}")
        return 0
    
    # 记录要设置的名称（如果有）
    pending_name = args.name if hasattr(args, 'name') else None
    
    # 解析 workspace 参数（支持名称或 ID）
    workspace_input = args.workspace
    cookie_data = get_cookie()
    
    # 如果使用 -u 但没有指定工作空间，自动发现所有可访问的工作空间
    if args.update and not workspace_input:
        if not cookie_data or not cookie_data.get("cookie"):
            display.print_error("未设置 cookie，请先运行: qzcli login")
            return 1
        
        cookie = cookie_data["cookie"]
        display.print("[dim]正在获取可访问的工作空间列表...[/dim]")
        
        try:
            workspaces = api.list_workspaces(cookie)
            if not workspaces:
                display.print_warning("未找到可访问的工作空间")
                return 0
            
            display.print(f"\n[bold]发现 {len(workspaces)} 个可访问的工作空间[/bold]\n")
            
            # 更新每个工作空间
            for ws in workspaces:
                ws_id = ws.get("id")
                ws_name = ws.get("name", "")
                display.print(f"[dim]正在更新 {ws_name or ws_id}...[/dim]")
                
                try:
                    # 1. 从任务历史提取项目和规格信息
                    result = api.list_jobs_with_cookie(ws_id, cookie, page_size=200)
                    jobs = result.get("jobs", [])
                    resources = api.extract_resources_from_jobs(jobs)
                    
                    # 2. 使用 cluster_basic_info API 获取完整的计算组列表
                    try:
                        cluster_info = api.get_cluster_basic_info(ws_id, cookie)
                        compute_groups_from_api = []
                        
                        for cg in cluster_info.get("compute_groups", []):
                            for lcg in cg.get("logic_compute_groups", []):
                                lcg_id = lcg.get("logic_compute_group_id", "")
                                lcg_name = lcg.get("logic_compute_group_name", "")
                                brand = lcg.get("brand", "")
                                resource_types = lcg.get("resource_types", [])
                                gpu_type = resource_types[0] if resource_types else ""
                                
                                if lcg_id:
                                    compute_groups_from_api.append({
                                        "id": lcg_id,
                                        "name": lcg_name,
                                        "gpu_type": brand or gpu_type,
                                        "workspace_id": ws_id,
                                    })
                        
                        # 合并计算组（API 获取的优先）
                        if compute_groups_from_api:
                            resources["compute_groups"] = compute_groups_from_api
                    except Exception:
                        pass  # 获取计算组失败不影响其他资源
                    
                    # 保存到本地缓存
                    save_resources(ws_id, resources, ws_name)
                    
                    projects_count = len(resources.get("projects", []))
                    cg_count = len(resources.get("compute_groups", []))
                    display.print(f"  ✓ {ws_name or ws_id}: {projects_count} 项目, {cg_count} 计算组")
                except Exception as e:
                    display.print_warning(f"  ✗ {ws_name or ws_id}: {e}")
            
            display.print("")
            display.print_success("工作空间缓存更新完成！")
            display.print("[dim]使用 qzcli res --list 查看所有已缓存的工作空间[/dim]")
            return 0
            
        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新登录: qzcli login")
            else:
                display.print_error(f"获取工作空间列表失败: {e}")
            return 1
    
    if not workspace_input:
        workspace_id = cookie_data.get("workspace_id", "") if cookie_data else ""
    elif workspace_input.startswith("ws-"):
        workspace_id = workspace_input
    else:
        # 尝试通过名称查找
        workspace_id = find_workspace_by_name(workspace_input)
        if workspace_id:
            display.print(f"[dim]匹配到工作空间: {workspace_input} -> {workspace_id}[/dim]")
        else:
            display.print_error(f"未找到名称为 '{workspace_input}' 的工作空间")
            display.print("[dim]使用 qzcli res --list 查看已缓存的工作空间[/dim]")
            return 1
    
    if not workspace_id:
        display.print_error("请指定工作空间: qzcli res -w <名称或ID>")
        display.print("[dim]使用 qzcli res --list 查看已缓存的工作空间[/dim]")
        return 1
    
    # 检查是否需要从 API 更新
    cached_resources = get_workspace_resources(workspace_id)
    use_cache = cached_resources and not args.update
    
    if use_cache:
        # 使用缓存
        import datetime
        updated = datetime.datetime.fromtimestamp(cached_resources.get("updated_at", 0)).strftime("%Y-%m-%d %H:%M")
        ws_name = cached_resources.get("name", "")
        title = f"资源配置"
        if ws_name:
            title += f" [{ws_name}]"
        title += f" (缓存于 {updated})"
        
        display.print(f"\n[bold]{title}[/bold]")
        display.print(f"[dim]工作空间: {workspace_id}[/dim]\n")
        
        # 转换缓存格式为列表格式
        projects = list(cached_resources.get("projects", {}).values())
        compute_groups = list(cached_resources.get("compute_groups", {}).values())
        specs = list(cached_resources.get("specs", {}).values())
    else:
        # 从 API 获取
        if not cookie_data or not cookie_data.get("cookie"):
            display.print_error("未设置 cookie，请先运行: qzcli cookie -f cookies.txt")
            display.print("[dim]提示: 从浏览器 F12 获取 cookie[/dim]")
            return 1
        
        cookie = cookie_data["cookie"]
        
        try:
            display.print("[dim]正在从历史任务中提取资源配置...[/dim]")
            
            # 获取任务列表
            result = api.list_jobs_with_cookie(workspace_id, cookie, page_size=200)
            jobs = result.get("jobs", [])
            total = result.get("total", 0)
            
            if not jobs:
                display.print("[dim]未找到自己的任务，尝试从工作空间任务获取资源信息...[/dim]")
                
                projects_found = {}
                compute_groups_found = {}
                gpu_types_found = {}  # 从节点获取 GPU 类型
                
                # 1. 先从 list_task_dimension 获取项目信息（包含所有用户的任务）
                try:
                    task_data = api.list_task_dimension(workspace_id, cookie, page_size=200)
                    tasks = task_data.get("task_dimensions", [])
                    
                    for task in tasks:
                        # 提取项目
                        proj = task.get("project", {})
                        proj_id = proj.get("id", "")
                        proj_name = proj.get("name", "")
                        if proj_id and proj_id not in projects_found:
                            projects_found[proj_id] = {
                                "id": proj_id,
                                "name": proj_name,
                                "workspace_id": workspace_id,
                            }
                except QzAPIError:
                    pass
                
                # 2. 从 list_node_dimension 获取计算组和 GPU 类型信息
                try:
                    node_data = api.list_node_dimension(workspace_id, cookie, page_size=500)
                    nodes = node_data.get("node_dimensions", [])
                    
                    for node in nodes:
                        # 尝试从 logic_compute_group 获取计算组
                        lcg_info = node.get("logic_compute_group", {})
                        lcg_id = lcg_info.get("id", "")
                        lcg_name = lcg_info.get("name", "")
                        if lcg_id and lcg_id not in compute_groups_found:
                            gpu_info = node.get("gpu_info", {})
                            gpu_type = gpu_info.get("gpu_product_simple", "")
                            compute_groups_found[lcg_id] = {
                                "id": lcg_id,
                                "name": lcg_name,
                                "gpu_type": gpu_type,
                                "workspace_id": workspace_id,
                            }
                        
                        # 收集 GPU 类型信息
                        gpu_info = node.get("gpu_info", {})
                        gpu_type = gpu_info.get("gpu_product_simple", "")
                        if gpu_type and gpu_type not in gpu_types_found:
                            gpu_types_found[gpu_type] = {
                                "type": gpu_type,
                                "display": gpu_info.get("gpu_type_display", ""),
                                "memory_gb": gpu_info.get("gpu_memory_size_gb", 0),
                            }
                except QzAPIError:
                    pass
                
                # 构建资源数据
                resources = {
                    "projects": list(projects_found.values()),
                    "compute_groups": list(compute_groups_found.values()),
                    "specs": [],
                }
                
                # 保存到本地缓存
                ws_name = pending_name or ""
                save_resources(workspace_id, resources, ws_name)
                
                # 显示结果
                display.print_success("已添加工作空间到缓存")
                
                if projects_found:
                    display.print(f"\n[bold]项目 ({len(projects_found)} 个)[/bold]")
                    for proj in projects_found.values():
                        display.print(f"  - {proj['name']}")
                        display.print(f"    [cyan]{proj['id']}[/cyan]")
                
                if compute_groups_found:
                    display.print(f"\n[bold]计算组 ({len(compute_groups_found)} 个)[/bold]")
                    for cg in compute_groups_found.values():
                        display.print(f"  - {cg['name']} [{cg['gpu_type']}]")
                        display.print(f"    [cyan]{cg['id']}[/cyan]")
                
                if gpu_types_found:
                    display.print(f"\n[bold]可用 GPU 类型 ({len(gpu_types_found)} 种)[/bold]")
                    for gt in gpu_types_found.values():
                        display.print(f"  - {gt['type']} ({gt['display']}, {gt['memory_gb']}GB)")
                
                if not projects_found and not compute_groups_found:
                    display.print("[dim]未发现项目或计算组信息[/dim]")
                
                return 0
            
            # 提取资源信息
            resources = api.extract_resources_from_jobs(jobs)
            
            # 保存到本地缓存
            ws_name = pending_name or (cached_resources.get("name", "") if cached_resources else "")
            save_resources(workspace_id, resources, ws_name)
            display.print_success("资源配置已保存到本地缓存")
            
            display.print(f"\n[bold]资源配置（从 {len(jobs)}/{total} 个任务中提取）[/bold]")
            display.print(f"[dim]工作空间: {workspace_id}[/dim]\n")
            
            projects = resources.get("projects", [])
            compute_groups = resources.get("compute_groups", [])
            specs = resources.get("specs", [])
            
        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>")
            else:
                display.print_error(f"获取失败: {e}")
            return 1
    
    # 显示项目
    if projects:
        display.print(f"[bold]项目 ({len(projects)} 个)[/bold]")
        for proj in projects:
            display.print(f"  - {proj['name']}")
            display.print(f"    [cyan]{proj['id']}[/cyan]")
        display.print("")
    
    # 显示计算组
    if compute_groups:
        display.print(f"[bold]计算组 ({len(compute_groups)} 个)[/bold]")
        for group in compute_groups:
            gpu_type = group.get("gpu_type", "")
            gpu_display = group.get("gpu_type_display", "")
            display.print(f"  - {group['name']} [{gpu_type}]")
            if gpu_display:
                display.print(f"    [dim]{gpu_display}[/dim]")
            display.print(f"    [cyan]{group['id']}[/cyan]")
        display.print("")
    
    # 显示规格
    if specs:
        display.print(f"[bold]GPU 规格 ({len(specs)} 个)[/bold]")
        for spec in specs:
            gpu_type = spec.get("gpu_type", "")
            gpu_count = spec.get("gpu_count", 0)
            cpu_count = spec.get("cpu_count", 0)
            mem_gb = spec.get("memory_gb", 0)
            display.print(f"  - {gpu_count}x {gpu_type} + {cpu_count}核CPU + {mem_gb}GB内存")
            display.print(f"    [cyan]{spec['id']}[/cyan]")
        display.print("")
    
    # 导出格式
    if args.export:
        display.print("[bold]导出格式（可用于 shell 脚本）:[/bold]")
        display.print(f'WORKSPACE_ID="{workspace_id}"')
        if projects:
            display.print(f'PROJECT_ID="{projects[0]["id"]}"  # {projects[0]["name"]}')
        if compute_groups:
            for group in compute_groups:
                display.print(f'# {group["name"]} [{group.get("gpu_type", "")}]')
                display.print(f'LOGIC_COMPUTE_GROUP_ID="{group["id"]}"')
        if specs:
            for spec in specs:
                display.print(f'# {spec.get("gpu_count", 0)}x {spec.get("gpu_type", "")}')
                display.print(f'SPEC_ID="{spec["id"]}"')
    
    return 0


def cmd_resources(args):
    """列出工作空间内可用的计算资源（cmd_workspaces 的别名）"""
    # 直接调用 workspaces 命令
    return cmd_workspaces(args)


def cmd_avail(args):
    """查询计算组空余节点，帮助决定任务应该提交到哪里"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未设置 cookie，请先运行: qzcli cookie -f cookies.txt")
        return 1
    
    cookie = cookie_data["cookie"]
    
    # 解析 workspace 参数（支持名称或 ID）
    workspace_input = args.workspace
    
    # 如果不指定 workspace，查询所有已缓存的工作空间
    if not workspace_input:
        all_resources = load_all_resources()
        if not all_resources:
            display.print_error("没有已缓存的工作空间")
            display.print("[dim]请先运行: qzcli res -w <workspace_id> -u[/dim]")
            return 1
        workspace_ids = list(all_resources.keys())
    elif workspace_input.startswith("ws-"):
        workspace_ids = [workspace_input]
    else:
        workspace_id = find_workspace_by_name(workspace_input)
        if workspace_id:
            workspace_ids = [workspace_id]
            display.print(f"[dim]匹配到工作空间: {workspace_input} -> {workspace_id}[/dim]")
        else:
            display.print_error(f"未找到名称为 '{workspace_input}' 的工作空间")
            display.print("[dim]使用 qzcli res --list 查看已缓存的工作空间[/dim]")
            return 1
    
    required_nodes = args.nodes
    group_filter = args.group
    all_results = []  # 所有工作空间的结果汇总
    
    from collections import defaultdict
    
    for workspace_id in workspace_ids:
        # 获取计算组列表（从缓存）
        cached_resources = get_workspace_resources(workspace_id)
        if not cached_resources:
            display.print_warning(f"未缓存工作空间 {workspace_id} 的资源信息，跳过")
            continue
        
        compute_groups = cached_resources.get("compute_groups", {})
        specs = cached_resources.get("specs", {})
        ws_name = cached_resources.get("name", "") or workspace_id
        
        # 如果指定了特定计算组
        if group_filter:
            if group_filter.startswith("lcg-"):
                if group_filter in compute_groups:
                    compute_groups = {group_filter: compute_groups[group_filter]}
                else:
                    continue  # 该工作空间没有这个计算组
            else:
                found = find_resource_by_name(workspace_id, "compute_groups", group_filter)
                if found:
                    compute_groups = {found["id"]: found}
                else:
                    continue
        
        if not compute_groups:
            continue
        
        display.print(f"[dim]正在查询 {ws_name} 的 {len(compute_groups)} 个计算组...[/dim]")
        
        # 低优任务统计（仅在 --lp 参数启用时计算）
        node_low_priority_gpu = defaultdict(int)  # node_name -> low_priority_gpu_count
        
        if args.low_priority:
            display.print(f"[dim]正在获取低优任务数据（这可能较慢）...[/dim]")
            low_priority_threshold = 3  # 优先级 <= 3 为低优任务
            
            try:
                # 分页获取所有任务
                tasks = []
                page_num = 1
                while True:
                    task_data = api.list_task_dimension(workspace_id, cookie, page_num=page_num, page_size=200)
                    page_tasks = task_data.get("task_dimensions", [])
                    tasks.extend(page_tasks)
                    if len(tasks) >= task_data.get("total", 0) or not page_tasks:
                        break
                    page_num += 1
                
                # 统计每个节点上低优任务占用的 GPU 数
                for task in tasks:
                    priority = task.get("priority", 10)
                    if priority <= low_priority_threshold:
                        gpu_total = task.get("gpu", {}).get("total", 0)
                        nodes_occupied = task.get("nodes_occupied", {}).get("nodes", [])
                        # 平均分配 GPU 到各节点（多节点任务）
                        gpu_per_node = gpu_total // len(nodes_occupied) if nodes_occupied else 0
                        for node_name in nodes_occupied:
                            node_low_priority_gpu[node_name] += gpu_per_node if len(nodes_occupied) > 1 else gpu_total
            except QzAPIError:
                pass  # 获取任务数据失败不影响主要功能
        
        try:
            for lcg_id, lcg_info in compute_groups.items():
                lcg_name = lcg_info.get("name", lcg_id)
                gpu_type = lcg_info.get("gpu_type", "")
                
                try:
                    data = api.list_node_dimension(workspace_id, cookie, lcg_id, page_size=1000)
                    nodes = data.get("node_dimensions", [])
                    total_nodes = len(nodes)
                    
                    # 统计空闲节点（GPU 使用数为 0）和空闲 GPU 分布
                    free_nodes = []
                    low_priority_free_nodes = []  # 低优空余节点
                    gpu_free_distribution = {}  # free_gpu_count -> node_count
                    total_free_gpus = 0
                    total_gpus = 0
                    
                    for node in nodes:
                        node_name = node.get("name", "")
                        node_status = node.get("status", "")
                        cordon_type = node.get("cordon_type", "")
                        gpu_info = node.get("gpu", {})
                        gpu_used = gpu_info.get("used", 0)
                        gpu_total = gpu_info.get("total", 0)
                        
                        # 跳过异常节点（gpu_total=0 但有任务在跑，可能是故障节点）
                        if gpu_total == 0:
                            continue
                        
                        # 判断节点是否可调度
                        # - 状态必须是 Ready
                        # - 不能有 cordon 标记（hardware-fault, software-fault 等）
                        is_schedulable = (node_status == "Ready" and not cordon_type)
                        
                        gpu_free = max(0, gpu_total - gpu_used)  # 避免负数
                        
                        total_gpus += gpu_total
                        
                        # 只有可调度节点的空闲 GPU 才计入统计
                        if is_schedulable:
                            total_free_gpus += gpu_free
                            
                            # 统计空闲 GPU 分布
                            if gpu_free > 0:
                                gpu_free_distribution[gpu_free] = gpu_free_distribution.get(gpu_free, 0) + 1
                            
                            if gpu_used == 0 and gpu_total > 0:
                                free_nodes.append({
                                    "name": node_name,
                                    "gpu_total": gpu_total,
                                })
                            
                            # 检查是否为低优空余节点（低优任务占满整节点）
                            low_priority_gpu = node_low_priority_gpu.get(node_name, 0)
                            if low_priority_gpu >= gpu_total and gpu_used > 0:
                                low_priority_free_nodes.append({
                                    "name": node_name,
                                    "low_priority_gpu": low_priority_gpu,
                                    "gpu_total": gpu_total,
                                })
                    
                    all_results.append({
                        "workspace_id": workspace_id,
                        "workspace_name": ws_name,
                        "id": lcg_id,
                        "name": lcg_name,
                        "gpu_type": gpu_type,
                        "total_nodes": total_nodes,
                        "free_nodes": len(free_nodes),
                        "free_node_list": free_nodes,
                        "low_priority_free_nodes": len(low_priority_free_nodes),
                        "low_priority_free_node_list": low_priority_free_nodes,
                        "total_gpus": total_gpus,
                        "total_free_gpus": total_free_gpus,
                        "gpu_free_distribution": gpu_free_distribution,
                        "specs": specs,
                    })
                except QzAPIError as e:
                    display.print_warning(f"查询 {lcg_name} 失败: {e}")
                    continue
        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>")
                return 1
            display.print_warning(f"查询 {ws_name} 失败: {e}")
            continue
    
    if not all_results:
        display.print_error("未能获取任何计算组的节点信息")
        return 1
    
    display.print(f"\n[bold]空余节点汇总[/bold]\n")
    
    # 如果指定了节点需求，过滤并推荐
    if required_nodes:
        # 按空闲节点数降序排序
        if args.low_priority:
            # 考虑低优空余
            all_results.sort(key=lambda x: (x["free_nodes"] + x.get("low_priority_free_nodes", 0), x["free_nodes"]), reverse=True)
            available = [r for r in all_results if r["free_nodes"] + r.get("low_priority_free_nodes", 0) >= required_nodes]
        else:
            all_results.sort(key=lambda x: x["free_nodes"], reverse=True)
            available = [r for r in all_results if r["free_nodes"] >= required_nodes]
        
        if not available:
            if args.low_priority:
                display.print(f"[red]没有计算组有 >= {required_nodes} 个可用节点（空闲+低优空余）[/red]\n")
            else:
                display.print(f"[red]没有计算组有 >= {required_nodes} 个空闲节点[/red]\n")
            display.print("当前各计算组节点情况：")
            for r in all_results:
                if args.low_priority:
                    lp_free = r.get('low_priority_free_nodes', 0)
                    display.print(f"  [{r['workspace_name']}] {r['name']}: {r['free_nodes']} 空节点 + {lp_free} 低优空余 [{r['gpu_type']}]")
                else:
                    display.print(f"  [{r['workspace_name']}] {r['name']}: {r['free_nodes']} 空节点 [{r['gpu_type']}]")
            return 1
        
        display.print(f"需要 {required_nodes} 个节点，以下计算组可用：\n")
        
        for r in available:
            if args.low_priority:
                lp_free = r.get('low_priority_free_nodes', 0)
                total_avail = r['free_nodes'] + lp_free
                display.print(f"[green]✓[/green] [{r['workspace_name']}] [bold]{r['name']}[/bold]  {r['free_nodes']} 空节点 + {lp_free} 低优空余 = {total_avail} 可用 [{r['gpu_type']}]")
            else:
                display.print(f"[green]✓[/green] [{r['workspace_name']}] [bold]{r['name']}[/bold]  {r['free_nodes']} 空节点 [{r['gpu_type']}]")
            display.print(f"  [cyan]{r['id']}[/cyan]")
            # 显示空闲节点列表
            if args.verbose and r.get('free_node_list'):
                node_names = [n['name'] for n in r['free_node_list']]
                display.print(f"  [dim]空闲节点: {', '.join(node_names)}[/dim]")
            if args.verbose and args.low_priority and r.get('low_priority_free_node_list'):
                lp_node_names = [n['name'] for n in r['low_priority_free_node_list']]
                display.print(f"  [dim]低优空余: {', '.join(lp_node_names)}[/dim]")
        
        # 导出格式
        if args.export:
            display.print("")
            best = available[0]
            display.print(f"# 推荐: [{best['workspace_name']}] {best['name']} ({best['free_nodes']} 空节点)")
            display.print(f'WORKSPACE_ID="{best["workspace_id"]}"')
            display.print(f'LOGIC_COMPUTE_GROUP_ID="{best["id"]}"')
            specs = best.get("specs", {})
            if specs:
                spec = list(specs.values())[0]
                display.print(f'SPEC_ID="{spec["id"]}"  # {spec.get("gpu_count", 0)}x {spec.get("gpu_type", "")}')
    else:
        # 全分区统一大表展示
        if args.low_priority:
            sorted_results = sorted(
                all_results,
                key=lambda x: (
                    x["free_nodes"] + x.get("low_priority_free_nodes", 0),
                    x["free_nodes"],
                    x.get("total_free_gpus", 0),
                ),
                reverse=True,
            )
        else:
            sorted_results = sorted(
                all_results,
                key=lambda x: (x["free_nodes"], x.get("total_free_gpus", 0)),
                reverse=True,
            )

        workspace_order: List[str] = []
        workspace_grouped_results: dict[str, List[dict]] = {}
        for r in sorted_results:
            ws_name = r.get("workspace_name", "")
            if ws_name not in workspace_grouped_results:
                workspace_grouped_results[ws_name] = []
                workspace_order.append(ws_name)
            workspace_grouped_results[ws_name].append(r)

        grouped_results: List[dict] = []
        section_break_after_rows: List[int] = []
        row_cursor = 0
        for ws_name in workspace_order:
            ws_rows = workspace_grouped_results[ws_name]
            grouped_results.extend(ws_rows)
            row_cursor += len(ws_rows)
            if row_cursor < len(sorted_results):
                section_break_after_rows.append(row_cursor - 1)

        total_groups = len(sorted_results)
        total_free_nodes = sum(r.get("free_nodes", 0) for r in sorted_results)
        total_nodes = sum(r.get("total_nodes", 0) for r in sorted_results)
        total_free_gpus = sum(r.get("total_free_gpus", 0) for r in sorted_results)
        total_gpus = sum(r.get("total_gpus", 0) for r in sorted_results)
        total_used_gpus = max(0, total_gpus - total_free_gpus)
        total_gpu_util_ratio = _format_percent(total_used_gpus, total_gpus)

        display.print(f"[bold]全分区总览 ({total_groups} 个计算组)[/bold]")
        display.print(
            f"[dim]空节点 {total_free_nodes}/{total_nodes} | 空GPU {total_free_gpus}/{total_gpus} | GPU利用率 {total_gpu_util_ratio}[/dim]"
        )

        if RICH_TABLE_AVAILABLE and getattr(display, "console", None):
            table = Table(
                box=box.MINIMAL,
                show_header=True,
                header_style="bold",
                expand=False,
                padding=(0, 1),
            )
            table.add_column("排名", justify="right", style="dim")
            table.add_column("分区", style="cyan", overflow="fold")
            table.add_column("计算组", style="white", overflow="fold")
            table.add_column("空节点", justify="right")
            if args.low_priority:
                table.add_column("低优空余", justify="right")
                table.add_column("可用节点", justify="right")
            table.add_column("总节点", justify="right", style="dim")
            table.add_column("空GPU", justify="right")
            table.add_column("GPU利用率", justify="right")
            table.add_column("GPU类型", style="magenta", no_wrap=True)

            section_break_set = set(section_break_after_rows)
            for idx, r in enumerate(grouped_results, 1):
                free_nodes = r.get("free_nodes", 0)
                low_priority_free = r.get("low_priority_free_nodes", 0)
                total_available = free_nodes + low_priority_free
                total_gpu = r.get("total_gpus", 0)
                total_free_gpu = r.get("total_free_gpus", 0)

                free_nodes_text = f"[green]{free_nodes}[/green]" if free_nodes > 0 else "[dim]0[/dim]"
                low_priority_text = (
                    f"[yellow]{low_priority_free}[/yellow]"
                    if low_priority_free > 0
                    else "[dim]0[/dim]"
                )
                total_available_text = (
                    f"[green]{total_available}[/green]"
                    if total_available > 0
                    else "[dim]0[/dim]"
                )

                used_gpu = max(0, total_gpu - total_free_gpu)
                gpu_util_text = _format_percent(used_gpu, total_gpu)
                if total_gpu > 0:
                    gpu_util_ratio = used_gpu / total_gpu
                    if gpu_util_ratio >= 0.8:
                        gpu_util_text = f"[green]{gpu_util_text}[/green]"
                    elif gpu_util_ratio >= 0.4:
                        gpu_util_text = f"[yellow]{gpu_util_text}[/yellow]"
                    else:
                        gpu_util_text = f"[red]{gpu_util_text}[/red]"
                else:
                    gpu_util_text = "[dim]-[/dim]"

                row = [
                    str(idx),
                    r.get("workspace_name", ""),
                    r.get("name", ""),
                    free_nodes_text,
                ]
                if args.low_priority:
                    row.extend([low_priority_text, total_available_text])
                row.extend(
                    [
                        str(r.get("total_nodes", 0)),
                        f"{total_free_gpu}/{total_gpu}",
                        gpu_util_text,
                        r.get("gpu_type", "") or "-",
                    ]
                )
                table.add_row(*row, end_section=((idx - 1) in section_break_set))

            display.console.print(table)
        else:
            table_rows = []
            for idx, r in enumerate(grouped_results, 1):
                total_gpu = r.get("total_gpus", 0)
                total_free_gpu = r.get("total_free_gpus", 0)
                row = [
                    idx,
                    r.get("workspace_name", ""),
                    r.get("name", ""),
                    r.get("free_nodes", 0),
                ]
                if args.low_priority:
                    low_priority_free = r.get("low_priority_free_nodes", 0)
                    row.extend([low_priority_free, r.get("free_nodes", 0) + low_priority_free])
                row.extend(
                    [
                        r.get("total_nodes", 0),
                        f"{total_free_gpu}/{total_gpu}",
                        _format_percent(max(0, total_gpu - total_free_gpu), total_gpu),
                        r.get("gpu_type", "") or "-",
                    ]
                )
                table_rows.append(row)

            headers = ["排名", "分区", "计算组", "空节点"]
            aligns = ["right", "left", "left", "right"]
            max_widths = [4, 24, 30, 6]
            if args.low_priority:
                headers.extend(["低优空余", "可用节点"])
                aligns.extend(["right", "right"])
                max_widths.extend([8, 8])
            headers.extend(["总节点", "空GPU", "GPU利用率", "GPU类型"])
            aligns.extend(["right", "right", "right", "left"])
            max_widths.extend([6, 12, 9, 10])

            table_lines = _render_plain_table(
                headers=headers,
                rows=table_rows,
                aligns=aligns,
                max_widths=max_widths,
                section_break_after_rows=section_break_after_rows,
            )
            for line in table_lines:
                display.print(line)

        # 显示空闲 GPU 分布（-v 模式）
        if args.verbose:
            display.print("")
            display.print("[bold]详细分布[/bold]")
            has_detail = False
            for r in grouped_results:
                prefix = f"[{r.get('workspace_name', '')}] {r.get('name', '')}"
                dist = r.get("gpu_free_distribution", {})
                if dist:
                    dist_parts = []
                    for gpu_count in sorted(dist.keys(), reverse=True):
                        node_count = dist[gpu_count]
                        dist_parts.append(f"空{gpu_count}卡×{node_count}")
                    display.print(f"  [dim]{prefix}: {', '.join(dist_parts)}[/dim]")
                    has_detail = True
                if r.get("free_node_list"):
                    node_names = [n["name"] for n in r["free_node_list"]]
                    display.print(f"  [dim]{prefix} 全空节点: {', '.join(node_names)}[/dim]")
                    has_detail = True
                if args.low_priority and r.get("low_priority_free_node_list"):
                    lp_node_names = [n["name"] for n in r["low_priority_free_node_list"]]
                    display.print(f"  [dim]{prefix} 低优空余: {', '.join(lp_node_names)}[/dim]")
                    has_detail = True
            if not has_detail:
                display.print("  [dim]暂无可展示的详细分布[/dim]")
        display.print("")
        
        # 导出格式
        if args.export:
            display.print("[bold]导出格式:[/bold]")
            for r in sorted(all_results, key=lambda x: x["free_nodes"], reverse=True):
                if r['free_nodes'] > 0:
                    display.print(f"# [{r['workspace_name']}] {r['name']} ({r['free_nodes']} 空节点)")
                    display.print(f'WORKSPACE_ID="{r["workspace_id"]}"')
                    display.print(f'LOGIC_COMPUTE_GROUP_ID="{r["id"]}"')
    
    return 0


def cmd_usage(args):
    """统计工作空间的 GPU 使用分布"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未设置 cookie，请先运行: qzcli login")
        return 1
    
    cookie = cookie_data["cookie"]
    
    # 解析 workspace 参数
    workspace_input = args.workspace
    
    if not workspace_input:
        # 查询所有已缓存的工作空间
        all_resources = load_all_resources()
        if not all_resources:
            display.print_error("没有已缓存的工作空间")
            display.print("[dim]请先运行: qzcli res -u[/dim]")
            return 1
        workspace_ids = [(ws_id, data.get("name", "")) for ws_id, data in all_resources.items()]
    elif workspace_input.startswith("ws-"):
        ws_resources = get_workspace_resources(workspace_input)
        ws_name = ws_resources.get("name", "") if ws_resources else ""
        workspace_ids = [(workspace_input, ws_name)]
    else:
        workspace_id = find_workspace_by_name(workspace_input)
        if workspace_id:
            ws_resources = get_workspace_resources(workspace_id)
            ws_name = ws_resources.get("name", "") if ws_resources else workspace_input
            workspace_ids = [(workspace_id, ws_name)]
        else:
            display.print_error(f"未找到名称为 '{workspace_input}' 的工作空间")
            return 1
    
    from collections import defaultdict
    
    all_stats = []
    
    for workspace_id, ws_name in workspace_ids:
        display.print(f"[dim]正在查询 {ws_name or workspace_id}...[/dim]")
        
        try:
            # 分页获取所有任务
            tasks = []
            page_num = 1
            page_size = 200
            while True:
                data = api.list_task_dimension(workspace_id, cookie, page_num=page_num, page_size=page_size)
                page_tasks = data.get("task_dimensions", [])
                total_count = data.get("total", 0)
                tasks.extend(page_tasks)
                
                if len(tasks) >= total_count or not page_tasks:
                    break
                page_num += 1
            
            if not tasks:
                continue
            
            # 统计 GPU 分布
            gpu_distribution = defaultdict(int)  # gpu_count -> task_count
            user_gpu = defaultdict(int)  # user -> total_gpu
            project_gpu = defaultdict(int)  # project -> total_gpu
            type_stats = defaultdict(lambda: {"count": 0, "gpu": 0})  # type -> {count, gpu}
            priority_stats = defaultdict(lambda: {"count": 0, "gpu": 0})  # priority -> {count, gpu}
            total_gpu = 0
            total_tasks = len(tasks)
            
            # 任务类型中文映射
            type_names = {
                "distributed_training": "分布式训练",
                "interactive_modeling": "交互式建模",
                "inference_serving_customize": "推理服务",
                "inference_serving": "推理服务",
                "training": "训练",
            }
            
            # 提取项目信息用于更新 resources.json
            projects_found = {}
            
            for task in tasks:
                gpu_info = task.get("gpu", {})
                gpu_total = gpu_info.get("total", 0)
                user_name = task.get("user", {}).get("name", "未知")
                project_info = task.get("project", {})
                project_name = project_info.get("name", "未知")
                project_id = project_info.get("id", "")
                task_type = task.get("type", "unknown")
                priority = task.get("priority", 0)
                
                # 收集项目信息
                if project_id and project_id not in projects_found:
                    projects_found[project_id] = {
                        "id": project_id,
                        "name": project_name,
                    }
                
                gpu_distribution[gpu_total] += 1
                user_gpu[user_name] += gpu_total
                project_gpu[project_name] += gpu_total
                type_stats[task_type]["count"] += 1
                type_stats[task_type]["gpu"] += gpu_total
                priority_stats[priority]["count"] += 1
                priority_stats[priority]["gpu"] += gpu_total
                total_gpu += gpu_total
            
            # 增量更新 resources.json 中的项目列表
            if projects_found:
                new_count = update_workspace_projects(
                    workspace_id, 
                    list(projects_found.values()),
                    ws_name
                )
                if new_count > 0:
                    display.print(f"[dim]发现 {new_count} 个新项目，已更新到本地缓存[/dim]")
            
            # 通过 list_node_dimension 发现计算组
            try:
                node_data = api.list_node_dimension(workspace_id, cookie, page_size=500)
                nodes = node_data.get("node_dimensions", [])
                
                # 从节点信息中提取计算组
                compute_groups_found = {}
                for node in nodes:
                    lcg_info = node.get("logic_compute_group", {})
                    lcg_id = lcg_info.get("id", "")
                    lcg_name = lcg_info.get("name", "")
                    if lcg_id and lcg_id not in compute_groups_found:
                        # 获取 GPU 类型信息
                        gpu_info = node.get("gpu", {})
                        gpu_type = gpu_info.get("type", "")
                        compute_groups_found[lcg_id] = {
                            "id": lcg_id,
                            "name": lcg_name,
                            "gpu_type": gpu_type,
                            "workspace_id": workspace_id,
                        }
                
                if compute_groups_found:
                    new_cg_count = update_workspace_compute_groups(
                        workspace_id,
                        list(compute_groups_found.values()),
                        ws_name
                    )
                    if new_cg_count > 0:
                        display.print(f"[dim]发现 {new_cg_count} 个新计算组，已更新到本地缓存[/dim]")
            except QzAPIError:
                pass  # 忽略节点查询失败，不影响主要功能
            
            all_stats.append({
                "workspace_id": workspace_id,
                "workspace_name": ws_name,
                "total_tasks": total_tasks,
                "total_gpu": total_gpu,
                "gpu_distribution": dict(gpu_distribution),
                "user_gpu": dict(user_gpu),
                "project_gpu": dict(project_gpu),
                "type_stats": dict(type_stats),
                "type_names": type_names,
                "priority_stats": dict(priority_stats),
            })
            
        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新设置: qzcli login")
                return 1
            display.print_warning(f"查询 {ws_name or workspace_id} 失败: {e}")
            continue
    
    if not all_stats:
        display.print("[dim]暂无运行中的任务[/dim]")
        return 0
    
    # 显示结果
    for stats in all_stats:
        ws_name = stats["workspace_name"] or stats["workspace_id"]
        display.print(f"\n[bold]{ws_name}[/bold]")
        display.print(f"运行中: {stats['total_tasks']} 个任务, 共 {stats['total_gpu']} GPU\n")
        
        # GPU 卡数分布
        display.print("[bold]GPU 卡数分布:[/bold]")
        gpu_dist = stats["gpu_distribution"]
        for gpu_count in sorted(gpu_dist.keys()):
            task_count = gpu_dist[gpu_count]
            bar = "█" * min(task_count, 30)
            display.print(f"  {gpu_count:>3} GPU: {task_count:>3} 任务 {bar}")
        
        # 按用户统计（可选）
        if args.by_user:
            display.print("\n[bold]按用户统计:[/bold]")
            user_gpu = stats["user_gpu"]
            for user, gpu in sorted(user_gpu.items(), key=lambda x: -x[1]):
                display.print(f"  {user:<12} {gpu:>4} GPU")
        
        # 按项目统计（可选）
        if args.by_project:
            display.print("\n[bold]按项目统计:[/bold]")
            project_gpu = stats["project_gpu"]
            for project, gpu in sorted(project_gpu.items(), key=lambda x: -x[1]):
                proj_display = project[:25] if len(project) > 25 else project
                display.print(f"  {proj_display:<27} {gpu:>4} GPU")
        
        # 按任务类型统计（可选）
        if args.by_type:
            display.print("\n[bold]按任务类型统计:[/bold]")
            type_stats = stats["type_stats"]
            type_names = stats["type_names"]
            for task_type, info in sorted(type_stats.items(), key=lambda x: -x[1]["gpu"]):
                type_display = type_names.get(task_type, task_type)
                display.print(f"  {type_display:<20} {info['count']:>4} 任务  {info['gpu']:>5} GPU")
        
        # 按优先级统计（可选）
        if args.by_priority:
            display.print("\n[bold]按优先级统计:[/bold]")
            priority_stats = stats["priority_stats"]
            for priority, info in sorted(priority_stats.items(), key=lambda x: -x[0]):
                display.print(f"  优先级 {priority:<10} {info['count']:>4} 任务  {info['gpu']:>5} GPU")
        
        display.print("")
    
    # 汇总
    if len(all_stats) > 1:
        total_tasks = sum(s["total_tasks"] for s in all_stats)
        total_gpu = sum(s["total_gpu"] for s in all_stats)
        display.print(f"[bold]总计: {total_tasks} 个任务, {total_gpu} GPU[/bold]")
    
    return 0


def cmd_workspace(args):
    """查看工作空间内所有运行任务"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未设置 cookie，请先运行: qzcli cookie -f cookies.txt")
        display.print("[dim]提示: 从浏览器 F12 获取 cookie[/dim]")
        return 1
    
    cookie = cookie_data["cookie"]
    workspace_id = args.workspace or cookie_data.get("workspace_id", "")
    
    # 如果没有指定 workspace，列出可用的 workspace 供选择
    if not workspace_id:
        display.print("[yellow]未设置默认工作空间，正在获取可用列表...[/yellow]\n")
        try:
            workspaces = api.list_workspaces(cookie)
            if workspaces:
                display.print("[bold]请选择一个工作空间:[/bold]\n")
                for idx, ws in enumerate(workspaces, 1):
                    ws_id = ws.get("id", "")
                    ws_name = ws.get("name", "未命名")
                    display.print(f"  [{idx}] {ws_name}")
                    display.print(f"      [dim]{ws_id}[/dim]")
                display.print("")
                display.print("[dim]使用方法:[/dim]")
                display.print("  qzcli ws -w <workspace_id>")
                display.print("  qzcli cookie -w <workspace_id>  # 设置默认")
            else:
                display.print_error("未找到可访问的工作空间")
        except QzAPIError as e:
            display.print_error(f"获取工作空间列表失败: {e}")
        return 1
    
    # 项目过滤
    project_filter = None if args.all else args.project
    
    try:
        display.print("[dim]正在获取工作空间任务...[/dim]")
        result = api.list_workspace_tasks(
            workspace_id, 
            cookie,
            page_num=args.page,
            page_size=args.size,
            project_filter=project_filter,
        )
        
        tasks = result.get("task_dimensions", [])
        total = result.get("total", 0)
        
        if not tasks:
            if project_filter:
                display.print(f"[dim]项目 '{project_filter}' 暂无运行中的任务[/dim]")
            else:
                display.print("工作空间内暂无运行中的任务")
            return 0
        
        # 统计 GPU 使用
        total_gpu = sum(t.get("gpu", {}).get("total", 0) for t in tasks)
        avg_gpu_usage = sum(t.get("gpu", {}).get("usage_rate", 0) for t in tasks) / len(tasks) * 100 if tasks else 0
        
        title = f"工作空间任务概览"
        if project_filter:
            title += f" [{project_filter}]"
        title += f" (显示 {len(tasks)}/{total} 个, {total_gpu} GPU, 平均利用率 {avg_gpu_usage:.1f}%)"
        
        display.print(f"\n[bold]{title}[/bold]\n")
        
        # 同步到本地任务列表
        synced_count = 0
        if args.sync:
            store = get_store()
            for task in tasks:
                job_id = task.get("id", "")
                if job_id and not store.get(job_id):
                    # 创建简化的 JobRecord
                    from .store import JobRecord
                    job = JobRecord(
                        job_id=job_id,
                        name=task.get("name", ""),
                        status=task.get("status", "UNKNOWN").lower(),
                        source="workspace_sync",
                        workspace_id=workspace_id,
                        project_name=task.get("project", {}).get("name", ""),
                    )
                    store.add(job)
                    synced_count += 1
            if synced_count > 0:
                display.print_success(f"已同步 {synced_count} 个新任务到本地")
        
        for idx, task in enumerate(tasks, 1):
            name = task.get("name", "")
            status = task.get("status", "UNKNOWN")
            gpu_total = task.get("gpu", {}).get("total", 0)
            gpu_usage = task.get("gpu", {}).get("usage_rate", 0) * 100
            cpu_usage = task.get("cpu", {}).get("usage_rate", 0) * 100
            mem_usage = task.get("memory", {}).get("usage_rate", 0) * 100
            nodes_info = task.get("nodes_occupied", {})
            nodes_count = nodes_info.get("count", 0)
            nodes_list = nodes_info.get("nodes", [])
            user_name = task.get("user", {}).get("name", "")
            project_name = task.get("project", {}).get("name", "")
            running_time = format_duration(task.get("running_time_ms", ""))
            job_id = task.get("id", "")
            
            # 状态颜色
            if status == "RUNNING":
                status_icon = "[cyan]●[/cyan]"
            elif status == "QUEUING":
                status_icon = "[yellow]◌[/yellow]"
            else:
                status_icon = "[dim]?[/dim]"
            
            # GPU 使用率颜色
            if gpu_usage >= 80:
                gpu_color = "green"
            elif gpu_usage >= 50:
                gpu_color = "yellow"
            else:
                gpu_color = "red"
            
            display.print(f"[bold][{idx:2d}][/bold] {status_icon} {name}")
            display.print(f"     [{gpu_color}]{gpu_total} GPU ({gpu_usage:.0f}%)[/{gpu_color}] | CPU {cpu_usage:.0f}% | MEM {mem_usage:.0f}% | {running_time} | {user_name}")
            display.print(f"     [dim]{project_name} | {nodes_count} 节点: {', '.join(nodes_list[:3])}{'...' if len(nodes_list) > 3 else ''}[/dim]")
            display.print(f"     [dim]{job_id}[/dim]")
            display.print("")
        
        return 0
        
    except QzAPIError as e:
        if "401" in str(e) or "过期" in str(e):
            display.print_error("Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file> -w <workspace_id>")
        else:
            display.print_error(f"获取失败: {e}")
        return 1


def _resolve_resource_id(workspace_id, resource_type, value):
    """Resolve a resource name or ID to its ID. Returns (resolved_id, display_name)."""
    if not value:
        return None, None
    prefixes = {"projects": "project-", "compute_groups": "lcg-", "specs": ""}
    prefix = prefixes.get(resource_type, "")
    if prefix and value.startswith(prefix):
        return value, value
    if resource_type == "specs" and len(value) > 20:
        return value, value
    found = find_resource_by_name(workspace_id, resource_type, value)
    if found:
        return found["id"], found.get("name", value)
    return None, None


def _auto_select_resource(workspace_id, resource_type):
    """Auto-select the first resource of a given type from cache."""
    ws_resources = get_workspace_resources(workspace_id)
    if not ws_resources:
        return None, None
    resources = ws_resources.get(resource_type, {})
    if not resources:
        return None, None
    first = next(iter(resources.values()))
    return first["id"], first.get("name", first["id"])


def cmd_create(args):
    """创建任务"""
    display = get_display()
    api = get_api()
    store = get_store()

    # --- Resolve workspace ---
    workspace_id = None
    ws_display = ""
    if args.workspace:
        if args.workspace.startswith("ws-"):
            workspace_id = args.workspace
        else:
            workspace_id = find_workspace_by_name(args.workspace)
            if not workspace_id:
                display.print_error(f"未找到名称为 '{args.workspace}' 的工作空间")
                display.print("[dim]使用 qzcli res --list 查看已缓存的工作空间[/dim]")
                return 1
        ws_resources = get_workspace_resources(workspace_id)
        ws_display = (ws_resources or {}).get("name", workspace_id)
    else:
        display.print_error("请指定工作空间: --workspace <名称或ID>")
        display.print("[dim]使用 qzcli res --list 查看已缓存的工作空间[/dim]")
        return 1

    # --- Resolve project ---
    project_id = None
    proj_display = ""
    if args.project:
        if args.project.startswith("project-"):
            project_id = args.project
            proj_display = project_id
        else:
            project_id, proj_display = _resolve_resource_id(workspace_id, "projects", args.project)
            if not project_id:
                display.print_error(f"未找到项目 '{args.project}'")
                display.print("[dim]使用 qzcli res -w <workspace> 查看可用项目[/dim]")
                return 1
    else:
        project_id, proj_display = _auto_select_resource(workspace_id, "projects")
        if not project_id:
            display.print_error("未指定项目且缓存中无可用项目")
            display.print("[dim]使用 --project 指定，或先运行 qzcli res -u[/dim]")
            return 1
        display.print(f"[dim]自动选择项目: {proj_display} ({project_id})[/dim]")

    # --- Resolve compute group ---
    compute_group_id = None
    cg_display = ""
    if args.compute_group:
        if args.compute_group.startswith("lcg-"):
            compute_group_id = args.compute_group
            cg_display = compute_group_id
        else:
            compute_group_id, cg_display = _resolve_resource_id(workspace_id, "compute_groups", args.compute_group)
            if not compute_group_id:
                display.print_error(f"未找到计算组 '{args.compute_group}'")
                display.print("[dim]使用 qzcli res -w <workspace> 查看可用计算组[/dim]")
                return 1
    else:
        compute_group_id, cg_display = _auto_select_resource(workspace_id, "compute_groups")
        if not compute_group_id:
            display.print_error("未指定计算组且缓存中无可用计算组")
            display.print("[dim]使用 --compute-group 指定，或先运行 qzcli res -u[/dim]")
            return 1
        display.print(f"[dim]自动选择计算组: {cg_display} ({compute_group_id})[/dim]")

    # --- Resolve spec ---
    spec_id = None
    if args.spec:
        spec_id = args.spec
        if not (spec_id.count("-") >= 4 or len(spec_id) > 20):
            resolved, _ = _resolve_resource_id(workspace_id, "specs", spec_id)
            if resolved:
                spec_id = resolved
    else:
        spec_id, spec_display = _auto_select_resource(workspace_id, "specs")
        if not spec_id:
            display.print_error("未指定资源规格且缓存中无可用规格")
            display.print("[dim]使用 --spec 指定，或先运行 qzcli res -u[/dim]")
            return 1
        display.print(f"[dim]自动选择规格: {spec_display} ({spec_id})[/dim]")

    # --- Build payload ---
    payload = {
        "name": args.name,
        "logic_compute_group_id": compute_group_id,
        "project_id": project_id,
        "workspace_id": workspace_id,
        "framework": args.framework,
        "command": args.cmd_str,
        "task_priority": args.priority,
        "auto_fault_tolerance": False,
        "framework_config": [{
            "spec_id": spec_id,
            "image": args.image,
            "image_type": args.image_type,
            "instance_count": args.instances,
            "shm_gi": args.shm,
        }],
    }

    # --- Dry run ---
    if args.dry_run:
        import json as json_mod
        display.print("[bold]Dry run - 以下为将要提交的 payload:[/bold]\n")
        print(json_mod.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    # --- Show summary ---
    display.print(f"\n[bold]创建任务[/bold]")
    display.print(f"  名称: {args.name}")
    display.print(f"  工作空间: {ws_display} ({workspace_id})")
    display.print(f"  项目: {proj_display} ({project_id})")
    display.print(f"  计算组: {cg_display} ({compute_group_id})")
    display.print(f"  规格: {spec_id}")
    display.print(f"  镜像: {args.image}")
    display.print(f"  实例数: {args.instances}")
    display.print(f"  共享内存: {args.shm} GiB")
    display.print(f"  优先级: {args.priority}")
    display.print(f"  命令: {args.cmd_str[:120]}{'...' if len(args.cmd_str) > 120 else ''}")
    display.print("")

    # --- Submit ---
    try:
        result = api.create_job(payload)
    except QzAPIError as e:
        display.print_error(f"任务创建失败: {e}")
        return 1

    job_id = result.get("job_id", "")
    resp_ws_id = result.get("workspace_id", workspace_id)

    if not job_id:
        display.print_error("任务创建失败: 响应中未包含 job_id")
        if args.output_json:
            import json as json_mod
            print(json_mod.dumps(result, indent=2, ensure_ascii=False))
        return 1

    job_url = f"https://qz.sii.edu.cn/jobs/distributedTrainingDetail/{job_id}?spaceId={resp_ws_id}"

    display.print_success(f"任务创建成功!")
    display.print(f"  Job ID: [cyan]{job_id}[/cyan]")
    display.print(f"  链接: {job_url}")

    # --- Auto track ---
    if not args.no_track:
        job = JobRecord(
            job_id=job_id,
            name=args.name,
            status="job_pending",
            workspace_id=resp_ws_id,
            project_id=project_id,
            source="qzcli create",
            command=args.cmd_str,
            url=job_url,
            instance_count=args.instances,
            priority_level=str(args.priority),
        )
        store.add(job)
        display.print(f"  [dim]已自动追踪到本地[/dim]")

    # --- JSON output ---
    if args.output_json:
        import json as json_mod
        output = {
            "job_id": job_id,
            "workspace_id": resp_ws_id,
            "url": job_url,
            "name": args.name,
        }
        print(json_mod.dumps(output, ensure_ascii=False))

    return 0


def cmd_batch(args):
    """批量提交任务"""
    import json as json_mod
    import itertools

    display = get_display()

    config_path = Path(args.config)
    if not config_path.exists():
        display.print_error(f"配置文件不存在: {config_path}")
        return 1

    with open(config_path, "r", encoding="utf-8") as f:
        config = json_mod.load(f)

    defaults = config.get("defaults", {})
    matrix = config.get("matrix", {})
    name_template = config.get("name_template", "job-{_index}")
    command_template = config.get("command_template", "")

    if not command_template:
        display.print_error("配置文件中缺少 command_template")
        return 1

    # Generate all combinations from matrix
    keys = list(matrix.keys())
    if not keys:
        display.print_error("配置文件中 matrix 为空")
        return 1

    values = [matrix[k] if isinstance(matrix[k], list) else [matrix[k]] for k in keys]
    combinations = list(itertools.product(*values))
    total = len(combinations)

    display.print(f"\n[bold]批量任务提交[/bold]")
    display.print(f"  配置文件: {config_path}")
    display.print(f"  矩阵维度: {' x '.join(f'{k}({len(matrix[k]) if isinstance(matrix[k], list) else 1})' for k in keys)}")
    display.print(f"  总任务数: {total}")
    display.print("")

    if args.dry_run:
        display.print("[bold]Dry run - 预览所有任务:[/bold]\n")

    successful = 0
    failed = 0
    failed_tasks = []

    for idx, combo in enumerate(combinations, 1):
        # Build template variables
        variables = dict(zip(keys, combo))
        variables["_index"] = idx
        for k, v in variables.items():
            if isinstance(v, str) and "/" in v:
                import os as os_mod
                variables[f"{k}_basename"] = os_mod.path.basename(v)

        try:
            job_name = name_template.format(**variables)
        except KeyError as e:
            display.print_warning(f"任务 {idx}: name_template 变量缺失: {e}")
            job_name = f"batch-job-{idx}"

        try:
            command = command_template.format(**variables)
        except KeyError as e:
            display.print_error(f"任务 {idx}: command_template 变量缺失: {e}")
            failed += 1
            failed_tasks.append(f"{idx}: template error {e}")
            continue

        if args.dry_run:
            display.print(f"  [{idx}/{total}] {job_name}")
            display.print(f"    命令: {command[:120]}{'...' if len(command) > 120 else ''}")
            display.print("")
            continue

        display.print(f"[bold][{idx}/{total}][/bold] 提交: {job_name}")

        # Build argparse-like namespace for cmd_create
        create_args = argparse.Namespace(
            name=job_name,
            cmd_str=command,
            workspace=defaults.get("workspace", ""),
            project=defaults.get("project", ""),
            compute_group=defaults.get("compute_group", ""),
            spec=defaults.get("spec", ""),
            image=defaults.get("image", "docker.sii.shaipower.online/inspire-studio/dhyu-wan-torch29:0.4"),
            image_type=defaults.get("image_type", "SOURCE_PRIVATE"),
            instances=defaults.get("instances", 1),
            shm=defaults.get("shm", 1200),
            priority=defaults.get("priority", 10),
            framework=defaults.get("framework", "pytorch"),
            no_track=False,
            dry_run=False,
            output_json=False,
        )

        ret = cmd_create(create_args)
        if ret == 0:
            successful += 1
        else:
            failed += 1
            failed_tasks.append(f"{idx}: {job_name}")
            if not args.continue_on_error:
                display.print_error("任务提交失败，停止批量提交（使用 --continue-on-error 忽略错误）")
                break

        # Delay between submissions
        if idx < total and not args.dry_run:
            time.sleep(args.delay)

    if args.dry_run:
        display.print(f"[bold]预览完成，共 {total} 个任务[/bold]")
        return 0

    display.print(f"\n[bold]批量提交完成[/bold]")
    display.print(f"  总任务数: {total}")
    display.print(f"  成功: {successful}")
    display.print(f"  失败: {failed}")

    if failed_tasks:
        display.print("\n[bold]失败的任务:[/bold]")
        for task in failed_tasks:
            display.print(f"  - {task}")
        return 1

    return 0


def _find_notebook_jupyter_info(notebook_name, display):
    """
    根据开发机名称，查找 notebook_id，通过平台 API 获取 Jupyter 连接信息。

    流程：notebook/list → notebook_id → /api/v1/notebook/lab/{id} 301 → Jupyter URL with token

    Returns: dict with {base_url, token, notebook_id} or None
    """
    import re
    import requests as _requests

    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未登录，请先 qzcli login")
        return None

    cookie = cookie_data["cookie"]
    api = get_api()
    config = load_config()
    user_id = config.get("user_id", "")

    # 1. 从 API 查找运行中的开发机
    all_resources = load_all_resources()
    if not all_resources:
        display.print_error("没有已缓存的工作空间，请先 qzcli res -u")
        return None

    notebook_id = None
    for ws_id, ws_data in all_resources.items():
        try:
            user_ids = [user_id] if user_id else []
            nb_result = api.list_notebooks_with_cookie(
                ws_id, cookie, page_size=50,
                user_ids=user_ids, status=["RUNNING"],
            )
            for nb in nb_result.get("list", []):
                if nb.get("name") == notebook_name:
                    notebook_id = nb.get("notebook_id")
                    break
        except QzAPIError:
            continue
        if notebook_id:
            break

    if not notebook_id:
        display.print_error(f"未找到名为 '{notebook_name}' 的运行中开发机")
        return None

    display.print(f"[dim]找到开发机: {notebook_name} (notebook_id: {notebook_id[:8]}...)[/dim]")

    # 2. 通过 /api/v1/notebook/lab/{notebook_id} 获取 Jupyter URL（含 token）
    try:
        resp = _requests.get(
            f"https://qz.sii.edu.cn/api/v1/notebook/lab/{notebook_id}",
            headers={"cookie": cookie, "user-agent": "Mozilla/5.0", "accept": "text/html"},
            allow_redirects=False,
            timeout=15,
        )
        if resp.status_code in (301, 302, 303, 307):
            jupyter_url = resp.headers.get("Location", "")
            # URL 格式: https://{domain}/{ws}/{proj}/{user}/jupyter/{nb_id}/{token}/lab?token={token}
            match = re.search(
                r'(https://[^/]+/[^/]+/[^/]+/[^/]+/jupyter/[^/]+/([^/]+))/lab',
                jupyter_url
            )
            if match:
                base_url = match.group(1)
                token = match.group(2)
                display.print(f"[dim]已获取 Jupyter 连接信息[/dim]")
                return {
                    "base_url": base_url,
                    "token": token,
                    "notebook_id": notebook_id,
                }

        if resp.status_code == 401 or resp.status_code == 302 and "keycloak" in resp.headers.get("Location", ""):
            display.print_error("Cookie 已过期，请重新登录: qzcli login")
            return None

        display.print_error(f"获取 Jupyter URL 失败: HTTP {resp.status_code}")
        return None

    except Exception as e:
        display.print_error(f"请求失败: {e}")
        return None


def _exec_via_jupyter(jupyter_info, cmd_str, display, timeout=120):
    """
    通过 Jupyter Contents API + Terminal 稳健执行命令。

    流程：
    1. Terminal 发送脚本命令：在 /tmp 执行，完成后拷贝结果到 Contents API 可读目录
    2. Contents API 轮询读取输出文件
    3. 清理临时文件

    命令执行与网络连接解耦：即使 WebSocket 断连，命令仍在服务端完整运行。

    Returns: (exit_code, output_str)
    """
    import json as _json
    import time as _time

    try:
        import websocket
    except ImportError:
        display.print_error("需要 websocket-client: pip install websocket-client")
        return 1, ""

    import requests as _requests

    base_http = jupyter_info["base_url"]
    base_ws = base_http.replace("https://", "wss://")
    token = jupyter_info["token"]
    headers = {"authorization": f"token {token}", "content-type": "application/json"}

    job_id = f"qzcli_{int(_time.time())}"
    tmp_dir = "/tmp/.qzcli"
    # Contents API 通过 symlink 读取 /tmp/.qzcli
    api_dir = "_qzcli"
    api_out = f"{api_dir}/{job_id}_out"
    api_exit = f"{api_dir}/{job_id}_exit"

    def cleanup():
        # Contents API 删除会同时删掉 /tmp 里的文件（因为 symlink）
        for fname in [api_out, api_exit]:
            try:
                _requests.delete(f"{base_http}/api/contents/{fname}", headers=headers, timeout=5)
            except Exception:
                pass

    # 1. 确保 Contents API 中转目录存在
    try:
        _requests.put(
            f"{base_http}/api/contents/{api_dir}",
            headers=headers,
            json={"type": "directory"},
            timeout=10,
        )
    except Exception:
        pass

    # 2. 通过 Terminal 发送一条复合命令（fire-and-forget）
    #    输出写到 /tmp/.qzcli/，通过 symlink 让 Contents API 可读
    shell_cmd = (
        f'mkdir -p {tmp_dir} && '
        f'{{ [ -L "$PWD/{api_dir}" ] || {{ rm -rf "$PWD/{api_dir}" && ln -sf {tmp_dir} "$PWD/{api_dir}"; }}; }} && '
        f'( {cmd_str} ) > {tmp_dir}/{job_id}_out 2>&1; '
        f'echo $? > {tmp_dir}/{job_id}_exit'
    )

    launched = False
    for attempt in range(3):
        try:
            resp_t = _requests.get(f"{base_http}/api/terminals", headers=headers, timeout=10)
            terms = resp_t.json() if resp_t.status_code == 200 else []
            if terms:
                term_name = terms[0]["name"]
            else:
                resp_t = _requests.post(f"{base_http}/api/terminals", headers=headers, timeout=10)
                term_name = resp_t.json()["name"]

            ws = websocket.create_connection(
                f"{base_ws}/terminals/websocket/{term_name}?token={token}", timeout=10,
            )
            _time.sleep(0.3)
            while True:
                try:
                    ws.settimeout(0.3)
                    ws.recv()
                except:
                    break

            ws.send(_json.dumps(["stdin", shell_cmd + "\r"]))
            _time.sleep(0.5)
            ws.close()
            launched = True
            break
        except Exception as e:
            if attempt < 2:
                display.print_warning(f"连接失败，重试中... ({attempt + 1}/3)")
                _time.sleep(2)
            else:
                display.print_error(f"启动命令失败: {e}")
                return 1, ""

    if not launched:
        return 1, ""

    # 3. 轮询 Contents API 读取输出
    deadline = _time.time() + timeout
    exit_code = 1
    output = ""
    poll_interval = 1

    while _time.time() < deadline:
        _time.sleep(poll_interval)
        try:
            resp_exit = _requests.get(
                f"{base_http}/api/contents/{api_exit}",
                headers=headers, timeout=10,
            )
            if resp_exit.status_code == 200:
                exit_str = resp_exit.json().get("content", "").strip()
                exit_code = int(exit_str) if exit_str.isdigit() else 1

                resp_out = _requests.get(
                    f"{base_http}/api/contents/{api_out}",
                    headers=headers, timeout=10,
                )
                if resp_out.status_code == 200:
                    output = resp_out.json().get("content", "").rstrip()

                cleanup()
                return exit_code, output
        except Exception:
            pass

        poll_interval = min(poll_interval * 1.5, 5)

    cleanup()
    display.print_warning("命令执行超时，输出可能不完整")
    return 124, output


def cmd_exec(args):
    """在开发机上执行命令（通过 Jupyter terminal API）"""
    display = get_display()
    host = args.host
    cmd_parts = args.remote_cmd

    if not cmd_parts:
        display.print_error("请指定要执行的命令")
        display.print("[dim]用法: qzcli exec <host> <command>[/dim]")
        display.print("[dim]示例: qzcli exec blender-rl nvidia-smi[/dim]")
        return 1

    cmd_str = " ".join(cmd_parts)

    # 查找 Jupyter 连接信息
    jupyter_info = _find_notebook_jupyter_info(host, display)
    if jupyter_info is None:
        return 1

    display.print(f"[dim]在 {host} 上执行: {cmd_str}[/dim]")

    exit_code, output = _exec_via_jupyter(jupyter_info, cmd_str, display)
    if output:
        print(output)
    return exit_code


def cmd_login(args):
    """通过 CAS 登录获取 cookie"""
    import getpass
    
    display = get_display()
    api = get_api()
    
    # 获取用户名
    username = args.username
    if not username:
        try:
            username = input("学工号: ").strip()
        except (EOFError, KeyboardInterrupt):
            display.print("\n[dim]已取消[/dim]")
            return 1
    
    if not username:
        display.print_error("用户名不能为空")
        return 1
    
    # 获取密码
    password = args.password
    if getattr(args, 'password_stdin', False):
        try:
            password = sys.stdin.readline().rstrip('\n')
        except (EOFError, KeyboardInterrupt):
            display.print_error("未从 stdin 读取到密码")
            return 1
    if not password:
        try:
            password = getpass.getpass("密码: ")
        except (EOFError, KeyboardInterrupt):
            display.print("\n[dim]已取消[/dim]")
            return 1
    
    if not password:
        display.print_error("密码不能为空")
        return 1
    
    display.print("[dim]正在登录...[/dim]")
    
    try:
        cookie = api.login_with_cas(username, password)
        
        # 保存 cookie
        save_cookie(cookie, workspace_id=args.workspace)
        
        display.print_success("登录成功！Cookie 已保存")
        
        # 显示 cookie 前几个字符
        cookie_preview = cookie[:50] + "..." if len(cookie) > 50 else cookie
        display.print(f"[dim]Cookie: {cookie_preview}[/dim]")
        
        if args.workspace:
            display.print(f"[dim]默认工作空间: {args.workspace}[/dim]")
        
        return 0
        
    except QzAPIError as e:
        display.print_error(f"登录失败: {e}")
        return 1


def main():
    """主入口"""
    parser = argparse.ArgumentParser(
        prog="qzcli",
        description="启智平台任务管理 CLI 工具",
    )
    parser.add_argument(
        "--version", "-V",
        action="version",
        version=f"qzcli {__version__}"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="子命令")
    
    # init 命令
    init_parser = subparsers.add_parser("init", help="初始化配置")
    init_parser.add_argument("--username", "-u", help="用户名")
    init_parser.add_argument("--password", "-p", help="密码")
    
    # list 命令
    list_parser = subparsers.add_parser("list", aliases=["ls"], help="列出任务")
    list_parser.add_argument("--limit", "-n", type=int, default=20, help="显示数量限制")
    list_parser.add_argument("--status", "-s", help="按状态过滤")
    list_parser.add_argument("--running", "-r", action="store_true", help="只显示运行中/排队中的任务")
    list_parser.add_argument("--no-refresh", action="store_true", help="不更新状态")
    list_parser.add_argument("--verbose", "-v", action="store_true", help="显示详细信息")
    list_parser.add_argument("--url", "-u", action="store_true", default=True, help="显示任务链接（默认开启）")
    list_parser.add_argument("--wide", action="store_true", default=True, help="宽格式显示（默认开启）")
    list_parser.add_argument("--compact", action="store_true", help="紧凑表格格式（关闭宽格式）")
    # Cookie 模式参数
    list_parser.add_argument("--cookie", "-c", action="store_true", help="使用 cookie 从 API 获取任务（无需本地 store）")
    list_parser.add_argument("--workspace", "-w", help="工作空间（名称或 ID，cookie 模式）")
    list_parser.add_argument("--all-ws", action="store_true", help="查询所有已缓存的工作空间（cookie 模式）")
    # 交互式建模（开发机）
    list_parser.add_argument("--include-interactive", "-I", action="store_true", help="同时显示交互式建模实例（开发机）")
    list_parser.add_argument("--only-interactive", "-i", action="store_true", help="只显示交互式建模实例（开发机）")
    list_parser.add_argument("--all-users", action="store_true", help="显示所有用户的开发机（默认只显示自己的）")
    
    # status 命令
    status_parser = subparsers.add_parser("status", aliases=["st"], help="查看任务状态")
    status_parser.add_argument("job_id", help="任务 ID")
    status_parser.add_argument("--json", "-j", action="store_true", help="输出 JSON")
    
    # stop 命令
    stop_parser = subparsers.add_parser("stop", help="停止任务")
    stop_parser.add_argument("job_id", help="任务 ID")
    stop_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")
    
    # watch 命令
    watch_parser = subparsers.add_parser("watch", aliases=["w"], help="实时监控")
    watch_parser.add_argument("--interval", "-i", type=int, default=10, help="刷新间隔（秒）")
    watch_parser.add_argument("--limit", "-n", type=int, default=30, help="显示数量限制")
    watch_parser.add_argument("--keep-alive", "-k", action="store_true", help="所有任务完成后继续监控")
    
    # track 命令（供脚本调用）
    track_parser = subparsers.add_parser("track", help="追踪任务")
    track_parser.add_argument("job_id", help="任务 ID")
    track_parser.add_argument("--name", help="任务名称")
    track_parser.add_argument("--source", help="来源脚本")
    track_parser.add_argument("--workspace", help="工作空间 ID")
    track_parser.add_argument("--quiet", "-q", action="store_true", help="静默模式")
    
    # import 命令
    import_parser = subparsers.add_parser("import", help="从文件导入任务")
    import_parser.add_argument("file", help="包含任务 ID 的文件")
    import_parser.add_argument("--source", help="来源标记")
    import_parser.add_argument("--refresh", "-r", action="store_true", help="导入后更新状态")
    
    # remove 命令
    remove_parser = subparsers.add_parser("remove", aliases=["rm"], help="删除任务记录")
    remove_parser.add_argument("job_id", help="任务 ID")
    remove_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")
    
    # clear 命令
    clear_parser = subparsers.add_parser("clear", help="清空所有任务记录")
    clear_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")
    
    # cookie 命令
    cookie_parser = subparsers.add_parser("cookie", help="设置浏览器 cookie（用于访问内部 API）")
    cookie_parser.add_argument("cookie", nargs="?", help="浏览器 cookie 字符串")
    cookie_parser.add_argument("--file", "-f", help="从文件读取 cookie")
    cookie_parser.add_argument("--workspace", "-w", help="默认工作空间 ID")
    cookie_parser.add_argument("--show", action="store_true", help="显示当前 cookie")
    cookie_parser.add_argument("--clear", action="store_true", help="清除 cookie")
    cookie_parser.add_argument("--no-test", action="store_true", help="不测试 cookie 有效性")
    
    # login 命令
    login_parser = subparsers.add_parser("login", help="通过 CAS 统一认证登录获取 cookie")
    login_parser.add_argument("--username", "-u", help="学工号")
    login_parser.add_argument("--password", "-p", help="密码（含特殊字符时建议用单引号或 --password-stdin）")
    login_parser.add_argument("--password-stdin", action="store_true", help="从 stdin 读取密码（适合脚本: echo 'pass' | qzcli login -u user --password-stdin）")
    login_parser.add_argument("--workspace", "-w", help="默认工作空间 ID")
    
    # exec 命令
    exec_parser = subparsers.add_parser("exec", help="在开发机上执行命令（通过 Jupyter API，无需 SSH）")
    exec_parser.add_argument("host", help="开发机名称（如 blender-rl、rtx-gpu8）")
    exec_parser.add_argument("remote_cmd", nargs=argparse.REMAINDER, help="要执行的命令")

    # workspace 命令
    workspace_parser = subparsers.add_parser("workspace", aliases=["ws"], help="查看工作空间内所有运行任务")
    workspace_parser.add_argument("--workspace", "-w", help="工作空间 ID")
    workspace_parser.add_argument("--project", "-p", default="扩散", help="按项目名称过滤（默认: 扩散）")
    workspace_parser.add_argument("--all", "-a", action="store_true", help="显示所有项目（不过滤）")
    workspace_parser.add_argument("--page", type=int, default=1, help="页码")
    workspace_parser.add_argument("--size", type=int, default=100, help="每页数量（默认 100）")
    workspace_parser.add_argument("--sync", "-s", action="store_true", help="同步到本地任务列表")
    
    # workspaces 命令 - 从历史任务提取资源配置
    workspaces_parser = subparsers.add_parser("workspaces", aliases=["lsws", "res", "resources"], help="从历史任务提取资源配置（项目、计算组、规格）")
    workspaces_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    workspaces_parser.add_argument("--export", "-e", action="store_true", help="输出可用于脚本的环境变量格式")
    workspaces_parser.add_argument("--update", "-u", action="store_true", help="强制从 API 更新缓存")
    workspaces_parser.add_argument("--list", "-l", action="store_true", help="列出所有已缓存的工作空间")
    workspaces_parser.add_argument("--name", help="设置工作空间名称（别名）")
    
    # avail 命令 - 查询空余节点
    avail_parser = subparsers.add_parser("avail", aliases=["av"], help="查询计算组空余节点，帮助决定任务应该提交到哪里")
    avail_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    avail_parser.add_argument("--group", "-g", help="计算组 ID 或名称（可选，不指定则查询所有）")
    avail_parser.add_argument("--nodes", "-n", type=int, help="需要的节点数（推荐模式：找出满足条件的计算组）")
    avail_parser.add_argument("--export", "-e", action="store_true", help="输出可用于脚本的环境变量格式")
    avail_parser.add_argument("--verbose", "-v", action="store_true", help="显示空闲节点名称列表")
    avail_parser.add_argument("--lp", "--low-priority", action="store_true", dest="low_priority", help="计算低优任务占用节点（较慢）")
    
    # usage 命令
    usage_parser = subparsers.add_parser("usage", help="统计工作空间的 GPU 使用分布")
    usage_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    usage_parser.add_argument("--by-user", "-u", action="store_true", help="按用户统计 GPU 使用")
    usage_parser.add_argument("--by-project", "-p", action="store_true", help="按项目统计 GPU 使用")
    usage_parser.add_argument("--by-type", "-t", action="store_true", help="按任务类型统计（训练/建模/部署）")
    usage_parser.add_argument("--by-priority", "-r", action="store_true", help="按优先级统计")
    
    # create 命令 - 创建任务
    create_parser = subparsers.add_parser("create", aliases=["create-job"], help="创建并提交任务到启智平台")
    create_parser.add_argument("--name", "-n", required=True, help="任务名称")
    create_parser.add_argument("--command", "-c", dest="cmd_str", required=True, help="执行命令")
    create_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称（从 qzcli res 缓存解析）")
    create_parser.add_argument("--project", "-p", help="项目 ID 或名称（不指定则自动选择）")
    create_parser.add_argument("--compute-group", "-g", dest="compute_group", help="计算组 ID 或名称")
    create_parser.add_argument("--spec", "-s", help="资源规格 ID（不指定则自动选择）")
    create_parser.add_argument("--image", "-i", default="docker.sii.shaipower.online/inspire-studio/dhyu-wan-torch29:0.4", help="Docker 镜像")
    create_parser.add_argument("--image-type", dest="image_type", default="SOURCE_PRIVATE", help="镜像类型（默认 SOURCE_PRIVATE）")
    create_parser.add_argument("--instances", type=int, default=1, help="实例数量（默认 1）")
    create_parser.add_argument("--shm", type=int, default=1200, help="共享内存 GiB（默认 1200）")
    create_parser.add_argument("--priority", type=int, default=10, help="任务优先级 1-10（默认 10）")
    create_parser.add_argument("--framework", default="pytorch", help="框架类型（默认 pytorch）")
    create_parser.add_argument("--no-track", action="store_true", help="不自动追踪任务")
    create_parser.add_argument("--dry-run", action="store_true", help="只显示 payload 不提交")
    create_parser.add_argument("--json", dest="output_json", action="store_true", help="输出 JSON 格式（供脚本集成）")
    
    # batch 命令 - 批量提交任务
    batch_parser = subparsers.add_parser("batch", help="从 JSON 配置文件批量提交任务")
    batch_parser.add_argument("config", help="批量配置文件路径（JSON 格式）")
    batch_parser.add_argument("--dry-run", action="store_true", help="只预览不提交")
    batch_parser.add_argument("--delay", type=float, default=3, help="任务间延迟秒数（默认 3）")
    batch_parser.add_argument("--continue-on-error", action="store_true", help="遇到错误继续提交")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 0
    
    # 命令分发
    commands = {
        "init": cmd_init,
        "list": cmd_list,
        "ls": cmd_list,
        "status": cmd_status,
        "st": cmd_status,
        "stop": cmd_stop,
        "watch": cmd_watch,
        "w": cmd_watch,
        "track": cmd_track,
        "import": cmd_import,
        "remove": cmd_remove,
        "rm": cmd_remove,
        "clear": cmd_clear,
        "cookie": cmd_cookie,
        "exec": cmd_exec,
        "login": cmd_login,
        "workspace": cmd_workspace,
        "ws": cmd_workspace,
        "workspaces": cmd_workspaces,
        "lsws": cmd_workspaces,
        "resources": cmd_workspaces,
        "res": cmd_workspaces,
        "avail": cmd_avail,
        "av": cmd_avail,
        "usage": cmd_usage,
        "create": cmd_create,
        "create-job": cmd_create,
        "batch": cmd_batch,
    }
    
    cmd_func = commands.get(args.command)
    if cmd_func:
        try:
            return cmd_func(args)
        except KeyboardInterrupt:
            print("\n操作已取消")
            return 130
        except Exception as e:
            display = get_display()
            display.print_error(str(e))
            return 1
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
