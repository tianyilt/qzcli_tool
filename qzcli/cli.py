#!/usr/bin/env python3
"""
qzcli - 启智平台任务管理 CLI
"""

import argparse
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import __version__
from .api import QzAPIError, get_api
from .config import (
    CONFIG_DIR,
    clear_cookie,
    find_resource_by_name,
    find_workspace_by_name,
    get_cookie,
    get_credentials,
    get_workspace_resources,
    init_config,
    list_cached_workspaces,
    load_all_resources,
    load_config,
    load_create_interactive_snapshot,
    save_config,
    save_cookie,
    save_create_interactive_snapshot,
    save_resources,
    set_workspace_name,
    update_workspace_compute_groups,
    update_workspace_projects,
)
from .display import format_duration, get_display
from .store import JobRecord, get_store

try:
    from rich import box
    from rich.table import Table

    RICH_TABLE_AVAILABLE = True
except ImportError:
    RICH_TABLE_AVAILABLE = False
    Table = None  # type: ignore
    box = None  # type: ignore

try:
    from prompt_toolkit.application import Application
    from prompt_toolkit.filters import Condition
    from prompt_toolkit.key_binding import KeyBindings
    from prompt_toolkit.layout import Layout
    from prompt_toolkit.layout.containers import DynamicContainer, HSplit, Window
    from prompt_toolkit.layout.controls import FormattedTextControl
    from prompt_toolkit.layout.dimension import D
    from prompt_toolkit.layout.margins import ScrollbarMargin
    from prompt_toolkit.shortcuts import choice as prompt_toolkit_choice
    from prompt_toolkit.styles import Style
    from prompt_toolkit.widgets import Frame, RadioList, TextArea

    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    prompt_toolkit_choice = None  # type: ignore
    Application = None  # type: ignore
    Condition = None  # type: ignore
    KeyBindings = None  # type: ignore
    Layout = None  # type: ignore
    DynamicContainer = None  # type: ignore
    HSplit = None  # type: ignore
    Window = None  # type: ignore
    FormattedTextControl = None  # type: ignore
    D = None  # type: ignore
    ScrollbarMargin = None  # type: ignore
    Style = None  # type: ignore
    Frame = None  # type: ignore
    RadioList = None  # type: ignore
    TextArea = None  # type: ignore
    PROMPT_TOOLKIT_AVAILABLE = False


DEFAULT_CREATE_IMAGE = "docker.sii.shaipower.online/inspire-studio/dhyu-wan-torch29:0.4"
DEFAULT_CREATE_IMAGE_TYPE = "SOURCE_PRIVATE"
DEFAULT_CREATE_INSTANCES = 1
DEFAULT_CREATE_SHM = 1200
DEFAULT_CREATE_PRIORITY = 10
DEFAULT_CREATE_FRAMEWORK = "pytorch"


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
        display.print_error("未设置 cookie，请先运行: qzcli login")
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
        workspace_ids = [
            (ws_id, data.get("name", "")) for ws_id, data in all_resources.items()
        ]
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
                ws_name = (
                    ws_resources.get("name", "") if ws_resources else workspace_input
                )
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
    only_interactive = getattr(args, "only_interactive", False)
    include_interactive = (
        getattr(args, "include_interactive", False) or only_interactive
    )
    all_users = getattr(args, "all_users", False)

    # 获取当前用户 ID（用于开发机过滤）
    current_user_id = None
    if include_interactive and not all_users:
        config = load_config()
        current_user_id = config.get("user_id", "")
        if not current_user_id:
            # 首次：从 train_job/list 获取用户 ID 并缓存
            try:
                probe = api.list_jobs_with_cookie(
                    workspace_ids[0][0], cookie, page_size=1
                )
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
                display.print(
                    f"[dim]正在获取 {ws_name or workspace_id} 的任务...[/dim]"
                )
            else:
                display.print("[dim]正在从 API 获取任务列表...[/dim]")

            # 获取训练任务（除非 --only-interactive）
            if not only_interactive:
                result = api.list_jobs_with_cookie(
                    workspace_id,
                    cookie,
                    page_size=args.limit * 2 if args.running else args.limit,
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
                    user_ids = (
                        [current_user_id] if current_user_id and not all_users else []
                    )
                    status_filter = ["RUNNING"] if args.running else []
                    nb_result = api.list_notebooks_with_cookie(
                        workspace_id,
                        cookie,
                        page_size=args.limit,
                        user_ids=user_ids,
                        status=status_filter,
                    )
                    for nb_data in nb_result.get("list", []):
                        job = JobRecord.from_notebook_response(
                            nb_data, workspace_id=workspace_id, workspace_name=ws_name
                        )
                        all_jobs.append(job)
                except QzAPIError as e:
                    if only_interactive:
                        raise
                    display.print_warning(
                        f"获取 {ws_name or workspace_id} 的开发机列表失败: {e}"
                    )

        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error(
                    "Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>"
                )
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
        active_statuses = {
            "job_running",
            "job_queuing",
            "job_pending",
            "running",
            "queuing",
            "pending",
        }
        all_jobs = [
            j
            for j in all_jobs
            if j.status.lower() in active_statuses
            or "running" in j.status.lower()
            or "queue" in j.status.lower()
        ]

    # 限制数量
    all_jobs = all_jobs[: args.limit]

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
        display.print(
            "[dim]暂无任务记录，使用 qzcli import 导入或 qzcli track 添加任务[/dim]"
        )
        return 0

    # 更新任务状态
    if not args.no_refresh:
        display.print("[dim]正在更新任务状态...[/dim]")

        # 只更新非终态任务
        job_ids_to_update = [
            j.job_id
            for j in jobs
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
        active_statuses = {
            "job_running",
            "job_queuing",
            "job_pending",
            "running",
            "queuing",
            "pending",
        }
        jobs = [
            j
            for j in jobs
            if j.status.lower() in active_statuses
            or "running" in j.status.lower()
            or "queue" in j.status.lower()
        ]
        # 应用 limit
        jobs = jobs[: args.limit]

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
                j
                for j in jobs
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
                jobs, title=f"启智平台任务监控 (每 {interval}s 刷新)"
            )

            # 检查是否还有活跃任务
            active_count = sum(
                1
                for j in jobs
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
        display.print(
            "[dim]提示: 在 qz.sii.edu.cn 页面按 F12 -> Console -> 输入 document.cookie[/dim]"
        )
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
            display.print(
                "[dim]暂无已缓存的工作空间，使用 qzcli res -w <workspace_id> 添加[/dim]"
            )
            return 0

        display.print(f"\n[bold]已缓存的工作空间 ({len(cached)} 个)[/bold]\n")
        for ws in cached:
            name = ws.get("name") or "[未命名]"
            import datetime

            updated = datetime.datetime.fromtimestamp(ws.get("updated_at", 0)).strftime(
                "%Y-%m-%d %H:%M"
            )
            display.print(f"  [bold]{name}[/bold]")
            display.print(f"    ID: [cyan]{ws['id']}[/cyan]")
            display.print(
                f"    资源: {ws['project_count']} 项目, {ws['compute_group_count']} 计算组, {ws['spec_count']} 规格"
            )
            display.print(f"    更新: {updated}")
            display.print("")

        display.print("[dim]使用方法:[/dim]")
        display.print("  qzcli res -w <名称或ID>      # 查看资源")
        display.print("  qzcli res -w <ID> -u         # 更新缓存")
        display.print("  qzcli res -w <ID> --name 别名  # 设置名称")
        return 0

    # 如果只设置名称（没有 -u 参数）
    if hasattr(args, "name") and args.name and not args.update:
        workspace_id = args.workspace
        if not workspace_id:
            display.print_error(
                "请指定工作空间 ID: qzcli res -w <workspace_id> --name <名称>"
            )
            return 1
        set_workspace_name(workspace_id, args.name)
        display.print_success(f"已设置工作空间名称: {args.name}")
        return 0

    # 记录要设置的名称（如果有）
    pending_name = args.name if hasattr(args, "name") else None

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
                    resources, jobs_count = _collect_workspace_resources_from_live_apis(
                        api, ws_id, cookie
                    )
                    # 保存到本地缓存
                    save_resources(ws_id, resources, ws_name)

                    projects_count = len(resources.get("projects", []))
                    cg_count = len(resources.get("compute_groups", []))
                    display.print(
                        f"  ✓ {ws_name or ws_id}: {projects_count} 项目, {cg_count} 计算组, {jobs_count} 历史任务"
                    )
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
            display.print(
                f"[dim]匹配到工作空间: {workspace_input} -> {workspace_id}[/dim]"
            )
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

        updated = datetime.datetime.fromtimestamp(
            cached_resources.get("updated_at", 0)
        ).strftime("%Y-%m-%d %H:%M")
        ws_name = cached_resources.get("name", "")
        title = "资源配置"
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

            resources, jobs_count = _collect_workspace_resources_from_live_apis(
                api, workspace_id, cookie
            )

            # 保存到本地缓存
            ws_name = pending_name or (
                cached_resources.get("name", "") if cached_resources else ""
            )
            save_resources(workspace_id, resources, ws_name)
            display.print_success("资源配置已保存到本地缓存")

            display.print(
                f"\n[bold]资源配置（从 {jobs_count} 个历史任务和 workspace 资源接口聚合）[/bold]"
            )
            display.print(f"[dim]工作空间: {workspace_id}[/dim]\n")

            projects = resources.get("projects", [])
            compute_groups = resources.get("compute_groups", [])
            specs = resources.get("specs", [])

        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error(
                    "Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>"
                )
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
            display.print(
                f"  - {gpu_count}x {gpu_type} + {cpu_count}核CPU + {mem_gb}GB内存"
            )
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
                display.print(
                    f'# {spec.get("gpu_count", 0)}x {spec.get("gpu_type", "")}'
                )
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

    # 解析 workspace 参数（支持名称或 ID）
    workspace_input = args.workspace
    try:
        available_workspace_options = _sort_workspace_options_for_selection(
            _list_available_workspaces(api, display)
        )
    except QzAPIError as e:
        if _is_auth_related_error(e) or "未设置 cookie" in str(e):
            display.print_error("未设置有效 cookie，请先运行: qzcli login")
        else:
            display.print_error(f"获取工作空间列表失败: {e}")
        return 1

    workspace_options: List[Dict[str, Any]] = []
    if not workspace_input:
        workspace_options = list(available_workspace_options)
        if not workspace_options:
            display.print_error("未找到可访问的工作空间")
            display.print(
                "[dim]请先运行 qzcli login，确认 cookie 有效后再执行 qzcli avail[/dim]"
            )
            return 1
    else:
        workspace_id, ws_display = _resolve_workspace_option_from_snapshot(
            available_workspace_options, workspace_input
        )
        if workspace_id:
            workspace_options = [
                option
                for option in available_workspace_options
                if str(option.get("id", "")) == workspace_id
            ]
        elif workspace_input.startswith("ws-"):
            cached_resources = get_workspace_resources(workspace_input) or {}
            workspace_options = [
                {
                    "id": workspace_input,
                    "name": cached_resources.get("name", workspace_input),
                }
            ]
            workspace_id = workspace_input
        else:
            display.print_error(f"未找到名称为 '{workspace_input}' 的工作空间")
            display.print(
                "[dim]请先运行 qzcli avail 刷新 workspace 列表，或改用 workspace ID[/dim]"
            )
            return 1

        if workspace_id and workspace_input != workspace_id:
            display.print(
                f"[dim]匹配到工作空间: {workspace_input} -> {workspace_id}[/dim]"
            )
    workspace_ids = [
        str(option.get("id", "") or "")
        for option in workspace_options
        if str(option.get("id", "") or "")
    ]
    workspace_options_by_id = {
        str(option.get("id", "") or ""): option
        for option in workspace_options
        if str(option.get("id", "") or "")
    }

    required_nodes = args.nodes
    group_filter = args.group
    all_results = []  # 所有工作空间的结果汇总

    from collections import defaultdict

    for workspace_id in workspace_ids:
        workspace_option = workspace_options_by_id.get(workspace_id, {})
        ws_name = str(workspace_option.get("name", "") or workspace_id)
        cached_resources = get_workspace_resources(workspace_id)
        if not cached_resources or not cached_resources.get("compute_groups"):
            try:
                cached_resources = _load_workspace_resources_for_create(
                    api, display, workspace_id, ws_name
                )
            except QzAPIError as e:
                if cached_resources:
                    display.print(
                        f"[dim]{ws_name} 的资源刷新失败，继续使用缓存资源: {e}[/dim]"
                    )
                else:
                    display.print_warning(f"未能加载工作空间 {ws_name} 的资源信息: {e}")
                    continue
        if not cached_resources:
            display.print_warning(f"未缓存工作空间 {workspace_id} 的资源信息，跳过")
            continue

        compute_groups = cached_resources.get("compute_groups", {})
        specs = cached_resources.get("specs", {})
        ws_name = cached_resources.get("name", "") or ws_name or workspace_id

        # 如果指定了特定计算组
        if group_filter:
            if group_filter.startswith("lcg-"):
                if group_filter in compute_groups:
                    compute_groups = {group_filter: compute_groups[group_filter]}
                else:
                    continue  # 该工作空间没有这个计算组
            else:
                found = find_resource_by_name(
                    workspace_id, "compute_groups", group_filter
                )
                if found:
                    compute_groups = {found["id"]: found}
                else:
                    continue

        if not compute_groups:
            continue

        display.print(
            f"[dim]正在查询 {ws_name} 的 {len(compute_groups)} 个计算组...[/dim]"
        )

        # 低优任务统计（仅在 --lp 参数启用时计算）
        node_low_priority_gpu = defaultdict(int)  # node_name -> low_priority_gpu_count

        if args.low_priority:
            display.print("[dim]正在获取低优任务数据（这可能较慢）...[/dim]")
            low_priority_threshold = 3  # 优先级 <= 3 为低优任务

            try:
                tasks = _with_live_cookie(
                    api,
                    display,
                    lambda live_cookie: _fetch_all_task_dimensions(
                        api,
                        workspace_id,
                        live_cookie,
                        page_size=200,
                    ),
                    workspace_id=workspace_id,
                )

                # 统计每个节点上低优任务占用的 GPU 数
                for task in tasks:
                    priority = task.get("priority", 10)
                    if priority <= low_priority_threshold:
                        gpu_total = task.get("gpu", {}).get("total", 0)
                        nodes_occupied = task.get("nodes_occupied", {}).get("nodes", [])
                        # 平均分配 GPU 到各节点（多节点任务）
                        gpu_per_node = (
                            gpu_total // len(nodes_occupied) if nodes_occupied else 0
                        )
                        for node_name in nodes_occupied:
                            node_low_priority_gpu[node_name] += (
                                gpu_per_node if len(nodes_occupied) > 1 else gpu_total
                            )
            except QzAPIError:
                pass  # 获取任务数据失败不影响主要功能

        for lcg_id, lcg_info in compute_groups.items():
            lcg_name = lcg_info.get("name", lcg_id)
            gpu_type = lcg_info.get("gpu_type", "")

            try:
                nodes = _with_live_cookie(
                    api,
                    display,
                    lambda live_cookie, current_lcg_id=lcg_id: _fetch_all_node_dimensions(
                        api,
                        workspace_id,
                        live_cookie,
                        logic_compute_group_id=current_lcg_id,
                        page_size=1000,
                    ),
                    workspace_id=workspace_id,
                )
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
                    is_schedulable = node_status == "Ready" and not cordon_type

                    gpu_free = max(0, gpu_total - gpu_used)  # 避免负数

                    total_gpus += gpu_total

                    # 只有可调度节点的空闲 GPU 才计入统计
                    if is_schedulable:
                        total_free_gpus += gpu_free

                        # 统计空闲 GPU 分布
                        if gpu_free > 0:
                            gpu_free_distribution[gpu_free] = (
                                gpu_free_distribution.get(gpu_free, 0) + 1
                            )

                        if gpu_used == 0 and gpu_total > 0:
                            free_nodes.append(
                                {
                                    "name": node_name,
                                    "gpu_total": gpu_total,
                                }
                            )

                        # 检查是否为低优空余节点（低优任务占满整节点）
                        low_priority_gpu = node_low_priority_gpu.get(node_name, 0)
                        if low_priority_gpu >= gpu_total and gpu_used > 0:
                            low_priority_free_nodes.append(
                                {
                                    "name": node_name,
                                    "low_priority_gpu": low_priority_gpu,
                                    "gpu_total": gpu_total,
                                }
                            )

                all_results.append(
                    {
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
                    }
                )
            except QzAPIError as e:
                display.print_warning(f"查询 {lcg_name} 失败: {e}")
                continue

    if not all_results:
        display.print_error("未能获取任何计算组的节点信息")
        return 1

    display.print("\n[bold]空余节点汇总[/bold]\n")

    # 如果指定了节点需求，过滤并推荐
    if required_nodes:
        # 按空闲节点数降序排序
        if args.low_priority:
            # 考虑低优空余
            all_results.sort(
                key=lambda x: (
                    x["free_nodes"] + x.get("low_priority_free_nodes", 0),
                    x["free_nodes"],
                ),
                reverse=True,
            )
            available = [
                r
                for r in all_results
                if r["free_nodes"] + r.get("low_priority_free_nodes", 0)
                >= required_nodes
            ]
        else:
            all_results.sort(key=lambda x: x["free_nodes"], reverse=True)
            available = [r for r in all_results if r["free_nodes"] >= required_nodes]

        if not available:
            if args.low_priority:
                display.print(
                    f"[red]没有计算组有 >= {required_nodes} 个可用节点（空闲+低优空余）[/red]\n"
                )
            else:
                display.print(
                    f"[red]没有计算组有 >= {required_nodes} 个空闲节点[/red]\n"
                )
            display.print("当前各计算组节点情况：")
            for r in all_results:
                if args.low_priority:
                    lp_free = r.get("low_priority_free_nodes", 0)
                    display.print(
                        f"  [{r['workspace_name']}] {r['name']}: {r['free_nodes']} 空节点 + {lp_free} 低优空余 [{r['gpu_type']}]"
                    )
                else:
                    display.print(
                        f"  [{r['workspace_name']}] {r['name']}: {r['free_nodes']} 空节点 [{r['gpu_type']}]"
                    )
            return 1

        display.print(f"需要 {required_nodes} 个节点，以下计算组可用：\n")

        for r in available:
            if args.low_priority:
                lp_free = r.get("low_priority_free_nodes", 0)
                total_avail = r["free_nodes"] + lp_free
                display.print(
                    f"[green]✓[/green] [{r['workspace_name']}] [bold]{r['name']}[/bold]  {r['free_nodes']} 空节点 + {lp_free} 低优空余 = {total_avail} 可用 [{r['gpu_type']}]"
                )
            else:
                display.print(
                    f"[green]✓[/green] [{r['workspace_name']}] [bold]{r['name']}[/bold]  {r['free_nodes']} 空节点 [{r['gpu_type']}]"
                )
            display.print(f"  [cyan]{r['id']}[/cyan]")
            # 显示空闲节点列表
            if args.verbose and r.get("free_node_list"):
                node_names = [n["name"] for n in r["free_node_list"]]
                display.print(f"  [dim]空闲节点: {', '.join(node_names)}[/dim]")
            if (
                args.verbose
                and args.low_priority
                and r.get("low_priority_free_node_list")
            ):
                lp_node_names = [n["name"] for n in r["low_priority_free_node_list"]]
                display.print(f"  [dim]低优空余: {', '.join(lp_node_names)}[/dim]")

        # 导出格式
        if args.export:
            display.print("")
            best = available[0]
            display.print(
                f"# 推荐: [{best['workspace_name']}] {best['name']} ({best['free_nodes']} 空节点)"
            )
            display.print(f'WORKSPACE_ID="{best["workspace_id"]}"')
            display.print(f'LOGIC_COMPUTE_GROUP_ID="{best["id"]}"')
            specs = best.get("specs", {})
            if specs:
                spec = list(specs.values())[0]
                display.print(
                    f'SPEC_ID="{spec["id"]}"  # {spec.get("gpu_count", 0)}x {spec.get("gpu_type", "")}'
                )
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

                free_nodes_text = (
                    f"[green]{free_nodes}[/green]" if free_nodes > 0 else "[dim]0[/dim]"
                )
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
                    row.extend(
                        [low_priority_free, r.get("free_nodes", 0) + low_priority_free]
                    )
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
                    display.print(
                        f"  [dim]{prefix} 全空节点: {', '.join(node_names)}[/dim]"
                    )
                    has_detail = True
                if args.low_priority and r.get("low_priority_free_node_list"):
                    lp_node_names = [
                        n["name"] for n in r["low_priority_free_node_list"]
                    ]
                    display.print(
                        f"  [dim]{prefix} 低优空余: {', '.join(lp_node_names)}[/dim]"
                    )
                    has_detail = True
            if not has_detail:
                display.print("  [dim]暂无可展示的详细分布[/dim]")
        display.print("")

        # 导出格式
        if args.export:
            display.print("[bold]导出格式:[/bold]")
            for r in sorted(all_results, key=lambda x: x["free_nodes"], reverse=True):
                if r["free_nodes"] > 0:
                    display.print(
                        f"# [{r['workspace_name']}] {r['name']} ({r['free_nodes']} 空节点)"
                    )
                    display.print(f'WORKSPACE_ID="{r["workspace_id"]}"')
                    display.print(f'LOGIC_COMPUTE_GROUP_ID="{r["id"]}"')

    # HPC 节点 CPU/内存利用率汇总
    hpc_any = False
    lcg_filter = (
        group_filter if group_filter and group_filter.startswith("lcg-") else None
    )
    for workspace_id in workspace_ids:
        cached = get_workspace_resources(workspace_id)
        workspace_option = workspace_options_by_id.get(workspace_id, {})
        ws_label = (cached or {}).get("name", "") or str(
            workspace_option.get("name", "") or workspace_id
        )
        try:
            hpc_nodes = [
                node
                for node in _with_live_cookie(
                    api,
                    display,
                    lambda live_cookie: _fetch_all_node_dimensions(
                        api,
                        workspace_id,
                        live_cookie,
                        logic_compute_group_id=lcg_filter,
                        page_size=200,
                    ),
                    workspace_id=workspace_id,
                )
                if node.get("node_type") == "hpc"
            ]
            if not hpc_nodes:
                continue
            if not hpc_any:
                display.print("\n[bold]HPC 节点 CPU/内存利用率[/bold]")
                hpc_any = True
            total_hpc = len(hpc_nodes)
            cpu_rates = [n.get("cpu", {}).get("usage_rate", 0) for n in hpc_nodes]
            mem_rates = [n.get("memory", {}).get("usage_rate", 0) for n in hpc_nodes]
            avg_cpu = sum(cpu_rates) / total_hpc * 100
            avg_mem = sum(mem_rates) / total_hpc * 100
            busy = sum(1 for r in cpu_rates if r > 0.05)
            display.print(
                f"  {ws_label}: 节点 {total_hpc} | 忙碌 {busy} "
                f"| 平均CPU [cyan]{avg_cpu:.1f}%[/cyan] "
                f"| 平均MEM [cyan]{avg_mem:.1f}%[/cyan]"
            )
        except Exception:
            pass
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
        workspace_ids = [
            (ws_id, data.get("name", "")) for ws_id, data in all_resources.items()
        ]
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
                data = api.list_task_dimension(
                    workspace_id, cookie, page_num=page_num, page_size=page_size
                )
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
            type_stats = defaultdict(
                lambda: {"count": 0, "gpu": 0}
            )  # type -> {count, gpu}
            priority_stats = defaultdict(
                lambda: {"count": 0, "gpu": 0}
            )  # priority -> {count, gpu}
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
                    workspace_id, list(projects_found.values()), ws_name
                )
                if new_count > 0:
                    display.print(
                        f"[dim]发现 {new_count} 个新项目，已更新到本地缓存[/dim]"
                    )

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
                        workspace_id, list(compute_groups_found.values()), ws_name
                    )
                    if new_cg_count > 0:
                        display.print(
                            f"[dim]发现 {new_cg_count} 个新计算组，已更新到本地缓存[/dim]"
                        )
            except QzAPIError:
                pass  # 忽略节点查询失败，不影响主要功能

            all_stats.append(
                {
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
                }
            )

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
        display.print(
            f"运行中: {stats['total_tasks']} 个任务, 共 {stats['total_gpu']} GPU\n"
        )

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
            for task_type, info in sorted(
                type_stats.items(), key=lambda x: -x[1]["gpu"]
            ):
                type_display = type_names.get(task_type, task_type)
                display.print(
                    f"  {type_display:<20} {info['count']:>4} 任务  {info['gpu']:>5} GPU"
                )

        # 按优先级统计（可选）
        if args.by_priority:
            display.print("\n[bold]按优先级统计:[/bold]")
            priority_stats = stats["priority_stats"]
            for priority, info in sorted(priority_stats.items(), key=lambda x: -x[0]):
                display.print(
                    f"  优先级 {priority:<10} {info['count']:>4} 任务  {info['gpu']:>5} GPU"
                )

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
        avg_gpu_usage = (
            sum(t.get("gpu", {}).get("usage_rate", 0) for t in tasks) / len(tasks) * 100
            if tasks
            else 0
        )

        title = "工作空间任务概览"
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
            display.print(
                f"     [{gpu_color}]{gpu_total} GPU ({gpu_usage:.0f}%)[/{gpu_color}] | CPU {cpu_usage:.0f}% | MEM {mem_usage:.0f}% | {running_time} | {user_name}"
            )
            display.print(
                f"     [dim]{project_name} | {nodes_count} 节点: {', '.join(nodes_list[:3])}{'...' if len(nodes_list) > 3 else ''}[/dim]"
            )
            display.print(f"     [dim]{job_id}[/dim]")
            display.print("")

        return 0

    except QzAPIError as e:
        if "401" in str(e) or "过期" in str(e):
            display.print_error(
                "Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file> -w <workspace_id>"
            )
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


def _first_non_empty(*values):
    """返回第一个非空值。"""
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _collect_non_empty_strings(*values: Any) -> List[str]:
    """收集并去重非空字符串，保持原有顺序。"""
    result: List[str] = []
    seen = set()

    for value in values:
        if value is None:
            continue
        items = value if isinstance(value, (list, tuple, set)) else [value]
        for item in items:
            if item is None:
                continue
            text = str(item).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            result.append(text)

    return result


def _normalize_logic_compute_group_ids(
    item: Dict[str, Any], fallback_compute_group_id: str = ""
) -> List[str]:
    """规范 spec 关联的逻辑计算组 ID 列表。"""
    return _collect_non_empty_strings(
        item.get("logic_compute_group_ids"),
        item.get("logic_compute_group_id"),
        fallback_compute_group_id,
    )


def _merge_resource_lists(
    existing_items: List[Dict[str, Any]], new_items: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """按资源 ID 合并列表，尽量保留已有的非空字段。"""
    merged: Dict[str, Dict[str, Any]] = {}

    def upsert(item: Dict[str, Any]) -> None:
        item_id = _first_non_empty(
            item.get("id"),
            item.get("quota_id"),
            item.get("spec_id"),
            item.get("predef_quota_id"),
        )
        if not item_id:
            return

        existing = merged.get(str(item_id), {}).copy()
        for key, value in item.items():
            if value is None:
                continue
            if key == "logic_compute_group_ids":
                existing[key] = _collect_non_empty_strings(existing.get(key), value)
                continue
            if isinstance(value, str) and not value.strip() and existing.get(key):
                continue
            existing[key] = value
        logic_compute_group_ids = _normalize_logic_compute_group_ids(existing)
        if logic_compute_group_ids:
            existing["logic_compute_group_ids"] = logic_compute_group_ids
            existing["logic_compute_group_id"] = logic_compute_group_ids[0]
        existing["id"] = str(item_id)
        merged[str(item_id)] = existing

    for item in existing_items or []:
        upsert(item)
    for item in new_items or []:
        upsert(item)

    return sorted(
        merged.values(),
        key=lambda item: (
            str(item.get("name") or item.get("id") or "").lower(),
            str(item.get("id") or ""),
        ),
    )


def _normalize_spec_item(
    spec: Dict[str, Any], fallback_compute_group_id: str = ""
) -> Optional[Dict[str, Any]]:
    """将不同来源的 spec 字段规范到统一结构。"""
    gpu_info = spec.get("gpu_info") if isinstance(spec.get("gpu_info"), dict) else {}
    spec_id = _first_non_empty(
        spec.get("id"),
        spec.get("quota_id"),
        spec.get("spec_id"),
        spec.get("predef_quota_id"),
    )
    if not spec_id:
        return None

    logic_compute_group_ids = _normalize_logic_compute_group_ids(
        spec, fallback_compute_group_id
    )
    return {
        "id": str(spec_id),
        "name": _first_non_empty(
            spec.get("name"),
            spec.get("display_name"),
            spec.get("quota_name"),
            str(spec_id),
        ),
        "logic_compute_group_id": (
            logic_compute_group_ids[0] if logic_compute_group_ids else ""
        ),
        "logic_compute_group_ids": logic_compute_group_ids,
        "gpu_count": _first_non_empty(
            spec.get("gpu_count"), spec.get("gpu_num"), spec.get("gpu"), 0
        )
        or 0,
        "cpu_count": _first_non_empty(spec.get("cpu_count"), spec.get("cpu"), 0) or 0,
        "memory_gb": _first_non_empty(
            spec.get("memory_gb"),
            spec.get("memory_size_gib"),
            spec.get("mem_gi"),
            spec.get("memory"),
            0,
        )
        or 0,
        "gpu_type": _first_non_empty(
            spec.get("gpu_type"),
            spec.get("resource_type"),
            gpu_info.get("gpu_product_simple"),
            "",
        )
        or "",
        "gpu_type_display": _first_non_empty(
            spec.get("gpu_type_display"),
            gpu_info.get("gpu_type_display"),
            "",
        )
        or "",
    }


def _scope_specs_to_compute_group(
    specs: List[Dict[str, Any]],
    compute_group_id: str,
    workspace_compute_groups: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """仅保留当前逻辑计算组可用的 spec，避免跨组串用缓存。"""
    if not compute_group_id:
        return list(specs)

    known_compute_group_ids = _collect_non_empty_strings(
        [
            item.get("id", cache_key)
            for cache_key, item in (workspace_compute_groups or {}).items()
            if isinstance(item, dict)
        ]
    )
    can_infer_legacy_scope = (
        len(known_compute_group_ids) == 1
        and known_compute_group_ids[0] == compute_group_id
    )

    scoped_specs: List[Dict[str, Any]] = []
    for spec in specs:
        logic_compute_group_ids = _normalize_logic_compute_group_ids(spec)
        if logic_compute_group_ids:
            if compute_group_id in logic_compute_group_ids:
                scoped_specs.append(spec)
            continue
        if not can_infer_legacy_scope:
            continue
        inferred_spec = dict(spec)
        inferred_spec["logic_compute_group_id"] = compute_group_id
        inferred_spec["logic_compute_group_ids"] = [compute_group_id]
        scoped_specs.append(inferred_spec)

    return scoped_specs


def _resolve_cached_resource_value(
    workspace_id: str,
    resource_type: str,
    value: str,
    workspace_resources: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """优先按缓存中的 ID/名称解析资源值。"""
    if not value:
        return None, None

    resources = (
        workspace_resources or get_workspace_resources(workspace_id) or {}
    ).get(resource_type, {})
    resource = resources.get(value)
    if resource:
        return resource["id"], resource.get("name", resource["id"])

    found = find_resource_by_name(workspace_id, resource_type, value)
    if found:
        return found["id"], found.get("name", value)

    if resource_type == "specs" and (value.count("-") >= 4 or len(value) > 20):
        return value, value

    prefixes = {"projects": "project-", "compute_groups": "lcg-"}
    prefix = prefixes.get(resource_type, "")
    if prefix and value.startswith(prefix):
        return value, value

    return None, None


def _validate_cached_resource_membership(
    workspace_id: str,
    resource_type: str,
    resource_id: str,
    workspace_resources: Optional[Dict[str, Any]] = None,
) -> Optional[bool]:
    """基于本地缓存校验资源是否属于当前 workspace。"""
    if not resource_id:
        return None

    resources = (
        workspace_resources or get_workspace_resources(workspace_id) or {}
    ).get(resource_type, {})
    if not resources:
        return None
    return resource_id in resources


def _validate_cached_spec_membership(
    workspace_id: str,
    compute_group_id: str,
    spec_id: str,
    workspace_resources: Optional[Dict[str, Any]] = None,
) -> Optional[bool]:
    """基于缓存校验 spec 是否属于当前 compute group。"""
    if not spec_id or not compute_group_id:
        return None

    ws_resources = workspace_resources or get_workspace_resources(workspace_id) or {}
    cached_specs = [
        normalized
        for spec in ws_resources.get("specs", {}).values()
        for normalized in [_normalize_spec_item(spec)]
        if normalized
    ]
    if not cached_specs:
        return None

    all_spec_ids = {item["id"] for item in cached_specs}
    if spec_id not in all_spec_ids:
        return False

    scoped_specs = _scope_specs_to_compute_group(
        cached_specs,
        compute_group_id,
        ws_resources.get("compute_groups", {}),
    )
    if not scoped_specs:
        return None
    return any(item["id"] == spec_id for item in scoped_specs)


def _get_cookie_value() -> str:
    """返回已保存的 cookie 值。"""
    cookie_data = get_cookie()
    return (cookie_data or {}).get("cookie", "")


def _is_auth_related_error(error: Exception) -> bool:
    """判断是否为 cookie/token 失效类错误。"""
    message = str(error)
    keywords = ("401", "过期", "无效", "Cookie 已", "cookie 是否正确")
    return any(keyword in message for keyword in keywords)


def _refresh_cookie_for_interactive(api, display, workspace_id: str = "") -> str:
    """使用已保存的 CAS 凭证自动刷新 cookie。"""
    if not hasattr(api, "login_with_cas"):
        return ""

    username, password = get_credentials()
    if not username or not password:
        return ""

    display.print("[dim]检测到登录态失效，正在自动刷新 cookie...[/dim]")
    cookie = api.login_with_cas(username, password)
    saved = get_cookie() or {}
    save_cookie(cookie, workspace_id=workspace_id or saved.get("workspace_id", ""))
    return cookie


def _with_live_cookie(api, display, fn, workspace_id: str = ""):
    """执行依赖 cookie 的请求，必要时自动刷新 cookie 后重试一次。"""
    cookie = _get_cookie_value()
    refreshed = False

    while True:
        if not cookie:
            if refreshed:
                raise QzAPIError("未找到有效 cookie，且无法自动刷新")
            cookie = _refresh_cookie_for_interactive(
                api, display, workspace_id=workspace_id
            )
            if not cookie:
                raise QzAPIError("未设置 cookie，且未配置可用的 CAS 账号密码")
            refreshed = True

        try:
            return fn(cookie)
        except QzAPIError as e:
            if refreshed or not _is_auth_related_error(e):
                raise
            cookie = _refresh_cookie_for_interactive(
                api, display, workspace_id=workspace_id
            )
            if not cookie:
                raise
            refreshed = True


def _fetch_all_node_dimensions(
    api,
    workspace_id: str,
    cookie: str,
    logic_compute_group_id: Optional[str] = None,
    compute_group_id: Optional[str] = None,
    page_size: int = 500,
) -> List[Dict[str, Any]]:
    """分页获取节点维度数据。"""
    nodes: List[Dict[str, Any]] = []
    page_num = 1

    while True:
        data = api.list_node_dimension(
            workspace_id,
            cookie,
            logic_compute_group_id=logic_compute_group_id,
            compute_group_id=compute_group_id,
            page_num=page_num,
            page_size=page_size,
        )
        batch = data.get("node_dimensions", [])
        nodes.extend(batch)
        if len(batch) < page_size:
            break
        page_num += 1

    return nodes


def _fetch_all_jobs_with_cookie(
    api,
    workspace_id: str,
    cookie: str,
    *,
    page_size: int = 200,
    created_by: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """分页获取 workspace 内所有任务。"""
    jobs: List[Dict[str, Any]] = []
    page_num = 1

    while True:
        data = api.list_jobs_with_cookie(
            workspace_id,
            cookie,
            page_num=page_num,
            page_size=page_size,
            created_by=created_by,
        )
        batch = data.get("jobs", [])
        jobs.extend(batch)

        total = data.get("total")
        if isinstance(total, int) and total >= 0:
            if len(jobs) >= total or not batch:
                break
        elif len(batch) < page_size:
            break
        page_num += 1

    return jobs


def _fetch_all_task_dimensions(
    api,
    workspace_id: str,
    cookie: str,
    project_id: Optional[str] = None,
    *,
    page_size: int = 200,
) -> List[Dict[str, Any]]:
    """分页获取 workspace 内所有 task dimensions。"""
    tasks: List[Dict[str, Any]] = []
    page_num = 1

    while True:
        data = api.list_task_dimension(
            workspace_id,
            cookie,
            project_id=project_id,
            page_num=page_num,
            page_size=page_size,
        )
        batch = data.get("task_dimensions", [])
        tasks.extend(batch)

        total = data.get("total")
        if isinstance(total, int) and total >= 0:
            if len(tasks) >= total or not batch:
                break
        elif len(batch) < page_size:
            break
        page_num += 1

    return tasks


def _summarize_node_capacity(nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
    """汇总节点实时容量，用于交互式选择时展示占用情况。"""
    total_nodes = 0
    schedulable_nodes = 0
    free_nodes = 0
    total_gpus = 0
    free_gpus = 0

    for node in nodes:
        gpu_info = node.get("gpu", {})
        gpu_total = gpu_info.get("total", 0)
        gpu_used = gpu_info.get("used", 0)
        if gpu_total <= 0:
            continue

        total_nodes += 1
        total_gpus += gpu_total

        is_schedulable = node.get("status", "") == "Ready" and not node.get(
            "cordon_type", ""
        )
        if not is_schedulable:
            continue

        schedulable_nodes += 1
        node_free_gpus = max(0, gpu_total - gpu_used)
        free_gpus += node_free_gpus
        if gpu_used == 0:
            free_nodes += 1

    used_gpus = max(0, total_gpus - free_gpus)
    gpu_util_ratio = (used_gpus / total_gpus) if total_gpus > 0 else None
    return {
        "total_nodes": total_nodes,
        "schedulable_nodes": schedulable_nodes,
        "free_nodes": free_nodes,
        "total_gpus": total_gpus,
        "free_gpus": free_gpus,
        "gpu_util_ratio": gpu_util_ratio,
    }


def _load_workspace_usage_snapshot(api, display, workspace_id: str) -> Dict[str, Any]:
    """加载 workspace 及各 compute group 的实时占用快照。"""
    nodes = _with_live_cookie(
        api,
        display,
        lambda cookie: _fetch_all_node_dimensions(api, workspace_id, cookie),
        workspace_id=workspace_id,
    )
    workspace_summary = _summarize_node_capacity(nodes)

    compute_group_nodes: Dict[str, List[Dict[str, Any]]] = {}
    for node in nodes:
        lcg = node.get("logic_compute_group", {})
        lcg_id = _first_non_empty(lcg.get("id"), node.get("logic_compute_group_id"))
        if not lcg_id:
            continue
        compute_group_nodes.setdefault(str(lcg_id), []).append(node)

    compute_group_summaries = {
        lcg_id: _summarize_node_capacity(group_nodes)
        for lcg_id, group_nodes in compute_group_nodes.items()
    }

    return {
        "workspace": workspace_summary,
        "compute_groups": compute_group_summaries,
    }


def _collect_workspace_resources_from_live_apis(
    api,
    workspace_id: str,
    cookie: str,
) -> Tuple[Dict[str, Any], int]:
    """从任务、task_dimension、cluster_info 等接口聚合 workspace 资源。"""
    jobs = _fetch_all_jobs_with_cookie(api, workspace_id, cookie, page_size=200)
    resources = (
        api.extract_resources_from_jobs(jobs)
        if jobs
        else {
            "projects": [],
            "compute_groups": [],
            "specs": [],
        }
    )

    if hasattr(api, "list_task_dimension"):
        try:
            tasks = _fetch_all_task_dimensions(api, workspace_id, cookie, page_size=200)
            task_projects = []
            for task in tasks:
                proj = task.get("project", {})
                proj_id = proj.get("id", "")
                if not proj_id:
                    continue
                task_projects.append(
                    {
                        "id": proj_id,
                        "name": proj.get("name", ""),
                        "workspace_id": workspace_id,
                    }
                )
            resources["projects"] = _merge_resource_lists(
                resources.get("projects", []), task_projects
            )
        except QzAPIError:
            pass

    cluster_info_failed = False
    try:
        cluster_info = api.get_cluster_basic_info(workspace_id, cookie)
        compute_groups_from_api = []
        for cluster in cluster_info.get("compute_groups", []):
            for lcg in cluster.get("logic_compute_groups", []):
                lcg_id = lcg.get("logic_compute_group_id", "")
                if not lcg_id:
                    continue
                resource_types = lcg.get("resource_types", [])
                compute_groups_from_api.append(
                    {
                        "id": lcg_id,
                        "name": lcg.get("logic_compute_group_name", ""),
                        "compute_group_id": cluster.get("compute_group_id", ""),
                        "compute_group_name": cluster.get("compute_group_name", ""),
                        "cluster_id": cluster.get("cluster_id", ""),
                        "gpu_type": _first_non_empty(
                            lcg.get("brand"),
                            resource_types[0] if resource_types else "",
                            "",
                        ),
                        "workspace_id": workspace_id,
                    }
                )
        resources["compute_groups"] = _merge_resource_lists(
            resources.get("compute_groups", []),
            compute_groups_from_api,
        )
    except QzAPIError:
        cluster_info_failed = True

    if cluster_info_failed or not resources.get("compute_groups"):
        try:
            nodes = _fetch_all_node_dimensions(api, workspace_id, cookie, page_size=500)
            compute_groups_from_nodes = []
            for node in nodes:
                lcg = node.get("logic_compute_group", {})
                lcg_id = lcg.get("id", "")
                if not lcg_id:
                    continue
                gpu_info = node.get("gpu_info", {})
                compute_groups_from_nodes.append(
                    {
                        "id": lcg_id,
                        "name": lcg.get("name", ""),
                        "gpu_type": gpu_info.get("gpu_product_simple", ""),
                        "workspace_id": workspace_id,
                    }
                )
            resources["compute_groups"] = _merge_resource_lists(
                resources.get("compute_groups", []),
                compute_groups_from_nodes,
            )
        except QzAPIError:
            pass

    return resources, len(jobs)


def _load_compute_group_usage_snapshot(
    api, display, workspace_id: str, compute_groups: List[Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """按 compute group 查询实时占用，并映射回逻辑计算组。"""
    usage_by_filter: Dict[str, Dict[str, Any]] = {}
    usage_by_logic_group: Dict[str, Dict[str, Any]] = {}
    filter_counts: Dict[str, int] = {}

    for group in compute_groups:
        logic_group_id = str(group.get("id", ""))
        physical_group_id = str(group.get("compute_group_id", ""))
        if not logic_group_id:
            continue
        filter_key = physical_group_id or logic_group_id
        filter_counts[filter_key] = filter_counts.get(filter_key, 0) + 1

    for group in compute_groups:
        logic_group_id = str(group.get("id", ""))
        physical_group_id = str(group.get("compute_group_id", ""))
        if not logic_group_id:
            continue

        filter_key = physical_group_id or logic_group_id
        if filter_key not in usage_by_filter:
            try:
                nodes = _with_live_cookie(
                    api,
                    display,
                    lambda cookie, lcg_id=logic_group_id, cg_id=physical_group_id: _fetch_all_node_dimensions(
                        api,
                        workspace_id,
                        cookie,
                        logic_compute_group_id=None if cg_id else lcg_id,
                        compute_group_id=cg_id or None,
                    ),
                    workspace_id=workspace_id,
                )
                usage_by_filter[filter_key] = _summarize_node_capacity(nodes)
            except QzAPIError:
                usage_by_filter[filter_key] = {}

        usage = dict(usage_by_filter.get(filter_key, {}))
        if physical_group_id and filter_counts.get(filter_key, 0) > 1:
            usage["usage_scope"] = "shared_physical_pool"
            usage["shared_logic_group_count"] = filter_counts[filter_key]
            usage["shared_compute_group_name"] = group.get("compute_group_name", "")
        usage_by_logic_group[logic_group_id] = usage

    return usage_by_logic_group


def _build_compute_group_options_with_usage(
    api,
    display,
    workspace_id: str,
    compute_group_items: List[Dict[str, Any]],
    workspace_usage_snapshot: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """为计算组补齐占用信息，优先复用 workspace 级快照。"""
    if not compute_group_items:
        return []

    workspace_usage_by_group = {}
    if workspace_usage_snapshot:
        workspace_usage_by_group = (
            workspace_usage_snapshot.get("compute_groups", {}) or {}
        )

    physical_group_ids = [
        str(item.get("compute_group_id", "") or "")
        for item in compute_group_items
        if str(item.get("compute_group_id", "") or "")
    ]
    has_shared_physical_pool = len(physical_group_ids) != len(set(physical_group_ids))
    can_reuse_workspace_snapshot = (
        bool(workspace_usage_by_group)
        and not has_shared_physical_pool
        and all(
            str(item.get("id", "")) in workspace_usage_by_group
            for item in compute_group_items
            if item.get("id")
        )
    )

    compute_group_usage = (
        workspace_usage_by_group
        if can_reuse_workspace_snapshot
        else _load_compute_group_usage_snapshot(
            api, display, workspace_id, compute_group_items
        )
    )
    return _sort_compute_group_options_for_selection(
        [
            {
                **item,
                **compute_group_usage.get(item.get("id", ""), {}),
            }
            for item in compute_group_items
        ]
    )


def _sort_project_options_for_selection(
    options: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """按名称稳定排序项目选项。"""
    return sorted(
        options,
        key=lambda item: (
            str(item.get("name") or item.get("id") or "").lower(),
            str(item.get("id") or ""),
        ),
    )


def _build_cached_spec_result(
    workspace_resources: Optional[Dict[str, Any]],
    compute_group_id: str,
) -> Dict[str, Any]:
    """仅基于已缓存资源构造 spec 结果，不触发实时查询。"""
    ws_resources = workspace_resources or {}
    all_cached_specs = [
        normalized
        for spec in ws_resources.get("specs", {}).values()
        for normalized in [_normalize_spec_item(spec)]
        if normalized
    ]
    cached_specs = _scope_specs_to_compute_group(
        all_cached_specs,
        compute_group_id,
        ws_resources.get("compute_groups", {}),
    )
    return {
        "items": list(cached_specs),
        "status": "cache" if cached_specs else "empty",
        "error": None,
    }


def _resolve_workspace_option_from_snapshot(
    workspace_options: List[Dict[str, Any]],
    workspace_value: str,
) -> Tuple[Optional[str], str]:
    """仅基于已预加载的 workspace 快照解析名称或 ID。"""
    if not workspace_value:
        return None, ""

    for option in workspace_options:
        option_id = str(option.get("id", "") or "")
        option_name = str(option.get("name", "") or option_id)
        if not option_id:
            continue
        if workspace_value == option_id:
            return option_id, option_name or option_id

    lowered = workspace_value.lower()
    exact_match: Optional[Dict[str, Any]] = None
    fuzzy_matches: List[Dict[str, Any]] = []
    for option in workspace_options:
        option_id = str(option.get("id", "") or "")
        option_name = str(option.get("name", "") or option_id)
        if not option_id:
            continue
        if option_name == workspace_value:
            exact_match = option
            break
        if option_name and lowered in option_name.lower():
            fuzzy_matches.append(option)

    matched = exact_match or (fuzzy_matches[0] if len(fuzzy_matches) == 1 else None)
    if not matched:
        return None, ""
    matched_id = str(matched.get("id", "") or "")
    matched_name = str(matched.get("name", "") or matched_id)
    return matched_id or None, matched_name


def _load_create_interactive_snapshot_if_available() -> Optional[Dict[str, Any]]:
    """读取并清洗 create -i 交互快照；不存在时返回 None。"""
    snapshot = load_create_interactive_snapshot() or {}
    workspace_options = list(snapshot.get("workspace_options") or [])
    workspace_details = snapshot.get("workspace_details_by_id") or {}
    for workspace_detail in workspace_details.values():
        if not isinstance(workspace_detail, dict):
            continue
        spec_result_by_compute_group = (
            workspace_detail.get("spec_result_by_compute_group") or {}
        )
        for compute_group_id, spec_result in spec_result_by_compute_group.items():
            if not isinstance(spec_result, dict):
                continue
            error_message = str(spec_result.get("error", "") or "")
            if not _is_unsupported_spec_listing_error(error_message):
                continue
            spec_items = list(spec_result.get("items") or [])
            spec_result["error"] = None
            spec_result["status"] = "cache" if spec_items else "empty"
    if workspace_options and workspace_details:
        return snapshot

    return None


def _load_required_create_interactive_snapshot(display) -> Optional[Dict[str, Any]]:
    """读取 create -i 所需快照。"""
    snapshot = _load_create_interactive_snapshot_if_available()
    if snapshot is not None:
        return snapshot

    display.print_error("未找到 create -i 所需的资源快照")
    display.print(
        "[dim]create -i 将按需预加载资源快照；如需提前热身，可先执行一次 qzcli create --interactive[/dim]"
    )
    return None


def _prefetch_create_interactive_snapshot_on_demand(
    api,
    display,
    workspace_value: str = "",
) -> Optional[Dict[str, Any]]:
    """create -i 缺少可用快照时，按需预加载并落盘。"""
    locked_workspace_id = ""
    locked_ws_display = ""

    if workspace_value:
        locked_workspace_id, locked_ws_display = _resolve_workspace_value(
            api, display, workspace_value
        )
        if not locked_workspace_id and workspace_value.startswith("ws-"):
            ws_resources = get_workspace_resources(workspace_value) or {}
            locked_workspace_id = workspace_value
            locked_ws_display = ws_resources.get("name", workspace_value)

    snapshot = _prefetch_create_interactive_snapshot(
        api,
        display,
        locked_workspace_id=locked_workspace_id,
        locked_ws_display=locked_ws_display,
    )
    if not snapshot.get("workspace_options") or not snapshot.get(
        "workspace_details_by_id"
    ):
        return None
    save_create_interactive_snapshot(snapshot)
    return snapshot


def _prefetch_create_interactive_snapshot(
    api,
    display,
    *,
    workspace_options: Optional[List[Dict[str, Any]]] = None,
    locked_workspace_id: str = "",
    locked_ws_display: str = "",
) -> Dict[str, Any]:
    """启动时一次性预加载 create -i 所需的交互快照。"""
    display.print(
        "[dim]正在一次性预加载 create -i 资源快照，后续选择将只使用这次查询结果...[/dim]"
    )

    resolved_workspace_options: List[Dict[str, Any]] = []
    if workspace_options is not None:
        resolved_workspace_options = list(workspace_options)
    elif locked_workspace_id:
        ws_resources = get_workspace_resources(locked_workspace_id) or {}
        resolved_workspace_options = [
            {
                "id": locked_workspace_id,
                "name": locked_ws_display
                or ws_resources.get("name", locked_workspace_id),
            }
        ]
    else:
        resolved_workspace_options = _sort_workspace_options_for_selection(
            _list_available_workspaces(api, display)
        )

    snapshot = {
        "workspace_options": list(resolved_workspace_options),
        "workspace_details_by_id": {},
    }
    total_workspaces = len(resolved_workspace_options)
    if total_workspaces == 0:
        return snapshot

    for idx, workspace_option in enumerate(resolved_workspace_options, 1):
        ws_id = str(workspace_option.get("id", ""))
        ws_name = str(workspace_option.get("name", "") or ws_id)
        if not ws_id:
            continue

        display.print(
            f"[dim]预加载 [{idx}/{total_workspaces}] {ws_name} 的项目 / 计算组 / 规格快照...[/dim]"
        )

        try:
            ws_resources = (
                _load_workspace_resources_for_create(
                    api,
                    display,
                    ws_id,
                    ws_name,
                    force_refresh=True,
                )
                or {}
            )
        except QzAPIError as e:
            ws_resources = get_workspace_resources(ws_id) or {}
            if ws_resources:
                display.print(
                    f"[dim]{ws_name} 的资源刷新失败，继续使用缓存快照: {e}[/dim]"
                )
            else:
                raise

        project_options = _sort_project_options_for_selection(
            list((ws_resources or {}).get("projects", {}).values())
        )
        compute_group_items = list(
            (ws_resources or {}).get("compute_groups", {}).values()
        )

        try:
            compute_group_options = _build_compute_group_options_with_usage(
                api,
                display,
                ws_id,
                compute_group_items,
                workspace_usage_snapshot=workspace_option.get("_usage_snapshot"),
            )
        except QzAPIError as e:
            display.print(
                f"[dim]{ws_name} 的计算组占用刷新失败，继续使用缓存快照: {e}[/dim]"
            )
            compute_group_options = _sort_compute_group_options_for_selection(
                compute_group_items
            )

        spec_result_by_compute_group: Dict[str, Dict[str, Any]] = {}
        for compute_group in compute_group_options:
            compute_group_id = str(compute_group.get("id", ""))
            if not compute_group_id:
                continue
            spec_result_by_compute_group[compute_group_id] = (
                _load_specs_for_create_result(
                    api,
                    ws_id,
                    ws_name,
                    compute_group_id,
                    display=display,
                    emit_messages=False,
                )
            )

        ws_resources = get_workspace_resources(ws_id) or ws_resources
        compute_group_options = [
            {
                **item,
                "spec_status": str(
                    (
                        spec_result_by_compute_group.get(str(item.get("id", ""))) or {}
                    ).get("status", item.get("spec_status", "unprobed"))
                    or "unprobed"
                ),
            }
            for item in compute_group_options
        ]

        snapshot["workspace_details_by_id"][ws_id] = {
            "id": ws_id,
            "name": ws_name,
            "resources": ws_resources
            or {
                "projects": {},
                "compute_groups": {},
                "specs": {},
            },
            "project_options": project_options,
            "compute_group_options": compute_group_options,
            "spec_result_by_compute_group": spec_result_by_compute_group,
        }

    display.print(
        "[dim]交互式资源快照预加载完成，后续层级选择不会再触发实时查询。[/dim]"
    )
    return snapshot


def _has_capacity_summary(option: Dict[str, Any]) -> bool:
    """判断候选项是否携带实时容量摘要。"""
    return any(
        option.get(key, 0)
        for key in ("total_nodes", "total_gpus", "free_nodes", "free_gpus")
    )


def _format_capacity_summary(option: Dict[str, Any]) -> str:
    """将容量摘要格式化为短文本。"""
    parts = []
    total_nodes = option.get("total_nodes", 0)
    free_nodes = option.get("free_nodes", 0)
    total_gpus = option.get("total_gpus", 0)
    free_gpus = option.get("free_gpus", 0)
    gpu_util_ratio = option.get("gpu_util_ratio")

    if total_nodes:
        parts.append(f"空节点 {free_nodes}/{total_nodes}")
    if total_gpus:
        parts.append(f"空GPU {free_gpus}/{total_gpus}")
    if gpu_util_ratio is not None:
        parts.append(f"GPU利用率 {gpu_util_ratio * 100:.1f}%")

    return " | ".join(parts)


def _list_available_workspaces(api, display) -> List[Dict[str, Any]]:
    """优先从当前可访问 workspace API 获取工作空间，失败时回退到本地缓存。"""
    workspaces: List[Dict[str, Any]] = []

    try:
        workspaces = _with_live_cookie(
            api, display, lambda cookie: api.list_workspaces(cookie)
        )
        for ws in workspaces:
            ws_id = ws.get("id", "")
            ws_name = ws.get("name", "")
            if ws_id:
                set_workspace_name(ws_id, ws_name)
                try:
                    usage_snapshot = _load_workspace_usage_snapshot(api, display, ws_id)
                    ws.update(usage_snapshot.get("workspace", {}))
                    if usage_snapshot.get("compute_groups"):
                        ws["_usage_snapshot"] = usage_snapshot
                except QzAPIError as e:
                    if not _is_auth_related_error(e):
                        display.print(
                            f"[dim]{ws_name or ws_id} 的实时占用获取失败，使用缓存列表: {e}[/dim]"
                        )
    except QzAPIError as e:
        cached = list_cached_workspaces()
        if cached:
            if not _is_auth_related_error(e):
                display.print(f"[dim]获取当前工作空间列表失败，使用本地缓存: {e}[/dim]")
        else:
            raise

    if not workspaces:
        cached = list_cached_workspaces()
        workspaces = [{"id": ws["id"], "name": ws.get("name", "")} for ws in cached]

    return sorted(
        workspaces,
        key=lambda item: (
            str(item.get("name") or item.get("id") or "").lower(),
            str(item.get("id") or ""),
        ),
    )


def _resolve_workspace_value(
    api, display, workspace_value: str
) -> Tuple[Optional[str], str]:
    """将 workspace 名称或 ID 解析为 ID。"""
    if not workspace_value:
        return None, ""

    if workspace_value.startswith("ws-"):
        ws_resources = get_workspace_resources(workspace_value)
        return workspace_value, (ws_resources or {}).get("name", workspace_value)

    workspace_id = find_workspace_by_name(workspace_value)
    if workspace_id:
        ws_resources = get_workspace_resources(workspace_id)
        return workspace_id, (ws_resources or {}).get("name", workspace_value)

    try:
        for ws in _list_available_workspaces(api, display):
            ws_name = ws.get("name", "")
            if ws_name == workspace_value or workspace_value.lower() in ws_name.lower():
                ws_id = ws.get("id", "")
                if ws_id:
                    set_workspace_name(ws_id, ws_name)
                    return ws_id, ws_name or ws_id
    except QzAPIError:
        pass

    return None, ""


def _refresh_workspace_resources_for_create(
    api, display, workspace_id: str, ws_name: str = ""
) -> Optional[Dict[str, Any]]:
    """为 create 交互模式刷新单个 workspace 的资源缓存。"""
    cached_resources = get_workspace_resources(workspace_id) or {}

    try:
        resources, _ = _with_live_cookie(
            api,
            display,
            lambda cookie: _collect_workspace_resources_from_live_apis(
                api, workspace_id, cookie
            ),
            workspace_id=workspace_id,
        )
    except QzAPIError:
        if cached_resources:
            return cached_resources
        raise

    merged_resources = {
        "projects": _merge_resource_lists(
            list(cached_resources.get("projects", {}).values()),
            resources.get("projects", []),
        ),
        "compute_groups": _merge_resource_lists(
            list(cached_resources.get("compute_groups", {}).values()),
            resources.get("compute_groups", []),
        ),
        "specs": _merge_resource_lists(
            list(cached_resources.get("specs", {}).values()),
            resources.get("specs", []),
        ),
    }
    save_resources(
        workspace_id, merged_resources, ws_name or cached_resources.get("name", "")
    )
    return get_workspace_resources(workspace_id)


def _load_workspace_resources_for_create(
    api, display, workspace_id: str, ws_name: str = "", force_refresh: bool = False
) -> Optional[Dict[str, Any]]:
    """获取 workspace 资源缓存，不足时尝试刷新。"""
    cached_resources = get_workspace_resources(workspace_id)
    has_projects = bool((cached_resources or {}).get("projects"))
    has_compute_groups = bool((cached_resources or {}).get("compute_groups"))

    if cached_resources and not force_refresh and has_projects and has_compute_groups:
        return cached_resources

    refreshed = _refresh_workspace_resources_for_create(
        api, display, workspace_id, ws_name
    )
    return refreshed or cached_resources


def _is_unsupported_spec_listing_error(error: Exception) -> bool:
    """判断平台是否未暴露可用的实时 spec 枚举接口。"""
    message = str(error or "")
    return "HTTP 404" in message or "/openapi/v1/specs/list" in message


def _load_specs_for_create_result(
    api,
    workspace_id: str,
    ws_name: str,
    compute_group_id: str,
    display=None,
    *,
    emit_messages: bool = True,
) -> Dict[str, Any]:
    """加载 spec 列表，并返回数据来源状态与错误信息。"""
    cached_resources = get_workspace_resources(workspace_id) or {}
    all_cached_specs = [
        normalized
        for spec in cached_resources.get("specs", {}).values()
        for normalized in [_normalize_spec_item(spec)]
        if normalized
    ]
    cached_specs = _scope_specs_to_compute_group(
        all_cached_specs,
        compute_group_id,
        cached_resources.get("compute_groups", {}),
    )

    result = {
        "items": list(cached_specs),
        "status": "cache" if cached_specs else "unprobed",
        "error": None,
    }
    try:
        fetched_specs = [
            normalized
            for spec in api.list_specs(compute_group_id)
            for normalized in [_normalize_spec_item(spec, compute_group_id)]
            if normalized
        ]
        if fetched_specs:
            merged_spec_items = _merge_resource_lists(all_cached_specs, fetched_specs)
            save_resources(
                workspace_id,
                {
                    "projects": list(cached_resources.get("projects", {}).values()),
                    "compute_groups": list(
                        cached_resources.get("compute_groups", {}).values()
                    ),
                    "specs": merged_spec_items,
                },
                ws_name or cached_resources.get("name", ""),
            )
            result["items"] = _scope_specs_to_compute_group(
                merged_spec_items,
                compute_group_id,
                cached_resources.get("compute_groups", {}),
            )
            result["status"] = "realtime"
        elif cached_specs:
            result["items"] = list(cached_specs)
            result["status"] = "cache"
        else:
            result["items"] = []
            result["status"] = "empty"
    except Exception as e:
        if _is_unsupported_spec_listing_error(e):
            result["error"] = None
            result["items"] = list(cached_specs)
            result["status"] = "cache" if cached_specs else "empty"
            return result
        result["error"] = str(e)
        result["items"] = list(cached_specs)
        result["status"] = "cache" if cached_specs else "error"
        if display and emit_messages:
            if cached_specs:
                display.print(f"[dim]获取实时规格列表失败，当前展示缓存规格: {e}[/dim]")
            else:
                display.print(f"[dim]获取实时规格列表失败: {e}[/dim]")

    return result


def _load_specs_for_create(
    api, workspace_id: str, ws_name: str, compute_group_id: str, display=None
) -> List[Dict[str, Any]]:
    """优先从 OpenAPI 拉取当前计算组 spec，再与缓存合并。"""
    return _load_specs_for_create_result(
        api, workspace_id, ws_name, compute_group_id, display=display
    )["items"]


def _auto_select_spec_for_compute_group(
    workspace_id: str, compute_group_id: str
) -> Tuple[Optional[str], Optional[str]]:
    """从缓存中为当前计算组选择一个 spec。"""
    cached_resources = get_workspace_resources(workspace_id) or {}
    cached_specs = [
        normalized
        for spec in cached_resources.get("specs", {}).values()
        for normalized in [_normalize_spec_item(spec)]
        if normalized
    ]
    scoped_specs = _scope_specs_to_compute_group(
        cached_specs,
        compute_group_id,
        cached_resources.get("compute_groups", {}),
    )
    if not scoped_specs:
        return None, None
    first = scoped_specs[0]
    return first["id"], first.get("name", first["id"])


def _match_interactive_choice(
    options: List[Dict[str, Any]], raw_value: str
) -> Optional[Dict[str, Any]]:
    """支持序号、ID、名称和唯一模糊匹配。"""
    if not raw_value:
        return options[0] if options else None

    if raw_value.isdigit():
        idx = int(raw_value)
        if 1 <= idx <= len(options):
            return options[idx - 1]

    lowered = raw_value.lower()
    for option in options:
        option_id = str(option.get("id", ""))
        option_name = str(option.get("name", ""))
        if raw_value == option_id or raw_value == option_name:
            return option

    fuzzy_matches = []
    for option in options:
        option_id = str(option.get("id", "")).lower()
        option_name = str(option.get("name", "")).lower()
        if lowered in option_id or lowered in option_name:
            fuzzy_matches.append(option)
    if len(fuzzy_matches) == 1:
        return fuzzy_matches[0]
    return None


def _prompt_select_option(
    display, title: str, options: List[Dict[str, Any]], formatter, renderer=None
) -> Optional[Dict[str, Any]]:
    """打印候选项并让用户选择。"""
    if not options:
        return None

    display.print(f"\n[bold]{title}[/bold]")
    if renderer:
        renderer(display, options)
    else:
        for idx, option in enumerate(options, 1):
            display.print(f"  [{idx}] {formatter(option)}")

    while True:
        try:
            raw_value = input("选择序号/ID/名称 [1]: ").strip()
        except (EOFError, KeyboardInterrupt):
            display.print_error("交互输入已中断")
            return None
        selected = _match_interactive_choice(options, raw_value)
        if selected:
            return selected
        display.print_warning("输入无效，请重新选择")


def _sort_workspace_options_for_selection(
    options: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """按 avail 风格排序 workspace，优先展示有实时容量且空闲资源更多的项。"""
    return sorted(
        options,
        key=lambda item: (
            0 if _has_capacity_summary(item) else 1,
            -int(item.get("free_nodes", 0) or 0),
            -int(item.get("free_gpus", 0) or 0),
            -int(item.get("total_nodes", 0) or 0),
            str(item.get("name") or item.get("id") or "").lower(),
            str(item.get("id") or ""),
        ),
    )


def _render_workspace_selection_table(display, options: List[Dict[str, Any]]) -> None:
    """按 qzcli avail 风格渲染 workspace 总览表，供 create -i 选择时复用。"""
    total_workspaces = len(options)
    known_capacity_options = [
        option for option in options if _has_capacity_summary(option)
    ]

    display.print(f"[bold]工作空间总览 ({total_workspaces} 个)[/bold]")
    if known_capacity_options:
        total_free_nodes = sum(
            int(option.get("free_nodes", 0) or 0) for option in known_capacity_options
        )
        total_nodes = sum(
            int(option.get("total_nodes", 0) or 0) for option in known_capacity_options
        )
        total_free_gpus = sum(
            int(option.get("free_gpus", 0) or 0) for option in known_capacity_options
        )
        total_gpus = sum(
            int(option.get("total_gpus", 0) or 0) for option in known_capacity_options
        )
        total_used_gpus = max(0, total_gpus - total_free_gpus)
        total_gpu_util_ratio = _format_percent(total_used_gpus, total_gpus)
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
        table.add_column("工作空间", style="cyan", overflow="fold")
        table.add_column("空节点", justify="right")
        table.add_column("总节点", justify="right", style="dim")
        table.add_column("空GPU", justify="right")
        table.add_column("GPU利用率", justify="right")
        table.add_column("ID", style="magenta", no_wrap=True)

        for idx, option in enumerate(options, 1):
            has_capacity = _has_capacity_summary(option)
            free_nodes = int(option.get("free_nodes", 0) or 0)
            total_nodes = int(option.get("total_nodes", 0) or 0)
            free_gpus = int(option.get("free_gpus", 0) or 0)
            total_gpus = int(option.get("total_gpus", 0) or 0)

            if has_capacity:
                free_nodes_text = (
                    f"[green]{free_nodes}[/green]" if free_nodes > 0 else "[dim]0[/dim]"
                )
                free_gpu_text = f"{free_gpus}/{total_gpus}" if total_gpus > 0 else "-"
                used_gpus = max(0, total_gpus - free_gpus)
                gpu_util_text = _format_percent(used_gpus, total_gpus)
                if total_gpus > 0:
                    gpu_util_ratio = used_gpus / total_gpus
                    if gpu_util_ratio >= 0.8:
                        gpu_util_text = f"[green]{gpu_util_text}[/green]"
                    elif gpu_util_ratio >= 0.4:
                        gpu_util_text = f"[yellow]{gpu_util_text}[/yellow]"
                    else:
                        gpu_util_text = f"[red]{gpu_util_text}[/red]"
                else:
                    gpu_util_text = "[dim]-[/dim]"
            else:
                free_nodes_text = "[dim]-[/dim]"
                free_gpu_text = "[dim]-[/dim]"
                gpu_util_text = "[dim]-[/dim]"

            table.add_row(
                str(idx),
                option.get("name") or option.get("id", ""),
                free_nodes_text,
                str(total_nodes) if has_capacity else "-",
                free_gpu_text,
                gpu_util_text,
                option.get("id", ""),
            )

        display.console.print(table)
        return

    table_rows = []
    for idx, option in enumerate(options, 1):
        has_capacity = _has_capacity_summary(option)
        total_gpus = int(option.get("total_gpus", 0) or 0)
        free_gpus = int(option.get("free_gpus", 0) or 0)
        table_rows.append(
            [
                idx,
                option.get("name") or option.get("id", ""),
                int(option.get("free_nodes", 0) or 0) if has_capacity else "-",
                int(option.get("total_nodes", 0) or 0) if has_capacity else "-",
                f"{free_gpus}/{total_gpus}" if has_capacity and total_gpus > 0 else "-",
                (
                    _format_percent(max(0, total_gpus - free_gpus), total_gpus)
                    if has_capacity and total_gpus > 0
                    else "-"
                ),
                option.get("id", ""),
            ]
        )

    table_lines = _render_plain_table(
        headers=["排名", "工作空间", "空节点", "总节点", "空GPU", "GPU利用率", "ID"],
        rows=table_rows,
        aligns=["right", "left", "right", "right", "right", "right", "left"],
        max_widths=[4, 24, 6, 6, 12, 9, 40],
    )
    for line in table_lines:
        display.print(line)


def _sort_compute_group_options_for_selection(
    options: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """按实时空闲容量排序计算组，容量未知项排在后面。"""
    return sorted(
        options,
        key=lambda item: (
            0 if _has_capacity_summary(item) else 1,
            -int(item.get("free_nodes", 0) or 0),
            -int(item.get("free_gpus", 0) or 0),
            -int(item.get("total_nodes", 0) or 0),
            str(item.get("name") or item.get("id") or "").lower(),
            str(item.get("id") or ""),
        ),
    )


def _get_compute_group_usage_scope_label(option: Dict[str, Any]) -> str:
    """返回计算组占用口径标签。"""
    if option.get("usage_scope") == "shared_physical_pool":
        return "共享池"
    if _has_capacity_summary(option):
        return "逻辑组"
    return "缓存"


def _get_compute_group_spec_status_label(option: Dict[str, Any]) -> str:
    """返回计算组规格状态标签。"""
    status = str(option.get("spec_status", "") or "unprobed")
    return {
        "realtime": "实时",
        "cache": "缓存",
        "error": "异常",
        "empty": "空",
        "unprobed": "未探测",
    }.get(status, "未探测")


def _render_compute_group_selection_table(
    display, options: List[Dict[str, Any]]
) -> None:
    """按表格形式渲染计算组选择列表。"""
    display.print(f"[bold]计算组总览 ({len(options)} 个)[/bold]")

    unique_capacity_options: List[Dict[str, Any]] = []
    seen_usage_keys = set()
    has_shared_pool = False
    for option in options:
        if not _has_capacity_summary(option):
            continue
        usage_key = str(option.get("compute_group_id") or option.get("id") or "")
        if not usage_key:
            continue
        if usage_key in seen_usage_keys:
            continue
        seen_usage_keys.add(usage_key)
        unique_capacity_options.append(option)
        if option.get("usage_scope") == "shared_physical_pool":
            has_shared_pool = True

    if unique_capacity_options:
        total_free_nodes = sum(
            int(option.get("free_nodes", 0) or 0) for option in unique_capacity_options
        )
        total_nodes = sum(
            int(option.get("total_nodes", 0) or 0) for option in unique_capacity_options
        )
        total_free_gpus = sum(
            int(option.get("free_gpus", 0) or 0) for option in unique_capacity_options
        )
        total_gpus = sum(
            int(option.get("total_gpus", 0) or 0) for option in unique_capacity_options
        )
        total_used_gpus = max(0, total_gpus - total_free_gpus)
        total_gpu_util_ratio = _format_percent(total_used_gpus, total_gpus)
        prefix = "按唯一资源池汇总: " if has_shared_pool else ""
        display.print(
            f"[dim]{prefix}空节点 {total_free_nodes}/{total_nodes} | 空GPU {total_free_gpus}/{total_gpus} | GPU利用率 {total_gpu_util_ratio}[/dim]"
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
        table.add_column("计算组", style="cyan", overflow="fold")
        table.add_column("GPU类型", style="magenta", no_wrap=True)
        table.add_column("占用口径", style="white", no_wrap=True)
        table.add_column("规格状态", style="white", no_wrap=True)
        table.add_column("空节点", justify="right")
        table.add_column("总节点", justify="right", style="dim")
        table.add_column("空GPU", justify="right")
        table.add_column("GPU利用率", justify="right")
        table.add_column("ID", style="dim", no_wrap=True)

        for idx, option in enumerate(options, 1):
            has_capacity = _has_capacity_summary(option)
            free_nodes = int(option.get("free_nodes", 0) or 0)
            total_nodes = int(option.get("total_nodes", 0) or 0)
            free_gpus = int(option.get("free_gpus", 0) or 0)
            total_gpus = int(option.get("total_gpus", 0) or 0)

            if has_capacity:
                free_nodes_text = (
                    f"[green]{free_nodes}[/green]" if free_nodes > 0 else "[dim]0[/dim]"
                )
                free_gpu_text = f"{free_gpus}/{total_gpus}" if total_gpus > 0 else "-"
                used_gpus = max(0, total_gpus - free_gpus)
                gpu_util_text = _format_percent(used_gpus, total_gpus)
                if total_gpus > 0:
                    gpu_util_ratio = used_gpus / total_gpus
                    if gpu_util_ratio >= 0.8:
                        gpu_util_text = f"[green]{gpu_util_text}[/green]"
                    elif gpu_util_ratio >= 0.4:
                        gpu_util_text = f"[yellow]{gpu_util_text}[/yellow]"
                    else:
                        gpu_util_text = f"[red]{gpu_util_text}[/red]"
                else:
                    gpu_util_text = "[dim]-[/dim]"
            else:
                free_nodes_text = "[dim]-[/dim]"
                free_gpu_text = "[dim]-[/dim]"
                gpu_util_text = "[dim]-[/dim]"

            table.add_row(
                str(idx),
                option.get("name") or option.get("id", ""),
                option.get("gpu_type", "") or "-",
                _get_compute_group_usage_scope_label(option),
                _get_compute_group_spec_status_label(option),
                free_nodes_text,
                str(total_nodes) if has_capacity else "-",
                free_gpu_text,
                gpu_util_text,
                option.get("id", ""),
            )

        display.console.print(table)
        return

    table_rows = []
    for idx, option in enumerate(options, 1):
        has_capacity = _has_capacity_summary(option)
        total_gpus = int(option.get("total_gpus", 0) or 0)
        free_gpus = int(option.get("free_gpus", 0) or 0)
        table_rows.append(
            [
                idx,
                option.get("name") or option.get("id", ""),
                option.get("gpu_type", "") or "-",
                _get_compute_group_usage_scope_label(option),
                _get_compute_group_spec_status_label(option),
                int(option.get("free_nodes", 0) or 0) if has_capacity else "-",
                int(option.get("total_nodes", 0) or 0) if has_capacity else "-",
                f"{free_gpus}/{total_gpus}" if has_capacity and total_gpus > 0 else "-",
                (
                    _format_percent(max(0, total_gpus - free_gpus), total_gpus)
                    if has_capacity and total_gpus > 0
                    else "-"
                ),
                option.get("id", ""),
            ]
        )

    table_lines = _render_plain_table(
        headers=[
            "排名",
            "计算组",
            "GPU类型",
            "占用口径",
            "规格状态",
            "空节点",
            "总节点",
            "空GPU",
            "GPU利用率",
            "ID",
        ],
        rows=table_rows,
        aligns=[
            "right",
            "left",
            "left",
            "left",
            "left",
            "right",
            "right",
            "right",
            "right",
            "left",
        ],
        max_widths=[4, 20, 10, 8, 8, 6, 6, 12, 9, 40],
    )
    for line in table_lines:
        display.print(line)


def _can_use_arrow_select() -> bool:
    """判断当前终端是否支持上下键菜单交互。"""
    return bool(PROMPT_TOOLKIT_AVAILABLE and sys.stdin.isatty() and sys.stdout.isatty())


def _run_choice_prompt(**kwargs):
    """运行 prompt_toolkit 的上下键选择器。"""
    if not PROMPT_TOOLKIT_AVAILABLE or prompt_toolkit_choice is None:
        raise RuntimeError("prompt_toolkit 不可用")
    return prompt_toolkit_choice(**kwargs)


def _compact_resource_id(value: str, head: int = 12, tail: int = 6) -> str:
    """压缩展示较长的资源 ID。"""
    text = str(value or "")
    if len(text) <= head + tail + 3:
        return text
    return f"{text[:head]}...{text[-tail:]}"


def _build_arrow_choice_table(
    headers: Sequence[str],
    rows: Sequence[Sequence[object]],
    aligns: Sequence[str],
    *,
    max_widths: Optional[Sequence[int]] = None,
    min_widths: Optional[Sequence[int]] = None,
) -> Tuple[List[str], List[str]]:
    """为上下键菜单构造对齐表头和选项行。"""
    lines = _render_plain_table(
        headers=headers,
        rows=rows,
        aligns=aligns,
        indent="",
        col_gap=2,
        max_widths=max_widths,
        min_widths=min_widths,
    )
    if len(lines) < 2:
        return lines, []
    return lines[:2], lines[2:]


def _build_workspace_choice_context_lines(options: List[Dict[str, Any]]) -> List[str]:
    """生成工作空间箭头选择器的说明文本。"""
    known_capacity_options = [
        option for option in options if _has_capacity_summary(option)
    ]
    if not known_capacity_options:
        return ["当前未获取到实时占用，以下为缓存工作空间列表。"]

    total_free_nodes = sum(
        int(option.get("free_nodes", 0) or 0) for option in known_capacity_options
    )
    total_nodes = sum(
        int(option.get("total_nodes", 0) or 0) for option in known_capacity_options
    )
    total_free_gpus = sum(
        int(option.get("free_gpus", 0) or 0) for option in known_capacity_options
    )
    total_gpus = sum(
        int(option.get("total_gpus", 0) or 0) for option in known_capacity_options
    )
    total_used_gpus = max(0, total_gpus - total_free_gpus)
    total_gpu_util_ratio = _format_percent(total_used_gpus, total_gpus)
    return [
        f"总览: 空节点 {total_free_nodes}/{total_nodes} | 空GPU {total_free_gpus}/{total_gpus} | GPU利用率 {total_gpu_util_ratio}"
    ]


def _build_compute_group_choice_context_lines(
    ws_display: str,
    project_display: str,
    options: List[Dict[str, Any]],
) -> List[str]:
    """生成计算组箭头选择器的说明文本。"""
    lines = [f"工作空间: {ws_display}", f"项目: {project_display}"]

    unique_capacity_options: List[Dict[str, Any]] = []
    seen_usage_keys = set()
    has_shared_pool = False
    for option in options:
        if not _has_capacity_summary(option):
            continue
        usage_key = str(option.get("compute_group_id") or option.get("id") or "")
        if not usage_key or usage_key in seen_usage_keys:
            continue
        seen_usage_keys.add(usage_key)
        unique_capacity_options.append(option)
        if option.get("usage_scope") == "shared_physical_pool":
            has_shared_pool = True

    if unique_capacity_options:
        total_free_nodes = sum(
            int(option.get("free_nodes", 0) or 0) for option in unique_capacity_options
        )
        total_nodes = sum(
            int(option.get("total_nodes", 0) or 0) for option in unique_capacity_options
        )
        total_free_gpus = sum(
            int(option.get("free_gpus", 0) or 0) for option in unique_capacity_options
        )
        total_gpus = sum(
            int(option.get("total_gpus", 0) or 0) for option in unique_capacity_options
        )
        total_used_gpus = max(0, total_gpus - total_free_gpus)
        total_gpu_util_ratio = _format_percent(total_used_gpus, total_gpus)
        prefix = "按唯一资源池汇总: " if has_shared_pool else "总览: "
        lines.append(
            f"{prefix}空节点 {total_free_nodes}/{total_nodes} | 空GPU {total_free_gpus}/{total_gpus} | GPU利用率 {total_gpu_util_ratio}"
        )
    else:
        lines.append("当前未获取到实时占用，以下为缓存计算组列表。")

    if has_shared_pool:
        lines.append(
            "带“共享池”的选项反映底层物理资源池占用，不等价于逻辑组真实可提交容量。"
        )

    return lines


def _build_project_choice_context_lines(ws_display: str) -> List[str]:
    """生成项目箭头选择器的说明文本。"""
    return [f"工作空间: {ws_display}"]


def _build_spec_choice_context_lines(
    ws_display: str, project_display: str, compute_group_display: str
) -> List[str]:
    """生成规格箭头选择器的说明文本。"""
    return [
        f"工作空间: {ws_display}",
        f"项目: {project_display}",
        f"计算组: {compute_group_display}",
    ]


def _build_workspace_choice_table(
    options: List[Dict[str, Any]]
) -> Tuple[List[str], List[str]]:
    """构造工作空间箭头菜单表格。"""
    rows = []
    for option in options:
        has_capacity = _has_capacity_summary(option)
        total_nodes = int(option.get("total_nodes", 0) or 0)
        free_nodes = int(option.get("free_nodes", 0) or 0)
        total_gpus = int(option.get("total_gpus", 0) or 0)
        free_gpus = int(option.get("free_gpus", 0) or 0)
        rows.append(
            [
                option.get("name") or "[未命名]",
                (
                    f"{free_nodes}/{total_nodes}"
                    if has_capacity and total_nodes > 0
                    else "-"
                ),
                f"{free_gpus}/{total_gpus}" if has_capacity and total_gpus > 0 else "-",
                (
                    _format_percent(max(0, total_gpus - free_gpus), total_gpus)
                    if has_capacity and total_gpus > 0
                    else "-"
                ),
                _compact_resource_id(str(option.get("id", "")), head=8, tail=4),
            ]
        )
    return _build_arrow_choice_table(
        headers=["工作空间", "空节点", "空GPU", "利用率", "ID"],
        rows=rows,
        aligns=["left", "right", "right", "right", "left"],
        max_widths=[20, 8, 10, 7, 15],
    )


def _build_project_choice_table(
    options: List[Dict[str, Any]]
) -> Tuple[List[str], List[str]]:
    """构造项目箭头菜单表格。"""
    rows = [
        [
            option.get("name") or option.get("id", ""),
            _compact_resource_id(str(option.get("id", "")), head=8, tail=4),
        ]
        for option in options
    ]
    return _build_arrow_choice_table(
        headers=["项目", "ID"],
        rows=rows,
        aligns=["left", "left"],
        max_widths=[28, 15],
    )


def _build_compute_group_choice_table(
    options: List[Dict[str, Any]]
) -> Tuple[List[str], List[str]]:
    """构造计算组箭头菜单表格。"""
    rows = []
    for option in options:
        has_capacity = _has_capacity_summary(option)
        total_nodes = int(option.get("total_nodes", 0) or 0)
        free_nodes = int(option.get("free_nodes", 0) or 0)
        total_gpus = int(option.get("total_gpus", 0) or 0)
        free_gpus = int(option.get("free_gpus", 0) or 0)
        rows.append(
            [
                option.get("name") or option.get("id", ""),
                option.get("gpu_type", "") or "-",
                _get_compute_group_usage_scope_label(option),
                _get_compute_group_spec_status_label(option),
                (
                    f"{free_nodes}/{total_nodes}"
                    if has_capacity and total_nodes > 0
                    else "-"
                ),
                f"{free_gpus}/{total_gpus}" if has_capacity and total_gpus > 0 else "-",
                (
                    _format_percent(max(0, total_gpus - free_gpus), total_gpus)
                    if has_capacity and total_gpus > 0
                    else "-"
                ),
                _compact_resource_id(str(option.get("id", "")), head=8, tail=4),
            ]
        )
    return _build_arrow_choice_table(
        headers=["计算组", "GPU", "口径", "规格", "空节点", "空GPU", "利用率", "ID"],
        rows=rows,
        aligns=["left", "left", "left", "left", "right", "right", "right", "left"],
        max_widths=[14, 7, 6, 8, 7, 10, 6, 14],
    )


def _build_spec_choice_table(
    options: List[Dict[str, Any]]
) -> Tuple[List[str], List[str]]:
    """构造规格箭头菜单表格。"""
    rows = []
    for option in options:
        gpu_type = option.get("gpu_type_display") or option.get("gpu_type") or "-"
        gpu_count = int(option.get("gpu_count", 0) or 0)
        gpu_text = f"{gpu_type} x{gpu_count}" if gpu_count > 0 else str(gpu_type)
        rows.append(
            [
                option.get("name") or option.get("id", ""),
                gpu_text,
                str(option.get("cpu_count", 0) or "-"),
                (
                    f"{option.get('memory_gb', 0) or '-'}GiB"
                    if option.get("memory_gb")
                    else "-"
                ),
                _compact_resource_id(str(option.get("id", "")), head=8, tail=4),
            ]
        )
    return _build_arrow_choice_table(
        headers=["规格", "GPU", "CPU", "内存", "ID"],
        rows=rows,
        aligns=["left", "left", "right", "right", "left"],
        max_widths=[18, 18, 5, 8, 15],
    )


def _build_picker_step_fragments(
    levels: List[str], current_index: int
) -> List[Tuple[str, str]]:
    """构造层级导航栏。"""
    names = {
        "workspace": "工作空间",
        "project": "项目",
        "compute_group": "计算组",
        "spec": "规格",
    }
    fragments: List[Tuple[str, str]] = []
    for idx, level in enumerate(levels):
        style = "class:picker-step-pending"
        if idx < current_index:
            style = "class:picker-step-done"
        elif idx == current_index:
            style = "class:picker-step-active"
        fragments.append((style, names.get(level, level)))
        if idx < len(levels) - 1:
            fragments.append(("class:picker-sep", "  >  "))
    return fragments


def _run_resource_hierarchy_tui(
    api,
    display,
    *,
    initial_workspace_id: str = "",
    initial_ws_display: str = "",
    initial_project_value: str = "",
    initial_project_display: str = "",
    initial_compute_group_value: str = "",
    initial_compute_group_display: str = "",
    initial_spec_value: str = "",
    prefetched_snapshot: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """在单个全屏界面中完成 workspace/project/compute_group/spec 选择。"""
    if (
        not PROMPT_TOOLKIT_AVAILABLE
        or RadioList is None
        or Application is None
        or Condition is None
        or DynamicContainer is None
        or TextArea is None
    ):
        return None

    state: Dict[str, Any] = {
        "workspace_id": initial_workspace_id or "",
        "ws_display": initial_ws_display or "",
        "project_id": initial_project_value or "",
        "project_display": initial_project_display or "",
        "compute_group_id": initial_compute_group_value or "",
        "compute_group_display": initial_compute_group_display or "",
        "spec_id": initial_spec_value or "",
        "manual_spec_required": False,
        "manual_spec_input": False,
    }
    level_field_names = {
        "workspace": "workspace_id",
        "project": "project_id",
        "compute_group": "compute_group_id",
        "spec": "spec_id",
    }
    levels = ["workspace", "project", "compute_group", "spec"]
    if all(state[field_name] for field_name in level_field_names.values()):
        return state

    has_prefetched_snapshot = prefetched_snapshot is not None
    snapshot_data = prefetched_snapshot or {}
    snapshot_workspace_options = list(snapshot_data.get("workspace_options") or [])
    snapshot_workspace_details = snapshot_data.get("workspace_details_by_id") or {}
    cache: Dict[str, Any] = {
        "workspace_options": (
            list(snapshot_workspace_options) if has_prefetched_snapshot else None
        ),
        "ws_resources_by_id": {
            str(ws_id): dict(detail.get("resources") or {})
            for ws_id, detail in snapshot_workspace_details.items()
            if ws_id
        },
        "project_options_by_ws": {
            str(ws_id): list(detail.get("project_options") or [])
            for ws_id, detail in snapshot_workspace_details.items()
            if ws_id
        },
        "compute_group_options_by_ws": {
            str(ws_id): list(detail.get("compute_group_options") or [])
            for ws_id, detail in snapshot_workspace_details.items()
            if ws_id
        },
        "spec_options_by_key": {},
        "spec_result_by_key": {},
        "spec_status_by_compute_group": {},
    }
    for ws_id, detail in snapshot_workspace_details.items():
        ws_key = str(ws_id or "")
        if not ws_key:
            continue
        for compute_group_id, spec_result in (
            detail.get("spec_result_by_compute_group") or {}
        ).items():
            compute_group_key = str(compute_group_id or "")
            cache_key = (ws_key, compute_group_key)
            normalized_result = dict(spec_result or {})
            cache["spec_result_by_key"][cache_key] = normalized_result
            cache["spec_options_by_key"][cache_key] = list(
                normalized_result.get("items") or []
            )
            cache["spec_status_by_compute_group"][compute_group_key] = str(
                normalized_result.get("status", "") or "unprobed"
            )
    selected_ids: Dict[str, str] = {}
    current_level_index = next(
        (
            idx
            for idx, level in enumerate(levels)
            if not state.get(level_field_names[level], "")
        ),
        len(levels) - 1,
    )
    current_options: List[Dict[str, Any]] = []
    current_context_lines: List[str] = []
    current_header_lines: List[str] = []
    current_notice_lines: List[str] = []
    current_title = ""
    current_error: List[str] = []
    current_mode = "list"
    app: Any = None

    def _find_next_missing_level_index(start_index: int) -> Optional[int]:
        for idx in range(start_index, len(levels)):
            if not state.get(level_field_names[levels[idx]], ""):
                return idx
        return None

    def _clear_downstream(level: str) -> None:
        if level == "workspace":
            state["project_id"] = ""
            state["project_display"] = ""
            state["compute_group_id"] = ""
            state["compute_group_display"] = ""
            state["spec_id"] = ""
            state["manual_spec_required"] = False
        elif level == "project":
            state["compute_group_id"] = ""
            state["compute_group_display"] = ""
            state["spec_id"] = ""
            state["manual_spec_required"] = False
        elif level == "compute_group":
            state["spec_id"] = ""
            state["manual_spec_required"] = False

    def _ensure_ws_resources() -> Optional[Dict[str, Any]]:
        workspace_id = state.get("workspace_id", "")
        if not workspace_id:
            return None
        if workspace_id not in cache["ws_resources_by_id"]:
            if has_prefetched_snapshot:
                workspace_detail = snapshot_workspace_details.get(workspace_id) or {}
                cache["ws_resources_by_id"][workspace_id] = dict(
                    workspace_detail.get("resources") or {}
                )
            else:
                cache["ws_resources_by_id"][workspace_id] = (
                    _load_workspace_resources_for_create(
                        api,
                        display,
                        workspace_id,
                        state.get("ws_display", ""),
                        force_refresh=True,
                    )
                )
        return cache["ws_resources_by_id"].get(workspace_id)

    def _resolve_explicit_project_if_needed() -> None:
        if (
            not initial_project_value
            or not state.get("workspace_id")
            or state.get("project_id") != initial_project_value
        ):
            return
        resolved_id, resolved_display = _resolve_cached_resource_value(
            state["workspace_id"],
            "projects",
            initial_project_value,
            workspace_resources=_ensure_ws_resources(),
        )
        if resolved_id:
            state["project_id"] = resolved_id
            state["project_display"] = resolved_display or resolved_id

    def _resolve_explicit_compute_group_if_needed() -> None:
        if (
            not initial_compute_group_value
            or not state.get("workspace_id")
            or state.get("compute_group_id") != initial_compute_group_value
        ):
            return
        resolved_id, resolved_display = _resolve_cached_resource_value(
            state["workspace_id"],
            "compute_groups",
            initial_compute_group_value,
            workspace_resources=_ensure_ws_resources(),
        )
        if resolved_id:
            state["compute_group_id"] = resolved_id
            state["compute_group_display"] = resolved_display or resolved_id

    def _load_level_payload(
        level: str,
    ) -> Tuple[str, List[Dict[str, Any]], List[str], List[str], List[str]]:
        if level == "workspace":
            workspace_options = cache.get("workspace_options")
            if workspace_options is None:
                workspace_options = _sort_workspace_options_for_selection(
                    _list_available_workspaces(api, display)
                )
                cache["workspace_options"] = workspace_options
            header_lines, _ = _build_workspace_choice_table(workspace_options)
            return (
                "选择工作空间",
                workspace_options,
                _build_workspace_choice_context_lines(workspace_options),
                header_lines,
                [],
            )

        ws_resources = _ensure_ws_resources() or {}
        _resolve_explicit_project_if_needed()
        _resolve_explicit_compute_group_if_needed()

        if level == "project":
            workspace_id = state.get("workspace_id", "")
            project_options = cache["project_options_by_ws"].get(workspace_id)
            if project_options is None:
                project_options = _sort_project_options_for_selection(
                    list(ws_resources.get("projects", {}).values())
                )
                cache["project_options_by_ws"][workspace_id] = project_options
            header_lines, _ = _build_project_choice_table(project_options)
            return (
                "选择项目",
                project_options,
                _build_project_choice_context_lines(state.get("ws_display", "")),
                header_lines,
                [],
            )

        if level == "compute_group":
            workspace_id = state.get("workspace_id", "")
            base_compute_group_options = cache["compute_group_options_by_ws"].get(
                workspace_id
            )
            if base_compute_group_options is None:
                if has_prefetched_snapshot:
                    base_compute_group_options = (
                        _sort_compute_group_options_for_selection(
                            list(ws_resources.get("compute_groups", {}).values())
                        )
                    )
                else:
                    compute_group_items = list(
                        ws_resources.get("compute_groups", {}).values()
                    )
                    workspace_usage_snapshot = None
                    for option in cache.get("workspace_options") or []:
                        if str(option.get("id", "")) == workspace_id:
                            workspace_usage_snapshot = option.get("_usage_snapshot")
                            break
                    base_compute_group_options = [
                        {
                            **item,
                            "spec_status": "unprobed",
                        }
                        for item in _build_compute_group_options_with_usage(
                            api,
                            display,
                            workspace_id,
                            compute_group_items,
                            workspace_usage_snapshot=workspace_usage_snapshot,
                        )
                    ]
                cache["compute_group_options_by_ws"][
                    workspace_id
                ] = base_compute_group_options
            compute_group_options = [
                {
                    **item,
                    "spec_status": cache["spec_status_by_compute_group"].get(
                        str(item.get("id", "")), item.get("spec_status", "unprobed")
                    ),
                }
                for item in base_compute_group_options
            ]
            header_lines, _ = _build_compute_group_choice_table(compute_group_options)
            return (
                "选择计算组",
                compute_group_options,
                _build_compute_group_choice_context_lines(
                    state.get("ws_display", ""),
                    state.get("project_display") or state.get("project_id", ""),
                    compute_group_options,
                ),
                header_lines,
                [],
            )

        compute_group_id = state.get("compute_group_id", "")
        cache_key = (state.get("workspace_id", ""), compute_group_id)
        spec_result = cache["spec_result_by_key"].get(cache_key)
        if spec_result is None:
            if has_prefetched_snapshot:
                spec_result = _build_cached_spec_result(ws_resources, compute_group_id)
            else:
                spec_result = _load_specs_for_create_result(
                    api,
                    state.get("workspace_id", ""),
                    state.get("ws_display", ""),
                    compute_group_id,
                    display=display,
                    emit_messages=False,
                )
            cache["spec_result_by_key"][cache_key] = dict(spec_result)
            cache["spec_options_by_key"][cache_key] = list(spec_result.get("items", []))
        spec_options = list(spec_result.get("items", []))
        spec_status = str(spec_result.get("status", "") or "unprobed")
        cache["spec_status_by_compute_group"][compute_group_id] = spec_status
        notice_lines: List[str] = []
        error_message = str(spec_result.get("error", "") or "").strip()
        if error_message and spec_options:
            notice_lines.append(
                f"获取实时规格列表失败，当前展示预加载缓存规格: {error_message}"
            )
        elif error_message:
            notice_lines.append(f"获取实时规格列表失败: {error_message}")
            notice_lines.append("按 m 手动输入 spec ID，或按 ← 返回上一级更换计算组。")
        elif spec_status == "empty":
            notice_lines.append(
                "当前计算组暂无可用规格。按 m 手动输入 spec ID，或按 ← 返回上一级更换计算组。"
            )
        header_lines, _ = (
            _build_spec_choice_table(spec_options) if spec_options else ([], [])
        )
        return (
            "选择资源规格",
            spec_options,
            _build_spec_choice_context_lines(
                state.get("ws_display", ""),
                state.get("project_display") or state.get("project_id", ""),
                state.get("compute_group_display") or compute_group_id,
            ),
            header_lines,
            notice_lines,
        )

    def _apply_level_selection(level: str, option: Dict[str, Any]) -> None:
        option_id = str(option.get("id", ""))
        previous_id = state.get(f"{level}_id", "")
        if level == "workspace":
            state["workspace_id"] = option_id
            state["ws_display"] = option.get("name", option_id)
        elif level == "project":
            state["project_id"] = option_id
            state["project_display"] = option.get("name", option_id)
        elif level == "compute_group":
            state["compute_group_id"] = option_id
            state["compute_group_display"] = option.get("name", option_id)
        elif level == "spec":
            state["spec_id"] = option_id
            state["manual_spec_input"] = False
        selected_ids[level] = option_id
        if previous_id != option_id:
            _clear_downstream(level)
            if level != "spec":
                selected_ids.pop("spec", None)
            if level not in ("project", "spec"):
                selected_ids.pop("compute_group", None)
            if level == "workspace":
                selected_ids.pop("project", None)

    def _set_radio_placeholder(message: str) -> None:
        radio_list.values = [("__placeholder__", message)]
        radio_list.current_value = "__placeholder__"
        radio_list._selected_index = 0

    def _set_radio_options(options: List[Dict[str, Any]]) -> None:
        values = [
            (str(option.get("id", "")), label)
            for option, label in zip(options, _build_current_rows(options))
        ]
        radio_list.values = values
        if not values:
            radio_list.current_value = ""
            radio_list._selected_index = 0
            return
        level = levels[current_level_index]
        preferred_id = selected_ids.get(level) or values[0][0]
        selected_index = 0
        for idx, (value, _) in enumerate(values):
            if value == preferred_id:
                selected_index = idx
                break
        radio_list._selected_index = selected_index
        radio_list.current_value = values[selected_index][0]

    def _build_current_rows(options: List[Dict[str, Any]]) -> List[str]:
        level = levels[current_level_index]
        if level == "workspace":
            _, rows = _build_workspace_choice_table(options)
        elif level == "project":
            _, rows = _build_project_choice_table(options)
        elif level == "compute_group":
            _, rows = _build_compute_group_choice_table(options)
        else:
            _, rows = _build_spec_choice_table(options)
        return rows

    def _refresh_level() -> bool:
        nonlocal current_options, current_context_lines, current_header_lines, current_notice_lines, current_title, current_error, current_mode
        current_error = []
        current_notice_lines = []
        if current_mode != "manual_spec":
            current_mode = "list"
        level = levels[current_level_index]
        try:
            title, options, context_lines, header_lines, notice_lines = (
                _load_level_payload(level)
            )
        except QzAPIError as e:
            current_title = "资源选择失败"
            current_options = []
            current_context_lines = []
            current_header_lines = []
            current_notice_lines = []
            current_error = [str(e)]
            _set_radio_placeholder("资源选择失败")
            return False

        current_title = title
        current_options = options
        current_context_lines = context_lines
        current_header_lines = header_lines
        current_notice_lines = notice_lines

        if not current_options:
            if level == "spec":
                _set_radio_placeholder("暂无可选规格")
                return True
            current_error = ["当前层级没有可选项"]
            _set_radio_placeholder("当前层级没有可选项")
            return False

        _set_radio_options(current_options)
        if app is not None and current_mode == "list":
            app.layout.focus(radio_list)
        return True

    def _top_text():
        lines: List[Any] = [
            ("class:picker-title", "交互式资源选择"),
            ("", "\n"),
            *_build_picker_step_fragments(levels, current_level_index),
        ]
        lines.append(("", "\n\n"))
        lines.append(("class:picker-subtitle", current_title))
        for line in current_context_lines:
            lines.append(("", "\n"))
            lines.append(("class:picker-context", line))
        if current_header_lines:
            lines.append(("", "\n\n"))
            for idx, line in enumerate(current_header_lines):
                lines.append(("class:picker-header", line))
                if idx < len(current_header_lines) - 1:
                    lines.append(("", "\n"))
        if current_notice_lines:
            lines.append(("", "\n\n"))
            for idx, line in enumerate(current_notice_lines):
                lines.append(("class:warning", line))
                if idx < len(current_notice_lines) - 1:
                    lines.append(("", "\n"))
        if current_error:
            lines.append(("", "\n\n"))
            for idx, line in enumerate(current_error):
                lines.append(("class:error", line))
                if idx < len(current_error) - 1:
                    lines.append(("", "\n"))
        return lines

    def _footer_text() -> str:
        level = levels[current_level_index]
        if current_mode == "manual_spec":
            return "输入 spec ID 后 Enter/→ 确认  ← 返回规格列表  q/Ctrl-C 取消"
        if level == "spec":
            return "↑/↓ 选择  Enter/→ 确认规格  m 手动输入  ← 返回上一层  q/Ctrl-C 取消"
        return "↑/↓ 选择  Enter/→ 下一层  ← 返回上一层  q/Ctrl-C 取消"

    spec_list_mode_filter = Condition(
        lambda: levels[current_level_index] == "spec" and current_mode != "manual_spec"
    )

    kb = KeyBindings()

    @kb.add("enter", eager=True)
    @kb.add("right")
    def _next(event) -> None:
        nonlocal current_level_index, current_mode
        if current_mode == "manual_spec":
            manual_spec_id = manual_spec_input.text.strip()
            if not manual_spec_id:
                return
            state["spec_id"] = manual_spec_id
            state["manual_spec_required"] = False
            state["manual_spec_input"] = True
            selected_ids["spec"] = manual_spec_id
            event.app.exit(result=dict(state))
            return
        if not current_options:
            return
        selected_option = current_options[radio_list._selected_index]
        level = levels[current_level_index]
        _apply_level_selection(level, selected_option)
        next_level_index = _find_next_missing_level_index(current_level_index + 1)
        if next_level_index is None:
            event.app.exit(result=dict(state))
            return
        current_level_index = next_level_index
        _refresh_level()
        event.app.invalidate()

    @kb.add("left")
    def _back(event) -> None:
        nonlocal current_level_index, current_mode
        if current_mode == "manual_spec":
            current_mode = "list"
            event.app.layout.focus(radio_list)
            event.app.invalidate()
            return
        if current_level_index <= 0:
            return
        current_level_index -= 1
        _refresh_level()
        event.app.invalidate()

    @kb.add("m", filter=spec_list_mode_filter)
    def _manual_spec(event) -> None:
        nonlocal current_mode
        current_mode = "manual_spec"
        manual_spec_input.text = state.get("spec_id", "")
        event.app.layout.focus(manual_spec_input)
        event.app.invalidate()

    @kb.add("c-c")
    @kb.add("q")
    def _quit(event) -> None:
        event.app.exit(result=None)

    radio_list = RadioList(
        values=[("", "")],
        default="",
        show_numbers=True,
        select_on_focus=True,
        open_character="",
        select_character="❯",
        close_character="",
        default_style="class:picker-row",
        selected_style="class:picker-row-selected",
        checked_style="class:picker-row-selected",
        number_style="class:picker-number",
        show_scrollbar=True,
    )
    if ScrollbarMargin is not None:
        radio_list.window.right_margins = [ScrollbarMargin(display_arrows=True)]
    manual_spec_input = TextArea(
        text="",
        height=1,
        multiline=False,
        prompt="spec ID: ",
        wrap_lines=False,
        style="class:picker-row",
    )

    top_window = Window(
        content=FormattedTextControl(_top_text),
        dont_extend_height=True,
        wrap_lines=False,
    )
    footer_window = Window(
        content=FormattedTextControl(_footer_text),
        height=D.exact(1),
        dont_extend_height=True,
        style="class:picker-footer",
    )
    body_container = DynamicContainer(
        lambda: manual_spec_input if current_mode == "manual_spec" else radio_list
    )
    root = Frame(
        body=HSplit([top_window, body_container, footer_window]),
        style="class:picker-frame",
    )
    style = Style.from_dict(
        {
            "picker-frame": "",
            "frame.border": "ansicyan",
            "frame.label": "bold",
            "picker-title": "bold",
            "picker-subtitle": "bold ansicyan",
            "picker-context": "ansigray",
            "picker-header": "bold",
            "picker-step-active": "bold ansicyan",
            "picker-step-done": "ansigreen",
            "picker-step-pending": "ansigray",
            "picker-sep": "ansigray",
            "picker-row-selected": "reverse",
            "picker-number": "ansigray",
            "picker-footer": "reverse",
            "warning": "ansiyellow",
            "error": "ansired",
        }
    )
    app = Application(
        layout=Layout(root, focused_element=radio_list),
        key_bindings=kb,
        full_screen=True,
        mouse_support=True,
        style=style,
    )

    if not _refresh_level():
        if current_error:
            display.print_error(current_error[0])
            return None
        return dict(state)

    try:
        return app.run()
    except (KeyboardInterrupt, EOFError):
        display.print_error("交互输入已中断")
        return None


def _prompt_select_option_arrow(
    display,
    title: str,
    options: List[Dict[str, Any]],
    context_lines: Optional[List[str]] = None,
    table_builder=None,
) -> Optional[Dict[str, Any]]:
    """使用上下键选择列表。"""
    if not options:
        return None

    header_lines: List[str] = []
    row_lines: List[str] = []
    if table_builder:
        header_lines, row_lines = table_builder(options)
    if not row_lines:
        row_lines = [
            str(option.get("name") or option.get("id", ""))
            for option in options
            if option.get("id")
        ]
    indexed_options = [option for option in options if option.get("id")]
    choice_options = [
        (str(option.get("id", "")), row_lines[idx])
        for idx, option in enumerate(indexed_options)
        if idx < len(row_lines)
    ]
    if not choice_options:
        return None

    message_lines = [title]
    if context_lines:
        message_lines.extend(["", *context_lines])
    if header_lines:
        message_lines.extend(["", *header_lines])

    try:
        selected_id = _run_choice_prompt(
            message="\n".join(message_lines),
            options=choice_options,
            default=choice_options[0][0],
            symbol="❯",
            show_frame=True,
            bottom_toolbar="↑/↓ 选择  Enter 确认  Ctrl-C 取消",
        )
    except (KeyboardInterrupt, EOFError):
        display.print_error("交互输入已中断")
        return None

    if selected_id is None:
        display.print_error("交互输入已中断")
        return None

    for option in options:
        if str(option.get("id", "")) == str(selected_id):
            return option
    return None


def _select_interactive_option(
    display,
    title: str,
    options: List[Dict[str, Any]],
    formatter,
    renderer=None,
    *,
    arrow_context_lines: Optional[List[str]] = None,
    arrow_table_builder=None,
) -> Optional[Dict[str, Any]]:
    """根据终端能力选择箭头菜单或文本菜单。"""
    if arrow_table_builder and _can_use_arrow_select():
        return _prompt_select_option_arrow(
            display,
            title,
            options,
            context_lines=arrow_context_lines,
            table_builder=arrow_table_builder,
        )
    return _prompt_select_option(display, title, options, formatter, renderer=renderer)


def _prompt_text_input(
    display, prompt: str, default: Optional[str] = None, required: bool = True
) -> Optional[str]:
    """读取字符串输入。"""
    suffix = f" [{default}]" if default not in (None, "") else ""
    while True:
        try:
            raw_value = input(f"{prompt}{suffix}: ").strip()
        except (EOFError, KeyboardInterrupt):
            display.print_error("交互输入已中断")
            return None
        if raw_value:
            return raw_value
        if default not in (None, ""):
            return default
        if not required:
            return ""
        display.print_warning("该参数不能为空")


def _prompt_int_input(
    display,
    prompt: str,
    default: int,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> Optional[int]:
    """读取整数输入。"""
    while True:
        raw_value = _prompt_text_input(display, prompt, str(default), required=True)
        if raw_value is None:
            return None
        try:
            value = int(raw_value)
        except ValueError:
            display.print_warning("请输入整数")
            continue
        if min_value is not None and value < min_value:
            display.print_warning(f"请输入 >= {min_value} 的整数")
            continue
        if max_value is not None and value > max_value:
            display.print_warning(f"请输入 <= {max_value} 的整数")
            continue
        return value


def _format_workspace_option(option: Dict[str, Any]) -> str:
    name = option.get("name") or "[未命名]"
    summary = _format_capacity_summary(option)
    if summary:
        return f"{name} [{summary}] ({option.get('id', '')})"
    return f"{name} ({option.get('id', '')})"


def _format_project_option(option: Dict[str, Any]) -> str:
    name = option.get("name") or option.get("id", "")
    return f"{name} ({option.get('id', '')})"


def _format_compute_group_option(option: Dict[str, Any]) -> str:
    name = option.get("name") or option.get("id", "")
    gpu_type = option.get("gpu_type", "")
    parts = [gpu_type] if gpu_type else []
    capacity_summary = _format_capacity_summary(option)
    if capacity_summary:
        if option.get("usage_scope") == "shared_physical_pool":
            parts.append(f"共享池占用 {capacity_summary}")
        else:
            parts.append(capacity_summary)
    suffix = f" [{' | '.join(parts)}]" if parts else ""
    return f"{name}{suffix} ({option.get('id', '')})"


def _format_spec_option(option: Dict[str, Any]) -> str:
    name = option.get("name") or option.get("id", "")
    parts = []
    gpu_type = option.get("gpu_type_display") or option.get("gpu_type")
    if gpu_type:
        parts.append(str(gpu_type))
    if option.get("gpu_count"):
        parts.append(f"{option['gpu_count']} GPU")
    if option.get("cpu_count"):
        parts.append(f"{option['cpu_count']} CPU")
    if option.get("memory_gb"):
        parts.append(f"{option['memory_gb']} GiB")
    detail = " | ".join(parts)
    if detail:
        return f"{name} [{detail}] ({option.get('id', '')})"
    return f"{name} ({option.get('id', '')})"


def _run_create_interactive(args, display, api) -> int:
    """仅补齐 create 缺失参数；已显式传入的参数全部跳过。"""
    display.print("\n[bold]交互式任务提交[/bold]")

    workspace_id = None
    ws_display = ""
    project_display = ""
    compute_group_display = ""
    prefetched_snapshot: Optional[Dict[str, Any]] = None
    workspace_options: List[Dict[str, Any]] = []
    workspace_detail: Dict[str, Any] = {}
    needs_resource_interaction = (
        not args.workspace
        or not args.project
        or not args.compute_group
        or not args.spec
    )

    if needs_resource_interaction:
        prefetched_snapshot = _load_create_interactive_snapshot_if_available()
        if prefetched_snapshot is None:
            display.print(
                "[dim]未找到可复用的 create -i 本地快照，正在按需预加载...[/dim]"
            )
            prefetched_snapshot = _prefetch_create_interactive_snapshot_on_demand(
                api,
                display,
                args.workspace or "",
            )
            if prefetched_snapshot is None:
                display.print_error("未能为 create -i 预加载可用资源快照")
                display.print(
                    "[dim]请先运行 qzcli login，确认工作空间可访问后重试[/dim]"
                )
                return 1

        workspace_options = list(
            (prefetched_snapshot or {}).get("workspace_options") or []
        )
        if args.workspace:
            workspace_id, ws_display = _resolve_workspace_option_from_snapshot(
                workspace_options, args.workspace
            )
            if not workspace_id:
                display.print(
                    f"[dim]当前本地快照未包含工作空间 '{args.workspace}'，正在按需刷新该 workspace...[/dim]"
                )
                prefetched_snapshot = _prefetch_create_interactive_snapshot_on_demand(
                    api,
                    display,
                    args.workspace,
                )
                if prefetched_snapshot is None:
                    display.print_error(
                        f"未能为工作空间 '{args.workspace}' 预加载资源快照"
                    )
                    display.print(
                        "[dim]请先运行 qzcli login，确认工作空间可访问后重试[/dim]"
                    )
                    return 1
                workspace_options = list(
                    (prefetched_snapshot or {}).get("workspace_options") or []
                )
                workspace_id, ws_display = _resolve_workspace_option_from_snapshot(
                    workspace_options, args.workspace
                )
                if not workspace_id:
                    display.print_error(f"未找到工作空间 '{args.workspace}'")
                    display.print(
                        "[dim]请确认名称/ID 正确，或检查当前账号是否有该 workspace 访问权限[/dim]"
                    )
                    return 1
            args.workspace = workspace_id

    use_hierarchy_tui = _can_use_arrow_select() and needs_resource_interaction

    if use_hierarchy_tui:
        picker_result = _run_resource_hierarchy_tui(
            api,
            display,
            initial_workspace_id=workspace_id or "",
            initial_ws_display=ws_display,
            initial_project_value=args.project or "",
            initial_project_display=project_display,
            initial_compute_group_value=args.compute_group or "",
            initial_compute_group_display=compute_group_display,
            initial_spec_value=args.spec or "",
            prefetched_snapshot=prefetched_snapshot,
        )
        if picker_result is None:
            return 1
        workspace_id = picker_result.get("workspace_id", workspace_id)
        ws_display = picker_result.get("ws_display", ws_display)
        project_display = picker_result.get("project_display", project_display)
        compute_group_display = picker_result.get(
            "compute_group_display", compute_group_display
        )
        args.workspace = workspace_id or args.workspace
        args.project = picker_result.get("project_id") or args.project
        args.compute_group = picker_result.get("compute_group_id") or args.compute_group
        args.spec = picker_result.get("spec_id") or args.spec
        setattr(
            args, "_manual_spec_input", bool(picker_result.get("manual_spec_input"))
        )
    elif not args.workspace:
        if not workspace_options:
            display.print_error("未找到可用工作空间")
            display.print(
                "[dim]请先运行 qzcli login，确认当前账号存在可访问的工作空间[/dim]"
            )
            return 1
        if not any(_has_capacity_summary(option) for option in workspace_options):
            display.print("[dim]当前未获取到实时占用，以下为缓存的工作空间列表[/dim]")

        selected_workspace = _select_interactive_option(
            display,
            "选择工作空间",
            workspace_options,
            _format_workspace_option,
            renderer=_render_workspace_selection_table,
            arrow_table_builder=_build_workspace_choice_table,
            arrow_context_lines=_build_workspace_choice_context_lines(
                workspace_options
            ),
        )
        if not selected_workspace:
            return 1
        workspace_id = selected_workspace["id"]
        ws_display = selected_workspace.get("name", workspace_id)
        args.workspace = workspace_id
    elif args.workspace:
        workspace_id = args.workspace

    if prefetched_snapshot and workspace_id:
        workspace_detail = dict(
            (
                (prefetched_snapshot.get("workspace_details_by_id") or {}).get(
                    workspace_id
                )
                or {}
            )
        )
        if not ws_display:
            ws_display = str(
                workspace_detail.get("name", "") or ws_display or workspace_id
            )

    ws_resources = (
        (workspace_detail.get("resources") or {})
        if workspace_detail
        else (get_workspace_resources(workspace_id) if workspace_id else None)
    )

    if args.project and ws_resources:
        resolved_project_id, resolved_project_display = _resolve_cached_resource_value(
            workspace_id,
            "projects",
            args.project,
            workspace_resources=ws_resources,
        )
        if not resolved_project_id:
            display.print_error(f"未找到项目 '{args.project}'")
            display.print(
                "[dim]请重试 create -i 以触发按需刷新，或先运行 qzcli res -u 更新资源缓存[/dim]"
            )
            return 1
        if (
            _validate_cached_resource_membership(
                workspace_id,
                "projects",
                resolved_project_id,
                workspace_resources=ws_resources,
            )
            is False
        ):
            display.print_error(
                f"项目 '{args.project}' 不属于当前工作空间 '{ws_display or workspace_id}'"
            )
            display.print(
                "[dim]请重新选择项目，或重试 create -i 以刷新当前工作空间快照[/dim]"
            )
            return 1
        args.project = resolved_project_id
        project_display = resolved_project_display or resolved_project_id

    if args.compute_group and ws_resources:
        resolved_compute_group_id, resolved_compute_group_display = (
            _resolve_cached_resource_value(
                workspace_id,
                "compute_groups",
                args.compute_group,
                workspace_resources=ws_resources,
            )
        )
        if not resolved_compute_group_id:
            display.print_error(f"未找到计算组 '{args.compute_group}'")
            display.print(
                "[dim]请重试 create -i 以触发按需刷新，或先运行 qzcli res -u 更新资源缓存[/dim]"
            )
            return 1
        if (
            _validate_cached_resource_membership(
                workspace_id,
                "compute_groups",
                resolved_compute_group_id,
                workspace_resources=ws_resources,
            )
            is False
        ):
            display.print_error(
                f"计算组 '{args.compute_group}' 不属于当前工作空间 '{ws_display or workspace_id}'"
            )
            display.print(
                "[dim]请重新选择计算组，或重试 create -i 以刷新当前工作空间快照[/dim]"
            )
            return 1
        args.compute_group = resolved_compute_group_id
        compute_group_display = (
            resolved_compute_group_display or resolved_compute_group_id
        )

    if not args.project:
        project_options = list(workspace_detail.get("project_options") or [])
        if not project_options:
            project_options = _sort_project_options_for_selection(
                list((ws_resources or {}).get("projects", {}).values())
            )
        if not project_options:
            display.print_error("当前工作空间没有可选项目")
            display.print(
                "[dim]请重试 create -i 以刷新当前工作空间快照，或先运行 qzcli res -u[/dim]"
            )
            return 1
        selected_project = _select_interactive_option(
            display,
            "选择项目",
            project_options,
            _format_project_option,
            arrow_table_builder=_build_project_choice_table,
            arrow_context_lines=[f"工作空间: {ws_display}"],
        )
        if not selected_project:
            return 1
        args.project = selected_project["id"]
        project_display = selected_project.get("name", args.project)

    if not args.compute_group:
        compute_group_options = list(
            workspace_detail.get("compute_group_options") or []
        )
        if not compute_group_options:
            compute_group_options = _sort_compute_group_options_for_selection(
                list((ws_resources or {}).get("compute_groups", {}).values())
            )
        if not compute_group_options:
            display.print_error("当前工作空间没有可选计算组")
            display.print(
                "[dim]请重试 create -i 以刷新当前工作空间快照，或先运行 qzcli res -u[/dim]"
            )
            return 1
        if not any(_has_capacity_summary(option) for option in compute_group_options):
            display.print("[dim]当前未获取到实时占用，以下为缓存的计算组列表[/dim]")
        elif any(
            option.get("usage_scope") == "shared_physical_pool"
            for option in compute_group_options
        ):
            display.print(
                "[dim]带“共享池占用”的数值反映底层物理 compute group 的实时占用，不等价于该逻辑组的真实可提交容量[/dim]"
            )
        selected_compute_group = _select_interactive_option(
            display,
            "选择计算组",
            compute_group_options,
            _format_compute_group_option,
            renderer=_render_compute_group_selection_table,
            arrow_table_builder=_build_compute_group_choice_table,
            arrow_context_lines=_build_compute_group_choice_context_lines(
                ws_display, project_display or args.project, compute_group_options
            ),
        )
        if not selected_compute_group:
            return 1
        args.compute_group = selected_compute_group["id"]
        compute_group_display = selected_compute_group.get("name", args.compute_group)

    if args.spec and ws_resources:
        resolved_spec_id, _ = _resolve_cached_resource_value(
            workspace_id,
            "specs",
            args.spec,
            workspace_resources=ws_resources,
        )
        if resolved_spec_id:
            args.spec = resolved_spec_id
        spec_membership = _validate_cached_spec_membership(
            workspace_id,
            args.compute_group,
            args.spec,
            workspace_resources=ws_resources,
        )
        if spec_membership is False:
            display.print_error(
                f"规格 '{args.spec}' 不属于当前计算组 '{compute_group_display or args.compute_group}'"
            )
            display.print(
                "[dim]请重新选择规格，或重试 create -i 以刷新当前计算组快照[/dim]"
            )
            return 1

    if not args.spec:
        if not args.compute_group.startswith("lcg-"):
            display.print_error(f"未找到计算组 '{args.compute_group}'")
            display.print(
                "[dim]请使用计算组 ID，或重试 create -i 以刷新当前工作空间快照[/dim]"
            )
            return 1
        spec_result = dict(
            (
                (workspace_detail.get("spec_result_by_compute_group") or {}).get(
                    args.compute_group
                )
                or {}
            )
        )
        if not spec_result:
            spec_result = _build_cached_spec_result(ws_resources, args.compute_group)
        spec_options = list(spec_result.get("items") or [])
        error_message = str(spec_result.get("error", "") or "").strip()
        if error_message and spec_options:
            display.print(
                f"[dim]获取实时规格列表失败，当前展示预加载缓存规格: {error_message}[/dim]"
            )
        elif error_message:
            display.print(f"[dim]获取实时规格列表失败: {error_message}[/dim]")
        if not spec_options:
            display.print("[dim]未拿到可选规格列表，请手动输入 spec ID[/dim]")
            args.spec = _prompt_text_input(display, "资源规格 ID")
            if args.spec is None:
                return 1
            setattr(args, "_manual_spec_input", True)
        else:
            selected_spec = _select_interactive_option(
                display,
                "选择资源规格",
                spec_options,
                _format_spec_option,
                arrow_table_builder=_build_spec_choice_table,
                arrow_context_lines=[
                    f"工作空间: {ws_display}",
                    f"项目: {project_display or args.project}",
                    f"计算组: {compute_group_display or args.compute_group}",
                ],
            )
            if not selected_spec:
                return 1
            args.spec = selected_spec["id"]
            setattr(args, "_manual_spec_input", False)

    if not args.name:
        args.name = _prompt_text_input(display, "任务名称")
        if args.name is None:
            return 1

    if not args.cmd_str:
        args.cmd_str = _prompt_text_input(display, "执行命令")
        if args.cmd_str is None:
            return 1

    if args.image is None:
        args.image = _prompt_text_input(
            display, "Docker 镜像", DEFAULT_CREATE_IMAGE, required=True
        )
        if args.image is None:
            return 1

    if args.image_type is None:
        args.image_type = _prompt_text_input(
            display, "镜像类型", DEFAULT_CREATE_IMAGE_TYPE, required=True
        )
        if args.image_type is None:
            return 1

    if args.instances is None:
        args.instances = _prompt_int_input(
            display, "实例数量", DEFAULT_CREATE_INSTANCES, min_value=1
        )
        if args.instances is None:
            return 1

    if args.shm is None:
        args.shm = _prompt_int_input(
            display, "共享内存 GiB", DEFAULT_CREATE_SHM, min_value=1
        )
        if args.shm is None:
            return 1

    if args.priority is None:
        args.priority = _prompt_int_input(
            display, "任务优先级", DEFAULT_CREATE_PRIORITY, min_value=1, max_value=10
        )
        if args.priority is None:
            return 1

    if args.framework is None:
        args.framework = _prompt_text_input(
            display, "框架类型", DEFAULT_CREATE_FRAMEWORK, required=True
        )
        if args.framework is None:
            return 1

    return 0


def cmd_create(args):
    """创建任务"""
    display = get_display()
    api = get_api()
    store = get_store()

    if getattr(args, "interactive", False):
        ret = _run_create_interactive(args, display, api)
        if ret != 0:
            return ret

    if not args.name:
        display.print_error("请指定任务名称: --name <name>")
        return 1
    if not args.cmd_str:
        display.print_error("请指定执行命令: --command <cmd>")
        return 1

    if args.image is None:
        args.image = DEFAULT_CREATE_IMAGE
    if args.image_type is None:
        args.image_type = DEFAULT_CREATE_IMAGE_TYPE
    if args.instances is None:
        args.instances = DEFAULT_CREATE_INSTANCES
    if args.shm is None:
        args.shm = DEFAULT_CREATE_SHM
    if args.priority is None:
        args.priority = DEFAULT_CREATE_PRIORITY
    if args.framework is None:
        args.framework = DEFAULT_CREATE_FRAMEWORK

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
        project_id, proj_display = _resolve_cached_resource_value(
            workspace_id,
            "projects",
            args.project,
            workspace_resources=ws_resources,
        )
        if not project_id:
            display.print_error(f"未找到项目 '{args.project}'")
            display.print("[dim]使用 qzcli res -w <workspace> 查看可用项目[/dim]")
            return 1
        if (
            _validate_cached_resource_membership(
                workspace_id,
                "projects",
                project_id,
                workspace_resources=ws_resources,
            )
            is False
        ):
            display.print_error(
                f"项目 '{args.project}' 不属于当前工作空间 '{ws_display}'"
            )
            display.print(
                "[dim]请先运行 qzcli res -w <workspace> -u 刷新缓存，或改用正确的项目 ID[/dim]"
            )
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
        compute_group_id, cg_display = _resolve_cached_resource_value(
            workspace_id,
            "compute_groups",
            args.compute_group,
            workspace_resources=ws_resources,
        )
        if not compute_group_id:
            display.print_error(f"未找到计算组 '{args.compute_group}'")
            display.print("[dim]使用 qzcli res -w <workspace> 查看可用计算组[/dim]")
            return 1
        if (
            _validate_cached_resource_membership(
                workspace_id,
                "compute_groups",
                compute_group_id,
                workspace_resources=ws_resources,
            )
            is False
        ):
            display.print_error(
                f"计算组 '{args.compute_group}' 不属于当前工作空间 '{ws_display}'"
            )
            display.print(
                "[dim]请先运行 qzcli res -w <workspace> -u 刷新缓存，或改用正确的计算组 ID[/dim]"
            )
            return 1
    else:
        compute_group_id, cg_display = _auto_select_resource(
            workspace_id, "compute_groups"
        )
        if not compute_group_id:
            display.print_error("未指定计算组且缓存中无可用计算组")
            display.print("[dim]使用 --compute-group 指定，或先运行 qzcli res -u[/dim]")
            return 1
        display.print(f"[dim]自动选择计算组: {cg_display} ({compute_group_id})[/dim]")

    # --- Resolve spec ---
    spec_id = None
    if args.spec:
        if getattr(args, "_manual_spec_input", False):
            spec_id = args.spec
        else:
            spec_id, _ = _resolve_cached_resource_value(
                workspace_id,
                "specs",
                args.spec,
                workspace_resources=ws_resources,
            )
            if not spec_id:
                display.print_error(f"未找到资源规格 '{args.spec}'")
                display.print(
                    "[dim]使用 qzcli res -w <workspace> 查看缓存规格，或直接传入完整 spec UUID[/dim]"
                )
                return 1
            spec_membership = _validate_cached_spec_membership(
                workspace_id,
                compute_group_id,
                spec_id,
                workspace_resources=ws_resources,
            )
            if spec_membership is False:
                display.print_error(
                    f"规格 '{args.spec}' 不属于当前计算组 '{cg_display or compute_group_id}'"
                )
                display.print(
                    "[dim]请先运行 qzcli res -w <workspace> -u 刷新缓存，或改用正确的 spec ID[/dim]"
                )
                return 1
    else:
        spec_id, spec_display = _auto_select_spec_for_compute_group(
            workspace_id, compute_group_id
        )
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
        "framework_config": [
            {
                "spec_id": spec_id,
                "image": args.image,
                "image_type": args.image_type,
                "instance_count": args.instances,
                "shm_gi": args.shm,
            }
        ],
    }

    # --- Dry run ---
    if args.dry_run:
        import json as json_mod

        display.print("[bold]Dry run - 以下为将要提交的 payload:[/bold]\n")
        print(json_mod.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    # --- Show summary ---
    display.print("\n[bold]创建任务[/bold]")
    display.print(f"  名称: {args.name}")
    display.print(f"  工作空间: {ws_display} ({workspace_id})")
    display.print(f"  项目: {proj_display} ({project_id})")
    display.print(f"  计算组: {cg_display} ({compute_group_id})")
    display.print(f"  规格: {spec_id}")
    display.print(f"  镜像: {args.image}")
    display.print(f"  实例数: {args.instances}")
    display.print(f"  共享内存: {args.shm} GiB")
    display.print(f"  优先级: {args.priority}")
    display.print(
        f"  命令: {args.cmd_str[:120]}{'...' if len(args.cmd_str) > 120 else ''}"
    )
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

    display.print_success("任务创建成功!")
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
        display.print("  [dim]已自动追踪到本地[/dim]")

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


def cmd_hpc(args):
    """提交 HPC/CPU 任务"""
    import json as json_mod

    display = get_display()
    api = get_api()
    store = get_store()

    cookie_data = get_cookie()
    if not cookie_data:
        display.print_error("未找到 cookie，请先运行: qzcli login")
        return 1
    cookie = cookie_data.get("cookie", "")
    if not cookie:
        display.print_error("cookie 为空，请先运行: qzcli login")
        return 1

    # Resolve workspace
    workspace_id = args.workspace
    if not workspace_id.startswith("ws-"):
        workspace_id = find_workspace_by_name(args.workspace)
        if not workspace_id:
            display.print_error(f"未找到名称为 '{args.workspace}' 的工作空间")
            return 1

    # Resolve project
    project_id = args.project
    if project_id and not project_id.startswith("project-"):
        pid, _ = _resolve_resource_id(workspace_id, "projects", project_id)
        if not pid:
            display.print_error(f"未找到项目 '{args.project}'")
            return 1
        project_id = pid
    if not project_id:
        project_id, _ = _auto_select_resource(workspace_id, "projects")
        if not project_id:
            display.print_error("未指定项目且缓存中无可用项目，请用 --project 指定")
            return 1

    display.print("\n[bold]HPC 任务提交[/bold]")
    display.print(f"  名称: {args.name}")
    display.print(f"  计算组: {args.compute_group}")
    display.print(
        f"  规格: {args.predef_quota_id} (cpu={args.cpu}, mem={args.mem_gi}GiB)"
    )
    display.print(f"  节点数: {args.instances}  cpus/task: {args.cpus_per_task}")
    display.print(
        f"  命令: {args.entrypoint[:120]}{'...' if len(args.entrypoint) > 120 else ''}"
    )
    display.print("")

    try:
        result = api.create_hpc_job(
            cookie=cookie,
            job_name=args.name,
            workspace_id=workspace_id,
            project_id=project_id,
            logic_compute_group_id=args.compute_group,
            entrypoint=args.entrypoint,
            image=args.image,
            predef_quota_id=args.predef_quota_id,
            cpu=args.cpu,
            mem_gi=args.mem_gi,
            instances=args.instances,
            cpus_per_task=args.cpus_per_task,
            memory_per_cpu=args.memory_per_cpu,
            image_type=args.image_type,
        )
    except QzAPIError as e:
        display.print_error(f"任务创建失败: {e}")
        return 1

    job_id = result.get("job_id", "")
    if not job_id:
        display.print_error("任务创建失败: 响应中未包含 job_id")
        if args.output_json:
            print(json_mod.dumps(result, indent=2, ensure_ascii=False))
        return 1

    job_url = f"https://qz.sii.edu.cn/jobs/hpc?spaceId={workspace_id}"
    display.print_success("HPC 任务创建成功!")
    display.print(f"  Job ID: [cyan]{job_id}[/cyan]")
    display.print(f"  链接: {job_url}")

    if not args.no_track:
        job = JobRecord(
            job_id=job_id,
            name=args.name,
            status="job_pending",
            workspace_id=workspace_id,
            project_id=project_id,
            source="qzcli hpc",
            command=args.entrypoint,
            url=job_url,
            instance_count=args.instances,
        )
        store.add(job)
        display.print("  [dim]已自动追踪到本地[/dim]")

    if args.output_json:
        print(
            json_mod.dumps(
                {
                    "job_id": job_id,
                    "workspace_id": workspace_id,
                    "url": job_url,
                    "name": args.name,
                },
                ensure_ascii=False,
            )
        )

    return 0


def cmd_hpc_usage(args):
    """查看 HPC 任务 CPU/内存利用率（基于节点维度统计）"""
    display = get_display()
    api = get_api()

    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未设置 cookie，请先运行: qzcli login")
        return 1
    cookie = cookie_data["cookie"]

    workspace_input = args.workspace
    if not workspace_input:
        all_resources = load_all_resources()
        if not all_resources:
            display.print_error("没有已缓存的工作空间，请先运行: qzcli res -u")
            return 1
        workspace_ids = [
            (ws_id, data.get("name", "")) for ws_id, data in all_resources.items()
        ]
    elif workspace_input.startswith("ws-"):
        ws_resources = get_workspace_resources(workspace_input)
        workspace_ids = [
            (workspace_input, ws_resources.get("name", "") if ws_resources else "")
        ]
    else:
        wid = find_workspace_by_name(workspace_input)
        if not wid:
            display.print_error(f"未找到名称为 '{workspace_input}' 的工作空间")
            return 1
        ws_resources = get_workspace_resources(wid)
        workspace_ids = [(wid, ws_resources.get("name", wid) if ws_resources else wid)]

    lcg_id = args.compute_group or ""

    for workspace_id, ws_name in workspace_ids:
        display.print(
            f"[dim]正在查询 {ws_name or workspace_id} 的 HPC 节点利用率...[/dim]"
        )
        try:
            # 分页获取所有节点
            nodes = []
            page_num = 1
            page_size = 200
            while True:
                data = api.list_node_dimension(
                    workspace_id,
                    cookie,
                    logic_compute_group_id=lcg_id or None,
                    page_num=page_num,
                    page_size=page_size,
                )
                batch = data.get("node_dimensions", [])
                total = data.get("total", 0)
                nodes.extend(batch)
                if len(nodes) >= total or len(batch) < page_size:
                    break
                page_num += 1

            # 只保留 HPC 节点
            hpc_nodes = [n for n in nodes if n.get("node_type", "") == "hpc"]
            if not hpc_nodes:
                display.print(f"  [dim]{ws_name or workspace_id}: 无 HPC 节点[/dim]")
                continue

            total_nodes = len(hpc_nodes)
            cpu_rates = [n.get("cpu", {}).get("usage_rate", 0) for n in hpc_nodes]
            mem_rates = [n.get("memory", {}).get("usage_rate", 0) for n in hpc_nodes]
            avg_cpu = sum(cpu_rates) / total_nodes * 100
            avg_mem = sum(mem_rates) / total_nodes * 100
            busy_nodes = sum(1 for r in cpu_rates if r > 0.05)

            display.print(f"\n[bold]{ws_name or workspace_id}[/bold]")
            display.print(
                f"  HPC 节点总数: {total_nodes}  忙碌节点 (CPU>5%): {busy_nodes}"
            )
            display.print(f"  平均 CPU 利用率: [cyan]{avg_cpu:.1f}%[/cyan]")
            display.print(f"  平均内存利用率: [cyan]{avg_mem:.1f}%[/cyan]")

            if args.verbose:
                display.print(
                    f"\n  {'节点名称':<20} {'CPU%':>7} {'MEM%':>7} {'CPU用/总':>12} {'MEM用/总(GiB)':>16}"
                )
                display.print("  " + "-" * 65)
                for node in sorted(
                    hpc_nodes,
                    key=lambda n: -n.get("cpu", {}).get("usage_rate", 0),
                )[: args.top]:
                    name = node.get("name", "")
                    cpu = node.get("cpu", {})
                    mem = node.get("memory", {})
                    cpu_pct = cpu.get("usage_rate", 0) * 100
                    mem_pct = mem.get("usage_rate", 0) * 100
                    cpu_used = cpu.get("used", 0)
                    cpu_total = cpu.get("total", 0)
                    mem_used = mem.get("used", 0)
                    mem_total = mem.get("total", 0)
                    display.print(
                        f"  {name:<20} {cpu_pct:>6.1f}% {mem_pct:>6.1f}% {cpu_used:>5}/{cpu_total:<5} {mem_used:>7.1f}/{mem_total:<7.1f}"
                    )

        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新设置: qzcli login")
                return 1
            display.print_warning(f"查询 {ws_name or workspace_id} 失败: {e}")

    return 0


def cmd_batch(args):
    """批量提交任务"""
    import itertools
    import json as json_mod

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

    display.print("\n[bold]批量任务提交[/bold]")
    display.print(f"  配置文件: {config_path}")
    display.print(
        f"  矩阵维度: {' x '.join(f'{k}({len(matrix[k]) if isinstance(matrix[k], list) else 1})' for k in keys)}"
    )
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
            display.print(
                f"    命令: {command[:120]}{'...' if len(command) > 120 else ''}"
            )
            display.print("")
            continue

        display.print(f"[bold][{idx}/{total}][/bold] 提交: {job_name}")

        # Build argparse-like namespace for cmd_create
        create_args = argparse.Namespace(
            interactive=False,
            name=job_name,
            cmd_str=command,
            workspace=defaults.get("workspace", ""),
            project=defaults.get("project", ""),
            compute_group=defaults.get("compute_group", ""),
            spec=defaults.get("spec", ""),
            image=defaults.get(
                "image",
                "docker.sii.shaipower.online/inspire-studio/dhyu-wan-torch29:0.4",
            ),
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
                display.print_error(
                    "任务提交失败，停止批量提交（使用 --continue-on-error 忽略错误）"
                )
                break

        # Delay between submissions
        if idx < total and not args.dry_run:
            time.sleep(args.delay)

    if args.dry_run:
        display.print(f"[bold]预览完成，共 {total} 个任务[/bold]")
        return 0

    display.print("\n[bold]批量提交完成[/bold]")
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
                ws_id,
                cookie,
                page_size=50,
                user_ids=user_ids,
                status=["RUNNING"],
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

    display.print(
        f"[dim]找到开发机: {notebook_name} (notebook_id: {notebook_id[:8]}...)[/dim]"
    )

    # 2. 通过 /api/v1/notebook/lab/{notebook_id} 获取 Jupyter URL（含 token）
    try:
        resp = _requests.get(
            f"https://qz.sii.edu.cn/api/v1/notebook/lab/{notebook_id}",
            headers={
                "cookie": cookie,
                "user-agent": "Mozilla/5.0",
                "accept": "text/html",
            },
            allow_redirects=False,
            timeout=15,
        )
        if resp.status_code in (301, 302, 303, 307):
            jupyter_url = resp.headers.get("Location", "")
            # URL 格式: https://{domain}/{ws}/{proj}/{user}/jupyter/{nb_id}/{token}/lab?token={token}
            match = re.search(
                r"(https://[^/]+/[^/]+/[^/]+/[^/]+/jupyter/[^/]+/([^/]+))/lab",
                jupyter_url,
            )
            if match:
                base_url = match.group(1)
                token = match.group(2)
                display.print("[dim]已获取 Jupyter 连接信息[/dim]")
                return {
                    "base_url": base_url,
                    "token": token,
                    "notebook_id": notebook_id,
                }

        if (
            resp.status_code == 401
            or resp.status_code == 302
            and "keycloak" in resp.headers.get("Location", "")
        ):
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
                _requests.delete(
                    f"{base_http}/api/contents/{fname}", headers=headers, timeout=5
                )
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
        f"mkdir -p {tmp_dir} && "
        f'{{ [ -L "$PWD/{api_dir}" ] || {{ rm -rf "$PWD/{api_dir}" && ln -sf {tmp_dir} "$PWD/{api_dir}"; }}; }} && '
        f"( {cmd_str} ) > {tmp_dir}/{job_id}_out 2>&1; "
        f"echo $? > {tmp_dir}/{job_id}_exit"
    )

    launched = False
    for attempt in range(3):
        try:
            resp_t = _requests.get(
                f"{base_http}/api/terminals", headers=headers, timeout=10
            )
            terms = resp_t.json() if resp_t.status_code == 200 else []
            if terms:
                term_name = terms[0]["name"]
            else:
                resp_t = _requests.post(
                    f"{base_http}/api/terminals", headers=headers, timeout=10
                )
                term_name = resp_t.json()["name"]

            ws = websocket.create_connection(
                f"{base_ws}/terminals/websocket/{term_name}?token={token}",
                timeout=10,
            )
            _time.sleep(0.3)
            while True:
                try:
                    ws.settimeout(0.3)
                    ws.recv()
                except Exception:
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
                headers=headers,
                timeout=10,
            )
            if resp_exit.status_code == 200:
                exit_str = resp_exit.json().get("content", "").strip()
                exit_code = int(exit_str) if exit_str.isdigit() else 1

                resp_out = _requests.get(
                    f"{base_http}/api/contents/{api_out}",
                    headers=headers,
                    timeout=10,
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
    stored_username, stored_password = get_credentials()

    # 获取用户名
    username = (args.username or stored_username or "").strip()
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
    password = ""
    if args.password:
        password = args.password
    elif getattr(args, "password_stdin", False):
        try:
            password = sys.stdin.readline().rstrip("\n")
        except (EOFError, KeyboardInterrupt):
            display.print_error("未从 stdin 读取到密码")
            return 1
        if not password:
            display.print_error("未从 stdin 读取到密码")
            return 1
    elif stored_password:
        password = stored_password
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


def _rewrite_legacy_create_short_flags(argv: List[str]) -> List[str]:
    """兼容历史上的 `qzcli create -i <image>` 用法。"""
    if len(argv) < 3 or argv[1] not in {"create", "create-job"}:
        return list(argv)

    rewritten = list(argv[:2])
    idx = 2
    while idx < len(argv):
        token = argv[idx]
        if token == "-i":
            next_token = argv[idx + 1] if idx + 1 < len(argv) else ""
            rewritten.append(
                "--image"
                if next_token and not next_token.startswith("-")
                else "--interactive"
            )
            idx += 1
            continue
        if token.startswith("-i="):
            rewritten.append(f"--image={token[3:]}")
            idx += 1
            continue
        if token.startswith("-i") and len(token) > 2:
            rewritten.extend(["--image", token[2:]])
            idx += 1
            continue
        rewritten.append(token)
        idx += 1
    return rewritten


def main():
    """主入口"""
    parser = argparse.ArgumentParser(
        prog="qzcli",
        description="启智平台任务管理 CLI 工具",
    )
    parser.add_argument(
        "--version", "-V", action="version", version=f"qzcli {__version__}"
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
    list_parser.add_argument(
        "--running", "-r", action="store_true", help="只显示运行中/排队中的任务"
    )
    list_parser.add_argument("--no-refresh", action="store_true", help="不更新状态")
    list_parser.add_argument(
        "--verbose", "-v", action="store_true", help="显示详细信息"
    )
    list_parser.add_argument(
        "--url",
        "-u",
        action="store_true",
        default=True,
        help="显示任务链接（默认开启）",
    )
    list_parser.add_argument(
        "--wide", action="store_true", default=True, help="宽格式显示（默认开启）"
    )
    list_parser.add_argument(
        "--compact", action="store_true", help="紧凑表格格式（关闭宽格式）"
    )
    # Cookie 模式参数
    list_parser.add_argument(
        "--cookie",
        "-c",
        action="store_true",
        help="使用 cookie 从 API 获取任务（无需本地 store）",
    )
    list_parser.add_argument(
        "--workspace", "-w", help="工作空间（名称或 ID，cookie 模式）"
    )
    list_parser.add_argument(
        "--all-ws", action="store_true", help="查询所有已缓存的工作空间（cookie 模式）"
    )
    # 交互式建模（开发机）
    list_parser.add_argument(
        "--include-interactive",
        "-I",
        action="store_true",
        help="同时显示交互式建模实例（开发机）",
    )
    list_parser.add_argument(
        "--only-interactive",
        "-i",
        action="store_true",
        help="只显示交互式建模实例（开发机）",
    )
    list_parser.add_argument(
        "--all-users",
        action="store_true",
        help="显示所有用户的开发机（默认只显示自己的）",
    )

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
    watch_parser.add_argument(
        "--interval", "-i", type=int, default=10, help="刷新间隔（秒）"
    )
    watch_parser.add_argument(
        "--limit", "-n", type=int, default=30, help="显示数量限制"
    )
    watch_parser.add_argument(
        "--keep-alive", "-k", action="store_true", help="所有任务完成后继续监控"
    )

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
    import_parser.add_argument(
        "--refresh", "-r", action="store_true", help="导入后更新状态"
    )

    # remove 命令
    remove_parser = subparsers.add_parser("remove", aliases=["rm"], help="删除任务记录")
    remove_parser.add_argument("job_id", help="任务 ID")
    remove_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")

    # clear 命令
    clear_parser = subparsers.add_parser("clear", help="清空所有任务记录")
    clear_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")

    # cookie 命令
    cookie_parser = subparsers.add_parser(
        "cookie", help="设置浏览器 cookie（用于访问内部 API）"
    )
    cookie_parser.add_argument("cookie", nargs="?", help="浏览器 cookie 字符串")
    cookie_parser.add_argument("--file", "-f", help="从文件读取 cookie")
    cookie_parser.add_argument("--workspace", "-w", help="默认工作空间 ID")
    cookie_parser.add_argument("--show", action="store_true", help="显示当前 cookie")
    cookie_parser.add_argument("--clear", action="store_true", help="清除 cookie")
    cookie_parser.add_argument(
        "--no-test", action="store_true", help="不测试 cookie 有效性"
    )

    # login 命令
    login_parser = subparsers.add_parser(
        "login", help="通过 CAS 统一认证登录获取 cookie"
    )
    login_parser.add_argument("--username", "-u", help="学工号")
    login_parser.add_argument(
        "--password", "-p", help="密码（含特殊字符时建议用单引号或 --password-stdin）"
    )
    login_parser.add_argument(
        "--password-stdin",
        action="store_true",
        help="从 stdin 读取密码（适合脚本: echo 'pass' | qzcli login -u user --password-stdin）",
    )
    login_parser.add_argument("--workspace", "-w", help="默认工作空间 ID")

    # exec 命令
    exec_parser = subparsers.add_parser(
        "exec", help="在开发机上执行命令（通过 Jupyter API，无需 SSH）"
    )
    exec_parser.add_argument("host", help="开发机名称（如 blender-rl、rtx-gpu8）")
    exec_parser.add_argument(
        "remote_cmd", nargs=argparse.REMAINDER, help="要执行的命令"
    )

    # workspace 命令
    workspace_parser = subparsers.add_parser(
        "workspace", aliases=["ws"], help="查看工作空间内所有运行任务"
    )
    workspace_parser.add_argument("--workspace", "-w", help="工作空间 ID")
    workspace_parser.add_argument(
        "--project", "-p", default="扩散", help="按项目名称过滤（默认: 扩散）"
    )
    workspace_parser.add_argument(
        "--all", "-a", action="store_true", help="显示所有项目（不过滤）"
    )
    workspace_parser.add_argument("--page", type=int, default=1, help="页码")
    workspace_parser.add_argument(
        "--size", type=int, default=100, help="每页数量（默认 100）"
    )
    workspace_parser.add_argument(
        "--sync", "-s", action="store_true", help="同步到本地任务列表"
    )

    # workspaces 命令 - 从历史任务提取资源配置
    workspaces_parser = subparsers.add_parser(
        "workspaces",
        aliases=["lsws", "res", "resources"],
        help="从历史任务提取资源配置（项目、计算组、规格）",
    )
    workspaces_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    workspaces_parser.add_argument(
        "--export", "-e", action="store_true", help="输出可用于脚本的环境变量格式"
    )
    workspaces_parser.add_argument(
        "--update", "-u", action="store_true", help="强制从 API 更新缓存"
    )
    workspaces_parser.add_argument(
        "--list", "-l", action="store_true", help="列出所有已缓存的工作空间"
    )
    workspaces_parser.add_argument("--name", help="设置工作空间名称（别名）")

    # avail 命令 - 查询空余节点
    avail_parser = subparsers.add_parser(
        "avail", aliases=["av"], help="查询计算组空余节点，帮助决定任务应该提交到哪里"
    )
    avail_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    avail_parser.add_argument(
        "--group", "-g", help="计算组 ID 或名称（可选，不指定则查询所有）"
    )
    avail_parser.add_argument(
        "--nodes", "-n", type=int, help="需要的节点数（推荐模式：找出满足条件的计算组）"
    )
    avail_parser.add_argument(
        "--export", "-e", action="store_true", help="输出可用于脚本的环境变量格式"
    )
    avail_parser.add_argument(
        "--verbose", "-v", action="store_true", help="显示空闲节点名称列表"
    )
    avail_parser.add_argument(
        "--lp",
        "--low-priority",
        action="store_true",
        dest="low_priority",
        help="计算低优任务占用节点（较慢）",
    )

    # usage 命令
    usage_parser = subparsers.add_parser("usage", help="统计工作空间的 GPU 使用分布")
    usage_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    usage_parser.add_argument(
        "--by-user", "-u", action="store_true", help="按用户统计 GPU 使用"
    )
    usage_parser.add_argument(
        "--by-project", "-p", action="store_true", help="按项目统计 GPU 使用"
    )
    usage_parser.add_argument(
        "--by-type", "-t", action="store_true", help="按任务类型统计（训练/建模/部署）"
    )
    usage_parser.add_argument(
        "--by-priority", "-r", action="store_true", help="按优先级统计"
    )

    # create 命令 - 创建任务
    create_parser = subparsers.add_parser(
        "create", aliases=["create-job"], help="创建并提交任务到启智平台"
    )
    create_parser.add_argument(
        "--interactive",
        "-i",
        action="store_true",
        help="进入交互式任务提交模式，仅补齐未显式传入的参数",
    )
    create_parser.add_argument("--name", "-n", help="任务名称")
    create_parser.add_argument("--command", "-c", dest="cmd_str", help="执行命令")
    create_parser.add_argument(
        "--workspace", "-w", help="工作空间 ID 或名称（从 qzcli res 缓存解析）"
    )
    create_parser.add_argument(
        "--project", "-p", help="项目 ID 或名称（不指定则自动选择）"
    )
    create_parser.add_argument(
        "--compute-group", "-g", dest="compute_group", help="计算组 ID 或名称"
    )
    create_parser.add_argument("--spec", "-s", help="资源规格 ID（不指定则自动选择）")
    create_parser.add_argument(
        "--image", "-m", help=f"Docker 镜像（默认 {DEFAULT_CREATE_IMAGE}）"
    )
    create_parser.add_argument(
        "--image-type",
        dest="image_type",
        help=f"镜像类型（默认 {DEFAULT_CREATE_IMAGE_TYPE}）",
    )
    create_parser.add_argument(
        "--instances", type=int, help=f"实例数量（默认 {DEFAULT_CREATE_INSTANCES}）"
    )
    create_parser.add_argument(
        "--shm", type=int, help=f"共享内存 GiB（默认 {DEFAULT_CREATE_SHM}）"
    )
    create_parser.add_argument(
        "--priority",
        type=int,
        help=f"任务优先级 1-10（默认 {DEFAULT_CREATE_PRIORITY}）",
    )
    create_parser.add_argument(
        "--framework", help=f"框架类型（默认 {DEFAULT_CREATE_FRAMEWORK}）"
    )
    create_parser.add_argument("--no-track", action="store_true", help="不自动追踪任务")
    create_parser.add_argument(
        "--dry-run", action="store_true", help="只显示 payload 不提交"
    )
    create_parser.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        help="输出 JSON 格式（供脚本集成）",
    )

    # batch 命令 - 批量提交任务
    hpc_parser = subparsers.add_parser("hpc", help="提交 HPC/CPU 任务到启智平台")
    hpc_parser.add_argument("--name", required=True, help="任务名称")
    hpc_parser.add_argument("--workspace", required=True, help="工作空间名称或 ID")
    hpc_parser.add_argument(
        "--project", default="", help="项目名称或 ID（省略则自动选择）"
    )
    hpc_parser.add_argument(
        "--compute-group",
        dest="compute_group",
        required=True,
        help="计算组 ID（lcg-...）",
    )
    hpc_parser.add_argument(
        "--predef-quota-id", dest="predef_quota_id", required=True, help="预定义配额 ID"
    )
    hpc_parser.add_argument("--cpu", type=int, required=True, help="每节点 CPU 核心数")
    hpc_parser.add_argument(
        "--mem-gi", dest="mem_gi", type=int, required=True, help="每节点内存 GiB"
    )
    hpc_parser.add_argument("--instances", type=int, default=1, help="节点数（默认 1）")
    hpc_parser.add_argument(
        "--cpus-per-task",
        dest="cpus_per_task",
        type=int,
        default=1,
        help="每任务 CPU 数（默认同 --cpu）",
    )
    hpc_parser.add_argument(
        "--memory-per-cpu",
        dest="memory_per_cpu",
        default="5G",
        help="每 CPU 内存（默认 5G）",
    )
    hpc_parser.add_argument("--image", required=True, help="容器镜像地址")
    hpc_parser.add_argument(
        "--image-type",
        dest="image_type",
        default="SOURCE_PRIVATE",
        help="镜像类型（默认 SOURCE_PRIVATE）",
    )
    hpc_parser.add_argument("--entrypoint", required=True, help="运行命令")
    hpc_parser.add_argument("--no-track", action="store_true", help="不追踪任务")
    hpc_parser.add_argument(
        "--json", dest="output_json", action="store_true", help="JSON 输出"
    )

    hpc_usage_parser = subparsers.add_parser(
        "hpc-usage", help="查看 HPC 节点 CPU/内存利用率"
    )
    hpc_usage_parser.add_argument(
        "--workspace", "-w", help="工作空间 ID 或名称（默认查询所有已缓存工作空间）"
    )
    hpc_usage_parser.add_argument(
        "--compute-group",
        dest="compute_group",
        default="",
        help="计算组 ID（lcg-...），省略则查所有 HPC 节点",
    )
    hpc_usage_parser.add_argument(
        "--verbose", "-v", action="store_true", help="显示每个节点的详细利用率"
    )
    hpc_usage_parser.add_argument(
        "--top", type=int, default=30, help="详细模式下显示前 N 个节点（默认 30）"
    )

    batch_parser = subparsers.add_parser("batch", help="从 JSON 配置文件批量提交任务")
    batch_parser.add_argument("config", help="批量配置文件路径（JSON 格式）")
    batch_parser.add_argument("--dry-run", action="store_true", help="只预览不提交")
    batch_parser.add_argument(
        "--delay", type=float, default=3, help="任务间延迟秒数（默认 3）"
    )
    batch_parser.add_argument(
        "--continue-on-error", action="store_true", help="遇到错误继续提交"
    )

    argv = _rewrite_legacy_create_short_flags(sys.argv)
    args = parser.parse_args(argv[1:])

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
        "hpc": cmd_hpc,
        "hpc-usage": cmd_hpc_usage,
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
