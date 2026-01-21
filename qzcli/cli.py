#!/usr/bin/env python3
"""
qzcli - 启智平台任务管理 CLI
"""

import sys
import time
import argparse
from pathlib import Path
from typing import Optional, List

from . import __version__
from .config import init_config, get_credentials, load_config, CONFIG_DIR, save_cookie, get_cookie, clear_cookie
from .api import get_api, QzAPIError
from .store import get_store, JobRecord
from .display import get_display, format_duration, format_time_ago


def cmd_init(args):
    """初始化配置"""
    display = get_display()
    
    username = args.username
    password = args.password
    
    if not username:
        username = input("请输入启智平台用户名: ").strip()
    if not password:
        import getpass
        password = getpass.getpass("请输入密码: ").strip()
    
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


def cmd_list(args):
    """列出任务"""
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
    
    # 测试 cookie 是否有效
    if not args.no_test and workspace_id:
        display.print("正在验证 cookie...")
        api = get_api()
        try:
            result = api.list_workspace_tasks(workspace_id, cookie)
            total = result.get("total", 0)
            display.print_success(f"Cookie 有效！工作空间内有 {total} 个任务")
        except QzAPIError as e:
            display.print_error(f"Cookie 无效: {e}")
            return 1
    
    save_cookie(cookie, workspace_id)
    display.print_success("Cookie 已保存")
    return 0


def cmd_workspace(args):
    """查看工作空间内所有运行任务"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未设置 cookie，请先运行: qzcli cookie --workspace <workspace_id>")
        display.print("[dim]提示: 从浏览器 F12 获取 cookie[/dim]")
        return 1
    
    cookie = cookie_data["cookie"]
    workspace_id = args.workspace or cookie_data.get("workspace_id", "")
    
    if not workspace_id:
        display.print_error("请指定工作空间 ID: qzcli workspace --workspace <id>")
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
                if job_id and not store.get_job(job_id):
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
                    store.add_job(job)
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
    list_parser.add_argument("--wide", "-w", action="store_true", default=True, help="宽格式显示（默认开启）")
    list_parser.add_argument("--compact", "-c", action="store_true", help="紧凑表格格式（关闭宽格式）")
    
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
    
    # workspace 命令
    workspace_parser = subparsers.add_parser("workspace", aliases=["ws"], help="查看工作空间内所有运行任务")
    workspace_parser.add_argument("--workspace", "-w", help="工作空间 ID")
    workspace_parser.add_argument("--project", "-p", default="扩散", help="按项目名称过滤（默认: 扩散）")
    workspace_parser.add_argument("--all", "-a", action="store_true", help="显示所有项目（不过滤）")
    workspace_parser.add_argument("--page", type=int, default=1, help="页码")
    workspace_parser.add_argument("--size", type=int, default=100, help="每页数量（默认 100）")
    workspace_parser.add_argument("--sync", "-s", action="store_true", help="同步到本地任务列表")
    
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
        "workspace": cmd_workspace,
        "ws": cmd_workspace,
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
