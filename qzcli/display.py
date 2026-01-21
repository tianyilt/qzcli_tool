"""
显示渲染模块 - 使用 rich 库
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich.live import Live
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich import box
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from .store import JobRecord


# 状态颜色映射
STATUS_STYLES = {
    "job_succeeded": ("bold green", "✓"),
    "job_failed": ("bold red", "✗"),
    "job_stopped": ("bold yellow", "⏹"),
    "job_running": ("bold cyan", "●"),
    "job_pending": ("bold blue", "◌"),
    "job_queued": ("bold blue", "◌"),
    "unknown": ("dim", "?"),
}

# 状态中文名
STATUS_NAMES = {
    "job_succeeded": "成功",
    "job_failed": "失败",
    "job_stopped": "已停止",
    "job_running": "运行中",
    "job_pending": "等待中",
    "job_queued": "排队中",
    "unknown": "未知",
}


def get_status_display(status: str) -> tuple[str, str, str]:
    """获取状态显示信息 (样式, 图标, 名称)"""
    style, icon = STATUS_STYLES.get(status, STATUS_STYLES["unknown"])
    name = STATUS_NAMES.get(status, status)
    return style, icon, name


def format_time_ago(iso_time: str) -> str:
    """格式化为相对时间"""
    if not iso_time:
        return "-"
    
    try:
        dt = datetime.fromisoformat(iso_time)
        now = datetime.now()
        diff = now - dt
        
        seconds = diff.total_seconds()
        if seconds < 0:
            return "刚刚"
        elif seconds < 60:
            return f"{int(seconds)}秒前"
        elif seconds < 3600:
            return f"{int(seconds / 60)}分钟前"
        elif seconds < 86400:
            return f"{int(seconds / 3600)}小时前"
        elif seconds < 604800:
            return f"{int(seconds / 86400)}天前"
        else:
            return dt.strftime("%m-%d %H:%M")
    except (ValueError, TypeError):
        return "-"


def format_duration(ms_str: str) -> str:
    """格式化运行时长"""
    if not ms_str:
        return "-"
    
    try:
        ms = int(ms_str)
        seconds = ms // 1000
        
        if seconds < 60:
            return f"{seconds}秒"
        elif seconds < 3600:
            minutes = seconds // 60
            secs = seconds % 60
            return f"{minutes}分{secs}秒"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}小时{minutes}分"
    except (ValueError, TypeError):
        return "-"


def truncate_string(s: str, max_len: int) -> str:
    """截断字符串"""
    if len(s) <= max_len:
        return s
    return s[:max_len - 3] + "..."


class Display:
    """显示渲染器"""
    
    def __init__(self):
        if RICH_AVAILABLE:
            self.console = Console()
        else:
            self.console = None
    
    def print(self, *args, **kwargs):
        """打印输出"""
        if self.console:
            self.console.print(*args, **kwargs)
        else:
            print(*args)
    
    def print_error(self, message: str):
        """打印错误"""
        if self.console:
            self.console.print(f"[bold red]错误:[/bold red] {message}")
        else:
            print(f"错误: {message}")
    
    def print_success(self, message: str):
        """打印成功"""
        if self.console:
            self.console.print(f"[bold green]✓[/bold green] {message}")
        else:
            print(f"✓ {message}")
    
    def print_warning(self, message: str):
        """打印警告"""
        if self.console:
            self.console.print(f"[bold yellow]⚠[/bold yellow] {message}")
        else:
            print(f"⚠ {message}")
    
    def print_jobs_table(
        self,
        jobs: List[JobRecord],
        title: Optional[str] = None,
        show_command: bool = False,
        show_url: bool = False,
    ):
        """打印任务列表表格"""
        if not RICH_AVAILABLE:
            self._print_jobs_plain(jobs)
            return
        
        if not jobs:
            self.console.print("[dim]暂无任务记录[/dim]")
            return
        
        # 统计状态
        status_counts: Dict[str, int] = {}
        for job in jobs:
            status_counts[job.status] = status_counts.get(job.status, 0) + 1
        
        # 构建标题
        if title is None:
            title = f"启智平台任务列表 (共 {len(jobs)} 个)"
        
        # 创建表格
        table = Table(
            title=title,
            box=box.SIMPLE,
            show_header=True,
            header_style="bold",
            title_style="bold",
            expand=False,
            padding=(0, 1),
        )
        
        table.add_column("Job ID", style="cyan", no_wrap=True)
        table.add_column("Name", style="white")
        table.add_column("Status", justify="center")
        table.add_column("Created", justify="right")
        table.add_column("Duration", justify="right")
        
        if show_command:
            table.add_column("Command", style="dim")
        
        for job in jobs:
            style, icon, status_name = get_status_display(job.status)
            status_text = Text(f"{icon} {status_name}", style=style)
            
            # 截短 job_id 便于显示
            short_job_id = job.job_id[:20] + "..." if len(job.job_id) > 23 else job.job_id
            
            row = [
                short_job_id,
                truncate_string(job.name, 45),
                status_text,
                format_time_ago(job.created_at),
                format_duration(job.running_time_ms),
            ]
            
            if show_command:
                row.append(truncate_string(job.command, 40))
            
            table.add_row(*row)
        
        self.console.print(table)
        
        # 打印状态统计
        status_summary = []
        for status, count in sorted(status_counts.items()):
            style, icon, name = get_status_display(status)
            status_summary.append(f"[{style}]{icon} {name}: {count}[/{style}]")
        
        if status_summary:
            self.console.print(" | ".join(status_summary))
        
        # 显示 URL 列表
        if show_url:
            self.console.print("")
            self.console.print("[bold]任务链接:[/bold]")
            for job in jobs:
                if job.url:
                    style, icon, _ = get_status_display(job.status)
                    short_name = truncate_string(job.name, 30) if job.name else job.job_id[:20]
                    self.console.print(f"  [{style}]{icon}[/{style}] {short_name}")
                    self.console.print(f"     [link={job.url}]{job.url}[/link]")
    
    def print_jobs_wide(
        self,
        jobs: List[JobRecord],
        title: Optional[str] = None,
    ):
        """宽格式打印任务列表（多行卡片，完整显示 URL）"""
        if not jobs:
            if RICH_AVAILABLE:
                self.console.print("[dim]暂无任务记录[/dim]")
            else:
                print("暂无任务记录")
            return
        
        # 统计状态
        status_counts: Dict[str, int] = {}
        for job in jobs:
            status_counts[job.status] = status_counts.get(job.status, 0) + 1
        
        # 构建标题
        if title is None:
            title = f"启智平台任务列表 (共 {len(jobs)} 个)"
        
        if RICH_AVAILABLE:
            self.console.print(f"[bold]{title}[/bold]")
            self.console.print("")
        else:
            print(f"\n{title}\n")
        
        for idx, job in enumerate(jobs, 1):
            style, icon, status_name = get_status_display(job.status)
            
            # 构建 GPU 和计算组信息行
            gpu_info_parts = []
            if job.gpu_count and job.gpu_type:
                gpu_info_parts.append(f"{job.gpu_count}×{job.gpu_type}")
            elif job.gpu_count:
                gpu_info_parts.append(f"{job.gpu_count} GPU")
            if job.compute_group_name:
                gpu_info_parts.append(job.compute_group_name)
            gpu_info_line = " | ".join(gpu_info_parts) if gpu_info_parts else ""
            
            if RICH_AVAILABLE:
                # 第一行：序号 + 状态 + 时间信息
                self.console.print(
                    f"[bold][{idx}][/bold] [{style}]{icon} {status_name}[/{style}] | "
                    f"{format_time_ago(job.created_at)} | {format_duration(job.running_time_ms)}"
                )
                # 第二行：任务名称
                self.console.print(f"    [white]{job.name}[/white]")
                # 第三行：GPU 和计算组信息
                if gpu_info_line:
                    self.console.print(f"    [dim]{gpu_info_line}[/dim]")
                # 第四行：完整 URL（直接 print 避免 rich 换行）
                if job.url:
                    print(f"    {job.url}")
                self.console.print("")
            else:
                print(f"[{idx}] {icon} {status_name} | {format_time_ago(job.created_at)} | {format_duration(job.running_time_ms)}")
                print(f"    {job.name}")
                if gpu_info_line:
                    print(f"    {gpu_info_line}")
                if job.url:
                    print(f"    {job.url}")
                print("")
        
        # 打印状态统计
        if RICH_AVAILABLE:
            status_summary = []
            for status, count in sorted(status_counts.items()):
                st, ic, nm = get_status_display(status)
                status_summary.append(f"[{st}]{ic} {nm}: {count}[/{st}]")
            if status_summary:
                self.console.print(" | ".join(status_summary))
        else:
            status_summary = []
            for status, count in sorted(status_counts.items()):
                _, ic, nm = get_status_display(status)
                status_summary.append(f"{ic} {nm}: {count}")
            if status_summary:
                print(" | ".join(status_summary))
    
    def _print_jobs_plain(self, jobs: List[JobRecord]):
        """纯文本打印任务列表（无 rich）"""
        if not jobs:
            print("暂无任务记录")
            return
        
        print(f"\n启智平台任务列表 (共 {len(jobs)} 个)")
        print("-" * 100)
        print(f"{'Job ID':<44} {'Name':<30} {'Status':<12} {'Created':<12}")
        print("-" * 100)
        
        for job in jobs:
            _, icon, status_name = get_status_display(job.status)
            print(f"{job.job_id:<44} {truncate_string(job.name, 28):<30} {icon} {status_name:<10} {format_time_ago(job.created_at):<12}")
        
        print("-" * 100)
    
    def print_job_detail(self, job: JobRecord, api_data: Optional[Dict[str, Any]] = None):
        """打印任务详情"""
        if not RICH_AVAILABLE:
            self._print_job_detail_plain(job, api_data)
            return
        
        style, icon, status_name = get_status_display(job.status)
        
        # 基本信息
        info_lines = [
            f"[bold]Job ID:[/bold] {job.job_id}",
            f"[bold]Name:[/bold] {job.name}",
            f"[bold]Status:[/bold] [{style}]{icon} {status_name}[/{style}]",
            f"[bold]Created:[/bold] {job.created_at} ({format_time_ago(job.created_at)})",
        ]
        
        if job.finished_at:
            info_lines.append(f"[bold]Finished:[/bold] {job.finished_at}")
        
        if job.running_time_ms:
            info_lines.append(f"[bold]Duration:[/bold] {format_duration(job.running_time_ms)}")
        
        if job.url:
            info_lines.append(f"[bold]URL:[/bold] [link={job.url}]{job.url}[/link]")
        
        if job.source:
            info_lines.append(f"[bold]Source:[/bold] {job.source}")
        
        # 资源信息
        if job.gpu_count or job.instance_count or job.gpu_type or job.compute_group_name:
            info_lines.append("")
            info_lines.append("[bold]Resources:[/bold]")
            if job.gpu_type and job.gpu_count:
                info_lines.append(f"  GPU: {job.gpu_count}×{job.gpu_type}")
            elif job.gpu_count:
                info_lines.append(f"  GPU: {job.gpu_count}")
            if job.instance_count:
                info_lines.append(f"  Instances: {job.instance_count}")
            if job.compute_group_name:
                info_lines.append(f"  Compute Group: {job.compute_group_name}")
            if job.project_name:
                info_lines.append(f"  Project: {job.project_name}")
        
        # 命令
        if job.command:
            info_lines.append("")
            info_lines.append("[bold]Command:[/bold]")
            info_lines.append(f"  [dim]{truncate_string(job.command, 100)}[/dim]")
        
        panel = Panel(
            "\n".join(info_lines),
            title=f"任务详情: {job.name or job.job_id}",
            border_style="blue",
        )
        self.console.print(panel)
    
    def _print_job_detail_plain(self, job: JobRecord, api_data: Optional[Dict[str, Any]] = None):
        """纯文本打印任务详情"""
        _, icon, status_name = get_status_display(job.status)
        
        print(f"\n{'=' * 60}")
        print(f"任务详情: {job.name or job.job_id}")
        print(f"{'=' * 60}")
        print(f"Job ID:   {job.job_id}")
        print(f"Name:     {job.name}")
        print(f"Status:   {icon} {status_name}")
        print(f"Created:  {job.created_at} ({format_time_ago(job.created_at)})")
        
        if job.finished_at:
            print(f"Finished: {job.finished_at}")
        
        if job.running_time_ms:
            print(f"Duration: {format_duration(job.running_time_ms)}")
        
        if job.url:
            print(f"URL:      {job.url}")
        
        if job.source:
            print(f"Source:   {job.source}")
        
        if job.command:
            print(f"\nCommand:")
            print(f"  {truncate_string(job.command, 100)}")
        
        print(f"{'=' * 60}\n")
    
    def create_progress(self) -> "Progress":
        """创建进度条"""
        if RICH_AVAILABLE:
            return Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=self.console,
            )
        return None
    
    def create_live(self) -> "Live":
        """创建实时显示"""
        if RICH_AVAILABLE:
            return Live(console=self.console, refresh_per_second=1)
        return None


# 全局显示实例
_display_instance: Optional[Display] = None


def get_display() -> Display:
    """获取全局显示实例"""
    global _display_instance
    if _display_instance is None:
        _display_instance = Display()
    return _display_instance
