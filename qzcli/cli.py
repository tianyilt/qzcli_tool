#!/usr/bin/env python3
"""
qzcli - 启智平台任务管理 CLI
"""

import argparse
import contextlib
import re
import shlex
import sys
import threading
import time
import unicodedata
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from . import __version__
from .api import (
    QzAPIError,
    _clear_relogin_failure,
    _relogin_file_lock,
    build_resource_spec_price,
    get_api,
)
from .config import (
    clear_cookie,
    CONFIG_DIR,
    FALLBACK_DEFAULT_PRIORITY,
    find_resource_by_name,
    find_workspace_by_name,
    get_cookie,
    get_credentials,
    get_default_priority,
    get_session_id,
    get_workspace_resources,
    init_config,
    list_cached_workspaces,
    load_all_resources,
    load_config,
    load_create_interactive_snapshot,
    mark_workspace_unavailable,
    save_config,
    save_cookie,
    save_create_interactive_snapshot,
    save_resources,
    set_workspace_name,
    update_workspace_compute_groups,
    update_workspace_projects,
)
from . import priority as _priority
from .diag import last_reason, swallowed
from .display import get_display
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


#: ``qzcli create`` 不指定镜像时的兜底。**它会过期**，所以只作为最后一档，
#: 且用它之前会先试平台和历史 —— 见 :func:`resolve_create_image`。
#:
#: 2026-08-12 的教训：这两个常量曾被无条件使用，而其时
#: ``dhyu-wan-torch29:0.4`` 已从平台删除、``SOURCE_PRIVATE`` 又和公共 registry
#: 冲突（二分实测：同 payload 换成 ``SOURCE_PUBLIC`` 就成功）。于是**任何不指定
#: 镜像的用户必然失败**，还只能拿到一句指不到镜像上的 ``InternalError:
#: Unauthorized``。交互式创建时两者都是默认值，一路回车就中招。
class _ImageResolutionError(Exception):
    """定不出镜像时抛这个，由调用方转成友好报错。"""


def _image_type_from_platform(api, cookie, image):
    """问平台这个镜像到底是公开还是私有。拿不准就返回 ``None``（**不猜**）。

    ⚠️ 这条路径**尚未在真机验证过**（写它时账号处于锁定状态）。``image`` 服务的
    请求体形状比较刁（实测只发 ``page``/``page_size`` 会返回空列表，需要带
    ``filter``），所以这里失败就安静地落到下一档，绝不因为它把 create 打挂。
    """
    try:
        result = api._request_v2(
            "image",
            "ListImages",
            {
                "page": 1,
                "page_size": 200,
                "filter": {"name": image.split("/")[-1].split(":")[0]},
            },
            cookie=cookie,
            referer_path="/jobs/images",
        )
    except Exception as exc:  # noqa: BLE001 —— 这只是加分项，不能拖垮主流程
        swallowed("create/查镜像可见性", exc)
        return None
    for item in (result or {}).get("images") or (result or {}).get("items") or []:
        url = item.get("image_url") or item.get("url") or ""
        if url and url != image:
            continue
        vis = (item.get("visibility") or item.get("source") or "").upper()
        if "PUBLIC" in vis:
            return "SOURCE_PUBLIC"
        if "PRIVATE" in vis:
            return "SOURCE_PRIVATE"
    return None


def _image_from_history(api, cookie, workspace_id, want_image=None):
    """从**用户自己近期真实跑过**的任务里取 ``(image, image_type)``。

    比写死的常量新鲜：每个人都有历史，而常量会随平台删镜像而烂掉。
    ``want_image`` 给定时只找用了那个镜像的任务（用来补它的 image_type）。
    跳过创建就失败的任务和本工具自己提交的任务 —— 它们的镜像多半正是坏的那个。
    """
    try:
        jobs = api.list_jobs_with_cookie(workspace_id, cookie, page_size=100)
        rows = (jobs.get("jobs") if isinstance(jobs, dict) else jobs) or []
    except Exception as exc:  # noqa: BLE001
        swallowed("create/反推历史镜像", exc)
        return None, None
    for job in rows:
        if job.get("status") in ("job_create_failed", "job_queuing"):
            continue
        if "qzcli-" in (job.get("name") or ""):
            continue
        try:
            detail = api._get_job_detail_v2(job["job_id"], cookie) or {}
        except Exception as exc:  # noqa: BLE001
            swallowed("create/反推历史镜像", exc)
            continue
        for fc in detail.get("framework_config") or []:
            img, itype = fc.get("image"), fc.get("image_type")
            if not img or not itype:
                continue
            if want_image and img != want_image:
                continue
            return img, itype
    return None, None


def resolve_create_image(api, cookie, workspace_id, image, image_type, display=None):
    """定出提交用的 ``(image, image_type)``。

    优先级（**用户控制权永远第一**）：

    1. 显式传了就用，**任何推断都不许覆盖**
    2. 没传 ``image_type`` 时向平台查该镜像的真实可见性（best-effort）
    3. 再退回用户自己近期成功任务用过的镜像
    4. 都拿不到就**明确报错说清要传什么** —— 而不是拿一个必然失败的默认值去撞，
       再甩一个 ``InternalError: Unauthorized`` 给用户

    Raises:
        _ImageResolutionError: 第 4 档。
    """
    if image and image_type:
        return image, image_type  # 1. 用户说了算

    if image:
        # 2. 有镜像没类型：先问平台
        guessed = _image_type_from_platform(api, cookie, image)
        if guessed:
            if display:
                display.print(f"[dim]镜像类型取自平台：{guessed}[/dim]")
            return image, guessed
        # 3. 再看自己历史上用同一个镜像时填的什么
        _, hist_type = _image_from_history(api, cookie, workspace_id, want_image=image)
        if hist_type:
            if display:
                display.print(f"[dim]镜像类型取自你近期同镜像的任务：{hist_type}[/dim]")
            return image, hist_type
        raise _ImageResolutionError(
            f"无法确定镜像 {image} 的类型（公开/私有）。\n"
            "  请显式指定：--image-type SOURCE_PUBLIC 或 --image-type SOURCE_PRIVATE\n"
            "  （公共 registry 的镜像填 SOURCE_PRIVATE 会让平台按私有仓鉴权，"
            "报 InternalError: Unauthorized）"
        )

    # 3. 两个都没传：整体取自历史
    hist_image, hist_type = _image_from_history(api, cookie, workspace_id)
    if hist_image and hist_type:
        if display:
            display.print(f"[dim]未指定镜像，沿用你近期任务的：{hist_image}[/dim]")
        return hist_image, image_type or hist_type

    # 4. 明确报错，不拿必然失败的默认值去撞
    raise _ImageResolutionError(
        "未指定镜像，且在你近期的任务里也没找到可参考的镜像。\n"
        "  请显式指定：--image <镜像地址> --image-type SOURCE_PUBLIC|SOURCE_PRIVATE\n"
        "  可用镜像见平台「镜像」页面。"
    )


#: ``qzcli create`` 不带 ``--priority`` 时用的优先级。
#:
#: **数字越小优先级越低。** 实测提交值 → 平台档位：1→LOW、3→LOW、4→NORMAL、
#: 9/10→HIGH（和 HPC 完全同向，不是相反）。
#:
#: 默认取 3（LOW）而不是 10：不显式指定优先级的多半是调试 / 试跑 / 脚本随手提的
#: 任务，用最高优去和别人的生产任务抢卡是不合理的默认。要抢卡请显式写
#: ``--priority``，让这件事是个明确的决定而不是默认副作用。
#:
#: **这个值可以被覆盖**（``QZCLI_DEFAULT_PRIORITY`` / ``.env`` / ``config.json``
#: 的 ``default_priority``），见 ``config.get_default_priority``。改默认值对
#: 「原来不写 --priority 靠默认拿高优」的老脚本是行为变更，得给一条不改调用点
#: 就能恢复原状的路。
DEFAULT_CREATE_PRIORITY = FALLBACK_DEFAULT_PRIORITY
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


def _note_workspace_unavailable(workspace_id, exc):
    """撞到「已禁用 / 无权限」时把工作空间标记掉，后续命令不再查它。

    这些空间会从本地缓存被反复枚举，每次刷一屏 AccessForbidden 警告，
    把真正的问题淹掉。标一次，之后静默跳过；`res -u` 重刷时清标记再验证。
    """
    if getattr(exc, "api_code", None) not in _UNAVAILABLE_API_CODES:
        return False
    mark_workspace_unavailable(workspace_id, str(exc))
    return True


_UNAVAILABLE_API_CODES = {"AccessForbidden", "Forbidden", "PermissionDenied"}


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
            # 首次：从 train_job/list 找当前用户的 user_id
            try:
                probe = api.list_jobs_with_cookie(
                    workspace_ids[0][0], cookie, page_size=50
                )
                current_user_id = _detect_user_id_from_probe(
                    probe.get("jobs", []), (config.get("username") or "").strip()
                )
                if current_user_id:
                    config["user_id"] = current_user_id
                    save_config(config)
                # 找不到匹配就不写 —— 宁可让 filter 退化成"看全部 notebook"，
                # 也不要把别人 id 当成自己缓存（bug ekon@6ee33c6 的根因）
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


# ---- 调度诊断（events）共享工具 ----

_WAITING_STATUS_TOKENS = ("queue", "queued", "queuing", "pending", "waiting")
# 排不上：调度器直接判定无可用节点。
_SCHED_PROBLEM_REASONS = ("unschedulable", "failedscheduling")
# 被抢占：低优/碎卡卡被高优任务挤掉（碎卡治理的典型信号）。
_PREEMPT_REASONS = ("evict", "preempted")


def _status_is_waiting(status: Optional[str]) -> bool:
    """任务是否处于排队/等待态。"""
    s = (status or "").lower()
    return any(tok in s for tok in _WAITING_STATUS_TOKENS)


def _fmt_event_ts(ms) -> str:
    """毫秒 epoch → 本地 'MM-DD HH:MM:SS'。"""
    try:
        from datetime import datetime

        return datetime.fromtimestamp(int(ms) / 1000).strftime("%m-%d %H:%M:%S")
    except Exception:
        return "-"


def _event_sort_key(e: Dict[str, Any]):
    return int(e.get("last_timestamp") or e.get("first_timestamp") or 0)


def _pick_scheduling_reason(events) -> Optional[tuple]:
    """从事件里挑最能解释「排不上 / 被抢占」的一条。

    优先返回 Unschedulable/FailedScheduling（真·排不上），其次 Evict/Preempted
    （被高优抢占）。返回 ``(reason, message)`` 或 None。
    """
    problem, preempt = [], []
    for e in events:
        rl = (e.get("reason") or "").lower()
        if any(k in rl for k in _SCHED_PROBLEM_REASONS):
            problem.append(e)
        elif any(k in rl for k in _PREEMPT_REASONS):
            preempt.append(e)
    pool = problem or preempt
    if not pool:
        return None
    pool.sort(key=_event_sort_key)
    latest = pool[-1]
    return (latest.get("reason") or "", (latest.get("message") or "").strip())


def _events_for_notebook(api, cookie, target, display, args):
    """开发机的事件 + 一句可执行的诊断。

    返回 ``None`` 表示"这不是开发机"，让调用方继续按训练任务处理 —— 不能直接
    报错，否则传训练任务 id 时会被这条分支吃掉。
    """
    from . import nbevents

    nb_id = _extract_notebook_id(target)
    if not nb_id:
        info = _find_notebook_jupyter_info(target, _QuietNoop())
        nb_id = (info or {}).get("notebook_id") or ""
    if not nb_id:
        return None

    try:
        events = api.get_notebook_events(nb_id, cookie)
    except QzAPIError as exc:
        display.print_error(f"拉开发机事件失败：{exc}")
        return 1

    if not events:
        display.print("该开发机没有事件记录")
        return 0

    diag = nbevents.diagnose(events)
    if diag:
        display.print(f"\n[bold]诊断：{diag['title']}[/bold]")
        if diag.get("advice"):
            display.print(f"  {diag['advice']}")
        for line in diag.get("breakdown") or []:
            display.print(f"    · {line}")
        display.print(f"[dim]  平台原文：{diag['raw'][:160]}[/dim]")
    else:
        display.print("\n[bold]诊断：没发现卡点[/bold]（最近一轮事件都是正常进展）")

    shown = nbevents.current_round(events)
    if getattr(args, "all_instances", False):
        shown = events  # --all-instances 对开发机的含义：连历史轮次一起看
    tail = getattr(args, "tail", 0) or 0
    if tail:
        shown = shown[-tail:]
    display.print(f"\n[bold]事件（{len(shown)} 条）[/bold]")
    for ev in shown:
        ts = _fmt_event_time(ev.get("created_at"))
        display.print(f"  {ts}  {(ev.get('content') or '')[:150]}")
    return 0


def _fmt_event_time(raw):
    """平台给的是毫秒时间戳字符串；取不到就原样返回，别让格式化把命令搞挂。"""
    try:
        return time.strftime("%m-%d %H:%M:%S", time.localtime(int(raw) / 1000))
    except (TypeError, ValueError):
        return str(raw or "")[:19]


class _QuietNoop:
    """吞掉查找开发机时的进度输出 —— 这里只想拿 id，不想打乱 events 的排版。"""

    def __getattr__(self, name):
        return lambda *a, **kw: None


def _events_for_nodes(api, cookie, node_names, display, args):
    """`qzcli events --node <名字>`：这台机器还能不能用。

    输出刻意分成两栏：**现在有问题** 和 **曾经出过、已恢复**。

    这个区分不是修辞。平台把 condition 的两个方向都记成事件
    （``XIDIsUnhealthy`` 之后往往跟着 ``XIDIsHealthy``），只 grep "Unhealthy"
    会把已经恢复的机器天天报成有病 —— 实测那 6 台生产机**每一台**都出现过
    XID / GPFS / NotReady，全都已恢复。照着误报去 `--exclude-node`，
    可用机器会被排到没有。
    """
    import json as _json

    from . import nodeevents as _ne

    events = api.get_node_events(node_names, cookie, page_size=200)
    if getattr(args, "output_json", False):
        print(_json.dumps(events, indent=2, ensure_ascii=False))
        return 0

    bad_nodes = []
    for name in node_names:
        mine = [e for e in events if e.get("node_name") == name]
        diag = _ne.diagnose_node(mine)
        exclude = _ne.should_exclude(diag)
        if exclude:
            bad_nodes.append(name)
        mark = "[red]✗[/red]" if exclude else ("[yellow]![/yellow]" if diag["problems"] else "[green]✓[/green]")
        display.print(f"\n{mark} [bold]{name}[/bold]  ({len(mine)} 条事件)")
        if not mine:
            display.print("  [dim]查不到事件记录 —— 可能是节点名写错了，"
                          "也可能这台确实一直没出过状况[/dim]")
            continue
        display.print(f"  {_ne.verdict(diag)}")
        for p in diag["problems"]:
            # `p['at']` 已经是**毫秒**，别再乘 1000（第一版乘了，日期显示成 2 月）
            when = _fmt_event_time(p["at"])
            days = _ne.age_days(p)
            if _ne.is_stale(p):
                # 平台只在状态翻转时记事件，不保证记恢复。陈旧的未恢复记录
                # 如实说清楚，不替用户下"现在坏着"的结论。
                display.print(
                    f"  [yellow]· {p['title']}[/yellow]（{p['reason']}，"
                    f"最后一次 {when}，约 {days:.0f} 天前，之后没有恢复记录）"
                )
                display.print(
                    "    [dim]年代久远：可能早就好了只是没记录，也可能一直没修。"
                    "**不据此建议排除**，真撞上失败再回来看这条。[/dim]"
                )
            else:
                display.print(
                    f"  [red]· {p['title']}[/red]（{p['reason']}，{when}）"
                )
                display.print(f"    {p['advice']}")
        if diag["recovered"]:
            names = "、".join(f"{r['title']}" for r in diag["recovered"])
            display.print(f"  [dim]· 曾经出过但已恢复：{names}[/dim]")

    hint = _ne.exclude_hint(bad_nodes)
    if hint:
        display.print(f"\n[bold]提交时避开这些机器：[/bold]\n  {hint}")
    return 0


def cmd_events(args):
    """查看任务的平台事件（调度 / 抢占 / 拉镜像 / 失败诊断）。

    默认拉任务（控制器）级事件——排队排不上的真因（``Unschedulable``：
    "0/N nodes are unavailable..."）就在这里。``--all-instances`` 追加 Pod 级
    事件（``FailedScheduling`` / ``Scheduled`` / ``Evict`` / ``Preempted``，更细）。
    """
    display = get_display()
    api = get_api()
    job_id = args.job_id

    cookie = _get_cookie_value()
    if not cookie:
        display.print_error("未找到有效 cookie，请先 `qzcli login`")
        return 1

    # `--node` 走完全不同的一条路：问的是**机器**的健康，不是某个任务的遭遇。
    if getattr(args, "node", None):
        return _events_for_nodes(api, cookie, args.node, display, args)

    if not job_id:
        display.print_error("要么给一个任务 ID / 开发机，要么用 `--node <节点名>`")
        return 1

    # **开发机走另一条接口。** 训练任务是 train ListJobEvents，开发机是
    # notebook ListNotebookEvents —— 以前只接了前者，于是开发机排队时这条命令
    # 什么都给不出来。用户拿到一个 id 时并不关心它属于哪类对象，所以在这里
    # 自动分流，而不是让他先搞清楚再选命令。
    nb_id = _extract_notebook_id(job_id)
    if nb_id or not str(job_id).startswith("job-"):
        rc = _events_for_notebook(api, cookie, nb_id or job_id, display, args)
        if rc is not None:
            return rc

    source = {}
    try:
        events = list(api.get_job_events_with_cookie(job_id, cookie))
        for e in events:
            source[id(e)] = "job"
        if getattr(args, "all_instances", False):
            iev = api.get_job_instance_events_with_cookie(job_id, cookie)
            for e in iev:
                source[id(e)] = "pod"
            events += iev
    except QzAPIError as e:
        display.print_error(f"查询事件失败: {e}")
        return 1

    # 过滤：--reason 子串（大小写不敏感），--type 精确（Normal/Warning）
    if getattr(args, "reason", None):
        rq = args.reason.lower()
        events = [e for e in events if rq in (e.get("reason") or "").lower()]
    if getattr(args, "type", None):
        tq = args.type.lower()
        events = [e for e in events if (e.get("type") or "").lower() == tq]

    events.sort(key=_event_sort_key)
    if getattr(args, "tail", None):
        events = events[-args.tail :]

    if getattr(args, "output_json", False):
        import json

        print(json.dumps(events, indent=2, ensure_ascii=False))
        return 0

    if not events:
        display.print("[dim]无匹配事件[/dim]")
        return 0

    for e in events:
        etype = (e.get("type") or "").strip()
        reason = (e.get("reason") or "").strip()
        msg = (e.get("message") or "").strip()
        ts = _fmt_event_ts(e.get("last_timestamp") or e.get("first_timestamp"))
        scope = source.get(id(e)) or (e.get("object_type") or "")
        color = "yellow" if etype.lower() == "warning" else "green"
        display.print(
            f"[dim]{ts}[/dim] [{color}]{etype:<7}[/{color}] "
            f"[bold]{reason}[/bold] [dim]({scope})[/dim]"
        )
        if msg:
            display.print(f"    {msg}")

    sched = _pick_scheduling_reason(events)
    if sched:
        r, m = sched
        display.print(f"\n[yellow]⚠ 调度诊断[/yellow]: {r} — {m}")
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

        # 排队/等待态时，best-effort 补一行「为什么排不上」——接碎卡闭环：
        # 看碎卡 → exclude → 提交 → 若还排队，这里直接给真因。
        if _status_is_waiting(api_data.get("status")):
            try:
                cookie = _get_cookie_value()
                if cookie:
                    events = api.get_job_events_with_cookie(job_id, cookie)
                    sched = _pick_scheduling_reason(events)
                    if sched:
                        r, m = sched
                        display.print(f"[yellow]排队原因[/yellow]: {r} — {m}")
            except (QzAPIError, requests.RequestException) as exc:
                # 诊断是附加信息，绝不打断 status 主流程；但原因要留痕。
                swallowed("status/排队原因诊断", exc)

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


def _parse_since(s: Optional[str]) -> Optional[str]:
    """``--since`` 解析。

    支持:
      - 相对值: ``5m`` / ``2h`` / ``30s`` / ``1d``
      - ISO 时间(带或不带毫秒): ``2026-05-02T19:22:00`` 等
    返回 ms 整数字符串(平台 v2 API 要求字符串形式),失败返回 None。
    """
    if not s:
        return None
    import re
    import time as _time

    m = re.fullmatch(r"\s*(\d+)\s*([smhd])\s*", s)
    if m:
        n = int(m.group(1))
        unit = {"s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)]
        return str(int((_time.time() - n * unit) * 1000))
    try:
        from datetime import datetime

        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return str(int(dt.timestamp() * 1000))
    except Exception:
        return None


def cmd_logs(args):
    """拉取任务日志(v2 GetJobLog)"""
    display = get_display()
    api = get_api()

    job_id = args.job_id
    pod_names = [args.pod] if getattr(args, "pod", None) else None
    start_ms = _parse_since(getattr(args, "since", None))

    page_size = max(args.tail, 1)

    def _fetch(start: Optional[str], sort: str = "ascend"):
        return api.get_job_logs(
            job_id,
            page_size=page_size,
            pod_names=pod_names,
            start_timestamp_ms=start,
            sort=sort,
        )

    try:
        first = _fetch(start_ms, sort="descend")
    except QzAPIError as e:
        display.print_error(f"拉取日志失败: {e}")
        return 1

    entries: List[dict] = sorted(
        first.get("logs", []),
        key=lambda e: (int(e.get("timestamp_ms") or 0), e.get("log_id") or ""),
    )
    # 初次请求按 descend 拿最近 tail 条；终端仍按时间升序打印。
    entries = entries[-args.tail :]
    display.print_logs(entries, raw=args.raw, json_mode=args.output_json)

    if not args.follow:
        return 0

    seen = {e.get("log_id") for e in entries if e.get("log_id")}
    last_ms = max(
        (int(e.get("timestamp_ms") or 0) for e in entries),
        default=int(start_ms or 0),
    )

    try:
        while True:
            time.sleep(max(args.interval, 0.5))
            try:
                batch = _fetch(str(last_ms + 1) if last_ms else None)
            except QzAPIError as e:
                display.print_error(f"轮询失败: {e}")
                return 1
            new = []
            for e in batch.get("logs", []):
                lid = e.get("log_id")
                if lid and lid in seen:
                    continue
                if lid:
                    seen.add(lid)
                new.append(e)
                ts = int(e.get("timestamp_ms") or 0)
                if ts > last_ms:
                    last_ms = ts
            if new:
                display.print_logs(new, raw=args.raw, json_mode=args.output_json)
    except KeyboardInterrupt:
        return 0


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

            # 默认走 quick；显式 --full 才扫历史任务反推 specs
            use_full = bool(getattr(args, "full", False))
            # 并行刷新各 workspace —— 网络调用并发，磁盘写入仍在主线程串行
            parallel_workers = max(1, int(getattr(args, "parallel", 8) or 1))

            def _fetch_one(ws):
                ws_id_ = ws.get("id")
                ws_name_ = ws.get("name", "")
                try:
                    resources_, jobs_count_ = (
                        _collect_workspace_resources_from_live_apis(
                            api, ws_id_, cookie, quick=not use_full
                        )
                    )
                    return {
                        "ok": True,
                        "ws_id": ws_id_,
                        "ws_name": ws_name_,
                        "resources": resources_,
                        "jobs_count": jobs_count_,
                    }
                except Exception as e:
                    return {
                        "ok": False,
                        "ws_id": ws_id_,
                        "ws_name": ws_name_,
                        "error": str(e),
                    }

            # 更新每个工作空间（带进度条，沿用 cmd_avail 的 pattern）
            progress = None
            if hasattr(display, "create_progress"):
                progress = display.create_progress()
                if progress:
                    progress.start()
            progress_task_id = None
            if progress:
                progress_task_id = progress.add_task(
                    f"并行刷新（max_workers={parallel_workers}）",
                    total=len(workspaces),
                )

            from concurrent.futures import ThreadPoolExecutor, as_completed

            # 扇出前串行确认鉴权状态（不发请求，见 api.ensure_authenticated）。
            # 少了这步，cookie 失效那一刻每个 worker 都会各自撞 401 去登录。
            _ensure_auth_before_fanout(api, "")

            try:
                with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                    futures = [executor.submit(_fetch_one, ws) for ws in workspaces]
                    for future in as_completed(futures):
                        result = future.result()
                        ws_id = result["ws_id"]
                        ws_name = result["ws_name"]
                        try:
                            if not result["ok"]:
                                display.print_warning(
                                    f"  ✗ {ws_name or ws_id}: {result['error']}"
                                )
                                continue
                            resources = result["resources"]
                            jobs_count = result["jobs_count"]
                            # 保存到本地缓存（主线程串行，避免 read-modify-write 竞争）
                            save_resources(ws_id, resources, ws_name)

                            projects_count = len(resources.get("projects", []))
                            cg_count = len(resources.get("compute_groups", []))
                            if use_full:
                                display.print(
                                    f"  ✓ {ws_name or ws_id}: {projects_count} 项目, {cg_count} 计算组, {jobs_count} 历史任务"
                                )
                            else:
                                display.print(
                                    f"  ✓ {ws_name or ws_id}: {projects_count} 项目, {cg_count} 计算组 (默认 quick: 跳过历史任务/specs，--full 强制扫描)"
                                )
                        finally:
                            if progress and progress_task_id is not None:
                                progress.advance(progress_task_id)
            finally:
                if progress:
                    progress.stop()

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
            use_full = bool(getattr(args, "full", False))
            if use_full:
                display.print("[dim]--full 模式：扫描全部历史任务以反推 specs...[/dim]")
            else:
                display.print(
                    "[dim]quick 默认模式：跳过历史任务，只走 cluster_info + task_dimension（--full 强制完整扫描）...[/dim]"
                )

            resources, jobs_count = _collect_workspace_resources_from_live_apis(
                api, workspace_id, cookie, quick=not use_full
            )

            # 保存到本地缓存
            ws_name = pending_name or (
                cached_resources.get("name", "") if cached_resources else ""
            )
            # **拉到空就不许覆盖非空缓存。**
            #
            # _collect_workspace_resources_from_live_apis 在鉴权失败时会把 API 错误
            # 吞成空列表（见那边的 diag.swallowed 注释），于是这行会把一份好端端的
            # 缓存写成 0 个 compute_groups / 0 个 projects。2026-08-16 真发生过：
            # 账号锁定期间跑了一次 `qzcli res -w 分布式训练空间 -u`，那个工作空间的
            # 缓存被清空，后续所有命令报「未找到计算组」，看着像平台改了名。
            #
            # 并行路径（本函数上面那支）早就有守卫（`if not result["ok"]: continue`），
            # 只有这条单工作空间路径漏了。
            if _resources_look_empty(resources) and not _resources_look_empty(
                cached_resources
            ):
                display.print_warning(
                    f"刷新没拿到任何计算组/项目（多半是登录态失效或被限流），"
                    f"**保留原缓存不覆盖**。原缓存里有 "
                    f"{len((cached_resources or {}).get('compute_groups') or {})} 个计算组。"
                )
                display.print("[dim]先解决登录问题再重试：qzcli login[/dim]")
            else:
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
    cached_workspace_id = ""
    cached_workspace_name = ""
    if workspace_input and not workspace_input.startswith("ws-"):
        # Capture the pre-refresh cache match. _list_available_workspaces updates
        # workspace names as a side effect, so checking after it runs would turn
        # live ambiguity into an arbitrary cache-order match.
        cached_workspace_id = find_workspace_by_name(workspace_input) or ""
        if cached_workspace_id:
            cached_resources = get_workspace_resources(cached_workspace_id) or {}
            cached_workspace_name = str(
                cached_resources.get("name", "") or workspace_input
            )

    try:
        available_workspace_options = _sort_workspace_options_for_selection(
            _list_workspace_options_for_avail(
                api,
                display,
                workspace_input=workspace_input,
                cached_workspace_id=cached_workspace_id,
                include_usage_snapshot=False,
                show_progress=not args.export,
            )
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
        (
            workspace_id,
            ws_display,
            ambiguous_matches,
        ) = _resolve_workspace_option_for_avail(
            available_workspace_options,
            workspace_input,
            cached_workspace_id=cached_workspace_id,
            cached_workspace_name=cached_workspace_name,
        )
        if workspace_id:
            workspace_options = _workspace_options_for_resolved_id(
                available_workspace_options, workspace_id, ws_display
            )
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
            if ambiguous_matches:
                display.print(
                    "[dim]该名称匹配到多个工作空间，请改用完整名称或 ID:[/dim]"
                )
                for match in ambiguous_matches:
                    match_id = str(match.get("id", "") or "")
                    match_name = str(match.get("name", "") or match_id)
                    display.print(f"  [dim]- {match_name}: {match_id}[/dim]")
            else:
                display.print(
                    "[dim]请使用 qzcli res --list 查看已缓存工作空间，或改用 workspace ID[/dim]"
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

    progress = None
    if not args.export and hasattr(display, "create_progress"):
        progress = display.create_progress()
        if progress:
            progress.start()

    workspace_jobs = []
    for workspace_id in workspace_ids:
        workspace_option = workspace_options_by_id.get(workspace_id, {})
        ws_name = str(workspace_option.get("name", "") or workspace_id)
        cached_resources = get_workspace_resources(workspace_id)
        if not cached_resources or not cached_resources.get("compute_groups"):
            if not workspace_input:
                continue
            try:
                cached_resources = _load_workspace_resources_for_avail(
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

        progress_task_id = None
        if progress:
            progress_task_id = progress.add_task(
                f"{ws_name}: 准备查询 {len(compute_groups)} 个计算组",
                total=len(compute_groups) + 1 + (1 if args.low_priority else 0),
            )
        else:
            display.print(
                f"[dim]正在查询 {ws_name} 的 {len(compute_groups)} 个计算组...[/dim]"
            )

        workspace_jobs.append(
            {
                "workspace_id": workspace_id,
                "workspace_name": ws_name,
                "compute_groups": compute_groups,
                "specs": specs,
                "progress_task_id": progress_task_id,
            }
        )

    def _query_workspace_availability(
        job: Dict[str, Any],
    ) -> Tuple[str, List[Dict[str, Any]], Optional[str]]:
        workspace_id = job["workspace_id"]
        ws_name = job["workspace_name"]
        compute_groups = job["compute_groups"]
        specs = job["specs"]
        progress_task_id = job["progress_task_id"]

        # 低优任务统计（仅在 --lp 参数启用时计算）
        node_low_priority_gpu = defaultdict(int)  # node_name -> low_priority_gpu_count

        if args.low_priority:
            if progress and progress_task_id is not None:
                progress.update(
                    progress_task_id, description=f"{ws_name}: 获取低优任务"
                )
            else:
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
                        page_size=1000,
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
            finally:
                if progress and progress_task_id is not None:
                    progress.advance(progress_task_id)

        try:
            if progress and progress_task_id is not None:
                progress.update(
                    progress_task_id, description=f"{ws_name}: 获取节点数据"
                )
            if len(compute_groups) == 1:
                only_lcg_id = next(iter(compute_groups.keys()))
                all_nodes = _with_live_cookie(
                    api,
                    display,
                    lambda live_cookie: _fetch_all_node_dimensions(
                        api,
                        workspace_id,
                        live_cookie,
                        logic_compute_group_id=only_lcg_id,
                        page_size=1000,
                    ),
                    workspace_id=workspace_id,
                )
                nodes_by_lcg = {only_lcg_id: all_nodes}
            else:
                all_nodes = _with_live_cookie(
                    api,
                    display,
                    lambda live_cookie: _fetch_all_node_dimensions(
                        api,
                        workspace_id,
                        live_cookie,
                        page_size=1000,
                    ),
                    workspace_id=workspace_id,
                )
                nodes_by_lcg: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
                for node in all_nodes:
                    lcg = node.get("logic_compute_group", {})
                    lcg_id = _first_non_empty(
                        lcg.get("id"), node.get("logic_compute_group_id")
                    )
                    if lcg_id:
                        nodes_by_lcg[str(lcg_id)].append(node)
                if all_nodes and not nodes_by_lcg:

                    def _fetch_lcg_nodes(
                        current_lcg_id: str,
                    ) -> Tuple[str, List[Dict[str, Any]]]:
                        nodes = _with_live_cookie(
                            api,
                            display,
                            lambda live_cookie: _fetch_all_node_dimensions(
                                api,
                                workspace_id,
                                live_cookie,
                                logic_compute_group_id=current_lcg_id,
                                page_size=1000,
                            ),
                            workspace_id=workspace_id,
                        )
                        return current_lcg_id, nodes

                    max_lcg_workers = min(6, len(compute_groups))
                    # 扇出前串行确认鉴权（不发请求）。avail 是最容易触发这个问题的
                    # 命令：按计算组并发，cookie 一失效就是 6 个线程同时登录。
                    _ensure_auth_before_fanout(api, "")
                    with ThreadPoolExecutor(max_workers=max_lcg_workers) as executor:
                        for current_lcg_id, nodes in executor.map(
                            _fetch_lcg_nodes, compute_groups.keys()
                        ):
                            nodes_by_lcg[current_lcg_id] = nodes
            if progress and progress_task_id is not None:
                progress.advance(progress_task_id)
        except QzAPIError as e:
            if progress and progress_task_id is not None:
                progress.advance(progress_task_id, len(compute_groups))
            if _note_workspace_unavailable(workspace_id, e):
                # 已禁用 / 无权限：标记后静默跳过，别刷屏
                return workspace_id, [], ""
            return workspace_id, [], f"查询 {ws_name} 节点数据失败: {e}"

        workspace_results = []
        for lcg_id, lcg_info in compute_groups.items():
            lcg_name = lcg_info.get("name", lcg_id)
            gpu_type = lcg_info.get("gpu_type", "")

            if progress and progress_task_id is not None:
                progress.update(
                    progress_task_id,
                    description=f"{ws_name}: 统计 {lcg_name}",
                )
            nodes = nodes_by_lcg.get(lcg_id, [])
            total_nodes = len(nodes)

            # 统计空闲节点（GPU 使用数为 0）和空闲 GPU 分布
            free_nodes = []
            low_priority_free_nodes = []  # 低优空余节点
            gpu_free_distribution = {}  # free_gpu_count -> node_count
            total_free_gpus = 0
            total_gpus = 0
            # 碎卡:散在"非整块"节点上、凑不成整节点的可回收卡
            # （低优空余只数"整节点 100% 低优"，看不到这部分——就是看板里那些低优卡）
            fragmented_low_priority = 0
            fragmented_free = 0
            fragmented_free_node_list = []  # 有空卡的碎卡节点(可 --exclude-node 避开)

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
                    elif low_priority_gpu > 0:
                        # 低优没占满整节点 → 碎片低优卡(可抢占但凑不成整节点)
                        fragmented_low_priority += min(low_priority_gpu, gpu_used)
                    # 空卡散在已被占用的节点上 → 碎片空卡;记下节点名供 --exclude-node
                    if 0 < gpu_free < gpu_total:
                        fragmented_free += gpu_free
                        fragmented_free_node_list.append(node_name)

            workspace_results.append(
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
                    "fragmented_low_priority_gpus": fragmented_low_priority,
                    "fragmented_free_gpus": fragmented_free,
                    "fragmented_free_node_list": fragmented_free_node_list,
                    "total_gpus": total_gpus,
                    "total_free_gpus": total_free_gpus,
                    "gpu_free_distribution": gpu_free_distribution,
                    "specs": specs,
                }
            )
            if progress and progress_task_id is not None:
                progress.advance(progress_task_id)

        if progress and progress_task_id is not None:
            progress.update(progress_task_id, description=f"{ws_name}: 查询完成")

        return workspace_id, workspace_results, None

    if workspace_jobs:
        max_workers = min(6, len(workspace_jobs))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_query_workspace_availability, job)
                for job in workspace_jobs
            ]
            for future in as_completed(futures):
                try:
                    _, workspace_results, warning = future.result()
                except QzAPIError as e:
                    display.print_warning(f"查询节点数据失败: {e}")
                    continue
                if warning:
                    display.print_warning(warning)
                    continue
                all_results.extend(workspace_results)

    if progress:
        progress.stop()

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
            # 碎卡治理:有空卡的碎卡节点 → 可直接粘的 --exclude-node 串,
            # 提交时排掉它们逼调度器用整节点/空节点。
            if args.verbose and r.get("fragmented_free_node_list"):
                from .fragmentation import format_exclude_args

                excl = format_exclude_args(r["fragmented_free_node_list"])
                if excl:
                    display.print(
                        f"  [dim]避开碎卡(粘到 qzcli create 后): {excl}[/dim]"
                    )

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
        total_gpu_alloc_ratio = _format_percent(total_used_gpus, total_gpus)

        display.print(f"[bold]全分区总览 ({total_groups} 个计算组)[/bold]")
        display.print(
            f"[dim]空节点 {total_free_nodes}/{total_nodes} | 空GPU {total_free_gpus}/{total_gpus} | GPU利用率 {total_gpu_alloc_ratio}[/dim]"
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
                table.add_column("碎片低优", justify="right")
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
                frag_lp = r.get("fragmented_low_priority_gpus", 0)
                frag_lp_text = (
                    f"[magenta]{frag_lp}[/magenta]" if frag_lp > 0 else "[dim]0[/dim]"
                )
                total_available_text = (
                    f"[green]{total_available}[/green]"
                    if total_available > 0
                    else "[dim]0[/dim]"
                )

                used_gpu = max(0, total_gpu - total_free_gpu)
                gpu_alloc_text = _format_percent(used_gpu, total_gpu)
                if total_gpu > 0:
                    gpu_alloc_ratio = used_gpu / total_gpu
                    if gpu_alloc_ratio >= 0.8:
                        gpu_alloc_text = f"[green]{gpu_alloc_text}[/green]"
                    elif gpu_alloc_ratio >= 0.4:
                        gpu_alloc_text = f"[yellow]{gpu_alloc_text}[/yellow]"
                    else:
                        gpu_alloc_text = f"[red]{gpu_alloc_text}[/red]"
                else:
                    gpu_alloc_text = "[dim]-[/dim]"

                row = [
                    str(idx),
                    r.get("workspace_name", ""),
                    r.get("name", ""),
                    free_nodes_text,
                ]
                if args.low_priority:
                    row.extend([low_priority_text, frag_lp_text, total_available_text])
                row.extend(
                    [
                        str(r.get("total_nodes", 0)),
                        f"{total_free_gpu}/{total_gpu}",
                        gpu_alloc_text,
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
                        [
                            low_priority_free,
                            r.get("fragmented_low_priority_gpus", 0),
                            r.get("free_nodes", 0) + low_priority_free,
                        ]
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
                headers.extend(["低优空余", "碎片低优", "可用节点"])
                aligns.extend(["right", "right", "right"])
                max_widths.extend([8, 8, 8])
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
        # try 只包住**取数**。以前它连后面的统计一起包了，于是字段改名、除零、
        # 某个空间没权限，都会让「HPC 节点 CPU/内存利用率」整段静默消失 ——
        # 退出码还是 0，看着像"今天就是没有 HPC 节点"。
        try:
            all_nodes = _with_live_cookie(
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
        except (QzAPIError, requests.RequestException) as exc:
            # 单个空间取不到不该拖垮整张表，但要让人看见少了谁。
            swallowed("avail/HPC 节点取数", exc)
            display.print(
                f"  [dim]{ws_label}: 节点利用率取数失败"
                f"（{type(exc).__name__}），已跳过[/dim]"
            )
            continue

        hpc_nodes = [n for n in all_nodes if n.get("node_type") == "hpc"]
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
    return 0


# ---------------------------------------------------------------------------
# 使用情况数据层（cmd_usage 与 dashboard 共用；提供可测缝）
# ---------------------------------------------------------------------------

# 任务类型 -> 中文显示名（cmd_usage 与 dashboard 共用）
TYPE_NAMES = {
    "distributed_training": "分布式训练",
    "interactive_modeling": "交互式建模",
    "inference_serving_customize": "推理服务",
    "inference_serving": "推理服务",
    "training": "训练",
}

# 任务类型 -> 启智任务详情页路径段
_JOB_DETAIL_PATH_BY_TYPE = {
    "distributed_training": "distributedTrainingDetail",
    "interactive_modeling": "interactiveModelingDetail",
}


def _ensure_auth_before_fanout(api, cookie):
    """**并发扇出之前**串行把鉴权走完，返回此后该用的 cookie。

    为什么必须有这一步：本仓每个 API 方法都挂 ``@with_auth_retry``，撞 401 就
    自己重登。单线程没问题；从 N 个 worker 里调用时，cookie 失效那一刻
    **N 个 worker 会同时撞 401、同时去登录**。CAS 按失败次数延长锁定 ——
    2026-08-12 就是这么把账号锁死的。

    历史上为此加过四层保护（进程内锁、跨进程文件锁、按失败 cookie 去重、
    失败冷却），**全在治「抢着登录时怎么办」，没有一层在治「为什么让 worker
    去登录」**。这个函数治的是后者。

    对照 inspire 插件：它压根没有自动重登，401 直接让用户去登录，并且把
    「拉 permissions / user detail 建立鉴权上下文」列为业务动作之前的固定步骤。

    **必须使用返回值** —— 重登后盘上 cookie 已经换了，继续用传进来的旧字符串
    就退回「每个 worker 各自撞 401」的老路。

    测试里的假 API 没有 ``ensure_authenticated``，原样返回即可：那种场景是
    单线程 mock，不存在踩踏。
    """
    fn = getattr(api, "ensure_authenticated", None)
    if fn is None:
        return cookie
    return fn(cookie) or cookie


def fetch_all_task_dimensions(api, workspace_id, cookie, page_size=200, max_workers=4):
    """分页获取工作空间**当前在跑**任务的资源维度数据（list_task_dimension）。

    第一页即返回 ``total``，据此算出总页数后并发拉取其余页（顺序无关，聚合/可视
    化都不依赖任务顺序），避免逐页串行的高延迟。
    """
    cookie = _ensure_auth_before_fanout(api, cookie)  # 扇出前，串行
    first = api.list_task_dimension(
        workspace_id, _live_cookie_for_paging(cookie), page_num=1, page_size=page_size
    )
    tasks = list(first.get("task_dimensions", []))
    total = first.get("total", 0) or 0
    if not tasks or len(tasks) >= total:
        return tasks

    # 服务端可能把 page_size 压到更小，用首页实际条数推导有效页大小
    effective = len(tasks)
    page_count = (total + effective - 1) // effective

    def _fetch(page_num):
        return api.list_task_dimension(
            workspace_id,
            _live_cookie_for_paging(cookie),
            page_num=page_num,
            page_size=page_size,
        ).get("task_dimensions", [])

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for batch in executor.map(_fetch, range(2, page_count + 1)):
            tasks.extend(batch)
    return tasks


def _short_gpu_type(gpu_type):
    """把 NVIDIA_H100_SXM_80G 之类的规格缩成 H100 / H200 / A100 等短名。"""
    if not gpu_type:
        return ""
    match = re.search(r"[HAB]\d{2,3}", gpu_type)
    return match.group(0) if match else gpu_type


def build_node_to_lcg_map(api, workspace_id, cookie):
    """反建 ``{node_name: {"lcg", "gpu_type", "cluster", "gpu_total", "gpu_used"}}``。

    平台的 task/node 资源维度接口都不直接带 logic_compute_group（计算组/机房），
    但 ``list_node_dimension`` 支持按 ``logic_compute_group_id`` 过滤，因此逐个
    lcg 拉取其成员节点即可反建「节点 → 计算组」映射（``cmd_avail`` 同法）。
    """
    cookie = _ensure_auth_before_fanout(api, cookie)  # 扇出前，串行
    node_map = {}
    cluster_info = api.get_cluster_basic_info(workspace_id, cookie)
    lcgs = [
        (
            lcg["logic_compute_group_id"],
            lcg.get("logic_compute_group_name") or lcg["logic_compute_group_id"],
        )
        for compute_group in cluster_info.get("compute_groups", [])
        for lcg in compute_group.get("logic_compute_groups", [])
        if lcg.get("logic_compute_group_id")
    ]

    def _fetch(item):
        lcg_id, lcg_name = item
        nodes = _fetch_all_node_dimensions(
            api, workspace_id, cookie, logic_compute_group_id=lcg_id, page_size=1000
        )
        return lcg_name, nodes

    # 各 lcg 的节点查询相互独立，并发拉取。内层并发压到 4 削平对启智的读取峰值 QPS
    # (看板里外层 workspace 并发 × 此内层 = 峰值,见 dashboard load_all_frag)。
    with ThreadPoolExecutor(max_workers=4) as executor:
        for lcg_name, nodes in executor.map(_fetch, lcgs):
            for node in nodes:
                name = node.get("name")
                if not name:
                    continue
                node_gpu = node.get("gpu") or {}
                node_status = node.get("status", "")
                cordon_type = str(node.get("cordon_type") or "").strip()
                node_map[name] = {
                    "lcg": lcg_name,
                    "gpu_type": _short_gpu_type(node.get("gpu_type")),
                    "cluster": node.get("cluster_name", ""),
                    "gpu_total": node_gpu.get("total", 0) or 0,  # 该节点 GPU 容量
                    "gpu_used": node_gpu.get("used", 0) or 0,  # 已占用
                    "status": node_status,
                    "cordon_type": cordon_type,
                    # 可调度 = Ready 且无 cordon(machine-migration/software-fault 等)。
                    # 与 cmd_avail 的 is_schedulable 同口径:SchedulingDisabled 节点即便
                    # 有空卡也调度不上去,碎卡计算必须排除。
                    "schedulable": node_status == "Ready" and not cordon_type,
                }
    return node_map


def _priority_band(priority):
    """优先级数值 -> 高/中/低优档位。"""
    if priority >= 6:
        return "高优(≥6)"
    if priority >= 4:
        return "中优(4-5)"
    return "低优(≤3)"


def _job_detail_url(job_id, task_type, workspace_id):
    """按任务类型构造启智任务详情页 URL（镜像 cmd_* 里的详情链接规则）。"""
    if not job_id:
        return ""
    path = _JOB_DETAIL_PATH_BY_TYPE.get(task_type)
    if path:
        return f"https://qz.sii.edu.cn/jobs/{path}/{job_id}?spaceId={workspace_id}"
    # 推理服务 / 未知类型 → 回退到空间概览
    return f"https://qz.sii.edu.cn/jobs/spacesOverview?spaceId={workspace_id}"


def task_dimension_to_row(task, node_map, workspace_id=""):
    """把一条 list_task_dimension 记录拍平成 dashboard 用的一行（含计算组归属）。"""
    gpu = task.get("gpu") or {}
    gpu_total = gpu.get("total", 0) or 0
    nodes_occupied = task.get("nodes_occupied") or {}
    nodes = nodes_occupied.get("nodes") or []
    node_info = node_map.get(nodes[0]) if nodes else None
    priority = task.get("priority", 0) or 0
    task_type = task.get("type", "unknown")
    running_ms = int(task.get("running_time_ms") or 0)
    return {
        "任务名": task.get("name", ""),
        "用户": (task.get("user") or {}).get("name", "未知"),
        "项目": (task.get("project") or {}).get("name", "未知"),
        "任务类型": TYPE_NAMES.get(task_type, task_type),
        "优先级": priority,
        "优先级档": _priority_band(priority),
        "计算组": node_info["lcg"] if node_info else "排队/未分配",
        "GPU类型": node_info["gpu_type"] if node_info else "",
        "集群": node_info["cluster"] if node_info else "",
        "GPU": gpu_total,
        "GPU利用率": round((gpu.get("usage_rate") or 0) * 100, 1),
        "状态": task.get("status", ""),
        "节点数": nodes_occupied.get("count", 0),
        "节点": ",".join(nodes),
        "运行时长h": round(running_ms / 3_600_000, 1),
        "创建时间": (task.get("created_at") or "")[:19],
        "job_url": _job_detail_url(task.get("id"), task_type, workspace_id),
        "id": task.get("id", ""),
    }


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
            tasks = fetch_all_task_dimensions(api, workspace_id, cookie)

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

            # 任务类型中文映射（模块级共享常量）
            type_names = TYPE_NAMES

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
            if not _note_workspace_unavailable(workspace_id, e):
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
    """查看工作空间任务概览"""
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

    # `-w` 允许传名字（其他子命令都支持），这里必须解析成 ws-<uuid>。
    # 不解析的话名字会原样拼进 referer 头，中文字符触发
    # `'latin-1' codec can't encode` —— 请求根本发不出去。
    if workspace_id and not workspace_id.startswith("ws-"):
        resolved, ws_name = _resolve_workspace_value(api, display, workspace_id)
        if not resolved:
            display.print_error(f"未找到工作空间: {workspace_id}")
            return 1
        display.print(f"[dim]匹配到工作空间: {ws_name} -> {resolved}[/dim]")
        workspace_id = resolved

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

    # 弃用选项提示
    deprecated_used = []
    if getattr(args, "project", None) is not None:
        deprecated_used.append("--project/-p")
    if getattr(args, "all", False):
        deprecated_used.append("--all/-a")
    if getattr(args, "page", 1) != 1:
        deprecated_used.append("--page")
    if getattr(args, "size", 100) != 100:
        deprecated_used.append("--size")
    if getattr(args, "sync", False):
        deprecated_used.append("--sync/-s")
    if deprecated_used:
        display.print(
            f"[yellow]警告: {', '.join(deprecated_used)} 已弃用（上游 API 不再支持），将被忽略[/yellow]"
        )

    try:
        display.print("[dim]正在获取工作空间任务概览...[/dim]")
        data = api.list_workspace_tasks(workspace_id, cookie)

        task_groups = data.get("task_groups", [])

        if not task_groups:
            display.print("工作空间内暂无任务数据")
            return 0

        # 任务类型中文映射
        type_names = {
            "distributed_training": "分布式训练",
            "interactive_modeling": "交互式开发",
            "hpc_job": "HPC 作业",
            "inference_serving_customize": "推理服务(自定义)",
            "inference_serving_dynamic": "推理服务(动态)",
            "ray_job": "Ray 作业",
            "sandbox": "沙盒",
        }

        # 状态颜色/图标
        status_style = {
            "RUNNING": ("[cyan]", "●"),
            "PENDING": ("[yellow]", "◌"),
            "CREATING": ("[yellow]", "◌"),
            "DEPLOYING": ("[yellow]", "◌"),
            "SUCCEEDED": ("[green]", "✓"),
            "STOPPED": ("[dim]", "■"),
            "FAILED": ("[red]", "✗"),
        }

        grand_total = sum(g.get("task_total", 0) for g in task_groups)
        display.print(
            f"\n[bold]工作空间任务概览[/bold] (最近 24h, 共 {grand_total} 个任务)\n"
        )

        for group in task_groups:
            task_type = group.get("task_type", "unknown")
            task_total = group.get("task_total", 0)
            entries = group.get("task_status_count_entries", [])
            type_label = type_names.get(task_type, task_type)

            if task_total == 0 and not entries:
                continue

            display.print(f"  [bold]{type_label}[/bold]  ({task_total} 个)")

            if entries:
                parts = []
                for entry in sorted(
                    entries, key=lambda e: e.get("count", 0), reverse=True
                ):
                    status = entry.get("status", "UNKNOWN")
                    count = entry.get("count", 0)
                    color, icon = status_style.get(status, ("[dim]", "?"))
                    close_tag = color.replace("[", "[/")
                    parts.append(f"{color}{icon} {status} {count}{close_tag}")
                display.print(f"    {' | '.join(parts)}")
            else:
                display.print("    [dim]无任务[/dim]")
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


def _project_belongs_to_workspace_on_platform(api, workspace_id, project_id):
    """跟平台确认项目是否真属于该工作空间（不看本地缓存）。

    与 `_compute_group_exists_on_platform` 同构 —— 两处归属校验本来就是同一套
    逻辑，之前只给计算组加了平台复核，项目这条漏了，于是**新建/新加入的项目
    会原样重演那个 bug**：报「项目 X 不属于当前工作空间」，而它其实属于。

    数据源是项目列表的 `items[].space_list[]`，即"项目 → 它属于哪些工作空间"。
    走 v2 `project GetProjectForPage`（上游 2026-08 放开了普通用户权限；在那之前
    它是 `AccessForbidden`，只能用 v1），v2 路由不通时自动回落。

    返回 True/False；查询失败或列表为空返回 None（不确定 → 上层放行，
    让平台自己拒，总好过拿过期缓存误伤一个真实项目）。
    """
    try:
        items = api.list_projects_raw()
    except QzAPIError:
        return None
    if not items:
        return None
    for proj in items:
        if proj.get("id") != project_id:
            continue
        spaces = {s.get("id") for s in (proj.get("space_list") or [])}
        return workspace_id in spaces
    return False


def _compute_group_exists_on_platform(
    api, workspace_id, compute_group_id, display=None
):
    """跟平台确认计算组是否真属于该工作空间（不看本地缓存）。

    存在的意义：本地缓存**总会过期**。新建的计算组在缓存刷新前是查不到的，
    而 `_validate_cached_resource_membership` 只看缓存，会把一个**真实存在、
    此刻正在跑任务**的计算组判成「不属于当前工作空间」—— 报错内容本身是错的，
    而且提示去 `res -u` 也未必解决（缓存刷新有自己的失败模式）。

    所以缓存说「没有」时不能直接拒，要跟平台再确认一次。
    `workspace ListLogicComputeGroups` 是权威来源，不依赖任何缓存。

    返回 True/False；查询失败返回 None（此时按「不确定」处理，放行，
    让平台自己去拒——总好过我们拿过期缓存误伤）。
    """
    try:
        r = api._request_v2(
            "workspace",
            "ListLogicComputeGroups",
            {"filter": {"workspace_id": workspace_id}, "page_num": 1, "page_size": 200},
            referer_path=f"/jobs/spacesOverview?spaceId={workspace_id}",
        )
    except QzAPIError:
        return None
    groups = r.get("logic_compute_groups") or []
    if not groups:
        return None
    return any(g.get("logic_compute_group_id") == compute_group_id for g in groups)


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


#: 进程内只提示一次"正在刷新 cookie"。并发扇出时 N 个线程都会走到这里，
#: 但实际只会发生一次 CAS 登录（去重在 ``_relogin`` 里），所以提示也该只有一条。
_REFRESH_NOTICE_LOCK = threading.Lock()
_refresh_notice_shown = False


def _refresh_cookie_for_interactive(api, display, workspace_id: str = "") -> str:
    """使用已保存的 CAS 凭证自动刷新 cookie。

    **必须走 ``api._relogin()``，不能自己调 ``login_with_cas``。**

    这个函数被 ``_with_live_cookie`` 调用，而后者出现在 11 个命令里，其中
    ``avail`` / ``usage`` / ``list -c`` 都是**并发扇出**的 —— 每个计算组一个线程。
    以前这里直接调 ``login_with_cas``，等于绕开了 ``_relogin`` 的三层保护：
    进程内锁、跨进程文件锁、以及"拿到锁后重读 cookie 看别人是否已登好"的去重。

    结果就是 cookie 一过期，一条 ``qzcli avail`` 会朝 CAS 打出十几次并发登录，
    **CAS 判定为异常行为并要求输入验证码，然后连自动重登本身也失效** ——
    账号被锁在外面，只能去浏览器手工取 cookie。

    v0.4.1 加的锁只覆盖了 ``_relogin`` 和 ``qzcli login`` 两条路，恰恰漏掉了这条
    最常触发的。这里补上。
    """
    global _refresh_notice_shown

    relogin = getattr(api, "_relogin", None)
    if relogin is None:
        # 测试里的假 API 可能没有 _relogin，退回老路径（单线程场景不会放大）
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

    with _REFRESH_NOTICE_LOCK:
        if not _refresh_notice_shown:
            display.print("[dim]检测到登录态失效，正在自动刷新 cookie...[/dim]")
            _refresh_notice_shown = True

    # propagate_errors：把"需要输入验证码"这类用户能据此行动的信息透出去，
    # 而不是压成一句笼统的"未找到有效 cookie"。
    cookie = relogin(propagate_errors=True)
    if not cookie:
        return ""
    if workspace_id:
        save_cookie(cookie, workspace_id=workspace_id)
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


def _live_cookie_for_paging(fallback: str) -> str:
    """分页 / 循环调用里取当前 cookie —— **不要闭包一个字符串**。

    盘上那份是唯一事实来源。某一页触发自动重登后，新 cookie 会被写回磁盘；下一页
    从磁盘取就自动用上了新的，不会再拿着已经失效的那个去白撞 401。

    这是 inspire-skill 那套「凭据是可变对象、重登后原地刷新」的等价做法 ——
    它靠对象引用让持有者自动看到新值，我们靠"每次回源读盘"达到同样效果，
    不必把 qzcli 全链路的 cookie 字符串改造成对象。

    读不到盘上 cookie 时退回入参，保持原行为（比如测试里直接传字符串的场景）。
    """
    return _get_cookie_value() or fallback


def _fetch_all_node_dimensions(
    api,
    workspace_id: str,
    cookie: str,
    logic_compute_group_id: Optional[str] = None,
    compute_group_id: Optional[str] = None,
    page_size: int = 500,
) -> List[Dict[str, Any]]:
    """分页获取节点维度数据。"""

    def _fetch_page(page_num: int) -> Dict[str, Any]:
        return api.list_node_dimension(
            workspace_id,
            _live_cookie_for_paging(cookie),
            logic_compute_group_id=logic_compute_group_id,
            compute_group_id=compute_group_id,
            page_num=page_num,
            page_size=page_size,
        )

    data = _fetch_page(1)
    first_batch = data.get("node_dimensions", [])
    nodes: List[Dict[str, Any]] = list(first_batch)
    total = data.get("total")

    if isinstance(total, int) and total > len(nodes) and first_batch:
        # The service may cap page_size below the requested value, so derive the
        # effective page size from the first response.
        effective_page_size = len(first_batch)
        page_count = (total + effective_page_size - 1) // effective_page_size
        for page_num in range(2, page_count + 1):
            nodes.extend(_fetch_page(page_num).get("node_dimensions", []))
        return nodes

    page_num = 2
    while len(first_batch) >= page_size:
        data = _fetch_page(page_num)
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
            _live_cookie_for_paging(cookie),
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

    def _fetch_page(page_num: int) -> Dict[str, Any]:
        return api.list_task_dimension(
            workspace_id,
            cookie,
            project_id=project_id,
            page_num=page_num,
            page_size=page_size,
        )

    data = _fetch_page(1)
    first_batch = data.get("task_dimensions", [])
    tasks: List[Dict[str, Any]] = list(first_batch)
    total = data.get("total")

    if isinstance(total, int) and total > len(tasks) and first_batch:
        effective_page_size = len(first_batch)
        page_count = (total + effective_page_size - 1) // effective_page_size
        for page_num in range(2, page_count + 1):
            tasks.extend(_fetch_page(page_num).get("task_dimensions", []))
        return tasks

    page_num = 2
    while len(first_batch) >= page_size:
        data = _fetch_page(page_num)
        batch = data.get("task_dimensions", [])
        tasks.extend(batch)
        if len(batch) < page_size:
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
    # **这个数是「分配率」，不是「利用率」。变量名按实际含义叫 alloc。**
    #
    # 它算的是 `(总卡 − 空闲卡) / 总卡`，即有多少卡被**分配出去**了。一个占着
    # 8 张卡跑 0% 的任务，在这里是 100%。
    #
    # ⚠️ **但 `avail` 表头故意仍然显示「GPU利用率」，这不是没改完。** 这是这个
    # 命令最常被看的一列，用途是"还能不能起任务"，在这个用途上两个口径给出的
    # 判断一样；为一个次要场景去动天天在看的表头，收益不抵改动成本。
    # （2026-08-27 一度改成「GPU分配率」，用户反馈后改回。）
    #
    # 真正要小心的是**别拿这一列去判断"这台机器会不会被空闲回收"** —— 平台那个
    # 判据看的是**真实利用率**（GPU 低于阈值持续数小时就收），跟分配率结论相反。
    # 真实利用率另有来源：`task_dimension_to_row()` 里平台直接给的
    # `gpu.usage_rate`（`dashboard` 走这条）。`tests/test_gpu_alloc_vs_util.py`
    # 钉着这个区别和那条来源不被误删。
    gpu_alloc_ratio = (used_gpus / total_gpus) if total_gpus > 0 else None
    return {
        "total_nodes": total_nodes,
        "schedulable_nodes": schedulable_nodes,
        "free_nodes": free_nodes,
        "total_gpus": total_gpus,
        "free_gpus": free_gpus,
        "gpu_alloc_ratio": gpu_alloc_ratio,
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
    quick: bool = False,
) -> Tuple[Dict[str, Any], int]:
    """从任务、task_dimension、cluster_info 等接口聚合 workspace 资源。

    Args:
        quick: 跳过 _fetch_all_jobs_with_cookie 的全量分页，秒级返回。代价是
            不发现 specs（specs 只能从历史任务里反推），但 compute_groups 和
            projects 仍然完整（由 cluster_info / task_dimension 提供）。
    """
    if quick:
        jobs: List[Dict[str, Any]] = []
        resources: Dict[str, Any] = {
            "projects": [],
            "compute_groups": [],
            "specs": [],
        }
    else:
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
        except QzAPIError as exc:
            # 原来是裸 `pass`。后果是鉴权失败被吞成「拉到 0 个项目」，调用方分不清
            # 「这个空间真没项目」和「我根本没拉到」，再往下就把空结果写回缓存，
            # 把好数据冲掉（2026-08-16 真发生过）。现在至少留痕，
            # 调用方也改成「拉到空就不覆盖非空缓存」。
            swallowed("res/拉取任务维度项目", exc)

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
    options: List[Dict[str, Any]],
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


def _list_workspace_options_for_avail(
    api,
    display,
    *,
    workspace_input: Optional[str],
    cached_workspace_id: str = "",
    include_usage_snapshot: bool = False,
    show_progress: bool = False,
) -> List[Dict[str, Any]]:
    """Return workspace options for avail without forcing a slow live refresh."""
    cached = list_cached_workspaces()
    if cached:
        if not workspace_input:
            return cached
        if workspace_input.startswith("ws-") and any(
            str(option.get("id", "") or "") == workspace_input for option in cached
        ):
            return cached
        if cached_workspace_id:
            return cached

    return _list_available_workspaces(
        api,
        display,
        include_usage_snapshot=include_usage_snapshot,
        show_progress=show_progress,
    )


def _workspace_options_for_resolved_id(
    workspace_options: List[Dict[str, Any]], workspace_id: str, ws_display: str = ""
) -> List[Dict[str, Any]]:
    """Return the live option for a resolved workspace, or a cache-backed option."""
    for option in workspace_options:
        if str(option.get("id", "") or "") == workspace_id:
            return [option]

    cached_resources = get_workspace_resources(workspace_id) or {}
    return [
        {
            "id": workspace_id,
            "name": ws_display or cached_resources.get("name", workspace_id),
        }
    ]


def _resolve_workspace_option_for_avail(
    workspace_options: List[Dict[str, Any]],
    workspace_value: str,
    *,
    cached_workspace_id: str = "",
    cached_workspace_name: str = "",
) -> Tuple[Optional[str], str, List[Dict[str, Any]]]:
    """Resolve workspace for avail while preserving legacy cached fuzzy aliases."""
    if not workspace_value:
        return None, "", []

    for option in workspace_options:
        option_id = str(option.get("id", "") or "")
        option_name = str(option.get("name", "") or option_id)
        if not option_id:
            continue
        if workspace_value == option_id:
            return option_id, option_name or option_id, []

    exact_match: Optional[Dict[str, Any]] = None
    fuzzy_matches: List[Dict[str, Any]] = []
    lowered = workspace_value.lower()
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

    if exact_match:
        matched_id = str(exact_match.get("id", "") or "")
        matched_name = str(exact_match.get("name", "") or matched_id)
        return matched_id or None, matched_name, []

    if cached_workspace_id:
        for option in workspace_options:
            if str(option.get("id", "") or "") == cached_workspace_id:
                return (
                    cached_workspace_id,
                    str(
                        option.get("name", "")
                        or cached_workspace_name
                        or cached_workspace_id
                    ),
                    [],
                )
        return cached_workspace_id, cached_workspace_name or cached_workspace_id, []

    if len(fuzzy_matches) == 1:
        matched = fuzzy_matches[0]
        matched_id = str(matched.get("id", "") or "")
        matched_name = str(matched.get("name", "") or matched_id)
        return matched_id or None, matched_name, []

    return None, "", fuzzy_matches


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
    gpu_alloc_ratio = option.get("gpu_alloc_ratio")

    if total_nodes:
        parts.append(f"空节点 {free_nodes}/{total_nodes}")
    if total_gpus:
        parts.append(f"空GPU {free_gpus}/{total_gpus}")
    if gpu_alloc_ratio is not None:
        parts.append(f"GPU利用率 {gpu_alloc_ratio * 100:.1f}%")

    return " | ".join(parts)


def _list_available_workspaces(
    api,
    display,
    *,
    include_usage_snapshot: bool = True,
    show_progress: bool = False,
) -> List[Dict[str, Any]]:
    """优先从当前可访问 workspace API 获取工作空间，失败时回退到本地缓存。"""
    workspaces: List[Dict[str, Any]] = []

    try:
        workspaces = _with_live_cookie(
            api, display, lambda cookie: api.list_workspaces(cookie)
        )
        progress = None
        progress_task_id = None
        if (
            include_usage_snapshot
            and show_progress
            and hasattr(display, "create_progress")
        ):
            progress = display.create_progress()
            if progress:
                progress.start()
                progress_task_id = progress.add_task(
                    "刷新 workspace 实时占用", total=len(workspaces)
                )
        for ws in workspaces:
            ws_id = ws.get("id", "")
            ws_name = ws.get("name", "")
            if ws_id:
                set_workspace_name(ws_id, ws_name)
            if ws_id and include_usage_snapshot:
                try:
                    if progress and progress_task_id is not None:
                        progress.update(
                            progress_task_id,
                            description=f"刷新 {ws_name or ws_id} 实时占用",
                        )
                    usage_snapshot = _load_workspace_usage_snapshot(api, display, ws_id)
                    ws.update(usage_snapshot.get("workspace", {}))
                    if usage_snapshot.get("compute_groups"):
                        ws["_usage_snapshot"] = usage_snapshot
                except QzAPIError as e:
                    if not _is_auth_related_error(e):
                        display.print(
                            f"[dim]{ws_name or ws_id} 的实时占用获取失败，使用缓存列表: {e}[/dim]"
                        )
            if progress and progress_task_id is not None:
                progress.advance(progress_task_id)
        if progress:
            progress.stop()
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


def _refresh_workspace_resources_for_avail(
    api, display, workspace_id: str, ws_name: str = ""
) -> Optional[Dict[str, Any]]:
    """为 avail 轻量刷新 compute group 缓存，避免拉历史任务和规格。"""
    cached_resources = get_workspace_resources(workspace_id) or {}

    def _fetch_compute_groups(cookie: str) -> List[Dict[str, Any]]:
        compute_groups_from_api: List[Dict[str, Any]] = []
        try:
            cluster_info = api.get_cluster_basic_info(workspace_id, cookie)
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
        except QzAPIError:
            compute_groups_from_api = []

        if compute_groups_from_api:
            return compute_groups_from_api

        nodes = _fetch_all_node_dimensions(api, workspace_id, cookie, page_size=500)
        compute_groups_from_nodes: List[Dict[str, Any]] = []
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
        return compute_groups_from_nodes

    compute_groups = _with_live_cookie(
        api,
        display,
        _fetch_compute_groups,
        workspace_id=workspace_id,
    )
    merged_resources = {
        "projects": list(cached_resources.get("projects", {}).values()),
        "compute_groups": _merge_resource_lists(
            list(cached_resources.get("compute_groups", {}).values()),
            compute_groups,
        ),
        "specs": list(cached_resources.get("specs", {}).values()),
    }
    save_resources(
        workspace_id, merged_resources, ws_name or cached_resources.get("name", "")
    )
    return get_workspace_resources(workspace_id)


def _load_workspace_resources_for_avail(
    api, display, workspace_id: str, ws_name: str = ""
) -> Optional[Dict[str, Any]]:
    """获取 avail 所需资源缓存，不足时仅轻量刷新 compute groups。"""
    cached_resources = get_workspace_resources(workspace_id)
    if cached_resources and cached_resources.get("compute_groups"):
        return cached_resources

    refreshed = _refresh_workspace_resources_for_avail(
        api, display, workspace_id, ws_name
    )
    return refreshed or cached_resources


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
            # 带上 workspace_id：/openapi/v1/specs/list 已 404，第二级要按
            # 工作空间翻历史任务才能反推出 spec
            for spec in api.list_specs(compute_group_id, workspace_id)
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


def _lookup_spec_for_payload(
    api,
    workspace_id: str,
    ws_name: str,
    compute_group_id: str,
    spec_id: str,
    display=None,
) -> Dict[str, Any]:
    """获取构造 resource_spec_price 所需的完整 spec 字段。

    优先读 resources.json 缓存；若缺 cpu_count/gpu_count/memory_gb，自动调一次
    /openapi/v1/specs/list 刷新再读；仍不齐则抛错，提示 ``qzcli res -w <ws> -u``。
    """

    def _read_normalized_spec(workspace_id_: str, spec_id_: str) -> Dict[str, Any]:
        cached = get_workspace_resources(workspace_id_) or {}
        raw = (cached.get("specs") or {}).get(spec_id_) or {}
        normalized = _normalize_spec_item(raw, compute_group_id) if raw else None
        return normalized or {}

    def _belongs_to_target_group(spec: Dict[str, Any]) -> bool:
        """这条缓存记录是不是给**目标计算组**缓存的。

        规格是工作空间级的，同一个 id 在别的计算组缓存过 —— 直接拿来用会把那边的
        ``gpu_type`` 带进 payload。实测向「训练区-H200-1号机房」提交时，缓存里那条
        8卡160核 归属的是开发区-H100-183核，于是 payload 写成
        ``NVIDIA_H100_SXM_80G``，而目标组 180 个节点全是 H200。

        **这比报错更糟**：任务会一直排队等一种该组里不存在的卡，看起来"成功进入
        排队"，实际永远起不来。

        没有归属字段时返回 True（维持旧行为）—— 判不出来不等于不属于，
        这跟缓存残缺矩阵那条纪律一致：缓存无从判断时放行，别造假错误。

        **必须看原始缓存记录，不能看规范化之后的。** ``_normalize_spec_item``
        会把目标计算组当 fallback 注入进去，规范化之后再判断就永远为真。
        """
        if not compute_group_id:
            return True
        cached = get_workspace_resources(workspace_id) or {}
        raw = (cached.get("specs") or {}).get(spec_id) or {}
        owned = raw.get("logic_compute_group_ids") or (
            [raw["logic_compute_group_id"]] if raw.get("logic_compute_group_id") else []
        )
        if not owned:
            return True  # 缓存没说归属，无从判断 → 放行
        return compute_group_id in owned

    spec_obj = _read_normalized_spec(workspace_id, spec_id)
    has_resource_fields = bool(
        spec_obj.get("cpu_count")
        or spec_obj.get("gpu_count")
        or spec_obj.get("memory_gb")
    )
    if has_resource_fields and _belongs_to_target_group(spec_obj):
        return spec_obj

    # 缓存里没有可用的 cpu/gpu/mem 字段，尝试一次实时刷新。
    try:
        _load_specs_for_create_result(
            api,
            workspace_id,
            ws_name,
            compute_group_id,
            display=display,
            emit_messages=False,
        )
    except (QzAPIError, requests.RequestException) as exc:
        # 预加载失败不致命：下面读缓存还有机会命中。但真拿不到规格时的报错
        # 要能回溯到这里，别只剩一句「找不到规格」。
        swallowed("create/规格预加载", exc)

    spec_obj = _read_normalized_spec(workspace_id, spec_id)
    if not (
        spec_obj.get("cpu_count")
        or spec_obj.get("gpu_count")
        or spec_obj.get("memory_gb")
    ):
        raise QzAPIError(
            f"无法解析规格 '{spec_id}' 的 cpu/gpu/memory 信息，"
            "请运行 `qzcli res -w <workspace> -u` 刷新缓存后再试"
        )
    return spec_obj


def _auto_select_spec_for_compute_group(
    workspace_id: str, compute_group_id: str, api=None
) -> Tuple[Optional[str], Optional[str]]:
    """为当前计算组自动挑一个 spec：先看缓存，缓存没有就问平台。

    **缓存没有是常态，不是边缘情况。** `res -u` 默认走 quick 模式，而 quick
    明确不产出 specs（specs 只能从历史任务反推），所以 `specs={}` 是默认稳态 ——
    实测本机 16 个工作空间里 15 个是空的。只看缓存的话，`create` 不带 `--spec`
    在绝大多数工作空间上直接报「未指定资源规格且缓存中无可用规格」。

    平台侧 `api.list_specs()` 已经有权威来源了
    （`workspace GetScheduleConfig` 的 predef_train_spec，v0.4.0 加的），
    这里接上即可。**缓存有就用缓存**，不改变现有行为。
    """
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
    if not scoped_specs and api is not None:
        # 缓存没有 → 问平台。挑 GPU 数最小的那个，别默认就占最大的机器。
        try:
            platform_specs = [
                normalized
                for spec in api.list_specs(compute_group_id, workspace_id)
                for normalized in [_normalize_spec_item(spec, compute_group_id)]
                if normalized
            ]
        except QzAPIError:
            platform_specs = []
        if platform_specs:
            scoped_specs = sorted(
                platform_specs, key=lambda s: (s.get("gpu_count") or 0)
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
    options: List[Dict[str, Any]],
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
        total_gpu_alloc_ratio = _format_percent(total_used_gpus, total_gpus)
        display.print(
            f"[dim]空节点 {total_free_nodes}/{total_nodes} | 空GPU {total_free_gpus}/{total_gpus} | GPU利用率 {total_gpu_alloc_ratio}[/dim]"
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
                gpu_alloc_text = _format_percent(used_gpus, total_gpus)
                if total_gpus > 0:
                    gpu_alloc_ratio = used_gpus / total_gpus
                    if gpu_alloc_ratio >= 0.8:
                        gpu_alloc_text = f"[green]{gpu_alloc_text}[/green]"
                    elif gpu_alloc_ratio >= 0.4:
                        gpu_alloc_text = f"[yellow]{gpu_alloc_text}[/yellow]"
                    else:
                        gpu_alloc_text = f"[red]{gpu_alloc_text}[/red]"
                else:
                    gpu_alloc_text = "[dim]-[/dim]"
            else:
                free_nodes_text = "[dim]-[/dim]"
                free_gpu_text = "[dim]-[/dim]"
                gpu_alloc_text = "[dim]-[/dim]"

            table.add_row(
                str(idx),
                option.get("name") or option.get("id", ""),
                free_nodes_text,
                str(total_nodes) if has_capacity else "-",
                free_gpu_text,
                gpu_alloc_text,
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
    options: List[Dict[str, Any]],
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
        total_gpu_alloc_ratio = _format_percent(total_used_gpus, total_gpus)
        prefix = "按唯一资源池汇总: " if has_shared_pool else ""
        display.print(
            f"[dim]{prefix}空节点 {total_free_nodes}/{total_nodes} | 空GPU {total_free_gpus}/{total_gpus} | GPU利用率 {total_gpu_alloc_ratio}[/dim]"
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
                gpu_alloc_text = _format_percent(used_gpus, total_gpus)
                if total_gpus > 0:
                    gpu_alloc_ratio = used_gpus / total_gpus
                    if gpu_alloc_ratio >= 0.8:
                        gpu_alloc_text = f"[green]{gpu_alloc_text}[/green]"
                    elif gpu_alloc_ratio >= 0.4:
                        gpu_alloc_text = f"[yellow]{gpu_alloc_text}[/yellow]"
                    else:
                        gpu_alloc_text = f"[red]{gpu_alloc_text}[/red]"
                else:
                    gpu_alloc_text = "[dim]-[/dim]"
            else:
                free_nodes_text = "[dim]-[/dim]"
                free_gpu_text = "[dim]-[/dim]"
                gpu_alloc_text = "[dim]-[/dim]"

            table.add_row(
                str(idx),
                option.get("name") or option.get("id", ""),
                option.get("gpu_type", "") or "-",
                _get_compute_group_usage_scope_label(option),
                _get_compute_group_spec_status_label(option),
                free_nodes_text,
                str(total_nodes) if has_capacity else "-",
                free_gpu_text,
                gpu_alloc_text,
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
    total_gpu_alloc_ratio = _format_percent(total_used_gpus, total_gpus)
    return [
        f"总览: 空节点 {total_free_nodes}/{total_nodes} | 空GPU {total_free_gpus}/{total_gpus} | GPU利用率 {total_gpu_alloc_ratio}"
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
        total_gpu_alloc_ratio = _format_percent(total_used_gpus, total_gpus)
        prefix = "按唯一资源池汇总: " if has_shared_pool else "总览: "
        lines.append(
            f"{prefix}空节点 {total_free_nodes}/{total_nodes} | 空GPU {total_free_gpus}/{total_gpus} | GPU利用率 {total_gpu_alloc_ratio}"
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
    options: List[Dict[str, Any]],
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
    options: List[Dict[str, Any]],
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
    options: List[Dict[str, Any]],
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
    options: List[Dict[str, Any]],
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
            # 缓存说「不属于」时**不能直接拒** —— 新建的计算组在缓存刷新前必然
            # 查不到，而它可能正跑着任务。跟平台确认一次再决定。
            on_platform = _compute_group_exists_on_platform(
                api, workspace_id, resolved_compute_group_id, display
            )
            if on_platform is True:
                display.print(
                    "[dim]计算组不在本地缓存里，但平台确认它属于该工作空间，继续。"
                    "（缓存已过期，可稍后 qzcli res -u 更新）[/dim]"
                )
            elif on_platform is None:
                display.print(
                    "[dim]计算组不在本地缓存里，且无法向平台确认；继续提交，"
                    "由平台校验。[/dim]"
                )
            else:
                display.print_error(
                    f"计算组 '{args.compute_group}' 不属于当前工作空间 "
                    f"'{ws_display or workspace_id}'（已向平台确认）"
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

    # ⚠️ 这里**不再预填**写死的默认镜像。
    #
    # 以前预填 DEFAULT_CREATE_IMAGE + SOURCE_PRIVATE，用户一路回车就把它们变成了
    # 「显式指定」——而那个镜像 2026-08 已从平台删除、类型又和公共 registry 冲突，
    # 结果是必然失败并甩一句指不到镜像上的 InternalError: Unauthorized。
    #
    # 现在留空表示「让 qzcli 去平台/你的历史任务里找」，由 resolve_create_image
    # 处理；找不到会明确告诉你要传什么。
    if args.image is None:
        entered = _prompt_text_input(
            display, "Docker 镜像（留空=按你近期任务自动选）", "", required=False
        )
        args.image = entered or None

    if args.image_type is None:
        entered = _prompt_text_input(
            display,
            "镜像类型 SOURCE_PUBLIC/SOURCE_PRIVATE（留空=自动判定）",
            "",
            required=False,
        )
        args.image_type = entered or None

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


def cmd_dashboard(args):
    """启动 GPU 使用「成分下钻」可视化看板（Streamlit + treemap）。"""
    display = get_display()

    # 可选依赖检测
    missing = []
    for mod in ("streamlit", "plotly", "pandas"):
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
    if missing:
        # 注意：display.print_error 走 rich，`[dashboard]` 会被当作 markup，需转义
        display.print_error(
            "看板需要额外依赖：" + ", ".join(missing) + "\n"
            r"请安装：pip install 'qzcli\[dashboard]'"
            "（或 pip install streamlit plotly pandas）"
        )
        return 1

    # 解析 workspace（名称 -> ID），经环境变量传给 streamlit 子进程
    ws_input = args.workspace or "分布式"
    if ws_input.startswith("ws-"):
        ws_id = ws_input
    else:
        ws_id = find_workspace_by_name(ws_input)
        if not ws_id:
            display.print_error(f"未找到工作空间 “{ws_input}”，请先运行: qzcli res -u")
            return 1

    import os
    import subprocess

    app_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "dashboard_app.py"
    )
    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        app_path,
        "--server.port",
        str(args.port),
    ]
    if args.no_browser:
        cmd += ["--server.headless", "true"]

    env = dict(os.environ, QZCLI_DASHBOARD_WS=ws_id)
    display.print(
        f"[dim]启动看板：workspace={ws_input} 端口={args.port}（Ctrl+C 退出）[/dim]"
    )
    try:
        return subprocess.run(cmd, env=env).returncode
    except KeyboardInterrupt:
        return 0



def _spec_gpu_count(spec_id):
    """尽力从本地缓存拿这个规格的 GPU 数，只用于把报错说得具体一点。

    **拿不到就返回 None**，绝不能因为它让提交流程报错 —— 它只是文案里的一个数字。
    """
    if not spec_id:
        return None
    try:
        for ws in (load_all_resources() or {}).values():
            spec = (ws.get("specs") or {}).get(spec_id)
            if spec:
                return spec.get("gpu_count")
    except Exception:  # noqa: BLE001
        swallowed("create/查规格卡数", RuntimeError("spec lookup failed"))
    return None


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

    if args.instances is None:
        args.instances = DEFAULT_CREATE_INSTANCES
    if args.shm is None:
        args.shm = DEFAULT_CREATE_SHM
    if args.priority is None:
        args.priority = get_default_priority()
        # 行为变过（曾经默认 10=最高优），就不能悄悄变 —— 明确说清用了什么、
        # 怎么改。否则老脚本会从"直接跑"变成"排队"而用户找不到原因。
        display.print(
            f"[dim]未指定 --priority，使用默认 {args.priority}"
            f"（{'LOW' if args.priority <= 3 else 'NORMAL' if args.priority <= 4 else 'HIGH'}）。"
            f"需要更高优先级请显式 --priority，或设 QZCLI_DEFAULT_PRIORITY[/dim]"
        )
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
            # 缓存说「不属于」不能直接拒 —— 新建/新加入的项目必然还没进缓存。
            # 跟平台确认一次再决定（与计算组同一模式）。
            on_platform = _project_belongs_to_workspace_on_platform(
                api, workspace_id, project_id
            )
            if on_platform is True:
                display.print(
                    "[dim]项目不在本地缓存里，但平台确认它属于该工作空间，继续。"
                    "（缓存已过期，可稍后 qzcli res -u 更新）[/dim]"
                )
            elif on_platform is None:
                display.print(
                    "[dim]项目不在本地缓存里，且无法向平台确认；继续提交，"
                    "由平台校验。[/dim]"
                )
            else:
                display.print_error(
                    f"项目 '{args.project}' 不属于当前工作空间 "
                    f"'{ws_display}'（已向平台确认）"
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
            # 同上：缓存说「不属于」不能直接拒，新建的计算组必然还没进缓存。
            on_platform = _compute_group_exists_on_platform(
                api, workspace_id, compute_group_id, display
            )
            if on_platform is True:
                display.print(
                    "[dim]计算组不在本地缓存里，但平台确认它属于该工作空间，继续。"
                    "（缓存已过期，可稍后 qzcli res -u 更新）[/dim]"
                )
            elif on_platform is None:
                display.print(
                    "[dim]计算组不在本地缓存里，且无法向平台确认；继续提交，"
                    "由平台校验。[/dim]"
                )
            else:
                display.print_error(
                    f"计算组 '{args.compute_group}' 不属于当前工作空间 "
                    f"'{ws_display}'（已向平台确认）"
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
            workspace_id, compute_group_id, api=api
        )
        if not spec_id:
            display.print_error("未指定资源规格且缓存中无可用规格")
            display.print("[dim]使用 --spec 指定，或先运行 qzcli res -u[/dim]")
            return 1
        display.print(f"[dim]自动选择规格: {spec_display} ({spec_id})[/dim]")

    # --- Resolve spec details for resource_spec_price ---
    try:
        spec_obj = _lookup_spec_for_payload(
            api,
            workspace_id,
            ws_display,
            compute_group_id,
            spec_id,
            display=display,
        )
    except QzAPIError as e:
        if args.dry_run:
            # 在 dry-run 下，缺规格字段不阻塞 payload 预览。
            display.print(f"[dim]规格字段不完整 ({e})；dry-run 仍会输出 payload[/dim]")
            spec_obj = {"id": spec_id}
        else:
            display.print_error(str(e))
            return 1
    # 镜像：显式传的一律照用；没传才去平台/历史里找，都找不到就明确报错。
    #
    # 这里以前是无条件套两个写死的常量（在 args 解析处），而其时默认镜像已从
    # 平台删除、默认 image_type 又和公共 registry 冲突 —— **任何不指定镜像的
    # 用户必然失败**，拿到的还是一句指不到镜像上的 InternalError: Unauthorized。
    # 交互式创建时两者都是默认值，一路回车就中招。
    #
    # 放在这里而不是 args 解析处：解析处还没有 workspace_id / cookie，
    # 查平台和查历史都做不了。
    try:
        args.image, args.image_type = resolve_create_image(
            api,
            (get_cookie() or {}).get("cookie", ""),
            workspace_id,
            args.image,
            args.image_type,
            display=display,
        )
    except _ImageResolutionError as exc:
        display.print_error(str(exc))
        return 1

    spec_price = build_resource_spec_price(spec_obj, compute_group_id)

    # --- Build payload ---
    # 平台已弃用 framework_config[0].spec_id，改用嵌套 resource_spec_price，
    # 同时 framework_config[0] 顶层也要带 cpu/mem_gi/gpu_count 否则会被拒
    # ("Cpu and Mem can't be empty.")。
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
                "cpu": int(spec_obj.get("cpu_count") or 0),
                "gpu_count": int(spec_obj.get("gpu_count") or 0),
                "mem_gi": int(spec_obj.get("memory_gb") or 0),
                "resource_spec_price": spec_price,
                "image": args.image,
                "image_type": args.image_type,
                "instance_count": args.instances,
                "shm_gi": args.shm,
            }
        ],
    }

    # --- Exclude / include nodes（碎卡治理:顶层 exclude_nodes / specified_nodes，v2 选项）---
    def _norm_nodes(raw_list, flag):
        seen = set()
        out = []
        for raw in raw_list:
            node = str(raw).strip()
            if not node:
                display.print_error(f"{flag} 不能为空节点名")
                return None
            if node not in seen:
                seen.add(node)
                out.append(node)
        return out

    if getattr(args, "exclude_node", None):
        vals = _norm_nodes(args.exclude_node, "--exclude-node")
        if vals is None:
            return 1
        payload["exclude_nodes"] = vals
    if getattr(args, "include_node", None):
        vals = _norm_nodes(args.include_node, "--include-node")
        if vals is None:
            return 1
        payload["specified_nodes"] = vals

    # --- Dataset mounting ---
    if getattr(args, "dataset", None):
        dataset_info = []
        for ds_spec in args.dataset:
            parts = ds_spec.split(":")
            if len(parts) == 2:
                ds_id, ver_id = parts
            elif len(parts) == 1:
                ds_id, ver_id = parts[0], "v1"
            else:
                display.print_error(
                    f"无效的数据集格式: {ds_spec}，应为 dataset_id:version_id"
                )
                return 1
            dataset_info.append(
                {
                    "dataset_id": ds_id,
                    "path": f"rclone-worker-1/{ds_id}/{ver_id}",
                    "version_id": ver_id,
                }
            )
        payload["dataset_info"] = dataset_info
        display.print(f"  挂载数据集: {len(dataset_info)} 个")
        for di in dataset_info:
            display.print(f"    - {di['dataset_id']} ({di['version_id']})")

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
    # 平台 Web UI 已把作业创建迁到 v2 Console API
    # (/api/v2/train?Action=CreateJobConsole)，payload 结构与 v1 一致。已真机验证 v2
    # create 可正常创建作业(真机提交并停止成功)，故默认走 create_job_v2；
    # 老 v1 create_job_with_cookie 保留作回退。无 cookie 时退老 openapi token path。
    # 注：exclude_nodes 需 workspace 级启用，未启用的空间平台会报
    # "exclude_nodes not enable in workspace"(如 分布式训练空间)。
    # 提交前：这个（空间, 规格, 优先级）组合上次被平台拒过吗？
    #
    # **只拦已知被拒的组合，没见过的一律放行。** 反过来做（维护一张"允许"白名单）
    # 会把平台后来放开的组合永久挡在门外 —— 而允许范围我们根本读不到，
    # 白名单从第一天起就是错的。
    if not getattr(args, "force_priority", False):
        known = _priority.known_rejection(workspace_id, spec_id, args.priority)
        if known:
            display.print_error(_priority.explain_known(known, cg_display))
            return 1

    try:
        cookie_data = get_cookie()
        if cookie_data and cookie_data.get("cookie"):
            result = api.create_job_v2(cookie_data["cookie"], payload)
        else:
            result = api.create_job(payload)
    except QzAPIError as e:
        # 「优先级不在该资源规格允许的范围内」原样抛给用户等于没说 —— 他不知道
        # 该改什么。翻译成下一步，并记住这次拒绝供下次提前拦。
        if _priority.is_priority_rejection(str(e)):
            _priority.remember_rejection(
                workspace_id, spec_id, args.priority, _spec_gpu_count(spec_id)
            )
            display.print_error(
                "任务创建失败：优先级不被这个资源规格接受\n\n"
                + _priority.explain(
                    workspace_id,
                    spec_id,
                    args.priority,
                    _spec_gpu_count(spec_id),
                    cg_display,
                )
            )
            return 1
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


def cmd_ops(args):
    """查看操作日志。

    ``--merge`` 是为查锁号这类问题准备的：本机 ``~/.qzcli``、开发机上的、以及
    项目组冻结版各写各的日志，只看一份会漏掉「另一边在同一时刻也登了一次」。
    """
    from . import opslog

    display = get_display()
    rows = opslog.read(op=getattr(args, "op", None), since_hours=getattr(args, "since", None))
    seen_paths = [str(opslog.log_path())]
    for extra in getattr(args, "merge", []) or []:
        p = Path(extra).expanduser()
        if p.is_dir():
            p = p / opslog.LOG_NAME
        rows += opslog.read(p, op=getattr(args, "op", None), since_hours=getattr(args, "since", None))
        seen_paths.append(str(p))

    rows.sort(key=lambda r: r.get("ts_utc", ""))
    if not rows:
        display.print(f"没有记录。日志位置: {'、'.join(seen_paths)}")
        display.print("[dim]只读命令（list/status/avail/usage）刻意不记，所以空是正常的[/dim]")
        return 0

    display.print(f"共 {len(rows)} 条（来自 {len(seen_paths)} 份日志）\n")
    for r in rows:
        mark = "✓" if r.get("outcome") == "ok" else "✗"
        dur = f" {r['duration_ms']}ms" if r.get("duration_ms") is not None else ""
        tgt = f"  {r['target']}" if r.get("target") else ""
        err = f"  [{r['err_class']}]" if r.get("err_class") else ""
        display.print(
            f"  {r.get('ts_utc','')}  {mark} {r.get('op',''):<12}"
            f" pid={r.get('pid','?'):<7} {r.get('host','')[:24]}{tgt}{err}{dur}"
        )
    return 0


def _opslog_name(args):
    """把「子命令 + 参数」映射成操作日志里的 op 名。

    多数命令直接用子命令名；三个例外是因为**同一个子命令有只读和有副作用两种形态**，
    只有后者值得记：

    - ``res`` 只读，但 ``res -u`` 会覆盖本地缓存（已知它在鉴权失败时会把缓存清空）
    - ``worker exec`` 是远程执行，``worker diag`` 只读
    - ``devbox status`` 只读，``devbox init`` 会动文件
    """
    cmd = getattr(args, "command", "") or ""
    if cmd in ("res", "resources"):
        return "res-update" if getattr(args, "update", False) else ""
    if cmd == "worker":
        return "worker-exec" if getattr(args, "worker_action", "") == "exec" else ""
    if cmd == "devbox":
        return "devbox-init" if getattr(args, "devbox_action", "") == "init" else ""
    return cmd


def _opslog_target(args):
    """操作对象，便于回溯「是哪个任务/哪台机器」。挑第一个非空的，不拼长串。"""
    for attr in ("job_id", "name", "target", "host", "notebook_id", "workspace"):
        val = getattr(args, attr, None)
        if isinstance(val, str) and val:
            return val[:120]
    return ""


def _devbox_render(display, report, dry_run=False):
    """把 devbox 的结构化报告渲染成人话。本地和远端共用同一套渲染。"""
    if report.get("error"):
        display.print_error(report["error"])
        return 1

    if report.get("mode") == "init":
        prefix = "[dry-run] " if dry_run or report.get("dry_run") else ""
        display.print(f"{prefix}持久盘: {report.get('persist_root','')}")
        for item in report["items"]:
            line = f"  {item['name']:<14} {item.get('action', '')}"
            counts = item.get("counts")
            if counts:
                line += (
                    f"（持久 {counts['persist']} 条 + 本机 {counts['local']} 条"
                    f" → 合并 {counts['merged']} 条）"
                )
            merge = item.get("merge")
            if merge:
                line += (
                    f"（新增 {merge.get('copied', 0)}、"
                    f"保留 {merge.get('kept_persist', 0)}、"
                    f"替换 {merge.get('replaced', 0)}）"
                )
            if item.get("conflict"):
                line += "  ⚠ 有冲突已备份"
            if item.get("error"):
                line += f"  ✗ {item['error']}"
            display.print(line)

        conflicts = [i for i in report["items"] if i.get("conflict") or (i.get("merge") or {}).get("conflicts")]
        if conflicts:
            display.print_warning(
                f"有冲突文件已备份到 {report.get('conflict_dir','')}，"
                "两边内容都在，请自行 diff 后取舍"
            )
        hist = report.get("histfile") or {}
        for rc, state in hist.items():
            display.print(f"  HISTFILE/{rc:<10} {state}")
        return 0

    # status 形态
    display.print(f"home: {report.get('home','')}")
    for item in report.get("items", []):
        mark = "→ " + item["resolved"] if item.get("is_symlink") else ""
        display.print(
            f"  {item['name']:<14} {item['fs']:<14} "
            f"{'软链' if item.get('is_symlink') else ('存在' if item.get('exists') else '缺失')} {mark}"
        )
    return 0


def _resources_look_empty(resources):
    """这份资源结果是不是「什么都没拉到」。

    判据只看 compute_groups 和 projects —— specs 在 quick 模式下本来就是空的，
    拿它当判据会把正常的 quick 刷新误判成失败。
    """
    if not resources:
        return True
    for key in ("compute_groups", "projects"):
        val = resources.get(key)
        if val:
            return False
    return True


def _upload_remote_script(jupyter_info, script_text, display):
    """把一段脚本经 Contents API 传到开发机，返回远端绝对路径（失败返回 ``""``）。

    为什么不直接把脚本当命令发：devbox 的自包含脚本约 18 KB，base64 之后接近
    30 KB，**PTY 塞不下这么长的命令行**——实测远端一声不吭、什么都不返回。
    ``exec`` 自己回传输出用的就是 Contents API，这里复用同一条通道。

    落点是 ``_qzcli`` 这个 Contents API 中转目录（symlink 指向 ``/tmp/.qzcli``），
    先跑一条空命令保证它被建出来，再 PUT。
    """
    import base64 as _b64

    base_http = jupyter_info["base_url"]
    token = jupyter_info["token"]
    headers = {"authorization": f"token {token}", "content-type": "application/json"}

    # 借一次 exec 把 _qzcli -> /tmp/.qzcli 的中转目录和 symlink 建出来
    _exec_via_jupyter(jupyter_info, "true", display, timeout=60)

    name = f"devbox_{uuid.uuid4().hex[:8]}.py"
    try:
        resp = requests.put(
            f"{base_http}/api/contents/_qzcli/{name}",
            headers=headers,
            json={
                "type": "file",
                "format": "base64",
                "content": _b64.b64encode(script_text.encode("utf-8")).decode(),
            },
            timeout=60,
        )
        if resp.status_code >= 400:
            display.print_error(
                f"上传脚本失败：HTTP {resp.status_code} {resp.text[:120]}"
            )
            return ""
    except requests.RequestException as exc:
        display.print_error(f"上传脚本失败：{type(exc).__name__}: {exc}")
        return ""
    return f"/tmp/.qzcli/{name}"


def cmd_devbox(args):
    """把开发机上易失的 dotfile / agent home 挪到持久盘。

    ``target`` 与 ``qzcli exec <target>`` 是同一套契约（名称 / notebook_id / URL），
    所以用户可以**直接粘一个 notebook URL**，不必先 ssh 进去。不传 target 就操作本机。

    远端形态**一次往返**：把自包含脚本 base64 送过去跑完回传 JSON，而不是逐条 exec
    （那个通道有超时史，多轮往返很容易半路断在中间状态）。
    """
    from . import devbox as devbox_mod

    display = get_display()
    action = getattr(args, "devbox_action", None) or "status"
    target = getattr(args, "target", None)

    if not target:
        try:
            if action == "status":
                report = devbox_mod.status(home=None)
            else:
                root = devbox_mod.detect_persist_root(getattr(args, "target_dir", None))
                report = devbox_mod.run(
                    root,
                    only=(getattr(args, "only", "") or "").split(",") or None,
                    include_ssh=getattr(args, "include_ssh", False),
                    dry_run=getattr(args, "dry_run", False),
                )
        except devbox_mod.DevboxError as exc:
            display.print_error(str(exc))
            return 1
        return _devbox_render(display, report, getattr(args, "dry_run", False))

    jupyter_info = _find_notebook_jupyter_info(target, display)
    if jupyter_info is None:
        return 1

    opts = {}
    if action != "status":
        opts = {
            "target_dir": getattr(args, "target_dir", None),
            "only": [s for s in (getattr(args, "only", "") or "").split(",") if s],
            "include_ssh": getattr(args, "include_ssh", False),
            "dry_run": getattr(args, "dry_run", False),
        }

    # 脚本走 **Contents API 上传成文件**，而不是塞进命令行。
    # 第一版把整段 base64 当命令发，实测 29920 字节 —— PTY 根本吃不下，
    # 远端一声不吭什么都不回。exec 自己传输出文件用的就是 Contents API，
    # 这里复用同一条通道。
    script = devbox_mod.build_remote_script(action, **opts)
    remote_path = _upload_remote_script(jupyter_info, script, display)
    if not remote_path:
        return 1

    display.print(f"[dim]在开发机上执行 devbox {action}...[/dim]")
    _exit_code, output = _exec_via_jupyter(
        jupyter_info, f"python3 {remote_path}", display, timeout=300
    )
    report = devbox_mod.parse_remote_output(output)
    return _devbox_render(display, report, getattr(args, "dry_run", False))


def cmd_worker(args):
    """在分布式训练任务的 worker 容器里执行命令 / 做通信体检。

    和 ``qzcli exec`` 分开：那个服务 notebook 开发机（Jupyter proxy），
    这个服务训练任务的 worker（WebSocket PTY），是两类完全不同的对象。
    """
    from .worker_exec import (
        WorkerExecError,
        default_instance_name,
        run_in_worker,
        run_many_in_worker,
    )

    display = get_display()
    action = getattr(args, "worker_action", None)
    job_id = getattr(args, "job_id", "")
    inst = getattr(args, "instance", None) or default_instance_name(
        job_id, getattr(args, "index", 0)
    )

    if action == "exec":
        command = " ".join(getattr(args, "cmd_args", None) or []).strip()
        if not command:
            display.print_error("请指定要执行的命令")
            display.print("[dim]示例: qzcli worker exec <job_id> nvidia-smi[/dim]")
            return 1
        display.print(f"[dim]目标实例: {inst}[/dim]")
        try:
            code, out = run_in_worker(job_id, command, inst)
        except WorkerExecError as exc:
            display.print_error(str(exc))
            return 1
        if out:
            display.print(out)
        if code != 0:
            display.print(f"[yellow]exit_code={code}[/yellow]")
        return 0 if code == 0 else code

    if action == "diag":
        # 通信体检：这几项是 2026-08-12 排查 142 节点 MoE 训练时验证有效的判据。
        checks = [
            (
                "主机/GPU",
                "hostname; nvidia-smi --query-gpu=index,name,utilization.gpu,memory.used --format=csv,noheader",
            ),
            (
                "RDMA 端口",
                "cat /sys/class/infiniband/*/ports/1/state 2>/dev/null | sort | uniq -c",
            ),
            (
                # 网卡名和 counters 路径**因机型/容器挂载而异**：不能写死 mlx5_0，
                # 实测有的节点根本没暴露 .../ports/1/counters（拿不到不等于有故障，
                # 所以下面用 NO_COUNTERS 明示"测不了"，而不是伪装成 0 误码）。
                "RDMA 误码",
                "d=$(ls -d /sys/class/infiniband/*/ports/*/counters 2>/dev/null | head -1); "
                'if [ -n "$d" ]; then for c in link_downed symbol_error port_rcv_errors; do '
                'echo -n "$c="; cat "$d/$c" 2>/dev/null || echo NA; done; '
                "else echo NO_COUNTERS（该容器未暴露 RDMA 计数器，无法判定）; fi",
            ),
            (
                "到 master 的 TCP",
                'test -n "$MASTER_ADDR" && (getent hosts "$MASTER_ADDR"; '
                'timeout 5 bash -c "</dev/tcp/$MASTER_ADDR/${MASTER_PORT:-23456}" '
                "&& echo TCP_OK || echo TCP_FAIL) || echo NO_MASTER_ADDR",
            ),
            ("NCCL 配置", "env | grep -E '^NCCL_|^MASTER_|^GLOO_' | sort"),
        ]
        display.print(f"[bold]worker 通信体检[/bold]  实例: {inst}\n")
        failed = 0
        # 所有检查跑在**同一条连接**里：逐条新建会被平台拒（socket already closed）
        try:
            outcomes = run_many_in_worker(job_id, checks, inst)
        except WorkerExecError as exc:
            display.print_error(str(exc))
            return 1
        for title, code, out in outcomes:
            # ⚠️ 光看 exit_code 会误判：像 `... || echo TCP_FAIL` 这种，
            # echo 本身成功 → code=0，但语义上是失败。所以再看输出里的失败标记。
            # （实测踩过：单机任务 master 端口没监听，TCP_FAIL 却显示 ✓。）
            bad_markers = ("TCP_FAIL", "NO_MASTER_ADDR", "Connection refused")
            unhealthy = code != 0 or any(m in (out or "") for m in bad_markers)
            # RDMA 误码是这套体检的核心判据：非 0 就是物理链路有问题，必须报红。
            # 但「拿不到计数器」不等于「有故障」，NO_COUNTERS 只提示测不了、不算失败。
            for line in (out or "").splitlines():
                m = re.match(
                    r"\s*(link_downed|symbol_error|port_rcv_errors)=(\d+)", line
                )
                if m and int(m.group(2)) > 0:
                    unhealthy = True
            mark = "[red]✗[/red]" if unhealthy else "[green]✓[/green]"
            if unhealthy:
                failed += 1
            display.print(f"{mark} [bold]{title}[/bold]")
            for line in (out or "").splitlines():
                display.print(f"    {line}")
            display.print("")
        return 1 if failed else 0

    display.print_error("请指定动作: exec / diag")
    return 1


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
            priority=args.priority,
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
            if not _note_workspace_unavailable(workspace_id, e):
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

        # 注意：dry-run **不在这里 continue**。以前是在这里就跳过了，导致
        # `batch --dry-run` 只校验模板字符串，**完全不校验 workspace / project /
        # compute-group / spec 是否解析得出来** —— 用户拿它当提交前预检必然翻车。
        # 现在走完整的 cmd_create 链路，由 cmd_create 自己的 --dry-run 在最后
        # 一步停住（它是走完整解析的），这样两个 dry-run 的语义才一致。
        if args.dry_run:
            display.print(f"[bold][{idx}/{total}][/bold] 预检: {job_name}")
        else:
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
            # 必须透传 —— 否则 batch --dry-run 会真的提交任务
            dry_run=args.dry_run,
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


_NOTEBOOK_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-" r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

_NOTEBOOK_UUID_GROUP = (
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-" r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
)

# 覆盖 SII 平台已知的 notebook 链接形态
_NOTEBOOK_URL_PATTERNS = (
    re.compile(rf"[?&]notebook_id={_NOTEBOOK_UUID_GROUP}"),
    re.compile(rf"/interactiveModel(?:ing)?Detail/{_NOTEBOOK_UUID_GROUP}"),
    re.compile(rf"/jupyter/{_NOTEBOOK_UUID_GROUP}/"),
    re.compile(rf"/api/v1/notebook/lab/{_NOTEBOOK_UUID_GROUP}"),
    re.compile(rf"/notebook/(?:lab|code)/{_NOTEBOOK_UUID_GROUP}"),
)


def _extract_notebook_id(target):
    """从 UUID / URL 中抽出 notebook_id；普通 name 返回 None。"""
    if not target:
        return None
    if _NOTEBOOK_UUID_RE.match(target):
        return target
    for pattern in _NOTEBOOK_URL_PATTERNS:
        match = pattern.search(target)
        if match:
            return match.group(1)
    return None


def _detect_user_id_from_probe(probe_jobs, username):
    """从 list_jobs 探针响应里按 username 反查当前用户的 user_id。

    比对 `created_by.extra_info.login_name == username`（platform 的学工号）。
    找不到匹配返回空字符串 —— 让调用方决定是否写 config，避免把别人 id 当成
    自己缓存（bug ekon@6ee33c6 的根因）。
    """
    if not username or not probe_jobs:
        return ""
    for job in probe_jobs:
        created_by = job.get("created_by") or {}
        login_name = (created_by.get("extra_info") or {}).get("login_name") or ""
        if login_name == username:
            return created_by.get("id", "") or ""
    return ""


def _resolve_notebook_id_by_name(target, cookie, display):
    """target → 在已缓存 workspaces 里搜 RUNNING notebook 拿 notebook_id。

    target 可以是开发机名字、完整 notebook_id，或 notebook_id 的前缀（粘贴一段
    就行）。匹配顺序：

      1. 精确命中 —— ``name == target`` 或 ``notebook_id == target``，意图明确，直接返回。
      2. 前缀模糊 —— ``notebook_id.startswith(target)``；恰好 1 个命中才返回，
         撞到多个时列出候选并报错（不默默取第一个），0 个落到"未找到"。

    不按 user_id 过滤 —— 协作者机器也能命中，且免疫 user_id 缓存 bug。
    """
    api = get_api()
    all_resources = load_all_resources()
    if not all_resources:
        display.print_error("没有已缓存的工作空间，请先 qzcli res -u")
        return None

    # 先把所有 workspace 的 RUNNING notebook 收集成候选，便于做精确/前缀两级匹配。
    notebooks = []
    for ws_id, _ws_data in all_resources.items():
        try:
            nb_result = api.list_notebooks_with_cookie(
                ws_id,
                cookie,
                page_size=50,
                user_ids=[],
                status=["RUNNING"],
            )
        except QzAPIError:
            continue
        notebooks.extend(nb_result.get("list", []))

    # 1. 精确命中：name 或 notebook_id 完全相等。
    for nb in notebooks:
        if nb.get("name") == target or nb.get("notebook_id") == target:
            return nb.get("notebook_id")

    # 2. 前缀模糊：notebook_id 以 target 开头。
    if target:
        prefix_hits = [
            nb for nb in notebooks if str(nb.get("notebook_id", "")).startswith(target)
        ]
        if len(prefix_hits) == 1:
            return prefix_hits[0].get("notebook_id")
        if len(prefix_hits) > 1:
            candidates = "、".join(
                f"{nb.get('name')} ({str(nb.get('notebook_id', ''))[:8]}…)"
                for nb in prefix_hits
            )
            display.print_error(
                f"'{target}' 前缀匹配到多个开发机：{candidates}；请给更长的前缀或完整名字/UUID"
            )
            return None

    display.print_error(f"未找到名为 '{target}' 的运行中开发机")
    return None


#: Jupyter 访问地址的形状：
#: ``https://{domain}/{ws}/{proj}/{user}/jupyter/{nb_id}/{token}/lab?token={token}``
#: v1 的 301 Location 和 v2 的 ``jupyter_url`` **是同一条 URL**（实测同 host、同路径、
#: 同 token），所以两条路共用这一个正则。
_JUPYTER_URL_RE = re.compile(
    r"(https://[^/]+/[^/]+/[^/]+/[^/]+/jupyter/[^/]+/([^/]+))/lab"
)


def _get_jupyter_info(notebook_id, cookie, display):
    """notebook_id → Jupyter ``base_url`` + ``token``。

    数据源是 ``api.get_notebook_access_url``（v2 ``notebook GetNotebookAccessUrl``，
    v2 不通时它自己回落 v1 的 301）。

    **上游 2026-08 之前 v2 全域拿不到 Jupyter 地址**，这里只能直接打 v1 的
    ``/api/v1/notebook/lab/{id}`` 读 301 响应头 —— 那是 qzcli 最后一个 v1 依赖。
    现在下沉到 api 层了，这里只负责把 URL 解析成 exec 要的三个键。

    返回 ``{base_url, token, notebook_id}``；下游 ``_exec_launch`` / ``_exec_poll`` /
    MCP 的 exec 工具全靠这三个键，**形状不能变**。
    """
    try:
        urls = get_api().get_notebook_access_url(notebook_id, cookie)
    except QzAPIError:
        raise
    except Exception as e:
        display.print_error(f"请求失败: {e}")
        return None

    jupyter_url = (urls or {}).get("jupyter_url") or ""
    if not jupyter_url:
        display.print_error("平台没有返回 Jupyter 访问地址（开发机可能未在运行）")
        return None

    match = _JUPYTER_URL_RE.search(jupyter_url)
    if not match:
        display.print_error(f"解析 Jupyter URL 失败: {jupyter_url[:200]}")
        return None

    return {
        "base_url": match.group(1),
        "token": match.group(2),
        "notebook_id": notebook_id,
    }


def _find_notebook_jupyter_info(target, display):
    """
    name | notebook_id (UUID 或前缀) | 完整 URL → Jupyter base_url + token。

    流程：
      1. UUID/URL: 直接抽 notebook_id（跳过 list_notebooks）
      2. name / notebook_id 前缀: 在已缓存 workspaces 里搜 RUNNING notebook
         （精确 name/notebook_id，或唯一的 notebook_id 前缀）
      3. notebook_id → /api/v1/notebook/lab/<id> 301 → base_url + token

    Returns: dict with {base_url, token, notebook_id} or None
    """
    cookie_data = get_cookie()
    if not cookie_data or not cookie_data.get("cookie"):
        display.print_error("未登录，请先 qzcli login")
        return None

    cookie = cookie_data["cookie"]
    notebook_id = _extract_notebook_id(target)
    if notebook_id:
        display.print(f"[dim]目标 notebook_id: {notebook_id[:8]}...[/dim]")
    else:
        notebook_id = _resolve_notebook_id_by_name(target, cookie, display)
        if not notebook_id:
            return None
        display.print(
            f"[dim]找到开发机: {target} (notebook_id: {notebook_id[:8]}...)[/dim]"
        )

    try:
        info = _get_jupyter_info(notebook_id, cookie, display)
    except QzAPIError as exc:
        if exc.code != 401:
            display.print_error(str(exc))
            return None
        # cookie 过期：用本地凭据自动重登一次再试（避免 exec 前手动 login）
        new_cookie = get_api()._relogin()
        if not new_cookie:
            display.print_error("Cookie 已过期，请重新登录: qzcli login")
            return None
        display.print("[dim]cookie 已过期，已自动重新登录[/dim]")
        try:
            info = _get_jupyter_info(notebook_id, new_cookie, display)
        except QzAPIError:
            display.print_error("Cookie 已过期，请重新登录: qzcli login")
            return None

    if info:
        display.print("[dim]已获取 Jupyter 连接信息[/dim]")
    return info


# 远端 /tmp/.qzcli/<session>/ 目录的保留天数。超过就在下次 launch 时清掉。
EXEC_SESSION_TTL_DAYS = 7

# 新格式 job_id：qzcli_<session>_<秒级时间戳>_<8位随机>
_JOB_ID_RE = re.compile(
    r"^qzcli_(?P<session>[A-Za-z0-9_-]+?)_(?P<ts>\d{9,})_[0-9a-f]{8}$"
)


def _session_of(job_id):
    """从 job_id 反推它属于哪个 session；老格式返回 ``""``。

    老格式是 ``qzcli_<ts>`` 或 ``qzcli_<ts>_<rand>``，文件平铺在 ``/tmp/.qzcli/`` 下。
    **必须能识别并回落到平铺路径** —— 否则升级前 `--detach` 拿到的 job_id
    升级后就 attach 不回来了。
    """
    if not job_id:
        return ""
    m = _JOB_ID_RE.match(job_id)
    return m.group("session") if m else ""


def _exec_paths(job_id):
    """返回 (out, exit) 两个 Contents API 相对路径。"""
    session = _session_of(job_id)
    base = f"_qzcli/{session}" if session else "_qzcli"
    return f"{base}/{job_id}_out", f"{base}/{job_id}_exit"


#: 等开发机 shell 就绪的上限。项目盘上的 rc 文件可能要十几秒才跑完，
#: 给足余量 —— 这段等待只在慢机器上真的花掉，快机器一两秒就返回。
EXEC_SHELL_READY_TIMEOUT = 40


def _wait_shell_ready(ws, _json, timeout=EXEC_SHELL_READY_TIMEOUT):
    """确认终端里的 shell **真的会执行命令**，不只是回显。

    PTY 一连上就回显输入，哪怕 shell 还没起来 —— 所以「发出去没报错」完全
    不能说明命令跑了。这里打一个 sentinel 进去：

    - 回显让 sentinel 出现**第 1 次**（shell 没起来也会有）
    - shell 真执行了 ``echo``，sentinel 才出现**第 2 次**

    所以判据是**出现 ≥2 次**。返回 ``False`` 表示到点还没执行。
    """
    import time as _t

    sentinel = f"QZRDY{uuid.uuid4().hex[:8]}"
    try:
        ws.send(_json.dumps(["stdin", f"echo {sentinel}\r"]))
    except Exception as exc:  # noqa: BLE001
        swallowed("exec/就绪探测发送", exc)
        return False

    buf = ""
    last_exc = None
    deadline = _t.time() + timeout
    while _t.time() < deadline:
        try:
            ws.settimeout(1.0)
            buf += str(ws.recv())
        except Exception as exc:  # noqa: BLE001
            # 单轮 recv 超时是**正常**的：shell 还没输出而已，每轮都记一条
            # 就成了刷屏。但真失败（连接断了、协议错）不能没有现场 ——
            # 所以留住最后一个，只在最终没等到时报出去。
            last_exc = exc
        if buf.count(sentinel) >= 2:
            return True

    if last_exc is not None:
        swallowed("exec/就绪探测轮询", last_exc)
    return False


def _exec_launch(jupyter_info, cmd_str, display, job_id=None):
    """发起 fire-and-forget 执行：建好 Contents API 中转目录，再通过 Terminal 写入
    一条复合命令（输出落到 /tmp/.qzcli/<job_id>_out、退出码落到 _exit）。

    只负责“启动”，不轮询、不清理——输出文件留待 ``_exec_poll`` / ``exec-attach`` 读取。
    命令执行与网络连接解耦：即使 WebSocket 断连，命令仍在服务端完整运行。

    Returns: job_id（成功）或 None（失败）。
    """
    import json as _json
    import time as _time

    try:
        import websocket
    except ImportError:
        display.print_error("需要 websocket-client: pip install websocket-client")
        return None

    import requests as _requests

    base_http = jupyter_info["base_url"]
    base_ws = base_http.replace("https://", "wss://")
    token = jupyter_info["token"]
    headers = {"authorization": f"token {token}", "content-type": "application/json"}

    if job_id is None:
        # job_id 由三段构成：session + 秒级时间戳 + 随机后缀。
        #
        # - **随机后缀**：原来只有时间戳，同一秒内起的多个 exec 会拿到完全相同的
        #   job_id，共用一个输出文件、互相覆盖（实测 3 路并发时第 3 路收到的是
        #   第 2 路的输出，第 1 路什么都没收到）。
        # - **session 段**：让任务可归属。多个 agent 同时用同一台开发机时，
        #   `exec --list` 靠它只列自己的，TTL 清理也按 session 整体删。
        #   编在 job_id 里而不是单独传参，是为了让 `exec-attach <job_id>`
        #   的 CLI 契约完全不用改 —— 从 id 就能反推出目录。
        job_id = f"qzcli_{get_session_id()}_{int(_time.time())}_{uuid.uuid4().hex[:8]}"

    session = _session_of(job_id)
    tmp_root = "/tmp/.qzcli"
    tmp_dir = f"{tmp_root}/{session}" if session else tmp_root
    # Contents API 通过 symlink 读取 /tmp/.qzcli
    api_dir = "_qzcli"

    # 1. **不要**在这里用 Contents API 建 `_qzcli` 目录。
    #
    # 原来这里有一句 `PUT /api/contents/_qzcli`，注释写的是「确保中转目录存在」。
    # 它恰恰**制造了**下一步的障碍：Contents API 建出来的是一个**真目录**，而
    # 下一步 `ln -sfn /tmp/.qzcli "$PWD/_qzcli"` 的 `-n` 只防「_qzcli 已经是指向
    # 目录的**符号链接**」这一种情况 —— 对真目录无效，链接会被建到目录**里面**
    # （`_qzcli/.qzcli`），`_qzcli` 本身留成一个空真目录。于是轮询
    # `_qzcli/<session>/<job>_exit` 永远 404，一路等到超时报 124。
    #
    # 后果是 **exec 在任何没用过 exec 的机器上都是坏的**：老机器上 `_qzcli` 早就
    # 是符号链接（那时还没有这句 PUT），`ln -sfn` 能正确替换，所以一直没暴露。
    #
    # 现在这个目录只由下面那条 shell 命令里的 `ln -sfn` 创建，是唯一来源。

    # 2. 通过 Terminal 发送一条复合命令（fire-and-forget）
    #    输出写到 /tmp/.qzcli/<session>/，通过 symlink 让 Contents API 可读
    # 用 setsid 把命令从这个终端会话里摘出去：终端一关（我们随后会主动删掉它），
    # 命令仍在服务端跑完。没有 setsid 的镜像回退到 nohup。
    inner = (
        f"( {cmd_str} ) > {tmp_dir}/{job_id}_out 2>&1; "
        f"echo $? > {tmp_dir}/{job_id}_exit"
    )
    # symlink 用 `ln -sfn` 幂等重建，**不再先 rm -rf**：原来的
    # `rm -rf "$PWD/_qzcli" && ln -sf` 在并发下可能把别的 exec 正在读的目录删掉。
    # `-n` 是关键，否则当 _qzcli 已是指向目录的 symlink 时，`ln -sf` 会把新链接
    # 建到目录**里面**去（变成 _qzcli/.qzcli），而不是替换它。
    #
    # 顺带清理超过 TTL 的旧 session 目录 —— 这是目前唯一的泄漏出口：
    # `--detach` 后没 attach 的、Ctrl-C 的、超时的输出文件本来会永久留着。
    # 只删 tmp_root 下一层的目录，且失败不影响主流程。
    prune = (
        f"find {tmp_root} -mindepth 1 -maxdepth 1 -type d "
        f"-mtime +{EXEC_SESSION_TTL_DAYS} -exec rm -rf {{}} + 2>/dev/null || true"
    )
    # 已经被上一版搞坏的机器要能自愈：`_qzcli` 若是**真目录**（不是符号链接），
    # 清掉再建链接。
    #
    # 两步缺一不可 —— 只 `rmdir` 是不够的：坏掉的 `_qzcli` 里通常已经躺着上一轮
    # `ln -sfn` 建歪进去的 `.qzcli` 链接，目录非空，`rmdir` 直接失败、自愈无效
    # （第一版就是这么写的，实测四台机器照样 124）。所以先删那个内层链接。
    #
    # 用 `rm -f <内层链接>` + `rmdir`，**不用 `rm -rf`**：`rm -f` 删的是符号链接
    # 本身而不是它指向的 /tmp/.qzcli；`rmdir` 只删空目录。万一那底下真有用户的
    # 东西，宁可这次 exec 失败，也不能删掉人家的数据 —— 这个目录在 `/inspire/...`
    # 项目盘上，是持久且可能共享的。
    heal = (
        f'[ -L "$PWD/{api_dir}" ] || {{ '
        f'rm -f "$PWD/{api_dir}/.qzcli"; '
        f'rmdir "$PWD/{api_dir}" 2>/dev/null; '
        f"}}; true"
    )
    shell_cmd = (
        f"mkdir -p {tmp_dir} && "
        f"{{ {heal}; }}; "
        f'ln -sfn {tmp_root} "$PWD/{api_dir}" && '
        f"{{ {prune}; }}; "
        f"{{ command -v setsid >/dev/null && setsid bash -c {shlex.quote(inner)} "
        f"|| nohup bash -c {shlex.quote(inner)}; }} >/dev/null 2>&1 &"
    )

    for attempt in range(3):
        term_name = None
        try:
            # **每次都开自己的终端，绝不复用已有的。**
            # 老实现是 `terms[0]`：不但两个并发 exec 会抢同一个终端、命令字符
            # 交错，还会直接往别人开着的交互式会话里打字（实测有开发机上躺着
            # 4 个两天前的人工终端，exec 会挑中第一个）。
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

            # **必须先确认 shell 真的能执行命令，再发正事。**
            #
            # PTY 一连上就会回显你打的字 —— 哪怕 shell 还没起来。而下面发完命令
            # 只等 1 秒就 close + DELETE 终端。慢机器上 shell 还在跑 rc 文件
            # （项目盘上的 .bashrc 可能要十几秒），命令还躺在 PTY 缓冲里，
            # 终端就被删了，命令**一次都没执行**，于是轮询一路 404 到 124。
            #
            # 2026-08-27 实测：一个网关下的机器全中招，终端只回显我打的字、
            # 不返回展开后的 `$PWD`；同一时刻另一个网关的机器秒通。我先后把这
            # 误判成「Mac 环境特有」「昇腾机器」，都错了。
            if not _wait_shell_ready(ws, _json):
                raise RuntimeError(
                    f"终端 {EXEC_SHELL_READY_TIMEOUT}s 内没就绪（只回显、不执行），"
                    "这台开发机的 shell 可能起得特别慢或已卡死"
                )

            ws.send(_json.dumps(["stdin", shell_cmd + "\r"]))
            # 给 setsid/nohup 一点时间把子进程摘出去，再关终端
            _time.sleep(1.0)
            ws.close()
            _delete_terminal(base_http, headers, term_name)
            return job_id
        except Exception as e:
            # 建了终端但没走完，别留垃圾
            _delete_terminal(base_http, headers, term_name)
            if attempt < 2:
                display.print_warning(f"连接失败，重试中... ({attempt + 1}/3)")
                _time.sleep(2)
            else:
                display.print_error(f"启动命令失败: {e}")
                return None

    return None


def _delete_terminal(base_http, headers, term_name):
    """删掉本次 exec 自建的终端。

    不删的话每跑一次 exec 就在开发机上留一个终端，久了会攒一堆（实测已经见过
    单机 4 个残留）。命令本身用 setsid 摘出去了，删终端不会把它带走。
    删除失败不影响主流程 —— 命令已经在跑了。
    """
    if not term_name:
        return
    import requests as _requests

    try:
        _requests.delete(
            f"{base_http}/api/terminals/{term_name}", headers=headers, timeout=10
        )
    except _requests.RequestException as exc:
        swallowed("exec/关闭 terminal", exc)


def _exec_poll(jupyter_info, job_id, display, timeout=120, cleanup_on_done=True):
    """轮询某个 job_id 的输出/退出码。

    Returns: (exit_code, output, finished)。finished=False 表示 timeout 内命令仍在
    运行——此时不清理输出文件，可再次 attach 继续拉取。
    """
    import time as _time

    import requests as _requests

    base_http = jupyter_info["base_url"]
    token = jupyter_info["token"]
    headers = {"authorization": f"token {token}", "content-type": "application/json"}
    # 路径由 job_id 反推（新格式带 session 段 → <session>/ 子目录；
    # 老格式回落到平铺，保证升级前发出的 job_id 仍能 attach）
    api_out, api_exit = _exec_paths(job_id)

    def cleanup():
        # Contents API 删除会同时删掉 /tmp 里的文件（因为 symlink）
        for fname in [api_out, api_exit]:
            try:
                _requests.delete(
                    f"{base_http}/api/contents/{fname}", headers=headers, timeout=5
                )
            except _requests.RequestException as exc:
                swallowed("exec/清理临时文件", exc)

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

                if cleanup_on_done:
                    cleanup()
                return exit_code, output, True
        except (_requests.RequestException, ValueError) as exc:
            # 单轮失败不该中断轮询（命令可能还没写出 exit 文件）。但**必须留痕**：
            # 这里以前是 except Exception: pass，结果 403 / 坏 JSON 连吞 120 秒，
            # 最后只报一句「超时」，跟真实原因毫无关系。ValueError 覆盖 json 解析失败。
            swallowed("exec/轮询", exc)

        poll_interval = min(poll_interval * 1.5, 5)

    # 超时：命令仍在远端运行，保留输出文件以便 exec-attach 续读
    return 124, output, False


def _exec_via_jupyter(jupyter_info, cmd_str, display, timeout=120):
    """启动并轮询命令直到完成或超时。Returns: (exit_code, output_str)。"""
    job_id = _exec_launch(jupyter_info, cmd_str, display)
    if job_id is None:
        return 1, ""

    exit_code, output, finished = _exec_poll(
        jupyter_info, job_id, display, timeout=timeout
    )
    if not finished:
        why = last_reason("exec/")
        hint = f"\n  轮询期间最后一次失败: {why}" if why else ""
        display.print_warning(
            f"命令执行超时（{timeout}s），远端命令仍在运行，输出可能不完整。{hint}"
            f"\n  继续拉取剩余输出: qzcli exec-attach {jupyter_info['notebook_id']} {job_id}"
        )
    return exit_code, output


def _exec_list(jupyter_info, display, show_all=False):
    """列出开发机上 qzcli 留下的 exec 任务。

    默认只列**本 session** 的 —— 多个 agent 共用一台开发机时，别人的任务不该
    出现在你的列表里。``--all`` 看全部（含老格式的平铺文件）。
    """
    import requests as _requests

    base_http = jupyter_info["base_url"]
    headers = {"authorization": f"token {jupyter_info['token']}"}
    mine = get_session_id()

    def ls(path):
        try:
            r = _requests.get(
                f"{base_http}/api/contents/{path}", headers=headers, timeout=15
            )
            if r.status_code != 200:
                return []
            return r.json().get("content") or []
        except Exception:
            return []

    rows = []  # (session, job_id, 是否完成, 修改时间)
    for entry in ls("_qzcli"):
        if entry.get("type") == "directory":
            session = entry.get("name", "")
            if not show_all and session != mine:
                continue
            listing = ls(f"_qzcli/{session}")
        else:
            # 老格式：文件直接平铺在 _qzcli/ 下，没有 session 归属
            session, listing = "", [entry]
            if not show_all:
                continue

        names = {c.get("name", "") for c in listing}
        for c in listing:
            name = c.get("name", "")
            if not name.endswith("_out"):
                continue
            job_id = name[: -len("_out")]
            rows.append(
                (
                    session or "(老格式)",
                    job_id,
                    f"{job_id}_exit" in names,
                    c.get("last_modified", ""),
                )
            )

    if not rows:
        scope = "全部" if show_all else f"session {mine}"
        display.print(f"没有找到 exec 任务（范围：{scope}）")
        return 0

    rows.sort(key=lambda r: r[3], reverse=True)
    display.print(f"[bold]{'SESSION':<14} {'状态':<6} {'JOB ID':<46} 最后更新[/bold]")
    for session, job_id, done, mtime in rows:
        state = "已完成" if done else "运行中"
        display.print(f"{session:<14} {state:<6} {job_id:<46} {mtime}")
    display.print("")
    display.print("[dim]拉取输出: qzcli exec-attach <开发机> <JOB ID>[/dim]")
    return 0


def cmd_exec(args):
    """在开发机上执行命令（通过 Jupyter terminal API）"""
    display = get_display()
    target = args.host
    cmd_parts = args.remote_cmd
    timeout = getattr(args, "timeout", 120)
    detach = getattr(args, "detach", False)

    if getattr(args, "list_jobs", False):
        jupyter_info = _find_notebook_jupyter_info(target, display)
        if jupyter_info is None:
            return 1
        return _exec_list(jupyter_info, display, show_all=getattr(args, "all", False))

    if not cmd_parts:
        display.print_error("请指定要执行的命令")
        display.print(
            "[dim]用法: qzcli exec [--timeout SEC] [--detach] <name|UUID|URL> <command>[/dim]"
        )
        display.print("[dim]示例: qzcli exec blender-rl nvidia-smi[/dim]")
        display.print("[dim]      qzcli exec cfe43e55-... nvidia-smi[/dim]")
        display.print(
            "[dim]      qzcli exec 'https://qz.sii.edu.cn/ide?notebook_id=cfe43e55-...' nvidia-smi[/dim]"
        )
        return 1

    cmd_str = " ".join(cmd_parts)

    # 查找 Jupyter 连接信息（target 可以是 name / notebook_id / URL）
    jupyter_info = _find_notebook_jupyter_info(target, display)
    if jupyter_info is None:
        return 1

    if detach:
        job_id = _exec_launch(jupyter_info, cmd_str, display)
        if job_id is None:
            return 1
        display.print_success(f"已后台启动: {job_id}")
        display.print(
            f"[dim]拉取输出: qzcli exec-attach {jupyter_info['notebook_id']} {job_id}[/dim]"
        )
        # job_id 单独打到 stdout，方便脚本/agent 直接捕获
        print(job_id)
        return 0

    display.print(f"[dim]执行: {cmd_str}[/dim]")

    exit_code, output = _exec_via_jupyter(
        jupyter_info, cmd_str, display, timeout=timeout
    )
    if output:
        print(output)
    return exit_code


def cmd_exec_attach(args):
    """重新连上一个 `exec --detach` 启动的命令，继续轮询其输出/退出码。"""
    display = get_display()
    timeout = getattr(args, "timeout", 120)

    jupyter_info = _find_notebook_jupyter_info(args.host, display)
    if jupyter_info is None:
        return 1

    display.print(f"[dim]attach: {args.job_id}[/dim]")
    exit_code, output, finished = _exec_poll(
        jupyter_info, args.job_id, display, timeout=timeout
    )
    if output:
        print(output)
    if not finished:
        display.print_warning(
            f"命令仍在运行（{timeout}s 内未结束）。"
            f"可再次执行同样的 exec-attach 继续等待，或调大 --timeout。"
        )
    return exit_code


def cmd_login(args):
    """通过 CAS 登录获取 cookie"""
    import getpass

    display = get_display()
    api = get_api()
    stored_username, stored_password = get_credentials()

    # fallback 顺序: CLI 参数 → 环境变量 QZCLI_USERNAME/QZCLI_PASSWORD → config.json → 交互式输入
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
        # 拿跨进程锁再登。v0.4.1 的锁只保护了自动重登（`_relogin`），
        # **显式 `qzcli login` 没拿** —— 多个 agent 同时敲 login 仍会并发撞 CAS，
        # 被判为异常登录要求验证码，然后所有人一起被锁在外面。
        # 拿到锁后如果别的进程刚登好（cookie 变了），直接用它的结果。
        before = (get_cookie() or {}).get("cookie")
        with _relogin_file_lock():
            fresh = (get_cookie() or {}).get("cookie")
            if fresh and fresh != before:
                display.print("[dim]另一个进程刚刚登录过，直接复用其 cookie[/dim]")
                cookie = fresh
            else:
                cookie = api.login_with_cas(username, password)

        # 保存 cookie
        save_cookie(cookie, workspace_id=args.workspace)
        # 登录成功就清掉失败冷却，与 api._relogin 的收尾一致。少了这一步，手工
        # `qzcli login` 成功之后的 60s 内，自动重登仍会被上一次失败的冷却挡着。
        _clear_relogin_failure()

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

    # logs 命令 (v2 GetJobLog)
    logs_parser = subparsers.add_parser(
        "logs", help="查看任务日志（v2 接口，直连 pod）"
    )
    logs_parser.add_argument("job_id", help="任务 ID")
    logs_parser.add_argument(
        "--tail", "-n", type=int, default=200, help="最近 N 条(默认 200)"
    )
    logs_parser.add_argument(
        "--follow", "-f", action="store_true", help="持续轮询新日志(类似 tail -f)"
    )
    logs_parser.add_argument(
        "--interval", type=float, default=3.0, help="--follow 轮询间隔秒(默认 3)"
    )
    logs_parser.add_argument(
        "--pod", help="只看指定 pod(默认所有 instance: <job-id>-worker-0..N)"
    )
    logs_parser.add_argument(
        "--since", help="只取此时间后日志: ISO 时间或相对值如 5m/1h/30s/1d"
    )
    logs_parser.add_argument(
        "--raw", action="store_true", help="只打 message,不带时间/pod 前缀"
    )
    logs_parser.add_argument(
        "--json", dest="output_json", action="store_true", help="原始 JSON 输出"
    )

    # events 命令（调度/抢占诊断）
    events_parser = subparsers.add_parser(
        "events", aliases=["ev"], help="查看任务平台事件（排队/调度/抢占诊断）"
    )
    events_parser.add_argument(
        "job_id",
        nargs="?",
        help="任务 ID / 开发机（名字、notebook_id 或 URL）。用 --node 时可省略",
    )
    events_parser.add_argument(
        "--node",
        action="append",
        metavar="节点名",
        help=(
            "查**机器自己**的健康事件（可重复）。这是平台上唯一按节点组织的事件源，"
            "回答的是 `events <job>` 答不了的那个问题：**是不是这台机器本身有病**。"
            "撞上「同一台机器反复失败」时用它；qzcli 早就有 --exclude-node，"
            "但此前没有任何东西告诉你该排除谁"
        ),
    )
    events_parser.add_argument(
        "--reason", help="按 reason 子串过滤（大小写不敏感，如 Unschedulable）"
    )
    events_parser.add_argument(
        "--type", choices=["Normal", "Warning"], help="按事件类型过滤"
    )
    events_parser.add_argument("--tail", "-n", type=int, help="只看末 N 条（过滤后）")
    events_parser.add_argument(
        "--all-instances",
        dest="all_instances",
        action="store_true",
        help="追加 Pod 级事件（FailedScheduling/Evict/Preempted，更细）",
    )
    events_parser.add_argument(
        "--json", dest="output_json", action="store_true", help="原始 JSON 输出"
    )

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
    exec_parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="命令超时秒数（默认 120）。超时只切回本地，远端命令继续跑。"
        " 注意：因 remote_cmd 用 REMAINDER 吸收，--timeout/--detach 必须放在 host 之前。",
    )
    exec_parser.add_argument(
        "--detach",
        "--no-wait",
        action="store_true",
        help="后台启动后立即返回 job_id，不等待结果；之后用 `qzcli exec-attach` 拉取输出。",
    )
    exec_parser.add_argument(
        "--list",
        dest="list_jobs",
        action="store_true",
        help="列出该开发机上的 exec 任务（默认只列本 session），不执行命令。"
        " 用于找回 `--detach` 之后忘记记录的 job_id。",
    )
    exec_parser.add_argument(
        "--all",
        action="store_true",
        help="配合 --list：列出所有 session 的任务，不只是本 session。",
    )
    exec_parser.add_argument(
        "host",
        metavar="target",
        help="开发机标识：name (如 blender-rl) / notebook_id (UUID 或前缀) / "
        "完整 URL (/ide?notebook_id=..., /jobs/interactiveModel(ing)?Detail/..., /jupyter/...)",
    )
    exec_parser.add_argument(
        "remote_cmd", nargs=argparse.REMAINDER, help="要执行的命令"
    )

    # exec-attach 命令：重连一个 `exec --detach` 启动的命令
    exec_attach_parser = subparsers.add_parser(
        "exec-attach", help="重连 `exec --detach` 启动的命令，继续拉取输出"
    )
    exec_attach_parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="本次轮询的最长等待秒数（默认 120）。未结束可再次 attach。",
    )
    exec_attach_parser.add_argument(
        "host",
        metavar="target",
        help="开发机标识：name / notebook_id (UUID 或前缀) / 完整 URL（同 exec）",
    )
    exec_attach_parser.add_argument(
        "job_id",
        help="exec --detach 返回的 job_id（如 qzcli_a1b2c3d4_1785400000_deadbeef）。"
        "忘了可以用 `qzcli exec --list <开发机>` 查",
    )

    # workspace 命令
    workspace_parser = subparsers.add_parser(
        "workspace", aliases=["ws"], help="查看工作空间任务概览"
    )
    workspace_parser.add_argument("--workspace", "-w", help="工作空间 ID")
    # 以下选项已弃用（上游 API 不再支持），保留以兼容现有脚本
    workspace_parser.add_argument(
        "--project", "-p", default=None, help=argparse.SUPPRESS
    )
    workspace_parser.add_argument(
        "--all", "-a", action="store_true", help=argparse.SUPPRESS
    )
    workspace_parser.add_argument("--page", type=int, default=1, help=argparse.SUPPRESS)
    workspace_parser.add_argument(
        "--size", type=int, default=100, help=argparse.SUPPRESS
    )
    workspace_parser.add_argument(
        "--sync", "-s", action="store_true", help=argparse.SUPPRESS
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
    workspaces_parser.add_argument(
        "--full",
        "-F",
        action="store_true",
        help="完整刷新：扫描全部历史任务以反推 specs。在分布式训练空间等大型共享空间"
        "上可能耗时数十分钟。仅在需要更新 specs 缓存时使用。",
    )
    workspaces_parser.add_argument(
        "--quick",
        "-q",
        action="store_true",
        help="（已是默认行为，保留 flag 仅做向后兼容；显式传 --full 走完整扫描。）",
    )
    workspaces_parser.add_argument(
        "--parallel",
        type=int,
        default=8,
        metavar="N",
        help="并行刷新的 workspace 数量（默认 8）。设为 1 可恢复串行行为。",
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

    # dashboard 命令 - 成分下钻可视化看板
    dashboard_parser = subparsers.add_parser(
        "dashboard",
        help="启动 GPU 使用成分下钻可视化看板（treemap，需 qzcli[dashboard]）",
    )
    dashboard_parser.add_argument(
        "--workspace", "-w", default="分布式", help="工作空间 ID 或名称（默认：分布式）"
    )
    dashboard_parser.add_argument(
        "--port", type=int, default=8520, help="Streamlit 端口（默认 8520）"
    )
    dashboard_parser.add_argument(
        "--no-browser", action="store_true", help="不自动打开浏览器（headless）"
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
        help=f"任务优先级 1-10，**数字越小越低优**（默认 {DEFAULT_CREATE_PRIORITY}=LOW；10 是最高优，会和生产任务抢卡）",
    )
    create_parser.add_argument(
        "--force-priority",
        action="store_true",
        help=(
            "跳过「这个规格上次拒过这个优先级」的本地拦截。平台**逐个规格行**限制"
            "可用优先级，且允许范围没有任何接口能读，所以 qzcli 只能记住真实被拒过的"
            "组合。确认平台已放开时用这个绕过"
        ),
    )
    create_parser.add_argument(
        "--framework", help=f"框架类型（默认 {DEFAULT_CREATE_FRAMEWORK}）"
    )
    create_parser.add_argument(
        "--dataset",
        "-d",
        action="append",
        metavar="ID:VERSION",
        help="挂载公共数据集，格式 dataset_id:version_id（可多次指定），如 --dataset open-p2p-full:v1 --dataset videogamebunny:v1",
    )
    create_parser.add_argument(
        "--exclude-node",
        dest="exclude_node",
        action="append",
        metavar="NODE",
        help="排除某个 Ready 节点不参与本作业调度（可多次指定，非节点锁定）。"
        "配合碎卡治理：把碎卡节点排掉，逼平台把作业排到整节点。"
        "如 --exclude-node qb-prod-gpu105 --exclude-node qb-prod-gpu418。"
        "注：需 workspace 启用该能力，未启用时平台报 exclude_nodes not enable。",
    )
    create_parser.add_argument(
        "--include-node",
        dest="include_node",
        action="append",
        metavar="NODE",
        help="把作业锁定到指定节点（node pinning，可多次指定 → 平台 specified_nodes）。"
        "与 --exclude-node 相对。注：需 workspace 启用该能力，未启用时平台报 "
        "specified_nodes not enable。",
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
    worker_parser = subparsers.add_parser(
        "worker", help="操作分布式训练任务的 worker 容器（exec / diag）"
    )
    worker_sub = worker_parser.add_subparsers(dest="worker_action")

    _w_exec = worker_sub.add_parser("exec", help="在 worker 容器里执行命令")
    _w_exec.add_argument("job_id", help="训练任务 ID（job-xxxx）")
    # dest 不能叫 command —— 顶层 subparsers 已经用 args.command 存子命令名，
    # 重名会让分发表拿到一个 list 而不是字符串（TypeError: unhashable type: list）。
    _w_exec.add_argument("cmd_args", nargs="*", metavar="COMMAND", help="要执行的命令")
    _w_exec.add_argument("--instance", help="实例名；缺省用 <job_id>-worker-<index>")
    _w_exec.add_argument("--index", type=int, default=0, help="worker 序号（默认 0）")

    _w_diag = worker_sub.add_parser("diag", help="worker 通信体检（GPU/RDMA/TCP/NCCL）")
    _w_diag.add_argument("job_id", help="训练任务 ID（job-xxxx）")
    _w_diag.add_argument("--instance", help="实例名；缺省用 <job_id>-worker-<index>")
    _w_diag.add_argument("--index", type=int, default=0, help="worker 序号（默认 0）")

    devbox_parser = subparsers.add_parser(
        "devbox", help="把开发机上易失的 dotfile / agent home 挪到持久盘"
    )
    devbox_sub = devbox_parser.add_subparsers(dest="devbox_action")

    # target 与 `qzcli exec <target>` 是**同一套契约**（名字 / notebook_id / URL），
    # 复用 _extract_notebook_id。不传 target = 操作本机。
    _db_status = devbox_sub.add_parser("status", help="查看哪些路径已持久化（只读）")
    _db_status.add_argument(
        "target", nargs="?", help="开发机：名称 / notebook_id / URL；不传则查本机"
    )

    _db_init = devbox_sub.add_parser("init", help="持久化（可重复跑，重启后再跑会合并）")
    _db_init.add_argument(
        "target", nargs="?", help="开发机：名称 / notebook_id / URL；不传则操作本机"
    )
    _db_init.add_argument("--dry-run", action="store_true", help="只打印要做什么")
    _db_init.add_argument("--only", help="只处理这些项，逗号分隔（如 claude,zsh_history）")
    _db_init.add_argument("--target-dir", help="持久盘目录；不传则自动探测")
    _db_init.add_argument(
        "--include-ssh",
        action="store_true",
        help="连 .ssh 一起托管（默认不托管：个人持久目录同组可读）",
    )

    ops_parser = subparsers.add_parser(
        "ops", help="查看操作日志（提交/停止/登录/远程执行等有副作用的操作）"
    )
    ops_parser.add_argument("--op", help="只看某一类操作，如 create / login")
    ops_parser.add_argument("--since", type=float, help="只看最近 N 小时")
    ops_parser.add_argument(
        "--merge",
        action="append",
        default=[],
        help="额外并入其它 home 的日志（可多次给）。三份 home 各写各的，"
        "查锁号这类问题要合起来看时间线",
    )

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
    hpc_parser.add_argument(
        "--priority",
        type=int,
        default=1,
        help=(
            "优先级 1-10，**数字越大越高**（1-4→LOW，5-10→HIGH）。"
            "注意与训练任务的 --priority 方向相反（那边 10 是低优）。"
            "默认 1=LOW，与现有生产 HPC 任务一致，不抢资源"
        ),
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
        "logs": cmd_logs,
        "events": cmd_events,
        "ev": cmd_events,
        "watch": cmd_watch,
        "w": cmd_watch,
        "track": cmd_track,
        "import": cmd_import,
        "remove": cmd_remove,
        "rm": cmd_remove,
        "clear": cmd_clear,
        "cookie": cmd_cookie,
        "exec": cmd_exec,
        "exec-attach": cmd_exec_attach,
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
        "dashboard": cmd_dashboard,
        "create": cmd_create,
        "create-job": cmd_create,
        "devbox": cmd_devbox,
        "ops": cmd_ops,
        "worker": cmd_worker,
        "hpc": cmd_hpc,
        "hpc-usage": cmd_hpc_usage,
        "batch": cmd_batch,
    }

    cmd_func = commands.get(args.command)
    if cmd_func:
        # 操作日志挂在**分发点**而不是逐个 cmd_* 函数里：单一插入点，新命令只要
        # 进 opslog.RECORDED_OPS 就自动被覆盖，不会因为改了七八处漏掉一处。
        # 只读命令不在那张表里，record() 会直接忽略。
        op = _opslog_name(args)
        try:
            if op:
                from . import opslog

                with opslog.timed(op, target=_opslog_target(args)):
                    return cmd_func(args)
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
