#!/usr/bin/env python3
"""扫描官方 `qz` CLI 的接口定义，生成结构化 JSON + 飞书 markdown 接口文档。

三个只读数据源，全部来自本机的 `qz` 二进制（spec 内嵌在二进制里，**不需要认证**）：

  * ``qz spec``                      → service / action 清单与描述
  * ``qz schema <svc>.<Action>``     → 参数 JSON Schema
  * ``qz <svc> <Action> --dry-run``  → 真实 v2 URL

产出：

  * ``docs/api_spec_v2.json`` —— 结构化中间产物，平台改接口后可直接 diff
  * ``docs/api_spec_v2.md``   —— 飞书 docx 用 markdown（表格是 ``<lark-table>``，GFM 表格在飞书里渲染为空）

用法::

    python3 tools/gen_api_spec_doc.py                    # 全量
    python3 tools/gen_api_spec_doc.py --service train    # 只扫某个 service
    python3 tools/gen_api_spec_doc.py --skip-dry-run     # 跳过逐 action 的 --dry-run（快 3 倍）
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = REPO_ROOT / "docs"

# `qz spec` 的缩进结构：
#   "  qz train                  # Distributed training jobs"
#   "    CreateJob                 Create and submit a new training job ..."
SERVICE_RE = re.compile(r"^  qz (\S+)\s*(?:#\s*(.*))?$")
ACTION_RE = re.compile(r"^    ([A-Z]\S*)\s+(.*)$")
DRY_RUN_RE = re.compile(r"^DRY RUN:\s+(\S+)\s+(\S+)")
# `qz <svc> <Action> --help` 的 Flags 段：'      --page-size string   说明'
FLAG_RE = re.compile(r"^\s+(?:-\w, )?(--[a-z0-9-]+)\s+(\S*)\s*(.*)$")
# 这些是所有 action 共享的框架 flag，不是接口参数
GLOBAL_FLAGS = {
    "--data",
    "--set",
    "--help",
    "--dry-run",
    "--output",
    "--server",
    "--token",
}


def run(cmd: List[str], timeout: int = 30, merge_stderr: bool = False) -> str:
    """跑一条只读 qz 命令，返回输出（失败时返回空串，不抛）。

    ``merge_stderr``：``qz ... --help`` 走的是 cobra 的 usage 输出，**写 stderr 不写 stdout**，
    抓 flag 列表时必须合流。``spec`` / ``schema`` / ``--dry-run`` 都在 stdout。
    """
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, check=False
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        print(f"  ! {' '.join(cmd)}: {exc}", file=sys.stderr)
        return ""
    return proc.stdout + proc.stderr if merge_stderr else proc.stdout


def parse_spec(text: str) -> Dict[str, Dict[str, Any]]:
    """把 `qz spec` 的缩进输出解析成 {service: {desc, actions: {name: desc}}}。"""
    services: Dict[str, Dict[str, Any]] = {}
    current: Optional[str] = None
    for line in text.splitlines():
        m = SERVICE_RE.match(line)
        if m:
            current = m.group(1)
            services[current] = {
                "description": (m.group(2) or "").strip(),
                "actions": {},
            }
            continue
        m = ACTION_RE.match(line)
        if m and current:
            services[current]["actions"][m.group(1)] = m.group(2).strip()
    return services


def fetch_schema(service: str, action: str) -> Optional[Dict[str, Any]]:
    out = run(["qz", "schema", f"{service}.{action}"])
    if not out.strip():
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


_PLACEHOLDER = {
    "string": "PLACEHOLDER",
    "int32": 1,
    "int64": 1,
    "float64": 1.0,
    "bool": False,
    "array": [],
    "object": {},
}


def placeholder_body(params: List[Dict[str, Any]]) -> Dict[str, Any]:
    """按 schema 的必填字段拼一个最小 body。

    ``--dry-run`` 在必填字段缺失时**直接报错、不打印 URL**，所以要先把必填项填上
    占位值，才能拿到真实请求行。这些值只进 ``--dry-run``，永远不会发出去。
    """
    body: Dict[str, Any] = {}
    for p in params or []:
        if not p.get("required"):
            continue
        field = p.get("jsonField") or p.get("name")
        if not field:
            continue
        body[field] = _PLACEHOLDER.get(p.get("type", "string"), "PLACEHOLDER")
    return body


def fetch_dry_run(
    service: str, action: str, params: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    cmd = ["qz", service, action, "--dry-run"]
    body = placeholder_body(params)
    if body:
        cmd += ["--data", json.dumps(body)]
    out = run(cmd)
    for line in out.splitlines():
        m = DRY_RUN_RE.match(line.strip())
        if m:
            return {
                "method": m.group(1),
                "url": m.group(2),
                "used_placeholder_body": bool(body),
            }
    return None


def fetch_flags(service: str, action: str) -> List[Dict[str, str]]:
    """从 `--help` 抓该 action 暴露的 CLI flag（去掉全局 flag）。"""
    out = run(["qz", service, action, "--help"], merge_stderr=True)
    flags: List[Dict[str, str]] = []
    in_flags = False
    for line in out.splitlines():
        stripped = line.strip()
        if stripped == "Flags:":
            in_flags = True
            continue
        if stripped.startswith("Global Flags:"):
            break
        if not in_flags:
            continue
        m = FLAG_RE.match(line)
        if m and m.group(1) not in GLOBAL_FLAGS:
            flags.append(
                {"flag": m.group(1), "type": m.group(2), "help": m.group(3).strip()}
            )
    return flags


# --------------------------------------------------------------------------
# 参数摊平：schema 里的 parameters 是递归的（properties / items），
# 摊成 "a.b[].c" 这种点路径，文档里一张表就能读完。
# --------------------------------------------------------------------------
def flatten_params(
    params: List[Dict[str, Any]], prefix: str = "", depth: int = 0
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if depth > 4:  # 防御性：真实 schema 最深 3 层
        return rows
    for p in params or []:
        field = p.get("jsonField") or p.get("name") or ""
        ptype = p.get("type", "")
        path = f"{prefix}{field}"
        if ptype == "array":
            path += "[]"
        rows.append(
            {
                "path": path,
                "name": p.get("name", ""),
                "type": ptype,
                "required": bool(p.get("required")),
            }
        )
        # object 直接有 properties；array 的元素结构在 items 里
        children = p.get("properties")
        items = p.get("items")
        if not children and isinstance(items, dict):
            children = items.get("properties")
        if children:
            rows.extend(flatten_params(children, prefix=f"{path}.", depth=depth + 1))
    return rows


def collect(services: Dict[str, Dict[str, Any]], skip_dry_run: bool) -> Dict[str, Any]:
    """并行拉取每个 action 的 schema + dry-run。"""
    jobs = [(svc, act) for svc, info in services.items() for act in info["actions"]]
    print(f"→ {len(services)} services / {len(jobs)} actions", file=sys.stderr)

    def one(pair):
        svc, act = pair
        schema = fetch_schema(svc, act)
        params = (schema or {}).get("parameters", [])
        dry = None if skip_dry_run else fetch_dry_run(svc, act, params)
        flags = fetch_flags(svc, act)
        return svc, act, schema, params, dry, flags

    # qz 是本地二进制，并发拉满没意义也没风险；8 路够快
    with ThreadPoolExecutor(max_workers=8) as pool:
        for svc, act, schema, params, dry, flags in pool.map(one, jobs):
            entry = services[svc].setdefault("action_details", {})
            entry[act] = {
                "description": services[svc]["actions"][act],
                "url": (dry or {}).get("url"),
                "method": (dry or {}).get("method"),
                "schema_ok": schema is not None,
                "cli_flags": flags,
                "parameters": params,
                "parameters_flat": flatten_params(params),
            }
    return services


# --------------------------------------------------------------------------
# 飞书 markdown 渲染
# --------------------------------------------------------------------------
def lark_table(headers: List[str], rows: List[List[str]]) -> str:
    """飞书 docx 的表格必须用 <lark-table>；单元格文字前后要留空行。"""
    out = ["<lark-table>", "<lark-tr>"]
    for h in headers:
        out.append(f"<lark-td>\n\n{h}\n\n</lark-td>")
    out.append("</lark-tr>")
    for row in rows:
        out.append("<lark-tr>")
        for cell in row:
            text = str(cell) if cell not in (None, "") else "—"
            out.append(f"<lark-td>\n\n{text}\n\n</lark-td>")
        out.append("</lark-tr>")
    out.append("</lark-table>")
    return "\n".join(out)


def render_service(svc: str, info: Dict[str, Any], qz_version: str) -> str:
    """单个 service 的详细文档（拆分模式下的子文档）。"""
    details = info.get("action_details", {})
    md = [
        '<callout emoji="🔧" background-color="light-blue">\n\n'
        f"`qz {svc}` — {info.get('description', '')}\n\n"
        f"共 **{len(details)} 个 action**，全部走 `POST {{server}}/api/v2/{svc}?Action={{Action}}`。\n\n"
        f"来自 `qz` spec `{qz_version}`，由 `tools/gen_api_spec_doc.py` 生成。\n\n"
        "</callout>"
    ]
    md.append(
        lark_table(
            ["Action", "说明", "必填参数"],
            [
                [
                    f"`{a}`",
                    d["description"],
                    ", ".join(
                        f"`{r['path']}`" for r in d["parameters_flat"] if r["required"]
                    )
                    or "—",
                ]
                for a, d in sorted(details.items())
            ],
        )
    )
    md.extend(_render_action_sections(svc, details))
    return "\n\n".join(md) + "\n"


def _render_action_sections(
    svc: str, details: Dict[str, Any], heading: str = "###"
) -> List[str]:
    md: List[str] = []
    for act, d in sorted(details.items()):
        md.append(f"{heading} {svc}.{act}")
        md.append(d["description"])
        url = d.get("url") or f"POST {{server}}/api/v2/{svc}?Action={act}"
        method = d.get("method") or "POST"
        lines = [f"{method} {url}" if d.get("url") else url]
        flags = d.get("cli_flags") or []
        if flags:
            lines.append("")
            lines.append(
                f"# qz {svc} {act} "
                + " ".join(f"{f['flag']} <{f['type'] or 'val'}>" for f in flags)
            )
        md.append("```\n" + "\n".join(lines) + "\n```")

        flat = d.get("parameters_flat") or []
        if not flat:
            md.append("无请求参数。")
            continue
        required = [r for r in flat if r["required"]]
        md.append(
            f"参数 {len(flat)} 项，必填 {len(required)} 项"
            + (
                f"：`{'` `'.join(r['path'] for r in required)}`"
                if required
                else "（全部可选）"
            )
        )
        md.append(
            lark_table(
                ["字段", "类型", "必填", "Go 字段名"],
                [
                    [
                        f"`{r['path']}`",
                        r["type"],
                        "✅" if r["required"] else "",
                        r["name"],
                    ]
                    for r in flat
                ],
            )
        )
        nested = [r for r in flat if "." in r["path"]]
        if nested:
            md.append(
                f"> 嵌套字段 {len(nested)} 项走 `--data '<json>'` 或 "
                f"`--set {nested[0]['path']}=<值>` 传（CLI flag 只覆盖顶层标量）。"
            )
    return md


def render_markdown(
    data: Dict[str, Any],
    qz_version: str,
    generated_by: str,
    index_only: bool = False,
) -> str:
    md: List[str] = []
    total_actions = sum(len(i.get("action_details", {})) for i in data.values())
    empty = [s for s, i in data.items() if not i.get("action_details")]

    md.append(
        '<callout emoji="📖" background-color="light-blue">\n\n'
        f"启智平台 v2 接口全量清单，共 **{len(data)} 个 service / {total_actions} 个 action**。\n\n"
        f"数据源：本机 `qz` 二进制 spec `{qz_version}`（`qz spec` + `qz schema` + `--dry-run`，全部只读）。\n\n"
        f"生成命令：`{generated_by}`，结构化产物见同目录 `api_spec_v2.json`（平台改接口后可直接 diff）。\n\n"
        "</callout>"
    )

    md.append("## 调用约定")
    md.append(
        "所有 v2 接口是**同一个形状**：`POST {server}/api/v2/{service}?Action={Action}`，"
        "参数全部放 JSON body，没有 path 参数，没有 GET。"
    )
    md.append(
        "认证有两条路，**接口相同、认证不同**：\n"
        "- **Bearer**（官方 `qz` 用）：Keycloak device code 登录后拿 access token，放 `Authorization: Bearer <token>`\n"
        "- **CAS cookie**（`qzcli` 用）：`inspire-session` cookie + `x-inspire-client-source` 头。"
        "**缺 `x-inspire-client-source` 时 APISIX 网关会 302 到 Keycloak 并返回 HTML**，即使 token 合法"
    )
    md.append(
        '响应信封：成功是 `{"Result": {...}}`，失败是 `{"ResponseMetadata": {"Error": '
        '{"Code": ..., "Message": ...}}}`。部分接口仍返回旧的 `{"code": 0, "data": {...}}`，'
        "客户端两种都要认。"
    )

    md.append("## Service 总览")
    md.append(
        lark_table(
            ["Service", "Action 数", "说明"],
            [
                [
                    f"`{svc}`",
                    str(len(info.get("action_details", {}))),
                    info.get("description", ""),
                ]
                for svc, info in sorted(data.items())
            ],
        )
    )

    if empty:
        md.append(
            '<callout emoji="⚠️" background-color="yellow">\n\n'
            f"**平台侧缺口**：`{'` / `'.join(sorted(empty))}` 在 `qz spec` 里注册为 service，"
            "但**零 action**（`qz <svc> --help` 没有 `Available Commands` 块）。"
            "这些能力目前没有 v2 CLI/API 入口。\n\n"
            "</callout>"
        )

    md.append(
        '<callout emoji="⚠️" background-color="yellow">\n\n'
        "**另外两个已知缺口**（全量 grep 过所有 action schema）：\n\n"
        "1. **没有 `ListWorkspaces`** —— workspace 枚举只能从 `project.GetProjectForPage` 的 "
        "`items[].space_list[]` 推导。（上游 2026-08 放开了该 action 的普通用户权限，"
        "此前它是 `AccessForbidden`，qzcli 只能走 v1；现已可用。）\n\n"
        "2. **没有任何 action 返回 `spec_id`** —— `spec_id` 在 v2 里只作为**请求**字段存在"
        "（`train.CreateJob.framework_config[]` / `hpc.CreateJob` / `inference-serving.CreateServing`）。"
        "`workspace.GetLogicComputeGroupNodeSpecs` 返回的 `node_specs[]` 只有硬件规格没有 id；"
        "唯一能拿到 id 的路径是历史任务响应里的 `framework_config[].predef_id`。\n\n"
        "</callout>"
    )

    for svc, info in sorted(data.items()):
        details = info.get("action_details", {})
        md.append(f"## `qz {svc}` — {info.get('description', '')}")
        if not details:
            md.append("> 该 service 在当前 spec 下没有任何 action。")
            continue
        if index_only:
            # 主文档只放索引；完整参数表在各 service 的子文档里
            md.append(
                lark_table(
                    ["Action", "说明", "参数数", "必填参数"],
                    [
                        [
                            f"`{a}`",
                            d["description"],
                            str(len(d["parameters_flat"])),
                            ", ".join(
                                f"`{r['path']}`"
                                for r in d["parameters_flat"]
                                if r["required"]
                            )
                            or "—",
                        ]
                        for a, d in sorted(details.items())
                    ],
                )
            )
            continue
        md.extend(_render_action_sections(svc, details))

    return "\n\n".join(md) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--service", action="append", help="只扫指定 service（可重复）")
    ap.add_argument(
        "--skip-dry-run", action="store_true", help="跳过逐 action 的 --dry-run"
    )
    ap.add_argument(
        "--split",
        action="store_true",
        help="额外产出拆分版：主文档只放索引 + 每个 service 一份子文档（飞书单篇装不下 144 个 action 时用）",
    )
    args = ap.parse_args()

    spec_text = run(["qz", "spec"], timeout=60)
    if not spec_text.strip():
        print("✗ `qz spec` 无输出 —— 确认 qz 在 PATH 里", file=sys.stderr)
        return 1

    version = "unknown"
    m = re.search(r"^Version:\s+(\S+)", spec_text, re.M)
    if m:
        version = m.group(1)

    services = parse_spec(spec_text)
    if args.service:
        services = {k: v for k, v in services.items() if k in set(args.service)}
        if not services:
            print(f"✗ 没匹配到 service: {args.service}", file=sys.stderr)
            return 1

    services = collect(services, skip_dry_run=args.skip_dry_run)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.out_dir / "api_spec_v2.json"
    md_path = args.out_dir / "api_spec_v2.md"

    json_path.write_text(
        json.dumps(
            {"qz_spec_version": version, "services": services},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    md_path.write_text(
        render_markdown(services, version, "python3 tools/gen_api_spec_doc.py"),
        encoding="utf-8",
    )

    if args.split:
        split_dir = args.out_dir / "api_spec_v2"
        split_dir.mkdir(parents=True, exist_ok=True)
        (split_dir / "00_index.md").write_text(
            render_markdown(
                services,
                version,
                "python3 tools/gen_api_spec_doc.py --split",
                index_only=True,
            ),
            encoding="utf-8",
        )
        written = 1
        for svc, info in sorted(services.items()):
            if not info.get("action_details"):
                continue
            (split_dir / f"{svc}.md").write_text(
                render_service(svc, info, version), encoding="utf-8"
            )
            written += 1
        print(f"✓ 拆分版 {written} 篇 → {split_dir}")

    total = sum(len(i.get("action_details", {})) for i in services.values())
    missing = [
        f"{s}.{a}"
        for s, i in services.items()
        for a, d in i.get("action_details", {}).items()
        if not d["schema_ok"]
    ]
    print(f"✓ {len(services)} services / {total} actions → {json_path}, {md_path}")
    if missing:
        print(
            f"! {len(missing)} 个 action 拿不到 schema: {', '.join(missing[:10])}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
