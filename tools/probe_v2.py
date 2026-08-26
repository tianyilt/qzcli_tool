#!/usr/bin/env python3
"""探针：用 qzcli 的 CAS cookie 打 v2 只读接口，回答"哪些 Action 认 cookie"。

背景：官方 `qz` CLI 走 Keycloak Bearer；qzcli 走 CAS cookie。目前只**实测过 2 个**
v2 Action 接受 cookie（``train GetJobLog`` / ``train CreateJobConsole``），
其余 142 个未知。v1→v2 迁移能迁多少，完全取决于这个答案，所以先探针再动代码。

安全边界（硬编码，不可通过参数放宽）：
  * 只打 ``List*`` / ``Get*`` 开头的 Action
  * 任何名字里带 Create/Delete/Stop/Start/Update/Scale/Rollback/Cordon/Maint/
    Transfer/Commit/Save/Preheat/Inspect 的一律跳过
  * 单并发 + 请求间隔，别给平台压力

产出 ``docs/v2_probe_report.md`` + ``docs/v2_probe_raw.json``。

用法::

    python3 tools/probe_v2.py                       # 全量只读 Action
    python3 tools/probe_v2.py --service train hpc   # 只探指定 service
    python3 tools/probe_v2.py --list                # 只列要探的 Action，不发请求
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from qzcli import api  # noqa: E402
from qzcli.config import RESOURCES_FILE, get_cookie  # noqa: E402

SPEC_JSON = REPO_ROOT / "docs" / "api_spec_v2.json"
OUT_DIR = REPO_ROOT / "docs"

# 产物要能直接进仓（本仓是公开的，CONTRIBUTING 明确禁止提交真实 workspace UUID /
# 内部项目名）。落盘前把 `ws-<uuid>` / `project-<uuid>` / 裸 uuid 一律打码，
# 只留前 8 位便于人工对照。
_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_REDACT_RE = re.compile(rf"\b((?:ws|project|lcg|cg|job|user|nb)-)?({_UUID})\b", re.I)


def redact(obj: Any) -> Any:
    """递归给 ID 打码：``ws-1234abcd-5678-...`` → ``ws-<redacted>``。

    **十六进制一位都不留。** 第一版留了前 8 位（``ws-1234abcd-<redacted>``），
    以为「只是前缀、认不出来」—— 错了：平台 ID 的前 8 位在全平台唯一，等于没打码，
    照样能反查到是哪个空间。这份产物进的是 public 仓库，所以这里只保留**类型**
    （是 ws 还是 lcg），标识位全丢掉。
    """
    if isinstance(obj, str):
        return _REDACT_RE.sub(lambda m: f"{m.group(1) or ''}<redacted>", obj)
    if isinstance(obj, dict):
        return {k: redact(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact(v) for v in obj]
    return obj

READ_PREFIXES = ("List", "Get")
# 名字里出现任何一个就跳过 —— 比白名单前缀更保险（防 "GetXxx" 里藏副作用）
MUTATING_TOKENS = (
    "Create",
    "Delete",
    "Stop",
    "Start",
    "Update",
    "Scale",
    "Rollback",
    "Cordon",
    "Maint",
    "Transfer",
    "Commit",
    "Save",
    "Preheat",
    "Inspect",
    "Restart",
    "Remove",
    "Add",
    "Set",
    "Enable",
    "Disable",
    "Publish",
)


def is_read_only(action: str) -> bool:
    if not action.startswith(READ_PREFIXES):
        return False
    return not any(tok in action for tok in MUTATING_TOKENS)


def load_real_ids() -> Dict[str, Any]:
    """从 ``~/.qzcli/resources.json`` 取真实 ID，让探针请求尽量合法。

    用假 ID 也能区分"认证过了但参数不对"（JSON 错误）和"认证没过"（HTML），
    但真 ID 能顺带看到响应字段名，对后面写映射更有用。
    """
    ids: Dict[str, Any] = {
        "workspace_id": "",
        "project_id": "",
        "logic_compute_group_id": "",
    }
    try:
        data = json.loads(Path(RESOURCES_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return ids
    for ws in data.values():
        if not isinstance(ws, dict):
            continue
        if not ids["workspace_id"]:
            ids["workspace_id"] = ws.get("id", "")
        for pid in ws.get("projects") or {}:
            ids["project_id"] = ids["project_id"] or pid
            break
        for cg in ws.get("compute_groups") or {}:
            ids["logic_compute_group_id"] = ids["logic_compute_group_id"] or cg
            break
        if all(ids.values()):
            break
    return ids


_PLACEHOLDER = {
    "string": "probe",
    "int32": 1,
    "int64": 1,
    "float64": 1.0,
    "bool": False,
    "array": [],
    "object": {},
}


def build_body(params: List[Dict[str, Any]], real: Dict[str, Any]) -> Dict[str, Any]:
    """只填必填字段：真 ID 优先，其次按类型填占位。"""
    body: Dict[str, Any] = {}
    for p in params or []:
        if not p.get("required"):
            continue
        field = p.get("jsonField") or p.get("name")
        if not field:
            continue
        body[field] = real.get(field) or _PLACEHOLDER.get(
            p.get("type", "string"), "probe"
        )
    # 分页类接口给个小页，别拉全量
    names = {p.get("jsonField") for p in params or []}
    for key in ("page_size",):
        if key in names:
            body.setdefault(key, 1)
    return body


def classify(status: int, ctype: str, payload: Any) -> str:
    """判读探针结果。

    区分的是**认证是否通过**，不是业务成功与否 —— 参数不对但返回 JSON，
    说明 cookie 已经被接受，该 Action 可迁。
    """
    # 顺序有讲究：404 必须先判。网关对未注册路由回的是 `404 page not found`
    # （text/plain），按 content-type 判会被误当成"认证失败"。
    if status == 404:
        return "NOT_ROUTED"  # Action 没挂在 /api/v2/{service} 上
    if status == 401:
        return "AUTH_401"
    if "application/json" not in ctype:
        return "AUTH_FAIL_HTML"  # APISIX 302 到 Keycloak，cookie 不被接受
    if status == 403:
        return "FORBIDDEN"  # 认证过了，是权限不够
    if isinstance(payload, dict):
        err = (payload.get("ResponseMetadata") or {}).get("Error")
        if err:
            return f"JSON_ERR:{err.get('Code', '?')}"
        if payload.get("code") not in (None, 0):
            return f"LEGACY_ERR:{payload.get('code')}"
        if "Result" in payload:
            return "OK_RESULT"
        if "data" in payload:
            return "OK_LEGACY_DATA"
    if status >= 400:
        return f"HTTP_{status}"
    return "OK_BARE"


def envelope_shape(payload: Any) -> str:
    if not isinstance(payload, dict):
        return type(payload).__name__
    top = sorted(payload.keys())[:6]
    inner = ""
    if isinstance(payload.get("Result"), dict):
        inner = " → Result{" + ", ".join(sorted(payload["Result"].keys())[:8]) + "}"
    return "{" + ", ".join(top) + "}" + inner


def probe_one(
    cookie: str, base_url: str, service: str, action: str, body: Dict[str, Any]
) -> Dict[str, Any]:
    """发一次请求，**捕获原始响应**（不走 _request_v2，因为它会抛异常吞掉细节）。"""
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "cookie": cookie,
        "origin": base_url,
        "referer": f"{base_url}/jobs",
        "user-agent": api.V2_BROWSER_UA,
        "x-inspire-client-source": api.V2_CLIENT_SOURCE,
    }
    rec: Dict[str, Any] = {"service": service, "action": action, "request_body": body}
    try:
        resp = api._curl_post(
            f"{base_url}/api/v2/{service}",
            params={"Action": action},
            json=body,
            headers=headers,
            timeout=30,
        )
    except Exception as exc:  # 网络层失败也是有效信号
        rec.update(verdict="TRANSPORT_ERR", error=f"{type(exc).__name__}: {exc}")
        return rec

    ctype = resp.headers.get("Content-Type", "")
    payload: Any = None
    try:
        payload = resp.json()
    except ValueError:
        payload = None
    rec.update(
        status=resp.status_code,
        content_type=ctype,
        verdict=classify(resp.status_code, ctype, payload),
        envelope=envelope_shape(payload) if payload is not None else resp.text[:120],
    )
    return rec


def render_report(records: List[Dict[str, Any]], spec_version: str) -> str:
    ok = [r for r in records if str(r["verdict"]).startswith("OK_")]
    auth_fail = [r for r in records if r["verdict"] in ("AUTH_FAIL_HTML", "AUTH_401")]
    not_routed = [r for r in records if r["verdict"] == "NOT_ROUTED"]
    other = [
        r for r in records if r not in ok and r not in auth_fail and r not in not_routed
    ]
    reachable = len(ok) + len(other)

    lines = [
        "# v2 接口 cookie 认证探针报告",
        "",
        f"- qz spec 版本：`{spec_version}`",
        f"- 探测 Action 数：**{len(records)}**（只读 `List*`/`Get*`，写操作全部跳过）",
        f"- ✅ **cookie 被接受：{reachable}**（返回 JSON = 认证通过）",
        f"  - 其中直接拿到数据（`Result`）：**{len(ok)}**",
        f"  - 业务/参数/权限错，但认证已过：**{len(other)}**",
        f"- ❌ 认证失败（302 到 Keycloak / 401）：**{len(auth_fail)}**",
        f"- ⬜ 网关未注册该路由（404 `page not found`）：**{len(not_routed)}**",
        "",
        "> **判读原则**：这个探针问的是「cookie 认证过不过」，不是「业务成不成功」。",
        "> 只要返回 JSON（哪怕是 `AccessForbidden` / `InvalidParameter`），就说明 cookie 已被接受、该 Action 可迁；",
        "> 只有 302 到 Keycloak 返回 HTML、或 401，才是认证失败。",
        "> 404 `page not found` 是路由压根没挂在 `/api/v2/{service}` 上，与认证无关。",
        "",
    ]
    if not auth_fail:
        lines += [
            "**结论：cookie 在整个 v2 只读面上通用**，"
            "没有任何一个 Action 因为认证被挡。v1→v2 迁移不受认证阻塞。",
            "",
        ]
    lines += [
        "## 明细",
        "",
        "| Service | Action | 判定 | HTTP | 响应信封 / 片段 |",
        "|---|---|---|---|---|",
    ]
    for r in sorted(records, key=lambda x: (x["service"], x["action"])):
        env = str(r.get("envelope", ""))[:110].replace("|", "\\|").replace("\n", " ")
        lines.append(
            f"| `{r['service']}` | `{r['action']}` | {r['verdict']} | "
            f"{r.get('status', '—')} | `{env}` |"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--service", nargs="*", help="只探指定 service")
    ap.add_argument("--list", action="store_true", help="只列要探的 Action，不发请求")
    ap.add_argument("--sleep", type=float, default=0.3, help="请求间隔秒（默认 0.3）")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    ap.add_argument(
        "--workspace-id", help="覆盖探针用的 workspace_id（缓存里第一个未必有权限）"
    )
    ap.add_argument("--project-id", help="覆盖探针用的 project_id")
    ap.add_argument("--compute-group-id", help="覆盖探针用的 logic_compute_group_id")
    ap.add_argument(
        "--from-raw",
        action="store_true",
        help="不发请求，用已有的 v2_probe_raw.json 重新判读并重渲报告（改了判读逻辑时用）",
    )
    args = ap.parse_args()

    if args.from_raw:
        raw_path = args.out_dir / "v2_probe_raw.json"
        raw = json.loads(raw_path.read_text(encoding="utf-8"))
        for rec in raw["records"]:
            if "status" not in rec:
                continue
            body = rec.get("envelope")
            payload = body if isinstance(body, dict) else None
            # envelope 是渲染过的字符串，重判只能靠 status + content_type；
            # 这对区分 404/401/HTML 足够，JSON 业务错的原判定直接保留。
            new = classify(rec["status"], rec.get("content_type", ""), payload)
            if new in ("NOT_ROUTED", "AUTH_401", "AUTH_FAIL_HTML") or str(
                rec["verdict"]
            ).startswith(("AUTH_", "NOT_")):
                rec["verdict"] = new
        raw_path.write_text(
            json.dumps(raw, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (args.out_dir / "v2_probe_report.md").write_text(
            render_report(raw["records"], raw.get("qz_spec_version", "?")),
            encoding="utf-8",
        )
        print(
            f"✓ 已按新判读逻辑重渲 {len(raw['records'])} 条 → {args.out_dir}/v2_probe_report.md"
        )
        return 0

    if not SPEC_JSON.exists():
        print(
            f"✗ 缺 {SPEC_JSON} —— 先跑 `python3 tools/gen_api_spec_doc.py`",
            file=sys.stderr,
        )
        return 1
    spec = json.loads(SPEC_JSON.read_text(encoding="utf-8"))
    services = spec["services"]
    if args.service:
        services = {k: v for k, v in services.items() if k in set(args.service)}

    targets = [
        (svc, act, d)
        for svc, info in sorted(services.items())
        for act, d in sorted(info.get("action_details", {}).items())
        if is_read_only(act)
    ]
    skipped = sum(
        1
        for info in services.values()
        for act in info.get("action_details", {})
        if not is_read_only(act)
    )
    print(f"→ 只读 Action {len(targets)} 个，跳过写操作 {skipped} 个", file=sys.stderr)

    if args.list:
        for svc, act, _ in targets:
            print(f"{svc}.{act}")
        return 0

    cookie_data = get_cookie()
    cookie = (cookie_data or {}).get("cookie")
    if not cookie:
        print(
            "✗ 没有有效 cookie —— 先跑 `qzcli login -u <学工号> -p <密码>`",
            file=sys.stderr,
        )
        return 1

    base_url = api.get_api_base_url()
    real = load_real_ids()
    for key, override in (
        ("workspace_id", args.workspace_id),
        ("project_id", args.project_id),
        ("logic_compute_group_id", args.compute_group_id),
    ):
        if override:
            real[key] = override
    print(f"→ base={base_url} 真实 ID: {real}", file=sys.stderr)

    records: List[Dict[str, Any]] = []
    for i, (svc, act, d) in enumerate(targets, 1):
        body = build_body(d.get("parameters", []), real)
        rec = probe_one(cookie, base_url, svc, act, body)
        records.append(rec)
        print(
            f"  [{i}/{len(targets)}] {svc}.{act:38} {rec['verdict']}",
            file=sys.stderr,
        )
        time.sleep(args.sleep)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    safe = redact(records)
    (args.out_dir / "v2_probe_raw.json").write_text(
        json.dumps(
            {"qz_spec_version": spec.get("qz_spec_version"), "records": safe},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (args.out_dir / "v2_probe_report.md").write_text(
        render_report(safe, spec.get("qz_spec_version", "?")), encoding="utf-8"
    )
    ok = sum(1 for r in records if str(r["verdict"]).startswith("OK_"))
    fail = sum(1 for r in records if r["verdict"] in ("AUTH_FAIL_HTML", "AUTH_401"))
    print(
        f"✓ {len(records)} 探完：OK {ok} / 认证失败 {fail} → {args.out_dir}/v2_probe_report.md"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
