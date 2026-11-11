"""Official qz credentials and noninteractive renewal, without password fallback."""

import base64
import binascii
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .config import (
    get_proxy,
    load_qz_settings,
    read_qz_credentials,
    qz_refresh_lock,
    load_qz_refresh_failure,
    save_qz_refresh_failure,
)


class OfficialAuthError(RuntimeError):
    def __init__(self, message, *, action_required=False):
        super().__init__(message)
        self.action_required = action_required


class LoginRequired(OfficialAuthError):
    def __init__(self, message="官方登录凭据无法续期，请运行 qzcli login 完成网页授权"):
        super().__init__(message, action_required=True)


@dataclass
class Token:
    value: str = field(default="", repr=False)
    expires_at: Optional[float] = None

    @classmethod
    def parse(cls, value):
        if not isinstance(value, str):
            return cls()
        expiration = None
        try:
            part = value.split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
            exp = claims.get("exp") if isinstance(claims, dict) else None
            if (
                isinstance(exp, (int, float))
                and not isinstance(exp, bool)
                and math.isfinite(exp)
                and 0 <= exp <= 253402300799
            ):
                expiration = exp
        except (IndexError, ValueError, UnicodeDecodeError, binascii.Error):
            pass  # Opaque tokens are unknown, not expired.
        return cls(value, expiration)

    def expired(self, now):
        return self.expires_at is not None and now >= self.expires_at

    def status(self, now):
        if not self.value:
            return {"state": "missing", "expires_at": None}
        return {
            "state": (
                "unknown"
                if self.expires_at is None
                else "expired" if self.expired(now) else "unexpired"
            ),
            "expires_at": (
                datetime.fromtimestamp(self.expires_at, timezone.utc).isoformat()
                if self.expires_at is not None
                else None
            ),
        }


class OfficialAuth:
    def __init__(self, directory=None, *, now=time.time, runner=subprocess.run):
        self.settings = load_qz_settings(directory)
        self.directory = self.settings["directory"]
        self.server = str(self.settings["server"]).rstrip("/")
        self.now, self.runner = now, runner
        self._lock = threading.Lock()
        self._secrets = set()

    def _read(self):
        static = os.environ.get("INSPIRE_TOKEN")
        values = {"token": static} if static else read_qz_credentials(self.directory)
        access, refresh = Token.parse(values.get("token", "")), Token.parse(
            values.get("refresh_token", "")
        )
        self._secrets.update(t.value for t in (access, refresh) if t.value)
        return access, refresh, bool(static)

    def redact(self, text):
        from .auth_diag import AuthDiagnostic

        text = str(text)
        for secret in sorted(self._secrets, key=len, reverse=True):
            text = text.replace(secret, "[REDACTED]")
        text = re.sub(
            r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b",
            "[REDACTED JWT]",
            text,
        )
        return AuthDiagnostic().redact(text)

    def status(self):
        access, refresh, static = self._read()
        return {
            "auth_mode": "qz",
            "source": "INSPIRE_TOKEN" if static else str(self.directory),
            "server": self.server,
            "auth_url": self.settings["auth_url"],
            "qz_binary": shutil.which("qz"),
            "access_token": access.status(self.now()),
            "refresh_token": refresh.status(self.now()),
        }

    def _environment(self):
        env = os.environ.copy()
        env.pop("INSPIRE_TOKEN", None)
        env["QZ_CLI_CONFIG_DIR"] = str(self.directory)
        proxy = get_proxy(self.server) or get_proxy(str(self.settings["auth_url"]))
        if proxy:
            for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
                env[name] = proxy
        return env

    def _binary(self):
        binary = shutil.which("qz")
        if not binary:
            raise OfficialAuthError(
                "未找到官方 qz。请先安装官方 Inspire-CLI（qz），再运行 qzcli login；Python inspire 是另一工具。"
            )
        return binary

    def _probe_with_qz(self):
        command = [self._binary(), "--server", self.server, "user", "GetUserDetail"]
        try:
            result = self.runner(
                command,
                env=self._environment(),
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.TimeoutExpired as exc:
            raise OfficialAuthError(
                "官方 qz 刷新超时，结果尚未确认；凭据已保留，未重复刷新"
            ) from exc
        if result.returncode:
            output = self.redact((result.stderr or "") + (result.stdout or ""))
            if "invalid_grant" in output or "Token is not active" in output:
                raise LoginRequired(
                    f"官方 qz 认证失败（exit {result.returncode}）：\n{output}"
                )
            raise OfficialAuthError(
                f"官方 qz 请求失败（exit {result.returncode}）：\n{output}"
            )
        try:
            data = json.loads(result.stdout)
        except ValueError as exc:
            raise OfficialAuthError("官方 qz 返回成功，但身份响应不是 JSON") from exc
        user = data.get("Result", data) if isinstance(data, dict) else {}
        if not isinstance(user, dict) or not user.get("id"):
            raise OfficialAuthError("官方 qz 返回成功，但未取得当前用户身份")

    def token(self, *, force_refresh=False, failing_token=None):
        access, refresh, static = self._read()
        if access.value and not access.expired(self.now()):
            if not force_refresh or access.value != failing_token:
                return access.value
        if static:
            raise LoginRequired(
                "INSPIRE_TOKEN 已过期或被服务端拒绝；请更新该静态凭据，或移除后运行 qzcli login"
            )
        if not refresh.value or refresh.expired(self.now()):
            raise LoginRequired()
        with self._lock:
            with qz_refresh_lock(self.directory):
                access, refresh, _ = self._read()
                if access.value and not access.expired(self.now()):
                    if not force_refresh or access.value != failing_token:
                        return access.value
                if not refresh.value or refresh.expired(self.now()):
                    raise LoginRequired()
                fingerprint = hashlib.sha256(
                    (access.value + "\0" + refresh.value).encode()
                ).hexdigest()
                failure = load_qz_refresh_failure(self.directory)
                if failure.get("fingerprint") == fingerprint:
                    if (
                        failure.get("action_required")
                        or self.now() - failure.get("at", 0) < 15
                    ):
                        raise OfficialAuthError(
                            failure["message"],
                            action_required=failure.get("action_required", False),
                        )
                try:
                    self._probe_with_qz()
                    updated, _, _ = self._read()
                    if not updated.value or updated.expired(self.now()):
                        raise OfficialAuthError(
                            "官方 qz 未保存可用的新 access token，未继续业务请求"
                        )
                    if force_refresh and updated.value == failing_token:
                        raise OfficialAuthError(
                            "官方身份探针可用，但业务接口拒绝了同一 token；未重复刷新，请检查该接口的原始响应和网关头"
                        )
                except OfficialAuthError as exc:
                    # An external qz may have updated the pair while our process failed.
                    latest, _, _ = self._read()
                    if (
                        latest.value
                        and latest.value != access.value
                        and not latest.expired(self.now())
                    ):
                        return latest.value
                    save_qz_refresh_failure(
                        self.directory,
                        {
                            "fingerprint": fingerprint,
                            "at": self.now(),
                            "message": self.redact(exc),
                            "action_required": exc.action_required,
                        },
                    )
                    raise
                save_qz_refresh_failure(self.directory, {})
                return updated.value

    def login(self):
        if os.environ.get("INSPIRE_TOKEN"):
            raise OfficialAuthError(
                "INSPIRE_TOKEN 正在覆盖官方凭据，请先移除该环境变量再进行网页登录"
            )
        # Login creates a new independent authorization chain. Coordinate with
        # qzcli refreshers; an independently launched qz does not honor our lock.
        self._binary()
        self.directory.mkdir(parents=True, exist_ok=True)
        with self._lock, qz_refresh_lock(self.directory):
            # Upstream v1.0 removed --auth-url; its server owns device OAuth.
            # Probe help locally so initialization reports incompatible flags
            # before opening a login flow instead of invoking a broken command.
            help_result = self.runner(
                [self._binary(), "login", "--help"],
                env=self._environment(),
                capture_output=True,
                text=True,
                timeout=30,
            )
            if help_result.returncode:
                raise OfficialAuthError(
                    self.redact((help_result.stderr or "") + (help_result.stdout or ""))
                )
            command = [self._binary(), "--server", self.server, "login"]
            if "--auth-url" in help_result.stdout:
                command.extend(["--auth-url", str(self.settings["auth_url"])])
            result = self.runner(
                command,
                env=self._environment(),
            )
        if result.returncode:
            raise OfficialAuthError(
                f"官方 qz login 失败（exit {result.returncode}），请保留上方原始响应"
            )
        access, _, _ = self._read()
        if not access.value or access.expired(self.now()):
            raise OfficialAuthError("官方登录结束，但未读取到可用凭据")
        return self.status()
