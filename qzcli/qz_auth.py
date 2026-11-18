"""Native platform OAuth with a compatible encrypted store and no password fallback."""

import base64
import binascii
import hashlib
import json
import math
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .config import (
    load_qz_settings,
    read_qz_credentials,
    save_qz_credentials,
    qz_refresh_lock,
    load_qz_refresh_failure,
    save_qz_refresh_failure,
)


class OfficialAuthError(RuntimeError):
    def __init__(self, message, *, action_required=False):
        super().__init__(message)
        self.action_required = action_required
        from .auth_diag import AuthDiagnostic

        self.diagnostic = AuthDiagnostic(stage="official OAuth decision")


class LoginRequired(OfficialAuthError):
    def __init__(self, message="官方登录凭据无法续期，请运行 qzcli login 完成网页授权"):
        super().__init__(message, action_required=True)


@dataclass
class Token:
    value: str = field(default="", repr=False)
    expires_at: Optional[float] = None
    token_type: Optional[str] = None
    client_id: Optional[str] = None

    @classmethod
    def parse(cls, value, expires_at=None):
        if not isinstance(value, str):
            return cls()
        expiration = (
            expires_at
            if (
                isinstance(expires_at, (int, float))
                and not isinstance(expires_at, bool)
                and math.isfinite(expires_at)
                and 0 < expires_at <= 253402300799
            )
            else None
        )
        token_type = None
        client_id = None
        try:
            part = value.split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
            if isinstance(claims, dict):
                typ = claims.get("typ")
                if isinstance(typ, str) and typ:
                    token_type = typ
                azp = claims.get("azp")
                if isinstance(azp, str) and azp:
                    client_id = azp
                exp = claims.get("exp")
                if exp == 0 and not isinstance(exp, bool):
                    # Keycloak Offline tokens carry exp=0 (no fixed MaxLifespan).
                    expiration = None
                elif (
                    isinstance(exp, (int, float))
                    and not isinstance(exp, bool)
                    and math.isfinite(exp)
                    and 0 < exp <= 253402300799
                ):
                    expiration = exp
        except (IndexError, ValueError, UnicodeDecodeError, binascii.Error):
            pass  # Opaque tokens are unknown, not expired.
        return cls(value, expiration, token_type=token_type, client_id=client_id)

    def expired(self, now):
        return self.expires_at is not None and now >= self.expires_at

    def status(self, now):
        if not self.value:
            return {"state": "missing", "expires_at": None}
        if self.expires_at is not None:
            state = "expired" if self.expired(now) else "unexpired"
        elif self.token_type == "Offline":
            state = "unexpired"
        else:
            state = "unknown"
        info = {
            "state": state,
            "expires_at": (
                datetime.fromtimestamp(self.expires_at, timezone.utc).isoformat()
                if self.expires_at is not None
                else None
            ),
        }
        if self.token_type is not None:
            info["token_type"] = self.token_type
        if self.client_id is not None:
            info["client_id"] = self.client_id
        return info


class OfficialAuth:
    def __init__(self, directory=None, *, now=time.time, oauth=None):
        self.settings = load_qz_settings(directory)
        self.directory = self.settings["directory"]
        self.server = str(self.settings["server"]).rstrip("/")
        self.now = now
        self._lock = threading.Lock()
        self._secrets = set()
        from .oauth import DeviceOAuth

        self.oauth = oauth or DeviceOAuth(
            self.server,
            now=now,
            remember=self._secrets.add,
            auth_url=str(self.settings["auth_url"]),
        )

    def _read(self):
        static = os.environ.get("INSPIRE_TOKEN")
        values = {"token": static} if static else read_qz_credentials(self.directory)
        access = Token.parse(values.get("token", ""), values.get("access_expires_at"))
        refresh = Token.parse(
            values.get("refresh_token", ""), values.get("refresh_expires_at")
        )
        self._secrets.update(t.value for t in (access, refresh) if t.value)
        return access, refresh, bool(static)

    def _save(self, pair):
        self._secrets.update((pair.access, pair.refresh))
        candidate = Token.parse(pair.access, pair.access_expires_at)
        if not candidate.value or candidate.expired(self.now()):
            raise OfficialAuthError("认证服务返回的 access token 已过期，原凭据未覆盖")
        values = read_qz_credentials(self.directory)
        values.update(
            token=pair.access,
            refresh_token=pair.refresh,
            access_expires_at=pair.access_expires_at,
            refresh_expires_at=pair.refresh_expires_at,
        )
        save_qz_credentials(self.directory, values)

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
            "auth_mode": "qz",  # Historical config/API name for official OAuth.
            "provider": "native-oauth",
            "source": "INSPIRE_TOKEN" if static else str(self.directory),
            "server": self.server,
            "auth_url": f"{self.server}/api/v1/public/auth",
            "qz_binary": None,  # Retained status field; no executable is required.
            "access_token": access.status(self.now()),
            "refresh_token": refresh.status(self.now()),
        }

    def _refresh_locked(
        self, access, refresh, *, force_refresh=False, failing_token=None
    ):
        fingerprint = hashlib.sha256(
            ("native-oauth-v2\0" + access.value + "\0" + refresh.value).encode()
        ).hexdigest()
        failure = load_qz_refresh_failure(self.directory)
        if failure.get("fingerprint") == fingerprint and (
            failure.get("action_required") or self.now() - failure.get("at", 0) < 15
        ):
            raise OfficialAuthError(
                failure["message"]
                + ("\n" + failure["diagnostic"] if failure.get("diagnostic") else ""),
                action_required=failure.get("action_required", False),
            )
        try:
            pair = self.oauth.refresh(refresh.value)
            current, current_refresh, _ = self._read()
            if (current.value, current_refresh.value) != (access.value, refresh.value):
                if current.value and not current.expired(self.now()):
                    return current.value
                raise OfficialAuthError("刷新期间凭据被其他进程更改，已保留该版本，未覆盖或重复刷新")
            self._save(pair)
            updated, _, _ = self._read()
            if not updated.value or updated.expired(self.now()):
                raise OfficialAuthError("认证服务未返回可用的新 access token，未继续业务请求")
            if force_refresh and updated.value == failing_token:
                raise OfficialAuthError("认证服务返回了业务接口已拒绝的同一 token，未重复刷新；请检查原始响应")
        except OfficialAuthError as exc:
            exc.args = (self.redact(exc),)
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
                    "diagnostic": getattr(exc, "diagnostic", None).render()
                    if getattr(exc, "diagnostic", None)
                    else "",
                },
            )
            raise
        save_qz_refresh_failure(self.directory, {})
        return updated.value

    def _require_refresh(self, refresh):
        if not refresh.value:
            raise LoginRequired(
                f"刷新令牌缺失（凭据目录：{self.directory}）；请运行 qzcli login 完成一次网页授权"
            )
        if refresh.expired(self.now()):
            expiry = refresh.status(self.now())["expires_at"]
            raise LoginRequired(
                f"刷新令牌已于 {expiry} 到期（凭据目录：{self.directory}）；请运行 qzcli login 完成一次网页授权"
            )

    def token(self, *, force_refresh=False, failing_token=None):
        access, refresh, static = self._read()
        if access.value and not access.expired(self.now()):
            if not force_refresh or access.value != failing_token:
                return access.value
        if static:
            raise LoginRequired("INSPIRE_TOKEN 已过期或被服务端拒绝；请更新该静态凭据，或移除后运行 qzcli login")
        self._require_refresh(refresh)
        with self._lock:
            with qz_refresh_lock(self.directory):
                access, refresh, _ = self._read()
                if access.value and not access.expired(self.now()):
                    if not force_refresh or access.value != failing_token:
                        return access.value
                self._require_refresh(refresh)
                return self._refresh_locked(
                    access,
                    refresh,
                    force_refresh=force_refresh,
                    failing_token=failing_token,
                )

    @staticmethod
    def _notify(uri, code):
        print("请在浏览器完成官方网页授权：", flush=True)
        print(f"  URL:  {uri}", flush=True)
        print(f"  Code: {code}", flush=True)
        print("等待授权…", flush=True)

    def login(self, *, force=False, notify=None, oauth_client=None):
        if os.environ.get("INSPIRE_TOKEN"):
            raise OfficialAuthError("INSPIRE_TOKEN 正在覆盖官方凭据，请先移除该环境变量再进行网页登录")
        if oauth_client is not None:
            force = True
        if not force:
            try:
                self.token()
                return self.status()
            except OfficialAuthError as exc:
                if not exc.action_required:
                    raise
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self._lock, qz_refresh_lock(self.directory):
            access, refresh, _ = self._read()
            if not force:
                if access.value and not access.expired(self.now()):
                    return self.status()
                if refresh.value and not refresh.expired(self.now()):
                    try:
                        self._refresh_locked(access, refresh)
                        return self.status()
                    except OfficialAuthError as exc:
                        if not exc.action_required:
                            raise
            callback = notify or self._notify
            pair = (
                self.oauth.login(callback)
                if oauth_client is None
                else self.oauth.login(callback, client=oauth_client)
            )
            self._save(pair)
            access, _, _ = self._read()
            if not access.value or access.expired(self.now()):
                raise OfficialAuthError("网页授权未返回可用凭据，已保留原始错误")
            save_qz_refresh_failure(self.directory, {})
        return self.status()
