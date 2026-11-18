"""Platform device authorization over HTTP, without an external CLI or browser runtime."""

import base64
import json
import math
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

from . import __version__
from .auth_diag import AuthDiagnostic
from .config import get_proxy
from .qz_auth import LoginRequired, OfficialAuthError


DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
DEFAULT_SCOPE = "openid profile email"


@dataclass(frozen=True)
class TokenPair:
    access: str = field(repr=False)
    refresh: str = field(repr=False)
    access_expires_at: float
    refresh_expires_at: Optional[float]


class DeviceOAuth:
    def __init__(
        self,
        server,
        *,
        session=None,
        now=time.time,
        clock=time.monotonic,
        sleep=time.sleep,
        remember=None,
        auth_url="https://keycloak-inspire-prod.sii.edu.cn",
    ):

        self.server = server.rstrip("/")
        self.keycloak_issuer = auth_url.rstrip("/") + "/realms/inf-internal"
        self.session = session or requests.Session()
        self.session.trust_env = False
        self.now, self.clock, self.sleep = now, clock, sleep
        self._remember_callback = remember
        self._secrets = set()

    def remember(self, value):
        if isinstance(value, str) and value:
            self._secrets.add(value)
            if self._remember_callback:
                self._remember_callback(value)

    def _remember_fields(self, obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in {
                    "access_token",
                    "refresh_token",
                    "id_token",
                    "device_code",
                    "user_code",
                }:
                    self.remember(value)
                else:
                    self._remember_fields(value)
        elif isinstance(obj, list):
            for value in obj:
                self._remember_fields(value)

    def _trace(self, endpoint):
        trace = AuthDiagnostic(stage=f"native OAuth {endpoint}")
        for value in self._secrets:
            trace.remember(value)
        return trace

    @staticmethod
    def _failure(endpoint, response, trace, body, *, offline_token=False):
        error = (
            (body.get("error") or body.get("message"))
            if isinstance(body, dict)
            else None
        )
        cls = (
            LoginRequired
            if endpoint == "refresh"
            and response.status_code in (200, 400, 401)
            and error
            in {
                "invalid_grant",
                "invalid_token",
                "expired_token",
                "Token is not active",
            }
            else OfficialAuthError
        )
        prefix = (
            "平台已停用离线刷新令牌（offline_access），当前 Offline 令牌无法续期；"
            "请运行 qzcli login --reauth 重新获取在线会话令牌。\n"
            if cls is LoginRequired and offline_token
            else ""
        )
        exc = cls(
            f"{prefix}OAuth {endpoint}: HTTP {response.status_code}\n{trace.redact(response.text)}"
        )
        exc.diagnostic = trace
        return exc

    def _post(self, endpoint, payload, *, deadline=None, url=None, form=False):
        url = url or f"{self.server}/api/v1/public/auth/{endpoint}"
        self._remember_fields(payload)
        trace = self._trace(endpoint)
        timeout = min(30.0, deadline - self.clock()) if deadline is not None else 30.0
        if timeout <= 0:
            raise OfficialAuthError("设备授权已到期，请重新运行 qzcli login")
        proxy = get_proxy(url)
        try:
            response = self.session.post(
                url,
                **({"data": payload} if form else {"json": payload}),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded"
                    if form
                    else "application/json",
                    "X-Inspire-Client-Source": f"inspire-cli/qzcli-{__version__}",
                },
                proxies={"http": proxy, "https": proxy} if proxy else {},
                timeout=timeout,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            failure = OfficialAuthError(f"OAuth {endpoint} 网络请求失败：{trace.redact(exc)}")
            failure.diagnostic = trace
            raise failure from exc
        try:
            body = response.json()
        except ValueError:
            body = None
        self._remember_fields(body)
        for value in self._secrets:
            trace.remember(value)
        trace.record(response, "POST", url)
        if not isinstance(body, dict):
            raise self._failure(endpoint, response, trace, body)
        return response, body, trace

    def _tokens(self, endpoint, response, body, trace):
        data = body.get("data")
        if not isinstance(data, dict):
            raise self._failure(endpoint, response, trace, body)
        access, refresh = data.get("access_token"), data.get("refresh_token")
        access_in, refresh_in = data.get("expires_in"), data.get("refresh_expires_in")
        if (
            not isinstance(access, str)
            or not access
            or not isinstance(refresh, str)
            or not refresh
            or isinstance(access_in, bool)
            or not isinstance(access_in, (int, float))
            or not math.isfinite(access_in)
            or access_in <= 0
            or self.now() + access_in > 253402300799
            or isinstance(refresh_in, bool)
            or not isinstance(refresh_in, (int, float))
            or not math.isfinite(refresh_in)
            or refresh_in < 0
            or self.now() + refresh_in > 253402300799
        ):
            raise self._failure(endpoint, response, trace, body)
        received = self.now()
        # Keycloak returns refresh_expires_in=0 for offline_access tokens (no fixed MaxLifespan).
        refresh_expires_at = None if refresh_in == 0 else received + refresh_in
        return TokenPair(access, refresh, received + access_in, refresh_expires_at)

    def refresh(self, refresh_token):
        # Old Go qz used the public inspire-code client directly in Keycloak.
        # Its RT is not interchangeable with a platform-gateway client's RT.
        # Claims select a known protocol only; they never supply a network URL.
        try:
            part = refresh_token.split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
        except (IndexError, ValueError, UnicodeDecodeError):
            claims = {}
        is_offline = isinstance(claims, dict) and claims.get("typ") == "Offline"
        old_client = (
            isinstance(claims, dict)
            and claims.get("azp") == "inspire-code"
            and claims.get("iss") == self.keycloak_issuer
        )
        if old_client:
            response, body, trace = self._post(
                "refresh",
                {
                    "client_id": "inspire-code",
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                url=self.keycloak_issuer + "/protocol/openid-connect/token",
                form=True,
            )
            if response.status_code != 200 or body.get("error"):
                raise self._failure(
                    "refresh", response, trace, body, offline_token=is_offline
                )
            normalized = {"data": body}
        else:
            response, body, trace = self._post(
                "refresh", {"refresh_token": refresh_token}
            )
            if response.status_code != 200 or body.get("code") != 0:
                raise self._failure(
                    "refresh", response, trace, body, offline_token=is_offline
                )
            normalized = body
        return self._tokens("refresh", response, normalized, trace)

    def login(self, notify, *, client="platform", scope=DEFAULT_SCOPE):
        if client not in ("platform", "inspire-code"):
            raise OfficialAuthError(f"不支持的 OAuth 客户端：{client}")
        if client == "inspire-code":
            response, body, trace = self._post(
                "device-code",
                {"client_id": "inspire-code", "scope": scope},
                url=self.keycloak_issuer + "/protocol/openid-connect/auth/device",
                form=True,
            )
            if response.status_code != 200 or body.get("error"):
                raise self._failure("device-code", response, trace, body)
            data = body
        else:
            response, body, trace = self._post("device-code", {"scope": scope})
            if response.status_code != 200 or body.get("code") != 0:
                raise self._failure("device-code", response, trace, body)
            data = body.get("data")
            if not isinstance(data, dict):
                raise self._failure("device-code", response, trace, body)
        code, user_code = data.get("device_code"), data.get("user_code")
        uri = data.get("verification_uri_complete") or data.get("verification_uri")
        expires, interval = data.get("expires_in"), data.get("interval", 5)
        if not all(isinstance(v, str) and v for v in (code, user_code, uri)) or any(
            isinstance(v, bool)
            or not isinstance(v, (int, float))
            or not math.isfinite(v)
            or v <= 0
            for v in (expires, interval)
        ):
            raise self._failure("device-code", response, trace, body)
        deadline = self.clock() + expires
        interval = max(1.0, interval)
        notify(uri, user_code)
        while self.clock() < deadline:
            self.sleep(min(interval, max(0, deadline - self.clock())))
            if self.clock() >= deadline:
                break
            if client == "inspire-code":
                response, body, trace = self._post(
                    "token",
                    {
                        "client_id": "inspire-code",
                        "device_code": code,
                        "grant_type": DEVICE_GRANT,
                    },
                    deadline=deadline,
                    url=self.keycloak_issuer + "/protocol/openid-connect/token",
                    form=True,
                )
                if response.status_code == 200 and not body.get("error"):
                    return self._tokens("token", response, {"data": body}, trace)
            else:
                response, body, trace = self._post(
                    "token",
                    {"device_code": code, "grant_type": DEVICE_GRANT},
                    deadline=deadline,
                )
                if response.status_code == 200 and body.get("code") == 0:
                    return self._tokens("token", response, body, trace)
            error = body.get("error") or body.get("message")
            if error == "authorization_pending" and response.status_code in (200, 400):
                continue
            if error == "slow_down" and response.status_code in (200, 400):
                interval += 5
                continue
            raise self._failure("token", response, trace, body)
        raise OfficialAuthError("设备授权已到期，请重新运行 qzcli login")
