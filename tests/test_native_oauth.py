"""Native OAuth wire protocol, evidence redaction, and process-level rotation."""
import json
import multiprocessing
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

import requests

from qzcli import config
from qzcli.auth_diag import format_exception
from qzcli.oauth import DEFAULT_SCOPE, DEVICE_GRANT, DeviceOAuth
from qzcli.qz_auth import OfficialAuth, OfficialAuthError, Token

NOW = 1794895200.0


def response(body, status=200):
    r = requests.Response()
    r.status_code = status
    r._content = (body if isinstance(body, str) else json.dumps(body)).encode()
    r.headers["X-Request-ID"] = "trace-unique-end"
    return r


def tokens(access="test-access-value", refresh="test-refresh-value"):
    return {
        "code": 0,
        "data": {
            "access_token": access,
            "refresh_token": refresh,
            "expires_in": 3600,
            "refresh_expires_in": 604800,
        },
    }


class NativeProtocolTests(unittest.TestCase):
    def setUp(self):
        self.http = Mock()
        self.elapsed = 0
        self.delays = []
        self.oauth = DeviceOAuth(
            "https://example.test",
            session=self.http,
            now=lambda: NOW,
            clock=lambda: self.elapsed,
            sleep=self.sleep,
        )
        p = patch("qzcli.oauth.get_proxy", return_value="")
        p.start()
        self.addCleanup(p.stop)

    def sleep(self, delay):
        self.delays.append(delay)
        self.elapsed += delay

    def device(self, expires=60):
        return response(
            {
                "code": 0,
                "data": {
                    "device_code": "private-device-value",
                    "user_code": "private-user-value",
                    "verification_uri_complete": "https://example.test/device",
                    "expires_in": expires,
                    "interval": 2,
                },
            }
        )

    def test_device_pending_slow_down_then_authorized(self):
        self.http.post.side_effect = [
            self.device(),
            response({"error": "authorization_pending"}, 400),
            response({"error": "slow_down"}, 400),
            response(tokens()),
        ]
        notify = Mock()
        pair = self.oauth.login(notify)
        notify.assert_called_once_with(
            "https://example.test/device", "private-user-value"
        )
        self.assertEqual(self.delays, [2, 2, 7])
        self.assertEqual(pair.access_expires_at, NOW + 3600)
        calls = self.http.post.call_args_list
        self.assertTrue(calls[0].args[0].endswith("/api/v1/public/auth/device-code"))
        self.assertEqual(calls[0].kwargs["json"], {"scope": DEFAULT_SCOPE})
        self.assertEqual(DEFAULT_SCOPE, "openid profile email")
        self.assertNotIn("offline_access", DEFAULT_SCOPE)
        self.assertEqual(
            calls[1].kwargs["json"],
            {"device_code": "private-device-value", "grant_type": DEVICE_GRANT},
        )
        for call in calls:
            self.assertFalse(call.kwargs["allow_redirects"])
            self.assertNotIn("Authorization", call.kwargs["headers"])
        self.assertFalse(self.http.trust_env)

    def test_deadline_stops_without_extra_poll(self):
        self.http.post.side_effect = [
            self.device(expires=3),
            response({"error": "authorization_pending"}, 400),
        ]
        with self.assertRaises(OfficialAuthError):
            self.oauth.login(Mock())
        self.assertEqual(self.http.post.call_count, 2)
        self.assertEqual(self.elapsed, 3)
        self.assertEqual(self.http.post.call_args.kwargs["timeout"], 1)

    def test_denied_and_server_failures_are_not_retried(self):
        for status, body in [
            (400, {"error": "access_denied"}),
            (503, {"error": "authorization_pending"}),
            (429, {"message": "slow_down"}),
        ]:
            with self.subTest(status=status):
                self.elapsed = 0
                self.http.post.reset_mock()
                self.http.post.side_effect = [self.device(), response(body, status)]
                with self.assertRaises(OfficialAuthError) as ctx:
                    self.oauth.login(Mock())
                self.assertEqual(self.http.post.call_count, 2)
                self.assertIn(str(status), str(ctx.exception))

    def test_refresh_raw_error_headers_and_secrets(self):
        for status in (400, 401, 403, 429, 500):
            self.http.post.return_value = response(
                {
                    "error": "invalid_grant",
                    "refresh_token": "private-refresh-value",
                    "description": "ORIGINAL_END",
                },
                status,
            )
            with self.assertRaises(OfficialAuthError) as ctx:
                self.oauth.refresh("private-refresh-value")
            self.assertEqual(ctx.exception.action_required, status in (400, 401))
            rendered = format_exception(ctx.exception)
            self.assertIn("ORIGINAL_END", rendered)
            self.assertIn("trace-unique-end", rendered)
            self.assertNotIn("private-refresh-value", rendered)

    def test_html_redirect_and_non_object_json_preserved(self):
        for status, body in [
            (302, "<html>ORIGINAL_END</html>"),
            (502, "<html>ORIGINAL_END</html>"),
            (200, '["ORIGINAL_END"]'),
        ]:
            self.http.post.return_value = response(body, status)
            with self.assertRaises(OfficialAuthError) as ctx:
                self.oauth.refresh("private-refresh-value")
            self.assertFalse(ctx.exception.action_required)
            self.assertIn("ORIGINAL_END", format_exception(ctx.exception))

    def test_network_error_preserved_and_proxy_password_redacted(self):
        self.http.post.side_effect = requests.ConnectionError(
            "https://alice:secret@proxy.test DNS ORIGINAL_END"
        )
        with self.assertRaises(OfficialAuthError) as ctx:
            self.oauth.refresh("private-refresh-value")
        self.assertFalse(ctx.exception.action_required)
        rendered = format_exception(ctx.exception)
        self.assertIn("DNS ORIGINAL_END", rendered)
        self.assertNotIn("secret@", rendered)

    def test_malformed_token_and_lifetime_dont_escape_in_diagnostic(self):
        for key, value in [
            ("access_token", ""),
            ("refresh_token", None),
            ("expires_in", True),
            ("expires_in", float("inf")),
            ("expires_in", -1),
            ("refresh_expires_in", "3600"),
        ]:
            body = tokens()
            body["data"][key] = value
            self.http.post.return_value = response(body)
            with self.assertRaises(OfficialAuthError) as ctx:
                self.oauth.refresh("private-refresh-value")
            output = format_exception(ctx.exception)
            self.assertNotIn("test-access-value", output)
            self.assertNotIn("test-refresh-value", output)

    def test_device_codes_never_in_error_evidence(self):
        self.http.post.side_effect = [
            self.device(),
            response(
                {
                    "error": "access_denied",
                    "device_code": "private-device-value",
                    "user_code": "private-user-value",
                },
                400,
            ),
        ]
        with self.assertRaises(OfficialAuthError) as ctx:
            self.oauth.login(Mock())
        output = format_exception(ctx.exception)
        self.assertNotIn("private-device-value", output)
        self.assertNotIn("private-user-value", output)

    def test_existing_go_client_refreshes_at_configured_keycloak_without_gateway_retry(
        self,
    ):
        import base64

        claims = {
            "azp": "inspire-code",
            "iss": "https://keycloak-inspire-prod.sii.edu.cn/realms/inf-internal",
        }
        encoded = (
            base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
        )
        rt = "header." + encoded + ".signature"
        self.http.post.return_value = response(tokens()["data"])
        self.assertEqual(self.oauth.refresh(rt).access, "test-access-value")
        call = self.http.post.call_args
        self.assertEqual(call.args[0], claims["iss"] + "/protocol/openid-connect/token")
        self.assertEqual(
            call.kwargs["data"],
            {
                "client_id": "inspire-code",
                "grant_type": "refresh_token",
                "refresh_token": rt,
            },
        )
        self.assertNotIn("json", call.kwargs)
        self.http.post.return_value = response(
            {
                "error": "invalid_grant",
                "error_description": "Session not active ORIGINAL_END",
            },
            400,
        )
        self.http.post.reset_mock()
        with self.assertRaises(OfficialAuthError) as ctx:
            self.oauth.refresh(rt)
        self.assertTrue(ctx.exception.action_required)
        self.assertIn(
            "Session not active ORIGINAL_END", format_exception(ctx.exception)
        )
        self.http.post.assert_called_once()

    def test_untrusted_token_issuer_never_supplies_network_url(self):
        import base64

        claims = {
            "azp": "inspire-code",
            "iss": "https://attacker.test/realms/inf-internal",
        }
        rt = (
            "header."
            + base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
            + ".signature"
        )
        self.http.post.return_value = response(tokens())
        self.oauth.refresh(rt)
        self.assertEqual(
            self.http.post.call_args.args[0],
            "https://example.test/api/v1/public/auth/refresh",
        )

    def test_cached_refresh_failure_keeps_response_evidence_across_providers(self):
        self.http.post.return_value = response(
            {"error": "invalid_grant", "description": "ORIGINAL_END"}, 400
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config.save_qz_credentials(
                root,
                {
                    "token": "expired",
                    "access_expires_at": NOW - 1,
                    "refresh_token": "private-refresh-value",
                    "refresh_expires_at": NOW + 3600,
                },
            )
            for _ in range(2):
                provider = OfficialAuth(root, now=lambda: NOW, oauth=self.oauth)
                with self.assertRaises(OfficialAuthError) as ctx:
                    provider.token()
                rendered = format_exception(ctx.exception)
                self.assertIn("trace-unique-end", rendered)
                self.assertIn("ORIGINAL_END", rendered)
                self.assertNotIn("private-refresh-value", rendered)
            self.http.post.assert_called_once()

    def test_configured_proxy_and_no_proxy(self):
        self.http.post.return_value = response(tokens())
        with patch("qzcli.oauth.get_proxy", return_value="socks5h://localhost:1080"):
            self.oauth.refresh("private-refresh-value")
        self.assertEqual(
            self.http.post.call_args.kwargs["proxies"]["https"],
            "socks5h://localhost:1080",
        )
        with patch.dict(
            os.environ,
            {
                "NO_PROXY": "example.test",
                "no_proxy": "example.test",
                "ALL_PROXY": "http://localhost:7897",
            },
        ), patch("qzcli.oauth.get_proxy", config.get_proxy), patch(
            "qzcli.config.load_config", return_value={}
        ):
            self.oauth.refresh("private-refresh-value")
        self.assertEqual(self.http.post.call_args.kwargs["proxies"], {})

    def test_offline_token_exp_zero_and_refresh_expires_in_zero_never_treated_as_1970_expired(
        self,
    ):
        import base64

        def make_jwt(claims):
            encoded = (
                base64.urlsafe_b64encode(json.dumps(claims).encode())
                .decode()
                .rstrip("=")
            )
            return "header." + encoded + ".signature"

        offline_rt = make_jwt(
            {
                "iss": "https://keycloak-inspire-prod.sii.edu.cn/realms/inf-internal",
                "azp": "inspire-code",
                "typ": "Offline",
                "exp": 0,
            }
        )
        parsed = Token.parse(offline_rt, expires_at=None)
        self.assertFalse(parsed.expired(NOW))
        self.assertIsNone(parsed.expires_at)
        self.assertEqual(
            parsed.status(NOW),
            {
                "state": "unexpired",
                "expires_at": None,
                "token_type": "Offline",
                "client_id": "inspire-code",
            },
        )
        # Keycloak OIDC device flow with client='inspire-code' returning refresh_expires_in=0
        new_at = make_jwt({"exp": int(NOW + 3600), "typ": "Bearer", "azp": "inspire-code"})
        self.http.post.side_effect = [
            response(
                {
                    "device_code": "kc-device-code",
                    "user_code": "KC-USER",
                    "verification_uri_complete": "https://keycloak-inspire-prod.sii.edu.cn/realms/inf-internal/device?user_code=KC-USER",
                    "expires_in": 60,
                    "interval": 2,
                }
            ),
            response(
                {
                    "access_token": new_at,
                    "refresh_token": offline_rt,
                    "expires_in": 3600,
                    "refresh_expires_in": 0,
                }
            ),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            provider = OfficialAuth(root, now=lambda: NOW, oauth=self.oauth)
            st = provider.login(notify=Mock(), oauth_client="inspire-code")
            self.assertEqual(st["refresh_token"]["state"], "unexpired")
            self.assertEqual(st["refresh_token"]["token_type"], "Offline")
            self.assertIsNone(st["refresh_token"]["expires_at"])
            self.assertTrue(st.get("offline_deprecated"))
            self.assertIn("offline_access", st.get("warning", ""))
            calls = self.http.post.call_args_list
            self.assertEqual(
                calls[0].args[0],
                "https://keycloak-inspire-prod.sii.edu.cn/realms/inf-internal/protocol/openid-connect/auth/device",
            )
            self.assertEqual(
                calls[0].kwargs["data"],
                {"client_id": "inspire-code", "scope": DEFAULT_SCOPE},
            )
            # Advance clock 3 days past AT expiry: exp=0 RT still attempts refresh if server accepts
            rotated_at = make_jwt({"exp": int(NOW + 400000), "typ": "Bearer"})
            self.http.post.reset_mock()
            self.http.post.side_effect = None
            self.http.post.return_value = response(
                {
                    "access_token": rotated_at,
                    "refresh_token": offline_rt,
                    "expires_in": 3600,
                    "refresh_expires_in": 0,
                }
            )
            later = OfficialAuth(root, now=lambda: NOW + 259200, oauth=self.oauth)
            self.assertEqual(later.token(), rotated_at)
            self.http.post.assert_called_once()

    def test_revoked_offline_token_explains_reauth_and_plain_login_replaces_it(self):
        import base64

        def make_jwt(claims):
            encoded = (
                base64.urlsafe_b64encode(json.dumps(claims).encode())
                .decode()
                .rstrip("=")
            )
            return "header." + encoded + ".signature"

        offline_rt = make_jwt(
            {
                "iss": "https://keycloak-inspire-prod.sii.edu.cn/realms/inf-internal",
                "azp": "inspire-studio",
                "typ": "Offline",
                "exp": 0,
            }
        )
        online_rt = make_jwt(
            {
                "iss": "https://keycloak-inspire-prod.sii.edu.cn/realms/inf-internal",
                "azp": "inspire-studio",
                "typ": "Refresh",
                "exp": int(NOW + 604800),
            }
        )
        valid_at = make_jwt({"exp": int(NOW + 1800), "typ": "Bearer"})
        fresh_at = make_jwt({"exp": int(NOW + 3600), "typ": "Bearer"})

        # 1. Refreshing a revoked Offline RT gives actionable message + preserves raw evidence
        self.http.post.return_value = response(
            {"code": -100000, "message": "invalid_grant"}, 400
        )
        with self.assertRaises(OfficialAuthError) as ctx:
            self.oauth.refresh(offline_rt)
        self.assertTrue(ctx.exception.action_required)
        rendered = format_exception(ctx.exception)
        self.assertIn("平台已停用离线刷新令牌", rendered)
        self.assertIn("qzcli login --reauth", rendered)
        self.assertIn("invalid_grant", rendered)
        self.assertIn("trace-unique-end", rendered)

        # 2. Plain `login()` replaces an existing Offline RT even while AT is still unexpired
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config.save_qz_credentials(
                root,
                {
                    "token": valid_at,
                    "access_expires_at": NOW + 1800,
                    "refresh_token": offline_rt,
                    "refresh_expires_at": None,
                },
            )
            self.http.post.reset_mock()
            self.http.post.side_effect = [
                self.device(),
                response(tokens(fresh_at, online_rt)),
            ]
            provider = OfficialAuth(root, now=lambda: NOW, oauth=self.oauth)
            st = provider.login(notify=Mock())
            self.assertEqual(st["refresh_token"]["token_type"], "Refresh")
            self.assertFalse(st.get("offline_deprecated", False))
            self.assertEqual(self.http.post.call_count, 2)

    def test_proactive_refresh_renews_even_when_access_token_is_still_valid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config.save_qz_credentials(
                root,
                {
                    "token": "still-valid-access",
                    "access_expires_at": NOW + 1800,
                    "refresh_token": "active-online-refresh",
                    "refresh_expires_at": NOW + 43200,
                },
            )
            self.http.post.return_value = response(
                tokens("proactively-rotated-access", "proactively-rotated-refresh")
            )
            provider = OfficialAuth(root, now=lambda: NOW, oauth=self.oauth)
            st = provider.refresh()
            self.http.post.assert_called_once()
            saved = config.read_qz_credentials(root)
            self.assertEqual(saved["token"], "proactively-rotated-access")
            self.assertEqual(saved["refresh_token"], "proactively-rotated-refresh")
            self.assertEqual(st["access_token"]["state"], "unexpired")

    def test_notify_auto_opens_browser_only_on_interactive_local_desktop(self):
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("sys.platform", "darwin"),
            patch("sys.stdout.isatty", return_value=True),
            patch("webbrowser.open", return_value=True) as wb_open,
        ):
            OfficialAuth._notify("https://example.invalid/device", "ABCD-EFGH")
            wb_open.assert_called_once_with("https://example.invalid/device")

        for env_extra, isatty, open_flag in [
            ({"SSH_CONNECTION": "1.2.3.4 1234 5.6.7.8 22"}, True, True),
            ({"QZCLI_NO_BROWSER": "1"}, True, True),
            ({}, False, True),
            ({}, True, False),
        ]:
            with (
                patch.dict(os.environ, env_extra, clear=True),
                patch("sys.platform", "darwin"),
                patch("sys.stdout.isatty", return_value=isatty),
                patch("webbrowser.open", return_value=True) as wb_open,
            ):
                OfficialAuth._notify(
                    "https://example.invalid/device",
                    "ABCD-EFGH",
                    open_browser=open_flag,
                )
                wb_open.assert_not_called()


class AutoFallbackModeTests(unittest.TestCase):
    def test_auto_mode_uses_oauth_when_valid_and_falls_back_to_cas_when_rt_expired(self):
        from qzcli import api as api_mod

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / "state"
            official = root / "official"
            state.mkdir()
            official.mkdir()
            with (
                patch.object(config, "CONFIG_DIR", state),
                patch.object(config, "CONFIG_FILE", state / "config.json"),
                patch.object(config, "COOKIE_FILE", state / ".cookie"),
                patch.object(api_mod, "CONFIG_DIR", state),
                patch.object(api_mod, "get_credentials", return_value=("fake-u", "fake-p")),
                patch.object(
                    api_mod,
                    "get_credentials_with_source",
                    return_value=("fake-u", "fake-p", "config", "config"),
                ),
                patch.object(api_mod, "credential_conflict", return_value=[]),
            ):
                api_mod._clear_relogin_failure()
                # 1. Valid OAuth AT -> stays in qz mode, zero CAS calls
                config.save_qz_credentials(
                    official,
                    {
                        "token": "valid-auto-at",
                        "access_expires_at": time.time() + 3600,
                        "refresh_token": "valid-auto-rt",
                        "refresh_expires_at": time.time() + 43200,
                    },
                )
                client = api_mod.QzAPI(auth_mode="auto", qz_config_dir=official)
                with (
                    patch.object(
                        api_mod,
                        "_curl_post",
                        return_value=response(
                            {"Result": {"id": "user-fake", "name": "fake-auto"}}
                        ),
                    ) as post,
                    patch.object(client, "login_with_cas") as cas_login,
                ):
                    user = client.get_user_detail()
                    self.assertEqual(user["id"], "user-fake")
                    self.assertEqual(client.auth_mode, "qz")
                    cas_login.assert_not_called()
                    self.assertIn(
                        "Bearer valid-auto-at",
                        post.call_args.kwargs["headers"]["authorization"],
                    )

                # 2. Expired OAuth RT (LoginRequired) -> seamlessly falls back to CAS session
                config.save_qz_credentials(
                    official,
                    {
                        "token": "expired-auto-at",
                        "access_expires_at": time.time() - 60,
                        "refresh_token": "",
                    },
                )
                client2 = api_mod.QzAPI(auth_mode="auto", qz_config_dir=official)

                def fake_post(url, **kwargs):
                    hdrs = kwargs.get("headers") or {}
                    if hdrs.get("cookie") == "inspire-session=fresh-cas-cookie":
                        r = response({"Result": {"id": "user-cas", "name": "cas-ok"}})
                        r.headers["Content-Type"] = "application/json"
                        return r
                    r = response({"error": "unauthorized"}, 401)
                    r.headers["Content-Type"] = "application/json"
                    return r

                with (
                    patch.object(api_mod, "_curl_post", side_effect=fake_post),
                    patch.object(
                        client2,
                        "login_with_cas",
                        return_value="inspire-session=fresh-cas-cookie",
                    ) as cas_login,
                ):
                    user2 = client2.get_user_detail()
                    self.assertEqual(user2["id"], "user-cas")
                    self.assertEqual(client2.auth_mode, "legacy")
                    self.assertTrue(client2._auto_fallback_active)
                    cas_login.assert_called_once_with("fake-u", "fake-p")

                # 2b. Default mode (no --auth-mode auto needed!) ALSO falls back at 3 AM when credentials exist,
                # whereas explicit auth_mode="native" stays strict OAuth-only.
                (state / ".cookie").unlink(missing_ok=True)
                default_client = api_mod.QzAPI(qz_config_dir=official)
                with (
                    patch.object(api_mod, "_curl_post", side_effect=fake_post),
                    patch.object(
                        default_client,
                        "login_with_cas",
                        return_value="inspire-session=fresh-cas-cookie",
                    ) as cas_login_def,
                ):
                    self.assertEqual(default_client.get_user_detail()["id"], "user-cas")
                    cas_login_def.assert_called_once_with("fake-u", "fake-p")

                strict_client = api_mod.QzAPI(auth_mode="native", qz_config_dir=official)
                with patch.object(strict_client, "login_with_cas") as cas_login_strict:
                    with self.assertRaises(api_mod.QzAPIError):
                        strict_client.get_user_detail()
                    cas_login_strict.assert_not_called()

                # 3. Transient OAuth network failure (action_required=False) -> NO CAS fallback
                client3 = api_mod.QzAPI(auth_mode="auto", qz_config_dir=official)
                with (
                    patch.object(
                        client3._official_auth,
                        "token",
                        side_effect=OfficialAuthError("502 Bad Gateway", action_required=False),
                    ),
                    patch.object(client3, "login_with_cas") as cas_login,
                ):
                    with self.assertRaisesRegex(api_mod.QzAPIError, "502 Bad Gateway"):
                        client3.get_user_detail()
                    cas_login.assert_not_called()

                # 4. CAS hits CAPTCHA -> 0 retries, immediate human-login prompt, cooldown blocks repeat
                api_mod._clear_relogin_failure()
                (state / ".cookie").unlink(missing_ok=True)
                client4 = api_mod.QzAPI(auth_mode="auto", qz_config_dir=official)
                captcha_err = api_mod.QzAPIError(
                    "CAS 返回：请输入图形验证码\n当前密码登录方式尚不支持图形验证码输入。",
                    401,
                )
                with (
                    patch.object(
                        api_mod,
                        "_curl_post",
                        return_value=response({"error": "unauthorized"}, 401),
                    ),
                    patch.object(
                        client4, "login_with_cas", side_effect=captcha_err
                    ) as cas_login,
                ):
                    for _ in range(2):
                        c = api_mod.QzAPI(auth_mode="auto", qz_config_dir=official)
                        with patch.object(c, "login_with_cas", cas_login):
                            with self.assertRaises(api_mod.QzAPIError) as ctx:
                                c.get_user_detail()
                            self.assertIn("需人工介入", str(ctx.exception))
                            self.assertIn("qzcli login", str(ctx.exception))
                    # Cooldown prevents hammering CAS when CAPTCHA is active
                    cas_login.assert_called_once()
                api_mod._clear_relogin_failure()


def _child_refresh(directory, gate, results):
    try:
        gate.wait(10)
        results.put(OfficialAuth(directory).token())
    except Exception as exc:
        results.put(type(exc).__name__)


class ProcessRotationTests(unittest.TestCase):
    def test_four_processes_refresh_once_and_next_process_reuses_rotated_pair(self):
        requests_seen = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests_seen.append((self.path, body))
                result = tokens("rotated-process-access", "rotated-process-refresh")
                encoded = json.dumps(result).encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(encoded)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "config.yaml").write_text(
                "server: http://127.0.0.1:%s\n" % server.server_port
            )
            config.save_qz_credentials(
                root,
                {
                    "token": "expired-process-access",
                    "access_expires_at": time.time() - 30,
                    "refresh_token": "initial-process-refresh",
                    "refresh_expires_at": time.time() + 7200,
                },
            )
            ctx = multiprocessing.get_context("spawn")
            gate, results = ctx.Event(), ctx.Queue()
            children = [
                ctx.Process(target=_child_refresh, args=(root, gate, results))
                for _ in range(4)
            ]
            with patch.dict(os.environ, {"NO_PROXY": "127.0.0.1", "INSPIRE_TOKEN": ""}):
                for child in children:
                    child.start()
                gate.set()
                values = [results.get(timeout=30) for _ in children]
                for child in children:
                    child.join(10)
                    self.assertEqual(child.exitcode, 0)
                self.assertEqual(OfficialAuth(root).token(), "rotated-process-access")
            self.assertEqual(values, ["rotated-process-access"] * 4)
            self.assertEqual(
                requests_seen,
                [
                    (
                        "/api/v1/public/auth/refresh",
                        {"refresh_token": "initial-process-refresh"},
                    )
                ],
            )
            saved = config.read_qz_credentials(root)
            self.assertEqual(saved["refresh_token"], "rotated-process-refresh")
            self.assertGreater(saved["access_expires_at"], time.time())


class OrdinaryCliExpiryTests(unittest.TestCase):
    def test_login_then_natural_short_at_expiry_refreshes_without_login_again(self):
        import base64

        seen = []

        def jwt(exp, label):
            claims = (
                base64.urlsafe_b64encode(
                    json.dumps({"exp": exp, "jti": label}).encode()
                )
                .decode()
                .rstrip("=")
            )
            return "fake-header." + claims + ".fake-signature"

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                payload = json.loads(
                    self.rfile.read(int(self.headers["Content-Length"]))
                )
                seen.append((self.path, payload))
                if self.path.endswith("/device-code"):
                    value = {
                        "code": 0,
                        "data": {
                            "device_code": "fake-device-code",
                            "user_code": "fake-user-code",
                            "verification_uri": "https://example.invalid/device",
                            "expires_in": 30,
                            "interval": 1,
                        },
                    }
                elif self.path.endswith("/token"):
                    value = tokens(
                        jwt(time.time() + 2, "initial-cli-access"),
                        "fake-initial-cli-refresh",
                    )
                    value["data"]["expires_in"] = 2
                elif self.path.endswith("/refresh"):
                    if payload.get("refresh_token") not in (
                        "fake-initial-cli-refresh",
                        "fake-rotated-cli-refresh",
                    ):
                        self.send_response(401)
                        self.send_header("X-Trace-ID", "fake-http-trace")
                        self.end_headers()
                        self.wfile.write(
                            b'{"error":"invalid_grant","error_description":"Session inactive ORIGINAL_END"}'
                        )
                        return
                    next_rt = (
                        "fake-rotated-cli-refresh"
                        if payload.get("refresh_token") == "fake-initial-cli-refresh"
                        else "fake-keepalive-cli-refresh"
                    )
                    value = tokens(
                        jwt(time.time() + 3600, "renewed-cli-access"),
                        next_rt,
                    )
                elif "Action=GetUserDetail" in self.path:
                    value = {"Result": {"id": "user-fake-cli", "name": "fake-user"}}
                elif "Action=ListJobs" in self.path:
                    value = {
                        "Result": {
                            "jobs": [
                                {
                                    "id": "job-fake-cli",
                                    "name": "fake-readonly-job",
                                    "status": "job_succeeded",
                                    "workspace_id": "ws-fake-cli",
                                    "user_id": "user-fake-cli",
                                    "created_at": "2026-11-17T12:00:00Z",
                                }
                            ],
                            "total": 1,
                        }
                    }
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                encoded = json.dumps(value).encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(encoded)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            official = root / "official"
            official.mkdir()
            (official / "config.yaml").write_text(
                "server: http://127.0.0.1:%s\n" % server.server_port
            )
            state = root / "state"
            state.mkdir()
            (state / "config.json").write_text(
                json.dumps({"auth_mode": "native", "qz_config_dir": str(official)})
            )
            (state / "resources.json").write_text(
                json.dumps({"ws-fake-cli": {"id": "ws-fake-cli", "name": "分布式测试"}})
            )
            env = {
                **os.environ,
                "QZCLI_HOME": str(state),
                "NO_PROXY": "127.0.0.1",
                "INSPIRE_TOKEN": "",
                "QZCLI_AUTH_MODE": "native",
                "QZ_CLI_CONFIG_DIR": str(official),
            }

            def command(*args):
                return subprocess.run(
                    [sys.executable, "-m", "qzcli.cli", *args],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=20,
                )

            login = command("login")
            self.assertEqual(login.returncode, 0, login.stderr + login.stdout)
            old = config.read_qz_credentials(official)
            from qzcli.qz_auth import Token

            expiry = Token.parse(old["token"]).expires_at
            time.sleep(max(0, expiry - time.time()) + 0.1)
            listed = command("list", "-c", "-w", "ws-fake-cli", "-n", "1")
            self.assertEqual(listed.returncode, 0, listed.stderr + listed.stdout)
            self.assertIn("fake-readonly-job", listed.stdout)
            current = config.read_qz_credentials(official)
            self.assertNotEqual(old["token"], current["token"])
            self.assertEqual(current["refresh_token"], "fake-rotated-cli-refresh")
            checked = command("auth", "status", "--check")
            self.assertEqual(checked.returncode, 0, checked.stderr + checked.stdout)
            self.assertTrue(json.loads(checked.stdout)["authenticated"])
            self.assertEqual(sum(p.endswith("/device-code") for p, b in seen), 1)
            self.assertEqual(sum(p.endswith("/token") for p, b in seen), 1)
            self.assertEqual(sum(p.endswith("/refresh") for p, b in seen), 1)
            # Proactive `qzcli auth refresh --check` renews even while AT is unexpired
            keepalive = command("auth", "refresh", "--check")
            self.assertEqual(
                keepalive.returncode, 0, keepalive.stderr + keepalive.stdout
            )
            self.assertTrue(json.loads(keepalive.stdout)["authenticated"])
            self.assertEqual(
                config.read_qz_credentials(official)["refresh_token"],
                "fake-keepalive-cli-refresh",
            )
            self.assertEqual(sum(p.endswith("/refresh") for p, b in seen), 2)
            # When renewal is impossible, preserve the actual cause at the CLI boundary.
            config.save_qz_credentials(
                official,
                {"token": jwt(time.time() - 20, "expired-cli"), "refresh_token": ""},
            )
            rejected = command("list", "-c", "-w", "ws-fake-cli", "-n", "1")
            self.assertNotEqual(rejected.returncode, 0)
            output = rejected.stdout + rejected.stderr
            self.assertIn("刷新令牌缺失", output)
            self.assertIn(str(official), output)
            self.assertNotIn("Cookie 已过期", output)
            self.assertEqual(sum(p.endswith("/refresh") for p, b in seen), 2)

            config.save_qz_credentials(
                official,
                {
                    "token": jwt(time.time() - 20, "expired-cli"),
                    "refresh_token": "fake-dead-cli-refresh",
                    "refresh_expires_at": time.time() + 3600,
                },
            )
            for _ in range(2):
                failed = command("list", "-c", "-w", "ws-fake-cli", "-n", "1")
                self.assertNotEqual(failed.returncode, 0)
                output = failed.stdout + failed.stderr
                self.assertIn("ORIGINAL_END", output)
                self.assertIn("fake-http-trace", output)
                self.assertNotIn("Cookie 已过期", output)
            self.assertEqual(sum(p.endswith("/refresh") for p, b in seen), 3)
