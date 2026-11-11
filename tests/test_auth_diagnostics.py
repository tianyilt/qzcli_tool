"""Exercise argparse, credential files and the real Requests redirect machinery."""

import contextlib
import email.message
import io
import json
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib.parse import urlsplit

import requests
from requests.adapters import BaseAdapter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from support.sandbox import sandbox_home
from qzcli import api, cli, config
from qzcli.auth_diag import AuthDiagnostic, CasPage, format_exception

_REAL_SESSION = requests.Session
_USER = "fake-login-user"
_GOOD = "fake-current-password"
_BAD = "fake-wrong-password"
_FORM = """<form id="fm1"><input name="username"><input name="password" type="hidden">
<input name="execution" type="hidden" value="fake-execution-secret"></form>"""


class AuthTransport(BaseAdapter):
    def __init__(self, scenario="captcha"):
        self.scenario = scenario
        self.seen = []

    def close(self):
        pass

    def send(self, request, **kwargs):
        self.seen.append(request)
        host, path = urlsplit(request.url).hostname, urlsplit(request.url).path
        status, headers, body = 200, {"Content-Type": "text/html; charset=utf-8"}, ""
        if host == "qz.sii.edu.cn" and path == "/":
            status, headers["Location"] = 302, "https://keycloak.example.invalid/auth"
        elif host == "keycloak.example.invalid" and path == "/auth":
            body = '{"loginUrl":"https://keycloak.example.invalid/broker/cas/login"}'
        elif path == "/broker/cas/login":
            status, headers["Location"] = (
                302,
                "https://cas.sii.edu.cn/cas/login?service=fake-service",
            )
        elif host == "cas.sii.edu.cn" and request.method == "GET":
            body = (
                _FORM.replace(
                    "</form>",
                    '<input name="authcode" placeholder="验证码 Verification Code"></form>',
                )
                if self.scenario == "initial_captcha"
                else _FORM
            )
        elif host == "cas.sii.edu.cn" and request.method == "POST":
            if self.scenario == "timeout":
                raise requests.ReadTimeout(
                    "fixture timeout after password POST?ticket=fake-ticket-secret",
                    request=request,
                )
            if self.scenario == "redirect307":
                status, headers["Location"] = 307, "https://cas.sii.edu.cn/replay"
            elif self.scenario in {"captcha", "initial_captcha", "password"}:
                detail = (
                    "验证码信息无效。"
                    if self.scenario in {"captcha", "initial_captcha"}
                    else "账号或密码错误。再输错4次，账号将被锁定。"
                )
                body = (
                    '<form id="fm1"><input name="authcode"><div class="form-error"><span>'
                    + detail
                    + "</span><br>完整错误尾句</div></form>"
                    + "\n"
                    + "x" * 10000
                    + "<!-- RAW_RESPONSE_END -->"
                )
                headers["X-Fixture-Request"] = "fake-request-id"
            elif self.scenario == "keycloak400":
                status, headers["Location"] = (
                    302,
                    "https://keycloak.example.invalid/denied?ticket=fake-ticket-secret",
                )
            else:
                status, headers["Location"] = (
                    302,
                    "https://qz.sii.edu.cn/callback?code=fake-code-secret",
                )
        elif host == "keycloak.example.invalid" and path == "/denied":
            status, body = (
                400,
                '{"summary":"账户已被禁用，请联系您的管理员。","tail":"RAW_RESPONSE_END"}',
            )
        elif host == "qz.sii.edu.cn" and path == "/callback":
            headers["Set-Cookie"] = (
                "inspire-session=fake-session-secret; Path=/; Secure; HttpOnly"
            )
        elif host == "qz.sii.edu.cn" and path == "/api/v2/user":
            headers["Content-Type"] = "application/json"
            result = {
                "id": "fake-user-id",
                "extra_info": {"login_name": _USER, "institution_id": _USER},
            }
            if self.scenario == "html200":
                headers["Content-Type"], body = "text/html", "<html>login page</html>"
            elif self.scenario == "error200":
                body = json.dumps(
                    {
                        "ResponseMetadata": {
                            "Error": {
                                "Code": "AccessForbidden",
                                "Message": "fake business failure",
                            }
                        }
                    }
                )
            elif self.scenario == "wrong_user":
                result["extra_info"] = {"login_name": "fake-other-user"}
            elif self.scenario == "missing_user":
                result = {}
            elif self.scenario == "verify401":
                status = 401
            if not body:
                body = json.dumps({"Result": result})
        else:
            raise AssertionError(
                f"unexpected auth request: {request.method} {host}{path}"
            )
        response = requests.Response()
        response.status_code, response.headers, response.url = (
            status,
            headers,
            request.url,
        )
        response.request, response.encoding = request, "utf-8"
        response._content = body.encode()
        message = email.message.Message()
        for key, value in headers.items():
            message[key] = value
        response.raw = io.BytesIO(response._content)
        response.raw._original_response = SimpleNamespace(msg=message)
        return response

    @property
    def password_posts(self):
        return [
            r
            for r in self.seen
            if r.method == "POST" and urlsplit(r.url).hostname == "cas.sii.edu.cn"
        ]

    def session(self):
        session = _REAL_SESSION()
        session.mount("https://", self)
        session.mount("http://", self)
        return session


class LoginEvidenceTests(unittest.TestCase):
    def run_login(
        self,
        scenario="captcha",
        args=(),
        cfg=None,
        env=None,
        env_file=None,
        malformed=False,
        cookie=None,
    ):
        transport = AuthTransport(scenario)
        cfg = cfg if cfg is not None else {"username": _USER, "password": _GOOD}
        with sandbox_home(
            config_json=cfg, env=env, cookie=json.dumps(cookie) if cookie else None
        ) as home:
            os.environ["QZCLI_OPS_LOG"] = str(home / "ops.log")
            if env_file is not None:
                destination = Path(
                    os.environ.get("QZCLI_ENV_FILE") or str(home / ".env")
                ).expanduser()
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(env_file)
            if malformed:
                config.CONFIG_FILE.write_text("{ malformed JSON")
            out, err = io.StringIO(), io.StringIO()
            with mock.patch.object(
                api.requests, "Session", side_effect=transport.session
            ), mock.patch.object(api, "_api_instance", None), mock.patch.object(
                api, "encrypt_password", wraps=api.encrypt_password
            ) as encrypt, mock.patch.object(
                sys, "argv", ["qzcli", "--auth-mode", "legacy", "login", *args]
            ), contextlib.redirect_stdout(
                out
            ), contextlib.redirect_stderr(
                err
            ):
                rc = cli.main()
            stored = config.get_cookie()
            encrypted_input = encrypt.call_args[0][0] if encrypt.called else None
            return (
                rc,
                out.getvalue(),
                err.getvalue(),
                transport,
                encrypted_input,
                stored,
            )

    def test_wrong_password_from_cli_and_config_preserves_full_evidence(self):
        for scenario in ("password", "captcha", "initial_captcha"):
            for source in ("cli", "config"):
                with self.subTest(scenario=scenario, source=source):
                    args = ["-u", _USER, "-p", _BAD] if source == "cli" else []
                    cfg = {
                        "username": _USER,
                        "password": _GOOD if source == "cli" else _BAD,
                    }
                    rc, out, err, t, pw, stored = self.run_login(scenario, args, cfg)
                    self.assertEqual(1, rc)
                    self.assertEqual(_BAD, pw)
                    self.assertEqual(1, len(t.password_posts))
                    self.assertIsNone(stored)
                    self.assertIn("RAW_RESPONSE_END", err)
                    self.assertIn("完整错误尾句", err)
                    self.assertIn("HTTP 200", err)
                    self.assertIn("X-Fixture-Request: fake-request-id", err)
                    self.assertIn("Traceback (most recent call last)", err)
                    self.assertNotIn("等几分钟", out + err)
                    self.assertNotIn("暂时要求验证码", out + err)
                    if scenario in {"captcha", "initial_captcha"}:
                        self.assertIn("提示（平台已确认）", err)
                        self.assertIn("IP 和 User-Agent", err)
                        self.assertIn("连续登录失败 3 次", err)
                        self.assertIn("验证成功或缓存 30 分钟过期后解除", err)
                        self.assertIn("尚不支持图形验证码输入", err)
                    else:
                        self.assertNotIn("提示（平台已确认）", err)
                    self.assertNotIn("RAW_RESPONSE_END", out)
                    self.assertNotIn(_BAD, out + err)
                    self.assertIn(
                        "命令行 --username" if source == "cli" else "config.json", out
                    )

    def test_env_conflict_sends_no_password(self):
        rc, out, err, t, pw, stored = self.run_login(env={"QZCLI_PASSWORD": _BAD})
        self.assertEqual(1, rc)
        self.assertEqual([], t.seen)
        self.assertIsNone(pw)
        self.assertIn("已阻止", out + err)

    def test_explicit_source_and_env_files(self):
        variants = [
            (["--source", "env"], {"QZCLI_PASSWORD": _BAD}, None),
            (
                ["--source", "envfile"],
                None,
                "QZCLI_USERNAME=" + _USER + "\nQZCLI_PASSWORD=" + _BAD + "\n",
            ),
        ]
        for args, env, env_file in variants:
            with self.subTest(args=args):
                rc, out, err, t, pw, stored = self.run_login(
                    args=args, env=env, env_file=env_file
                )
                self.assertEqual(_BAD, pw)
                self.assertEqual(1, len(t.password_posts))
                self.assertIn("RAW_RESPONSE_END", err)

    def test_custom_env_file_under_ssh_is_only_used_when_explicitly_selected(self):
        rc, out, err, t, pw, stored = self.run_login(
            args=["--source", "envfile"],
            env={"QZCLI_ENV_FILE": "~/.ssh/qzcli-fixture.env"},
            env_file="QZCLI_USERNAME=" + _USER + "\nQZCLI_PASSWORD=" + _BAD + "\n",
        )
        self.assertEqual(_BAD, pw)
        self.assertEqual(1, len(t.password_posts))
        self.assertIn("RAW_RESPONSE_END", err)

    def test_failed_verification_preserves_the_old_cookie(self):
        import time

        old = {
            "cookie": "inspire-session=fake-old-cookie",
            "workspace_id": "fake-workspace",
            "saved_at": time.time(),
        }
        rc, out, err, t, pw, stored = self.run_login("wrong_user", cookie=old)
        self.assertEqual(1, rc)
        self.assertEqual(old, stored)

    def test_malformed_config_keeps_original_exception(self):
        rc, out, err, t, pw, stored = self.run_login(malformed=True)
        self.assertEqual(1, rc)
        self.assertIn("JSONDecodeError", err)
        self.assertIn("config.json", err)
        self.assertEqual([], t.seen)

    def test_post_timeout_and_redirect_never_replay_password(self):
        for scenario in ("timeout", "redirect307", "keycloak400"):
            with self.subTest(scenario=scenario):
                rc, out, err, t, pw, stored = self.run_login(scenario)
                self.assertEqual(1, rc)
                self.assertEqual(1, len(t.password_posts))
                self.assertEqual(
                    1, sum(r.url == "https://qz.sii.edu.cn/" for r in t.seen)
                )
                self.assertIsNone(stored)
                if scenario == "timeout":
                    self.assertIn("ReadTimeout", err)
                    self.assertIn("结果未知", err)
                if scenario == "keycloak400":
                    self.assertIn("账户已被禁用", err)
                    self.assertIn("RAW_RESPONSE_END", err)
                self.assertNotIn("fake-ticket-secret", err)

    def test_fake_200_responses_never_save_a_cookie(self):
        for scenario in (
            "html200",
            "error200",
            "wrong_user",
            "missing_user",
            "verify401",
        ):
            with self.subTest(scenario=scenario):
                rc, out, err, t, pw, stored = self.run_login(scenario)
                self.assertEqual(1, rc)
                self.assertIsNone(stored)
                self.assertNotIn("登录成功", out)
                self.assertEqual(1, len(t.password_posts))
                self.assertNotIn("fake-session-secret", out + err)
                self.assertNotIn("fake-code-secret", out + err)

    def test_automatic_relogin_keeps_the_original_401_and_cas_response(self):
        transport = AuthTransport("captcha")
        with sandbox_home(
            config_json={"username": _USER, "password": _GOOD},
            cookie=json.dumps({"cookie": "inspire-session=fake-stale-cookie"}),
        ) as home:
            os.environ["QZCLI_OPS_LOG"] = str(home / "ops.log")
            out, err = io.StringIO(), io.StringIO()
            response = api._CurlResponse(
                status_code=401,
                text="ORIGINAL_401_END",
                headers={"Content-Type": "text/plain"},
                url="https://qz.sii.edu.cn/api/v2/project",
            )
            with mock.patch.object(
                api.requests, "Session", side_effect=transport.session
            ), mock.patch.object(api, "_api_instance", None), mock.patch.object(
                api, "_curl_post", return_value=response
            ), mock.patch.object(
                sys, "argv", ["qzcli", "--auth-mode", "legacy", "ws"]
            ), contextlib.redirect_stdout(
                out
            ), contextlib.redirect_stderr(
                err
            ):
                rc = cli.main()
            self.assertEqual(1, rc)
            self.assertEqual(1, len(transport.password_posts))
            self.assertIn("ORIGINAL_401_END", err.getvalue())
            self.assertIn("RAW_RESPONSE_END", err.getvalue())
            self.assertIn("config.json", err.getvalue())
            self.assertNotIn(_GOOD, out.getvalue() + err.getvalue())

    def test_success_requires_real_user_probe_and_does_not_echo_cookie(self):
        rc, out, err, t, pw, stored = self.run_login("success")
        self.assertEqual(0, rc, err)
        self.assertEqual("inspire-session=fake-session-secret", stored["cookie"])
        self.assertEqual(1, sum(urlsplit(r.url).path == "/api/v2/user" for r in t.seen))
        self.assertNotIn("fake-session-secret", out + err)
        self.assertEqual("", err)


class DiagnosticDetailsTests(unittest.TestCase):
    def test_form_errors_are_scoped_and_nested_text_is_complete(self):
        page = CasPage(
            "<form id='fm2'><div class='form-error'>other error</div></form><form id='fm1'><div class='x form-error'><span>first</span><br/>last &amp; tail</div></form>"
        )
        self.assertEqual("first last & tail", page.error)

    def test_redaction_keeps_the_rest_of_the_body(self):
        trace = AuthDiagnostic()
        trace.remember("fake-plain-secret#")
        raw = """Set-Cookie: session=fake-cookie-secret
<input value='fake-execution-secret' name='execution' type='hidden'>
{"access_token":"fake-token-secret","message":"完整信息 RAW_RESPONSE_END"}
https://example.invalid/?ticket=fake-ticket-secret&state=fake-state-secret
fake-plain-secret#"""
        result = trace.redact(raw)
        for value in (
            "fake-cookie-secret",
            "fake-execution-secret",
            "fake-token-secret",
            "fake-ticket-secret",
            "fake-state-secret",
            "fake-plain-secret#",
        ):
            self.assertNotIn(value, result)
        self.assertIn("完整信息 RAW_RESPONSE_END", result)

    def test_cookie_path_and_business_error_code_are_preserved(self):
        trace = AuthDiagnostic()
        response = SimpleNamespace(
            status_code=200,
            url="https://cas.sii.edu.cn/cas/login",
            headers={
                "Set-Cookie": "JSESSIONID=fake-cookie-secret; Path=/cas/; HttpOnly"
            },
            text='<img src="/cas/captcha.jpg">',
        )
        trace.record(response, "GET", response.url)
        self.assertIn("/cas/captcha.jpg", trace.render())
        self.assertIn(
            '"Code":"AccessForbidden"', trace.redact('{"Code":"AccessForbidden"}')
        )
        self.assertNotIn("fake-cookie-secret", trace.render())

    def test_redaction_preserves_public_hidden_fields_and_masks_unquoted_secrets(self):
        raw = """<input name=execution type=hidden value=fake-execution-secret>
<input name=loginType type=hidden value=1>
<input name='encrypted' type='hidden' value='true'>
<input name='_eventId' type='hidden' value='submit'>
{"authSessionId":"fake-auth-session-secret"} fake-execution-secret"""
        result = AuthDiagnostic().redact(raw)
        self.assertNotIn("fake-execution-secret", result)
        self.assertNotIn("fake-auth-session-secret", result)
        self.assertIn("name=loginType type=hidden value=1", result)
        self.assertIn("name='encrypted' type='hidden' value='true'", result)
        self.assertIn("name='_eventId' type='hidden' value='submit'", result)

    def test_probe_programming_error_is_not_turned_into_false(self):
        client = api.QzAPI.__new__(api.QzAPI)
        client.auth_mode = "legacy"
        client.base_url = "https://qz.sii.edu.cn"
        session = mock.Mock()
        session.cookies = []
        session.post.side_effect = AttributeError("fixture programming bug")
        with self.assertRaisesRegex(AttributeError, "programming bug"):
            client._session_is_authenticated(session, "fake-cookie", _USER)

    def test_lock_does_not_replace_a_body_oserror(self):
        with sandbox_home():
            with self.assertRaisesRegex(OSError, "fixture body IO failure"):
                with api._relogin_file_lock():
                    raise OSError("fixture body IO failure")

    def test_configuration_permission_error_is_preserved(self):
        with sandbox_home(config_json={"username": _USER, "password": _GOOD}):
            original_open = open

            def denied(path, *a, **kw):
                if Path(path) == config.CONFIG_FILE:
                    raise PermissionError(13, "fixture permission denied", str(path))
                return original_open(path, *a, **kw)

            with mock.patch("builtins.open", side_effect=denied):
                with self.assertRaises(config.ConfigError) as caught:
                    config.load_config()
            self.assertIsInstance(caught.exception.__cause__, PermissionError)
            self.assertIn("config.json", format_exception(caught.exception))

    def test_preflight_error_propagates_before_fanout(self):
        with sandbox_home(), mock.patch.object(
            api, "_recent_relogin_failure", return_value=None
        ):
            client = api.QzAPI(username=_USER, password=_GOOD, auth_mode="legacy")
            with mock.patch.object(
                client, "get_user_detail", side_effect=ValueError("bad JSON")
            ):
                with self.assertRaisesRegex(ValueError, "bad JSON"):
                    client.ensure_authenticated("fake-cookie")
