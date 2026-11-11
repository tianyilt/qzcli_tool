"""Provider decisions, real encrypted store IO, renewal and API error contracts."""

import base64
import concurrent.futures
import json
import os
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from qzcli import api, config
from qzcli.auth_diag import AuthDiagnostic, format_exception, has_auth_diagnostic
from qzcli.qz_auth import LoginRequired, OfficialAuth, OfficialAuthError, Token

NOW = datetime(2026, 11, 11, 8, tzinfo=timezone.utc).timestamp()


def fake_token(exp, label="access"):
    claims = (
        base64.urlsafe_b64encode(
            json.dumps({"exp": exp, "sub": "fake-user", "jti": label}).encode()
        )
        .decode()
        .rstrip("=")
    )
    return "fake-header." + claims + ".fake-signature"


def write_store(root, access, refresh):
    root.mkdir(parents=True, exist_ok=True)
    key_file = root / ".encryption_key"
    key = (
        key_file.read_bytes()
        if key_file.exists()
        else AESGCM.generate_key(bit_length=256)
    )
    key_file.write_bytes(key)
    nonce = os.urandom(12)
    payload = json.dumps({"token": access, "refresh_token": refresh}).encode()
    (root / "credentials.enc").write_bytes(
        nonce + AESGCM(key).encrypt(nonce, payload, None)
    )


class OfficialAuthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve() / "official"
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)
        self.binary = patch("qzcli.qz_auth.shutil.which", return_value="/fake/bin/qz")
        self.binary.start()
        self.addCleanup(self.binary.stop)
        self.runner = Mock(
            return_value=subprocess.CompletedProcess(
                [], 0, '{"Result":{"id":"fake-user"}}', ""
            )
        )

    def auth(self):
        return OfficialAuth(self.root, now=lambda: NOW, runner=self.runner)

    def test_access_valid_with_any_refresh_state_needs_no_qz(self):
        for refresh in [
            "",
            fake_token(NOW - 1),
            fake_token(NOW + 7200),
            "fake-opaque-refresh",
        ]:
            with self.subTest(refresh_state=Token.parse(refresh).status(NOW)["state"]):
                access = fake_token(NOW + 3600)
                write_store(self.root, access, refresh)
                before = (self.root / "credentials.enc").read_bytes()
                self.assertEqual(self.auth().token(), access)
                self.assertEqual((self.root / "credentials.enc").read_bytes(), before)
        self.runner.assert_not_called()

    def test_missing_or_expired_access_and_unusable_refresh_require_login(self):
        for access in ["", fake_token(NOW - 1)]:
            for refresh in ["", fake_token(NOW - 1)]:
                write_store(self.root, access, refresh)
                with self.assertRaises(LoginRequired):
                    self.auth().token()
        self.runner.assert_not_called()

    def test_missing_directory_requires_login_without_creating_it(self):
        with self.assertRaises(LoginRequired):
            self.auth().token()
        self.assertFalse(self.root.exists())

    def test_expired_pair_rotates_using_official_probe_and_rereads(self):
        write_store(self.root, fake_token(NOW - 1), fake_token(NOW + 7200))
        access, refresh = fake_token(NOW + 3600, "new-access"), fake_token(
            NOW + 7200, "new-refresh"
        )
        self.runner.side_effect = lambda *a, **kw: (
            write_store(self.root, access, refresh)
            or subprocess.CompletedProcess([], 0, '{"Result":{"id":"fake-user"}}', "")
        )
        self.assertEqual(self.auth().token(), access)
        self.assertEqual(
            config.read_qz_credentials(self.root)["refresh_token"], refresh
        )
        command = self.runner.call_args.args[0]
        self.assertEqual(command[-2:], ["user", "GetUserDetail"])
        self.assertEqual(
            self.runner.call_args.kwargs["env"]["QZ_CLI_CONFIG_DIR"], str(self.root)
        )
        self.assertEqual(self.runner.call_count, 1)

    def test_unknown_expiration_is_not_expired(self):
        for value in [
            "fake-opaque",
            fake_token("unknown"),
            fake_token(float("inf")),
            fake_token(-1),
            fake_token(True),
        ]:
            self.assertEqual(Token.parse(value).status(NOW)["state"], "unknown")
        write_store(self.root, "fake-opaque-access", "")
        self.assertEqual(self.auth().token(), "fake-opaque-access")
        self.runner.assert_not_called()

    def test_401_reuses_external_update_before_touching_refresh_chain(self):
        old, new = fake_token(NOW + 100), fake_token(NOW + 100, "external-new")
        write_store(self.root, new, fake_token(NOW + 7200))
        self.assertEqual(self.auth().token(force_refresh=True, failing_token=old), new)
        self.runner.assert_not_called()

    def test_401_reuses_updated_access_even_without_refresh(self):
        new = fake_token(NOW + 3600, "external-new")
        write_store(self.root, new, "")
        self.assertEqual(
            self.auth().token(force_refresh=True, failing_token="fake-old"), new
        )
        self.runner.assert_not_called()

    def test_login_supports_old_and_new_official_flags(self):
        for flags in ["--auth-url string", "--scope string"]:
            self.runner.reset_mock()

            def run(command, **kwargs):
                if command[-1] == "--help":
                    return subprocess.CompletedProcess(command, 0, flags, "")
                write_store(self.root, fake_token(NOW + 3600), fake_token(NOW + 7200))
                return subprocess.CompletedProcess(command, 0)

            self.runner.side_effect = run
            self.auth().login()
            command = self.runner.call_args.args[0]
            self.assertEqual("--auth-url" in command, "--auth-url" in flags)
            self.assertNotIn("capture_output", self.runner.call_args.kwargs)

    def test_independent_providers_share_directory_lock(self):
        # Separate provider objects model separate callers, each with its own mutex.
        write_store(self.root, fake_token(NOW - 1), fake_token(NOW + 7200))
        new = fake_token(NOW + 3600, "new")

        def renew(*args, **kwargs):
            write_store(self.root, new, fake_token(NOW + 7200, "rotated"))
            return subprocess.CompletedProcess(
                [], 0, '{"Result":{"id":"fake-user"}}', ""
            )

        self.runner.side_effect = renew
        providers = [self.auth() for _ in range(8)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(lambda x: x.token(), providers))
        self.assertEqual(values, [new] * 8)
        self.assertEqual(self.runner.call_count, 1)

    def test_invalid_grant_is_preserved_redacted_and_deduplicated(self):
        access, refresh = fake_token(NOW - 1), fake_token(NOW + 7200)
        write_store(self.root, access, refresh)
        self.runner.return_value = subprocess.CompletedProcess(
            [], 1, "", "invalid_grant: Token is not active " + refresh
        )
        for _ in range(3):
            with self.assertRaises(OfficialAuthError) as ctx:
                self.auth().token()
            self.assertTrue(ctx.exception.action_required)
            self.assertIn("invalid_grant", str(ctx.exception))
            self.assertNotIn(refresh, str(ctx.exception))
        self.assertEqual(self.runner.call_count, 1)
        self.assertEqual(config.read_qz_credentials(self.root)["token"], access)

    def test_network_failure_does_not_request_login_or_delete_pair(self):
        write_store(self.root, fake_token(NOW - 1), fake_token(NOW + 7200))
        before = (self.root / "credentials.enc").read_bytes()
        self.runner.return_value = subprocess.CompletedProcess(
            [], 1, "", "dial tcp: DNS failure; ORIGINAL_END"
        )
        with self.assertRaises(OfficialAuthError) as ctx:
            self.auth().token()
        self.assertFalse(ctx.exception.action_required)
        self.assertIn("DNS failure; ORIGINAL_END", str(ctx.exception))
        self.assertEqual((self.root / "credentials.enc").read_bytes(), before)

    def test_external_login_overlapping_failed_refresh_is_reread(self):
        write_store(self.root, fake_token(NOW - 1), fake_token(NOW + 7200))
        new = fake_token(NOW + 3600, "independent-login")

        def overlap(*args, **kwargs):
            write_store(self.root, new, fake_token(NOW + 7200, "independent-refresh"))
            return subprocess.CompletedProcess([], 1, "", "invalid_grant")

        self.runner.side_effect = overlap
        self.assertEqual(self.auth().token(), new)

    def test_static_token_never_refreshes_or_logs_in(self):
        os.environ["INSPIRE_TOKEN"] = fake_token(NOW + 3600)
        self.assertEqual(self.auth().token(), os.environ["INSPIRE_TOKEN"])
        with self.assertRaises(LoginRequired):
            self.auth().token(
                force_refresh=True, failing_token=os.environ["INSPIRE_TOKEN"]
            )
        with self.assertRaises(OfficialAuthError):
            self.auth().login()
        self.runner.assert_not_called()

    def test_proxy_bridge_preserves_no_proxy_and_config_directory(self):
        os.environ["NO_PROXY"] = ".internal.example"
        with patch("qzcli.qz_auth.get_proxy", return_value="http://proxy.example:7897"):
            env = self.auth()._environment()
        self.assertEqual(env["https_proxy"], "http://proxy.example:7897")
        self.assertEqual(env["NO_PROXY"], ".internal.example")
        self.assertEqual(env["QZ_CLI_CONFIG_DIR"], str(self.root))

    def test_corrupt_store_is_explicit_and_not_overwritten(self):
        write_store(self.root, "fake-access", "fake-refresh")
        (self.root / ".encryption_key").write_bytes(b"wrong-key")
        before = (self.root / "credentials.enc").read_bytes()
        with self.assertRaises(config.ConfigError):
            self.auth().status()
        self.assertEqual((self.root / "credentials.enc").read_bytes(), before)
        self.runner.assert_not_called()


class OfficialApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.client = api.QzAPI(auth_mode="qz", qz_config_dir=self.tmp.name)
        self.provider = Mock()
        self.provider.server = "https://qz.sii.edu.cn"
        self.provider.token.return_value = "fake-access"
        self.provider.redact.side_effect = lambda text: str(text).replace(
            "fake-access", "[REDACTED]"
        )
        self.client._official_auth = self.provider

    def response(self, code, body):
        return api._CurlResponse(
            code, body if isinstance(body, str) else json.dumps(body)
        )

    def test_bearer_header_and_result_contract_without_cookie_or_cas(self):
        with patch.object(
            api,
            "_curl_post",
            return_value=self.response(200, {"Result": {"id": "fake-user"}}),
        ) as request, patch.object(self.client, "login_with_cas") as cas:
            self.assertEqual(self.client.get_user_detail(), {"id": "fake-user"})
        headers = request.call_args.kwargs["headers"]
        self.assertEqual(headers["authorization"], "Bearer fake-access")
        self.assertTrue(
            headers["x-inspire-client-source"].startswith("inspire-cli/qzcli-")
        )
        self.assertNotIn("cookie", headers)
        cas.assert_not_called()

    def test_401_has_one_refresh_and_one_authenticated_retry(self):
        self.provider.token.side_effect = ["fake-access", "fake-rotated"]
        with patch.object(
            api,
            "_curl_post",
            side_effect=[
                self.response(401, "original-401"),
                self.response(200, {"Result": {"id": "fake-user"}}),
            ],
        ) as request:
            self.assertEqual(self.client.get_user_detail()["id"], "fake-user")
        self.assertEqual(request.call_count, 2)
        self.provider.token.assert_called_with(
            force_refresh=True, failing_token="fake-access"
        )

    def test_second_401_keeps_raw_response_and_never_loops(self):
        with patch.object(
            api,
            "_curl_post",
            return_value=self.response(401, "<html>ORIGINAL_END</html>"),
        ) as request:
            with self.assertRaises(api.QzAPIError) as ctx:
                self.client.get_user_detail()
        self.assertEqual(request.call_count, 2)
        self.assertIn("ORIGINAL_END", format_exception(ctx.exception))

    def test_403_and_business_forbidden_are_permission_errors(self):
        for code, body in [
            (403, "ORIGINAL_403"),
            (
                200,
                {
                    "ResponseMetadata": {
                        "Error": {
                            "Code": "AccessForbidden",
                            "Message": "ORIGINAL_PERMISSION",
                        }
                    }
                },
            ),
        ]:
            self.provider.reset_mock()
            with patch.object(
                api, "_curl_post", return_value=self.response(code, body)
            ) as request:
                with self.assertRaises(api.QzAPIError) as ctx:
                    self.client.get_user_detail()
            self.assertEqual(request.call_count, 1)
            self.provider.token.assert_called_once_with()
            self.assertIn("ORIGINAL_", format_exception(ctx.exception))

    def test_transport_error_does_not_refresh_or_replay_create(self):
        with patch.object(
            api, "_curl_post", side_effect=OSError("TLS EOF ORIGINAL_END")
        ) as request:
            with self.assertRaisesRegex(OSError, "TLS EOF ORIGINAL_END"):
                self.client.create_job({"name": "fake-test"})
        self.assertEqual(request.call_count, 1)
        self.provider.token.assert_called_once_with()

    def test_new_mode_never_falls_back_to_password_or_v1(self):
        with patch.object(
            self.client,
            "_request_v2",
            side_effect=api.QzAPIError("no such action", 404),
        ), patch.object(self.client, "_get_job_detail_v1") as legacy:
            with self.assertRaises(api.QzAPIError):
                self.client.get_job_detail("job-fake")
        legacy.assert_not_called()

    def test_explicit_cookie_and_password_require_legacy(self):
        with self.assertRaises(api.QzAPIError):
            self.client.get_user_detail(cookie="fake-cookie")
        with self.assertRaises(api.QzAPIError):
            api.QzAPI(username="fake-user", password="fake-password", auth_mode="qz")

    def test_workspace_list_does_not_use_official_mine_scope(self):
        with patch.object(
            self.client, "_request_v2", return_value={"jobs": [], "total": 0}
        ) as request:
            self.client.list_jobs_with_cookie(
                "ws-fake", "", page_num=2, created_by="user-fake"
            )
        body = request.call_args.args[2]
        self.assertEqual(body["scope"], "TASK_LIST_SCOPE_ALL")
        self.assertEqual(body["created_by"], "user-fake")
        self.assertEqual(body["page_num"], 2)

    def test_denied_admin_metadata_preserves_error_and_uses_history(self):
        forbidden = {
            "ResponseMetadata": {
                "Error": {"Code": "AccessForbidden", "Message": "admin only"}
            }
        }
        with patch.object(
            api, "_curl_post", return_value=self.response(200, forbidden)
        ), patch.object(
            self.client, "_specs_from_job_history", return_value=[{"id": "fake-spec"}]
        ) as history, patch.object(
            self.client, "_get_token", side_effect=AssertionError("legacy token path")
        ):
            self.assertEqual(
                self.client.list_specs("lcg-fake", "ws-fake"), [{"id": "fake-spec"}]
            )
        history.assert_called_once_with("lcg-fake", "ws-fake")
        self.provider.token.assert_called_once_with()


class OfficialInitializationTests(unittest.TestCase):
    def test_init_is_idempotent_and_preserves_existing_state(self):
        import argparse
        from qzcli import cli

        with tempfile.TemporaryDirectory() as d, patch.object(
            config, "CONFIG_DIR", Path(d)
        ), patch.object(config, "CONFIG_FILE", Path(d) / "config.json"):
            config.save_config(
                {
                    "custom": "keep",
                    "default_workspace_id": "ws-fake",
                    "password": "fake-legacy-password",
                }
            )
            client = Mock(auth_mode="qz")
            client.base_url = "https://example.invalid"
            client._official_auth.directory = Path(d) / "official"
            client.auth_status.return_value = {
                "user": {"id": "fake-user", "name": "fake-name"}
            }
            with patch.object(cli, "get_api", return_value=client), patch.object(
                cli, "get_display"
            ):
                for _ in range(2):
                    self.assertEqual(
                        cli.cmd_init(argparse.Namespace(username="", password="")), 0
                    )
            stored = config.load_config()
            self.assertEqual(stored["custom"], "keep")
            self.assertEqual(stored["password"], "fake-legacy-password")
            self.assertEqual(stored["default_workspace_id"], "ws-fake")
            self.assertEqual(stored["auth_mode"], "qz")
            self.assertEqual((Path(d) / "config.json").stat().st_mode & 0o777, 0o600)
            client._official_auth.login.assert_not_called()

    def test_init_failure_does_not_save_or_log_in_for_network_error(self):
        import argparse
        from qzcli import cli

        client = Mock(auth_mode="qz")
        client._official_auth.token.side_effect = OfficialAuthError("DNS ORIGINAL_END")
        with patch.object(cli, "get_api", return_value=client), patch.object(
            cli, "get_display"
        ), patch.object(config, "save_config") as save:
            with self.assertRaisesRegex(OfficialAuthError, "DNS ORIGINAL_END"):
                cli.cmd_init(argparse.Namespace(username="", password=""))
        save.assert_not_called()
        client._official_auth.login.assert_not_called()

    def test_default_is_official_and_help_creates_no_state(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            env = os.environ.copy()
            env.update(
                HOME=d,
                QZCLI_HOME=str(root / "state"),
                QZ_CLI_CONFIG_DIR=str(root / "official"),
            )
            env.pop("QZCLI_AUTH_MODE", None)
            env.pop("INSPIRE_TOKEN", None)
            result = subprocess.run(
                [__import__("sys").executable, "-m", "qzcli.cli", "--help"],
                capture_output=True,
                text=True,
                env=env,
                timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(config.DEFAULT_CONFIG["auth_mode"], "qz")
            self.assertFalse((root / "state").exists())
            self.assertFalse((root / "official").exists())

    def test_mcp_does_not_require_cookie_for_official_provider(self):
        from qzcli import mcp_server

        client = Mock(auth_mode="qz")
        with patch.object(mcp_server, "get_api", return_value=client), patch.object(
            mcp_server, "get_cookie", side_effect=AssertionError("legacy Cookie read")
        ):
            cookie, state = mcp_server._require_cookie()
        self.assertEqual(cookie, "")
        self.assertEqual(state["auth_mode"], "qz")
        client.ensure_authenticated.assert_called_once_with(probe=False)

    def test_proxy_and_notebook_token_redaction(self):
        from qzcli.auth_diag import AuthDiagnostic
        from qzcli import diag

        # Synthetic data derived from the test name, never a service credential.
        proxy_user = "unit-user"
        proxy_password = self._testMethodName
        with patch.object(
            config,
            "get_proxy",
            return_value=f"http://{proxy_user}:{proxy_password}@proxy.example.invalid:7897",
        ):
            options = config.websocket_proxy_options("wss://notebook.example/api")
        self.assertEqual(options["http_proxy_host"], "proxy.example.invalid")
        self.assertEqual(options["http_proxy_auth"], (proxy_user, proxy_password))
        raw = "TLS error at https://nb.example/jupyter/notebook-fake/fake-sensitive-value/api/contents/a"
        redacted = AuthDiagnostic().redact(raw)
        self.assertNotIn("fake-sensitive-value", redacted)
        self.assertIn("TLS error", redacted)
        diag.swallowed("fake-test/redaction", RuntimeError(raw))
        self.assertNotIn("fake-sensitive-value", diag.last_reason("fake-test/"))


if __name__ == "__main__":
    unittest.main()
