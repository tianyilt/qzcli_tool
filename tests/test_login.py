import argparse
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from qzcli import cli, config


def build_fixture_value(label: str) -> str:
    return f"fixture-{label}"


class FakeDisplay:
    def __init__(self):
        self.messages = []

    def print(self, *args, **kwargs):
        self.messages.append(" ".join(str(arg) for arg in args))

    def print_error(self, *args, **kwargs):
        self.messages.append(" ".join(str(arg) for arg in args))

    def print_success(self, *args, **kwargs):
        self.messages.append(" ".join(str(arg) for arg in args))


class FakeLoginAPI:
    def __init__(self):
        self.calls = []

    def login_with_cas(self, username, password):
        self.calls.append((username, password))
        return "cookie-value"


class LoginConfigTests(unittest.TestCase):
    def test_get_credentials_reads_qzcli_dotenv_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            env_username = build_fixture_value("env-user")
            env_auth = build_fixture_value("env-auth")
            username_key = "QZCLI_" + "USERNAME"
            password_key = "QZCLI_" + "PASSWORD"
            env_file = Path(tmpdir) / ".env"
            env_file.write_text(
                "\n".join(
                    [
                        "# shell-compatible env for qzcli workflow",
                        f"export {username_key}={env_username}",
                        f"{password_key}='{env_auth}'",
                        "QZCLI_API_URL=https://qz.example.test",
                    ]
                ),
                encoding="utf-8",
            )

            with patch.dict(os.environ, {"QZCLI_ENV_FILE": str(env_file)}, clear=True):
                self.assertEqual(
                    (env_username, env_auth),
                    config.get_credentials(),
                )
                self.assertEqual("https://qz.example.test", config.get_api_base_url())


class LoginCommandTests(unittest.TestCase):
    def test_cmd_login_uses_saved_credentials_without_prompt(self):
        saved_user = build_fixture_value("saved-user")
        saved_auth = build_fixture_value("saved-auth")
        args = argparse.Namespace(
            username=None,
            password=None,
            password_stdin=False,
            workspace="ws-1",
        )
        display = FakeDisplay()
        api = FakeLoginAPI()

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(
            cli, "get_credentials", return_value=(saved_user, saved_auth)
        ), patch.object(
            cli, "save_cookie"
        ) as mock_save_cookie, patch(
            "builtins.input",
            side_effect=AssertionError("username prompt should not run"),
        ), patch(
            "getpass.getpass",
            side_effect=AssertionError("password prompt should not run"),
        ):
            ret = cli.cmd_login(args)

        self.assertEqual(0, ret)
        self.assertEqual([(saved_user, saved_auth)], api.calls)
        mock_save_cookie.assert_called_once_with("cookie-value", workspace_id="ws-1")

    def test_cmd_login_password_stdin_empty_does_not_fallback_to_saved_password(self):
        saved_user = build_fixture_value("saved-user")
        saved_auth = build_fixture_value("saved-auth")
        args = argparse.Namespace(
            username="cli-user",
            password=None,
            password_stdin=True,
            workspace=None,
        )
        display = FakeDisplay()
        api = FakeLoginAPI()

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(
            cli, "get_credentials", return_value=(saved_user, saved_auth)
        ), patch.object(
            sys, "stdin", io.StringIO("")
        ):
            ret = cli.cmd_login(args)

        self.assertEqual(1, ret)
        self.assertEqual([], api.calls)
        self.assertTrue(any("未从 stdin 读取到密码" in msg for msg in display.messages))

    def test_cmd_login_cli_password_wins_over_password_stdin(self):
        cli_user = build_fixture_value("cli-user")
        cli_auth = build_fixture_value("cli-auth")
        saved_user = build_fixture_value("saved-user")
        saved_auth = build_fixture_value("saved-auth")
        stdin_auth = build_fixture_value("stdin-auth")
        args = argparse.Namespace(
            username=cli_user,
            password=cli_auth,
            password_stdin=True,
            workspace=None,
        )
        display = FakeDisplay()
        api = FakeLoginAPI()

        with patch.object(cli, "get_display", return_value=display), patch.object(
            cli, "get_api", return_value=api
        ), patch.object(
            cli, "get_credentials", return_value=(saved_user, saved_auth)
        ), patch.object(
            cli, "save_cookie"
        ), patch.object(
            sys, "stdin", io.StringIO(f"{stdin_auth}\n")
        ):
            ret = cli.cmd_login(args)

        self.assertEqual(0, ret)
        self.assertEqual([(cli_user, cli_auth)], api.calls)


if __name__ == "__main__":
    unittest.main()
