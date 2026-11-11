"""Regression test for #35: login must not downgrade socks5h:// → socks5://.

`login_with_cas` used to rewrite the configured proxy with
``proxy.replace("socks5h://", "socks5://")``, which silently disabled
remote-DNS SOCKS resolution. In environments where only the proxy can
resolve ``qz.sii.edu.cn`` (WSL/VPN), that broke login entirely. The proxy
string must reach ``session.proxies`` verbatim.
"""

import unittest
from unittest.mock import patch

import requests

from qzcli import api


class _FakeSession:
    """Minimal requests.Session stand-in that records .proxies then aborts.

    ``.get`` raises RequestException so ``login_with_cas`` returns early
    (as QzTransientError) right after the proxy block — we only care that
    the proxy string was assigned unchanged.
    """

    instances = []

    def __init__(self):
        self.trust_env = True
        self.proxies = {}
        self.headers = {}
        self.cookies = []
        _FakeSession.instances.append(self)

    def update(self, *a, **k):  # not used; headers.update path
        pass

    def get(self, *a, **k):
        raise requests.RequestException("aborted after proxy setup")


class _FakeHeaders(dict):
    def update(self, *a, **k):
        super().update(*a, **k)


class LoginProxyPreservationTests(unittest.TestCase):
    def setUp(self):
        _FakeSession.instances = []

    def _run_login(self, proxy_value):
        # headers.update is called on session.headers; give it a dict.
        def _session_factory():
            s = _FakeSession()
            s.headers = _FakeHeaders()
            return s

        with patch.object(api, "get_api_base_url", return_value="https://qz.sii.edu.cn"), \
                patch.object(api, "get_proxy", return_value=proxy_value), \
                patch.object(api.requests, "Session", _session_factory):
            client = api.QzAPI(username="u", password="p", auth_mode="legacy")
            # login_with_cas retries transient errors internally, so it may
            # raise QzTransientError after several attempts (each builds a
            # fresh session). We assert on every session created.
            with self.assertRaises(api.QzTransientError):
                client.login_with_cas("u", "p")
        self.assertGreaterEqual(len(_FakeSession.instances), 1)
        return _FakeSession.instances

    def test_socks5h_preserved_verbatim(self):
        sessions = self._run_login("socks5h://127.0.0.1:7897")
        # The whole point of #35: no downgrade to socks5://.
        for sess in sessions:
            self.assertEqual(
                {
                    "http": "socks5h://127.0.0.1:7897",
                    "https": "socks5h://127.0.0.1:7897",
                },
                sess.proxies,
            )
            self.assertFalse(sess.trust_env)  # env proxies still disabled

    def test_plain_socks5_unchanged(self):
        sessions = self._run_login("socks5://127.0.0.1:7897")
        for sess in sessions:
            self.assertEqual(
                {
                    "http": "socks5://127.0.0.1:7897",
                    "https": "socks5://127.0.0.1:7897",
                },
                sess.proxies,
            )

    def test_no_proxy_leaves_proxies_untouched(self):
        sessions = self._run_login("")
        for sess in sessions:
            self.assertEqual({}, sess.proxies)
            self.assertTrue(sess.trust_env)  # no proxy → trust_env not flipped


if __name__ == "__main__":
    unittest.main()
