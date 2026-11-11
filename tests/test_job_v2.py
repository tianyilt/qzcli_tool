"""v2 Console API: create_job_v2 endpoint/headers + 响应封装 _unwrap_v2_result
+ _v2_then_v1 回落判据。"""

import threading
import unittest
from unittest import mock

from qzcli import api
from qzcli.api import QzAPIError, _unwrap_v2_result


class UnwrapV2Tests(unittest.TestCase):
    def test_result_dict_returned(self):
        self.assertEqual(
            _unwrap_v2_result({"Result": {"job_id": "j-1"}}), {"job_id": "j-1"}
        )

    def test_legacy_data_fallback(self):
        self.assertEqual(
            _unwrap_v2_result({"data": {"job_id": "j-2"}}), {"job_id": "j-2"}
        )

    def test_response_metadata_error_raises(self):
        with self.assertRaises(QzAPIError) as cm:
            _unwrap_v2_result(
                {
                    "ResponseMetadata": {
                        "Error": {"Code": "InvalidNode", "Message": "node not ready"}
                    }
                }
            )
        self.assertIn("InvalidNode", str(cm.exception))
        self.assertIn("node not ready", str(cm.exception))

    def test_legacy_code_nonzero_raises(self):
        with self.assertRaises(QzAPIError):
            _unwrap_v2_result({"code": 500, "message": "boom"})

    def test_code_zero_ok(self):
        self.assertEqual(_unwrap_v2_result({"code": 0, "Result": {"ok": 1}}), {"ok": 1})

    def test_empty_when_no_payload(self):
        self.assertEqual(_unwrap_v2_result({"Result": None}), {})
        self.assertEqual(_unwrap_v2_result("nope"), {})


class _Resp:
    def __init__(self, status, payload, content_type="application/json"):
        self.status_code = status
        self._payload = payload
        self.headers = {"Content-Type": content_type}
        self.text = str(payload)

    def json(self):
        return self._payload


class CreateJobV2Tests(unittest.TestCase):
    def _api(self, username=None, password=None):
        """绕开 __init__ 构造客户端，不读配置也不碰凭据。

        ``create_job_v2`` 现在走 ``_request_v2``，因此继承了 ``@with_auth_retry`` ——
        401 时会调 ``_relogin()``。这里把凭据相关字段补齐：默认无凭据，
        ``_relogin`` 返回 None，原始 401 照常抛出。
        """
        a = api.QzAPI.__new__(api.QzAPI)
        a.auth_mode = "legacy"
        a.base_url = "https://qz.sii.edu.cn"
        a._username = username
        a._password = password
        a._token = None
        a._auto_relogin = True
        a._relogin_lock = threading.Lock()
        return a

    def test_posts_v2_console_endpoint_with_headers(self):
        seen = {}

        def fake_post(url, *, json=None, headers=None, params=None, timeout=60, **_):
            seen["url"] = url
            seen["params"] = params
            seen["headers"] = headers
            seen["body"] = json
            return _Resp(200, {"Result": {"job_id": "j-9"}})

        with mock.patch.object(api, "_curl_post", side_effect=fake_post):
            out = self._api().create_job_v2("ck", {"workspace_id": "ws-1", "name": "t"})

        self.assertEqual(out, {"job_id": "j-9"})
        # 走 _request_v2 之后 Action 从 URL 字面量变成 params
        self.assertEqual(seen["url"], "https://qz.sii.edu.cn/api/v2/train")
        self.assertEqual(seen["params"], {"Action": "CreateJobConsole"})
        self.assertEqual(seen["headers"]["cookie"], "ck")
        self.assertIn("Mozilla", seen["headers"]["user-agent"])
        self.assertIn("distributedTraining", seen["headers"]["referer"])
        self.assertEqual(seen["body"]["workspace_id"], "ws-1")
        # 折进 _request_v2 的正收益：以前 create_job_v2 手搓 header，
        # 漏了这个头 —— 缺它 APISIX 会把请求 302 到 Keycloak。
        self.assertEqual(
            seen["headers"]["x-inspire-client-source"], api.V2_CLIENT_SOURCE
        )
        self.assertNotIn("Authorization", seen["headers"])

    def test_explicit_cookie_wins_over_disk(self):
        """create_job_v2 的 cookie 是上层传进来的，不该被磁盘上的覆盖。"""
        seen = {}

        def fake_post(url, *, headers=None, **_):
            seen["cookie"] = headers["cookie"]
            return _Resp(200, {"Result": {"job_id": "j-1"}})

        with mock.patch.object(
            api, "_curl_post", side_effect=fake_post
        ), mock.patch.object(api, "get_cookie", return_value={"cookie": "disk-cookie"}):
            self._api().create_job_v2("explicit-cookie", {})
        self.assertEqual(seen["cookie"], "explicit-cookie")

    def test_401_raises_when_no_credentials(self):
        with mock.patch.object(api, "_curl_post", return_value=_Resp(401, {})):
            with self.assertRaises(QzAPIError):
                self._api().create_job_v2("ck", {})

    def test_401_triggers_relogin_and_retries(self):
        """折进 _request_v2 的另一个正收益：以前 create_job_v2 没挂
        @with_auth_retry，提交中途 cookie 过期就直接失败。"""
        calls = []

        def fake_post(url, *, headers=None, **_):
            calls.append(headers["cookie"])
            if len(calls) == 1:
                return _Resp(401, {})
            return _Resp(200, {"Result": {"job_id": "j-2"}})

        client = self._api(username="u", password="p")
        with mock.patch.object(
            api, "_curl_post", side_effect=fake_post
        ), mock.patch.object(client, "_relogin", return_value="fresh-cookie"):
            out = client.create_job_v2("stale-cookie", {})

        self.assertEqual(out, {"job_id": "j-2"})
        self.assertEqual(calls, ["stale-cookie", "fresh-cookie"])

    def test_404_raises_not_routed(self):
        """网关未注册路由回 404 text/plain —— 要报成 404，而不是"认证失败"，
        否则 _v2_then_v1 无法判断该回落 v1。"""
        resp = _Resp(404, None, content_type="text/plain; charset=utf-8")
        with mock.patch.object(api, "_curl_post", return_value=resp):
            with self.assertRaises(QzAPIError) as cm:
                self._api().create_job_v2("ck", {})
        self.assertEqual(cm.exception.code, 404)

    def test_v2_error_envelope_raises(self):
        payload = {"ResponseMetadata": {"Error": {"Code": "X", "Message": "bad"}}}
        with mock.patch.object(api, "_curl_post", return_value=_Resp(200, payload)):
            with self.assertRaises(QzAPIError):
                self._api().create_job_v2("ck", {})


class V2ThenV1Tests(unittest.TestCase):
    """_v2_then_v1 的回落判据 —— 这是迁移期最容易写错的一段逻辑。"""

    def setUp(self):
        api._V2_FALLBACK_WARNED.clear()

    def test_v2_success_never_calls_v1(self):
        v1 = mock.Mock()
        out = api._v2_then_v1("t", lambda: {"ok": 1}, v1)
        self.assertEqual(out, {"ok": 1})
        v1.assert_not_called()

    def test_404_falls_back(self):
        def v2():
            raise QzAPIError("no route", 404)

        out = api._v2_then_v1("t", v2, lambda: {"from": "v1"}, logger=lambda m: None)
        self.assertEqual(out, {"from": "v1"})

    def test_non_json_falls_back(self):
        def v2():
            raise QzAPIError("v2 API 返回非 JSON（200, content-type=text/html）")

        out = api._v2_then_v1("t", v2, lambda: {"from": "v1"}, logger=lambda m: None)
        self.assertEqual(out, {"from": "v1"})

    def test_business_error_does_not_fall_back(self):
        """AccessForbidden 说明 v2 通了但权限/参数不对 —— 回落 v1 会把问题藏起来。"""
        v1 = mock.Mock()

        def v2():
            raise QzAPIError("API 请求失败: AccessForbidden: Access denied")

        with self.assertRaises(QzAPIError):
            api._v2_then_v1("t", v2, v1)
        v1.assert_not_called()

    def test_401_does_not_fall_back(self):
        """401 归 with_auth_retry 处理（重登），不该静默降级到 v1。"""
        v1 = mock.Mock()

        def v2():
            raise QzAPIError("cookie expired", 401)

        with self.assertRaises(QzAPIError):
            api._v2_then_v1("t", v2, v1)
        v1.assert_not_called()

    def test_warns_once_per_endpoint(self):
        msgs = []

        def v2():
            raise QzAPIError("no route", 404)

        for _ in range(3):
            api._v2_then_v1("same", v2, lambda: {}, logger=msgs.append)
        self.assertEqual(len(msgs), 1)


if __name__ == "__main__":
    unittest.main()
