"""Unit tests for `qzcli logs` (v2 GetJobLog) — pure-Python, no network."""

import argparse
import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

from qzcli.api import QzAPI, QzAPIError, V2_CLIENT_SOURCE
from qzcli.cli import _parse_since, cmd_logs
from qzcli.display import Display


def _entry(log_id, ts_ms, msg="hi", pod="job-x-worker-0"):
    return {
        "log_id": log_id,
        "message": msg,
        "node": "n01",
        "pod_name": pod,
        "time": "2026-05-02T19:22:00",
        "timestamp_ms": str(ts_ms),
        "timestamp_str": "2026-05-02 19:22:00",
    }


class ParseSinceTests(unittest.TestCase):
    def test_relative_units(self):
        self.assertIsNotNone(_parse_since("5m"))
        self.assertIsNotNone(_parse_since("2h"))
        self.assertIsNotNone(_parse_since("30s"))
        self.assertIsNotNone(_parse_since("1d"))
        # 5m should be smaller (later) ts than 1h
        self.assertGreater(int(_parse_since("5m")), int(_parse_since("1h")))

    def test_iso(self):
        self.assertEqual(_parse_since("2026-05-02T19:22:00"), str(int(__import__("datetime").datetime.fromisoformat("2026-05-02T19:22:00").timestamp() * 1000)))

    def test_bogus(self):
        self.assertIsNone(_parse_since("bogus"))
        self.assertIsNone(_parse_since(None))
        self.assertIsNone(_parse_since(""))


class ResolvePodNamesTests(unittest.TestCase):
    """实例名要**问平台**，不要猜命名约定。

    ## 真实故障（2026-08-27）

    这里原来只按 ``{job_id}-worker-{i}`` 拼。而平台真实的实例名是
    ``{job_id}-worker-{i}-{round}`` —— **末尾多一段**。少那一段，日志接口报：

        InvalidParameter: Invalid instance names, the job ids length of
        instances except 1, but got 0

    于是 ``qzcli logs`` 对**所有**任务全挂（实测扫 60 个任务 0 条日志），
    而报错文案跟真实原因（名字拼错了）毫无关系。修完 5/5 全部取到。

    v0.4.12 也一样挂 —— 不是新回归，是约定漂了之后一直没人对过。
    **这是第二次栽在"猜平台的命名约定"上**（上一次是 exec 的中转目录）。

    所以下面的用例分两层：平台给得出真名时**必须用真名**；
    只有平台那条路不通时才回落到拼接。
    """

    def setUp(self):
        with patch("qzcli.api.get_api_base_url", return_value="https://qz.test"), \
             patch("qzcli.api.get_credentials", return_value=("u", "p")):
            self.api = QzAPI("u", "p")
            self.api._token = "tok"

    def test_platform_names_win_over_convention(self):
        """**核心**：平台给了真名就用真名，哪怕它跟约定长得不一样。"""
        real = [
            {"name": "job-abc-worker-0-0"},
            {"name": "job-abc-worker-1-0"},
        ]
        with patch.object(self.api, "list_job_instances", return_value=real), \
             patch.object(self.api, "get_job_detail") as detail:
            self.assertEqual(
                self.api._resolve_pod_names("job-abc"),
                ["job-abc-worker-0-0", "job-abc-worker-1-0"],
            )
            detail.assert_not_called()

    def test_real_platform_shape_has_trailing_round(self):
        """钉住真机抓到的形状：末尾那个 ``-0`` 不能被当成噪声去掉。"""
        with patch.object(
            self.api,
            "list_job_instances",
            return_value=[{"name": "job-56d9fa68-worker-0-0"}],
        ):
            names = self.api._resolve_pod_names("job-56d9fa68")
        self.assertEqual(names, ["job-56d9fa68-worker-0-0"])
        self.assertNotEqual(names, ["job-56d9fa68-worker-0"], "退回猜的约定了")

    def test_falls_back_to_convention_when_platform_unavailable(self):
        """平台那条路不通时至少还能试一把 —— 但只在那时。"""
        with patch.object(
            self.api, "list_job_instances", side_effect=QzAPIError("boom")
        ), patch.object(
            self.api,
            "get_job_detail",
            return_value={"framework_config": [{"instance_count": 3}]},
        ):
            self.assertEqual(
                self.api._resolve_pod_names("job-abc"),
                ["job-abc-worker-0", "job-abc-worker-1", "job-abc-worker-2"],
            )

    def test_empty_platform_list_also_falls_back(self):
        """返回空列表和报错一样，都要回落 —— 否则日志接口会收到空的实例名。"""
        with patch.object(self.api, "list_job_instances", return_value=[]), \
             patch.object(self.api, "get_job_detail", return_value={"instance_count": 2}):
            self.assertEqual(
                self.api._resolve_pod_names("job-x"),
                ["job-x-worker-0", "job-x-worker-1"],
            )

    def test_default_one_when_missing(self):
        with patch.object(self.api, "list_job_instances", return_value=[]), \
             patch.object(self.api, "get_job_detail", return_value={}):
            self.assertEqual(self.api._resolve_pod_names("job-y"), ["job-y-worker-0"])

    def test_explicit_n_still_consults_platform_first(self):
        """显式传 n_instances 只影响**兜底**那条路，不该让我们跳过问平台。"""
        with patch.object(self.api, "list_job_instances", return_value=[]), \
             patch.object(self.api, "get_job_detail") as m:
            self.assertEqual(
                self.api._resolve_pod_names("job-z", n_instances=4),
                [f"job-z-worker-{i}" for i in range(4)],
            )
            m.assert_not_called()


class RequestV2HeadersTests(unittest.TestCase):
    """Verify body shape + that cookie auth + APISIX header are sent."""

    def setUp(self):
        with patch("qzcli.api.get_api_base_url", return_value="https://qz.test"), \
             patch("qzcli.api.get_credentials", return_value=("u", "p")):
            self.api = QzAPI("u", "p")

    def test_post_body_and_headers(self):
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.headers = {"Content-Type": "application/json"}
        fake_resp.json.return_value = {"ResponseMetadata": {}, "Result": {"logs": [], "total": 0}}

        with patch("qzcli.api.get_cookie", return_value={"cookie": "SESSION=abc"}), \
             patch("qzcli.api._curl_post", return_value=fake_resp) as post:
            result = self.api.get_job_logs(
                "job-abc",
                pod_names=["job-abc-worker-0"],
                start_timestamp_ms="111",
                page_size=50,
            )
            self.assertEqual(result, {"logs": [], "total": 0})
            kwargs = post.call_args.kwargs
            self.assertEqual(kwargs["params"], {"Action": "GetJobLog"})
            self.assertEqual(kwargs["headers"]["x-inspire-client-source"], V2_CLIENT_SOURCE)
            self.assertEqual(kwargs["headers"]["cookie"], "SESSION=abc")
            self.assertNotIn("Authorization", kwargs["headers"])
            body = kwargs["json"]
            self.assertEqual(body["filter"]["podNames"], ["job-abc-worker-0"])
            self.assertEqual(body["filter"]["start_timestamp_ms"], "111")
            self.assertEqual(body["page_size"], 50)
            # ascend so terminal output reads chronologically
            self.assertEqual(body["sorter"][0]["sort"], "ascend")

    def test_missing_cookie_raises_with_login_hint(self):
        from qzcli.api import QzAPIError
        with patch("qzcli.api.get_cookie", return_value=None):
            with self.assertRaises(QzAPIError) as ctx:
                self.api._request_v2("train", "GetJobLog", {})
            self.assertIn("qzcli login", str(ctx.exception))

    def test_html_response_raises_with_hint(self):
        from qzcli.api import QzAPIError
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.headers = {"Content-Type": "text/html"}
        fake_resp.text = "<html>keycloak login</html>"
        with patch("qzcli.api.get_cookie", return_value={"cookie": "x"}), \
             patch("qzcli.api._curl_post", return_value=fake_resp):
            with self.assertRaises(QzAPIError) as ctx:
                self.api._request_v2("train", "GetJobLog", {})
            self.assertIn("非 JSON", str(ctx.exception))

    def test_401_raises_cookie_expired(self):
        from qzcli.api import QzAPIError
        unauthorized = MagicMock(status_code=401, headers={"Content-Type": "application/json"})
        unauthorized.json.return_value = {"error": "unauthorized"}
        with patch("qzcli.api.get_cookie", return_value={"cookie": "x"}), \
             patch("qzcli.api._curl_post", return_value=unauthorized):
            with self.assertRaises(QzAPIError) as ctx:
                self.api._request_v2("train", "GetJobLog", {})
            self.assertIn("Cookie 已过期", str(ctx.exception))


class CmdLogsTailTests(unittest.TestCase):
    def _args(self, **overrides):
        ns = argparse.Namespace(
            job_id="job-x",
            tail=2,
            follow=False,
            interval=3.0,
            pod=None,
            since=None,
            raw=False,
            output_json=False,
        )
        for k, v in overrides.items():
            setattr(ns, k, v)
        return ns

    def test_single_shot_tail_truncation(self):
        # API returns 5 entries; tail=2 means only the last 2 should be printed.
        api = MagicMock()
        api.get_job_logs.return_value = {
            "logs": [_entry(f"L{i}", 1000 + i, msg=f"line{i}") for i in range(5)],
            "total": 5,
        }
        buf = io.StringIO()
        with patch("qzcli.cli.get_api", return_value=api), \
             patch("qzcli.cli.get_display", return_value=Display()), \
             redirect_stdout(buf):
            rc = cmd_logs(self._args(tail=2, raw=True))
        self.assertEqual(rc, 0)
        out = buf.getvalue().strip().splitlines()
        # raw mode prints just messages
        self.assertEqual(out, ["line3", "line4"])
        self.assertEqual(api.get_job_logs.call_args.kwargs["sort"], "descend")

    def test_pod_filter_passed_through(self):
        api = MagicMock()
        api.get_job_logs.return_value = {"logs": [], "total": 0}
        with patch("qzcli.cli.get_api", return_value=api), \
             patch("qzcli.cli.get_display", return_value=Display()):
            cmd_logs(self._args(pod="job-x-worker-2"))
        api.get_job_logs.assert_called_once()
        call = api.get_job_logs.call_args
        self.assertEqual(call.kwargs["pod_names"], ["job-x-worker-2"])


if __name__ == "__main__":
    unittest.main()
