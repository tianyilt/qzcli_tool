"""Tests for the job-events API + `qzcli events` command + status diagnosis.

Covers the InspireSkill-borrowed scheduling-diagnosis feature: the unified
events endpoint —— 现在优先 v2 ``train ListJobEvents``、路由不通回落
``/api/v1/train_job/events/list``（job vs instance via
``filter.object_type``), CLI filtering/formatting, and the queuing-reason line
appended to ``qzcli status``.
"""

import argparse
import io
import json
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from unittest import mock

from qzcli import api, cli

# ---- API layer ----


class _FakeResp:
    def __init__(
        self, status_code=200, payload=None, text="", content_type="application/json"
    ):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        # v2 路径会 sniff content-type 来识别「被 302 到 Keycloak 的 HTML」
        self.headers = {"Content-Type": content_type}

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


_JOB_EVENTS = [
    {
        "type": "Normal",
        "reason": "SuccessfulCreatePod",
        "message": "Created pod worker-0",
        "last_timestamp": "1000",
        "object_type": "job",
    },
    {
        "type": "Warning",
        "reason": "Unschedulable",
        "message": "0/680 nodes are unavailable: Insufficient cpu.",
        "last_timestamp": "2000",
        "object_type": "job",
    },
]


def _dated_events(count):
    base_ms = int(datetime(2026, 10, 9, 6, 0, tzinfo=timezone.utc).timestamp() * 1000)
    return [
        {
            "type": "Warning", "reason": "FailedScheduling",
            "message": f"queue diagnostic {i}",
            "first_timestamp": str(base_ms + i * 1000),
            "last_timestamp": str(base_ms + i * 1000),
            "object_id": "job-x-worker-0",
        }
        for i in range(count)
    ]


class JobEventsAPITests(unittest.TestCase):
    def _api(self):
        with mock.patch.object(
            api, "get_api_base_url", return_value="https://qz.sii.edu.cn"
        ), mock.patch.object(api, "get_credentials", return_value=("u", "p")):
            client = api.QzAPI()
            client._auto_relogin = False
            return client

    def test_job_events_hits_v2_first(self):
        """默认走 v2 ``train ListJobEvents``。"""
        captured = {}

        def _fake_post(url, json=None, headers=None, params=None, timeout=None):
            captured["url"] = url
            captured["params"] = params
            captured["body"] = json
            captured["headers"] = headers
            return _FakeResp(200, {"Result": {"events": _JOB_EVENTS, "total": 2}})

        with mock.patch.object(api, "_curl_post", side_effect=_fake_post):
            out = self._api().get_job_events_with_cookie("job-x", "cookie-v")

        self.assertEqual(captured["url"], "https://qz.sii.edu.cn/api/v2/train")
        self.assertEqual(captured["params"], {"Action": "ListJobEvents"})
        self.assertEqual(
            captured["body"]["filter"], {"object_type": "job", "object_ids": ["job-x"]}
        )
        self.assertEqual(captured["headers"]["cookie"], "cookie-v")
        self.assertEqual(out, _JOB_EVENTS)

    def test_falls_back_to_v1_when_v2_route_missing(self):
        """v2 路由不通时回落 v1，请求体一致、返回形状一致。"""
        urls = []

        def _fake_post(url, json=None, headers=None, params=None, timeout=None):
            urls.append(url)
            if "/api/v2/" in url:
                return _FakeResp(
                    404, None, text="404 page not found", content_type="text/plain"
                )
            return _FakeResp(
                200, {"code": 0, "data": {"events": _JOB_EVENTS, "total": 2}}
            )

        api._V2_FALLBACK_WARNED.clear()
        with mock.patch.object(
            api, "_curl_post", side_effect=_fake_post
        ), mock.patch.object(api, "print"):
            out = self._api().get_job_events_with_cookie("job-x", "cookie-v")

        self.assertTrue(urls[0].endswith("/api/v2/train"))
        self.assertTrue(urls[1].endswith("/api/v1/train_job/events/list"))
        self.assertEqual(out, _JOB_EVENTS)

    def test_instance_events_resolves_pods_and_object_type(self):
        captured = {}

        def _fake_post(url, json=None, headers=None, params=None, timeout=None):
            captured["body"] = json
            return _FakeResp(200, {"Result": {"events": [], "total": 0}})

        client = self._api()
        with mock.patch.object(
            client,
            "_resolve_pod_names",
            return_value=["job-x-worker-0", "job-x-worker-1"],
        ), mock.patch.object(api, "_curl_post", side_effect=_fake_post):
            client.get_job_instance_events_with_cookie("job-x", "cookie-v")

        self.assertEqual(captured["body"]["filter"]["object_type"], "instance")
        self.assertEqual(
            captured["body"]["filter"]["object_ids"],
            ["job-x-worker-0", "job-x-worker-1"],
        )

    def test_401_raises_qz_error(self):
        with mock.patch.object(
            api, "_curl_post", return_value=_FakeResp(401, text="<html>302")
        ):
            with self.assertRaises(api.QzAPIError):
                self._api().get_job_events_with_cookie("job-x", "cookie-v")

    def test_business_error_raises(self):
        """v2 的错误信封同样要抛出来，不能当成空结果。"""
        payload = {
            "ResponseMetadata": {
                "Error": {"Code": "InvalidParameter", "Message": "boom"}
            }
        }
        with mock.patch.object(api, "_curl_post", return_value=_FakeResp(200, payload)):
            with self.assertRaises(api.QzAPIError):
                self._api().get_job_events_with_cookie("job-x", "cookie-v")

    def test_missing_events_returns_empty_list(self):
        with mock.patch.object(
            api, "_curl_post", return_value=_FakeResp(200, {"Result": {}})
        ):
            self.assertEqual(
                [], self._api().get_job_events_with_cookie("job-x", "cookie-v")
            )

    def test_cli_tail_includes_latest_event_beyond_first_200(self):
        events = _dated_events(201)
        events[-1]["message"] = "Parent GPU quota insufficient: 569 used, 8 requested, 570 limit"
        pages = []

        def fake_post(url, json=None, **kwargs):
            pages.append(json["page_num"])
            start = (json["page_num"] - 1) * json["page_size"]
            return _FakeResp(200, {"Result": {"events": events[start:start + 200], "total": 201}})

        stdout = io.StringIO()
        with mock.patch.object(api, "_curl_post", side_effect=fake_post), \
             mock.patch.object(cli, "get_api", return_value=self._api()), \
             mock.patch.object(cli, "get_display", return_value=_FakeDisplay()), \
             mock.patch.object(cli, "_get_cookie_value", return_value="cookie-v"), \
             redirect_stdout(stdout):
            rc = cli.cmd_events(_events_args(output_json=True, tail=1))
        self.assertEqual(rc, 0)
        self.assertEqual(pages, [1, 2])
        self.assertEqual(json.loads(stdout.getvalue()), [events[-1]])

    def test_v1_fallback_retains_total_and_reads_every_page(self):
        events = _dated_events(3)
        calls = []

        def fake_post(url, json=None, **kwargs):
            calls.append(("v2" if "/api/v2/" in url else "v1", json["page_num"]))
            if "/api/v2/" in url:
                return _FakeResp(404, text="not found", content_type="text/plain")
            start = (json["page_num"] - 1) * 2
            return _FakeResp(200, {"code": 0, "data": {"events": events[start:start + 2], "total": 3}})

        with mock.patch.object(api, "_curl_post", side_effect=fake_post), \
             mock.patch.object(api, "print"):
            out = self._api().get_job_events_with_cookie("job-x", "cookie-v", page_size=2)
        self.assertEqual(out, events)
        self.assertEqual(calls, [("v2", 1), ("v1", 1), ("v2", 2), ("v1", 2)])

    def test_instance_events_read_multiple_pages(self):
        events = _dated_events(3)
        client = self._api()
        pages = []

        def fake_post(url, json=None, **kwargs):
            self.assertEqual(json["filter"], {"object_type": "instance", "object_ids": ["job-x-worker-0"]})
            pages.append(json["page_num"])
            start = (json["page_num"] - 1) * 2
            return _FakeResp(200, {"Result": {"events": events[start:start + 2], "total": 3}})

        with mock.patch.object(api, "_curl_post", side_effect=fake_post):
            out = client.get_job_instance_events_with_cookie(
                "job-x", "cookie-v", pod_names=["job-x-worker-0"], page_size=2,
            )
        self.assertEqual(out, events)
        self.assertEqual(pages, [1, 2])

    def test_total_prevents_short_page_from_truncating_results(self):
        events = _dated_events(3)
        responses = [_FakeResp(200, {"Result": {"events": [e], "total": 3}}) for e in events]
        with mock.patch.object(api, "_curl_post", side_effect=responses) as post:
            out = self._api().get_job_events_with_cookie("job-x", "cookie-v", page_size=200)
        self.assertEqual(out, events)
        self.assertEqual(post.call_count, 3)

    def test_exact_page_multiple_stops_at_total(self):
        events = _dated_events(4)
        responses = [_FakeResp(200, {"Result": {"events": events[i:i + 2], "total": 4}}) for i in (0, 2)]
        with mock.patch.object(api, "_curl_post", side_effect=responses) as post:
            out = self._api().get_job_events_with_cookie("job-x", "cookie-v", page_size=2)
        self.assertEqual(out, events)
        self.assertEqual(post.call_count, 2)

    def test_missing_total_reads_through_full_pages_to_empty(self):
        events = _dated_events(4)
        responses = [_FakeResp(200, {"Result": {"events": events[i:i + 2]}}) for i in (0, 2, 4)]
        with mock.patch.object(api, "_curl_post", side_effect=responses) as post:
            out = self._api().get_job_events_with_cookie("job-x", "cookie-v", page_size=2)
        self.assertEqual(out, events)
        self.assertEqual(post.call_count, 3)

    def test_later_page_failure_does_not_return_partial_events_or_fall_back(self):
        responses = [
            _FakeResp(200, {"Result": {"events": _dated_events(2), "total": 3}}),
            _FakeResp(401, text="unauthorized"),
        ]
        with mock.patch.object(api, "_curl_post", side_effect=responses) as post:
            with self.assertRaises(api.QzAPIError):
                self._api().get_job_events_with_cookie("job-x", "cookie-v", page_size=2)
        self.assertEqual(post.call_count, 2)
        self.assertTrue(all("/api/v2/" in call.args[0] for call in post.call_args_list))

    def test_repeated_page_with_changing_age_is_an_explicit_error(self):
        page = _dated_events(2)
        responses = [
            _FakeResp(200, {"Result": {"events": [dict(e, age=age) for e in page], "total": 4}})
            for age in ("1h", "1h1m")
        ]
        with mock.patch.object(api, "_curl_post", side_effect=responses):
            with self.assertRaisesRegex(api.QzAPIError, "重复返回分页"):
                self._api().get_job_events_with_cookie("job-x", "cookie-v", page_size=2)

    def test_empty_page_before_total_is_an_explicit_error(self):
        responses = [
            _FakeResp(200, {"Result": {"events": _dated_events(2), "total": 3}}),
            _FakeResp(200, {"Result": {"events": [], "total": 3}}),
        ]
        with mock.patch.object(api, "_curl_post", side_effect=responses):
            with self.assertRaisesRegex(api.QzAPIError, "分页提前结束"):
                self._api().get_job_events_with_cookie("job-x", "cookie-v", page_size=2)

    def test_nonpositive_page_size_fails_before_request(self):
        with mock.patch.object(api, "_curl_post") as post:
            for size in (0, -1, True, 1.5):
                with self.assertRaises(ValueError):
                    self._api().get_job_events_with_cookie("job-x", "cookie-v", page_size=size)
        post.assert_not_called()

    def test_invalid_total_is_an_explicit_error(self):
        for total in (True, 1.5, -1, "not-an-integer"):
            with self.subTest(total=total), mock.patch.object(
                api, "_curl_post", return_value=_FakeResp(200, {"Result": {"events": [], "total": total}}),
            ):
                with self.assertRaisesRegex(api.QzAPIError, "total"):
                    self._api().get_job_events_with_cookie("job-x", "cookie-v")

    def test_malformed_empty_events_are_not_accepted_as_an_empty_list(self):
        for events in ({}, "", 0, False):
            with self.subTest(events=events), mock.patch.object(
                api, "_curl_post", return_value=_FakeResp(200, {"Result": {"events": events, "total": 0}}),
            ):
                with self.assertRaisesRegex(api.QzAPIError, "events"):
                    self._api().get_job_events_with_cookie("job-x", "cookie-v")


# ---- helpers ----


class SchedulingHelperTests(unittest.TestCase):
    def test_status_is_waiting(self):
        for s in ("job_queuing", "job_pending", "Queued", "waiting", "QUEUE"):
            self.assertTrue(cli._status_is_waiting(s), s)
        for s in ("job_running", "job_failed", "job_stopped", "", None):
            self.assertFalse(cli._status_is_waiting(s), s)

    def test_pick_reason_prefers_unschedulable_over_preempt(self):
        events = [
            {"reason": "Evict", "message": "preempted", "last_timestamp": "3000"},
            {
                "reason": "Unschedulable",
                "message": "no nodes",
                "last_timestamp": "1000",
            },
        ]
        self.assertEqual(
            ("Unschedulable", "no nodes"), cli._pick_scheduling_reason(events)
        )

    def test_pick_reason_latest_when_multiple_problems(self):
        events = [
            {"reason": "Unschedulable", "message": "old", "last_timestamp": "1000"},
            {"reason": "FailedScheduling", "message": "new", "last_timestamp": "5000"},
        ]
        self.assertEqual(
            ("FailedScheduling", "new"), cli._pick_scheduling_reason(events)
        )

    def test_pick_reason_falls_back_to_preempt(self):
        events = [
            {"reason": "Evict", "message": "preempted by hi-pri", "last_timestamp": "1"}
        ]
        self.assertEqual(
            ("Evict", "preempted by hi-pri"), cli._pick_scheduling_reason(events)
        )

    def test_pick_reason_none_when_no_scheduling(self):
        events = [
            {"reason": "Started", "message": "container up", "last_timestamp": "1"}
        ]
        self.assertIsNone(cli._pick_scheduling_reason(events))


# ---- cmd_events ----


class _FakeDisplay:
    def __init__(self):
        self.lines = []

    def print(self, *a, **k):
        self.lines.append(" ".join(str(x) for x in a))

    def print_error(self, *a, **k):
        self.lines.append("ERR " + " ".join(str(x) for x in a))


class _FakeEventsAPI:
    def __init__(self, job_events=None, instance_events=None):
        self._job = job_events or []
        self._inst = instance_events or []
        self.job_calls = 0
        self.inst_calls = 0

    def get_job_events_with_cookie(self, job_id, cookie, page_size=200):
        self.job_calls += 1
        return list(self._job)

    def get_job_instance_events_with_cookie(
        self, job_id, cookie, pod_names=None, page_size=200
    ):
        self.inst_calls += 1
        return list(self._inst)


def _events_args(**over):
    ns = argparse.Namespace(
        job_id="job-x",
        reason=None,
        type=None,
        tail=None,
        all_instances=False,
        output_json=False,
    )
    for k, v in over.items():
        setattr(ns, k, v)
    return ns


class CmdEventsTests(unittest.TestCase):
    def _run(self, args, fapi, cookie="cookie-v"):
        display = _FakeDisplay()
        with mock.patch.object(
            cli, "get_display", return_value=display
        ), mock.patch.object(cli, "get_api", return_value=fapi), mock.patch.object(
            cli, "_get_cookie_value", return_value=cookie
        ):
            with redirect_stdout(io.StringIO()) as out:
                rc = cli.cmd_events(args)
        return rc, display, out.getvalue()

    def test_no_cookie_errors(self):
        rc, display, _ = self._run(_events_args(), _FakeEventsAPI(), cookie="")
        self.assertEqual(1, rc)
        self.assertTrue(any("cookie" in ln.lower() for ln in display.lines))

    def test_reason_filter(self):
        fapi = _FakeEventsAPI(job_events=_JOB_EVENTS)
        rc, display, _ = self._run(_events_args(reason="unschedulable"), fapi)
        self.assertEqual(0, rc)
        joined = "\n".join(display.lines)
        self.assertIn("Unschedulable", joined)
        self.assertNotIn("SuccessfulCreatePod", joined)

    def test_type_filter(self):
        fapi = _FakeEventsAPI(job_events=_JOB_EVENTS)
        rc, display, _ = self._run(_events_args(type="Normal"), fapi)
        joined = "\n".join(display.lines)
        self.assertIn("SuccessfulCreatePod", joined)
        self.assertNotIn("Unschedulable", joined)

    def test_tail_keeps_last_n(self):
        many = [
            {
                "type": "Normal",
                "reason": f"R{i}",
                "message": "m",
                "last_timestamp": str(i),
            }
            for i in range(5)
        ]
        fapi = _FakeEventsAPI(job_events=many)
        rc, display, _ = self._run(_events_args(tail=2), fapi)
        joined = "\n".join(display.lines)
        self.assertIn("R4", joined)
        self.assertIn("R3", joined)
        self.assertNotIn("R0", joined)

    def test_all_instances_merges_pod_events(self):
        fapi = _FakeEventsAPI(
            job_events=_JOB_EVENTS,
            instance_events=[
                {
                    "type": "Warning",
                    "reason": "FailedScheduling",
                    "message": "pod stuck",
                    "last_timestamp": "9000",
                }
            ],
        )
        rc, display, _ = self._run(_events_args(all_instances=True), fapi)
        self.assertEqual(1, fapi.inst_calls)
        joined = "\n".join(display.lines)
        self.assertIn("FailedScheduling", joined)

    def test_all_instances_not_called_by_default(self):
        fapi = _FakeEventsAPI(job_events=_JOB_EVENTS)
        self._run(_events_args(), fapi)
        self.assertEqual(0, fapi.inst_calls)

    def test_json_output(self):
        fapi = _FakeEventsAPI(job_events=_JOB_EVENTS)
        rc, display, out = self._run(_events_args(output_json=True), fapi)
        self.assertEqual(0, rc)
        parsed = json.loads(out)
        self.assertEqual(2, len(parsed))


# ---- cmd_status diagnosis ----


class _FakeStore:
    def update_from_api(self, job_id, api_data):
        return mock.MagicMock()


class _StatusAPI(_FakeEventsAPI):
    def __init__(self, status, **kw):
        super().__init__(**kw)
        self._status = status

    def get_job_detail(self, job_id):
        return {"status": self._status}


class CmdStatusDiagnosisTests(unittest.TestCase):
    def _run(self, api_obj):
        display = _FakeDisplay()
        display.print_job_detail = lambda *a, **k: None
        args = argparse.Namespace(job_id="job-x", json=False)
        with mock.patch.object(
            cli, "get_display", return_value=display
        ), mock.patch.object(
            cli, "get_store", return_value=_FakeStore()
        ), mock.patch.object(
            cli, "get_api", return_value=api_obj
        ), mock.patch.object(
            cli, "_get_cookie_value", return_value="cookie-v"
        ):
            rc = cli.cmd_status(args)
        return rc, display

    def test_queuing_job_appends_reason(self):
        api_obj = _StatusAPI("job_queuing", job_events=_JOB_EVENTS)
        rc, display = self._run(api_obj)
        self.assertEqual(0, rc)
        self.assertEqual(1, api_obj.job_calls)
        self.assertTrue(
            any("排队原因" in ln and "Unschedulable" in ln for ln in display.lines)
        )

    def test_running_job_no_events_call(self):
        api_obj = _StatusAPI("job_running", job_events=_JOB_EVENTS)
        rc, display = self._run(api_obj)
        self.assertEqual(0, rc)
        self.assertEqual(0, api_obj.job_calls)  # no extra request for a running job
        self.assertFalse(any("排队原因" in ln for ln in display.lines))

    def test_queuing_but_no_scheduling_event_is_silent(self):
        api_obj = _StatusAPI(
            "job_pending",
            job_events=[
                {
                    "type": "Normal",
                    "reason": "Started",
                    "message": "up",
                    "last_timestamp": "1",
                }
            ],
        )
        rc, display = self._run(api_obj)
        self.assertEqual(0, rc)
        self.assertFalse(any("排队原因" in ln for ln in display.lines))


if __name__ == "__main__":
    unittest.main()
