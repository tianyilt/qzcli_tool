"""项目列表走 v2 ``project GetProjectForPage``。

## 上游改了什么

2026-08 之前 v2 的 ``project ListProjects`` 对普通账号是 ``AccessForbidden``，
所以 qzcli 只能走 v1 ``/api/v1/project/list`` —— 这是文档里记了很久的
「v1 下线阻塞项」之一。上游放开权限并改名为 ``GetProjectForPage`` 后可以迁了。

## 换接口顺带修掉的真 bug

v1 的 ``ListProjects`` 会把**已结束且当前用户不在其中**的项目也返回。
``GetProjectForPage`` 干净得多，但 **spec 描述的「只返回当前用户所属」是不准的** ——
实测 11 条里有 1 条 ``is_member=False``（``某工作空间下的一个项目``，状态
``PASS_MODIFY_RESOURCE``）。要按成员身份过滤的调用方必须自己滤。

实测本账号（别照推，这两边的差异不是简单的子集关系）：

===================  ====  ================================  ===================
接口                 数量  ``is_member``                     独有
===================  ====  ================================  ===================
``GetProjectForPage``  11  True×10 / False×1                 1 个（v1 漏掉的）
``ListProjects``       12  **False×12（该字段在 v1 里恒 False，不可信）**  2 个，均 ``FINISHED`` 且非成员
===================  ====  ================================  ===================

即 v2 **不是** v1 的子集：它排除了 2 个已结束的，又补上了 1 个 v1 没给的。

后果很具体：``qzcli create -i`` 把提交不了的项目列出来给用户选，选中就报
``AccessForbidden: 您已离开所选项目，无法创建``。低优排队的冒烟用例也栽在这里 ——
它从缓存取第一个项目，而缓存是用 v1 建的。

**这条行为必须有测试钉住** —— 否则哪天因为某个原因回落到 v1，用户又会撞上同一个
错误，而没有任何信号。

## 形状契约

下游 9 个调用点（``list_workspaces`` 8 处 + ``_project_belongs_to_workspace_on_platform``）
依赖 ``[{id, name, space_list:[{id, usage_status, …}]}]``。
**形状变了不会报错，只会让 ``list_workspaces`` 静默返空** —— 比崩掉更难查。
逐字段对过 v1/v2：下游用到的字段完全一致。
"""

import unittest
from unittest.mock import patch

from qzcli.api import QzAPI, QzAPIError

_WS = "ws-11111111-1111-1111-1111-111111111111"


def _project(pid, name, spaces=(_WS,), **extra):
    p = {
        "id": pid,
        "name": name,
        "space_list": [
            {"id": s, "name": "空间", "usage_status": 0, "type": "", "status": ""}
            for s in spaces
        ],
    }
    p.update(extra)
    return p


_V2_ITEMS = [_project("project-a", "在职项目"), _project("project-b", "另一个")]
# v1 多吐两条已退出 / 已结束的
_V1_ITEMS = _V2_ITEMS + [
    _project("project-left", "已退出的", is_member=False, status="FINISHED")
]


def _api():
    return QzAPI(username="u", password="p")


class ProjectListV2Tests(unittest.TestCase):
    def test_v2_is_used_by_default(self):
        api = _api()
        with patch.object(
            api, "_request_v2", return_value={"items": _V2_ITEMS}
        ) as v2, patch.object(api, "_project_list_items_v1") as v1:
            items = api.list_projects_raw()
        v1.assert_not_called()
        self.assertEqual(v2.call_args[0][1], "GetProjectForPage")
        self.assertEqual({p["id"] for p in items}, {"project-a", "project-b"})

    def test_left_projects_are_not_returned(self):
        """**本轮最有价值的行为**：已退出的项目不该出现在列表里。

        v1 会把它们吐出来，用户选中就报「您已离开所选项目，无法创建」。
        """
        api = _api()
        with patch.object(api, "_request_v2", return_value={"items": _V2_ITEMS}):
            ids = {p["id"] for p in api.list_projects_raw()}
        self.assertNotIn("project-left", ids)

        # 对照：回落到 v1 时那条会回来 —— 这正是为什么不能默认用 v1
        with patch.object(
            api, "_request_v2", side_effect=QzAPIError("404 page not found", 404)
        ), patch.object(api, "_project_list_items_v1", return_value=_V1_ITEMS):
            ids_v1 = {p["id"] for p in api.list_projects_raw()}
        self.assertIn("project-left", ids_v1, "v1 腿的行为没变，说明这条对照有效")

    def test_falls_back_when_v2_route_is_missing(self):
        api = _api()
        with patch.object(
            api, "_request_v2", side_effect=QzAPIError("404 page not found", 404)
        ), patch.object(api, "_project_list_items_v1", return_value=_V1_ITEMS) as v1:
            items = api.list_projects_raw()
        v1.assert_called_once()
        self.assertEqual(len(items), 3)

    def test_business_error_does_not_fall_back(self):
        """业务错误不回落 —— 回落只会把同一个错误再撞一遍，还翻倍请求量。"""
        api = _api()
        with patch.object(
            api, "_request_v2", side_effect=QzAPIError("AccessForbidden: 无权限")
        ), patch.object(api, "_project_list_items_v1") as v1:
            with self.assertRaises(QzAPIError):
                api.list_projects_raw()
        v1.assert_not_called()

    def test_shape_contract_is_preserved(self):
        """下游 9 个调用点靠这个形状；变了会让 list_workspaces 静默返空。"""
        api = _api()
        with patch.object(api, "_request_v2", return_value={"items": _V2_ITEMS}):
            items = api.list_projects_raw()
        for p in items:
            self.assertIn("id", p)
            self.assertIn("name", p)
            self.assertIsInstance(p.get("space_list"), list)
            for sp in p["space_list"]:
                self.assertIn("id", sp)
                self.assertIn("usage_status", sp, "过滤禁用工作空间要用它")

    def test_list_workspaces_still_filters_disabled_spaces(self):
        """禁用空间过滤建立在 usage_status 上，换接口后必须仍然生效。"""
        api = _api()
        items = [
            _project("p1", "x", spaces=(_WS,)),
            {
                "id": "p2",
                "name": "y",
                "space_list": [
                    {"id": "ws-disabled", "name": "被禁用", "usage_status": 1}
                ],
            },
        ]
        with patch.object(api, "_request_v2", return_value={"items": items}):
            ws = api.list_workspaces("cookie")
        ids = {w["id"] for w in ws}
        self.assertIn(_WS, ids)
        self.assertNotIn("ws-disabled", ids, "usage_status 非 0 的空间应被跳过")

    def test_empty_items_does_not_crash(self):
        api = _api()
        with patch.object(api, "_request_v2", return_value={}):
            self.assertEqual(api.list_projects_raw(), [])


if __name__ == "__main__":
    unittest.main()


class PaginationTests(unittest.TestCase):
    """``total`` 说有多少条，就必须真拿到多少条。

    以前只发一次 ``{page:1, page_size:200}`` 就返回 ``items``，超出 200 的部分
    被静默丢掉 —— 症状是 ``list_workspaces`` 少列工作空间、``qzcli ws`` 少几行，
    **退出码还是 0**。和 ``except: pass`` 是同一类病。
    """

    def test_total_is_a_string_on_the_wire(self):
        """平台实测返回 ``'11'`` 而不是 ``11``。

        直接拿 len() 跟它比大小会 TypeError；写成 ``if total > len(items)``
        这种「看着对」的代码会在真实数据上炸。这条把类型钉死。
        """
        api = _api()
        with patch.object(
            api, "_request_v2", return_value={"items": _V2_ITEMS, "total": "2"}
        ) as v2:
            items = api.list_projects_raw()
        self.assertEqual(len(items), 2)
        self.assertEqual(v2.call_count, 1, "拿满了就不该再翻页")

    def test_keeps_paging_until_total_is_reached(self):
        api = _api()
        page1 = {
            "items": [_project(f"p{i}", f"项目{i}") for i in range(200)],
            "total": "250",
        }
        page2 = {
            "items": [_project(f"q{i}", f"项目{i}") for i in range(50)],
            "total": "250",
        }
        with patch.object(api, "_request_v2", side_effect=[page1, page2]) as v2:
            items = api.list_projects_raw()
        self.assertEqual(len(items), 250, "第 2 页的 50 个项目被丢了")
        self.assertEqual(v2.call_count, 2)
        self.assertEqual(v2.call_args_list[1][0][2]["page"], 2)

    def test_stops_when_a_page_comes_back_empty(self):
        """平台的 total 报大了也不能死循环。"""
        api = _api()
        with patch.object(
            api,
            "_request_v2",
            side_effect=[
                {"items": _V2_ITEMS, "total": "999"},
                {"items": [], "total": "999"},
            ],
        ) as v2:
            items = api.list_projects_raw()
        self.assertEqual(len(items), 2)
        self.assertEqual(v2.call_count, 2, "空页之后就该停")

    def test_malformed_total_returns_what_we_got_and_leaves_a_trace(self):
        """``total`` 形状变了不猜、不崩、不装作没事。"""
        from qzcli import diag

        diag.clear()
        api = _api()
        with patch.object(
            api, "_request_v2", return_value={"items": _V2_ITEMS, "total": {"n": 2}}
        ):
            items = api.list_projects_raw()
        self.assertEqual(len(items), 2, "拿到手的照常返回")
        self.assertIsNotNone(
            diag.last_reason("project/list 分页"),
            "翻页判断失效本身也是个故障，必须留痕",
        )
