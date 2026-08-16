"""``QZCLI_HOME``：把整个状态目录搬走的契约。

## 为什么要有这些测试

MoVA2 训练脚本需要一份**冻结版** qzcli —— 版本钉死、凭据自带、我在 ``~/.qzcli``
这边怎么折腾都影响不到它。实现只有 ``config.py`` 里一行，但它撑着两个容易被后来
改动无声破坏的性质：

1. **不设 ``QZCLI_HOME`` 时行为必须和以前**一字不差**。** 这条最重要 —— 所有现存
   用户和脚本都依赖 ``~/.qzcli``，回归了就是把所有人搞挂。
2. **所有状态都得跟着走**，漏一个就是"以为隔离了其实没有"：冻结版写 jobs.json
   写回了我的 home，或者 cookie 各自一份但冷却记录还共用。这类错误不会报错，
   只会在某天表现成诡异的串台 —— 正是本仓最忌讳的静默失败。

## 为什么用子进程而不是 importlib.reload

``CONFIG_DIR`` 是**模块级常量**，import 那一刻就求值完了。这不是缺陷而是约定：
wrapper 必须在 python 启动**之前** export，运行中改 ``os.environ`` 不生效。
用子进程测才是在测真实语义；顺带也避免 reload 把同一批跑的其它测试搞脏
（reload 会换掉类对象，别的模块里 ``from .api import QzAPI`` 拿的还是旧的）。
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from qzcli import api  # noqa: E402


def _probe(home, expressions):
    """在一个干净子进程里（按需设 QZCLI_HOME）求值若干表达式，返回结果列表。

    Args:
        home: ``QZCLI_HOME`` 的值；``None`` 表示不设这个变量。
        expressions: 要在子进程里求值的表达式，可以引用 ``c``（qzcli.config）
            和 ``a``（qzcli.api）。
    """
    env = dict(os.environ)
    env.pop("QZCLI_HOME", None)
    if home is not None:
        env["QZCLI_HOME"] = str(home)
    # 子进程别去碰真实的 HOME 之外的东西；只 import，不执行任何命令
    code = (
        "import json, qzcli.config as c, qzcli.api as a\n"
        "print(json.dumps([str(x) for x in [%s]]))" % ", ".join(expressions)
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        raise AssertionError(f"子进程失败：{proc.returncode}\n{proc.stderr}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


#: 所有会落盘的状态。加了新的状态文件要往这里补一条，否则"隔离"就是漏的。
_STATE_EXPRESSIONS = [
    "c.CONFIG_DIR",
    "c.CONFIG_FILE",
    "c.COOKIE_FILE",
    "c.JOBS_FILE",
    "c.RESOURCES_FILE",
    "c.TOKEN_CACHE_FILE",
    "c.DEFAULT_ENV_FILE",
    "a._cooldown_path()",
]


class QzcliHomeRelocatesStateTests(unittest.TestCase):
    def test_every_state_path_moves_under_qzcli_home(self):
        """设了 QZCLI_HOME，**每一个**状态文件都要落在里面 —— 漏一个就是假隔离。"""
        with tempfile.TemporaryDirectory() as tmp:
            paths = _probe(tmp, _STATE_EXPRESSIONS)
            for expr, got in zip(_STATE_EXPRESSIONS, paths):
                with self.subTest(path=expr):
                    self.assertTrue(
                        got == tmp or got.startswith(tmp + os.sep),
                        f"{expr} = {got}，没有落在 QZCLI_HOME({tmp}) 下",
                    )

    def test_relogin_lock_also_moves(self):
        """登录互斥锁也得跟着走：两份 home 是两份独立 cookie，本来就该各登各的。"""
        with tempfile.TemporaryDirectory() as tmp:
            (lock,) = _probe(tmp, ['a.Path(a.CONFIG_DIR) / ".relogin.lock"'])
            self.assertTrue(lock.startswith(tmp + os.sep), lock)

    def test_two_homes_do_not_share_any_path(self):
        """两份 home 之间不能有任何一条路径撞车。"""
        with tempfile.TemporaryDirectory() as a_dir, tempfile.TemporaryDirectory() as b_dir:
            a_paths = _probe(a_dir, _STATE_EXPRESSIONS)
            b_paths = _probe(b_dir, _STATE_EXPRESSIONS)
            self.assertEqual(
                set(a_paths) & set(b_paths),
                set(),
                "两份 QZCLI_HOME 之间存在共用路径，隔离不成立",
            )


class BackwardCompatibilityTests(unittest.TestCase):
    """不设 QZCLI_HOME 时必须和以前一模一样 —— 这条挂了就是把所有现存用户搞挂。"""

    def test_unset_falls_back_to_home_dot_qzcli(self):
        expected = Path.home() / ".qzcli"
        paths = _probe(None, _STATE_EXPRESSIONS)
        self.assertEqual(paths[0], str(expected))
        for expr, got in zip(_STATE_EXPRESSIONS, paths):
            with self.subTest(path=expr):
                self.assertTrue(got.startswith(str(expected)), f"{expr} = {got}")

    def test_blank_value_is_treated_as_unset(self):
        """空串/纯空白要当成没设。否则 ``export QZCLI_HOME=`` 会让状态落到 cwd。"""
        expected = str(Path.home() / ".qzcli")
        for blank in ("", "   "):
            with self.subTest(value=repr(blank)):
                (got,) = _probe(blank, ["c.CONFIG_DIR"])
                self.assertEqual(got, expected)


class CredentialBlockIsPerHomeTests(unittest.TestCase):
    """把「分家最多多试 1 次就停住」这个论证钉成测试。

    冻结版和主用版共用同一个平台账号、却各有一份 ``.relogin.cooldown``。设计上
    接受这一点，依据是：封锁在**每个 home 内部**是永久的，所以账号被锁时另一份
    home 至多多打 1 次 CAS 就自己停住，不会演变成重试风暴。这个依据一旦被改坏
    （比如有人把凭据类也退回 60 秒冷却），下面第一条就会变红。
    """

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.home = Path(self._tmp.name)
        # 清掉进程内记忆，强制走磁盘那条路径（跨进程判据靠的就是它）
        self._saved = dict(api._relogin_failure)
        api._relogin_failure.update({"at": 0.0, "message": ""})
        self.addCleanup(api._relogin_failure.update, self._saved)
        self.addCleanup(self._tmp.cleanup)

    def test_credential_failure_blocks_forever_within_its_own_home(self):
        locked = "用户名或密码错误：您的账号被锁定，请联系管理员。"
        with mock.patch.object(api, "CONFIG_DIR", self.home):
            api._record_relogin_failure(locked)
            api._relogin_failure.update({"at": 0.0, "message": ""})  # 只剩磁盘那份
            blocked = api._recent_relogin_failure()
        self.assertIsNotNone(blocked, "凭据类失败必须永久封锁自动重登")
        self.assertIn("锁定", blocked)

    def test_cooldown_file_is_written_into_that_home(self):
        with mock.patch.object(api, "CONFIG_DIR", self.home):
            api._record_relogin_failure("网络超时")
        self.assertTrue(
            (self.home / api._RELOGIN_COOLDOWN_FILE).exists(),
            "冷却记录没写进 QZCLI_HOME，跨进程去重会失效",
        )

    def test_transient_failure_expires_but_credential_one_does_not(self):
        """对照：瞬时失败过了冷却期就放行 —— 证明上面那条是「凭据」在起作用，
        不是「只要有文件就一律封锁」。"""
        stale = api._time.time() - api._RELOGIN_COOLDOWN_S - 5
        with mock.patch.object(api, "CONFIG_DIR", self.home):
            (self.home / api._RELOGIN_COOLDOWN_FILE).write_text(
                f"{stale}\n网络超时", encoding="utf-8"
            )
            self.assertIsNone(api._recent_relogin_failure())

            (self.home / api._RELOGIN_COOLDOWN_FILE).write_text(
                f"{stale}\n您的账号被锁定，请联系管理员", encoding="utf-8"
            )
            self.assertIsNotNone(api._recent_relogin_failure())


if __name__ == "__main__":
    unittest.main()
