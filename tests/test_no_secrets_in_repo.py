"""仓库里不许出现真实凭据。

## 为什么有这条

我把真机探测拿到的 **Jupyter 访问 token 原样抄进了测试 fixture**，被 GitGuardian
拦下。Jupyter 的 token 就写在访问 URL 里，粘一条真 URL 进来就等于把开发机的门钥匙
提交进仓库 —— 拿到它的人能在那台机器上执行任意命令。

这类错误的特点是**写的时候一点都不觉得是在写凭据**：我以为自己在贴一个"响应样例"。
所以不能靠自觉，得有一道机器检查。

## 判据

按**形状**匹配，不是按变量名 —— 变量名叫 `_JUPYTER_URL` 的地方一样能藏 token。
明显是占位值的（含 fake / example.invalid / placeholder …）放行，因为测试 fixture
本来就该长成真值的形状。

## 这条测试的局限

它只看**工作区当前内容**，不看 git 历史，也拦不住 commit message 和 PR 正文
（我这次那两处恰好是干净的，但下次不一定）。真正的防线应该是 pre-commit 钩子 +
平台侧扫描；这里是最后一道、也是最便宜的一道。
"""

import pathlib
import re
import subprocess
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent

#: 按形状匹配的凭据模式。加新模式时请附一句"这东西泄漏了会怎样"。
_PATTERNS = {
    # Jupyter 的 token 直接放在访问 URL 的路径段和 query 里，拿到即可执行命令
    "Jupyter token（URL 路径段）": re.compile(
        r"/jupyter/[0-9a-f-]{16,}/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    ),
    "Jupyter token（?token=）": re.compile(
        r"[?&]token=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    ),
    # 平台登录态，拿到即可冒充本人调用全部接口
    "平台 session cookie": re.compile(r"inspire-session=([A-Za-z0-9+/_-]{20,})"),
    # wandb key 泄漏可写他人实验数据
    "WandB API key": re.compile(r"(local-[0-9a-f]{24,}|\bWANDB_API_KEY=[0-9a-f]{40})"),
    # 键名两侧的引号必须允许，否则 JSON / dict 字面量形式整类漏过 ——
    # ``{"password": "……"}`` 里 password 后面先是引号再是冒号，旧正则
    # ``password\s*[=:]`` 在那个引号上就断了。粘一段带密码的接口响应样例
    # 进测试 fixture 正是这个形状，是最容易发生的泄漏方式之一。
    # （2026-09-12 实锤：本仓测试 fixture 被 GitGuardian 按形状告警，
    #   而这条本地闸门当时一声不响。）
    "明文密码赋值": re.compile(
        r"(?:PASSWORD|password)['\"]?\s*[=:]\s*['\"]([^'\"\s{}$<][^'\"\s]{5,})['\"]"
    ),
    # **最容易漏的一类**：把凭据抽成常量。真实事故就是这个形状 ——
    # `_TOKEN = "1f70d0dc-f1db-40e3-826d-8d84d160d440"`。
    # 只匹配 URL 形状抓不到它，因为源码里的 URL 是 f-string、写的是 {_TOKEN} 占位符。
    # GitGuardian 靠「熵 + 变量名」抓到的，这里照同样的思路补上。
    "凭据被抽成常量": re.compile(
        r"(?i)\b\w*(?:token|secret|passwd|password|apikey|api_key|cookie|credential)\w*"
        r"\s*[=:]\s*['\"]([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        r"|[A-Za-z0-9+/_-]{24,})['\"]"
    ),
}

#: 一眼能看出是编造的，放行。测试 fixture 本来就该长成真值的形状。
_OBVIOUSLY_FAKE = (
    "fake",
    "example.invalid",
    "example.com",
    "placeholder",
    "dummy",
    "your_",
    "xxx",
    "stale",
    "changeme",
    "<",
    "${",
)

_SCAN_SUFFIXES = (".py", ".md", ".json", ".txt", ".sh", ".yml", ".yaml", ".toml")


def _tracked_files():
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files"], capture_output=True, text=True
    ).stdout
    return [REPO / f for f in out.split("\n") if f.endswith(_SCAN_SUFFIXES)]


class NoSecretsInRepoTests(unittest.TestCase):
    def test_no_real_credentials_in_tracked_files(self):
        offenders = []
        for path in _tracked_files():
            if path.name == pathlib.Path(__file__).name:
                continue  # 本文件里的正则会自我命中
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for label, rx in _PATTERNS.items():
                for m in rx.finditer(text):
                    value = m.group(1)
                    if any(k in value.lower() for k in _OBVIOUSLY_FAKE):
                        continue
                    line = text[: m.start()].count("\n") + 1
                    rel = path.relative_to(REPO)
                    offenders.append(f"{rel}:{line}  [{label}]  {value[:12]}…")
        self.assertEqual(
            offenders,
            [],
            "仓库里出现疑似真实凭据：\n  "
            + "\n  ".join(offenders)
            + "\n\n若确属测试占位值，请改成明显是假的（含 fake / example.invalid 等）；"
            "若是真凭据，**不要只删掉重新提交** —— 历史里还在，"
            "要 amend/rebase 重写那个提交，并轮换该凭据。",
        )

    def test_the_check_catches_the_shape_of_the_real_incident(self):
        """自检必须喂**真实事故的形状**，不是我想象中的形状。

        第一版自检喂的是一条完整 URL，通过了 —— 但真实泄漏是把 token 抽成了常量
        （`_TOKEN = "1f70d0dc-…"`），源码里的 URL 只是 f-string 占位符。
        于是扫描器对着真事故报 OK，自检还告诉我一切正常。

        **自检的样本必须来自真实事故，否则它只是在证明我的假设自洽。**
        """
        real_shape = '_TOKEN = "1f70d0dc-f1db-40e3-826d-8d84d160d440"'
        hit = None
        for label, rx in _PATTERNS.items():
            m = rx.search(real_shape)
            if m and not any(k in m.group(1).lower() for k in _OBVIOUSLY_FAKE):
                hit = (label, m.group(1))
                break
        self.assertIsNotNone(hit, "扫描器抓不到真实事故的形状（常量赋值）")

    def test_also_catches_the_url_shape(self):
        """URL 里内联的 token 也要抓 —— 两种形状都会出现。"""
        planted = (
            "https://nb.host/ws-a/project-b/user-c/jupyter/"
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/"
            "12345678-90ab-cdef-1234-567890abcdef/lab"
        )
        m = _PATTERNS["Jupyter token（URL 路径段）"].search(planted)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1), "12345678-90ab-cdef-1234-567890abcdef")

    def test_fake_values_are_not_flagged(self):
        """对照：占位值不该被报 —— 否则大家会学会忽略这条测试。"""
        benign = (
            "https://notebook.example.invalid/ws-fake/project-fake/user-fake/"
            "jupyter/notebook-0000-fake-0000-000000000000/"
            "token-0000-fake-0000-000000000000/lab"
        )
        for label, rx in _PATTERNS.items():
            m = rx.search(benign)
            if m and not any(k in m.group(1).lower() for k in _OBVIOUSLY_FAKE):
                self.fail(f"占位值被 {label} 误报: {m.group(1)}")


#: 内部项目名 / 分区名。**这个仓库是 public 的**，这些名字对外等于组织架构情报。
_INTERNAL_NAMES = (
    "情境智能",
    "MOSS-VL",
    "MOVA2.0",
    "纯交付",
    "infra-debug",
    "ncu-debug",
)

#: 平台资源 ID 的形状。**十六进制一位都不能留** —— 前 8 位在全平台唯一，
#: 留着等于没打码（第一版 ``redact()`` 就是这么漏的：``ws-8207e9e2-<redacted>``）。
_REAL_ID_RE = re.compile(r"\b(ws|project|lcg|cg|user|nb)-([0-9a-f]{8})\b", re.I)

#: 明显编造的占位 ID：同一个字符重复，或 1234abcd 这种键盘序。
_FAKE_ID_RE = re.compile(r"^(?:(.)\1{7}|1234abcd|0*)$", re.I)


class NoInternalIdentifiersTests(unittest.TestCase):
    """public 仓库里不许出现内部项目名和真实资源 ID。

    ## 为什么单独立一条

    2026-08-26 我做过一轮"清掉内部空间名"，报的是 6 处 → 0。**一天后复查，
    同一个仓库里还留着 5 处** —— 那轮只按字面名字 grep，漏了：

    1. 空间 **ID**（`ws-8207e9e2-…`）—— 名字清了，ID 还在，一样能反查
    2. `redact()` 自己：它把 UUID 打码成 `ws-8207e9e2-<redacted>`，**保留了前 8 位**。
       写的时候觉得"只是前缀"，实际前 8 位全平台唯一，等于没打
    3. 测试 docstring 里的分区名（`MOVA2.0纯交付分区`）—— 不在源码，在注释里

    共同点是：**靠人 grep 一遍就宣布干净，下次照样漏。** 所以这里钉成测试。

    这条只看工作区，拦不住 git 历史和 commit message（本仓历史里确实还有，
    需要另行处置）—— 但至少能保证"从今天起不再新增"。
    """

    def _tracked_text(self):
        for path in _tracked_files():
            if path.name == pathlib.Path(__file__).name:
                continue  # 本文件写着这些词本身
            try:
                yield path, path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue

    def test_no_internal_project_names(self):
        offenders = [
            f"{path.relative_to(REPO)}:{text[: text.index(name)].count(chr(10)) + 1}  {name}"
            for path, text in self._tracked_text()
            for name in _INTERNAL_NAMES
            if name in text
        ]
        self.assertEqual(
            offenders,
            [],
            "public 仓库里出现内部项目名：\n  "
            + "\n  ".join(offenders)
            + "\n\n举例统一用「分布式空间」这类通用说法。",
        )

    def test_no_real_platform_ids(self):
        offenders = []
        for path, text in self._tracked_text():
            for m in _REAL_ID_RE.finditer(text):
                if _FAKE_ID_RE.match(m.group(2)):
                    continue
                line = text[: m.start()].count("\n") + 1
                offenders.append(f"{path.relative_to(REPO)}:{line}  {m.group(0)}")
        self.assertEqual(
            offenders,
            [],
            "public 仓库里出现真实平台资源 ID：\n  "
            + "\n  ".join(offenders)
            + "\n\n换成明显编造的（如 ws-11111111-1111-4111-8111-111111111111）。",
        )

    def test_the_check_would_have_caught_the_half_redaction(self):
        """自检：喂**上次真漏掉的那个形状**，必须被抓到。

        不是"我想象中的泄漏"，是实际躺在 docs/v2_probe_raw.json 里那行。
        """
        half = '"workspace_id": "ws-8207e9e2-<redacted>"'
        self.assertIsNotNone(_REAL_ID_RE.search(half))
        self.assertIsNone(_FAKE_ID_RE.match("8207e9e2"))

    def test_fake_ids_are_not_flagged(self):
        """对照：占位 ID 不该被报，否则大家会学会忽略这条测试。"""
        for fake in ("ws-11111111", "lcg-22222222", "project-44444444", "ws-1234abcd"):
            m = _REAL_ID_RE.search(fake)
            self.assertIsNotNone(m, fake)
            self.assertIsNotNone(
                _FAKE_ID_RE.match(m.group(2)), f"{fake} 被误报成真 ID"
            )


class PlaintextPasswordShapeTests(unittest.TestCase):
    r"""自检「明文密码赋值」那条模式。

    2026-09-12：GitGuardian 在 ``tests/test_credential_source.py`` 上报了
    Generic Password，而这条本地闸门当时**一声不响**。真因是旧正则写作
    ``password\s*[=:]``，碰上 ``{"password": "……"}`` 这种键名带引号的
    JSON / dict 字面量，在键后面那个引号上就断了。粘一段带密码的接口响应
    样例进 fixture 正是这个形状 —— 属于最容易发生、也最容易漏的一类。
    """

    _RX = _PATTERNS["明文密码赋值"]

    def test_json_shaped_assignment_is_caught(self):
        """键名带引号的形式必须抓到 —— 这是上次真漏掉的形状。"""
        for sample in (
            '{"password": "hunter2-not-real"}',
            "{'password': 'hunter2-not-real'}",
            '  "PASSWORD" : "hunter2-not-real",',
        ):
            with self.subTest(sample=sample):
                m = self._RX.search(sample)
                self.assertIsNotNone(m, f"JSON 形状的明文密码没被抓到: {sample}")
                self.assertEqual("hunter2-not-real", m.group(1))

    def test_bare_assignment_still_caught(self):
        """老形状不能因为放宽正则而失守。"""
        m = self._RX.search('password = "hunter2-not-real"')
        self.assertIsNotNone(m)
        self.assertEqual("hunter2-not-real", m.group(1))

    def test_obviously_fake_values_are_allowed_through(self):
        """占位值必须放行 —— 误报会让人学会忽略告警，那时真漏就没人看了。"""
        for sample in (
            '{"password": "fake-from-config"}',
            '{"password": "placeholder-value"}',
            '{"password": "${QZCLI_PASSWORD}"}',
        ):
            with self.subTest(sample=sample):
                m = self._RX.search(sample)
                allowed = m is None or any(
                    k in m.group(1).lower() for k in _OBVIOUSLY_FAKE
                )
                self.assertTrue(allowed, f"占位值被误报: {sample}")


if __name__ == "__main__":
    unittest.main()
