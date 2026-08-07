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
    "明文密码赋值": re.compile(
        r"(?:PASSWORD|password)\s*[=:]\s*['\"]([^'\"\s{}$<][^'\"\s]{5,})['\"]"
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


if __name__ == "__main__":
    unittest.main()
