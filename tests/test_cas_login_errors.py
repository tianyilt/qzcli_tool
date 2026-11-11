"""Only errors in the password form describe the failed authentication.

The legacy SMS-tab fixture deliberately contains captcha labels and an image.
Those static elements cannot establish why CAS rejected a password request.
The server's form-error text must remain visible without inferred causes.
"""

import unittest

from qzcli.api import _describe_cas_login_failure

#: Historical template markup, kept to prevent matching static captcha labels.
_BENIGN_SMS_TAB_MARKUP = """
<li class="auth_ul_li2"><h3>验证码登录</h3></li>
<input class="auth_input paw_input" type="text" placeholder="验证码">
<img src="http://mapp.suda.edu.cn/_control/validateimage?tt=0.226">
<!-- 验证码登录 -->
<span id="send_button" class="sendMsg">发送验证码</span>
<input autocomplete="off" type="text" class="auth_input" placeholder="动态验证码"/>
"""


def _page(error_html=""):
    """拼一个逼近真实结构的登录页：短信标签页固定文案 + 可选的错误容器。"""
    return f"""<html><body>
    {_BENIGN_SMS_TAB_MARKUP}
    <form><div class="form-error">{error_html}</div></form>
    </body></html>"""


class CasLoginFailureMessageTests(unittest.TestCase):
    def test_benign_captcha_markup_is_not_reported_as_captcha(self):
        """核心回归钉子：页面自带的"验证码"文案不能被当成"需要验证码"。

        这条在修复前必红 —— 那正是用户踩到的。
        """
        message = _describe_cas_login_failure(_page())
        self.assertNotIn("要求验证码", message)
        self.assertNotIn("需要输入验证码", message)

    def test_no_error_text_says_so_honestly(self):
        """页面没给原因时要如实说读不到，不推断触发原因。"""
        message = _describe_cas_login_failure(_page())
        self.assertIn("未提供错误说明", message)
        self.assertNotIn("频繁", message)
        self.assertNotIn("自行恢复", message)

    def test_real_captcha_error_is_reported(self):
        """Keep the server's captcha error intact."""
        message = _describe_cas_login_failure(_page("验证码信息无效。"))
        self.assertIn("验证码", message)
        self.assertIn("验证码信息无效", message, "原文要带上")

    def test_captcha_error_includes_separately_labeled_confirmed_policy(self):
        message = _describe_cas_login_failure(_page("验证码信息无效。"))
        server_error, _, guidance = message.partition("\n")
        self.assertEqual("CAS 返回：验证码信息无效。", server_error)
        self.assertIn("提示（平台已确认）", guidance)
        self.assertIn("IP 和 User-Agent", guidance)
        self.assertIn("连续登录失败 3 次", guidance)
        self.assertIn("验证成功或缓存 30 分钟过期后解除", guidance)
        self.assertIn("尚不支持图形验证码输入", guidance)
        self.assertNotIn("等几分钟", message)
        self.assertNotIn("无需重新登录", message)
        self.assertNotIn("暂时", message)

    def test_password_error_is_reported(self):
        message = _describe_cas_login_failure(_page("用户名或密码错误"))
        self.assertEqual("CAS 返回：用户名或密码错误", message)

    def test_account_locked_error_is_passed_through(self):
        """没预料到的错误也要原样透出，而不是套一句泛化文案。

        以前这条会掉进"请检查用户名和密码"，把真正的原因盖掉。
        """
        message = _describe_cas_login_failure(_page("账号已被锁定，请 30 分钟后重试"))
        self.assertIn("账号已被锁定，请 30 分钟后重试", message)

    def test_html_tags_inside_error_are_stripped(self):
        message = _describe_cas_login_failure(
            _page("<span>登录过于频繁</span><br/>请稍后再试")
        )
        self.assertNotIn("<span>", message)
        self.assertIn("登录过于频繁", message)

    def test_empty_and_garbage_input_do_not_crash(self):
        for bad in ("", None, "not html at all", "<html>"):
            with self.subTest(html=bad):
                self.assertTrue(_describe_cas_login_failure(bad))


if __name__ == "__main__":
    unittest.main()
