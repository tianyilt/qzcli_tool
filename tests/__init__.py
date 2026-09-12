"""测试包初始化 —— 把测试与用户真实状态隔离开（**只覆盖一半的调用方式**）。

## 这份文件覆盖不了默认命令，别指望它

实测结论：``python3 -m unittest discover -s tests``（本仓 CONTRIBUTING 写的那条）
**不会导入本文件**。``-s`` 指向目录时 unittest 把该目录当 top-level dir、按顶层
模块导入测试文件，整个包的 ``__init__`` 被跳过。只有
``python3 -m unittest tests.test_x`` 这种按包路径指定的方式才会走到这里。

所以真正兜住全部用例的隔离在 ``tests/test_opslog_isolation.py`` 的 import 期
（依据：discover 会先导入全部 ``test*.py``，再执行任何一条用例）。本文件是那条
路径的补充，不是主力 —— 把它当主力过一次，结果是「看起来做了隔离、日志照样被
写脏」，白查一轮。

## 为什么要隔离

测试往用户真实的 ``~/.qzcli/qzcli_ops.log`` 里写过假记录，而那份日志是排查
「账号为什么反复被锁」的取证来源；被写脏比没记更糟，会看到一堆本机从未发生
过的失败。详细经过见 ``tests/test_opslog_isolation.py`` 的模块注释。
"""

import atexit
import os
import tempfile

if not os.environ.get("QZCLI_OPS_LOG"):
    _tmp = tempfile.mkdtemp(prefix="qzcli-test-opslog-")
    os.environ["QZCLI_OPS_LOG"] = os.path.join(_tmp, "ops.log")

    @atexit.register
    def _cleanup():  # pragma: no cover —— 退出时清理，失败无所谓
        import shutil

        shutil.rmtree(_tmp, ignore_errors=True)
