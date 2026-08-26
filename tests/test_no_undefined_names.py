"""静态扫描：函数里用到的全局名字必须真的存在。

## 为什么要有这条

2026-08-26 我给 devbox 写远端上传时用了 `_requests.put(...)`，而 `_requests`
只是**另一个函数内部**的局部别名（`import requests as _requests` 写在
`_exec_launch` 里），模块级只有 `requests`。这段代码：

- `python -m ast` 解析**通过**（语法没问题）
- 单元测试**全绿**（那条路径要连真机才走到）
- 直到在真机上跑 `qzcli devbox init`，才炸出 `name '_requests' is not defined`

也就是说，语法检查和单测都拦不住它，只有真机能。而真机验证很贵（本轮为此建了
三台开发机、耗掉几小时）。所以补这条静态闸门，把这类错拉回到本地一秒钟就能发现。

扫的是「函数体里被**读取**的全局名」是否能在模块全局、内建、或该函数自己的
局部绑定里找到。只报确定的错，宁可漏也不误报 —— 误报会让人把闸门关掉。
"""

import ast
import builtins
import os
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
PKG = REPO / "qzcli"


def _collect_local_names(fn: ast.AST):
    """这个函数自己绑定了哪些名字（参数、赋值、import、for、with as、except as…）。"""
    names = set()
    args = getattr(fn, "args", None)
    if args:
        for group in ("posonlyargs", "args", "kwonlyargs"):
            for a in getattr(args, group, []) or []:
                names.add(a.arg)
        for extra in (args.vararg, args.kwarg):
            if extra:
                names.add(extra.arg)

    for node in ast.walk(fn):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            names.add(node.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            names.add(node.name)
        elif isinstance(node, (ast.Global, ast.Nonlocal)):
            names.update(node.names)
        elif isinstance(node, ast.comprehension):
            for t in ast.walk(node.target):
                if isinstance(t, ast.Name):
                    names.add(t.id)
    return names


def _module_level_names(tree: ast.Module):
    names = set(dir(builtins))
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                for n in ast.walk(t):
                    if isinstance(n, ast.Name):
                        names.add(n.id)
        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
            for n in ast.walk(node.target):
                if isinstance(n, ast.Name):
                    names.add(n.id)
        elif isinstance(node, ast.Try):
            for sub in node.body + node.orelse + node.finalbody:
                if isinstance(sub, (ast.Import, ast.ImportFrom)):
                    for alias in sub.names:
                        names.add(alias.asname or alias.name.split(".")[0])
    return names


#: 模块里天然存在、不需要显式定义的名字。
_MODULE_DUNDERS = {"__file__", "__name__", "__doc__", "__package__", "__spec__"}


def _walk_scopes(node, inherited, module_names, path, problems):
    """递归下钻函数作用域，把外层函数绑定的名字继承下去。

    第一版没做这个，于是把 `cleanup()` 里用外层 `_exec_poll()` 的
    `_requests` 别名当成了错误 —— 闭包是合法的，误报会让人干脆把闸门关掉。
    """
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            scope = inherited | _collect_local_names(child)
            # 只看**直属于**这个函数的语句，嵌套函数交给下一层
            for sub in ast.iter_child_nodes(child):
                for n in ast.walk(sub):
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    if not (isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)):
                        continue
                    name = n.id
                    if (
                        name in scope
                        or name in module_names
                        or name in _MODULE_DUNDERS
                        or not name.startswith("_")
                    ):
                        continue
                    problems.append(f"{path}:{n.lineno}  {name}")
            _walk_scopes(child, scope, module_names, path, problems)
        else:
            _walk_scopes(child, inherited, module_names, path, problems)


class NoUndefinedGlobalNamesTests(unittest.TestCase):
    def test_functions_only_reference_names_that_exist(self):
        problems = []
        for path in sorted(PKG.glob("*.py")):
            src = path.read_text(encoding="utf-8")
            tree = ast.parse(src, filename=str(path))
            module_names = _module_level_names(tree)
            _walk_scopes(
                tree, set(), module_names, str(path.relative_to(REPO)), problems
            )

        self.assertEqual(
            problems,
            [],
            "这些名字在函数里被用到、但模块里根本不存在（多半是抄了别的函数里的"
            "局部 import 别名）。语法检查和单测都拦不住这类错，只有真机能 ——"
            "\n  " + "\n  ".join(problems),
        )


if __name__ == "__main__":
    unittest.main()
