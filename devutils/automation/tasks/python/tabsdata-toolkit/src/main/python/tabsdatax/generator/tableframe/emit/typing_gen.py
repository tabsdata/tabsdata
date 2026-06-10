#
# Copyright 2026 Tabs Data Inc.
#

"""Rewrite signature type annotations so no polars (or numpy) type names leak.

Option C: keep our wrapper types and safe stdlib/typing constructs, rewrite
``LazyFrame`` -> ``TableFrame``, and DROP polars/numpy types from unions (a
standalone foreign type, or an emptied union, becomes ``Any``). Polars type
*aliases* (``IntoExpr``, ``ConcatMethod`` (a ``Literal``), …) are redefined in a
generated ``_typing.pyi`` — their definitions resolved from ``polars._typing``
and rewritten by the same rules.
"""

from __future__ import annotations

import ast
import inspect

# stdlib / typing constructs that are fine to keep verbatim (not leaks).
_SAFE = {
    "Any",
    "None",
    "True",
    "False",
    "str",
    "int",
    "float",
    "bool",
    "bytes",
    "bytearray",
    "object",
    "type",
    "range",
    "complex",
    "list",
    "dict",
    "tuple",
    "set",
    "frozenset",
    "Sequence",
    "Iterable",
    "Iterator",
    "Mapping",
    "MutableMapping",
    "Collection",
    "Optional",
    "Union",
    "Literal",
    "Callable",
    "Awaitable",
    "Coroutine",
    "ContextManager",
    "date",
    "datetime",
    "time",
    "timedelta",
    "Decimal",
    "Path",
    "IO",
    "TextIO",
    "BinaryIO",
    "IOBase",
    "Ellipsis",
    "memoryview",
}
# fixed import preamble for the safe names above that need importing.
_PREAMBLE = [
    "from typing import (",
    "    Any, Awaitable, Callable, Collection, ContextManager, "
    "Iterable, Iterator, Literal,",
    "    Mapping, MutableMapping, Optional, Sequence, Union,",
    ")",
    "from datetime import date, datetime, time, timedelta",
    "from decimal import Decimal",
    "from pathlib import Path",
]


class _Rewriter:
    """Rewrites one annotation AST, tracking which of our types / aliases it used."""

    def __init__(self, our_types: set[str], alias_names: set[str]) -> None:
        self._our = our_types
        self._aliases = alias_names
        self.used_our: set[str] = set()
        self.used_aliases: set[str] = set()

    def rewrite_standalone(self, node: ast.expr) -> ast.expr:
        return self._rewrite(node) or ast.Name("Any", ast.Load())

    def _rewrite(self, node: ast.expr) -> ast.expr | None:
        if isinstance(node, ast.Name):
            return self._name(node.id)
        if isinstance(node, ast.Constant):
            if node.value is None:
                return ast.Constant(None)
            if isinstance(node.value, str):  # forward-ref string -> parse + rewrite
                try:
                    return self._rewrite(ast.parse(node.value, mode="eval").body)
                except SyntaxError:
                    return None
            return node  # numeric/bool literal value (inside Literal[...])
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            return self._union([self._rewrite(m) for m in _flatten_or(node)])
        if isinstance(node, ast.Subscript):
            return self._subscript(node)
        if isinstance(node, ast.Attribute):  # e.g. pl.DataTypeExpr, np.ndarray
            return None
        return None

    def _name(self, name: str) -> ast.expr | None:
        if name == "LazyFrame":
            self.used_our.add("TableFrame")
            return ast.Name("TableFrame", ast.Load())
        if name in self._our:
            self.used_our.add(name)
            return ast.Name(name, ast.Load())
        if name in self._aliases:
            self.used_aliases.add(name)
            return ast.Name(name, ast.Load())
        if name in _SAFE:
            return ast.Name(name, ast.Load())
        return None  # foreign: polars/numpy class or unknown TypeVar

    def _subscript(self, node: ast.Subscript) -> ast.expr | None:
        head = _head(node.value)
        if head == "Literal":
            return node  # literal values are ours now; keep verbatim
        if head == "Optional":
            inner = self.rewrite_standalone(node.slice)
            return ast.Subscript(ast.Name("Optional", ast.Load()), inner, ast.Load())
        if head == "Union":
            members = (
                node.slice.elts if isinstance(node.slice, ast.Tuple) else [node.slice]
            )
            return self._union([self._rewrite(m) for m in members])
        if head not in _SAFE:
            return None  # foreign container/generic
        return ast.Subscript(node.value, self._rewrite_slice(node.slice), ast.Load())

    def _rewrite_slice(self, node: ast.expr) -> ast.expr:
        if isinstance(node, ast.Tuple):
            return ast.Tuple([self._slice_elt(e) for e in node.elts], ast.Load())
        return self._slice_elt(node)

    def _slice_elt(self, node: ast.expr) -> ast.expr:
        if isinstance(node, ast.List):  # Callable[[args], ret] arg list
            return ast.List([self.rewrite_standalone(e) for e in node.elts], ast.Load())
        return self.rewrite_standalone(node)

    def _union(self, parts: list[ast.expr | None]) -> ast.expr:
        kept: list[ast.expr] = []
        seen: set[str] = set()
        for part in parts:
            if part is None:
                continue
            key = ast.dump(part)
            if key not in seen:
                seen.add(key)
                kept.append(part)
        if not kept:
            return ast.Name("Any", ast.Load())
        result = kept[0]
        for part in kept[1:]:
            result = ast.BinOp(result, ast.BitOr(), part)
        return result


def _head(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _flatten_or(node: ast.expr) -> list[ast.expr]:
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return _flatten_or(node.left) + _flatten_or(node.right)
    return [node]


class TypingGen:
    """Rewrites signatures and emits a `_typing.pyi` for the aliases used."""

    def __init__(
        self, our_types: set[str], class_module: dict[str, str], root_module: str
    ) -> None:
        self._our = our_types
        self._class_module = class_module  # our class name -> module it's defined in
        self._root = root_module
        self._alias_defs = _polars_alias_defs()  # polars._typing alias name -> def AST
        self._alias_names = set(self._alias_defs)

    def rewrite_signature(self, signature: str) -> tuple[str, set[str], set[str]]:
        """Return (rewritten signature, used our-types, used alias names)."""
        rewriter = _Rewriter(self._our, self._alias_names)
        try:
            fn = ast.parse(f"def _f{signature}: ...").body[0]
        except SyntaxError:
            return signature, set(), set()
        for arg in _all_args(fn):
            if arg.annotation is not None:
                arg.annotation = rewriter.rewrite_standalone(arg.annotation)
        ret = ""
        if fn.returns is not None:
            ret = " -> " + ast.unparse(rewriter.rewrite_standalone(fn.returns))
        return (
            f"({ast.unparse(fn.args)}){ret}",
            rewriter.used_our,
            rewriter.used_aliases,
        )

    def render_typing_pyi(self, used_aliases: set[str]) -> str:
        """Generate _typing.pyi defining every used alias (resolved transitively)."""
        resolved: dict[str, str] = {}
        used_our: set[str] = set()
        pending = set(used_aliases)
        while pending:
            name = pending.pop()
            if name in resolved:
                continue
            rewriter = _Rewriter(self._our, self._alias_names)
            definition = self._alias_defs.get(name)
            if definition is None:
                resolved[name] = "Any"
                continue
            resolved[name] = ast.unparse(rewriter.rewrite_standalone(definition))
            used_our |= rewriter.used_our
            pending |= rewriter.used_aliases - set(resolved)
        lines = ["from __future__ import annotations", ""]
        lines += _PREAMBLE + ["from typing import TypeAlias", ""]
        lines += self._our_imports(used_our, in_module=None)
        lines.append("")
        for name in sorted(resolved):
            lines.append(f"{name}: TypeAlias = {resolved[name]}")
        return "\n".join(lines) + "\n"

    def imports_for(
        self, used_our: set[str], used_aliases: set[str], in_module: str
    ) -> list[str]:
        lines = list(_PREAMBLE)
        lines += self._our_imports(used_our, in_module)
        if used_aliases:
            lines.append(
                f"from {self._root}._typing import " + ", ".join(sorted(used_aliases))
            )
        return lines

    def _our_imports(self, used_our: set[str], in_module: str | None) -> list[str]:
        lines = []
        for name in sorted(used_our):
            module = self._class_module.get(name, self._root)
            if module == in_module:
                continue  # defined in this very stub
            lines.append(f"from {module} import {name}")
        return lines


def _all_args(fn: ast.FunctionDef) -> list[ast.arg]:
    args = list(fn.args.posonlyargs) + list(fn.args.args) + list(fn.args.kwonlyargs)
    if fn.args.vararg:
        args.append(fn.args.vararg)
    if fn.args.kwarg:
        args.append(fn.args.kwarg)
    return args


def _polars_alias_defs() -> dict[str, ast.expr]:
    """Map every type-alias assignment in polars._typing to its definition AST."""
    import polars._typing as pt

    tree = ast.parse(inspect.getsource(pt))
    defs: dict[str, ast.expr] = {}

    def collect(body):
        for node in body:
            if isinstance(node, ast.Assign) and node.value is not None:
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        defs[target.id] = node.value
            elif (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.value is not None
            ):
                defs[node.target.id] = node.value
            elif isinstance(node, ast.If):  # TYPE_CHECKING blocks
                collect(node.body)

    collect(tree.body)
    return defs
