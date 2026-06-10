#
# Copyright 2026 Tabs Data Inc.
#

"""Parsing + signature canonicalization for .pyi stub regeneration.

On regeneration the existing ``.pyi`` is parsed so hand-written docstrings can be
preserved: unchanged signatures keep their pydoc, changed/new ones are flagged.
Signatures are compared after canonicalization (via ``ast``) so formatting noise
doesn't register as a change.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field


@dataclass
class PyiEntry:
    canonical_signature: str
    docstring: str | None


@dataclass
class PyiDoc:
    """Docstrings recovered from an existing .pyi, for preservation on regen."""

    module_doc: str | None = None
    class_docs: dict[str, str | None] = field(default_factory=dict)
    funcs: dict[str, PyiEntry] = field(default_factory=dict)


def parse_pyi(text: str) -> PyiDoc:
    """Recover module / class / function docstrings (and signatures) from a .pyi."""
    tree = ast.parse(text)
    doc = PyiDoc(module_doc=ast.get_docstring(tree))
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            doc.class_docs[node.name] = ast.get_docstring(node)
        elif isinstance(node, ast.FunctionDef):
            doc.funcs[node.name] = PyiEntry(
                _canonical(node.args, node.returns), ast.get_docstring(node)
            )
    return doc


def canonical_signature(signature: str) -> str:
    """Canonical form of a ``(params) -> ret`` signature string."""
    try:
        fn = ast.parse(f"def _f{signature}: ...").body[0]
    except SyntaxError:
        return signature.strip()
    return _canonical(fn.args, fn.returns)  # type: ignore[attr-defined]


def _canonical(args: ast.arguments, returns) -> str:
    text = ast.unparse(args)
    if returns is not None:
        text += " -> " + ast.unparse(returns)
    return text
