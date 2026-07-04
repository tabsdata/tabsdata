#
# Copyright 2026 Tabs Data Inc.
#

"""Parsing + signature canonicalization for .pyi stub regeneration.

On regeneration the existing ``.pyi`` is parsed so handwritten docstrings can be
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
    """Docstrings recovered from an existing .pyi, for preservation on regen.

    ``functions`` is keyed by ``(class_name | None, method_name)`` so same-named
    methods in different classes (e.g. dunders across the 'when/then' builders) do
    not collide.
    """

    module_doc: str | None = None
    class_docs: dict[str, str | None] = field(default_factory=dict)
    functions: dict[tuple[str | None, str], PyiEntry] = field(default_factory=dict)


def parse_pyi(text: str) -> PyiDoc:
    """Recover module / class / function docstrings (and signatures) from a .pyi."""
    tree = ast.parse(text)
    doc = PyiDoc(module_doc=ast.get_docstring(tree))
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            doc.class_docs[node.name] = ast.get_docstring(node)
            for member in node.body:
                if isinstance(member, ast.FunctionDef):
                    doc.functions[(node.name, member.name)] = PyiEntry(
                        _canonical(member.args, member.returns),
                        ast.get_docstring(member),
                    )
        elif isinstance(node, ast.FunctionDef):
            doc.functions[(None, node.name)] = PyiEntry(
                _canonical(node.args, node.returns), ast.get_docstring(node)
            )
    return doc


def canonical_signature(signature: str) -> str:
    """Canonical form of a ``(params) -> ret`` signature string."""
    try:
        function = ast.parse(f"def _f{signature}: ...").body[0]
    except SyntaxError:
        return signature.strip()
    return _canonical(function.args, function.returns)  # type: ignore[attr-defined]


def _canonical(args: ast.arguments, returns) -> str:
    text = ast.unparse(args)
    if returns is not None:
        text += " -> " + ast.unparse(returns)
    return text
