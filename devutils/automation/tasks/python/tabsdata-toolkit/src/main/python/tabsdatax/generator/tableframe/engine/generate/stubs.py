#
# Copyright 2026 Tabsdata Inc.
#

"""Parsing + signature canonicalization for .pyi stub regeneration.

On regeneration the existing ``.pyi`` is parsed so handwritten content can be
preserved: unchanged signatures keep their pydoc, changed/new ones are flagged.
Signatures are compared after canonicalization (via ``ast``) so formatting noise
doesn't register as a change. Comment blocks written above a member or at the end
of a class, and each member's full source (for members the generator no longer
emits but a handwritten base class still provides), are recovered too.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field


@dataclass
class PyiEntry:
    canonical_signature: str
    docstring: str | None
    comments: list[str] = field(default_factory=list)
    trailing: list[str] = field(default_factory=list)
    source: list[str] = field(default_factory=list)
    names: set[str] = field(default_factory=set)


@dataclass
class PyiDoc:
    """Content recovered from an existing .pyi, for preservation on regen.

    ``functions`` is keyed by ``(class_name | None, method_name)`` so same-named
    methods in different classes (e.g. dunders across the 'when/then' builders) do
    not collide. ``imports`` maps each imported name to the import that binds it.
    """

    module_doc: str | None = None
    class_docs: dict[str, str | None] = field(default_factory=dict)
    class_comments: dict[str, list[str]] = field(default_factory=dict)
    class_trailing: dict[str, list[str]] = field(default_factory=dict)
    functions: dict[tuple[str | None, str], PyiEntry] = field(default_factory=dict)
    imports: dict[str, str] = field(default_factory=dict)


def parse_pyi(text: str) -> PyiDoc:
    """Recover docstrings, comments, sources and imports from a .pyi."""
    tree = ast.parse(text)
    lines = text.splitlines()
    doc = PyiDoc(module_doc=_module_docstring(tree), imports=_imports(tree))
    previous: PyiEntry | None = None
    previous_end = 0
    for index, node in enumerate(tree.body):
        following = (
            _start(tree.body[index + 1])
            if index + 1 < len(tree.body)
            else len(lines) + 1
        )
        leading = [
            line
            for line in _split(lines, previous_end, _start(node), 0, previous)
            if not line[:1].isspace()
        ]
        if isinstance(node, ast.ClassDef):
            doc.class_docs[node.name] = ast.get_docstring(node)
            doc.class_comments[node.name] = leading
            member: PyiEntry | None = None
            member_end = node.lineno
            member_indent = node.col_offset + 4
            for child in node.body:
                if isinstance(child, ast.FunctionDef):
                    member_indent = child.col_offset
                    comments = _split(
                        lines, member_end, _start(child), member_indent, member
                    )
                    member = _entry(child, lines, comments)
                    doc.functions[(node.name, child.name)] = member
                member_end = child.end_lineno or member_end
            doc.class_trailing[node.name] = [
                line
                for line in _split(lines, member_end, following, member_indent, member)
                if line[:1].isspace()
            ]
            previous = None
        elif isinstance(node, ast.FunctionDef):
            previous = _entry(node, lines, leading)
            doc.functions[(None, node.name)] = previous
        else:
            previous = None
        previous_end = node.end_lineno or previous_end
    return doc


def canonical_signature(signature: str) -> str:
    """Canonical form of a ``(params) -> ret`` signature string."""
    try:
        function = ast.parse(f"def _f{signature}: ...").body[0]
    except SyntaxError:
        return signature.strip()
    return _canonical(function.args, function.returns)  # type: ignore[attr-defined]


def _entry(node: ast.FunctionDef, lines: list[str], comments: list[str]) -> PyiEntry:
    return PyiEntry(
        canonical_signature=_canonical(node.args, node.returns),
        docstring=ast.get_docstring(node),
        comments=comments,
        source=lines[_start(node) - 1 : node.end_lineno],
        names={name.id for name in ast.walk(node) if isinstance(name, ast.Name)},
    )


def _split(
    lines: list[str], after: int, before: int, indent: int, owner: PyiEntry | None
) -> list[str]:
    """Comments between ``after`` and ``before``: those indented deeper than
    ``indent`` trail ``owner`` (the member above); the rest are returned."""
    leading = []
    for line in _comments(lines, after, before):
        if owner is not None and len(line) - len(line.lstrip()) > indent:
            owner.trailing.append(line)
        else:
            leading.append(line)
    return leading


def _module_docstring(tree: ast.Module) -> str | None:
    # Earlier generator runs emitted the module docstring after the imports, where
    # Python does not treat it as one; recover it from there too.
    docstring = ast.get_docstring(tree)
    if docstring is not None:
        return docstring
    for node in tree.body:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            return None
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            return ast.get_docstring(
                ast.Module(body=[node], type_ignores=[]), clean=True
            )
    return None


def _imports(tree: ast.Module) -> dict[str, str]:
    imports: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module != "__future__":
            for alias in node.names:
                bound = alias.asname or alias.name
                imports[bound] = ast.unparse(
                    ast.ImportFrom(module=node.module, names=[alias], level=node.level)
                )
        elif isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".")[0]
                imports[bound] = ast.unparse(ast.Import(names=[alias]))
    return imports


def _start(node: ast.stmt) -> int:
    decorators = getattr(node, "decorator_list", [])
    return min([decorator.lineno for decorator in decorators] + [node.lineno])


def _comments(lines: list[str], after: int, before: int) -> list[str]:
    """Comment lines strictly between line numbers ``after`` and ``before``."""
    return [line for line in lines[after : before - 1] if line.strip().startswith("#")]


def _canonical(args: ast.arguments, returns) -> str:
    text = ast.unparse(args)
    if returns is not None:
        text += " -> " + ast.unparse(returns)
    return text
