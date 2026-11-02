#
# Copyright 2026 Tabsdata Inc.
#

"""Signature, parameter and docstring extraction from live SDK objects."""

from __future__ import annotations

import ast
import dataclasses
import inspect
import re
import typing
from pathlib import Path

_DEGENERATE_SIGNATURES = ("(*args, **kwargs)", "(self, *args, **kwargs)")
_ABC_BOILERPLATE_PREFIX = "Helper class that provides a standard way to create an ABC"
_GENERATOR_NOTE = "#MANUAL CHANGE"
_FUNCTIONS = (ast.FunctionDef, ast.AsyncFunctionDef)
_SECTION = re.compile(
    r"^(Args|Arguments|Parameters|Attributes|Returns|Raises|Yields|Examples?|Notes?):\s*$"
)
_ARG = re.compile(r"^(\*{0,2}\w+)\s*(?:\(([^)]*)\))?:\s*(.*)$")


def is_tabsdatak(obj) -> bool:
    return str(getattr(obj, "__module__", "")).startswith("tabsdatak")


def public_names(module):
    for name in sorted(dir(module)):
        if not name.startswith("_"):
            yield name, getattr(module, name, None)


def sanitize_repr(text: str) -> str:
    text = re.sub(
        r"<(?:function|bound method|built-in \w+) ([\w.<>]+) at 0x[0-9a-fA-F]+>",
        r"\1",
        text,
    )
    text = re.sub(r" at 0x[0-9a-fA-F]+", "", text)
    return re.sub(r"\btyping\.", "", text)


def clean_docstring(doc: str | None) -> str:
    if not doc or doc.startswith(_ABC_BOILERPLATE_PREFIX):
        return ""
    lines = [line for line in doc.splitlines() if not re.match(r"^\s*:[\w-]+:", line)]
    paragraphs = re.split(r"\n\s*\n", "\n".join(lines))
    kept = [p for p in paragraphs if not p.strip().startswith(_GENERATOR_NOTE)]
    return re.sub(r"\n{3,}", "\n\n", "\n\n".join(kept)).strip()


def docstring_of(obj) -> str:
    return clean_docstring(inspect.getdoc(obj))


def summary_of(description: str) -> str:
    return " ".join(description.split("\n\n")[0].split())


def argument_descriptions(description: str) -> dict[str, str]:
    """The ``Args:`` entries of a Google-style docstring, by parameter name."""
    found: dict[str, str] = {}
    in_args = False
    current: str | None = None
    current_indent = 0
    for line in description.splitlines():
        stripped = line.strip()
        section = _SECTION.match(stripped)
        if section:
            in_args = section.group(1) in ("Args", "Arguments", "Parameters")
            current = None
            continue
        if not in_args:
            continue
        if not stripped:
            current = None
            continue
        indent = len(line) - len(line.lstrip())
        match = _ARG.match(stripped)
        if match and (current is None or indent <= current_indent):
            current = match.group(1).lstrip("*")
            current_indent = indent
            found[current] = match.group(3)
        elif current is not None:
            found[current] = f"{found[current]} {stripped}".strip()
        else:
            in_args = False
    return found


def signature_of(obj) -> str:
    try:
        return sanitize_repr(str(inspect.signature(obj)))
    except (ValueError, TypeError):
        return "(...)"


def _annotation_label(annotation) -> str | None:
    if annotation is inspect.Parameter.empty or annotation is dataclasses.MISSING:
        return None
    if isinstance(annotation, str):
        return annotation
    name = getattr(annotation, "__name__", None)
    if name and name not in ("Annotated", "Optional", "Union"):
        return name
    label = sanitize_repr(str(annotation))
    return label if len(label) <= 120 else label[:117] + "..."


def _default_label(value):
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple, dict)) and len(repr(value)) <= 80:
        return value
    return sanitize_repr(repr(value))


def _literal_options(hint) -> list[str] | None:
    if typing.get_origin(hint) is typing.Literal:
        return [str(arg) for arg in typing.get_args(hint)]
    return None


def _parameter(name, annotation, required, default, description, hint=None) -> dict:
    entry = {
        "name": name,
        "type": _annotation_label(annotation),
        "required": required,
        "default": None if required else _default_label(default),
        "description": description,
    }
    options = _literal_options(hint)
    if options:
        entry["options"] = options
    return entry


def _dataclass_parameters(cls, descriptions: dict[str, str]) -> list[dict]:
    try:
        hints = typing.get_type_hints(cls)
    except Exception:
        hints = {}
    parameters = []
    for field in dataclasses.fields(cls):
        if not field.init:
            continue
        required = (
            field.default is dataclasses.MISSING
            and field.default_factory is dataclasses.MISSING
        )
        if required:
            default = None
        elif field.default is not dataclasses.MISSING:
            default = field.default
        else:
            default = field.default_factory()
        parameters.append(
            _parameter(
                field.name,
                field.type,
                required,
                default,
                descriptions.get(field.name),
                hints.get(field.name),
            )
        )
    return parameters


def _callable_parameters(fn, descriptions: dict[str, str]) -> list[dict]:
    try:
        parameters = inspect.signature(fn).parameters.items()
    except (ValueError, TypeError):
        return []
    entries = []
    for name, param in parameters:
        if name in ("self", "cls", "__dataclass_self__") or param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        required = param.default is inspect.Parameter.empty
        entries.append(
            _parameter(
                name,
                param.annotation,
                required,
                param.default,
                descriptions.get(name),
            )
        )
    return entries


def parameters_of(obj, description: str) -> list[dict]:
    descriptions = argument_descriptions(description)
    if isinstance(obj, type) and dataclasses.is_dataclass(obj):
        return _dataclass_parameters(obj, descriptions)
    return _callable_parameters(obj, descriptions)


def _stub_tree(obj) -> ast.Module | None:
    try:
        stub = Path(inspect.getfile(obj)).with_suffix(".pyi")
        return ast.parse(stub.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, TypeError):
        return None


def _stub_signature(node, *, method: bool) -> str:
    args = ast.unparse(node.args)
    if method:
        args = re.sub(r"^self(, )?", "", args)
    returns = f" -> {ast.unparse(node.returns)}" if node.returns else ""
    return f"{node.name}({args}){returns}"


@dataclasses.dataclass(frozen=True)
class StubMember:
    signature: str
    doc: str


def _stub_member(node, *, method: bool) -> StubMember:
    return StubMember(
        _stub_signature(node, method=method), clean_docstring(ast.get_docstring(node))
    )


def pyi_class(cls) -> tuple[str, dict[str, StubMember]]:
    """The docstring and members of ``cls`` in the ``.pyi`` stub next to it:
    engine-backed classes such as ``TableFrame`` expose neither real
    signatures nor docstrings at runtime."""
    tree = _stub_tree(cls)
    for node in tree.body if tree is not None else ():
        if isinstance(node, ast.ClassDef) and node.name == cls.__name__:
            members = {
                item.name: _stub_member(item, method=True)
                for item in node.body
                if isinstance(item, _FUNCTIONS)
            }
            return clean_docstring(ast.get_docstring(node)), members
    return "", {}


def pyi_functions(module) -> dict[str, StubMember]:
    tree = _stub_tree(module)
    if tree is None:
        return {}
    return {
        item.name: _stub_member(item, method=False)
        for item in tree.body
        if isinstance(item, _FUNCTIONS)
    }


def members_of(cls, stubs: dict[str, StubMember] | None = None) -> list[dict]:
    stubs = stubs or {}
    object_attrs = set(dir(object))
    members = []
    for name in sorted(dir(cls)):
        if name.startswith("_") or name in object_attrs:
            continue
        static = inspect.getattr_static(cls, name, None)
        live = getattr(cls, name, None)
        stub = stubs.get(name)
        if isinstance(static, property):
            description = docstring_of(static) or (stub.doc if stub else "")
            entry = {"name": name, "kind": "property", "signature": name}
        elif callable(live) and not isinstance(live, type):
            description = docstring_of(live) or (stub.doc if stub else "")
            signature = signature_of(live)
            signature = signature.replace("(self, ", "(").replace("(self)", "()")
            if signature in _DEGENERATE_SIGNATURES and stub is not None:
                signature = stub.signature
            else:
                signature = f"{name}{signature}"
            entry = {"name": name, "kind": "method", "signature": signature}
        else:
            continue
        entry["summary"] = summary_of(description)
        entry["description"] = description
        entry["parameters"] = (
            _callable_parameters(live, argument_descriptions(description))
            if entry["kind"] == "method"
            else []
        )
        members.append(entry)
    return members


def alias_summary(alias) -> str:
    if typing.get_origin(alias) is typing.Literal:
        return "One of: " + ", ".join(repr(v) for v in typing.get_args(alias))
    if isinstance(alias, type):
        return f"Alias of {alias.__module__}.{alias.__name__}"
    definition = sanitize_repr(str(alias))
    return definition if len(definition) <= 300 else definition[:297] + "..."


def iter_type_classes(annotation):
    if isinstance(annotation, type):
        yield annotation
        return
    for arg in typing.get_args(annotation):
        yield from iter_type_classes(arg)


def field_modules(classes) -> set[str]:
    """Modules of ``classes`` plus those of every class their type hints
    reach (a connector's ``credentials: GCPJson`` pulls in the module that
    defines ``GCPJson``)."""
    modules = set()
    for cls in classes:
        modules.add(cls.__module__)
        try:
            hints = typing.get_type_hints(cls)
        except Exception:
            continue
        for hint in hints.values():
            for inner in iter_type_classes(hint):
                modules.add(inner.__module__)
    return modules
