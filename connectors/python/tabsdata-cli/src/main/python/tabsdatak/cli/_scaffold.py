#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk examples scaffold``: the skeleton of a publisher, transformer or subscriber,
its source or destination built from the connector registry with ``<value>``
placeholders where the connector needs a value. The function never names a
connection: the collection's connection is resolved on the server."""

from __future__ import annotations

import enum
import json
import keyword
import re
import typing
from dataclasses import dataclass

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.cli._examples import FUNCTION_KINDS

# noinspection PyProtectedMember
from tabsdatak.cli.template import _first_concrete, _members

PLACEHOLDER = "<value>"

_ROLE_KIND = {
    "source": "publisher",
    "stream_source": "stream_publisher",
    "destination": "subscriber",
}
_NAME_PREFIX = {"publisher": "pub", "stream_publisher": "stream", "subscriber": "sub"}
_BATCH_MAX_SECS = 300
_LINE = 88


class ScaffoldError(Exception):
    """A scaffold that cannot be made from the options given."""


@dataclass(frozen=True)
class Connector:
    type: str
    system: str
    kind: str
    cls: type
    many_files: bool

    def brief(self) -> dict:
        return {
            "type": self.type,
            "system": self.system,
            "kind": self.kind,
            "class": self.cls.__name__,
            "module": self.cls.__module__,
        }


@dataclass(frozen=True)
class Scaffold:
    source: str
    name: str
    kind: str
    connector: Connector | None


def bindings() -> list[tuple[str, str, object]]:
    from tabsdatak._connectors import avail_dests, avail_srcs, avail_stream_srcs

    found = []
    for role, registry in (
        ("source", avail_srcs()),
        ("stream_source", avail_stream_srcs()),
        ("destination", avail_dests()),
    ):
        for type_, defx in sorted(registry.items()):
            # A leading "." marks framework plumbing such as ".cdc-stage".
            if not type_.startswith("."):
                found.append((role, type_, defx))
    return found


def binding_class(role: str, defx):
    return defx.dest if role == "destination" else defx.src


def connectors() -> list[Connector]:
    """Every registered connector a function can bind to."""
    from tabsdatak.spi import TableMode

    return [
        Connector(
            type=type_,
            system=defx.system,
            kind=_ROLE_KIND[role],
            cls=binding_class(role, defx),
            many_files=getattr(defx, "table_mode", None) is TableMode.ARRAY,
        )
        for role, type_, defx in bindings()
    ]


def connector(type_: str) -> Connector:
    for candidate in connectors():
        if candidate.type == type_:
            return candidate
    known = ", ".join(c.type for c in connectors())
    raise ScaffoldError(f"Unknown connector '{type_}'; known connectors: {known}.")


class _Imports:
    def __init__(self):
        self.api: set[str] = {"TableFrameSpec"}
        self.modules: dict[str, set[str]] = {}
        self.plain: set[str] = set()

    def add(self, cls: type) -> str:
        self.modules.setdefault(cls.__module__, set()).add(cls.__name__)
        return cls.__name__

    def names(self) -> set[str]:
        """Every name the imports bind in the module."""
        bound = {line.split()[-1] for line in self.plain}
        return bound.union(self.api, *self.modules.values())

    def render(self) -> str:
        lines = [*sorted(self.plain)]
        if lines:
            lines.append("")
        lines.append(f"from tabsdatak.api import {', '.join(sorted(self.api))}")
        for module in sorted(self.modules):
            names = ", ".join(sorted(self.modules[module]))
            lines.append(f"from {module} import {names}")
        return "\n".join(lines)


def _datagen_data(imports: _Imports, cls: type) -> str:
    imports.plain.add("import polars as pl")
    return f'{imports.add(cls)}(pl.DataFrame({{"<column>": ["{PLACEHOLDER}"]}}))'


def _qualified(cls: type) -> str:
    return f"{cls.__module__}:{cls.__qualname__}"


_SPECIAL = {"tabsdatak.conn.dev:Data": _datagen_data}

# What the field types alone do not tell: a name format stricter than a bare
# placeholder, or the value a required field should start from.
_PLACEHOLDERS = {"tabsdatak.conn.bigquery:BigQueryDest": "`<value>`"}
_CDC_START = {"start_from": '"full"'}
_VALUES = {
    "tabsdatak.conn.db2:Db2CdcStreamSrc": _CDC_START,
    "tabsdatak.conn.mysql:MySQLCdcStreamSrc": _CDC_START,
    "tabsdatak.conn.postgres:PostgresCdcStreamSrc": _CDC_START,
}


def _value(
    annotation,
    imports: _Imports,
    count: int = 1,
    placeholder: str = PLACEHOLDER,
) -> str:
    members = _members(annotation)
    if not members:
        return "None"
    first = members[0]
    origin = typing.get_origin(first)
    if origin is typing.Literal:
        return _literal(typing.get_args(first)[0])
    if origin is list:
        (item,) = typing.get_args(first)
        values = [_value(item, imports, placeholder=placeholder)] * count
        return "[" + ", ".join(values) + "]"
    if origin is tuple:
        items = [
            "None"
            if type(None) in typing.get_args(item)
            else _value(item, imports, placeholder=placeholder)
            for item in typing.get_args(first)
        ]
        return "(" + ", ".join(items) + ("," if len(items) == 1 else "") + ")"
    if first is str:
        return _literal(placeholder)
    if first is bool:
        return "False"
    if first in (int, float):
        return repr(first(0))
    if isinstance(first, type) and issubclass(first, enum.Enum):
        return f"{imports.add(first)}.{next(iter(first)).name}"
    if isinstance(first, type):
        special = _SPECIAL.get(_qualified(first))
        if special is not None:
            return special(imports, first)
        if hasattr(first, "__pydantic_fields__") or first.__subclasses__():
            return _constructor(_first_concrete(first), imports)
    raise ScaffoldError(f"Cannot write a placeholder for a '{first}' value.")


def _constructor(cls: type, imports: _Imports, count: int = 1) -> str:
    """``Class(field=placeholder, ...)`` over the required fields; the first
    list field holds one placeholder per table."""
    placeholder = _PLACEHOLDERS.get(_qualified(cls), PLACEHOLDER)
    values = _VALUES.get(_qualified(cls), {})
    arguments = []
    sized = False
    for field_name, field in cls.__pydantic_fields__.items():
        if not field.is_required():
            continue
        is_list = any(typing.get_origin(m) is list for m in _members(field.annotation))
        slots = count if is_list and not sized else 1
        sized = sized or is_list
        value = values.get(field_name) or _value(
            field.annotation, imports, slots, placeholder
        )
        arguments.append(f"{field_name}={value}")
    if count > 1 and not sized:
        raise ScaffoldError(
            f"{cls.__name__} takes a single table; pass one table, not {count}."
        )
    name = imports.add(cls)
    if not arguments:
        return f"{name}()"
    return f"{name}(\n" + "".join(f"        {a},\n" for a in arguments) + "    )"


def _literal(value) -> str:
    """A Python literal in the double-quoted style of formatted code."""
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(_literal(item) for item in value) + "]"
    return repr(value)


def _signature(name: str, params: list[str], returns: str) -> str:
    head, tail = f"def {name}(", f") -> {returns}:"
    inline = head + ", ".join(params) + tail
    if len(inline) <= _LINE:
        return inline
    wrapped = "    " + ", ".join(params)
    if len(wrapped) <= _LINE:
        return f"{head}\n{wrapped}\n{tail}"
    return head + "\n" + "".join(f"    {param},\n" for param in params) + tail


def _identifier(reference: str, taken: set[str]) -> str:
    table = reference.split("@", 1)[0].rsplit("/", 1)[-1]
    name = re.sub(r"\W", "_", table).strip("_").lower() or "table"
    if name[0].isdigit():
        name = f"t_{name}"
    base, index = name, 2
    while name in taken:
        name, index = f"{base}_{index}", index + 1
    taken.add(name)
    return name


def _params(tables: list[str], taken: set[str]) -> list[str]:
    taken = {*taken, *keyword.kwlist}
    return [_identifier(table, taken) for table in tables]


def _returns(values: list[str]) -> tuple[str, str]:
    if len(values) == 1:
        return "TableFrameSpec", values[0]
    annotation = "tuple[" + ", ".join(["TableFrameSpec"] * len(values)) + "]"
    return annotation, ", ".join(values)


def _default_name(kind: str, bound: Connector | None) -> str:
    if bound is None:
        return "transform"
    return f"{_NAME_PREFIX[kind]}_{bound.type.replace('-', '_')}"


def _resolve(kind: str | None, type_: str | None) -> tuple[str, Connector | None]:
    bound = connector(type_) if type_ else None
    kind = kind or (bound.kind if bound else None)
    if kind not in FUNCTION_KINDS:
        raise ScaffoldError(f"Pass --kind, one of {', '.join(FUNCTION_KINDS)}.")
    if kind == "transformer":
        if bound is not None:
            raise ScaffoldError("A transformer binds no connector; drop --connector.")
        return kind, None
    if bound is None:
        choices = ", ".join(c.type for c in connectors() if c.kind == kind)
        raise ScaffoldError(f"A {kind} needs --connector, one of: {choices}.")
    if bound.kind != kind:
        raise ScaffoldError(
            f"Connector '{bound.type}' binds a {bound.kind}, not a {kind}."
        )
    return kind, bound


def build_scaffold(
    kind: str | None,
    type_: str | None = None,
    name: str | None = None,
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
) -> Scaffold:
    """A function module of ``kind``, bound to the connector registered as
    ``type_``, reading ``inputs`` and writing ``outputs``."""
    kind, bound = _resolve(kind, type_)
    name = name or _default_name(kind, bound)
    if not name.isidentifier() or keyword.iskeyword(name):
        raise ScaffoldError(f"'{name}' is not a valid Python function name.")
    if kind in ("publisher", "stream_publisher") and inputs:
        raise ScaffoldError(f"A {kind} reads its connector; it takes no --input.")
    if kind == "subscriber" and outputs:
        raise ScaffoldError("A subscriber writes its connector; it takes no --output.")
    inputs = list(inputs or ["input"]) if kind in ("transformer", "subscriber") else []
    outputs = list(outputs or ["output"]) if kind != "subscriber" else []
    imports = _Imports()
    imports.api.add(kind)
    arguments = []
    if bound is not None:
        role = "destination" if kind == "subscriber" else "source"
        count = len(inputs or outputs)
        arguments.append(f"{role}={_constructor(bound.cls, imports, count)}")
    if kind == "stream_publisher":
        arguments.append(f"batch_max_secs={_BATCH_MAX_SECS}")
    if inputs:
        arguments.append(f"input_tables={_literal(inputs)}")
    if outputs:
        arguments.append(f"output_tables={_literal(outputs)}")

    frames = kind == "stream_publisher" or (
        kind == "publisher" and bound is not None and bound.many_files
    )
    # A slot holds one TableFrame per version: a list for a source that hands
    # several, or an input that can yield several versions (@NEW, a range,
    # a list), which registration refuses to a single slot.
    lists = (
        [_api.reads_several_versions(table) for table in inputs]
        if inputs
        else [frames] * len(outputs)
    )
    if any(lists):
        imports.api.add("TableFramesSpec")
        imports.modules.setdefault("tabsdatak.tableframe.functions", set()).add(
            "concat"
        )
    if name in imports.names():
        raise ScaffoldError(
            f"'{name}' would shadow the imported '{name}'; pick another name."
        )
    params = _params(inputs or outputs, {name, *imports.names()})
    values = [
        f"concat({param} or [])" if many else param
        for param, many in zip(params, lists)
    ]
    if kind == "transformer":
        values = [values[0]] * len(outputs)
    returns, body = _returns(values)
    signature = _signature(
        name,
        [
            f"{param}: {'TableFramesSpec' if many else 'TableFrameSpec'}"
            for param, many in zip(params, lists)
        ],
        returns,
    )
    decorator = "".join(f"    {argument},\n" for argument in arguments)
    source = (
        f"{imports.render()}\n\n\n"
        f"@{kind}(\n{decorator})\n"
        f"{signature}\n"
        f"    return {body}\n"
    )
    return Scaffold(source=source, name=name, kind=kind, connector=bound)
