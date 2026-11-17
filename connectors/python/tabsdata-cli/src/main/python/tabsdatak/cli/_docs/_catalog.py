#
# Copyright 2026 Tabsdata Inc.
#

"""The ``tdk docs`` catalog: every documented symbol of the installed
``tabsdatak`` packages, enumerated from the live objects."""

from __future__ import annotations

import dataclasses
import enum
import importlib
import inspect
import pkgutil
import re
import sys
from collections.abc import Iterable

from tabsdatak.cli._docs._introspect import (
    alias_summary,
    docstring_of,
    field_modules,
    is_tabsdatak,
    members_of,
    parameters_of,
    public_names,
    pyi_class,
    pyi_functions,
    signature_of,
    summary_of,
)

_WORD_RANK = 100

_ROLE_GROUP = {
    "source": "sources",
    "stream_source": "sources",
    "destination": "destinations",
}
_STATUS_ALIASES = {
    "execution": ("plan",),
    "function_run": ("fnrun",),
    "transaction": ("trx",),
}
_STATUS_COMMANDS = {
    "execution": "tdk plan list",
    "function_run": "tdk fnrun list",
    "transaction": "tdk trx list",
    "worker": "tdk worker list",
}


@dataclasses.dataclass
class DocEntry:
    symbol: str
    qualified_name: str
    category: str
    kind: str
    signature: str | None = None
    summary: str = ""
    description: str = ""
    parameters: list[dict] = dataclasses.field(default_factory=list)
    members: list[dict] = dataclasses.field(default_factory=list)
    connection_types: list[str] = dataclasses.field(default_factory=list)
    see_also: list[str] = dataclasses.field(default_factory=list)
    aliases: tuple[str, ...] = ()
    extra: dict = dataclasses.field(default_factory=dict)

    def brief(self) -> dict:
        return {
            "symbol": self.symbol,
            "qualified_name": self.qualified_name,
            "category": self.category,
            "kind": self.kind,
            "summary": self.summary,
        }

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "qualified_name": self.qualified_name,
            "category": self.category,
            "kind": self.kind,
            "signature": self.signature,
            "summary": self.summary,
            "description": self.description,
            "parameters": self.parameters,
            "members": [
                {key: member[key] for key in ("name", "kind", "signature", "summary")}
                for member in self.members
            ],
            "connection_types": self.connection_types,
            "see_also": self.see_also,
            **self.extra,
        }

    def member(self, name: str) -> DocEntry | None:
        for member in self.members:
            if member["name"] == name:
                return DocEntry(
                    symbol=f"{self.symbol}.{name}",
                    qualified_name=f"{self.qualified_name}.{name}",
                    category=self.category,
                    kind=member["kind"],
                    signature=member["signature"],
                    summary=member["summary"],
                    description=member["description"],
                    parameters=member["parameters"],
                    see_also=[f"tdk docs {self.symbol}"],
                )
        return None


class UnknownSymbol(LookupError):
    def __init__(self, symbol: str, suggestions: list[DocEntry]):
        super().__init__(symbol)
        self.symbol = symbol
        self.suggestions = suggestions


class AmbiguousSymbol(LookupError):
    def __init__(self, symbol: str, candidates: list[DocEntry]):
        super().__init__(symbol)
        self.symbol = symbol
        self.candidates = candidates


def _qualified(obj) -> str:
    return f"{obj.__module__}.{obj.__qualname__}"


def _class_entry(
    cls, category: str, kind: str = "class", doc: str = "", **fields
) -> DocEntry:
    description = docstring_of(cls) or doc
    return DocEntry(
        symbol=cls.__name__,
        qualified_name=_qualified(cls),
        category=category,
        kind=kind,
        signature=f"{cls.__name__}{signature_of(cls)}",
        summary=summary_of(description),
        description=description,
        parameters=parameters_of(cls, description),
        **fields,
    )


def _function_entry(name: str, fn, category: str, doc: str = "", **fields) -> DocEntry:
    description = docstring_of(fn) or doc
    return DocEntry(
        symbol=name,
        qualified_name=f"{fn.__module__}.{name}",
        category=category,
        kind="function",
        signature=f"{name}{signature_of(fn)}",
        summary=summary_of(description),
        description=description,
        parameters=parameters_of(fn, description),
        **fields,
    )


def _template_hints(types: Iterable[str]) -> list[str]:
    return [f"tdk connection template -t {t} -f conn.yaml" for t in types]


def _spec_entry(module, name: str, alias, category: str) -> DocEntry:
    return DocEntry(
        symbol=name,
        qualified_name=f"{module.__name__}.{name}",
        category=category,
        kind="type",
        summary=alias_summary(alias),
    )


def _api() -> list[DocEntry]:
    import tabsdatak.api as api
    from tabsdatak.error import ErrorCode
    from tabsdatak.tableframe import TableFrame

    entries = []
    for name, obj in public_names(api):
        if name.endswith("Spec"):
            entries.append(_spec_entry(api, name, obj, "api"))
        elif isinstance(obj, type):
            if (
                not is_tabsdatak(obj)
                or obj is TableFrame
                or (issubclass(obj, api.Trigger) and obj is not api.Trigger)
                or issubclass(obj, ErrorCode)
            ):
                continue
            entry = _class_entry(obj, "api")
            entry.members = members_of(obj)
            entries.append(entry)
        elif inspect.isfunction(obj) and is_tabsdatak(obj):
            entries.append(_function_entry(name, obj, "api"))
    return entries


def _triggers() -> list[DocEntry]:
    import tabsdatak.api as api

    return [
        _class_entry(obj, "triggers")
        for _, obj in public_names(api)
        if isinstance(obj, type)
        and issubclass(obj, api.Trigger)
        and obj is not api.Trigger
    ]


def _connectors() -> list[DocEntry]:
    from tabsdatak.cli._scaffold import binding_class, bindings

    by_class: dict[type, DocEntry] = {}
    connections: dict[type, dict] = {}
    for role, type_, defx in bindings():
        cls = binding_class(role, defx)
        entry = by_class.get(cls)
        if entry is None:
            entry = _class_entry(
                cls,
                "connectors",
                extra={
                    "role": role,
                    "system": defx.system,
                    "connection_class": defx.conn.__name__,
                    "collection_group": _ROLE_GROUP[role],
                },
            )
            entry.members = members_of(cls)
            by_class[cls] = entry
        entry.connection_types.append(type_)
        bound = connections.setdefault(
            defx.conn, {"system": defx.system, "types": [], "classes": []}
        )
        bound["types"].append(type_)
        if cls.__name__ not in bound["classes"]:
            bound["classes"].append(cls.__name__)
    for entry in by_class.values():
        entry.see_also = [
            *_template_hints(entry.connection_types),
            f"tdk docs {entry.extra['connection_class']}",
        ]
    entries = list(by_class.values())
    for conn, bound in connections.items():
        entries.append(
            _class_entry(
                conn,
                "connectors",
                connection_types=sorted(bound["types"]),
                see_also=[
                    *_template_hints(sorted(bound["types"])),
                    *(f"tdk docs {name}" for name in sorted(bound["classes"])),
                ],
                extra={"role": "connection", "system": bound["system"]},
            )
        )
    import tabsdatak.conn.common.types as common_types

    modules = {common_types.__name__: common_types}
    for cls in (*by_class, *connections):
        module = sys.modules.get(cls.__module__)
        if module is not None and is_tabsdatak(cls):
            modules[module.__name__] = module
    for module_name in sorted(modules):
        for name, obj in public_names(modules[module_name]):
            if name.endswith("Spec"):
                entries.append(
                    _spec_entry(modules[module_name], name, obj, "connectors")
                )
    return sorted(entries, key=lambda e: (e.symbol, e.qualified_name))


def _credentials() -> list[DocEntry]:
    from tabsdatak.cli._scaffold import binding_class, bindings

    classes = []
    for role, _, defx in bindings():
        classes.extend((defx.conn, binding_class(role, defx)))
    seen: set[str] = set()
    entries = []
    for module_name in sorted(field_modules(classes)):
        module = sys.modules.get(module_name)
        if module is None:
            continue
        for name, obj in public_names(module):
            if not isinstance(obj, type) or not (
                "Credentials" in name or name.endswith("Secret")
            ):
                continue
            qualified = _qualified(obj)
            if qualified in seen or name != obj.__name__:
                continue
            seen.add(qualified)
            entries.append(_class_entry(obj, "credentials"))
    return sorted(entries, key=lambda e: e.symbol)


def _engine_class(cls) -> DocEntry:
    doc, stubs = pyi_class(cls)
    entry = _class_entry(cls, "tableframe", doc=doc)
    if "__init__" in stubs:
        entry.signature = stubs["__init__"].signature.replace(
            "__init__", cls.__name__, 1
        )
    entry.members = members_of(cls, stubs)
    return entry


def _tableframe() -> list[DocEntry]:
    import tabsdatak.tableframe.functions as functions
    from tabsdatak.tableframe import TableFrame
    from tabsdatak.tableframe.datetime import ExprDateTimeNameSpace
    from tabsdatak.tableframe.expr import Expr
    from tabsdatak.tableframe.group_by import LazyGroupBy
    from tabsdatak.tableframe.string import ExprStringNameSpace

    entries = [
        _engine_class(cls)
        for cls in (
            TableFrame,
            Expr,
            LazyGroupBy,
            ExprStringNameSpace,
            ExprDateTimeNameSpace,
        )
    ]
    stubs = pyi_functions(functions)
    for name, obj in public_names(functions):
        if name in stubs and callable(obj):
            fn_entry = _function_entry(name, obj, "tableframe", doc=stubs[name].doc)
            fn_entry.signature = stubs[name].signature
            fn_entry.qualified_name = f"{functions.__name__}.{name}"
            entries.append(fn_entry)
    return entries


def _types() -> list[DocEntry]:
    import tabsdatak.tableframe.datatypes as datatypes

    entries = []
    for name, obj in public_names(datatypes):
        if isinstance(obj, type) and name == obj.__name__:
            entry = _class_entry(obj, "types", kind="type")
            entry.qualified_name = f"{datatypes.__name__}.{name}"
            entries.append(entry)
    return entries


def _statuses() -> list[DocEntry]:
    import tabsdatak.sdk.status_utils as status_utils
    from tabsdatak.sdk.status_utils.groups import Entity, health, is_final

    entries = []
    for info in sorted(
        pkgutil.iter_modules(status_utils.__path__), key=lambda m: m.name
    ):
        module = importlib.import_module(f"{status_utils.__name__}.{info.name}")
        prefix = info.name.upper()
        mapping = getattr(module, f"{prefix}_STATUS_MAPPING", None)
        if not mapping:
            continue
        entity = Entity(info.name)
        statuses = [
            {
                "label": label,
                "name": name,
                "health": health(entity, label).value,
                "final": is_final(entity, label),
            }
            for label, name in mapping.items()
        ]
        extra = {"statuses": statuses}
        triggers = getattr(module, f"{prefix}_TRIGGER_TYPE_MAPPING", None)
        if triggers:
            extra["trigger_types"] = [
                {"label": label, "name": name} for label, name in triggers.items()
            ]
        entries.append(
            DocEntry(
                symbol=info.name,
                qualified_name=module.__name__,
                category="statuses",
                kind="status",
                summary=f"Statuses of the {info.name} entity.",
                aliases=_STATUS_ALIASES.get(info.name, ()),
                see_also=[_STATUS_COMMANDS[info.name]]
                if info.name in _STATUS_COMMANDS
                else [],
                extra=extra,
            )
        )
    return entries


def _import_surface() -> None:
    import tabsdatak.api  # noqa: F401
    import tabsdatak.sdk.tabsdata_server  # noqa: F401
    import tabsdatak.tableframe  # noqa: F401
    from tabsdatak.cli._scaffold import bindings

    bindings()


def _errors() -> list[DocEntry]:
    from tabsdatak.error import ErrorCode

    _import_surface()
    code_enums: set[type] = set()
    for module_name, module in list(sys.modules.items()):
        if not module_name.startswith("tabsdatak") or module is None:
            continue
        for obj in list(vars(module).values()):
            if (
                isinstance(obj, type)
                and issubclass(obj, ErrorCode)
                and obj is not ErrorCode
                and issubclass(obj, enum.Enum)
            ):
                code_enums.add(obj)
    entries = []
    exceptions: dict[str, type] = {}
    for code_enum in code_enums:
        for name, member in code_enum.__members__.items():
            definition = member.value
            exception = definition.exception
            exceptions[_qualified(exception)] = exception
            entries.append(
                DocEntry(
                    symbol=name,
                    qualified_name=f"{_qualified(code_enum)}.{name}",
                    category="errors",
                    kind="error",
                    summary=definition.message_template,
                    see_also=[f"tdk docs {_qualified(exception)}"],
                    extra={
                        "enum": code_enum.__name__,
                        "exception": exception.__name__,
                        "message": definition.message_template,
                    },
                )
            )
    entries.extend(
        _class_entry(exception, "errors") for _, exception in sorted(exceptions.items())
    )
    return sorted(entries, key=lambda e: (e.symbol, e.qualified_name))


def _practices() -> list[DocEntry]:
    from tabsdatak.cli._docs._practices import load_practices

    return [
        DocEntry(
            symbol=practice.slug,
            qualified_name=f"practices.{practice.slug}",
            category="practices",
            kind="practice",
            summary=practice.summary,
            description=f"**{practice.title}**\n\n{practice.body}",
            see_also=[
                *(f"tdk docs show --name {symbol}" for symbol in practice.applies_to),
                *(f"tdk examples show --name {name}" for name in practice.examples),
            ],
        )
        for practice in load_practices()
    ]


_BUILDERS = {
    "api": _api,
    "triggers": _triggers,
    "connectors": _connectors,
    "tableframe": _tableframe,
    "types": _types,
    "errors": _errors,
    "statuses": _statuses,
    "credentials": _credentials,
    "practices": _practices,
}
CATEGORIES = tuple(_BUILDERS)


class Catalog:
    def __init__(self, entries: Iterable[DocEntry]):
        self.entries: list[DocEntry] = []
        self._by_qualified: dict[str, DocEntry] = {}
        self._by_name: dict[str, list[DocEntry]] = {}
        self._by_type: dict[str, DocEntry] = {}
        for entry in entries:
            if entry.qualified_name in self._by_qualified:
                continue
            self.entries.append(entry)
            self._by_qualified[entry.qualified_name] = entry
            for name in (entry.symbol, *entry.aliases):
                self._by_name.setdefault(name, []).append(entry)
            if entry.extra.get("role") not in (None, "connection"):
                for type_ in entry.connection_types:
                    self._by_type[type_] = entry

    def resolve(self, symbol: str) -> DocEntry:
        if symbol in self._by_qualified:
            return self._by_qualified[symbol]
        matches = self._by_name.get(symbol) or [
            entry
            for name, entries in self._by_name.items()
            if name.lower() == symbol.lower()
            for entry in entries
        ]
        if len(matches) == 1:
            return matches[0]
        if matches:
            raise AmbiguousSymbol(symbol, matches)
        if symbol in self._by_type:
            return self._by_type[symbol]
        owner_name, _, member_name = symbol.rpartition(".")
        if owner_name:
            try:
                owner = self.resolve(owner_name)
            except UnknownSymbol:
                owner = None
            member = owner.member(member_name) if owner is not None else None
            if member is not None:
                return member
        raise UnknownSymbol(symbol, self.search(symbol)[:5])

    def search(self, term: str) -> list[DocEntry]:
        needle = term.lower()
        scored = []
        for entry in self.entries:
            for candidate in _with_members(entry):
                score = _score(candidate, needle)
                if score is not None:
                    scored.append((score, candidate.symbol, candidate))
        if not scored:
            scored = self._by_words(needle)
        scored.sort(key=lambda item: (item[0], item[1]))
        return [candidate for _, _, candidate in scored]

    def _by_words(self, needle: str) -> list[tuple[int, str, DocEntry]]:
        """A phrase no name or summary contains: rank by its words (three
        letters or more, or holding a digit) found in a name, then in a
        summary; error codes last."""
        words = [
            word
            for word in re.findall(r"\w+", needle)
            if len(word) >= 3 or any(c.isdigit() for c in word)
        ]
        if len(words) < 2:
            return []
        scored = []
        for entry in self.entries:
            for candidate in _with_members(entry):
                name = candidate.symbol.lower()
                summary = candidate.summary.lower()
                weight = sum(3 * (w in name) + (w in summary) for w in words)
                if weight:
                    last = _WORD_RANK if candidate.kind == "error" else 0
                    rank = _WORD_RANK - weight + last
                    scored.append((rank, candidate.symbol, candidate))
        return scored


def _with_members(entry: DocEntry) -> list[DocEntry]:
    return [entry, *(entry.member(member["name"]) for member in entry.members)]


def _score(entry: DocEntry, needle: str) -> int | None:
    names = [entry.symbol.lower(), *(alias.lower() for alias in entry.aliases)]
    tail = entry.symbol.rpartition(".")[2].lower()
    if needle in names or needle == tail:
        return 0
    if any(name.startswith(needle) for name in names) or tail.startswith(needle):
        return 1
    if any(needle in name for name in names):
        return 2
    if needle in entry.qualified_name.lower() or needle in entry.connection_types:
        return 3
    if needle in entry.summary.lower():
        return 4
    return None


def _link_practices(entries: list[DocEntry]) -> list[DocEntry]:
    from tabsdatak.cli._docs._practices import load_practices

    by_symbol: dict[str, list[str]] = {}
    for practice in load_practices():
        for symbol in practice.applies_to:
            by_symbol.setdefault(symbol, []).append(practice.slug)
    for entry in entries:
        if entry.category == "practices":
            continue
        entry.see_also += [
            f"tdk docs show --name {slug}" for slug in by_symbol.get(entry.symbol, ())
        ]
    return entries


def build_catalog(categories: Iterable[str] | None = None) -> Catalog:
    selected = list(categories) if categories else CATEGORIES
    return Catalog(
        _link_practices([entry for name in selected for entry in _BUILDERS[name]()])
    )
