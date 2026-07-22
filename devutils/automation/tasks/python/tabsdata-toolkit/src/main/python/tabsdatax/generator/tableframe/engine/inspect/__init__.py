#
# Copyright 2026 Tabsdata Inc.
#

"""Build a manifest by introspecting the Polars in the current environment."""

from __future__ import annotations

import importlib
import inspect
import re

import polars as pl

from tabsdatax.generator.tableframe.model.model import Report, Symbol
from tabsdatax.generator.tableframe.model.spec import (
    ClassRoot,
    FunctionRoot,
    class_roots,
    function_roots,
)

# Operator dunders worth delegating (they return values, e.g. Expr arithmetic).
# Included only when a class actually defines them (not inherited from object).
OPERATOR_DUNDERS = (
    "__add__",
    "__radd__",
    "__sub__",
    "__rsub__",
    "__mul__",
    "__rmul__",
    "__truediv__",
    "__rtruediv__",
    "__floordiv__",
    "__rfloordiv__",
    "__mod__",
    "__rmod__",
    "__pow__",
    "__rpow__",
    "__neg__",
    "__pos__",
    "__abs__",
    "__invert__",
    "__eq__",
    "__ne__",
    "__lt__",
    "__le__",
    "__gt__",
    "__ge__",
    "__and__",
    "__rand__",
    "__or__",
    "__ror__",
    "__xor__",
    "__rxor__",
    "__getitem__",
)

# Augmenting ops seeded with a hook_name (the user can still edit); "-" means none.
DEFAULT_HOOKS = {
    "LazyFrame.select": "project",
    "LazyFrame.with_columns": "project",
    "LazyFrame.join": "join",
    "LazyFrame.group_by": "group_by",
    # The rolling and dynamic group-bys are also LazyGroupBy constructors.
    "LazyFrame.rolling": "group_by",
    "LazyFrame.group_by_dynamic": "group_by",
    "LazyFrame.drop": "drop",
    "LazyFrame.rename": "rename",
    "LazyGroupBy.agg": "agg",
}

# A return mentioning one of these would leak a raw polars object we do not wrap,
# so it is denied by default (the user can re-enable). Schema is wrapped and
# DataType is an aliased passthrough leaf type, so neither is a leak.
LEAKY_RETURN = re.compile(r"\b(DataFrame|Series|DataTypeExpr|Selector)\b")

# Non-literal default reprs (sentinels like <no_default>, object addresses) are
# neither valid Python nor stable across runs, so they render as `...`.
OBJECT_REPR = re.compile(r"<[^<>]*>")


def perform(module: str) -> tuple[Report, list[str]]:
    skipped: list[str] = []
    ranked: list[tuple] = []

    for class_root in class_roots():
        target_module = (
            module
            if class_root.target_submodule is None
            else f"{module}.{class_root.target_submodule}"
        )
        ranked += _scan_class(class_root, target_module, skipped)
    for function_root in function_roots():
        target_module = f"{module}.{function_root.target_submodule}"
        ranked += _scan_functions(function_root, target_module, skipped)

    ranked.sort(key=lambda entry: (entry[0], entry[1], entry[2]))
    symbols = [entry[3] for entry in ranked]
    _apply_defaults(symbols)
    return Report(
        polars_version=pl.__version__, root_module=module, symbols=symbols
    ), skipped


def _apply_defaults(symbols: list[Symbol]) -> None:
    for symbol in symbols:
        if symbol.id in DEFAULT_HOOKS:
            symbol.hook_name = DEFAULT_HOOKS[symbol.id]
        if LEAKY_RETURN.search(symbol.returns):
            symbol.whitelisted = False
        elif symbol.target_class is None and not hasattr(
            pl, symbol.id.split(".", 1)[1]
        ):
            # Free function not re-exported at the polars top level.
            symbol.whitelisted = False


def _scan_class(
    class_root: ClassRoot, target_module: str, skipped: list[str]
) -> list[tuple]:
    entries = []
    class_object = class_root.class_object
    for name in _member_names(class_object, class_root.extra_dunders):
        static = inspect.getattr_static(class_object, name, None)
        is_property = isinstance(static, property)
        function = static.fget if is_property else getattr(class_object, name)
        if function is None or not callable(function):
            continue
        signature = _signature(function)
        if signature is None:
            skipped.append(f"{class_object.__name__}.{name}")
            continue
        symbol = Symbol(
            id=f"{class_object.__name__}.{name}",
            polars_qualname=(
                f"{class_object.__module__}.{class_object.__qualname__}.{name}"
            ),
            target_module=target_module,
            target_class=class_root.target_class,
            signature=_signature_text(signature),
            returns=_format_return(signature),
            is_property=is_property,
        )
        entries.append((
            target_module,
            class_root.target_class,
            _line_number(function),
            symbol,
        ))
    return entries


def _scan_functions(
    function_root: FunctionRoot, target_module: str, skipped: list[str]
) -> list[tuple]:
    module = importlib.import_module(function_root.module_name)
    entries = []
    for name in _public_names(module):
        member = getattr(module, name)
        # Keep functions, plus callable instances like `col`/`lit` (a `Col`
        # instance); exclude re-exported classes and submodules.
        if inspect.isclass(member) or inspect.ismodule(member) or not callable(member):
            continue
        home = getattr(member, "__module__", None) or type(member).__module__
        if not home.startswith("polars.functions"):
            continue
        signature = _signature(member)
        if signature is None:
            skipped.append(f"functions.{name}")
            continue
        qualname = (
            member.__qualname__
            if inspect.isfunction(member)
            else type(member).__qualname__
        )
        symbol = Symbol(
            id=f"functions.{name}",
            polars_qualname=f"{home}.{qualname}",
            target_module=target_module,
            target_class=None,
            signature=_signature_text(signature),
            returns=_format_return(signature),
            is_property=False,
        )
        # Group by source file, then source order, so consolidated functions stay tidy.
        entries.append((target_module, "", (home, _line_number(member)), symbol))
    return entries


def _public_names(container) -> list[str]:
    return [name for name in dir(container) if not name.startswith("_")]


def _member_names(class_object, extra_dunders: tuple[str, ...] = ()) -> list[str]:
    names = _public_names(class_object)
    for dunder in OPERATOR_DUNDERS + tuple(extra_dunders):
        attribute = getattr(class_object, dunder, None)
        if attribute is not None and attribute is not getattr(object, dunder, None):
            names.append(dunder)
    return names


def _signature_text(signature: inspect.Signature) -> str:
    # noinspection PyStringConversionWithoutDunderMethod
    return OBJECT_REPR.sub("...", str(signature))


def _signature(function) -> inspect.Signature | None:
    try:
        return inspect.signature(function)
    except (ValueError, TypeError):
        # Callable instance with a hostile __getattr__/__signature__ (e.g. polars
        # `col`): introspect its __call__ and drop `self`.
        call = getattr(type(function), "__call__", None)
        if not callable(call):
            return None
        try:
            signature = inspect.signature(call)
        except (ValueError, TypeError):
            return None
        return signature.replace(parameters=list(signature.parameters.values())[1:])


def _format_return(signature: inspect.Signature) -> str:
    annotation = signature.return_annotation
    return "" if annotation is inspect.Signature.empty else str(annotation)


def _line_number(function) -> int:
    try:
        return inspect.getsourcelines(inspect.unwrap(function))[1]
    except (OSError, TypeError):
        return 0
