#
# Copyright 2026 Tabs Data Inc.
#

"""Build a signature report by introspecting the Polars in the current env."""

from __future__ import annotations

import importlib
import inspect
import re

import polars as pl

from ..model.model import Report, Symbol
from .spec import ClassRoot, FunctionRoot, class_roots, function_roots

# Operator dunders worth delegating (they return values, e.g. Expr arithmetic).
# Included only when a class actually defines them (not inherited from object).
_OPERATOR_DUNDERS = (
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


# Augmenting ops get a hook_name by default (the user can still edit). "-" = none.
_DEFAULT_HOOKS = {
    "LazyFrame.select": "project",
    "LazyFrame.with_columns": "project",
    "LazyFrame.join": "join",
    "LazyFrame.group_by": "group_by",
    # rolling / dynamic group-bys are also LazyGroupBy constructors
    "LazyFrame.rolling": "group_by",
    "LazyFrame.group_by_dynamic": "group_by",
    "LazyFrame.drop": "drop",
    "LazyFrame.rename": "rename",
    "LazyGroupBy.agg": "agg",
}
# A return mentioning one of these would leak a raw polars object we don't wrap
# -> deny by default (the user can re-enable). Schema is wrapped and DataType is an
# aliased passthrough leaf type, so neither is a leak.
_LEAKY_RETURN = re.compile(r"\b(DataFrame|Series|DataTypeExpr|Selector)\b")


def build_report(root_module: str) -> tuple[Report, list[str]]:
    """Return a report of the delegatable surface plus a list of skipped symbols."""
    skipped: list[str] = []
    ranked: list[tuple] = []  # (target_module, target_class_key, order_key, Symbol)

    for cr in class_roots():
        target_module = (
            root_module
            if cr.target_submodule is None
            else f"{root_module}.{cr.target_submodule}"
        )
        ranked += _scan_class(cr, target_module, skipped)
    for fr in function_roots():
        target_module = f"{root_module}.{fr.target_submodule}"
        ranked += _scan_functions(fr, target_module, skipped)

    ranked.sort(key=lambda r: (r[0], r[1], r[2]))
    symbols = [r[3] for r in ranked]
    _apply_defaults(symbols)
    return Report(
        polars_version=pl.__version__, root_module=root_module, symbols=symbols
    ), skipped


def _apply_defaults(symbols: list[Symbol]) -> None:
    """Seed sensible hook_names/whitelist so a fresh report generates a working
    package."""
    for sym in symbols:
        if sym.id in _DEFAULT_HOOKS:
            sym.hook_name = _DEFAULT_HOOKS[sym.id]
        if _LEAKY_RETURN.search(sym.returns):
            sym.whitelisted = False  # would return an un-wrapped polars type
        elif sym.target_class is None and not hasattr(pl, sym.id.split(".", 1)[1]):
            sym.whitelisted = (
                False  # free function not re-exported at the polars top level
            )


def _scan_class(cr: ClassRoot, target_module: str, skipped: list[str]) -> list[tuple]:
    out = []
    for name in _member_names(cr.obj, cr.extra_dunders):
        static = inspect.getattr_static(cr.obj, name, None)
        is_property = isinstance(static, property)
        func = static.fget if is_property else getattr(cr.obj, name)
        if func is None or not callable(func):
            continue
        sig = _signature(func)
        if sig is None:
            skipped.append(f"{cr.obj.__name__}.{name}")
            continue
        sym = Symbol(
            id=f"{cr.obj.__name__}.{name}",
            polars_qualname=f"{cr.obj.__module__}.{cr.obj.__qualname__}.{name}",
            target_module=target_module,
            target_class=cr.target_class,
            signature=_signature_text(sig),
            returns=_fmt_return(sig),
            is_property=is_property,
        )
        out.append((target_module, cr.target_class, _lineno(func), sym))
    return out


def _scan_functions(
    fr: FunctionRoot, target_module: str, skipped: list[str]
) -> list[tuple]:
    module = importlib.import_module(fr.module_name)
    out = []
    for name in _public_names(module):
        obj = getattr(module, name)
        # functions, plus callable instances like `col`/`lit` (a `Col` instance);
        # exclude re-exported classes and submodules.
        if inspect.isclass(obj) or inspect.ismodule(obj) or not callable(obj):
            continue
        home = getattr(obj, "__module__", None) or type(obj).__module__
        if not home.startswith("polars.functions"):
            continue
        sig = _signature(obj)
        if sig is None:
            skipped.append(f"functions.{name}")
            continue
        qualname = (
            obj.__qualname__ if inspect.isfunction(obj) else type(obj).__qualname__
        )
        sym = Symbol(
            id=f"functions.{name}",
            polars_qualname=f"{home}.{qualname}",
            target_module=target_module,
            target_class=None,
            signature=_signature_text(sig),
            returns=_fmt_return(sig),
            is_property=False,
        )
        # group by source file, then source order, so consolidated functions stay tidy
        out.append((target_module, "", (home, _lineno(obj)), sym))
    return out


def _public_names(obj) -> list[str]:
    return [n for n in dir(obj) if not n.startswith("_")]


def _member_names(cls, extra_dunders: tuple[str, ...] = ()) -> list[str]:
    """Public members plus any operator dunders the class defines itself (the global
    operator set, plus any class-specific ``extra_dunders``)."""
    names = _public_names(cls)
    for dunder in _OPERATOR_DUNDERS + tuple(extra_dunders):
        attr = getattr(cls, dunder, None)
        if attr is not None and attr is not getattr(object, dunder, None):
            names.append(dunder)
    return names


# Non-literal default reprs (sentinels like <no_default>, object addresses) are
# neither valid Python nor stable across runs — render them as `...`.
_OBJECT_REPR = re.compile(r"<[^<>]*>")


def _signature_text(sig: inspect.Signature) -> str:
    return _OBJECT_REPR.sub("...", str(sig))


def _signature(func) -> inspect.Signature | None:
    try:
        return inspect.signature(func)
    except (ValueError, TypeError):
        # Callable instance with a hostile __getattr__/__signature__ (e.g. polars
        # `col`): introspect its __call__ and drop `self`.
        call = getattr(type(func), "__call__", None)
        if call is None:
            return None
        try:
            sig = inspect.signature(call)
        except (ValueError, TypeError):
            return None
        return sig.replace(parameters=list(sig.parameters.values())[1:])


def _fmt_return(sig: inspect.Signature) -> str:
    ann = sig.return_annotation
    return "" if ann is inspect.Signature.empty else str(ann)


def _lineno(func) -> int:
    try:
        return inspect.getsourcelines(inspect.unwrap(func))[1]
    except (OSError, TypeError):
        return 0
