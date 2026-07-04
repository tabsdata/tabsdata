#
# Copyright 2026 Tabs Data Inc.
#

"""What Polars API to scan, and where each piece lands.

This encodes the *policy* for a lazy, lineage-tracking wrapper: which Polars
classes/functions are delegatable and how they map onto target modules under the
user-supplied root module. The genericity of the tool is in the root
module/folder; the Polars-wrapping policy lives here.

Target submodules mirror the canonical public Polars module for each symbol
(e.g. ``Expr`` -> ``polars.expr`` -> ``<root>.expr``); free functions consolidate
into ``<root>.functions``; ``LazyFrame`` is hoisted to the package root as
``TableFrame``.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class ClassRoot:
    class_object: type
    target_class: str  # Wrapper class name.
    target_submodule: str | None  # None => package root (__init__.py).
    # Extra dunders to capture for THIS class only (beyond the global operator
    # set), e.g. the mapping protocol on Schema. Kept class-specific because some
    # are unsafe elsewhere (LazyFrame.__contains__ returns bool, not a frame).
    extra_dunders: tuple[str, ...] = ()


@dataclass(frozen=True)
class FunctionRoot:
    module_name: str  # e.g. "polars.functions"
    target_submodule: str  # e.g. "functions"


def class_roots() -> list[ClassRoot]:
    """The Polars' classes to wrap, including the Expr namespace classes."""
    group_by = importlib.import_module("polars.lazyframe.group_by")
    roots = [
        ClassRoot(pl.LazyFrame, "TableFrame", None),
        ClassRoot(group_by.LazyGroupBy, "LazyGroupBy", "group_by"),
        ClassRoot(pl.Expr, "Expr", "expr"),
        # Schema is a mapping of column name -> datatype; capture its mapping
        # protocol so the wrapper is iterable / sized / membership-testable.
        ClassRoot(
            pl.Schema,
            "Schema",
            "schema",
            extra_dunders=("__iter__", "__len__", "__contains__"),
        ),
    ]
    # Expr namespaces -> flat leaf modules (str -> string.py, dt -> datetime.py).
    probe = pl.col("_")
    for accessor in sorted(pl.Expr._accessors):
        namespace_class = type(getattr(probe, accessor))
        leaf = namespace_class.__module__.rsplit(".", 1)[-1]
        roots.append(ClassRoot(namespace_class, namespace_class.__name__, leaf))
    # Route the 'when/then' builders -> whenthen.py, so when() returns wrapped
    # objects (no leak).
    whenthen = importlib.import_module("polars.expr.whenthen")
    for name in ("When", "Then", "ChainedWhen", "ChainedThen"):
        roots.append(ClassRoot(getattr(whenthen, name), name, "whenthen"))
    return roots


def function_roots() -> list[FunctionRoot]:
    return [FunctionRoot("polars.functions", "functions")]


def datatype_names() -> list[str]:
    """Public polars ``DataType`` subclasses (incl. the ``DataType`` base) to alias
    as ours in ``datatypes.py``. They are aliased leaf types (passthrough), not
    wrapped — Schema values, casts, etc. expose them directly."""
    from polars.datatypes import DataType

    return sorted(
        name
        for name in dir(pl)
        if isinstance(getattr(pl, name), type)
        and issubclass(getattr(pl, name), DataType)
    )
