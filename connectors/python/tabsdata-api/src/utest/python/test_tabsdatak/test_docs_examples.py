#
# Copyright 2026 Tabsdata Inc.
#

"""Run every ``Examples:`` block of the documented API against real data.

The blocks are pulled out with the API-REF generator's own parser, so what
runs here is exactly what the generator publishes. A block that no longer
matches the API -- a renamed argument, a dropped keyword, a method that
moved -- fails here instead of shipping as a broken example.

The duplicate-definition test guards a hole in the tooling: ruff excludes
``*.pyi``, so a member declared twice in a stub is reported by nothing, and
the second declaration silently shadows the first one's documentation.
"""

from __future__ import annotations

import ast
import datetime as dt
import io
import pathlib
from contextlib import redirect_stdout

import pandas as pd
import polars as pl
import pytest

import tabsdatak.dataquality as dq
import tabsdatak.tableframe.datatypes as datatypes
from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import col, concat, lit
import tabsdatak.tableframe.grok as grok
from tabsdatak.tableframe.grok import ColumnDefinition

apidoc = pytest.importorskip(
    "tabsdatax.generator.apidoc.cli",
    reason="the apidoc generator (tabsdata-toolkit) is not installed",
)

pytestmark = pytest.mark.tableframe

_API = pathlib.Path(__file__).resolve().parents[3] / "main/python/tabsdatak"
_TABLEFRAME = _API / "tableframe"

# Members documented without an example on purpose.
_NO_EXAMPLE_NEEDED = {
    # Built by TableFrame.group_by, never by a caller.
    "group_by.pyi::LazyGroupBy.__init__",
    # dict plumbing on Schema: these mutate a detached copy of the schema,
    # so an example would suggest they change the frame they came from.
    *(
        f"schema.pyi::Schema.{name}"
        for name in (
            "clear",
            "copy",
            "fromkeys",
            "move_to_end",
            "pop",
            "popitem",
            "setdefault",
            "update",
            "__lt__",
            "__le__",
            "__gt__",
            "__ge__",
            "__or__",
            "__ror__",
        )
    ),
}

# Engine introspection rather than something a user writes: it reports which
# column types a classifier accepts.
_NO_EXAMPLE_NEEDED_MEMBERS = {"supported_dtypes"}

# Blocks that cannot run headless.
_NEEDS_DISPLAY = {"tf.show_graph()"}


def _sources() -> list[pathlib.Path]:
    """The stubs, plus the modules whose docs live in their .py."""
    found = [
        *sorted(_TABLEFRAME.glob("*.pyi")),
        *sorted((_TABLEFRAME / "grok").glob("*.py")),
        *sorted((_API / "dataquality").glob("*.py")),
    ]
    # An empty list would turn every test below into a silent skip.
    assert found, f"no documented sources found under {_API}"
    return found


def _public_names(path: pathlib.Path) -> set[str] | None:
    """Top-level names the API reference publishes from a source, or None
    when everything in it is published (the stubs)."""
    if path.suffix == ".pyi":
        return None
    module = dq if path.parent.name == "dataquality" else grok
    return {name for name in dir(module) if not name.startswith("_")}


def _is_accessor(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """A property setter/deleter or a typing overload: same name by design."""
    for decorator in node.decorator_list:
        rendered = ast.unparse(decorator)
        if rendered.endswith((".setter", ".deleter")) or rendered.split(".")[-1] == (
            "overload"
        ):
            return True
    return False


def _documented(path: pathlib.Path):
    """Every documented member of a source, as (qualified name, docstring)."""
    tree = ast.parse(path.read_text())

    def visit(node, prefix=""):
        for child in node.body:
            if isinstance(child, ast.ClassDef):
                doc = ast.get_docstring(child)
                if doc:
                    yield f"{prefix}{child.name}", doc
                yield from visit(child, f"{prefix}{child.name}.")
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                doc = ast.get_docstring(child)
                if doc:
                    yield f"{prefix}{child.name}", doc

    yield from visit(tree)


def _blocks() -> list[tuple[str, str]]:
    """Every python example block, as (label, code)."""
    found = []
    for path in _sources():
        for name, doc in _documented(path):
            body, _ = apidoc.split_off_examples(doc)
            if not body:
                continue
            for item in apidoc.parse_examples(body):
                code = item.get("code-block:: python")
                if code is not None:
                    found.append((f"{path.name}::{name}", code))
    return found


def _sample() -> TableFrame:
    return TableFrame.from_polars(
        pl.DataFrame({
            "id": [1, 2, 3, 4],
            "a": [1, 2, 3, 4],
            "b": [10, 20, 30, 40],
            "c": [7, 8, 9, 10],
            "n": [4, 3, 2, 1],
            "v": [1.5, None, 3.25, float("nan")],
            "g": ["x", "y", "x", "z"],
            "k": ["k1", "k2", "k1", "k3"],
            "flag": [True, False, True, False],
            "processed": [True, False, True, False],
            "name": ["  Ada  ", "Grace", "alan", "Edsger"],
            "day": ["2026-01-15", "2026-02-20", "2026-03-05", "2026-03-31"],
            "when": [
                "2026-01-15 10:30:00",
                "2026-02-20 23:05:00",
                "2026-03-05 00:00:00",
                "2026-03-31 18:45:00",
            ],
            "clock": ["10:30:00", "23:05:00", "00:00:00", "18:45:00"],
            "qty": ["42", "7", "128", "3"],
            "email": [
                "ada@example.com",
                "grace@navy.mil",
                "alan@bletchley.uk",
                "e@utexas.edu",
            ],
            "logs": [
                "alice-login-2023",
                "bob-logout-2024",
                "carol-login-2025",
                "garbage",
            ],
            "ts": [
                dt.datetime(2026, 1, 15, 10, 30),
                dt.datetime(2026, 2, 20, 23, 5),
                dt.datetime(2026, 3, 5, 0, 0),
                dt.datetime(2026, 3, 31, 18, 45),
            ],
            "d": [
                dt.date(2026, 1, 15),
                dt.date(2026, 2, 20),
                dt.date(2026, 3, 5),
                dt.date(2026, 3, 31),
            ],
            "dur": [
                dt.timedelta(days=1, hours=2),
                dt.timedelta(hours=5),
                dt.timedelta(minutes=90),
                dt.timedelta(seconds=45),
            ],
            "s": [{"x": i, "y": c} for i, c in enumerate("pqrs", start=1)],
            "t": [{"u": float(i)} for i in range(1, 5)],
        })
    )


def _namespace() -> dict:
    """The names an example may lean on, as a reader would import them."""
    frame = _sample()
    names = {
        "TableFrame": TableFrame,
        "col": col,
        "lit": lit,
        "concat": concat,
        "pl": pl,
        "pd": pd,
        "tf": frame,
        "other": _sample(),
        "left": TableFrame.from_dict({"id": [1, 2], "x": ["a", "b"]}),
        "right": TableFrame.from_dict({"id": [1, 3], "rid": [1, 3], "y": [9, 8]}),
        "schema": frame.schema,
        "ColumnDefinition": ColumnDefinition,
        "datetime": dt.datetime,
        "date": dt.date,
        "time": dt.time,
        "timedelta": dt.timedelta,
    }
    names.update({n: getattr(datatypes, n) for n in dir(datatypes) if n[0].isupper()})
    names.update({n: getattr(dq, n) for n in dir(dq) if n[0].isupper()})
    return names


def _materialize(value, names: dict) -> None:
    """Force the lazy work, so an example is checked against data."""
    if isinstance(value, TableFrame):
        value._to_polars_df()
    elif type(value).__name__ == "Expr":
        names["tf"].select(value.alias("probe"))._to_polars_df()


@pytest.mark.parametrize(
    "label, code", _blocks(), ids=[label for label, _ in _blocks()]
)
def test_example_runs(label: str, code: str) -> None:
    names = _namespace()
    for node in ast.parse(code).body:
        statement = ast.get_source_segment(code, node) or ""
        if statement in _NEEDS_DISPLAY:
            continue
        with redirect_stdout(io.StringIO()):
            if isinstance(node, ast.Expr):
                value = eval(  # noqa: S307 - the examples are the fixture
                    compile(ast.Expression(node.value), label, "eval"), names
                )
                _materialize(value, names)
            else:
                exec(  # noqa: S102 - the examples are the fixture
                    compile(ast.Module([node], []), label, "exec"), names
                )


@pytest.mark.parametrize("path", _sources(), ids=lambda p: p.name)
def test_no_member_is_declared_twice(path: pathlib.Path) -> None:
    # ruff skips *.pyi, so nothing else catches a member declared twice --
    # and the second declaration shadows the first one's documentation.
    for node in ast.walk(ast.parse(path.read_text())):
        if not isinstance(node, ast.ClassDef):
            continue
        names = [
            child.name
            for child in node.body
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
            and not _is_accessor(child)
        ]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        assert not duplicates, f"{path.name}: {node.name} declares {duplicates} twice"


@pytest.mark.parametrize("path", _sources(), ids=lambda p: p.name)
def test_every_documented_member_has_an_example(path: pathlib.Path) -> None:
    published = _public_names(path)
    missing = [
        f"{path.name}::{name}"
        for name, doc in _documented(path)
        if not apidoc.split_off_examples(doc)[0]
        and (published is None or name.split(".")[0] in published)
        and name.split(".")[-1] not in _NO_EXAMPLE_NEEDED_MEMBERS
    ]
    assert not [m for m in missing if m not in _NO_EXAMPLE_NEEDED]
