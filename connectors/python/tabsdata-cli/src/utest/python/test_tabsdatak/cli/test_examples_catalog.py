#
# Copyright 2026 Tabsdata Inc.
#

"""Every bundled example snippet is executed: its metadata parses, it imports
(the decorators validate each signature at decoration time), each function
matches the declared kind and connection types, it uses no deprecated API,
and a transformer with a ``SAMPLE`` runs on it. A parameter typed
``TableFramesSpec`` (every ``@NEW`` input) gets its sample as one version."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import typing
from pathlib import Path
from unittest.mock import patch

import pytest

from tabsdatak.cli._docs._catalog import build_catalog
from tabsdatak.cli._examples import (
    PROJECT,
    SNIPPET,
    examples_root,
    load_examples,
)
from tabsdatak.tableframe import TableFrame

pytestmark = pytest.mark.filterwarnings("error::DeprecationWarning")

ROOT = examples_root()
EXAMPLES = load_examples(ROOT)
SNIPPETS = [example for example in EXAMPLES if example.form == SNIPPET]
KIND_OF_INFO = {
    "PublisherInfo": "publisher",
    "StreamPublisherInfo": "stream_publisher",
    "TransformerInfo": "transformer",
    "SubscriberInfo": "subscriber",
}


@pytest.fixture(scope="module")
def connection_types() -> dict[str, list[str]]:
    return {
        entry.qualified_name: entry.connection_types
        for entry in build_catalog(["connectors"]).entries
        if entry.connection_types
    }


def _module(path: Path):
    spec = importlib.util.spec_from_file_location(
        f"tdk_example_{path.stem.replace('-', '_')}", path
    )
    module = importlib.util.module_from_spec(spec)
    with patch.object(sys, "dont_write_bytecode", True):
        spec.loader.exec_module(module)
    return module


def _functions(module) -> list:
    return [
        value
        for value in vars(module).values()
        if type(value).__name__ in KIND_OF_INFO
    ]


def _qualified(obj) -> str:
    cls = type(obj)
    module = cls.__module__
    public = ".".join(part for part in module.split(".") if not part.startswith("_"))
    return f"{public}.{cls.__name__}"


def test_the_curated_snippets_are_bundled():
    assert len(SNIPPETS) >= 30
    names = [example.name for example in EXAMPLES]
    assert len(names) == len(set(names))
    assert {"quickstart-ai", "quickstart-dev"} <= {
        example.name for example in EXAMPLES if example.form == PROJECT
    }


@pytest.mark.parametrize("example", SNIPPETS, ids=lambda example: example.name)
def test_snippet_metadata_is_complete(example):
    assert example.title
    assert example.summary
    assert example.tags
    assert all(isinstance(tag, str) for tag in example.tags)


@pytest.mark.parametrize("example", SNIPPETS, ids=lambda example: example.name)
def test_snippet_functions_match_kind_and_connectors(example, connection_types):
    functions = _functions(_module(example.path))
    kinds = {KIND_OF_INFO[type(function).__name__] for function in functions}
    if example.kind == "flow":
        assert len(functions) >= 2
    else:
        assert len(functions) == 1
        assert kinds == {example.kind}
    used = set()
    for function in functions:
        endpoint = getattr(function, "source", None) or getattr(
            function, "destination", None
        )
        if endpoint is None:
            continue
        types = connection_types.get(_qualified(endpoint))
        assert types, f"{_qualified(endpoint)} maps to no connection type"
        assert set(types) & set(example.connectors), (
            f"{function.func.__name__} uses {types}, not in {example.connectors}"
        )
        used |= set(types) & set(example.connectors)
    assert used == set(example.connectors)


def _sampled():
    for example in SNIPPETS:
        if example.kind == "transformer":
            module = _module(example.path)
            if hasattr(module, "SAMPLE"):
                yield pytest.param(module, id=example.name)


def _is_versions(annotation) -> bool:
    return any(
        typing.get_origin(member) is list for member in typing.get_args(annotation)
    )


@pytest.mark.parametrize("module", list(_sampled()))
def test_transformer_runs_on_its_sample(module):
    (function,) = _functions(module)
    hints = typing.get_type_hints(function.func)
    frames = {}
    for parameter, columns in module.SAMPLE.items():
        frame = TableFrame.from_dict(columns)
        frames[parameter] = [frame] if _is_versions(hints.get(parameter)) else frame
    result = function.func(**frames)
    outputs = result if isinstance(result, tuple) else (result,)
    assert len(outputs) == len(function.output_tables)
    assert all(output is None or isinstance(output, TableFrame) for output in outputs)
    assert any(output is not None for output in outputs)


def test_most_transformers_carry_a_sample():
    assert len(list(_sampled())) >= 5


def test_snippets_pass_ruff():
    pytest.importorskip("ruff")
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--no-cache",
            "--select",
            "E,F",
            *[str(example.path) for example in SNIPPETS],
        ],
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
