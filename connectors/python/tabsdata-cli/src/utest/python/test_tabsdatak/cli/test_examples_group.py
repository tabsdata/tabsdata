#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli._errors import ExitCode
from tabsdatak.cli.td.cli import cli

SNIPPET = '''#
# Copyright 2026 Tabsdata Inc.
#

"""
title: Demo publisher
kind: publisher
connectors: [local-file-in]
tags: [csv, demo]
summary: >-
  Reads a CSV.
"""

VALUE = 1
'''


@pytest.fixture
def root(tmp_path) -> Path:
    root = tmp_path / "examples"
    root.mkdir()
    (root / "pub-demo.py").write_text(SNIPPET)
    (root / "tfr-other.py").write_text(
        SNIPPET.replace("kind: publisher", "kind: transformer")
        .replace("[local-file-in]", "[]")
        .replace("[csv, demo]", "[join]")
    )
    project = root / "proj"
    (project / "functions").mkdir(parents=True)
    (project / "_meta.yaml").write_text("title: A project\ndescription: Whole flow\n")
    (project / "README.txt").write_text("Read me\n")
    (project / "functions" / "f.py").write_text("X = 1\n")
    (root / "__pycache__").mkdir()
    return root


def _invoke(argv, tmp_path, root=None, server=None):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = server

    patches = [
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ]
    if root is not None:
        patches.append(
            patch("tabsdatak.cli.td.examples_group.examples_root", return_value=root)
        )
    for active in patches:
        active.start()
    try:
        return CliRunner().invoke(cli, argv)
    finally:
        for active in patches:
            active.stop()


def _machine(argv, tmp_path, root=None, server=None):
    result = _invoke([*argv, "--machine"], tmp_path, root, server)
    lines = result.stdout.splitlines()
    assert len(lines) == 1, result.stdout
    return result, json.loads(lines[0])


def test_list_merges_snippets_and_projects(root, tmp_path):
    result, document = _machine(["examples", "list"], tmp_path, root)
    assert result.exit_code == 0
    assert document["truncated"] is False
    assert document["items"] == [
        {
            "name": "proj",
            "form": "project",
            "title": "A project",
            "kind": None,
            "connectors": [],
            "tags": [],
            "summary": "Whole flow",
            "source": "bundled",
        },
        {
            "name": "pub-demo",
            "form": "snippet",
            "title": "Demo publisher",
            "kind": "publisher",
            "connectors": ["local-file-in"],
            "tags": ["csv", "demo"],
            "summary": "Reads a CSV.",
            "source": "bundled",
        },
        {
            "name": "tfr-other",
            "form": "snippet",
            "title": "Demo publisher",
            "kind": "transformer",
            "connectors": [],
            "tags": ["join"],
            "summary": "Reads a CSV.",
            "source": "bundled",
        },
    ]


@pytest.mark.parametrize(
    "filters, names",
    [
        (["--kind", "publisher"], ["pub-demo"]),
        (["--kind", "transformer"], ["tfr-other"]),
        (["--connector", "local-file-in"], ["pub-demo"]),
        (["--tag", "join"], ["tfr-other"]),
        (["--kind", "publisher", "--tag", "join"], []),
    ],
)
def test_list_filters(root, tmp_path, filters, names):
    _, document = _machine(["examples", "list", *filters], tmp_path, root)
    assert [item["name"] for item in document["items"]] == names


def test_list_json_is_the_items_with_their_description(root, tmp_path):
    result = _invoke(["examples", "list", "-o", "json"], tmp_path, root)
    assert result.exit_code == 0, result.output
    items = json.loads(result.stdout)
    assert [(item["name"], item["description"]) for item in items] == [
        ("proj", "Whole flow"),
        ("pub-demo", "Reads a CSV."),
        ("tfr-other", "Reads a CSV."),
    ]


def test_a_machine_run_given_json_gets_the_envelope(root, tmp_path):
    result, document = _machine(["examples", "list", "-o", "json"], tmp_path, root)
    assert result.exit_code == 0
    names = [item["name"] for item in document["items"]]
    assert names == ["proj", "pub-demo", "tfr-other"]
    assert "description" not in document["items"][0]


def test_list_table(root, tmp_path):
    result = _invoke(["examples", "list"], tmp_path, root)
    assert result.exit_code == 0, result.output
    assert "pub-demo" in result.output
    assert "Number of examples: 3" in result.output


def test_show_snippet_returns_its_source(root, tmp_path):
    _, document = _machine(["examples", "show", "--name", "pub-demo"], tmp_path, root)
    assert document["name"] == "pub-demo"
    assert document["files"] == [{"path": "pub-demo.py", "content": SNIPPET}]


def test_show_snippet_table_prints_the_source(root, tmp_path):
    result = _invoke(["examples", "show", "--name", "pub-demo"], tmp_path, root)
    assert result.stdout == SNIPPET


def test_show_project_returns_readme_and_functions(root, tmp_path):
    _, document = _machine(["examples", "show", "--name", "proj"], tmp_path, root)
    assert document["files"] == [
        {"path": "README.txt", "content": "Read me\n"},
        {"path": "functions/f.py", "content": "X = 1\n"},
    ]


def test_show_unknown_example_is_not_found(root, tmp_path):
    result, document = _machine(["examples", "show", "--name", "nope"], tmp_path, root)
    assert result.exit_code == ExitCode.NOT_FOUND
    assert document["error"]["type"] == "not_found"


def test_path(root, tmp_path):
    _, whole = _machine(["examples", "path"], tmp_path, root)
    _, one = _machine(["examples", "path", "--name", "pub-demo"], tmp_path, root)
    assert whole == {"path": str(root.resolve())}
    assert one == {"path": str((root / "pub-demo.py").resolve())}


def test_extract_snippet_writes_name_py_into_an_existing_dir(root, tmp_path):
    target = tmp_path / "out"
    target.mkdir()
    _, document = _machine(
        ["examples", "extract", "-n", "pub-demo", "-d", str(target)], tmp_path, root
    )
    assert document == {
        "done": {
            "action": "extract",
            "kind": "example",
            "id": "pub-demo",
            "dir": str(target),
            "file": str(target / "pub-demo.py"),
        }
    }
    assert (target / "pub-demo.py").read_text() == SNIPPET


def test_extract_snippet_refuses_to_overwrite(root, tmp_path):
    target = tmp_path / "out"
    target.mkdir()
    (target / "pub-demo.py").write_text("mine")
    result, document = _machine(
        ["examples", "extract", "-n", "pub-demo", "-d", str(target)], tmp_path, root
    )
    assert result.exit_code == ExitCode.CONFLICT
    assert (target / "pub-demo.py").read_text() == "mine"


def test_extract_project_refuses_an_existing_dir(root, tmp_path):
    target = tmp_path / "out"
    target.mkdir()
    result, document = _machine(
        ["examples", "extract", "-n", "proj", "-d", str(target)], tmp_path, root
    )
    assert result.exit_code == ExitCode.CONFLICT
    assert document["error"]["type"] == "conflict"


def test_bundled_examples_list_and_show(tmp_path):
    _, listing = _machine(["examples", "list", "--kind", "transformer"], tmp_path)
    names = [item["name"] for item in listing["items"]]
    assert names
    _, shown = _machine(["examples", "show", "--name", names[0]], tmp_path)
    assert "@transformer" in shown["files"][0]["content"]


def test_extract_snippet_never_writes_through_a_dangling_symlink(root, tmp_path):
    target = tmp_path / "out"
    target.mkdir()
    elsewhere = tmp_path / "elsewhere.py"
    (target / "pub-demo.py").symlink_to(elsewhere)
    result, document = _machine(
        ["examples", "extract", "-n", "pub-demo", "-d", str(target)], tmp_path, root
    )
    assert result.exit_code == ExitCode.CONFLICT, document
    assert not elsewhere.exists()


def test_extract_project_refuses_a_dangling_symlink(root, tmp_path):
    target = tmp_path / "out"
    target.symlink_to(tmp_path / "elsewhere")
    result, document = _machine(
        ["examples", "extract", "-n", "proj", "-d", str(target)], tmp_path, root
    )
    assert result.exit_code == ExitCode.CONFLICT, document
    assert not (tmp_path / "elsewhere").exists()


def test_list_and_show_read_only_the_bundled_examples(root, tmp_path):
    server = SimpleNamespace()
    _, listed = _machine(["examples", "list"], tmp_path, root, server)
    _, shown = _machine(["examples", "show", "--name", "pub-demo"], tmp_path, root, server)
    assert [item["name"] for item in listed["items"]] == ["proj", "pub-demo", "tfr-other"]
    assert {item["source"] for item in listed["items"]} == {"bundled"}
    assert shown["files"][0]["path"] == "pub-demo.py"


def test_a_program_listing_examples_gets_the_first_sentence_of_each():
    from click.testing import CliRunner as _Runner

    from tabsdatak.cli.td.cli import cli as _cli

    listed = json.loads(_Runner().invoke(_cli, ["examples", "list", "--machine"]).stdout)
    full = json.loads(_Runner().invoke(_cli, ["examples", "list", "-o", "json"]).stdout)
    by_name = {row["name"]: row["summary"] for row in full}
    for row in listed["items"]:
        assert by_name[row["name"]].startswith(row["summary"])
        assert len(row["summary"]) <= len(by_name[row["name"]])
