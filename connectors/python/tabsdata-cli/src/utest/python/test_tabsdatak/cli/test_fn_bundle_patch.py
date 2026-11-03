#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import os
import tarfile
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
import zstandard
from click.testing import CliRunner

from tabsdatak.cli._errors import ExitCode
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer

_MODULE = textwrap.dedent(
    """
    from tabsdatak.api import TableFrameSpec, transformer

    from pkg.helpers import FACTOR


    @transformer(input_tables=["orders"], output_tables=["out"])
    def clean(orders: TableFrameSpec) -> TableFrameSpec:
        return orders
    """
)
_HELPERS = "FACTOR = 2\n"
_MANIFEST = {"kind": "transformerDef", "spec": {"func": "pkg.mod:clean"}}


def _write_bundle(path: Path, members: dict[str, str]) -> None:
    staging = path.parent / f"{path.name}.staging"
    for name, text in members.items():
        (staging / name).parent.mkdir(parents=True, exist_ok=True)
        (staging / name).write_bytes(text.encode("utf-8"))
    with open(path, "wb") as raw:
        with zstandard.ZstdCompressor().stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                tar.add(staging, arcname=".")


_PARENT = {
    "function_def.json": json.dumps(_MANIFEST),
    "requirements.yaml": yaml.safe_dump({
        "pythonVersion": "3.12.1",
        "publicPackages": ["polars==1.0"],
    }),
    "user_code/pkg/mod.py": _MODULE,
    "user_code/pkg/helpers.py": _HELPERS,
}


def _client(members: dict[str, str] | None = None) -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    client.connection.project = "p1"
    client.patch_function.return_value = {"id": "PV1"}
    client.get_function.return_value.id = "FV9"
    client.list_tables.return_value = []
    client.valid_python_versions = None

    def download(fn_version, destination):
        _write_bundle(Path(destination), members or _PARENT)

    client.download_function_bundle.side_effect = download
    return client


def _invoke(argv, tmp_path, client, env=None):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        return CliRunner().invoke(cli, argv, env=env)


def _document(result) -> dict:
    lines = result.stdout.splitlines()
    assert len(lines) == 1, result.output
    return json.loads(lines[0])


_BUNDLE = ["fn", "bundle", "--fn-version", "FV1"]


def test_bundle_print_shows_the_defining_module(tmp_path):
    result = _invoke([*_BUNDLE, "--print"], tmp_path, _client())
    assert result.exit_code == 0, result.output
    assert result.stdout == _MODULE


def test_bundle_print_in_json_carries_the_bundle_sources(tmp_path):
    result = _invoke([*_BUNDLE, "--print", "--machine"], tmp_path, _client())
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["function_version_id"] == "FV1"
    assert document["defining_module"] == "user_code/pkg/mod.py"
    assert document["files"]["user_code/pkg/mod.py"] == _MODULE
    assert document["files"]["user_code/pkg/helpers.py"] == _HELPERS
    assert document["skipped"] == []
    assert document["truncated"] == []


def test_bundle_extract_unpacks_into_the_directory(tmp_path):
    target = tmp_path / "out"
    result = _invoke(
        [*_BUNDLE, "--extract", str(target), "--machine"], tmp_path, _client()
    )
    assert result.exit_code == ExitCode.OK, result.output
    assert _document(result) == {
        "done": {"action": "extract", "kind": "bundle", "id": "FV1", "dir": str(target)}
    }
    assert (target / "user_code" / "pkg" / "mod.py").read_text() == _MODULE


def test_bundle_by_name_reads_the_current_version(tmp_path):
    client = _client()
    argv = ["fn", "bundle", "-c", "c1", "-n", "f1", "--print", "--machine"]
    document = _document(_invoke(argv, tmp_path, client))
    assert document["function_version_id"] == "FV9"
    assert client.get_function.call_args.args == ("c1", "f1")
    assert client.download_function_bundle.call_args.args[0] == "FV9"


@pytest.mark.parametrize(
    "argv",
    [
        [*_BUNDLE, "--print", "--file", "b.tar.zst"],
        [*_BUNDLE, "--print", "--extract", "d"],
        ["fn", "bundle", "--fn-version", "FV1", "-n", "f1", "--print"],
    ],
)
def test_bundle_usage_errors(argv, tmp_path):
    client = _client()
    result = _invoke([*argv, "--machine"], tmp_path, client)
    assert result.exit_code == ExitCode.USAGE, result.output
    assert _document(result)["error"]["type"] == "usage"
    assert client.download_function_bundle.call_count == 0


_PATCH = ["fn", "patch", "-c", "c1", "-n", "f1", "--fn-version", "FV1"]
_NEW = _MODULE.replace("return orders", "return orders.head(FACTOR)")


def _source(folder: Path, text: str = _NEW) -> str:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "new.py").write_text(text)
    return str(folder / "new.py")


def test_patch_source_splices_into_the_parent_bundle(tmp_path):
    client = _client()
    seen = {}

    def patch_function(coll, name, info, project_root, **kwargs):
        seen["root"] = Path(project_root)
        seen["module"] = (Path(project_root) / "pkg" / "mod.py").read_text()
        seen["helpers"] = (Path(project_root) / "pkg" / "helpers.py").exists()
        seen["requirements"] = yaml.safe_load(Path(kwargs["requirements"]).read_text())
        seen["info"] = info
        return {"id": "PV1"}

    client.patch_function.side_effect = patch_function
    argv = [*_PATCH, "--source", _source(tmp_path / "src"), "--add-package", "x==1"]
    result = _invoke([*argv, "--machine"], tmp_path, client)
    assert result.exit_code == ExitCode.OK, result.output
    assert _document(result) == {"id": "PV1"}
    assert client.download_function_bundle.call_args.args[0] == "FV1"
    assert seen["module"] == _NEW
    assert seen["helpers"] is True
    assert seen["requirements"]["publicPackages"] == ["polars==1.0", "x==1"]
    assert seen["info"].func.__name__ == "clean"
    assert seen["info"].func.__module__ == "pkg.mod"
    assert not seen["root"].exists()


def test_patch_source_keeps_an_explicit_requirements_file(tmp_path):
    client = _client()
    requirements = tmp_path / "req.yaml"
    requirements.write_text("pythonVersion: 3.12.1\npublicPackages: []\n")
    argv = [
        *_PATCH,
        "--source",
        _source(tmp_path / "src"),
        "--requirements",
        str(requirements),
    ]
    result = _invoke(argv, tmp_path, client)
    assert result.exit_code == 0, result.output
    assert client.patch_function.call_args.kwargs["requirements"] == str(requirements)


def test_patch_source_rejects_source_without_the_function(tmp_path):
    client = _client()
    source = _source(tmp_path / "src", "def other():\n    pass\n")
    result = _invoke([*_PATCH, "--source", source, "--machine"], tmp_path, client)
    assert result.exit_code != ExitCode.OK
    assert "'clean' is not defined" in _document(result)["error"]["message"]
    assert client.patch_function.call_count == 0


def test_patch_source_dry_run_checks_the_spliced_tree(tmp_path):
    client = _client()
    argv = [*_PATCH, "--source", _source(tmp_path / "src"), "--dry-run", "--machine"]
    result = _invoke(argv, tmp_path, client)
    assert result.exit_code == ExitCode.OK, result.output
    document = _document(result)
    assert document["valid"] is True, document
    assert document["function"]["name"] == "clean"
    assert document["would"] == {
        "action": "patch",
        "collection": "c1",
        "name": "f1",
        "fn_version": "FV1",
    }
    assert client.patch_function.call_count == 0


@pytest.mark.parametrize(
    "extra",
    [["--path", "/x.py::f"], ["--dir-to-bundle", "/d"]],
)
def test_patch_source_excludes_path_options(extra, tmp_path):
    client = _client()
    argv = [*_PATCH, "--source", _source(tmp_path / "src"), *extra, "--machine"]
    result = _invoke(argv, tmp_path, client)
    assert result.exit_code == ExitCode.USAGE, result.output
    assert client.download_function_bundle.call_count == 0


def test_add_package_needs_source(tmp_path):
    client = _client()
    argv = [*_PATCH, "--path", "/x.py::f", "--add-package", "x==1", "--machine"]
    result = _invoke(argv, tmp_path, client)
    assert result.exit_code == ExitCode.USAGE, result.output


def test_patch_source_is_bundled_confined_to_the_machine_directory(
    tmp_path, machine_settings
):
    workspace = tmp_path / "ws"
    client = _client()
    source = _source(workspace)
    machine_settings.update(confine_to=str(workspace))
    result = _invoke([*_PATCH, "--source", source, "--machine"], tmp_path, client)
    assert result.exit_code == ExitCode.OK, result.output
    kwargs = client.patch_function.call_args.kwargs
    assert kwargs["confine_to"] == os.path.realpath(workspace)
    assert os.listdir(workspace) == ["new.py"]


def test_patch_source_bundles_the_parent_local_packages_again(tmp_path):
    members = {
        **_PARENT,
        "local_packages/0/mypkg/setup.py": "from setuptools import setup\n",
        "local_packages/1/otherpkg/pyproject.toml": "[project]\n",
    }
    client = _client(members)
    seen = {}

    def patch_function(coll, name, info, project_root, **kwargs):
        seen["packages"] = [
            (Path(p).name, sorted(c.name for c in Path(p).iterdir()))
            for p in kwargs["local_packages"]
        ]
        return {"id": "PV1"}

    client.patch_function.side_effect = patch_function
    argv = [*_PATCH, "--source", _source(tmp_path / "src"), "--machine"]
    result = _invoke(argv, tmp_path, client)
    assert result.exit_code == ExitCode.OK, result.output
    assert seen["packages"] == [
        ("mypkg", ["setup.py"]),
        ("otherpkg", ["pyproject.toml"]),
    ]


def test_add_package_replaces_the_pin_of_the_same_package(tmp_path):
    client = _client()
    seen = {}

    def patch_function(coll, name, info, project_root, **kwargs):
        seen["requirements"] = yaml.safe_load(Path(kwargs["requirements"]).read_text())
        return {"id": "PV1"}

    client.patch_function.side_effect = patch_function
    argv = [*_PATCH, "--source", _source(tmp_path / "src")]
    argv += [
        "--add-package",
        "Polars==1.5",
        "--add-package",
        "requests>=2",
        "--machine",
    ]
    result = _invoke(argv, tmp_path, client)
    assert result.exit_code == ExitCode.OK, result.output
    assert seen["requirements"]["publicPackages"] == ["Polars==1.5", "requests>=2"]


def test_add_package_and_requirements_exclude_each_other(tmp_path):
    client = _client()
    requirements = tmp_path / "req.yaml"
    requirements.write_text("pythonVersion: 3.12.1\n")
    argv = [*_PATCH, "--source", _source(tmp_path / "src"), "--add-package", "x==1"]
    result = _invoke(
        [*argv, "--requirements", str(requirements), "--machine"], tmp_path, client
    )
    assert result.exit_code == ExitCode.USAGE, result.output
    assert client.download_function_bundle.call_count == 0


def test_bundle_extract_refuses_a_non_empty_directory(tmp_path):
    target = tmp_path / "out"
    target.mkdir()
    (target / "keep.txt").write_text("mine")
    client = _client()
    result = _invoke(
        [*_BUNDLE, "--extract", str(target), "--machine"], tmp_path, client
    )
    assert result.exit_code == ExitCode.CONFLICT, result.output
    assert client.download_function_bundle.call_count == 0
    forced = _invoke(
        [*_BUNDLE, "--extract", str(target), "--force", "--machine"], tmp_path, client
    )
    assert forced.exit_code == ExitCode.OK, forced.output
    assert (target / "keep.txt").read_text() == "mine"
    assert (target / "user_code" / "pkg" / "mod.py").is_file()


def test_bundle_force_needs_extract(tmp_path):
    result = _invoke([*_BUNDLE, "--print", "--force", "--machine"], tmp_path, _client())
    assert result.exit_code == ExitCode.USAGE, result.output
