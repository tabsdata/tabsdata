#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk fn check``: what ``fn register|update|patch`` would register, found by
importing the file and building the bundle exactly as they do, without
uploading anything."""

from __future__ import annotations

import ast
import os
import tarfile
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
import zstandard

from tabsdatak.cli.cli_utils import load_info_and_project_root

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import TabsdataServer

_DANGEROUS_BUILTINS = frozenset({"eval", "exec", "compile", "__import__"})
_DANGEROUS_ATTRS = frozenset({
    ("os", "system"),
    ("os", "popen"),
    ("subprocess", "run"),
    ("subprocess", "Popen"),
    ("subprocess", "call"),
    ("subprocess", "check_output"),
    ("subprocess", "check_call"),
    ("pickle", "loads"),
    ("pickle", "load"),
    ("marshal", "loads"),
    ("marshal", "load"),
    ("shelve", "open"),
})


def _finding(severity: str, code: str, message: str, line: int | None = None):
    return {"severity": severity, "code": code, "message": message, "line": line}


def dangerous_calls(tree: ast.Module) -> list[dict]:
    """A warning for every call that runs code or commands, or deserializes
    data, however the module was imported (``import subprocess as s``,
    ``from subprocess import run``)."""
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                top = alias.name.split(".", 1)[0]
                aliases[alias.asname or top] = top
        elif isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func, called = node.func, None
        if isinstance(func, ast.Name):
            if func.id in _DANGEROUS_BUILTINS:
                called = func.id
            else:
                module, _, attr = aliases.get(func.id, "").partition(".")
                if (module, attr) in _DANGEROUS_ATTRS:
                    called = f"{module}.{attr}"
        elif isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            module = aliases.get(func.value.id, func.value.id)
            if (module, func.attr) in _DANGEROUS_ATTRS:
                called = f"{module}.{func.attr}"
        if called is not None:
            findings.append(_finding("warning", "dangerous_call", called, node.lineno))
    return findings


def _static(source: str, filename: str) -> tuple[ast.Module | None, list[dict]]:
    try:
        tree = ast.parse(source, filename)
    except SyntaxError as error:
        return None, [_finding("error", "syntax_error", str(error), error.lineno)]
    return tree, dangerous_calls(tree)


def check_source(source: str, filename: str, function_name: str) -> list[dict]:
    """Static findings on the text of a function file: an error when it does
    not compile or does not define ``function_name``, else its dangerous
    calls."""
    tree, findings = _static(source, filename)
    if tree is None:
        return findings
    defined = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    if function_name not in defined:
        return [
            _finding(
                "error",
                "function_not_found",
                f"'{function_name}' is not defined in {filename}; it defines: "
                f"{', '.join(sorted(defined)) or 'no function'}.",
            )
        ]
    return findings


def _kind(info) -> str | None:
    from tabsdatak._info import (
        KIND_PUBLISHER_DEF,
        KIND_STREAM_PUBLISHER_DEF,
        KIND_SUBSCRIBER_DEF,
        KIND_TRANSFORMER_DEF,
    )

    return {
        KIND_PUBLISHER_DEF: "publisher",
        KIND_STREAM_PUBLISHER_DEF: "stream_publisher",
        KIND_TRANSFORMER_DEF: "transformer",
        KIND_SUBSCRIBER_DEF: "subscriber",
    }.get(getattr(info, "__kind__", None))


def _unversioned(reference: str) -> str:
    return reference.split("@", 1)[0]


def _triggers(info, inputs: list[str]) -> list[str]:
    from tabsdatak.api import AllTablesTrigger, CronTrigger, TablesTrigger

    trigger_by = getattr(info, "trigger_by", None)
    if isinstance(trigger_by, TablesTrigger):
        return list(trigger_by.tables)
    if isinstance(trigger_by, CronTrigger):
        return [f"cron:{trigger_by.mask}"]
    if trigger_by is None or isinstance(trigger_by, AllTablesTrigger):
        return [_unversioned(reference) for reference in inputs]
    return []


def describe(info) -> dict:
    """What registering ``info`` declares: its name, kind, input and output
    tables, triggers and connector."""
    # noinspection PyProtectedMember
    from tabsdatak.sdk._function_archive import (
        _connector,
        _input_tables,
        _output_tables,
    )

    inputs = _input_tables(info)
    return {
        "name": getattr(info.func, "__name__", None),
        "kind": _kind(info),
        "inputs": inputs,
        "outputs": _output_tables(info),
        "triggers": _triggers(info, inputs),
        "connector": _connector(info),
    }


def _bundle_summary(archive: Path) -> dict:
    # noinspection PyProtectedMember
    from tabsdatak.common._archive.bundle import BUNDLE_REQUIREMENTS_FILENAME

    files, python_version = 0, None
    with open(archive, "rb") as raw:
        with zstandard.ZstdDecompressor().stream_reader(raw) as stream:
            with tarfile.open(fileobj=stream, mode="r|") as tar:
                for member in tar:
                    if not member.isfile():
                        continue
                    files += 1
                    if member.name.removeprefix("./") == BUNDLE_REQUIREMENTS_FILENAME:
                        content = tar.extractfile(member)
                        manifest = yaml.safe_load(content.read()) if content else None
                        python_version = (manifest or {}).get("pythonVersion")
    return {
        "files": files,
        "bytes": archive.stat().st_size,
        "python_version": python_version,
    }


def _build(
    info,
    project_root: str,
    requirements: str | None,
    local_packages: list[str] | None,
    confine_to: str | None,
) -> dict:
    # noinspection PyProtectedMember
    from tabsdatak.common._archive.bundle import build_bundle

    packages = [Path(package) for package in local_packages or ()]
    with tempfile.TemporaryDirectory() as tmp:
        archive = build_bundle(
            info,
            Path(project_root),
            Path(tmp) / "bundle.tar.zst",
            local_packages=packages or None,
            requirements_yaml=Path(requirements) if requirements else None,
            confine_to=Path(confine_to) if confine_to else None,
        )
        return _bundle_summary(archive)


def _is_confinement(error: Exception) -> bool:
    from tabsdatak.common._error import CommonErrorCode, CommonRuntimeException

    return (
        isinstance(error, CommonRuntimeException)
        and error.api_error is CommonErrorCode.COMMON_14
    )


def _tables(server: TabsdataServer, collection: str) -> dict[str, str | None]:
    return {
        table.name: table.function.name if table.function is not None else None
        for table in server.list_tables(collection)
    }


def _server_checks(
    server: TabsdataServer,
    collection: str,
    function: dict,
    name: str | None,
    python_version: str | None,
    findings: list[dict],
) -> dict:
    # noinspection PyProtectedMember
    from tabsdatak.sdk._apiserver import APIServerError

    checks: dict = {
        "collection": "ok",
        "inputs_exist": None,
        "output_conflicts": [],
        "python_version": None,
    }
    try:
        server.get_collection(collection)
    except APIServerError:
        checks["collection"] = "missing"
        findings.append(
            _finding(
                "error",
                "collection_missing",
                f"Collection '{collection}' does not exist.",
            )
        )
        return checks
    listed: dict[str, dict[str, str | None]] = {}

    def tables_of(owner: str) -> dict[str, str | None]:
        if owner not in listed:
            try:
                listed[owner] = _tables(server, owner)
            except APIServerError:
                listed[owner] = {}
        return listed[owner]

    missing = []
    for reference in function["inputs"]:
        owner, _, table = _unversioned(reference).rpartition("/")
        if table not in tables_of(owner or collection):
            missing.append(reference)
    checks["inputs_exist"] = not missing
    for reference in missing:
        findings.append(
            _finding(
                "warning",
                "input_missing",
                f"Input table '{reference}' does not exist yet.",
            )
        )
    owner_name = name or function["name"]
    own = tables_of(collection)
    for output in function["outputs"]:
        producer = own.get(output)
        if producer is not None and producer != owner_name:
            checks["output_conflicts"].append({"table": output, "function": producer})
            findings.append(
                _finding(
                    "error",
                    "output_conflict",
                    f"Table '{output}' is already produced by function "
                    f"'{producer}' in collection '{collection}'.",
                )
            )
    if python_version is not None:
        accepted = server.valid_python_versions or []
        checks["python_version"] = "ok" if python_version in accepted else "rejected"
        if python_version not in accepted:
            findings.append(
                _finding(
                    "error",
                    "python_version",
                    f"The server runs functions on Python {', '.join(accepted)}, "
                    f"not {python_version}.",
                )
            )
    return checks


def check_function(
    path: str,
    dir_to_bundle: str | None = None,
    *,
    requirements: str | None = None,
    local_packages: list[str] | None = None,
    confine_to: str | None = None,
    server: TabsdataServer | None = None,
    collection: str | None = None,
    name: str | None = None,
) -> dict:
    """Check ``path`` (``file.py::function``) as register would take it: parse
    it, import it, build its bundle and, with ``collection``, check it against
    the server. Runs the file's module code. ``valid`` is false when any
    finding is an error; a bundled file outside ``confine_to`` raises."""
    file_path, _, function_name = path.partition("::")
    result: dict = {
        "valid": False,
        "function": None,
        "bundle": None,
        "findings": [],
        "server_checks": None,
    }
    findings = result["findings"]
    try:
        source = Path(file_path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        findings.append(_finding("error", "unreadable", str(error)))
        return result
    tree, static = _static(source, os.path.basename(file_path))
    findings.extend(static)
    if tree is None:
        return result
    try:
        info, project_root = load_info_and_project_root(path, dir_to_bundle)
    except Exception as error:
        findings.append(_finding("error", "import_error", str(error)))
        return result
    if _kind(info) is None:
        findings.append(
            _finding(
                "error",
                "not_a_function",
                f"'{function_name}' is not decorated with @publisher, "
                "@stream_publisher, @transformer or @subscriber.",
            )
        )
        return result
    result["function"] = describe(info)
    try:
        result["bundle"] = _build(
            info, project_root, requirements, local_packages, confine_to
        )
    except Exception as error:
        if _is_confinement(error):
            raise
        findings.append(_finding("error", "bundle_error", str(error)))
    if server is not None and collection is not None:
        result["server_checks"] = _server_checks(
            server,
            collection,
            result["function"],
            name,
            None if requirements else (result["bundle"] or {}).get("python_version"),
            findings,
        )
    result["valid"] = not any(finding["severity"] == "error" for finding in findings)
    return result
