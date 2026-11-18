#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import secrets
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent

import yaml
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.version import Version

from tabsdatak.common._interpreter.python.venv import PythonEnvironment

PYTHON_VERSION = "3.12.13"
RELEASE_INDEX = "https://pypi.tabsdata.com/simple/"
RELEASE_PACKAGES = (
    "tabsdata",
    "tabsdata-agent",
)
SPLIT_RELEASE_PACKAGES = (
    "tabsdata[all]",
    "tabsdata-agent",
)
FIRST_WHOLE_RELEASE = Version("2.4.0")
EXTRA_PACKAGES = ("kubernetes",)
INSTALL_TIMEOUT = 1800
INSTANCE_ROLE = "upgrade"
TOLERATED_STOP_ERRORS = "tolerated_stop_errors"
ENCODING = "utf-8"
DECODING_ERRORS = "replace"
ISOLATED_VARIABLES = (
    "PYTHONPATH",
    "PYTHONHOME",
    "VIRTUAL_ENV",
)
VERSION_PROBE = dedent(
    """\
    import importlib.metadata

    print(importlib.metadata.version("tabsdata"))
    """,
)


def utf8_environment(base: Mapping[str, str] | None = None) -> dict[str, str]:
    return dict(
        base if base is not None else os.environ,
        PYTHONUTF8="1",
    )


def isolated_environment(base: Mapping[str, str] | None = None) -> dict[str, str]:
    environment = utf8_environment(base)
    for name in ISOLATED_VARIABLES:
        environment.pop(name, None)
    return environment


def run(argv: list[str], env: Mapping[str, str] | None = None) -> str:
    result = subprocess.run(
        argv,
        env=utf8_environment(env),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        encoding=ENCODING,
        errors=DECODING_ERRORS,
        timeout=INSTALL_TIMEOUT,
    )
    if result.returncode:
        raise RuntimeError(
            f"'{' '.join(argv)}' failed ({result.returncode}): "
            f"{result.stdout}{result.stderr}",
        )
    return result.stdout.strip()


def uv(*arguments: str) -> None:
    command = shutil.which("uv")
    if command is None:
        raise RuntimeError("Creating a release environment needs 'uv' on the path.")
    environment = isolated_environment(
        dict(
            os.environ,
            UV_LINK_MODE="copy",
        ),
    )
    run([command, *arguments], env=environment)


def release_packages(version: str) -> tuple[str, ...]:
    if Version(version) < FIRST_WHOLE_RELEASE:
        return SPLIT_RELEASE_PACKAGES
    return RELEASE_PACKAGES


def create_environment(version: str, folder: Path) -> PythonEnvironment:
    folder.mkdir(parents=True, exist_ok=True)
    environment = PythonEnvironment(
        root=folder / f"td-{version}-{secrets.token_hex(4)}",
    )
    python = str(environment.python())
    uv(
        "python",
        "install",
        "--quiet",
        "--managed-python",
        "--no-bin",
        "--no-registry",
        PYTHON_VERSION,
    )
    uv(
        "venv",
        "--quiet",
        "--seed",
        "--clear",
        "--managed-python",
        "--python",
        PYTHON_VERSION,
        str(environment.root),
    )
    uv(
        "pip",
        "install",
        "--quiet",
        "--python",
        python,
        "--upgrade",
        "pip",
    )
    uv(
        "pip",
        "install",
        "--quiet",
        "--python",
        python,
        "--index",
        RELEASE_INDEX,
        *(f"{package}=={version}" for package in release_packages(version)),
        *EXTRA_PACKAGES,
    )
    return environment


def installed_version(environment: PythonEnvironment) -> str:
    return run(
        [str(environment.python()), "-c", VERSION_PROBE],
        env=isolated_environment(),
    )


def unpinned_requirements(source: Path, target: Path) -> Path:
    requirements = []
    for line in source.read_text(encoding=ENCODING).splitlines():
        entry = line.strip()
        if not entry or entry.startswith("#"):
            continue
        requirement = Requirement(entry)
        requirement.specifier = SpecifierSet()
        requirements.append(f"{requirement}\n")
    target.write_text("".join(requirements), encoding=ENCODING)
    return target


@dataclass(frozen=True)
class LaunchCommand:
    arguments: list[str]
    environment: dict[str, str]


def release_entry(rulebook: Path, version: str) -> dict:
    releases = yaml.safe_load(rulebook.read_text(encoding=ENCODING))["releases"]
    for entry in releases:
        if Version(version) in SpecifierSet(entry["versions"]):
            return entry
    raise RuntimeError(
        f"No entry of {rulebook} covers release {version}; add one.",
    )


def launch_arguments(
    arguments: list[str | dict],
    values: dict[str, object],
) -> list[str]:
    """Rulebook arguments, formatted; a ``{when_set: NAME, arguments: [...]}``
    entry contributes its arguments only when the environment sets NAME."""
    formatted = []
    for argument in arguments:
        if isinstance(argument, dict):
            if values["environment"].get(argument["when_set"]):
                formatted += launch_arguments(argument["arguments"], values)
        else:
            formatted.append(argument.format(**values))
    return formatted


def launch_commands(
    rulebook: Path,
    version: str,
    values: dict[str, object],
) -> list[LaunchCommand]:
    entry = release_entry(rulebook, version)
    try:
        return [
            LaunchCommand(
                arguments=launch_arguments(command["arguments"], values),
                environment={
                    name: value.format(**values)
                    for name, value in command.get("environment", {}).items()
                },
            )
            for command in entry["commands"]
        ]
    except KeyError as missing:
        raise RuntimeError(
            f"Starting release {version} needs {missing}, which is not set.",
        ) from missing


def tolerated_stop_errors(rulebook: Path, version: str) -> list[str]:
    return list(release_entry(rulebook, version).get(TOLERATED_STOP_ERRORS, []))


def toolkit(*arguments: str) -> str:
    command = str(PythonEnvironment().binary("tdx-itest"))
    return run([command, *arguments])


def claim_base_port(lock: Path) -> int:
    return int(toolkit("port", str(lock)))


def instance_name() -> str:
    return toolkit("name", INSTANCE_ROLE)


def register(registry: Path, instance: str) -> None:
    toolkit(
        "registry",
        "append",
        str(registry),
        instance,
    )


def unregister(registry: Path, instance: str) -> None:
    toolkit(
        "registry",
        "remove",
        str(registry),
        instance,
    )
