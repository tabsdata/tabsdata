#
# Copyright 2026 Tabsdata Inc.
#

import os
from pathlib import Path

import rich_click as click

from tabsdatax.itest.tdkinstance.commands.storage import STORAGE_OPTION, storage_stub
from tabsdatax.itest.tdkinstance.commands.stubs import (
    check_variables,
    missing_variables,
    read_yaml,
)

KEY_STUBS = "stubs"
KEY_MANDATORY = "mandatory"
KEY_OPTIONAL = "optional"
GLOB_CHARACTERS = frozenset("*?[")
CONFIG_OPTION = "--config"
FILE_PREFIX = "@"


def declared_stubs(data, instance_type: str, key: str) -> list[str]:
    options = data.get(instance_type) if isinstance(data, dict) else None
    groups = options.get(KEY_STUBS) if isinstance(options, dict) else None
    if groups is None:
        groups = {}
    elif not isinstance(groups, dict) or set(groups) - {KEY_MANDATORY, KEY_OPTIONAL}:
        raise click.ClickException(
            f"Field '{KEY_STUBS}' for instance type '{instance_type}' must map "
            f"'{KEY_MANDATORY}' and '{KEY_OPTIONAL}' to lists of stub file paths."
        )
    declared = groups.get(key)
    if declared is None:
        stubs = []
    elif isinstance(declared, list) and all(isinstance(stub, str) for stub in declared):
        stubs = declared
    else:
        raise click.ClickException(
            f"Field '{KEY_STUBS}.{key}' for instance type '{instance_type}' must be a "
            "list of stub file paths."
        )
    return stubs


def stub_paths(folder: Path, declared: str, *, required: bool) -> list[Path]:
    """The stubs ``declared`` names under ``folder``: itself, or every file a
    glob pattern matches, in name order."""
    if not GLOB_CHARACTERS.intersection(declared):
        return [(folder / declared).resolve()]
    matched = sorted(path.resolve() for path in folder.glob(declared) if path.is_file())
    if required and not matched:
        raise click.ClickException(
            f"Stub pattern '{declared}' matches no file under '{folder}'."
        )
    return matched


@click.command(name="arguments")
@click.argument("config", type=click.Path(path_type=Path))
@click.argument("instance_type")
@STORAGE_OPTION
def arguments(config: Path, instance_type: str, storage: str):
    """Emit the '--config @STUB' tokens of an instance type.

    Each entry under the instance type's 'stubs.mandatory' is a tdkserver stub file,
    relative to the config's folder, or a glob pattern for several, taken in
    name order. tdkserver reads their 'env:' values from
    the environment itself, so a secret never appears on the command line, and
    it rejects a setting declared by more than one stub. The run fails here,
    before tdkserver starts, if a variable an 'env:' value reads is undefined or
    empty. A stub under 'stubs.optional' is left out instead, with a warning on
    stderr, so the instance starts without what it sets. The storage stub comes
    last; 'storage exec' checks its variables.
    """
    folder = config.resolve().parent
    data = read_yaml(config, "config")
    stubs = [
        stub
        for declared in declared_stubs(data, instance_type, KEY_MANDATORY)
        for stub in stub_paths(folder, declared, required=True)
    ]
    for stub in stubs:
        check_variables(stub, instance_type, os.environ)
    optional = [
        stub
        for declared in declared_stubs(data, instance_type, KEY_OPTIONAL)
        for stub in stub_paths(folder, declared, required=False)
    ]
    for stub in optional:
        if missing := missing_variables(stub, os.environ):
            click.echo(
                f"Stub '{stub}' of '{instance_type}' is left out: "
                f"{', '.join(missing)} not set.",
                err=True,
            )
        else:
            stubs.append(stub)
    extra = storage_stub(config, storage)
    if extra is not None:
        stubs.append(extra)
    for stub in stubs:
        click.echo(CONFIG_OPTION)
        click.echo(f"{FILE_PREFIX}{stub}")
