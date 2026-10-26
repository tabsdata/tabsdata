#
# Copyright 2026 Tabsdata Inc.
#

import os
from pathlib import Path

import rich_click as click

from tabsdatax.itest.tdkinstance.commands.storage import STORAGE_OPTION, storage_stub
from tabsdatax.itest.tdkinstance.commands.stubs import check_variables, read_yaml

KEY_STUBS = "stubs"
CONFIG_OPTION = "--config"
FILE_PREFIX = "@"


def declared_stubs(data, instance_type: str) -> list[str]:
    options = data.get(instance_type) if isinstance(data, dict) else None
    declared = options.get(KEY_STUBS) if isinstance(options, dict) else None
    if declared is None:
        stubs = []
    elif isinstance(declared, list) and all(isinstance(stub, str) for stub in declared):
        stubs = declared
    else:
        raise click.ClickException(
            f"Field '{KEY_STUBS}' for instance type '{instance_type}' must be a "
            "list of stub file paths."
        )
    return stubs


@click.command(name="arguments")
@click.argument("config", type=click.Path(path_type=Path))
@click.argument("instance_type")
@STORAGE_OPTION
def arguments(config: Path, instance_type: str, storage: str):
    """Emit the '--config @STUB' tokens of an instance type.

    Each entry under the instance type's 'stubs' is a tdkserver stub file,
    relative to the config's folder. tdkserver reads their 'env:' values from
    the environment itself, so a secret never appears on the command line, and
    it rejects a setting declared by more than one stub. The run fails here,
    before tdkserver starts, if a variable an 'env:' value reads is undefined or
    empty. The storage stub comes last; 'storage exec' checks its variables.
    """
    folder = config.resolve().parent
    stubs = [
        (folder / declared).resolve()
        for declared in declared_stubs(read_yaml(config, "config"), instance_type)
    ]
    for stub in stubs:
        check_variables(stub, instance_type, os.environ)
    extra = storage_stub(config, storage)
    if extra is not None:
        stubs.append(extra)
    for stub in stubs:
        click.echo(CONFIG_OPTION)
        click.echo(f"{FILE_PREFIX}{stub}")
