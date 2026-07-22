#
# Copyright 2026 Tabsdata Inc.
#

import os
from pathlib import Path

import rich_click as click
import yaml

KEY_ARGUMENTS = "arguments"


@click.command(name="arguments")
@click.argument("config", type=click.Path(path_type=Path))
@click.argument("instance_type")
def arguments(config: Path, instance_type: str):
    """Emit resolved '--flag value' tokens for an instance type.

    Each entry under the instance type's 'arguments' maps a flag to the name
    of an environment variable. The variable is resolved here and the run
    fails if it is undefined.
    """
    try:
        with open(config, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except OSError as error:
        raise click.ClickException(f"Could not read config '{config}': {error}")

    if not isinstance(data, dict) or instance_type not in data:
        return
    options = data[instance_type]
    if not isinstance(options, dict):
        return
    declared = options.get(KEY_ARGUMENTS)
    if declared is None:
        return
    if not isinstance(declared, dict):
        raise click.ClickException(
            f"Field '{KEY_ARGUMENTS}' for instance type '{instance_type}' must be a "
            "mapping of flag to environment variable name."
        )

    for flag, variable in declared.items():
        variable = str(variable)
        value = os.environ.get(variable, "")
        if value.strip() == "":
            raise click.ClickException(
                f"Environment variable '{variable}' required by argument '--{flag}' "
                f"of instance type '{instance_type}' is not defined or is empty."
            )
        click.echo(f"--{flag}")
        click.echo(value)
