#
# Copyright 2026 Tabsdata Inc.
#

import os
import re
from pathlib import Path

import rich_click as click
import yaml

KEY_STUBS = "stubs"
CONFIG_OPTION = "--config"
FILE_PREFIX = "@"
ENV_PREFIX = "env:"
ENV_REFERENCE = re.compile(r"\$\{([^}]+)\}")


def read_yaml(path: Path, role: str):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    except OSError as error:
        raise click.ClickException(f"Could not read {role} '{path}': {error}")


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


def scalars(node) -> list[str]:
    if isinstance(node, dict):
        found = [text for value in node.values() for text in scalars(value)]
    elif isinstance(node, list):
        found = [text for item in node for text in scalars(item)]
    elif isinstance(node, str):
        found = [node]
    else:
        found = []
    return found


def env_variables(value: str) -> list[str]:
    """The environment variables an 'env:' value reads: each '${NAME}' in it,
    or the whole rest when it has none."""
    if value.startswith(ENV_PREFIX):
        rest = value[len(ENV_PREFIX) :].strip()
        variables = ENV_REFERENCE.findall(rest) or [rest]
    else:
        variables = []
    return variables


def check_variables(stub: Path, instance_type: str):
    for value in scalars(read_yaml(stub, "stub")):
        for variable in env_variables(value):
            if os.environ.get(variable, "").strip() == "":
                raise click.ClickException(
                    f"Environment variable '{variable}' required by stub '{stub}' "
                    f"of instance type '{instance_type}' is not defined or is empty."
                )


@click.command(name="arguments")
@click.argument("config", type=click.Path(path_type=Path))
@click.argument("instance_type")
def arguments(config: Path, instance_type: str):
    """Emit the '--config @STUB' tokens of an instance type.

    Each entry under the instance type's 'stubs' is a tdkserver stub file,
    relative to the config's folder. tdkserver reads their 'env:' values from
    the environment itself, so a secret never appears on the command line, and
    it rejects a setting declared by more than one stub. The run fails here,
    before tdkserver starts, if a variable an 'env:' value reads is undefined or
    empty.
    """
    folder = config.resolve().parent
    stubs = [
        (folder / declared).resolve()
        for declared in declared_stubs(read_yaml(config, "config"), instance_type)
    ]
    for stub in stubs:
        check_variables(stub, instance_type)
    for stub in stubs:
        click.echo(CONFIG_OPTION)
        click.echo(f"{FILE_PREFIX}{stub}")
