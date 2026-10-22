#
# Copyright 2026 Tabsdata Inc.
#

import os
import re
from pathlib import Path

import rich_click as click
import yaml

KEY_CONFIGS = "configs"
CONFIG_OPTION = "--config"
ENV_PREFIX = "env:"
ENV_REFERENCE = re.compile(r"\$\{([^}]+)\}")


def config_text(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def env_variables(value: str) -> list[str]:
    """The environment variables an 'env:' value reads: each '${NAME}' in it,
    or the whole rest when it has none."""
    if not value.startswith(ENV_PREFIX):
        return []
    rest = value[len(ENV_PREFIX) :]
    return ENV_REFERENCE.findall(rest) or [rest]


@click.command(name="arguments")
@click.argument("config", type=click.Path(path_type=Path))
@click.argument("instance_type")
def arguments(config: Path, instance_type: str):
    """Emit the '--config KEY=VALUE' tokens of an instance type.

    Each entry under the instance type's 'configs' is a tdkserver setting and
    its value, passed as written: tdkserver itself reads an 'env:' value from
    the environment, so a secret never appears on the command line. The run
    fails here, before tdkserver starts, if a variable an 'env:' value reads is
    undefined or empty.
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
    declared = options.get(KEY_CONFIGS)
    if declared is None:
        return
    if not isinstance(declared, dict):
        raise click.ClickException(
            f"Field '{KEY_CONFIGS}' for instance type '{instance_type}' must be a "
            "mapping of tdkserver setting to value."
        )

    for key, value in declared.items():
        value = config_text(value)
        for variable in env_variables(value):
            if os.environ.get(variable, "").strip() == "":
                raise click.ClickException(
                    f"Environment variable '{variable}' required by setting '{key}' "
                    f"of instance type '{instance_type}' is not defined or is empty."
                )
        click.echo(CONFIG_OPTION)
        click.echo(f"{key}={value}")
