#
# Copyright 2026 Tabsdata Inc.
#

import re
from collections.abc import Mapping
from pathlib import Path

import rich_click as click
import yaml

ENV_PREFIX = "env:"
ENV_REFERENCE = re.compile(r"\$\{([^}]+)\}")


def read_yaml(path: Path, role: str):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)
    except OSError as error:
        raise click.ClickException(f"Could not read {role} '{path}': {error}")


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
    if value.startswith(ENV_PREFIX):
        rest = value[len(ENV_PREFIX) :].strip()
        variables = [variable.strip() for variable in ENV_REFERENCE.findall(rest)] or [
            rest
        ]
    else:
        variables = []
    return variables


def check_variables(stub: Path, owner: str, environment: Mapping[str, str]):
    for value in scalars(read_yaml(stub, "stub")):
        for variable in env_variables(value):
            if environment.get(variable, "").strip() == "":
                raise click.ClickException(
                    f"Environment variable '{variable}' required by stub '{stub}' "
                    f"of '{owner}' is not defined or is empty."
                )
