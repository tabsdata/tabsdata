#
# Copyright 2026 Tabs Data Inc.
#

from pathlib import Path

import rich_click as click
import yaml

KEY_INSTANCES = "instances"


def load_names(registry_file: Path) -> list[str]:
    if not registry_file.exists():
        return []
    try:
        with open(registry_file, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except OSError:
        return []
    if not isinstance(data, dict):
        return []
    names = data.get(KEY_INSTANCES)
    if not isinstance(names, list):
        return []
    return [str(item) for item in names]


def save_names(registry_file: Path, names: list[str]):
    registry_file.parent.mkdir(parents=True, exist_ok=True)
    with open(registry_file, "w", encoding="utf-8") as handle:
        yaml.safe_dump({KEY_INSTANCES: names}, handle, default_flow_style=False)


@click.group(name="registry")
def registry():
    """Manage the file recording the created itest instances."""


@registry.command(name="list")
@click.argument("registry_file", type=click.Path(path_type=Path))
def list_names(registry_file: Path):
    """Print the recorded instance names, one per line."""
    for name in load_names(registry_file):
        click.echo(name)


@registry.command(name="append")
@click.argument("registry_file", type=click.Path(path_type=Path))
@click.argument("name")
def append_name(registry_file: Path, name: str):
    """Record an instance name."""
    names = load_names(registry_file)
    if name not in names:
        names.append(name)
    save_names(registry_file, names)


@registry.command(name="remove")
@click.argument("registry_file", type=click.Path(path_type=Path))
@click.argument("name")
def remove_name(registry_file: Path, name: str):
    """Delete an instance name from the registry."""
    names = load_names(registry_file)
    if name in names:
        names.remove(name)
        save_names(registry_file, names)


@registry.command(name="clear")
@click.argument("registry_file", type=click.Path(path_type=Path))
def clear_names(registry_file: Path):
    """Remove the registry file."""
    if registry_file.exists():
        registry_file.unlink()
