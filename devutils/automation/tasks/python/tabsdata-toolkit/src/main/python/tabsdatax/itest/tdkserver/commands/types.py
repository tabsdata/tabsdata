#
# Copyright 2026 Tabs Data Inc.
#

from pathlib import Path

import rich_click as click
import yaml


@click.command(name="types")
@click.argument("config", type=click.Path(path_type=Path))
def types(config: Path):
    """List the instance types declared in a kubernetes.yaml config."""
    try:
        with open(config, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except OSError:
        return
    if isinstance(data, dict):
        for instance_type in data:
            click.echo(instance_type)
