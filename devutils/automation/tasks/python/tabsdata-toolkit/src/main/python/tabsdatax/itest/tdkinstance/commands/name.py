#
# Copyright 2026 Tabsdata Inc.
#

from datetime import datetime

import rich_click as click

from tabsdatax.itest.tdkinstance.commands.storage import STORAGE_OPTION


@click.command(name="name")
@click.argument("role")
@STORAGE_OPTION
def name(role: str, storage: str):
    """Print a collision-proof instance name for an instance role."""
    now = datetime.now()
    stamp = now.strftime("%Y%m%d%H%M%S") + f"{now.microsecond // 1000:03d}"
    click.echo(f"{role}-{storage}-{stamp}")
