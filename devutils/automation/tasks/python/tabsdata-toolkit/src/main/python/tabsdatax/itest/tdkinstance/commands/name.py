#
# Copyright 2026 Tabs Data Inc.
#

from datetime import datetime

import rich_click as click


@click.command(name="name")
@click.argument("role")
def name(role: str):
    """Print a collision-proof instance name for an instance role."""
    now = datetime.now()
    stamp = now.strftime("%Y.%m.%d.%H.%M.%S.") + f"{now.microsecond // 1000:03d}"
    click.echo(f"{role}-{stamp}")
