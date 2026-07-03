#
# Copyright 2026 Tabs Data Inc.
#

import rich_click as click

from tabsdatax.itest.tdkserver.commands.arguments import arguments
from tabsdatax.itest.tdkserver.commands.banner import banner
from tabsdatax.itest.tdkserver.commands.name import name
from tabsdatax.itest.tdkserver.commands.port import port
from tabsdatax.itest.tdkserver.commands.ready import ready
from tabsdatax.itest.tdkserver.commands.registry import registry
from tabsdatax.itest.tdkserver.commands.types import types


@click.group(
    name="tdk-itest",
    context_settings={"help_option_names": ["-h", "--help"]},
)
def main():
    """Helpers for tdkserver-driven integration test instances."""


main.add_command(types)
main.add_command(arguments)
main.add_command(banner)
main.add_command(name)
main.add_command(port)
main.add_command(ready)
main.add_command(registry)


if __name__ == "__main__":
    main()
