#
# Copyright 2026 Tabsdata Inc.
#

import rich_click as click

from tabsdatax.itest.tdkinstance.commands.arguments import arguments
from tabsdatax.itest.tdkinstance.commands.banner import banner
from tabsdatax.itest.tdkinstance.commands.name import name
from tabsdatax.itest.tdkinstance.commands.port import port
from tabsdatax.itest.tdkinstance.commands.ps import ps
from tabsdatax.itest.tdkinstance.commands.ready import ready
from tabsdatax.itest.tdkinstance.commands.registry import registry
from tabsdatax.itest.tdkinstance.commands.types import types


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
main.add_command(ps)
main.add_command(ready)
main.add_command(registry)


if __name__ == "__main__":
    main()
