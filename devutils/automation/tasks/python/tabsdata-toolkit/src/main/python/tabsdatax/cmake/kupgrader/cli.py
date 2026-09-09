#
# Copyright 2026 Tabsdata Inc.
#

import logging
import sys

import rich_click as click

from tabsdatax.cmake.kupgrader.commands.build import build
from tabsdatax.cmake.kupgrader.commands.check import check
from tabsdatax.cmake.kupgrader.common.errors import KupgraderError
from tabsdatax.cmake.kupgrader.common.group import OrderedGroup
from tabsdatax.cmake.kupgrader.common.logging import configure

configure()

log = logging.getLogger(__name__)


class ReportingGroup(OrderedGroup):
    def invoke(self, ctx: click.Context):
        try:
            return super().invoke(ctx)
        except KupgraderError as error:
            log.error(str(error))
            sys.exit(1)


@click.group(
    name="kupgrader",
    cls=ReportingGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
)
def main() -> None:
    """Check and build the kupgrade upgraders of a tabsdata repository.

    \b
    The `check` command verifies the shipped upgrader is correct and complete.
    The `build` command regenerates the upgrader and verifies its outcome.
    """


main.add_command(check)
main.add_command(build)


if __name__ == "__main__":
    main()
