#
# Copyright 2026 Tabsdata Inc.
#

import rich_click as click

from tabsdatax.generator.tableframe.commands.compare import compare
from tabsdatax.generator.tableframe.commands.generate import generate
from tabsdatax.generator.tableframe.commands.inspect import inspect
from tabsdatax.generator.tableframe.common.group import OrderedGroup
from tabsdatax.generator.tableframe.common.logging import configure

configure()


@click.group(
    name="gtableframe",
    cls=OrderedGroup,
    context_settings={"help_option_names": ["-h", "--help"]},
)
def main() -> None:
    """Generate the TableFrame wrapper API over the Polars lazy API.

    \b
    The `inspect` command scans the installed Polars API into a polars manifest.
    The `generate` command emits the wrapper from the curated manifest.
    The `compare` command diffs two manifests across Polars versions.
    """


main.add_command(inspect)
main.add_command(compare)
main.add_command(generate)


if __name__ == "__main__":
    main()
