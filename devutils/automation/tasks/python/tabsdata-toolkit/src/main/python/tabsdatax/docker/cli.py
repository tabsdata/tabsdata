#
# Copyright 2026 Tabsdata Inc.
#

import logging

import rich_click as click
from rich.logging import RichHandler

from tabsdatax.docker.commands.build import build
from tabsdatax.docker.commands.create import create
from tabsdatax.docker.commands.destroy import destroy
from tabsdatax.docker.commands.install import install
from tabsdatax.docker.commands.shell import shell
from tabsdatax.docker.constants import LOG_FORMAT, LOG_LEVEL_COLORS, LOG_STYLE


class LevelColoredFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        color = LOG_LEVEL_COLORS.get(record.levelname)
        if color:
            return f"[{color}]{message}[/{color}]"
        return message


handler = RichHandler(
    show_time=False,
    show_level=False,
    show_path=False,
    rich_tracebacks=True,
    markup=True,
)
handler.setFormatter(LevelColoredFormatter(LOG_FORMAT, style=LOG_STYLE))

logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


@click.group(
    name="docker",
    context_settings={"help_option_names": ["-h", "--help"]},
)
def main():
    """Build and manage Tabsdata Docker images."""


main.add_command(create)
main.add_command(build)
main.add_command(install)
main.add_command(shell)
main.add_command(destroy)
