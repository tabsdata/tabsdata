#
# Copyright 2026 Tabsdata Inc.
#

import logging

import rich_click as click
from rich.logging import RichHandler

from tabsdatax.kubernetes.docker.ubuntu.constants import (
    DEFAULT_PLATFORM,
    DEFAULT_REPOSITORIES,
    IMAGES,
    KNOWN_REPOSITORIES,
    LOG_FORMAT,
    LOG_LEVEL_COLORS,
    LOG_STYLE,
    PLATFORMS,
    VENV_MODE_COPY,
    VENV_MODE_MOUNT,
)


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
    name="tdkubuntu",
    context_settings={"help_option_names": ["-h", "--help"]},
    help="Build and run Tabsdata on portable Ubuntu Docker images.",
)
def main():
    pass


@main.command(
    name="build-builder",
    help="Build the tabsdatax builder image.",
)
@click.option(
    "--platform",
    type=click.Choice(PLATFORMS),
    default=DEFAULT_PLATFORM,
    show_default=True,
)
@click.option("--no-cache", is_flag=True)
@click.option("--verbose", "-v", is_flag=True)
def build_builder(platform: str, no_cache: bool, verbose: bool):
    raise NotImplementedError


@main.command(
    name="start-builder",
    help="Start a long-lived builder container.",
)
@click.option(
    "--platform",
    type=click.Choice(PLATFORMS),
    default=DEFAULT_PLATFORM,
    show_default=True,
)
def start_builder(platform: str):
    raise NotImplementedError


@main.command(
    name="copy-repositories",
    help="Copy repositories into the running builder container.",
)
@click.option(
    "--repositories",
    type=click.Choice(KNOWN_REPOSITORIES),
    multiple=True,
    default=DEFAULT_REPOSITORIES,
    show_default=True,
)
def copy_repositories(repositories: tuple[str, ...]):
    raise NotImplementedError


@main.command(
    context_settings={"ignore_unknown_options": True},
    help="Build and package Tabsdata wheels inside the builder container.",
)
@click.option("--repository", default="tabsdata-os", show_default=True)
@click.argument("extra", nargs=-1, type=click.UNPROCESSED)
def package(repository: str, extra: tuple[str, ...]):
    raise NotImplementedError


@main.command(
    context_settings={"ignore_unknown_options": True},
    help="Repackage Tabsdata wheels without recompiling, from existing artifacts.",
)
@click.option("--repository", default="tabsdata-os", show_default=True)
@click.argument("extra", nargs=-1, type=click.UNPROCESSED)
def repackage(repository: str, extra: tuple[str, ...]):
    raise NotImplementedError


@main.command(
    name="export-venvs",
    help="Copy the runtime venv archives out of the builder to a local folder.",
)
def export_venvs():
    raise NotImplementedError


@main.command(
    name="build-runner",
    help="Build the tabsdata runner image.",
)
@click.option(
    "--platform",
    type=click.Choice(PLATFORMS),
    default=DEFAULT_PLATFORM,
    show_default=True,
)
@click.option("--no-cache", is_flag=True)
@click.option("--verbose", "-v", is_flag=True)
def build_runner(platform: str, no_cache: bool, verbose: bool):
    raise NotImplementedError


@main.command(
    name="start-runner",
    context_settings={"ignore_unknown_options": True},
    help="Start a runner container with copied or mounted venvs.",
)
@click.option(
    "--venvs",
    type=click.Choice([VENV_MODE_COPY, VENV_MODE_MOUNT]),
    default=VENV_MODE_COPY,
    show_default=True,
)
@click.argument("command", nargs=-1, type=click.UNPROCESSED)
def start_runner(venvs: str, command: tuple[str, ...]):
    raise NotImplementedError


@main.command(
    name="list",
    help="List built Tabsdata Ubuntu images.",
)
@click.option("--image", type=click.Choice(IMAGES), default=None)
def list_images(image: str | None):
    raise NotImplementedError


@main.command(
    help="Remove timestamped images, optionally keeping the one tagged latest.",
)
@click.option("--image", type=click.Choice(IMAGES), default=None)
@click.option("--keep-latest", is_flag=True)
def clean(image: str | None, keep_latest: bool):
    raise NotImplementedError
