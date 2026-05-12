#
# Copyright 2026 Tabs Data Inc.
#

import logging

import rich_click as click

from tabsdatax.docker.builder import build_image
from tabsdatax.docker.commands.build import run_build
from tabsdatax.docker.commands.install import run_install
from tabsdatax.docker.constants import IMAGE_ALL, IMAGE_TABSDATA, IMAGES
from tabsdatax.docker.packages import collect_packages
from tabsdatax.docker.secrets import assert_secrets
from tabsdatax.docker.version import project_version
from tabsdatax.docker.volume import build_volumes

log = logging.getLogger(__name__)


@click.command()
@click.option(
    "--image",
    type=click.Choice([IMAGE_ALL, *IMAGES]),
    default=IMAGE_ALL,
    show_default=True,
    help="Which image to build.",
)
@click.option(
    "--no-cache",
    is_flag=True,
    help="Bypass the build cache.",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show plain build logs (full output, no progress UI).",
)
@click.option(
    "--keep",
    is_flag=True,
    help="Keep the temporary build context after the build completes.",
)
def create(image: str, no_cache: bool, verbose: bool, keep: bool):
    """Build the Tabsdata Docker images."""
    version = project_version()
    log.info(f"Project version: {version}")
    targets = list(IMAGES) if image == IMAGE_ALL else [image]
    for name in targets:
        assert_secrets(name)
    packages_per_image = {name: collect_packages(name) for name in targets}
    for name in targets:
        build_volumes(name, version)
        build_image(
            name,
            version,
            no_cache=no_cache,
            verbose=verbose,
            keep=keep,
            packages=packages_per_image[name],
        )
    if IMAGE_TABSDATA in targets:
        log.info("Building before hydrate...")
        run_build(None)
        log.info("Hydrating shared venv for tabsdata...")
        run_install(None)
