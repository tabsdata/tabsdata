#
# Copyright 2026 Tabsdata Inc.
#

import logging

import rich_click as click
from python_on_whales import docker

from tabsdatax.docker.builder import container_name, ensure_image
from tabsdatax.docker.constants import (
    IMAGE_TD,
    INSTALL_SCRIPT_NAME,
    PROJECT_ENV,
    WORKSPACE_MOUNT,
)
from tabsdatax.docker.packages import require_repositories_home, resolve_project
from tabsdatax.docker.resources.images import image_resources
from tabsdatax.docker.version import project_version
from tabsdatax.docker.volume import project_volumes, shared_volume_map, volume_map

log = logging.getLogger(__name__)


def run_install(project: str | None, extra: tuple[str, ...] = ()) -> None:
    repositories_home = require_repositories_home()
    project = resolve_project(repositories_home, project)

    version = project_version()
    tag = ensure_image(IMAGE_TD, version)

    volumes = [(str(repositories_home), WORKSPACE_MOUNT)]
    volumes += list(volume_map(IMAGE_TD, version).items())
    volumes += list(project_volumes(repositories_home, version).items())
    volumes += list(shared_volume_map(IMAGE_TD, version).items())

    script = image_resources(IMAGE_TD).joinpath(INSTALL_SCRIPT_NAME).read_text()

    log.info(f"Installing {project} in {tag}")
    log.info(f"Workspace: {repositories_home} -> {WORKSPACE_MOUNT}")

    docker.run(
        tag,
        ["bash", "-c", script, INSTALL_SCRIPT_NAME, *extra],
        envs={PROJECT_ENV: project},
        volumes=volumes,
        name=container_name(IMAGE_TD, version),
        remove=True,
        tty=True,
        interactive=True,
    )


@click.command(context_settings={"ignore_unknown_options": True})
@click.option(
    "--project",
    default=None,
    help="Project under TABSDATA_REPOSITORIES_HOME to install.",
)
@click.argument("extra", nargs=-1, type=click.UNPROCESSED)
def install(project: str | None, extra: tuple[str, ...]):
    """Install (hydrate) a Tabsdata project inside the td build image."""
    run_install(project, extra)
