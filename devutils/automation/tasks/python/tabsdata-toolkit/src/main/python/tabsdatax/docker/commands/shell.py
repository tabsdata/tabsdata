#
# Copyright 2026 Tabsdata Inc.
#

import logging
import sys

import rich_click as click
from python_on_whales import docker
from python_on_whales.exceptions import DockerException

from tabsdatax.docker.builder import container_name, ensure_image
from tabsdatax.docker.constants import (
    IMAGE_TABSDATA,
    IMAGE_TD,
    WORKSPACE_MOUNT,
)
from tabsdatax.docker.packages import require_repositories_home, resolve_project
from tabsdatax.docker.secrets import secrets_map_present
from tabsdatax.docker.version import project_version
from tabsdatax.docker.volume import project_volumes, shared_volume_map, volume_map

log = logging.getLogger(__name__)

SHELL_COMMAND = ("bash",)


def run_interactive_shell(**docker_run_arguments) -> None:
    try:
        docker.run(**docker_run_arguments)
    except DockerException as exception:
        sys.exit(exception.return_code or 0)


@click.group()
def shell():
    """Drop into an interactive shell in a Tabsdata container."""


@shell.command(name=IMAGE_TD)
@click.option(
    "--project",
    default=None,
    help="Project under TABSDATA_REPOSITORIES_HOME to start in.",
)
def shell_td(project: str | None):
    """Shell into the td build container."""
    repositories_home = require_repositories_home()
    project = resolve_project(repositories_home, project)
    workdir = f"{WORKSPACE_MOUNT}/{project}"

    version = project_version()
    tag = ensure_image(IMAGE_TD, version)

    volumes = [(str(repositories_home), WORKSPACE_MOUNT)]
    volumes += list(volume_map(IMAGE_TD, version).items())
    volumes += list(project_volumes(repositories_home, version).items())
    volumes += list(shared_volume_map(IMAGE_TD, version).items())

    log.info(f"Shelling into {tag} (workdir {workdir})")

    run_interactive_shell(
        image=tag,
        command=list(SHELL_COMMAND),
        volumes=volumes,
        workdir=workdir,
        name=container_name(IMAGE_TD, version),
        remove=True,
        tty=True,
        interactive=True,
    )


@shell.command(name=IMAGE_TABSDATA)
def shell_tabsdata():
    """Shell into the tabsdata runtime container."""
    repositories_home = require_repositories_home()

    version = project_version()
    tag = ensure_image(IMAGE_TABSDATA, version)

    volumes = [(str(repositories_home), WORKSPACE_MOUNT)]
    volumes += list(volume_map(IMAGE_TABSDATA, version).items())
    volumes += list(project_volumes(repositories_home, version).items())
    volumes += list(shared_volume_map(IMAGE_TABSDATA, version).items())
    envs = secrets_map_present(IMAGE_TABSDATA)

    log.info(f"Shelling into {tag}")
    log.info(f"Workspace: {repositories_home} -> {WORKSPACE_MOUNT}")
    if envs:
        log.info(f"Overriding {len(envs)} env var(s) from host")

    run_interactive_shell(
        image=tag,
        command=list(SHELL_COMMAND),
        envs=envs,
        volumes=volumes,
        name=container_name(IMAGE_TABSDATA, version),
        remove=True,
        tty=True,
        interactive=True,
    )
