#
# Copyright 2026 Tabs Data Inc.
#

import logging
import sys

import rich_click as click
from python_on_whales import docker

from tabsdatax.docker.builder import container_name, ensure_image
from tabsdatax.docker.constants import (
    BUILD_SCRIPT_NAME,
    DEFAULT_BUILD_TARGET,
    IMAGE_TD,
    PROJECT_ENV,
    TARGET_ENV,
    WORKSPACE_MOUNT,
    WORKSPACE_PROJECTS,
    WORKSPACE_TARGETS,
    WORKSPACES,
)
from tabsdatax.docker.packages import require_repositories_home, resolve_project
from tabsdatax.docker.resources.images import image_resources
from tabsdatax.docker.version import project_version
from tabsdatax.docker.volume import project_volumes, shared_volume_map, volume_map

log = logging.getLogger(__name__)


def run_build(
    project: str | None,
    workspace: str | None = None,
    extra: tuple[str, ...] = (),
) -> None:
    repositories_home = require_repositories_home()
    project = resolve_project(repositories_home, project)

    if workspace and project not in WORKSPACE_PROJECTS:
        log.error(
            f"--workspace is only valid when building one of: "
            f"{', '.join(WORKSPACE_PROJECTS)} (got '{project}')."
        )
        sys.exit(1)

    target = WORKSPACE_TARGETS[workspace] if workspace else DEFAULT_BUILD_TARGET

    version = project_version()
    tag = ensure_image(IMAGE_TD, version)

    volumes = [(str(repositories_home), WORKSPACE_MOUNT)]
    volumes += list(volume_map(IMAGE_TD, version).items())
    volumes += list(project_volumes(repositories_home, version).items())
    volumes += list(shared_volume_map(IMAGE_TD, version).items())

    script = image_resources(IMAGE_TD).joinpath(BUILD_SCRIPT_NAME).read_text()

    log.info(f"Building {project} ({target}) in {tag}")
    log.info(f"Workspace: {repositories_home} -> {WORKSPACE_MOUNT}")

    docker.run(
        tag,
        ["bash", "-c", script, BUILD_SCRIPT_NAME, *extra],
        envs={PROJECT_ENV: project, TARGET_ENV: target},
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
    help="Project under TABSDATA_REPOSITORIES_HOME to build.",
)
@click.option(
    "--workspace",
    type=click.Choice(WORKSPACES),
    default=None,
    help="Cargo workspace slice to build (only for tabsdata-os/ee/tabsdata).",
)
@click.argument("extra", nargs=-1, type=click.UNPROCESSED)
def build(project: str | None, workspace: str | None, extra: tuple[str, ...]):
    """Build a Tabsdata project inside the td build image."""
    run_build(project, workspace, extra)
