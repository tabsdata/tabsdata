#
# Copyright 2026 Tabs Data Inc.
#

import logging
import shutil
import subprocess

import rich_click as click
from python_on_whales import docker

from tabsdatax.docker.constants import (
    CONTEXT_PREFIX,
    DOCKER_EXECUTABLE,
    IMAGE_NAMESPACE,
    RESOURCE_BUILDS,
    RESOURCE_IMAGES,
    RESOURCE_VOLUMES,
    RESOURCES,
    VOLUME_NAMESPACE,
)

log = logging.getLogger(__name__)


def remove_volumes() -> None:
    volume_prefix = f"{VOLUME_NAMESPACE}."
    for volume in docker.volume.list():
        if volume.name.startswith(volume_prefix):
            log.info(f"Removing volume {volume.name}")
            docker.volume.remove(volume)


def remove_images() -> None:
    image_prefix = f"{IMAGE_NAMESPACE}/"
    for image in docker.image.list():
        for tag in image.repo_tags or []:
            if tag.startswith(image_prefix):
                log.info(f"Removing image {tag}")
                docker.image.remove(tag)
                break


def remove_builds() -> None:
    docker_path = shutil.which(DOCKER_EXECUTABLE)
    if not docker_path:
        return
    try:
        listing = subprocess.run(
            [
                docker_path,
                "buildx",
                "history",
                "ls",
                "--format",
                "{{.Ref}}\t{{.Name}}",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if listing.returncode != 0:
            return
        for line in listing.stdout.splitlines():
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            ref, name = parts
            if not name.startswith(CONTEXT_PREFIX):
                continue
            log.info(f"Removing build {ref} ({name})")
            subprocess.run(
                [docker_path, "buildx", "history", "rm", ref],
                capture_output=True,
                text=True,
                check=False,
                timeout=10,
            )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass


RESOURCE_HANDLERS = {
    RESOURCE_IMAGES: remove_images,
    RESOURCE_VOLUMES: remove_volumes,
    RESOURCE_BUILDS: remove_builds,
}


@click.command()
@click.option(
    "--resource",
    type=click.Choice(RESOURCES),
    default=None,
    help="Restrict removal to a single resource type (default: all).",
)
def destroy(resource: str | None):
    """Remove all Tabsdata-managed Docker images, volumes, and builds."""
    selected = [resource] if resource else list(RESOURCES)
    for name in selected:
        RESOURCE_HANDLERS[name]()
