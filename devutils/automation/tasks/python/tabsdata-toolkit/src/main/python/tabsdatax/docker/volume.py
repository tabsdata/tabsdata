#
# Copyright 2026 Tabs Data Inc.
#

import logging
from fnmatch import fnmatch
from pathlib import Path

from python_on_whales import docker

from tabsdatax.docker.constants import (
    IMAGE_TD,
    PROJECT_OUTPUTS,
    SHARED_VOLUME_NAME_FORMAT,
    SHARED_VOLUME_ROLES,
    VOLUME_NAME_FORMAT,
    VOLUME_NAMESPACE,
    VOLUME_ROLES,
    WORKSPACE_MOUNT,
)

log = logging.getLogger(__name__)


def shared_volume_map(image: str, version: str) -> dict[str, str]:
    return {
        SHARED_VOLUME_NAME_FORMAT.format(
            namespace=VOLUME_NAMESPACE,
            version=version,
            role=role,
        ): mount_path
        for role, mount_path in SHARED_VOLUME_ROLES.get(image, {}).items()
    }


def volume_map(image: str, version: str) -> dict[str, str]:
    return {
        VOLUME_NAME_FORMAT.format(
            namespace=VOLUME_NAMESPACE,
            image=image,
            version=version,
            role=role,
        ): mount_path
        for role, mount_path in VOLUME_ROLES.get(image, {}).items()
    }


def expand_paths(project_root: Path, pattern: str) -> list[str]:
    if "*" not in pattern:
        return [pattern]
    parts = pattern.split("/")
    star_index = next(index for index, segment in enumerate(parts) if "*" in segment)
    prefix_parts = parts[:star_index]
    star_segment = parts[star_index]
    suffix_parts = parts[star_index + 1 :]
    parent = project_root.joinpath(*prefix_parts) if prefix_parts else project_root
    if not parent.is_dir():
        return []
    matches: list[str] = []
    for entry in sorted(parent.iterdir()):
        if not entry.is_dir():
            continue
        if not fnmatch(entry.name, star_segment):
            continue
        joined = "/".join([*prefix_parts, entry.name, *suffix_parts])
        matches.append(joined)
    return matches


def project_volumes(repositories_home: Path, version: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for project_folder, patterns in PROJECT_OUTPUTS.items():
        project_root = repositories_home / project_folder
        if not project_root.is_dir():
            continue
        for pattern in patterns:
            for relative_path in expand_paths(project_root, pattern):
                role = f"{project_folder}-{relative_path.replace('/', '-')}"
                name = VOLUME_NAME_FORMAT.format(
                    namespace=VOLUME_NAMESPACE,
                    image=IMAGE_TD,
                    version=version,
                    role=role,
                )
                result[name] = f"{WORKSPACE_MOUNT}/{project_folder}/{relative_path}"
    return result


def build_volumes(image: str, version: str) -> None:
    expected = volume_map(image, version)
    if not expected:
        return
    existing = {volume.name for volume in docker.volume.list()}
    for name in expected:
        if name in existing:
            continue
        docker.volume.create(name)
        log.info(f"Created volume {name}")
