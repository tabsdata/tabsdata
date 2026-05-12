#
# Copyright 2026 Tabs Data Inc.
#

import logging
import shutil
import subprocess
import tempfile
import uuid
from contextlib import contextmanager
from importlib.resources import as_file
from pathlib import Path

from python_on_whales import docker

from tabsdatax.docker.constants import (
    BUILD_SCRIPT_NAME,
    CONTAINER_NAME_FORMAT,
    CONTEXT_PREFIX,
    DOCKER_EXECUTABLE,
    DOCKERFILE_NAME,
    IMAGE_NAMESPACE,
    INSTALL_SCRIPT_NAME,
    SECRETS_FILE_NAME,
    TAG_FORMAT,
    VOLUME_NAMESPACE,
)
from tabsdatax.docker.packages import collect_packages
from tabsdatax.docker.resources.images import image_resources
from tabsdatax.docker.secrets import secrets_keys, secrets_map
from tabsdatax.docker.volume import build_volumes

log = logging.getLogger(__name__)


def container_name(image: str, version: str) -> str:
    return CONTAINER_NAME_FORMAT.format(
        namespace=VOLUME_NAMESPACE,
        image=image,
        version=version,
        suffix=uuid.uuid4().hex[:8],
    )


def append_secrets(dockerfile_path: Path, image_name: str) -> None:
    secrets = secrets_keys(image_name)
    if not secrets:
        return
    with dockerfile_path.open("a") as handle:
        handle.write("\n")
        for name in secrets:
            handle.write(f"ARG {name}\n")
            handle.write(f"ENV {name}=${{{name}}}\n")


def report_build() -> None:
    docker_path = shutil.which(DOCKER_EXECUTABLE)
    if not docker_path:
        return
    try:
        list_process = subprocess.run(
            [docker_path, "buildx", "history", "--format", "{{.Ref}}", "--limit", "1"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        ref = list_process.stdout.strip()
        if not ref or list_process.returncode != 0:
            return
        inspect_process = subprocess.run(
            [docker_path, "buildx", "history", "inspect", ref],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if inspect_process.returncode != 0:
            return
        log.info("Build report:")
        for line in inspect_process.stdout.rstrip().splitlines():
            log.info(line)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass


def copy_packages(context_path: Path, packages: dict[str, Path] | None) -> None:
    if not packages:
        return
    for relative_target, source in packages.items():
        target = context_path / relative_target
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)


@contextmanager
def prepare_context(
    image_name: str,
    keep: bool = False,
    packages: dict[str, Path] | None = None,
):
    with as_file(image_resources(image_name)) as image_folder:
        context_path = Path(tempfile.mkdtemp(prefix=CONTEXT_PREFIX))
        try:
            shutil.copytree(
                image_folder,
                context_path,
                dirs_exist_ok=True,
                ignore=shutil.ignore_patterns(
                    "__init__.py",
                    "__pycache__",
                    SECRETS_FILE_NAME,
                    BUILD_SCRIPT_NAME,
                    INSTALL_SCRIPT_NAME,
                ),
            )
            copy_packages(context_path, packages)
            dockerfile_path = context_path / DOCKERFILE_NAME
            append_secrets(dockerfile_path, image_name)
            yield context_path, dockerfile_path
        finally:
            if keep:
                log.info(f"Keeping build context: {context_path}")
            else:
                shutil.rmtree(context_path, ignore_errors=True)


def build_image(
    image_name: str,
    version: str,
    no_cache: bool = False,
    verbose: bool = False,
    keep: bool = False,
    packages: dict[str, Path] | None = None,
) -> str:
    tag = TAG_FORMAT.format(
        image_namespace=IMAGE_NAMESPACE,
        image_name=image_name,
        version=version,
    )
    log.info(f"Building {tag}")
    with prepare_context(image_name, keep=keep, packages=packages) as (
        context_path,
        dockerfile_path,
    ):
        log.info(f"Build context: {context_path}")
        log.info(f"Dockerfile: {dockerfile_path}")
        build_command = [
            DOCKER_EXECUTABLE,
            "buildx",
            "build",
            "--file",
            str(dockerfile_path),
            "--tag",
            tag,
            "--load",
            "--provenance=false",
            "--sbom=false",
            "--progress",
            "plain" if verbose else "auto",
        ]
        if no_cache:
            build_command.append("--no-cache")
        for key, value in secrets_map(image_name).items():
            build_command += ["--build-arg", f"{key}={value}"]
        build_command.append(str(context_path))
        subprocess.run(build_command, check=True)
    log.info(f"Built {tag}")
    report_build()
    return tag


def ensure_image(image_name: str, version: str) -> str:
    tag = TAG_FORMAT.format(
        image_namespace=IMAGE_NAMESPACE,
        image_name=image_name,
        version=version,
    )
    if docker.image.exists(tag):
        return tag
    log.info(f"Image {tag} not found; building it first.")
    build_volumes(image_name, version)
    build_image(image_name, version, packages=collect_packages(image_name))
    return tag
