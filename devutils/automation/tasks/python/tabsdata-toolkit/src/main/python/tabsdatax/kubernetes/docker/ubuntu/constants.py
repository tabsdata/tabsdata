#
# Copyright 2026 Tabsdata Inc.
#

from typing import Literal

LOG_FORMAT = "{message}"
LOG_STYLE: Literal["{"] = "{"

LOG_LEVEL_COLORS = {
    "DEBUG": "dim",
    "INFO": "cyan",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "red",
}

DOCKER_EXECUTABLE = "docker"

UBUNTU_BASE_IMAGE = "ubuntu:24.04"

IMAGE_BUILDER = "tabsdatax"
IMAGE_RUNNER = "tabsdata"
IMAGES = (IMAGE_BUILDER, IMAGE_RUNNER)

IMAGE_NAMESPACE = "tabsdata/kube/ubuntu"
IMAGE_TAG_FORMAT = "{namespace}/{image}:{version}"
CONTAINER_NAME_FORMAT = "{namespace}.{image}.{suffix}"
CONTAINER_NAMESPACE = "tabsdata.kube.ubuntu"

TAG_LATEST = "latest"
VERSION_TIMESTAMP_FORMAT = "%Y%m%d-%H%M%S"

PLATFORM_ARM_64 = "linux/arm64"
PLATFORM_AMD_64 = "linux/amd64"
PLATFORMS = (PLATFORM_ARM_64, PLATFORM_AMD_64)
DEFAULT_PLATFORM = PLATFORM_ARM_64

BUILD_ARGUMENT_TARGET_ARCHITECTURE = "TARGETARCH"

WORKSPACE_ROOT = "/workspace"
DEFAULT_REPOSITORIES = ("tabsdata-os",)
KNOWN_REPOSITORIES = (
    "tabsdata-os",
    "tabsdata-ee",
    "tabsdata-ag",
    "tabsdata-ui",
    "tabsdata-km",
)

DEVPI_PORT = 3141
DEVPI_INDEX = "tabsdata/td"
DEVPI_USER = "tabsdata"
DEVPI_PASSWORD = "tabsdata"
DEVPI_URL_FORMAT = "http://localhost:{port}/{index}/"

APISERVER_PORT = 2457

VENV_MODE_COPY = "copy"
VENV_MODE_MOUNT = "mount"

VENV_MODE_ENVIRONMENT_VARIABLE = "TD_VENV_MODE"
VENV_ARCHIVE_GLOB = "*.tar.zst"
VENV_ARCHIVES_MOUNT = "/mnt/venvs"

INSTANCE_NAME = "tabsdata"
RUNTIMES_IN_CONTAINER = "/root/.tabsdata/instances/tabsdata/runtimes/python"

WHEELS_FOLDER = "target/wheels"

REPOSITORIES_HOME_ENVIRONMENT_VARIABLE = "TABSDATA_REPOSITORIES_HOME"

VERSION_FILE = "assets/manifest/VERSION"
CARGO_FILE = "Cargo.toml"
GIT_FOLDER = ".git"
ROOT_FILE = ".root"
