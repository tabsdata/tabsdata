#
# Copyright 2026 Tabs Data Inc.
#

from typing import Literal

LOG_FORMAT = "{message}"
LOG_STYLE: Literal["{"] = "{"

LOG_LEVEL_COLORS = {
    "DEBUG": "dim",
    "INFO": "cyan",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold red",
}

GIT_FOLDER = ".git"
ROOT_FILE = ".root"

CARGO_FILE = "Cargo.toml"
WORKSPACE_KEY = "workspace"
PACKAGE_KEY = "package"
VERSION_KEY = "version"

DOCKERFILE_NAME = "dockerfile"
SECRETS_FILE_NAME = "secrets.env"
CONTEXT_PREFIX = "tdkdocker-"

DOCKER_EXECUTABLE = "docker"

IMAGE_NAMESPACE = "tabsdata/kube"
VOLUME_NAMESPACE = "tabsdata.kube"
TAG_FORMAT = "{image_namespace}/{image_name}:{version}"
CONTAINER_NAME_FORMAT = "{namespace}.{image}.{version}.{suffix}"

IMAGE_ALL = "all"
IMAGE_TD = "td"
IMAGE_TABSDATA = "tabsdata"

IMAGES = (IMAGE_TD, IMAGE_TABSDATA)

RESOURCE_IMAGES = "images"
RESOURCE_VOLUMES = "volumes"
RESOURCE_BUILDS = "builds"

RESOURCES = (RESOURCE_IMAGES, RESOURCE_VOLUMES, RESOURCE_BUILDS)

WORKSPACE_MOUNT = "/workspace"
TABSDATA_REPOSITORIES_HOME = "TABSDATA_REPOSITORIES_HOME"
DEFAULT_PROJECTS = ("tabsdata-ee", "tabsdata-os", "tabsdata")
BUILD_SCRIPT_NAME = "build.sh"
INSTALL_SCRIPT_NAME = "install.sh"
PROJECT_ENV = "PROJECT"
TARGET_ENV = "TARGET"

WORKSPACE_MAIN = "main"
WORKSPACE_MACROS = "macros"
WORKSPACE_PYO3 = "pyo3"
WORKSPACES = (WORKSPACE_MAIN, WORKSPACE_MACROS, WORKSPACE_PYO3)

WORKSPACE_TARGETS = {
    WORKSPACE_MAIN: "build_rs_tabsdata",
    WORKSPACE_MACROS: "build_rs_macros",
    WORKSPACE_PYO3: "build_rs_expansions",
}

DEFAULT_BUILD_TARGET = "build"
WORKSPACE_PROJECTS = ("tabsdata-os", "tabsdata-ee", "tabsdata")

PYTHON_REQUIREMENTS_DEVELOPMENT_RELATIVE = (
    "variant/resources/lock/tabsdata-requirements-dev.txt"
)
PYTHON_REQUIREMENTS_DEVELOPMENT_NAME = "tabsdata-requirements-dev.txt"
PYTHON_REQUIREMENTS_PRODUCTION_RELATIVE = (
    "variant/resources/lock/tabsdata-requirements.txt"
)
PYTHON_REQUIREMENTS_PRODUCTION_NAME = "tabsdata-requirements.txt"
NODE_PROJECT_FOLDER = "tabsdata-ui"
NODE_PACKAGE_FILE = "package.json"
NODE_LOCK_FILE = "package-lock.json"
PACKAGES_CONTEXT_FOLDER = "packages"

VOLUME_NAME_FORMAT = "{namespace}.{image}.{version}-{role}"
SHARED_VOLUME_NAME_FORMAT = "{namespace}.shared.{version}-{role}"

VENV_TD_PATH = "/root/.uv/envs/td"
VENV_TABSDATA_PATH = "/root/.uv/envs/tabsdata"
UV_CACHE_PATH = "/root/.uv/cache/1"

SHARED_VOLUME_ROLES = {
    IMAGE_TD: {
        "venv-td": VENV_TD_PATH,
        "venv-tabsdata": VENV_TABSDATA_PATH,
        "uv-cache": UV_CACHE_PATH,
    },
    IMAGE_TABSDATA: {
        "venv-tabsdata": VENV_TABSDATA_PATH,
        "uv-cache": UV_CACHE_PATH,
    },
}

VOLUME_ROLES = {
    IMAGE_TD: {
        "cargo-bin": "/root/.cargo/bin",
        "cargo-git": "/root/.cargo/git",
        "cargo-registry": "/root/.cargo/registry",
        "sccache": "/root/.cache/sccache",
        "go-mod": "/root/go/pkg/mod",
        "go-build": "/root/.cache/go-build",
        "ui-node-modules": "/workspace/tabsdata-ui/node_modules",
    },
    IMAGE_TABSDATA: {
        "tabsdata": "/root/.tabsdata",
    },
}

TABSDATA_OS_OUTPUTS = [
    "target",
    "macros/target",
    "expansions/python/target",
    "expansions/polars/target",
    "connectors/python/*/target",
]

TABSDATA_EE_OUTPUTS = [
    "connectors.ee/python/*/target",
]

TABSDATA_KM_OUTPUTS = [
    "target",
    "mods/*/target",
]

PROJECT_OUTPUTS = {
    "tabsdata-os": TABSDATA_OS_OUTPUTS,
    "tabsdata-ee": TABSDATA_OS_OUTPUTS + TABSDATA_EE_OUTPUTS,
    "tabsdata-km": TABSDATA_KM_OUTPUTS,
    "tabsdata-ui": ["dist"],
    "tabsdata": TABSDATA_OS_OUTPUTS,
    "kminus": TABSDATA_KM_OUTPUTS,
}
