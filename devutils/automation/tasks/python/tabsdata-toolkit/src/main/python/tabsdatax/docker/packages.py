#
# Copyright 2026 Tabsdata Inc.
#

import logging
import os
import sys
from pathlib import Path

from tabsdatax.docker.constants import (
    DEFAULT_PROJECTS,
    IMAGE_TD,
    NODE_LOCK_FILE,
    NODE_PACKAGE_FILE,
    NODE_PROJECT_FOLDER,
    NODE_PROJECTS,
    PACKAGES_CONTEXT_FOLDER,
    PYTHON_REQUIREMENTS_DEVELOPMENT_NAME,
    PYTHON_REQUIREMENTS_DEVELOPMENT_RELATIVE,
    PYTHON_REQUIREMENTS_PRODUCTION_NAME,
    PYTHON_REQUIREMENTS_PRODUCTION_RELATIVE,
    TABSDATA_REPOSITORIES_HOME,
)

log = logging.getLogger(__name__)


def require_repositories_home() -> Path:
    value = os.environ.get(TABSDATA_REPOSITORIES_HOME, "").strip()
    if not value:
        log.error(
            f"Environment variable {TABSDATA_REPOSITORIES_HOME} is not set or empty."
        )
        sys.exit(1)
    return Path(value)


def resolve_project(repositories_home: Path, project: str | None) -> str:
    if project:
        if not (repositories_home / project).is_dir():
            log.error(
                f"Project '{project}' not found at {repositories_home / project}."
            )
            sys.exit(1)
        return project
    for candidate in DEFAULT_PROJECTS:
        if (repositories_home / candidate).is_dir():
            return candidate
    listing = ", ".join(DEFAULT_PROJECTS)
    log.error(
        f"No default project found under {repositories_home} (looked for: {listing}). "
        f"Pass --project <name> explicitly."
    )
    sys.exit(1)


def resolve_python_requirements(relative_path: str, label: str) -> Path:
    repositories_home = require_repositories_home()
    tried = []
    for project in DEFAULT_PROJECTS:
        candidate = repositories_home / project / relative_path
        tried.append(str(candidate))
        if candidate.is_file():
            return candidate
    listing = "\n  ".join(tried)
    log.error(f"Could not find {label}:\n  {listing}")
    sys.exit(1)


def resolve_node_files() -> dict[str, tuple[Path, Path]]:
    repositories_home = require_repositories_home()
    node_folder = repositories_home / NODE_PROJECT_FOLDER
    if not node_folder.is_dir():
        return {}
    found: dict[str, tuple[Path, Path]] = {}
    for project in NODE_PROJECTS:
        package_json = node_folder / project / NODE_PACKAGE_FILE
        package_lock = node_folder / project / NODE_LOCK_FILE
        if not package_json.is_file() or not package_lock.is_file():
            log.warning(
                f"{NODE_PROJECT_FOLDER}/{project} has no {NODE_PACKAGE_FILE} or "
                f"{NODE_LOCK_FILE}; skipping its node modules pre-install."
            )
            continue
        found[project] = (package_json, package_lock)
    return found


def collect_packages(image_name: str) -> dict[str, Path]:
    if image_name != IMAGE_TD:
        return {}
    development_key = (
        f"{PACKAGES_CONTEXT_FOLDER}/{PYTHON_REQUIREMENTS_DEVELOPMENT_NAME}"
    )
    production_key = f"{PACKAGES_CONTEXT_FOLDER}/{PYTHON_REQUIREMENTS_PRODUCTION_NAME}"
    packages: dict[str, Path] = {
        development_key: resolve_python_requirements(
            PYTHON_REQUIREMENTS_DEVELOPMENT_RELATIVE,
            PYTHON_REQUIREMENTS_DEVELOPMENT_NAME,
        ),
        production_key: resolve_python_requirements(
            PYTHON_REQUIREMENTS_PRODUCTION_RELATIVE,
            PYTHON_REQUIREMENTS_PRODUCTION_NAME,
        ),
    }
    for project, (package_json, package_lock) in resolve_node_files().items():
        folder = f"{PACKAGES_CONTEXT_FOLDER}/{project}"
        packages[f"{folder}/{NODE_PACKAGE_FILE}"] = package_json
        packages[f"{folder}/{NODE_LOCK_FILE}"] = package_lock
    return packages
