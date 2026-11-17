#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from tabsdatax.setup.common.edition import edition_connectors_folders
from tabsdatax.setup.common.requirements import read_requirements

logger = logging.getLogger(__name__)


def gather_connectors(folder: str) -> list[str]:
    if not os.path.isdir(folder):
        return []
    connectors: list[str] = []
    for entry in os.scandir(folder):
        if not entry.is_dir():
            continue
        connector_name = entry.name.split("-", 1)
        if len(connector_name) != 2 or connector_name[0] != "tabsdata":
            logger.debug(f"⛔️ Invalid connector folder name: {entry.name}")
        else:
            connectors.append(connector_name[1])
    return sorted(connectors)


def gather_edition_packages(
    root: str,
    connectors_folders: list[str] | None = None,
) -> list[Path]:
    if connectors_folders is None:
        connectors_folders = edition_connectors_folders(root)
    return sorted(
        {
            Path(folder) / f"tabsdata-{connector}"
            for folder in connectors_folders
            for connector in gather_connectors(folder)
        },
        key=lambda package: package.name,
    )


def build_install_requires(
    root: str,
    version: str,
    connectors_folders: list[str] | None = None,
    requirements_path: str = "requirements.txt",
) -> list[str]:
    requirements = read_requirements(
        os.path.join(root, requirements_path), ignore_tabsdata_packages=False
    )
    known = {canonicalize_name(Requirement(line).name): line for line in requirements}
    for package in gather_edition_packages(root, connectors_folders):
        package_requirements = [
            f"{package.name}=={version}",
            *read_requirements(package / requirements_path),
        ]
        for line in package_requirements:
            name = canonicalize_name(Requirement(line).name)
            if name not in known:
                logger.info(f"📦️ Requiring {line} for {package.name}")
                known[name] = line
                requirements.append(line)
            elif not same_requirement(known[name], line):
                raise ValueError(
                    f"Package {package.name} requires '{line}', which conflicts "
                    f"with '{known[name]}'"
                )
    return requirements


def same_requirement(left: str, right: str) -> bool:
    first, second = Requirement(left), Requirement(right)
    return (
        canonicalize_name(first.name) == canonicalize_name(second.name)
        and first.specifier == second.specifier
        and first.extras == second.extras
        and first.marker == second.marker
        and first.url == second.url
    )


def build_extras_require(
    root: str,
    connectors_folders: list[str] | None = None,
    test_requirements_path: str = "requirements-test.txt",
) -> dict[str, list[str]]:
    extras: dict[str, list[str]] = {
        package.name.removeprefix("tabsdata-"): []
        for package in gather_edition_packages(root, connectors_folders)
    }
    extras["all"] = []
    extras["test"] = read_requirements(os.path.join(root, test_requirements_path))
    return extras


def build_agent_extras_require(
    root: str,
    test_requirements_path: str = "requirements-test.txt",
) -> dict[str, list[str]]:
    return {
        "test": read_requirements(
            os.path.join(
                root,
                test_requirements_path,
            )
        ),
        "all": [],
    }
