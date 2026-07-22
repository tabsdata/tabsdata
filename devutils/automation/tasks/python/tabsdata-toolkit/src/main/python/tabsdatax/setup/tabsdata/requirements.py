#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os

from tabsdatax.setup.common.requirements import read_requirements

logger = logging.getLogger(__name__)


def build_extras_require(
    root: str,
    version: str,
    connectors_folders: list[str] | None = None,
    test_requirements_path: str = "requirements-test.txt",
) -> dict[str, list[str]]:
    if connectors_folders is None:
        connectors_folders = [
            os.path.join(
                root,
                "connectors",
                "python",
            )
        ]

    extras: dict[str, list[str]] = {
        "test": read_requirements(
            os.path.join(
                root,
                test_requirements_path,
            )
        ),
    }

    all_connectors_requirements: list[str] = []
    requirement_template = "tabsdata-{connector}[deps]=={version}"

    for folder in connectors_folders:
        if not os.path.isdir(folder):
            continue

        for entry in os.scandir(folder):
            if entry.is_dir():
                connector_name = entry.name.split("-", 1)
                if len(connector_name) != 2 or connector_name[0] != "tabsdata":
                    logger.debug(f"⛔️ Invalid connector folder name: {entry.name}")
                else:
                    connector = connector_name[1]
                    logger.info(
                        "📦️ Adding requirements of connector requirements of "
                        f"{connector}: {entry.name}"
                    )
                    connector_requirement = requirement_template.format(
                        connector=connector,
                        version=version,
                    )
                    extras[connector] = [connector_requirement]
                    all_connectors_requirements.append(connector_requirement)

    extras["all"] = sorted(all_connectors_requirements)
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
