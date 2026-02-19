#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from pathlib import Path
from uuid import uuid4

from collections.abc import Set

logger = logging.getLogger(__name__)

TABSDATA_PACKAGES_PREFIX = "tabsdata"


def read_requirements(
    requirements_file: str | Path,
    ignore_tabsdata_packages: bool = True,
    root: str | Path | None = None,
    token: str | None = None,
    visited: Set[Path] | None = None,
) -> list[str]:
    if token is None:
        token = str(uuid4())
    if visited is None:
        visited = set()

    requirements_file = Path(requirements_file).resolve()

    logger.debug(f" - 🥁 {token} · Visiting requirements path: {root} - {requirements_file}")

    if requirements_file in visited:
        raise ValueError(f"Circular dependency detected: {requirements_file}")
    visited.add(requirements_file)

    requirements: list[str] = []

    with requirements_file.open(encoding="utf-8") as file:
        for requirement in file:
            requirement = requirement.strip()

            if not requirement or requirement.startswith(("#", '"')):
                continue

            if requirement.startswith("-r"):
                included_path = requirement.split(maxsplit=1)[1]
                if not os.path.isabs(included_path):
                    included_path = requirements_file.parent / included_path
                requirements.extend(
                    read_requirements(
                        included_path,
                        ignore_tabsdata_packages=ignore_tabsdata_packages,
                        root=requirements_file,
                        token=token,
                        visited=visited,
                    )
                )
            elif not requirement.startswith(("-", "git+")):
                requirements.append(requirement)
            else:
                logger.warning(f"📦 Discarding unsupported requirement entry: {requirements_file} - {requirement}")

    if ignore_tabsdata_packages:
        requirements = [
            requirement
            for requirement in requirements
            if not requirement.startswith(TABSDATA_PACKAGES_PREFIX)
        ]

    logger.debug("📦 List of application requirements in setup.py:")
    for requirement in requirements:
        logger.debug(f" - 📚 {requirement}")

    return requirements
