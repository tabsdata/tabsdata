#
# Copyright 2025 Tabs Data Inc.
#

import logging
import os
import subprocess
from datetime import datetime

from tabsdatax.setup.common.config import TD_SETUP_GENERATE_LOCKS, TRUE_VALUES

TABSDATA = "tabsdata"
TABSDATA_OS = "tabsdata-os"
TABSDATA_EE = "tabsdata-ee"
TABSDATA_AG = "tabsdata-ag"
TABSDATA_UI = "tabsdata-ui"

logger = logging.getLogger(__name__)


def write_requirements_lock(requirements, output_path, cwd=None):
    command = [
        "uv",
        "pip",
        "compile",
        "--no-annotate",
        "--no-header",
        "--quiet",
        "--format",
        "requirements.txt",
        "--output-file",
        output_path,
    ]
    command.extend(requirements)
    try:
        uv_output = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,
            cwd=cwd,
        )
        if uv_output.returncode == 0:
            with open(output_path, "r") as f:
                content = f.read()
            year = datetime.now().year
            header = f"#\n# Copyright {year} Tabs Data Inc.\n#\n\n"
            with open(output_path, "w") as f:
                f.write(header + content)
            return
    except Exception as exception:
        raise RuntimeError(
            f"Failed to write requirements lock to {output_path}: {exception}"
        ) from exception


def generate_requirements_lock(root_path, package_resources_folder):
    if os.getenv(TD_SETUP_GENERATE_LOCKS, "False").lower() not in TRUE_VALUES:
        logger.info("Skipping lock generation (TD_SETUP_GENERATE_LOCKS is not set)")
        return

    output_path = os.path.join(
        package_resources_folder, "lock", "tabsdata-requirements.txt"
    )

    requirements = [
        os.path.join(root_path, "requirements", "requirements-third-party-all.txt")
    ]

    root_basename = os.path.basename(os.path.normpath(root_path))
    if root_basename in (TABSDATA, TABSDATA_EE):
        tabsdata_agent_root = os.path.join(root_path, "..", TABSDATA_AG)
        if os.path.isdir(tabsdata_agent_root):
            requirements.append(
                os.path.join(
                    tabsdata_agent_root,
                    "requirements",
                    "requirements-third-party-all.txt",
                )
            )

    write_requirements_lock(requirements, output_path, cwd=root_path)
