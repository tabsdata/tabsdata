#
#  Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
import platform
from sysconfig import get_path, get_platform

from tabsdatax.setup.common.config import (
    TD_USE_MUSLLINUX,
    TRUE_VALUES,
    get_build_profile,
    get_td_target,
)

logger = logging.getLogger(__name__)


def check_cpython() -> None:
    if platform.python_implementation() != "CPython":
        raise RuntimeError(
            "The Tabsdata package requires CPython to function correctly."
        )


# PEP-513: A Platform Tag for Portable Linux Built Distributions
#     https://peps.python.org/pep-0513/
#
# PEP-599: The manylinux2014 Platform Tag
#     https://peps.python.org/pep-0599/
#
# PEP-656: Platform Tag for Linux Distributions Using Musl
#     https://peps.python.org/pep-0656/
def get_platname() -> str:
    system = platform.system()
    architecture = platform.machine().lower()
    use_musllinux = os.getenv(TD_USE_MUSLLINUX, "False").lower() in TRUE_VALUES

    # Linux
    if system == "Linux":
        if architecture in ["x86_64", "amd64"]:
            if use_musllinux:
                return "musllinux_1_1_x86_64"
            else:
                return "manylinux1_x86_64"
        elif architecture in ["aarch64", "arm64"]:
            if use_musllinux:
                return "musllinux_1_1_aarch64"
            else:
                return "manylinux2014_aarch64"
        else:
            if use_musllinux:
                platname = f"musllinux_1_1_{architecture}"
                return platname.replace("-", "_").replace(".", "_")
            else:
                platname = f"manylinux1_{architecture}"
                return platname.replace("-", "_").replace(".", "_")
    # macOS
    elif system == "Darwin":
        if architecture in ["aarch64", "arm64"]:
            return "macosx_11_0_arm64"
        elif architecture == "x86_64":
            return "macosx_10_15_x86_64"
        else:
            platname = f"macosx_11_0_{architecture}"
            return platname.replace("-", "_").replace(".", "_")
    # Windows
    else:
        return get_platform().replace("-", "_").replace(".", "_")


def get_python_binaries_folder() -> str:
    return os.path.basename(get_path("scripts"))


def get_python_tags(
    base: str,
    path: str,
    setup: str,
) -> tuple[str, str, str]:
    tags: dict = {}

    def check_for_spec_py(base_path: str) -> str | None:
        spec_path = os.path.join(base_path, path)
        return spec_path if os.path.isfile(spec_path) else None

    spec_py = check_for_spec_py(base)

    if not spec_py:
        root = os.path.dirname(os.path.dirname(setup))
        if os.path.basename(root) == "local_packages":
            for entry in os.listdir(root):
                entry_path = os.path.join(root, entry)
                if os.path.isdir(entry_path):
                    candidate = check_for_spec_py(entry_path)
                    if candidate:
                        spec_py = candidate
                        break

    if not spec_py:
        raise FileNotFoundError(
            f"Could not locate '{path}' in any of the expected locations: {base}."
        )

    with open(spec_py) as f:
        exec(f.read(), tags)

    return (
        tags["MIN_PYTHON_VERSION"],
        tags["PYTHON_IMPLEMENTATION"],
        tags["MIN_PYTHON_ABI"],
    )


def get_target_release_folder() -> str:
    profile = get_build_profile()
    td_target = get_td_target()

    if td_target is None:
        td_target = os.getenv("td-target", "")

    return os.path.join("target", td_target, profile)


KM_PROJECT_PRIMARY_NAME = "tabsdata-km"
KM_PROJECT_ALTERNATE_NAME = "kminus"


def resolve_km_project_relative(root: str = "") -> str | None:
    for name in (KM_PROJECT_PRIMARY_NAME, KM_PROJECT_ALTERNATE_NAME):
        candidate = os.path.join(root, "..", name)
        if os.path.isdir(candidate):
            return candidate
    return None
