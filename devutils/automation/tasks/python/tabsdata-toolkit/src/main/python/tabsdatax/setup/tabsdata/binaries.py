#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from collections.abc import Sequence

from tabsdatax.setup.common.platform import get_python_binaries_folder

logger = logging.getLogger(__name__)


def detect_binaries(
    base_binaries: Sequence[str],
    target_release_folder: str,
    require_server_binaries: bool = False,
) -> tuple[list[str], list[str]]:
    existing_binaries = [
        binary
        for base in base_binaries
        for binary in (base, f"{base}.exe")
        if os.path.exists(os.path.join(target_release_folder, binary))
    ]

    missing_binaries = [
        base
        for base in base_binaries
        if not any(
            os.path.exists(os.path.join(target_release_folder, binary))
            for binary in (base, f"{base}.exe")
        )
    ]

    if missing_binaries and require_server_binaries:
        raise FileNotFoundError(
            "The following binaries are missing in "
            f"{target_release_folder}: {', '.join(missing_binaries)}"
        )

    return existing_binaries, missing_binaries


def create_datafiles(
    binaries: Sequence[str],
    target: str,
) -> list[tuple[str, list[str]]]:
    datafiles = [
        (
            get_python_binaries_folder(),
            [os.path.join(target, binary) for binary in binaries],
        )
    ]
    logger.debug(f"Including tabsdata binaries: {datafiles}")
    return datafiles
