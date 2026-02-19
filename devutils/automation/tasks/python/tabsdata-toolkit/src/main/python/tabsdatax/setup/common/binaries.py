#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging

from tabsdatax.setup.common.platform import get_python_binaries_folder

logger = logging.getLogger(__name__)


def create_empty_datafiles() -> list[tuple[str, list[str]]]:
    return [(get_python_binaries_folder(), [])]
