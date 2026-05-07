#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from distlib.scripts import ScriptMaker

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

LAUNCHER_ENTRY_FUNCTION = "main"


class TabsdataScriptMaker(ScriptMaker):
    def _get_shebang(self, encoding, post_interp=b"", options=None):
        return b""


def build_launcher(
    script_name: str,
    module: str,
    target_folder: str,
) -> str:
    return build_launchers([(script_name, module)], target_folder)[0]


def build_launchers(
    specs: Sequence[tuple[str, str]],
    target_folder: str,
) -> list[str]:
    if not specs:
        return []
    os.makedirs(target_folder, exist_ok=True)
    formatted = [
        f"{script_name} = {module}:{LAUNCHER_ENTRY_FUNCTION}"
        for script_name, module in specs
    ]
    maker = TabsdataScriptMaker(source_dir=None, target_dir=target_folder)
    maker.add_launchers = False
    maker.variants = {""}
    maker.clobber = True
    return list(maker.make_multiple(formatted))
