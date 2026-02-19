#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


# noinspection PyBroadException
def load_console_scripts(setup_json_path: str | Path = "setup.json") -> list[str]:
    setup_json = Path(setup_json_path)
    if not setup_json.exists():
        return []

    try:
        with setup_json.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []

    scripts = []
    try:
        for entry in data.get("entry_points", {}).get("console_scripts", []):
            name = entry.get("name")
            function = entry.get("function")
            if name and function:
                scripts.append(f"{name} = {function}")
    except Exception:
        pass

    return scripts
