#
#  Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os

from tabsdatax.setup.tabsdata.parser import (
    find_decorator_call,
    get_decorator_string_keyword,
    parse_python_file,
)

logger = logging.getLogger(__name__)

COMMAND_DECORATOR = "tabsdata_command"
COMMAND_ENTRY_FUNCTION = "main"
COMMAND_NAME_KEYWORD = "name"


def path_to_module(path: str, source_root: str) -> str | None:
    absolute_path = os.path.abspath(path)
    absolute_source_root = os.path.abspath(source_root)
    if not absolute_path.startswith(absolute_source_root + os.sep):
        return None
    relative = os.path.relpath(absolute_path, absolute_source_root)
    if not relative.endswith(".py"):
        return None
    parts = relative[: -len(".py")].split(os.sep)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    if not parts:
        return None
    return ".".join(parts)


def discover_tabsdata_commands(
    commands_folder: str,
    source_root: str,
) -> list[tuple[str, str]]:
    discovered: list[tuple[str, str]] = []
    for root, _, files in os.walk(commands_folder):
        for name in sorted(files):
            if not name.endswith(".py"):
                continue
            path = os.path.join(root, name)
            tree = parse_python_file(path)
            if tree is None:
                continue
            decorator = find_decorator_call(
                tree,
                COMMAND_ENTRY_FUNCTION,
                COMMAND_DECORATOR,
            )
            if decorator is None:
                continue
            module = path_to_module(path, source_root)
            if module is None:
                continue
            override = get_decorator_string_keyword(decorator, COMMAND_NAME_KEYWORD)
            default_name = (
                os.path.basename(root) if name == "__init__.py" else name[: -len(".py")]
            )
            script_name = override or default_name
            discovered.append((script_name, module))
    return sorted(discovered)
