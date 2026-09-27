#
# Copyright 2026 Tabsdata Inc.
#

import re
from pathlib import Path

import yaml

from tabsdatax.cmake.kupgrader.common.operations import Operation

PLACEHOLDERS_KEY = "placeholders"


def read_renames(renames_file: Path, version: str) -> dict[str, str]:
    if not renames_file.is_file():
        return {}
    document = yaml.safe_load(renames_file.read_text()) or {}
    return dict((document.get(version) or {}).get(PLACEHOLDERS_KEY) or {})


def placeholder(name: str) -> str:
    return f"${{{{{name}}}}}"


def renamed(text: str, renames: dict[str, str]) -> str:
    for old, new in renames.items():
        text = text.replace(placeholder(old), placeholder(new))
    return text


def rename_operations(text: str, renames: dict[str, str]) -> list[Operation]:
    return [
        Operation(
            op="modify-text-if",
            fields={
                "pattern": re.escape(placeholder(old)),
                "replacement": placeholder(new),
            },
        )
        for old, new in renames.items()
        if placeholder(old) in text
    ]
