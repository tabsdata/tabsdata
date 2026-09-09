#
# Copyright 2026 Tabsdata Inc.
#

from pathlib import Path

import yaml

from tabsdatax.cmake.kupgrader.common.errors import KupgraderError
from tabsdatax.cmake.kupgrader.common.naming import parsable


class NormalizationError(KupgraderError):
    pass


def strip_comments(text: str) -> str:
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("#")]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(line.rstrip() for line in lines) + "\n" if lines else ""


def normalize_text(text: str) -> str:
    return strip_comments(text)


def normalize_yaml(path: Path, text: str) -> str:
    try:
        document = yaml.safe_load(text)
    except yaml.YAMLError as error:
        raise NormalizationError(f"Could not parse '{path}': {error}") from error
    if document is None:
        return ""
    return yaml.safe_dump(document, sort_keys=True, default_flow_style=False)


def normalize(family: str, path: Path) -> str:
    text = path.read_text()
    if parsable(family, path.name):
        return normalize_yaml(path, text)
    return normalize_text(text)


def equivalent(family: str, previous: Path, current: Path) -> bool:
    return normalize(family, previous) == normalize(family, current)
