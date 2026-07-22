#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import re

from tabsdatak.common._configuration.materializer.error import MaterializerErrorCode

SECRET_PLACEHOLDER_PATTERN = re.compile(r"\$\{\{secret:([^}?]+)(\?)?}}")

OPTIONAL_GROUP = 2

PATH_SEPARATOR = "."
SECRET_KEY_SEPARATOR = "__"
SECRET_ENV_PREFIX = "SECRETS__"
SEGMENT_REPLACEMENT = "_"
DIGIT_PREFIX = "_"
EMPTY_VALUE = ""


def encode_segment(segment: str) -> str:
    characters = []
    for character in segment:
        upper = character.upper()
        if (upper.isascii() and upper.isalnum()) or upper == SEGMENT_REPLACEMENT:
            characters.append(upper)
        else:
            characters.append(SEGMENT_REPLACEMENT)
    encoded = EMPTY_VALUE.join(characters)
    if encoded and encoded[0].isdigit():
        encoded = f"{DIGIT_PREFIX}{encoded}"
    return encoded


def encode_secret_variable(path: list[str]) -> str:
    for segment in path:
        if not segment:
            raise MaterializerErrorCode.MATERIALIZER_1.exception(
                path=PATH_SEPARATOR.join(path)
            )
    encoded = SECRET_KEY_SEPARATOR.join(encode_segment(segment) for segment in path)
    return f"{SECRET_ENV_PREFIX}{encoded}"


def resolve_scalar(value: str, path: list[str]) -> str:
    def substitute(match: re.Match[str]) -> str:
        optional = match.group(OPTIONAL_GROUP) is not None
        variable = encode_secret_variable(path)
        resolved = os.environ.get(variable)
        if resolved is None:
            if optional:
                return EMPTY_VALUE
            raise MaterializerErrorCode.MATERIALIZER_2.exception(
                variable=variable, path=PATH_SEPARATOR.join(path)
            )
        return resolved.strip()

    return SECRET_PLACEHOLDER_PATTERN.sub(substitute, value)


def materialize(node: object, path: list[str] | None = None) -> object:
    path = path or []
    if isinstance(node, dict):
        return {
            key: materialize(value, path + [str(key)]) for key, value in node.items()
        }
    if isinstance(node, list):
        return [
            materialize(item, path + [str(index)]) for index, item in enumerate(node)
        ]
    if isinstance(node, str):
        return resolve_scalar(node, path)
    return node
