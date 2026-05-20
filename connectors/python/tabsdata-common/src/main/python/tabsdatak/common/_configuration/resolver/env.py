#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import os
import re

ENV_VAR_PATTERN = re.compile(r"\$\{env:(\w+)(\?)?}")


def resolve(text: str) -> str:
    current = text
    while True:
        match = ENV_VAR_PATTERN.search(current)
        if match is None:
            break
        placeholder = match.group(0)
        variable = match.group(1)
        optional = match.group(2) is not None
        value = os.environ.get(variable)
        if value is None:
            if optional:
                value = ""
            else:
                raise ValueError(f"Environment variable '{variable}' not found")
        else:
            value = value.strip()
        current = current.replace(placeholder, value)
    return current.strip()
