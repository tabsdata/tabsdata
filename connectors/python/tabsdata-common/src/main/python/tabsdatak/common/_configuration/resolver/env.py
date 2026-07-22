#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import re

from tabsdatak.common._configuration.resolver.error import ResolverErrorCode

ENV_VAR_PATTERN = re.compile(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)(\?)?}")


def resolve(text: str) -> str:
    def substitute(match: re.Match[str]) -> str:
        variable = match.group(1)
        optional = match.group(2) is not None
        value = os.environ.get(variable)
        if value is None:
            if optional:
                return ""
            raise ResolverErrorCode.RESOLVER_1.exception(variable=variable)
        return value.strip()

    return ENV_VAR_PATTERN.sub(substitute, text).strip()
