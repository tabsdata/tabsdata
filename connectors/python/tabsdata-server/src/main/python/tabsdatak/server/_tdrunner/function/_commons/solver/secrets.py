#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
from collections.abc import Mapping, MutableMapping

PREFIX = "SECRET_PRJ__"
SEPARATOR = "__"
REFERENCE_SEPARATOR = "."

logger = logging.getLogger(__name__)


class ProjectSecret:
    def __init__(self, secrets: Mapping[str, Mapping[str, str]]):
        self._secrets = {name: dict(entries) for name, entries in secrets.items()}

    @property
    def names(self) -> list[str]:
        return list(self._secrets)

    def keys(self, name: str) -> list[str]:
        return list(self._entries(name))

    def value(self, name: str, key: str | None = None) -> str:
        if key is None:
            name, separator, key = name.partition(REFERENCE_SEPARATOR)
            if not separator:
                raise KeyError(
                    f"The secret reference '{name}' has no key; "
                    f"use '{name}{REFERENCE_SEPARATOR}<key>' or give the key apart."
                )
        entries = self._entries(name)
        found = _ignoring_case(entries, key)
        if found is None:
            raise KeyError(
                f"The project secret '{name}' has no key '{key}'. "
                f"Available keys: {', '.join(entries) or 'none'}."
            )
        return entries[found]

    def _entries(self, name: str) -> dict[str, str]:
        found = _ignoring_case(self._secrets, name)
        if found is None:
            raise KeyError(
                f"The project has no secret named '{name}'. "
                f"Available secrets: {', '.join(self._secrets) or 'none'}."
            )
        return self._secrets[found]

    def __repr__(self) -> str:
        return f"ProjectSecret(names={self.names})"


def _ignoring_case(candidates: Mapping[str, object], wanted: str) -> str | None:
    lowered = wanted.lower()
    return next(
        (candidate for candidate in candidates if candidate.lower() == lowered), None
    )


def take_project_secret(
    environment: MutableMapping[str, str] = os.environ,
) -> ProjectSecret:
    secrets: dict[str, dict[str, str]] = {}
    for variable in [name for name in environment if name.upper().startswith(PREFIX)]:
        value = environment.pop(variable)
        name, separator, key = variable[len(PREFIX) :].partition(SEPARATOR)
        if not name or not separator or not key:
            logger.warning(
                "Ignoring the malformed project secret variable '%s'", variable
            )
            continue
        name = _ignoring_case(secrets, name) or name
        secrets.setdefault(name, {})[key] = value
    return ProjectSecret(secrets)
