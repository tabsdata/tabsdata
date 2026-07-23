#
# Copyright 2026 Tabsdata Inc.
#

"""External entity-container naming.

A domain's descriptor name is a plain local identifier, but the entity it
creates in the *shared* external system (a database / schema / dataset) is named
to avoid collisions across concurrent CI runs and developers. By default that
name is ``d_<user>_<build>_<domain>``: ``d_`` marks it a tdx domain container,
``<user>_<build>`` (the build identity) namespaces the run.
``create --name-in-system NAME`` overrides it verbatim (no ``d_``).

The chosen name is trimmed to fit the system's max identifier length: if too
long, it is cut to ``max-4`` and given a 2-digit ``__NN`` suffix, picking the
lowest counter whose full name isn't already present in the system.
"""

from __future__ import annotations

import getpass
import os
import re

from tabsdatax.itest.tdksystems2 import constants
from tabsdatax.itest.tdksystems2.errors import TdxError

_NON_IDENT = re.compile(r"[^A-Za-z0-9_]+")
_DOMAIN_ENTITY_PREFIX = "d_"
_ENTITY_COUNTER_MAX = 100


def _ident_token(value: str) -> str:
    tok = _NON_IDENT.sub("_", value).strip("_")
    return tok or "x"


def _os_username() -> str:
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001 - no controlling user (odd CI)
        return os.environ.get("USER") or os.environ.get("USERNAME") or "user"


def build_identity() -> str:
    """``<user>_<build>`` — ``$TD_BUILD_USER`` (else OS login) and
    ``$TD_BUILD_NUMBER`` (else ``0``), each squeezed to an identifier token."""
    user = os.environ.get(constants.ENV_BUILD_USER) or _os_username()
    build = os.environ.get(constants.ENV_BUILD_NUMBER) or "0"
    return f"{_ident_token(user)}_{_ident_token(build)}"


def default_domain_prefix() -> str:
    """``d_<user>_<build>_`` — the marker plus the build identity."""
    return _DOMAIN_ENTITY_PREFIX + build_identity() + "_"


def external_name(domain: str) -> str:
    """Default entity-container name for `domain`: ``d_<user>_<build>_<domain>``."""
    return default_domain_prefix() + domain


def fit_entity_name(name: str, max_len: int, existing: set[str]) -> str:
    """Fit `name` to `max_len`. If it fits, unchanged; otherwise trim to
    ``max-4`` and append the lowest ``__NN`` whose full name isn't in `existing`
    (compared case-insensitively)."""
    if len(name) <= max_len:
        return name
    stem = name[: max_len - 4]
    taken = {e.lower() for e in existing}
    for n in range(_ENTITY_COUNTER_MAX):
        candidate = f"{stem}__{n:02d}"
        if candidate.lower() not in taken:
            return candidate
    raise TdxError(
        f"cannot fit entity name into {max_len} chars: every "
        f"'{stem}__NN' (00-{_ENTITY_COUNTER_MAX - 1:02d}) is taken"
    )


def purge_match(name: str, prefix: str | None) -> bool:
    """Whether entity `name` matches the purge prefix (case-insensitive
    ``startswith``). ``prefix is None`` -> the default ``d_<user>_<build>_``
    (this run's own containers); ``""`` -> every container; else the raw prefix."""
    eff = default_domain_prefix() if prefix is None else prefix
    return name.lower().startswith(eff.lower())
