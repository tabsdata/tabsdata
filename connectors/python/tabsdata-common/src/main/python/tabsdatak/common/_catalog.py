#
# Copyright 2026 Tabsdata Inc.
#

"""Canonical form and fingerprint of a connection's entity catalog.

The server stores ``schema_hash`` next to a connection's catalog and compares
it to tell a drifted schema from an unchanged one, so the algorithm is part of
the catalog contract: a reordering of entities or columns must not change it.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tabsdatak.spi import Entity


VOLATILE_KEYS = ("size", "last_modified", "schema_from")


def canonical_entity(entity: Entity) -> dict:
    """``entity.to_dict()`` without the :data:`VOLATILE_KEYS` (a file rewritten
    with the same schema is no drift, nor is its siblings' ``schema_from``
    moving to it as the newest), columns sorted by name (extraction order
    varies)."""
    d = {k: v for k, v in entity.to_dict().items() if k not in VOLATILE_KEYS}
    cols = d.get("columns")
    if isinstance(cols, list):
        d["columns"] = sorted(
            cols,
            key=lambda c: c.get("name", "") if isinstance(c, dict) else str(c),
        )
    return d


def schema_fingerprint(entities: list[Entity]) -> str:
    """Reproducible, order-independent sha256 over a connection's catalog structure.

    Hashes ``to_dict()`` output, so a field a connector omits from it (such
    as column ``nullable``) is not part of the fingerprint.
    """
    canonical = sorted(
        (canonical_entity(e) for e in entities),
        key=lambda d: d.get("fqn", ""),
    )
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()
