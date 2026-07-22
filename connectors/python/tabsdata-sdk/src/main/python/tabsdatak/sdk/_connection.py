#
# Copyright 2026 Tabsdata Inc.
#

"""Load a stored connection into a live :class:`~tabsdatak.spi.Conn`.

The apiserver stores a *dehydrated* ``connectionDef`` document (secret leaves
reduced to ``":secret:"``) and returns it, on read, alongside the secret
*names* -- never the values. To use a connection locally (e.g. an
:class:`~tabsdatak.spi.Explorer` probe) the secret values must be supplied from
the caller's environment, matching the ``SECRET_CONN__<KEY>`` convention the
worker path uses (see :func:`tabsdatak.common._json_serde.env_hydrate` and
``tabsdatak.server._tdrunner``'s ``load_connection`` for the pod-side twin).
"""

from __future__ import annotations

from tabsdatak.common._json_serde import env_hydrate
from tabsdatak.spi import Conn


def load_connection(api, name: str, *, hydrate: bool = True) -> Conn:
    """Fetch the connection stored under collection *name* and return a live
    :class:`~tabsdatak.spi.Conn`.

    *api* is an apiserver client exposing ``collection_connection_get``. When
    *hydrate* is true (the default) each dehydrated secret leaf is filled from
    its ``SECRET_CONN__<KEY>`` environment variable via
    :func:`~tabsdatak.common._json_serde.env_hydrate` -- required before an
    explorer probe, since explorers call ``Secret.value()`` (which raises on a
    dehydrated secret). Pass ``hydrate=False`` for identity/metadata use that
    never reads secret values, to avoid pulling credentials into memory.
    """
    response = api.collection_connection_get(name)
    read = response.json() if hasattr(response, "json") else response
    # The apiserver wraps every body in a ``{"data": ...}`` envelope.
    document = read["data"]["connection"]
    if hydrate:
        document = env_hydrate(document)
    return Conn.from_dict(document)
