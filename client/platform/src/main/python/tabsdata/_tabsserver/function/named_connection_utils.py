#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import json
import logging
import os

from tabsdata._io.connections.connections import Conn
from tabsdata._io.connections.named_connection.named_connection import NamedConn

logger = logging.getLogger(__name__)

TD_URI_REPOSITORY = "TD_URI_REPOSITORY"
CONNECTIONS_FOLDER = "connections"


def resolve_named_conn_if_needed(plugin, project_id: str) -> None:
    """Check if a plugin's ``conn`` attribute is a NamedConn and resolve it.

    If the plugin has a ``conn`` attribute that is a :class:`NamedConn`, it is
    resolved into a concrete :class:`Conn` via :func:`load_named_conn` and
    injected back into the plugin.  If ``conn`` is already a concrete
    connection or the plugin has no ``conn`` attribute, this is a no-op.

    Args:
        plugin: The source or destination plugin to inspect.
        project_id: The run's project (connection artifacts are
            project-namespaced).
    """
    conn = getattr(plugin, "conn", None)
    if isinstance(conn, NamedConn):
        logger.info(
            f"Resolving NamedConn '{conn.name}' for {plugin.__class__.__name__}"
        )
        resolved = load_named_conn(conn, project_id)
        plugin.conn = resolved
        logger.info(
            f"Resolved NamedConn '{conn.name}' to "
            f"{resolved} of type {resolved.__class__.__name__}"
        )


def load_named_conn(named_conn: NamedConn, project_id: str) -> Conn:
    """Resolve a NamedConn into a concrete Conn instance.

    Reads the connection configuration from
    ``${TD_URI_REPOSITORY}/connections/<project_id>/<name>.json`` and
    delegates to :meth:`NamedConn._load` for type resolution and env-var
    substitution.

    Args:
        named_conn: The named connection reference to resolve.
        project_id: The run's project (connection artifacts are
            project-namespaced — same-named collections in different
            projects must not collide).

    Returns:
        The resolved concrete connection configuration.

    Raises:
        ValueError: If the ``TD_URI_REPOSITORY`` environment variable is not
            set or no project is provided.
        FileNotFoundError: If the connection configuration file does not exist.
    """
    repository_uri = os.environ.get(TD_URI_REPOSITORY)
    if not repository_uri:
        raise ValueError(
            f"Environment variable '{TD_URI_REPOSITORY}' is not set. "
            f"Cannot resolve named connection '{named_conn.name}'."
        )
    if not project_id:
        raise ValueError(
            f"No project provided. Cannot resolve named connection '{named_conn.name}'."
        )

    connection_file = os.path.join(
        repository_uri,
        CONNECTIONS_FOLDER,
        project_id,
        f"{named_conn.name.lower()}.json",
    )
    logger.debug(
        f"Loading named connection '{named_conn.name}' from '{connection_file}'"
    )

    if not os.path.isfile(connection_file):
        raise FileNotFoundError(
            f"Connection configuration file '{connection_file}' not found "
            f"for named connection '{named_conn.name}'."
        )

    with open(connection_file, "r") as f:
        cx_dict = json.load(f)

    logger.debug(f"Loaded connection configuration for '{named_conn.name}'")
    return named_conn._load(cx_dict)
