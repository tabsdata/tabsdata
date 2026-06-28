#
# Copyright 2026 Tabs Data Inc.
#
"""Apiserver-backed :class:`ConnStore` implementation.

Connections live server-side, hanging off their source/destination
collection — the collection's name IS the connection's name, so this store
maps the name-keyed :class:`ConnStore` API straight onto the
``/collections/{name}/connection`` endpoints. The instance filesystem is
never touched: the server's own connection store (repository files in
supervisor mode, the bridge in kubernetes mode) persists the content and
produces the worker artifact.

Secret values: the document carries ``env:<param> -> ENV_KEY`` references,
and the server requires every referenced key to be covered by the upsert's
secrets (otherwise the connection could not hydrate). The values are
resolved from THIS process's environment at ``put`` time — the same
variables :class:`NamedConn._load` would need for a local validation pass.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tabsdata._cli.tdconn._internal._debug import log_call
from tabsdata._cli.tdconn._internal.os.conn_store import ConnStore
from tabsdata._io.connections.named_connection.kind_registry import split_kind
from tabsdata._io.connections.named_connection.named_connection import (
    _resolve_env_vars,
    secrets_from_env,
)
from tabsdata.api.apiserver import APIServer, APIServerError


class ApiConnStore(ConnStore):
    """Server-side store over the collection-connection endpoints."""

    def __init__(self, connection: APIServer):
        self.connection = connection

    def exists(self, name: str, debug: bool = False) -> bool:
        log_call(debug, f"ApiConnStore.exists(name={name!r})")
        response = self.connection.collection_connection_get(
            name, raise_for_status=False
        )
        return response.status_code == 200

    def put(self, name: str, source_path: Path, debug: bool = False) -> None:
        log_call(
            debug,
            f"ApiConnStore.put(name={name!r}, source_path={str(source_path)!r})",
        )
        with source_path.open("r", encoding="utf-8") as f:
            document = json.load(f)
        secrets = secrets_from_env(document)
        try:
            if self.exists(name, debug=debug):
                self.connection.collection_connection_upsert(
                    name, connection=document, secrets=secrets
                )
            else:
                # A connection is 1:1 with its source/destination collection
                # and is created WITH it; the kind's Src/Dest suffix picks the
                # group.
                _, mode = split_kind(document.get("kind", ""))
                group = "source" if mode == "src" else "destination"
                self.connection.collection_create(
                    name,
                    description=f"Connection '{name}'",
                    group=group,
                    connection=document,
                    secrets=secrets,
                )
        except APIServerError as e:
            raise ValueError(str(e)) from e

    def get(
        self,
        name: str,
        resolved_envs: bool = False,
        debug: bool = False,
    ) -> dict[str, Any]:
        log_call(
            debug,
            f"ApiConnStore.get(name={name!r}, resolved_envs={resolved_envs})",
        )
        try:
            response = self.connection.collection_connection_get(name)
        except APIServerError as e:
            raise ValueError(str(e)) from e
        cx_dict = response.json()["data"]["connection"]
        if resolved_envs and isinstance(cx_dict.get("conn_configs"), dict):
            cx_dict["conn_configs"] = _resolve_env_vars(cx_dict["conn_configs"])
        return cx_dict

    def delete(self, name: str, debug: bool = False) -> None:
        log_call(debug, f"ApiConnStore.delete(name={name!r})")
        raise ValueError(
            "a connection is deleted with its collection; use "
            f"'td collection delete --name {name}' instead"
        )

    def names(self, debug: bool = False) -> list[str]:
        log_call(debug, "ApiConnStore.names()")
        try:
            response = self.connection.collection_list(request_len=1000)
        except APIServerError as e:
            raise ValueError(str(e)) from e
        collections = response.json()["data"]["data"]
        return sorted(
            collection["name"]
            for collection in collections
            if collection.get("connection_id")
        )
