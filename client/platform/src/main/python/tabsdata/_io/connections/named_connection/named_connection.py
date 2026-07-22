#
# Copyright 2026 Tabsdata Inc.
#

import os
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

from tabsdata._io.connections.connections import Conn
from tabsdata._io.connections.named_connection.kind_registry import (
    conn_class_for,
    split_kind,
)

ENV_PREFIX = "env:"


class ConnMode(str, Enum):
    Src = "src"
    Dest = "dest"


@dataclass(frozen=True)
class _ConnDef:
    """Schema of a connection definition document.

    The canonical shape — the same document the apiserver validates, the
    stores persist, and the workers hydrate::

        {
            "kind": "mysqlSrcConn",
            "apiVersion": "1.0",
            "name": "<connection name>",
            "conn_configs": {
                "<param>": "<value>",
                "env:<param>": "<ENV_KEY>",
                ...
            }
        }

    ``kind`` is the connector identifier (``<token><Src|Dest>Conn``); the
    old ``conn_type``/``conn_mode`` pair is derived from it via the
    [`kind_registry`](kind_registry.py).
    """

    API_VERSION: ClassVar[str] = "1.0"

    kind: str
    apiVersion: str
    name: str
    conn_configs: dict

    @classmethod
    def from_dict(cls, cx_dict: dict) -> "_ConnDef":
        kind = cx_dict.get("kind")
        if not isinstance(kind, str):
            raise ValueError(f"Expected a string 'kind', got '{kind}'")
        split_kind(kind)  # validates the shape; class resolution is lazy
        api_version = cx_dict.get("apiVersion", cls.API_VERSION)
        if api_version != cls.API_VERSION:
            raise ValueError(
                f"Expected apiVersion '{cls.API_VERSION}', got '{api_version}'"
            )
        name = cx_dict.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError("Connection document missing 'name'")
        return cls(
            kind=kind,
            apiVersion=api_version,
            name=name,
            conn_configs=cx_dict.get("conn_configs", {}),
        )

    @property
    def conn_type(self) -> str:
        """The ``Conn`` subclass name the kind maps to (e.g. ``MySQLConn``)."""
        return conn_class_for(self.kind).__name__

    @property
    def conn_mode(self) -> ConnMode:
        """The src/dest mode carried by the kind's suffix."""
        _, mode = split_kind(self.kind)
        return ConnMode(mode)


@dataclass(init=False)
class NamedConn(Conn):
    """Named connection reference.

    Required: name (str). Resolved at runtime.
    Represents a named connection configuration that can be referenced by name in source
    and destination plugins.
    """

    def __init__(self, name: str):
        """
        Initialize a NamedConn instance.

        Args:
            name: The unique name of the connection.  This name can be used to reference
                this connection configuration in source and destination plugins.
        """
        self._name = name

    @property
    def name(self) -> str:
        """
        The unique name of the connection.
        """
        return self._name

    def _load(self, cx_dict: dict) -> Conn:
        """
        Load the actual connection configuration from the provided connection
        dictionary using the name of this NamedConn.

        Args:
            cx_dict: A connection document (see :class:`_ConnDef`). Keys in
                ``conn_configs`` prefixed with ``env:`` indicate that the
                value is the name of an environment variable to resolve.
        Returns:
            The actual connection configuration corresponding to this NamedConn's name.
        """
        conn_def = _ConnDef.from_dict(cx_dict)

        if conn_def.name != self.name:
            raise ValueError(
                f"Connection dictionary name '{conn_def.name}' does not match "
                f"NamedConn name '{self.name}'"
            )

        conn_cls = conn_class_for(conn_def.kind)

        resolved = _resolve_env_vars(conn_def.conn_configs)

        return conn_cls._load_from_dict(resolved)


def collect_env_refs(configs) -> list[str]:
    """Env keys referenced via ``env:`` entries, recursively — the set the
    server checks coverage for on create/upsert."""
    refs: list[str] = []
    if isinstance(configs, dict):
        for key, value in configs.items():
            if key.startswith(ENV_PREFIX) and isinstance(value, str):
                refs.append(value)
            elif isinstance(value, dict):
                refs.extend(collect_env_refs(value))
    return refs


def secrets_from_env(document: dict) -> list[dict]:
    """The ``secrets`` payload for a dehydrated document: every referenced
    env key resolved from THIS process's environment."""
    secrets = []
    for env_key in sorted(set(collect_env_refs(document.get("conn_configs")))):
        value = os.environ.get(env_key)
        if value is None:
            raise ValueError(
                f"Environment variable '{env_key}' is referenced by the "
                "connection but not set; export it before sending the"
                " connection"
            )
        secrets.append({"key": env_key, "value": value})
    return secrets


def _resolve_env_vars(configs: dict) -> dict:
    resolved = {}
    for key, value in configs.items():
        if key.startswith(ENV_PREFIX):
            param_name = key[len(ENV_PREFIX) :]
            env_value = os.environ.get(value)
            if env_value is None:
                raise ValueError(
                    f"Environment variable '{value}' not found "
                    f"for parameter '{param_name}'"
                )
            resolved[param_name] = env_value
        elif isinstance(value, dict):
            resolved[key] = _resolve_env_vars(value)
        else:
            resolved[key] = value
    return resolved
