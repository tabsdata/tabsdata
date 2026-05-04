import os
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

from tabsdata._io.connections.connections import Conn

ENV_PREFIX = "env:"


class ConnMode(str, Enum):
    Src = "src"
    Dest = "dest"


@dataclass(frozen=True)
class _ConnDef:
    """Schema of a connection definition document."""

    KIND: ClassVar[str] = "connectionDef"
    API_VERSION: ClassVar[str] = "1.0"

    kind: str
    apiVersion: str
    name: str
    conn_mode: ConnMode
    conn_type: str
    conn_configs: dict

    @classmethod
    def from_dict(cls, cx_dict: dict) -> "_ConnDef":
        kind = cx_dict.get("kind")
        if kind != cls.KIND:
            raise ValueError(f"Expected kind '{cls.KIND}', got '{kind}'")
        api_version = cx_dict.get("apiVersion")
        if api_version != cls.API_VERSION:
            raise ValueError(
                f"Expected apiVersion '{cls.API_VERSION}', got '{api_version}'"
            )
        return cls(
            kind=kind,
            apiVersion=api_version,
            name=cx_dict["name"],
            conn_mode=ConnMode(cx_dict["conn_mode"]),
            conn_type=cx_dict["conn_type"],
            conn_configs=cx_dict.get("conn_configs", {}),
        )


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
            cx_dict: A dictionary with the following structure:
                {
                    "kind": "connectionDef",
                    "apiVersion": "1.0",
                    "name": "<connection name>",
                    "conn_mode": "src" | "dest",
                    "conn_type": "<Conn class name, e.g. MySQLConn>",
                    "conn_configs": {
                        "<param>": "<value>",
                        "env:<param>": "<ENV_VAR_NAME>",
                        ...
                    }
                }
                Keys in conn_configs prefixed with "env:" indicate that the value
                is the name of an environment variable to resolve.
        Returns:
            The actual connection configuration corresponding to this NamedConn's name.
        """
        conn_def = _ConnDef.from_dict(cx_dict)

        if conn_def.name != self.name:
            raise ValueError(
                f"Connection dictionary name '{conn_def.name}' does not match "
                f"NamedConn name '{self.name}'"
            )

        subclasses = {sub.__name__: sub for sub in Conn.__subclasses__()}
        conn_cls = subclasses.get(conn_def.conn_type)
        if conn_cls is None:
            raise ValueError(f"Unknown connection type: '{conn_def.conn_type}'")

        resolved = _resolve_env_vars(conn_def.conn_configs)

        return conn_cls._load_from_dict(resolved)


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
