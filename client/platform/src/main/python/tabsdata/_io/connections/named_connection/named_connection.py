import os
from dataclasses import dataclass

from tabsdata._io.connections.connections import Conn

ENV_PREFIX = "env:"


@dataclass(init=False)
class NamedConn(Conn):
    """
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
                    "label": "<connection name>",
                    "type": "<Conn class name, e.g. MySQLConn>",
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
        label = cx_dict["name"]
        if label != self.name:
            raise ValueError(
                f"Connection dictionary label '{label}' does not match "
                f"NamedConn name '{self.name}'"
            )

        conn_type = cx_dict["type"]
        subclasses = {sub.__name__: sub for sub in Conn.__subclasses__()}
        conn_cls = subclasses.get(conn_type)
        if conn_cls is None:
            raise ValueError(f"Unknown connection type: '{conn_type}'")

        conn_configs = cx_dict.get("conn_configs", {})
        resolved = _resolve_env_vars(conn_configs)

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
