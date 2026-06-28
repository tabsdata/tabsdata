#
# Copyright 2026 Tabs Data Inc.
#
"""The canonical connection-document ``kind`` convention.

A connection document's top-level ``kind`` (e.g. ``mysqlSrcConn``) is THE
connector identifier product-wide: the server pins it per connection
(immutable across upserts) and the stores key content under it. Shape:
``<token><Src|Dest>Conn`` — the token is the ``Conn`` subclass name minus the
``Conn`` suffix, lowercased (``MySQLConn`` → ``mysql``, ``Db2CdcConn`` →
``db2cdc``), and the ``Src``/``Dest`` suffix carries what ``conn_mode`` used
to. Derivation instead of a static table keeps separately-distributed
connector packages (``tabsdata.connector.*``) registered for free: any loaded
``Conn`` subclass resolves, exactly like the class-name lookup it replaces.
"""

from __future__ import annotations

from tabsdata._io.connections.connections import Conn

_MODE_TO_SUFFIX = {"src": "Src", "dest": "Dest"}
_SUFFIX_TO_MODE = {v: k for k, v in _MODE_TO_SUFFIX.items()}


def split_kind(kind: str) -> tuple[str, str]:
    """Split a ``kind`` into ``(token, mode)`` syntactically.

    ``mode`` is ``"src"`` or ``"dest"``. Raises ``ValueError`` when the value
    is not shaped ``<token><Src|Dest>Conn``.
    """
    if isinstance(kind, str) and kind.endswith("Conn"):
        stem = kind[: -len("Conn")]
        for suffix, mode in _SUFFIX_TO_MODE.items():
            if stem.endswith(suffix) and len(stem) > len(suffix):
                return stem[: -len(suffix)], mode
    raise ValueError(
        f"Invalid connection kind {kind!r}; expected '<connector><Src|Dest>Conn'"
        " (e.g. 'mysqlSrcConn')"
    )


def conn_class_for(kind: str) -> type[Conn]:
    """Resolve a ``kind`` to the loaded ``Conn`` subclass it names.

    Raises ``ValueError`` when no loaded subclass matches the token — the
    same failure surface the old class-name lookup had for unknown types.
    """
    token, _ = split_kind(kind)
    for sub in _subclasses_of(Conn):
        if _token_of(sub.__name__) == token:
            return sub
    raise ValueError(f"Unknown connection kind: '{kind}'")


def kind_for(conn_type: str, mode: str) -> str:
    """Compose the ``kind`` for a ``Conn`` subclass name and a mode."""
    suffix = _MODE_TO_SUFFIX.get(mode)
    if suffix is None:
        raise ValueError(f"Unknown connection mode {mode!r}; expected 'src' or 'dest'")
    return f"{_token_of(conn_type)}{suffix}Conn"


def _token_of(conn_type: str) -> str:
    stem = conn_type[: -len("Conn")] if conn_type.endswith("Conn") else conn_type
    return stem.lower()


def _subclasses_of(cls: type) -> list[type]:
    result = []
    for sub in cls.__subclasses__():
        result.append(sub)
        result.extend(_subclasses_of(sub))
    return result
