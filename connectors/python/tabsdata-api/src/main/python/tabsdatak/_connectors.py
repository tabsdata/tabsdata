"""Connector registry built from the ``tabsdatak.connectors``
entry-point group.

On first lookup, :func:`_scan` walks every installed entry point,
loads the registration dataclass (:class:`~tabsdatak.spi.SrcDef` /
:class:`~tabsdatak.spi.StreamSrcDef` / :class:`~tabsdatak.spi.DestDef`),
and indexes it by its wire-stable ``type_`` identifier. ``type_`` is
unique across *all* plugins -- batch sources, stream sources and
destinations share a single namespace -- so one dictionary keyed by
``type_`` holds every binding. Duplicates warn-and-skip; the first
registration wins.

Per-process in-memory registrations made via the ``register_*``
hooks are tracked separately so they survive
:func:`reload_connectors` -- useful for tests that inject ad-hoc
bindings without publishing a package.
"""

from __future__ import annotations

import importlib.metadata
import warnings
from typing import Type, Union

from tabsdatak.api import Dest, Src, StreamSrc
from tabsdatak.spi import Conn, DestDef, Explorer, SrcDef, StreamSrcDef

_ENTRY_POINT_GROUP = "tabsdatak.connectors"

# A registration is one of the three Def dataclasses; they all share a
# single ``type_`` namespace.
_Def = Union[SrcDef, StreamSrcDef, DestDef]


# Lazy cache. Stays ``None`` until something actually triggers the
# scan; the public ``avail_*`` accessors and the internal ``get_*``
# helpers build the index on first access. Keyed by the binding's
# wire-stable, globally-unique ``type_``.
_REGISTRY: dict[str, _Def] | None = None

# Persistent record of in-process registrations made via
# :func:`register_src` / :func:`register_stream_src` / :func:`register_dest`.
# Re-applied after every :func:`reload_connectors` so test fixtures
# (and any other dynamic registrations) survive a registry reload.
_INJECTED: list[_Def] = []


def _scan() -> None:
    global _REGISTRY

    registry: dict[str, _Def] = {}

    for ep in importlib.metadata.entry_points(group=_ENTRY_POINT_GROUP):
        try:
            obj = ep.load()
        except Exception as e:  # noqa: BLE001 -- 3p imports may raise anything
            warnings.warn(
                f"tabsdatak.connectors: failed to load entry point "
                f"{ep.name!r} ({ep.value!r}): {e!r}",
                RuntimeWarning,
                stacklevel=2,
            )
            continue

        if not isinstance(obj, (SrcDef, StreamSrcDef, DestDef)):
            warnings.warn(
                f"tabsdatak.connectors: entry point {ep.name!r} "
                f"resolved to {type(obj).__name__}, expected "
                f"SrcDef / StreamSrcDef / DestDef; skipping",
                RuntimeWarning,
                stacklevel=2,
            )
            continue

        if obj.type_ in registry:
            warnings.warn(
                f"tabsdatak.connectors: duplicate connector type "
                f"{obj.type_!r} (existing wins)",
                RuntimeWarning,
                stacklevel=2,
            )
            continue

        registry[obj.type_] = obj

    _REGISTRY = dict(sorted(registry.items()))

    # Always re-apply previously-injected fixtures so they survive any
    # scan -- whether triggered by lazy ``_ensure`` or by an explicit
    # ``reload_connectors``.
    for defx in _INJECTED:
        _REGISTRY[defx.type_] = defx


def _ensure() -> None:
    if _REGISTRY is None:
        _scan()


def _registry() -> dict[str, _Def]:
    _ensure()
    assert _REGISTRY is not None  # noqa: S101 -- _ensure populates
    return _REGISTRY


# ---------------------------------------------------------------------------
# Public discovery surface (LLM / scripted introspection).
# ---------------------------------------------------------------------------


def avail_srcs() -> dict[str, SrcDef]:
    """Snapshot of registered batch sources, keyed by ``type_``."""
    return {k: v for k, v in _registry().items() if isinstance(v, SrcDef)}


def avail_stream_srcs() -> dict[str, StreamSrcDef]:
    """Snapshot of registered stream sources, keyed by ``type_``."""
    return {k: v for k, v in _registry().items() if isinstance(v, StreamSrcDef)}


def avail_dests() -> dict[str, DestDef]:
    """Snapshot of registered destinations, keyed by ``type_``."""
    return {k: v for k, v in _registry().items() if isinstance(v, DestDef)}


def def_for(type_: str) -> _Def | None:
    """Return the connector def registered under ``type_`` (across
    sources, stream sources and destinations), or ``None`` if unknown."""
    return _registry().get(type_)


def explorer_for(type_: str) -> type[Explorer] | None:
    """Return the :class:`~tabsdatak.spi.Explorer` class for ``type_``,
    or ``None`` if the type is unregistered or wires no explorer."""
    defx = _registry().get(type_)
    return defx.explorer if defx is not None else None


def explorer_for_conn(conn: Conn | type[Conn]) -> type[Explorer] | None:
    """Return the :class:`~tabsdatak.spi.Explorer` class bound to ``conn``'s
    connector, or ``None`` if none is registered.

    Resolves by the connection's concrete :class:`~tabsdatak.spi.Conn` class
    rather than the wire ``type_``: a caller that deserialized a stored
    ``connectionDef`` document has the ``Conn`` in hand but not the ``type_``.
    A ``Conn`` subclass may back several registry entries (e.g. Salesforce's
    SOSQL and report sources) -- they share one explorer, so the first
    registered def whose ``conn`` matches and wires an explorer wins."""
    cls = conn if isinstance(conn, type) else type(conn)
    for defx in _registry().values():
        if defx.conn is cls and defx.explorer is not None:
            return defx.explorer
    return None


# ---------------------------------------------------------------------------
# Framework-internal lookups: resolve a concrete config class to its def.
# ---------------------------------------------------------------------------


def get_src(cls: Type[Src]) -> SrcDef:
    """Resolve a concrete :class:`Src` subclass to its registered
    :class:`SrcDef`. Raises ``KeyError`` if no binding exists."""
    for defx in _registry().values():
        if isinstance(defx, SrcDef) and defx.src is cls:
            return defx
    raise KeyError(
        f"no SrcDef registered for {cls.__qualname__}; install "
        f"the connector package or check its entry-point "
        f"declaration in pyproject.toml"
    )


def get_stream_src(cls: Type[StreamSrc]) -> StreamSrcDef:
    """Resolve a concrete :class:`StreamSrc` subclass to its
    registered :class:`StreamSrcDef`. Raises ``KeyError`` if no
    binding exists."""
    for defx in _registry().values():
        if isinstance(defx, StreamSrcDef) and defx.src is cls:
            return defx
    raise KeyError(
        f"no StreamSrcDef registered for {cls.__qualname__}; "
        f"install the connector package or check its entry-point "
        f"declaration in pyproject.toml"
    )


def get_dest(cls: Type[Dest]) -> DestDef:
    """Resolve a concrete :class:`Dest` subclass to its registered
    :class:`DestDef`. Raises ``KeyError`` if no binding exists."""
    for defx in _registry().values():
        if isinstance(defx, DestDef) and defx.dest is cls:
            return defx
    raise KeyError(
        f"no DestDef registered for {cls.__qualname__}; install "
        f"the connector package or check its entry-point "
        f"declaration in pyproject.toml"
    )


# ---------------------------------------------------------------------------
# Dynamic registration (test fixtures, in-process plugins).
# ---------------------------------------------------------------------------


def register_src(defx: SrcDef) -> None:
    """Inject a :class:`SrcDef` at runtime (test fixtures,
    in-process plugins). Survives :func:`reload_connectors`."""
    _registry()[defx.type_] = defx
    _INJECTED.append(defx)


def register_stream_src(defx: StreamSrcDef) -> None:
    """Inject a :class:`StreamSrcDef` at runtime. Survives
    :func:`reload_connectors`."""
    _registry()[defx.type_] = defx
    _INJECTED.append(defx)


def register_dest(defx: DestDef) -> None:
    """Inject a :class:`DestDef` at runtime. Survives
    :func:`reload_connectors`."""
    _registry()[defx.type_] = defx
    _INJECTED.append(defx)


def reload_connectors() -> None:
    """Drop the cached registry and rescan entry points. Any
    in-process ``register_*`` injections are reapplied."""
    # ``_scan`` re-applies ``_INJECTED`` at the end, so a reload is
    # just "clear cache + re-scan".
    global _REGISTRY
    _REGISTRY = None
    _scan()
