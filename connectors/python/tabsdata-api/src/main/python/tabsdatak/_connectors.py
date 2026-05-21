from __future__ import annotations

import importlib.metadata
import warnings
from typing import Type

from tabsdatak.api import StreamSrc, Dest, Src
from tabsdatak.spi import StreamSrcDef, DestDef, SrcDef

_ENTRY_POINT_GROUP = "tabsdatak.connectors"


# Lazy caches. Stay ``None`` until something actually triggers the
# scan; the public ``avail_*`` accessors and the internal ``_get_*``
# helpers build the indices on first access.
_SRC_BY_CLASS: dict[Type[Src], SrcDef] | None = None
_STREAM_SRC_BY_CLASS: dict[Type[StreamSrc], StreamSrcDef] | None = None
_DEST_BY_CLASS: dict[Type[Dest], DestDef] | None = None
_SRC_BY_TYPE: dict[str, SrcDef] | None = None
_STREAM_SRC_BY_TYPE: dict[str, StreamSrcDef] | None = None
_DEST_BY_TYPE: dict[str, DestDef] | None = None

# Persistent record of in-process registrations made via
# :func:`register_src` / :func:`register_stream_src` / :func:`register_dest`.
# Re-applied after every :func:`reload_connectors` so test fixtures
# (and any other dynamic registrations) survive a registry reload.
_INJECTED_SRCS: list[SrcDef] = []
_INJECTED_STREAM_SRCS: list[StreamSrcDef] = []
_INJECTED_DESTS: list[DestDef] = []


def _scan() -> None:
    global _SRC_BY_CLASS, _STREAM_SRC_BY_CLASS, _DEST_BY_CLASS
    global _SRC_BY_TYPE, _STREAM_SRC_BY_TYPE, _DEST_BY_TYPE

    src_by_cls: dict[Type[Src], SrcDef] = {}
    stream_src_by_cls: dict[Type[StreamSrc], StreamSrcDef] = {}
    dest_by_cls: dict[Type[Dest], DestDef] = {}
    src_by_type: dict[str, SrcDef] = {}
    stream_src_by_type: dict[str, StreamSrcDef] = {}
    dest_by_type: dict[str, DestDef] = {}

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

        if isinstance(obj, SrcDef):
            if obj.src in src_by_cls:
                warnings.warn(
                    f"tabsdatak.connectors: duplicate SrcDef for "
                    f"{obj.src.__qualname__} (existing wins)",
                    RuntimeWarning,
                    stacklevel=2,
                )
                continue
            src_by_cls[obj.src] = obj
            src_by_type[obj.src_type] = obj
        elif isinstance(obj, StreamSrcDef):
            if obj.src in stream_src_by_cls:
                warnings.warn(
                    f"tabsdatak.connectors: duplicate StreamSrcDef "
                    f"for {obj.src.__qualname__} (existing wins)",
                    RuntimeWarning,
                    stacklevel=2,
                )
                continue
            stream_src_by_cls[obj.src] = obj
            stream_src_by_type[obj.src_type] = obj
        elif isinstance(obj, DestDef):
            if obj.dest in dest_by_cls:
                warnings.warn(
                    f"tabsdatak.connectors: duplicate DestDef for "
                    f"{obj.dest.__qualname__} (existing wins)",
                    RuntimeWarning,
                    stacklevel=2,
                )
                continue
            dest_by_cls[obj.dest] = obj
            dest_by_type[obj.dest_type] = obj
        else:
            warnings.warn(
                f"tabsdatak.connectors: entry point {ep.name!r} "
                f"resolved to {type(obj).__name__}, expected "
                f"SrcDef / StreamSrcDef / DestDef; skipping",
                RuntimeWarning,
                stacklevel=2,
            )

    _SRC_BY_CLASS = src_by_cls
    _STREAM_SRC_BY_CLASS = stream_src_by_cls
    _DEST_BY_CLASS = dest_by_cls
    _SRC_BY_TYPE = dict(sorted(src_by_type.items()))
    _STREAM_SRC_BY_TYPE = dict(sorted(stream_src_by_type.items()))
    _DEST_BY_TYPE = dict(sorted(dest_by_type.items()))

    # Always re-apply previously-injected fixtures so they survive any
    # scan -- whether triggered by lazy ``_ensure`` or by an explicit
    # ``reload_connectors``.
    for defx in _INJECTED_SRCS:
        _SRC_BY_CLASS[defx.src] = defx
        _SRC_BY_TYPE[defx.src_type] = defx
    for defx in _INJECTED_STREAM_SRCS:
        _STREAM_SRC_BY_CLASS[defx.src] = defx
        _STREAM_SRC_BY_TYPE[defx.src_type] = defx
    for defx in _INJECTED_DESTS:
        _DEST_BY_CLASS[defx.dest] = defx
        _DEST_BY_TYPE[defx.dest_type] = defx


def _ensure() -> None:
    if _SRC_BY_CLASS is None:
        _scan()


# ---------------------------------------------------------------------------
# Public discovery surface (LLM / scripted introspection).
# ---------------------------------------------------------------------------


def avail_srcs() -> dict[str, SrcDef]:
    _ensure()
    assert _SRC_BY_TYPE is not None
    return dict(_SRC_BY_TYPE)


def avail_stream_srcs() -> dict[str, StreamSrcDef]:
    _ensure()
    assert _STREAM_SRC_BY_TYPE is not None
    return dict(_STREAM_SRC_BY_TYPE)


def avail_dests() -> dict[str, DestDef]:
    _ensure()
    assert _DEST_BY_TYPE is not None
    return dict(_DEST_BY_TYPE)


# ---------------------------------------------------------------------------
# Framework-internal lookups: resolve a concrete config class to its def.
# ---------------------------------------------------------------------------


def get_src(cls: Type[Src]) -> SrcDef:
    _ensure()
    assert _SRC_BY_CLASS is not None  # noqa: S101 -- _ensure populates
    try:
        return _SRC_BY_CLASS[cls]
    except KeyError:
        raise KeyError(
            f"no SrcDef registered for {cls.__qualname__}; install "
            f"the connector package or check its entry-point "
            f"declaration in pyproject.toml"
        ) from None


def get_stream_src(cls: Type[StreamSrc]) -> StreamSrcDef:
    _ensure()
    assert _STREAM_SRC_BY_CLASS is not None
    try:
        return _STREAM_SRC_BY_CLASS[cls]
    except KeyError:
        raise KeyError(
            f"no StreamSrcDef registered for {cls.__qualname__}; "
            f"install the connector package or check its entry-point "
            f"declaration in pyproject.toml"
        ) from None


def get_dest(cls: Type[Dest]) -> DestDef:
    _ensure()
    assert _DEST_BY_CLASS is not None
    try:
        return _DEST_BY_CLASS[cls]
    except KeyError:
        raise KeyError(
            f"no DestDef registered for {cls.__qualname__}; install "
            f"the connector package or check its entry-point "
            f"declaration in pyproject.toml"
        ) from None


# ---------------------------------------------------------------------------
# Dynamic registration (test fixtures, in-process plugins).
# ---------------------------------------------------------------------------


def register_src(defx: SrcDef) -> None:
    _ensure()
    assert _SRC_BY_CLASS is not None and _SRC_BY_TYPE is not None
    _SRC_BY_CLASS[defx.src] = defx
    _SRC_BY_TYPE[defx.src_type] = defx
    _INJECTED_SRCS.append(defx)


def register_stream_src(defx: StreamSrcDef) -> None:
    _ensure()
    assert _STREAM_SRC_BY_CLASS is not None and _STREAM_SRC_BY_TYPE is not None
    _STREAM_SRC_BY_CLASS[defx.src] = defx
    _STREAM_SRC_BY_TYPE[defx.src_type] = defx
    _INJECTED_STREAM_SRCS.append(defx)


def register_dest(defx: DestDef) -> None:
    _ensure()
    assert _DEST_BY_CLASS is not None and _DEST_BY_TYPE is not None
    _DEST_BY_CLASS[defx.dest] = defx
    _DEST_BY_TYPE[defx.dest_type] = defx
    _INJECTED_DESTS.append(defx)


def reload_connectors() -> None:
    # ``_scan`` re-applies ``_INJECTED_*`` at the end, so a reload is
    # just "clear caches + re-scan".
    global _SRC_BY_CLASS, _STREAM_SRC_BY_CLASS, _DEST_BY_CLASS
    global _SRC_BY_TYPE, _STREAM_SRC_BY_TYPE, _DEST_BY_TYPE
    _SRC_BY_CLASS = None
    _STREAM_SRC_BY_CLASS = None
    _DEST_BY_CLASS = None
    _SRC_BY_TYPE = None
    _STREAM_SRC_BY_TYPE = None
    _DEST_BY_TYPE = None
    _scan()
