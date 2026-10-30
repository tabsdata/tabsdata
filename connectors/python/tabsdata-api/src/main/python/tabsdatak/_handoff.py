#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import inspect
import sys
from collections.abc import Iterable
from typing import (
    Any,
    ForwardRef,
    TypeAliasType,
    get_args,
    get_origin,
    get_type_hints,
)

from tabsdatak.spi import (
    DestPlugin,
    LazyFrameInput,
    SrcPlugin,
    SrcTablesSpec,
    StreamStageSrcPlugin,
    TableFileSpec,
)

LEGACY_DEST_TABLES = list[TableFileSpec]


def frame_types() -> tuple[type, ...]:
    # An annotation naming a polars LazyFrame resolves only once polars is
    # imported, so the SPI never has to import it.
    polars = sys.modules.get("polars")
    if polars is None:
        return (LazyFrameInput,)
    return (polars.LazyFrame, LazyFrameInput)


def lazy_frames() -> Any:
    polars = sys.modules.get("polars")
    return None if polars is None else list[polars.LazyFrame]


def mentions(
    annotation: Any,
    family: tuple[type, ...],
) -> bool:
    if isinstance(annotation, ForwardRef):
        return mentions(annotation.__forward_arg__, family)
    if isinstance(annotation, str):
        return any(member.__name__ in annotation for member in family)
    if isinstance(annotation, TypeAliasType):
        return mentions(annotation.__value__, family)
    if isinstance(annotation, type):
        return issubclass(annotation, family)
    origin = get_origin(annotation)
    if origin is not None and mentions(origin, family):
        return True
    return any(mentions(argument, family) for argument in get_args(annotation))


def src_frames(plugin: type[SrcPlugin]) -> bool:
    """Whether `plugin.read_in` returns `LazyFrameInput`s, or the legacy
    list of local parquet files.

    Reads `read_in`'s return annotation: one naming `LazyFrameInput`
    declares frames, and must be `list[Iterable[LazyFrameInput]]`
    exactly; `list[list[TableFileInput]]`, `SrcTablesSpec`, any other
    type, or no annotation means the legacy files. Used by `SrcDef` at
    registration and by the runtime to choose the handoff.

    Raises:
        ValueError: The annotation names bare `LazyFrame`s, names
            `LazyFrameInput` in any other shape, or names frames but does
            not resolve at runtime.
    """
    method = plugin.read_in
    if method is SrcPlugin.read_in:
        return False
    try:
        returned = get_type_hints(method).get("return")
    except Exception as error:
        if mentions(method.__annotations__.get("return"), frame_types()):
            raise ValueError(
                f"{plugin.__qualname__}.read_in: the return annotation does not "
                f"resolve at runtime ({error}); import what it names at module "
                f"level",
            ) from error
        return False
    if returned == SrcTablesSpec:
        return False
    if not mentions(returned, (LazyFrameInput,)):
        if mentions(returned, frame_types()):
            raise ValueError(
                f"{plugin.__qualname__}.read_in: returns {returned!r}; return "
                f"list[Iterable[LazyFrameInput]], wrapping bare frames with "
                f"to_lazy_frame_inputs",
            )
        return False
    if not iterables_of_inputs(returned):
        raise ValueError(
            f"{plugin.__qualname__}.read_in: returns {returned!r}; annotate "
            f"read_in with -> list[Iterable[LazyFrameInput]]",
        )
    return True


def iterables_of_inputs(annotation: Any) -> bool:
    if get_origin(annotation) is not list:
        return False
    [entry] = get_args(annotation)
    return get_origin(entry) is Iterable and get_args(entry) == (LazyFrameInput,)


def deprecated_src(plugin: type[SrcPlugin]) -> bool:
    # A stream stage plugin's read_in is framework code its subclasses must
    # not override, so they could not act on the warning.
    return not src_frames(plugin) and not issubclass(
        plugin,
        StreamStageSrcPlugin,
    )


def declared_dest_tables(plugin: type[DestPlugin]) -> Any:
    for klass in plugin.__mro__:
        for base in klass.__dict__.get("__orig_bases__", ()):
            if get_origin(base) is DestPlugin:
                return get_args(base)[2]
    return LEGACY_DEST_TABLES


def dest_frames(plugin: type[DestPlugin]) -> bool:
    """Whether `plugin.write_out` receives `LazyFrame`s, or the legacy
    list of local parquet files.

    Reads the third type argument of `DestPlugin` in the plugin's bases;
    left out, it defaults to `list[TableFileSpec]`, the legacy form.
    `list[pl.LazyFrame]` must also be the annotation of `write_out`'s
    fourth parameter, exactly. Used by `DestDef` at registration and by
    the runtime to choose the handoff.

    Raises:
        ValueError: The third type argument is neither form; `write_out`
            does not take the declared form; a legacy `write_out` takes
            frames; or the annotation does not resolve at runtime.
    """
    declared = declared_dest_tables(plugin)
    frames = declared != LEGACY_DEST_TABLES
    if frames and declared != lazy_frames():
        raise ValueError(
            f"{plugin.__qualname__} declares {declared!r} as its tables; a "
            f"destination plugin declares list[pl.LazyFrame] as the third type "
            f"argument of DestPlugin, or none for the legacy files",
        )
    method = plugin.write_out
    if method is DestPlugin.write_out:
        return frames
    parameters = list(inspect.signature(method).parameters.values())
    parameter = parameters[4] if len(parameters) > 4 else None
    if parameter is None or parameter.annotation is inspect.Parameter.empty:
        if frames:
            raise ValueError(
                f"{plugin.__qualname__}.write_out: annotate its fourth "
                f"parameter with list[pl.LazyFrame]",
            )
        return False
    try:
        received = get_type_hints(method).get(parameter.name)
    except Exception as error:
        if frames or mentions(parameter.annotation, frame_types()):
            raise ValueError(
                f"{plugin.__qualname__}.write_out: the annotation of "
                f"`{parameter.name}` does not resolve at runtime ({error}); "
                f"import polars at module level",
            ) from error
        return False
    if frames:
        if received != declared:
            raise ValueError(
                f"{plugin.__qualname__}.write_out: `{parameter.name}` is typed "
                f"as {received!r}, but the plugin declares list[pl.LazyFrame]; "
                f"annotate it with list[pl.LazyFrame]",
            )
        return True
    if mentions(received, frame_types()):
        raise ValueError(
            f"{plugin.__qualname__}.write_out: `{parameter.name}` is typed as "
            f"{received!r}, but the plugin declares no frames; declare "
            f"list[pl.LazyFrame] as the third type argument of DestPlugin",
        )
    return False
