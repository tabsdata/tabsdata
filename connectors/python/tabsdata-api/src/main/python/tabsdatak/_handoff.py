#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import inspect
from typing import Any, get_args, get_origin, get_type_hints

from tabsdatak.spi import (
    DestBus,
    DestBusOfArrow,
    DestBusOfDataFrame,
    DestBusOfFile,
    DestBusOfLazyFrame,
    DestChannel,
    DestPlugin,
    SrcBus,
    SrcBusOfArrow,
    SrcBusOfDataFrame,
    SrcBusOfFile,
    SrcBusOfLazyFrame,
    SrcChannel,
    SrcPlugin,
    SrcTablesSpec,
)

# The `SrcBus` variants a plugin may declare, in order of preference.
SRC_BUSES: tuple[type[SrcBus], ...] = (
    SrcBusOfLazyFrame,
    SrcBusOfDataFrame,
    SrcBusOfArrow,
    SrcBusOfFile,
)

# The `DestBus` variants a plugin may declare, in order of preference.
DEST_BUSES: tuple[type[DestBus], ...] = (
    DestBusOfLazyFrame,
    DestBusOfDataFrame,
    DestBusOfArrow,
    DestBusOfFile,
)


def src_bus_names() -> str:
    return ", ".join(bus.__name__ for bus in SRC_BUSES)


def dest_bus_names() -> str:
    return ", ".join(bus.__name__ for bus in DEST_BUSES)


def mentions_bus(
    annotation: Any,
    family: tuple[type, ...],
) -> bool:
    if isinstance(annotation, str):
        return any(member.__name__ in annotation for member in family)
    if isinstance(annotation, type):
        return issubclass(annotation, family)
    origin = get_origin(annotation)
    if origin is not None and mentions_bus(origin, family):
        return True
    return any(mentions_bus(argument, family) for argument in get_args(annotation))


def declared_src_bus(plugin: type[SrcPlugin]) -> Any:
    for klass in plugin.__mro__:
        for base in klass.__dict__.get("__orig_bases__", ()):
            if get_origin(base) is SrcPlugin:
                return get_args(base)[2]
    return SrcBus


def src_handoff(plugin: type[SrcPlugin]) -> type[SrcBus] | None:
    """The `SrcBus` variant `plugin` writes into, or `None` when its
    `read_in` returns the legacy list of local parquet files.

    Reads the third type argument of `SrcPlugin` in the plugin's bases;
    left out, it defaults to `SrcBus`, the legacy form. A declared
    variant must also be `read_in`'s return annotation, exactly. Used by
    `SrcDef` at registration and by the runtime to choose the handoff.

    Raises:
        ValueError: The third type argument is not one of the `SrcBus`
            variants; `read_in` does not return the declared variant;
            a legacy `read_in` returns a `SrcBus`; or the return
            annotation does not resolve at runtime.
    """
    declared = declared_src_bus(plugin)
    if declared is not SrcBus and declared not in SRC_BUSES:
        raise ValueError(
            f"{plugin.__qualname__} declares {declared!r} as its bus; a source "
            f"plugin declares one of {src_bus_names()} (in order of "
            f"preference) as the third type argument of SrcPlugin, or none "
            f"for the legacy files",
        )
    variant = None if declared is SrcBus else declared
    method = plugin.read_in
    if method is SrcPlugin.read_in:
        return variant
    family = (SrcBus, SrcChannel)
    try:
        returned = get_type_hints(method).get("return")
    except Exception as error:
        if variant is not None or mentions_bus(
            method.__annotations__.get("return"),
            family,
        ):
            raise ValueError(
                f"{plugin.__qualname__}.read_in: the return annotation does not "
                f"resolve at runtime ({error}); import the SrcBus variant at "
                f"module level",
            ) from error
        return None
    if variant is not None:
        if returned is not variant:
            raise ValueError(
                f"{plugin.__qualname__}.read_in: returns {returned!r}, but the "
                f"plugin declares {variant.__name__}; annotate read_in with "
                f"-> {variant.__name__}",
            )
        return variant
    if returned != SrcTablesSpec and mentions_bus(returned, family):
        raise ValueError(
            f"{plugin.__qualname__}.read_in: returns {returned!r}, but the "
            f"plugin declares no SrcBus variant; declare it as the third type "
            f"argument of SrcPlugin",
        )
    return None


def dest_handoff(plugin: type[DestPlugin]) -> type[DestBus] | None:
    """The `DestBus` variant `plugin.write_out` receives, or `None` when
    it receives the legacy list of local parquet files.

    Reads the annotation of `write_out`'s fourth parameter, the data the
    runtime passes after `ctx`, `conn` and `dest`. `list[TableFileSpec]`,
    any other type unrelated to `DestBus`, or no annotation means the
    legacy file list. Used by `DestDef` at registration and by the
    runtime to choose the handoff.

    Raises:
        ValueError: The annotation names `DestBus` or `DestChannel` but
            is not exactly one of the `DestBus` variants, or it does not
            resolve at runtime.
    """
    method = plugin.write_out
    if method is DestPlugin.write_out:
        return None
    parameters = list(inspect.signature(method).parameters.values())
    if len(parameters) < 5 or parameters[4].kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        return None
    parameter = parameters[4]
    if parameter.annotation is inspect.Parameter.empty:
        return None
    family = (DestBus, DestChannel)
    try:
        declared = get_type_hints(method).get(parameter.name)
    except Exception as error:
        if mentions_bus(parameter.annotation, family):
            raise ValueError(
                f"{plugin.__qualname__}.write_out: the annotation of "
                f"`{parameter.name}` does not resolve at runtime ({error}); "
                f"import the DestBus variant at module level",
            ) from error
        return None
    if declared in DEST_BUSES:
        return declared
    if mentions_bus(declared, family):
        raise ValueError(
            f"{plugin.__qualname__}.write_out: `{parameter.name}` is typed as "
            f"{declared!r}; a destination plugin declares one of "
            f"{dest_bus_names()} (in order of preference), or the legacy "
            f"list[TableFileSpec]",
        )
    return None
