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
)

# The `DestBus` variants a plugin may declare, in order of preference.
DEST_BUSES: tuple[type[DestBus], ...] = (
    DestBusOfLazyFrame,
    DestBusOfDataFrame,
    DestBusOfArrow,
    DestBusOfFile,
)


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
