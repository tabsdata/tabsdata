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


class PluginContract[Plugin]:
    plugin: type[Plugin]

    def __init__(self, plugin: type[Plugin]) -> None:
        self.plugin = plugin

    @staticmethod
    def frame_types() -> tuple[type, ...]:
        # An annotation naming a polars LazyFrame resolves only once polars is
        # imported, so the SPI never has to import it.
        polars = sys.modules.get("polars")
        if polars is None:
            return (LazyFrameInput,)
        return (polars.LazyFrame, LazyFrameInput)

    @staticmethod
    def lazy_frames() -> Any:
        polars = sys.modules.get("polars")
        return None if polars is None else list[polars.LazyFrame]

    @staticmethod
    def mentions(
        annotation: Any,
        family: tuple[type, ...],
    ) -> bool:
        if isinstance(annotation, ForwardRef):
            return PluginContract.mentions(annotation.__forward_arg__, family)
        if isinstance(annotation, str):
            return any(member.__name__ in annotation for member in family)
        if isinstance(annotation, TypeAliasType):
            return PluginContract.mentions(annotation.__value__, family)
        if isinstance(annotation, type):
            return issubclass(annotation, family)
        origin = get_origin(annotation)
        if origin is not None and PluginContract.mentions(origin, family):
            return True
        return any(
            PluginContract.mentions(argument, family)
            for argument in get_args(annotation)
        )


# Whether a source plugin's read_in returns LazyFrameInputs or the legacy list of
# local parquet files, read from its return annotation.
class SourceContract(PluginContract[SrcPlugin]):
    def frames(self) -> bool:
        method = self.plugin.read_in
        if method is SrcPlugin.read_in:
            return False
        try:
            returned = get_type_hints(method).get("return")
        except Exception as error:
            if self.mentions(method.__annotations__.get("return"), self.frame_types()):
                raise ValueError(
                    f"{self.plugin.__qualname__}.read_in: the return annotation "
                    f"does not resolve at runtime ({error}); import what it names "
                    f"at module level",
                ) from error
            return False
        if returned == SrcTablesSpec:
            return False
        if not self.mentions(returned, (LazyFrameInput,)):
            if self.mentions(returned, self.frame_types()):
                raise ValueError(
                    f"{self.plugin.__qualname__}.read_in: returns {returned!r}; return "
                    f"list[Iterable[LazyFrameInput]], wrapping bare frames with "
                    f"to_lazy_frame_inputs",
                )
            return False
        if not self.iterables_of_inputs(returned):
            raise ValueError(
                f"{self.plugin.__qualname__}.read_in: returns {returned!r}; annotate "
                f"read_in with -> list[Iterable[LazyFrameInput]]",
            )
        return True

    @staticmethod
    def iterables_of_inputs(annotation: Any) -> bool:
        if get_origin(annotation) is not list:
            return False
        [entry] = get_args(annotation)
        return get_origin(entry) is Iterable and get_args(entry) == (LazyFrameInput,)

    def deprecated(self) -> bool:
        # A stream stage plugin's read_in is framework code its subclasses must
        # not override, so they could not act on the warning.
        return not self.frames() and not issubclass(
            self.plugin,
            StreamStageSrcPlugin,
        )


# Whether a destination plugin's write_out receives LazyFrames or the legacy list
# of local parquet files, read from DestPlugin's third type argument.
class DestinationContract(PluginContract[DestPlugin]):
    def tables(self) -> Any:
        for klass in self.plugin.__mro__:
            for base in klass.__dict__.get("__orig_bases__", ()):
                if get_origin(base) is DestPlugin:
                    return get_args(base)[2]
        return LEGACY_DEST_TABLES

    def frames(self) -> bool:
        declared = self.tables()
        frames = declared != LEGACY_DEST_TABLES
        if frames and declared != self.lazy_frames():
            raise ValueError(
                f"{self.plugin.__qualname__} declares {declared!r} as its tables; a "
                f"destination plugin declares list[pl.LazyFrame] as the third type "
                f"argument of DestPlugin, or none for the legacy files",
            )
        method = self.plugin.write_out
        if method is DestPlugin.write_out:
            return frames
        parameters = list(inspect.signature(method).parameters.values())
        parameter = parameters[4] if len(parameters) > 4 else None
        if parameter is None or parameter.annotation is inspect.Parameter.empty:
            if frames:
                raise ValueError(
                    f"{self.plugin.__qualname__}.write_out: annotate its fourth "
                    f"parameter with list[pl.LazyFrame]",
                )
            return False
        try:
            received = get_type_hints(method).get(parameter.name)
        except Exception as error:
            if frames or self.mentions(parameter.annotation, self.frame_types()):
                raise ValueError(
                    f"{self.plugin.__qualname__}.write_out: the annotation of "
                    f"`{parameter.name}` does not resolve at runtime ({error}); "
                    f"import polars at module level",
                ) from error
            return False
        if frames:
            if received != declared:
                raise ValueError(
                    f"{self.plugin.__qualname__}.write_out: `{parameter.name}` is "
                    f"typed as {received!r}, but the plugin declares "
                    f"list[pl.LazyFrame]; annotate it with list[pl.LazyFrame]",
                )
            return True
        if self.mentions(received, self.frame_types()):
            raise ValueError(
                f"{self.plugin.__qualname__}.write_out: `{parameter.name}` is typed as "
                f"{received!r}, but the plugin declares no frames; declare "
                f"list[pl.LazyFrame] as the third type argument of DestPlugin",
            )
        return False
