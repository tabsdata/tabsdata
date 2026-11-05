#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import inspect
import sys
from collections.abc import Iterable
from dataclasses import replace
from typing import (
    Any,
    ForwardRef,
    TypeAliasType,
    get_args,
    get_origin,
    get_type_hints,
)

import tabsdatak._api as _api
from tabsdatak.spi import (
    DestPlugin,
    FrameMetadata,
    LazyFrameInput,
    SrcPlugin,
    SrcPluginCtx,
    SrcTablesSpec,
    StreamStageSrcPlugin,
    TableFileInput,
    TableFileSpec,
)

LEGACY_DEST_TABLES = list[TableFileSpec]


class RestartRead(Exception):
    cause: Exception
    wait: float
    within: float

    def __init__(
        self,
        cause: Exception,
        *,
        wait: float,
        within: float,
    ) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.wait = wait
        self.within = within


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

    @staticmethod
    def iterables_of_inputs(annotation: Any) -> bool:
        if get_origin(annotation) is not list:
            return False
        [entry] = get_args(annotation)
        return get_origin(entry) is Iterable and get_args(entry) == (LazyFrameInput,)


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

    def deprecated(self) -> bool:
        # A stream stage plugin's read_in is framework code its subclasses must
        # not override, so they could not act on the warning.
        return not self.frames() and not issubclass(
            self.plugin,
            StreamStageSrcPlugin,
        )


# Whether a destination plugin's write_out receives LazyFrames or the legacy list
# of local parquet files, read from DestPlugin's third type argument.
# Whether a stream stage source plugin's process_files takes the staged tables as
# LazyFrameInputs or the legacy local parquet files, read from the annotation of
# its files parameter; the base hook takes frames.
class StageContract(PluginContract[StreamStageSrcPlugin]):
    def frames(self) -> bool:
        method = self.plugin.process_files
        if method is StreamStageSrcPlugin.process_files:
            return True
        try:
            files = get_type_hints(method).get("files")
        except Exception as error:
            if self.mentions(
                method.__annotations__.get("files"),
                self.frame_types(),
            ):
                raise ValueError(
                    f"{self.plugin.__qualname__}.process_files: the files "
                    f"annotation does not resolve at runtime ({error}); import "
                    f"what it names at module level",
                ) from error
            return False
        if not self.mentions(files, (LazyFrameInput,)):
            if self.mentions(files, self.frame_types()):
                raise ValueError(
                    f"{self.plugin.__qualname__}.process_files: takes {files!r}; "
                    f"take list[Iterable[LazyFrameInput]]",
                )
            return False
        if not self.iterables_of_inputs(files):
            raise ValueError(
                f"{self.plugin.__qualname__}.process_files: takes {files!r}; "
                f"annotate files as list[Iterable[LazyFrameInput]]",
            )
        return True

    def process(
        self,
        plugin: StreamStageSrcPlugin,
        ctx: SrcPluginCtx,
        tables: list[Iterable[LazyFrameInput]],
    ) -> list[Iterable[LazyFrameInput]]:
        if self.frames():
            processed = plugin.process_files(ctx, tables)
            return [
                self.checked(position, slot, LazyFrameInput)
                for position, slot in enumerate(processed)
            ]
        _api.warn_legacy_stage_files(self.plugin)
        files = self.copies(ctx, tables)
        processed = plugin.process_files(ctx, files)  # type: ignore[arg-type]
        return [
            [
                LazyFrameInput(
                    scan_parquet(file.file),
                    metadata=FrameMetadata(columns=file.metadata),
                )
                for file in self.checked(position, slot, TableFileInput)
            ]
            for position, slot in enumerate(processed)
        ]

    def checked[Table](
        self,
        position: int,
        slot: Iterable[Any],
        table_type: type[Table],
    ) -> list[Table]:
        tables = list(slot)
        for table in tables:
            if not isinstance(table, table_type):
                raise TypeError(
                    f"{self.plugin.__qualname__}.process_files returned a "
                    f"{type(table).__name__} for slot {position}; it returns "
                    f"{table_type.__name__}s",
                )
        return tables

    def copies(
        self,
        ctx: SrcPluginCtx,
        tables: list[Iterable[LazyFrameInput]],
    ) -> list[list[TableFileInput]]:
        files: list[list[TableFileInput]] = []
        for position, slot in enumerate(tables):
            folder = ctx.work_dir / "_stream_stage" / f"slot{position:04d}"
            folder.mkdir(parents=True, exist_ok=True)
            copied: list[TableFileInput] = []
            for index, table in enumerate(slot):
                file = folder / f"{index:04d}.parquet"
                table.frame.sink_parquet(file)
                metadata = table.metadata
                if callable(metadata):
                    metadata = metadata()
                if metadata is None:
                    metadata = FrameMetadata()
                if replace(metadata, columns=None) != FrameMetadata():
                    raise TypeError(
                        f"{self.plugin.__qualname__}.process_files takes the "
                        f"legacy list[list[TableFileInput]], which cannot carry "
                        f"how table {index} of slot {position} is served; take "
                        f"list[Iterable[LazyFrameInput]]",
                    )
                copied.append(TableFileInput(file=file, metadata=metadata.columns))
            files.append(copied)
        return files


def scan_parquet(file: Any) -> Any:
    import polars as pl

    return pl.scan_parquet(file)


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
