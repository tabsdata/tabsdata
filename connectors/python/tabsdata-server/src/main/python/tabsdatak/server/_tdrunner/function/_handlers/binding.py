#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import inspect
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from tabsdatak._connectors import get_src, get_stream_src
from tabsdatak._info import PublisherInfo, StreamPublisherInfo, _unwrap_info
from tabsdatak.api import Src
from tabsdatak.common._storage.transfer import (
    StorageLocation,
    StoreKind,
    TransferError,
)
from tabsdatak.server._tdrunner.function._commons.document.connection import (
    ConnectionDocument,
)
from tabsdatak.server._tdrunner.function._commons.document.message.request import (
    Location,
)
from tabsdatak.spi import Conn, SrcPlugin

Publisher = PublisherInfo | StreamPublisherInfo


class UnknownSourceError(Exception):
    source_type: str
    detail: str

    def __init__(self, source_type: str, detail: str) -> None:
        super().__init__(f"No source plugin is registered for {source_type}: {detail}")
        self.source_type = source_type
        self.detail = detail


class UnsupportedStageError(Exception):
    uri: str
    scheme: str

    def __init__(self, uri: str, scheme: str) -> None:
        super().__init__(f"Stream stage {uri} has the unsupported scheme {scheme!r}")
        self.uri = uri
        self.scheme = scheme


class StageConnection(Conn):
    location: StorageLocation

    def __init__(self, location: StorageLocation) -> None:
        self.location = location

    @classmethod
    def of(cls, function_stage: Location) -> StageConnection:
        try:
            StoreKind.of(function_stage.uri)
        except TransferError as error:
            scheme = urlparse(function_stage.uri).scheme
            raise UnsupportedStageError(function_stage.uri, scheme) from error
        return cls(function_stage.storage_location())


@dataclass(frozen=True)
class SourceBinding:
    plugin: SrcPlugin
    connection: Conn
    source: Src
    cardinality: int

    @classmethod
    def of(
        cls,
        publisher: Publisher,
        function_stage: Location,
        connection_file: Path,
    ) -> SourceBinding:
        if isinstance(publisher, StreamPublisherInfo):
            stream_type = type(publisher.source)
            try:
                stream_definition = get_stream_src(stream_type)
            except KeyError as error:
                raise UnknownSourceError(stream_type.__name__, str(error)) from error
            stage_source_type = stream_definition.publisher_src
            try:
                definition = get_src(stage_source_type)
            except KeyError as error:
                raise UnknownSourceError(
                    stage_source_type.__name__,
                    str(error),
                ) from error
            stage_source = stage_source_type()
            return cls(
                plugin=definition.plugin(),
                connection=StageConnection.of(function_stage),
                source=stage_source,
                cardinality=definition.cardinality(stage_source),
            )
        source_type = type(publisher.source)
        try:
            definition = get_src(source_type)
        except KeyError as error:
            raise UnknownSourceError(source_type.__name__, str(error)) from error
        return cls(
            plugin=definition.plugin(),
            connection=ConnectionDocument(connection_file).load(),
            source=publisher.source,
            cardinality=definition.cardinality(publisher.source),
        )

    @property
    def variable(self) -> bool:
        return self.cardinality < 0

    def input_names(self, publisher: Publisher) -> list[str]:
        parameters = list(inspect.signature(_unwrap_info(publisher.func)).parameters)
        if self.variable:
            return parameters
        return parameters[: self.cardinality]
