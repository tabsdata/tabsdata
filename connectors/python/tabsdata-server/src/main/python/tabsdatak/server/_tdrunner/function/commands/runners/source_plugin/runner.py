#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

from tabsdatak._connectors import get_src, get_stream_src
from tabsdatak._info import (
    KIND_PUBLISHER_DEF,
    KIND_STREAM_PUBLISHER_DEF,
    PublisherInfo,
    StreamPublisherInfo,
    _unwrap_info,
)
from tabsdatak.api import Src
from tabsdatak.conn.stream.stage._backend import StageBackend
from tabsdatak.server._tdrunner.function._commons.document.request import (
    Location,
    Request,
)
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputEntry,
    InputManifest,
    InputRecord,
)
from tabsdatak.server._tdrunner.function._handlers.state import State
from tabsdatak.server._tdrunner.function._lib.connection import load_connection
from tabsdatak.server._tdrunner.function._lib.endpoint import UnsupportedSchemeError
from tabsdatak.server._tdrunner.function._lib.function_def import (
    load_function_def_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request_ctx import FromRequestCtx
from tabsdatak.server._tdrunner.function._lib.stage_backend import (
    stage_backend_from_location,
)
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.error import (
    RunSourcePluginErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.runners.source_plugin.options import (  # noqa: E501
    RunSourcePluginArguments,
)
from tabsdatak.spi import Conn, SrcPlugin, SrcPluginCtx, TableFileInput

logger = logging.getLogger(__name__)

FILE_SCHEME = "file"
FILE_PREFIX = "file:"

Publisher = PublisherInfo | StreamPublisherInfo


class SourceContext(FromRequestCtx, SrcPluginCtx):
    pass


class StageConnection(Conn):
    def __init__(
        self,
        *,
        stage_backend: StageBackend | None = None,
        local: Conn | None = None,
    ) -> None:
        self.stage_backend = stage_backend
        self.local = local
        self.s3: Conn | None = None
        self.az: Conn | None = None
        self.gcs: Conn | None = None


@dataclass(frozen=True)
class SourceBinding:
    plugin: SrcPlugin
    connection: Conn
    source: Src
    cardinality: int


def load_function_definition(bundle_folder: Path) -> Publisher:
    return load_function_def_or_raise(
        bundle_folder,
        allowed_kinds={
            KIND_PUBLISHER_DEF: PublisherInfo,
            KIND_STREAM_PUBLISHER_DEF: StreamPublisherInfo,
        },
        bad_bundle_code=RunSourcePluginErrorCode.RSP_2,
        bad_user_code_code=RunSourcePluginErrorCode.RSP_3,
        wrong_kind_code=RunSourcePluginErrorCode.RSP_4,
    )


def build_manifest_stage_connection(function_stage: Location) -> StageConnection:
    parsed = urlparse(function_stage.uri)
    if parsed.scheme == FILE_SCHEME:
        from tabsdatak.conn.localfile import LocalFileSrcConn

        base_path = Path(url2pathname(parsed.path)).as_posix()
        return StageConnection(local=LocalFileSrcConn(base_path=base_path))
    try:
        backend = stage_backend_from_location(function_stage)
    except UnsupportedSchemeError as error:
        raise RunSourcePluginErrorCode.RSP_11.exception(
            uri=function_stage.uri,
            scheme=parsed.scheme,
        ) from error
    return StageConnection(stage_backend=backend)


def bind_source(
    publisher: Publisher,
    function_stage: Location,
    connection_file: Path,
) -> SourceBinding:
    # A stream publisher's own source and connection are for the streamer pod;
    # here its staged manifests are read instead, through the publisher-side
    # source the stream source names and a connection to the stage.
    if isinstance(publisher, StreamPublisherInfo):
        try:
            stream_definition = get_stream_src(type(publisher.source))
        except KeyError as error:
            raise RunSourcePluginErrorCode.RSP_5.exception(
                cause=error,
                source_type=type(publisher.source).__name__,
                detail=str(error),
            )
        stage_source_type = stream_definition.publisher_src
        try:
            definition = get_src(stage_source_type)
        except KeyError as error:
            raise RunSourcePluginErrorCode.RSP_5.exception(
                cause=error,
                source_type=stage_source_type.__name__,
                detail=str(error),
            )
        stage_source = stage_source_type()
        return SourceBinding(
            plugin=definition.plugin(),
            connection=build_manifest_stage_connection(function_stage),
            source=stage_source,
            cardinality=definition.cardinality(stage_source),
        )
    try:
        definition = get_src(type(publisher.source))
    except KeyError as error:
        raise RunSourcePluginErrorCode.RSP_5.exception(
            cause=error,
            source_type=type(publisher.source).__name__,
            detail=str(error),
        )
    return SourceBinding(
        plugin=definition.plugin(),
        connection=load_connection(connection_file),
        source=publisher.source,
        cardinality=definition.cardinality(publisher.source),
    )


def read_in(
    binding: SourceBinding,
    context: SrcPluginCtx,
) -> list[list[TableFileInput]]:
    try:
        return binding.plugin.read_in(
            context,
            binding.connection,
            binding.source,
        )
    except Exception as error:
        raise RunSourcePluginErrorCode.RSP_6.exception(cause=error, detail=str(error))


def table_file_path(file: Path) -> str:
    # Some plugins hand back a file URI wrapped in a Path; the manifest always
    # holds an absolute posix path.
    text = str(file)
    if text[: len(FILE_PREFIX)].lower() == FILE_PREFIX:
        return Path(url2pathname(urlparse(text).path)).as_posix()
    return Path(text).absolute().as_posix()


def write_inputs_manifest(
    publisher: Publisher,
    produced: list[list[TableFileInput]],
    path: Path,
    *,
    cardinality: int,
) -> None:
    parameters = list(inspect.signature(_unwrap_info(publisher.func)).parameters)
    # A negative cardinality means the source decides its slot count at read
    # time: missing slots are padded empty, so the function always gets its
    # declared parameters.
    if cardinality < 0:
        expected = len(parameters)
        if len(produced) > expected:
            raise RunSourcePluginErrorCode.RSP_7.exception(
                returned=len(produced),
                expected=expected,
            )
        produced = [*produced, *([] for _ in range(expected - len(produced)))]
        cardinality = expected
    elif len(produced) != cardinality:
        raise RunSourcePluginErrorCode.RSP_7.exception(
            returned=len(produced),
            expected=cardinality,
        )
    manifest = InputManifest(
        inputs=tuple(
            InputEntry(
                name=parameters[position],
                table_pos=position,
                tables=tuple(
                    InputRecord(
                        file=table_file_path(table.file),
                        metadata=table.metadata,
                    )
                    for table in produced[position]
                ),
            )
            for position in range(cardinality)
        )
    )
    try:
        manifest.write(path)
    except OSError as error:
        raise RunSourcePluginErrorCode.RSP_8.exception(
            cause=error,
            path=path,
            detail=str(error),
        )


def run(arguments: RunSourcePluginArguments) -> int:
    arguments.output_folder.mkdir(parents=True, exist_ok=True)

    logger.info("Running the source plugin")
    request = Request.load(arguments.request, RunSourcePluginErrorCode.RSP_1)
    publisher = load_function_definition(arguments.bundle_folder)
    binding = bind_source(
        publisher,
        request.info.function_stage,
        arguments.connection_file,
    )
    logger.debug(
        f"Source plugin {type(binding.plugin).__name__} "
        f"for connection {type(binding.connection).__name__} "
        f"({type(publisher).__name__}, cardinality {binding.cardinality})"
    )
    context = SourceContext.from_info(arguments.output_folder, request.info)
    state = State(request, arguments.state_folder)
    state.hydrate(context, error_code=RunSourcePluginErrorCode.RSP_9)
    logger.debug(f"Source context ready on {arguments.output_folder}")

    logger.info("Reading the inputs through the source plugin")
    produced = read_in(binding, context)
    logger.info(f"Source plugin produced {len(produced)} slot(s)")

    state.drain(context, error_code=RunSourcePluginErrorCode.RSP_10)
    logger.debug(f"State drained to {state.file}")

    write_inputs_manifest(
        publisher,
        produced,
        arguments.inputs_manifest,
        cardinality=binding.cardinality,
    )
    logger.info(f"Inputs manifest written to {arguments.inputs_manifest}")
    return 0
