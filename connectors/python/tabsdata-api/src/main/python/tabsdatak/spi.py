#
# Copyright 2026 Tabsdata Inc.
#

"""Service-provider interface (SPI) for connector authors.

The user-facing `tabsdatak.api` defines *what* a flow looks like;
this module defines *how* a connector plugs into that flow. Each
connector package registers one of the registration dataclasses below
via the `tabsdatak.connectors` entry point:

* `SrcDef` -- binds a `Src` subclass to its `SrcPlugin`
  (`read_in`) and optional `Explorer`.
* `StreamSrcDef` -- binds a `StreamSrc` subclass to its
  `StreamSrcPlugin` (`run`).
* `DestDef` -- binds a `Dest` subclass to its `DestPlugin`
  (`write_out`).

The plugin bases (`SrcPlugin`, `StreamSrcPlugin`, `DestPlugin`)
and their per-mode context types (`SrcPluginCtx`,
`StreamSrcPluginCtx`, `DestContext`) are what plugin code consumes.
The remaining pieces -- `TableFileSpec`, `TableFileInput`
(`MetadataSpec`), `TableMode`, `StreamStateSpec` (`BasicDictSpec`
| `CommitStateFn`) -- are the data-shape contracts that travel
between framework and plugin during a trx.
"""

from __future__ import annotations

import importlib
import inspect
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    ClassVar,
    Iterator,
    Sequence,
    Type,
    TypeAlias,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import ConfigDict, model_validator

import tabsdatak._api as _api
from tabsdatak._legacy import module_getattr
from tabsdatak.api import (
    BasicDictSpec,
    Dest,
    FlowCtx as _FlowCtx,
    Src,
    StreamSrc,
    StreamThresholds,
    TransactionCtx,
)
from tabsdatak.error import ApiErrorCode

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa


class Conn(_api.DescriptorBase, ABC):
    """Abstract base for every connection.

    :categories: spi

    Concrete subclasses live in the connector packages (e.g.
    `S3SrcConn`, `MySQLConn`) and declare their own `uri` /
    `credentials` / `conn_cfg` fields. A connection is never
    declared on a flow function; it is registered on the function's
    collection and the runtime resolves it and hands it to the
    connector's plugin (`read_in` / `write_out` / `run`) at run
    time. Subclasses share the `"connectionDef"` `__kind__` and must
    not redefine it (they are told apart by their envelope `type`).
    """

    # devguide-ref: connector connection resolution

    __kind__ = "connectionDef"


# The on-disk transport for one in-flight table: a single Parquet file as
# a plain `pathlib.Path`. Plugins materialise rows to these paths during
# reads (wrapping each in a `TableFileInput`) and consume them
# during writes (as bare paths via `DestPlugin.write_out`).
TableFileSpec: TypeAlias = Path

# Optional per-file metadata a reader may attach to a `TableFileInput`.
# A convenience for the framework to add each metadata entry as a metadata
# column on the given parquet file's table: every entry becomes one column, by
# convention named with a leading `@`. Restricted to a flat `str`-keyed dict
# of `str`, `int`, `float` and `bool`.
_MetadataBasicType: TypeAlias = Union[str, int, float, bool]
MetadataSpec: TypeAlias = dict[str, _MetadataBasicType]


@_api.dataclass(ApiErrorCode.API_22)
class TableFileInput:
    """One parquet file `SrcPlugin.read_in` produces for an output
    slot, paired with optional metadata to carry alongside it.

    :categories: spi

    Args:
        file: Path to the parquet file the plugin materialised under
            `ctx.work_dir`.
        metadata: Optional per-file `MetadataSpec`. Each entry is
            added as a metadata column on `file`'s table, by
            convention named with a leading `@`. `None` (the common
            case) attaches nothing -- see `to_table_file_inputs`.
    """

    file: Path
    metadata: MetadataSpec | None = None


def to_table_file_inputs(
    files: list[list[TableFileSpec]],
) -> list[list[TableFileInput]]:
    """Wrap per-slot parquet paths as `TableFileInput`\\ s with no
    metadata -- the shape `SrcPlugin.read_in` returns.

    :categories: spi

    A reader that produces bare `TableFileSpec` paths funnels them
    through this helper; one that has per-file metadata constructs
    `TableFileInput` directly instead. Imported from `tabsdatak.spi`.
    """
    return [[TableFileInput(file=f) for f in slot] for slot in files]


class TableMode(Enum):
    """How a `SrcPlugin` returns slot contents -- the length of each
    inner list in `SrcPlugin.read_in`'s return.

    :categories: spi

    * `SINGLE` -- exactly one `TableFileInput` per output slot.
    * `ARRAY` -- one or more `TableFileInput` per output slot
      (file-shard style).
    """

    SINGLE = "single"
    ARRAY = "array"


class PluginCtx(_FlowCtx, ABC):
    """Common context surface shared by every plugin variant
    (batch source, stream source, destination).

    :categories: spi

    Extends `FlowCtx` with the per-trx working directory where the
    plugin materialises parquet files.
    """

    @property
    @abstractmethod
    def work_dir(self) -> Path:
        """Per-trx scratch directory the plugin writes parquet to.
        Cleaned up by the runtime after the trx completes."""


class SrcPluginCtx(TransactionCtx, PluginCtx, ABC):
    """Context handed to `SrcPlugin.read_in` -- combines `TransactionCtx`
    (trx identifiers + scratch state) with `PluginCtx` (flow
    identifiers + work dir).

    :categories: spi
    """


class SrcPlugin[C: Conn, S: Src](ABC):
    """Plugin implementing the read-side of a `Src` subclass.

    :categories: spi
    """

    @abstractmethod
    def read_in(self, ctx: SrcPluginCtx, conn: C, src: S) -> list[list[TableFileInput]]:
        """Materialise `src`'s output slots to parquet under
        `ctx.work_dir`.

        Args:
            ctx: The `SrcPluginCtx` for this trx (work dir, state).
            conn: The resolved connection.
            src: The `Src` config declared on the flow.

        Returns:
            One inner list of `TableFileInput` per output slot. In
            `SINGLE` `TableMode` each inner list holds exactly one
            file; in `ARRAY` mode it may hold more. Wrap bare paths
            with `to_table_file_inputs`.
        """

    # if TableMode is SINGLE, inner list must always have 1 element.


FilesSpec: TypeAlias = Annotated[list[TableFileSpec], _api.NON_EMPTY_LIST_FIELD]

# `arbitrary_types_allowed=True` escape hatch for the `SrcDef` /
# `StreamSrcDef` / `DestDef` dataclasses below, whose `Type[...]`
# fields reference abstract classes (`SrcPlugin`, `Explorer`, ...)
# that pydantic validates via issubclass / isinstance.
_ARBITRARY = ConfigDict(arbitrary_types_allowed=True)

# Commit callback for the case where the *external* system owns the
# resume offset. Hand it to `StreamSrcPluginCtx.stage` and the
# runtime invokes it when it commits the staged batch, so the plugin can
# advance/ack the source (e.g. `lambda: external.commit()`). Tabsdata
# keeps no state in this case, so `StreamSrcPluginCtx.stream_state`
# reads back `None`.
CommitStateFn: TypeAlias = Callable[[], None]

# What `StreamSrcPluginCtx.stage` accepts as a batch's new resume
# state:
#   * a `BasicDictSpec` -- a flat `{str: scalar}` dict of resume
#     state *Tabsdata* owns; the runtime persists it (atomically with the
#     staged batch) and replays it on the next run via
#     `StreamSrcPluginCtx.stream_state`; or
#   * a `CommitStateFn` -- the *external* system owns the offset.
StreamStateSpec: TypeAlias = Union[BasicDictSpec, CommitStateFn]


class StreamSrcPluginCtx(PluginCtx, ABC):
    """Context handed to `StreamSrcPlugin.run` -- the plugin's
    long-lived control surface.

    :categories: spi

    * `active` -- loop predicate the plugin polls.
    * `first_stage` -- whether nothing has been staged yet, so the
      plugin seeds from config rather than from `stream_state`.
    * `stream_state` -- the per-flow resume state persisted by the
      last `BasicDictSpec` handed to `stage`.
    * `thresholds` -- the `StreamThresholds` of the function's definition:
      the `batch` level bounding what the plugin stages (how far apart
      batches are at least, how long data may wait at most, how long the
      stream may go without a batch, and the rows and bytes that stage
      one); the `buffer` and `poll` levels when the definition names them.
    * `stage` -- publish a batch and commit its new resume state.
    * `checkpoint` -- commit a new resume state with no batch.
    """

    @property
    @abstractmethod
    def active(self) -> bool:
        """`True` while the runtime wants the plugin to keep polling.
        The plugin loop should exit (cleanly) once this is `False`."""

    @property
    @abstractmethod
    def first_stage(self) -> bool:
        """`True` until the first batch has been staged + committed.

        The plugin uses it to decide where to seed its resume position:
        when `first_stage` is `True` nothing has been published yet,
        so seed from the source's configured initial values; otherwise a
        prior batch exists, so resume from `stream_state` (or, when the
        external system owns the offset, from the external system)."""

    @property
    @abstractmethod
    def stream_state(self) -> BasicDictSpec | None:
        """The per-flow resume state persisted by the most recent
        `BasicDictSpec` handed to `stage`.

        `None` when the last commit handed `stage` a
        `CommitStateFn` callable (a `lambda`) instead of a
        `BasicDictSpec` -- i.e. the *external* system owns the offset
        and Tabsdata keeps no state. Distinguish "first run" (nothing
        staged yet) via `first_stage`, not by this being `None`."""

    @property
    @abstractmethod
    def thresholds(self) -> StreamThresholds:
        """The `StreamThresholds` of the function's definition: the batch
        bounds the plugin stages by, the same for every stream source."""

    @abstractmethod
    def stage(
        self,
        tables_files: list[list[Path]],
        new_state: StreamStateSpec,
    ) -> None:
        """Publish a batch and commit its new resume state in one step.

        Args:
            tables_files: Per-output-table parquet files, positional by
                index (entry `i` -> table `i`; an empty inner list
                means no files for that table). Files must live under
                `work_dir`.
            new_state: The new resume state -- a `BasicDictSpec` the
                runtime persists, or a `CommitStateFn` it invokes for
                its side effect (external system owns the offset).
        """

    @abstractmethod
    def checkpoint(self, new_state: BasicDictSpec) -> None:
        """Commit a new resume state without publishing anything: the
        position moved past source data that carried nothing to stage.

        It replaces the state `stage` last persisted, and is read back
        through `stream_state` like it. Call it only once `first_stage`
        is `False`: a state is what tells the next run that this stream
        has staged before.

        Args:
            new_state: The new resume state, persisted as is.
        """


class _StreamStageSrcConn(Conn):
    pass


class StreamStageSrc(Src):
    """Config type for the hidden batch source that ingests a stream's
    staged manifests.

    :categories: spi

    A `StreamSrcDef` names it (or a subclass) via `publisher_src`.
    Subclass it only when pairing it with a customised
    `StreamStageSrcPlugin`.
    """


class StreamStageSrcPlugin(SrcPlugin[_StreamStageSrcConn, StreamStageSrc]):
    """Hidden batch-side (publisher) source that ingests a stream
    stage's manifests.

    :categories: spi

    A `StreamSrcPlugin` only *stages* data (accumulates batches into
    storage and records each in a manifest); every `StreamSrc` needs a
    paired hidden `SrcPlugin` on the batch side to consume it, which
    must be this class or a subclass. Each execution consumes exactly one
    manifest -- the oldest unconsumed one -- advancing a per-flow cursor;
    it yields zero output slots when none remain.

    The manifest scan + ingest is fixed in `read_in` and **must not be
    overridden**; customise the ingested data (e.g. de-duplication) by
    overriding `process_files` instead. To register a customised
    variant, pair a `StreamStageSrc` subclass with this plugin in a
    `SrcDef` whose `cardinality` returns `-1` (disables the
    slot-count check) and whose `explorer` is `None`.
    """

    # devguide-ref: stream-stage source customization

    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: _StreamStageSrcConn,
        src: StreamStageSrc,
    ) -> list[list[TableFileInput]]:
        """Ingest the oldest unconsumed manifest's files.

        Final -- subclasses must **not** override this; customise the
        ingested data via `process_files` instead."""
        plugin_cls = getattr(
            importlib.import_module("tabsdatak.conn.stream.stage"),
            "ManifestStageSrcPlugin",
        )
        plugin = cast(SrcPlugin, plugin_cls())
        return self.process_files(ctx, plugin.read_in(ctx, conn, src))

    # noinspection PyMethodMayBeStatic
    def process_files(
        self,
        _ctx: SrcPluginCtx,
        files: list[list[TableFileInput]],
    ) -> list[list[TableFileInput]]:
        """Hook for subclass post-processing of the ingested files
        (e.g. de-duplication).

        Receives the per-slot file lists `read_in` materialised from
        the manifest and returns the per-slot file lists the plugin
        ultimately yields. The base implementation returns `files`
        unchanged."""
        return files


class StreamSrcPlugin[C: Conn, SS: StreamSrc](ABC):
    """Plugin implementing the long-running side of a `StreamSrc`
    subclass (CDC, log tail, queue tail).

    :categories: spi
    """

    @abstractmethod
    def run(
        self,
        ctx: StreamSrcPluginCtx,
        conn: C,
        stream_src: SS,
    ):
        """Drive the change stream until `ctx.active` flips to
        `False` or a signal arrives. Implementations seed from config
        when `ctx.first_stage` else from `ctx.stream_state`, then
        call `ctx.stage` for batches as they complete (honouring
        `ctx.thresholds`)."""


class DestChannel(ABC):
    """One position of the subscriber's outputs, as a destination plugin
    receives it.

    :categories: spi

    Abstract, and never built by a plugin: the runtime hands over the
    channels of the `DestBus` variant the plugin's `write_out`
    declares. A channel holds one table, which `read()` returns in the
    variant's format.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """The output's name."""

    @property
    @abstractmethod
    def position(self) -> int:
        """The output's position among the subscriber's outputs."""

    @abstractmethod
    def read(self) -> object:
        """The table, in the variant's format."""


class DestChannelOfLazyFrame(DestChannel, ABC):
    """A `DestChannel` whose table is a polars `LazyFrame` scanning it
    from storage: nothing is read until the plugin collects or sinks it.

    :categories: spi
    """

    @abstractmethod
    def read(self) -> pl.LazyFrame:
        """The table, as a `LazyFrame`."""


class DestChannelOfDataFrame(DestChannel, ABC):
    """A `DestChannel` whose table is a polars `DataFrame`: the whole
    table in memory.

    :categories: spi
    """

    @abstractmethod
    def read(self) -> pl.DataFrame:
        """The table, as a `DataFrame`."""


class DestChannelOfArrow(DestChannel, ABC):
    """A `DestChannel` whose table is a pyarrow `RecordBatchReader`
    streaming it one record batch at a time.

    :categories: spi
    """

    @abstractmethod
    def read(self) -> pa.RecordBatchReader:
        """The table, as a `RecordBatchReader`."""


class DestChannelOfFile(DestChannel, ABC):
    """A `DestChannel` whose table is a local parquet file under the
    plugin's work folder, copied from storage before `write_out` runs.

    :categories: spi
    """

    @abstractmethod
    def read(self) -> Path:
        """The table, as a local parquet file."""


class DestBus[Channel: DestChannel]:
    """The channels a destination plugin receives in one run, one per
    subscriber output, by position (`bus[0]`) or by name
    (`bus["out_0"]`).

    :categories: spi

    Use one of its variants, which fix the format of every channel:
    `DestBusOfLazyFrame`, `DestBusOfDataFrame`, `DestBusOfArrow` or
    `DestBusOfFile`. The variant a plugin
    declares on `write_out` is the format it receives.

    Args:
        channels: The channels, ordered by position.
    """

    channel_type: ClassVar[type[DestChannel] | None] = None

    def __init__(self, channels: Sequence[Channel]) -> None:
        if self.channel_type is None:
            raise TypeError(
                f"{type(self).__qualname__} has no channel type; use one of "
                f"{dest_bus_names()}",
            )
        for position, channel in enumerate(channels):
            if not isinstance(channel, self.channel_type):
                raise TypeError(
                    f"{type(self).__qualname__} holds "
                    f"{self.channel_type.__qualname__} channels, not "
                    f"{type(channel).__qualname__}",
                )
            if channel.position != position:
                raise ValueError(
                    f"Channel {channel.name!r} is at position {position} but "
                    f"declares position {channel.position}",
                )
        self._channels = list(channels)
        self._names = {channel.name: channel for channel in self._channels}

    def __len__(self) -> int:
        return len(self._channels)

    def __iter__(self) -> Iterator[Channel]:
        return iter(self._channels)

    def __getitem__(self, key: int | str) -> Channel:
        if isinstance(key, str):
            try:
                return self._names[key]
            except KeyError:
                raise KeyError(
                    f"No channel named {key!r}; channels are {list(self._names)}",
                ) from None
        return self._channels[key]


class DestBusOfLazyFrame(DestBus[DestChannelOfLazyFrame]):
    """A `DestBus` of `DestChannelOfLazyFrame`: the preferred format,
    streamed by polars from storage.

    :categories: spi
    """

    channel_type = DestChannelOfLazyFrame


class DestBusOfDataFrame(DestBus[DestChannelOfDataFrame]):
    """A `DestBus` of `DestChannelOfDataFrame`: each table in memory.

    :categories: spi
    """

    channel_type = DestChannelOfDataFrame


class DestBusOfArrow(DestBus[DestChannelOfArrow]):
    """A `DestBus` of `DestChannelOfArrow`: each table streamed as Arrow
    record batches.

    :categories: spi
    """

    channel_type = DestChannelOfArrow


class DestBusOfFile(DestBus[DestChannelOfFile]):
    """A `DestBus` of `DestChannelOfFile`: each table copied to a local
    parquet file first. Least preferred; for plugins that need a file on
    disk.

    :categories: spi
    """

    channel_type = DestChannelOfFile


# The `DestBus` variants a plugin may declare, in order of preference.
DEST_BUSES: tuple[type[DestBus], ...] = (
    DestBusOfLazyFrame,
    DestBusOfDataFrame,
    DestBusOfArrow,
    DestBusOfFile,
)

# What `DestPlugin.write_out` receives after `ctx`, `conn` and `dest`: a
# `DestBus` variant, or the legacy list of local parquet files.
DestTablesSpec: TypeAlias = Union[list[TableFileSpec], DestBus]


def dest_bus_names() -> str:
    """The `DestBus` variants a plugin may declare, in order of
    preference, as text.

    :categories: spi
    """
    return ", ".join(bus.__name__ for bus in DEST_BUSES)


class DestContext(TransactionCtx, PluginCtx, ABC):
    """Context handed to `DestPlugin.write_out` -- combines `TransactionCtx`
    with `PluginCtx`.

    :categories: spi
    """


class DestPlugin[C: Conn, D: Dest](ABC):
    """Plugin implementing the write-side of a `Dest` subclass.

    :categories: spi

    The annotation of `write_out`'s `tables` parameter is the plugin's
    contract with the runtime: a `DestBus` variant receives the
    subscriber's outputs in that variant's format, and
    `list[TableFileSpec]` (or no annotation) receives them as local
    parquet files, the legacy form. Declare exactly one variant;
    `DestDef` refuses any other `DestBus` or `DestChannel` type.
    Variants, in order of preference: `DestBusOfLazyFrame`,
    `DestBusOfDataFrame`, `DestBusOfArrow`, `DestBusOfFile`. Import the
    variant at module level, so the annotation resolves at runtime.
    """

    # devguide-ref: destination handoff

    @abstractmethod
    def write_out(
        self,
        ctx: DestContext,
        conn: C,
        dest: D,
        tables: DestTablesSpec,
    ):
        """Push the subscriber's outputs in `tables` to `dest`.

        Args:
            ctx: The `DestContext` for this trx.
            conn: The resolved connection.
            dest: The `Dest` config declared on the flow.
            tables: One table per target, in the format the override
                declares: a `DestBus` variant, or a list of local
                parquet files. Passed positionally, so any parameter
                name works. The runtime guarantees as many tables as
                `dest`'s declared target count.
        """


class ConnMetaModel(ABC):
    """Abstract base for a connection *kind* (SQL, objects, storage, ...).

    :categories: spi
    """

    @abstractmethod
    def entity_fqn(self, entity: dict) -> str:
        """Format the fully qualified name of an entity."""


@_api.dataclass(ApiErrorCode.API_23, config=_ARBITRARY)
class Entity(ABC):
    """Abstract base for an entity within a connection.

    :categories: spi

    Every concrete entity carries a back-reference to the
    `ConnMetaModel` that produced it so that `fqn` can format the
    entity's fully qualified name using the meta-model's hierarchy.

    Args:
        connection_type: The `ConnMetaModel` that produced this
            entity.
    """

    connection_type: ConnMetaModel

    @abstractmethod
    def fqn(self) -> str:
        """Return the fully qualified name of this entity.

        The format follows the meta-model's hierarchy (e.g. SQL uses
        `database.schema.table`, storage uses `prefix/path`).
        """

    @abstractmethod
    def field_names(self) -> list[str]:
        """Return the names of all fields/columns/properties here."""

    @abstractmethod
    def to_dict(self) -> dict:
        """Serialize the entity to a dict, omitting falsy values."""

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict, connection_type: "ConnMetaModel") -> "Entity":
        """Rebuild an entity from `to_dict` output.

        `connection_type` is supplied explicitly because it is a
        runtime reference, not part of the serialized dict.
        """


class Explorer[C: Conn](ABC):
    """Optional connector capability for ad-hoc inspection of a live
    `Conn`: catalog metadata and arbitrary query execution.

    :categories: spi

    Used by tooling (UIs, schema diff), not by the flow runtime. Bound
    to a `SrcDef` / `StreamSrcDef` / `DestDef` via its
    `explorer` field.

    Attributes:
        meta_model: The `ConnMetaModel` describing this connection
            kind (set by the subclass).
        entity_type: The concrete `Entity` type this explorer returns
            (set by the subclass).
    """

    meta_model: ClassVar[ConnMetaModel]
    entity_type: ClassVar[type[Entity]]

    @abstractmethod
    def check_connect(self, conn: C) -> None:
        """Verify that a connection can be established to `conn`.

        Performs the smallest possible round-trip (open a session, issue
        a single-row query, list one resource, ...) and raises if it
        fails; returns `None` on success.

        Args:
            conn: The connection to check.
        """

    @abstractmethod
    def metadata(self, conn: C) -> list[Entity] | None:
        """Return the connection's catalog metadata (schemas, tables,
        columns)."""

    def data(self, conn: C, entity: Entity, limit: int = 100) -> list[dict] | None:
        """Return up to `limit` sample rows from `entity`.

        The base implementation returns `None` -- the connector does
        not support a row-level preview (file / object storage inherits
        this). SQL-like extractors override to issue a `SELECT` and
        return a list (empty when the entity has no rows -- still
        distinct from `None`).

        Args:
            conn: The connection to query.
            entity: The entity to sample rows from.
            limit: Maximum number of rows to return.

        Returns:
            One dict per row, or `None` when row-level preview is
            unsupported.
        """
        return None


def check_plugin_param_type(
    plugin_cls: type,
    method_name: str,
    param_name: str,
    expected: type,
) -> None:
    """Validate a plugin method's parameter type matches `expected`.

    :categories: spi

    Resolves `plugin_cls.<method_name>`'s type hints and requires the
    `param_name` parameter to be annotated with a class that
    `expected` is a subclass of. Skips silently when the parameter has
    no concrete annotation (e.g. an inherited abstract method whose type
    is still a TypeVar), so the check only fires on concretely typed
    plugins. Used by the `*Def` validators.

    Args:
        plugin_cls: The plugin class to inspect.
        method_name: The method whose parameter is checked.
        param_name: The parameter to check.
        expected: The type the parameter's annotation must accept.

    Raises:
        ValueError: If the method is missing or the annotation rejects
            `expected`.
    """
    method = getattr(plugin_cls, method_name, None)
    if method is None:
        raise ValueError(
            f"{plugin_cls.__qualname__}: missing required `{method_name}` method"
        )
    try:
        hints = get_type_hints(method)
    except Exception:
        return
    declared = hints.get(param_name)
    if declared is None or not isinstance(declared, type):
        return
    if not issubclass(expected, declared):
        raise ValueError(
            f"{plugin_cls.__qualname__}.{method_name}: `{param_name}` "
            f"parameter is typed as {declared.__qualname__}, but "
            f"{expected.__qualname__} is required"
        )


def _mentions_bus(annotation: Any) -> bool:
    if isinstance(annotation, str):
        return "DestBus" in annotation or "DestChannel" in annotation
    if isinstance(annotation, type):
        return issubclass(annotation, (DestBus, DestChannel))
    origin = get_origin(annotation)
    if origin is not None and _mentions_bus(origin):
        return True
    return any(_mentions_bus(argument) for argument in get_args(annotation))


def dest_handoff(plugin: type[DestPlugin]) -> type[DestBus] | None:
    """The `DestBus` variant `plugin.write_out` receives, or `None` when
    it receives the legacy list of local parquet files.

    :categories: spi

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
    try:
        declared = get_type_hints(method).get(parameter.name)
    except Exception as error:
        if _mentions_bus(parameter.annotation):
            raise ValueError(
                f"{plugin.__qualname__}.write_out: the annotation of "
                f"`{parameter.name}` does not resolve at runtime ({error}); "
                f"import the DestBus variant at module level",
            ) from error
        return None
    if declared in DEST_BUSES:
        return declared
    if _mentions_bus(declared):
        raise ValueError(
            f"{plugin.__qualname__}.write_out: `{parameter.name}` is typed as "
            f"{declared!r}; a destination plugin declares one of "
            f"{dest_bus_names()} (in order of preference), or the legacy "
            f"list[TableFileSpec]",
        )
    return None


# The `SrcDef` / `StreamSrcDef` / `DestDef` dataclasses carry
# `Type[...]` fields whose argument types are abstract classes
# (`SrcPlugin`, `Explorer`, ...), not pydantic types.
# `arbitrary_types_allowed=True` (the `_ARBITRARY` ConfigDict
# defined above) tells pydantic to validate those fields via
# issubclass / isinstance checks.


@_api.dataclass(ApiErrorCode.API_14, config=_ARBITRARY)
class SrcDef[S: Src]:
    """Entry-point registration for a batch `Src` binding.

    :categories: spi

    Registered via the `tabsdatak.connectors` entry point. Binds a
    `Src` config type to the `SrcPlugin` that reads it.

    Args:
        type_: Wire-stable identifier, unique across *all* plugins
            (sources, stream sources and destinations share one
            namespace), e.g. `"s3-file-in"`. The registry's key.
        system: Human-readable name of the target system (e.g.
            `"AWS S3"`); shown by `tdk connection types`.
        src_version: Connector schema version.
        conn: Concrete `Conn` subclass the source binds to.
        src: Concrete `Src` subclass this binding registers.
        table_mode: Whether `read_in` produces one file per slot or
            many; see `TableMode`.
        cardinality: Callable returning the number of output slots
            `src` declares (e.g. `lambda s: len(s.paths)`).
        plugin: `SrcPlugin` subclass implementing `read_in`.
        explorer: Optional `Explorer` for tooling introspection.
        icon: Path to the connector's icon relative to the
            `tabsdatak` namespace root (e.g.
            `"conn/s3/resources/s3.svg"`), or `None` for none.
    """

    type_: str
    system: str
    src_version: str
    conn: Type[Conn]
    src: Type[S]
    table_mode: TableMode
    cardinality: Callable[[S], int]
    plugin: Type[SrcPlugin]
    explorer: Type[Explorer] | None
    icon: str | None

    @model_validator(mode="after")
    def _validate(self) -> "SrcDef":
        check_plugin_param_type(self.plugin, "read_in", "conn", self.conn)
        check_plugin_param_type(self.plugin, "read_in", "src", self.src)
        if self.explorer is not None:
            check_plugin_param_type(self.explorer, "metadata", "conn", self.conn)
            check_plugin_param_type(self.explorer, "data", "conn", self.conn)
        return self


@_api.dataclass(ApiErrorCode.API_15, config=_ARBITRARY)
class StreamSrcDef[S: StreamSrc]:
    """Entry-point registration for a `StreamSrc` binding.

    :categories: spi

    Registered via the `tabsdatak.connectors` entry point. Binds a
    `StreamSrc` config type to the `StreamSrcPlugin` that runs it.

    Args:
        type_: Wire-stable identifier, unique across *all* plugins
            (e.g. `"postgres-cdc"`, `"kafka-topic-in"`).
        system: Human-readable name of the target system (e.g.
            `"Apache Kafka"`); shown by `tdk connection types`.
        src_version: Connector schema version.
        conn: Concrete `Conn` subclass the stream source binds to.
        src: Concrete `StreamSrc` subclass this binding registers.
        cardinality: Callable returning the number of output slots
            `src` declares (e.g. `lambda s: len(s.tables)`).
        plugin: `StreamSrcPlugin` subclass implementing `run`.
        publisher_src: `StreamStageSrc` subclass naming the hidden
            batch source that ingests this stream's staged manifests;
            use `StreamStageSrc` itself for the default behaviour.
        explorer: Optional `Explorer` for tooling.
        icon: Path to the connector's icon relative to the
            `tabsdatak` namespace root, or `None` for none.
    """

    type_: str
    system: str
    src_version: str
    conn: Type[Conn]
    src: Type[S]
    cardinality: Callable[[S], int]
    plugin: Type[StreamSrcPlugin]
    publisher_src: Type[StreamStageSrc]
    explorer: Type[Explorer] | None
    icon: str | None

    @model_validator(mode="after")
    def _validate(self) -> "StreamSrcDef":
        # `StreamSrcPlugin` defines `run(ctx, conn, stream_src)`, not
        # the batch-mode `read_in(ctx, conn, src)` from `SrcPlugin`.
        check_plugin_param_type(self.plugin, "run", "conn", self.conn)
        check_plugin_param_type(self.plugin, "run", "stream_src", self.src)
        if self.explorer is not None:
            check_plugin_param_type(self.explorer, "metadata", "conn", self.conn)
            check_plugin_param_type(self.explorer, "data", "conn", self.conn)
        return self


@_api.dataclass(ApiErrorCode.API_16, config=_ARBITRARY)
class DestDef[D: Dest]:
    """Entry-point registration for a `Dest` binding.

    :categories: spi

    Registered via the `tabsdatak.connectors` entry point. Binds a
    `Dest` config type to the `DestPlugin` that writes it.

    Args:
        type_: Wire-stable identifier, unique across *all* plugins
            (e.g. `"s3-file-out"`).
        system: Human-readable name of the target system (e.g.
            `"AWS S3"`); shown by `tdk connection types`.
        dest_version: Connector schema version.
        conn: Concrete `Conn` subclass the destination binds to.
        dest: Concrete `Dest` subclass this binding registers.
        cardinality: Callable returning the number of input slots
            `dest` declares (e.g. `lambda d: len(d.paths)`).
        plugin: `DestPlugin` subclass implementing `write_out`.
        explorer: Optional `Explorer` for tooling.
        icon: Path to the connector's icon relative to the
            `tabsdatak` namespace root, or `None` for none.
    """

    type_: str
    system: str
    dest_version: str
    conn: Type[Conn]
    dest: Type[D]
    cardinality: Callable[[D], int]
    plugin: Type[DestPlugin]
    explorer: Type[Explorer] | None
    icon: str | None

    @model_validator(mode="after")
    def _validate(self) -> "DestDef":
        check_plugin_param_type(self.plugin, "write_out", "conn", self.conn)
        check_plugin_param_type(self.plugin, "write_out", "dest", self.dest)
        if dest_handoff(self.plugin) is None:
            _api.warn_legacy_dest(self.plugin)
        if self.explorer is not None:
            check_plugin_param_type(self.explorer, "metadata", "conn", self.conn)
            check_plugin_param_type(self.explorer, "data", "conn", self.conn)
        return self


__getattr__ = module_getattr(
    __name__,
    {
        "FlowCtx": ("tabsdatak.api", "FlowCtx"),
        "FunctionCtx": ("tabsdatak.api", "FunctionCtx"),
    },
)
