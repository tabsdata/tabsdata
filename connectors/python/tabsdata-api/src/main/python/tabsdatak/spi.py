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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterable,
    Type,
    TypeAlias,
    Union,
    cast,
    get_type_hints,
    overload,
)

from pydantic import ConfigDict, model_validator
from typing_extensions import TypeVar

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
    """How the data a `SrcPlugin` lands for one publisher input is
    served to the function. Declared once, on the `SrcDef`.

    :categories: spi

    * `SINGLE` -- one table: the frames (or files) `read_in` returns
      for the input are chunks of that table, concatenated.
    * `ARRAY` -- a list of tables, one per frame (or file).
    """

    SINGLE = "single"
    ARRAY = "array"


class Layout(Enum):
    """How the columns of a `LazyFrameInput` frame hold its table.

    :categories: spi

    * `COLUMNAR` -- One column per source column.
    * `SERIALIZED` -- One `String` column with one value per source row.
    """

    COLUMNAR = "columnar"
    SERIALIZED = "serialized"


@dataclass(frozen=True)
class ParseOptions:
    """How the `String` values of a `SourceFormat` are split and typed
    when served. Each format has its own subclass.

    :categories: spi
    """


@dataclass(frozen=True)
class CsvParseOptions(ParseOptions):
    """How the `String` values of a CSV source are typed.

    :categories: spi

    Args:
        decimal_comma: Floats use `,` as their decimal separator.
    """

    decimal_comma: bool = False


@dataclass(frozen=True)
class NdjsonParseOptions(ParseOptions):
    """How the rows of an NDJSON source are split and typed.

    :categories: spi
    """


class SourceFormat(Enum):
    """The format the `String` values of a frame come from.

    :categories: spi

    * `CSV` -- Each value is a CSV cell.
    * `NDJSON` -- Each value is a JSON object.
    """

    CSV = "csv"
    NDJSON = "ndjson"

    @property
    def parse_options_class(self) -> type[ParseOptions]:
        """The `ParseOptions` subclass of the format."""
        return {
            SourceFormat.CSV: CsvParseOptions,
            SourceFormat.NDJSON: NdjsonParseOptions,
        }[self]


@dataclass(frozen=True)
class FrameMetadata:
    """How the frame of a `LazyFrameInput` is served to the function.

    :categories: spi

    Args:
        schema: The types each column is served with, in order. `None`
            serves the frame with its own types. With a schema:

            * a column already of its type is served as it is;
            * a `String` column is parsed into its type;
            * any other column is cast to its type.

            A value that cannot be parsed or cast fails the function.
        layout: How the frame's columns hold the table.
        format: The format the frame's `String` values come from.
            Required by `Layout.SERIALIZED` and by untyped frames.
        typed: Whether each column holds values of its real type.
            `False` when other types are stored as text, such as the
            cells of a CSV file or the rows of an NDJSON file.
        parsing: How the format's values are split and typed, an
            instance of its `SourceFormat.parse_options_class`. `None` takes
            the format's defaults.
        columns: Each entry is added as a column holding its value on
            every row, by convention named with a leading `@`.

    Raises:
        ValueError: A serialized or untyped frame has no format, or
            parse options are given without one.
        TypeError: The parse options are not the format's.
    """

    schema: pl.Schema | None = None
    layout: Layout = Layout.COLUMNAR
    format: SourceFormat | None = None
    typed: bool = True
    parsing: ParseOptions | None = None
    columns: MetadataSpec | None = None

    def __post_init__(self) -> None:
        if self.format is None:
            if self.layout is Layout.SERIALIZED:
                raise ValueError("A serialized frame needs the format of its rows")
            if not self.typed:
                raise ValueError(
                    "An untyped frame needs the format its values come from"
                )
            if self.parsing is not None:
                raise ValueError("Parse options need the format their values come from")
            return
        expected = self.format.parse_options_class
        if self.parsing is not None and type(self.parsing) is not expected:
            raise TypeError(
                f"A {self.format.name} frame takes {expected.__name__}, not "
                f"{type(self.parsing).__name__}"
            )


@dataclass(frozen=True)
class LazyFrameInput:
    """One table `SrcPlugin.read_in` produces for a publisher input: its
    data, as a polars `LazyFrame`, and the metadata that says how to
    serve it.

    :categories: spi

    Args:
        frame: The table, or one chunk of it.
        metadata: Optional `FrameMetadata`, or a function returning it.
            A function is called once the frame has landed, for
            metadata learnt while the frame streams.
    """

    frame: pl.LazyFrame
    metadata: FrameMetadata | Callable[[], FrameMetadata] | None = None


def to_lazy_frame_inputs(
    frames: list[list[pl.LazyFrame]],
) -> list[Iterable[LazyFrameInput]]:
    """Wrap per-input `LazyFrame`\\ s as `LazyFrameInput`\\ s with no
    metadata -- the shape `SrcPlugin.read_in` returns.

    :categories: spi
    """
    return [[LazyFrameInput(frame=frame) for frame in slot] for slot in frames]


# What `SrcPlugin.read_in` returns, one entry per publisher input: the
# tables produced for it, all `LazyFrameInput` (a list, or a generator), or
# all the legacy local parquet files. A union of lists, not a list of a
# union, so that a plugin declares one form for every table.
SrcTablesSpec: TypeAlias = Union[
    list[Iterable[LazyFrameInput]],
    list[list[TableFileInput]],
]


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

    The return annotation of `read_in` is the plugin's contract with the
    runtime: `list[Iterable[LazyFrameInput]]` returns `LazyFrame`\\ s,
    which the runtime lands as `read_in` produces them;
    `list[list[TableFileInput]]`, or no annotation, is the legacy form,
    local parquet files.
    """

    # devguide-ref: source handoff

    @abstractmethod
    def read_in(self, ctx: SrcPluginCtx, conn: C, src: S) -> SrcTablesSpec:
        """Read `src` into the publisher's inputs.

        Args:
            ctx: The `SrcPluginCtx` for this trx (work dir, state).
            conn: The resolved connection.
            src: The `Src` config declared on the flow.

        Returns:
            One entry per input, by position: position `i` is the `i`-th
            entry of the configuration (as many as `SrcDef.cardinality`
            counts) and feeds the function's `i`-th parameter. Each
            entry holds the tables produced for that input, served as
            `SrcDef.table_mode` says: a list, or any iterable, such as a
            generator yielding one page at a time, so that only one is
            in memory. The runtime takes the entries in order, and
            lands each table as it takes it. Legacy plugins return one
            inner list of `TableFileInput` per input, parquet files
            under `ctx.work_dir`. Wrap bare frames with
            `to_lazy_frame_inputs`, bare paths with
            `to_table_file_inputs`.
        """


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


@dataclass(frozen=True)
class StageFileStats:
    """One table file `StreamSrcPluginCtx.stage` wrote to the stage.

    :categories: spi

    Args:
        name: The file's name in its batch, not its full location.
        rows: The rows it holds.
        bytes: Its size on the stage.
    """

    name: str
    rows: int
    bytes: int


class StreamSrcPluginCtx(PluginCtx, ABC):
    """Context handed to `StreamSrcPlugin.run` -- the plugin's
    long-lived control surface.

    :categories: spi

    * `active` -- loop predicate the plugin polls.
    * `first_stage` -- whether nothing has been staged yet, so the
      plugin seeds from config rather than from `stream_state`.
    * `stream_state` -- the per-flow resume state persisted by the
      last `BasicDictSpec` handed to `stage` or `checkpoint`.
    * `thresholds` -- the `StreamThresholds` of the function's definition:
      the `batch` level bounding what the plugin stages (how far apart
      batches are at least, how long data may wait at most, how long the
      stream may go without a batch, and the rows and bytes that stage
      one); the `buffer` and `poll` levels when the definition names them.
    * `stage` -- send tables to the stage, publish them as a batch and
      commit its new resume state, together or apart.
    * `scan` -- read back a table sent and not yet published.
    * `unstage` -- drop tables sent and not yet published.
    * `pending` -- what was sent and not yet published.
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

    @overload
    def stage(
        self,
        tables_files: list[list[Path]],
        new_state: StreamStateSpec,
    ) -> None: ...

    @overload
    def stage(
        self,
        tables_files: list[Iterable[LazyFrameInput] | None],
        new_state: StreamStateSpec | None = None,
    ) -> list[list[StageFileStats]]: ...

    @overload
    def stage(
        self,
        tables_files: None,
        new_state: StreamStateSpec,
    ) -> None: ...

    @abstractmethod
    def stage(
        self,
        tables_files: list[list[Path]] | list[Iterable[LazyFrameInput] | None] | None,
        new_state: StreamStateSpec | None = None,
    ) -> list[list[StageFileStats]] | None:
        """Send tables to the stage, publish them as a batch, and commit
        the batch's new resume state.

        Tables sent and not yet published are pending, per output table.
        The arguments say what one call does:

        * `stage(tables, new_state)` sends `tables`, then publishes
          everything pending as one batch and commits `new_state`.
        * `stage(tables)` (or `stage(tables, None)`) only sends `tables`;
          send as often as memory demands, and publish them later.
        * `stage(None, new_state)` publishes everything pending (an empty
          batch when nothing is) and commits `new_state`.
        * `stage(files, new_state)`, with lists of local parquet files, is
          the legacy form: it uploads the files and publishes them with
          anything pending. It is deprecated in favour of sending
          `LazyFrameInput` tables.

        Args:
            tables_files: Per-output-table entries, positional by index
                (entry `i` -> table `i`), one for every output table. Each
                is the tables to send for it -- `LazyFrameInput`\\ s, a
                list or any iterable, taken and sunk one at a time -- or
                `None` (or an empty list) for no table. In the legacy form,
                each is a list of parquet files under `work_dir`. `None`
                to send nothing.
            new_state: The new resume state -- a `BasicDictSpec` the
                runtime persists, or a `CommitStateFn` it invokes for
                its side effect (external system owns the offset). `None`
                to send without publishing.

        Returns:
            When it sends `LazyFrameInput` tables, the `StageFileStats` of the
            files this call wrote, per output table; otherwise `None`.
        """

    @abstractmethod
    def scan(self, file: StageFileStats) -> pl.LazyFrame:
        """Read back a table sent and not yet published, as it was sent,
        from where it is staged.

        Use it to work on what was sent without keeping it: e.g. to
        de-duplicate a load against the changes read after it, sending the
        result and unstaging what it replaces.

        Args:
            file: The table, as `stage` returned it or `pending` lists it.
        """

    @overload
    def unstage(self) -> None: ...

    @overload
    def unstage(self, files: Iterable[StageFileStats]) -> None: ...

    @abstractmethod
    def unstage(self, files: Iterable[StageFileStats] | None = None) -> None:
        """Drop tables sent and not yet published, so no batch publishes
        them, and delete their files as far as it can. The resume state
        does not change.

        * `unstage()` drops every one, e.g. to start over from
          `stream_state`.
        * `unstage(files)` drops only `files`.

        Args:
            files: The tables to drop, as `stage` returned them or
                `pending` lists them.
        """

    @property
    @abstractmethod
    def pending(self) -> list[list[StageFileStats]]:
        """The `StageFileStats` of every table sent and not yet published,
        per output table."""

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
    overriding `process_files` instead, annotating its `files` as
    `list[Iterable[LazyFrameInput]]` to receive the tables where they are
    staged (an override annotated, or left, with the legacy
    `list[list[TableFileInput]]` receives local parquet copies, and is
    deprecated). To register a customised
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
    ) -> list[Iterable[LazyFrameInput]]:
        """Ingest the oldest unconsumed manifest's tables.

        Final -- subclasses must **not** override this; customise the
        ingested data via `process_files` instead."""
        from tabsdatak._handoff import StageContract

        plugin_cls = getattr(
            importlib.import_module("tabsdatak.conn.stream.stage"),
            "ManifestStageSrcPlugin",
        )
        plugin = cast(SrcPlugin, plugin_cls())
        tables = cast(list[Iterable[LazyFrameInput]], plugin.read_in(ctx, conn, src))
        return StageContract(type(self)).process(self, ctx, tables)

    # noinspection PyMethodMayBeStatic
    def process_files(
        self,
        _ctx: SrcPluginCtx,
        files: list[Iterable[LazyFrameInput]],
    ) -> list[Iterable[LazyFrameInput]]:
        """Hook for subclass post-processing of the ingested tables
        (e.g. de-duplication).

        Receives the per-slot tables `read_in` took from the manifest,
        lazily over where they are staged, and returns the per-slot
        tables the plugin ultimately yields. The base implementation
        returns `files` unchanged.

        An override whose `files` is annotated (or left unannotated) as
        the legacy `list[list[TableFileInput]]` receives local parquet
        copies and returns `TableFileInput`\\ s; it is deprecated, and
        cannot take untyped tables (those staged with a `schema`)."""
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


# What `DestPlugin.write_out` receives after `ctx`, `conn` and `dest`, one
# table per subscriber output: all `LazyFrame`s scanning them from storage,
# or all the legacy local parquet files. A union of lists, not a list of a
# union, so that a plugin declares one form for every table.
type DestTablesSpec = Union[list["pl.LazyFrame"], list[TableFileSpec]]


class DestContext(TransactionCtx, PluginCtx, ABC):
    """Context handed to `DestPlugin.write_out` -- combines `TransactionCtx`
    with `PluginCtx`.

    :categories: spi
    """


_C = TypeVar("_C", bound=Conn)
_D = TypeVar("_D", bound=Dest)
_T = TypeVar("_T", bound=DestTablesSpec, default=list[TableFileSpec])


class DestPlugin(ABC, Generic[_C, _D, _T]):
    """Plugin implementing the write-side of a `Dest` subclass.

    :categories: spi

    The third type argument is the plugin's contract with the runtime:
    `list[pl.LazyFrame]` (`DestPlugin[MyConn, MyDest, list[pl.LazyFrame]]`)
    receives each of the subscriber's outputs as a `LazyFrame` scanning
    it from storage, and `write_out`'s `tables` is annotated with the
    same type. Without it, the legacy form, `tables` is a list of local
    parquet files. `DestDef` refuses any other type, and a `tables`
    annotation that differs. Import polars at module level, so the
    annotation resolves at runtime.
    """

    # devguide-ref: destination handoff

    @abstractmethod
    def write_out(
        self,
        ctx: DestContext,
        conn: _C,
        dest: _D,
        tables: _T,
    ):
        """Push the subscriber's outputs in `tables` to `dest`.

        Args:
            ctx: The `DestContext` for this trx.
            conn: The resolved connection.
            dest: The `Dest` config declared on the flow.
            tables: One table per target, by position, in the form the
                plugin declares: `LazyFrame`s, or local parquet files.
                The runtime guarantees as many tables as `dest`'s
                declared target count.
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
        table_mode: How the data landed for each publisher input is
            served: one table or a list of tables; see `TableMode`.
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
        from tabsdatak._handoff import SourceContract

        check_plugin_param_type(self.plugin, "read_in", "conn", self.conn)
        check_plugin_param_type(self.plugin, "read_in", "src", self.src)
        if SourceContract(self.plugin).deprecated():
            _api.warn_legacy_src(self.plugin)
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


# A `DestPlugin` of either form. Type checkers need every type argument, or
# a bare `DestPlugin` would mean the legacy form only; pydantic validates the
# class itself, as it refuses a parametrized class in `type[...]`.
if TYPE_CHECKING:
    _AnyDestPlugin: TypeAlias = Type[DestPlugin[Any, Any, Any]]
else:
    _AnyDestPlugin: TypeAlias = Type[DestPlugin]


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
    plugin: _AnyDestPlugin
    explorer: Type[Explorer] | None
    icon: str | None

    @model_validator(mode="after")
    def _validate(self) -> "DestDef":
        check_plugin_param_type(self.plugin, "write_out", "conn", self.conn)
        check_plugin_param_type(self.plugin, "write_out", "dest", self.dest)
        from tabsdatak._handoff import DestinationContract

        if not DestinationContract(self.plugin).frames():
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
