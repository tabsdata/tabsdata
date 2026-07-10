"""Service-provider interface (SPI) for connector authors.

The user-facing :mod:`tabsdatak.api` defines *what* a flow looks
like; this module defines *how* a connector plugs into that flow.
Per-database connector packages register an instance of one of
the registration dataclasses below via the
``tabsdatak.connectors`` entry point:

* :class:`SrcDef` -- binds a :class:`tabsdatak.api.Src` subclass
  to its :class:`SrcPlugin` (``read_in``) and optional
  :class:`Explorer`.
* :class:`StreamSrcDef` -- binds a
  :class:`tabsdatak.api.StreamSrc` subclass to its
  :class:`StreamSrcPlugin` (``run``).
* :class:`DestDef` -- binds a :class:`tabsdatak.api.Dest`
  subclass to its :class:`DestPlugin` (``write_out``).

The plugin abstracts (:class:`SrcPlugin`, :class:`StreamSrcPlugin`,
:class:`DestPlugin`) and their per-mode context types
(:class:`SrcPluginCtx`, :class:`StreamSrcPluginCtx`,
:class:`DestContext`) are what plugin code consumes. The remaining
pieces -- :data:`TableFileSpec`, :class:`TableFileInput`
(:data:`MetadataSpec`), :class:`TableMode`, :data:`StreamStateSpec`
(:data:`BasicDictSpec` | :data:`CommitStateFn`) -- are the data-shape
contracts that travel between framework and plugin during a trx.
"""

from __future__ import annotations

import importlib
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import (
    Annotated,
    Callable,
    ClassVar,
    Type,
    TypeAlias,
    Union,
    cast,
    get_type_hints,
)

from pydantic import ConfigDict, model_validator

import tabsdatak._api as _api
from tabsdatak.api import (
    BasicDictSpec,
    Dest,
    Src,
    StreamSrc,
    TrxCtx,
)
from tabsdatak.error import ApiErrorCode


class Conn(_api.DescriptorBase, ABC):
    """Abstract base for every connection.

    Concrete subclasses live in the per-database connector
    packages (e.g. ``MySQLConn``, ``MySQLCdcConn``) and declare
    their own ``uri`` / ``credentials`` / ``conn_cfg`` fields.

    A connection is never declared on a flow function; it is registered
    on the function's collection and the runtime resolves it and hands
    it to the connector's plugin (``read_in`` / ``write_out`` / ``run``)
    at run time.

    Every connection shares the same on-wire ``__kind__`` discriminator
    (``"connectionDef"``); subclasses are told apart by their envelope
    ``type`` (the qualified class name), not by ``__kind__``, and must
    not redefine it."""

    __kind__ = "connectionDef"


# The on-disk transport for one in-flight table: a single Parquet file as
# a plain ``pathlib.Path``. Plugins materialise rows to these paths during
# reads (wrapping each in a :class:`TableFileInput`) and consume them
# during writes (as bare paths via :meth:`DestPlugin.write_out`).
TableFileSpec: TypeAlias = Path

# Optional per-file metadata a reader may attach to a :class:`TableFileInput`.
# A convenience for the framework to add each metadata entry as a metadata
# column on the given parquet file's table: every entry becomes one column, by
# convention named with a leading ``@``. Restricted to JSON-scalar values
# (``str`` / ``int`` / ``float`` / ``bool``), a flat list of them, or a flat
# ``str``-keyed dict of them -- nothing nested.
_MetadataBasicType: TypeAlias = Union[str, int, float, bool]
MetadataSpec: TypeAlias = Union[
    _MetadataBasicType,
    list[_MetadataBasicType],
    dict[str, _MetadataBasicType],
]


@_api.dataclass(ApiErrorCode.API_22)
class TableFileInput:
    """One parquet file :meth:`SrcPlugin.read_in` produces for an output
    slot, paired with optional metadata to carry alongside it.

    :param file: Path to the parquet file the plugin materialised under
        :attr:`PluginCtx.work_dir`.
    :param metadata: Optional per-file :data:`MetadataSpec`. A convenience
        that has the framework add each metadata entry as a metadata column
        on ``file``'s table -- every entry becomes one column, by convention
        named with a leading ``@``. ``None`` when the reader has nothing to
        attach -- the common case, see :func:`to_table_file_inputs`.
    """

    file: Path
    metadata: MetadataSpec | None = None


def to_table_file_inputs(
    files: list[list[TableFileSpec]],
) -> list[list[TableFileInput]]:
    """Wrap per-slot parquet paths as :class:`TableFileInput`\\ s, leaving
    ``metadata`` at its default (``None``) -- the shape
    :meth:`SrcPlugin.read_in` returns.

    A reader that produces bare :data:`TableFileSpec` paths funnels them
    through this helper; a reader that has per-file metadata to attach
    constructs :class:`TableFileInput` directly instead."""
    return [[TableFileInput(file=f) for f in slot] for slot in files]


class TableMode(Enum):
    """How a :class:`SrcPlugin` returns slot contents -- the length of
    each inner list in :meth:`SrcPlugin.read_in`'s return:

    * ``SINGLE`` -- exactly one :class:`TableFileInput` per output slot.
    * ``ARRAY`` -- one or more :class:`TableFileInput` per output slot
      (file-shard style).
    """

    SINGLE = "single"
    ARRAY = "array"


class FlowCtx(ABC):
    """Flow-level identifiers (collection / function) shared by every
    role that runs inside a trx -- plugins *and* user-decorated
    functions. Carries no plugin-specific surface (no ``work_dir``)."""

    @property
    @abstractmethod
    def collection_id(self) -> str:
        """Unique id of the collection the function belongs to."""

    @property
    @abstractmethod
    def collection_name(self) -> str:
        """Human-readable collection name."""

    @property
    @abstractmethod
    def function_id(self) -> str:
        """Unique id of the running function."""

    @property
    @abstractmethod
    def function_name(self) -> str:
        """Human-readable function name."""


class PluginCtx(FlowCtx, ABC):
    """Common context surface shared by every plugin variant
    (batch source, stream source, destination).

    Extends :class:`FlowCtx` with the per-trx working directory where
    the plugin materialises parquet files."""

    @property
    @abstractmethod
    def work_dir(self) -> Path:
        """Per-trx scratch directory the plugin writes parquet to.
        Cleaned up by the runtime after the trx completes."""


class SrcPluginCtx(TrxCtx, PluginCtx, ABC):
    """Context handed to :meth:`SrcPlugin.read_in` -- combines
    :class:`tabsdatak.api.TrxCtx` (trx identifiers + scratch state)
    with :class:`PluginCtx` (flow identifiers + work dir)."""


class SrcPlugin[C: Conn, S: Src](ABC):
    """Plugin implementing the read-side of a :class:`Src` subclass."""

    @abstractmethod
    def read_in(self, ctx: SrcPluginCtx, conn: C, src: S) -> list[list[TableFileInput]]:
        """Materialise ``src``'s output slots to parquet under
        ``ctx.work_dir``.

        Returns one inner list of :class:`TableFileInput` per output
        slot: when the binding's :class:`TableMode` is ``SINGLE`` each
        inner list has exactly one file; in ``ARRAY`` mode it may have
        more. Readers that produce bare paths can wrap them via
        :func:`to_table_file_inputs` (metadata defaults to ``None``).
        """

    # if TableMode is SINGLE, inner list must always have 1 element.


FilesSpec: TypeAlias = Annotated[list[TableFileSpec], _api.NON_EMPTY_LIST_CONSTRAINTS]

# ``arbitrary_types_allowed=True`` escape hatch for the ``SrcDef`` /
# ``StreamSrcDef`` / ``DestDef`` dataclasses below, whose ``Type[...]``
# fields reference abstract classes (``SrcPlugin``, ``Explorer``, ...)
# that pydantic validates via issubclass / isinstance.
_ARBITRARY = ConfigDict(arbitrary_types_allowed=True)

# Commit callback for the case where the *external* system owns the
# resume offset. Hand it to :meth:`StreamSrcPluginCtx.stage` and the
# runtime invokes it when it commits the staged batch, so the plugin can
# advance/ack the source (e.g. ``lambda: external.commit()``). Tabsdata
# keeps no state in this case, so :attr:`StreamSrcPluginCtx.stream_state`
# reads back ``None``.
CommitStateFn: TypeAlias = Callable[[], None]

# What :meth:`StreamSrcPluginCtx.stage` accepts as a batch's new resume
# state:
#   * a :data:`BasicDictSpec` -- a flat ``{str: scalar}`` dict of resume
#     state *Tabsdata* owns; the runtime persists it (atomically with the
#     staged batch) and replays it on the next run via
#     :attr:`StreamSrcPluginCtx.stream_state`; or
#   * a :data:`CommitStateFn` -- the *external* system owns the offset.
StreamStateSpec: TypeAlias = Union[BasicDictSpec, CommitStateFn]


class StreamSrcPluginCtx(PluginCtx, ABC):
    """Context handed to :meth:`StreamSrcPlugin.run`. Provides the
    plugin its long-lived control surface:

    * :attr:`active` -- loop predicate the plugin polls.
    * :attr:`first_stage` -- whether nothing has been staged yet, so the
      plugin seeds from config rather than from :attr:`stream_state`.
    * :attr:`stream_state` -- the per-flow resume state persisted by the
      last :data:`BasicDictSpec` handed to :meth:`stage`.
    * :attr:`flush_interval_mins` -- max minutes the plugin may buffer
      before it must :meth:`stage`.
    * :meth:`stage` -- publish a batch and commit its new resume state.
    """

    @property
    @abstractmethod
    def active(self) -> bool:
        """``True`` while the runtime wants the plugin to keep polling.
        The plugin loop should exit (cleanly) once this is ``False``."""

    @property
    @abstractmethod
    def first_stage(self) -> bool:
        """``True`` until the first batch has been staged + committed.

        The plugin uses it to decide where to seed its resume position:
        when ``first_stage`` is ``True`` nothing has been published yet,
        so seed from the source's configured initial values; otherwise a
        prior batch exists, so resume from :attr:`stream_state` (or, when
        the external system owns the offset, from the external system)."""

    @property
    @abstractmethod
    def stream_state(self) -> BasicDictSpec | None:
        """The per-flow resume state persisted by the most recent
        :data:`BasicDictSpec` handed to :meth:`stage`.

        ``None`` when the last commit handed :meth:`stage` a
        :data:`CommitStateFn` callable (a ``lambda``) instead of a
        :data:`BasicDictSpec` -- i.e. the *external* system owns the
        offset and Tabsdata keeps no state. Distinguish "first run"
        (nothing staged yet) via :attr:`first_stage`, not by this being
        ``None``."""

    @property
    @abstractmethod
    def flush_interval_mins(self) -> int:
        """Maximum minutes the plugin may accumulate before it must
        :meth:`stage`, regardless of its own size/row thresholds."""

    @abstractmethod
    def stage(
        self,
        tables_files: list[list[Path]],
        new_state: StreamStateSpec,
    ) -> None:
        """Publish a batch and commit its new resume state in one step.

        ``tables_files`` is positional by output-table index (entry
        ``i`` -> table ``i``; an empty inner list means no files for that
        table this batch); files must live under :attr:`work_dir`. The
        runtime stages + commits the files, then commits ``new_state``:
        persisting it when ``new_state`` is a :data:`BasicDictSpec`, or
        invoking it for its side effect when it is a :data:`CommitStateFn`."""


class _StreamStageSrcConn(Conn):
    pass


class StreamStageSrc(Src):
    pass


class StreamStageSrcPlugin(SrcPlugin[_StreamStageSrcConn, StreamStageSrc]):
    """Batch-side (publisher) source that ingests a stream stage's manifest.

    A :class:`StreamSrcPlugin` only *stages* data: its long-running
    ``run`` accumulates batches into Tabsdata's storage staging area and
    records each one in a manifest file. Those staged batches are not
    pulled into a flow by the stream plugin itself -- every
    :class:`StreamSrc` requires a paired **hidden** :class:`SrcPlugin`
    that runs on the batch (publisher) side and consumes the staged data.
    That hidden source plugin must be this ``StreamStageSrcPlugin`` (or a
    subclass of it); a :class:`StreamSrcDef` names it via its
    ``publisher_src`` :class:`StreamStageSrc` binding.

    On each execution this plugin scans the connection's staging area for
    manifest files and ingests the data files a manifest lists. It
    consumes exactly **one manifest per execution -- the oldest manifest
    that has not been consumed yet** -- advancing a per-flow cursor so the
    next execution picks up the following one. When no unconsumed manifest
    is found it produces zero output slots.

    Customising ingestion
    ---------------------
    The manifest scan + file ingestion is fixed and lives in
    :meth:`read_in`, which **must not be overridden**. A subclass that
    needs special logic over the incoming data -- e.g. de-duplicating
    rows -- overrides :meth:`process_files` instead. ``read_in`` ingests
    the manifest's files and passes them to :meth:`process_files`, whose
    (possibly transformed) return value becomes the plugin's output.

    Subclassing checklist
    ---------------------
    To register a customised stream-stage source:

    * Subclass ``StreamStageSrcPlugin`` and override :meth:`process_files`.
    * Subclass :class:`StreamStageSrc` to serve as the source's config
      type.
    * Create a :class:`SrcDef` binding the two and declare it as a
      ``tabsdatak.connectors`` entry point. Because the number of output
      slots is decided by the manifest at read time (not by static
      config), its ``cardinality`` **must be a lambda returning ``-1``**
      -- the sentinel that disables the framework's
      produced-vs-declared slot-count check -- and its ``explorer`` must
      be ``None``.

    The concrete manifest scan/ingest is implemented by
    ``ManifestStageSrcPlugin`` in the ``tabsdata-conn-stream-stage``
    package; it is imported lazily so ``tabsdata-api`` keeps no
    dependency on the connector."""

    def read_in(
        self,
        ctx: SrcPluginCtx,
        conn: _StreamStageSrcConn,
        src: StreamStageSrc,
    ) -> list[list[TableFileInput]]:
        """Ingest the oldest unconsumed manifest's files.

        Final -- subclasses must **not** override this; customise the
        ingested data via :meth:`process_files` instead."""
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

        Receives the per-slot file lists :meth:`read_in` materialised
        from the manifest and returns the per-slot file lists the plugin
        ultimately yields. The base implementation returns ``files``
        unchanged."""
        return files


class StreamSrcPlugin[C: Conn, SS: StreamSrc](ABC):
    """Plugin implementing the long-running side of a
    :class:`StreamSrc` subclass (CDC, log tail, queue tail)."""

    @abstractmethod
    def run(
        self,
        ctx: StreamSrcPluginCtx,
        conn: C,
        stream_src: SS,
    ):
        """Drive the change stream until ``ctx.active`` flips to
        ``False`` or a signal arrives. Implementations seed from config
        when ``ctx.first_stage`` else from ``ctx.stream_state``, then
        :meth:`StreamSrcPluginCtx.stage` batches as they complete
        (honouring ``ctx.flush_interval_mins``)."""


class DestContext(TrxCtx, PluginCtx, ABC):
    """Context handed to :meth:`DestPlugin.write_out` -- combines
    :class:`tabsdatak.api.TrxCtx` with :class:`PluginCtx`."""


class DestPlugin[C: Conn, D: Dest](ABC):
    """Plugin implementing the write-side of a :class:`Dest`
    subclass."""

    @abstractmethod
    def write_out(
        self,
        ctx: DestContext,
        conn: C,
        dest: D,
        tables: list[TableFileSpec],
    ):
        """Push the per-slot parquet files in ``tables`` to
        ``dest``. The runtime guarantees ``len(tables)`` equals
        ``dest``'s declared target-table count.
        """


class FunctionCtx(TrxCtx, FlowCtx, ABC):
    """Context handed to a user-decorated function (``@publisher`` /
    ``@transformer`` / ``@subscriber``) when its signature declares a
    trailing ``ctx`` parameter typed as :class:`FunctionCtx` (or any
    :class:`tabsdatak.api.TrxCtx` subclass).

    Combines :class:`tabsdatak.api.TrxCtx` (trx identifiers + per-flow
    scratch state via ``get_attr`` / ``set_attr``) with
    :class:`FlowCtx` (flow identifiers). The user function uses it to
    read state from the previous run and persist new state for the
    next.

    Sibling of :class:`SrcPluginCtx` and :class:`DestContext`: same
    flow-level identifiers, but deliberately **does not** inherit
    :class:`PluginCtx` -- user functions are not plugins and should
    not see plugin-only surface like ``work_dir`` (the plugin's
    parquet-staging directory). Keeping the bases disjoint stops
    that detail from leaking into user code at type-check time.
    """


class Entity(ABC):
    """Abstract base for an entity within a connection.

    Every concrete entity carries a back-reference to the
    :class:`ConnMetaModel` that produced it so that :meth:`fqn` can
    format the entity's fully qualified name using the hierarchy
    declared by the meta-model.
    """

    connection_type: "ConnMetaModel"

    @abstractmethod
    def fqn(self) -> str:
        """Return the fully qualified name of this entity.

        The format is dictated by the meta-model's
        ``entity_fully_qualified_name`` hierarchy (e.g. SQL uses
        ``"database.schema.table"``, storage uses ``"prefix/path"``).
        """

    @abstractmethod
    def field_names(self) -> list[str]:
        """Return the names of all fields/columns/properties in this entity."""

    @abstractmethod
    def to_dict(self) -> dict:
        """Serialize the entity to a dict, omitting falsy values."""

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict, connection_type: "ConnMetaModel") -> "Entity":
        """Rebuild an entity from :meth:`to_dict` output.

        The caller supplies *connection_type* explicitly because it's
        not serialized inside the dict — the meta-model instance is a
        runtime reference, not a persisted payload.
        """


class ConnMetaModel(ABC):
    """Abstract base for a connection *kind* (SQL, objects, storage, …)."""


class Explorer[C: Conn](ABC):
    """Optional connector capability for ad-hoc inspection of a
    live :class:`tabsdatak.spi.Conn`: catalog metadata and arbitrary
    query execution. Used by tooling (UIs, schema diff), not by the
    flow runtime."""

    meta_model: ClassVar[ConnMetaModel]
    entity_type: ClassVar[type[Entity]]

    @abstractmethod
    def check_connect(self, conn: C) -> None:
        """Verify that a connection can be established to ``conn``

        Should perform the smallest possible round-trip (open a session,
        issue a single-row query, list one resource, …) and raise if the
        attempt fails. Returns ``None`` on success.

        Args:
            conn: The connection to check.
        """

    @abstractmethod
    def metadata(self, conn: C) -> list[Entity] | None:
        """Return the connection's catalog metadata (schemas,
        tables, columns)."""

    @abstractmethod
    def data(self, conn: C, entity: Entity, limit: int = 100) -> list[dict] | None:
        """
        Return up to *limit* sample rows from *entity*.

        The base implementation returns ``None`` to signal that this
        connector does not support a row-level data preview (file /
        object storage inherits this). SQL-like extractors override
        to issue a ``SELECT`` and return a list (possibly empty when
        the entity has no rows — still distinct from ``None``).

        Args:
            conn: The connection to query.
            entity: The entity to sample rows from.
            limit: Maximum number of rows to return.

        Returns:
            A list of dicts (one per row) or ``None`` when the connector
            does not support row-level preview.
        """


def check_plugin_param_type(
    plugin_cls: type,
    method_name: str,
    param_name: str,
    expected: type,
) -> None:
    """Validate a plugin method's parameter type matches ``expected``.

    Looks up ``plugin_cls.<method_name>``, resolves its type hints, and
    requires that the ``param_name`` parameter is annotated with a class
    that ``expected`` is a subclass of. Skips silently if the parameter
    has no concrete annotation -- e.g. an inherited abstract method
    where the type is still a TypeVar -- so the check only fires on
    plugins that have been concretely typed.
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
# `arbitrary_types_allowed=True` (the ``_ARBITRARY`` ConfigDict
# defined above) tells pydantic to validate those fields via
# issubclass / isinstance checks.


@_api.dataclass(ApiErrorCode.API_14, config=_ARBITRARY)
class SrcDef[S: Src]:
    """Entry-point registration for a batch :class:`Src` binding.

    :param type_: Wire-stable identifier, unique across *all* plugins
        -- batch sources, stream sources and destinations share one
        namespace (``"mysql-sql-in"``, ``"s3-file-in"``, ...). The
        registry uses it as the single key for this binding.
    :param system: Human-readable name of the external system this
        connector targets (e.g. ``"MySQL"``, ``"AWS S3"``); shown by
        ``tdk connection types``.
    :param src_version: Connector schema version.
    :param conn: Concrete :class:`Conn` subclass the source binds
        to.
    :param src: Concrete :class:`Src` subclass this binding
        registers.
    :param table_mode: Whether ``read_in`` produces one file per
        slot or many; see :class:`TableMode`.
    :param cardinality: Callable returning the number of output
        slots ``src`` declares (typically ``lambda s: len(s.queries)``).
    :param plugin: :class:`SrcPlugin` subclass that implements
        ``read_in``.
    :param explorer: Optional :class:`Explorer` for tooling
        introspection of the connection.
    :param icon: Path to the connector's icon, relative to the
        ``tabsdatak`` namespace root so it can be fetched directly with
        ``td_resource(icon)`` (e.g. ``"conn/mysql/resources/mysql.svg"``).
        The file lives in the plugin package's ``resources/`` folder.
        ``None`` when the connector ships no icon; the caller supplies a
        default.
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
    """Entry-point registration for a :class:`StreamSrc` binding.

    :param type_: Wire-stable identifier, unique across *all* plugins
        (``"mysql-cdc"``, ``"postgres-cdc"``, ``"kafka-topic-in"``, ...).
    :param system: Human-readable name of the external system this
        connector targets (e.g. ``"MySQL"``, ``"Apache Kafka"``); shown by
        ``tdk connection types``.
    :param src_version: Connector schema version.
    :param conn: Concrete :class:`Conn` subclass the stream source
        binds to.
    :param src: Concrete :class:`StreamSrc` subclass this binding
        registers.
    :param cardinality: Callable returning the number of output
        slots ``src`` declares (typically
        ``lambda s: len(s.tables)``).
    :param plugin: :class:`StreamSrcPlugin` subclass that
        implements ``run``.
    :param publisher_src: :class:`StreamStageSrc` subclass naming the
        hidden batch (publisher) source that ingests this stream's
        staged manifests. Its own :class:`SrcDef` binds it to a
        :class:`StreamStageSrcPlugin`; use :class:`StreamStageSrc`
        itself for the default manifest behaviour.
    :param explorer: Optional :class:`Explorer` for tooling.
    :param icon: Path to the connector's icon, relative to the
        ``tabsdatak`` namespace root so it can be fetched directly with
        ``td_resource(icon)`` (e.g.
        ``"conn/mysql/resources/mysql-cdc.svg"``). The file lives in the
        plugin package's ``resources/`` folder. ``None`` when the
        connector ships no icon; the caller supplies a default.
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
        # ``StreamSrcPlugin`` defines ``run(ctx, conn, stream_src)``, not
        # the batch-mode ``read_in(ctx, conn, src)`` from ``SrcPlugin``.
        check_plugin_param_type(self.plugin, "run", "conn", self.conn)
        check_plugin_param_type(self.plugin, "run", "stream_src", self.src)
        if self.explorer is not None:
            check_plugin_param_type(self.explorer, "metadata", "conn", self.conn)
            check_plugin_param_type(self.explorer, "data", "conn", self.conn)
        return self


@_api.dataclass(ApiErrorCode.API_16, config=_ARBITRARY)
class DestDef[D: Dest]:
    """Entry-point registration for a :class:`Dest` binding.

    :param type_: Wire-stable identifier, unique across *all* plugins
        (``"mysql-sql-out"``, ``"s3-file-out"``, ...).
    :param system: Human-readable name of the external system this
        connector targets (e.g. ``"MySQL"``, ``"AWS S3"``); shown by
        ``tdk connection types``.
    :param dest_version: Connector schema version.
    :param conn: Concrete :class:`Conn` subclass the destination
        binds to.
    :param dest: Concrete :class:`Dest` subclass this binding
        registers.
    :param cardinality: Callable returning the number of input
        slots ``dest`` declares (typically
        ``lambda d: len(d.tables)``).
    :param plugin: :class:`DestPlugin` subclass that implements
        ``write_out``.
    :param explorer: Optional :class:`Explorer` for tooling.
    :param icon: Path to the connector's icon, relative to the
        ``tabsdatak`` namespace root so it can be fetched directly with
        ``td_resource(icon)`` (e.g. ``"conn/mysql/resources/mysql.svg"``).
        The file lives in the plugin package's ``resources/`` folder.
        ``None`` when the connector ships no icon; the caller supplies a
        default.
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
        if self.explorer is not None:
            check_plugin_param_type(self.explorer, "metadata", "conn", self.conn)
            check_plugin_param_type(self.explorer, "data", "conn", self.conn)
        return self
