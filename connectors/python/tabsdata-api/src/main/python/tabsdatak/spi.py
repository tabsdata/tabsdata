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
pieces -- :data:`TableFileSpec`, :class:`TableMode`,
:data:`StreamManifestSpec`, :class:`StageRef` -- are the data-shape
contracts that travel between framework and plugin during a trx.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Callable,
    Mapping,
    Type,
    TypeAlias,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import ConfigDict, model_validator

import tabsdatak._api as _api
from tabsdatak.api import (
    Conn,
    ContextKeySpec,
    ContextValueSpec,
    Dest,
    Src,
    StreamSrc,
    TrxCtx,
)
from tabsdatak.error import ApiErrorCode


# All in-flight tables travel between framework and plugin as Parquet
# files on disk (a plain ``pathlib.Path``). Plugins materialise rows to
# these paths during reads and consume them during writes.
TableFileSpec: TypeAlias = Path


class TableMode(Enum):
    """How a :class:`SrcPlugin` returns slot contents:

    * ``SINGLE`` -- exactly one parquet file per output slot.
    * ``ARRAY`` -- one or more parquet files per output slot
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


class SrcPlugin[S: Src](ABC):
    """Plugin implementing the read-side of a :class:`Src` subclass."""

    @abstractmethod
    def read_in(self, ctx: SrcPluginCtx, src: S) -> list[list[TableFileSpec]]:
        """Materialise ``src``'s output slots to parquet under
        ``ctx.work_dir``.

        Returns one inner list per output slot: when the binding's
        :class:`TableMode` is ``SINGLE`` each inner list has
        exactly one file; in ``ARRAY`` mode it may have more.
        """

    # if TableMode is SINGLE, inner list must always have 1 element.


FilesSpec: TypeAlias = Annotated[list[TableFileSpec], _api.NON_EMPTY_LIST_CONSTRAINTS]


# ``arbitrary_types_allowed=True`` escape hatch for the ``SrcDef`` /
# ``StreamSrcDef`` / ``DestDef`` dataclasses below, whose ``Type[...]``
# fields reference abstract classes (``SrcPlugin``, ``Explorer``, ...)
# that pydantic validates via issubclass / isinstance.
_ARBITRARY = ConfigDict(arbitrary_types_allowed=True)


# One stage call's payload: positional per-output-table parquet file
# lists the stream-source plugin hands to ``ctx.stage`` at flush time.
# Entry ``i`` holds the files contributed to output-table index ``i``
# this batch; an empty inner list means that table produced no files.
# Paths must live under :attr:`StreamSrcPluginCtx.work_dir`.
StreamManifestSpec: TypeAlias = Annotated[
    list[list[TableFileSpec]], _api.NON_EMPTY_LIST_CONSTRAINTS
]


class StageRef(ABC):
    """Opaque handle returned by :meth:`StreamSrcPluginCtx.stage`
    and consumed by :meth:`StreamSrcPluginCtx.commit`. Concrete
    shape is runtime-private."""


# Serialisable key/value bag a stream-source plugin persists on
# the trx context across invocations (e.g. resume-offset state).
StreamStateSpec: TypeAlias = Mapping[ContextKeySpec, ContextValueSpec]


class StreamSrcPluginCtx(PluginCtx, ABC):
    """Context handed to :meth:`StreamSrcPlugin.run`. Provides the
    plugin its long-lived control surface:

    * :meth:`is_active` -- loop predicate the plugin polls.
    * :meth:`stage` + :meth:`commit` -- two-phase batch publish.
    * :meth:`trigger` + :meth:`trigger_monitor` -- backpressure
      gate against downstream consumers.
    * :meth:`read_state` + :meth:`write_state` -- per-flow resume
      state (offsets, watermarks, ...).
    """

    @abstractmethod
    def is_active(self) -> bool:
        """``True`` while the runtime wants the plugin to keep
        polling. The plugin loop should exit (cleanly) on
        ``False``."""

    @abstractmethod
    def stage(self, manifest: StreamManifestSpec) -> StageRef:
        """Hand a batch to the runtime for staging: a positional list
        of per-output-table file lists (entry ``i`` -> output-table
        index ``i``; an empty inner list means no files for that table
        this batch). Returns an opaque :class:`StageRef` to pass to
        :meth:`commit`. Files must live under :attr:`work_dir`."""

    @abstractmethod
    def commit(self, stage_ref: StageRef):
        """Commit a previously-staged batch. The runtime publishes
        the manifest atomically and clears the work-dir slot."""

    @abstractmethod
    def trigger(self) -> bool:
        """Signal that an output batch is available. ``False``
        means downstream is back-pressuring -- the plugin should
        pause publishing until :meth:`trigger_monitor` clears."""

    @abstractmethod
    def trigger_monitor(self) -> bool:
        """Poll backpressure recovery. ``True`` means downstream is
        consuming again and the plugin may resume."""

    @abstractmethod
    def read_state(self) -> StreamStateSpec:
        """Read the persisted per-flow state the plugin wrote last
        run (e.g. a CDC offset)."""

    @abstractmethod
    def write_state(self, state: StreamStateSpec):
        """Persist per-flow state for the next run."""


class StreamSrcPlugin[SS: StreamSrc](ABC):
    """Plugin implementing the long-running side of a
    :class:`StreamSrc` subclass (CDC, log tail, queue tail)."""

    @abstractmethod
    def run(
        self,
        ctx: StreamSrcPluginCtx,
        stream_src: SS,
    ):
        """Drive the change stream until ``ctx.is_active()`` flips
        to ``False`` or a signal arrives. Implementations should
        stage + commit through ``ctx`` as batches complete."""


class DestContext(TrxCtx, PluginCtx, ABC):
    """Context handed to :meth:`DestPlugin.write_out` -- combines
    :class:`tabsdatak.api.TrxCtx` with :class:`PluginCtx`."""


class DestPlugin[D: Dest](ABC):
    """Plugin implementing the write-side of a :class:`Dest`
    subclass."""

    @abstractmethod
    def write_out(
        self,
        ctx: DestContext,
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
    scratch state via ``get_attr`` / ``store_attr``) with
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


class Explorer[C: Conn](ABC):
    """Optional connector capability for ad-hoc inspection of a
    live :class:`tabsdatak.api.Conn`: catalog metadata and arbitrary
    query execution. Used by tooling (UIs, schema diff), not by the
    flow runtime."""

    # TODO nail down this API.
    @abstractmethod
    def metadata(self, conn: C) -> Any:
        """Return the connection's catalog metadata (schemas,
        tables, columns)."""

    # TODO nail down this API.
    @abstractmethod
    def data(self, conn: C, query: str) -> Any:
        """Run ``query`` against ``conn`` and return the result
        rows. Connector-specific row shape."""


def flatten_union(hint: object) -> tuple[type, ...]:
    if get_origin(hint) is Annotated:
        hint = get_args(hint)[0]
    if get_origin(hint) is Union:
        out: list[type] = []
        for arg in get_args(hint):
            out.extend(flatten_union(arg))
        return tuple(out)
    # Unbound generic type parameters (PEP 695) reach here as TypeVars.
    # An unparametrised subclass like ``class _Src(Src)`` inherits
    # ``Src.conn: C`` where ``C`` is the TypeVar bound by ``Conn``;
    # collapse it to the bound so the rest of the validator can treat
    # it as the effective type.
    if isinstance(hint, TypeVar):
        bound = hint.__bound__
        if bound is not None:
            return (bound,)  # type: ignore[return-value]
    return (hint,)  # type: ignore[return-value]


def check_conn_field(cls: type, side: str, expected: type) -> None:
    """Validate the `conn` field on ``cls``.

    - ``cls`` must declare a ``conn`` field.
    - Every member of the ``conn`` annotation (after Union flattening)
      must be a ``Conn`` subclass.
    - ``expected`` (the concrete conn type the binding declares) must
      be a subclass of at least one of those members, so a value of
      type ``expected`` is assignable to the field.
    """
    try:
        hints = get_type_hints(cls)
    except Exception as exc:
        raise ValueError(
            f"{cls.__qualname__}: cannot resolve type hints: {exc}"
        ) from exc
    if "conn" not in hints:
        raise ValueError(f"{cls.__qualname__}: missing required `conn` field")
    members = flatten_union(hints["conn"])
    if not members:
        raise ValueError(f"{cls.__qualname__}.conn: empty type annotation")
    bad = [m for m in members if not (isinstance(m, type) and issubclass(m, Conn))]
    if bad:
        names = ", ".join(getattr(m, "__qualname__", repr(m)) for m in bad)
        raise ValueError(
            f"{cls.__qualname__}.conn: every member of the type must be "
            f"a subclass of Conn ({side} side); offending: {names}"
        )
    if not any(issubclass(expected, m) for m in members):
        raise ValueError(
            f"{cls.__qualname__}.conn: must accept "
            f"{expected.__qualname__} ({side} side); "
            f"current annotation members: "
            f"{', '.join(m.__qualname__ for m in members)}"
        )


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
class SrcDef[C: Conn, S: Src]:
    """Entry-point registration for a batch :class:`Src` binding.

    :param src_type: Wire-stable identifier (``"mysql"``,
        ``"postgres"``, ...). Used by the registry to look up the
        binding when loading a serialised flow.
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
    """

    src_type: str
    src_version: str
    conn: Type[C]
    src: Type[S]
    table_mode: TableMode
    cardinality: Callable[[S], int]
    plugin: Type[SrcPlugin]
    explorer: Type[Explorer] | None

    @model_validator(mode="after")
    def _validate(self) -> "SrcDef":
        check_conn_field(self.src, "source", self.conn)
        check_plugin_param_type(self.plugin, "read_in", "src", self.src)
        if self.explorer is not None:
            check_plugin_param_type(self.explorer, "metadata", "conn", self.conn)
            check_plugin_param_type(self.explorer, "data", "conn", self.conn)
        return self


@_api.dataclass(ApiErrorCode.API_15, config=_ARBITRARY)
class StreamSrcDef[C: Conn, S: StreamSrc]:
    """Entry-point registration for a :class:`StreamSrc` binding.

    :param src_type: Wire-stable identifier (``"mysql-cdc"``,
        ``"postgres-cdc"``, ``"db2-cdc"``, ...).
    :param src_version: Connector schema version.
    :param conn: Concrete :class:`Conn` subclass the stream source
        binds to.
    :param src: Concrete :class:`StreamSrc` subclass this binding
        registers.
    :param cardinality: Callable returning the number of output
        slots ``src`` declares (typically
        ``lambda s: len(s.tables)``).
    :param uses_manifest: ``True`` when the plugin batches via
        :meth:`StreamSrcPluginCtx.stage` + ``commit`` (the CDC
        sources all set this to ``True``).
    :param plugin: :class:`StreamSrcPlugin` subclass that
        implements ``run``.
    :param explorer: Optional :class:`Explorer` for tooling.
    """

    src_type: str
    src_version: str
    conn: Type[C]
    src: Type[S]
    cardinality: Callable[[S], int]
    uses_manifest: bool
    plugin: Type[StreamSrcPlugin]
    explorer: Type[Explorer] | None

    @model_validator(mode="after")
    def _validate(self) -> "StreamSrcDef":
        check_conn_field(self.src, "stream_source", self.conn)
        # ``StreamSrcPlugin`` defines ``run(ctx, stream_src)``, not the
        # batch-mode ``read_in(ctx, src)`` from ``SrcPlugin``.
        check_plugin_param_type(self.plugin, "run", "stream_src", self.src)
        if self.explorer is not None:
            check_plugin_param_type(self.explorer, "metadata", "conn", self.conn)
            check_plugin_param_type(self.explorer, "data", "conn", self.conn)
        return self


@_api.dataclass(ApiErrorCode.API_16, config=_ARBITRARY)
class DestDef[C: Conn, D: Dest]:
    """Entry-point registration for a :class:`Dest` binding.

    :param dest_type: Wire-stable identifier (``"mysql"``,
        ``"postgres"``, ...).
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
    """

    dest_type: str
    dest_version: str
    conn: Type[C]
    dest: Type[D]
    cardinality: Callable[[D], int]
    plugin: Type[DestPlugin]
    explorer: Type[Explorer] | None

    @model_validator(mode="after")
    def _validate(self) -> "DestDef":
        check_conn_field(self.dest, "destination", self.conn)
        check_plugin_param_type(self.plugin, "write_out", "dest", self.dest)
        if self.explorer is not None:
            check_plugin_param_type(self.explorer, "metadata", "conn", self.conn)
            check_plugin_param_type(self.explorer, "data", "conn", self.conn)
        return self
