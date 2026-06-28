"""Public API for declaring tabsdata flows.

A flow is built from four decorated functions:

* ``@publisher`` -- batch pull from a :class:`Src` into one or
  more ``output_tables``.
* ``@stream_publisher`` -- long-lived pull from a :class:`StreamSrc`
  (CDC, log tail, queue tail) into ``output_tables``.
* ``@transformer`` -- read one or more ``input_tables`` and emit
  ``output_tables`` (no external connection).
* ``@subscriber`` -- batch push of ``input_tables`` to a
  :class:`Dest`.

Each decorator wraps the user function in an ``*Info`` descriptor
(``PublisherInfo`` / ``StreamPublisherInfo`` / ``TransformerInfo``
/ ``SubscriberInfo``) and the framework picks the descriptor up
during flow registration. Triggers (``CronTrigger`` /
``TablesTrigger`` / ``AllTablesTrigger``) declare *when* a function
fires; connections (:class:`Conn` / :class:`NamedConn`) and value
references (:class:`Var` / :class:`Secret`) declare the *where*
and the *credentials*.

The :class:`TrxCtx` interface is what the runtime hands to every
decorated function so it can read trx identifiers and persist
per-flow scratch state across invocations.
"""

import importlib
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Annotated, Callable, TypeAlias, Union

from pydantic import Field

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.error import ApiErrorCode
from tabsdatak.tableframe import TableFrame

# --- Decorated-function slot types ------------------------------------
#
# Every input parameter or output element of a ``@publisher`` /
# ``@stream_publisher`` / ``@transformer`` / ``@subscriber`` function
# must be annotated as one of these aliases. They make the nullability
# explicit:
#
#   * ``TableFrameSpec``  -- a single TableFrame slot. The runtime
#     may pass ``None`` (e.g. an upstream table did not produce a
#     batch for this trigger), so the slot must accept ``None``.
#
#   * ``TableFramesSpec`` -- an array of TableFrame slots. The runtime
#     may pass ``None`` for the whole list (the entire array slot is
#     empty) and may pass ``None`` for individual elements of the list
#     (some upstream tables had no batch), so both layers must accept
#     ``None``.
#
# The return is a fixed-length ``tuple`` with one ``TableFrameSpec``
# slot per output table. As a single-output convenience, a function
# that declares exactly one output table may return the lone
# ``TableFrameSpec`` bare, without the enclosing 1-tuple.
TableFrameSpec: TypeAlias = TableFrame | None
TableFramesSpec: TypeAlias = list[TableFrame | None] | None


# Output-table name in the local flow. Must match the per-flow
# table-name pattern (alphanumerics, underscores, leading letter).
TableNameSpec: TypeAlias = Annotated[str, _api.TABLE_NAME_CONSTRAINTS]
TableNamesSpec: TypeAlias = Annotated[
    list[TableNameSpec], _api.NON_EMPTY_LIST_CONSTRAINTS
]

# Cross-collection table reference, of shape ``"collection/table"``.
# ``TablesTrigger.tables`` uses this form so a function can fire on
# tables that live in a different collection.
CollTableNameSpec: TypeAlias = Annotated[str, _api.COLL_TABLE_NAME_CONSTRAINTS]
CollTableNamesSpec: TypeAlias = Annotated[
    list[CollTableNameSpec], _api.NON_EMPTY_LIST_CONSTRAINTS
]

# Cross-collection table reference with an optional version suffix, of
# shape ``"[collection/]table[@versions]"`` (e.g. ``people@HEAD~1..HEAD``).
# ``input_tables`` uses this form so a function can read a specific
# version, a range of versions, or a list of versions of a table.
CollTableVersionsNameSpec: TypeAlias = Annotated[
    str, _api.COLL_TABLE_VERSIONS_NAME_CONSTRAINTS
]
CollTableVersionsNamesSpec: TypeAlias = Annotated[
    list[CollTableVersionsNameSpec], _api.NON_EMPTY_LIST_CONSTRAINTS
]

# Five-field cron expression understood by ``croniter``.
CronMaskSpec: TypeAlias = Annotated[str, _api.CRON_MASK_CONSTRAINTS]


@_api.dataclass(ApiErrorCode.API_1)
class Trigger(ABC, _api.DescriptorBase):
    """Abstract base for the trigger family.

    Concrete triggers (:class:`CronTrigger`, :class:`TablesTrigger`,
    :class:`AllTablesTrigger`) live on a function's ``trigger_by``
    argument and control *when* the runtime fires it."""

    __kind__ = "trigger"


@_api.dataclass(ApiErrorCode.API_2)
class CronTrigger(Trigger):
    """Fire on a cron schedule.

    :param mask: Five-field cron expression (UTC).
    """

    mask: CronMaskSpec


@_api.dataclass(ApiErrorCode.API_3)
class TablesTrigger(Trigger):
    """Fire when one of a named set of upstream tables produces a
    new batch.

    :param tables: ``"collection/table"`` references to watch.
    """

    tables: CollTableNamesSpec


@_api.dataclass(ApiErrorCode.API_4)
class AllTablesTrigger(Trigger):
    """Fire when *any* upstream table the function reads from
    produces a new batch."""

    pass


# ``@publisher`` cannot use :class:`AllTablesTrigger` (publishers
# have no upstream tables to watch); the other two decorators do.
PublisherTriggerBySpec: TypeAlias = Union[CronTrigger, TablesTrigger]
TransformerTriggerBySpec: TypeAlias = Union[
    CronTrigger,
    TablesTrigger,
    AllTablesTrigger,
]
SubscriberTriggerBySpec: TypeAlias = Union[
    CronTrigger,
    TablesTrigger,
    AllTablesTrigger,
]

# stream-publisher fallback rollover window. Whole minutes; the runtime
# closes the current batch and starts a new one every
# ``fallback_rollover_mins`` minutes if the source's own rollover
# threshold has not fired. Lower-bounded at 1 (zero / negative would
# either spin or be undefined).
FlushIntervalSpec: TypeAlias = Annotated[int, Field(ge=1)]


class Var(_api.BaseRef):
    """Reference to a variable value resolved at flow run time via
    the framework's hydration pipeline.

    ``Var()`` is an *unhydrated* reference: the value is filled in by
    the hydration pipeline (its lookup key is the var-style form of
    the envelope's jsonpath, not a user-supplied name).
    ``Var("public")`` is an inline literal. Reading :attr:`value` on
    an unhydrated ``Var`` raises; on a literal it returns the value
    (after the optional ``_value_pattern`` check). A ``Var``
    serializes to the single string ``"var:<value>"`` (``"var:"``
    when unhydrated). Field annotations may pin a ``_value_pattern``
    regex (see :class:`~tabsdatak._api.VarValueConstraints`).
    """

    _prefix = "var:"
    _not_hydrated_error = ApiErrorCode.API_8


class Secret(_api.BaseRef):
    """Reference to a secret value resolved at flow run time via the
    framework's hydration pipeline.

    ``Secret()`` is an *unhydrated* reference; ``Secret("hunter2")``
    is an inline literal. Reading :attr:`value` on an unhydrated
    ``Secret`` raises; on a literal it returns the value (after the
    optional ``_value_pattern`` check). Inline values are redacted in
    ``repr``. A ``Secret`` serializes to the single string
    ``"secret:<value>"`` (``"secret:"`` when unhydrated). Field
    annotations may pin a ``_value_pattern`` regex (see
    :class:`~tabsdatak._api.SecretValueConstraints`).
    """

    _prefix = "secret:"
    _not_hydrated_error = ApiErrorCode.API_11

    def _repr_raw(self) -> str:
        # Never leak an inline secret value through repr / logs.
        return "'***redacted***'" if self._raw is not None else "None"


VarOrSecretSpec: TypeAlias = Union[Var, Secret]


class _Conn(ABC, _api.DescriptorBase):
    __kind__ = "connectionDef"


class Conn(_Conn, ABC):
    """Abstract base for every connection.

    Concrete subclasses live in the per-database connector
    packages (e.g. ``MySQLConn``, ``MySQLCdcConn``) and declare
    their own ``uri`` / ``credentials`` / ``conn_cfg`` fields."""

    __kind__ = "connectionDef"


# A connection slot resolved by *name* (looked up in the flow's
# named-connection registry). The name is a plain string literal.
ConnectionNameSpec: TypeAlias = Annotated[str, _api.NAMED_CONN_CONSTRAINTS]


@_api.dataclass(ApiErrorCode.API_13)
class NamedConn(Conn):
    """Reference to a connection by name; the runtime resolves the
    name to a concrete :class:`Conn` at flow run time.

    :param name: Connection identifier (a plain string) registered
        with the runtime.
    """

    __kind__ = "connectionRef"
    name: ConnectionNameSpec


# `__kind__` discriminator values for the descriptor-base JsonSerde
# envelopes. Centralised here so consumers (server-side dispatch,
# tests, migration tooling) compare against named constants instead
# of repeating string literals.
KIND_SOURCE_DEF = "sourceDef"
KIND_STREAM_SOURCE_DEF = "streamSourceDef"
KIND_DESTINATION_DEF = "destinationDef"
KIND_ON_TABLES_ACTION = "onTablesAction"


# Src / Dest / OnTablesAction stay as pure abstract classes (no dataclass
# decoration). Concrete fields live only on implementer subclasses, which
# carry their impl-layer wiring via a `SrcDef` / `streamSrcDef` / `DestDef`
# binding declared in `_<name>_plugin.py` (see `tabsdatak.spi`). The
# framework dataclasses that reference these bases as field types
# (`PublisherInfo.source: Src`, ...) opt into
# `arbitrary_types_allowed=True` so pydantic validates via isinstance.
class Src[C: Conn](ABC, _api.DescriptorBase):
    """Abstract batch source. Used as the ``source`` argument of
    ``@publisher``.
    """

    __kind__ = KIND_SOURCE_DEF


class StreamSrc[C: Conn](ABC, _api.DescriptorBase):
    """Abstract stream source. Used as the ``source`` argument of
    ``@stream_publisher``.
    """

    __kind__ = KIND_STREAM_SOURCE_DEF


class Dest[C: Conn](ABC, _api.DescriptorBase):
    """Abstract destination. Used as the ``destination`` argument
    of ``@subscriber``.
    """

    __kind__ = KIND_DESTINATION_DEF


class OnTablesAction(ABC, _api.JsonSerde):
    """Abstract base for declarative table-side-effect actions
    that fire alongside a function's normal output (e.g. publish a
    notification, write an audit row). The runtime executes the
    action only when the function's primary output succeeds."""

    __kind__ = KIND_ON_TABLES_ACTION


OnTablesActionSpec: TypeAlias = Annotated[
    list[OnTablesAction], _api.NON_EMPTY_LIST_CONSTRAINTS
]


def _info_cls(name: str):
    return getattr(importlib.import_module("tabsdatak._info"), name)


def publisher[C: Conn](
    conn: C,
    source: Src[C],
    output_tables: TableNamesSpec,
    trigger_by: PublisherTriggerBySpec | None = None,
    on_tables: OnTablesActionSpec | None = None,
) -> Callable:
    """Decorate a function as a batch publisher.

    The decorated function's signature is one parameter per input
    slot the source declares (mode ``single`` -> ``TableFrameSpec``,
    mode ``array`` -> ``TableFramesSpec``), followed by an
    *optional* trailing ``ctx: TrxCtx`` parameter. The runtime
    supplies a :class:`TrxCtx` when ``ctx`` is declared and skips
    it otherwise. No other extra parameters are allowed.

    :param source: The :class:`Src` to pull from.
    :param output_tables: Local table names this function emits;
        the function's return shape must match the cardinality
        of ``source``.
    :param trigger_by: Optional :class:`CronTrigger` /
        :class:`TablesTrigger`; if omitted the function fires only
        on manual run.
    :param on_tables: Optional declarative side-effects that fire
        alongside the function's primary output.
    """

    def _publisher(func):
        return _info_cls("PublisherInfo")(
            func, conn, source, output_tables, trigger_by, on_tables
        )

    return _publisher


def stream_publisher[C: Conn](
    conn: C,
    source: StreamSrc[C],
    flush_interval_mins: FlushIntervalSpec,
    output_tables: TableNamesSpec,
    on_tables: OnTablesActionSpec | None = None,
) -> Callable:
    """Decorate a function as a stream publisher.

    The decorated function's signature is one parameter per input
    slot the stream source declares (always array-mode --
    ``TableFramesSpec``). Unlike :func:`publisher` /
    :func:`transformer` / :func:`subscriber`, a trailing
    ``ctx: TrxCtx`` parameter is **not** allowed: stream publishers
    run inside the runtime's long-lived stream-plugin loop, not
    per-trx, so there is no :class:`TrxCtx` available at the call
    boundary.

    :param source: The :class:`StreamSrc` to consume.
    :param flush_interval_mins: Fallback rollover window: the
        runtime closes the current batch every N minutes if the
        source's own rollover threshold has not fired.
    :param output_tables: Local table names the function emits.
    :param on_tables: Optional declarative side-effects.
    """

    def _stream_publisher(func):
        return _info_cls("StreamPublisherInfo")(
            func,
            conn,
            source,
            flush_interval_mins,
            output_tables,
            on_tables,
        )

    return _stream_publisher


def transformer(
    input_tables: CollTableVersionsNamesSpec,
    output_tables: TableNamesSpec,
    trigger_by: TransformerTriggerBySpec | None = None,
    on_tables: OnTablesActionSpec | None = None,
) -> Callable:
    """Decorate a function as a transformer (table-to-table, no
    external connection).

    The decorated function's signature is one parameter per entry
    in ``input_tables`` (slot type accepts either
    :data:`TableFrameSpec` or :data:`TableFramesSpec`), followed by
    an *optional* trailing ``ctx: TrxCtx`` parameter. The runtime
    supplies a :class:`TrxCtx` when ``ctx`` is declared and skips
    it otherwise. No other extra parameters are allowed.

    :param input_tables: ``"collection/table"`` references to read.
    :param output_tables: Local table names this function emits.
    :param trigger_by: Optional :class:`CronTrigger` /
        :class:`TablesTrigger` / :class:`AllTablesTrigger`.
    :param on_tables: Optional declarative side-effects.
    """

    def _transformer(func):
        return _info_cls("TransformerInfo")(
            func, input_tables, output_tables, trigger_by, on_tables
        )

    return _transformer


def subscriber[C: Conn](
    conn: C,
    destination: Dest[C],
    input_tables: CollTableVersionsNamesSpec,
    trigger_by: SubscriberTriggerBySpec | None = None,
) -> Callable:
    """Decorate a function as a subscriber (push tables to an
    external destination).

    The decorated function's signature is one parameter per entry
    in ``input_tables`` (slot type :data:`TableFrameSpec`),
    followed by an *optional* trailing ``ctx: TrxCtx`` parameter.
    The runtime supplies a :class:`TrxCtx` when ``ctx`` is declared
    and skips it otherwise. No other extra parameters are allowed.

    :param destination: The :class:`Dest` to write to.
    :param input_tables: ``"collection/table"`` references to ship.
    :param trigger_by: Optional :class:`CronTrigger` /
        :class:`TablesTrigger` / :class:`AllTablesTrigger`.
    """

    def _subscriber(func):
        return _info_cls("SubscriberInfo")(
            func, conn, destination, input_tables, trigger_by
        )

    return _subscriber


# Per-flow scratch-key identifier. Plain string -- the framework
# packs all ``store_attr`` entries into a single ``user_namespace``
# Parquet cell, so the key never appears as a Parquet column name
# and doesn't need DNS-1123 / column-name constraints.
ContextKeySpec: TypeAlias = str

# Allowed per-flow scratch-value shapes. Primitives + flat lists /
# dicts whose values are also primitives. Round-trips through the
# state Parquet (see the initial-values codec). Heterogeneous values
# inside the same dict are fine (e.g. ``{"last_modified": "...",
# "min_id": 1234}``). ``Decimal`` is included so connectors can persist
# exact arbitrary-precision values (e.g. a decimal128 CDC cursor)
# without lossy promotion to ``float``.
_PRIM: TypeAlias = Union[str, int, float, bool, Decimal]
ContextValueSpec: TypeAlias = Union[_PRIM, list[_PRIM], dict[str, _PRIM]]


class TrxCtx(ABC):
    """Per-trx context handed by the runtime to every decorated
    function and connector plugin.

    Carries trx-level identifiers (``exec_plan_id``, ``trx_id``,
    ``exec_plan_ts``) plus a per-flow scratch store for state that
    needs to survive across invocations (``get_attr`` /
    ``store_attr``) and a metadata sink for trx-side observability
    (``add_metadata``).
    """

    @property
    @abstractmethod
    def exec_plan_id(self) -> str:
        """Unique id of the running execution plan."""

    @property
    @abstractmethod
    def trx_id(self) -> str:
        """Unique id of the current trx within the plan."""

    @property
    @abstractmethod
    def exec_plan_ts(self) -> str:
        """ISO-8601 timestamp of when the plan started."""

    @abstractmethod
    def get_attr(self, attr: ContextKeySpec) -> ContextValueSpec | None:
        """Read a per-flow scratch value (returns ``None`` if
        unset)."""

    @abstractmethod
    def store_attr(self, attr: ContextKeySpec, val: ContextValueSpec | None):
        """Write a per-flow scratch value -- persists across trx
        invocations within the same flow.

        Connectors may reserve namespaces under this scratch store
        for their own internal bookkeeping (``file-lastmod.*`` is
        the only one today). Writing to such names from user code
        can corrupt connector state -- pick names that don't
        collide with these prefixes.
        """

    @abstractmethod
    def add_metadata(self, attr: ContextKeySpec, val: ContextValueSpec | None):
        """Attach trx-level metadata for observability / lineage.
        Not durable across trx invocations."""
