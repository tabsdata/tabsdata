#
# Copyright 2026 Tabsdata Inc.
#

"""Public API for declaring tabsdata flows.

A flow is built from four decorated functions:

* `@publisher` -- batch pull from a `Src` into one or more
  `output_tables`.
* `@stream_publisher` -- long-lived pull from a `StreamSrc`
  (CDC, log tail, queue tail) into `output_tables`.
* `@transformer` -- read one or more `input_tables` and emit
  `output_tables` (no external connection).
* `@subscriber` -- batch push of `input_tables` to a `Dest`.

Each decorator wraps the user function in an `*Info` descriptor
(`PublisherInfo` / `StreamPublisherInfo` / `TransformerInfo`
/ `SubscriberInfo`) and the framework picks the descriptor up
during flow registration. Triggers (`CronTrigger` /
`TablesTrigger` / `AllTablesTrigger`) declare *when* a function
fires; the `source` / `destination` declares the *where*. The
*connection* (and its credentials) is not declared on the function
-- it is the connection registered on the function's collection,
which the runtime resolves and hands to the connector at run time.

The `TrxCtx` interface is what the runtime hands to every
decorated function so it can read trx identifiers and persist
per-flow scratch state across invocations.
"""

import importlib
import warnings
from abc import ABC, abstractmethod
from typing import Annotated, Callable, TypeAlias, Union

from pydantic import Field, model_validator

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.error import ApiErrorCode
from tabsdatak.tableframe import TableFrame

# --- Decorated-function slot types ------------------------------------
#
# Every input parameter or output element of a `@publisher` /
# `@stream_publisher` / `@transformer` / `@subscriber` function
# must be annotated as one of these aliases. They make the nullability
# explicit:
#
#   * `TableFrameSpec`  -- a single TableFrame slot. The runtime
#     may pass `None` (e.g. an upstream table did not produce a
#     batch for this trigger), so the slot must accept `None`.
#
#   * `TableFramesSpec` -- an array of TableFrame slots. The runtime
#     may pass `None` for the whole list (the entire array slot is
#     empty) and may pass `None` for individual elements of the list
#     (some upstream tables had no batch), so both layers must accept
#     `None`.
#
# The return is a fixed-length `tuple` with one `TableFrameSpec`
# slot per output table. As a single-output convenience, a function
# that declares exactly one output table may return the lone
# `TableFrameSpec` bare, without the enclosing 1-tuple.
TableFrameSpec: TypeAlias = TableFrame | None
TableFramesSpec: TypeAlias = list[TableFrame | None] | None


# Output-table name in the local flow. Must match the per-flow
# table-name pattern (alphanumerics, underscores, leading letter).
TableNameSpec: TypeAlias = Annotated[str, _api.TABLE_NAME_VALIDATION]
TableNamesSpec: TypeAlias = Annotated[list[TableNameSpec], _api.NON_EMPTY_LIST_FIELD]

# Cross-collection table reference, of shape `"collection/table"`.
# `TablesTrigger.tables` uses this form so a function can fire on
# tables that live in a different collection.
CollTableNameSpec: TypeAlias = Annotated[str, _api.COLL_TABLE_NAME_VALIDATION]
CollTableNamesSpec: TypeAlias = Annotated[
    list[CollTableNameSpec], _api.NON_EMPTY_LIST_FIELD
]

# Cross-collection table reference with an optional version suffix, of
# shape `"[collection/]table[@versions]"` (e.g. `people@HEAD~1..HEAD`).
# `input_tables` uses this form so a function can read a specific
# version, a range of versions, or a list of versions of a table.
CollTableVersionsNameSpec: TypeAlias = Annotated[
    str, _api.COLL_TABLE_VERSIONS_NAME_VALIDATION
]
CollTableVersionsNamesSpec: TypeAlias = Annotated[
    list[CollTableVersionsNameSpec], _api.NON_EMPTY_LIST_FIELD
]

# Five-field cron expression understood by `croniter`.
CronMaskSpec: TypeAlias = Annotated[str, _api.CRON_MASK_VALIDATION]


@_api.dataclass(ApiErrorCode.API_1)
class Trigger(ABC, _api.DescriptorBase):
    """Abstract base for the trigger family.

    :categories: trigger

    Concrete triggers (`CronTrigger`, `TablesTrigger`,
    `AllTablesTrigger`) live on a function's `trigger_by` argument
    and control *when* the runtime fires it.
    """

    __kind__ = "trigger"


@_api.dataclass(ApiErrorCode.API_2)
class CronTrigger(Trigger):
    """Fire on a cron schedule.

    :categories: trigger

    Args:
        mask: Five-field cron expression (UTC).
    """

    mask: CronMaskSpec


@_api.dataclass(ApiErrorCode.API_3)
class TablesTrigger(Trigger):
    """Fire when one of a named set of upstream tables produces a new
    batch.

    :categories: trigger

    Args:
        tables: `collection/table` references to watch.
    """

    tables: CollTableNamesSpec


@_api.dataclass(ApiErrorCode.API_4)
class AllTablesTrigger(Trigger):
    """Fire when *any* upstream table the function reads from produces
    a new batch.

    :categories: trigger
    """

    pass


# `@publisher` cannot use `AllTablesTrigger` (publishers
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

# Deprecated: the stream-publisher rollover window in whole minutes, now
# `batch_max_secs`. Kept so a definition that still names it is converted,
# with a warning, rather than refused.
FlushIntervalSpec: TypeAlias = Annotated[int, Field(ge=1)]

BatchSecondsSpec: TypeAlias = Annotated[int, Field(ge=0)]
BatchRowsSpec: TypeAlias = Annotated[int, Field(ge=1)]
BatchBytesSpec: TypeAlias = Annotated[int, Field(ge=1)]


@_api.dataclass(ApiErrorCode.API_29)
class BatchThresholds:
    """How a stream publisher's batches are bounded in time and size.

    :categories: functions

    A batch is the set of files one function run receives. Batches are
    spaced at least `min_secs` apart; with data waiting, one is staged at
    most `max_secs` after the previous one, or earlier once it holds
    `max_rows` rows or `max_bytes` bytes; with nothing waiting, an empty
    one is staged `idle_secs` after the previous one, so the function runs
    on a cadence while the source is quiet. A `None` is no limit, and at
    least one of `max_secs`, `max_rows` and `max_bytes` is set, so data
    never waits forever.

    Args:
        min_secs: Shortest interval between two batches, 60 by default and
            never less.
        max_secs: Longest interval between two batches while data waits.
        idle_secs: Longest interval between two batches while no data
            waits, `None` (the default) for no empty batches.
        max_rows: Rows that stage a batch as soon as `min_secs` allows.
        max_bytes: Bytes that stage a batch as soon as `min_secs` allows.
    """

    min_secs: BatchSecondsSpec = _api.BATCH_FLOOR_SECS
    max_secs: BatchSecondsSpec | None = None
    idle_secs: BatchSecondsSpec | None = None
    max_rows: BatchRowsSpec | None = None
    max_bytes: BatchBytesSpec | None = None

    @model_validator(mode="after")
    def _ordered_and_bounded(self):
        if self.max_secs is None and self.max_rows is None and self.max_bytes is None:
            raise ValueError(
                "one of batch_max_secs, batch_max_rows and batch_max_bytes must be "
                "set, or no batch would ever be staged"
            )
        if self.max_secs is not None and self.max_secs < self.min_secs:
            raise ValueError(
                f"batch_max_secs ({self.max_secs}) must be at least batch_min_secs "
                f"({self.min_secs})"
            )
        if self.idle_secs is not None:
            floor = self.max_secs if self.max_secs is not None else self.min_secs
            if self.idle_secs < floor:
                raise ValueError(
                    f"batch_idle_secs ({self.idle_secs}) must be at least "
                    f"batch_max_secs or, without it, batch_min_secs ({floor})"
                )
        return self


@_api.dataclass(ApiErrorCode.API_29)
class BufferThresholds:
    """How a stream publisher's in-memory buffer is bounded before it is
    written to a file: `max_rows` rows, `max_bytes` bytes (`None` for no
    byte limit) or `max_secs` since the last write.

    :categories: functions
    """

    max_rows: BatchRowsSpec
    max_bytes: BatchBytesSpec | None
    max_secs: BatchSecondsSpec


@_api.dataclass(ApiErrorCode.API_29)
class PollThresholds:
    """How a stream publisher polls its source: `max_secs` is the budget of
    one ingestion iteration, `idle_secs` the wait before the next one when
    the iteration read nothing.

    :categories: functions
    """

    max_secs: BatchSecondsSpec
    idle_secs: BatchSecondsSpec


@_api.dataclass(ApiErrorCode.API_29)
class StreamThresholds:
    """The thresholds of a stream publisher's data flow, one substructure
    per level: `batch` (what a function run receives), `buffer` (the
    in-memory buffer written to files) and `poll` (the source polling).
    The batch thresholds come from the `stream_publisher` definition;
    the others are `None` there and filled in by the connector from its
    own settings until they move to the definition as well.

    :categories: functions
    """

    batch: BatchThresholds
    buffer: BufferThresholds | None = None
    poll: PollThresholds | None = None


class Secret(_api._Secret):
    """Reference to a secret value, used for credential fields.

    :categories: credentials

    A `Secret` is either *hydrated* -- `Secret("hunter2")`, whose
    `value()` returns the value -- or *dehydrated* -- `Secret()`,
    whose `value()` raises. `None` is the dehydrated sentinel and is
    never a valid value. It always serializes to the redacted marker
    `":secret:"`, so the value never lands in the serialized form and
    `str(secret)` is always redacted.
    """

    # devguide-ref: secret hydration & serialization

    def __init__(self, value: str | None = None):
        super().__init__(value)


StrOrSecretSpec: TypeAlias = Annotated[Union[str, Secret], _api.StrOrSecretHandler()]
"""A connector field that accepts either a plain string or a `Secret`.

:categories: credentials

Annotate a connection/source/destination field with it directly.

Examples:
    .. code-block:: python

        class MyConn(Conn):
            bucket: StrOrSecretSpec
Each value carries a marker so the two shapes are unambiguous on the wire:

* `"str:<value>"`    -- a plain string (`<value>` may be empty);
* `"secret:<value>"` -- a secret carrying `<value>`;
* `":secret:"`       -- a secret with no value yet (resolved at run time).

An unmarked string is rejected. After validation a plain arm is a `str` and
a secret arm a `Secret`, so read the value with a normal `str` or with
`Secret.value()` respectively. Connector code that must resolve either arm to
a concrete value uses the connector-common `_convert` helper.
"""


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
class Src(ABC, _api.DescriptorBase):
    """Abstract batch source.

    :categories: source

    Passed as the `source` argument of `@publisher`. Concrete
    subclasses live in the connector packages (e.g. `S3Src`,
    `PostgresSrc`) and carry only per-flow config (paths, queries,
    ...). The connection and its credentials are not set here -- they
    live on the connection registered on the function's collection.
    """

    __kind__ = KIND_SOURCE_DEF


class StreamSrc(ABC, _api.DescriptorBase):
    """Abstract stream source.

    :categories: source

    Passed as the `source` argument of `@stream_publisher`.
    Concrete subclasses (e.g. `PostgresCdcStreamSrc`) drive a
    long-lived change stream. As with `Src`, the connection and
    credentials live on the collection's registered connection, not on
    the subclass.
    """

    __kind__ = KIND_STREAM_SOURCE_DEF


class Dest(ABC, _api.DescriptorBase):
    """Abstract destination.

    :categories: destination

    Passed as the `destination` argument of `@subscriber`.
    Concrete subclasses live in the connector packages (e.g.
    `S3Dest`, `PostgresDest`) and carry only per-flow config. The
    connection and credentials live on the collection's registered
    connection, not on the subclass.
    """

    __kind__ = KIND_DESTINATION_DEF


class OnTablesAction(ABC, _api.JsonSerde):
    """Abstract base for declarative table-side-effect actions.

    An action fires alongside a function's normal output (e.g. publish
    a notification, write an audit row) and only when the function's
    primary output succeeds. Passed via the `on_tables` argument of
    `@publisher` / `@stream_publisher` / `@transformer`.
    """

    __kind__ = KIND_ON_TABLES_ACTION


OnTablesActionSpec: TypeAlias = Annotated[
    list[OnTablesAction], _api.NON_EMPTY_LIST_FIELD
]


def _info_cls(name: str):
    return getattr(importlib.import_module("tabsdatak._info"), name)


def publisher(
    source: Src,
    output_tables: TableNamesSpec,
    trigger_by: PublisherTriggerBySpec | None = None,
    on_tables: OnTablesActionSpec | None = None,
) -> Callable:
    """Decorate a function as a batch publisher.

    :categories: functions

    Pulls from `source` and emits `output_tables`. The function
    takes one parameter per source slot -- `TableFrameSpec` (single
    mode) or `TableFramesSpec` (array mode) -- plus an *optional*
    trailing `ctx: TrxCtx`, and returns one `TableFrame` per output
    table (a lone output may be returned bare rather than as a 1-tuple).
    A slot may be `None` when an upstream batch is absent.

    Args:
        source: The `Src` to pull from; its slot cardinality fixes
            the parameter count.
        output_tables: Non-empty list of local table names to emit, one
            per return slot. Each name must start with a letter and use
            only letters, digits and underscores (max 64 chars).
        trigger_by: Optional `CronTrigger` / `TablesTrigger`; if
            omitted the function fires only on a manual run.
        on_tables: Optional list of `OnTablesAction` side-effects.

    Returns:
        The decorator that wraps the function as a publisher descriptor.

    Raises:
        ApiException: If the function signature does not match the
            source slot count / `output_tables` count.
    """

    def _publisher(func):
        return _info_cls("PublisherInfo")(
            func, source, output_tables, trigger_by, on_tables
        )

    return _publisher


def stream_publisher(
    source: StreamSrc,
    output_tables: TableNamesSpec,
    *,
    batch_min_secs: int | None = None,
    batch_max_secs: int | None = None,
    batch_idle_secs: int | None = None,
    batch_max_rows: int | None = None,
    batch_max_bytes: int | None = None,
    on_tables: OnTablesActionSpec | None = None,
    flush_interval_mins: FlushIntervalSpec | None = None,
) -> Callable:
    """Decorate a function as a stream publisher.

    :categories: functions

    Consumes a long-lived change stream (CDC, log/queue tail). The
    function takes one `TableFramesSpec` parameter per source slot
    (always array mode) and returns one `TableFrame` per output
    table. Unlike the other decorators it must **not** declare a
    `ctx` parameter -- it runs inside the runtime's stream loop, not
    per-trx, so no `TrxCtx` exists at the call boundary.

    The batch thresholds bound the batches the function receives, the
    same way for every stream source (see `BatchThresholds`): at least one
    of `batch_max_secs`, `batch_max_rows` and `batch_max_bytes` is required.

    Args:
        source: The `StreamSrc` to consume.
        output_tables: Non-empty list of local table names to emit, one
            per return slot (valid table names, see `publisher`).
        batch_min_secs: Shortest interval between two batches, 60 by
            default and never less.
        batch_max_secs: Longest interval between two batches while data
            waits; `None` (the default) for no time limit.
        batch_idle_secs: Longest interval between two batches while no
            data waits; `None` (the default) for no empty batches.
        batch_max_rows: Rows that stage a batch; `None` (the default) for
            no limit.
        batch_max_bytes: Bytes that stage a batch; `None` (the default)
            for no limit.
        on_tables: Optional list of `OnTablesAction` side-effects.
        flush_interval_mins: Deprecated, replaced by `batch_max_secs`:
            taken as `batch_max_secs = flush_interval_mins * 60`, with a
            warning, and refused next to any batch setting.

    Returns:
        The decorator that wraps the function as a stream-publisher
        descriptor.

    Raises:
        ApiException: If the function signature is invalid (e.g. it
            declares a `ctx` parameter), if the batch thresholds are not
            consistent, or if `flush_interval_mins` is given next to
            a batch threshold.
    """
    thresholds = StreamThresholds(
        batch=_batch_thresholds(
            min_secs=batch_min_secs,
            max_secs=batch_max_secs,
            idle_secs=batch_idle_secs,
            max_rows=batch_max_rows,
            max_bytes=batch_max_bytes,
            flush_interval_mins=flush_interval_mins,
        )
    )

    def _stream_publisher(func):
        return _info_cls("StreamPublisherInfo")(
            func=func,
            source=source,
            thresholds=thresholds,
            output_tables=output_tables,
            on_tables=on_tables,
            flush_interval_mins=flush_interval_mins,
        )

    return _stream_publisher


def _batch_thresholds(
    *,
    min_secs: int | None,
    max_secs: int | None,
    idle_secs: int | None,
    max_rows: int | None,
    max_bytes: int | None,
    flush_interval_mins: int | None,
) -> BatchThresholds:
    """The batch thresholds a `stream_publisher` definition gives, with the
    deprecated `flush_interval_mins` taken as `max_secs` and the seconds
    read through the floor's test backdoor."""
    if flush_interval_mins is not None:
        given = (min_secs, max_secs, idle_secs, max_rows, max_bytes)
        if any(value is not None for value in given):
            raise ApiErrorCode.API_30.exception()
        warnings.warn(
            "flush_interval_mins is deprecated; set batch_max_secs instead",
            DeprecationWarning,
            stacklevel=3,
        )
        max_secs = flush_interval_mins * 60
    try:
        return BatchThresholds(
            min_secs=(
                _api.decode_seconds("batch_min_secs", min_secs)
                if min_secs is not None
                else _api.BATCH_FLOOR_SECS
            ),
            max_secs=(
                _api.decode_seconds("batch_max_secs", max_secs)
                if max_secs is not None
                else None
            ),
            idle_secs=(
                _api.decode_seconds("batch_idle_secs", idle_secs)
                if idle_secs is not None
                else None
            ),
            max_rows=max_rows,
            max_bytes=max_bytes,
        )
    except ValueError as error:
        raise ApiErrorCode.API_29.exception(cause=error)


def transformer(
    input_tables: CollTableVersionsNamesSpec,
    output_tables: TableNamesSpec,
    trigger_by: TransformerTriggerBySpec | None = None,
    on_tables: OnTablesActionSpec | None = None,
) -> Callable:
    """Decorate a function as a transformer (table-to-table, no
    external connection).

    :categories: functions

    Reads `input_tables` and emits `output_tables`. The function
    takes one parameter per input table (`TableFrameSpec` or
    `TableFramesSpec`) plus an *optional* trailing `ctx: TrxCtx`,
    and returns one `TableFrame` per output table. Any input slot may
    be `None` when the upstream table produced no batch.

    Args:
        input_tables: Non-empty list of `[collection/]table[@versions]`
            references to read (`collection/` may be omitted for a
            same-collection table; `@versions` selects a version, a
            `v..v` range, a comma-separated list, or `NEW`).
        output_tables: Non-empty list of local table names to emit, one
            per return slot (valid table names, see `publisher`).
        trigger_by: Optional `CronTrigger` / `TablesTrigger` /
            `AllTablesTrigger`; if omitted the function fires only on
            a manual run.
        on_tables: Optional list of `OnTablesAction` side-effects.

    Returns:
        The decorator that wraps the function as a transformer
        descriptor.

    Raises:
        ApiException: If the function signature does not match the
            `input_tables` / `output_tables` counts.
    """

    def _transformer(func):
        return _info_cls("TransformerInfo")(
            func, input_tables, output_tables, trigger_by, on_tables
        )

    return _transformer


def subscriber(
    destination: Dest,
    input_tables: CollTableVersionsNamesSpec,
    trigger_by: SubscriberTriggerBySpec | None = None,
) -> Callable:
    """Decorate a function as a subscriber (push tables to an external
    destination).

    :categories: functions

    Reads `input_tables` and writes to `destination`. The function
    takes one `TableFrameSpec` parameter per input table plus an
    *optional* trailing `ctx: TrxCtx`, and returns one `TableFrame`
    per destination slot -- that return value is what gets written;
    return `None` for a slot to write nothing.

    Args:
        destination: The `Dest` to write to; its slot cardinality
            fixes the number of return slots.
        input_tables: Non-empty list of `[collection/]table[@versions]`
            references to ship (same form as `transformer`).
        trigger_by: Optional `CronTrigger` / `TablesTrigger` /
            `AllTablesTrigger`; if omitted the function fires only on
            a manual run.

    Returns:
        The decorator that wraps the function as a subscriber
        descriptor.

    Raises:
        ApiException: If the function signature does not match the
            `input_tables` count / destination slot count.
    """

    def _subscriber(func):
        return _info_cls("SubscriberInfo")(func, destination, input_tables, trigger_by)

    return _subscriber


# Per-flow scratch-key identifier. Plain string -- the framework
# packs all `set_attr` entries into a single `user_namespace`
# Parquet cell, so the key never appears as a Parquet column name
# and doesn't need DNS-1123 / column-name constraints.
ContextKeySpec: TypeAlias = str

# The framework's basic scalar value type: the JSON-scalar set shared by
# the per-flow scratch store (`get_attr` / `set_attr`), connector
# seed dicts, and stream resume state. Deliberately excludes `Decimal`
# and any nested (list / dict) shape -- a scratch value is a single
# scalar. A connector that needs arbitrary precision serialises it to
# `str` itself (see the CDC `(seg, ord)` cursor). Round-trips
# losslessly through the state Parquet (see the initial-values codec).
BasicTypeSpec: TypeAlias = Union[str, int, float, bool]

# A flat `str`-keyed dict of `BasicTypeSpec` values -- the shape
# used for `{name: value}` seed dicts (e.g. a SQL connector's
# `initial_values`) and for stream resume state.
BasicDictSpec: TypeAlias = dict[str, BasicTypeSpec]


class TrxCtx(ABC):
    """Per-trx context handed by the runtime to every decorated
    function and connector plugin.

    A function receives it by declaring a trailing `ctx: TrxCtx`
    parameter (not allowed on `@stream_publisher`). Carries trx-level
    identifiers (`exec_plan_id`, `trx_id`, `exec_plan_ts`), a
    per-flow scratch store that survives across invocations
    (`get_attr` / `set_attr`), and a metadata sink
    (`add_metadata`).
    """

    # devguide-ref: TrxCtx state semantics

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
    def get_attr(self, attr: ContextKeySpec) -> BasicTypeSpec | None:
        """Read a per-flow scratch value (returns `None` if
        unset)."""

    @abstractmethod
    def set_attr(self, attr: ContextKeySpec, val: BasicTypeSpec | None):
        """Write a per-flow scratch value -- persists across trx
        invocations within the same flow.

        Connectors may reserve namespaces under this scratch store
        for their own internal bookkeeping (`file-lastmod.*` is
        the only one today). Writing to such names from user code
        can corrupt connector state -- pick names that don't
        collide with these prefixes.
        """

    @abstractmethod
    def add_metadata(self, attr: ContextKeySpec, val: BasicTypeSpec | None):
        """Attach trx-level metadata for observability / lineage.
        Not durable across trx invocations."""
