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
`TablesTrigger` / `AllTablesTrigger`) declare
*when* a function fires; the `source` / `destination` declares the
*where*. The *connection* (and its credentials) is not declared on
the function -- it is the connection registered on the function's
collection, which the runtime resolves and hands to the connector at
run time.

The `FunctionCtx` interface (`TransactionCtx` + `FlowCtx`) is what the
runtime hands to every decorated function so it can read trx and
flow identifiers and persist per-flow attributes across
invocations.
"""

import importlib
import warnings
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Annotated, Callable, TypeAlias, Union

from pydantic import Field, model_validator

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak._legacy import FlowCtxLegacy, TransactionCtxLegacy, module_getattr
from tabsdatak.error import ApiErrorCode
from tabsdatak.tableframe import TableFrame

# --- Decorated-function slot types ------------------------------------
#
# Every input parameter or output element of a `@publisher` /
# `@stream_publisher` / `@transformer` / `@subscriber` function
# must be annotated as one of these aliases:
#
#   * `TableFrameSpec`  -- a single TableFrame slot. A source slot that
#     found no new data is an empty frame with the columns of the last
#     run that had data, and `None` while no run has had data, unless
#     the source types the columns itself (a SQL query, a Salesforce
#     sObject). A table input is `None` while its table has no version
#     with data yet.
#
#   * `TableFramesSpec` -- an array of TableFrame slots. A source slot
#     follows the same rule, with `[]` for nothing; a file source with
#     `incremental=False` hands `[]` for a path that takes no new file.
#     A table input is `[]` when no version
#     is in range (an `@NEW` read with no new version), and an element is
#     `None` for a version from before the table had data.
#
# The return is a fixed-length `tuple` with one `TableFrameSpec`
# slot per output table. As a single-output convenience, a function
# that declares exactly one output table may return the lone
# `TableFrameSpec` bare, without the enclosing 1-tuple. An output
# returned as `None` keeps its table's current data: the new version
# stands for the previous one, and an `@NEW` reader receives that data
# again. An empty frame is a version with no rows.
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
CollTableNamesSpec: TypeAlias = list[CollTableNameSpec]

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

CronMaskSpec: TypeAlias = Annotated[str, _api.CRON_MASK_VALIDATION]


@_api.dataclass(ApiErrorCode.API_1)
class Trigger(ABC, _api.DescriptorBase):
    """Abstract base for the trigger family.

    :categories: trigger

    Concrete triggers (`CronTrigger`, `TablesTrigger`,
    `AllTablesTrigger`) live on a function's
    `trigger_by` argument and control *when* the runtime fires it.
    Any function can also be run manually, whatever its trigger.
    """

    __kind__ = "trigger"


@_api.dataclass(ApiErrorCode.API_2)
class CronTrigger(Trigger):
    """Fire on a cron schedule.

    :categories: trigger

    Args:
        mask: A Kubernetes CronJob schedule: five fields, one of
            `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`,
            `@midnight`, `@hourly`, or `@every <duration>`. Always UTC.
    """

    mask: CronMaskSpec


@_api.dataclass(ApiErrorCode.API_3)
class TablesTrigger(Trigger):
    """Fire on a new version of any of the named tables.

    :categories: trigger

    Args:
        tables: `[collection/]table` references to watch; they need not
            be inputs of the function. An empty list never fires: the
            function runs only when run manually.
    """

    tables: CollTableNamesSpec


@_api.dataclass(ApiErrorCode.API_4)
class AllTablesTrigger(Trigger):
    """Fire on a new version of any table the function reads, other
    than the ones it writes. The default: the same as `trigger_by=None`.

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

    def _materialized(self) -> dict[str, str]:
        """The options a write applies once those left out are resolved, by
        name, for registration to report; empty where nothing is resolved."""
        return {}


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
    trailing `ctx: TransactionCtx`, and returns one `TableFrame` per output
    table (a lone output may be returned bare rather than as a 1-tuple).
    When a run finds no new data, a source hands an empty frame with the
    columns of the last run that had data: publishing it writes a version
    with no rows. A file source with `incremental=False` hands nothing
    instead (`[]`), meaning nothing changed: `concat` of it is `None`, and
    returning `None` keeps the table's data. A slot is `None` (`[]` for a
    list) while no run has had data for it, unless the source types the
    columns itself (a SQL query, a Salesforce sObject).

    A publisher is one read of its source, and runs as one unit: its
    outputs commit in the same transaction or not at all.

    Several outputs are annotated as a fixed-length tuple, one element
    per output; `tuple[TableFrameSpec, ...]` is refused at import.

    Args:
        source: The `Src` to pull from; its slot cardinality fixes
            the parameter count.
        output_tables: Non-empty list of local table names to emit, one
            per return slot. Each name must start with a letter and use
            only letters, digits and underscores (max 64 chars).
        trigger_by: When the function runs on its own:

            * `None` (default) or `TablesTrigger(tables=[])`: never;
              only when run manually.
            * `TablesTrigger(tables=[...])`: on a new version of any of
              those tables.
            * `CronTrigger(mask=...)`: on the schedule.
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
    (always array mode) plus an *optional* trailing
    `ctx: TransactionCtx`, and returns one `TableFrame` per output
    table. The function runs once per batch, in the transaction that
    commits it, so `ctx` is that transaction's context. A slot with no
    data in the batch holds one empty frame with the columns of the last
    batch that had data, and is `[]` while no batch has had data for it.

    Each batch is one unit: its outputs commit in the same transaction
    or not at all, so the tables of one stream advance together.

    Several outputs are annotated as a fixed-length tuple, one element
    per output; `tuple[TableFrameSpec, ...]` is refused at import.

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
        ApiException: If the function signature is invalid, if the batch
            thresholds are not consistent, or if `flush_interval_mins`
            is given next to a batch threshold.
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
    `TableFramesSpec`) plus an *optional* trailing `ctx: TransactionCtx`,
    and returns one `TableFrame` per output table. An input slot is
    `None` (an element, in a list slot) while its table has no version
    with data yet.

    Return an empty frame with the output's columns when there are no
    rows to write. When an `@NEW` input brings no version, read the same
    table at `@HEAD` too and run on its `clear()`, or return `None`
    while it is `None`.
    Returning `None` keeps the output's current data: its new version
    stands for the previous one, and a reader of that output at `@NEW`
    receives that data again.

    A transformer is one computation over the tables it lists, and runs
    as one unit: a run computes all its outputs, they commit in the
    same transaction or not at all, and `tdk table flow` shows every
    input upstream of each output.

    Several outputs are annotated as a fixed-length tuple, one element
    per output; `tuple[TableFrameSpec, ...]` is refused at import.

    Args:
        input_tables: Non-empty list of `[collection/]table[@versions]`
            references to read (`collection/` may be omitted for a
            same-collection table; `@versions` selects a version, a
            `v..v` range, a comma-separated list, or `NEW`). A range, a
            list or `NEW` hands over one `TableFrame` per version, so its
            parameter must be a `TableFramesSpec`.
        output_tables: Non-empty list of local table names to emit, one
            per return slot (valid table names, see `publisher`).
        trigger_by: When the function runs on its own:

            * `None` (default) or `AllTablesTrigger()`: on a new version
              of any input table, other than the ones it writes.
            * `TablesTrigger(tables=[...])`: on a new version of any of
              those tables only, inputs or not.
            * `CronTrigger(mask=...)`: on the schedule only.
            * `TablesTrigger(tables=[])`: never; only when run manually.
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
    takes one parameter per input table (`TableFrameSpec`, or
    `TableFramesSpec` for a read of several versions such as `@NEW`)
    plus an *optional* trailing `ctx: TransactionCtx`, and returns one
    `TableFrame` per destination slot -- that return value is what gets
    written.
    Return `None` for a slot to leave its target as it is; with `None`
    in every slot, the destination writes nothing.

    A destination appended to reads its tables at `@NEW`, so each
    version is written once, even when several commit between two runs;
    a destination replaced every run reads them at `@HEAD`.

    A subscriber hands all its slots to the destination in one write;
    a destination with a `transactional` option writes them in one
    session, which is one commit where the database can roll back
    what the write does (see the destination's page).

    Several slots are annotated as a fixed-length tuple, one element
    per slot; `tuple[TableFrameSpec, ...]` is refused at import.

    Args:
        destination: The `Dest` to write to; its slot cardinality
            fixes the number of return slots.
        input_tables: Non-empty list of `[collection/]table[@versions]`
            references to ship (same form as `transformer`).
        trigger_by: When the function runs on its own; the same options
            as `transformer`, by default on a new version of any input
            table.

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


# Per-flow attribute key. Plain string -- the framework
# packs all `set_attr` entries into a single `user_namespace`
# Parquet cell, so the key never appears as a Parquet column name
# and doesn't need DNS-1123 / column-name constraints.
ContextKeySpec: TypeAlias = str

# The framework's basic scalar value type: the JSON-scalar set shared by
# the per-flow attribute store (`get_attr` / `set_attr`), connector
# seed dicts, and stream resume state. Deliberately excludes `Decimal`
# and any nested (list / dict) shape -- an attribute value is a single
# scalar. A connector that needs arbitrary precision serialises it to
# `str` itself (see the CDC `(seg, ord)` cursor). Round-trips
# losslessly through the state Parquet (see the initial-values codec).
BasicTypeSpec: TypeAlias = Union[str, int, float, bool]

# A flat `str`-keyed dict of `BasicTypeSpec` values -- the shape
# used for `{name: value}` seed dicts (e.g. a SQL connector's
# `initial_values`) and for stream resume state.
BasicDictSpec: TypeAlias = dict[str, BasicTypeSpec]


class TransactionCtx(TransactionCtxLegacy, ABC):
    """Per-trx context handed by the runtime to every decorated
    function and connector plugin.

    A function receives it by declaring a trailing `ctx: TransactionCtx`
    parameter. Carries the
    identifiers of the execution, transaction and function run it
    belongs to (named as the server API names them), a per-flow
    attribute store that survives across invocations (`get_attr` /
    `set_attr`), and a metadata sink (`add_metadata`).
    """

    # devguide-ref: TransactionCtx state semantics

    @property
    @abstractmethod
    def execution_id(self) -> str:
        """Unique id of the execution the run belongs to."""

    @property
    @abstractmethod
    def execution(self) -> str | None:
        """Execution name, or `None` when the execution has none."""

    @property
    @abstractmethod
    def transaction_id(self) -> str:
        """Unique id of the transaction the run belongs to."""

    @property
    @abstractmethod
    def logical_function_run_id(self) -> str:
        """Unique id of the function run, the same across its
        retries."""

    @property
    @abstractmethod
    def physical_function_run_id(self) -> str:
        """Unique id of this attempt of the function run; each retry
        gets a new one."""

    @property
    @abstractmethod
    def triggered_on(self) -> datetime:
        """When the function run was triggered, as a timezone-aware
        UTC `datetime`."""

    @abstractmethod
    def get_attr(self, attr: ContextKeySpec) -> BasicTypeSpec | None:
        """Read a per-flow attribute (returns `None` if
        unset)."""

    @abstractmethod
    def set_attr(self, attr: ContextKeySpec, val: BasicTypeSpec | None):
        """Write a per-flow attribute -- persists across trx
        invocations within the same flow.

        Connectors may reserve namespaces under this attribute store
        for their own internal bookkeeping (`file-lastmod.*` and
        `@td.schema.*` today). Writing to such names from user code
        can corrupt connector state -- pick names that don't
        collide with these prefixes.
        """

    @abstractmethod
    def add_metadata(self, attr: ContextKeySpec, val: BasicTypeSpec | None):
        """Attach trx-level metadata for observability / lineage.
        Not durable across trx invocations."""


class FlowCtx(FlowCtxLegacy, ABC):
    """Flow-level identifiers (project / collection / function) shared
    by every role that runs inside a trx -- plugins *and*
    user-decorated functions.

    :categories: functions

    Carries no plugin-specific surface (no `work_dir`).
    """

    @property
    @abstractmethod
    def project_id(self) -> str:
        """Unique id of the project the function belongs to."""

    @property
    @abstractmethod
    def project(self) -> str:
        """Project name."""

    @property
    @abstractmethod
    def collection_id(self) -> str:
        """Unique id of the collection the function belongs to."""

    @property
    @abstractmethod
    def collection(self) -> str:
        """Collection name."""

    @property
    @abstractmethod
    # Until 2.2 this returned the function run id (the function version id in
    # stream publishers). The name is unchanged, so it cannot be deprecated:
    # code that needs the old value reads physical_function_run_id or
    # function_version_id instead.
    def function_id(self) -> str:
        """Unique id of the function, the same across its versions and
        runs."""

    @property
    @abstractmethod
    def function(self) -> str:
        """Function name."""

    @property
    @abstractmethod
    def function_version_id(self) -> str:
        """Unique id of the function version that runs (the patched
        version, when a patch applies)."""


class FunctionCtx(TransactionCtx, FlowCtx, ABC):
    """Context handed to a user-decorated function (`@publisher` /
    `@stream_publisher` / `@transformer` / `@subscriber`) that declares
    a trailing `ctx` parameter.

    :categories: functions

    Combines `TransactionCtx` (trx identifiers + per-flow attributes via
    `get_attr` / `set_attr`) with `FlowCtx` (flow identifiers), so
    the function can read state from the previous run and persist state
    for the next. It also gives the function the secrets of its project
    (`secret_names` / `secret`). Unlike the plugin contexts of
    `tabsdatak.spi` it does not carry plugin-only surface such as
    `work_dir`.
    """

    @property
    @abstractmethod
    def secret_names(self) -> list[str]:
        """Names of the secrets of the function's project."""

    @abstractmethod
    def secret_keys(self, name: str) -> list[str]:
        """Keys of the project secret `name`.

        Secret names ignore case. Raises `KeyError` when the project
        has no secret with that name.
        """

    @abstractmethod
    def secret(self, name: str, key: str | None = None) -> str:
        """Value of a key of a project secret.

        Accepts the secret and the key separately, `secret("aws",
        "key1")`, or joined by a dot, `secret("aws.key1")`. Secret
        names and keys ignore case. Raises `KeyError` when the project
        has no such secret or key.
        """


__getattr__ = module_getattr(__name__, {"TrxCtx": (__name__, "TransactionCtx")})
