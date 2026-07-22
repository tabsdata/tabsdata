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
from abc import ABC, abstractmethod
from typing import Annotated, Callable, TypeAlias, Union

from pydantic import Field

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
TableNameSpec: TypeAlias = Annotated[str, _api.TABLE_NAME_CONSTRAINTS]
TableNamesSpec: TypeAlias = Annotated[
    list[TableNameSpec], _api.NON_EMPTY_LIST_CONSTRAINTS
]

# Cross-collection table reference, of shape `"collection/table"`.
# `TablesTrigger.tables` uses this form so a function can fire on
# tables that live in a different collection.
CollTableNameSpec: TypeAlias = Annotated[str, _api.COLL_TABLE_NAME_CONSTRAINTS]
CollTableNamesSpec: TypeAlias = Annotated[
    list[CollTableNameSpec], _api.NON_EMPTY_LIST_CONSTRAINTS
]

# Cross-collection table reference with an optional version suffix, of
# shape `"[collection/]table[@versions]"` (e.g. `people@HEAD~1..HEAD`).
# `input_tables` uses this form so a function can read a specific
# version, a range of versions, or a list of versions of a table.
CollTableVersionsNameSpec: TypeAlias = Annotated[
    str, _api.COLL_TABLE_VERSIONS_NAME_CONSTRAINTS
]
CollTableVersionsNamesSpec: TypeAlias = Annotated[
    list[CollTableVersionsNameSpec], _api.NON_EMPTY_LIST_CONSTRAINTS
]

# Five-field cron expression understood by `croniter`.
CronMaskSpec: TypeAlias = Annotated[str, _api.CRON_MASK_CONSTRAINTS]


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

# stream-publisher fallback rollover window. Whole minutes; the runtime
# closes the current batch and starts a new one every
# `fallback_rollover_mins` minutes if the source's own rollover
# threshold has not fired. Lower-bounded at 1 (zero / negative would
# either spin or be undefined).
FlushIntervalSpec: TypeAlias = Annotated[int, Field(ge=1)]


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
    list[OnTablesAction], _api.NON_EMPTY_LIST_CONSTRAINTS
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
    flush_interval_mins: FlushIntervalSpec,
    output_tables: TableNamesSpec,
    on_tables: OnTablesActionSpec | None = None,
) -> Callable:
    """Decorate a function as a stream publisher.

    :categories: functions

    Consumes a long-lived change stream (CDC, log/queue tail). The
    function takes one `TableFramesSpec` parameter per source slot
    (always array mode) and returns one `TableFrame` per output
    table. Unlike the other decorators it must **not** declare a
    `ctx` parameter -- it runs inside the runtime's stream loop, not
    per-trx, so no `TrxCtx` exists at the call boundary.

    Args:
        source: The `StreamSrc` to consume.
        flush_interval_mins: Fallback rollover window in whole minutes
            (at least 1); the runtime closes the current batch every N
            minutes if the source's own threshold has not fired.
        output_tables: Non-empty list of local table names to emit, one
            per return slot (valid table names, see `publisher`).
        on_tables: Optional list of `OnTablesAction` side-effects.

    Returns:
        The decorator that wraps the function as a stream-publisher
        descriptor.

    Raises:
        ApiException: If the function signature is invalid (e.g. it
            declares a `ctx` parameter).
    """

    def _stream_publisher(func):
        return _info_cls("StreamPublisherInfo")(
            func,
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
