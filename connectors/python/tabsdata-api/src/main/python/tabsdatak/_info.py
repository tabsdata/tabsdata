"""Function descriptors built by the user-facing decorators.

The four decorators in :mod:`tabsdatak.api` (``@publisher``,
``@stream_publisher``, ``@transformer``, ``@subscriber``) wrap
the user function in the matching ``*Info`` dataclass below.
Each Info captures the decorator's static arguments + a callable
reference to the function, validates the signature against the
declared input/output shapes, and rejects the original
``func(...)`` call so users can't accidentally invoke the wrapped
function directly.

These dataclasses are also the JSON serialisation surface for a
flow definition; ``func`` round-trips as ``"module:qualname"``.
"""

from __future__ import annotations

from typing import Callable

from pydantic import ConfigDict, field_serializer, field_validator, model_validator

import tabsdatak._api as _api
import tabsdatak.api as api
from tabsdatak.error import ApiErrorCode
from tabsdatak.spi import TableMode

# Src / Dest / OnTablesAction (referenced via fields below) are plain
# abstract classes, not pydantic types. `arbitrary_types_allowed=True`
# tells pydantic to validate those fields via isinstance checks against
# the declared abstract bases. Concrete implementer subclasses still
# carry their own pydantic dataclass validation that runs when they are
# *constructed*, not when they are passed here.
_ARBITRARY = ConfigDict(arbitrary_types_allowed=True)


# `func` is a Callable -- on dump it is encoded as `"module:qualname"`,
# on load resolved via importlib. Reused across PublisherInfo /
# TransformerInfo / SubscriberInfo. Only top-level (importable)
# functions can round-trip;
# closures with `<locals>` in their qualname are rejected at dump time.
def _ser_func(func: Callable) -> str:
    return _api.callable_to_str(func)


def _val_func(v):
    if isinstance(v, str):
        return _api.callable_from_str(v)
    return v


@_api.dataclass(ApiErrorCode.API_17, config=_ARBITRARY)
class PublisherInfo(_api.JsonSerde):
    """Descriptor for a ``@publisher``-decorated function.

    Carries the user function plus its declared
    :class:`~tabsdatak.api.Src`, output-table names, optional
    trigger and on-tables side-effects. Construction validates
    the function signature against the source's slot count /
    mode and the declared outputs."""

    __kind__ = "publisherDef"

    func: Callable
    source: api.Src
    output_tables: api.TableNamesSpec
    trigger_by: api.PublisherTriggerBySpec | None
    on_tables: api.OnTablesActionSpec | None

    _ser_func = field_serializer("func")(_ser_func)
    _val_func = field_validator("func", mode="before")(_val_func)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            f"{type(self).__name__} is a flow-descriptor, not a callable. "
            f"The decorated function lives on `.func`; the framework "
            f"invokes it at runtime. Calling the decorated name in user "
            f"code is almost certainly a bug."
        )

    @model_validator(mode="after")
    def _check_func_signature(self):
        from tabsdatak import _connectors

        definition = _connectors.get_src(type(self.source))
        input_mode = "single" if definition.table_mode is TableMode.SINGLE else "array"
        _api.check_callable_signature(
            self.func,
            input_count=definition.cardinality(self.source),
            input_mode=input_mode,
            output_count=len(self.output_tables),
        )
        return self


@_api.dataclass(ApiErrorCode.API_18, config=_ARBITRARY)
class StreamPublisherInfo(_api.JsonSerde):
    """Descriptor for a ``@stream_publisher``-decorated function.

    Captures the :class:`~tabsdatak.api.StreamSrc`, the
    ``use_existing_data`` replay flag, the ``fallback_rollover_mins``
    batching window, and the output-table contract. Stream-publisher
    inputs are always array-mode (one event fans out across rows)."""

    __kind__ = "streamPublisherDef"

    func: Callable
    source: api.StreamSrc
    use_existing_data: bool
    fallback_rollover_mins: api.FlushIntervalSpec
    output_tables: api.TableNamesSpec
    on_tables: api.OnTablesActionSpec | None

    _ser_func = field_serializer("func")(_ser_func)
    _val_func = field_validator("func", mode="before")(_val_func)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            f"{type(self).__name__} is a flow-descriptor, not a callable. "
            f"The decorated function lives on `.func`; the framework "
            f"invokes it at runtime. Calling the decorated name in user "
            f"code is almost certainly a bug."
        )

    @model_validator(mode="after")
    def _check_func_signature(self):
        from tabsdatak import _connectors

        definition = _connectors.get_stream_src(type(self.source))
        # ``StreamSrcDef`` carries no ``table_mode`` -- stream sources
        # emit a batch of tableframes per event (one event can fan
        # out across rows), so the decorated function's input slots
        # are always array-mode (``TableFramesSpec``).
        # ``allow_trx_ctx=False`` -- stream publishers run inside the
        # runtime's long-lived stream-plugin loop, not per-trx, so
        # there is no ``TrxCtx`` at the call boundary.
        _api.check_callable_signature(
            self.func,
            input_count=definition.cardinality(self.source),
            input_mode="array",
            output_count=len(self.output_tables),
            allow_trx_ctx=False,
        )
        return self


@_api.dataclass(ApiErrorCode.API_19, config=_ARBITRARY)
class TransformerInfo(_api.JsonSerde):
    """Descriptor for a ``@transformer``-decorated function.

    Table-to-table inside the flow (no external connection).
    Inputs are cross-collection ``"collection/table"`` references;
    outputs are local table names."""

    __kind__ = "transformerDef"

    func: Callable
    input_tables: api.CollTableNamesSpec
    output_tables: api.TableNamesSpec
    trigger_by: api.TransformerTriggerBySpec | None
    on_tables: api.OnTablesActionSpec | None

    _ser_func = field_serializer("func")(_ser_func)
    _val_func = field_validator("func", mode="before")(_val_func)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            f"{type(self).__name__} is a flow-descriptor, not a callable. "
            f"The decorated function lives on `.func`; the framework "
            f"invokes it at runtime. Calling the decorated name in user "
            f"code is almost certainly a bug."
        )

    @model_validator(mode="after")
    def _check_func_signature(self):
        # Transformer inputs accept either single or array slots --
        # the framework does not yet enforce a mode here. Outputs are
        # always single-mode TableFrame.
        _api.check_callable_signature(
            self.func,
            input_count=len(self.input_tables),
            input_mode="any",
            output_count=len(self.output_tables),
        )
        return self


@_api.dataclass(ApiErrorCode.API_20, config=_ARBITRARY)
class SubscriberInfo(_api.JsonSerde):
    """Descriptor for a ``@subscriber``-decorated function.

    Carries the user function plus its declared
    :class:`~tabsdatak.api.Dest`, the input-table references
    (``"collection/table"``), and an optional trigger. The
    function receives one tableframe per input slot and returns
    nothing -- the destination plugin handles the actual write."""

    __kind__ = "subscriberDef"

    func: Callable
    destination: api.Dest
    input_tables: api.CollTableNamesSpec
    trigger_by: api.SubscriberTriggerBySpec | None

    _ser_func = field_serializer("func")(_ser_func)
    _val_func = field_validator("func", mode="before")(_val_func)

    def __call__(self, *args, **kwargs):
        raise TypeError(
            f"{type(self).__name__} is a flow-descriptor, not a callable. "
            f"The decorated function lives on `.func`; the framework "
            f"invokes it at runtime. Calling the decorated name in user "
            f"code is almost certainly a bug."
        )

    @model_validator(mode="after")
    def _check_func_signature(self):
        # Subscriber inputs accept either single or array slots --
        # the framework does not yet enforce a mode here. Outputs are
        # always single-mode TableFrame, one element per
        # `dest.cardinality`.
        from tabsdatak import _connectors

        definition = _connectors.get_dest(type(self.destination))
        _api.check_callable_signature(
            self.func,
            input_count=len(self.input_tables),
            input_mode="any",
            output_count=definition.cardinality(self.destination),
        )
        return self
