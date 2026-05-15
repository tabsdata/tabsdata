import importlib
import os
import re
from abc import ABC, abstractmethod
from typing import Annotated, Callable, TypeAlias, Union

from pydantic import Field, model_validator

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.error import ApiErrorCode
from tabsdatak.tableframe import TableFrame

# --- Decorated-function slot types ------------------------------------
#
# Every input parameter or output element of a ``@publisher`` /
# ``@auto_publisher`` / ``@transformer`` / ``@subscriber`` function
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
TableFrameSpec: TypeAlias = TableFrame | None
TableFramesSpec: TypeAlias = list[TableFrame | None] | None


TableNameSpec: TypeAlias = Annotated[str, _api.TABLE_NAME_CONSTRAINTS]
TableNamesSpec: TypeAlias = Annotated[
    list[TableNameSpec], _api.NON_EMPTY_LIST_CONSTRAINTS
]

CollTableNameSpec: TypeAlias = Annotated[str, _api.COLL_TABLE_NAME_CONSTRAINTS]
CollTableNamesSpec: TypeAlias = Annotated[
    list[CollTableNameSpec], _api.NON_EMPTY_LIST_CONSTRAINTS
]

CronMaskSpec: TypeAlias = Annotated[str, _api.CRON_MASK_CONSTRAINTS]


@_api.dataclass(ApiErrorCode.API_1)
class Trigger(ABC, _api.DescriptorBase):
    __kind__ = "trigger"


@_api.dataclass(ApiErrorCode.API_2)
class CronTrigger(Trigger):
    mask: CronMaskSpec


@_api.dataclass(ApiErrorCode.API_3)
class TablesTrigger(Trigger):
    tables: CollTableNamesSpec


@_api.dataclass(ApiErrorCode.API_4)
class AllTablesTrigger(Trigger):
    pass


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

# Auto-publisher fallback rollover window. Whole minutes; the runtime
# closes the current batch and starts a new one every
# ``fallback_rollover_mins`` minutes if the source's own rollover
# threshold has not fired. Lower-bounded at 1 (zero / negative would
# either spin or be undefined).
FallbackRolloverMinsSpec: TypeAlias = Annotated[int, Field(ge=1)]


@_api.dataclass(ApiErrorCode.API_5)
class _ValOrRef(_api.JsonSerde, ABC):
    s: str
    __kind__ = "valOrRef"
    _value_pattern: str | None = None

    @abstractmethod
    def _name(self) -> str: ...

    @abstractmethod
    def _value(self) -> str: ...

    def _pattern(self) -> str | None:
        return self._value_pattern

    def _validate(self, val: str) -> str:
        pattern = self._pattern()
        if pattern is not None:
            if re.fullmatch(pattern, val) is None:
                raise ApiErrorCode.API_6.exception(
                    name=self._name(),
                    pattern=pattern,
                    value=val,
                )
        return val

    def value(self) -> str:
        val = self._value()
        val = self._validate(val)
        return val


EnvVarNameSpec: TypeAlias = Annotated[str, _api.ENV_NAME_CONSTRAINTS]


@_api.dataclass(ApiErrorCode.API_7)
class Env(_ValOrRef):
    s: EnvVarNameSpec
    __kind__ = "envRef"

    def _name(self) -> str:
        return self.s

    def _value(self) -> str:
        val = os.environ.get(self.s)
        if val is None:
            raise ApiErrorCode.API_8.exception(name=self.s)
        return val

    @classmethod
    def lit(cls, value: str) -> "Env":
        return LiteralEnv(s=value, _value_pattern=cls._value_pattern)


@_api.dataclass(ApiErrorCode.API_9)
class LiteralEnv(Env):
    s: str
    __kind__ = "literalEnv"

    def _name(self) -> str:
        return "<literal>"

    def _value(self) -> str:
        # Literal: ``s`` is the value verbatim, not an env-var name.
        return self.s

    @model_validator(mode="after")
    def _check_literal_pattern(self):
        self._validate(self.s)
        return self


SecretNameSpec: TypeAlias = Annotated[str, _api.SECRET_NAME_CONSTRAINTS]


@_api.dataclass(ApiErrorCode.API_10)
class Secret(_ValOrRef):
    s: SecretNameSpec
    __kind__ = "secretRef"

    def _name(self) -> str:
        return self.s

    def _value(self) -> str:
        # No resolver wired up by default: refs only resolve under a
        # secret-store integration that is not part of the API package.
        raise ApiErrorCode.API_11.exception(name=self.s)

    @classmethod
    def lit(cls, value: str) -> "Secret":
        return LiteralSecret(s=value, _value_pattern=cls._value_pattern)


@_api.dataclass(ApiErrorCode.API_12)
class LiteralSecret(Secret):
    s: str
    __kind__ = "literalSecret"

    def _name(self) -> str:
        return "<literal>"

    def _value(self) -> str:
        # Literal: ``s`` is the value verbatim, not a secret ref.
        return self.s

    @model_validator(mode="after")
    def _check_literal_pattern(self):
        self._validate(self.s)
        return self


SecretOrEnvSpec: TypeAlias = Union[Secret, Env]


class _Conn(ABC, _api.DescriptorBase):
    __kind__ = "connectionDef"


class Conn(_Conn, ABC):
    __kind__ = "connectionDef"


ConnectionNameSpec: TypeAlias = Union[
    Annotated[str, _api.NAMED_CONN_CONSTRAINTS],
    Annotated[Env, _api.ENV_VALUE_NAMED_CONN_CONSTRAINTS],
]


@_api.dataclass(ApiErrorCode.API_13)
class NamedConn(Conn):
    __kind__ = "connectionRef"
    name: ConnectionNameSpec


# Src / Dest / OnTablesAction stay as pure abstract classes (no dataclass
# decoration). Concrete fields live only on implementer subclasses, which
# carry their impl-layer wiring via a `SrcDef` / `AutoSrcDef` / `DestDef`
# binding declared in `_<name>_plugin.py` (see `tabsdatak.spi`). The
# framework dataclasses that reference these bases as field types
# (`PublisherInfo.source: Src`, ...) opt into
# `arbitrary_types_allowed=True` so pydantic validates via isinstance.
class Src[C: Conn](ABC, _api.DescriptorBase):
    __kind__ = "sourceDef"
    # Abstract field-style annotation: subclasses bind `conn` to their
    # concrete connection union. Advertises the contract; pydantic does
    # not enforce it on the ABC itself -- the SrcDef does.
    conn: C


class AutoSrc[C: Conn](ABC, _api.DescriptorBase):
    __kind__ = "autoSourceDef"
    # Abstract field-style annotation: subclasses bind `conn` to their
    # concrete connection union. Advertises the contract; pydantic does
    # not enforce it on the ABC itself -- the AutoSrcDef does.
    conn: C


class Dest[C: Conn](ABC, _api.DescriptorBase):
    __kind__ = "destinationDef"
    conn: C


class OnTablesAction(ABC, _api.JsonSerde):
    __kind__ = "onTablesAction"


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
    def _publisher(func):
        return _info_cls("PublisherInfo")(
            func, source, output_tables, trigger_by, on_tables
        )

    return _publisher


def auto_publisher(
    source: AutoSrc,
    use_existing_data: bool,
    fallback_rollover_mins: FallbackRolloverMinsSpec,
    output_tables: TableNamesSpec,
    on_tables: OnTablesActionSpec | None = None,
) -> Callable:
    def _publisher(func):
        return _info_cls("AutoPublisherInfo")(
            func,
            source,
            use_existing_data,
            fallback_rollover_mins,
            output_tables,
            on_tables,
        )

    return _publisher


def transformer(
    input_tables: CollTableNamesSpec,
    output_tables: TableNamesSpec,
    trigger_by: TransformerTriggerBySpec | None = None,
    on_tables: OnTablesActionSpec | None = None,
) -> Callable:
    def _transformer(func):
        return _info_cls("TransformerInfo")(
            func, input_tables, output_tables, trigger_by, on_tables
        )

    return _transformer


def subscriber(
    destination: Dest,
    input_tables: CollTableNamesSpec,
    trigger_by: SubscriberTriggerBySpec | None = None,
) -> Callable:
    def _subscriber(func):
        return _info_cls("SubscriberInfo")(func, destination, input_tables, trigger_by)

    return _subscriber
