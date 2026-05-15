from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Callable,
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
from tabsdatak.api import AutoSrc, Conn, Dest, Src
from tabsdatak.error import ApiErrorCode


class ParquetFile(Path):
    pass


# All in-flight tables travel between framework and plugin as Parquet
# files on disk. Plugins materialise rows to these paths during reads
# and consume them during writes.
TableFileSpec: TypeAlias = ParquetFile

# Auto-source plugins persist a manifest file across rollover windows
# so the runtime can skip files that were already consumed. Format is
# plugin-defined; the framework only handles its location.
ManifestFileSpec: TypeAlias = Path


class TableMode(Enum):
    SINGLE = "single"
    ARRAY = "array"


class SrcContext(ABC):
    @abstractmethod
    def work_dir(self) -> Path: ...

    @property
    @abstractmethod
    def exec_plan_id(self) -> str: ...

    @property
    @abstractmethod
    def trx_id(self) -> str: ...

    @property
    @abstractmethod
    def exec_plan_ts(self) -> str: ...

    # TODO RO access to metadata, RW access to state (e.g. last_mod).


class SrcPlugin[S: Src](ABC):
    # TODO nail down this API.
    #
    # IDEA: when the source's TableMode is SINGLE, the inner lists
    # always have exactly one element; ARRAY sources may have
    # variable-length inner lists.
    @abstractmethod
    def read_in(self, ctx: SrcContext, src: S) -> list[list[TableFileSpec]]: ...


class AutoSrcContext(ABC):
    @abstractmethod
    def work_dir(self) -> Path: ...

    # TODO RO access to metadata, RW access to state (e.g. manifest).


class AutoSrcPlugin[AS: AutoSrc](ABC):
    # TODO nail down this API.
    #
    # IDEA: when the source's TableMode is SINGLE, the inner lists
    # always have exactly one element; ARRAY sources may have
    # variable-length inner lists.
    @abstractmethod
    def read_in(
        self,
        ctx: AutoSrcContext,
        src: AS,
    ) -> tuple[ManifestFileSpec, list[list[TableFileSpec]]]: ...


class DestContext(ABC):
    @abstractmethod
    def work_dir(self) -> Path: ...

    @property
    @abstractmethod
    def exec_plan_id(self) -> str: ...

    @property
    @abstractmethod
    def trx_id(self) -> str: ...

    @property
    @abstractmethod
    def exec_plan_ts(self) -> str: ...

    # TODO RO access to metadata, RW access to state.


class DestPlugin[D: Dest](ABC):
    # TODO nail down this API.
    @abstractmethod
    def write_out(
        self,
        ctx: DestContext,
        dest: D,
        tables: list[TableFileSpec],
    ) -> None: ...


class Explorer[C: Conn](ABC):
    # TODO nail down this API.
    @abstractmethod
    def metadata(self, conn: C) -> Any: ...

    # TODO nail down this API.
    @abstractmethod
    def data(self, conn: C, query: str) -> Any: ...


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


# The `SrcDef` / `AutoSrcDef` / `DestDef` dataclasses carry
# `Type[...]` fields whose argument types are abstract classes
# (`SrcPlugin`, `Explorer`, ...), not pydantic types.
# `arbitrary_types_allowed=True` tells pydantic to validate those
# fields via issubclass / isinstance checks.
_ARBITRARY = ConfigDict(arbitrary_types_allowed=True)


@_api.dataclass(ApiErrorCode.API_14, config=_ARBITRARY)
class SrcDef[C: Conn, S: Src]:
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
class AutoSrcDef[C: Conn, S: AutoSrc]:
    src_type: str
    src_version: str
    conn: Type[C]
    src: Type[S]
    table_mode: TableMode
    cardinality: Callable[[S], int]
    uses_manifest: bool
    plugin: Type[AutoSrcPlugin]
    explorer: Type[Explorer] | None

    @model_validator(mode="after")
    def _validate(self) -> "AutoSrcDef":
        check_conn_field(self.src, "auto_source", self.conn)
        check_plugin_param_type(self.plugin, "read_in", "src", self.src)
        if self.explorer is not None:
            check_plugin_param_type(self.explorer, "metadata", "conn", self.conn)
            check_plugin_param_type(self.explorer, "data", "conn", self.conn)
        return self


@_api.dataclass(ApiErrorCode.API_16, config=_ARBITRARY)
class DestDef[C: Conn, D: Dest]:
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
