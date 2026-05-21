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
    get_type_hints, Mapping,
)

from pydantic import ConfigDict, model_validator

import tabsdatak._api as _api
from tabsdatak.api import StreamSrc, Conn, Dest, Src, TrxCtx, TableNameSpec, ContextKeySpec, ContextValueSpec
from tabsdatak.error import ApiErrorCode


class ParquetFile(Path):
    pass


# All in-flight tables travel between framework and plugin as Parquet
# files on disk. Plugins materialise rows to these paths during reads
# and consume them during writes.
TableFileSpec: TypeAlias = ParquetFile


class TableMode(Enum):
    SINGLE = "single"
    ARRAY = "array"


class PluginCtx(ABC):

    @property
    @abstractmethod
    def collection_id(self) -> str: ...

    @property
    @abstractmethod
    def collection_name(self) -> str: ...

    @property
    @abstractmethod
    def function_id(self) -> str: ...

    @property
    @abstractmethod
    def function_name(self) -> str: ...

    @property
    @abstractmethod
    def work_dir(self) -> Path: ...



class SrcPluginCtx(TrxCtx, PluginCtx, ABC): ...


class SrcPlugin[S: Src](ABC):

    @abstractmethod
    def read_in(self, ctx: SrcPluginCtx, src: S) -> list[list[TableFileSpec]]: ...
    # if TableMode is SINGLE, inner list must always have 1 element.


FilesSpec: TypeAlias = Annotated[
    list[TableFileSpec], _api.NON_EMPTY_LIST_CONSTRAINTS
]


# ``ParquetFile`` is a ``pathlib.Path`` subclass, not a pydantic type;
# pydantic cannot synthesize a schema for it on its own. The same
# ``arbitrary_types_allowed=True`` escape hatch the ``SrcDef`` /
# ``StreamSrcDef`` / ``DestDef`` dataclasses use below applies to any
# dataclass that holds files (``TableManifest`` here).
_ARBITRARY = ConfigDict(arbitrary_types_allowed=True)


@_api.dataclass(
    ApiErrorCode.API_21, config=_ARBITRARY, frozen=True, kw_only=True
)
class TableManifest:
    table: TableNameSpec
    files: FilesSpec

StreamManifestSpec: TypeAlias = Annotated[
    list[TableManifest], _api.NON_EMPTY_LIST_CONSTRAINTS
]

class StageRef(ABC): ...

StreamStateSpec: TypeAlias = Mapping[ContextKeySpec, ContextValueSpec]

class StreamSrcPluginCtx(PluginCtx, ABC):

    @abstractmethod
    def is_active(self) -> bool: ...

    @abstractmethod
    def stage(self, manifest: StreamManifestSpec) -> StageRef: ...
    # Must be files within the working directory
    # table names in manifest must be a subset of tables in src

    @abstractmethod
    def commit(self, stage_ref: StageRef): ...

    @abstractmethod
    def trigger(self) -> bool: ...

    @abstractmethod
    def trigger_monitor(self) -> bool: ...

    @abstractmethod
    def read_state(self) -> StreamStateSpec: ...

    @abstractmethod
    def write_state(self, state: StreamStateSpec): ...




class StreamSrcPlugin[SS: StreamSrc](ABC):
    @abstractmethod
    def run(
        self,
        ctx: StreamSrcPluginCtx,
        stream_src: SS,
    ): ...


class DestContext(TrxCtx, PluginCtx, ABC): ...


class DestPlugin[D: Dest](ABC):

    @abstractmethod
    def write_out(
        self,
        ctx: DestContext,
        dest: D,
        tables: list[TableFileSpec],
    ): ...


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


# The `SrcDef` / `StreamSrcDef` / `DestDef` dataclasses carry
# `Type[...]` fields whose argument types are abstract classes
# (`SrcPlugin`, `Explorer`, ...), not pydantic types.
# `arbitrary_types_allowed=True` (the ``_ARBITRARY`` ConfigDict
# defined above) tells pydantic to validate those fields via
# issubclass / isinstance checks.


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
class StreamSrcDef[C: Conn, S: StreamSrc]:
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
