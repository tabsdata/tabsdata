from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Union

import pytest

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.api import StreamSrc, Conn, Dest, Src
from tabsdatak.error import ApiErrorCode, ApiException
from tabsdatak.api import TrxCtx
from tabsdatak.spi import (
    CommitStateSpec,
    FilesSpec,
    StoreStreamState,
    StreamSrcPluginCtx,
    StreamSrcDef,
    StreamSrcPlugin,
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    FlowCtx,
    FunctionCtx,
    PluginCtx,
    SrcPluginCtx,
    SrcDef,
    SrcPlugin,
    TableFileSpec,
    TableMode,
    check_conn_field,
    check_plugin_param_type,
    flatten_union,
)


# A concrete Conn subclass we use throughout this module for binding.
@_api.dataclass(ApiErrorCode.API_13)
class _Conn(Conn):
    pass


@_api.dataclass(ApiErrorCode.API_13)
class _OtherConn(Conn):
    pass


# ---------------------------------------------------------------------------
# 1. ``flatten_union`` shapes
# ---------------------------------------------------------------------------


def testflatten_union_plain_type_returns_singleton_tuple():
    assert flatten_union(int) == (int,)


def testflatten_union_simple_union():
    out = flatten_union(Union[int, str])
    assert set(out) == {int, str}


def testflatten_union_strips_outer_annotated():
    out = flatten_union(Annotated[int, "marker"])
    assert out == (int,)


def testflatten_union_strips_annotated_inside_union():
    # ``Annotated`` on each arm is unwrapped recursively.
    out = flatten_union(Union[Annotated[int, "m1"], Annotated[str, "m2"]])
    assert set(out) == {int, str}


def testflatten_union_nested_union_is_flattened_by_python_normalisation():
    # Python normalises ``Union[Union[A, B], C]`` to ``Union[A, B, C]``;
    # the helper still produces the flat tuple.
    out = flatten_union(Union[Union[int, str], float])
    assert set(out) == {int, str, float}


def testflatten_union_collapses_typevar_to_its_bound():
    # PEP 695 type parameters that reach a non-parametrised subclass
    # surface as TypeVars at ``get_type_hints`` time. The helper
    # collapses them to their bound so the rest of the validator can
    # reason in terms of concrete classes.
    from typing import TypeVar

    T = TypeVar("T", bound=Conn)
    assert flatten_union(T) == (Conn,)


def testflatten_union_typevar_without_bound_round_trips():
    from typing import TypeVar

    T = TypeVar("T")
    out = flatten_union(T)
    assert out == (T,)


# ---------------------------------------------------------------------------
# 2. ``check_conn_field`` error paths
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithBadConn(Src):
    conn: int  # type: ignore[assignment]


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithMixedUnion(Src):
    # Half-valid union: one arm is a Conn subclass, the other isn't.
    conn: Union[Conn, int]  # type: ignore[assignment]


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithAnnotatedNonConn(Src):
    conn: Annotated[int, "marker"]  # type: ignore[assignment]


def testcheck_conn_field_rejects_non_conn_member():
    with pytest.raises(ValueError, match="must be a subclass of Conn"):
        check_conn_field(_SrcWithBadConn, "source", Conn)


def testcheck_conn_field_rejects_mixed_union():
    with pytest.raises(ValueError, match="must be a subclass of Conn"):
        check_conn_field(_SrcWithMixedUnion, "source", Conn)


def testcheck_conn_field_rejects_annotated_non_conn():
    with pytest.raises(ValueError, match="must be a subclass of Conn"):
        check_conn_field(_SrcWithAnnotatedNonConn, "source", Conn)


def testcheck_conn_field_accepts_valid_subclass():
    @_api.dataclass(ApiErrorCode.API_14)
    class _GoodSrc(Src):
        conn: Conn

    check_conn_field(_GoodSrc, "source", Conn)  # no raise


def testcheck_conn_field_unknown_class_cannot_resolve_hints():
    class _NotADataclass:
        # Annotation referencing a name that won't resolve.
        conn: "NoSuchType"  # type: ignore[name-defined]  # noqa: F821

    with pytest.raises(ValueError, match="cannot resolve type hints"):
        check_conn_field(_NotADataclass, "source", Conn)


def testcheck_conn_field_rejects_when_expected_not_assignable():
    # ``conn`` annotation is a concrete subclass _OtherConn, the binding
    # declares ``_Conn``: a ``_Conn`` value is NOT assignable to a slot
    # typed ``_OtherConn``, so the check fails.
    @_api.dataclass(ApiErrorCode.API_14)
    class _SrcOtherConn(Src):
        conn: _OtherConn  # type: ignore[assignment]

    with pytest.raises(ValueError, match=r"must accept _Conn"):
        check_conn_field(_SrcOtherConn, "source", _Conn)


def testcheck_conn_field_accepts_when_expected_subclass_of_member():
    # ``conn: Conn`` accepts any Conn subclass, so a binding declaring
    # ``_Conn`` (a Conn subclass) is fine.
    @_api.dataclass(ApiErrorCode.API_14)
    class _SrcAnyConn(Src):
        conn: Conn

    check_conn_field(_SrcAnyConn, "source", _Conn)  # no raise


def testcheck_conn_field_accepts_unparametrised_generic_inheritance():
    # ``class _Src(Src)`` (no PEP 695 binding) inherits ``Src.conn: C``
    # where ``C`` is the TypeVar bound by ``Conn``. ``flatten_union``
    # collapses the TypeVar to ``Conn`` so the check passes.
    @_api.dataclass(ApiErrorCode.API_14)
    class _Inherited(Src):
        pass

    check_conn_field(_Inherited, "source", _Conn)  # no raise


# ---------------------------------------------------------------------------
# 3. ``check_plugin_param_type``
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_14)
class _CheckSrc(Src):
    conn: _Conn


@_api.dataclass(ApiErrorCode.API_14)
class _CheckOtherSrc(Src):
    conn: _Conn


class _GoodSrcPlugin(SrcPlugin[_CheckSrc]):
    def read_in(self, ctx: SrcPluginCtx, src: _CheckSrc) -> list[list[TableFileSpec]]:
        return [[]]


class _PluginNoAnnotation(SrcPlugin[_CheckSrc]):
    def read_in(self, ctx, src):  # type: ignore[no-untyped-def]
        return [[]]


class _PluginWrongType(SrcPlugin[_CheckSrc]):
    def read_in(
        self, ctx: SrcPluginCtx, src: _CheckOtherSrc
    ) -> list[list[TableFileSpec]]:
        return [[]]


def testcheck_plugin_param_type_accepts_concrete_match():
    check_plugin_param_type(_GoodSrcPlugin, "read_in", "src", _CheckSrc)  # no raise


def testcheck_plugin_param_type_accepts_subclass_of_declared():
    # If the plugin's parameter is annotated with a base class, the
    # binding's concrete subclass is still accepted.
    @_api.dataclass(ApiErrorCode.API_14)
    class _SubSrc(_CheckSrc):
        pass

    check_plugin_param_type(_GoodSrcPlugin, "read_in", "src", _SubSrc)


def testcheck_plugin_param_type_skips_when_no_annotation():
    # No annotation on the parameter -> skip silently.
    check_plugin_param_type(_PluginNoAnnotation, "read_in", "src", _CheckSrc)


def testcheck_plugin_param_type_rejects_incompatible_annotation():
    with pytest.raises(ValueError, match="`src` parameter is typed as"):
        check_plugin_param_type(_PluginWrongType, "read_in", "src", _CheckSrc)


def testcheck_plugin_param_type_missing_method_raises():
    class _NoMethod:
        pass

    with pytest.raises(ValueError, match="missing required `read_in`"):
        check_plugin_param_type(_NoMethod, "read_in", "src", _CheckSrc)


def testcheck_plugin_param_type_skips_when_param_unbound_typevar():
    # An abstract plugin without an override surfaces the parameter as
    # the bound TypeVar -- not a concrete class, so the check skips
    # rather than raising.
    class _NotOverridden(SrcPlugin):
        pass

    check_plugin_param_type(_NotOverridden, "read_in", "src", _CheckSrc)


# ---------------------------------------------------------------------------
# 4. ``SrcDef`` / ``AutoSrcDef`` / ``DestDef`` validators
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_14)
class _SDSrc(Src):
    conn: _Conn


@_api.dataclass(ApiErrorCode.API_14)
class _SDStreamSrc(StreamSrc):
    conn: _Conn


@_api.dataclass(ApiErrorCode.API_14)
class _SDDest(Dest):
    conn: _Conn


class _SDSrcPlugin(SrcPlugin[_SDSrc]):
    def read_in(self, ctx: SrcPluginCtx, src: _SDSrc) -> list[list[TableFileSpec]]:
        return [[]]


class _SDStreamSrcPlugin(StreamSrcPlugin[_SDStreamSrc]):
    def run(
        self,
        ctx: StreamSrcPluginCtx,
        src: _SDStreamSrc,
    ) -> tuple[ManifestFileSpec, list[list[TableFileSpec]]]:
        from pathlib import Path

        return (Path("/dev/null"), [[]])


class _SDDestPlugin(DestPlugin[_SDDest]):
    def write_out(
        self, ctx: DestContext, dest: _SDDest, tables: list[TableFileSpec]
    ) -> None:
        return None


class _SDExplorer(Explorer[_Conn]):
    def metadata(self, conn: _Conn) -> Any:
        return None

    def data(self, conn: _Conn, query: str) -> Any:
        return None


# --- successful construction ---------------------------------------


def test_src_def_constructs_with_valid_fields():
    SrcDef(
        conn=_Conn,
        src_type="ok",
        src_version="v1",
        src=_SDSrc,
        table_mode=TableMode.SINGLE,
        cardinality=lambda s: 1,
        plugin=_SDSrcPlugin,
        explorer=_SDExplorer,
    )


def test_auto_src_def_constructs_with_valid_fields():
    StreamSrcDef(
        conn=_Conn,
        src_type="ok",
        src_version="v1",
        src=_SDStreamSrc,
        table_mode=TableMode.SINGLE,
        cardinality=lambda s: 1,
        uses_manifest=False,
        plugin=_SDStreamSrcPlugin,
        explorer=_SDExplorer,
    )


def test_dest_def_constructs_with_valid_fields():
    DestDef(
        conn=_Conn,
        dest_type="ok",
        dest_version="v1",
        dest=_SDDest,
        cardinality=lambda d: 1,
        plugin=_SDDestPlugin,
        explorer=_SDExplorer,
    )


# --- bad src/dest conn type rejection ------------------------------


@_api.dataclass(ApiErrorCode.API_14)
class _BadSrcType(Src):
    conn: int  # type: ignore[assignment]


@_api.dataclass(ApiErrorCode.API_14)
class _BadStreamSrcType(StreamSrc):
    conn: int  # type: ignore[assignment]


@_api.dataclass(ApiErrorCode.API_14)
class _BadDestType(Dest):
    conn: int  # type: ignore[assignment]


class _DummySrcPlugin(SrcPlugin):
    def read_in(self, ctx, src):
        return [[]]


class _DummyStreamSrcPlugin(StreamSrcPlugin):
    def run(self, ctx, src):

        return (Path("/dev/null"), [[]])


class _DummyDestPlugin(DestPlugin):
    def write_out(self, ctx, dest, tables):
        return None


def test_src_def_rejects_bad_src_conn_type():
    with pytest.raises(ApiException) as excinfo:
        SrcDef(
            conn=Conn,
            src_type="bad",
            src_version="v1",
            src=_BadSrcType,
            table_mode=TableMode.SINGLE,
            cardinality=lambda s: 1,
            plugin=_DummySrcPlugin,
            explorer=None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_14


def test_auto_src_def_rejects_bad_src_conn_type():
    with pytest.raises(ApiException) as excinfo:
        StreamSrcDef(
            conn=Conn,
            src_type="bad",
            src_version="v1",
            src=_BadStreamSrcType,
            table_mode=TableMode.SINGLE,
            cardinality=lambda s: 1,
            uses_manifest=False,
            plugin=_DummyStreamSrcPlugin,
            explorer=None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_15


def test_dest_def_rejects_bad_dest_conn_type():
    with pytest.raises(ApiException) as excinfo:
        DestDef(
            conn=Conn,
            dest_type="bad",
            dest_version="v1",
            dest=_BadDestType,
            cardinality=lambda d: 1,
            plugin=_DummyDestPlugin,
            explorer=None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_16


# --- def-conn must be assignable to src/dest.conn -------------------


def test_src_def_rejects_when_def_conn_not_assignable_to_src_conn():
    # Src.conn is _OtherConn but Def declares _Conn -- not assignable.
    @_api.dataclass(ApiErrorCode.API_14)
    class _SrcOnlyOther(Src):
        conn: _OtherConn

    with pytest.raises(ApiException) as excinfo:
        SrcDef(
            conn=_Conn,
            src_type="bad",
            src_version="v1",
            src=_SrcOnlyOther,
            table_mode=TableMode.SINGLE,
            cardinality=lambda s: 1,
            plugin=_DummySrcPlugin,
            explorer=None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_14


# --- plugin signature mismatch --------------------------------------


# Module-level so forward-ref resolution in ``get_type_hints`` finds
# them via the module's globals when ``from __future__ import
# annotations`` turns annotations into strings.
@_api.dataclass(ApiErrorCode.API_14)
class _OtherSrc(Src):
    conn: _Conn


@_api.dataclass(ApiErrorCode.API_14)
class _OtherDest(Dest):
    conn: _Conn


class _MismatchSrcPlugin(SrcPlugin[_SDSrc]):
    # Annotated against ``_OtherSrc``, but the Def binds ``_SDSrc``.
    def read_in(self, ctx: SrcPluginCtx, src: _OtherSrc) -> list[list[TableFileSpec]]:
        return [[]]


class _MismatchDestPlugin(DestPlugin[_SDDest]):
    def write_out(
        self, ctx: DestContext, dest: _OtherDest, tables: list[TableFileSpec]
    ) -> None:
        return None


def test_src_def_rejects_plugin_with_mismatched_src_param():
    with pytest.raises(ApiException) as excinfo:
        SrcDef(
            conn=_Conn,
            src_type="bad",
            src_version="v1",
            src=_SDSrc,
            table_mode=TableMode.SINGLE,
            cardinality=lambda s: 1,
            plugin=_MismatchSrcPlugin,
            explorer=None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_14


def test_dest_def_rejects_plugin_with_mismatched_dest_param():
    with pytest.raises(ApiException) as excinfo:
        DestDef(
            conn=_Conn,
            dest_type="bad",
            dest_version="v1",
            dest=_SDDest,
            cardinality=lambda d: 1,
            plugin=_MismatchDestPlugin,
            explorer=None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_16


# --- explorer signature mismatch ------------------------------------


def test_src_def_rejects_explorer_with_mismatched_conn():
    class _BadExplorer(Explorer[_Conn]):
        def metadata(self, conn: _OtherConn) -> Any:
            return None

        def data(self, conn: _OtherConn, query: str) -> Any:
            return None

    with pytest.raises(ApiException) as excinfo:
        SrcDef(
            conn=_Conn,
            src_type="bad",
            src_version="v1",
            src=_SDSrc,
            table_mode=TableMode.SINGLE,
            cardinality=lambda s: 1,
            plugin=_SDSrcPlugin,
            explorer=_BadExplorer,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_14


def test_dest_def_rejects_explorer_with_mismatched_conn():
    class _BadExplorer(Explorer[_Conn]):
        def metadata(self, conn: _OtherConn) -> Any:
            return None

        def data(self, conn: _OtherConn, query: str) -> Any:
            return None

    with pytest.raises(ApiException) as excinfo:
        DestDef(
            conn=_Conn,
            dest_type="bad",
            dest_version="v1",
            dest=_SDDest,
            cardinality=lambda d: 1,
            plugin=_SDDestPlugin,
            explorer=_BadExplorer,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_16


def test_src_def_explorer_none_skips_explorer_check():
    # explorer=None: the explorer check is skipped, even though the
    # plugin / src checks still run.
    SrcDef(
        conn=_Conn,
        src_type="ok",
        src_version="v1",
        src=_SDSrc,
        table_mode=TableMode.SINGLE,
        cardinality=lambda s: 1,
        plugin=_SDSrcPlugin,
        explorer=None,
    )


def test_src_def_accepts_explorer_with_loose_conn_annotation():
    # Explorer's conn annotation is ``Conn`` (base) -- a binding's
    # concrete subclass is still assignable, so the check passes.
    class _LooseExplorer(Explorer[_Conn]):
        def metadata(self, conn: Conn) -> Any:
            return None

        def data(self, conn: Conn, query: str) -> Any:
            return None

    SrcDef(
        conn=_Conn,
        src_type="ok",
        src_version="v1",
        src=_SDSrc,
        table_mode=TableMode.SINGLE,
        cardinality=lambda s: 1,
        plugin=_SDSrcPlugin,
        explorer=_LooseExplorer,
    )


# --- FunctionCtx (sibling of SrcPluginCtx / DestContext) ----------------


def test_function_ctx_is_abstract():
    """FunctionCtx is purely abstract -- cannot instantiate directly."""
    with pytest.raises(TypeError, match="abstract"):
        FunctionCtx()  # type: ignore[abstract]


def test_function_ctx_subclasses_trx_and_flow_ctx():
    """The role tag composes the two orthogonal abstract bases."""
    assert issubclass(FunctionCtx, TrxCtx)
    assert issubclass(FunctionCtx, FlowCtx)


def test_function_ctx_is_not_a_plugin_ctx():
    """User functions are not plugins -- ``FunctionCtx`` deliberately
    excludes :class:`PluginCtx` so plugin-only surface (``work_dir``)
    doesn't leak into the user-function type contract."""
    assert not issubclass(FunctionCtx, PluginCtx)


def test_plugin_ctx_is_a_flow_ctx():
    """``PluginCtx`` carries the flow-level identifiers via
    :class:`FlowCtx`; user functions see only the identifiers."""
    assert issubclass(PluginCtx, FlowCtx)


def test_function_ctx_distinct_from_src_and_dest_ctx():
    """Three sibling role tags: separate identities so role-typed
    signatures stay honest."""
    assert FunctionCtx is not SrcPluginCtx
    assert FunctionCtx is not DestContext
    assert not issubclass(FunctionCtx, SrcPluginCtx)
    assert not issubclass(FunctionCtx, DestContext)
    assert not issubclass(SrcPluginCtx, FunctionCtx)
    assert not issubclass(DestContext, FunctionCtx)


def test_function_ctx_concrete_subclass_must_implement_abstracts():
    """A concrete subclass that doesn't implement every abstract member
    inherited from TrxCtx + FlowCtx is itself abstract."""

    class _Half(FunctionCtx):
        # Only some abstracts implemented -- still abstract.
        @property
        def exec_plan_id(self) -> str:
            return "x"

    with pytest.raises(TypeError, match="abstract"):
        _Half()  # type: ignore[abstract]


# --- StoreStreamState / CommitStateSpec ---------------------------------


def test_store_stream_state_exposes_state_verbatim():
    """The wrapped per-flow state dict is exposed verbatim on
    :attr:`StoreStreamState.state` (the runtime persists it)."""
    state = {"cdc.offset": {"lsn": 42}}
    store = StoreStreamState(state)
    assert store.state is state


def test_store_stream_state_call_is_a_noop():
    """The base :meth:`__call__` is a no-op: persisting ``state`` is the
    runtime's job, not the callback's. It returns ``None`` and must not
    mutate the wrapped state."""
    store = StoreStreamState({"k": 1})
    assert store() is None
    assert store.state == {"k": 1}


def test_store_stream_state_is_a_commit_state_spec():
    """A :class:`StoreStreamState` is callable, so it satisfies the
    :data:`CommitStateSpec` (``Callable[[], None]``) contract that
    :meth:`StreamSrcPluginCtx.stage` accepts."""
    assert callable(StoreStreamState({}))


def test_store_stream_state_subclass_call_side_effect_runs():
    """A subclass may override :meth:`__call__` for a commit-time side
    effect; the runtime invokes it in addition to persisting ``state``."""
    ran = []

    class _Acking(StoreStreamState):
        def __call__(self) -> None:
            ran.append(self.state)

    store = _Acking({"o": 7})
    store()
    assert ran == [{"o": 7}]


# --- FilesSpec ----------------------------------------------------------


def test_files_spec_rejects_empty_list():
    """:data:`FilesSpec` carries a ``min_length=1`` constraint: a slot
    that is present must hold at least one file."""
    from pydantic import BaseModel, ValidationError

    class _M(BaseModel):
        files: FilesSpec

    with pytest.raises(ValidationError):
        _M(files=[])


def test_files_spec_accepts_non_empty_list_of_paths():
    from pydantic import BaseModel

    class _M(BaseModel):
        files: FilesSpec

    model = _M(files=[Path("a.parquet"), Path("b.parquet")])
    assert model.files == [Path("a.parquet"), Path("b.parquet")]


# --- StreamSrcPluginCtx -------------------------------------------------


class _RecordingStreamCtx(StreamSrcPluginCtx):
    """Minimal concrete :class:`StreamSrcPluginCtx` that records what was
    staged and mimics the documented ``first_stage`` / ``stream_state``
    semantics, so the abstract contract has a reference implementation a
    signature drift would break."""

    def __init__(self) -> None:
        self.staged: list[tuple[list[list[Path]], CommitStateSpec]] = []
        self._stream_state: dict | None = None

    # PluginCtx surface (unused by these tests, stubbed).
    @property
    def collection_id(self) -> str:
        return "c-id"

    @property
    def collection_name(self) -> str:
        return "c"

    @property
    def function_id(self) -> str:
        return "f-id"

    @property
    def function_name(self) -> str:
        return "f"

    @property
    def work_dir(self) -> Path:
        return Path("/tmp")

    # StreamSrcPluginCtx surface.
    @property
    def active(self) -> bool:
        return True

    @property
    def first_stage(self) -> bool:
        return not self.staged

    @property
    def stream_state(self) -> dict | None:
        return self._stream_state

    @property
    def flush_interval_mins(self) -> int:
        return 0

    def stage(
        self,
        tables_files: list[list[Path]],
        new_state: CommitStateSpec,
    ) -> None:
        self.staged.append((tables_files, new_state))
        # Persist the state for a StoreStreamState; a plain callable means
        # the external system owns the offset, so Tabsdata keeps none.
        self._stream_state = (
            new_state.state if isinstance(new_state, StoreStreamState) else None
        )
        new_state()


def test_stream_src_plugin_ctx_is_abstract():
    with pytest.raises(TypeError, match="abstract"):
        StreamSrcPluginCtx()  # type: ignore[abstract]


def test_stream_src_plugin_ctx_subclasses_plugin_ctx():
    assert issubclass(StreamSrcPluginCtx, PluginCtx)


def test_stream_src_plugin_ctx_partial_subclass_stays_abstract():
    """Implementing only some of the four stream abstracts (here just
    ``active``) leaves the subclass abstract."""

    class _Half(StreamSrcPluginCtx):
        @property
        def active(self) -> bool:
            return True

    with pytest.raises(TypeError, match="abstract"):
        _Half()  # type: ignore[abstract]


def test_first_stage_flips_after_first_stage_call():
    ctx = _RecordingStreamCtx()
    assert ctx.first_stage is True
    ctx.stage([[Path("a.parquet")]], StoreStreamState({"cdc.offset": 1}))
    assert ctx.first_stage is False


def test_stage_with_store_stream_state_exposes_stream_state():
    """A :class:`StoreStreamState` commit makes its state readable on the
    next run via :attr:`stream_state`."""
    ctx = _RecordingStreamCtx()
    ctx.stage([[Path("a.parquet")]], StoreStreamState({"cdc.offset": {"lsn": 9}}))
    assert ctx.stream_state == {"cdc.offset": {"lsn": 9}}


def test_stage_with_plain_callable_keeps_stream_state_none():
    """When the external system owns the offset the plugin hands ``stage``
    a plain callable; ``stream_state`` stays ``None`` (and is *not* the
    'first run' signal -- ``first_stage`` is)."""
    ctx = _RecordingStreamCtx()
    committed = []
    ctx.stage([[Path("a.parquet")]], lambda: committed.append(True))
    assert ctx.stream_state is None
    assert ctx.first_stage is False
    assert committed == [True]


def test_stage_receives_positional_per_table_manifest():
    """``tables_files`` is positional by output-table index; an empty
    inner list means 'no files for that table this batch'."""
    ctx = _RecordingStreamCtx()
    manifest = [[Path("t0.parquet")], [], [Path("t2.parquet")]]
    ctx.stage(manifest, StoreStreamState({}))
    assert ctx.staged[0][0] == manifest
