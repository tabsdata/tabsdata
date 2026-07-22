#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

# noinspection PyProtectedMember
from tabsdatak import _api
from tabsdatak.api import BasicDictSpec, Dest, Src, StreamSrc, TrxCtx
from tabsdatak.error import ApiErrorCode, ApiException
from tabsdatak.spi import (
    Conn,
    DestContext,
    DestDef,
    DestPlugin,
    Explorer,
    FilesSpec,
    FlowCtx,
    FunctionCtx,
    PluginCtx,
    SrcDef,
    SrcPlugin,
    SrcPluginCtx,
    StreamSrcDef,
    StreamSrcPlugin,
    StreamSrcPluginCtx,
    StreamStageSrc,
    StreamStateSpec,
    TableFileInput,
    TableFileSpec,
    TableMode,
    check_plugin_param_type,
)


# A concrete Conn subclass we use throughout this module for binding.
@_api.dataclass(ApiErrorCode.API_14)
class _Conn(Conn):
    pass


@_api.dataclass(ApiErrorCode.API_14)
class _OtherConn(Conn):
    pass


# ---------------------------------------------------------------------------
# Src / StreamSrc / Dest no longer carry a ``conn`` field: ``conn`` is passed
# to the decorators (``@publisher`` ...) and to the plugin entry methods
# (``read_in`` / ``run`` / ``write_out``). The concrete config classes are
# plain abstract subclasses with their own per-source knobs.
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_14)
class _CheckSrc(Src):
    pass


@_api.dataclass(ApiErrorCode.API_14)
class _CheckOtherSrc(Src):
    pass


class _GoodSrcPlugin(SrcPlugin[_Conn, _CheckSrc]):
    def read_in(
        self, ctx: SrcPluginCtx, conn: _Conn, src: _CheckSrc
    ) -> list[list[TableFileInput]]:
        return [[]]


class _PluginNoAnnotation(SrcPlugin[_Conn, _CheckSrc]):
    def read_in(self, ctx, conn, src):  # type: ignore[no-untyped-def]
        return [[]]


class _PluginWrongSrcType(SrcPlugin[_Conn, _CheckSrc]):
    def read_in(
        self, ctx: SrcPluginCtx, conn: _Conn, src: _CheckOtherSrc
    ) -> list[list[TableFileInput]]:
        return [[]]


class _PluginWrongConnType(SrcPlugin[_Conn, _CheckSrc]):
    def read_in(
        self, ctx: SrcPluginCtx, conn: _OtherConn, src: _CheckSrc
    ) -> list[list[TableFileInput]]:
        return [[]]


def testcheck_plugin_param_type_accepts_concrete_match():
    check_plugin_param_type(_GoodSrcPlugin, "read_in", "src", _CheckSrc)  # no raise
    check_plugin_param_type(_GoodSrcPlugin, "read_in", "conn", _Conn)  # no raise


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
    check_plugin_param_type(_PluginNoAnnotation, "read_in", "conn", _Conn)


def testcheck_plugin_param_type_rejects_incompatible_src_annotation():
    with pytest.raises(ValueError, match="`src` parameter is typed as"):
        check_plugin_param_type(_PluginWrongSrcType, "read_in", "src", _CheckSrc)


def testcheck_plugin_param_type_rejects_incompatible_conn_annotation():
    with pytest.raises(ValueError, match="`conn` parameter is typed as"):
        check_plugin_param_type(_PluginWrongConnType, "read_in", "conn", _Conn)


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
    check_plugin_param_type(_NotOverridden, "read_in", "conn", _Conn)


# ---------------------------------------------------------------------------
# ``SrcDef`` / ``StreamSrcDef`` / ``DestDef`` validators
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_14)
class _SDSrc(Src):
    pass


@_api.dataclass(ApiErrorCode.API_14)
class _SDStreamSrc(StreamSrc):
    pass


@_api.dataclass(ApiErrorCode.API_14)
class _SDDest(Dest):
    pass


class _SDSrcPlugin(SrcPlugin[_Conn, _SDSrc]):
    def read_in(
        self, ctx: SrcPluginCtx, conn: _Conn, src: _SDSrc
    ) -> list[list[TableFileInput]]:
        return [[]]


class _SDStreamSrcPlugin(StreamSrcPlugin[_Conn, _SDStreamSrc]):
    def run(
        self,
        ctx: StreamSrcPluginCtx,
        conn: _Conn,
        stream_src: _SDStreamSrc,
    ) -> None:
        return None


class _SDDestPlugin(DestPlugin[_Conn, _SDDest]):
    def write_out(
        self, ctx: DestContext, conn: _Conn, dest: _SDDest, tables: list[TableFileSpec]
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
        type_="ok",
        system="Test",
        src_version="v1",
        src=_SDSrc,
        table_mode=TableMode.SINGLE,
        cardinality=lambda s: 1,
        plugin=_SDSrcPlugin,
        explorer=_SDExplorer,
        icon=None,
    )


def test_stream_src_def_constructs_with_valid_fields():
    StreamSrcDef(
        conn=_Conn,
        type_="ok",
        system="Test",
        src_version="v1",
        src=_SDStreamSrc,
        cardinality=lambda s: 1,
        plugin=_SDStreamSrcPlugin,
        publisher_src=StreamStageSrc,
        explorer=_SDExplorer,
        icon=None,
    )


def test_dest_def_constructs_with_valid_fields():
    DestDef(
        conn=_Conn,
        type_="ok",
        system="Test",
        dest_version="v1",
        dest=_SDDest,
        cardinality=lambda d: 1,
        plugin=_SDDestPlugin,
        explorer=_SDExplorer,
        icon=None,
    )


# --- plugin signature mismatch --------------------------------------


# Module-level so forward-ref resolution in ``get_type_hints`` finds
# them via the module's globals when ``from __future__ import
# annotations`` turns annotations into strings.
@_api.dataclass(ApiErrorCode.API_14)
class _OtherSrc(Src):
    pass


@_api.dataclass(ApiErrorCode.API_14)
class _OtherDest(Dest):
    pass


@_api.dataclass(ApiErrorCode.API_14)
class _OtherStreamSrc(StreamSrc):
    pass


class _MismatchSrcPlugin(SrcPlugin[_Conn, _SDSrc]):
    # Annotated against ``_OtherSrc``, but the Def binds ``_SDSrc``.
    def read_in(
        self, ctx: SrcPluginCtx, conn: _Conn, src: _OtherSrc
    ) -> list[list[TableFileInput]]:
        return [[]]


class _MismatchConnSrcPlugin(SrcPlugin[_Conn, _SDSrc]):
    # ``conn`` annotated against ``_OtherConn``, but the Def binds ``_Conn``.
    def read_in(
        self, ctx: SrcPluginCtx, conn: _OtherConn, src: _SDSrc
    ) -> list[list[TableFileInput]]:
        return [[]]


class _MismatchDestPlugin(DestPlugin[_Conn, _SDDest]):
    def write_out(
        self, ctx: DestContext, conn: _Conn, dest: _OtherDest, tables: list[TableFileSpec]
    ) -> None:
        return None


class _MismatchStreamSrcPlugin(StreamSrcPlugin[_Conn, _SDStreamSrc]):
    def run(
        self, ctx: StreamSrcPluginCtx, conn: _Conn, stream_src: _OtherStreamSrc
    ) -> None:
        return None


def test_src_def_rejects_plugin_with_mismatched_src_param():
    with pytest.raises(ApiException) as excinfo:
        SrcDef(
            conn=_Conn,
            type_="bad",
            system="Test",
            src_version="v1",
            src=_SDSrc,
            table_mode=TableMode.SINGLE,
            cardinality=lambda s: 1,
            plugin=_MismatchSrcPlugin,
            explorer=None,
            icon=None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_14


def test_src_def_rejects_plugin_with_mismatched_conn_param():
    with pytest.raises(ApiException) as excinfo:
        SrcDef(
            conn=_Conn,
            type_="bad",
            system="Test",
            src_version="v1",
            src=_SDSrc,
            table_mode=TableMode.SINGLE,
            cardinality=lambda s: 1,
            plugin=_MismatchConnSrcPlugin,
            explorer=None,
            icon=None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_14


def test_stream_src_def_rejects_plugin_with_mismatched_stream_src_param():
    with pytest.raises(ApiException) as excinfo:
        StreamSrcDef(
            conn=_Conn,
            type_="bad",
            system="Test",
            src_version="v1",
            src=_SDStreamSrc,
            cardinality=lambda s: 1,
            plugin=_MismatchStreamSrcPlugin,
            publisher_src=StreamStageSrc,
            explorer=None,
            icon=None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_15


def test_dest_def_rejects_plugin_with_mismatched_dest_param():
    with pytest.raises(ApiException) as excinfo:
        DestDef(
            conn=_Conn,
            type_="bad",
            system="Test",
            dest_version="v1",
            dest=_SDDest,
            cardinality=lambda d: 1,
            plugin=_MismatchDestPlugin,
            explorer=None,
            icon=None,
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
            type_="bad",
            system="Test",
            src_version="v1",
            src=_SDSrc,
            table_mode=TableMode.SINGLE,
            cardinality=lambda s: 1,
            plugin=_SDSrcPlugin,
            explorer=_BadExplorer,
            icon=None,
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
            type_="bad",
            system="Test",
            dest_version="v1",
            dest=_SDDest,
            cardinality=lambda d: 1,
            plugin=_SDDestPlugin,
            explorer=_BadExplorer,
            icon=None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_16


def test_src_def_explorer_none_skips_explorer_check():
    # explorer=None: the explorer check is skipped, even though the
    # plugin / src checks still run.
    SrcDef(
        conn=_Conn,
        type_="ok",
        system="Test",
        src_version="v1",
        src=_SDSrc,
        table_mode=TableMode.SINGLE,
        cardinality=lambda s: 1,
        plugin=_SDSrcPlugin,
        explorer=None,
        icon=None,
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
        type_="ok",
        system="Test",
        src_version="v1",
        src=_SDSrc,
        table_mode=TableMode.SINGLE,
        cardinality=lambda s: 1,
        plugin=_SDSrcPlugin,
        explorer=_LooseExplorer,
        icon=None,
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
        self.staged: list[tuple[list[list[Path]], StreamStateSpec]] = []
        self._stream_state: BasicDictSpec | None = None

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
    def stream_state(self) -> BasicDictSpec | None:
        return self._stream_state

    @property
    def flush_interval_mins(self) -> int:
        return 0

    def stage(
        self,
        tables_files: list[list[Path]],
        new_state: StreamStateSpec,
    ) -> None:
        self.staged.append((tables_files, new_state))
        # A dict is Tabsdata-owned state we persist; a plain callable means
        # the external system owns the offset, so Tabsdata keeps none and we
        # invoke the callable for its commit side effect.
        if isinstance(new_state, dict):
            self._stream_state = dict(new_state)
        else:
            self._stream_state = None
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
    ctx.stage([[Path("a.parquet")]], {"cdc.offset": 1})
    assert ctx.first_stage is False


def test_stage_with_dict_state_exposes_stream_state():
    """A :data:`BasicDictSpec` commit makes its state readable on the
    next run via :attr:`stream_state`."""
    ctx = _RecordingStreamCtx()
    ctx.stage([[Path("a.parquet")]], {"cdc.offset": 9, "topic": "people"})
    assert ctx.stream_state == {"cdc.offset": 9, "topic": "people"}


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
    ctx.stage(manifest, {})
    assert ctx.staged[0][0] == manifest
