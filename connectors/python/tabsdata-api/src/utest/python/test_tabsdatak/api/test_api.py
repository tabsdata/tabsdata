from abc import ABC

import pytest

from tabsdatak import _api
from tabsdatak.error import ApiErrorCode, ApiException, ApiInternalException
from tabsdatak import api as m, tableframe
from tabsdatak import _connectors as _connectors
from tabsdatak import spi
from tabsdatak import _info
import json
from typing import Annotated

from tabsdatak.api import (
    Conn,
    Var,
    Secret,
    Src,
    _LiteralVar,
    _LiteralSecret,
)


# Test-only SPI plugins. SrcDef / DestDef require a concrete
# `plugin` class; the framework never instantiates these in tests, they
# exist only to satisfy the field contract.
class _TestSrcPlugin(spi.SrcPlugin):
    ...


class _TestDestPlugin(spi.DestPlugin):
    ...


# Concrete fixtures. Each source / destination kind is registered via
# the connector registry (the same path entry-points use) so the
# framework's flow descriptors can resolve them at validate time.
@_api.dataclass(ApiErrorCode.API_14)
class _Src(m.Src):
    pass


_connectors.register_src(spi.SrcDef(
        conn=m.Conn,
    src_type="fs",
    src_version="v1",
    src=_Src,
    table_mode=spi.TableMode.SINGLE,
    cardinality=lambda src: 1,
    plugin=_TestSrcPlugin,
    explorer=None,
))


@_api.dataclass(ApiErrorCode.API_14)
class _Dest(m.Dest):
    pass


_connectors.register_dest(spi.DestDef(
        conn=m.Conn,
    dest_type="fs",
    dest_version="v1",
    dest=_Dest,
    cardinality=lambda dest: 1,
    plugin=_TestDestPlugin,
    explorer=None,
))


# Stream-source fixture: same shape as _Src above but on the
# StreamSrc / StreamSrcDef / StreamSrcPlugin rails. ``StreamSrcDef``
# carries no ``table_mode`` -- stream sources do not declare one --
# and its plugin uses ``run(ctx, stream_src)`` (validated by
# ``StreamSrcDef._validate``) rather than batch-mode ``read_in``.
class _TestStreamSrcPlugin(spi.StreamSrcPlugin):
    ...


@_api.dataclass(ApiErrorCode.API_14)
class _StreamSrc(m.StreamSrc):
    pass


_STREAM_SRC_DEFX = spi.StreamSrcDef(
    conn=m.Conn,
    src_type="watch",
    src_version="v1",
    src=_StreamSrc,
    cardinality=lambda src: 1,
    uses_manifest=False,
    plugin=_TestStreamSrcPlugin,
    explorer=None,
)
_connectors.register_stream_src(_STREAM_SRC_DEFX)


class _OnTab(m.OnTablesAction):
    pass


# Helper function whose flat-signature shape matches the default fixtures
# (SINGLE-mode src/dest table_count=1, one element in
# output_tables / input_tables). One positional input parameter typed as
# `TableFrame` (auto-nullable) and a `tuple[TableFrame]` return.
def _good_1in_1out(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
    return (t1,)


# Stream-source variant: input slot is array-mode (`TableFramesSpec`)
# because ``StreamSrcDef`` does not declare a table-mode and the
# framework treats stream sources as emitting a batch of tableframes
# per event. Output stays single-mode per output table.
def _good_1in_1out_stream(
    t1: list[tableframe.TableFrame | None] | None,
) -> tuple[tableframe.TableFrame | None]:
    return (None,)


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------

def test_trigger_is_abc_subclass():
    assert issubclass(m.Trigger, ABC)


def test_cron_trigger_constructs_with_mask():
    t = m.CronTrigger(mask="0 0 * * *")
    assert t.mask == "0 0 * * *"
    assert isinstance(t, m.Trigger)


def test_cron_trigger_missing_mask_raises_api_2():
    with pytest.raises(ApiException) as excinfo:
        m.CronTrigger()
    assert excinfo.value.api_error is ApiErrorCode.API_2


def test_tables_trigger_constructs_with_names():
    t = m.TablesTrigger(tables=["users", "orders"])
    assert t.tables == ["users", "orders"]
    assert isinstance(t, m.Trigger)


def test_tables_trigger_accepts_collection_qualified_names():
    # Looser CollTableNamesSpec shape, so transformer / subscriber
    # call sites can use the same trigger type.
    t = m.TablesTrigger(tables=["users", "src/orders"])
    assert t.tables == ["users", "src/orders"]


def test_tables_trigger_rejects_empty_list_with_api_11():
    with pytest.raises(ApiException) as excinfo:
        m.TablesTrigger(tables=[])
    assert excinfo.value.api_error is ApiErrorCode.API_3


def test_tables_trigger_rejects_invalid_name_with_api_11():
    with pytest.raises(ApiException) as excinfo:
        m.TablesTrigger(tables=["1bad"])
    assert excinfo.value.api_error is ApiErrorCode.API_3


def test_all_tables_trigger_constructs_with_no_args():
    t = m.AllTablesTrigger()
    assert isinstance(t, m.Trigger)


# ---------------------------------------------------------------------------
# PublisherInfo _api.dataclass
# ---------------------------------------------------------------------------

def _src() -> _Src:
    return _Src()


def _dest() -> _Dest:
    return _Dest()


def _auto_src() -> _StreamSrc:
    return _StreamSrc()


def test_publisher_dataclass_constructs_ok():
    src = _src()
    p = _info.PublisherInfo(_good_1in_1out, src, ["t1"], None, None)
    assert p.source is src
    assert p.output_tables == ["t1"]
    assert p.trigger_by is None
    assert p.on_tables is None


def test_publisher_dataclass_empty_tables_raises_api_4():
    # Field-level failure on TableNamesSpec min_length short-circuits before the
    # model_validator that checks `func` signature, so any callable is fine.
    with pytest.raises(ApiException) as excinfo:
        _info.PublisherInfo(_good_1in_1out, _src(), [], None, None)
    assert excinfo.value.api_error is ApiErrorCode.API_17


# ---------------------------------------------------------------------------
# @publisher decorator factory
# ---------------------------------------------------------------------------

def test_publisher_decorator_returns_publisher_instance():
    @m.publisher(source=_src(), output_tables=["t1"])
    def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_pub, _info.PublisherInfo)
    assert callable(my_pub)


def test_publisher_descriptor_call_raises_typeerror_pointing_at_func():
    @m.publisher(source=_src(), output_tables=["t1"])
    def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    # The decorated name is a PublisherInfo descriptor, not a callable.
    # Calling it from user code would be a bug; the friendly error
    # points at .func.
    with pytest.raises(TypeError, match=r"\.func"):
        my_pub(None)
    # The underlying function is still reachable on .func.
    assert my_pub.func(None) == (None,)


def test_signature_error_includes_expected_signature_block():
    # On a wrong-arity input, the error should suggest the correct
    # spelled-out signature, so an LLM iterating against the error
    # knows what to write.
    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=_src(), output_tables=["t1"])
        def _bad() -> tuple[tableframe.TableFrame | None]:  # zero params, expects 1
            return (None,)
    msg = str(excinfo.value)
    assert "Expected signature" in msg
    assert "TableFrame" in msg


def test_publisher_decorator_rejects_empty_tables_with_api_4():
    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=_src(), output_tables=[])
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_17


def test_publisher_decorator_accepts_cron_trigger():
    cron = m.CronTrigger(mask="*/5 * * * *")

    @m.publisher(source=_src(), output_tables=["t1"], trigger_by=cron)
    def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert my_pub.trigger_by is cron


def test_publisher_decorator_accepts_tables_trigger():
    # Typed wrapper around the bare-list shortcut.
    trig = m.TablesTrigger(tables=["users", "orders"])

    @m.publisher(source=_src(), output_tables=["t1"], trigger_by=trig)
    def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert my_pub.trigger_by is trig


def test_publisher_decorator_rejects_bare_table_list():
    # Bare-list shortcut was removed; only typed Trigger subclasses
    # are accepted (TablesTrigger / CronTrigger). The framework
    # tags the type-mismatch as API_17 (publisher validation).
    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=_src(), output_tables=["t1"],
                     trigger_by=["users", "orders"])
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_17


# ---------------------------------------------------------------------------
# @transformer decorator factory
# ---------------------------------------------------------------------------

def test_transformer_decorator_wraps_with_all_tables_trigger():
    trig = m.AllTablesTrigger()

    @m.transformer(
        input_tables=["in1"],
        output_tables=["out1"],
        trigger_by=trig,
        on_tables=[_OnTab()],
    )
    def my_xf(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_xf, _info.TransformerInfo)
    assert my_xf.trigger_by is trig


def test_transformer_decorator_rejects_bare_wildcard():
    # The bare-"*" shortcut was removed; AllTablesTrigger() is now
    # the only way to express "fire on any upstream table change".
    with pytest.raises(ApiException) as excinfo:
        @m.transformer(
            input_tables=["in1"], output_tables=["out1"], trigger_by="*",
        )
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_19


def test_transformer_decorator_rejects_empty_input_tables_with_api_5():
    with pytest.raises(ApiException) as excinfo:
        @m.transformer(input_tables=[], output_tables=["out"])
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_19


def test_transformer_decorator_allows_default_none_trigger_and_on_tables():
    @m.transformer(input_tables=["in1"], output_tables=["out1"])
    def my_xf(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_xf, _info.TransformerInfo)
    assert my_xf.trigger_by is None
    assert my_xf.on_tables is None


def test_transformer_decorator_accepts_all_tables_trigger():
    # Typed wrapper around the bare-"*" shortcut.
    trig = m.AllTablesTrigger()

    @m.transformer(
        input_tables=["a"], output_tables=["b"], trigger_by=trig,
    )
    def my_xf(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert my_xf.trigger_by is trig


def test_transformer_decorator_accepts_tables_trigger():
    trig = m.TablesTrigger(tables=["src/orders"])

    @m.transformer(
        input_tables=["a"], output_tables=["b"], trigger_by=trig,
    )
    def my_xf(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert my_xf.trigger_by is trig


# ---------------------------------------------------------------------------
# @subscriber decorator factory
# ---------------------------------------------------------------------------

def test_subscriber_decorator_wraps_with_all_tables_trigger():
    dest = _dest()
    trig = m.AllTablesTrigger()

    @m.subscriber(destination=dest, input_tables=["in1"], trigger_by=trig)
    def my_sub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_sub, _info.SubscriberInfo)
    assert my_sub.destination is dest
    assert my_sub.trigger_by is trig


def test_subscriber_decorator_rejects_empty_input_tables_with_api_6():
    with pytest.raises(ApiException) as excinfo:
        @m.subscriber(destination=_dest(), input_tables=[])
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_20


def test_subscriber_decorator_allows_default_none_trigger():
    @m.subscriber(destination=_dest(), input_tables=["in1"])
    def my_sub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_sub, _info.SubscriberInfo)
    assert my_sub.trigger_by is None


def test_subscriber_decorator_accepts_all_tables_trigger():
    trig = m.AllTablesTrigger()

    @m.subscriber(
        destination=_dest(), input_tables=["in1"], trigger_by=trig,
    )
    def my_sub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert my_sub.trigger_by is trig


def test_subscriber_decorator_accepts_tables_trigger():
    trig = m.TablesTrigger(tables=["src/orders", "src/users"])

    @m.subscriber(
        destination=_dest(), input_tables=["in1"], trigger_by=trig,
    )
    def my_sub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert my_sub.trigger_by is trig


# ---------------------------------------------------------------------------
# ApiError + ApiException shape
# ---------------------------------------------------------------------------

def test_api_validation_error_carries_tag_and_cause():
    try:
        m.CronTrigger()
    except ApiException as e:
        assert e.api_error is ApiErrorCode.API_2
        assert e.cause is not None
        assert "CronTrigger" in str(e)
    else:
        pytest.fail("expected ApiException")


# ---------------------------------------------------------------------------
# Pattern constraints: TableNameSpec, CollTableNameSpec, CronMaskSpec
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name", ["t1", "foo_bar", "A", "a_1_b", "X" * 64],
)
def test_table_name_accepts_valid_identifier(name):
    src = _src()
    m.publisher(source=src, output_tables=[name])  # no error


@pytest.mark.parametrize(
    "name", ["", "1foo", "foo bar", "foo-bar", "X" * 65, "_foo"],
)
def test_table_name_rejects_invalid(name):
    src = _src()
    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=src, output_tables=[name])
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_17


@pytest.mark.parametrize(
    "name", ["foo", "foo_bar", "ns/table", "ns_1/table_2"],
)
def test_coll_table_name_accepts_valid(name):
    m.transformer(input_tables=[name], output_tables=["out"])


@pytest.mark.parametrize(
    "name", ["foo/bar/baz", "ns / table", "/table", "ns/", "1foo/bar"],
)
def test_coll_table_name_rejects_invalid(name):
    with pytest.raises(ApiException) as excinfo:
        @m.transformer(input_tables=[name], output_tables=["out"])
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_19


# ---------------------------------------------------------------------------
# Signature validation -- publisher / transformer / subscriber
# Runs in each class's @model_validator(mode="after") using the spec bound
# by @src / @dest and the output_tables / input_tables length.
# ---------------------------------------------------------------------------

@_api.dataclass(ApiErrorCode.API_14)
class _Src2(m.Src):
    pass


_connectors.register_src(spi.SrcDef(
        conn=m.Conn,
    src_type="fs2",
    src_version="v1",
    src=_Src2,
    table_mode=spi.TableMode.SINGLE,
    cardinality=lambda src: 2,
    plugin=_TestSrcPlugin,
    explorer=None,
))


@_api.dataclass(ApiErrorCode.API_14)
class _Dest3(m.Dest):
    pass


_connectors.register_dest(spi.DestDef(
        conn=m.Conn,
    dest_type="fs3",
    dest_version="v1",
    dest=_Dest3,
    cardinality=lambda dest: 3,
    plugin=_TestDestPlugin,
    explorer=None,
))


def _src2() -> _Src2:
    return _Src2()


def _dest3() -> _Dest3:
    return _Dest3()


def test_publisher_signature_ok_for_2in_2out():
    src = _src2()

    @m.publisher(source=src, output_tables=["a", "b"])
    def my_pub(
        t1: tableframe.TableFrame | None, t2: tableframe.TableFrame | None,
    ) -> tuple[tableframe.TableFrame | None, tableframe.TableFrame | None]:
        return (t1, t2)

    assert isinstance(my_pub, _info.PublisherInfo)


def test_publisher_signature_rejects_wrong_input_arity():
    src = _src2()  # expects 2 input slots

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=src, output_tables=["a", "b"])
        def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None, tableframe.TableFrame | None]:  # only 1
            return (t1, t1)
    assert excinfo.value.api_error is ApiErrorCode.API_17
    assert "at least 2 parameter" in str(excinfo.value)


def test_publisher_signature_rejects_wrong_output_arity():
    src = _src()  # table_count=1

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=src, output_tables=["a", "b"])  # expects 2
        def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:  # returns 1
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_17


def test_publisher_signature_rejects_missing_return_annotation():
    src = _src()

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=src, output_tables=["a"])
        def my_pub(t1: tableframe.TableFrame | None):
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_17
    assert "return type must be annotated" in str(excinfo.value)


def test_publisher_signature_rejects_variable_length_tuple():
    src = _src()

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=src, output_tables=["a"])
        def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None, ...]:  # variable-length
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_17


def test_transformer_signature_ok_with_matching_input_and_output():
    @m.transformer(
        input_tables=["a", "b"],
        output_tables=["x", "y", "z"],
    )
    def my_xf(
        t1: tableframe.TableFrame | None, t2: tableframe.TableFrame | None,
    ) -> tuple[tableframe.TableFrame | None, tableframe.TableFrame | None, tableframe.TableFrame | None]:
        return (t1, t2, t1)

    assert isinstance(my_xf, _info.TransformerInfo)


def test_transformer_signature_rejects_mismatched_output_arity():
    with pytest.raises(ApiException) as excinfo:
        @m.transformer(input_tables=["a"], output_tables=["x", "y"])
        def my_xf(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:  # only 1, expected 2
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_19


def test_subscriber_signature_uses_destination_table_count_for_output():
    dest = _dest3()  # table_count=3

    @m.subscriber(destination=dest, input_tables=["a"])
    def my_sub(
        t1: tableframe.TableFrame | None,
    ) -> tuple[tableframe.TableFrame | None, tableframe.TableFrame | None, tableframe.TableFrame | None]:
        return (t1, t1, t1)

    assert isinstance(my_sub, _info.SubscriberInfo)


def test_subscriber_signature_rejects_mismatched_output_arity():
    dest = _dest3()

    with pytest.raises(ApiException) as excinfo:
        @m.subscriber(destination=dest, input_tables=["a"])
        def my_sub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:  # only 1, dest wants 3
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_20


# ---------------------------------------------------------------------------
# Element-type checking: publisher input honours src.table_mode.
# SINGLE -> TableFrame (or TableFrame | None) per slot.
# ARRAY  -> list[TableFrame] (or list[TableFrame | None]) per slot.
# Transformer / subscriber inputs are mode "any" -- both forms accepted.
# ---------------------------------------------------------------------------

@_api.dataclass(ApiErrorCode.API_14)
class _SrcArray(m.Src):
    pass


_connectors.register_src(spi.SrcDef(
        conn=m.Conn,
    src_type="fs_array",
    src_version="v1",
    src=_SrcArray,
    table_mode=spi.TableMode.ARRAY,
    cardinality=lambda src: 2,
    plugin=_TestSrcPlugin,
    explorer=None,
))


def _src_array() -> _SrcArray:
    return _SrcArray()


def test_publisher_input_accepts_list_elements_when_mode_is_array():
    src = _src_array()  # ARRAY, 2 slots

    @m.publisher(source=src, output_tables=["a"])
    def my_pub(
        t1: list[tableframe.TableFrame | None] | None, t2: list[tableframe.TableFrame | None] | None,
    ) -> tuple[tableframe.TableFrame | None]:
        return (None,)

    assert isinstance(my_pub, _info.PublisherInfo)


def test_publisher_input_rejects_single_element_when_mode_is_array():
    src = _src_array()  # ARRAY, 2 slots

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=src, output_tables=["a"])
        def my_pub(
            # wrong: TableFrame where list[TableFrame] expected
            t1: tableframe.TableFrame | None, t2: tableframe.TableFrame | None,
        ) -> tuple[tableframe.TableFrame | None]:
            return (None,)
    assert excinfo.value.api_error is ApiErrorCode.API_17
    assert "array-mode slot" in str(excinfo.value)


def test_publisher_input_rejects_list_element_when_mode_is_single():
    src = _src()  # SINGLE, 1 slot

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=src, output_tables=["a"])
        def my_pub(
            # wrong: list where TableFrame expected
            t1: list[tableframe.TableFrame | None] | None,
        ) -> tuple[tableframe.TableFrame | None]:
            return (None,)
    assert excinfo.value.api_error is ApiErrorCode.API_17
    assert "single-mode slot" in str(excinfo.value)


def test_publisher_output_rejects_list_element_regardless_of_mode():
    # Output is always single-TableFrame, even when the source is ARRAY.
    src = _src_array()

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=src, output_tables=["a"])
        def my_pub(
            t1: list[tableframe.TableFrame | None] | None, t2: list[tableframe.TableFrame | None] | None,
        ) -> tuple[list[tableframe.TableFrame | None] | None]:  # wrong output
            return ([],)
    assert excinfo.value.api_error is ApiErrorCode.API_17
    assert "return tuple element" in str(excinfo.value)


def test_publisher_signature_rejects_extra_non_ctx_params_after_input_slots():
    # The only param allowed past the input slots is an optional
    # trailing ``ctx: TrxCtx`` -- arbitrary extras (config / logger /
    # etc.) are rejected so the runtime can deterministically infer
    # whether to pass a ctx instance.
    src = _src()  # table_count=1

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=src, output_tables=["t"])
        def _my_pub(
            t1: tableframe.TableFrame | None,
            config: dict[str, str],
            logger: object,
        ) -> tuple[tableframe.TableFrame | None]:
            return (t1,)

    assert excinfo.value.api_error is ApiErrorCode.API_17


def test_publisher_signature_allows_optional_trailing_trx_ctx():
    src = _src()  # table_count=1

    @m.publisher(source=src, output_tables=["t"])
    def my_pub(
        t1: tableframe.TableFrame | None,
        ctx: m.TrxCtx,
    ) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_pub, _info.PublisherInfo)


def test_publisher_signature_rejects_trailing_non_trx_ctx_param():
    # A trailing param that is neither a slot nor a TrxCtx subclass is
    # rejected -- the contract is "tableframes... then an optional
    # TrxCtx", nothing else.
    src = _src()

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(source=src, output_tables=["t"])
        def _my_pub(
            t1: tableframe.TableFrame | None,
            ctx: dict,
        ) -> tuple[tableframe.TableFrame | None]:
            return (t1,)

    assert excinfo.value.api_error is ApiErrorCode.API_17


def test_has_trx_ctx_param_detects_trailing_ctx():
    def f(t1: tableframe.TableFrame | None, ctx: m.TrxCtx) -> tuple[
        tableframe.TableFrame | None
    ]:
        return (t1,)

    assert _api.has_trx_ctx_param(f) is True


def test_has_trx_ctx_param_false_without_ctx():
    def f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert _api.has_trx_ctx_param(f) is False


def test_has_trx_ctx_param_accepts_trx_ctx_subclass():
    # Subclasses of TrxCtx (e.g. plugin contexts) also count.
    class _MyCtx(m.TrxCtx):
        @property
        def exec_plan_id(self) -> str: return "x"
        @property
        def trx_id(self) -> str: return "x"
        @property
        def exec_plan_ts(self) -> str: return "x"
        def get_attr(self, attr): return None
        def store_attr(self, attr, val): pass
        def add_metadata(self, attr, val): pass

    def f(t1: tableframe.TableFrame | None, ctx: _MyCtx) -> tuple[
        tableframe.TableFrame | None
    ]:
        return (t1,)

    assert _api.has_trx_ctx_param(f) is True


def test_publisher_signature_accepts_explicit_table_frame_or_none():
    src = _src()

    @m.publisher(source=src, output_tables=["t"])
    def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_pub, _info.PublisherInfo)


@pytest.mark.parametrize(
    "mask",
    [
        "0 0 * * *",
        "*/5 * * * *",
        "0 8,12,17 * * *",
        "0 0-23/2 * * *",
        "* * * * *",
        "0 0 * * MON",         # named DOW
        "0 0 * JAN *",         # named month
        "0 0 * * MON-FRI",     # DOW range
        # --- K8s descriptors ---
        "@yearly",
        "@annually",
        "@monthly",
        "@weekly",
        "@daily",
        "@midnight",
        "@hourly",
    ],
)
def test_cron_mask_accepts_valid(mask):
    m.CronTrigger(mask=mask)


@pytest.mark.parametrize(
    "mask",
    [
        "",
        "0 0 * *",              # 4 fields
        "0 0 * * * *",          # 6 fields (seconds precision -- not K8s)
        "garbage",
        "60 0 * * *",           # minute out of range (semantic)
        "0 24 * * *",           # hour out of range
        "0 0 32 * *",           # dom out of range
        "0 0 * 13 *",           # month out of range
        "0 0 * * 8",            # dow out of range (>7)
        # --- descriptors NOT supported by K8s ---
        "@reboot",
        "@every 5m",
        "@EVERY 1h",
        "@YEARLY",              # case-sensitive (docs are lowercase)
        # --- Quartz / Jenkins extensions -- not K8s ---
        "L * * * *",            # Quartz L (last)
        "? * * * *",            # Quartz ? (no-care)
        "15W * * * *",          # Quartz W (nearest weekday)
        "0 0 * * 5#3",          # Quartz # (nth DOW)
        "H * * * *",            # Jenkins H (hash)
        "LW * * * *",
    ],
)
def test_cron_mask_rejects_invalid(mask):
    with pytest.raises(ApiException) as excinfo:
        m.CronTrigger(mask=mask)
    assert excinfo.value.api_error is ApiErrorCode.API_2


# ---------------------------------------------------------------------------
# AutoPublisherInfo _api.dataclass + @auto_publisher decorator
# ---------------------------------------------------------------------------


def test_auto_publisher_dataclass_constructs_ok():
    src = _auto_src()
    p = _info.StreamPublisherInfo(
        _good_1in_1out_stream, src, False, 5, ["t1"], None,
    )
    assert p.source is src
    assert p.use_existing_data is False
    assert p.fallback_rollover_mins == 5
    assert p.output_tables == ["t1"]
    assert p.on_tables is None


def test_auto_publisher_dataclass_rollover_below_one_raises_api_10():
    # Field-level Field(ge=1) on FallbackRolloverMinsSpec triggers
    # AutoPublisherInfo's @_api.dataclass tag.
    with pytest.raises(ApiException) as excinfo:
        _info.StreamPublisherInfo(
            _good_1in_1out, _auto_src(), False, 0, ["t1"], None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_auto_publisher_dataclass_negative_rollover_raises_api_10():
    with pytest.raises(ApiException) as excinfo:
        _info.StreamPublisherInfo(
            _good_1in_1out, _auto_src(), True, -1, ["t1"], None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_auto_publisher_dataclass_empty_output_tables_raises_api_10():
    # TableNamesSpec min_length=1 invariant inherited from regular publishers.
    with pytest.raises(ApiException) as excinfo:
        _info.StreamPublisherInfo(
            _good_1in_1out, _auto_src(), False, 1, [], None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_auto_publisher_dataclass_signature_mismatch_raises_api_10():
    # output_tables has 1 entry, but the function returns a 2-element
    # tuple -- the after-validator catches this. Tag is still API_18.
    def bad_outputs(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None, tableframe.TableFrame | None]:
        return (t1, t1)

    with pytest.raises(ApiException) as excinfo:
        _info.StreamPublisherInfo(
            bad_outputs, _auto_src(), False, 1, ["t1"], None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_auto_publisher_decorator_returns_auto_publisher_instance():
    @m.stream_publisher(
        source=_auto_src(),
        use_existing_data=True,
        flush_interval_mins=10,
        output_tables=["t1"],
    )
    def my_pub(t1: list[tableframe.TableFrame | None] | None) -> tuple[tableframe.TableFrame | None]:
        return (None,)

    assert isinstance(my_pub, _info.StreamPublisherInfo)
    assert my_pub.use_existing_data is True
    assert my_pub.fallback_rollover_mins == 10


def test_auto_publisher_decorator_rejects_rollover_below_one_with_api_10():
    # Field-level Field(ge=1) failure surfaces as API_18 when the
    # AutoPublisherInfo _api.dataclass is constructed (i.e. when the
    # factory's returned decorator is applied to a function).
    with pytest.raises(ApiException) as excinfo:
        @m.stream_publisher(
            source=_auto_src(),
            use_existing_data=False,
            flush_interval_mins=0,
            output_tables=["t1"],
        )
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_auto_publisher_decorator_accepts_on_tables_hooks():
    @m.stream_publisher(
        source=_auto_src(),
        use_existing_data=False,
        flush_interval_mins=15,
        output_tables=["t1"],
        on_tables=[_OnTab()],
    )
    def my_pub(t1: list[tableframe.TableFrame | None] | None) -> tuple[tableframe.TableFrame | None]:
        return (None,)

    assert my_pub.on_tables is not None
    assert len(my_pub.on_tables) == 1


def test_auto_publisher_decorator_default_none_on_tables():
    @m.stream_publisher(
        source=_auto_src(),
        use_existing_data=False,
        flush_interval_mins=1,
        output_tables=["t1"],
    )
    def my_pub(t1: list[tableframe.TableFrame | None] | None) -> tuple[tableframe.TableFrame | None]:
        return (None,)

    assert my_pub.on_tables is None


def test_stream_publisher_rejects_trailing_trx_ctx_param():
    # ``@stream_publisher`` is the one decorator where the trailing
    # ``ctx: TrxCtx`` shape is rejected -- stream publishers run
    # inside the runtime's long-lived plugin loop, not per-trx, so
    # no ``TrxCtx`` is available at the call boundary.
    with pytest.raises(ApiException) as excinfo:
        @m.stream_publisher(
            source=_auto_src(),
            use_existing_data=False,
            flush_interval_mins=1,
            output_tables=["t1"],
        )
        def _my_pub(
            t1: list[tableframe.TableFrame | None] | None,
            ctx: m.TrxCtx,
        ) -> tuple[tableframe.TableFrame | None]:
            return (None,)
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_stream_publisher_rejects_any_extra_trailing_param():
    # Confirm the same is true for a non-TrxCtx extra param; the
    # error message should mention the no-ctx-allowed rule, not the
    # "TrxCtx required" rule that applies to the other three
    # decorators.
    with pytest.raises(ApiException) as excinfo:
        @m.stream_publisher(
            source=_auto_src(),
            use_existing_data=False,
            flush_interval_mins=1,
            output_tables=["t1"],
        )
        def _my_pub(
            t1: list[tableframe.TableFrame | None] | None,
            extra: dict,
        ) -> tuple[tableframe.TableFrame | None]:
            return (None,)
    assert excinfo.value.api_error is ApiErrorCode.API_18
    assert "trailing TrxCtx parameter is not supported here" in str(excinfo.value)


def test_subscriber_signature_allows_optional_trailing_trx_ctx():
    @m.subscriber(destination=_dest(), input_tables=["coll/t1"])
    def my_sub(
        t1: tableframe.TableFrame | None,
        ctx: m.TrxCtx,
    ) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_sub, _info.SubscriberInfo)


def test_transformer_signature_allows_optional_trailing_trx_ctx():
    @m.transformer(input_tables=["coll/t1"], output_tables=["out"])
    def my_trf(
        t1: tableframe.TableFrame | None,
        ctx: m.TrxCtx,
    ) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_trf, _info.TransformerInfo)


# ---------------------------------------------------------------------------
# AutoSrc / AutoSrcDef
# ---------------------------------------------------------------------------


def test_auto_src_defx_resolves_via_registry():
    from tabsdatak import _connectors
    defx = _connectors.get_stream_src(_StreamSrc)
    assert isinstance(defx, spi.StreamSrcDef)
    assert defx.src is _StreamSrc
    assert defx.src_type == "watch"
    assert defx.uses_manifest is False


def test_auto_src_def_rejects_missing_conn_field():
    # A class that does not subclass AutoSrc skips the inherited
    # `conn: Conn` annotation, so AutoSrcDef's conn-field check
    # fails with API_15.
    @_api.dataclass(ApiErrorCode.API_14)
    class _BadAutoSrcBase(ABC):
        pass

    with pytest.raises(ApiException) as excinfo:
        spi.StreamSrcDef(
        conn=m.Conn,
            src_type="bad",
            src_version="v1",
            src=_BadAutoSrcBase,
            table_mode=spi.TableMode.SINGLE,
            cardinality=lambda src: 1,
            uses_manifest=False,
            plugin=_TestStreamSrcPlugin,
            explorer=None,
        )

    assert excinfo.value.api_error is ApiErrorCode.API_15

# ===========================================================================
# Var / _LiteralVar (was test_api_env.py)
# ===========================================================================


def test_env_extends_base_env():
    # ``BaseVar`` in ``_api`` is the method-host for the public
    # ``Var`` -- `_name`, `_value`, `_validate`, `value` live there.
    # Asserting the inheritance keeps the base from being silently
    # dropped (which would re-introduce method duplication in
    # ``api.py``).
    assert issubclass(Var, _api.BaseVar)
    assert issubclass(_LiteralVar, _api.BaseVar)


def test_env_attributes(monkeypatch):
    monkeypatch.setenv("FOO", "foo")
    e = Var("FOO")
    assert e._name() == "FOO"
    assert isinstance(e, Var)
    assert e.s == "FOO"
    assert e._name() == "FOO"
    assert e._value() == "foo"
    assert e._value_pattern is None
    assert e._validate("foo") == "foo"

    e = Var.lit("foo")
    assert isinstance(e, Var)
    assert e.s == "foo"
    assert e._name() == "<literal>"
    assert e._value() == "foo"
    assert e._value_pattern is None
    assert e._validate("foo") == "foo"

@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithEnv(Src):
    e: Annotated[Var, _api.VarValueConstraints(pattern=r".+")]


def test_env_direct_construction():
    e = Var("DB_PASSWORD")
    assert isinstance(e, Var)
    assert e.s == "DB_PASSWORD"


def test_env_rejects_invalid_name():
    with pytest.raises(ApiException) as excinfo:
        Var("1abc")
    assert excinfo.value.api_error is ApiErrorCode.API_7


def test_env_value_resolves_environment_variable(monkeypatch):
    monkeypatch.setenv("TABSDATA_TEST_ENV", "resolved-value")
    e = Var("TABSDATA_TEST_ENV")
    assert e.value() == "resolved-value"


def test_env_value_raises_when_unset(monkeypatch):
    monkeypatch.delenv("TABSDATA_TEST_MISSING", raising=False)
    e = Var("TABSDATA_TEST_MISSING")
    with pytest.raises(ApiException) as excinfo:
        e.value()
    assert excinfo.value.api_error is ApiErrorCode.API_8


def test_env_round_trips_through_json():
    e = Var("HOME")
    back = Var.from_json(e.to_json())
    assert isinstance(back, Var)
    assert back.s == "HOME"


def test_env_envelope_kind_and_type_tag():
    e = Var("HOME")
    d = e.to_dict()
    assert d["kind"] == "varRef"
    assert d["type"].endswith(":Var")
    assert d["spec"] == {"s": "HOME", "_value_pattern": None}


def test_env_field_coerces_bare_string_to_literal_env():
    s = _SrcWithEnv(Var.lit("literal-value"))
    assert isinstance(s.e, _LiteralVar)
    assert isinstance(s.e, Var)
    assert s.e.value() == "literal-value"
    assert s.e.s == "literal-value"


def test_env_field_injects_pattern_into_literal():
    s = _SrcWithEnv(Var.lit("literal-value"))
    # Field's pattern is carried on the literal so it travels through
    # serialization and is enforced at value() time.
    assert s.e._value_pattern == r".+"


def test_env_field_accepts_explicit_env_instance_and_injects_pattern():
    s = _SrcWithEnv(Var("HOME"))
    assert isinstance(s.e, Var)
    assert s.e.s == "HOME"
    # Field's pattern is reconstructed onto the ref so it survives
    # serialization and gates the resolved value at value() time.
    assert s.e._value_pattern == r".+"


def test_literal_env_value_returns_literal():
    lit = _LiteralVar("hello")
    assert lit.value() == "hello"


def test_literal_env_round_trips_through_json():
    lit = _LiteralVar("literal-secret")
    back = _LiteralVar.from_json(lit.to_json())
    assert isinstance(back, _LiteralVar)
    assert back.s == "literal-secret"


def test_literal_env_envelope_kind_and_type_tag():
    lit = _LiteralVar("x")
    d = lit.to_dict()
    assert d["kind"] == "varLiteral"
    assert d["type"].endswith(":_LiteralVar")
    assert d["spec"]["s"] == "x"


def test_env_field_serializes_literal_env_subclass_completely():
    s = _SrcWithEnv(Var.lit("from-literal"))
    d = s.to_dict()
    inner = d["spec"]["e"]
    assert inner["kind"] == "varLiteral"
    assert inner["spec"]["s"] == "from-literal"


def test_env_field_round_trips_literal_env_subclass():
    s = _SrcWithEnv(Var.lit("from-literal"))
    back = _SrcWithEnv.from_json(s.to_json())
    assert isinstance(back.e, _LiteralVar)
    assert back.e.s == "from-literal"


def test_env_field_round_trips_explicit_env():
    s = _SrcWithEnv(Var("HOME"))
    back = _SrcWithEnv.from_json(s.to_json())
    assert isinstance(back.e, Var)
    assert back.e.s == "HOME"


def test_env_value_no_pattern_accepts_anything(monkeypatch):
    monkeypatch.setenv("TABSDATA_TEST_FREE", "anything-goes!@#")
    e = Var("TABSDATA_TEST_FREE")
    assert e.value() == "anything-goes!@#"


def test_literal_env_value_pattern_accepts_matching_literal():
    lit = _LiteralVar(s="42", _value_pattern=r"\d+")
    assert lit.value() == "42"


def test_literal_env_value_pattern_rejects_non_matching_literal_at_construction():
    # Eager check: a literal whose value does not match its
    # ``_value_pattern`` is rejected at construction. The pattern code
    # (API_6) surfaces directly -- the eager model_validator raises
    # an ApiException that pydantic propagates unwrapped.
    with pytest.raises(ApiException) as excinfo:
        _LiteralVar("abc", _value_pattern=r"\d+")
    assert excinfo.value.api_error is ApiErrorCode.API_6


def test_literal_env_value_pattern_requires_full_match_at_construction():
    with pytest.raises(ApiException) as excinfo:
        _LiteralVar("42x", _value_pattern=r"\d+")
    assert excinfo.value.api_error is ApiErrorCode.API_6


def test_literal_env_no_pattern_accepts_any_literal():
    lit = _LiteralVar("anything-goes!@#")
    assert lit.value() == "anything-goes!@#"


def test_literal_env_value_pattern_round_trips_through_json():
    lit = _LiteralVar("42", _value_pattern=r"\d+")
    back = _LiteralVar.from_json(lit.to_json())
    assert back._value_pattern == r"\d+"
    assert back.value() == "42"


def test_literal_env_inconsistent_payload_rejected_on_deserialize():
    # An inconsistent payload (value does not match the carried
    # ``_value_pattern``) is rejected at load time -- the literal's
    # eager validator runs after the envelope dispatcher rebuilds the
    # instance, raising the API_6 pattern violation directly.
    bad = {
        "kind": "varLiteral",
        "apiVersion": "1.0",
        "type": "tabsdatak.api:_LiteralVar",
        "spec": {"s": "abc", "_value_pattern": r"\d+"},
    }
    with pytest.raises(ApiException) as excinfo:
        _LiteralVar.from_dict(bad)
    assert excinfo.value.api_error is ApiErrorCode.API_6


# ---------------------------------------------------------------------------
# ``valOrRefPattern`` annotation marker
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithDigitsField(Src):
    e: Annotated[Var, _api.VarValueConstraints(pattern=r"\d+")]


def test_env_pattern_annotation_str_shortcut_validates_eagerly():
    # The literal that the shortcut builds carries the field's regex;
    # the eager literal validator runs at construction.
    with pytest.raises(ApiException):
        _SrcWithDigitsField(Var.lit("abc"))


def test_env_pattern_annotation_str_shortcut_accepts_matching_value():
    s = _SrcWithDigitsField(Var.lit("42"))
    assert isinstance(s.e, _LiteralVar)
    assert s.e.s == "42"
    assert s.e._value_pattern == r"\d+"


def test_env_pattern_annotation_injects_into_ref():
    s = _SrcWithDigitsField(Var("DIGITS"))
    # Refs do not run the literal validator -- the pattern is just
    # injected so it travels with the instance and gates value().
    assert isinstance(s.e, Var)
    assert s.e._value_pattern == r"\d+"


def test_env_pattern_annotation_round_trips_pattern():
    s = _SrcWithDigitsField(Var("DIGITS"))
    back = _SrcWithDigitsField.from_json(s.to_json())
    assert back.e._value_pattern == r"\d+"


def test_pattern_annotation_ref_pattern_fires_on_value(monkeypatch):
    # With a ``valOrRefPattern`` injected, the resolved var value
    # is validated against the regex at ``value()`` time.
    monkeypatch.setenv("TEST_PATTERN_REF", "abc")
    s = _SrcWithDigitsField(Var("TEST_PATTERN_REF"))
    with pytest.raises(ApiException) as excinfo:
        s.e.value()
    assert excinfo.value.api_error is ApiErrorCode.API_6

    monkeypatch.setenv("TEST_PATTERN_REF", "42")
    assert s.e.value() == "42"


@_api.dataclass(ApiErrorCode.API_7)
class _MyData(Conn):
    digits: Annotated[Var, _api.VarValueConstraints(pattern=r"\d+")]


def test_env_pattern_in_conn_round_trips_through_json(monkeypatch):
    # A pattern-bearing Var field nested in a Conn round-trips with the
    # pattern intact. A resolved var that violates the pattern still
    # raises API_6 on the reconstructed instance.
    monkeypatch.setenv("VAL", "42x")
    data = _MyData(digits=Var("VAL"))
    back = _MyData.from_json(data.to_json())

    assert isinstance(back.digits, Var)
    assert back.digits.s == "VAL"
    assert back.digits._value_pattern == r"\d+"

    with pytest.raises(ApiException) as excinfo:
        back.digits.value()
    assert excinfo.value.api_error is ApiErrorCode.API_6

    monkeypatch.setenv("VAL", "42")
    assert back.digits.value() == "42"


def test_env_ref_to_literal_substitution_in_json_round_trip():
    # Realistic flow: a producer serializes a Conn carrying an Var
    # *ref*; an intermediary resolves the var and replaces the
    # inner envelope with a ``literalEnv`` carrying the resolved value;
    # the consumer deserializes through the same field and gets the
    # value back without needing the env var to exist at load time.
    holder = _MyData(digits=Var("VAL"))
    payload = json.loads(holder.to_json())

    # Confirm starting shape: the field carries an ``varRef`` envelope.
    assert payload["spec"]["digits"]["kind"] == "varRef"

    # Substitute the ref with a literal carrying the resolved value.
    # Pattern is preserved so the receiver still validates the literal.
    pattern = payload["spec"]["digits"]["spec"]["_value_pattern"]
    payload["spec"]["digits"] = {
        "kind": "varLiteral",
        "apiVersion": "1.0",
        "type": "tabsdatak.api:_LiteralVar",
        "spec": {"s": "42", "_value_pattern": pattern},
    }

    back = _MyData.from_json(json.dumps(payload))
    assert isinstance(back.digits, _LiteralVar)
    assert back.digits.value() == "42"
    assert back.digits._value_pattern == r"\d+"

# ===========================================================================
# Secret / _LiteralSecret (was test_api_secret.py)
# ===========================================================================


def test_secret_extends_base_secret():
    # ``BaseSecret`` in ``_api`` is the method-host for the public
    # ``Secret`` -- `_name`, `_value`, `_validate`, `value`,
    # `_secret_key`, and the empty-init guard live there. Asserting
    # the inheritance keeps the base from being silently dropped.
    assert issubclass(Secret, _api.BaseSecret)
    assert issubclass(_LiteralSecret, _api.BaseSecret)


def test_secret_attributes():
    # ``Secret()`` builds an anonymous *ref* with the placeholder
    # ``_s`` -- there is no public way to attach a key/name; the
    # secret-store integration produces the real ref out-of-band.
    e = Secret()
    assert isinstance(e, Secret)
    assert e._s == "<secret>"
    assert e._name() == "<secret>"
    # ``_value()`` is the unresolved-ref hook; without a resolver wired
    # up it raises API_11 (``ApiInternalException``).
    with pytest.raises(ApiInternalException):
        e._value()
    assert e._value_pattern is None
    assert e._validate("foo") == "foo"

    # ``Secret.lit(value)`` is the literal factory -- returns a
    # ``_LiteralSecret`` whose ``_s`` is the literal value.
    e = Secret.lit("foo")
    assert isinstance(e, _LiteralSecret)
    assert e._s == "foo"
    assert e._name() == "<literal>"
    assert e._value() == "foo"
    assert e._value_pattern is None
    assert e._validate("foo") == "foo"

@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithSecret(Src):
    s: Annotated[Secret, _api.SecretValueConstraints(pattern=r".+")]


def test_secret_direct_construction():
    s = Secret()
    assert isinstance(s, Secret)
    # Refs are anonymous by default; ``Secret()`` always builds the
    # same placeholder shape (the secret-store integration is what
    # eventually swaps it for a resolved literal).
    assert s._s == "<secret>"


def test_secret_constructor_is_empty_rejects_kwargs():
    with pytest.raises(ApiException) as ei:
        Secret(_s="vault#KEY")
    assert ei.value.api_error is ApiErrorCode.API_10


def test_secret_constructor_is_empty_rejects_value_pattern_kwarg():
    with pytest.raises(ApiException) as ei:
        Secret(_value_pattern=r".+")
    assert ei.value.api_error is ApiErrorCode.API_10


def test_secret_constructor_is_empty_rejects_positional_args():
    with pytest.raises(ApiException) as ei:
        Secret("vault#KEY")
    assert ei.value.api_error is ApiErrorCode.API_10


def test_literal_secret_constructor_still_accepts_args():
    # The empty-init guard fires only on `Secret` itself, never on
    # `_LiteralSecret` -- the literal subclass legitimately takes a
    # value at construction (it's how `Secret.lit` builds them).
    lit = _LiteralSecret("hunter2")
    assert lit._s == "hunter2"


def test_secret_value_raises_without_resolver():
    # Refs do not resolve in-process: there is no secret-store
    # integration in the API package, so ``value()`` always raises API_11.
    s = Secret()
    with pytest.raises(ApiException) as excinfo:
        s.value()
    assert excinfo.value.api_error is ApiErrorCode.API_11


def test_secret_round_trips_through_json():
    s = Secret()
    back = Secret.from_json(s.to_json())
    assert isinstance(back, Secret)
    assert back._s == "<secret>"


def test_secret_envelope_kind_and_type_tag():
    s = Secret()
    d = s.to_dict()
    assert d["kind"] == "secretRef"
    assert d["type"].endswith(":Secret")
    assert d["spec"] == {"_s": "<secret>", "_value_pattern": None}


def test_secret_field_coerces_bare_string_to_literal_secret():
    s = _SrcWithSecret(Secret.lit("hunter2"))
    assert isinstance(s.s, _LiteralSecret)
    assert isinstance(s.s, Secret)
    assert s.s.value() == "hunter2"
    assert s.s._s == "hunter2"


def test_secret_field_injects_pattern_into_literal():
    s = _SrcWithSecret(Secret.lit("hunter2"))
    # Field's pattern is carried on the literal so it travels through
    # serialization and is enforced at value() time.
    assert s.s._value_pattern == r".+"


def test_secret_field_accepts_explicit_secret_instance_and_injects_pattern():
    s = _SrcWithSecret(Secret())
    assert isinstance(s.s, Secret)
    assert s.s._s == "<secret>"
    # Field's pattern is reconstructed onto the ref so it survives
    # serialization and gates the resolved value at value() time.
    assert s.s._value_pattern == r".+"


def test_literal_secret_value_returns_literal():
    lit = _LiteralSecret("hunter2")
    assert lit.value() == "hunter2"


def test_literal_secret_round_trips_through_json():
    lit = _LiteralSecret("hunter2")
    back = _LiteralSecret.from_json(lit.to_json())
    assert isinstance(back, _LiteralSecret)
    assert back._s == "hunter2"


def test_literal_secret_envelope_kind_and_type_tag():
    lit = _LiteralSecret("x")
    d = lit.to_dict()
    assert d["kind"] == "secretLiteral"
    assert d["type"].endswith(":_LiteralSecret")
    assert d["spec"]["_s"] == "x"


def test_secret_field_serializes_literal_secret_subclass_completely():
    s = _SrcWithSecret(Secret.lit("from-literal"))
    d = s.to_dict()
    inner = d["spec"]["s"]
    assert inner["kind"] == "secretLiteral"
    assert inner["spec"]["_s"] == "from-literal"


def test_secret_field_round_trips_literal_secret_subclass():
    s = _SrcWithSecret(Secret.lit("from-literal"))
    back = _SrcWithSecret.from_json(s.to_json())
    assert isinstance(back.s, _LiteralSecret)
    assert back.s._s == "from-literal"


def test_secret_field_round_trips_explicit_secret():
    s = _SrcWithSecret(Secret())
    back = _SrcWithSecret.from_json(s.to_json())
    assert isinstance(back.s, Secret)
    assert back.s._s == "<secret>"


def test_literal_secret_value_pattern_accepts_matching_literal():
    lit = _LiteralSecret(_s="deadBEEF", _value_pattern=r"[0-9a-fA-F]+")
    assert lit.value() == "deadBEEF"


def test_literal_secret_value_pattern_rejects_non_matching_literal_at_construction():
    # Eager check: a literal whose value does not match its
    # ``_value_pattern`` is rejected at construction. The pattern code
    # (API_6) surfaces directly -- the eager model_validator raises
    # an ApiException that pydantic propagates unwrapped.
    with pytest.raises(ApiException) as excinfo:
        _LiteralSecret("not-hex!", _value_pattern=r"[0-9a-fA-F]+")
    assert excinfo.value.api_error is ApiErrorCode.API_6


def test_literal_secret_value_pattern_requires_full_match_at_construction():
    with pytest.raises(ApiException) as excinfo:
        _LiteralSecret("abc-not-hex", _value_pattern=r"[0-9a-fA-F]+")
    assert excinfo.value.api_error is ApiErrorCode.API_6


def test_literal_secret_no_pattern_accepts_any_literal():
    lit = _LiteralSecret("anything-goes!@#")
    assert lit.value() == "anything-goes!@#"


def test_literal_secret_value_pattern_round_trips_through_json():
    lit = _LiteralSecret("cafe", _value_pattern=r"[0-9a-fA-F]+")
    back = _LiteralSecret.from_json(lit.to_json())
    assert back._value_pattern == r"[0-9a-fA-F]+"
    assert back.value() == "cafe"


def test_literal_secret_inconsistent_payload_rejected_on_deserialize():
    # An inconsistent payload (value does not match the carried
    # ``_value_pattern``) is rejected at load time -- the literal's
    # eager validator runs after the envelope dispatcher rebuilds the
    # instance, raising the API_6 pattern violation directly.
    bad = {
        "kind": "secretLiteral",
        "apiVersion": "1.0",
        "type": "tabsdatak.api:_LiteralSecret",
        "spec": {"_s": "not-hex!", "_value_pattern": r"[0-9a-fA-F]+"},
    }
    with pytest.raises(ApiException) as excinfo:
        _LiteralSecret.from_dict(bad)
    assert excinfo.value.api_error is ApiErrorCode.API_6


# ---------------------------------------------------------------------------
# ``SecretValuePattern`` annotation marker
# ---------------------------------------------------------------------------


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithHexField(Src):
    s: Annotated[Secret, _api.SecretValueConstraints(pattern=r"[0-9a-fA-F]+")]


def test_secret_pattern_annotation_str_shortcut_validates_eagerly():
    # The literal that the shortcut builds carries the field's regex;
    # the eager literal validator runs at construction.
    with pytest.raises(ApiException):
        _SrcWithHexField(Secret.lit("not-hex!"))


def test_secret_pattern_annotation_str_shortcut_accepts_matching_value():
    s = _SrcWithHexField(Secret.lit("cafe"))
    assert isinstance(s.s, _LiteralSecret)
    assert s.s._s == "cafe"
    assert s.s._value_pattern == r"[0-9a-fA-F]+"


def test_secret_pattern_annotation_injects_into_ref():
    s = _SrcWithHexField(Secret())
    # Refs do not run the literal validator -- the pattern is just
    # injected so it travels with the instance and gates value().
    assert isinstance(s.s, Secret)
    assert s.s._value_pattern == r"[0-9a-fA-F]+"


def test_secret_pattern_annotation_round_trips_pattern():
    s = _SrcWithHexField(Secret())
    back = _SrcWithHexField.from_json(s.to_json())
    assert back.s._value_pattern == r"[0-9a-fA-F]+"


@_api.dataclass(ApiErrorCode.API_10)
class _HexHolder(Conn):
    token: Annotated[Secret, _api.SecretValueConstraints(pattern=r"[0-9a-fA-F]+")]


def test_secret_pattern_in_conn_round_trips_through_json():
    # A pattern-bearing Secret field nested in a Conn round-trips with
    # both the class identity and the pattern intact.
    holder = _HexHolder(token=Secret())
    back = _HexHolder.from_json(holder.to_json())

    assert isinstance(back.token, Secret)
    assert back.token._s == "<secret>"
    assert back.token._value_pattern == r"[0-9a-fA-F]+"


def test_secret_ref_to_literal_substitution_in_json_round_trip():
    # Realistic flow: a producer serializes a Conn carrying a Secret
    # *ref*; an intermediary (e.g. a secret-store integration)
    # resolves the ref and replaces the inner envelope with a
    # ``literalSecret`` carrying the resolved value; the consumer
    # deserializes through the same field and gets the value back.
    holder = _HexHolder(token=Secret())
    payload = json.loads(holder.to_json())

    # Confirm starting shape: the field carries a ``secretRef`` envelope.
    assert payload["spec"]["token"]["kind"] == "secretRef"

    # Substitute the ref with a literal carrying the resolved value.
    # Pattern is preserved so the receiver still validates the literal.
    pattern = payload["spec"]["token"]["spec"]["_value_pattern"]
    payload["spec"]["token"] = {
        "kind": "secretLiteral",
        "apiVersion": "1.0",
        "type": "tabsdatak.api:_LiteralSecret",
        "spec": {"_s": "cafe", "_value_pattern": pattern},
    }

    back = _HexHolder.from_json(json.dumps(payload))
    assert isinstance(back.token, _LiteralSecret)
    assert back.token.value() == "cafe"
    assert back.token._value_pattern == r"[0-9a-fA-F]+"


# ---------------------------------------------------------------------------
# ``Secret._secret_key`` classmethod
# ---------------------------------------------------------------------------


def test_secret_secret_key_builds_named_ref():
    # The default ``Secret()`` is anonymous (``_s == "<secret>"``).
    # ``_secret_key`` is the (private) factory secret-store integrations
    # use to attach a real key/name to a ref.
    s = Secret._secret_key("vault#TOKEN")
    assert isinstance(s, Secret)
    assert not isinstance(s, _LiteralSecret)
    assert s._s == "vault#TOKEN"
    assert s._name() == "vault#TOKEN"


def test_secret_secret_key_still_has_no_resolver():
    # Naming the ref does not wire up a resolver -- ``value()`` still
    # raises API_11.
    s = Secret._secret_key("vault#TOKEN")
    with pytest.raises(ApiException) as excinfo:
        s.value()
    assert excinfo.value.api_error is ApiErrorCode.API_11


def test_secret_secret_key_round_trips_through_json():
    s = Secret._secret_key("vault#TOKEN")
    back = Secret.from_json(s.to_json())
    assert isinstance(back, Secret)
    assert back._s == "vault#TOKEN"


# ---------------------------------------------------------------------------
# ``SecretOrVarSpec`` Union alias
# ---------------------------------------------------------------------------


from tabsdatak.api import SecretOrVarSpec  # noqa: E402


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithSecretOrEnv(Src):
    v: SecretOrVarSpec


def test_secret_or_env_accepts_env_ref():
    s = _SrcWithSecretOrEnv(Var("HOME"))
    assert isinstance(s.v, Var)
    assert not isinstance(s.v, Secret)
    assert s.v.s == "HOME"


def test_secret_or_env_accepts_secret_ref():
    s = _SrcWithSecretOrEnv(Secret())
    assert isinstance(s.v, Secret)
    assert not isinstance(s.v, Var)
    assert s.v._s == "<secret>"


def test_secret_or_env_accepts_literal_env():
    s = _SrcWithSecretOrEnv(Var.lit("hello"))
    assert isinstance(s.v, _LiteralVar)
    assert s.v.value() == "hello"


def test_secret_or_env_accepts_literal_secret():
    s = _SrcWithSecretOrEnv(Secret.lit("hunter2"))
    assert isinstance(s.v, _LiteralSecret)
    assert s.v.value() == "hunter2"


def test_secret_or_env_round_trips_env_branch():
    s = _SrcWithSecretOrEnv(Var("HOME"))
    back = _SrcWithSecretOrEnv.from_json(s.to_json())
    assert isinstance(back.v, Var)
    assert not isinstance(back.v, Secret)
    assert back.v.s == "HOME"


def test_secret_or_env_round_trips_secret_branch():
    s = _SrcWithSecretOrEnv(Secret())
    back = _SrcWithSecretOrEnv.from_json(s.to_json())
    assert isinstance(back.v, Secret)
    assert not isinstance(back.v, Var)
    assert back.v._s == "<secret>"


def test_secret_or_env_round_trips_literal_env_branch():
    s = _SrcWithSecretOrEnv(Var.lit("hello"))
    back = _SrcWithSecretOrEnv.from_json(s.to_json())
    assert isinstance(back.v, _LiteralVar)
    assert back.v.s == "hello"


def test_secret_or_env_round_trips_literal_secret_branch():
    s = _SrcWithSecretOrEnv(Secret.lit("hunter2"))
    back = _SrcWithSecretOrEnv.from_json(s.to_json())
    assert isinstance(back.v, _LiteralSecret)
    assert back.v._s == "hunter2"


# ---------------------------------------------------------------------------
# Wrap-validator: pattern reconstruction vs. fast-path skip
#
# When an instance arrives at a field annotated with a pattern, the
# wrap-validator either:
#   - returns it unchanged (fast path) when its ``_value_pattern``
#     already equals the field's regex, or
#   - reconstructs it with the field's regex installed -- which
#     re-runs the literal's eager ``model_validator``, so a literal
#     whose value violates the new pattern is rejected at the call site.
# ---------------------------------------------------------------------------


def test_env_pattern_skips_reconstruction_when_pattern_already_matches():
    # Fast path: literal already carries the field's regex -- return as-is.
    incoming = _LiteralVar(s="42", _value_pattern=r"\d+")
    s = _SrcWithDigitsField(incoming)
    assert s.e is incoming


def test_env_pattern_reconstructs_literal_when_incoming_pattern_differs():
    # Reconstruction: incoming literal carries ``r".+"`` but the field
    # imposes ``r"\d+"``. Value "42" still matches the new pattern, so
    # the rebuilt literal succeeds and carries the field's regex.
    incoming = _LiteralVar(s="42", _value_pattern=r".+")
    s = _SrcWithDigitsField(incoming)
    assert isinstance(s.e, _LiteralVar)
    assert s.e._value_pattern == r"\d+"
    assert s.e.s == "42"
    # Reconstruction produces a new instance, not the same identity.
    assert s.e is not incoming


def test_env_pattern_reconstructs_and_rejects_literal_failing_new_pattern():
    # Reconstruction triggers the literal's eager validator against
    # the field's regex. "abc" matched the old ``r".+"`` but not
    # ``r"\d+"`` -- the rebuilt literal raises API_6.
    incoming = _LiteralVar(s="abc", _value_pattern=r".+")
    with pytest.raises(ApiException) as excinfo:
        _SrcWithDigitsField(incoming)
    assert excinfo.value.api_error is ApiErrorCode.API_6


def test_env_pattern_reconstructs_ref_when_incoming_pattern_differs():
    # Refs don't run the literal validator, but the field's regex
    # still replaces the incoming one so it travels with the instance
    # and gates ``value()`` later.
    incoming = Var(s="HOME", _value_pattern=r".+")
    s = _SrcWithDigitsField(incoming)
    assert isinstance(s.e, Var)
    assert not isinstance(s.e, _LiteralVar)
    assert s.e._value_pattern == r"\d+"


def test_secret_pattern_skips_reconstruction_when_pattern_already_matches():
    incoming = _LiteralSecret(_s="cafe", _value_pattern=r"[0-9a-fA-F]+")
    s = _SrcWithHexField(incoming)
    assert s.s is incoming


def test_secret_pattern_reconstructs_literal_when_incoming_pattern_differs():
    incoming = _LiteralSecret(_s="cafe", _value_pattern=r".+")
    s = _SrcWithHexField(incoming)
    assert isinstance(s.s, _LiteralSecret)
    assert s.s._value_pattern == r"[0-9a-fA-F]+"
    assert s.s._s == "cafe"
    assert s.s is not incoming


def test_secret_pattern_reconstructs_and_rejects_literal_failing_new_pattern():
    incoming = _LiteralSecret(_s="not-hex!", _value_pattern=r".+")
    with pytest.raises(ApiException) as excinfo:
        _SrcWithHexField(incoming)
    assert excinfo.value.api_error is ApiErrorCode.API_6


def test_secret_pattern_reconstructs_ref_when_incoming_pattern_differs():
    # Frame this Secret with both private fields populated -- the
    # public `Secret()` user-init is empty-only, so synthesize via the
    # pydantic validator (the same path envelope round-trips use).
    incoming = Secret.__pydantic_validator__.validate_python(
        {"_s": "vault#TOKEN", "_value_pattern": r".+"}
    )
    s = _SrcWithHexField(incoming)
    assert isinstance(s.s, Secret)
    assert not isinstance(s.s, _LiteralSecret)
    assert s.s._value_pattern == r"[0-9a-fA-F]+"
