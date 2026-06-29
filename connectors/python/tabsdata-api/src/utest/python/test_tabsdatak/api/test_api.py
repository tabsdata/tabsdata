from abc import ABC

import pytest

from tabsdatak import _api
from tabsdatak.error import ApiErrorCode, ApiException, ApiInternalException
from tabsdatak import api as m, tableframe
from tabsdatak import _connectors as _connectors
from tabsdatak import spi
from tabsdatak import _info
import json
from typing import Annotated, Union

from pydantic import StringConstraints

from tabsdatak.api import (
    Conn,
    Secret,
    Src,
)


# Test-only SPI plugins. SrcDef / DestDef require a concrete
# `plugin` class; the framework never instantiates these in tests, they
# exist only to satisfy the field contract.
class _TestSrcPlugin(spi.SrcPlugin):
    ...


class _TestDestPlugin(spi.DestPlugin):
    ...


# Concrete connection fixture. ``conn`` is no longer a field on
# ``Src`` / ``Dest`` / ``StreamSrc`` -- it is passed to the decorators
# (``@publisher`` ...) and threaded to the plugin entry methods. The
# decorators take a ``Conn`` *instance*, so the tests need a concrete
# (non-abstract) subclass to construct.
@_api.dataclass(ApiErrorCode.API_13)
class _Conn(m.Conn):
    pass


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
    plugin=_TestStreamSrcPlugin,
    publisher_src=spi.StreamStageSrc,
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
    p = _info.PublisherInfo(_good_1in_1out, _Conn(), src, ["t1"], None, None)
    assert p.source is src
    assert p.output_tables == ["t1"]
    assert p.trigger_by is None
    assert p.on_tables is None


def test_publisher_dataclass_empty_tables_raises_api_4():
    # Field-level failure on TableNamesSpec min_length short-circuits before the
    # model_validator that checks `func` signature, so any callable is fine.
    with pytest.raises(ApiException) as excinfo:
        _info.PublisherInfo(_good_1in_1out, _Conn(), _src(), [], None, None)
    assert excinfo.value.api_error is ApiErrorCode.API_17


# ---------------------------------------------------------------------------
# @publisher decorator factory
# ---------------------------------------------------------------------------

def test_publisher_decorator_returns_publisher_instance():
    @m.publisher(conn=_Conn(), source=_src(), output_tables=["t1"])
    def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_pub, _info.PublisherInfo)
    assert callable(my_pub)


def test_publisher_descriptor_call_raises_typeerror_pointing_at_func():
    @m.publisher(conn=_Conn(), source=_src(), output_tables=["t1"])
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
        @m.publisher(conn=_Conn(), source=_src(), output_tables=["t1"])
        def _bad() -> tuple[tableframe.TableFrame | None]:  # zero params, expects 1
            return (None,)
    msg = str(excinfo.value)
    assert "Expected signature" in msg
    assert "TableFrame" in msg


def test_publisher_decorator_rejects_empty_tables_with_api_4():
    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=_src(), output_tables=[])
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_17


def test_publisher_decorator_accepts_cron_trigger():
    cron = m.CronTrigger(mask="*/5 * * * *")

    @m.publisher(conn=_Conn(), source=_src(), output_tables=["t1"], trigger_by=cron)
    def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert my_pub.trigger_by is cron


def test_publisher_decorator_accepts_tables_trigger():
    # Typed wrapper around the bare-list shortcut.
    trig = m.TablesTrigger(tables=["users", "orders"])

    @m.publisher(conn=_Conn(), source=_src(), output_tables=["t1"], trigger_by=trig)
    def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert my_pub.trigger_by is trig


def test_publisher_decorator_rejects_bare_table_list():
    # Bare-list shortcut was removed; only typed Trigger subclasses
    # are accepted (TablesTrigger / CronTrigger). The framework
    # tags the type-mismatch as API_17 (publisher validation).
    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=_src(), output_tables=["t1"],
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

    @m.subscriber(conn=_Conn(), destination=dest, input_tables=["in1"], trigger_by=trig)
    def my_sub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_sub, _info.SubscriberInfo)
    assert my_sub.destination is dest
    assert my_sub.trigger_by is trig


def test_subscriber_decorator_rejects_empty_input_tables_with_api_6():
    with pytest.raises(ApiException) as excinfo:
        @m.subscriber(conn=_Conn(), destination=_dest(), input_tables=[])
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_20


def test_subscriber_decorator_allows_default_none_trigger():
    @m.subscriber(conn=_Conn(), destination=_dest(), input_tables=["in1"])
    def my_sub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert isinstance(my_sub, _info.SubscriberInfo)
    assert my_sub.trigger_by is None


def test_subscriber_decorator_accepts_all_tables_trigger():
    trig = m.AllTablesTrigger()

    @m.subscriber(conn=_Conn(), 
        destination=_dest(), input_tables=["in1"], trigger_by=trig,
    )
    def my_sub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
        return (t1,)

    assert my_sub.trigger_by is trig


def test_subscriber_decorator_accepts_tables_trigger():
    trig = m.TablesTrigger(tables=["src/orders", "src/users"])

    @m.subscriber(conn=_Conn(), 
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
    m.publisher(conn=_Conn(), source=src, output_tables=[name])  # no error


@pytest.mark.parametrize(
    "name", ["", "1foo", "foo bar", "foo-bar", "X" * 65, "_foo"],
)
def test_table_name_rejects_invalid(name):
    src = _src()
    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=src, output_tables=[name])
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

    @m.publisher(conn=_Conn(), source=src, output_tables=["a", "b"])
    def my_pub(
        t1: tableframe.TableFrame | None, t2: tableframe.TableFrame | None,
    ) -> tuple[tableframe.TableFrame | None, tableframe.TableFrame | None]:
        return (t1, t2)

    assert isinstance(my_pub, _info.PublisherInfo)


def test_publisher_signature_rejects_wrong_input_arity():
    src = _src2()  # expects 2 input slots

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=src, output_tables=["a", "b"])
        def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None, tableframe.TableFrame | None]:  # only 1
            return (t1, t1)
    assert excinfo.value.api_error is ApiErrorCode.API_17
    assert "at least 2 parameter" in str(excinfo.value)


def test_publisher_signature_rejects_wrong_output_arity():
    src = _src()  # table_count=1

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=src, output_tables=["a", "b"])  # expects 2
        def my_pub(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:  # returns 1
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_17


def test_publisher_signature_rejects_missing_return_annotation():
    src = _src()

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=src, output_tables=["a"])
        def my_pub(t1: tableframe.TableFrame | None):
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_17
    assert "return type must be annotated" in str(excinfo.value)


def test_publisher_signature_rejects_variable_length_tuple():
    src = _src()

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=src, output_tables=["a"])
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

    @m.subscriber(conn=_Conn(), destination=dest, input_tables=["a"])
    def my_sub(
        t1: tableframe.TableFrame | None,
    ) -> tuple[tableframe.TableFrame | None, tableframe.TableFrame | None, tableframe.TableFrame | None]:
        return (t1, t1, t1)

    assert isinstance(my_sub, _info.SubscriberInfo)


def test_subscriber_signature_rejects_mismatched_output_arity():
    dest = _dest3()

    with pytest.raises(ApiException) as excinfo:
        @m.subscriber(conn=_Conn(), destination=dest, input_tables=["a"])
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

    @m.publisher(conn=_Conn(), source=src, output_tables=["a"])
    def my_pub(
        t1: list[tableframe.TableFrame | None] | None, t2: list[tableframe.TableFrame | None] | None,
    ) -> tuple[tableframe.TableFrame | None]:
        return (None,)

    assert isinstance(my_pub, _info.PublisherInfo)


def test_publisher_input_rejects_single_element_when_mode_is_array():
    src = _src_array()  # ARRAY, 2 slots

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=src, output_tables=["a"])
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
        @m.publisher(conn=_Conn(), source=src, output_tables=["a"])
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
        @m.publisher(conn=_Conn(), source=src, output_tables=["a"])
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
        @m.publisher(conn=_Conn(), source=src, output_tables=["t"])
        def _my_pub(
            t1: tableframe.TableFrame | None,
            config: dict[str, str],
            logger: object,
        ) -> tuple[tableframe.TableFrame | None]:
            return (t1,)

    assert excinfo.value.api_error is ApiErrorCode.API_17


def test_publisher_signature_allows_optional_trailing_trx_ctx():
    src = _src()  # table_count=1

    @m.publisher(conn=_Conn(), source=src, output_tables=["t"])
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
        @m.publisher(conn=_Conn(), source=src, output_tables=["t"])
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
        def set_attr(self, attr, val): pass
        def add_metadata(self, attr, val): pass

    def f(t1: tableframe.TableFrame | None, ctx: _MyCtx) -> tuple[
        tableframe.TableFrame | None
    ]:
        return (t1,)

    assert _api.has_trx_ctx_param(f) is True


def test_publisher_signature_accepts_explicit_table_frame_or_none():
    src = _src()

    @m.publisher(conn=_Conn(), source=src, output_tables=["t"])
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
        _good_1in_1out_stream, _Conn(), src, 5, ["t1"], None,
    )
    assert p.source is src
    assert p.fallback_rollover_mins == 5
    assert p.output_tables == ["t1"]
    assert p.on_tables is None


def test_auto_publisher_dataclass_rollover_below_one_raises_api_10():
    # Field-level Field(ge=1) on FallbackRolloverMinsSpec triggers
    # AutoPublisherInfo's @_api.dataclass tag.
    with pytest.raises(ApiException) as excinfo:
        _info.StreamPublisherInfo(
            _good_1in_1out, _Conn(), _auto_src(), 0, ["t1"], None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_auto_publisher_dataclass_negative_rollover_raises_api_10():
    with pytest.raises(ApiException) as excinfo:
        _info.StreamPublisherInfo(
            _good_1in_1out, _Conn(), _auto_src(), -1, ["t1"], None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_auto_publisher_dataclass_empty_output_tables_raises_api_10():
    # TableNamesSpec min_length=1 invariant inherited from regular publishers.
    with pytest.raises(ApiException) as excinfo:
        _info.StreamPublisherInfo(
            _good_1in_1out, _Conn(), _auto_src(), 1, [], None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_auto_publisher_dataclass_signature_mismatch_raises_api_10():
    # output_tables has 1 entry, but the function returns a 2-element
    # tuple -- the after-validator catches this. Tag is still API_18.
    def bad_outputs(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None, tableframe.TableFrame | None]:
        return (t1, t1)

    with pytest.raises(ApiException) as excinfo:
        _info.StreamPublisherInfo(
            bad_outputs, _Conn(), _auto_src(), 1, ["t1"], None,
        )
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_auto_publisher_decorator_returns_auto_publisher_instance():
    @m.stream_publisher(conn=_Conn(), 
        source=_auto_src(),
        flush_interval_mins=10,
        output_tables=["t1"],
    )
    def my_pub(t1: list[tableframe.TableFrame | None] | None) -> tuple[tableframe.TableFrame | None]:
        return (None,)

    assert isinstance(my_pub, _info.StreamPublisherInfo)
    assert my_pub.fallback_rollover_mins == 10


def test_auto_publisher_decorator_rejects_rollover_below_one_with_api_10():
    # Field-level Field(ge=1) failure surfaces as API_18 when the
    # AutoPublisherInfo _api.dataclass is constructed (i.e. when the
    # factory's returned decorator is applied to a function).
    with pytest.raises(ApiException) as excinfo:
        @m.stream_publisher(conn=_Conn(), 
            source=_auto_src(),
            flush_interval_mins=0,
            output_tables=["t1"],
        )
        def _f(t1: tableframe.TableFrame | None) -> tuple[tableframe.TableFrame | None]:
            return (t1,)
    assert excinfo.value.api_error is ApiErrorCode.API_18


def test_auto_publisher_decorator_accepts_on_tables_hooks():
    @m.stream_publisher(conn=_Conn(), 
        source=_auto_src(),
        flush_interval_mins=15,
        output_tables=["t1"],
        on_tables=[_OnTab()],
    )
    def my_pub(t1: list[tableframe.TableFrame | None] | None) -> tuple[tableframe.TableFrame | None]:
        return (None,)

    assert my_pub.on_tables is not None
    assert len(my_pub.on_tables) == 1


def test_auto_publisher_decorator_default_none_on_tables():
    @m.stream_publisher(conn=_Conn(), 
        source=_auto_src(),
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
        @m.stream_publisher(conn=_Conn(), 
            source=_auto_src(),
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
        @m.stream_publisher(conn=_Conn(), 
            source=_auto_src(),
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
    @m.subscriber(conn=_Conn(), destination=_dest(), input_tables=["coll/t1"])
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
# Single-output convenience: when exactly one output slot is declared the
# lone TableFrameSpec may be returned bare, without the enclosing 1-tuple.
# ---------------------------------------------------------------------------


def test_publisher_signature_allows_bare_tableframe_return():
    @m.publisher(conn=_Conn(), source=_src(), output_tables=["t1"])
    def my_pub(t1: tableframe.TableFrame | None) -> tableframe.TableFrame | None:
        return t1

    assert isinstance(my_pub, _info.PublisherInfo)


def test_transformer_signature_allows_bare_tableframe_return():
    @m.transformer(input_tables=["in1"], output_tables=["out1"])
    def my_xf(t1: tableframe.TableFrame | None) -> tableframe.TableFrame | None:
        return t1

    assert isinstance(my_xf, _info.TransformerInfo)


def test_subscriber_signature_allows_bare_tableframe_return():
    # `_dest()` has cardinality 1, so a single bare output is valid.
    @m.subscriber(conn=_Conn(), destination=_dest(), input_tables=["coll/t1"])
    def my_sub(t1: tableframe.TableFrame | None) -> tableframe.TableFrame | None:
        return t1

    assert isinstance(my_sub, _info.SubscriberInfo)


def test_stream_publisher_signature_allows_bare_tableframe_return():
    @m.stream_publisher(conn=_Conn(),
        source=_auto_src(),
        flush_interval_mins=10,
        output_tables=["t1"],
    )
    def my_pub(
        t1: list[tableframe.TableFrame | None] | None,
    ) -> tableframe.TableFrame | None:
        return None

    assert isinstance(my_pub, _info.StreamPublisherInfo)


def test_bare_return_allowed_with_trailing_trx_ctx():
    # The single-output bare-return convenience composes with the
    # optional trailing ``ctx: TrxCtx`` parameter.
    @m.transformer(input_tables=["coll/t1"], output_tables=["out"])
    def my_xf(
        t1: tableframe.TableFrame | None,
        ctx: m.TrxCtx,
    ) -> tableframe.TableFrame | None:
        return t1

    assert isinstance(my_xf, _info.TransformerInfo)


def test_bare_return_rejected_when_multiple_outputs():
    # The bare form is only a single-output convenience; with 2 output
    # slots the return must still be a fixed-length tuple.
    src = _src2()  # 2 input slots

    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=src, output_tables=["a", "b"])  # 2 outputs
        def my_pub(
            t1: tableframe.TableFrame | None, t2: tableframe.TableFrame | None,
        ) -> tableframe.TableFrame | None:  # bare, but 2 outputs expected
            return t1
    assert excinfo.value.api_error is ApiErrorCode.API_17
    assert "must be a tuple of 2" in str(excinfo.value)


def test_bare_return_rejects_wrong_slot_type():
    # A non-tuple return with a single output must still be a valid
    # single-mode slot -- a bare list is rejected.
    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=_src(), output_tables=["t1"])
        def my_pub(
            t1: tableframe.TableFrame | None,
        ) -> list[tableframe.TableFrame | None] | None:  # not a single slot
            return None
    assert excinfo.value.api_error is ApiErrorCode.API_17


def test_signature_error_block_mentions_bare_return_for_single_output():
    # On a single-output failure the "Expected signature" block should
    # surface the bare-return alternative so an iterating caller knows
    # it is allowed.
    with pytest.raises(ApiException) as excinfo:
        @m.publisher(conn=_Conn(), source=_src(), output_tables=["t1"])
        def _bad() -> tuple[tableframe.TableFrame | None]:  # zero params, expects 1
            return (None,)
    msg = str(excinfo.value)
    assert "returned bare" in msg
    assert "-> TableFrameSpec" in msg


# ---------------------------------------------------------------------------
# AutoSrc / AutoSrcDef
# ---------------------------------------------------------------------------


def test_auto_src_defx_resolves_via_registry():
    from tabsdatak import _connectors
    defx = _connectors.get_stream_src(_StreamSrc)
    assert isinstance(defx, spi.StreamSrcDef)
    assert defx.src is _StreamSrc
    assert defx.src_type == "watch"
    assert defx.publisher_src is spi.StreamStageSrc


# ===========================================================================
# Secret value references
#
# A ``Secret`` is either hydrated (``Secret("v")``, carries a value) or
# dehydrated (``Secret()``, carries none). ``value()`` returns the value
# of a hydrated secret and raises on a dehydrated one. A secret always
# serializes to the dehydrated marker ``":secret:"`` (its value never
# lands in the serialized form); deserialization accepts that marker as
# well as the hydrated forms ``"secret:<value>"`` and a bare ``"<value>"``.
# ===========================================================================


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithSecret(Src):
    s: Secret


# --- construction ----------------------------------------------------------


def test_secret_hydrated_holds_value():
    s = Secret("hunter2")
    assert s.value() == "hunter2"


def test_secret_dehydrated_raises_on_read():
    s = Secret()
    with pytest.raises(ApiInternalException) as excinfo:
        s.value()
    assert excinfo.value.api_error is ApiErrorCode.API_11


def test_secret_rejects_non_string_value():
    # A hydrated secret must carry a string; None is the dehydrated sentinel.
    assert Secret(None) == Secret()
    with pytest.raises(ApiException) as excinfo:
        Secret(123)
    assert excinfo.value.api_error is ApiErrorCode.API_5


def test_secret_repr_is_redacted():
    # Neither the value nor the hydration state leaks through repr / logs.
    assert "hunter2" not in repr(Secret("hunter2"))
    assert "redacted" in repr(Secret("hunter2"))
    assert repr(Secret()) == repr(Secret("hunter2"))


def test_secret_equality():
    assert Secret("a") == Secret("a")
    assert Secret() == Secret()
    assert Secret("a") != Secret("b")
    assert Secret("a") != Secret()


# --- serialization (string form) -------------------------------------------


def test_secret_always_serializes_dehydrated():
    # Both a hydrated and a dehydrated secret serialize to ``":secret:"``;
    # the value never lands in the serialized form.
    assert Secret("hunter2").to_dict() == ":secret:"
    assert Secret().to_dict() == ":secret:"


def test_secret_deserializes_dehydrated_marker():
    assert Secret.from_dict(":secret:") == Secret()


def test_secret_deserializes_hydrated_forms():
    # Both the prefixed and the bare form hydrate with the value.
    assert Secret.from_dict("secret:etl").value() == "etl"
    assert Secret.from_dict("etl").value() == "etl"


def test_secret_serialize_drops_value_on_round_trip():
    # Serializing is always dehydrating: a hydrated secret round-trips
    # back as a dehydrated one.
    assert Secret.from_dict(Secret("hunter2").to_dict()) == Secret()
    assert Secret.from_json(Secret("hunter2").to_json()) == Secret()


# --- fields ----------------------------------------------------------------


def test_secret_field_accepts_instance_and_bare_string():
    assert _SrcWithSecret(s=Secret("h")).s == Secret("h")
    assert _SrcWithSecret(s="h").s == Secret("h")
    assert _SrcWithSecret(s="h").s.value() == "h"


def test_secret_field_dumps_dehydrated():
    assert _SrcWithSecret(s=Secret("h")).to_dict()["spec"]["s"] == ":secret:"
    assert _SrcWithSecret(s=Secret()).to_dict()["spec"]["s"] == ":secret:"


def test_secret_field_round_trip_drops_value():
    # The field serializes dehydrated, so the value does not survive a
    # serialize/deserialize round-trip.
    s = _SrcWithSecret(s=Secret("hunter2"))
    back = _SrcWithSecret.from_dict(s.to_dict())
    with pytest.raises(ApiInternalException) as excinfo:
        back.s.value()
    assert excinfo.value.api_error is ApiErrorCode.API_11


def test_secret_field_accepts_hydrated_wire_form():
    # A field deserializing the hydrated wire form keeps the value.
    payload = _SrcWithSecret(s=Secret()).to_dict()
    payload["spec"]["s"] = "secret:hunter2"
    assert _SrcWithSecret.from_dict(payload).s.value() == "hunter2"


# ===========================================================================
# SecretValueConstraints -- field-site regex on a Secret's hydrated value.
#
# ``Annotated[Secret, SecretValueConstraints(pattern=...)]`` enforces the
# regex on the secret's *hydrated value*: eagerly at construction (for an
# inline value or a bare string) and again whenever the runtime
# re-hydrates the dehydrated marker back into a value. A dehydrated secret
# carries no value and is left unchecked. The pattern is never serialized.
# ===========================================================================


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithConstrainedSecret(Src):
    s: Annotated[Secret, _api.SecretValueConstraints(pattern=r"\d+")]


@_api.dataclass(ApiErrorCode.API_14)
class _SrcWithUnionSpec(Src):
    # The realistic spec shape: a plain string OR a Secret, both pinned to
    # the same regex.
    v: Union[
        Annotated[str, StringConstraints(pattern=r"\d+")],
        Annotated[Secret, _api.SecretValueConstraints(pattern=r"\d+")],
    ]


def test_secret_constraint_accepts_matching_hydrated_value():
    assert _SrcWithConstrainedSecret(s=Secret("123")).s.value() == "123"
    # A bare string is wrapped into a Secret and validated against the
    # field's pattern.
    assert _SrcWithConstrainedSecret(s="123").s == Secret("123")


def test_secret_constraint_rejects_violating_hydrated_value():
    with pytest.raises(ApiException) as excinfo:
        _SrcWithConstrainedSecret(s=Secret("nope"))
    assert excinfo.value.api_error is ApiErrorCode.API_6


def test_secret_constraint_skips_dehydrated_secret():
    # A dehydrated secret has no value, so the pattern is not applied.
    src = _SrcWithConstrainedSecret(s=Secret())
    assert src.to_dict()["spec"]["s"] == ":secret:"


def test_secret_constraint_enforced_on_rehydration():
    # The dehydrated field, re-hydrated from the wire form, is re-validated.
    payload = _SrcWithConstrainedSecret(s=Secret()).to_dict()
    payload["spec"]["s"] = "secret:123"
    assert _SrcWithConstrainedSecret.from_dict(payload).s.value() == "123"
    payload["spec"]["s"] = "secret:nope"
    with pytest.raises(ApiException) as excinfo:
        _SrcWithConstrainedSecret.from_dict(payload)
    assert excinfo.value.api_error is ApiErrorCode.API_6


def test_union_spec_bad_bare_string_does_not_fall_through_to_secret():
    # Regression guard: a bare string that fails the str member must not
    # silently become a literal Secret. The constrained Secret member
    # rejects it too, so construction fails instead of producing a Secret.
    assert _SrcWithUnionSpec(v="123").v == "123"  # str member
    assert _SrcWithUnionSpec(v=Secret("123")).v == Secret("123")  # secret member
    with pytest.raises(ApiException) as excinfo:
        _SrcWithUnionSpec(v="nope")
    assert excinfo.value.api_error is ApiErrorCode.API_6
