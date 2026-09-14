#
#  Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the shared
:class:`~tabsdatak.conn.common.sql._merge.MergeCfg`.

Covers the generic column validation, the structural invariants (`on`
non-empty, and neither `op_column` nor `watermark_column` doubling as
anything else), the envelope round-trip, and the
`_fold` hook a connector overrides -- including that the inherited
`validate()` compares *folded* names, which is what lets a case-folding
dialect catch a marker colliding with a key under another spelling.
"""

from __future__ import annotations

import pytest

# noinspection PyProtectedMember
import tabsdatak._api as _api
from tabsdatak.conn.common.error import ConnInitException

# noinspection PyProtectedMember
from tabsdatak.conn.common.sql._merge import (
    MergeCfg,
    MergeColumnsSpec,
    MergeColumnSpec,
)
from tabsdatak.conn.common.sql.error import (
    SqlCommonErrorCode,
    SqlCommonValidateException,
)


def _cfg(**overrides) -> MergeCfg:
    kwargs: dict = dict(on=["order_id"], op_column="op")
    kwargs.update(overrides)
    return MergeCfg(**kwargs)


# A stand-in for what a connector ships: an upper-folding dialect.
@_api.dataclass(SqlCommonErrorCode.SQL_COMMON_4, kw_only=True)
class _UpperMergeCfg(MergeCfg):
    __kind__ = "upperMergeCfg"

    on: MergeColumnsSpec
    op_column: MergeColumnSpec

    def _fold(self, name: str) -> str:
        return name.upper()


# --- Fields ----------------------------------------------------------


class TestFields:
    def test_holds_keys_and_marker(self):
        cfg = _cfg(on=["a", "b"], op_column="op")
        assert cfg.on == ["a", "b"]
        assert cfg.op_column == "op"

    def test_is_frozen(self):
        with pytest.raises(Exception):
            _cfg().op_column = "other"

    def test_op_column_is_required(self):
        with pytest.raises(ConnInitException) as ei:
            MergeCfg(on=["a"])
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_4

    def test_on_is_required(self):
        with pytest.raises(ConnInitException) as ei:
            MergeCfg(op_column="op")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_4

    @pytest.mark.parametrize("bad", ["", " ", "\t", "\n"])
    def test_blank_column_is_rejected(self, bad):
        with pytest.raises(ConnInitException) as ei:
            _cfg(on=[bad])
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_4

    def test_blank_op_column_is_rejected(self):
        with pytest.raises(ConnInitException) as ei:
            _cfg(op_column="")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_4

    def test_nul_byte_in_a_column_is_rejected(self):
        with pytest.raises(ConnInitException) as ei:
            _cfg(on=["a\x00b"])
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_4

    def test_nul_byte_in_the_op_column_is_rejected(self):
        with pytest.raises(ConnInitException) as ei:
            _cfg(op_column="o\x00p")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_4

    def test_dialect_shaped_names_are_accepted(self):
        """Identifier shape is the connector's business, so anything a dialect
        might legitimately spell has to survive this class."""
        for name in ['"quoted"', "`backticked`", "[bracketed]", "with space", "a.b"]:
            assert _cfg(on=[name]).on == [name]


# --- Invariants ------------------------------------------------------


class TestValidate:
    def test_empty_on_is_rejected(self):
        with pytest.raises(SqlCommonValidateException) as ei:
            _cfg(on=[])
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_5

    def test_op_column_as_a_key_is_rejected(self):
        with pytest.raises(SqlCommonValidateException) as ei:
            _cfg(on=["order_id", "op"], op_column="op")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_6

    def test_op_column_as_the_only_key_is_rejected(self):
        with pytest.raises(SqlCommonValidateException) as ei:
            _cfg(on=["op"], op_column="op")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_6

    def test_message_names_the_offending_column(self):
        with pytest.raises(SqlCommonValidateException) as ei:
            _cfg(on=["op"], op_column="op")
        assert "'op'" in str(ei.value)

    def test_several_keys_are_allowed(self):
        assert _cfg(on=["a", "b", "c"])._keys() == ["a", "b", "c"]

    def test_watermark_column_is_optional(self):
        assert _cfg().watermark_column is None
        assert _cfg()._watermark_column() is None

    def test_watermark_column_as_a_key_is_rejected(self):
        """A key is what a row is matched by; a watermark is written onto every
        row the merge touches, so one column cannot be both."""
        with pytest.raises(SqlCommonValidateException) as ei:
            _cfg(on=["order_id", "wm"], watermark_column="wm")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_7
        assert "'wm'" in str(ei.value)

    def test_watermark_column_as_the_marker_is_rejected(self):
        """The marker is read and never written, so it can carry no stamp."""
        with pytest.raises(SqlCommonValidateException) as ei:
            _cfg(op_column="op", watermark_column="op")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_8
        assert "'op'" in str(ei.value)

    def test_a_watermark_beside_the_keys_and_the_marker_is_allowed(self):
        assert _cfg(watermark_column="wm")._watermark_column() == "wm"


# --- The _fold hook --------------------------------------------------


class TestFold:
    def test_base_takes_names_verbatim(self):
        cfg = _cfg(on=["order_id", '"Mixed"'], op_column="op")
        assert cfg._keys() == ["order_id", '"Mixed"']
        assert cfg._op_column() == "op"

    def test_case_differing_names_are_distinct_without_folding(self):
        """A case-sensitive dialect has two columns here, so this is not a
        collision -- unlike under a folding subclass."""
        assert _cfg(on=["OP"], op_column="op")._op_column() == "op"

    def test_subclass_fold_applies_to_keys_and_marker(self):
        cfg = _UpperMergeCfg(on=["order_id"], op_column="op")
        assert cfg._keys() == ["ORDER_ID"]
        assert cfg._op_column() == "OP"

    def test_subclass_fold_applies_to_the_watermark(self):
        """The stored form is what the dialect probes the target for, so it has
        to be folded like every other name."""
        cfg = _UpperMergeCfg(on=["order_id"], op_column="op", watermark_column="wm")
        assert cfg._watermark_column() == "WM"

    def test_inherited_validate_compares_folded_names(self):
        """The invariant is checked after folding, so a collision only the
        dialect's folding reveals is still caught."""
        with pytest.raises(SqlCommonValidateException) as ei:
            _UpperMergeCfg(on=["OP"], op_column="op")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_6

    def test_subclass_still_enforces_a_non_empty_on(self):
        with pytest.raises(SqlCommonValidateException) as ei:
            _UpperMergeCfg(on=[], op_column="op")
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_5

    def test_subclass_is_a_merge_cfg(self):
        assert isinstance(_UpperMergeCfg(on=["a"], op_column="op"), MergeCfg)


# --- Wire ------------------------------------------------------------


class TestSerde:
    def test_round_trip(self):
        cfg = _cfg(on=["a", "b"], op_column="op")
        assert MergeCfg.from_json(cfg.to_json()) == cfg

    def test_envelope_carries_the_kind_and_type(self):
        envelope = _cfg().to_dict()
        assert envelope["kind"] == "sqlMergeCfg"
        assert envelope["type"] == "tabsdatak.conn.common.sql._merge:MergeCfg"
        assert envelope["spec"] == {
            "on": ["order_id"],
            "op_column": "op",
            "watermark_column": None,
        }

    def test_a_watermark_round_trips(self):
        cfg = _cfg(watermark_column="wm")
        assert MergeCfg.from_json(cfg.to_json()) == cfg

    def test_subclass_round_trips_back_to_the_subclass(self):
        cfg = _UpperMergeCfg(on=["order_id"], op_column="op")
        loaded = MergeCfg.from_json(cfg.to_json())
        assert type(loaded) is _UpperMergeCfg
        assert loaded._keys() == ["ORDER_ID"]

    def test_subclass_envelope_carries_its_own_kind(self):
        assert _UpperMergeCfg(on=["a"], op_column="op").to_dict()["kind"] == (
            "upperMergeCfg"
        )

    def test_an_invalid_spec_surfaces_the_error_code(self):
        """Deserializing a bad descriptor reports the same code construction
        does, rather than leaking a raw pydantic error."""
        envelope = _cfg().to_dict()
        envelope["spec"]["op_column"] = ""
        with pytest.raises(ConnInitException) as ei:
            MergeCfg.from_dict(envelope)
        assert ei.value.api_error is SqlCommonErrorCode.SQL_COMMON_4
