from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation


# ---------------------------------------------------------------------------
# _validation.split_fqn
# ---------------------------------------------------------------------------


def test_split_fqn_no_quote_splits_on_dots():
    assert _validation.split_fqn("a.b.c") == ["a", "b", "c"]


def test_split_fqn_no_quote_single_segment():
    assert _validation.split_fqn("a") == ["a"]


def test_split_fqn_with_quote_keeps_dot_inside_quote():
    assert _validation.split_fqn('"a.b".c', quote='"') == ['"a.b"', "c"]


def test_split_fqn_with_quote_handles_doubled_quote_escape():
    # Doubled quote is the SQL-style escape for a literal quote inside
    # a quoted identifier; the helper preserves both characters.
    assert _validation.split_fqn('"a""b".c', quote='"') == ['"a""b"', "c"]


def test_split_fqn_with_backtick_quote():
    assert _validation.split_fqn("`a.b`.c", quote="`") == ["`a.b`", "c"]


def test_split_fqn_unterminated_quote_raises_value_error():
    with pytest.raises(ValueError, match="unterminated quoted identifier"):
        _validation.split_fqn('"a.b', quote='"')


# ---------------------------------------------------------------------------
# _validation.split_fqn_brackets
# ---------------------------------------------------------------------------


def test_split_fqn_brackets_no_brackets():
    assert _validation.split_fqn_brackets("a.b.c") == ["a", "b", "c"]


def test_split_fqn_brackets_keeps_dot_inside_brackets():
    assert _validation.split_fqn_brackets("[a.b].c") == ["[a.b]", "c"]


def test_split_fqn_brackets_doubled_close_bracket_escape():
    assert _validation.split_fqn_brackets("[a]]b].c") == ["[a]]b]", "c"]


def test_split_fqn_brackets_unterminated_raises_value_error():
    with pytest.raises(ValueError, match="unterminated bracketed identifier"):
        _validation.split_fqn_brackets("[a.b")


# ---------------------------------------------------------------------------
# _validation.check_basic / _validation.validate_conn_cfg
# ---------------------------------------------------------------------------


def test_check_basic_accepts_str():
    _validation.check_basic("ok", path="root")


def test_check_basic_accepts_int():
    _validation.check_basic(42, path="root")


def test_check_basic_accepts_float():
    _validation.check_basic(3.14, path="root")


def test_check_basic_accepts_bool():
    _validation.check_basic(True, path="root")


def test_check_basic_accepts_none():
    _validation.check_basic(None, path="root")


def test_check_basic_accepts_list_of_basics():
    _validation.check_basic([1, "x", None, 2.5, True], path="root")


def test_check_basic_accepts_nested_dict():
    _validation.check_basic({"a": {"b": [1, 2]}, "c": "x"}, path="root")


def test_check_basic_rejects_non_basic_value():
    with pytest.raises(ValueError, match="non-basic value"):
        _validation.check_basic({"obj": object()}, path="root")


def test_check_basic_rejects_non_str_dict_key():
    with pytest.raises(ValueError, match="non-str key"):
        _validation.check_basic({42: "value"}, path="root")  # type: ignore[dict-item]


def test_check_basic_rejects_object_at_root():
    with pytest.raises(ValueError, match="non-basic value"):
        _validation.check_basic(object(), path="")


def test_check_basic_path_is_reported_in_error():
    with pytest.raises(ValueError, match=r"'x\.y'"):
        _validation.check_basic({"x.y": object()}, path="")


def test_validate_conn_cfg_passes_through_none():
    assert _validation.validate_conn_cfg(None) is None


def test_validate_conn_cfg_passes_through_valid_dict():
    cfg = {"timeout": 30, "tags": ["a"]}
    assert _validation.validate_conn_cfg(cfg) is cfg


def test_validate_conn_cfg_rejects_invalid():
    with pytest.raises(ValueError):
        _validation.validate_conn_cfg({"bad": object()})


def test_conn_cfg_validator_constant_is_after_validator():
    # Sanity: the exported constant wraps the function in a pydantic
    # AfterValidator so callers can use ``Annotated[..., _validation.CONN_CFG_VALIDATOR]``.
    assert _validation.CONN_CFG_VALIDATOR.func is _validation.validate_conn_cfg


# ---------------------------------------------------------------------------
# _validation.validate_entity_whitelist
# ---------------------------------------------------------------------------


def test_validate_entity_whitelist_passes_through_none():
    assert _validation.validate_entity_whitelist(None) is None


def test_validate_entity_whitelist_accepts_valid_regexes():
    val = [r"^foo_.*$", r"bar"]
    assert _validation.validate_entity_whitelist(val) is val


def test_validate_entity_whitelist_rejects_invalid_regex():
    with pytest.raises(ValueError, match="invalid regex"):
        _validation.validate_entity_whitelist([r"valid", r"[unclosed"])


def test_validate_entity_whitelist_reports_index_of_bad_regex():
    with pytest.raises(ValueError, match="index 1"):
        _validation.validate_entity_whitelist([r"valid", r"[unclosed"])


def test_entity_whitelist_validator_constant_is_after_validator():
    assert _validation.ENTITY_WHITELIST_VALIDATOR.func is _validation.validate_entity_whitelist
