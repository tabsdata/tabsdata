from __future__ import annotations

import pytest

# noinspection PyProtectedMember
from tabsdatak.conn.common import _validation
from tabsdatak.conn.common.error import ConnErrorCode, ConnInitException
# noinspection PyProtectedMember
from tabsdatak.conn.common._validation import FqnDef, fqn, unfqn


# ---------------------------------------------------------------------------
# _validation.FqnDef -- construction-time invariants
# ---------------------------------------------------------------------------


def test_fqn_def_rejects_empty_separator():
    with pytest.raises(ConnInitException, match="separator must be non-empty") as ei:
        FqnDef(separator="")
    assert ei.value.api_error is ConnErrorCode.CONN_9


def test_fqn_def_rejects_empty_quote():
    with pytest.raises(ConnInitException, match="quote, when set, must be non-empty"):
        FqnDef(separator=".", quote="")


def test_fqn_def_rejects_escape_without_quote():
    with pytest.raises(ConnInitException, match=r"escape requires"):
        FqnDef(separator=".", escape="\\")


def test_fqn_def_rejects_outer_only_without_quote():
    with pytest.raises(ConnInitException, match=r"outer_only requires"):
        FqnDef(separator=".", outer_only=True)


def test_fqn_def_is_frozen():
    spec = FqnDef(separator=".", quote="`")
    with pytest.raises(Exception):
        spec.separator = ","  # type: ignore[misc]


def test_fqn_def_accepts_asymmetric_brackets():
    spec = FqnDef(separator=".", quote="[", close_quote="]")
    assert spec.quote == "["
    assert spec.close_quote == "]"


def test_fqn_def_accepts_asymmetric_brackets_with_doubled_close_escape():
    spec = FqnDef(separator=".", quote="[", close_quote="]", escape="]")
    assert spec.quote == "["
    assert spec.close_quote == "]"
    assert spec.escape == "]"


def test_fqn_def_rejects_close_quote_without_quote():
    with pytest.raises(ConnInitException, match=r"close_quote requires"):
        FqnDef(separator=".", close_quote="]")


def test_fqn_def_rejects_empty_close_quote():
    with pytest.raises(ConnInitException, match="quote, when set, must be non-empty"):
        FqnDef(separator=".", quote="[", close_quote="")


# ---------------------------------------------------------------------------
# _validation.fqn -- per-part quoting
# ---------------------------------------------------------------------------


def test_fqn_no_quote_joins_bare():
    assert fqn(["a", "b", "c"], FqnDef(separator=".")) == "a.b.c"


def test_fqn_no_quote_rejects_separator_in_element():
    with pytest.raises(ValueError, match="contains separator"):
        fqn(["a.x", "b"], FqnDef(separator="."))


def test_fqn_per_part_quote_always_wraps_each_element():
    # Predictable output: each element is wrapped, so the result is
    # always a valid identifier even when individual parts are bare-
    # safe (this keeps fqn(unfqn(x)) reasoning straightforward).
    spec = FqnDef(separator=".", quote="`")
    assert fqn(["a", "b", "c"], spec) == "`a`.`b`.`c`"


def test_fqn_per_part_quote_allows_separator_inside_element():
    spec = FqnDef(separator=".", quote="`")
    assert fqn(["a.b", "c"], spec) == "`a.b`.`c`"


def test_fqn_per_part_quote_with_doubled_quote_escape():
    spec = FqnDef(separator=".", quote='"', escape='"')
    assert fqn(['a"b', "c"], spec) == '"a""b"."c"'


def test_fqn_per_part_quote_with_backslash_escape():
    spec = FqnDef(separator=".", quote='"', escape="\\")
    assert fqn(['a"b', "c"], spec) == '"a\\"b"."c"'


def test_fqn_per_part_quote_rejects_quote_in_element_without_escape():
    spec = FqnDef(separator=".", quote="`")
    with pytest.raises(ValueError, match="contains quote"):
        fqn(["a`b", "c"], spec)


def test_fqn_per_part_quote_passes_through_already_quoted_element():
    spec = FqnDef(separator=".", quote="`")
    assert fqn(["`a-1`", "b", "`c`"], spec) == "`a-1`.`b`.`c`"


def test_fqn_per_part_quote_no_op_skips_escape_logic():
    # Element body contains the quote char, but since the element is
    # already quoted (caller's assertion) we don't try to escape it.
    spec = FqnDef(separator=".", quote="`")
    assert fqn(["`a``b`", "c"], spec) == "`a``b`.`c`"


def test_fqn_per_part_quote_partial_wrap_is_not_treated_as_quoted():
    # Starts with ``q`` but doesn't end with it -- not a no-op, falls
    # through to the normal quote/escape path, which rejects because
    # the inner ``q`` has no escape defined.
    spec = FqnDef(separator=".", quote="`")
    with pytest.raises(ValueError, match="contains quote"):
        fqn(["`a", "b"], spec)


def test_fqn_outer_only_wraps_whole_string_once():
    spec = FqnDef(separator=".", quote="`", outer_only=True)
    assert fqn(["proj", "ds", "tbl"], spec) == "`proj.ds.tbl`"


def test_fqn_outer_only_rejects_separator_safe_quote_in_element():
    spec = FqnDef(separator=".", quote="`", outer_only=True)
    with pytest.raises(ValueError, match="contains separator"):
        fqn(["a.b", "c"], spec)


def test_fqn_outer_only_with_escape_unescapes_inner_quote():
    spec = FqnDef(separator=".", quote='"', escape='"', outer_only=True)
    assert fqn(['weird"name', "x"], spec) == '"weird""name.x"'


def test_fqn_outer_only_rejects_quote_in_element_without_escape():
    spec = FqnDef(separator=".", quote="`", outer_only=True)
    with pytest.raises(ValueError, match="quote .* no escape defined"):
        fqn(["a`b", "c"], spec)


# ---------------------------------------------------------------------------
# _validation.fqn -- asymmetric (open / close) quoting
# ---------------------------------------------------------------------------


def test_fqn_asymmetric_brackets_per_part():
    spec = FqnDef(separator=".", quote="[", close_quote="]")
    assert fqn(["a", "b", "c"], spec) == "[a].[b].[c]"


def test_fqn_asymmetric_brackets_with_doubled_close_escape():
    spec = FqnDef(separator=".", quote="[", close_quote="]", escape="]")
    assert fqn(["a]b", "c"], spec) == "[a]]b].[c]"


def test_fqn_asymmetric_brackets_open_inside_part_is_literal():
    # ``[`` is the open delimiter only; literal ``[`` inside the body
    # passes through untouched -- the parser stops on ``]``.
    spec = FqnDef(separator=".", quote="[", close_quote="]", escape="]")
    assert fqn(["a[b", "c"], spec) == "[a[b].[c]"


def test_fqn_asymmetric_brackets_passthrough_already_wrapped():
    spec = FqnDef(separator=".", quote="[", close_quote="]", escape="]")
    assert fqn(["[a]", "[b]"], spec) == "[a].[b]"


def test_fqn_asymmetric_brackets_rejects_close_without_escape():
    spec = FqnDef(separator=".", quote="[", close_quote="]")
    with pytest.raises(ValueError, match=r"contains quote"):
        fqn(["a]b", "c"], spec)


def test_fqn_asymmetric_brackets_outer_only():
    spec = FqnDef(
        separator=".",
        quote="[",
        close_quote="]",
        escape="]",
        outer_only=True,
    )
    assert fqn(["proj", "ds", "tbl"], spec) == "[proj.ds.tbl]"


def test_fqn_rejects_empty_parts_list():
    with pytest.raises(ValueError, match="parts must be non-empty"):
        fqn([], FqnDef(separator="."))


def test_fqn_rejects_empty_element():
    with pytest.raises(ValueError, match="index 1 is empty"):
        fqn(["a", ""], FqnDef(separator=".", quote="`"))


# ---------------------------------------------------------------------------
# _validation.fqn_complete -- fill leading None slots with defaults
# ---------------------------------------------------------------------------


def test_fqn_complete_fills_single_leading_none():
    from tabsdatak.conn.common._validation import fqn_complete
    assert fqn_complete([None, "schema", "users"], 3, "main") == [
        "main", "schema", "users",
    ]


def test_fqn_complete_fills_multiple_leading_nones():
    from tabsdatak.conn.common._validation import fqn_complete
    assert fqn_complete([None, None, "users"], 3, "main", "default") == [
        "main", "default", "users",
    ]


def test_fqn_complete_left_pads_short_parts_then_fills():
    # ``parts`` of length 1 against total_parts=3 -> normalized to
    # ``[None, None, "users"]``, then two defaults fill the leading
    # Nones in order.
    from tabsdatak.conn.common._validation import fqn_complete
    assert fqn_complete(["users"], 3, "main", "default") == [
        "main", "default", "users",
    ]


def test_fqn_complete_no_op_when_already_fully_qualified():
    from tabsdatak.conn.common._validation import fqn_complete
    assert fqn_complete(["a", "b", "c"], 3) == ["a", "b", "c"]


def test_fqn_complete_round_trip_with_unfqn_and_fqn():
    from tabsdatak.conn.common._validation import fqn_complete
    spec = FqnDef(separator=".", quote="`")
    parsed = unfqn("schema.users", spec, total_parts=3)
    full = fqn_complete(parsed, 3, "main")
    assert fqn(full, spec) == "`main`.`schema`.`users`"


def test_fqn_complete_rejects_zero_total_parts():
    from tabsdatak.conn.common._validation import fqn_complete
    with pytest.raises(ValueError, match="total_parts must be a positive integer"):
        fqn_complete(["a"], 0)


def test_fqn_complete_rejects_negative_total_parts():
    from tabsdatak.conn.common._validation import fqn_complete
    with pytest.raises(ValueError, match="total_parts must be a positive integer"):
        fqn_complete(["a"], -1)


def test_fqn_complete_rejects_parts_longer_than_total_parts():
    from tabsdatak.conn.common._validation import fqn_complete
    with pytest.raises(ValueError, match="exceeds total_parts=2"):
        fqn_complete(["a", "b", "c"], 2)


def test_fqn_complete_rejects_none_after_leading_run():
    from tabsdatak.conn.common._validation import fqn_complete
    with pytest.raises(ValueError, match="None at position 1 after the leading run"):
        fqn_complete(["a", None, "c"], 3)


def test_fqn_complete_rejects_too_few_defaults():
    from tabsdatak.conn.common._validation import fqn_complete
    with pytest.raises(ValueError, match="expected 2 default value"):
        fqn_complete([None, None, "users"], 3, "main")


def test_fqn_complete_rejects_too_many_defaults():
    from tabsdatak.conn.common._validation import fqn_complete
    with pytest.raises(ValueError, match="expected 1 default value"):
        fqn_complete([None, "schema", "users"], 3, "main", "extra")


def test_fqn_complete_rejects_empty_default():
    from tabsdatak.conn.common._validation import fqn_complete
    with pytest.raises(ValueError, match="default at index 0 is empty"):
        fqn_complete([None, "schema", "users"], 3, "")


def test_fqn_complete_returns_new_list_not_aliased():
    from tabsdatak.conn.common._validation import fqn_complete
    parts: list[str | None] = ["a", "b", "c"]
    out = fqn_complete(parts, 3)
    out.append("mutated")
    assert parts == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# _validation.unfqn -- autodetect quoted vs bare
# ---------------------------------------------------------------------------


def test_unfqn_no_quote_splits_bare():
    assert unfqn("a.b.c", FqnDef(separator=".")) == ["a", "b", "c"]


def test_unfqn_per_part_quote_handles_all_quoted():
    assert unfqn("`a`.`b`.`c`", FqnDef(separator=".", quote="`")) == ["a", "b", "c"]


def test_unfqn_per_part_quote_handles_all_bare():
    assert unfqn("a.b.c", FqnDef(separator=".", quote="`")) == ["a", "b", "c"]


def test_unfqn_per_part_quote_handles_mixed():
    assert unfqn("`a-1`.b.`c`", FqnDef(separator=".", quote="`")) == ["a-1", "b", "c"]


def test_unfqn_per_part_quote_keeps_separator_inside_quoted():
    assert unfqn("`a.b`.c", FqnDef(separator=".", quote="`")) == ["a.b", "c"]


def test_unfqn_per_part_doubled_quote_escape_unescapes():
    assert unfqn('"a""b"."c"', FqnDef(separator=".", quote='"', escape='"')) == ['a"b', "c"]


def test_unfqn_per_part_backslash_escape_unescapes():
    assert unfqn('"a\\"b"."c"', FqnDef(separator=".", quote='"', escape="\\")) == ['a"b', "c"]


def test_unfqn_rejects_unterminated_quoted_identifier():
    with pytest.raises(ValueError, match="unterminated quoted identifier"):
        unfqn('"a.b', FqnDef(separator=".", quote='"'))


def test_unfqn_rejects_stray_quote_in_bare_element():
    with pytest.raises(ValueError, match="unexpected quote"):
        unfqn('a"b.c', FqnDef(separator=".", quote='"'))


def test_unfqn_rejects_trailing_separator():
    with pytest.raises(ValueError, match="trailing separator"):
        unfqn("a.b.", FqnDef(separator=".", quote="`"))


def test_unfqn_rejects_leading_separator():
    with pytest.raises(ValueError, match="empty element at position 0"):
        unfqn(".a.b", FqnDef(separator=".", quote="`"))


def test_unfqn_rejects_double_separator():
    with pytest.raises(ValueError, match="empty element"):
        unfqn("a..b", FqnDef(separator=".", quote="`"))


def test_unfqn_rejects_missing_separator_between_parts():
    # ``\`a\`\`b\``: two adjacent quoted parts with no separator.
    with pytest.raises(ValueError, match="expected separator"):
        unfqn("`a``b`", FqnDef(separator=".", quote="`"))


def test_unfqn_rejects_empty_value():
    with pytest.raises(ValueError, match="value must be non-empty"):
        unfqn("", FqnDef(separator="."))


def test_unfqn_outer_only_strips_outer_quotes_and_splits():
    spec = FqnDef(separator=".", quote="`", outer_only=True)
    assert unfqn("`proj.ds.tbl`", spec) == ["proj", "ds", "tbl"]


def test_unfqn_outer_only_unescapes_inner_quote():
    spec = FqnDef(separator=".", quote='"', escape='"', outer_only=True)
    assert unfqn('"weird""name.x"', spec) == ['weird"name', "x"]


def test_unfqn_outer_only_rejects_missing_outer_quotes():
    spec = FqnDef(separator=".", quote="`", outer_only=True)
    with pytest.raises(ValueError, match="must be wrapped"):
        unfqn("proj.ds.tbl", spec)


def test_unfqn_outer_only_rejects_too_short():
    spec = FqnDef(separator=".", quote="`", outer_only=True)
    with pytest.raises(ValueError, match="must be wrapped"):
        unfqn("`", spec)


# ---------------------------------------------------------------------------
# _validation.unfqn -- asymmetric (open / close) quoting
# ---------------------------------------------------------------------------


def test_unfqn_asymmetric_brackets_all_quoted():
    spec = FqnDef(separator=".", quote="[", close_quote="]")
    assert unfqn("[a].[b].[c]", spec) == ["a", "b", "c"]


def test_unfqn_asymmetric_brackets_mixed_bare_and_quoted():
    spec = FqnDef(separator=".", quote="[", close_quote="]")
    assert unfqn("[a-1].b.[c]", spec) == ["a-1", "b", "c"]


def test_unfqn_asymmetric_brackets_separator_inside_quoted():
    spec = FqnDef(separator=".", quote="[", close_quote="]")
    assert unfqn("[a.b].c", spec) == ["a.b", "c"]


def test_unfqn_asymmetric_brackets_doubled_close_escape_unescapes():
    spec = FqnDef(separator=".", quote="[", close_quote="]", escape="]")
    assert unfqn("[a]]b].[c]", spec) == ["a]b", "c"]


def test_unfqn_asymmetric_brackets_open_inside_quoted_is_literal():
    spec = FqnDef(separator=".", quote="[", close_quote="]", escape="]")
    assert unfqn("[a[b].[c]", spec) == ["a[b", "c"]


def test_unfqn_asymmetric_brackets_rejects_unterminated():
    spec = FqnDef(separator=".", quote="[", close_quote="]")
    with pytest.raises(ValueError, match="unterminated quoted identifier"):
        unfqn("[malformed", spec)


def test_unfqn_asymmetric_brackets_outer_only():
    spec = FqnDef(
        separator=".",
        quote="[",
        close_quote="]",
        escape="]",
        outer_only=True,
    )
    assert unfqn("[proj.ds.tbl]", spec) == ["proj", "ds", "tbl"]


def test_unfqn_asymmetric_brackets_outer_only_rejects_missing_close():
    spec = FqnDef(
        separator=".",
        quote="[",
        close_quote="]",
        outer_only=True,
    )
    with pytest.raises(ValueError, match="must be wrapped"):
        unfqn("[proj.ds.tbl", spec)


# ---------------------------------------------------------------------------
# _validation.unfqn -- total_parts left-padding
# ---------------------------------------------------------------------------


def test_unfqn_total_parts_pads_with_none_on_the_front():
    spec = FqnDef(separator=".", quote="`")
    assert unfqn("users", spec, total_parts=3) == [None, None, "users"]


def test_unfqn_total_parts_pads_partial_qualified_name():
    spec = FqnDef(separator=".", quote="`")
    assert unfqn("schema.users", spec, total_parts=3) == [None, "schema", "users"]


def test_unfqn_total_parts_no_padding_when_exact_match():
    spec = FqnDef(separator=".", quote="`")
    assert unfqn("catalog.schema.users", spec, total_parts=3) == [
        "catalog",
        "schema",
        "users",
    ]


def test_unfqn_total_parts_default_returns_unpadded():
    spec = FqnDef(separator=".", quote="`")
    assert unfqn("schema.users", spec) == ["schema", "users"]


def test_unfqn_total_parts_works_with_bare_no_quote_spec():
    spec = FqnDef(separator=".")
    assert unfqn("users", spec, total_parts=2) == [None, "users"]


def test_unfqn_total_parts_works_with_outer_only_spec():
    spec = FqnDef(separator=".", quote="`", outer_only=True)
    assert unfqn("`schema.users`", spec, total_parts=3) == [None, "schema", "users"]


def test_unfqn_total_parts_rejects_too_many_parts():
    spec = FqnDef(separator=".", quote="`")
    with pytest.raises(ValueError, match="exceeds total_parts=2"):
        unfqn("a.b.c", spec, total_parts=2)


def test_unfqn_total_parts_rejects_zero():
    spec = FqnDef(separator=".", quote="`")
    with pytest.raises(ValueError, match="total_parts must be a positive integer"):
        unfqn("a", spec, total_parts=0)


def test_unfqn_total_parts_rejects_negative():
    spec = FqnDef(separator=".", quote="`")
    with pytest.raises(ValueError, match="total_parts must be a positive integer"):
        unfqn("a", spec, total_parts=-1)


# ---------------------------------------------------------------------------
# fqn / unfqn round-trip
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec,parts",
    [
        (FqnDef(separator="."), ["a", "b", "c"]),
        (FqnDef(separator=".", quote="`"), ["catalog", "schema", "table"]),
        (FqnDef(separator=".", quote="`"), ["databricks-ci-catalog", "default", "users"]),
        (FqnDef(separator=".", quote='"', escape='"'), ['weird"name', "x"]),
        (FqnDef(separator=".", quote="`", outer_only=True), ["proj", "ds", "tbl"]),
        (FqnDef(separator=":", quote="'", escape="\\"), ["a:b", "c'd", "e"]),
        (
            FqnDef(separator=".", quote="[", close_quote="]", escape="]"),
            ["db", "schema", "table"],
        ),
        (
            FqnDef(separator=".", quote="[", close_quote="]", escape="]"),
            ["my db", "my schema", "my table"],
        ),
        (
            FqnDef(separator=".", quote="[", close_quote="]", escape="]"),
            ["a]b", "c"],
        ),
        (
            FqnDef(separator=".", quote="[", close_quote="]", escape="]", outer_only=True),
            ["proj", "ds", "tbl"],
        ),
    ],
)
def test_fqn_unfqn_round_trip(spec, parts):
    assert unfqn(fqn(parts, spec), spec) == parts


# ---------------------------------------------------------------------------
# _validation.name_validate -- pydantic AfterValidator builder
# ---------------------------------------------------------------------------


def test_name_validate_returns_after_validator_instance():
    from pydantic import AfterValidator
    from tabsdatak.conn.common._validation import name_validate
    v = name_validate(FqnDef(separator=".", quote="`"), max_parts=3)
    assert isinstance(v, AfterValidator)


def test_name_validate_returned_validator_passes_valid_name():
    from tabsdatak.conn.common._validation import name_validate
    v = name_validate(FqnDef(separator=".", quote="`"), max_parts=3)
    assert v.func("catalog.schema.users") == "catalog.schema.users"


def test_name_validate_returned_validator_passes_quoted_name():
    from tabsdatak.conn.common._validation import name_validate
    v = name_validate(FqnDef(separator=".", quote="`"), max_parts=3)
    assert v.func("`databricks-ci-catalog`.schema.users") == (
        "`databricks-ci-catalog`.schema.users"
    )


def test_name_validate_rejects_too_many_parts():
    from tabsdatak.conn.common._validation import name_validate
    v = name_validate(FqnDef(separator=".", quote="`"), max_parts=3)
    with pytest.raises(ValueError, match="exceeds max_parts=3"):
        v.func("a.b.c.d")


def test_name_validate_rejects_invalid_syntax():
    from tabsdatak.conn.common._validation import name_validate
    v = name_validate(FqnDef(separator=".", quote="`"), max_parts=3)
    with pytest.raises(ValueError, match="unterminated quoted identifier"):
        v.func("`unterminated")


def test_name_validate_rejects_empty_value():
    from tabsdatak.conn.common._validation import name_validate
    v = name_validate(FqnDef(separator=".", quote="`"), max_parts=3)
    with pytest.raises(ValueError, match="value must be non-empty"):
        v.func("")


def test_name_validate_max_parts_1_allows_single_part():
    from tabsdatak.conn.common._validation import name_validate
    v = name_validate(FqnDef(separator=".", quote="`"), max_parts=1)
    assert v.func("users") == "users"
    with pytest.raises(ValueError, match="exceeds max_parts=1"):
        v.func("schema.users")


def test_name_validate_rejects_zero_max_parts():
    from tabsdatak.conn.common._validation import name_validate
    with pytest.raises(ValueError, match="max_parts must be a positive integer"):
        name_validate(FqnDef(separator=".", quote="`"), max_parts=0)


def test_name_validate_rejects_negative_max_parts():
    from tabsdatak.conn.common._validation import name_validate
    with pytest.raises(ValueError, match="max_parts must be a positive integer"):
        name_validate(FqnDef(separator=".", quote="`"), max_parts=-1)


def test_name_validate_used_in_annotated_type_via_pydantic():
    from typing import Annotated
    from pydantic import TypeAdapter
    from tabsdatak.conn.common._validation import name_validate

    NameSpec = Annotated[str, name_validate(FqnDef(separator=".", quote="`"), max_parts=3)]
    ta = TypeAdapter(NameSpec)

    assert ta.validate_python("catalog.schema.users") == "catalog.schema.users"

    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        ta.validate_python("a.b.c.d")
    with pytest.raises(ValidationError):
        ta.validate_python("`unterminated")


def test_name_validate_works_with_outer_only_spec():
    from tabsdatak.conn.common._validation import name_validate
    v = name_validate(FqnDef(separator=".", quote="`", outer_only=True), max_parts=3)
    assert v.func("`proj.ds.tbl`") == "`proj.ds.tbl`"
    with pytest.raises(ValueError, match="exceeds max_parts=3"):
        v.func("`a.b.c.d`")


def test_name_validate_each_call_returns_fresh_validator():
    from tabsdatak.conn.common._validation import name_validate
    v1 = name_validate(FqnDef(separator=".", quote="`"), max_parts=3)
    v2 = name_validate(FqnDef(separator=".", quote="`"), max_parts=3)
    assert v1 is not v2


def test_name_validate_works_with_asymmetric_bracket_spec():
    from tabsdatak.conn.common._validation import name_validate
    spec = FqnDef(separator=".", quote="[", close_quote="]", escape="]")
    v = name_validate(spec, max_parts=3)
    assert v.func("[db].[schema].[table]") == "[db].[schema].[table]"
    assert v.func("db.schema.table") == "db.schema.table"
    with pytest.raises(ValueError, match="exceeds max_parts=3"):
        v.func("a.b.c.d")
    with pytest.raises(ValueError, match="unterminated quoted identifier"):
        v.func("[unterminated")


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
