"""Tests for report comparison."""

from tabsdatax.generator.tableframe.model.compare import compute_diff
from tabsdatax.generator.tableframe.model.model import Report, Symbol


def _sym(sid, sig="(self)", returns="Expr", whitelisted=True, hook_name="-"):
    return Symbol(
        id=sid,
        polars_qualname=f"polars.{sid}",
        target_module="m",
        target_class="Expr",
        signature=sig,
        returns=returns,
        is_property=False,
        whitelisted=whitelisted,
        hook_name=hook_name,
    )


def _report(version, symbols):
    return Report(polars_version=version, root_module="m", symbols=symbols)


def test_identical_reports_have_no_diff():
    r = _report("1.41.0", [_sym("Expr.a")])
    assert compute_diff(r, r).empty


def test_version_difference_is_not_empty():
    diff = compute_diff(_report("1.41.0", []), _report("1.42.0", []))
    assert not diff.empty
    assert diff.old_polars_version == "1.41.0"
    assert diff.new_polars_version == "1.42.0"


def test_added_and_removed_symbols():
    old = _report("1.41.0", [_sym("Expr.a"), _sym("Expr.b")])
    new = _report("1.41.0", [_sym("Expr.b"), _sym("Expr.c")])
    diff = compute_diff(old, new)
    assert diff.added == ["Expr.c"]
    assert diff.removed == ["Expr.a"]


def test_signature_whitelist_and_hook_changes():
    old = _report("1.41.0", [_sym("Expr.a")])
    new = _report(
        "1.41.0",
        [_sym("Expr.a", sig="(self, x)", whitelisted=False, hook_name="join")],
    )
    diff = compute_diff(old, new)
    assert [c.id for c in diff.signature_changed] == ["Expr.a"]
    assert [c.id for c in diff.whitelist_changed] == ["Expr.a"]
    assert diff.hook_changed[0].old == "-"
    assert diff.hook_changed[0].new == "join"


def test_diff_json_roundtrips_structurally():
    diff = compute_diff(_report("1.41.0", [_sym("Expr.a")]), _report("1.42.0", []))
    import json

    data = json.loads(diff.to_json())
    assert data["removed"] == ["Expr.a"]
    assert data["new_polars_version"] == "1.42.0"
