#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for the annotation rewriter / alias redefinition (Option C, no leaks)."""

from tabsdatax.generator.tableframe.engine.generate.annotations import (
    AnnotationRewriter,
)

OUR = {"Expr", "TableFrame", "When"}
MODULES = {"Expr": "pkg.expr", "TableFrame": "pkg", "When": "pkg.whenthen"}


def _rewriter() -> AnnotationRewriter:
    return AnnotationRewriter(OUR, MODULES, "pkg")


def test_lazyframe_rewritten_to_tableframe():
    signature, our, _ = _rewriter().rewrite_signature(
        "(self, other: 'LazyFrame') -> 'LazyFrame'"
    )
    assert signature == "(self, other: TableFrame) -> TableFrame"
    assert our == {"TableFrame"}


def test_polars_type_dropped_from_union_keeps_ours_and_safe():
    signature, _, _ = _rewriter().rewrite_signature(
        "(self, x: 'Expr | str | Series') -> 'Expr'"
    )
    assert signature == "(self, x: Expr | str) -> Expr"  # Series dropped (Option C)


def test_standalone_foreign_becomes_any():
    signature, _, _ = _rewriter().rewrite_signature(
        "(self, x: 'Series') -> 'DataFrame'"
    )
    assert signature == "(self, x: Any) -> Any"


def test_polars_alias_is_kept_as_name_and_tracked():
    signature, _, aliases = _rewriter().rewrite_signature(
        "(self, x: 'IntoExpr') -> 'Expr'"
    )
    assert "IntoExpr" in signature and "IntoExpr" in aliases


def test_literal_alias_redefined_without_leaks():
    rewriter = _rewriter()
    _, _, aliases = rewriter.rewrite_signature(
        "(self, *, how: 'ConcatMethod') -> 'Expr'"
    )
    typing_pyi = rewriter.render_typing_pyi(aliases)
    assert "ConcatMethod: TypeAlias = Literal['vertical'" in typing_pyi
    assert "polars" not in typing_pyi and "Series" not in typing_pyi


def test_numpy_is_not_exposed():
    signature, _, _ = _rewriter().rewrite_signature(
        "(self, x: 'Expr | np.ndarray') -> 'Expr'"
    )
    assert signature == "(self, x: Expr) -> Expr"  # np.ndarray dropped
