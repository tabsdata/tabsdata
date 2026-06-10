"""Tests for the annotation rewriter / alias redefinition (Option C, no leaks)."""

from tabsdatax.generator.tableframe.emit.typing_gen import TypingGen

OUR = {"Expr", "TableFrame", "When"}
MODULES = {"Expr": "pkg.expr", "TableFrame": "pkg", "When": "pkg.whenthen"}


def _gen() -> TypingGen:
    return TypingGen(OUR, MODULES, "pkg")


def test_lazyframe_rewritten_to_tableframe():
    sig, our, _ = _gen().rewrite_signature("(self, other: 'LazyFrame') -> 'LazyFrame'")
    assert sig == "(self, other: TableFrame) -> TableFrame"
    assert our == {"TableFrame"}


def test_polars_type_dropped_from_union_keeps_ours_and_safe():
    sig, _, _ = _gen().rewrite_signature("(self, x: 'Expr | str | Series') -> 'Expr'")
    assert sig == "(self, x: Expr | str) -> Expr"  # Series dropped (Option C)


def test_standalone_foreign_becomes_any():
    sig, _, _ = _gen().rewrite_signature("(self, x: 'Series') -> 'DataFrame'")
    assert sig == "(self, x: Any) -> Any"


def test_polars_alias_is_kept_as_name_and_tracked():
    sig, _, aliases = _gen().rewrite_signature("(self, x: 'IntoExpr') -> 'Expr'")
    assert "IntoExpr" in sig and "IntoExpr" in aliases


def test_literal_alias_redefined_without_leaks():
    gen = _gen()
    _, _, aliases = gen.rewrite_signature("(self, *, how: 'ConcatMethod') -> 'Expr'")
    typing_pyi = gen.render_typing_pyi(aliases)
    assert "ConcatMethod: TypeAlias = Literal['vertical'" in typing_pyi
    assert "polars" not in typing_pyi and "Series" not in typing_pyi


def test_numpy_is_not_exposed():
    sig, _, _ = _gen().rewrite_signature("(self, x: 'Expr | np.ndarray') -> 'Expr'")
    assert sig == "(self, x: Expr) -> Expr"  # np.ndarray dropped
