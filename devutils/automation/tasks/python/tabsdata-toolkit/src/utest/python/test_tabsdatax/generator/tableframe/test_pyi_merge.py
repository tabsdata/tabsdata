"""Tests for the .pyi merge/preserve behavior on regeneration."""

from tabsdatax.generator.tableframe.emit.generate import generate
from tabsdatax.generator.tableframe.emit.pyi import canonical_signature
from tabsdatax.generator.tableframe.model.model import Report, Symbol

ROOT = "pkg.wrap"


def _report(symbols):
    return Report(polars_version="1.41.0", root_module=ROOT, symbols=symbols)


def _expr(name, sig):
    return Symbol(
        f"Expr.{name}",
        f"polars.expr.expr.Expr.{name}",
        f"{ROOT}.expr",
        "Expr",
        sig,
        "Expr",
        False,
    )


def test_canonical_signature_ignores_formatting():
    assert canonical_signature("(self,   name:'str')->'Expr'") == canonical_signature(
        "(self, name: 'str') -> 'Expr'"
    )


def test_first_generation_has_no_markers(tmp_path):
    generate(
        _report([_expr("alias", "(self, name: 'str') -> 'Expr'")]), ROOT, str(tmp_path)
    )
    text = (tmp_path / "pkg" / "wrap" / "expr.pyi").read_text()
    assert "NEW SIGNATURE" not in text and "CHANGED SIGNATURE" not in text
    # annotations rewritten through the typing engine (unquoted, our types);
    # every entity gets an empty docstring placeholder instead of `...`
    assert "def alias(self, name: str) -> Expr:" in text
    assert '""" """' in text


def test_regeneration_preserves_docstring_and_flags_changes(tmp_path):
    base = tmp_path / "pkg" / "wrap"
    generate(
        _report([
            _expr("alias", "(self, name: 'str') -> 'Expr'"),
            _expr("keep", "(self) -> 'Expr'"),
        ]),
        ROOT,
        str(tmp_path),
    )

    # Human fills the empty docstring placeholders.
    pyi = base / "expr.pyi"
    edited = (
        pyi
        .read_text()
        .replace(
            '    def alias(self, name: str) -> Expr:\n        """ """',
            "    def alias(self, name: str) -> Expr:\n"
            '        """Alias the expression."""',
        )
        .replace(
            '    def keep(self) -> Expr:\n        """ """',
            '    def keep(self) -> Expr:\n        """Unchanged doc."""',
        )
    )
    assert edited != pyi.read_text()
    pyi.write_text(edited)

    # Regenerate: alias signature changed, keep unchanged, brand is new,
    # keep removed-check.
    generate(
        _report([
            _expr("alias", "(self, name: 'str', *, strict: 'bool') -> 'Expr'"),
            _expr("keep", "(self) -> 'Expr'"),
            _expr("brand", "(self) -> 'Expr'"),
        ]),
        ROOT,
        str(tmp_path),
    )
    result_text = pyi.read_text()

    # changed signature -> marker (ruff normalizes "# ") + kept old pydoc
    assert "# CHANGED SIGNATURE, VALIDATE/FIX PYDOC" in result_text
    assert '"""Alias the expression."""' in result_text
    # unchanged -> doc preserved verbatim
    assert '"""Unchanged doc."""' in result_text
    # new symbol -> marker
    assert "# NEW SIGNATURE, CREATE PYDOC" in result_text
    # .pyi-ori snapshot was created
    assert (base / "expr.pyi-ori").exists()


def test_dropped_symbols_reported(tmp_path):
    generate(
        _report([_expr("a", "(self) -> 'Expr'"), _expr("b", "(self) -> 'Expr'")]),
        ROOT,
        str(tmp_path),
    )
    result = generate(_report([_expr("a", "(self) -> 'Expr'")]), ROOT, str(tmp_path))
    assert result.dropped[f"{ROOT}.expr"] == ["b"]
    # b is gone from the regenerated stub
    assert "def b(" not in (tmp_path / "pkg" / "wrap" / "expr.pyi").read_text()
