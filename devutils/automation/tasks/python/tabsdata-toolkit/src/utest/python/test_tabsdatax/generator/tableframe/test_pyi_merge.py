#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for the .pyi merge/preserve behavior on regeneration."""

import pytest

from tabsdatax.generator.tableframe.engine.generate import perform
from tabsdatax.generator.tableframe.engine.generate.stubs import canonical_signature
from tabsdatax.generator.tableframe.model.model import Report, Symbol

ROOT = "pkg.wrap"


@pytest.fixture
def root_dir(tmp_path):
    # Mirror the Maven source layout the generator expects (see test_generate).
    return tmp_path / "src" / "main" / "python"


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


def test_first_generation_has_no_markers(root_dir):
    perform(
        _report([_expr("alias", "(self, name: 'str') -> 'Expr'")]), ROOT, str(root_dir)
    )
    text = (root_dir / "pkg" / "wrap" / "expr.pyi").read_text()
    assert "New signature" not in text and "Signature changed" not in text
    # annotations rewritten through the typing engine (unquoted, our types);
    # every entity gets an empty docstring placeholder instead of `...`
    assert "def alias(self, name: str) -> Expr:" in text
    assert '""" """' in text


def test_regeneration_preserves_docstring_and_flags_changes(root_dir):
    base = root_dir / "pkg" / "wrap"
    perform(
        _report([
            _expr("alias", "(self, name: 'str') -> 'Expr'"),
            _expr("keep", "(self) -> 'Expr'"),
        ]),
        ROOT,
        str(root_dir),
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
    perform(
        _report([
            _expr("alias", "(self, name: 'str', *, strict: 'bool') -> 'Expr'"),
            _expr("keep", "(self) -> 'Expr'"),
            _expr("brand", "(self) -> 'Expr'"),
        ]),
        ROOT,
        str(root_dir),
    )
    result_text = pyi.read_text()

    # changed signature -> marker + kept old docstring
    assert "# Signature changed; validate and fix the docstring below." in result_text
    assert '"""Alias the expression."""' in result_text
    # unchanged -> doc preserved verbatim
    assert '"""Unchanged doc."""' in result_text
    # new symbol -> marker
    assert "# New signature; write the docstring below." in result_text
    # the .pyi-ori snapshot is transient: read for docstring recovery, then dropped
    assert not (base / "expr.pyi-ori").exists()


def test_dropped_symbols_reported(root_dir):
    perform(
        _report([_expr("a", "(self) -> 'Expr'"), _expr("b", "(self) -> 'Expr'")]),
        ROOT,
        str(root_dir),
    )
    result = perform(_report([_expr("a", "(self) -> 'Expr'")]), ROOT, str(root_dir))
    assert result.dropped[f"{ROOT}.expr"] == ["b"]
    # b is gone from the regenerated stub
    assert "def b(" not in (root_dir / "pkg" / "wrap" / "expr.pyi").read_text()
