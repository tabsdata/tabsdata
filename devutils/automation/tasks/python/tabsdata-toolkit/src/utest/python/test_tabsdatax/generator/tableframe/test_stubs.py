#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for recovering handwritten content from an existing .pyi."""

from tabsdatax.generator.tableframe.engine.generate.stubs import parse_pyi

STUB = '''# Copyright 2026 Tabsdata Inc.

from __future__ import annotations

from pkg.grok import ColumnDefinition

"""Module doc placed after the imports."""

# leading comment of the class
class Frame:
    """Class doc."""

    # leading comment of first
    @property
    def first(self) -> int:
        """First doc."""
        # trailing comment of first

    def grok(self, schema: ColumnDefinition) -> Frame:
        """Grok doc."""

    # trailing comment of the class
'''


def test_module_docstring_after_imports_is_recovered():
    assert parse_pyi(STUB).module_doc == "Module doc placed after the imports."


def test_leading_and_trailing_comments_attach_to_their_member():
    doc = parse_pyi(STUB)
    first = doc.functions[("Frame", "first")]
    assert first.comments == ["    # leading comment of first"]
    assert first.trailing == ["        # trailing comment of first"]
    assert doc.functions[("Frame", "grok")].comments == []


def test_class_comments_are_recovered():
    doc = parse_pyi(STUB)
    assert doc.class_comments["Frame"] == ["# leading comment of the class"]
    assert doc.class_trailing["Frame"] == ["    # trailing comment of the class"]


def test_member_source_and_names_are_recovered():
    grok = parse_pyi(STUB).functions[("Frame", "grok")]
    assert grok.source[0] == "    def grok(self, schema: ColumnDefinition) -> Frame:"
    assert {"ColumnDefinition", "Frame"} <= grok.names


def test_imports_map_bound_names():
    imports = parse_pyi(STUB).imports
    assert imports["ColumnDefinition"] == "from pkg.grok import ColumnDefinition"
    assert "annotations" not in imports
