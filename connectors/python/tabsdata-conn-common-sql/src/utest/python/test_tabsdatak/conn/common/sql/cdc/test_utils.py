from __future__ import annotations

from tabsdatak.conn.common.sql.cdc._utils import sanitize_file_name


def test_sanitize_keeps_alphanumerics_underscore_dot():
    assert sanitize_file_name("schema.table") == "schema.table"
    assert sanitize_file_name("a_b.c") == "a_b.c"
    assert sanitize_file_name("Abc123") == "Abc123"


def test_sanitize_replaces_other_chars_with_dash():
    assert sanitize_file_name("a/b") == "a-b"
    assert sanitize_file_name("a b") == "a-b"
    assert sanitize_file_name("a:b") == "a-b"


def test_sanitize_empty_string():
    assert sanitize_file_name("") == ""
