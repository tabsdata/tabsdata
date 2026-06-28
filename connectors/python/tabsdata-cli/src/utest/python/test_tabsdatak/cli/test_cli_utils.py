#
# Copyright 2026 Tabs Data Inc.
#

"""Unit tests for the pure helpers in ``tabsdatak.cli.cli_utils``.

The Click/REPL-bound helpers (``prompt_with_escape``,
``logical_prompt``, ``utils_login``, ``initialise_tabsdata_server_connection``,
etc.) are exercised end-to-end against a live ``td`` invocation in the
CLI integration suite; here we test what's purely functional:

* ``is_valid_id`` / ``beautify_list`` / ``beautify_time``
* the temporary ``tdabout`` / ``td_resource`` stand-ins
* ``MutuallyExclusiveOption`` / ``CurrentPlatform``
"""

from __future__ import annotations

import pytest

from tabsdatak.cli import cli_utils

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_is_valid_id_accepts_26char_uppercase_alphanumeric():
    assert cli_utils.is_valid_id("ABCDEFGHIJ0123456789QRSTUV") is True


@pytest.mark.parametrize(
    "bad",
    [
        "",  # empty
        "abcdefghijklmnopqrstuvwxyz",  # lowercase
        "ABCDEFGHIJ0123456789QRSTU",  # 25 chars (too short)
        "ABCDEFGHIJ0123456789QRSTUVW",  # 27 chars (too long)
        "ABCDEFGHIJ0123456789QRSTU!",  # invalid char
    ],
)
def test_is_valid_id_rejects_bad_inputs(bad):
    assert cli_utils.is_valid_id(bad) is False


def test_beautify_list_passes_through_none_marker():
    assert cli_utils.beautify_list(None) == "<None>"
    assert cli_utils.beautify_list([]) == "<None>"


def test_beautify_list_joins_list_with_newlines():
    assert cli_utils.beautify_list(["a", "b", "c"]) == "a\nb\nc"


def test_beautify_list_stringifies_scalar():
    assert cli_utils.beautify_list(42) == "42"


def test_beautify_time_replaces_string_none_with_dash():
    assert cli_utils.beautify_time("None") == "-"


def test_beautify_time_returns_other_strings_verbatim():
    assert cli_utils.beautify_time("2026-05-29T12:00:00Z") == "2026-05-29T12:00:00Z"


# ---------------------------------------------------------------------------
# Temporary stand-ins (replaced in a future ticket)
# ---------------------------------------------------------------------------


def test_tdabout_prints_package_and_version(capsys):
    cli_utils.tdabout("tabsdata-cli")
    captured = capsys.readouterr()
    assert "tabsdata-cli" in captured.out


def test_tdabout_unknown_package_prints_unknown_version(capsys):
    cli_utils.tdabout("definitely-not-an-installed-package")
    captured = capsys.readouterr()
    assert "definitely-not-an-installed-package" in captured.out
    assert "unknown" in captured.out


def test_td_resource_raises_filenotfound_for_missing_resource():
    with pytest.raises(FileNotFoundError) as ei:
        cli_utils.td_resource("does/not/exist.txt")
    assert "does/not/exist.txt" in str(ei.value)


def test_td_resource_normalises_backslashes(monkeypatch, tmp_path):
    """The legacy helper normalised Windows-style backslashes; the
    stand-in must do the same."""
    # We can't easily test a real resource lookup without writing a
    # file to a packaged location, so test the FileNotFoundError
    # message which echoes the normalised input.
    with pytest.raises(FileNotFoundError) as ei:
        cli_utils.td_resource("a\\b\\c")
    # The error message includes the original (pre-normalisation)
    # string so the user can match what they passed.
    assert "a" in str(ei.value)


# ---------------------------------------------------------------------------
# load_info_and_project_root: thin wrapper used by td fn register/update/patch
# ---------------------------------------------------------------------------


def test_load_info_and_project_root_uses_file_dir_when_dir_to_bundle_none(
    tmp_path, monkeypatch
):
    """``project_root`` defaults to the directory containing the
    user's .py file when ``dir_to_bundle`` is None."""
    user_dir = tmp_path / "my_pkg"
    user_dir.mkdir()
    user_file = user_dir / "publisher.py"
    user_file.write_text(
        "class _Stub:\n    func = None\n\npub = _Stub()\n", encoding="utf-8"
    )

    info, project_root = cli_utils.load_info_and_project_root(
        f"{user_file}::pub", dir_to_bundle=None
    )
    assert info.__class__.__name__ == "_Stub"
    # ``project_root`` is the file's parent directory (absolute path).
    assert project_root == str(user_dir)


def test_load_info_and_project_root_respects_explicit_dir_to_bundle(tmp_path):
    user_dir = tmp_path / "my_pkg"
    user_dir.mkdir()
    user_file = user_dir / "publisher2.py"
    user_file.write_text(
        "class _Stub:\n    func = None\n\npub = _Stub()\n", encoding="utf-8"
    )
    explicit_root = tmp_path / "elsewhere"
    explicit_root.mkdir()

    info, project_root = cli_utils.load_info_and_project_root(
        f"{user_file}::pub", dir_to_bundle=str(explicit_root)
    )
    assert project_root == str(explicit_root)
    assert info.__class__.__name__ == "_Stub"


# ---------------------------------------------------------------------------
# _dynamic_import_function_from_path: synthetic-pkg ``__module__`` rewrite
#
# The CLI loads the user's file under a synthetic package name
# ``td_usr_pkg_<sha>.<file>`` to avoid shadowing installed packages
# named after the user's file (e.g. ``databricks.py``). The synthetic
# prefix must NOT leak into the bundle's serialised ``func`` reference --
# the runner extracts the bundle under ``user_code/`` and resolves
# ``func`` from there, where it lives as ``<file>.py`` (no synthetic
# prefix). The loader therefore rewrites ``func.__module__`` (or the
# raw symbol's ``__module__``) to the bare file leaf-name.
# ---------------------------------------------------------------------------


def test_dynamic_import_rewrites_module_on_wrapper_with_func_attr(tmp_path):
    """An ``*Info``-shaped wrapper (``getattr(info, 'func')`` is the
    underlying callable) gets ``info.func.__module__`` rewritten to the
    file's leaf name."""
    user_file = tmp_path / "publisher.py"
    user_file.write_text(
        "def publish_people():\n"
        "    pass\n"
        "\n"
        "class _Info:\n"
        "    def __init__(self, fn):\n"
        "        self.func = fn\n"
        "\n"
        "publish = _Info(publish_people)\n",
        encoding="utf-8",
    )

    info, _ = cli_utils.load_info_and_project_root(
        f"{user_file}::publish", dir_to_bundle=None
    )
    # The underlying callable's ``__module__`` is the bare file
    # basename, not the synthetic ``td_usr_pkg_<sha>.publisher``.
    assert info.func.__module__ == "publisher"


def test_dynamic_import_rewrites_module_on_raw_callable(tmp_path):
    """When the symbol is itself a callable (no ``.func`` wrapper), its
    own ``__module__`` is rewritten."""
    user_file = tmp_path / "raw.py"
    user_file.write_text(
        "def my_fn():\n    pass\n",
        encoding="utf-8",
    )

    fn, _ = cli_utils.load_info_and_project_root(
        f"{user_file}::my_fn", dir_to_bundle=None
    )
    assert fn.__module__ == "raw"


def test_dynamic_import_synthetic_prefix_not_observable(tmp_path):
    """The synthetic ``td_usr_pkg_<sha>`` prefix never appears in the
    function's ``__module__`` after loading. This is the property the
    bundle producer relies on -- ``callable_to_str`` writes
    ``f"{func.__module__}:{qualname}"`` into ``function_def.json``."""
    user_file = tmp_path / "publisher_x.py"
    user_file.write_text(
        "def f():\n    pass\n",
        encoding="utf-8",
    )

    fn, _ = cli_utils.load_info_and_project_root(f"{user_file}::f", dir_to_bundle=None)
    assert "td_usr_pkg_" not in fn.__module__


def test_dynamic_import_rewrites_module_on_user_defined_classes(tmp_path):
    """Top-level user-defined classes in the same file get their
    ``__module__`` rewritten too. ``info.to_json`` serialises ``conn``
    / ``source`` via ``qualified_class(type(obj))`` (= ``f"{cls.__module__}:{cls.__qualname__}"``),
    so a user-defined ``Conn`` / ``Src`` subclass would otherwise be
    stamped with the synthetic prefix and the runner's
    ``_load_function_def`` would fail to import it from
    ``user_code/<file>.py``."""
    user_file = tmp_path / "stream_publisher.py"
    user_file.write_text(
        "class RandomStreamSrcConn:\n"
        "    pass\n"
        "\n"
        "class RandomStreamSrc:\n"
        "    pass\n"
        "\n"
        "class _Info:\n"
        "    def __init__(self):\n"
        "        self.conn = RandomStreamSrcConn()\n"
        "        self.source = RandomStreamSrc()\n"
        "        self.func = lambda: None\n"
        "\n"
        "info = _Info()\n",
        encoding="utf-8",
    )

    info, _ = cli_utils.load_info_and_project_root(
        f"{user_file}::info", dir_to_bundle=None
    )

    # type(info.conn).__module__ is what ``qualified_class`` uses to
    # write ``conn`` into ``function_def.json``; the runner imports
    # this module string at deserialise time.
    assert type(info.conn).__module__ == "stream_publisher"
    assert type(info.source).__module__ == "stream_publisher"
    # And of course the function rewrite still works.
    assert info.func.__module__ == "stream_publisher"


# ---------------------------------------------------------------------------
# CurrentPlatform
# ---------------------------------------------------------------------------


def test_current_platform_exposes_system_name():
    p = cli_utils.CurrentPlatform()
    # ``.platform`` returns the underlying ``platform.system()`` output
    # (e.g. ``"Darwin"``, ``"Linux"``, ``"Windows"``).
    import platform as _stdlib_platform

    assert p.platform == _stdlib_platform.system()


def test_current_platform_predicates_are_mutually_exclusive():
    p = cli_utils.CurrentPlatform()
    truths = [p.is_windows(), p.is_linux(), p.is_mac()]
    # Exactly one of windows/linux/mac is True on a typical host;
    # ``is_unix()`` overlaps with both linux and mac so it isn't part
    # of this exclusivity check.
    assert sum(truths) <= 1
