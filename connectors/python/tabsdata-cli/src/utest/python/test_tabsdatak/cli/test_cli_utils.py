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

import sys

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
    # cli_utils no longer re-exports td_resource; it is resolved from reader.
    from tabsdatak.common._resources.reader import td_resource

    with pytest.raises(FileNotFoundError) as ei:
        td_resource("does/not/exist.txt")
    assert "does/not/exist.txt" in str(ei.value)


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
# _dynamic_import_function_from_path: absolute sibling imports
#
# The loader resolves bare sibling names (``import supporter``) to
# ``<folder>/supporter.py`` INSIDE the synthetic package, installed-first
# so a user file named after an installed package cannot shadow it, and
# without ever mutating ``sys.path``.
# ---------------------------------------------------------------------------


def test_dynamic_import_resolves_absolute_sibling(tmp_path):
    (tmp_path / "supporter.py").write_text(
        "def support():\n    return 'SUPPORT-OK'\n", encoding="utf-8"
    )
    (tmp_path / "publisher.py").write_text(
        "from supporter import support\n\ndef publish():\n    return support()\n",
        encoding="utf-8",
    )

    fn = cli_utils._dynamic_import_function_from_path(
        f"{tmp_path / 'publisher.py'}::publish"
    )
    assert fn() == "SUPPORT-OK"
    assert fn.__module__ == "publisher"
    assert "supporter" not in sys.modules


def test_dynamic_import_resolves_chained_absolute_siblings(tmp_path):
    (tmp_path / "b.py").write_text("VALUE = 'B'\n", encoding="utf-8")
    (tmp_path / "a.py").write_text(
        "import b\n\ndef from_a():\n    return b.VALUE\n", encoding="utf-8"
    )
    (tmp_path / "main.py").write_text(
        "import a\n\ndef run():\n    return a.from_a()\n", encoding="utf-8"
    )

    fn = cli_utils._dynamic_import_function_from_path(f"{tmp_path / 'main.py'}::run")
    assert fn() == "B"
    assert "a" not in sys.modules
    assert "b" not in sys.modules


def test_dynamic_import_does_not_shadow_installed_package(tmp_path):
    (tmp_path / "json.py").write_text("POISON = True\n", encoding="utf-8")
    (tmp_path / "user.py").write_text(
        "import json\n\ndef check():\n    return json.dumps({'ok': 1})\n",
        encoding="utf-8",
    )

    fn = cli_utils._dynamic_import_function_from_path(f"{tmp_path / 'user.py'}::check")
    assert fn() == '{"ok": 1}'
    import json as stdlib_json

    assert not hasattr(stdlib_json, "POISON")


def test_dynamic_import_resolves_sibling_package_dir(tmp_path):
    """A sibling that is a package directory (``<name>/__init__.py``), not
    just a ``.py`` file, resolves too."""
    pkg = tmp_path / "pkgmod"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("Y = 2\n", encoding="utf-8")
    (tmp_path / "user.py").write_text(
        "import pkgmod\n\ndef go():\n    return pkgmod.Y\n", encoding="utf-8"
    )

    fn = cli_utils._dynamic_import_function_from_path(f"{tmp_path / 'user.py'}::go")
    assert fn() == 2
    assert "pkgmod" not in sys.modules


def test_dynamic_import_dotted_installed_import_with_named_sibling(tmp_path):
    """A same-named sibling must not break a dotted import of the installed
    package (``import collections.abc`` with a sibling ``collections.py``)."""
    (tmp_path / "collections.py").write_text("X = 1\n", encoding="utf-8")
    (tmp_path / "user.py").write_text(
        "import collections.abc\n"
        "\n"
        "def go():\n"
        "    return collections.abc.Mapping is not None\n",
        encoding="utf-8",
    )

    fn = cli_utils._dynamic_import_function_from_path(f"{tmp_path / 'user.py'}::go")
    assert fn() is True


def test_dynamic_import_colliding_sibling_defers_to_installed(tmp_path):
    """A sibling whose name collides with an installed package is NOT
    reached by absolute import (installed-first): the installed package
    wins, so importing a sibling-only symbol from it fails."""
    (tmp_path / "json.py").write_text("SPECIAL = 'sibling'\n", encoding="utf-8")
    (tmp_path / "user.py").write_text(
        "from json import SPECIAL\n\ndef go():\n    return SPECIAL\n",
        encoding="utf-8",
    )

    with pytest.raises(ImportError) as ei:
        cli_utils._dynamic_import_function_from_path(f"{tmp_path / 'user.py'}::go")
    assert "SPECIAL" in str(ei.value)


def test_dynamic_import_two_functions_same_sibling_name_isolated(tmp_path):
    """Two functions in different folders, each with a sibling of the
    same basename but different contents, loaded back-to-back in one
    process must not bleed into each other."""
    dir_one = tmp_path / "one"
    dir_one.mkdir()
    (dir_one / "helper.py").write_text(
        "def tag():\n    return 'ONE'\n", encoding="utf-8"
    )
    (dir_one / "pub.py").write_text(
        "from helper import tag\n\ndef publish():\n    return tag()\n",
        encoding="utf-8",
    )

    dir_two = tmp_path / "two"
    dir_two.mkdir()
    (dir_two / "helper.py").write_text(
        "def tag():\n    return 'TWO'\n", encoding="utf-8"
    )
    (dir_two / "pub.py").write_text(
        "from helper import tag\n\ndef publish():\n    return tag()\n",
        encoding="utf-8",
    )

    first = cli_utils._dynamic_import_function_from_path(
        f"{dir_one / 'pub.py'}::publish"
    )
    second = cli_utils._dynamic_import_function_from_path(
        f"{dir_two / 'pub.py'}::publish"
    )
    assert first() == "ONE"
    assert second() == "TWO"
    # And re-invoking the first still sees its own helper, not the second's.
    assert first() == "ONE"
    assert "helper" not in sys.modules


def test_dynamic_import_relative_sibling_still_works(tmp_path):
    (tmp_path / "supporter.py").write_text(
        "def support():\n    return 'REL-OK'\n", encoding="utf-8"
    )
    (tmp_path / "pub.py").write_text(
        "from .supporter import support\n\ndef publish():\n    return support()\n",
        encoding="utf-8",
    )

    fn = cli_utils._dynamic_import_function_from_path(f"{tmp_path / 'pub.py'}::publish")
    assert fn() == "REL-OK"


# ---------------------------------------------------------------------------
# _dynamic_import_function_from_path: nested entry modules
#
# When the registered function lives in a subpackage of the bundle root
# (``a/b/c/d.py`` under ``dir_to_bundle``), the recorded ``__module__``
# is the dotted path relative to the bundle root (``a.b.c.d``) -- what the
# runner re-imports from ``user_code/a/b/c/d.py`` -- and relative imports
# at every level (``.sibling``, ``..cousin``) plus absolutes rooted at a
# bundle top-level package (``from a.top import ...``) all resolve.
# ---------------------------------------------------------------------------


def test_dynamic_import_nested_entry_records_dotted_module(tmp_path):
    sub = tmp_path / "a" / "b" / "c"
    sub.mkdir(parents=True)
    (sub / "d.py").write_text("def x():\n    return 1\n", encoding="utf-8")

    fn = cli_utils._dynamic_import_function_from_path(
        f"{sub / 'd.py'}::x", bundle_root=str(tmp_path)
    )
    assert fn() == 1
    assert fn.__module__ == "a.b.c.d"


def test_dynamic_import_nested_entry_resolves_all_relative_levels(tmp_path):
    sub = tmp_path / "a" / "b" / "c"
    sub.mkdir(parents=True)
    (sub / "sibling.py").write_text("S = 'SIB'\n", encoding="utf-8")
    (tmp_path / "a" / "b" / "cousin.py").write_text("C = 'COU'\n", encoding="utf-8")
    (tmp_path / "a" / "top.py").write_text("T = 'TOP'\n", encoding="utf-8")
    (sub / "d.py").write_text(
        "from .sibling import S\n"
        "from ..cousin import C\n"
        "from a.top import T\n"
        "\n"
        "def x():\n"
        "    return f'{S}/{C}/{T}'\n",
        encoding="utf-8",
    )

    fn = cli_utils._dynamic_import_function_from_path(
        f"{sub / 'd.py'}::x", bundle_root=str(tmp_path)
    )
    assert fn() == "SIB/COU/TOP"
    assert fn.__module__ == "a.b.c.d"


def test_dynamic_import_bundle_root_defaults_to_file_folder(tmp_path):
    (tmp_path / "supporter.py").write_text(
        "def s():\n    return 'FLAT'\n", encoding="utf-8"
    )
    (tmp_path / "publisher.py").write_text(
        "from .supporter import s\n\ndef publish():\n    return s()\n",
        encoding="utf-8",
    )
    # No bundle_root -> the file's own folder is the root; module stays bare.
    fn = cli_utils._dynamic_import_function_from_path(
        f"{tmp_path / 'publisher.py'}::publish"
    )
    assert fn() == "FLAT"
    assert fn.__module__ == "publisher"


def test_dynamic_import_unprefixes_class_from_sibling_module(tmp_path):
    """A user class imported into the entry from a sibling module has its
    ``__module__`` stamped as the bundle-relative dotted name (not the
    synthetic ``td_usr_pkg_<sha>.`` prefix), so ``qualified_class`` writes
    a runtime-importable ref into ``function_def.json``."""
    sub = tmp_path / "a" / "b"
    sub.mkdir(parents=True)
    (sub / "mod.py").write_text(
        "class Thing:\n    tag = 'NESTED'\n", encoding="utf-8"
    )
    (sub / "pub.py").write_text(
        "from .mod import Thing\n"
        "\n"
        "obj = Thing()\n"
        "\n"
        "def x():\n"
        "    return type(obj).__module__\n",
        encoding="utf-8",
    )

    fn = cli_utils._dynamic_import_function_from_path(
        f"{sub / 'pub.py'}::x", bundle_root=str(tmp_path)
    )
    assert fn() == "a.b.mod"
    assert "td_usr_pkg_" not in fn()


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


# ---------------------------------------------------------------------------
# proj_option / _apply_proj (per-command --proj override)
# ---------------------------------------------------------------------------


def test_apply_proj_overrides_active_project():
    from types import SimpleNamespace

    server = SimpleNamespace(connection=SimpleNamespace(project="pinned"))
    ctx = SimpleNamespace(obj={"tabsdataserver": server})

    # A value overrides the active project on the connection.
    cli_utils._apply_proj(ctx, None, "explicit")
    assert server.connection.project == "explicit"

    # No value leaves the active project untouched (pinned default stays).
    cli_utils._apply_proj(ctx, None, None)
    assert server.connection.project == "explicit"


def test_apply_proj_is_a_noop_without_a_server():
    from types import SimpleNamespace

    ctx = SimpleNamespace(obj={"tabsdataserver": None})
    # Must not raise when logged out (no server on the context).
    assert cli_utils._apply_proj(ctx, None, "whatever") == "whatever"


def test_proj_option_decorator_adds_proj_flag():
    import rich_click as click

    @click.command()
    @cli_utils.proj_option
    @click.pass_context
    def cmd(ctx):
        pass

    opts = [o for p in cmd.params for o in p.opts]
    assert "--proj" in opts and "-P" in opts
