#
# Copyright 2026 Tabsdata Inc.
#

"""Unit tests for the pure helpers in ``tabsdatak.cli.cli_utils``.

The Click/REPL-bound helpers (``prompt_with_escape``,
``logical_prompt``, ``utils_login``, ``initialise_tabsdata_server_connection``,
etc.) are exercised end-to-end against a live ``td`` invocation in the
CLI integration suite; here we test what's purely functional:

* ``is_valid_id`` / ``beautify_list`` / ``beautify_time``
* the temporary ``td_resource`` stand-in
* ``MutuallyExclusiveOption``
"""

from __future__ import annotations

import sys

import pytest

from tabsdatak.cli import cli_utils


@pytest.fixture(autouse=True)
def _restore_sys_modules():
    """``_dynamic_import_function_from_path`` loads user files under bare
    top-level names (``a``, ``helper``, ``supporter``, ...) and resolves
    bare siblings through a ``_SiblingFinder``. It unregisters what it
    resolves, but a bare name served from a pre-existing ``sys.modules``
    entry is never claimed (so never cleaned), and its own cleanup is
    path-prefix based (fragile across separators / case-insensitive
    filesystems). ``sys.modules`` is shared across tests on an xdist
    worker, so snapshot and restore it around every test -- mirroring the
    ``_api`` suite -- so no bare user-module name leaks between the two
    suites in either direction."""
    saved = dict(sys.modules)
    try:
        yield
    finally:
        for name in list(sys.modules):
            if name not in saved:
                del sys.modules[name]
        sys.modules.update(saved)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "good",
    [
        "06fqh2ec35u11f1vb50ubkoi48",  # 26-char lowercase base32hex id
        "ABCDEFGHIJ0123456789QRSTUV",  # uppercase form (decoder is case-insensitive)
        "wkr20260728-3",  # logical id, unpadded counter
        "fv20260728-0000000013",  # logical id, zero-padded counter
    ],
)
def test_is_valid_id_accepts_2_0_id_forms(good):
    assert cli_utils.is_valid_id(good) is True


@pytest.mark.parametrize(
    "bad",
    [
        "",  # empty
        "abcdefghijklmnopqrstuvwxyz",  # base32hex has no w-z
        "ABCDEFGHIJ0123456789QRSTU",  # 25 chars (too short)
        "ABCDEFGHIJ0123456789QRSTUVW",  # 27 chars (too long)
        "ABCDEFGHIJ0123456789QRSTU!",  # invalid char
        "wkr20260728",  # logical id missing the counter
        "3",  # a bare list index, not an id
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


def test_td_resource_raises_filenotfound_for_missing_resource():
    # cli_utils no longer re-exports td_resource; it is resolved from reader.
    from tabsdatak.common._resources.reader import td_resource

    with pytest.raises(FileNotFoundError) as ei:
        td_resource("does/not/exist.txt")
    assert "does/not/exist.txt" in str(ei.value)


# ---------------------------------------------------------------------------
# load_info_and_project_root: thin wrapper used by tdk fn register/update/patch
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


def test_dynamic_import_rejects_a_dotted_file_name(tmp_path):
    (tmp_path / "publisher.good.py").write_text(
        "def publish():\n    return 'OK'\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="not a valid Python module name"):
        cli_utils._dynamic_import_function_from_path(
            f"{tmp_path / 'publisher.good.py'}::publish"
        )


def test_dynamic_import_rejects_a_dotted_folder_name(tmp_path):
    folder = tmp_path / "my.pkg"
    folder.mkdir()
    (folder / "publisher.py").write_text(
        "def publish():\n    return 'OK'\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="not a valid Python module name"):
        cli_utils._dynamic_import_function_from_path(
            f"{folder / 'publisher.py'}::publish", bundle_root=str(tmp_path)
        )


def test_dynamic_import_unprefixes_class_from_sibling_module(tmp_path):
    """A user class imported into the entry from a sibling module has its
    ``__module__`` stamped as the bundle-relative dotted name (not the
    synthetic ``td_usr_pkg_<sha>.`` prefix), so ``qualified_class`` writes
    a runtime-importable ref into ``function_def.json``."""
    sub = tmp_path / "a" / "b"
    sub.mkdir(parents=True)
    (sub / "mod.py").write_text("class Thing:\n    tag = 'NESTED'\n", encoding="utf-8")
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


def test_dynamic_import_leaves_imported_builtin_types_untouched(tmp_path):
    """A user file importing an immutable builtin type (``from datetime
    import datetime``) must not fail: the ``__module__`` rewrite only
    touches the bundle's own synthetic-prefixed classes, never imported
    builtins whose ``__module__`` cannot be set (would raise 'cannot set
    __module__ attribute of immutable type')."""
    (tmp_path / "periodic_snapshot.py").write_text(
        "from datetime import datetime, timedelta\n"
        "from collections import OrderedDict\n"
        "\n"
        "class MyConn:\n"
        "    pass\n"
        "\n"
        "def pub_period_snapshot():\n"
        "    return datetime, OrderedDict, MyConn\n",
        encoding="utf-8",
    )

    fn = cli_utils._dynamic_import_function_from_path(
        f"{tmp_path / 'periodic_snapshot.py'}::pub_period_snapshot",
        bundle_root=str(tmp_path),
    )
    datetime_cls, ordered_dict_cls, my_conn_cls = fn()
    # Imported builtins keep their real module; the user's own class is
    # stripped to the bundle-relative name.
    assert datetime_cls.__module__ == "datetime"
    assert ordered_dict_cls.__module__ == "collections"
    assert my_conn_cls.__module__ == "periodic_snapshot"
    assert fn.__module__ == "periodic_snapshot"


# ---------------------------------------------------------------------------
# _dynamic_import_function_from_path: odd-but-legal user-file shapes.
#
# These all worked when first swept; the tests pin the behavior so a future
# change to the ``__module__`` rewrite / synthetic-import algorithm cannot
# silently regress them. Each registers a user file exercising one shape and
# asserts it loads without error.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name, body",
    [
        (
            "imports_module_objects",
            "import os\nimport json\n\ndef x():\n    return os, json\n",
        ),
        (
            "local_enum",
            "from enum import Enum\n\n"
            "class E(Enum):\n    A = 1\n\n"
            "def x():\n    return E.A\n",
        ),
        (
            "local_dataclass",
            "from dataclasses import dataclass\n\n"
            "@dataclass\nclass C:\n    a: int = 1\n\n"
            "def x():\n    return C()\n",
        ),
        (
            "namedtuple",
            "from collections import namedtuple\n\n"
            "Point = namedtuple('Point', ['a'])\n\n"
            "def x():\n    return Point(1)\n",
        ),
        (
            "metaclass",
            "class Meta(type):\n    pass\n\n"
            "class C(metaclass=Meta):\n    pass\n\n"
            "def x():\n    return C\n",
        ),
        (
            "local_exception",
            "class MyErr(Exception):\n    pass\n\ndef x():\n    return MyErr\n",
        ),
        (
            "aliased_builtin",
            "from collections import OrderedDict as OD\n\ndef x():\n    return OD\n",
        ),
        (
            "star_import_stdlib",
            "from os.path import *\n\ndef x():\n    return join('a', 'b')\n",
        ),
        (
            "type_checking_guard",
            "from typing import TYPE_CHECKING\n\n"
            "if TYPE_CHECKING:\n    from datetime import datetime\n\n"
            "def x():\n    return 1\n",
        ),
        (
            "uses_dunder_name",
            "NAME = __name__\n\ndef x():\n    return NAME\n",
        ),
        (
            "main_guard",
            "def x():\n    return 1\n\n\n"
            "if __name__ == '__main__':\n    raise SystemExit\n",
        ),
    ],
)
def test_dynamic_import_odd_but_legal_user_files_load(tmp_path, name, body):
    (tmp_path / f"{name}.py").write_text(body, encoding="utf-8")
    fn = cli_utils._dynamic_import_function_from_path(f"{tmp_path / f'{name}.py'}::x")
    # Loads and is callable; the file leaf is the recorded module.
    assert callable(fn)
    assert fn.__module__ == name


def test_dynamic_import_class_with_manually_faked_module(tmp_path):
    """A user class whose ``__module__`` was manually set to an unrelated
    string is not our synthetic prefix, so the rewrite leaves it alone."""
    (tmp_path / "faked.py").write_text(
        "class C:\n    pass\n\n"
        "C.__module__ = 'totally.unrelated'\n\n"
        "def x():\n    return C\n",
        encoding="utf-8",
    )
    fn = cli_utils._dynamic_import_function_from_path(f"{tmp_path / 'faked.py'}::x")
    assert fn().__module__ == "totally.unrelated"


def test_dynamic_import_symbol_is_a_builtin_callable(tmp_path):
    """The registered symbol can itself be a builtin (``x = len``); the
    rewrite must not attempt to set ``__module__`` on it."""
    (tmp_path / "aliases_builtin.py").write_text("x = len\n", encoding="utf-8")
    fn = cli_utils._dynamic_import_function_from_path(
        f"{tmp_path / 'aliases_builtin.py'}::x"
    )
    assert fn is len


def test_dynamic_import_wrapper_whose_func_is_a_builtin(tmp_path):
    """An info-shaped wrapper whose ``.func`` is a builtin must not fail
    the ``__module__`` rewrite of the underlying callable."""
    (tmp_path / "wraps_builtin.py").write_text(
        "class W:\n    func = len\n\nx = W()\n", encoding="utf-8"
    )
    info = cli_utils._dynamic_import_function_from_path(
        f"{tmp_path / 'wraps_builtin.py'}::x"
    )
    assert info.func is len


# ---------------------------------------------------------------------------
# proj_option / _apply_proj (per-command --proj override)
# ---------------------------------------------------------------------------


def _proj_ctx(server):
    import rich_click as click

    return click.Context(click.Command("cmd"), obj={"tabsdataserver": server})


def test_apply_proj_overrides_active_project():
    from types import SimpleNamespace

    server = SimpleNamespace(connection=SimpleNamespace(project="pinned"))
    ctx = _proj_ctx(server)

    # A value overrides the active project on the connection.
    cli_utils._apply_proj(ctx, None, "explicit")
    assert server.connection.project == "explicit"

    # No value leaves the active project untouched (pinned default stays).
    cli_utils._apply_proj(ctx, None, None)
    assert server.connection.project == "explicit"


def test_apply_proj_is_a_noop_without_a_server():
    ctx = _proj_ctx(None)
    # Must not raise when logged out (no server on the context).
    assert cli_utils._apply_proj(ctx, None, "whatever") == "whatever"


def test_apply_proj_marks_the_command_project_scoped():
    # The option's presence -- not its value -- is what tells
    # resolve_project_or_prompt to settle the project first.
    ctx = _proj_ctx(None)
    cli_utils._apply_proj(ctx, None, None)
    assert ctx.obj["project_scoped"] is True
    assert "proj" not in ctx.obj

    cli_utils._apply_proj(ctx, None, "explicit")
    assert ctx.obj["proj"] == "explicit"


def test_proj_option_decorator_adds_proj_flag():
    import rich_click as click

    @click.command()
    @cli_utils.proj_option
    @click.pass_context
    def cmd(ctx):
        pass

    opts = [o for p in cmd.params for o in p.opts]
    assert "--proj" in opts and "-P" in opts
