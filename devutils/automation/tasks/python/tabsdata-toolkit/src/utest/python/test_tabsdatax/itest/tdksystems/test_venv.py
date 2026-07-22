#
# Copyright 2026 Tabsdata Inc.
#

"""Tests for the self-managing uv-venv bootstrap (`_venv`) and its CLI wiring.

The actual venv creation/install shells out to `uv`; these tests cover the
pure logic — project discovery, the bootstrap no-op/short-circuit gates, the
delete-venvs behaviour, and that `delete-venvs` is wired into the parser —
without ever invoking `uv` or re-exec'ing.
"""

from __future__ import annotations

import sys

from tabsdatax.itest.tdksystems import _venv


def test_project_dir_has_pyproject():
    proj = _venv._project_dir()
    assert (proj / "pyproject.toml").is_file()


def test_venv_python_path():
    py = _venv._venv_python(_venv.MAIN_VENV)
    assert py.name == "python"
    assert py.parent.name in ("bin", "Scripts")
    assert py.parent.parent == _venv.MAIN_VENV


def _boom(*_a, **_k):  # fail loudly if a guard is bypassed
    raise AssertionError("must not be reached")


def test_ensure_reuses_installed_venv_without_touching_uv(tmp_path, monkeypatch):
    # A marker-present venv is returned as-is: no uv invocation, no reinstall.
    venv = tmp_path / ".tdx"
    venv.mkdir()
    (venv / _venv._MARKER).write_text("ready")
    monkeypatch.setattr(_venv, "_uv", _boom)
    monkeypatch.setattr(_venv, "_run", _boom)

    assert _venv._ensure(venv, editable_extra="main") == venv


def test_ensure_rebuilds_partial_venv_with_clear(tmp_path, monkeypatch):
    # A venv dir with no marker (interrupted first install) is rebuilt from
    # scratch: `uv venv --clear` heals it instead of erroring on the stale dir.
    venv = tmp_path / ".tdx"
    venv.mkdir()  # exists, but no marker

    monkeypatch.setattr(_venv, "_uv", lambda: "uv")
    monkeypatch.setattr(_venv, "_project_dir", lambda: tmp_path / "proj")
    calls = []
    monkeypatch.setattr(_venv, "_run", lambda cmd, *, what: calls.append(cmd))

    _venv._ensure(venv, editable_extra="main")

    venv_cmd = next(c for c in calls if c[:2] == ["uv", "venv"])
    assert "--clear" in venv_cmd
    assert (venv / _venv._MARKER).is_file()  # marker written after success


def test_bootstrap_noop_when_already_in_venv(monkeypatch):
    monkeypatch.setattr(sys, "prefix", str(_venv.MAIN_VENV))
    monkeypatch.setattr(_venv, "ensure_main_venv", _boom)
    monkeypatch.setattr(_venv.os, "execv", _boom)
    _venv.bootstrap()


def test_target_venv_routing():
    mv, sv = _venv.MAIN_VENV, _venv.SALESFORCE_VENV
    assert _venv.target_venv(["data", "load", "--model", "world"]) == mv
    assert _venv.target_venv(["sys", "salesforce"]) == mv  # no --model
    assert _venv.target_venv(["data", "load", "--model", "salesforce"]) == sv
    assert _venv.target_venv(["data", "create", "--model=salesforce"]) == sv


def test_bootstrap_reexecs_when_not_in_venv(monkeypatch):
    # A `tdx-system` spawned by a generated script runs in the launcher (not the
    # venv) and MUST re-exec into the managed venv. Decided solely by
    # sys.prefix (_in_venv), never an env var.
    monkeypatch.setattr(_venv, "_in_venv", lambda _v: False)
    monkeypatch.setattr(_venv, "ensure_main_venv", lambda: _venv.MAIN_VENV)
    monkeypatch.setattr(sys, "argv", ["tdx-system", "data", "load", "--model", "world"])
    seen = {}
    monkeypatch.setattr(
        _venv.os, "execv", lambda path, argv: seen.update(path=path, argv=argv)
    )
    _venv.bootstrap()
    assert seen.get("path") == str(_venv._venv_python(_venv.MAIN_VENV))


def test_bootstrap_skips_for_delete_venvs(monkeypatch):
    # delete-venvs must run in the launcher interpreter: no create, no re-exec.
    monkeypatch.setattr(_venv, "_in_venv", lambda _v: False)
    monkeypatch.setattr(_venv, "ensure_main_venv", _boom)
    monkeypatch.setattr(_venv.os, "execv", _boom)
    monkeypatch.setattr(sys, "argv", ["tdx-system", "delete-venvs"])
    _venv.bootstrap()


def test_delete_venvs_removes_dirs(tmp_path, monkeypatch, capsys):
    main = tmp_path / ".tdx"
    sf = tmp_path / ".tdz"
    for v in (main, sf):
        v.mkdir()
        (v / _venv._MARKER).write_text("x")
    monkeypatch.setattr(_venv, "MAIN_VENV", main)
    monkeypatch.setattr(_venv, "SALESFORCE_VENV", sf)

    rc = _venv.delete_venvs()

    assert rc == 0
    assert not main.exists() and not sf.exists()
    assert "removed" in capsys.readouterr().out
