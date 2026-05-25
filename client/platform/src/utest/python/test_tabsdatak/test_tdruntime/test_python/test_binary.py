#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import sysconfig
from pathlib import Path

import pytest

from tabsdatak._tdruntime.python.binary import (
    Binary,
    binaries_from_setup_toml,
    current_scripts_dir,
    default_scripts_prefix,
)

pytestmark = pytest.mark.unit


def test_binary_exe_appends_platform_extension():
    binary = Binary(name="td_apiserver", prefix="bin")
    expected = "td_apiserver" + sysconfig.get_config_var("EXE")
    assert binary.exe() == expected


def test_binary_exe_unchanged_on_posix(monkeypatch):
    """On POSIX, sysconfig's EXE config var is empty so ``exe()``
    returns the bare name."""
    monkeypatch.setattr(sysconfig, "get_config_var", lambda key: "")
    binary = Binary(name="td_apiserver", prefix="bin")
    assert binary.exe() == "td_apiserver"


def test_binary_exe_adds_dot_exe_on_windows(monkeypatch):
    """On Windows, sysconfig's EXE config var is ``.exe`` so ``exe()``
    appends it."""
    monkeypatch.setattr(
        sysconfig,
        "get_config_var",
        lambda key: ".exe" if key == "EXE" else None,
    )
    binary = Binary(name="td_apiserver", prefix="bin")
    assert binary.exe() == "td_apiserver.exe"


def test_binary_is_frozen_dataclass():
    binary = Binary(name="td_apiserver", prefix="bin")
    with pytest.raises((AttributeError, Exception)):
        binary.name = "td_other"


def test_default_scripts_prefix_is_a_relative_path():
    prefix = default_scripts_prefix()
    assert prefix
    assert not Path(prefix).is_absolute()


def test_current_scripts_dir_is_absolute():
    assert current_scripts_dir().is_absolute()


def test_current_scripts_dir_matches_sysconfig_scripts_path():
    """The helper is a thin wrapper around ``sysconfig.get_path("scripts")``,
    documented as the source of truth for "where pip installs CLI tools
    in the active interpreter"."""
    assert current_scripts_dir() == Path(sysconfig.get_path("scripts"))


def test_binaries_from_setup_toml_returns_empty_when_file_missing(tmp_path: Path):
    binaries = binaries_from_setup_toml(tmp_path)
    assert binaries == ()


def test_binaries_from_setup_toml_returns_empty_when_no_binary_groups(tmp_path: Path):
    (tmp_path / "setup.toml").write_text("[other]\nkey = 'value'\n", encoding="utf-8")
    binaries = binaries_from_setup_toml(tmp_path)
    assert binaries == ()


def test_binaries_from_setup_toml_reads_default_prefix_group(tmp_path: Path):
    content = """
[[binary_groups]]
name = "tabsdata"
names = ["td_apiserver", "td_supervisor"]
"""
    (tmp_path / "setup.toml").write_text(content, encoding="utf-8")
    binaries = binaries_from_setup_toml(tmp_path)
    assert len(binaries) == 2
    assert {entry.name for entry in binaries} == {"td_apiserver", "td_supervisor"}
    default_prefix = default_scripts_prefix()
    assert all(entry.prefix == default_prefix for entry in binaries)


def test_binaries_from_setup_toml_honours_install_dest(tmp_path: Path):
    content = """
[[binary_groups]]
name = "kubernetes-srv"
install_dest = "libexec/tabsdata"
names = ["control-plane", "etcd"]
"""
    (tmp_path / "setup.toml").write_text(content, encoding="utf-8")
    binaries = binaries_from_setup_toml(tmp_path)
    assert {entry.prefix for entry in binaries} == {"libexec/tabsdata"}
    assert {entry.name for entry in binaries} == {"control-plane", "etcd"}


def test_binaries_from_setup_toml_combines_multiple_groups(tmp_path: Path):
    content = """
[[binary_groups]]
name = "tabsdata"
names = ["td_apiserver"]

[[binary_groups]]
name = "kubernetes-srv"
install_dest = "libexec/tabsdata"
names = ["control-plane"]
"""
    (tmp_path / "setup.toml").write_text(content, encoding="utf-8")
    binaries = binaries_from_setup_toml(tmp_path)
    assert len(binaries) == 2
    by_name = {entry.name: entry for entry in binaries}
    assert by_name["td_apiserver"].prefix == default_scripts_prefix()
    assert by_name["control-plane"].prefix == "libexec/tabsdata"


def test_binary_link_creates_symlink(tmp_path: Path):
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    source_bin = source_root / "bin"
    source_bin.mkdir(parents=True)
    binary = Binary(name="td_apiserver", prefix="bin")
    source_file = source_bin / binary.exe()
    source_file.write_text("payload", encoding="utf-8")

    binary.link(source_root, target_root)

    target_file = target_root / "bin" / binary.exe()
    assert target_file.is_symlink()
    assert target_file.resolve() == source_file.resolve()


def test_binary_link_replaces_existing_target(tmp_path: Path):
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    (source_root / "bin").mkdir(parents=True)
    (target_root / "bin").mkdir(parents=True)
    binary = Binary(name="td_apiserver", prefix="bin")
    (source_root / "bin" / binary.exe()).write_text("new", encoding="utf-8")
    stale = target_root / "bin" / binary.exe()
    stale.write_text("stale", encoding="utf-8")

    binary.link(source_root, target_root)

    assert stale.is_symlink()
    assert stale.resolve().read_text(encoding="utf-8") == "new"
