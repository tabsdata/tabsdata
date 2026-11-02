#
# Copyright 2026 Tabsdata Inc.
#

import pytest

from tabsdatax.setup.common.groups import (
    load_edition_resource_groups,
    load_resource_groups,
)
from tabsdatax.setup.tabsdata.binaries import collect_resource_datafiles

SETUP_TOML = """
[[binary_groups]]
name = "backend"
names = ["td_apiserver"]

[[resource_groups]]
name = "model"
source = "target/models/m"
install_dest = "share/tabsdata/models/m"
names = ["config.json", "model.safetensors"]
"""


def groups(tmp_path):
    package = tmp_path / "package"
    package.mkdir(exist_ok=True)
    (package / "setup.toml").write_text(SETUP_TOML)
    return load_resource_groups(str(package))


def test_a_complete_group_is_installed_under_its_destination(tmp_path, monkeypatch):
    model = tmp_path / "target" / "models" / "m"
    model.mkdir(parents=True)
    for name in ("config.json", "model.safetensors"):
        (model / name).write_text("x")
    monkeypatch.chdir(tmp_path)
    datafiles = collect_resource_datafiles(groups(tmp_path), str(tmp_path))
    assert datafiles == [
        (
            "share/tabsdata/models/m",
            ["target/models/m/config.json", "target/models/m/model.safetensors"],
        )
    ]


def test_an_incomplete_group_is_skipped_or_fails_a_release_build(tmp_path):
    model = tmp_path / "target" / "models" / "m"
    model.mkdir(parents=True)
    (model / "config.json").write_text("x")
    assert collect_resource_datafiles(groups(tmp_path), str(tmp_path)) == []
    with pytest.raises(FileNotFoundError, match="model.safetensors"):
        collect_resource_datafiles(
            groups(tmp_path), str(tmp_path), require_server_binaries=True
        )


def test_a_package_without_resource_groups_has_none(tmp_path):
    assert load_resource_groups(str(tmp_path)) == []


def test_an_edition_adds_resource_groups_to_a_shared_package(tmp_path):
    setup = tmp_path / "variant" / "config" / "setup"
    setup.mkdir(parents=True)
    (setup / "package.toml").write_text(SETUP_TOML)
    assert [group["name"] for group in load_edition_resource_groups(str(tmp_path), "package")] == ["model"]
    assert load_edition_resource_groups(str(tmp_path), "other") == []
