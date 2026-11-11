#
# Copyright 2026 Tabsdata Inc.
#

import yaml

from tabsdatax.cmake.kupgrader.commands.noop import perform_noop
from tabsdatax.cmake.kupgrader.common.pipeline import NOOP_SPEC_KEYS
from tabsdatax.cmake.kupgrader.common.workflow import Context

INVENTORY = """\
schema: 1
versions:
  - version: 2.3.0
    upgrader: true
  - version: 2.0.0
    upgrader: false
"""


def repository(tmp_path, version: str) -> Context:
    context = Context(mode="noop", root_folder=tmp_path)
    context.version_file.parent.mkdir(parents=True)
    context.version_file.write_text(f"{version}\n", encoding="utf-8")
    context.upgrades_folder.mkdir(parents=True)
    context.upgrades_file.write_text(INVENTORY, encoding="utf-8")
    return context


def versions_of(context: Context) -> list[tuple[str, bool]]:
    document = yaml.safe_load(context.upgrades_file.read_text(encoding="utf-8"))
    return [(entry["version"], entry["upgrader"]) for entry in document["versions"]]


def test_noop_declares_an_upgrader_that_changes_nothing(tmp_path):
    context = repository(tmp_path, "2.4.0")
    perform_noop(tmp_path)

    spec = yaml.safe_load(
        (context.upgrades_folder / "2.4.0.yaml").read_text(encoding="utf-8")
    )
    assert set(spec) <= NOOP_SPEC_KEYS
    assert not (context.upgrades_folder / "2.4.0").exists()
    assert versions_of(context) == [
        ("2.4.0", True),
        ("2.3.0", True),
        ("2.0.0", False),
    ]
    assert (context.rules_folder / "2.4.0.yaml").is_file()


def test_noop_leaves_a_declared_version_alone(tmp_path):
    context = repository(tmp_path, "2.3.0")
    perform_noop(tmp_path)

    assert context.upgrades_file.read_text(encoding="utf-8") == INVENTORY
    assert not (context.upgrades_folder / "2.3.0.yaml").exists()
    assert not context.rules_folder.exists()
