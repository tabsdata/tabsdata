#
#  Copyright 2026 Tabsdata Inc.
#

"""``tabsdatak.cli.td.config_utils`` -- the variant-action config loader.

Two things pinned here:

* it parses the actions YAML (via ruamel) and defaults to ``{}`` for a missing
  or empty file, and
* it does NOT depend on PyYAML. PyYAML is not a declared tabsdata-cli dependency
  (only ruamel is, transitively via tabsdata-api), and this module is imported
  eagerly by the root CLI, so a stray top-level ``import yaml`` would fail the
  whole CLI on a minimal install. The env happens to have PyYAML pulled in by
  unrelated packages, which is exactly what masks such a regression -- so we
  reload the module with PyYAML made unavailable and assert it still imports.
"""

from __future__ import annotations

import importlib
import sys

from tabsdatak.cli.td import config_utils as cu


def test_load_actions_config_parses_yaml(tmp_path, monkeypatch):
    (tmp_path / "login.yaml").write_text(
        "actions:\n  - some_action:\n      enabled: true\n"
    )
    monkeypatch.setattr(cu, "td_resource", lambda _path: str(tmp_path))
    assert cu.load_actions_config_for_command("login") == {
        "actions": [{"some_action": {"enabled": True}}]
    }


def test_load_actions_config_missing_file_returns_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(cu, "td_resource", lambda _path: str(tmp_path))
    assert cu.load_actions_config_for_command("no_such_command") == {}


def test_load_actions_config_empty_file_returns_empty(tmp_path, monkeypatch):
    # An empty YAML document parses to None; the loader must still yield a dict.
    (tmp_path / "login.yaml").write_text("")
    monkeypatch.setattr(cu, "td_resource", lambda _path: str(tmp_path))
    assert cu.load_actions_config_for_command("login") == {}


def test_config_utils_imports_without_pyyaml():
    # Simulate a minimal install where PyYAML is absent: setting the module to
    # None in sys.modules makes ``import yaml`` raise ImportError. Reloading
    # config_utils must still succeed -- proving it does not import PyYAML.
    saved = sys.modules.get("yaml", "__absent__")
    sys.modules["yaml"] = None
    try:
        reloaded = importlib.reload(cu)
        assert hasattr(reloaded, "execute_actions_for_command")
    finally:
        if saved == "__absent__":
            sys.modules.pop("yaml", None)
        else:
            sys.modules["yaml"] = saved
        # Restore the pristine module now that PyYAML is importable again.
        importlib.reload(cu)
