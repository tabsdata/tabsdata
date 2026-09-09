#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def instances(tmp_path, monkeypatch) -> Path:
    root = tmp_path / "home"
    (root / ".tabsdata" / "instances").mkdir(parents=True)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: root))
    return root / ".tabsdata" / "instances"


@pytest.fixture
def prepared(instances):
    def make(name: str, settings: dict | None = None, configs: dict | None = None):
        folder = instances / name
        (folder / "init" / "input" / "configs").mkdir(parents=True)
        if settings is not None:
            (folder / ".settings.json").write_text(json.dumps(settings))
        for file, body in (configs or {}).items():
            (folder / "init" / "input" / "configs" / file).write_text(body)
        return folder

    return make
