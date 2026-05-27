#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import Callable

import pytest
import yaml


@pytest.fixture
def tmp_mount(tmp_path: Path) -> Path:
    mount = tmp_path / "mount" / "td"
    mount.mkdir(parents=True)
    return mount


@pytest.fixture
def make_request_yaml(tmp_path: Path) -> Callable[..., Path]:
    def _make(content: dict | None = None, *, path: Path | None = None) -> Path:
        target = path or (tmp_path / "request.yaml")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(yaml.safe_dump(content or {}))
        return target

    return _make
