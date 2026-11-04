#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]

ROOT_FILE = ".root"
TEMPLATES = Path("variant", "resources", "kprofile", "pods")
COMMAND = re.compile(r"^\s*command:\s*(.+)$", re.M)
TRANSPORTER = "transporter"
ENCODING = "utf-8"


def checkout() -> Path:
    for folder in Path(__file__).parents:
        if (folder / ROOT_FILE).is_file():
            return folder
    raise AssertionError(f"No {ROOT_FILE} above {__file__}")


def commands() -> dict[str, list[str]]:
    found = {}
    for template in sorted((checkout() / TEMPLATES).glob("*.yaml")):
        found[template.name] = COMMAND.findall(template.read_text(encoding=ENCODING))
    return found


def test_the_pod_templates_are_found():
    found = commands()
    assert found
    assert any(found.values())


def test_no_pod_step_runs_the_transporter():
    offending = {
        name: command
        for name, lines in commands().items()
        for command in lines
        if TRANSPORTER in command.lower()
    }
    assert offending == {}
