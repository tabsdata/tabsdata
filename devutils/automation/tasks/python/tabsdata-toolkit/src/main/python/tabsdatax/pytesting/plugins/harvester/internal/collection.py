#
#  Copyright 2026 Tabsdata Inc.
#

"""What the trackers need of the collected tests, as plain data.

Under xdist only the workers collect, so the first worker writes this
description to the session folder and the controller builds its trackers
from it; without xdist the only process builds them from the same data.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pytest

COLLECTION_FILENAME = "_collection.json"


@dataclass(frozen=True)
class CollectedFixture:
    name: str
    scope: str
    autouse: bool


@dataclass(frozen=True)
class CollectedTest:
    nodeid: str
    path: str
    markers: list[str] = field(default_factory=list)
    fixtures: list[CollectedFixture] = field(default_factory=list)


def describe(items: list[pytest.Item]) -> list[CollectedTest]:
    resolved: dict[str, CollectedFixture] = {}
    tests = []
    for item in items:
        fixtures = []
        if isinstance(item, pytest.Function):
            for name in item.fixturenames:
                if name not in resolved:
                    resolved[name] = resolve_fixture(item, name)
                fixtures.append(resolved[name])
        tests.append(
            CollectedTest(
                nodeid=item.nodeid,
                path=str(item.path),
                markers=sorted({marker.name for marker in item.iter_markers()}),
                fixtures=fixtures,
            )
        )
    return tests


def resolve_fixture(item: pytest.Function, name: str) -> CollectedFixture:
    try:
        definitions = item.session._fixturemanager.getfixturedefs(name, item)
    except Exception:
        definitions = None
    if not definitions:
        return CollectedFixture(name, "function", False)
    definition = definitions[-1]
    return CollectedFixture(
        name,
        str(definition.scope or "function"),
        bool(getattr(definition, "_autouse", False)),
    )


def write_collection(folder: str, tests: list[CollectedTest]) -> None:
    path = Path(folder, COLLECTION_FILENAME)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps([asdict(test) for test in tests]), encoding="utf-8")
    os.replace(temporary, path)


def read_collection(folder: str) -> list[CollectedTest] | None:
    try:
        data = json.loads(Path(folder, COLLECTION_FILENAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return [
        CollectedTest(
            nodeid=test["nodeid"],
            path=test["path"],
            markers=test["markers"],
            fixtures=[CollectedFixture(**fixture) for fixture in test["fixtures"]],
        )
        for test in data
    ]
