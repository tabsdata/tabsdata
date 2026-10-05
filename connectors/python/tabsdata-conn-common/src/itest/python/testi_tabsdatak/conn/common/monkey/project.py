#
# Copyright 2026 Tabsdata Inc.
#

"""The function project of a monkey run, written per run: a fleet module the
caller provides, copied as is, next to a ``fleet.json`` describing the run's
functions with their physical table, collection and topic names. The module
builds its functions from that file when imported, here and in the worker
alike, so concurrent runs register functions that touch only their own
objects."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import ModuleType

from testi_tabsdatak.conn.common.function_project import function_module
from testi_tabsdatak.conn.common.monkey.naming import (
    collection_name,
    table_name,
    topic_name,
)
from testi_tabsdatak.conn.common.monkey.plan import (
    STREAM_PUBLISHER,
    Fleet,
    FunctionSpec,
)

FLEET_FILE = "fleet.json"


def fleet_config(fleet: Fleet, namespace: str) -> dict:
    def upstream(spec: FunctionSpec) -> str | None:
        if spec.upstream is None:
            return None
        source = fleet.function(spec.upstream)
        return (
            f"{collection_name(namespace, source.collection)}/{table_name(source.name)}"
        )

    return {
        "functions": [
            {
                "name": spec.name,
                "kind": spec.kind,
                "table": table_name(spec.name),
                "seconds": spec.seconds,
                "upstream": upstream(spec),
                "fails": spec.fails,
                "topic": topic_name(namespace, spec.name)
                if spec.kind == STREAM_PUBLISHER
                else None,
            }
            for spec in fleet.functions
        ]
    }


def write_project(
    root: Path, fleet: Fleet, namespace: str, module_file: Path
) -> ModuleType:
    root.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(module_file, root / module_file.name)
    (root / FLEET_FILE).write_text(json.dumps(fleet_config(fleet, namespace), indent=2))
    return function_module(root, module_file.stem)
