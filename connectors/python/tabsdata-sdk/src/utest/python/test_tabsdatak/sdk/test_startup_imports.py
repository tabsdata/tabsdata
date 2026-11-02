#
# Copyright 2026 Tabsdata Inc.
#

import json
import subprocess
import sys

DEFERRED = (
    "croniter",
    "packaging",
    "polars",
    "pydantic",
    "tabsdatak.api",
    "tabsdatak.common._archive.bundle",
    "yaml",
)


def test_import_leaves_command_time_dependencies_unloaded():
    probe = (
        "import json, sys\n"
        "import tabsdatak.sdk.tabsdata_server\n"
        f"print(json.dumps([m for m in {list(DEFERRED)!r} if m in sys.modules]))"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert json.loads(result.stdout) == []
