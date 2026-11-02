#
# Copyright 2026 Tabsdata Inc.
#

import json
import subprocess
import sys

DEFERRED = (
    "croniter",
    "pydantic",
    "rich.live",
    "ruamel",
    "tabsdatak._api",
    "tabsdatak.api",
    "tabsdatak.sdk.tabsdata_server",
    "yaml",
    "zstandard",
)


def test_import_leaves_command_time_dependencies_unloaded():
    probe = (
        "import json, sys\n"
        "import tabsdatak.cli.td.cli\n"
        f"print(json.dumps([m for m in {list(DEFERRED)!r} if m in sys.modules]))"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert json.loads(result.stdout) == []
