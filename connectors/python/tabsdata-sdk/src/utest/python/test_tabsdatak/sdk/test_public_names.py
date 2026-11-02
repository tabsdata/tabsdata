#
# Copyright 2026 Tabsdata Inc.
#

import json
import subprocess
import sys


def test_api_server_error_is_public_and_loads_on_first_use():
    probe = (
        "import json, sys\n"
        "import tabsdatak.sdk\n"
        "before = 'requests' in sys.modules\n"
        "from tabsdatak.sdk import APIServerError\n"
        "from tabsdatak.sdk._apiserver import APIServerError as raised\n"
        "print(json.dumps([before, APIServerError is raised]))"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert json.loads(result.stdout) == [False, True]
