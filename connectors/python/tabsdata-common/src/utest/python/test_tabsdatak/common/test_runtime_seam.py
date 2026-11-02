#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.common import runtime

CONSTANTS = ["NO_VERSION", "TABSDATA_MODULE_NAME"]
FUNCTIONS = ["extend_path", "materialize_secrets", "resolve_env", "setup_logging"]


def test_the_runtime_contract_is_pinned():
    assert sorted(runtime.__all__) == CONSTANTS + FUNCTIONS
    for name in CONSTANTS:
        assert isinstance(getattr(runtime, name), str), name
    for name in FUNCTIONS:
        assert callable(getattr(runtime, name)), name
