#
# Copyright 2025 Tabs Data Inc.
#

import os

from tabsdata._utils.constants import TRUE_VALUES


def is_env_enabled(env: str) -> bool:
    return os.getenv(env, "False").lower() in TRUE_VALUES
