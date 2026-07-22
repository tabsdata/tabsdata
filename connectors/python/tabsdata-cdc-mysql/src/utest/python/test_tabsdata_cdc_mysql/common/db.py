#
#  Copyright 2026 Tabsdata Inc.
#

import random


def server_id() -> int:
    return random.randint(1, 2**32 - 1)
