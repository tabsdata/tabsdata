#
#  Copyright 2026 Tabs Data Inc.
#

import random


def server_id() -> int:
    return random.randint(1, 2**32 - 1)
