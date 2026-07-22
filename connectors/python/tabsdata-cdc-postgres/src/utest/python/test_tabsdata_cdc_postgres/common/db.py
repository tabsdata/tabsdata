#
#  Copyright 2026 Tabsdata Inc.
#

import random
import string


def replication_slot() -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=32))
    return f"td_slot_{suffix}"
