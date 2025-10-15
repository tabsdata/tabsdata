#
#  Copyright 2025 Tabs Data Inc.
#

import sys

import yaml

# noinspection PyBroadException
try:
    with open(sys.argv[1], "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
        if data:
            for instance in data.keys():
                print(instance)
except Exception:
    sys.exit(0)
