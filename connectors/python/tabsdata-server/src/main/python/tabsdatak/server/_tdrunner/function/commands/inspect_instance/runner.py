#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import logging
import os

logger = logging.getLogger(__name__)

MOUNTED_FOLDERS = [
    os.path.join("tabsdata", "pods"),
    os.path.join("tabsdata", "configs"),
]


def run(args: argparse.Namespace) -> int:
    for folder in MOUNTED_FOLDERS:
        print(f"Reading mounted '{folder}' folder", flush=True)
        try:
            names = sorted(os.listdir(folder))
        except OSError as error:
            print(f"[inspector]   {folder} = <unreadable: {error}>", flush=True)
            continue
        for name in names:
            path = os.path.join(folder, name)
            if not os.path.isfile(path):
                continue
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as handle:
                    contents = handle.read()
            except OSError as error:
                contents = f"<unreadable: {error}>"
            print(f"[inspector]   {folder}/{name} = {contents}", flush=True)
    print("Done!", flush=True)
    return 0
