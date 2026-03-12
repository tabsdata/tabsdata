#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations


def sanitize_file_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in "_." else "-" for c in name)
