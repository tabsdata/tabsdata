#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import base64
import logging

from uuid6 import UUID, uuid7

# Sub-DEBUG log level for very fine-grained CDC tracing. Duplicated
# here (was ``tabsdata._tabsserver.function.global_utils.TRACE``) so the
# shared CDC code carries no dependency on the legacy ``tabsdata.*``
# packages.
TRACE = logging.DEBUG - 1


def encode_id() -> tuple[UUID, str]:
    """Generate a fresh time-ordered id and its base32hex code.

    Duplicated from ``tabsdata._utils.id.encode_id`` so the shared CDC
    code stays free of legacy ``tabsdata.*`` dependencies. Returns the
    raw ``uuid7`` plus a 26-char base32hex string suitable for file
    names (lexicographic order tracks creation time).
    """
    id_uuid = uuid7()
    id_code = base64.b32hexencode(id_uuid.bytes).decode("ascii")[:26]
    return id_uuid, id_code


def sanitize_file_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in "_." else "-" for c in name)
