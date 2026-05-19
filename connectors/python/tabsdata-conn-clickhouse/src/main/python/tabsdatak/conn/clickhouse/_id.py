"""Upload-id generator for staged bulk loads.

Returns a 26-character base32hex string that:

- is unique per call (UUIDv7 backing),
- sorts lexicographically by creation time (the leading bits of
  a UUIDv7 are the unix-millis timestamp, and base32hex preserves
  ordering),
- is filesystem- / URL-safe so it can be used as a path segment.

The :func:`tabsdatak.conn.clickhouse._cloud_storage.GCSStorage.delete`
cleanup walks ``base_path/`` and removes every object whose
``upload_id <= up_to_id`` -- the sweep only behaves correctly when
``upload_id`` is time-ordered, hence UUIDv7 (not UUIDv4).
"""

from __future__ import annotations

import base64

from uuid6 import uuid7


def new_upload_id() -> str:
    return base64.b32hexencode(uuid7().bytes).decode("ascii")[:26]
