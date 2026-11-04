#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import sys

from peak import peak_bytes

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._codecs import decompressed

# noinspection PyProtectedMember
from tabsdatak.conn.common.file._store import FileStore

READ_SIZE = 16 << 20


def main(root: str, options: str, name: str, extension: str) -> None:
    stream = decompressed(FileStore(root, json.loads(options)).open(name), extension)
    total = 0
    while chunk := stream.read(READ_SIZE):
        total += len(chunk)
    stream.close()
    print(json.dumps({"total": total, "peak": peak_bytes()}))


if __name__ == "__main__":
    main(*sys.argv[1:])
