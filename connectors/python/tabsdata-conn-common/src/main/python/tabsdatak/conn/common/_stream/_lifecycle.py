#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from tabsdatak.conn.common._stream._offset import StreamOffset


class StreamLifecycle[O: StreamOffset]:
    def checkpoint(self, offset: O) -> bool:
        return False

    def advance(self, offset: O) -> None:
        pass

    def settle(self) -> None:
        pass
