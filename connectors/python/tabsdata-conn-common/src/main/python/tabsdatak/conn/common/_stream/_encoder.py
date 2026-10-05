#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

import pyarrow as pa

from tabsdatak.conn.common._stream._offset import StreamOffset


class StreamEncoder[R, O: StreamOffset](ABC):
    @abstractmethod
    def slot(self, record: R) -> str | None: ...

    @abstractmethod
    def size(self, record: R) -> int: ...

    @abstractmethod
    def offset(self, record: R) -> O: ...

    @abstractmethod
    def encode(self, slot: str, records: list[R]) -> Iterable[pa.Table]: ...

    def rows(self, record: R) -> int:
        return 1
