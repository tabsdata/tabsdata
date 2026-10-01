#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Self


class StreamOffset(ABC):
    @abstractmethod
    def update(self, other: Self) -> None: ...

    @abstractmethod
    def write(self) -> str:
        """Serialize this offset to an opaque string. Each impl owns its
        own encoding; the runner persists the string verbatim and hands
        it back to :meth:`read` on resume."""
        ...

    @classmethod
    @abstractmethod
    def read(cls, data: str) -> Self:
        """Reconstruct an offset from a string produced by :meth:`write`."""
        ...
