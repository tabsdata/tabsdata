#
# Copyright 2026 Tabsdata Inc.
#

"""How the file plugins turn a file into the data and metadata they land.

- inference: the mode a source asks for, `auto` only for now;
- policy: chosen from the inference and the file format; it produces the
  data, and the metadata that tells how to serve it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from enum import Enum
from typing import Any, Protocol

from tabsdatak.conn.common.file._read._blocks import BLOCK_SIZE, Readable
from tabsdatak.conn.common.file._read._csv import AutoCsv
from tabsdatak.conn.common.file._read._log import Log
from tabsdatak.conn.common.file._read._ndjson import AutoNdjson
from tabsdatak.spi import LazyFrameInput


class Inference(Enum):
    AUTO = "auto"


class Format(Enum):
    CSV = "csv"
    NDJSON = "ndjson"
    LOG = "log"


class Policy(Protocol):
    def read(self, open_: Callable[[], Readable]) -> LazyFrameInput: ...


def policy(
    file_format: Format,
    name: str,
    options: Mapping[str, Any] | None = None,
    *,
    inference: Inference = Inference.AUTO,
    block_size: int = BLOCK_SIZE,
) -> Policy:
    if inference is not Inference.AUTO:
        raise ValueError(f"The inference '{inference.value}' is unknown")
    if file_format is Format.CSV:
        return AutoCsv(options, block_size=block_size)
    if file_format is Format.NDJSON:
        return AutoNdjson(block_size=block_size)
    return Log(name, block_size=block_size)
