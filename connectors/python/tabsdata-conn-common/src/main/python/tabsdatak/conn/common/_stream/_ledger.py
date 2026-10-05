#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import base64
import logging
from collections.abc import Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from uuid6 import UUID, uuid7

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def encode_id() -> tuple[UUID, str]:
    id_uuid = uuid7()
    id_code = base64.b32hexencode(id_uuid.bytes).decode("ascii")[:26]
    return id_uuid, id_code


def sanitize_file_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in "_." else "-" for c in name)


class FileLedger:
    def __init__(self, *, landing: Path, slots: Iterable[str]):
        self._landing = landing
        self._files: dict[str, list[Path]] = {slot: [] for slot in slots}
        self._rows: int = 0
        self._bytes: int = 0

    @property
    def rows(self) -> int:
        return self._rows

    @property
    def bytes(self) -> int:
        return self._bytes

    @property
    def has_data(self) -> bool:
        return self._rows > 0

    def write(self, slot: str, table: pa.Table) -> Path:
        _, file_suffix = encode_id()
        file_prefix = sanitize_file_name(slot)
        file_name = f"{file_prefix}_{file_suffix}.parquet"
        file_path = self._landing / file_name
        pq.write_table(table, file_path)

        file_bytes = file_path.stat().st_size
        file_rows = table.num_rows

        self._files[slot].append(file_path)
        self._rows += file_rows
        self._bytes += file_bytes

        logger.debug(
            f"Wrote {file_rows} row(s) and {file_bytes} byte(s) of {slot} "
            f"to {file_name}"
        )
        return file_path

    def manifest(self) -> list[list[Path]]:
        return [list(files) for files in self._files.values()]

    def clear(self) -> None:
        for files in self._files.values():
            files.clear()
        self._rows = 0
        self._bytes = 0

    def discard(self) -> None:
        for files in self._files.values():
            for path in files:
                path.unlink(missing_ok=True)
        self.clear()
