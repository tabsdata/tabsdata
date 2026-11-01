#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, fields
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

ENCODING = "utf-8"


# Unix epoch milliseconds, the i64 `start` and `end` of the apiserver's
# CallbackRequest (td-objects dxo/callback.rs); keys this version does not know
# are ignored, so the file can grow without breaking older readers.
@dataclass
class Journal:
    start: int | None = None
    end: int | None = None

    @classmethod
    def load(cls, path: Path, *, strict: bool) -> Journal:
        if not strict:
            # noinspection PyBroadException
            try:
                return cls.load(path, strict=True)
            except Exception:
                logger.warning(
                    f"Cannot load the journal {path}; using defaults",
                    exc_info=True,
                )
                return cls()
        document = yaml.safe_load(path.read_text(encoding=ENCODING)) or {}
        known = {field.name for field in fields(cls)}
        return cls(**{key: value for key, value in document.items() if key in known})

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(asdict(self), sort_keys=False))
