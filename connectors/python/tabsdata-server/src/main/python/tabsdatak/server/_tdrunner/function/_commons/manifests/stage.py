#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

from tabsdatak.server._tdrunner.function._commons.storage.stage import (
    FILES_FOLDER,
    MANIFESTS_FOLDER,
    PATH_SEPARATOR,
    Stage,
)

MANIFEST_PREFIX = "stream-manifest-"
MANIFEST_SUFFIX = ".json"

TIMESTAMP_PATTERN = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})"
    r"T(?P<hours>\d{2})-(?P<minutes>\d{2})-(?P<seconds>\d{2}\.\d{6})$"
)


class InvalidManifestError(ValueError):
    pass


@dataclass(frozen=True)
class StageManifest:
    timestamp: datetime
    tables: list[list[str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is not None:
            utc = self.timestamp.astimezone(timezone.utc).replace(tzinfo=None)
            object.__setattr__(self, "timestamp", utc)

    @classmethod
    def now(cls) -> StageManifest:
        return cls(datetime.now(tz=timezone.utc))

    @classmethod
    def parse(cls, name: str) -> StageManifest:
        filename = name.rpartition(PATH_SEPARATOR)[2]
        if not filename.startswith(MANIFEST_PREFIX):
            raise InvalidManifestError(
                f"Stage manifest filename {filename!r} lacks the prefix "
                f"{MANIFEST_PREFIX!r}"
            )
        if not filename.endswith(MANIFEST_SUFFIX):
            raise InvalidManifestError(
                f"Stage manifest filename {filename!r} lacks the suffix "
                f"{MANIFEST_SUFFIX!r}"
            )
        return cls.of_batch(filename[len(MANIFEST_PREFIX) : -len(MANIFEST_SUFFIX)])

    @classmethod
    def of_batch(cls, batch: str) -> StageManifest:
        match = TIMESTAMP_PATTERN.match(batch)
        if match is None:
            raise InvalidManifestError(
                f"Stage batch {batch!r} is not in the format 2026-06-16T10-30-12.345678"
            )
        iso = (
            f"{match.group('date')}T{match.group('hours')}:"
            f"{match.group('minutes')}:{match.group('seconds')}"
        )
        try:
            return cls(datetime.fromisoformat(iso))
        except ValueError as error:
            raise InvalidManifestError(
                f"Stage batch {batch!r} is not a valid timestamp: {error}"
            ) from error

    @classmethod
    def read(cls, stage: Stage, path: str) -> StageManifest | None:
        manifest = cls.parse(path)
        text = stage.read(path)
        if text is None:
            return None
        try:
            tables = json.loads(text)
        except ValueError as error:
            raise InvalidManifestError(
                f"Stage manifest {path} is not valid JSON: {error}"
            ) from error
        if not isinstance(tables, list) or not all(
            isinstance(files, list) and all(isinstance(file, str) for file in files)
            for files in tables
        ):
            raise InvalidManifestError(
                f"Stage manifest {path} is a {type(tables).__name__}, not a list of "
                f"lists of file names"
            )
        return replace(manifest, tables=tables)

    @property
    def batch(self) -> str:
        return self.timestamp.isoformat(timespec="microseconds").replace(":", "-")

    @property
    def filename(self) -> str:
        return f"{MANIFEST_PREFIX}{self.batch}{MANIFEST_SUFFIX}"

    @property
    def path(self) -> str:
        return f"{MANIFESTS_FOLDER}{PATH_SEPARATOR}{self.filename}"

    @property
    def folder(self) -> str:
        return f"{FILES_FOLDER}{PATH_SEPARATOR}{self.batch}"

    def file(self, name: str) -> str:
        return f"{self.folder}{PATH_SEPARATOR}{name}"

    def with_tables(self, tables: list[list[str]]) -> StageManifest:
        return replace(self, tables=tables)

    def write(self, stage: Stage) -> None:
        stage.write(self.path, json.dumps(self.tables))
