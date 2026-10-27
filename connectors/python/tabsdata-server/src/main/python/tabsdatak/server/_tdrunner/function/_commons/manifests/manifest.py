#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Self

import yaml
from pydantic import TypeAdapter, ValidationError

ENCODING = "utf-8"
JSON_MODE = "json"


class ManifestError(ValueError):
    pass


@dataclass(frozen=True)
class Manifest:
    @classmethod
    def load(cls, path: Path) -> Self:
        try:
            document = yaml.safe_load(path.read_text(encoding=ENCODING))
            return TypeAdapter(cls).validate_python(document)
        except (OSError, yaml.YAMLError, ValidationError) as error:
            raise ManifestError(f"Cannot load the manifest {path}: {error}") from error

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            yaml.safe_dump(
                TypeAdapter(type(self)).dump_python(self, mode=JSON_MODE),
                sort_keys=False,
            ),
            encoding=ENCODING,
        )
