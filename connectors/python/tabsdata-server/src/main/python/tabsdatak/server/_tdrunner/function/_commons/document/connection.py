#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import cast

import yaml

from tabsdatak.common._json_serde import env_hydrate
from tabsdatak.spi import Conn

ENCODING = "utf-8"


class InvalidConnectionError(ValueError):
    path: Path
    detail: str

    def __init__(self, path: Path, detail: str) -> None:
        super().__init__(f"Cannot load the connection {path}: {detail}")
        self.path = path
        self.detail = detail


class ConnectionDocument:
    path: Path

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> Conn:
        try:
            document = yaml.safe_load(self.path.read_text(encoding=ENCODING))
        except (OSError, yaml.YAMLError) as error:
            raise InvalidConnectionError(self.path, str(error)) from error
        try:
            return Conn.from_dict(cast(dict, env_hydrate(document)))
        except Exception as error:
            raise InvalidConnectionError(self.path, str(error)) from error
