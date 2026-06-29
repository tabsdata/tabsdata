#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from pathlib import Path

import yaml

from tabsdatak.api import Conn
from tabsdatak.common._json_serde import env_hydrate


def load_connection(connection_file: Path) -> Conn:
    document = yaml.safe_load(connection_file.read_text(encoding="utf-8"))
    hydrated = env_hydrate(document)
    return Conn.from_dict(hydrated)
