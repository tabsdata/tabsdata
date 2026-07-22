#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from typing import cast

import yaml

from tabsdatak.common._json_serde import env_hydrate
from tabsdatak.spi import Conn


def load_connection(connection_file: Path) -> Conn:
    document = yaml.safe_load(connection_file.read_text(encoding="utf-8"))
    hydrated = cast(dict, env_hydrate(document))
    return Conn.from_dict(hydrated)
