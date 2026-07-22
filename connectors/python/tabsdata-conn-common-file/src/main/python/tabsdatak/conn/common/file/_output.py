#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from tabsdatak.common._transporter.handler.handler import (
    TransporterHandler,
    TransporterReport,
)


def copy_one(
    *,
    handler: TransporterHandler,
    source_target_pairs: list[tuple],
    work_dir: Path,
    file_format,
    paths: Sequence[str],
    format_cfg: dict | None = None,
    env: dict[str, str] | None = None,
) -> TransporterReport:
    request = handler.copy_request(
        source_target_pairs, work_dir, file_format, paths, format_cfg
    )
    return handler.execute(request, work_dir, env)
