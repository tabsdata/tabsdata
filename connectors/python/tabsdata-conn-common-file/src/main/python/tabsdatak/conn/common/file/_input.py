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


def import_one(
    *,
    handler: TransporterHandler,
    source,
    target_dir: Path,
    file_format,
    paths: Sequence[str],
    format_cfg: dict | None = None,
    initial_lastmod: str | None = None,
    lastmod_info: str | None = None,
    purge: bool = False,
    env: dict[str, str] | None = None,
) -> TransporterReport:
    request = handler.import_request(
        source=source,
        target_dir=target_dir,
        file_format=file_format,
        paths=paths,
        format_cfg=format_cfg,
        initial_lastmod=initial_lastmod,
        lastmod_info=lastmod_info,
        purge=purge,
    )
    return handler.execute(request, target_dir, env)


def wildcard_copy_one(
    *,
    handler: TransporterHandler,
    source,
    target_dir: Path,
    initial_lastmod: str | None = None,
    lastmod_info: str | None = None,
    max_files: int | None = None,
    env: dict[str, str] | None = None,
) -> TransporterReport:
    """Copy files matching ``source`` (a wildcard location) into
    ``target_dir`` verbatim -- no format transcoding -- via the
    transporter's wildcard-copy operation.

    ``max_files`` caps how many of the matched files are consumed (the
    oldest by last-modified time); pass ``1`` to drain a single file per
    call. ``initial_lastmod`` / ``lastmod_info`` drive incremental
    consumption exactly as for :func:`import_one`; the returned report's
    ``lastmod_info`` is the cursor to feed back on the next call.
    """
    request = handler.wildcard_request(
        source=source,
        target_dir=target_dir,
        initial_lastmod=initial_lastmod,
        lastmod_info=lastmod_info,
        max_files=max_files,
    )
    return handler.execute(request, target_dir, env)
