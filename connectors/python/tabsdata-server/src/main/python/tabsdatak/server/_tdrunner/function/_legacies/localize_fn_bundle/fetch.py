#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from pathlib import Path

from tabsdatak.common._transporter.invoker.transporter1 import (
    TransporterBinaryNotFoundError,
)
from tabsdatak.server._tdrunner.function._legacies.localize_fn_bundle.error import (
    LegacyLocalizeFnBundleErrorCode,
)
from tabsdatak.server._tdrunner.function._lib.transporter import (
    build_bundle_fetch_request,
    run_transporter,
    write_transporter_request,
)

logger = logging.getLogger(__name__)


def fetch_via_transporter(
    request,
    bundle_archive: Path,
    transporter_request: Path,
    transporter_response: Path,
    storage_config: Path,
) -> None:
    copy_request = build_bundle_fetch_request(request, bundle_archive, storage_config)
    logger.debug(
        f"Bundle fetch transporter request: {len(copy_request.pairs)} pair(s): "
        f"{copy_request.pairs} -> {transporter_request}"
    )
    write_transporter_request(copy_request, transporter_request)
    try:
        exit_code = run_transporter(transporter_request, transporter_response)
    except TransporterBinaryNotFoundError as error:
        raise LegacyLocalizeFnBundleErrorCode.LFB_2.exception(
            cause=error,
            detail=str(error),
        )
    if exit_code != 0:
        raise LegacyLocalizeFnBundleErrorCode.LFB_3.exception(rc=exit_code)
    logger.debug(f"Bundle archive downloaded to {bundle_archive}")
