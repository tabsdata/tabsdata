#
# Copyright 2026 Tabsdata Inc.
#

"""prep-upload-raw runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies pre-built ``PrepUploadRawArguments``.
"""

from __future__ import annotations

import logging
from pathlib import Path

from tabsdatak.server._tdrunner.function._commons.document.request import Request
from tabsdatak.server._tdrunner.function._commons.manifests.input import (
    InputManifest,
)
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import (
    ManifestError,
)
from tabsdatak.server._tdrunner.function._lib.transporter import (
    build_upload_landing_request,
    write_transporter_request,
)
from tabsdatak.server._tdrunner.function.commands.prep_upload_raw.error import (
    PrepUploadRawErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.prep_upload_raw.options import (
    PrepUploadRawArguments,
)

logger = logging.getLogger(__name__)


def _landing_files(manifest: InputManifest) -> list[tuple[int, Path]]:
    return [
        (record.input_idx, Path(record.file))
        for entry in manifest.inputs
        for record in entry.tables
        if record.file is not None and record.input_idx is not None
    ]


def run(arguments: PrepUploadRawArguments) -> int:
    request_path = arguments.request
    inputs_manifest_path = arguments.inputs_manifest
    transporter_request_path = arguments.transporter_request
    storage_configuration = arguments.storage_configuration

    logger.info("Preparing upload-raw transporter request")
    request = Request.load(request_path, PrepUploadRawErrorCode.PUR_1)
    try:
        manifest = InputManifest.load(inputs_manifest_path)
    except ManifestError as error:
        raise PrepUploadRawErrorCode.PUR_2.exception(
            cause=error, path=inputs_manifest_path, detail=str(error)
        )
    landing_files = _landing_files(manifest)
    logger.debug(
        "Enriched manifest has %d slot(s) holding %d stamped parquet(s): %s",
        len(manifest.inputs),
        len(landing_files),
        landing_files,
    )

    copy_req = build_upload_landing_request(
        request, landing_files, storage_configuration
    )
    if not copy_req.pairs:
        logger.debug("Upload-raw request has no pairs; nothing to persist")
    else:
        logger.debug(
            "Upload-raw copy request has %d pair(s): %s",
            len(copy_req.pairs),
            copy_req.pairs,
        )

    try:
        write_transporter_request(copy_req, transporter_request_path)
    except OSError as exc:
        raise PrepUploadRawErrorCode.PUR_4.exception(
            cause=exc, path=transporter_request_path, detail=str(exc)
        )
    logger.info("Upload-raw preparation complete; wrote %s", transporter_request_path)
    return 0
