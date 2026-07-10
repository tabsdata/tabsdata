#
# Copyright 2026 Tabs Data Inc.
#

"""prep-upload-tables runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    load_outputs_manifest_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function._lib.transporter import (
    OutputTableNotInManifestError,
    build_upload_tables_request,
    write_transporter_request,
)
from tabsdatak.server._tdrunner.function.commands.prep_upload_tables.error import (
    PrepUploadTablesErrorCode,
)

logger = logging.getLogger(__name__)


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    outputs_manifest_path = Path(args.outputs_manifest)
    out_path = Path(args.out)
    storage_config = Path(args.storage_config)

    logger.info("Preparing upload-tables transporter request")
    request = load_request_or_raise(request_path, PrepUploadTablesErrorCode.PUT_1)
    manifest_entries = load_outputs_manifest_or_raise(
        outputs_manifest_path, PrepUploadTablesErrorCode.PUT_2
    )
    logger.debug(
        "Outputs manifest has %d entr%s: %s (from %s); "
        "request declares %d output table(s): %s",
        len(manifest_entries),
        "y" if len(manifest_entries) == 1 else "ies",
        manifest_entries,
        outputs_manifest_path,
        len(request.output),
        request.output,
    )

    try:
        copy_req = build_upload_tables_request(
            request, manifest_entries, storage_config
        )
    except OutputTableNotInManifestError as exc:
        raise PrepUploadTablesErrorCode.PUT_3.exception(cause=exc, detail=str(exc))
    if not copy_req.pairs:
        logger.debug(
            "Upload-tables request has no pairs (every declared output table "
            "either had file: null in the manifest or no Location)"
        )
    else:
        logger.debug(
            "Upload-tables copy request has %d pair(s): %s",
            len(copy_req.pairs),
            copy_req.pairs,
        )

    try:
        write_transporter_request(copy_req, out_path)
    except OSError as exc:
        raise PrepUploadTablesErrorCode.PUT_5.exception(
            cause=exc, path=out_path, detail=str(exc)
        )
    logger.info("Upload-tables preparation complete; wrote %s", out_path)
    return 0
