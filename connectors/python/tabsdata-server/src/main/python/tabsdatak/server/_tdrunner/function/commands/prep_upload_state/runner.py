#
# Copyright 2026 Tabsdata Inc.
#

"""prep-upload-state runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function._lib.transporter import (
    build_upload_state_request,
    write_transporter_request,
)
from tabsdatak.server._tdrunner.function.commands.prep_upload_state.error import (
    PrepUploadStateErrorCode,
)

logger = logging.getLogger(__name__)


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)
    storage_config = Path(args.storage_config)

    logger.info("Preparing upload-state transporter request")
    request = load_request_or_raise(request_path, PrepUploadStateErrorCode.PUS_1)
    logger.debug(
        "Request declares %d system_output state slot(s): %s; state_dir=%s",
        len(request.system_output),
        request.system_output,
        state_dir,
    )

    copy_req = build_upload_state_request(request, state_dir, storage_config)
    if not copy_req.pairs:
        logger.debug(
            "Upload-state request has no pairs (no state files present on "
            "disk for any declared system_output slot)"
        )
    else:
        logger.debug(
            "Upload-state copy request has %d pair(s): %s",
            len(copy_req.pairs),
            copy_req.pairs,
        )

    try:
        write_transporter_request(copy_req, out_path)
    except OSError as exc:
        raise PrepUploadStateErrorCode.PUS_3.exception(
            cause=exc, path=out_path, detail=str(exc)
        )
    logger.info("Upload-state preparation complete; wrote %s", out_path)
    return 0
