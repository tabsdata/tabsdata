#
# Copyright 2026 Tabsdata Inc.
#

"""prep-upload-raw runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.inputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_INPUT_IDX_KEY,
    MANIFEST_TABLES_KEY,
    load_inputs_manifest_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function._lib.transporter import (
    build_upload_raw_request,
    write_transporter_request,
)
from tabsdatak.server._tdrunner.function.commands.prep_upload_raw.error import (
    PrepUploadRawErrorCode,
)

logger = logging.getLogger(__name__)


def _raw_files(entries: list[dict]) -> list[tuple[int, Path]]:
    files: list[tuple[int, Path]] = []
    for entry in entries:
        for record in entry.get(MANIFEST_TABLES_KEY) or []:
            file = record.get(MANIFEST_FILE_KEY)
            idx = record.get(MANIFEST_INPUT_IDX_KEY)
            if file is None or idx is None:
                continue
            files.append((idx, Path(file)))
    return files


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    inputs_manifest_path = Path(args.inputs_manifest)
    out_path = Path(args.out)
    storage_config = Path(args.storage_config)

    logger.info("Preparing upload-raw transporter request")
    request = load_request_or_raise(request_path, PrepUploadRawErrorCode.PUR_1)
    if request.info.function_raw is None:
        logger.debug(
            "The request carries no raw-data location; this function ingests "
            "nothing, so there is no raw data to upload"
        )
    entries = load_inputs_manifest_or_raise(
        inputs_manifest_path, PrepUploadRawErrorCode.PUR_2
    )
    raw_files = _raw_files(entries)
    logger.debug(
        "Enriched manifest has %d slot(s) holding %d stamped parquet(s): %s",
        len(entries),
        len(raw_files),
        raw_files,
    )

    copy_req = build_upload_raw_request(request, raw_files, storage_config)
    if not copy_req.pairs:
        logger.debug("Upload-raw request has no pairs; nothing to persist")
    else:
        logger.debug(
            "Upload-raw copy request has %d pair(s): %s",
            len(copy_req.pairs),
            copy_req.pairs,
        )

    try:
        write_transporter_request(copy_req, out_path)
    except OSError as exc:
        raise PrepUploadRawErrorCode.PUR_4.exception(
            cause=exc, path=out_path, detail=str(exc)
        )
    logger.info("Upload-raw preparation complete; wrote %s", out_path)
    return 0
