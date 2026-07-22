#
# Copyright 2026 Tabsdata Inc.
#

"""report-completion runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.command import notify_done
from tabsdatak.server._tdrunner.function._lib.journal import Journal
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    load_outputs_manifest_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request import load_request_or_raise
from tabsdatak.server._tdrunner.function._lib.response import (
    Response,
    write_response,
)
from tabsdatak.server._tdrunner.function.commands.report_completion.error import (
    ReportCompletionErrorCode,
)

logger = logging.getLogger(__name__)


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    outputs_manifest_path = Path(args.outputs_manifest)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)

    logger.info("Building and submitting function-run response")
    request = load_request_or_raise(request_path, ReportCompletionErrorCode.RC_1)
    manifest_entries = load_outputs_manifest_or_raise(
        outputs_manifest_path, ReportCompletionErrorCode.RC_2
    )
    logger.debug(
        "Outputs manifest has %d entr%s: %s; request declares %d output "
        "table(s): %s + %d system_output table(s): %s",
        len(manifest_entries),
        "y" if len(manifest_entries) == 1 else "ies",
        manifest_entries,
        len(request.output),
        request.output,
        len(request.system_output),
        request.system_output,
    )

    response = Response.from_fn_response(
        request,
        manifest_entries,
        state_dir,
        mismatch_error_code=ReportCompletionErrorCode.RC_3,
    )
    data_count = sum(1 for e in response.output if e.info is not None)
    nodata_count = len(response.output) - data_count
    logger.info(
        "Built response: %d table(s) with data, %d NoData",
        data_count,
        nodata_count,
    )

    # Write the response to --out FIRST so it's persisted even when a
    # later step fails.
    try:
        write_response(response, out_path)
    except OSError as exc:
        raise ReportCompletionErrorCode.RC_4.exception(
            cause=exc, path=out_path, detail=str(exc)
        )
    logger.debug("Wrote response.yaml to %s", out_path)

    end = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    journal_path = Path(args.journal_file)
    journal = Journal.load(journal_path, strict=True)
    journal.end = end
    try:
        journal.save(journal_path)
    except OSError as exc:
        raise ReportCompletionErrorCode.RC_4.exception(
            cause=exc,
            path=journal_path,
            detail=str(exc),
        )

    logger.info("report-completion: recorded end=%s", end)
    notify_done(args, context=response.to_worker_output())
    logger.info("Reported completion to apiserver successfully")
    return 0
