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

from tabsdatak.server._tdrunner.function._commons.document.request import Request
from tabsdatak.server._tdrunner.function._commons.document.response import Response
from tabsdatak.server._tdrunner.function._commons.manifests.manifest import (
    ManifestError,
)
from tabsdatak.server._tdrunner.function._commons.manifests.output import (
    OutputManifest,
    TableStats,
)
from tabsdatak.server._tdrunner.function._commons.run.command import notify_status_done
from tabsdatak.server._tdrunner.function._handlers.tables import TableFile
from tabsdatak.server._tdrunner.function._lib.journal import Journal
from tabsdatak.server._tdrunner.function.commands.report_completion.error import (
    ReportCompletionErrorCode,
)

logger = logging.getLogger(__name__)

PARQUET_SUFFIX = ".parquet"


def _state_stats(request: Request, state_dir: Path) -> dict[str, TableStats]:
    stats = {}
    for table in request.system_output:
        path = state_dir / f"{table.table}{PARQUET_SUFFIX}"
        if path.exists():
            stats[table.table] = TableFile.local(path).stats()
    return stats


def run(args: argparse.Namespace) -> int:
    request_path = Path(args.request)
    outputs_manifest_path = Path(args.outputs_manifest)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)

    logger.info("Building and submitting function-run response")
    request = Request.load(request_path, ReportCompletionErrorCode.RC_1)
    try:
        manifest = OutputManifest.load(outputs_manifest_path)
    except ManifestError as error:
        raise ReportCompletionErrorCode.RC_2.exception(
            cause=error, path=outputs_manifest_path, detail=str(error)
        )
    logger.debug(
        "Outputs manifest has %d entr%s: %s; request declares %d output "
        "table(s): %s + %d system_output table(s): %s",
        len(manifest.outputs),
        "y" if len(manifest.outputs) == 1 else "ies",
        manifest.outputs,
        len(request.output),
        request.output,
        len(request.system_output),
        request.system_output,
    )

    response = Response.build(
        request,
        manifest,
        _state_stats(request, state_dir),
        ReportCompletionErrorCode.RC_3,
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
        response.write(out_path)
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
    notify_status_done(args, output=response.function_output().document())
    logger.info("Reported completion to apiserver successfully")
    return 0
