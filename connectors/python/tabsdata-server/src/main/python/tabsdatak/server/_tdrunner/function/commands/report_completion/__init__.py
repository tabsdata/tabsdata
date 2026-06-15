#
# Copyright 2026 Tabs Data Inc.
#

"""report-completion command.

Step 8 (final) of the publisher init-container chain. Builds the
response.yaml from the outputs manifest produced by `run-function`
(for `request.output`) and the `--state-dir` parquets (for
`request.system_output`), writes a copy to `--out`, stamps the run's
end time into the journal, and posts a `Done` callback to the responder
carrying the response as the `WorkerOutput` context.

For each declared `request.output` table the response includes a
`Data` entry with column_count / row_count / schema_hash if the
matching outputs-manifest entry carries a non-null `file:`, or a
`NoData` entry (table name only) when the entry's `file:` is `null`
(the user function returned `None` for that slot).

For each declared `request.system_output` table the response includes
a `Data` entry if `<state_dir>/<name>.parquet` exists, or `NoData`
otherwise.

A `request.output` table that has no entry in the outputs manifest is
treated as a config mismatch (the function definition declared fewer
outputs than the request expects) and surfaces `ManifestMismatchError`.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.command import (
    build_parser,
    command,
    notify_done,
)
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


VERSION = "0.1.0"
INFO = "report-completion — report the function-run response to the API server"
MAN = (
    "report-completion --request <PATH> --outputs-manifest <PATH> "
    "--state-dir <PATH> --out <PATH> --liveness <PATH> "
    "--journal-file <PATH> --responder-url <URL> "
    "--responder-method <METHOD> --responder-authorization <AUTH>\n\n"
    "Builds response.yaml from the outputs manifest (for request.output) "
    "and the --state-dir parquets (for request.system_output), writes a "
    "copy to --out, and posts a Done callback to the responder."
)


def _build_parser():
    parser = build_parser(
        prog="report-completion",
        description="Build and submit the function-run response.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--outputs-manifest",
        required=True,
        dest="outputs_manifest",
        help="Path to the outputs manifest YAML produced by run-function",
    )
    parser.add_argument(
        "--state-dir",
        required=True,
        dest="state_dir",
        help="Dir of produced state tables (parquet)",
    )
    parser.add_argument("--out", required=True, help="Path to write response.yaml")
    return parser


@command(name="td_report_completion", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
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


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
