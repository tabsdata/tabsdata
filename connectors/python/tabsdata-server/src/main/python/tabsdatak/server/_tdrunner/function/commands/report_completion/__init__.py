#
# Copyright 2026 Tabsdata Inc.
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

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

import argparse

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function.commands.report_completion.runner import run

__all__ = ["run", "main"]

VERSION = "2.0.2"
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
    return run(args)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
