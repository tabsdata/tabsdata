#
# Copyright 2026 Tabsdata Inc.
#

"""report-completion command.

Step 8 (final) of the publisher init-container chain. Builds the
response.yaml from the outputs manifest produced by `function`,
writes a copy to `--response`, stamps the run's
end time into the journal, and posts a `Done` callback to the responder
carrying the response as the function `output`.

For each declared `request.output` table the response includes a
`Data` entry with the stats function read from the written table's
footer (column_count / row_count / schema_hash / data_bytes), or a
`NoData` entry (table name only) when the outputs-manifest entry has no
stats (the user function returned `None` for that slot).

For each declared `request.system_output` table the response includes
a `Data` entry with the stats of the state function wrote (the
manifest's `state` entry), or `NoData` when it wrote none.

A `request.output` table that has no entry in the outputs manifest is
treated as a config mismatch (the function definition declared fewer
outputs than the request expects) and surfaces `ManifestMismatchError`.

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to. The compound imports
:func:`.runner.run` directly instead of going through ``main``.
"""

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.report_completion.options import (
    ReportCompletionArguments,
)
from tabsdatak.server._tdrunner.function.commands.report_completion.runner import run

__all__ = ["run", "main"]

VERSION = version()
GENERATION = 1
INFORMATION = "report-completion — report the function-run response to the API server"
MANUAL = (
    "report-completion --request <PATH> --outputs-manifest <PATH> "
    "--response <PATH> --liveness <PATH> "
    "--journal-file <PATH> --responder-url <URL> "
    "--responder-method <METHOD> --responder-authorization <AUTH>\n\n"
    "Builds response.yaml from the outputs manifest (for request.output "
    "and request.system_output), writes a copy to --response, and posts a Done "
    "callback to the responder."
)


def build_command_parser() -> ArgumentsParser:
    return ReportCompletionArguments.parser(
        program="report-completion",
        description="Build and submit the function-run response.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_report_completion", parser=build_command_parser)
def main(arguments: ReportCompletionArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
