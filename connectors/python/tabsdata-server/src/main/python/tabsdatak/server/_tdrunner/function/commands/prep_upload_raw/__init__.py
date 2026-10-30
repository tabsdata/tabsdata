#
# Copyright 2026 Tabsdata Inc.
#

"""prep-upload-raw command.

Generates the transporter copy request that persists a publisher's
ingested data — the parquets `stamp-inputs` stamped with the framework's
system columns — from local disk to the run's landing-data location.

Reads:
  * `--request`:           the publisher request.yaml. `info.function_landing`
                           is the destination base.
  * `--inputs-manifest`:   the enriched manifest emitted by `stamp-inputs`,
                           supplying each stamped parquet and the input
                           index that names it in storage.

Writes a single `--transporter-request` YAML that `run-transporter` consumes.

The logic lives in :mod:`.runner`; this module is the thin ``@command``
entrypoint the console script resolves to.
"""

from __future__ import annotations

from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
)
from tabsdatak.server._tdrunner.function._commons.run.command import command
from tabsdatak.server._tdrunner.function._commons.run.version import version
from tabsdatak.server._tdrunner.function.commands.prep_upload_raw.arguments import (
    PrepUploadRawArguments,
)
from tabsdatak.server._tdrunner.function.commands.prep_upload_raw.runner import run

__all__ = ["run", "main"]

# TODO: After all commands are done, version will be unified in a single
#  source of truth file.
VERSION = version()
GENERATION = 1
INFORMATION = (
    "prep-upload-raw — generate the transporter request that persists a "
    "publisher's ingested data"
)
MANUAL = (
    "prep-upload-raw --request <PATH> --inputs-manifest <PATH> "
    "--transporter-request <PATH> --storage-configuration <PATH> "
    "--liveness <PATH>\n\n"
    "Reads the enriched inputs manifest written by stamp-inputs and "
    "builds the transporter copy request that uploads each stamped "
    "parquet to the run's landing-data location, named by its input index. "
    "Functions that ingest nothing produce an empty request."
)


def build_command_parser() -> ArgumentsParser:
    return PrepUploadRawArguments.parser(
        program="prep-upload-raw",
        description=("Generate the transporter request that persists ingested data."),
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_prep_upload_raw", parser=build_command_parser)
def main(arguments: PrepUploadRawArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
