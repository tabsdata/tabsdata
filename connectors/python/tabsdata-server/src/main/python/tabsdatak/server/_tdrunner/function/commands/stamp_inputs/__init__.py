#
# Copyright 2026 Tabsdata Inc.
#

"""stamp-inputs command.

Follows `run-source-plugin` in the publisher chain. Reads the raw
parquet files it produced, stamps each one with the framework's system
columns (per-row UUID identifier plus execution/transaction/version/
timestamp metadata broadcast from the request envelope), and writes the
enriched parquets under `--enriched-folder`. Emits an updated
inputs-manifest at `--enriched-inputs-manifest`, which `prep-upload-raw`
uses to persist the stamped parquets and `run-function` to consume
them.

Legacy counterpart: `tabsdata._tabsserver.function.execution_utils.
store_source_raw_data` — the legacy `_tabsserver` ran this in-process
as part of the function-execution wrapper. We extract it into its own
runner step so the data-shape boundary between "plugin output" and
"framework-managed input to user function" is explicit.

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
from tabsdatak.server._tdrunner.function.commands.stamp_inputs.arguments import (
    StampInputsArguments,
)
from tabsdatak.server._tdrunner.function.commands.stamp_inputs.runner import (
    _stamp_file,
    run,
)

__all__ = ["_stamp_file", "run", "main"]

# TODO: After all commands are done, version will be unified in a single
#  source of truth file.
VERSION = version()
GENERATION = 1
INFORMATION = (
    "stamp-inputs — stamp framework system columns onto the parquets "
    "produced by run-source-plugin"
)
MANUAL = (
    "stamp-inputs --request <PATH> --inputs-manifest <PATH> "
    "--enriched-folder <PATH> --enriched-inputs-manifest <PATH> "
    "--liveness <PATH>\n\n"
    "Reads the raw inputs manifest, stamps each parquet with the "
    "framework's system columns ($td.id per row plus $td.ver.execution"
    " / .transaction / .version / .timestamp broadcast across rows), "
    "writes enriched parquets under --enriched-folder, and emits an "
    "updated inputs manifest at --enriched-inputs-manifest for "
    "run-function to consume."
)


def build_command_parser() -> ArgumentsParser:
    return StampInputsArguments.parser(
        program="stamp-inputs",
        description=(
            "Stamp framework system columns onto run-source-plugin's parquets."
        ),
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_stamp_inputs", parser=build_command_parser)
def main(arguments: StampInputsArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
