#
# Copyright 2026 Tabsdata Inc.
#

"""begin-execution command.

First step of the function-run pod chain (publisher / subscriber /
transformer). Initializes the shared ``Journal`` YAML with the
run's startup-phase data — today just the start time (unix-epoch
milliseconds, matching the apiserver callback envelope's ``start``
field), but the file is the agreed home for any additional startup-phase
information later steps want to expose to the rest of the chain.

After recording the start time it posts a ``Running`` status to the
responder so the apiserver learns the run has started. A ``Running``
post that cannot reach the responder fails the command, which the
orchestration then reports to the responder as ``Failed``.

Runs exactly once per pod, before ``fetch-request``. The shared volume
is overlaid on the image-template-provided ``shared/journal/``
folder, so the parent always exists; only the file itself needs to be
written.

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
from tabsdatak.server._tdrunner.function.commands.begin_execution.options import (
    BeginExecutionArguments,
)
from tabsdatak.server._tdrunner.function.commands.begin_execution.runner import run

__all__ = ["run", "main"]

VERSION = version()
GENERATION = 1
INFORMATION = (
    "begin-execution — initialize the journal with the run's startup-phase data"
)
MANUAL = (
    "begin-execution\n"
    "    --shared-folder <PATH>\n"
    "    --work-folder <PATH>\n"
    "    --logging-configuration <PATH>\n"
    "    --liveness <PATH>\n"
    "    --responder-url <URL>\n"
    "    --responder-method <METHOD>\n"
    "    --responder-authorization <AUTH>\n"
    "    --journal-file <PATH>\n\n"
    "First step of the function-run pod chain. Writes the shared "
    "Journal YAML with the run's startup-phase data (start time "
    "today; additional fields/steps may grow the schema in the future)."
)


def build_command_parser() -> ArgumentsParser:
    return BeginExecutionArguments.parser(
        program="begin-execution",
        description="Initialize the shared Journal YAML for the run.",
        version=VERSION,
        information=INFORMATION,
        manual=MANUAL,
    )


@command(name="td_begin_execution", parser=build_command_parser)
def main(arguments: BeginExecutionArguments) -> int:
    return run(arguments)


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
