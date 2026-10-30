#
# Copyright 2026 Tabsdata Inc.
#

"""begin-execution runner.

The command's logic, callable without argparse or the responder wrapper.
The package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies pre-built ``BeginExecutionArguments``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from tabsdatak.server._tdrunner.function._commons.run.command import (
    notify_status_running,
)
from tabsdatak.server._tdrunner.function._lib.journal import Journal
from tabsdatak.server._tdrunner.function.commands.begin_execution.error import (
    BeginExecutionErrorCode,
)
from tabsdatak.server._tdrunner.function.commands.begin_execution.options import (
    BeginExecutionArguments,
)

logger = logging.getLogger(__name__)


def run(arguments: BeginExecutionArguments) -> int:
    logger.info("Initializing Journal for the function run")
    start = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    journal_path = arguments.journal_file
    journal = Journal(start=start)
    logger.debug("Journal start=%s; writing to %s", start, journal_path)
    try:
        journal.save(journal_path)
    except OSError as exc:
        raise BeginExecutionErrorCode.BE_1.exception(
            cause=exc,
            path=journal_path,
            detail=str(exc),
        )
    logger.info("begin-execution: recorded start=%s", start)
    notify_status_running(arguments)
    return 0
