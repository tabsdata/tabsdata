#
# Copyright 2026 Tabs Data Inc.
#

"""begin-execution command.

First step of the function-run pod chain (publisher / subscriber /
transformer). Initializes the shared ``ExecutionMeta`` YAML with the
run's startup-phase data — today just the start time (unix-epoch
seconds, matching the apiserver callback envelope's ``start`` field),
but the file is the agreed home for any additional startup-phase
information later steps want to expose to the rest of the chain.

Runs exactly once per pod, before ``fetch-request``. The shared volume
is overlaid on the image-template-provided ``shared/execution_meta/``
dir, so the parent always exists; only the file itself needs to be
written.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path

from tabsdatak.server._tdrunner.function._lib.command import build_parser, command
from tabsdatak.server._tdrunner.function._lib.execution_meta import ExecutionMeta
from tabsdatak.server._tdrunner.function.commands.begin_execution.error import (
    BeginExecutionErrorCode,
)

logger = logging.getLogger(__name__)

VERSION = "2.0.0"
INFO = "begin-execution — initialize execution-meta with the run's startup-phase data"
MAN = (
    "begin-execution\n"
    "    --shared-dir <PATH>\n"
    "    --work-dir <PATH>\n"
    "    --log-config <PATH>\n"
    "    --liveness <PATH>\n"
    "    --responder-url <URL>\n"
    "    --responder-method <METHOD>\n"
    "    --responder-authorization <AUTH>\n"
    "    --execution-meta-file <PATH>\n\n"
    "First step of the function-run pod chain. Writes the shared "
    "ExecutionMeta YAML with the run's startup-phase data (start time "
    "today; additional fields/steps may grow the schema in the future)."
)


def _build_parser():
    return build_parser(
        prog="begin-execution",
        description="Initialize the shared ExecutionMeta YAML for the run.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )


@command(name="td_begin_execution", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    out_path = Path(args.execution_meta_file)
    logger.info("Initializing ExecutionMeta for the function run")
    # i64 unix epoch seconds — matches ResponseMessagePayload.start in
    # td-common/src/server.rs so the value can flow through to the
    # eventual report-completion POST body unchanged.
    start = int(datetime.now(tz=timezone.utc).timestamp())
    meta = ExecutionMeta(start=start)
    logger.debug("ExecutionMeta start=%s; writing to %s", start, out_path)
    try:
        meta.save(out_path)
    except OSError as exc:
        raise BeginExecutionErrorCode.BE_1.exception(
            cause=exc, path=out_path, detail=str(exc)
        )
    logger.info("Recorded ExecutionMeta start=%s", start)
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
