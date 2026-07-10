#
# Copyright 2026 Tabs Data Inc.
#

"""run-transporter runner.

The command's logic, callable without argparse or the responder. The
package ``__init__`` wraps :func:`run` in the ``@command`` entrypoint
for the console script; the compound imports :func:`run` directly and
supplies a pre-built ``Namespace``.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from tabsdatak.common._transporter.invoker.transporter1 import (
    TransporterBinaryNotFoundError,
)
from tabsdatak.server._tdrunner.function._lib.transporter import (
    run_transporter,
)
from tabsdatak.server._tdrunner.function.commands.run_transporter.error import (
    RunTransporterErrorCode,
)

logger = logging.getLogger(__name__)


def run(args: argparse.Namespace) -> int:
    request_yaml = Path(args.request)
    report = Path(args.report)
    logger.info("Running td_transporter")
    logger.debug("Request: %s  Report: %s", request_yaml, report)
    try:
        rc = run_transporter(request_yaml, report)
    except TransporterBinaryNotFoundError as exc:
        raise RunTransporterErrorCode.RUN_TR_1.exception(cause=exc, detail=str(exc))
    if rc == 0:
        logger.info("td_transporter completed successfully")
    else:
        logger.warning("td_transporter exited with non-zero code %d", rc)
    return rc
