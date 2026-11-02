#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from datetime import datetime, timezone

from tabsdatak.server._tdrunner.function._commons.document.journal import Journal
from tabsdatak.server._tdrunner.function._commons.run.command import Command
from tabsdatak.server._tdrunner.probe._handlers.connection.explorer import (
    ConnectionExplorerProbe,
    Outcome,
)
from tabsdatak.server._tdrunner.probe._handlers.connection.request import (
    ConnectionProbeRequest,
    InvalidRequestError,
)
from tabsdatak.server._tdrunner.probe.commands.connection.error import (
    ConnectionProbeErrorCode,
)
from tabsdatak.server._tdrunner.probe.commands.connection.options import (
    ConnectionProbeArguments,
)

logger = logging.getLogger(__name__)

MILLISECONDS_PER_SECOND = 1000


class ConnectionProbe(Command[ConnectionProbeArguments]):
    journal: Journal
    request: ConnectionProbeRequest
    outcome: Outcome

    def run(self) -> int:
        self.begin()
        self.load()
        self.probe()
        self.end()
        self.responder.done(output=self.outcome)
        return 0

    def begin(self) -> None:
        self.journal = Journal(start=now())
        self.journal.save(self.arguments.journal_file)

    def load(self) -> None:
        path = self.arguments.request
        try:
            self.request = ConnectionProbeRequest.load(path)
        except InvalidRequestError as error:
            raise ConnectionProbeErrorCode.CP_1.exception(
                cause=error,
                path=path,
                detail=error.detail,
            )

    def probe(self) -> None:
        self.outcome = ConnectionExplorerProbe(self.arguments.connection_file).run(
            self.request
        )
        logger.info(
            f"The {self.request.kind.value} probe {self.request.probe_id} "
            f"{self.outcome['status']}"
        )

    def end(self) -> None:
        self.journal.end = now()
        self.journal.save(self.arguments.journal_file)


def now() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * MILLISECONDS_PER_SECOND)
