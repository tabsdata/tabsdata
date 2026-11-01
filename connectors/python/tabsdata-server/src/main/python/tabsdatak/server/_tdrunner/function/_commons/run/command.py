#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import logging
import sys
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from tabsdatak._logging import setup_logging
from tabsdatak.common._enums import WorkerCallbackStatus
from tabsdatak.common._runtime.pathenv import extend_path
from tabsdatak.server._tdrunner.function._commons.document.journal import Journal
from tabsdatak.server._tdrunner.function._commons.document.message.callback import (
    EXIT_STATUS_SUCCESS,
    CallbackRequest,
    DestinationWriteMessage,
)
from tabsdatak.server._tdrunner.function._commons.error.command import (
    CommandErrorCode,
    CommandException,
    InconsistentError,
    PrematureError,
    StaleReportError,
    UnexpectedError,
)
from tabsdatak.server._tdrunner.function._commons.http.request import (
    DEFAULT_TIMEOUT_SECONDS,
)
from tabsdatak.server._tdrunner.function._commons.http.retry import call_with_retry
from tabsdatak.server._tdrunner.function._commons.monitor.liveness import (
    install_liveness_handler,
)
from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
    CommandArguments,
)
from tabsdatak.server._tdrunner.function._commons.run.dataflow import Dataflow
from tabsdatak.server._tdrunner.function._commons.solver.arguments import (
    resolve_value,
)

logger = logging.getLogger(__name__)

ANSWER_DATA_KEY = "data"
ANSWER_OUTCOME_KEY = "outcome"
STALE_OUTCOME = "Stale"

DESTINATION_WRITE_REPORT = "destination write"


def run_command(main: Callable[[], int | None]) -> int:
    try:
        result = main()
    except CommandException as error:
        print(str(error), file=sys.stderr)
        return error.exit_code
    if result is None:
        return 0
    return int(result)


class Responder:
    url: str
    method: str
    authorization: str
    journal_file: Path

    def __init__(self, arguments: CommandArguments) -> None:
        self.url = arguments.responder_url
        self.method = arguments.responder_method
        self.authorization = arguments.responder_authorization
        self.journal_file = arguments.journal_file

    def running(self) -> None:
        journal = Journal.load(
            self.journal_file,
            strict=True,
        )
        self.report(
            CallbackRequest(
                status=WorkerCallbackStatus.RUNNING,
                start=journal.start,
            )
        )

    def done(
        self,
        *,
        output: dict[str, Any] | None = None,
        exit_code: int = EXIT_STATUS_SUCCESS,
    ) -> None:
        journal = Journal.load(
            self.journal_file,
            strict=True,
        )
        logger.debug(f"Done report output: {output}")
        self.report(
            CallbackRequest(
                status=WorkerCallbackStatus.DONE,
                start=journal.start,
                end=journal.end,
                output=output,
                exit_status=exit_code,
            )
        )

    def failed(self, exit_code: int, error: BaseException) -> None:
        # noinspection PyBroadException
        try:
            journal = Journal.load(self.journal_file, strict=False)
            if journal.end is not None:
                end = journal.end
            else:
                end = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            if journal.start is not None:
                start = journal.start
            else:
                start = end
            self.report(
                CallbackRequest(
                    status=WorkerCallbackStatus.FAILED,
                    start=start,
                    end=end,
                    exit_status=exit_code,
                    error_message=repr(error),
                )
            )
        except StaleReportError:
            logger.info("The apiserver answered the Failed report as stale")
        except Exception:
            logger.warning("Failed to post Failed status to responder", exc_info=True)

    def destination_write(self) -> None:
        self.notify(
            DestinationWriteMessage().to_body(),
            DESTINATION_WRITE_REPORT,
        )

    def report(self, payload: CallbackRequest) -> None:
        status = payload.status
        logger.info(f"Reporting {status}: exit_status={payload.exit_status}")
        self.notify(payload.to_body(), str(status))

    def notify(self, body: dict[str, Any], what: str) -> None:
        url = resolve_value(self.url)
        method = resolve_value(self.method)
        authorization = resolve_value(self.authorization)
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            method=method,
            headers={
                "Authorization": authorization,
                "Content-Type": "application/json",
            },
        )
        logger.info(f"Posting responder {what}: {method} {url}")
        answer = call_with_retry(self.post, request)
        logger.info(f"Posted responder {what}: {method} {url}")
        if self.outcome(answer) == STALE_OUTCOME:
            raise CommandErrorCode.CMD_8.exception(status=what)

    @staticmethod
    def post(request: urllib.request.Request) -> bytes:
        try:
            with urllib.request.urlopen(
                request, timeout=DEFAULT_TIMEOUT_SECONDS
            ) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            error_body = error.read().decode("utf-8", errors="replace")
            error.msg = f"{error.msg}: {error_body}" if error_body else error.msg
            raise

    @staticmethod
    def outcome(answer: bytes) -> str | None:
        try:
            data = json.loads(answer).get(ANSWER_DATA_KEY)
        except (ValueError, AttributeError):
            return None
        return data.get(ANSWER_OUTCOME_KEY) if isinstance(data, dict) else None


class Command[Arguments: CommandArguments](ABC):
    arguments: Arguments
    responder: Responder

    def __init__(self, arguments: Arguments) -> None:
        self.arguments = arguments
        self.responder = Responder(arguments)

    @abstractmethod
    def run(self) -> int: ...


def show_dataflow_settings() -> None:
    settings = Dataflow.settings_in_effect()
    if not settings:
        logger.info("The query engine runs on its own memory and spill defaults")
        return
    for name, value in settings.items():
        logger.info(f"Query engine setting {name} is '{value}'")


def report_failure(
    arguments: CommandArguments | None,
    exit_code: int,
    error: BaseException,
) -> None:
    if arguments is None:
        logger.warning(
            "Skipping responder Failed POST: command arguments not available."
        )
        return
    Responder(arguments).failed(exit_code, error)


def tabsdata_command(*, name: str) -> Callable[[Callable], Callable]:
    def wrap(target: Callable) -> Callable:
        target.tabsdata_command = True
        target.tabsdata_command_name = name
        return target

    return wrap


def command(
    *,
    name: str,
    parser: Callable[[], ArgumentsParser],
    reports: bool = True,
) -> Callable[[Callable[[Any], int | None]], Callable[[], int]]:
    def wrap(
        body: Callable[[Any], int | None],
    ) -> Callable[[], int]:
        @tabsdata_command(name=name)
        def main() -> int:
            extend_path()
            if not reports:
                return run_command(lambda: body(parser().parse()))
            arguments: CommandArguments | None = None
            try:
                arguments = parser().parse()
                setup_logging(arguments.logging_configuration)
                show_dataflow_settings()
                install_liveness_handler(arguments.liveness)
                result = body(arguments)
                exit_code = 0 if result is None else int(result)
                return exit_code
            except StaleReportError as error:
                logger.info(f"Stopping without reporting Failed: {error}")
                return error.exit_code
            except CommandException as error:
                exit_code = error.exit_code
                logger.exception("Handled exception caught by command")
                if exit_code == 0:
                    logger.error(
                        f"CommandException raised with exit_code=0; "
                        f"promoting to {InconsistentError.exit_code}"
                    )
                    exit_code = InconsistentError.exit_code
                report_failure(arguments, exit_code, error)
                return exit_code
            except Exception as exception:
                exit_code = UnexpectedError.exit_code
                logger.exception("Unhandled exception caught by command")
                report_failure(arguments, exit_code, exception)
                return exit_code
            except SystemExit as exit_event:
                exit_code = PrematureError.exit_code
                logger.error(
                    f"Exit ({exit_event.code}) intercepted by command; "
                    f"promoting to {exit_code}"
                )
                report_failure(arguments, exit_code, exit_event)
                return exit_code
            except BaseException as exception:
                exit_code = UnexpectedError.exit_code
                logger.exception("Unhandled core exception caught by command")
                report_failure(arguments, exit_code, exception)
                return exit_code

        return main

    return wrap
