#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import json
import logging
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from tabsdatak._logging import setup_logging
from tabsdatak.common._enums import WorkerCallbackStatus
from tabsdatak.common._runtime.pathenv import extend_path
from tabsdatak.server._tdrunner.function._commons.http.request import (
    DEFAULT_TIMEOUT_SECONDS,
)
from tabsdatak.server._tdrunner.function._commons.http.retry import call_with_retry
from tabsdatak.server._tdrunner.function._commons.run.arguments import (
    ArgumentsParser,
    CommandArguments,
    Parser,
    add_print_options,
)
from tabsdatak.server._tdrunner.function._commons.run.dataflow import Dataflow
from tabsdatak.server._tdrunner.function._commons.solver.arguments import (
    resolve_value,
)
from tabsdatak.server._tdrunner.function._lib.errors import (
    CommandErrorCode,
    CommandException,
    InconsistentError,
    PrematureError,
    StaleReportError,
    UnexpectedError,
)
from tabsdatak.server._tdrunner.function._lib.journal import Journal
from tabsdatak.server._tdrunner.function._lib.liveness import install_liveness_handler
from tabsdatak.server._tdrunner.function._lib.message import (
    EXIT_STATUS_SUCCESS,
    CallbackRequest,
    DestinationWriteMessage,
)

logger = logging.getLogger(__name__)

ANSWER_DATA_KEY = "data"
ANSWER_OUTCOME_KEY = "outcome"
STALE_OUTCOME = "Stale"

DESTINATION_WRITE_REPORT = "destination write"


def build_parser(
    program: str,
    description: str,
    *,
    version: str = "",
    info: str = "",
    manual: str = "",
) -> argparse.ArgumentParser:
    parser = Parser(prog=program, description=description)
    add_print_options(parser, version=version, information=info, manual=manual)
    parser.add_argument(
        "--shared-dir",
        "-s",
        default=None,
        metavar="PATH",
        help="Shared working dir (owned by the pod).",
    )
    parser.add_argument(
        "--work-dir",
        "-w",
        default=None,
        metavar="PATH",
        help="Private working dir (owned by the container).",
    )
    parser.add_argument(
        "--log-config",
        "-c",
        default=None,
        metavar="PATH",
        help=(
            "Path to a logging config YAML. When omitted, the bundled default is used."
        ),
    )
    parser.add_argument(
        "--liveness",
        "-l",
        required=True,
        metavar="PATH",
        help=(
            "File the command must touch at least once a minute to signal liveness to "
            "the framework."
        ),
    )
    parser.add_argument(
        "--responder-url",
        required=True,
        help="Responder endpoint URL.",
    )
    parser.add_argument(
        "--responder-method",
        required=True,
        help="Responder HTTP method.",
    )
    parser.add_argument(
        "--responder-authorization",
        required=True,
        help="Responder Authorization header value.",
    )
    parser.add_argument(
        "--journal-file",
        required=True,
        metavar="PATH",
        help=(
            "Path to the shared journal file every command in the chain reads/writes "
            "on the shared volume."
        ),
    )
    return parser


# Test entry point; production commands run through ``command``.
def run_command(main: Callable[[], int | None]) -> int:
    try:
        result = main()
    except CommandException as error:
        print(str(error), file=sys.stderr)
        return error.exit_code
    if result is None:
        return 0
    return int(result)


def notify(
    arguments: CommandArguments | argparse.Namespace,
    body: dict[str, Any],
    what: str,
) -> None:
    def post(notify_request: urllib.request.Request) -> bytes:
        try:
            with urllib.request.urlopen(
                notify_request, timeout=DEFAULT_TIMEOUT_SECONDS
            ) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            error_body = error.read().decode("utf-8", errors="replace")
            error.msg = f"{error.msg}: {error_body}" if error_body else error.msg
            raise

    url = resolve_value(arguments.responder_url)
    method = resolve_value(arguments.responder_method)
    authorization = resolve_value(arguments.responder_authorization)
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
    answer = call_with_retry(post, request)
    logger.info(f"Posted responder {what}: {method} {url}")
    if outcome(answer) == STALE_OUTCOME:
        raise CommandErrorCode.CMD_8.exception(status=what)


def outcome(answer: bytes) -> str | None:
    try:
        data = json.loads(answer).get(ANSWER_DATA_KEY)
    except (ValueError, AttributeError):
        return None
    return data.get(ANSWER_OUTCOME_KEY) if isinstance(data, dict) else None


def notify_status(
    arguments: CommandArguments | argparse.Namespace,
    payload: CallbackRequest,
) -> None:
    status = payload.status
    logger.info(f"Reporting {status}: exit_status={payload.exit_status}")
    notify(arguments, payload.to_body(), str(status))


def notify_status_running(arguments: CommandArguments | argparse.Namespace) -> None:
    journal = Journal.load(
        Path(arguments.journal_file),
        strict=True,
    )
    notify_status(
        arguments,
        CallbackRequest(
            status=WorkerCallbackStatus.RUNNING,
            start=journal.start,
        ),
    )


def notify_status_done(
    arguments: CommandArguments | argparse.Namespace,
    *,
    output: dict[str, Any] | None = None,
    exit_code: int = EXIT_STATUS_SUCCESS,
) -> None:
    journal = Journal.load(
        Path(arguments.journal_file),
        strict=True,
    )
    logger.debug(f"notify_status_done output: {output}")
    notify_status(
        arguments,
        CallbackRequest(
            status=WorkerCallbackStatus.DONE,
            start=journal.start,
            end=journal.end,
            output=output,
            exit_status=exit_code,
        ),
    )


def notify_status_failed(
    arguments: CommandArguments | argparse.Namespace | None,
    exit_code: int,
    error: BaseException,
) -> None:
    if arguments is None:
        logger.warning(
            "Skipping responder Failed POST: command arguments not available."
        )
        return
    # noinspection PyBroadException
    try:
        journal = Journal.load(Path(arguments.journal_file), strict=False)
        end = (
            journal.end
            if journal.end is not None
            else int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        )
        start = journal.start if journal.start is not None else end
        notify_status(
            arguments,
            CallbackRequest(
                status=WorkerCallbackStatus.FAILED,
                start=start,
                end=end,
                exit_status=exit_code,
                error_message=repr(error),
            ),
        )
    except StaleReportError:
        logger.info("The apiserver answered the Failed report as stale")
    except Exception:
        logger.warning("Failed to post Failed status to responder", exc_info=True)


def notify_step_destination_write(
    arguments: CommandArguments | argparse.Namespace,
) -> None:
    notify(
        arguments,
        DestinationWriteMessage().to_body(),
        DESTINATION_WRITE_REPORT,
    )


def show_dataflow_settings() -> None:
    settings = Dataflow.settings_in_effect()
    if not settings:
        logger.info("The query engine runs on its own memory and spill defaults")
        return
    for name, value in settings.items():
        logger.info(f"Query engine setting {name} is '{value}'")


def tabsdata_command(*, name: str) -> Callable[[Callable], Callable]:
    def wrap(target: Callable) -> Callable:
        target.tabsdata_command = True
        target.tabsdata_command_name = name
        return target

    return wrap


def parse(
    parser: argparse.ArgumentParser,
) -> tuple[CommandArguments | argparse.Namespace, Path | str | None]:
    if isinstance(parser, ArgumentsParser):
        arguments = parser.parse()
        return arguments, arguments.logging_configuration
    namespace = parser.parse_args()
    return namespace, namespace.log_config


def command(
    *,
    name: str,
    parser: Callable[[], argparse.ArgumentParser],
) -> Callable[[Callable[[Any], int | None]], Callable[[], int]]:
    def wrap(
        body: Callable[[Any], int | None],
    ) -> Callable[[], int]:
        @tabsdata_command(name=name)
        def main() -> int:
            extend_path()
            arguments: CommandArguments | argparse.Namespace | None = None
            try:
                parsed, logging_configuration = parse(parser())
                arguments = parsed
                setup_logging(logging_configuration)
                show_dataflow_settings()
                install_liveness_handler(Path(parsed.liveness))
                result = body(parsed)
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
                notify_status_failed(arguments, exit_code, error)
                return exit_code
            except Exception as exception:
                exit_code = UnexpectedError.exit_code
                logger.exception("Unhandled exception caught by command")
                notify_status_failed(arguments, exit_code, exception)
                return exit_code
            except SystemExit as exit_event:
                exit_code = PrematureError.exit_code
                logger.error(
                    f"Exit ({exit_event.code}) intercepted by command; "
                    f"promoting to {exit_code}"
                )
                notify_status_failed(arguments, exit_code, exit_event)
                return exit_code
            except BaseException as exception:
                exit_code = UnexpectedError.exit_code
                logger.exception("Unhandled core exception caught by command")
                notify_status_failed(arguments, exit_code, exception)
                return exit_code

        return main

    return wrap
