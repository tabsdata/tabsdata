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
from typing import Any, Callable, NoReturn

from tabsdatak._logging import setup_logging
from tabsdatak.common._enums import WorkerCallbackStatus
from tabsdatak.common._runtime.pathenv import extend_path
from tabsdatak.server._tdrunner.function._commons.http.request import (
    DEFAULT_TIMEOUT_SECONDS,
)
from tabsdatak.server._tdrunner.function._commons.http.retry import call_with_retry
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
    DestinationWriteMessage,
    ResponseMessagePayload,
)
from tabsdatak.server._tdrunner.function.commands import tabsdata_command

logger = logging.getLogger(__name__)

ANSWER_DATA_KEY = "data"
ANSWER_OUTCOME_KEY = "outcome"
STALE_OUTCOME = "Stale"

DESTINATION_WRITE_REPORT = "destination write"


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:  # type: ignore[override]
        raise CommandErrorCode.CMD_1.exception(detail=message)


def make_print_action(text: str) -> type[argparse.Action]:
    class PrintAction(argparse.Action):
        def __init__(self, option_strings, dest, **options):
            options["nargs"] = 0
            options.setdefault("default", argparse.SUPPRESS)
            super().__init__(option_strings, dest, **options)

        def __call__(self, parser, namespace, values, option_string=None):
            print(text)
            parser.exit(0)

    return PrintAction


def build_parser(
    program: str,
    description: str,
    *,
    version: str = "",
    info: str = "",
    manual: str = "",
) -> argparse.ArgumentParser:
    parser = Parser(prog=program, description=description)
    parser.add_argument(
        "--man",
        "-m",
        action=make_print_action(manual),
        help="Print the detailed user guide and exit.",
    )
    parser.add_argument(
        "--version",
        "-v",
        action=make_print_action(version),
        help="Print the command build version and exit.",
    )
    parser.add_argument(
        "--info",
        "-i",
        action=make_print_action(info),
        help="Print the command build info and exit.",
    )
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
    arguments: argparse.Namespace,
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
    arguments: argparse.Namespace,
    payload: ResponseMessagePayload,
) -> None:
    status = payload.status
    logger.info(f"Reporting {status}: exit_status={payload.exit_status}")
    notify(arguments, payload.to_body(), str(status))


def notify_status_running(arguments: argparse.Namespace) -> None:
    journal = Journal.load(
        Path(arguments.journal_file),
        strict=True,
    )
    notify_status(
        arguments,
        ResponseMessagePayload(
            status=WorkerCallbackStatus.RUNNING,
            start=journal.start,
        ),
    )


def notify_status_done(
    arguments: argparse.Namespace,
    *,
    context: Any | None = None,
    exit_code: int = EXIT_STATUS_SUCCESS,
) -> None:
    journal = Journal.load(
        Path(arguments.journal_file),
        strict=True,
    )
    logger.debug(f"notify_status_done context: {context}")
    notify_status(
        arguments,
        ResponseMessagePayload(
            status=WorkerCallbackStatus.DONE,
            start=journal.start,
            end=journal.end,
            context=context,
            exit_status=exit_code,
        ),
    )


def notify_status_failed(
    arguments: argparse.Namespace | None,
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
            ResponseMessagePayload(
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


def notify_step_destination_write(arguments: argparse.Namespace) -> None:
    notify(
        arguments,
        DestinationWriteMessage().to_body(),
        DESTINATION_WRITE_REPORT,
    )


def command(
    *,
    name: str,
    parser: Callable[[], argparse.ArgumentParser],
) -> Callable[[Callable[[argparse.Namespace], int | None]], Callable[[], int]]:
    def wrap(
        body: Callable[[argparse.Namespace], int | None],
    ) -> Callable[[], int]:
        @tabsdata_command(name=name)
        def main() -> int:
            extend_path()
            arguments: argparse.Namespace | None = None
            try:
                parsed = parser().parse_args()
                arguments = parsed
                setup_logging(parsed.log_config)
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
