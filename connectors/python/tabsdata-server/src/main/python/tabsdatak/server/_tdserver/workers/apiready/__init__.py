#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import argparse
import logging
import sys
import time
import urllib.error
import urllib.request

import yaml

from tabsdatak._logging import setup_logging
from tabsdatak.common._runtime.worker.error import ExitCode
from tabsdatak.common._runtime.worker.poll import PollWorker
from tabsdatak.server._tdserver.workers.apiready.error import ApiReadyErrorCode

logger = logging.getLogger(__name__)

ADDRESSES_KEY = "addresses"
WILDCARD_HOSTS = ("0.0.0.0", "", "::")
LOCAL_HOST = "localhost"
PING_URL_TEMPLATE = "http://{host}:{port}/api/v1/ping"

HTTP_OK = 200
REQUEST_TIMEOUT = 5.0

ARG_CONFIG_FILE = "--config-file"
ARG_LOG_CONFIG = "--log-config"
ARG_TIMEOUT = "--timeout"
ARG_INTERVAL = "--interval"

PROG = "td_apiready"
DESCRIPTION = "Wait until the apiserver becomes reachable."
CONFIG_FILE_HELP = "Path to an apiserver configuration file. Repeatable."
LOG_CONFIG_HELP = "Path to a logging configuration YAML for this worker."
TIMEOUT_HELP = "Maximum number of seconds to wait for the apiserver."
INTERVAL_HELP = "Number of seconds between readiness checks."
METAVAR_PATH = "PATH"


def ping_url(config_files: list[str]) -> str:
    merged: dict = {}
    for config_file in config_files:
        with open(config_file, encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
        merged.update(loaded)
    addresses = merged.get(ADDRESSES_KEY) or []
    if not addresses:
        raise ApiReadyErrorCode.API_READY_1.exception(key=ADDRESSES_KEY)
    host, _, port = str(addresses[0]).rpartition(":")
    if host in WILDCARD_HOSTS:
        host = LOCAL_HOST
    return PING_URL_TEMPLATE.format(host=host, port=port)


class ApiReady(PollWorker):
    def __init__(
        self,
        url: str,
        timeout: float,
        interval: float,
    ):
        super().__init__()
        self.url = url
        self.timeout = timeout
        self.interval = interval
        self.attempts = 0
        self.started = time.monotonic()

    def ready(self) -> bool:
        self.attempts += 1
        elapsed = time.monotonic() - self.started
        try:
            with urllib.request.urlopen(self.url, timeout=REQUEST_TIMEOUT) as response:
                if response.status == HTTP_OK:
                    return True
                logger.debug(
                    f"Attempt {self.attempts}: the apiserver returned status "
                    f"{response.status} at {self.url} [elapsed {elapsed:.1f}s]."
                )
                return False
        except (urllib.error.URLError, OSError) as urlerror:
            logger.debug(
                f"Attempt {self.attempts}: the apiserver is not ready at {self.url} "
                f"({urlerror}) [elapsed {elapsed:.1f}s]."
            )
            return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=PROG, description=DESCRIPTION)
    parser.add_argument(
        ARG_CONFIG_FILE,
        action="append",
        default=[],
        required=True,
        metavar=METAVAR_PATH,
        help=CONFIG_FILE_HELP,
    )
    parser.add_argument(
        ARG_LOG_CONFIG,
        required=True,
        metavar=METAVAR_PATH,
        help=LOG_CONFIG_HELP,
    )
    parser.add_argument(
        ARG_TIMEOUT,
        type=float,
        required=True,
        help=TIMEOUT_HELP,
    )
    parser.add_argument(
        ARG_INTERVAL,
        type=float,
        required=True,
        help=INTERVAL_HELP,
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    setup_logging(args.log_config)
    worker = ApiReady(
        url=ping_url(args.config_file),
        timeout=args.timeout,
        interval=args.interval,
    )
    logger.info(
        f"Polling the apiserver for readiness at {worker.url} "
        f"(timeout={args.timeout}s, interval={args.interval}s)."
    )
    code = worker.run()
    elapsed = time.monotonic() - worker.started
    if code == ExitCode.SUCCESS:
        logger.info(
            f"The apiserver is ready at {worker.url} after {worker.attempts} "
            f"attempts ({elapsed:.1f}s)."
        )
    elif code == ExitCode.ABORTED:
        logger.warning(
            f"Polling the apiserver for readiness at {worker.url} was aborted after "
            f"{worker.attempts} attempts ({elapsed:.1f}s)."
        )
    else:
        logger.warning(
            f"The apiserver was not ready at {worker.url} after {worker.attempts} "
            f"attempts ({elapsed:.1f}s)."
        )
    return code


if __name__ == "__main__":
    sys.exit(main())
