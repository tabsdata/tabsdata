#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import argparse
import logging
import sys
import time

from tabsdatak._logging import setup_logging
from tabsdatak.common._interpreter.python.binary import Binary, current_scripts_dir
from tabsdatak.common._runtime.worker.error import ExitCode, WorkerErrorCode
from tabsdatak.common._runtime.worker.poll import PollWorker

logger = logging.getLogger(__name__)

APISERVER_BINARY = "td_apiserver"

ARG_CONFIG_FILE = "--config-file"
ARG_DB_SCHEMA = "--db-schema"
DB_SCHEMA_PING = "ping"
ARG_LOG_CONFIG = "--log-config"

ARG_LOG_CONFIG_PY = "--log-config-py"
ARG_LOG_CONFIG_RS = "--log-config-rs"
ARG_TIMEOUT = "--timeout"
ARG_INTERVAL = "--interval"

PROG = "td_dbready"
DESCRIPTION = "Wait until the database is reachable."
CONFIG_FILE_HELP = "Path to a database configuration file. Repeatable."
LOG_CONFIG_PY_HELP = "Path to a logging configuration YAML for this worker."
LOG_CONFIG_RS_HELP = "Path to a logging configuration YAML forwarded to the apiserver."
TIMEOUT_HELP = "Maximum number of seconds to wait for the database."
INTERVAL_HELP = "Number of seconds between readiness checks."
METAVAR_PATH = "PATH"

BINARY_NOT_FOUND = "The {binary} binary was not found at {path}."


class DbReady(PollWorker):
    def __init__(
        self,
        config_files: list[str],
        log_config_rs: str,
        timeout: float,
        interval: float,
    ):
        super().__init__()
        self.config_files = config_files
        self.log_config_rs = log_config_rs
        self.timeout = timeout
        self.interval = interval
        self.attempts = 0
        self.started = time.monotonic()

    def ready(self) -> bool:
        self.attempts += 1
        binary = current_scripts_dir() / Binary(name=APISERVER_BINARY, prefix="").exe()
        if not binary.is_file():
            raise WorkerErrorCode.WORKER_1.exception(
                argv=[str(binary)],
                detail=BINARY_NOT_FOUND.format(binary=APISERVER_BINARY, path=binary),
            )
        argv = [str(binary)]
        for config_file in self.config_files:
            argv += [ARG_CONFIG_FILE, config_file]
        argv += [ARG_DB_SCHEMA, DB_SCHEMA_PING]
        argv += [ARG_LOG_CONFIG, self.log_config_rs]
        completed = self.submit(argv, timeout=self.timeout, check=False)
        if completed.returncode == 0:
            return True
        elapsed = time.monotonic() - self.started
        logger.debug(
            f"Attempt {self.attempts}: database not ready via {' '.join(argv)} "
            f"(status {completed.returncode}) [elapsed {elapsed:.1f}s]."
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
        ARG_LOG_CONFIG_PY,
        required=True,
        metavar=METAVAR_PATH,
        help=LOG_CONFIG_PY_HELP,
    )
    parser.add_argument(
        ARG_LOG_CONFIG_RS,
        required=True,
        metavar=METAVAR_PATH,
        help=LOG_CONFIG_RS_HELP,
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
    setup_logging(args.log_config_py)
    worker = DbReady(
        config_files=args.config_file,
        log_config_rs=args.log_config_rs,
        timeout=args.timeout,
        interval=args.interval,
    )
    logger.info(
        f"Polling database readiness via {APISERVER_BINARY} "
        f"(configs={args.config_file}, timeout={args.timeout}s, "
        f"interval={args.interval}s)."
    )
    code = worker.run()
    elapsed = time.monotonic() - worker.started
    if code == ExitCode.SUCCESS:
        logger.info(
            f"Database is ready after {worker.attempts} attempts ({elapsed:.1f}s)."
        )
    elif code == ExitCode.ABORTED:
        logger.warning(
            f"Database readiness polling was aborted after {worker.attempts} "
            f"attempts ({elapsed:.1f}s)."
        )
    else:
        logger.warning(
            f"Database was not ready after {worker.attempts} attempts ({elapsed:.1f}s)."
        )
    return code


if __name__ == "__main__":
    sys.exit(main())
