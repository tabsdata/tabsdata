#
#  Copyright 2025 Tabs Data Inc.
#

import argparse
import logging
import os
import shutil
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import List

from tabsdata._tabsserver.function.global_utils import setup_logging

# noinspection PyProtectedMember
from tabsdata._utils.internal._process import TaskRunner

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger(__name__)


def delete_file(file: Path):
    file.unlink(missing_ok=True)
    logger.info(f"Deleted file: {file}")


def delete_folder(folder: Path):
    shutil.rmtree(folder)
    logger.info(f"Deleted folder: {folder}")


def path_age(path: Path, now_ms: int) -> int:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return 0
    return now_ms - int(stat.st_mtime * 1000)


def is_file_expired(path: Path, now_ms: int, retention_ms: int) -> bool:
    age = path_age(path, now_ms)
    if age > retention_ms:
        logger.debug(f"Path '{path}' age ({age}) exceeds retention ({retention_ms})")
        return True
    logger.debug(
        f"Path '{path}' age ({age}) does not exceed retention ({retention_ms})"
    )
    return False


def is_folder_expired(folder: Path, now_ms: int, retention_ms: int) -> bool:
    if not folder.exists():
        return False
    if not is_file_expired(folder, now_ms, retention_ms):
        return False
    if folder.is_dir():
        for child in folder.iterdir():
            if not is_folder_expired(child, now_ms, retention_ms):
                return False
    return True


def has_ok_file(folder: Path) -> bool:
    ok_file = folder / "work" / "ok"
    exists = ok_file.exists()
    if exists:
        logger.debug(f"Folder '{folder}' has 'ok' file at '{ok_file}'")
    else:
        logger.debug(f"Folder '{folder}' does not have 'ok' file at '{ok_file}'")
    return exists


def perform(
    instance: Path,
    folders: List[Path],
    folders_requiring_ok: List[Path],
    retention: int,
    limit: int,
):
    logger.info("Performing instance cleanup")
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    logger.info(
        f"Performing instance '{instance}' cleanup with current utc timestamp"
        f" '{now_ms}'"
    )

    deleted = 0
    for folder in folders:
        if deleted >= limit:
            break
        deleted = clean_folder(
            folder, now_ms, retention, limit, deleted, require_ok=False
        )

    for folder in folders_requiring_ok:
        if deleted >= limit:
            break
        deleted = clean_folder(
            folder, now_ms, retention, limit, deleted, require_ok=True
        )

    logger.info(f"Deleted {deleted} items (limit is {limit}).")
    logger.info("Instance cleanup iteration completed")


def clean_folder(
    folder: Path,
    now_ms: int,
    retention: int,
    limit: int,
    deleted: int,
    require_ok: bool = False,
) -> int:
    if not folder.exists():  # Folder is now living in Meinong's Jungle.
        logger.debug(f"Skipping folder '{folder}' because it does not exist.")
        return deleted

    logger.info(f"Evaluating folder '{folder}' for cleanup (require_ok={require_ok}).")

    for entry in folder.iterdir():
        if deleted >= limit:
            break

        if entry.is_dir():
            if require_ok and not has_ok_file(entry):
                logger.debug(
                    f"Folder '{entry}' not removed because it does not have work/ok"
                    " file."
                )
                continue

            if is_folder_expired(entry, now_ms, retention):
                delete_folder(entry)
                deleted += 1
            else:
                logger.debug(
                    f"Folder '{entry}' not removed because one or more children are not"
                    " yet expired."
                )
        else:
            if is_file_expired(entry, now_ms, retention):
                delete_file(entry)
                deleted += 1
            else:
                logger.debug(
                    f"File '{entry}' not removed because it is not yet expired."
                )

    return deleted


def process(
    instance: Path,
    folders: List[Path],
    folders_requiring_ok: List[Path],
    frequency: int,
    retention: int,
    limit: int,
):
    logger.info("Starting instance cleanup process")
    task = partial(
        perform,
        instance=instance,
        folders=folders,
        folders_requiring_ok=folders_requiring_ok,
        retention=retention,
        limit=limit,
    )
    task_runner = TaskRunner(task, frequency)
    task_runner.schedule()
    logger.info("Exiting instance cleanup process")


def valid_instance(instance: str) -> Path:
    instance_path = Path(instance)
    if not instance_path.is_dir():
        raise argparse.ArgumentTypeError(f"'{instance}' is not folder.")
    version_path = instance_path.joinpath(".version")
    if not version_path.exists():
        raise argparse.ArgumentTypeError(f"'{instance}' is not a valid instance.")
    return instance_path


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Periodically delete older function execution folders from an instance."
        )
    )
    parser.add_argument(
        "--instance",
        type=valid_instance,
        required=True,
        help="Path to the instance folder.",
    )
    parser.add_argument(
        "--frequency",
        type=int,
        required=True,
        help="Interval between cleanup runs.",
    )
    parser.add_argument(
        "--retention",
        type=int,
        required=True,
        help="Maximum age of folders to retain.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        required=True,
        help="Maximum number of files to clean in each run.",
    )
    parser.add_argument(
        "--logs-folder",
        type=str,
        required=True,
        help="Path of the folder where the logs of the janitor are stored.",
    )
    parser.add_argument(
        "--log-config-file",
        type=str,
        required=True,
        help="Path of the log configuration descriptor.",
    )
    arguments = parser.parse_args()
    complete_messages_folder = (
        arguments.instance.joinpath("workspace")
        .joinpath("work")
        .joinpath("msg")
        .joinpath("complete")
    )
    stop_messages_folder = (
        arguments.instance.joinpath("workspace")
        .joinpath("work")
        .joinpath("msg")
        .joinpath("ongoing")
        .joinpath("stop")
    )
    function_cast_folder = (
        arguments.instance.joinpath("workspace")
        .joinpath("work")
        .joinpath("proc")
        .joinpath("ephemeral")
        .joinpath("function")
        .joinpath("work")
        .joinpath("cast")
    )
    crontrigger_cast_folder = (
        arguments.instance.joinpath("workspace")
        .joinpath("work")
        .joinpath("proc")
        .joinpath("ephemeral")
        .joinpath("crontrigger")
        .joinpath("work")
        .joinpath("cast")
    )
    stagetrigger_cast_folder = (
        arguments.instance.joinpath("workspace")
        .joinpath("work")
        .joinpath("proc")
        .joinpath("ephemeral")
        .joinpath("stagetrigger")
        .joinpath("work")
        .joinpath("cast")
    )

    logs_folder = arguments.logs_folder
    log_config_file = arguments.log_config_file
    setup_logging(
        default_path=log_config_file,
        logs_folder=logs_folder,
    )
    global logger
    logger = logging.getLogger(__name__)

    folders = [
        complete_messages_folder,
        stop_messages_folder,
    ]

    folders_requiring_ok = [
        function_cast_folder,
        crontrigger_cast_folder,
        stagetrigger_cast_folder,
    ]

    process(
        arguments.instance,
        folders,
        folders_requiring_ok,
        arguments.frequency,
        arguments.retention,
        arguments.limit,
    )


if __name__ == "__main__":
    main()
