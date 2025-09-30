#
#  Copyright 2025 Tabs Data Inc.
#

import argparse
import logging
import os
import re
import shutil
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

from tabsdata._tabsserver.function.global_utils import setup_logging
from tabsdata._utils.id import decode_id

# noinspection PyProtectedMember
from tabsdata._utils.internal._process import TaskRunner

ABSOLUTE_LOCATION = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger(__name__)


def delete_file(file: Path):
    file.unlink(missing_ok=True)
    logger.info(f"Deleted file: {file}")


def delete_folders(folder_b: Path, uuid: str, i: int):
    folder_pattern = re.compile(rf"^{re.escape(uuid)}_(\d+)$")
    for folder in folder_b.iterdir():
        if folder.is_dir():
            match = folder_pattern.match(folder.name)
            if match:
                j = int(match.group(1))
                if j <= i:
                    shutil.rmtree(folder)
                    logger.info(f"Deleted folder: {folder}")


def get_files(complete_messages_folder: Path):
    file_pattern = re.compile(r"^([a-zA-Z0-9]+)_(\d+)\.yaml$")
    files = []
    for file in complete_messages_folder.iterdir():
        if file.is_file():
            match = file_pattern.match(file.name)
            if match:
                code, i = match.group(1), int(match.group(2))
                files.append((file, code, i))
    return sorted(files, key=lambda x: (x[0], x[1]))


def perform(
    instance: Path,
    complete_messages_folder: Path,
    function_cast_folder: Path,
    retention: int,
    limit: int,
):
    logger.info("Performing instance cleanup")
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    logger.info(f"Performing {instance} cleanup with current utc timestamp {now}")
    files = get_files(complete_messages_folder)

    deleted = 0
    for file, id_code, i in files:
        if deleted >= limit:
            break
        try:
            id_uuid, id_timestamp, id_datetime = decode_id(id_code)
        except Exception as e:
            logger.error(f"Failed to decode code '{id_code}': {e}")
            continue
        age = now - id_timestamp
        if age > retention:
            logger.info(
                f"File {file} will be deleted as its age"
                f" {age} ({id_datetime.isoformat(timespec='milliseconds')}) is greater"
                f" than threshold {retention}"
            )
            delete_folders(function_cast_folder, id_code, i)
            delete_file(file)
            deleted += 1
        else:
            logger.info(
                f"File {file} kept as its age"
                f" {age} ({id_datetime.isoformat(timespec='milliseconds')}) is not"
                f" greater than threshold {retention}"
            )
    logger.info(f"Deleted {deleted} files (limit is {limit}).")
    logger.info("Instance cleanup iteration completed")


def process(
    instance: Path,
    complete_messages_folder: Path,
    function_cast_folder: Path,
    frequency: int,
    retention: int,
    limit: int,
):
    logger.info("Starting instance cleanup process")
    task = partial(
        perform,
        instance=instance,
        complete_messages_folder=complete_messages_folder,
        function_cast_folder=function_cast_folder,
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
        help="Maximum numbers of files to clean in each run.",
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
    function_cast_folder = (
        arguments.instance.joinpath("workspace")
        .joinpath("work")
        .joinpath("proc")
        .joinpath("ephemeral")
        .joinpath("function")
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

    process(
        arguments.instance,
        complete_messages_folder,
        function_cast_folder,
        arguments.frequency,
        arguments.retention,
        arguments.limit,
    )


if __name__ == "__main__":
    main()
