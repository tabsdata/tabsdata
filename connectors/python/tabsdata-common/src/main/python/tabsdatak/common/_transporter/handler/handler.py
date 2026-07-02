#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import subprocess
import threading
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Sequence

from tabsdatak.common._interpreter.python.error import BinaryNotFoundError
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.common._transporter.invoker.transporter1 import (
    TransporterBinaryNotFoundError,
    TransporterRunError,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TransporterReport:
    files: list[dict]
    lastmod_info: str | None


class TransporterHandler(ABC):
    binary_name: str

    @abstractmethod
    def make_request(self, pairs: Sequence[tuple[Any, Any]]) -> Any: ...

    @abstractmethod
    def write_request(self, request: Any, path: Path) -> None: ...

    @abstractmethod
    def command(self, request_yaml: Path, response_yaml: Path) -> list[str]: ...

    def execute(
        self,
        request: Any,
        work_dir: Path,
        environment: Mapping[str, str] | None = None,
        extra_args: tuple[str, ...] = (),
    ) -> TransporterReport:
        raise NotImplementedError(f"{type(self).__name__} does not support execute().")

    def s3_location(
        self,
        uri: str,
        access_key_env: str,
        secret_key_env: str,
        region_env: str | None = None,
    ) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} does not build provider locations."
        )

    def gcs_location(self, uri: str, service_account_key_env: str) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} does not build provider locations."
        )

    def azure_location(
        self, uri: str, account_name_env: str, account_key_env: str
    ) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} does not build provider locations."
        )

    def local_location(self, uri: str) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} does not build provider locations."
        )

    def local_path_location(self, path: Path) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} does not build provider locations."
        )

    def local_uri(self, path: Path | str) -> str:
        raise NotImplementedError(
            f"{type(self).__name__} does not build provider locations."
        )

    def import_request(
        self,
        source: Any,
        target_dir: Path,
        file_format: Any,
        paths: Sequence[str],
        format_cfg: dict | None = None,
        initial_lastmod: str | None = None,
        lastmod_info: str | None = None,
        file_info: str | None = None,
    ) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} does not build connector requests."
        )

    def wildcard_request(
        self,
        source: Any,
        target_dir: Path,
        initial_lastmod: str | None = None,
        lastmod_info: str | None = None,
        max_files: int | None = None,
    ) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} does not build connector requests."
        )

    def copy_request(
        self,
        source_target_pairs: Sequence[tuple[Any, Any]],
        work_dir: Path,
        file_format: Any,
        paths: Sequence[str],
        format_cfg: dict | None = None,
    ) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} does not build connector requests."
        )

    def base_environment(self) -> Mapping[str, str]:
        return os.environ

    def resolve_binary(self) -> str:
        try:
            return str(PythonEnvironment().binary(self.binary_name))
        except BinaryNotFoundError as error:
            raise TransporterBinaryNotFoundError() from error

    def run(
        self,
        request_yaml: Path,
        response_yaml: Path,
        environment: Mapping[str, str] | None = None,
    ) -> int:
        binary = self.resolve_binary()
        response_yaml.parent.mkdir(parents=True, exist_ok=True)
        command = [binary, *self.command(request_yaml, response_yaml)]
        process_environment = dict(self.base_environment())
        if environment:
            process_environment.update(environment)
        return run_command(command, process_environment)

    def run_or_raise(
        self,
        request_yaml: Path,
        response_yaml: Path,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        return_code = self.run(request_yaml, response_yaml, environment)
        if return_code != 0:
            raise TransporterRunError(return_code, "", "")


def stream_reader(stream: IO[str], level: int) -> threading.Thread:
    def loop() -> None:
        try:
            for line in stream:
                logger.log(level, line.rstrip(), extra={"noformat": True})
        finally:
            stream.close()

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    return thread


def run_command(command: Sequence[str], environment: Mapping[str, str]) -> int:
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        text=True,
        env=dict(environment),
    )
    assert process.stdout is not None
    assert process.stderr is not None
    stdout_reader = stream_reader(process.stdout, logging.INFO)
    stderr_reader = stream_reader(process.stderr, logging.WARNING)
    return_code = process.wait()
    stdout_reader.join()
    stderr_reader.join()
    return return_code
