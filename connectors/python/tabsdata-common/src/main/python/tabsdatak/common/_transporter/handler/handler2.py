#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Sequence

import yaml

from tabsdatak.common._transporter.handler.handler import (
    TransporterHandler,
    TransporterReport,
)
from tabsdatak.common._transporter.invoker import transporter2

logger = logging.getLogger(__name__)

TRANSPORTER_BINARY_NAME = "td_transporter2"

Location = transporter2.Location


def local_location(path: Path) -> transporter2.Location:
    return transporter2.Location(uri=path.absolute().as_uri())


def _copied_file_to_dict(copied: transporter2.CopiedFile) -> dict:
    return {
        "from": copied.from_,
        "to": copied.to,
        "size": copied.size,
        "rows": copied.rows,
        "last_modified": copied.last_modified,
    }


def _found_file_to_dict(found: transporter2.FoundFile) -> dict:
    return {
        "to": found.uri,
        "size": found.size,
        "last_modified": found.last_modified,
    }


class TransporterHandler2(TransporterHandler):
    binary_name = TRANSPORTER_BINARY_NAME

    aws_access_key_id_env = transporter2.SERVER_SIDE_AWS_ACCESS_KEY_ID
    aws_secret_access_key_env = transporter2.SERVER_SIDE_AWS_SECRET_ACCESS_KEY
    aws_region_env = transporter2.SERVER_SIDE_AWS_REGION
    azure_account_name_env = transporter2.SERVER_SIDE_AZURE_ACCOUNT_NAME
    azure_account_key_env = transporter2.SERVER_SIDE_AZURE_ACCOUNT_KEY
    gcp_service_account_json_env = transporter2.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON

    def make_request(
        self, pairs: Sequence[tuple[transporter2.Location, transporter2.Location]]
    ) -> transporter2.CopyRequest:
        return transporter2.CopyRequest(
            pairs=[
                transporter2.CopyPair(from_=source, to=target)
                for source, target in pairs
            ]
        )

    def write_request(self, request: transporter2.CopyRequest, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            yaml.safe_dump(transporter2._dump(request), fh, sort_keys=False)

    def command(self, request_yaml: Path, response_yaml: Path) -> list[str]:
        return [
            transporter2._COPY,
            "--request",
            str(request_yaml),
            "--response",
            str(response_yaml),
        ]

    def execute(
        self,
        request: Any,
        work_dir: Path,
        environment: Mapping[str, str] | None = None,
        extra_args: tuple[str, ...] = (),
    ) -> TransporterReport:
        env = dict(environment) if environment else None
        binary = self.resolve_binary()
        alpha_suborder = "--alpha-suborder" in extra_args
        match request:
            case transporter2.FindCopyRequest():
                response = transporter2.find_copy(
                    request,
                    work_dir,
                    alpha_suborder=alpha_suborder,
                    env=env,
                    binary_path=binary,
                )
                files = [_copied_file_to_dict(copied) for copied in response.files]
                return TransporterReport(
                    files=files, lastmod_info=response.lastmod_info
                )
            case transporter2.CopyRequest():
                response = transporter2.copy(
                    request, work_dir, env=env, binary_path=binary
                )
                files = [_copied_file_to_dict(copied) for copied in response.files]
                return TransporterReport(
                    files=files, lastmod_info=response.lastmod_info
                )
            case transporter2.FindRequest():
                find_response = transporter2.find(
                    request,
                    work_dir,
                    alpha_suborder=alpha_suborder,
                    env=env,
                    binary_path=binary,
                )
                files = [_found_file_to_dict(found) for found in find_response.files]
                return TransporterReport(
                    files=files, lastmod_info=find_response.lastmod_info
                )
            case _:
                raise TypeError(
                    f"Unsupported transporter request type: {type(request)!r}."
                )

    def s3_location(
        self,
        uri: str,
        access_key_env: str,
        secret_key_env: str,
        region_env: str | None = None,
    ) -> transporter2.TransporterS3:
        return transporter2.TransporterS3(
            url=uri,
            access_key=transporter2.TransporterEnv(access_key_env),
            secret_key=transporter2.TransporterEnv(secret_key_env),
            region=(
                transporter2.TransporterEnv(region_env)
                if region_env is not None
                else None
            ),
        )

    def gcs_location(
        self, uri: str, service_account_key_env: str
    ) -> transporter2.TransporterGCS:
        return transporter2.TransporterGCS(
            url=uri,
            service_account_key=transporter2.TransporterEnv(service_account_key_env),
        )

    def azure_location(
        self, uri: str, account_name_env: str, account_key_env: str
    ) -> transporter2.TransporterAzure:
        return transporter2.TransporterAzure(
            url=uri,
            account_name=transporter2.TransporterEnv(account_name_env),
            account_key=transporter2.TransporterEnv(account_key_env),
        )

    def local_location(self, uri: str) -> transporter2.TransporterLocalFile:
        return transporter2.TransporterLocalFile(uri)

    def local_path_location(self, path: Path) -> transporter2.TransporterLocalFile:
        return transporter2.TransporterLocalFile(transporter2.path_to_file_uri(path))

    def local_uri(self, path: Path | str) -> str:
        return transporter2.path_to_file_uri(path)

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
    ) -> transporter2.FindCopyRequest:
        resolved = transporter2.resolve_auto_src_format(file_format, list(paths))
        format = transporter2.src_format_to_transporter(
            resolved, (format_cfg or {}).get(resolved)
        )
        return transporter2.FindCopyRequest(
            source=transporter2.to_location(source),
            target_dir=transporter2.Location(
                uri=transporter2.path_to_file_uri(target_dir)
            ),
            source_format=transporter2.to_format_spec(format),
            target_format=transporter2.FormatSpec(format="parquet"),
            initial_lastmod=initial_lastmod,
            lastmod_info=lastmod_info,
        )

    def wildcard_request(
        self,
        source: Any,
        target_dir: Path,
        initial_lastmod: str | None = None,
        lastmod_info: str | None = None,
        max_files: int | None = None,
    ) -> transporter2.FindCopyRequest:
        return transporter2.FindCopyRequest(
            source=transporter2.to_location(source),
            target_dir=transporter2.Location(
                uri=transporter2.path_to_file_uri(target_dir)
            ),
            max_files=max_files,
            initial_lastmod=initial_lastmod,
            lastmod_info=lastmod_info,
        )

    def copy_request(
        self,
        source_target_pairs: Sequence[tuple[Any, Any]],
        work_dir: Path,
        file_format: Any,
        paths: Sequence[str],
        format_cfg: dict | None = None,
    ) -> transporter2.CopyRequest:
        resolved = transporter2.resolve_auto_dest_format(file_format, list(paths))
        format = transporter2.dest_format_to_transporter(
            resolved, (format_cfg or {}).get(resolved)
        )
        pairs = [
            transporter2.CopyPair(
                from_=transporter2.to_location(source),
                to=transporter2.to_location(target),
            )
            for source, target in source_target_pairs
        ]
        return transporter2.CopyRequest(
            pairs=pairs,
            source_format=transporter2.FormatSpec(format="parquet"),
            target_format=transporter2.to_format_spec(format),
        )
