#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import subprocess
import threading
from collections.abc import Callable
from pathlib import Path
from typing import IO, Any, Mapping, Sequence, Union
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, ConfigDict, Field

from tabsdatak.common._interpreter.python.error import BinaryNotFoundError
from tabsdatak.common._interpreter.python.venv import PythonEnvironment
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    AnyRequest,
    InputTable,
    InputTableVersions,
    Location,
    Request,
    input_tables,
)

logger = logging.getLogger(__name__)

TRANSPORTER_BINARY_NAME = "td_transporter"

SECRETS_MOUNT_OPTION_PREFIX = "SECRETS__STORAGE__MOUNTS__"
OPTIONS_INFIX = "__OPTIONS__"

OPTION_KEY_TO_TRANSPORTER_FIELD = {
    "AWS_ACCESS_KEY_ID": "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY": "AWS_SECRET_ACCESS_KEY",
    "AWS_REGION": "AWS_REGION",
    "AZURE_STORAGE_ACCOUNT_NAME": "AZURE_ACCOUNT_NAME",
    "AZURE_STORAGE_ACCOUNT_KEY": "AZURE_ACCOUNT_KEY",
    "GOOGLE_SERVICE_ACCOUNT_KEY": "GCP_SERVICE_ACCOUNT_KEY",
}


class EnvRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str


class LiteralRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    value: str

    def __repr_args__(self):
        return [("value", "***redacted***")]


CredRef = Union[EnvRef, LiteralRef]


class S3Configs(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    access_key: CredRef | None = None
    secret_key: CredRef | None = None
    region: CredRef | None = None
    extra_configs: dict[str, str] = Field(default_factory=dict)


class S3Endpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str
    configs: S3Configs = Field(default_factory=S3Configs)


class AzureConfigs(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    account_name: CredRef | None = None
    account_key: CredRef | None = None
    extra_configs: dict[str, str] = Field(default_factory=dict)


class AzureEndpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str
    configs: AzureConfigs = Field(default_factory=AzureConfigs)


class GcsConfigs(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    service_account_key: CredRef | None = None
    extra_configs: dict[str, str] = Field(default_factory=dict)


class GcsEndpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str
    configs: GcsConfigs = Field(default_factory=GcsConfigs)


class LocalFileEndpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str


Endpoint = Union[S3Endpoint, AzureEndpoint, GcsEndpoint, LocalFileEndpoint]


class UnsupportedSchemeError(ValueError):
    pass


def env_ref(prefix: str | None, name: str) -> EnvRef:
    return EnvRef(name=f"{prefix}_{name}" if prefix else name)


def build_s3(location: Location) -> Endpoint:
    prefix = location.env_prefix
    return S3Endpoint(
        url=location.uri,
        configs=S3Configs(
            access_key=env_ref(prefix, "AWS_ACCESS_KEY_ID"),
            secret_key=env_ref(prefix, "AWS_SECRET_ACCESS_KEY"),
            region=env_ref(prefix, "AWS_REGION"),
        ),
    )


def build_azure(location: Location) -> Endpoint:
    prefix = location.env_prefix
    return AzureEndpoint(
        url=location.uri,
        configs=AzureConfigs(
            account_name=env_ref(prefix, "AZURE_ACCOUNT_NAME"),
            account_key=env_ref(prefix, "AZURE_ACCOUNT_KEY"),
        ),
    )


def build_gcs(location: Location) -> Endpoint:
    prefix = location.env_prefix
    return GcsEndpoint(
        url=location.uri,
        configs=GcsConfigs(
            service_account_key=env_ref(prefix, "GCP_SERVICE_ACCOUNT_KEY"),
        ),
    )


def build_local(location: Location) -> Endpoint:
    return LocalFileEndpoint(url=location.uri)


BUILDERS: dict[str, Callable[[Location], Endpoint]] = {
    "s3": build_s3,
    "az": build_azure,
    "abfs": build_azure,
    "abfss": build_azure,
    "gs": build_gcs,
    "file": build_local,
}


def endpoint_from_location(location: Location) -> Endpoint:
    scheme = urlparse(location.uri).scheme
    builder = BUILDERS.get(scheme)
    if builder is None:
        raise UnsupportedSchemeError(f"Unsupported URI scheme: {location.uri!r}.")
    return builder(location)


class OutputTableNotInManifestError(ValueError):
    pass


class CopyPair(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: Endpoint
    target: Endpoint


class TransporterCopyRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    pairs: tuple[CopyPair, ...]
    parallelism: int | None = None


def localized_input_files(
    in_dir: Path, entry: InputTable | InputTableVersions
) -> list[tuple[InputTable, Path]]:
    if isinstance(entry, InputTableVersions):
        return [
            (member, in_dir / f"{member.name}.{member.version_pos}.parquet")
            for member in input_tables(entry)
        ]
    return [(entry, in_dir / f"{entry.name}.parquet")]


def build_localize_state_request(
    request: Request, in_dir: Path, state_dir: Path
) -> TransporterCopyRequest:
    pairs: list[CopyPair] = []
    for entry in request.input:
        for member, path in localized_input_files(in_dir, entry):
            if member.location is None:
                continue
            pairs.append(
                CopyPair(
                    source=endpoint_from_location(member.location),
                    target=LocalFileEndpoint(url=local_uri(path)),
                )
            )
    for table in request.system_input:
        if table.location is None:
            continue
        pairs.append(
            CopyPair(
                source=endpoint_from_location(table.location),
                target=LocalFileEndpoint(
                    url=local_uri(state_dir / f"{table.name}.parquet")
                ),
            )
        )
    return TransporterCopyRequest(pairs=tuple(pairs))


def build_bundle_fetch_request(
    request: Request | AnyRequest, target_path: Path
) -> TransporterCopyRequest:
    return TransporterCopyRequest(
        pairs=(
            CopyPair(
                source=endpoint_from_location(request.info.function_bundle),
                target=LocalFileEndpoint(url=local_uri(target_path)),
            ),
        )
    )


def build_upload_state_request(
    request: Request, state_dir: Path
) -> TransporterCopyRequest:
    pairs: list[CopyPair] = []
    for table in request.system_output:
        if table.location is None:
            continue
        source_path = state_dir / f"{table.name}.parquet"
        if not source_path.exists():
            continue
        pairs.append(
            CopyPair(
                source=LocalFileEndpoint(url=local_uri(source_path)),
                target=endpoint_from_location(table.location),
            )
        )
    return TransporterCopyRequest(pairs=tuple(pairs))


def build_upload_tables_request(
    request: Request, manifest_entries: list[dict[str, Any]]
) -> TransporterCopyRequest:
    files_by_name: dict[str, str | None] = {
        entry[MANIFEST_NAME_KEY]: entry.get(MANIFEST_FILE_KEY)
        for entry in manifest_entries
    }
    pairs: list[CopyPair] = []
    for table in request.output:
        if table.location is None:
            continue
        if table.name not in files_by_name:
            raise OutputTableNotInManifestError(
                f"Output table {table.name!r} declared in request "
                f"has no entry in the outputs manifest."
            )
        source = files_by_name[table.name]
        if source is None:
            continue
        pairs.append(
            CopyPair(
                source=LocalFileEndpoint(url=local_uri(Path(source))),
                target=endpoint_from_location(table.location),
            )
        )
    return TransporterCopyRequest(pairs=tuple(pairs))


def local_uri(path: Path) -> str:
    return path.absolute().as_uri()


def write_transporter_request(request: TransporterCopyRequest, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        yaml.dump(request, fh, Dumper=make_dumper(), sort_keys=False)


def dump_transporter_request(request: TransporterCopyRequest) -> str:
    return yaml.dump(request, Dumper=make_dumper(), sort_keys=False)


def make_dumper() -> type[yaml.SafeDumper]:
    class TransporterDumper(yaml.SafeDumper):
        pass

    TransporterDumper.add_representer(EnvRef, env_repr)
    TransporterDumper.add_representer(LiteralRef, literal_repr)
    TransporterDumper.add_representer(S3Endpoint, s3_repr)
    TransporterDumper.add_representer(AzureEndpoint, azure_repr)
    TransporterDumper.add_representer(GcsEndpoint, gcs_repr)
    TransporterDumper.add_representer(LocalFileEndpoint, local_repr)
    TransporterDumper.add_representer(TransporterCopyRequest, copy_repr)
    return TransporterDumper


def env_repr(dumper: yaml.SafeDumper, ref: EnvRef) -> yaml.nodes.ScalarNode:
    return dumper.represent_scalar("!Env", ref.name)


def literal_repr(dumper: yaml.SafeDumper, ref: LiteralRef) -> yaml.nodes.ScalarNode:
    return dumper.represent_scalar("!Literal", ref.value)


def s3_repr(dumper: yaml.SafeDumper, endpoint: S3Endpoint) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!S3",
        {
            "url": endpoint.url,
            "configs": {
                "access_key": endpoint.configs.access_key,
                "secret_key": endpoint.configs.secret_key,
                "region": endpoint.configs.region,
                "extra_configs": dict(endpoint.configs.extra_configs),
            },
        },
    )


def azure_repr(
    dumper: yaml.SafeDumper, endpoint: AzureEndpoint
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!Azure",
        {
            "url": endpoint.url,
            "configs": {
                "account_name": endpoint.configs.account_name,
                "account_key": endpoint.configs.account_key,
                "extra_configs": dict(endpoint.configs.extra_configs),
            },
        },
    )


def gcs_repr(dumper: yaml.SafeDumper, endpoint: GcsEndpoint) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!GCS",
        {
            "url": endpoint.url,
            "configs": {
                "service_account_key": endpoint.configs.service_account_key,
                "extra_configs": dict(endpoint.configs.extra_configs),
            },
        },
    )


def local_repr(
    dumper: yaml.SafeDumper, endpoint: LocalFileEndpoint
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping("!LocalFile", {"url": endpoint.url})


def copy_repr(
    dumper: yaml.SafeDumper, request: TransporterCopyRequest
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!CopyV1",
        {
            "source_target_pairs": [
                [pair.source, pair.target] for pair in request.pairs
            ],
            "parallelism": request.parallelism,
        },
    )


def transporter_environment(source: Mapping[str, str]) -> dict[str, str]:
    environment = dict(source)
    for name, value in source.items():
        if not name.startswith(SECRETS_MOUNT_OPTION_PREFIX):
            continue
        remainder = name[len(SECRETS_MOUNT_OPTION_PREFIX) :]
        separator = remainder.find(OPTIONS_INFIX)
        if separator == -1:
            continue
        mount_id = remainder[:separator]
        option_key = remainder[separator + len(OPTIONS_INFIX) :]
        transporter_field = OPTION_KEY_TO_TRANSPORTER_FIELD.get(option_key)
        if transporter_field is None:
            logger.warning(
                f"No transporter field mapping for storage option '{option_key}' "
                f"on mount '{mount_id}'; leaving it unmapped."
            )
            continue
        environment.setdefault(f"{mount_id}_{transporter_field}", value)
    return environment


class TransporterNotFoundError(FileNotFoundError):
    pass


def run_transporter(request_yaml: Path, report: Path) -> int:
    try:
        binary = PythonEnvironment().binary(TRANSPORTER_BINARY_NAME)
    except BinaryNotFoundError as error:
        raise TransporterNotFoundError() from error
    report.parent.mkdir(parents=True, exist_ok=True)
    command = [
        str(binary),
        "--request",
        str(request_yaml),
        "--report",
        str(report),
    ]
    return run_command(command)


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


def run_command(command: Sequence[str]) -> int:
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        text=True,
        env=transporter_environment(os.environ),
    )

    assert process.stdout is not None
    assert process.stderr is not None
    stdout_reader = stream_reader(process.stdout, logging.INFO)
    stderr_reader = stream_reader(process.stderr, logging.WARNING)

    return_code = process.wait()
    stdout_reader.join()
    stderr_reader.join()
    return return_code
