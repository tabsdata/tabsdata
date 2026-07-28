#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Sequence, Union
from urllib.parse import urlparse
from urllib.request import url2pathname

import yaml
from pydantic import BaseModel, ConfigDict, Field

from tabsdatak.common._transporter.handler.handler import (
    TransporterHandler,
    TransporterReport,
)
from tabsdatak.common._transporter.invoker import transporter1

logger = logging.getLogger(__name__)

TRANSPORTER_BINARY_NAME = "td_transporter1"

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


class GCSConfigs(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    service_account_key: CredRef | None = None
    extra_configs: dict[str, str] = Field(default_factory=dict)


class GCSEndpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str
    configs: GCSConfigs = Field(default_factory=GCSConfigs)


class LocalFileEndpoint(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    url: str


Endpoint = Union[S3Endpoint, AzureEndpoint, GCSEndpoint, LocalFileEndpoint]


class UnsupportedSchemeError(ValueError):
    pass


class CopyPair(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    source: Endpoint
    target: Endpoint


class TransporterCopyRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    pairs: tuple[CopyPair, ...]
    parallelism: int | None = None


def env_ref(prefix: str | None, name: str) -> EnvRef:
    return EnvRef(name=f"{prefix}_{name}" if prefix else name)


def build_s3(uri: str, prefix: str | None) -> Endpoint:
    return S3Endpoint(
        url=uri,
        configs=S3Configs(
            access_key=env_ref(prefix, "AWS_ACCESS_KEY_ID"),
            secret_key=env_ref(prefix, "AWS_SECRET_ACCESS_KEY"),
            region=env_ref(prefix, "AWS_REGION"),
        ),
    )


def build_azure(uri: str, prefix: str | None) -> Endpoint:
    return AzureEndpoint(
        url=uri,
        configs=AzureConfigs(
            account_name=env_ref(prefix, "AZURE_ACCOUNT_NAME"),
            account_key=env_ref(prefix, "AZURE_ACCOUNT_KEY"),
        ),
    )


def build_gcs(uri: str, prefix: str | None) -> Endpoint:
    return GCSEndpoint(
        url=uri,
        configs=GCSConfigs(
            service_account_key=env_ref(prefix, "GCP_SERVICE_ACCOUNT_KEY"),
        ),
    )


def build_local(uri: str, prefix: str | None) -> Endpoint:
    return LocalFileEndpoint(url=uri)


BUILDERS: dict[str, Callable[[str, str | None], Endpoint]] = {
    "s3": build_s3,
    "az": build_azure,
    "abfs": build_azure,
    "abfss": build_azure,
    "gs": build_gcs,
    "file": build_local,
}


def endpoint_from_location(uri: str, env_prefix: str | None) -> Endpoint:
    scheme = urlparse(uri).scheme
    builder = BUILDERS.get(scheme)
    if builder is None:
        raise UnsupportedSchemeError(f"Unsupported URI scheme: {uri!r}.")
    return builder(uri, env_prefix)


def local_endpoint(path: Path) -> LocalFileEndpoint:
    return LocalFileEndpoint(url=local_uri(path))


def local_uri(path: Path) -> str:
    return path.absolute().as_uri()


def make_dumper() -> type[yaml.SafeDumper]:
    class TransporterDumper(yaml.SafeDumper):
        pass

    TransporterDumper.add_representer(EnvRef, env_repr)
    TransporterDumper.add_representer(LiteralRef, literal_repr)
    TransporterDumper.add_representer(S3Endpoint, s3_repr)
    TransporterDumper.add_representer(AzureEndpoint, azure_repr)
    TransporterDumper.add_representer(GCSEndpoint, gcs_repr)
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


def gcs_repr(dumper: yaml.SafeDumper, endpoint: GCSEndpoint) -> yaml.nodes.MappingNode:
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


CSV_SINK_KEYS: frozenset[str] = frozenset({
    "include_header",
    "separator",
    "line_terminator",
    "quote_char",
    "batch_size",
    "datetime_format",
    "date_format",
    "time_format",
    "float_scientific",
    "float_precision",
    "null_value",
    "quote_style",
})


def csv_sink_kwargs(options: dict[str, Any]) -> dict[str, Any]:
    sink_kwargs: dict[str, Any] = {}
    for key, value in options.items():
        if key == "eol_char":
            sink_kwargs["line_terminator"] = value
        elif key in CSV_SINK_KEYS:
            sink_kwargs[key] = value
    return sink_kwargs


def file_uri_to_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"expected file:// URL, got {uri!r}.")
    # `url2pathname` is OS-aware: on Windows it turns `/C:/a/b` into the
    # native `C:\a\b`, on POSIX it leaves `/a/b`. Using `urlparse().path`
    # directly would keep the leading slash before a drive letter.
    return Path(url2pathname(parsed.path))


def transcode_to_local(
    source: transporter1.TransporterLocalFile,
    work_dir: Path,
    format: transporter1.TransporterFormat,
) -> transporter1.TransporterLocalFile:
    import polars

    source_path = file_uri_to_path(source.url)
    out_dir = work_dir / "_transcoded"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / uuid.uuid4().hex

    if isinstance(format, transporter1.TransporterCSVFormat):
        polars.scan_parquet(source_path).sink_csv(
            out_path, **csv_sink_kwargs(format.options)
        )
    elif isinstance(format, transporter1.TransporterJsonFormat):
        polars.scan_parquet(source_path).sink_ndjson(out_path)
    elif isinstance(format, transporter1.TransporterAvroFormat):
        polars.read_parquet(source_path).write_avro(out_path)
    elif isinstance(format, transporter1.TransporterParquetFormat):
        raise AssertionError("parquet format does not need transcoding")
    else:
        raise ValueError(f"unsupported transcode format: {type(format).__name__}.")

    return transporter1.TransporterLocalFile(transporter1.path_to_file_uri(out_path))


class TransporterHandler1(TransporterHandler):
    binary_name = TRANSPORTER_BINARY_NAME

    aws_access_key_id_env = transporter1.SERVER_SIDE_AWS_ACCESS_KEY_ID
    aws_secret_access_key_env = transporter1.SERVER_SIDE_AWS_SECRET_ACCESS_KEY
    aws_region_env = transporter1.SERVER_SIDE_AWS_REGION
    azure_account_name_env = transporter1.SERVER_SIDE_AZURE_ACCOUNT_NAME
    azure_account_key_env = transporter1.SERVER_SIDE_AZURE_ACCOUNT_KEY
    gcp_service_account_json_env = transporter1.SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON

    def make_request(
        self, pairs: Sequence[tuple[Endpoint, Endpoint]]
    ) -> TransporterCopyRequest:
        return TransporterCopyRequest(
            pairs=tuple(
                CopyPair(source=source, target=target) for source, target in pairs
            )
        )

    def write_request(self, request: Any, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        match request:
            case TransporterCopyRequest():
                with open(path, "w", encoding="utf-8") as fh:
                    yaml.dump(request, fh, Dumper=make_dumper(), sort_keys=False)
            case transporter1.V1ImportFormat():
                transporter1.dump_request_yaml(
                    request, path, dumper_cls=transporter1.INPUT_DUMPER
                )
            case transporter1.V1CopyFormat():
                transporter1.dump_request_yaml(
                    request, path, dumper_cls=transporter1.OUTPUT_DUMPER
                )
            case transporter1.V1WildcardCopyFormat():
                transporter1.dump_request_yaml(
                    request, path, dumper_cls=transporter1.WILDCARD_COPY_DUMPER
                )
            case _:
                raise TypeError(
                    f"Unsupported transporter request type: {type(request)!r}."
                )

    def command(self, request_yaml: Path, response_yaml: Path) -> list[str]:
        return ["--request", str(request_yaml), "--report", str(response_yaml)]

    def base_environment(self) -> Mapping[str, str]:
        return transporter_environment(os.environ)

    def execute(
        self,
        request: Any,
        work_dir: Path,
        environment: Mapping[str, str] | None = None,
        extra_args: tuple[str, ...] = (),
    ) -> TransporterReport:
        request_path, report_path = transporter1.new_request_paths(work_dir)
        self.write_request(request, request_path)
        self.run_or_raise(request_path, report_path, environment)
        report = transporter1.load_report_yaml(report_path)
        return TransporterReport(
            files=list(report.files), lastmod_info=report.lastmod_info
        )

    def s3_location(
        self,
        uri: str,
        access_key_env: str,
        secret_key_env: str,
        region_env: str | None = None,
    ) -> transporter1.TransporterS3:
        return transporter1.TransporterS3(
            url=uri,
            access_key=transporter1.TransporterEnv(access_key_env),
            secret_key=transporter1.TransporterEnv(secret_key_env),
            region=(
                transporter1.TransporterEnv(region_env)
                if region_env is not None
                else None
            ),
        )

    def gcs_location(
        self, uri: str, service_account_key_env: str
    ) -> transporter1.TransporterGCS:
        return transporter1.TransporterGCS(
            url=uri,
            service_account_key=transporter1.TransporterEnv(service_account_key_env),
        )

    def azure_location(
        self, uri: str, account_name_env: str, account_key_env: str
    ) -> transporter1.TransporterAzure:
        return transporter1.TransporterAzure(
            url=uri,
            account_name=transporter1.TransporterEnv(account_name_env),
            account_key=transporter1.TransporterEnv(account_key_env),
        )

    def local_location(self, uri: str) -> transporter1.TransporterLocalFile:
        return transporter1.TransporterLocalFile(uri)

    def local_path_location(self, path: Path) -> transporter1.TransporterLocalFile:
        return transporter1.TransporterLocalFile(transporter1.path_to_file_uri(path))

    def local_uri(self, path: Path | str) -> str:
        return transporter1.path_to_file_uri(path)

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
    ) -> transporter1.V1ImportFormat:
        resolved = transporter1.resolve_auto_src_format(file_format, list(paths))
        format = transporter1.src_format_to_transporter(
            resolved, (format_cfg or {}).get(resolved)
        )
        return transporter1.V1ImportFormat(
            source=source,
            format=format,
            target=transporter1.TransporterLocalFile(
                transporter1.path_to_file_uri(target_dir)
            ),
            initial_lastmod=initial_lastmod,
            lastmod_info=lastmod_info,
            file_info=file_info,
        )

    def wildcard_request(
        self,
        source: Any,
        target_dir: Path,
        initial_lastmod: str | None = None,
        lastmod_info: str | None = None,
        max_files: int | None = None,
    ) -> transporter1.V1WildcardCopyFormat:
        return transporter1.V1WildcardCopyFormat(
            source=source,
            target=transporter1.TransporterLocalFile(
                transporter1.path_to_file_uri(target_dir)
            ),
            initial_lastmod=initial_lastmod,
            lastmod_info=lastmod_info,
            max_files=max_files,
        )

    def copy_request(
        self,
        source_target_pairs: Sequence[tuple[Any, Any]],
        work_dir: Path,
        file_format: Any,
        paths: Sequence[str],
        format_cfg: dict | None = None,
    ) -> transporter1.V1CopyFormat:
        resolved = transporter1.resolve_auto_dest_format(file_format, list(paths))
        format = transporter1.dest_format_to_transporter(
            resolved, (format_cfg or {}).get(resolved)
        )
        if not isinstance(format, transporter1.TransporterParquetFormat):
            transcoded: list[tuple[Any, Any]] = []
            for source, target in source_target_pairs:
                if not isinstance(source, transporter1.TransporterLocalFile):
                    raise ValueError(
                        "source for transcoding must be a TransporterLocalFile; "
                        f"got {type(source).__name__}."
                    )
                transcoded.append((
                    transcode_to_local(source, work_dir, format),
                    target,
                ))
            source_target_pairs = transcoded
        pairs_yaml = [[source, target] for source, target in source_target_pairs]
        return transporter1.V1CopyFormat(pairs_yaml, format=format)
