from __future__ import annotations

import os
import shutil
import subprocess
import sys
import uuid
from abc import ABC
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

import yaml

from tabsdatak.conn.common.file.error import FileCommonErrorCode
from tabsdatak.conn.common.file.typing_ import DestFileFormat, SrcFileFormat

# --- Driver-level exception types -----------------------------------


class TransporterBinaryNotFoundError(FileNotFoundError):
    pass


class TransporterRunError(RuntimeError):
    def __init__(self, returncode: int, stdout: str, stderr: str):
        super().__init__(
            f"td_transporter exited {returncode}: {stderr.strip() or stdout.strip()}"
        )
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class UnresolvedCredentialError(RuntimeError):
    pass


# --- Binary lookup --------------------------------------------------


_BINARY_NAME = "td_transporter.exe" if sys.platform == "win32" else "td_transporter"
_BINARY_ENV_VAR = "TD_TRANSPORTER_BIN"


def find_transporter_binary() -> str:
    override = os.environ.get(_BINARY_ENV_VAR)
    if override:
        if os.path.isfile(override) and os.access(override, os.X_OK):
            return override
        raise TransporterBinaryNotFoundError(
            f"${_BINARY_ENV_VAR}={override!r} does not point to an executable file"
        )
    found = shutil.which(_BINARY_NAME)
    if found:
        return found
    raise TransporterBinaryNotFoundError(
        f"td_transporter binary not found on PATH and "
        f"${_BINARY_ENV_VAR} is not set; install the binary or set "
        f"the env var to its absolute path"
    )


# --- Driver helpers (used by _input / _output) ------------------


def new_request_paths(work_dir: Path) -> tuple[Path, Path]:
    work_dir.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    return work_dir / f"request_{token}.yaml", work_dir / f"report_{token}.yaml"


def run_binary(
    request_path: Path,
    report_path: Path,
    *,
    env: dict[str, str] | None,
    binary_path: str | None,
) -> None:
    binary = binary_path or find_transporter_binary()
    proc_env = dict(os.environ)
    if env:
        proc_env.update(env)
    completed = subprocess.run(
        [
            binary,
            "--request",
            str(request_path),
            "--report",
            str(report_path),
        ],
        capture_output=True,
        text=True,
        env=proc_env,
    )
    if completed.returncode != 0:
        raise TransporterRunError(
            completed.returncode,
            completed.stdout,
            completed.stderr,
        )


# --- URI helpers ----------------------------------------------------


def path_to_file_uri(path: Path | str) -> str:
    p = Path(path)
    abs_str = str(p.resolve())
    is_dir_marker = str(path).endswith(os.sep) or (p.exists() and p.is_dir())
    uri = Path(abs_str).as_uri()
    if is_dir_marker and not uri.endswith("/"):
        uri += "/"
    return uri


# --- Server-side env var names --------------------------------------

# Names the td_transporter binary reads at runtime to authenticate
# against cloud backends. Match tabsdata-os exactly so a binary
# built from either tree works against requests built here.
SERVER_SIDE_AWS_ACCESS_KEY_ID = "LOCATION_AWS_ACCESS_KEY_ID"
SERVER_SIDE_AWS_SECRET_ACCESS_KEY = "LOCATION_AWS_SECRET_ACCESS_KEY"
SERVER_SIDE_AWS_REGION = "LOCATION_AWS_REGION"
SERVER_SIDE_AZURE_ACCOUNT_NAME = "LOCATION_ACCOUNT_NAME"
SERVER_SIDE_AZURE_ACCOUNT_KEY = "LOCATION_ACCOUNT_KEY"
SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON = "LOCATION_GCS_SERVICE_ACCOUNT_JSON"


# --- Report parse error --------------------------------------------


class TransporterReportError(RuntimeError):
    pass


# --- Domain dataclasses --------------------------------------------
#
# Each Transporter* type is the natural Python shape callers want to
# construct. The wire format the binary consumes is computed by a
# small ``_<name>_to_dict`` transcoder right next to the YAML
# representer that uses it. Reading the transcoder tells you exactly
# what the binary will see.


class TransporterLocation(ABC):
    pass


class TransporterFormat(ABC):
    pass


@dataclass(frozen=True, slots=True)
class TransporterEnv:
    name: str


@dataclass(frozen=True, slots=True)
class TransporterLiteral:
    value: str

    def __repr__(self) -> str:
        return "TransporterLiteral(value='***redacted***')"


@dataclass(frozen=True, slots=True)
class TransporterS3(TransporterLocation):
    url: str
    access_key: TransporterEnv | TransporterLiteral
    secret_key: TransporterEnv | TransporterLiteral
    region: TransporterEnv | TransporterLiteral | None = None
    extra_configs: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TransporterGCS(TransporterLocation):
    url: str
    service_account_key: TransporterEnv | TransporterLiteral
    extra_configs: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TransporterAzure(TransporterLocation):
    url: str
    account_name: TransporterEnv | TransporterLiteral
    account_key: TransporterEnv | TransporterLiteral
    extra_configs: dict = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TransporterLocalFile(TransporterLocation):
    url: str


@dataclass(frozen=True, slots=True)
class TransporterAvroFormat(TransporterFormat):
    pass


@dataclass(frozen=True, slots=True)
class TransporterParquetFormat(TransporterFormat):
    pass


@dataclass(frozen=True, slots=True)
class TransporterLogFormat(TransporterFormat):
    pass


@dataclass(frozen=True, slots=True)
class TransporterJsonFormat(TransporterFormat):
    pass


@dataclass(frozen=True, slots=True)
class TransporterCSVFormat(TransporterFormat):
    options: dict = field(default_factory=dict)


# --- Request envelopes (one per transporter flavor) ---------------


@dataclass(frozen=True, slots=True)
class V1ImportFormat:
    source: TransporterLocation
    format: TransporterFormat
    target: TransporterLocation
    initial_lastmod: str | None = None
    lastmod_info: str | None = None
    file_info: str | None = None


@dataclass(frozen=True, slots=True)
class V1CopyFormat:
    source_target_pairs: list[list[TransporterLocation]]
    format: TransporterFormat


# --- Response (shared by all flavors) -----------------------------


@dataclass(frozen=True, slots=True)
class TransporterReport:
    files: list[dict]
    lastmod_info: str | None
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> TransporterReport:
        return cls(
            files=list(raw.get("files") or []),
            lastmod_info=raw.get("lastmod_info"),
            raw=raw,
        )


# --- Wire-format transcoders ---------------------------------------


def _s3_to_dict(loc: TransporterS3) -> dict:
    return {
        "url": loc.url,
        "configs": {
            "access_key": loc.access_key,
            "secret_key": loc.secret_key,
            "region": loc.region,
            "extra_configs": dict(loc.extra_configs),
        },
    }


def _gcs_to_dict(loc: TransporterGCS) -> dict:
    return {
        "url": loc.url,
        "configs": {
            "service_account_key": loc.service_account_key,
            "extra_configs": dict(loc.extra_configs),
        },
    }


def _azure_to_dict(loc: TransporterAzure) -> dict:
    return {
        "url": loc.url,
        "configs": {
            "account_name": loc.account_name,
            "account_key": loc.account_key,
            "extra_configs": dict(loc.extra_configs),
        },
    }


def _local_to_dict(loc: TransporterLocalFile) -> dict:
    return {"url": loc.url}


def _csv_to_dict(fmt: TransporterCSVFormat) -> dict:
    return dict(fmt.options)


def _v1_import_to_dict(env: V1ImportFormat) -> dict:
    d: dict[str, Any] = {
        "source": {
            "location": env.source,
            "initial_lastmod": env.initial_lastmod,
            "lastmod_info": env.lastmod_info,
        },
        "format": env.format,
        "target": {"location": env.target},
        "parallelism": 1,
    }
    if env.file_info is not None:
        d["file_info"] = env.file_info
    return d


def _v1_copy_to_dict(env: V1CopyFormat) -> dict:
    return {
        "source_target_pairs": env.source_target_pairs,
        "format": env.format,
        "parallelism": None,
    }


# --- YAML representers --------------------------------------------


def _env_repr(dumper: yaml.SafeDumper, env: TransporterEnv) -> yaml.nodes.ScalarNode:
    # The ``!Env`` tag is the contract with the Rust ``td_transporter``
    # binary -- it must stay ``!Env`` regardless of how the Python-side
    # ``Env`` class was renamed.
    return dumper.represent_scalar("!Env", env.name)


def _literal_repr(
    dumper: yaml.SafeDumper,
    lit: TransporterLiteral,
) -> yaml.nodes.ScalarNode:
    return dumper.represent_scalar("!Literal", lit.value)


def _s3_repr(dumper: yaml.SafeDumper, loc: TransporterS3) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping("!S3", _s3_to_dict(loc))


def _gcs_repr(dumper: yaml.SafeDumper, loc: TransporterGCS) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping("!GCS", _gcs_to_dict(loc))


def _azure_repr(
    dumper: yaml.SafeDumper,
    loc: TransporterAzure,
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping("!Azure", _azure_to_dict(loc))


def _local_repr(
    dumper: yaml.SafeDumper,
    loc: TransporterLocalFile,
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping("!LocalFile", _local_to_dict(loc))


def _avro_repr(
    dumper: yaml.SafeDumper, _: TransporterAvroFormat
) -> yaml.nodes.ScalarNode:
    return dumper.represent_str("Binary")


def _parquet_repr(
    dumper: yaml.SafeDumper,
    _: TransporterParquetFormat,
) -> yaml.nodes.ScalarNode:
    return dumper.represent_str("Parquet")


def _log_repr(
    dumper: yaml.SafeDumper,
    _: TransporterLogFormat,
) -> yaml.nodes.ScalarNode:
    return dumper.represent_str("Log")


def _json_repr(
    dumper: yaml.SafeDumper,
    _: TransporterJsonFormat,
) -> yaml.nodes.ScalarNode:
    return dumper.represent_str("Json")


def _csv_repr(
    dumper: yaml.SafeDumper,
    fmt: TransporterCSVFormat,
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping("!Csv", _csv_to_dict(fmt))


def _import_repr(
    dumper: yaml.SafeDumper,
    env: V1ImportFormat,
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping("!ImportV1", _v1_import_to_dict(env))


def _copy_repr(
    dumper: yaml.SafeDumper,
    env: V1CopyFormat,
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping("!CopyV1", _v1_copy_to_dict(env))


_COMMON_REPRESENTERS: tuple[tuple[type, Callable], ...] = (
    (TransporterEnv, _env_repr),
    (TransporterLiteral, _literal_repr),
    (TransporterS3, _s3_repr),
    (TransporterGCS, _gcs_repr),
    (TransporterAzure, _azure_repr),
    (TransporterLocalFile, _local_repr),
    (TransporterAvroFormat, _avro_repr),
    (TransporterParquetFormat, _parquet_repr),
    (TransporterLogFormat, _log_repr),
    (TransporterJsonFormat, _json_repr),
    (TransporterCSVFormat, _csv_repr),
)


def build_dumper(
    extra: Iterable[tuple[type, Callable]] = (),
) -> type[yaml.SafeDumper]:
    """Return a fresh ``yaml.SafeDumper`` subclass with the common
    location/format representers, plus any envelope-specific
    ``extra`` representers the caller wants to add."""

    class _Dumper(yaml.SafeDumper):
        pass

    for cls, fn in _COMMON_REPRESENTERS:
        _Dumper.add_representer(cls, fn)
    for cls, fn in extra:
        _Dumper.add_representer(cls, fn)
    return _Dumper


# Pre-built dumpers, one per request flavor.
INPUT_DUMPER = build_dumper(extra=((V1ImportFormat, _import_repr),))
OUTPUT_DUMPER = build_dumper(extra=((V1CopyFormat, _copy_repr),))


def dump_request_yaml(
    envelope: Any,
    path: Path,
    *,
    dumper_cls: type[yaml.SafeDumper],
) -> None:
    with open(path, "w") as fh:
        yaml.dump(envelope, fh, Dumper=dumper_cls)


# --- Report loader -------------------------------------------------
#
# A thin loader that pulls ``files`` and ``lastmod_info`` back out of
# the binary's YAML output. We deliberately match the tabsdata-os
# V1ImportReportFormat shape: tag is ``!ImportV1`` / ``!CopyV1``,
# payload is a mapping with ``files`` (list of ``{from, to}`` dicts)
# and ``lastmod_info`` (string or null).


def _build_loader() -> type[yaml.SafeLoader]:
    class _Loader(yaml.SafeLoader):
        pass

    def _ctor(loader, node):
        return loader.construct_mapping(node, deep=True)

    _Loader.add_constructor("!ImportV1", _ctor)
    _Loader.add_constructor("!CopyV1", _ctor)
    return _Loader


def load_report_yaml(path: Path) -> TransporterReport:
    if not path.exists():
        raise TransporterReportError(f"transporter report not found at {path}")
    try:
        with open(path, "r") as fh:
            data = yaml.load(fh, Loader=_build_loader())
    except yaml.YAMLError as e:
        raise TransporterReportError(
            f"failed to parse transporter report {path}: {e}"
        ) from e
    if not isinstance(data, dict):
        raise TransporterReportError(
            f"transporter report at {path} is not a mapping (got {type(data).__name__})"
        )
    return TransporterReport.from_dict(data)


# --- Format mappers ------------------------------------------------

# The transporter binary requires the full CSV options dict in the
# request YAML; partial dicts trip a "missing field" parse error. The
# defaults below match polars' CSV reader / writer defaults. The
# user's ``format_cfg`` is layered on top.

_CSV_SRC_DEFAULTS: dict = {
    "separator": ",",
    "quote_char": '"',
    "eol_char": "\n",
    "encoding": "utf8",
    "null_values": None,
    "missing_is_null": True,
    "truncate_ragged_lines": False,
    "comment_prefix": None,
    "try_parse_dates": False,
    "decimal_comma": False,
    "has_header": True,
    "skip_rows": 0,
    "skip_rows_after_header": 0,
    "raise_if_empty": True,
    "ignore_errors": False,
}

_CSV_DEST_DEFAULTS: dict = {
    "separator": ",",
    "quote_char": '"',
    "eol_char": "\n",
    "include_header": True,
    "datetime_format": None,
    "date_format": None,
    "time_format": None,
    "float_scientific": None,
    "float_precision": None,
    "null_value": "",
    "quote_style": "necessary",
    "maintain_order": True,
}


def src_format_to_transporter(
    fmt: SrcFileFormat,
    cfg: dict | None,
) -> TransporterFormat:
    if fmt is SrcFileFormat.CSV:
        return TransporterCSVFormat({**_CSV_SRC_DEFAULTS, **(cfg or {})})
    if fmt is SrcFileFormat.JSON:
        return TransporterJsonFormat()
    if fmt is SrcFileFormat.AVRO:
        return TransporterAvroFormat()
    if fmt is SrcFileFormat.PARQUET:
        return TransporterParquetFormat()
    if fmt is SrcFileFormat.LOG:
        return TransporterLogFormat()
    if fmt is SrcFileFormat.AUTO:
        return TransporterParquetFormat()
    # noinspection PyProtectedMember
    raise FileCommonErrorCode.FILE_COMMON_14.exception(format=fmt)


def dest_format_to_transporter(
    fmt: DestFileFormat,
    cfg: dict | None,
) -> TransporterFormat:
    if fmt is DestFileFormat.CSV:
        return TransporterCSVFormat({**_CSV_DEST_DEFAULTS, **(cfg or {})})
    if fmt is DestFileFormat.JSON:
        return TransporterJsonFormat()
    if fmt is DestFileFormat.AVRO:
        return TransporterAvroFormat()
    if fmt is DestFileFormat.PARQUET:
        return TransporterParquetFormat()
    if fmt is DestFileFormat.AUTO:
        return TransporterParquetFormat()
    # noinspection PyProtectedMember
    raise FileCommonErrorCode.FILE_COMMON_15.exception(format=fmt)
