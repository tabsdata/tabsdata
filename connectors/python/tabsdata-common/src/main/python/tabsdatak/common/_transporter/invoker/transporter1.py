from __future__ import annotations

import os
import uuid
from abc import ABC
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

import yaml

from tabsdatak.common._fileformat.error import FileFormatErrorCode
from tabsdatak.common._fileformat.taxonomy import DestFileFormat, SrcFileFormat

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


# --- Driver helpers (used by _input / _output) ------------------


def new_request_paths(work_dir: Path) -> tuple[Path, Path]:
    work_dir.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    return work_dir / f"request_{token}.yaml", work_dir / f"report_{token}.yaml"


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


@dataclass(frozen=True, slots=True)
class V1WildcardCopyFormat:
    # ``source`` is a wildcard location (its file name may contain a glob);
    # ``target`` is the base directory each matched file is copied under.
    # ``max_files`` caps how many of the matched files are consumed (the
    # oldest by last-modified time); ``None`` means no limit.
    source: TransporterLocation
    target: TransporterLocation
    initial_lastmod: str | None = None
    lastmod_info: str | None = None
    max_files: int | None = None


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


def _v1_wildcard_copy_to_dict(env: V1WildcardCopyFormat) -> dict:
    return {
        "source": {
            "location": env.source,
            "initial_lastmod": env.initial_lastmod,
            "lastmod_info": env.lastmod_info,
        },
        # ``target`` is a bare ``Location<BaseUrl>`` (the base directory),
        # not wrapped in a ``{location: ...}`` mapping like the import target.
        "target": env.target,
        "decompression": None,
        "compression": None,
        "parallelism": None,
        "max_files": env.max_files,
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


def _wildcard_copy_repr(
    dumper: yaml.SafeDumper,
    env: V1WildcardCopyFormat,
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping("!WildcardCopyV1", _v1_wildcard_copy_to_dict(env))


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
WILDCARD_COPY_DUMPER = build_dumper(
    extra=((V1WildcardCopyFormat, _wildcard_copy_repr),)
)


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
    _Loader.add_constructor("!WildcardCopyV1", _ctor)
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


# Extension → format mapping for AUTO inference. Keys are lower-case
# with the leading dot. `.jsonl` / `.ndjson` are common newline-delimited
# JSON variants and both round-trip through the JSON transporter format.
_SRC_EXTENSION_FORMAT: dict[str, SrcFileFormat] = {
    ".csv": SrcFileFormat.CSV,
    ".json": SrcFileFormat.JSON,
    ".jsonl": SrcFileFormat.JSON,
    ".ndjson": SrcFileFormat.JSON,
    ".avro": SrcFileFormat.AVRO,
    ".parquet": SrcFileFormat.PARQUET,
    ".pq": SrcFileFormat.PARQUET,
    ".log": SrcFileFormat.LOG,
}

_DEST_EXTENSION_FORMAT: dict[str, DestFileFormat] = {
    ".csv": DestFileFormat.CSV,
    ".json": DestFileFormat.JSON,
    ".jsonl": DestFileFormat.JSON,
    ".ndjson": DestFileFormat.JSON,
    ".avro": DestFileFormat.AVRO,
    ".parquet": DestFileFormat.PARQUET,
    ".pq": DestFileFormat.PARQUET,
}


def resolve_auto_src_format(fmt: SrcFileFormat, paths: list[str]) -> SrcFileFormat:
    """Resolve ``SrcFileFormat.AUTO`` to a concrete format via path
    extension inspection.

    If ``fmt`` is already concrete it is returned unchanged. Otherwise
    every path's extension is matched against
    :data:`_SRC_EXTENSION_FORMAT`; if all paths resolve to the same
    concrete format that format is returned. If extensions are missing,
    unknown, or mismatched the caller gets a ``FILE_FORMAT_3``
    ``ConnInitException`` pointing at the offending paths.

    The error is raised at config-build time (``ReaderConf.from_src``)
    so users see it before any transporter or runner work starts.
    """
    if fmt is not SrcFileFormat.AUTO:
        return fmt
    detected = {_SRC_EXTENSION_FORMAT.get(_extension(p)) for p in paths}
    detected_real = {d for d in detected if d is not None}
    if None in detected:
        unknown = [p for p in paths if _SRC_EXTENSION_FORMAT.get(_extension(p)) is None]
        raise FileFormatErrorCode.FILE_FORMAT_3.exception(
            paths=paths,
            reason=f"no recognised extension on {unknown!r}",
        )
    if len(detected_real) > 1:
        raise FileFormatErrorCode.FILE_FORMAT_3.exception(
            paths=paths,
            reason=f"mixed formats inferred ({sorted(d.value for d in detected_real)})",
        )
    return next(iter(detected_real))


def resolve_auto_dest_format(fmt: DestFileFormat, paths: list[str]) -> DestFileFormat:
    """Resolve ``DestFileFormat.AUTO`` to a concrete format via path
    extension inspection. Mirrors :func:`resolve_auto_src_format` but
    against the dest-side enum + extension map. Raises ``FILE_FORMAT_4``
    on missing / unknown / mismatched extensions.
    """
    if fmt is not DestFileFormat.AUTO:
        return fmt
    detected = {_DEST_EXTENSION_FORMAT.get(_extension(p)) for p in paths}
    detected_real = {d for d in detected if d is not None}
    if None in detected:
        unknown = [
            p for p in paths if _DEST_EXTENSION_FORMAT.get(_extension(p)) is None
        ]
        raise FileFormatErrorCode.FILE_FORMAT_4.exception(
            paths=paths,
            reason=f"no recognised extension on {unknown!r}",
        )
    if len(detected_real) > 1:
        raise FileFormatErrorCode.FILE_FORMAT_4.exception(
            paths=paths,
            reason=f"mixed formats inferred ({sorted(d.value for d in detected_real)})",
        )
    return next(iter(detected_real))


def _extension(path: str) -> str:
    """Lower-cased ``os.path.splitext`` extension. Includes the leading
    dot, or ``""`` if the path has no extension. Glob paths like
    ``*.parquet`` retain the ``.parquet`` extension, which is what the
    AUTO inference wants."""
    return os.path.splitext(path)[1].lower()


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
    # AUTO must be resolved by ``resolve_auto_src_format`` before reaching
    # here -- the previous behaviour silently returned Parquet, which
    # masked CSV/JSON/AVRO mismatches as parquet-magic-byte failures.
    # noinspection PyProtectedMember
    raise FileFormatErrorCode.FILE_FORMAT_1.exception(format=fmt)


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
    # AUTO must be resolved by ``resolve_auto_dest_format`` before
    # reaching here -- see the matching note on the src-side function.
    # noinspection PyProtectedMember
    raise FileFormatErrorCode.FILE_FORMAT_2.exception(format=fmt)
