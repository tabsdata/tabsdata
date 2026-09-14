#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import os
import subprocess
import types
import uuid
from abc import ABC
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, Union, get_args, get_origin, get_type_hints
from urllib.parse import urlparse
from urllib.request import url2pathname

import yaml

from tabsdatak.common._fileformat.error import FileFormatErrorCode
from tabsdatak.common._fileformat.taxonomy import (
    EXCEL_SHEET_KEY,
    DestFileFormat,
    SrcFileFormat,
)

# ===================================================================
# ``td_transporter2`` Python shim.
#
# Sibling of :mod:`transporter1` that drives the ``td_transporter2``
# binary. The connector-facing API (the ``Transporter*`` location /
# format types, the format mappers) mirrors :mod:`transporter1`; only the
# wire layer changes:
#
#   * ``td_transporter2`` is a multi-command CLI -- ``find`` /
#     ``find-copy`` / ``copy`` / ``delete``, each ``--request <in.yaml>
#     --response <out.yaml>``.
#   * The request / response files are flat, tag-free YAML. A location
#     is ``{uri, configs}`` with friendly per-provider config keys; a
#     format is ``{format, options?}``; every uri / config value goes
#     through the binary's ``env:`` / ``val:`` resolution.
#   * The binary converts formats natively (``source_format`` ->
#     ``target_format``, parquet is the pivot) and (de)compresses on the
#     fly, so -- unlike the ``td_file`` path -- there is **no** local
#     polars transcoding layered on top.
#
# The request / response wire shapes are modelled as dataclasses (see
# "Wire dataclasses" below). They are serialized by a single generic
# walk (:func:`_dump` / :func:`_load`) rather than per-class encoders:
# ``_dump`` drops ``None`` fields and aliases ``from_`` -> ``from``;
# ``_load`` does the inverse and rewrites local ``file://`` URIs back to
# OS paths. dict appears only as the transient YAML payload.
# ===================================================================


# --- Driver-level exception types -----------------------------------


class TransporterBinaryNotFoundError(FileNotFoundError):
    pass


class TransporterRunError(RuntimeError):
    def __init__(self, returncode: int, stdout: str, stderr: str):
        super().__init__(
            f"td_transporter2 exited {returncode}: {stderr.strip() or stdout.strip()}"
        )
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class UnresolvedCredentialError(RuntimeError):
    pass


class TransporterReportError(RuntimeError):
    pass


# --- Subcommands ----------------------------------------------------
#
# ``td_transporter2`` is a multi-command CLI; each request is one
# subcommand plus ``--request``/``--response`` YAML files.
#   * ``find``      -- list files matching a (wildcard) source.
#   * ``find-copy`` -- find files under a (wildcard) source and copy /
#     convert each into a target directory (the read / import path and
#     the verbatim wildcard-copy path).
#   * ``copy``      -- copy / convert an explicit list of (from, to)
#     pairs (the write / output path).
#   * ``delete``    -- delete a list of files (per-file failures are
#     recorded, not fatal).
_FIND = "find"
_FIND_COPY = "find-copy"
_COPY = "copy"
_DELETE = "delete"


# --- Driver helpers -------------------------------------------------


def new_request_paths(work_dir: Path) -> tuple[Path, Path]:
    work_dir.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    return work_dir / f"request_{token}.yaml", work_dir / f"response_{token}.yaml"


def run_binary(
    subcommand: str,
    request_path: Path,
    response_path: Path,
    *,
    env: dict[str, str] | None,
    binary_path: str,
    extra_args: tuple[str, ...] = (),
) -> None:
    proc_env = dict(os.environ)
    if env:
        proc_env.update(env)
    completed = subprocess.run(
        [
            binary_path,
            subcommand,
            "--request",
            str(request_path),
            "--response",
            str(response_path),
            *extra_args,
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


def _file_uri_to_path(uri: str) -> str:
    """``file://`` URI -> local filesystem path string. Non-``file``
    URIs (``s3://`` etc.) are returned unchanged."""
    if not isinstance(uri, str) or not uri.startswith("file://"):
        return uri
    # `url2pathname` is OS-aware: on Windows `/C:/a/b` -> `C:\a\b`, on POSIX
    # `/a/b` stays. Raw `urlparse().path` would keep a leading slash before
    # a drive letter, yielding a non-absolute value.
    return url2pathname(urlparse(uri).path)


# --- Server-side env var names --------------------------------------

# Names used to hand cloud credentials to the binary *out of band* -- via
# the subprocess environment, never the on-disk request YAML. A connector
# builds its location with ``TransporterEnv(SERVER_SIDE_*)`` (so the
# request only carries ``env:LOCATION_...`` references) and separately
# passes ``{SERVER_SIDE_*: <secret value>}`` as ``env`` to the binary,
# which resolves the references at load time. Shared with tabsdata-os so a
# binary built from either tree resolves the same names. Used by the
# s3 / gcs / azureblob / druid / starrocks connectors.
SERVER_SIDE_AWS_ACCESS_KEY_ID = "LOCATION_AWS_ACCESS_KEY_ID"
SERVER_SIDE_AWS_SECRET_ACCESS_KEY = "LOCATION_AWS_SECRET_ACCESS_KEY"
SERVER_SIDE_AWS_REGION = "LOCATION_AWS_REGION"
SERVER_SIDE_AZURE_ACCOUNT_NAME = "LOCATION_ACCOUNT_NAME"
SERVER_SIDE_AZURE_ACCOUNT_KEY = "LOCATION_ACCOUNT_KEY"
SERVER_SIDE_GCP_SERVICE_ACCOUNT_JSON = "LOCATION_GCS_SERVICE_ACCOUNT_JSON"


# --- Connector-facing domain dataclasses ---------------------------
#
# The natural Python shape callers (the per-connector ``build_location``
# builders) construct. Identical to :mod:`transporter1`. These are
# mapped onto the td_transporter2 wire dataclasses by :func:`to_location`
# / :func:`to_format_spec` below.


class TransporterLocation(ABC):
    pass


class TransporterFormat(ABC):
    pass


@dataclass(frozen=True, slots=True)
class TransporterEnv:
    name: str

    def wire(self) -> str:
        """``env:<NAME>`` -- a reference the binary resolves from the
        process environment at load time."""
        return f"env:{self.name}"


@dataclass(frozen=True, slots=True)
class TransporterLiteral:
    value: str

    def wire(self) -> str:
        """``val:<VALUE>`` -- a verbatim literal (escape hatch)."""
        return f"val:{self.value}"

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


@dataclass(frozen=True, slots=True)
class TransporterExcelFormat(TransporterFormat):
    """Read-only. ``sheet`` is the 1-based position of the worksheet in
    the workbook, counted over every sheet it declares -- hidden and chart
    sheets included -- in tab order. It comes from the path's ``#N``
    fragment, not from ``format_cfg``, so one path can name one sheet.

    ``has_header`` is an ordinary ``format_cfg`` option: with it off no row
    names the columns, every row is data, and the columns are called
    ``column_1``, ``column_2``, ..."""

    sheet: int = 1
    has_header: bool = True


# --- Wire dataclasses (request side) -------------------------------
#
# Mirror the td_transporter2 request structs. Serialized to YAML by the
# generic :func:`_dump` -- no per-class encoders.


@dataclass(frozen=True, slots=True)
class Location:
    """A resolved location: a ``uri`` plus backend ``configs`` (creds,
    region, ...). Config values are ``env:<VAR>`` / ``val:<VALUE>`` /
    verbatim strings the binary resolves at load time."""

    uri: str
    configs: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CsvOptions:
    """The only format with knobs on the wire. Everything beyond
    ``has_header`` / ``delimiter`` is reader-side only and optional:
    ``None`` keeps the key off the wire, deferring to the binary's
    default."""

    has_header: bool = True
    delimiter: str = ","
    quote_char: str | None = None
    comment_prefix: str | None = None
    null_values: list[str] | None = None
    decimal_comma: bool | None = None
    truncate_ragged_lines: bool | None = None
    skip_rows: int | None = None
    skip_rows_after_header: int | None = None


@dataclass(frozen=True, slots=True)
class ExcelOptions:
    """Excel reader options, carried in :class:`FormatSpec`'s ``options``.
    Mirrors ``ExcelOptions`` in the binary's ``spec.rs`` field for field --
    a name that drifts is a YAML parse failure there, not here."""

    sheet: int = 1
    has_header: bool = True


@dataclass(frozen=True, slots=True)
class FormatSpec:
    """A data format the binary can decode / encode: ``parquet``,
    ``csv`` (with :class:`CsvOptions`), ``ndjson``, ``avro``, or -- read-only,
    so valid only as a ``source_format`` -- ``text`` and ``excel`` (with
    :class:`ExcelOptions`).

    ``options`` carries whichever block the format takes, untagged; the
    binary reads it against the sibling ``format`` field rather than
    against the block's own keys, so ``{}`` is unambiguous.
    """

    format: str
    options: CsvOptions | ExcelOptions | None = None


@dataclass(frozen=True, slots=True)
class FindRequest:
    source: Location
    initial_lastmod: str | None = None
    lastmod_info: str | None = None


@dataclass(frozen=True, slots=True)
class FindCopyRequest:
    source: Location
    target_dir: Location
    source_format: FormatSpec | None = None
    target_format: FormatSpec | None = None
    decompress: str | None = None
    compress: str | None = None
    max_files: int | None = None
    initial_lastmod: str | None = None
    lastmod_info: str | None = None


@dataclass(frozen=True, slots=True)
class CopyPair:
    # ``from`` is a Python keyword, so the field is ``from_``; :func:`_dump`
    # aliases it back to ``from`` on the wire.
    from_: Location
    to: Location


@dataclass(frozen=True, slots=True)
class CopyRequest:
    pairs: list[CopyPair]
    source_format: FormatSpec | None = None
    target_format: FormatSpec | None = None
    decompress: str | None = None
    compress: str | None = None


@dataclass(frozen=True, slots=True)
class DeleteRequest:
    files: list[Location]


# --- Wire dataclasses (response side) ------------------------------
#
# Mirror the td_transporter2 response structs. Parsed from YAML by the
# generic :func:`_load` -- no per-class decoders.


@dataclass(frozen=True, slots=True)
class FoundFile:
    uri: str
    size: int
    last_modified: str


@dataclass(frozen=True, slots=True)
class FindResponse:
    files: list[FoundFile]
    lastmod_info: str | None = None


@dataclass(frozen=True, slots=True)
class CopiedFile:
    """A file moved by ``copy`` / ``find-copy``. ``rows`` is present only
    when the file was converted (a pure byte move reports ``size`` but no
    row count). ``from``/``to`` local ``file://`` URIs are rewritten back
    to OS paths by :func:`_load`; cloud URIs pass through unchanged."""

    from_: str
    to: str
    size: int
    rows: int | None
    last_modified: str
    started_at: str
    ended_at: str


@dataclass(frozen=True, slots=True)
class CopyResponse:
    """The ``copy`` / ``find-copy`` response. Also exported as
    :data:`TransporterReport` -- the connector-facing report name."""

    files: list[CopiedFile]
    lastmod_info: str | None = None


# Connector-facing alias: the read (find-copy) and write (copy) paths
# both return this report shape, named ``TransporterReport`` to match
# the :mod:`transporter1` API.
TransporterReport = CopyResponse


@dataclass(frozen=True, slots=True)
class DeleteFailure:
    uri: str
    error: str


@dataclass(frozen=True, slots=True)
class DeleteResponse:
    deleted: list[str]
    failed: list[DeleteFailure]


# --- Generic wire (de)serialization --------------------------------
#
# One walk each way over the wire dataclasses, instead of a hand-written
# encoder/decoder per class. ``dict`` is only ever the transient YAML
# payload; the request / response are always modelled as dataclasses.

# Field name <-> wire key fixups (``from`` is a Python keyword).
_WIRE_KEY_ALIASES = {"from_": "from"}

# Response string fields whose local ``file://`` URIs are rewritten back
# to OS paths on load (cloud URIs pass through). Matches the ``from``/``to``
# pair fields of :class:`CopiedFile`; ``uri`` listings are left as URIs.
_PATH_FIELDS = {"from_", "to"}


def _dump(obj: Any) -> Any:
    """Render a request dataclass to a plain mapping for YAML: drop
    ``None`` fields (the binary defaults absent optionals) and empty
    ``configs`` maps, alias ``from_`` -> ``from``, and recurse into nested
    dataclasses and lists."""
    if is_dataclass(obj) and not isinstance(obj, type):
        out: dict[str, Any] = {}
        for f in fields(obj):
            v = getattr(obj, f.name)
            if v is None or (isinstance(v, dict) and not v):
                continue
            out[_WIRE_KEY_ALIASES.get(f.name, f.name)] = _dump(v)
        return out
    if isinstance(obj, list):
        return [_dump(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _dump(v) for k, v in obj.items()}
    return obj


def _load(cls: type, data: dict[str, Any]) -> Any:
    """Construct a response dataclass from parsed YAML: alias ``from`` ->
    ``from_``, recurse into nested dataclasses / lists by field type, and
    rewrite local ``file://`` URIs on the ``from``/``to`` fields back to
    OS paths. The inverse of :func:`_dump`."""
    hints = get_type_hints(cls)
    kwargs = {
        f.name: _coerce(
            hints[f.name],
            data.get(_WIRE_KEY_ALIASES.get(f.name, f.name)),
            f.name,
        )
        for f in fields(cls)
    }
    return cls(**kwargs)


def _coerce(tp: Any, value: Any, field_name: str | None = None) -> Any:
    if value is None:
        return None
    origin = get_origin(tp)
    if origin is Union or origin is types.UnionType:
        non_none = [a for a in get_args(tp) if a is not type(None)]
        return _coerce(non_none[0], value, field_name) if len(non_none) == 1 else value
    if origin is list:
        (elem,) = get_args(tp)
        return [_coerce(elem, v) for v in value]
    if is_dataclass(tp):
        return _load(tp, value)
    if field_name in _PATH_FIELDS and isinstance(value, str):
        return _file_uri_to_path(value)
    return value


# --- Domain -> wire mappers ----------------------------------------
#
# Map the connector-facing ``Transporter*`` types onto the td_transporter2
# wire dataclasses. Credential values render to ``env:`` / ``val:`` strings
# via the location type's own ``wire()`` (see :class:`TransporterEnv` /
# :class:`TransporterLiteral`).


def _config_value(value: Any) -> str:
    if isinstance(value, (TransporterEnv, TransporterLiteral)):
        return value.wire()
    return str(value)


def _extra_configs(extra: dict | None) -> dict[str, str]:
    return {str(k): _config_value(v) for k, v in (extra or {}).items()}


def to_location(loc: TransporterLocation) -> Location:
    """Map a ``Transporter*`` location to a td_transporter2
    :class:`Location` with friendly per-provider config keys."""
    if isinstance(loc, TransporterLocalFile):
        return Location(uri=loc.url)
    if isinstance(loc, TransporterS3):
        configs: dict[str, str] = {
            "access_key_id": _config_value(loc.access_key),
            "secret_access_key": _config_value(loc.secret_key),
        }
        if loc.region is not None:
            configs["region"] = _config_value(loc.region)
        configs.update(_extra_configs(loc.extra_configs))
        return Location(uri=loc.url, configs=configs)
    if isinstance(loc, TransporterGCS):
        configs = {"service_account_key": _config_value(loc.service_account_key)}
        configs.update(_extra_configs(loc.extra_configs))
        return Location(uri=loc.url, configs=configs)
    if isinstance(loc, TransporterAzure):
        configs = {
            "account_name": _config_value(loc.account_name),
            "account_key": _config_value(loc.account_key),
        }
        configs.update(_extra_configs(loc.extra_configs))
        return Location(uri=loc.url, configs=configs)
    raise ValueError(f"unsupported transporter location: {type(loc).__name__}")


def _csv_char(value: Any, default: str) -> str:
    if isinstance(value, int):  # callers may pass a byte/char code
        value = chr(value)
    value = str(value)
    return value[0] if value else default


def _csv_null_values(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        raise ValueError(
            "per-column null_values mappings are not supported by the"
            " transporter; pass a string or a list of strings"
        )
    return [str(v) for v in value]


def _csv_options_to_spec(options: dict) -> CsvOptions:
    """Map the (polars-shaped) CSV option dict to td_transporter2's CSV
    options, normalizing default values to ``None`` so they stay off the
    wire. The remaining polars knobs (eol_char/encoding/missing_is_null/
    try_parse_dates/raise_if_empty/ignore_errors and the writer-side set)
    are not representable on the arrow-rs CSV reader/writer and are
    dropped."""
    # Src dicts carry ``has_header``; dest dicts carry ``include_header``.
    has_header = options.get("has_header", options.get("include_header", True))
    quote_char = _csv_char(options.get("quote_char", '"'), '"')
    skip_rows = int(options.get("skip_rows", 0) or 0)
    skip_rows_after_header = int(options.get("skip_rows_after_header", 0) or 0)
    return CsvOptions(
        has_header=bool(has_header),
        delimiter=_csv_char(options.get("separator", ","), ","),
        quote_char=None if quote_char == '"' else quote_char,
        comment_prefix=options.get("comment_prefix") or None,
        null_values=_csv_null_values(options.get("null_values")) or None,
        decimal_comma=True if options.get("decimal_comma") else None,
        truncate_ragged_lines=True if options.get("truncate_ragged_lines") else None,
        skip_rows=skip_rows or None,
        skip_rows_after_header=skip_rows_after_header or None,
    )


def to_format_spec(fmt: TransporterFormat) -> FormatSpec:
    """Map a ``Transporter*Format`` to a td_transporter2 :class:`FormatSpec`."""
    if isinstance(fmt, TransporterCSVFormat):
        return FormatSpec(format="csv", options=_csv_options_to_spec(fmt.options))
    if isinstance(fmt, TransporterJsonFormat):
        # The connector's JSON is newline-delimited JSON.
        return FormatSpec(format="ndjson")
    if isinstance(fmt, TransporterAvroFormat):
        return FormatSpec(format="avro")
    if isinstance(fmt, TransporterParquetFormat):
        return FormatSpec(format="parquet")
    if isinstance(fmt, TransporterLogFormat):
        # ``log`` files are read as the binary's read-only ``text`` format.
        return FormatSpec(format="text")
    if isinstance(fmt, TransporterExcelFormat):
        return FormatSpec(
            format="excel",
            options=ExcelOptions(sheet=fmt.sheet, has_header=fmt.has_header),
        )
    raise ValueError(f"unsupported transporter format: {type(fmt).__name__}")


# --- Run + response parsing ----------------------------------------
#
# td_transporter2 writes a flat, tag-free response. On failure it writes
# ``{error: <msg>}`` and exits non-zero, so ``run_binary`` raises a
# :class:`TransporterRunError` before we ever parse a success response.


def _load_response(path: Path, response_cls: type) -> Any:
    if not path.exists():
        raise TransporterReportError(f"transporter response not found at {path}")
    try:
        with open(path, "r") as fh:
            data = yaml.safe_load(fh)
    except yaml.YAMLError as e:
        raise TransporterReportError(
            f"failed to parse transporter response {path}: {e}"
        ) from e
    if not isinstance(data, dict):
        raise TransporterReportError(
            f"transporter response at {path} is not a mapping "
            f"(got {type(data).__name__})"
        )
    # Defensive: a stray error-only response (the binary normally exits
    # non-zero, so ``run_binary`` would have raised first).
    if "error" in data and not any(k in data for k in ("files", "deleted")):
        raise TransporterReportError(f"transporter reported error: {data['error']}")
    return _load(response_cls, data)


def _run(
    subcommand: str,
    request: Any,
    work_dir: Path,
    response_cls: type,
    *,
    env: dict[str, str] | None,
    binary_path: str,
    extra_args: tuple[str, ...] = (),
) -> Any:
    """Write ``request`` (a request dataclass) as a flat YAML file, run
    the ``td_transporter2`` ``subcommand`` against it, and parse the
    response YAML back into a ``response_cls`` dataclass."""
    request_path, response_path = new_request_paths(Path(work_dir))
    with open(request_path, "w") as fh:
        yaml.safe_dump(_dump(request), fh, sort_keys=False)
    run_binary(
        subcommand,
        request_path,
        response_path,
        env=env,
        binary_path=binary_path,
        extra_args=extra_args,
    )
    return _load_response(response_path, response_cls)


def find(
    request: FindRequest,
    work_dir: Path,
    *,
    alpha_suborder: bool = False,
    env: dict[str, str] | None = None,
    binary_path: str,
) -> FindResponse:
    """List files matching the (wildcard) source. No conversion.

    ``alpha_suborder`` (``--alpha-suborder``) gives files sharing a
    last-modified timestamp a total order by filename, instead of the
    default (which remembers every name at the boundary timestamp). The
    mode is recorded in the returned ``lastmod_info`` cursor; resuming a
    later call with a different setting is rejected by the binary.
    """
    extra = ("--alpha-suborder",) if alpha_suborder else ()
    return _run(
        _FIND,
        request,
        work_dir,
        FindResponse,
        env=env,
        binary_path=binary_path,
        extra_args=extra,
    )


def find_copy(
    request: FindCopyRequest,
    work_dir: Path,
    *,
    alpha_suborder: bool = False,
    env: dict[str, str] | None = None,
    binary_path: str,
) -> TransporterReport:
    """Find files under the (wildcard) source and copy / convert each
    into ``target_dir`` (byte move when both formats are absent).

    ``alpha_suborder`` behaves as for :func:`find` -- it sub-orders files
    sharing a last-modified timestamp by filename and is recorded in the
    cursor.
    """
    extra = ("--alpha-suborder",) if alpha_suborder else ()
    return _run(
        _FIND_COPY,
        request,
        work_dir,
        CopyResponse,
        env=env,
        binary_path=binary_path,
        extra_args=extra,
    )


def copy(
    request: CopyRequest,
    work_dir: Path,
    *,
    env: dict[str, str] | None = None,
    binary_path: str,
) -> TransporterReport:
    """Copy / convert an explicit list of (from, to) pairs."""
    return _run(
        _COPY, request, work_dir, CopyResponse, env=env, binary_path=binary_path
    )


def delete(
    request: DeleteRequest,
    work_dir: Path,
    *,
    env: dict[str, str] | None = None,
    binary_path: str,
) -> DeleteResponse:
    """Delete a list of files (per-file failures are recorded in the
    response's ``failed`` list, not raised)."""
    return _run(
        _DELETE, request, work_dir, DeleteResponse, env=env, binary_path=binary_path
    )


# --- Format mappers ------------------------------------------------
#
# Identical to :mod:`transporter1`. The reader-side keys mapped by
# :func:`_csv_options_to_spec` take effect; the rest are accepted for
# forward-compatibility and layered under the user's ``format_cfg``.

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
    ".xlsx": SrcFileFormat.EXCEL,
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
    """``cfg`` may carry :data:`EXCEL_SHEET_KEY`, the 1-based worksheet a
    path's ``#N`` fragment named. It is taken back out here -- mutating the
    caller's dict, which is the per-slot copy ``ReaderConf`` built for
    exactly this -- so no format ever sees a key its own options do not
    define.

    A sheet under any other format means a fragment survived onto a path
    that turned out not to be a workbook: the first point that knows what
    ``AUTO`` resolved to is this one, so it is also the first that can say
    so rather than quietly discard it.
    """
    sheet = cfg.pop(EXCEL_SHEET_KEY, None) if cfg is not None else None
    if sheet is not None and fmt is not SrcFileFormat.EXCEL:
        raise FileFormatErrorCode.FILE_FORMAT_5.exception(sheet=sheet, format=fmt.value)
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
    if fmt is SrcFileFormat.EXCEL:
        # No fragment means the first sheet, which is what the binary
        # defaults to as well. ``has_header`` is a real user option, so it
        # is read from what is left of ``cfg`` once the carrier is gone.
        return TransporterExcelFormat(
            sheet=1 if sheet is None else sheet,
            has_header=bool((cfg or {}).get("has_header", True)),
        )
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
