#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import gzip
import json
import logging
import re
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Literal

import polars as pl

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.salesforce import (
    SalesforceReportSrc,
    SalesforceSrc,
    SalesforceSrcConn,
    SalesforceTokenCredentials,
)
from tabsdatak.conn.salesforce.constants import (
    SALESFORCE_META_LAST_MODIFIED,
    SALESFORCE_META_REPORT_ID,
    SALESFORCE_META_SOQL,
)
from tabsdatak.conn.salesforce.error import (
    SalesforceErrorCode,
    SalesforceRuntimeException,
)
from tabsdatak.spi import FrameMetadata, LazyFrameInput, MetadataSpec

logger = logging.getLogger(__name__)


# Salesforce REST API datetime format (with informed timezone).
_SF_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# Default modstamp column used by the SrcPlugin when the user's SOQL
# selects it (mirrors the legacy plugin's convention).
_SF_LAST_MODIFIED_COLUMN = "SystemModstamp"

# Token replaced inside SOQL queries with the current "max last modified"
# value (carried across runs by the framework's initial-values state).
_SF_LAST_MODIFIED_TOKEN = "$lastModified"

# Salesforce Analytics REST API keys.
_SFR_COLUMN_NAME_KEY = "columnName"
_SFR_DATATYPE_KEY = "dataType"
_SFR_DEFAULT_VALUE_FIELD = "dataCells.item.label"
_SFR_DETAIL_COLUMN_INFO_KEY = "detailColumnInfo"
_SFR_DETAIL_COLUMNS_KEY = "detailColumns"
_SFR_LABEL_KEY = "label"
_SFR_REPORT_EXTENDED_METADATA_KEY = "reportExtendedMetadata"
_SFR_REPORT_METADATA_KEY = "reportMetadata"
_SFR_VALID_FORMATS = ["Tabular"]

# Salesforce report-id pattern: 15 or 18 chars, base62-ish. Used to
# guess find_report_by when the user didn't set it.
_SFR_ID_PATTERN = r"[a-zA-Z0-9]{15}|[a-zA-Z0-9]{18}"

# Salesforce dataCell value field for each report column type. ``string``
# uses the label since the API returns the formatted display value there;
# numeric / temporal use the raw value.
_SFR_TYPE_TO_VALUE_FIELD = {
    "boolean": "dataCells.item.value",
    "date": "dataCells.item.value",
    "datetime": "dataCells.item.value",
    "double": "dataCells.item.value",
    "int": "dataCells.item.value",
    "long": "dataCells.item.value",
    "string": "dataCells.item.label",
    "time": "dataCells.item.value",
    "url": "dataCells.item.value",
}

_SFR_CHUNK_ROWS = 50000

_SFR_EPOCH_DAY = "1970-01-01T"

# Codes that already carry a precise diagnosis of a report read: re-raised
# as they are instead of wrapped in SALESFORCE_16.
_SFR_PASSTHROUGH = {
    SalesforceErrorCode.SALESFORCE_9,
    SalesforceErrorCode.SALESFORCE_12,
    SalesforceErrorCode.SALESFORCE_13,
    SalesforceErrorCode.SALESFORCE_14,
    SalesforceErrorCode.SALESFORCE_15,
    SalesforceErrorCode.SALESFORCE_19,
    SalesforceErrorCode.SALESFORCE_20,
    SalesforceErrorCode.SALESFORCE_21,
}
_SFR_FILTER_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_SFR_GREATER_THAN = "greaterThan"
_SFR_ALL_DATA_KEY = "allData"
_SFR_CONNECT_SECONDS = 30
_SFR_READ_SECONDS = 300

# What an aggregate SOQL query returns its rows as; it has no description.
_SF_AGGREGATE_RESULT = "AggregateResult"
_SF_SIMPLE_QUERY = re.compile(
    r"\s*SELECT\s+(?P<fields>.+?)\s+FROM\s+(?P<sobject>\w+)\b",
    re.IGNORECASE | re.DOTALL,
)
_SF_PLAIN_FIELD = re.compile(r"\w+")
_SF_MAX_DECIMAL_PRECISION = 38

# A described field's dtype, by its Salesforce type, as its JSON value
# decodes to it. Currency and percent are decimals of their own precision.
_SF_FIELD_TYPES: dict[str, pl.DataType] = {
    "boolean": pl.Boolean(),
    "int": pl.Int64(),
    "long": pl.Int64(),
    "double": pl.Float64(),
    "date": pl.Date(),
    "datetime": pl.Datetime("ns", "UTC"),
    "id": pl.String(),
    "reference": pl.String(),
    "string": pl.String(),
    "textarea": pl.String(),
    "picklist": pl.String(),
    "multipicklist": pl.String(),
    "combobox": pl.String(),
    "email": pl.String(),
    "phone": pl.String(),
    "url": pl.String(),
    "encryptedstring": pl.String(),
    "base64": pl.String(),
    "time": pl.String(),
}


# --- Salesforce client login ----------------------------------------------


def login(conn: SalesforceSrcConn, api_version: str | None = None) -> Any:
    """Create a ``simple_salesforce.Salesforce`` client from the conn's
    credentials. Wraps simple-salesforce auth errors as
    ``SalesforceErrorCode.SALESFORCE_10``.

    ``api_version`` overrides the default API version.
    """
    from simple_salesforce import Salesforce, api as sf_api

    credentials = conn.credentials
    if not isinstance(credentials, SalesforceTokenCredentials):
        raise SalesforceErrorCode.SALESFORCE_12.exception(
            kind=type(credentials).__name__,
        )
    instance_url = conn._instance_url() or None
    version = api_version or sf_api.DEFAULT_API_VERSION
    try:
        return Salesforce(
            username=_convert(credentials.username, str),
            password=_convert(credentials.password, str),
            security_token=_convert(credentials.security_token, str),
            instance_url=instance_url,
            version=version,
        )
    except Exception as e:
        raise SalesforceErrorCode.SALESFORCE_9.exception(
            cause=e,
            instance_url=instance_url,
        )


class Client:
    def __init__(self, conn: SalesforceSrcConn, api_version: str | None = None) -> None:
        self.conn = conn
        self.api_version = api_version
        self.sf: Any = None

    def __call__(self) -> Any:
        if self.sf is None:
            self.sf = login(self.conn, api_version=self.api_version)
        return self.sf


# --- SOQL source ----------------------------------------------------------


def read_soql_src(
    client: Client,
    src: SalesforceSrc,
    drop_metadata: bool = False,
) -> list[Iterable[LazyFrameInput]]:
    """One input per SOQL query in ``src``, each holding the query's
    records as JSON lines, read only when the input is landed. Each carries
    the query it ran and the last-modified cutoff as metadata -- or no
    metadata at all when ``drop_metadata`` is set (the source's
    ``tabsdata.src_metadata.drop`` config key).

    Updates ``src.initial_last_modified`` is *not* attempted -- offset
    state lives in the framework's initial-values surface, which the
    connector layer doesn't touch directly yet. The token replacement
    only uses the value the framework hands us.
    """
    initial_last_modified = src.initial_last_modified
    # Refuse to run a query that mentions $lastModified unless the
    # caller supplied an initial value -- the legacy contract.
    for i, query in enumerate(src.queries):
        if _SF_LAST_MODIFIED_TOKEN in query and initial_last_modified is None:
            raise SalesforceErrorCode.SALESFORCE_11.exception(query_index=i)
    return [
        [
            _soql_input(
                client=client,
                index=i,
                query=_substituted(query, initial_last_modified),
                include_deleted=src.include_deleted,
                initial_last_modified=initial_last_modified,
                drop_metadata=drop_metadata,
            )
        ]
        for i, query in enumerate(src.queries)
    ]


def _substituted(query: str, initial_last_modified: datetime | None) -> str:
    if _SF_LAST_MODIFIED_TOKEN not in query:
        return query
    assert initial_last_modified is not None  # guarded by read_soql_src
    return query.replace(
        _SF_LAST_MODIFIED_TOKEN,
        _format_sf_datetime(initial_last_modified),
    )


def _soql_input(
    *,
    client: Client,
    index: int,
    query: str,
    include_deleted: bool,
    initial_last_modified: datetime | None,
    drop_metadata: bool,
) -> LazyFrameInput:
    # noinspection PyProtectedMember
    from tabsdatak.conn.common.file._read._ndjson import AutoNdjson

    sobjects = SObjects()
    describe = Describer(client)

    def opened() -> RecordLines:
        logger.debug("Executing SOQL query %d: %s", index, query)
        sf = client()
        try:
            records = sf.query_all_iter(query, include_deleted=include_deleted)
        except Exception as e:
            raise SalesforceErrorCode.SALESFORCE_10.exception(
                cause=e,
                query_index=index,
            )
        return RecordLines(records, index, sobjects)

    lines = AutoNdjson().read(opened)
    assert callable(lines.metadata)
    folded = lines.metadata
    columns: MetadataSpec | None = (
        None
        if drop_metadata
        else {
            SALESFORCE_META_SOQL: query,
            SALESFORCE_META_LAST_MODIFIED: _last_modified_metadata(
                initial_last_modified
            ),
        }
    )

    def metadata() -> FrameMetadata:
        read = folded()
        if not read.schema:
            schema = queried_schema(query, describe)
            if schema is None:
                return replace(read, columns=columns, known=False)
            return replace(read, schema=schema, columns=columns)
        schema = described_schema(read.schema, sobjects, describe)
        return replace(read, schema=schema, columns=columns)

    return LazyFrameInput(lines.frame, metadata=metadata)


class SObjects:
    """The sObject at each path of a query's records, as the records name it
    in their ``attributes`` -- the queried object at the top, a parent's under
    its relationship field."""

    def __init__(self) -> None:
        self.by_path: dict[tuple[str, ...], str] = {}

    def observe(self, record: dict, path: tuple[str, ...] = ()) -> None:
        kind = (record.get("attributes") or {}).get("type")
        if isinstance(kind, str):
            self.by_path.setdefault(path, kind)
        for key, value in record.items():
            if isinstance(value, dict) and "attributes" in value:
                self.observe(value, (*path, key))


class Describer:
    """The dtype of each field of an sObject, from its ``describe()``, once per
    sObject. One that cannot be described has its fields typed from their
    values, with a warning."""

    def __init__(self, client: Client) -> None:
        self.client = client
        self.described: dict[str, dict[str, pl.DataType]] = {}

    def __call__(self, sobject: str) -> dict[str, pl.DataType]:
        if sobject not in self.described:
            self.described[sobject] = self._describe(sobject)
        return self.described[sobject]

    def _describe(self, sobject: str) -> dict[str, pl.DataType]:
        if sobject == _SF_AGGREGATE_RESULT:
            return {}
        try:
            described = self.client().restful(f"sobjects/{sobject}/describe")
        except Exception:  # noqa: BLE001 -- the values still type the fields
            logger.warning(
                "Cannot describe the Salesforce object %r; its fields are typed"
                " from their values",
                sobject,
                exc_info=True,
            )
            return {}
        dtypes = {}
        for field in (described or {}).get("fields", []):
            dtype = field_dtype(field)
            if dtype is not None:
                dtypes[field["name"]] = dtype
        return dtypes


def field_dtype(field: dict) -> pl.DataType | None:
    """The dtype of a described field, as its JSON value decodes to it. A time
    is left to its text, which Polars cannot decode as a time; a compound or
    untyped field is left to its values."""
    kind = field.get("type")
    if kind in ("currency", "percent"):
        precision, scale = field.get("precision"), field.get("scale")
        if isinstance(precision, int) and isinstance(scale, int):
            if 0 < precision <= _SF_MAX_DECIMAL_PRECISION and 0 <= scale <= precision:
                return pl.Decimal(precision, scale)
        return pl.Float64()
    return _SF_FIELD_TYPES.get(kind)


def queried_schema(
    query: str, describe: Callable[[str], dict[str, pl.DataType]]
) -> pl.Schema | None:
    """The schema of a query that returned no records, as the describe of the
    sObject it selects from types each of its fields, or ``None`` when that
    cannot be known: a field other than a plain one of that sObject, or one
    its describe does not type."""
    match = _SF_SIMPLE_QUERY.match(query)
    if match is None:
        return None
    names = [name.strip() for name in match["fields"].split(",")]
    if not all(_SF_PLAIN_FIELD.fullmatch(name) for name in names):
        return None
    described = {
        name.lower(): (name, dtype)
        for name, dtype in describe(match["sobject"]).items()
    }
    fields = [described.get(name.lower()) for name in names]
    if any(field is None for field in fields):
        return None
    return pl.Schema(fields)


def described_schema(
    schema: pl.Schema | None,
    sobjects: SObjects,
    describe: Callable[[str], dict[str, pl.DataType]],
) -> pl.Schema | None:
    """``schema`` with each field of a described sObject typed as described,
    a parent's fields included, and every other field as its values typed
    it."""
    if schema is None or not sobjects.by_path:
        return schema
    return pl.Schema(_described_fields(dict(schema), (), sobjects, describe))


def _described_fields(
    fields: dict[str, pl.DataType],
    path: tuple[str, ...],
    sobjects: SObjects,
    describe: Callable[[str], dict[str, pl.DataType]],
) -> dict[str, pl.DataType]:
    sobject = sobjects.by_path.get(path)
    described = describe(sobject) if sobject else {}
    typed = {}
    for name, dtype in fields.items():
        nested = (*path, name)
        if isinstance(dtype, pl.Struct) and nested in sobjects.by_path:
            inner = {field.name: field.dtype for field in dtype.fields}
            typed[name] = pl.Struct(
                _described_fields(inner, nested, sobjects, describe)
            )
        else:
            typed[name] = described.get(name, dtype)
    return typed


class RecordLines:
    """The records of a SOQL query as UTF-8 JSON lines, one record per
    line, fetched page by page as they are read; the sObjects they hold are
    noted in ``sobjects``."""

    def __init__(
        self, records: Iterator[dict], index: int, sobjects: SObjects | None = None
    ) -> None:
        self.records = records
        self.index = index
        self.sobjects = sobjects if sobjects is not None else SObjects()
        self.buffer = bytearray()

    def read(self, size: int = -1) -> bytes:
        while size < 0 or len(self.buffer) < size:
            try:
                record = next(self.records, None)
            except Exception as e:
                raise SalesforceErrorCode.SALESFORCE_10.exception(
                    cause=e,
                    query_index=self.index,
                )
            if record is None:
                break
            self.sobjects.observe(record)
            self.buffer += json.dumps(
                _without_attributes(record),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            self.buffer += b"\n"
        taken = len(self.buffer) if size < 0 else min(size, len(self.buffer))
        data = bytes(self.buffer[:taken])
        del self.buffer[:taken]
        return data


def _without_attributes(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_attributes(item)
            for key, item in value.items()
            if key != "attributes"
        }
    if isinstance(value, list):
        return [_without_attributes(item) for item in value]
    return value


def _format_sf_datetime(dt: datetime) -> str:
    if dt.tzinfo is None:
        # Force UTC for naive datetimes so simple-salesforce never sees
        # a tz-less timestamp (the API rejects those).
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime(_SF_DATE_FORMAT)


def _last_modified_metadata(dt: datetime | None) -> str:
    """The last-modified cutoff as it goes into a query, or empty when
    the source declares none. Always reported so the metadata does not
    change shape between an incremental and a full read."""
    return _format_sf_datetime(dt) if dt is not None else ""


# --- Report source -------------------------------------------------------


def read_report_src(
    client: Client,
    src: SalesforceReportSrc,
    drop_metadata: bool = False,
) -> list[Iterable[LazyFrameInput]]:
    """One input per Salesforce report in ``src``, each run only when the
    input is read and handed over in typed batches, carrying the resolved
    report id and the last-modified cutoff as metadata -- or no metadata at
    all when ``drop_metadata`` is set (the source's
    ``tabsdata.src_metadata.drop`` config key)."""
    return [
        _report_inputs(
            client=client,
            index=i,
            report=report,
            src=src,
            drop_metadata=drop_metadata,
        )
        for i, report in enumerate(src.reports)
    ]


def _report_inputs(
    *,
    client: Client,
    index: int,
    report: str,
    src: SalesforceReportSrc,
    drop_metadata: bool,
) -> Iterator[LazyFrameInput]:
    try:
        yield from _run_one_report(
            sf=client(),
            report=report,
            src=src,
            drop_metadata=drop_metadata,
        )
    except SalesforceRuntimeException as e:
        if e.api_error in _SFR_PASSTHROUGH:
            raise
        raise SalesforceErrorCode.SALESFORCE_16.exception(
            cause=e,
            report_index=index,
        )
    except Exception as e:
        raise SalesforceErrorCode.SALESFORCE_16.exception(
            cause=e,
            report_index=index,
        )


def _run_one_report(
    *,
    sf: Any,
    report: str,
    src: SalesforceReportSrc,
    drop_metadata: bool,
) -> Iterator[LazyFrameInput]:
    report_id = _resolve_report_id(sf, report, src.find_report_by)
    (
        col_sys_order,
        reverse_lookup,
        ordered_col_types,
        col_sys_meta,
    ) = _obtain_report_metadata(sf, report_id, report)

    sf_filters, filter_logic = _report_filters(src, report_id, reverse_lookup)
    instance_id = _launch_report_instance(
        sf=sf,
        report_id=report_id,
        filters=sf_filters,
        filter_logic=filter_logic,
    )
    response = _await_report_stream(
        sf=sf,
        report_id=report_id,
        instance_id=instance_id,
    )
    columns: MetadataSpec | None = (
        None
        if drop_metadata
        else {
            SALESFORCE_META_REPORT_ID: report_id,
            SALESFORCE_META_LAST_MODIFIED: _last_modified_metadata(
                src.initial_last_modified
            ),
        }
    )
    metadata = None if columns is None else FrameMetadata(columns=columns)
    # Translate the system column names to the requested strategy.
    column_names = [
        col_sys_meta[col][src.column_name_strategy] for col in col_sys_order
    ]
    try:
        for batch in _report_batches(
            report_id=report_id,
            raw_stream=_report_body(response),
            ordered_col_types=ordered_col_types,
            column_names=column_names,
        ):
            yield LazyFrameInput(batch.lazy(), metadata=metadata)
    finally:
        response.close()


def _report_filters(
    src: SalesforceReportSrc,
    report_id: str,
    reverse_lookup: dict[str, dict[str, str]],
) -> tuple[list[tuple[str, str, str]] | None, str | None]:
    """The report filters to run with, in the report's own column names, and
    their boolean logic. The last-modified cutoff becomes one more filter,
    ANDed onto whatever logic the user gave."""
    names = reverse_lookup[src.column_name_strategy]

    def column(name: str) -> str:
        if name not in names:
            raise SalesforceErrorCode.SALESFORCE_19.exception(
                report_id=report_id,
                column=name,
                strategy=src.column_name_strategy,
            )
        return names[name]

    filters: list[tuple[str, str, str]] = [
        (column(name), operator, value) for name, operator, value in src.filter or []
    ]
    logic = src.filter_logic
    if src.last_modified_column and src.initial_last_modified is not None:
        filters.append((
            column(src.last_modified_column),
            _SFR_GREATER_THAN,
            _report_datetime(src.initial_last_modified),
        ))
        if logic:
            logic = f"({logic}) AND {len(filters)}"
    return (filters or None), logic


def _report_datetime(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime(_SFR_FILTER_DATE_FORMAT)


def _resolve_report_id(
    sf: Any,
    report: str,
    find_by: Literal["id", "name"] | None,
) -> str:
    import re

    if find_by is None:
        # Inferred lookup -- try both id and name and disambiguate.
        looks_like_id = bool(re.fullmatch(_SFR_ID_PATTERN, report))
        id_match = _lookup_report(sf, report, "id") if looks_like_id else None
        name_match = _lookup_report(sf, report, "name")
        if id_match and name_match and id_match != name_match:
            raise SalesforceErrorCode.SALESFORCE_14.exception(report=report)
        report_id = id_match or name_match
    else:
        report_id = _lookup_report(sf, report, find_by)
    if not report_id:
        raise SalesforceErrorCode.SALESFORCE_13.exception(
            find_by=find_by or "auto",
            report=report,
        )
    return report_id


def _lookup_report(
    sf: Any,
    report: str,
    find_by: Literal["id", "name"],
) -> str | None:
    from simple_salesforce import format_soql

    soql = """
        select Id, Format
          from Report
         where ( DeveloperName in {name} or Id in {id} )
           and IsDeleted = false
    """
    id_arg = [report] if find_by == "id" else [""]
    name_arg = [report] if find_by == "name" else [""]
    soql = format_soql(soql, id=id_arg, name=name_arg)
    results = sf.query(soql)
    records = results.get("records", [])
    if len(records) > 1:
        # Multiple matches -- caller can disambiguate by passing the id.
        return None
    if not records:
        return None
    record = records[0]
    if record.get("Format") not in _SFR_VALID_FORMATS:
        raise SalesforceErrorCode.SALESFORCE_15.exception(
            report=report,
            format=record.get("Format"),
        )
    return record["Id"]


def _obtain_report_metadata(
    sf: Any,
    report_id: str,
    report_name: str,
) -> tuple[list[str], dict[str, dict[str, str]], list[str], dict[str, dict[str, str]]]:
    url = f"{sf.base_url}analytics/reports/{report_id}/describe"
    response = sf.session.get(
        url, headers=sf.headers, timeout=(_SFR_CONNECT_SECONDS, _SFR_READ_SECONDS)
    )
    response.raise_for_status()
    s = response.json()

    col_sys_order: list[str] = s[_SFR_REPORT_METADATA_KEY][_SFR_DETAIL_COLUMNS_KEY]
    reverse_lookup: dict[str, dict[str, str]] = {
        _SFR_LABEL_KEY: {},
        _SFR_COLUMN_NAME_KEY: {},
    }
    col_sys_meta: dict[str, dict[str, str]] = {}
    for col_sys_name, value in s[_SFR_REPORT_EXTENDED_METADATA_KEY][
        _SFR_DETAIL_COLUMN_INFO_KEY
    ].items():
        datatype = value.get(_SFR_DATATYPE_KEY)
        label = value.get(_SFR_LABEL_KEY)
        reverse_lookup[_SFR_LABEL_KEY][label] = col_sys_name
        reverse_lookup[_SFR_COLUMN_NAME_KEY][col_sys_name] = col_sys_name
        col_sys_meta[col_sys_name] = {
            _SFR_LABEL_KEY: label,
            _SFR_DATATYPE_KEY: datatype,
            _SFR_COLUMN_NAME_KEY: col_sys_name,
        }
    ordered_col_types = [col_sys_meta[col][_SFR_DATATYPE_KEY] for col in col_sys_order]
    return col_sys_order, reverse_lookup, ordered_col_types, col_sys_meta


def _launch_report_instance(
    *,
    sf: Any,
    report_id: str,
    filters: Sequence[tuple[str, str, str]] | None,
    filter_logic: str | None,
) -> str:
    url = f"analytics/reports/{report_id}/instances"
    if filters:
        constructed_filters = [
            {"column": c, "operator": o, "value": v} for c, o, v in filters
        ]
        body = {
            _SFR_REPORT_METADATA_KEY: {
                "reportFormat": "TABULAR",
                "reportBooleanFilter": filter_logic,
                "reportFilters": constructed_filters,
            }
        }
    else:
        body = {
            _SFR_REPORT_METADATA_KEY: {
                "reportFormat": "TABULAR",
                "reportFilters": [],
            }
        }
    result = sf.restful(url, method="POST", json=body)
    if not result:
        raise RuntimeError(f"Failed to launch report instance for {report_id}")
    return result["id"]


def _await_report_stream(
    *,
    sf: Any,
    report_id: str,
    instance_id: str,
    maximum_wait_time: int = 600,
    poll_interval: int = 5,
) -> Any:
    import requests

    url = f"analytics/reports/{report_id}/instances/{instance_id}"
    deadline = time.monotonic() + maximum_wait_time
    while True:
        response = sf.restful(url)
        status = response.get("attributes", {}).get("status")
        if status == "Success":
            break
        if status == "Error":
            msg = response.get("attributes", {}).get("errorMessage", "<unknown>")
            raise RuntimeError(f"Report generation failed: {msg}")
        if time.monotonic() >= deadline:
            raise SalesforceErrorCode.SALESFORCE_20.exception(
                report_id=report_id, seconds=maximum_wait_time
            )
        time.sleep(poll_interval)

    result = requests.get(
        f"{sf.base_url}{url}",
        headers={"Authorization": f"Bearer {sf.session_id}"},
        stream=True,
        timeout=(_SFR_CONNECT_SECONDS, _SFR_READ_SECONDS),
    )
    try:
        result.raise_for_status()
    except BaseException:
        result.close()
        raise
    return result


def _report_body(response: Any) -> Any:
    if response.headers.get("content-encoding") == "gzip":
        return gzip.GzipFile(fileobj=response.raw)
    return response.raw


def _report_batches(
    *,
    report_id: str,
    raw_stream: Any,
    ordered_col_types: list[str],
    column_names: list[str],
    chunk_size: int = _SFR_CHUNK_ROWS,
) -> Iterator[pl.DataFrame]:
    import ijson

    chunk: list[list[Any]] = []
    current_row: list[Any] = []
    current_value: Any = None
    yielded = False

    parser = ijson.parse(raw_stream)
    for prefix, event, value in parser:
        if prefix == _SFR_ALL_DATA_KEY and value is False:
            raise SalesforceErrorCode.SALESFORCE_21.exception(report_id=report_id)
        # Compute current pointers (column type + matching value field).
        if len(current_row) < len(ordered_col_types):
            current_type = ordered_col_types[len(current_row)]
            value_field = _SFR_TYPE_TO_VALUE_FIELD.get(
                current_type, _SFR_DEFAULT_VALUE_FIELD
            )
        else:
            current_type = None
            value_field = None

        if prefix.endswith("rows.item") and event == "start_map":
            current_row = []
        elif prefix.endswith("rows.item") and event == "end_map":
            chunk.append(current_row.copy())

        if current_type is not None:
            if prefix.endswith("dataCells.item") and event == "end_map":
                current_row.append(current_value)
                current_value = None
            elif prefix.endswith(value_field):
                current_value = value if value != "-" else None

        if len(chunk) >= chunk_size:
            yield _typed(chunk, ordered_col_types, column_names)
            yielded = True
            chunk = []

    if chunk or not yielded:
        yield _typed(chunk, ordered_col_types, column_names)


def _typed(
    rows: list[list[Any]],
    ordered_col_types: list[str],
    column_names: list[str],
) -> pl.DataFrame:
    columns = list(zip(*rows)) if rows else [() for _ in column_names]
    return pl.DataFrame([
        _column(name, kind, values)
        for name, kind, values in zip(column_names, ordered_col_types, columns)
    ])


def _column(name: str, kind: str, values: Iterable[Any]) -> pl.Series:
    text = pl.Series(
        name,
        [None if value is None else str(value) for value in values],
        dtype=pl.String,
    )
    if kind in ("int", "long"):
        return text.cast(pl.Int64)
    if kind == "double":
        return text.cast(pl.Float64)
    if kind == "boolean":
        return text.str.to_lowercase().replace_strict(
            {"true": True, "false": False},
            return_dtype=pl.Boolean,
        )
    if kind in ("date", "datetime"):
        return _instant(text)
    if kind == "time":
        return _instant((_SFR_EPOCH_DAY + text.str.strip_suffix("Z")).alias(name))
    return text


def _instant(text: pl.Series) -> pl.Series:
    if text.null_count() == text.len():
        return text.cast(pl.Datetime("ns", "UTC"))
    parsed = text.str.to_datetime(time_unit="ns")
    if isinstance(parsed.dtype, pl.Datetime) and parsed.dtype.time_zone is None:
        return parsed.dt.replace_time_zone("UTC")
    return parsed.dt.convert_time_zone("UTC")
