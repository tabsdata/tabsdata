from __future__ import annotations

import csv
import gzip
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

# noinspection PyProtectedMember
from tabsdatak.conn.common.types import _convert
from tabsdatak.conn.salesforce import (
    SalesforceSrcConn,
    SalesforceReportSrc,
    SalesforceSrc,
    SalesforceTokenCredentials,
)
from tabsdatak.conn.salesforce.error import SalesforceErrorCode

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
    "date": "dataCells.item.value",
    "datetime": "dataCells.item.value",
    "double": "dataCells.item.value",
    "int": "dataCells.item.value",
    "long": "dataCells.item.value",
    "string": "dataCells.item.label",
    "time": "dataCells.item.value",
    "url": "dataCells.item.value",
}

_SFR_TYPE_TO_PANDAS_TYPE = {
    "boolean": "bool",
    "date": "datetime64[ns, UTC]",
    "datetime": "datetime64[ns, UTC]",
    "double": "float64",
    "int": "int64",
    "long": "int64",
    "string": "string",
    "time": "datetime64[ns, UTC]",
    "url": "string",
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
    instance_url = (
        _convert(conn.instance_url, str) if conn.instance_url else None
    )
    version = api_version or sf_api.DEFAULT_API_VERSION
    try:
        return Salesforce(
            username=credentials.username.value(),
            password=credentials.password.value(),
            security_token=credentials.security_token.value(),
            instance_url=instance_url,
            version=version,
        )
    except Exception as e:
        raise SalesforceErrorCode.SALESFORCE_9.exception(
            cause=e,
            instance_url=instance_url,
        )


# --- SOQL source ----------------------------------------------------------


def read_soql_src(
    sf: Any,
    src: SalesforceSrc,
    work_dir: Path,
) -> list[list[Path]]:
    """Execute every SOQL query in ``src`` against the open Salesforce
    client ``sf`` and return one ``TableMode.SINGLE``-shaped output per
    query (each inner list has exactly one parquet file).

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

    outputs: list[list[Path]] = []
    for i, query in enumerate(src.queries):
        outputs.append(
            _run_one_soql(
                sf=sf,
                index=i,
                query=query,
                include_deleted=src.include_deleted,
                initial_last_modified=initial_last_modified,
                work_dir=work_dir,
            )
        )
    return outputs


def _run_one_soql(
    *,
    sf: Any,
    index: int,
    query: str,
    include_deleted: bool,
    initial_last_modified: datetime | None,
    work_dir: Path,
) -> list[Path]:
    import polars as pl

    # Token replacement: the legacy contract is that the value is a
    # Salesforce-formatted datetime string. We accept ``datetime`` and
    # serialise it ourselves; callers passing a string were never
    # supported on the new dataclass (its field is ``datetime | None``).
    if _SF_LAST_MODIFIED_TOKEN in query:
        assert initial_last_modified is not None  # guarded above
        query = query.replace(
            _SF_LAST_MODIFIED_TOKEN,
            _format_sf_datetime(initial_last_modified),
        )

    logger.debug("Executing SOQL query %d: %s", index, query)

    csv_path = work_dir / f"{index}.csv"
    parquet_path = work_dir / f"{index}.parquet"

    try:
        records = sf.query_all_iter(query, include_deleted=include_deleted)
    except Exception as e:
        raise SalesforceErrorCode.SALESFORCE_10.exception(
            cause=e,
            query_index=index,
        )

    try:
        writer = None
        with open(csv_path, "w", newline="") as f:
            for rec in records:
                rec.pop("attributes", None)
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=list(rec.keys()))
                    writer.writeheader()
                writer.writerow(rec)
    except Exception as e:
        raise SalesforceErrorCode.SALESFORCE_10.exception(
            cause=e,
            query_index=index,
        )

    if writer is None:
        # No records at all -- emit an empty parquet so the downstream
        # always sees a file. Polars cannot infer a schema from zero
        # rows so we produce an explicit empty DataFrame.
        pl.DataFrame().write_parquet(parquet_path)
    else:
        try:
            pl.scan_csv(csv_path, raise_if_empty=True).sink_parquet(parquet_path)
        except pl.exceptions.NoDataError:
            pl.DataFrame().write_parquet(parquet_path)
    return [parquet_path]


def _format_sf_datetime(dt: datetime) -> str:
    if dt.tzinfo is None:
        # Force UTC for naive datetimes so simple-salesforce never sees
        # a tz-less timestamp (the API rejects those).
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime(_SF_DATE_FORMAT)


# --- Report source -------------------------------------------------------


def read_report_src(
    sf: Any,
    src: SalesforceReportSrc,
    work_dir: Path,
) -> list[list[Path]]:
    """Run every Salesforce report in ``src`` and return per-report
    parquet outputs."""
    outputs: list[list[Path]] = []
    for i, report in enumerate(src.reports):
        outputs.append(
            _run_one_report(
                sf=sf,
                index=i,
                report=report,
                src=src,
                work_dir=work_dir,
            )
        )
    return outputs


def _run_one_report(
    *,
    sf: Any,
    index: int,
    report: str,
    src: SalesforceReportSrc,
    work_dir: Path,
) -> list[Path]:
    from tabsdatak.conn.salesforce.error import SalesforceRuntimeException

    # Codes that already carry a precise diagnosis: re-raise as-is so
    # the user keeps the original signal instead of getting the
    # generic SALESFORCE_16 wrapper.
    _passthrough = {
        SalesforceErrorCode.SALESFORCE_13,
        SalesforceErrorCode.SALESFORCE_14,
        SalesforceErrorCode.SALESFORCE_15,
    }
    try:
        report_id = _resolve_report_id(sf, report, src.find_report_by)
        (
            col_sys_order,
            reverse_lookup,
            ordered_col_types,
            col_sys_meta,
        ) = _obtain_report_metadata(sf, report_id, report)

        # Apply the user's filter list -- map user-supplied column
        # references through the requested naming strategy.
        sf_filters = None
        if src.filter:
            sf_filters = [
                (
                    reverse_lookup[src.column_name_strategy][column],
                    operator,
                    value,
                )
                for column, operator, value in src.filter
            ]

        instance_id = _launch_report_instance(
            sf=sf,
            report_id=report_id,
            filters=sf_filters,
            filter_logic=src.filter_logic,
        )
        raw_stream = _await_report_stream(
            sf=sf,
            report_id=report_id,
            instance_id=instance_id,
        )

        # Translate the system column names to the requested strategy.
        column_names = [
            col_sys_meta[col][src.column_name_strategy] for col in col_sys_order
        ]

        parquet_path = _stream_report_to_parquet(
            raw_stream=raw_stream,
            index=index,
            work_dir=work_dir,
            ordered_col_types=ordered_col_types,
            column_names=column_names,
        )
    except SalesforceRuntimeException as e:
        if e.api_error in _passthrough:
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
    return [parquet_path]


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
    response = sf.session.get(url, headers=sf.headers)
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
    filters: list[tuple[str, str, str]] | None,
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
    start = time.time()
    while time.time() - start < maximum_wait_time:
        response = sf.restful(url)
        status = response.get("attributes", {}).get("status")
        if status == "Success":
            break
        if status == "Error":
            msg = response.get("attributes", {}).get("errorMessage", "<unknown>")
            raise RuntimeError(f"Report generation failed: {msg}")
        time.sleep(poll_interval)

    resp = requests.get(
        f"{sf.base_url}{url}",
        headers={"Authorization": f"Bearer {sf.session_id}"},
        stream=True,
    )
    raw = resp.raw
    if resp.headers.get("content-encoding") == "gzip":
        raw = gzip.GzipFile(fileobj=resp.raw)
    return raw


def _stream_report_to_parquet(
    *,
    raw_stream: Any,
    index: int,
    work_dir: Path,
    ordered_col_types: list[str],
    column_names: list[str],
    chunk_size: int = 50000,
) -> Path:
    import ijson
    import pandas as pd

    destination = work_dir / f"{index}.parquet"
    chunk: list[list[Any]] = []
    current_row: list[Any] = []
    current_value: Any = None
    first_chunk = True
    stored_any = False

    parser = ijson.parse(raw_stream)
    for prefix, event, value in parser:
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
            _sink_chunk(
                chunk=chunk,
                column_names=column_names,
                ordered_col_types=ordered_col_types,
                destination=destination,
                first_chunk=first_chunk,
            )
            stored_any = True
            chunk = []
            first_chunk = False

    if chunk:
        _sink_chunk(
            chunk=chunk,
            column_names=column_names,
            ordered_col_types=ordered_col_types,
            destination=destination,
            first_chunk=first_chunk,
        )
        stored_any = True

    if not stored_any:
        # Empty result -- emit empty parquet with the expected columns.
        empty = pd.DataFrame({c: [] for c in column_names})
        empty.to_parquet(destination, engine="fastparquet", index=False)
    return destination


def _sink_chunk(
    *,
    chunk: list[list[Any]],
    column_names: list[str],
    ordered_col_types: list[str],
    destination: Path,
    first_chunk: bool,
) -> None:
    import pandas as pd

    columns = list(zip(*chunk)) or [[] for _ in column_names]
    df = pd.DataFrame(dict(zip(column_names, columns)))
    pandas_types = [
        _SFR_TYPE_TO_PANDAS_TYPE.get(t, "string") for t in ordered_col_types
    ]
    df = df.astype(dict(zip(column_names, pandas_types)))
    df.to_parquet(
        destination,
        engine="fastparquet",
        index=False,
        append=(not first_chunk),
    )
