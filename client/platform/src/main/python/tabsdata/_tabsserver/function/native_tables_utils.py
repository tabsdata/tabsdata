#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
import subprocess
import uuid
from typing import TYPE_CHECKING

import polars as pl

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._constants as td_constants
from tabsdata._tabsserver.function.global_utils import (
    CURRENT_PLATFORM,
    convert_path_to_uri,
    convert_uri_to_path,
)
from tabsdata._tabsserver.function.yaml_parsing import (
    TransporterAzure,
    TransporterGCS,
    TransporterLiteral,
    TransporterLocalFile,
    TransporterS3,
    V1CopyFormat,
    store_copy_as_yaml,
)
from tabsdata.tableframe.lazyframe.frame import TableFrame
from tabsdata.tableframe.lazyframe.properties import TableFrameProperties

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.execution_context import ExecutionContext
    from tabsdata._tabsserver.function.yaml_parsing import Location, Table

logger = logging.getLogger(__name__)


def scan_tf_from_table(
    execution_context: ExecutionContext,
    table: Table,
    fail_on_none_uri: bool = False,
) -> TableFrame | None:
    lf = scan_lf_from_location(
        execution_context,
        table.location,
        fail_on_none_uri=fail_on_none_uri,
    )
    if lf is None:
        tf = None
    else:
        properties = (
            TableFrameProperties
            .builder()
            .with_execution(table.execution_id)
            .with_transaction(table.transaction_id)
            .with_version(table.table_data_version_id)
            .with_timestamp(table.triggered_on)
            .build()
        )
        # noinspection PyProtectedMember
        tf = TableFrame.__build__(
            df=lf,
            # Loading a stored table as the input of a transformer/subscriber:
            # regenerate the pristine columns ($td.id, $td.row, $td.src) so each
            # input row is re-stamped with a fresh sequential $td.row and a
            # single-entry $td.src pointing to itself. The provenance 'tab' is this
            # input's position in the function signature, given by 'table_pos'.
            mode="mut",
            idx=table.table_pos,
            properties=properties,
        )
    return tf


def scan_lf_from_location(
    execution_context: ExecutionContext,
    location: Location,
    fail_on_none_uri: bool = False,
) -> pl.LazyFrame | None:
    if location.uri is None:
        if fail_on_none_uri:
            raise ValueError(
                "Location URI must be specified to scan a LazyFrame. Got "
                f"location '{location}' instead."
            )
        else:
            logger.debug("Location URI is None, returning None.")
            return None
    uri = location.uri
    if uri.startswith("file://"):
        uri = convert_uri_to_path(uri)
        logger.debug(f"Scanning LazyFrame from local location: '{uri}'")
        lf = pl.scan_parquet(uri)
        logger.debug("LazyFrame scanned successfully.")
        return lf
    # Cloud URI — use the transporter to download to a local file, then scan
    storage_options = {}
    if prefix := location.env_prefix:
        logger.debug(f"Using prefix '{prefix}' for storage options.")
        storage_options = execution_context.mount_options.get_options_for_prefix(prefix)
    destination_dir = execution_context.paths.output_folder
    local_file = _transport_cloud_file_to_local(uri, destination_dir, storage_options)
    logger.debug(f"Scanning LazyFrame from transported local file: '{local_file}'")
    lf = pl.scan_parquet(local_file)
    logger.debug("LazyFrame scanned successfully.")
    return lf


def _get_option(storage_options: dict, key: str, default=None) -> str:
    """Case-insensitive lookup in storage_options."""
    lower_key = key.lower()
    for k, v in storage_options.items():
        if k.lower() == lower_key:
            return v
    return default


def _build_transporter_source(
    uri: str,
    storage_options: dict,
) -> TransporterS3 | TransporterAzure | TransporterGCS:
    """Build a transporter source object from a cloud URI and storage options.

    The storage_options values are passed as TransporterLiteral credentials
    to the transporter.
    """
    if uri.startswith("s3://"):
        access_key = _get_option(storage_options, "aws_access_key_id")
        secret_key = _get_option(storage_options, "aws_secret_access_key")
        region = _get_option(storage_options, "aws_region")
        return TransporterS3(
            uri,
            access_key=TransporterLiteral(access_key) if access_key else None,
            secret_key=TransporterLiteral(secret_key) if secret_key else None,
            region=TransporterLiteral(region) if region else None,
        )
    if uri.startswith(("az://", "abfs://", "abfss://")):
        account_name = _get_option(storage_options, "account_name")
        account_key = _get_option(storage_options, "account_key")
        return TransporterAzure(
            uri,
            account_name=TransporterLiteral(account_name) if account_name else None,
            account_key=TransporterLiteral(account_key) if account_key else None,
        )
    if uri.startswith("gs://"):
        service_account_key = _get_option(storage_options, "service_account_key")
        return TransporterGCS(
            uri,
            service_account_key=TransporterLiteral(service_account_key)
            if service_account_key
            else None,
        )
    raise ValueError(f"Unsupported cloud URI scheme: '{uri}'")


def _run_transporter_copy(
    copy_pairs: list[list],
    destination_dir: str,
) -> None:
    """Run the transporter binary to execute one or more copy operations."""
    yaml_request_file = os.path.join(destination_dir, f"request_{uuid.uuid4()}.yaml")
    store_copy_as_yaml(V1CopyFormat(copy_pairs), yaml_request_file)

    binary = "td_transporter.exe" if CURRENT_PLATFORM.is_windows() else "td_transporter"
    report_file = os.path.join(destination_dir, f"report_{uuid.uuid4()}.yaml")
    logger.debug(
        f"Running transporter with {len(copy_pairs)} copy pair(s): "
        f"{binary} --request {yaml_request_file} --report {report_file}"
    )
    subprocess_result = subprocess.run(
        [binary, "--request", yaml_request_file, "--report", report_file],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="strict",
    )
    if subprocess_result.returncode != 0:
        logger.error(
            f"Error running transporter (return code "
            f"'{subprocess_result.returncode}'): {subprocess_result.stderr}"
        )
        raise Exception(
            f"Error running transporter (return code "
            f"'{subprocess_result.returncode}'): {subprocess_result.stderr}"
        )
    logger.debug("Transporter completed successfully.")


def _transport_cloud_file_to_local(
    uri: str,
    destination_dir: str,
    storage_options: dict,
) -> str:
    """Use the transporter binary to copy a cloud file to a local path."""
    source = _build_transporter_source(uri, storage_options)
    local_file = os.path.join(
        destination_dir, f"transported_{uuid.uuid4().hex[:16]}.parquet"
    )
    target = TransporterLocalFile(convert_path_to_uri(local_file))
    logger.debug(f"Transporting cloud file '{uri}' to local '{local_file}'")
    _run_transporter_copy([[source, target]], destination_dir)
    logger.debug(f"Cloud file transported successfully to '{local_file}'")
    return local_file


class PendingCloudCopies:
    """Accumulates deferred cloud sink operations for batch execution.

    When ``sink_lf_to_location`` is called with ``lazy=True`` on a cloud URI,
    the local Parquet sink is deferred and the corresponding cloud copy pair is
    stored here.  Calling :meth:`collect` materialises all local sinks and then
    runs the transporter once with every accumulated copy pair.
    """

    def __init__(self, destination_dir: str):
        self._destination_dir = destination_dir
        self._lazy_sinks: list[pl.LazyFrame] = []
        self._copy_pairs: list[list] = []

    # -- mutation ----------------------------------------------------------

    def add(
        self,
        lazy_sink_result: pl.LazyFrame | None,
        copy_pair: list,
    ) -> None:
        """Register a pending cloud copy.

        If *lazy_sink_result* is ``None`` the call is treated as a no-op so
        that callers do not need to guard against the non-lazy path.
        """
        if lazy_sink_result is None:
            return
        self._lazy_sinks.append(lazy_sink_result)
        self._copy_pairs.append(copy_pair)

    # -- query -------------------------------------------------------------

    def __bool__(self) -> bool:
        return len(self._copy_pairs) > 0

    def __len__(self) -> int:
        return len(self._copy_pairs)

    def __repr__(self) -> str:
        return (
            f"PendingCloudCopies("
            f"destination_dir={self._destination_dir!r}, "
            f"pending={len(self._copy_pairs)}, "
            f"lazy_sinks={self._lazy_sinks!r}, "
            f"copy_pairs={self._copy_pairs!r})"
        )

    def __str__(self) -> str:
        if not self:
            return (
                f"PendingCloudCopies(destination_dir={self._destination_dir!r}, "
                f"pending=0)"
            )
        lines = [
            f"PendingCloudCopies(destination_dir={self._destination_dir!r}, "
            f"pending={len(self._copy_pairs)}):"
        ]
        for i, (sink, pair) in enumerate(zip(self._lazy_sinks, self._copy_pairs)):
            lines.append(f"  [{i}] sink={sink!r}, copy_pair={pair!r}")
        return "\n".join(lines)

    # -- execution ---------------------------------------------------------

    def collect(self) -> None:
        """Execute all pending lazy sinks and transport the files to cloud."""
        if not self:
            return
        logger.debug(f"Collecting {len(self._lazy_sinks)} pending cloud sink(s).")
        for sink in self._lazy_sinks:
            sink.collect()
        logger.debug("All local sinks materialised, running batch transport.")
        _run_transporter_copy(list(self._copy_pairs), self._destination_dir)
        self._lazy_sinks.clear()
        self._copy_pairs.clear()
        logger.debug("Pending cloud copies collected successfully.")


def _drop_virtual_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Drop internal virtual columns before writing."""
    columns_to_drop = [
        column
        for column in lf.collect_schema().names()
        if any(
            column.startswith(prefix)
            for prefix in td_constants.TD_NAMESPACED_VIRTUAL_COLUMN_PREFIXES
        )
    ]
    if columns_to_drop:
        lf = lf.drop(columns_to_drop)
    return lf


def sink_lf_to_location(
    lf: pl.LazyFrame,
    execution_context: ExecutionContext,
    location: Location,
    *,
    lazy: bool = False,
) -> pl.LazyFrame | None:
    if location.uri is None:
        raise ValueError(
            "Location URI must be specified to sink a LazyFrame. Got "
            f"location '{location}' instead."
        )
    uri = location.uri
    lf = _drop_virtual_columns(lf)

    # --- Local file sink (unchanged behaviour) ----------------------------
    if uri.startswith("file://"):
        uri = convert_uri_to_path(uri)
        try:
            logger.debug("Creating the folders before sinking")
            logger.debug(f"Location for sinking: '{uri}'")
            uri_folder = os.path.dirname(uri)
            logger.debug(f"Folders to create: '{uri_folder}'")
            os.makedirs(uri_folder, exist_ok=True)
        except Exception as e:
            logger.warning(
                f"Error creating the folder for the raw data with uri '{uri}': {e}"
            )
        logger.debug(f"Sinking LazyFrame to local location: '{uri}'")
        if lazy:
            sink_result = lf.sink_parquet(uri, lazy=True, maintain_order=True)
            logger.debug("LazyFrame sink prepared successfully (lazy execution).")
            return sink_result
        lf.sink_parquet(uri, lazy=False, maintain_order=True)
        logger.debug("LazyFrame sunk successfully.")
        return None

    # --- Cloud sink via transporter ---------------------------------------
    storage_options = {}
    if prefix := location.env_prefix:
        logger.debug(f"Using prefix '{prefix}' for storage options.")
        storage_options = execution_context.mount_options.get_options_for_prefix(prefix)

    destination_dir = execution_context.paths.output_folder
    local_file = os.path.join(destination_dir, f"sink_{uuid.uuid4().hex[:16]}.parquet")
    os.makedirs(destination_dir, exist_ok=True)

    cloud_target = _build_transporter_source(uri, storage_options)
    local_source = TransporterLocalFile(convert_path_to_uri(local_file))
    copy_pair = [local_source, cloud_target]

    logger.debug(
        f"Sinking LazyFrame to cloud '{uri}' via local intermediate '{local_file}'"
    )

    if lazy:
        sink_result = lf.sink_parquet(local_file, lazy=True, maintain_order=True)
        logger.debug("LazyFrame cloud sink prepared (lazy execution).")
        if execution_context.pending_cloud_copies is None:
            execution_context.pending_cloud_copies = PendingCloudCopies(destination_dir)
        execution_context.pending_cloud_copies.add(sink_result, copy_pair)
        return None

    lf.sink_parquet(local_file, lazy=False, maintain_order=True)
    logger.debug("LazyFrame sunk to local intermediate, transporting to cloud.")
    _run_transporter_copy([copy_pair], destination_dir)
    logger.debug(f"LazyFrame sunk to cloud location '{uri}' successfully.")
    return None
