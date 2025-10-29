#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

import polars as pl

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._constants as td_constants
from tabsdata._tabsserver.function.global_utils import convert_uri_to_path
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
            TableFrameProperties.builder()
            .with_execution(table.execution_id)
            .with_transaction(table.transaction_id)
            .with_version(table.table_data_version_id)
            .with_timestamp(table.triggered_on)
            .build()
        )
        # noinspection PyProtectedMember
        tf = TableFrame.__build__(
            df=lf,
            mode="tab",
            idx=table.input_idx,
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
    storage_options = {}
    uri = location.uri
    if prefix := location.env_prefix:
        logger.debug(f"Using prefix '{prefix}' for storage options.")
        storage_options = execution_context.mount_options.get_options_for_prefix(prefix)
    if uri.startswith("file://"):
        uri = convert_uri_to_path(uri)
    logger.debug(f"Scanning LazyFrame from location: '{uri}'")
    lf = pl.scan_parquet(uri, storage_options=storage_options)
    logger.debug("LazyFrame scanned successfully.")
    return lf


def sink_lf_to_location(
    lf: pl.LazyFrame,
    execution_context: ExecutionContext,
    location: Location,
):
    if location.uri is None:
        raise ValueError(
            "Location URI must be specified to sink a LazyFrame. Got "
            f"location '{location}' instead."
        )
    storage_options = {}
    if prefix := location.env_prefix:
        logger.debug(f"Using prefix '{prefix}' for storage options.")
        storage_options = execution_context.mount_options.get_options_for_prefix(prefix)
    uri = location.uri
    if uri.startswith("file://"):
        uri = convert_uri_to_path(uri)
        try:
            logger.debug("Creating the folders before sinking")
            logger.debug(f"Location for sinking: '{uri}'")
            uri_folder = os.path.dirname(uri)
            logger.debug(f"Folders to create: '{uri_folder}'")
            os.makedirs(
                uri_folder,
                exist_ok=True,
            )
        except Exception as e:
            logger.warning(
                f"Error creating the folder for the raw data with uri '{uri}': {e}"
            )
    logger.debug(f"Sinking LazyFrame to location: '{uri}'")
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
    lf.sink_parquet(uri, storage_options=storage_options, maintain_order=True)
    logger.debug("LazyFrame sunk successfully.")
    return
