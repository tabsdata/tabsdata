#
# Copyright 2024 Tabs Data Inc.
#

import logging
from enum import Enum
from typing import Any, Type

import polars as pl

# noinspection PyProtectedMember
import tabsdata._utils.tableframe._constants as td_constants
from tabsdata._utils.tableframe._appliers import apply_constant_system_column

# noinspection PyProtectedMember
from tabsdata.extensions._features.api.features import Feature, FeaturesManager
from tabsdata.extensions._tableframe.api.api import Extension
from tabsdata.extensions._tableframe.version import version
from tabsdata.tableframe.lazyframe.properties import TableFrameProperties

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ExtendedSystemColumns(Enum):
    pass


class ExtendedSystemColumnsMetadata(Enum):
    pass


class SystemColumns(Enum):
    TD_IDENTIFIER = td_constants.StandardSystemColumns.TD_IDENTIFIER.value
    TD_VER_EXECUTION = td_constants.StandardSystemColumns.TD_VER_EXECUTION.value
    TD_VER_TRANSACTION = td_constants.StandardSystemColumns.TD_VER_TRANSACTION.value
    TD_VER_VERSION = td_constants.StandardSystemColumns.TD_VER_VERSION.value
    TD_VER_TIMESTAMP = td_constants.StandardSystemColumns.TD_VER_TIMESTAMP.value


class RequiredColumns(Enum):
    TD_IDENTIFIER = td_constants.StandardSystemColumns.TD_IDENTIFIER.value
    TD_VER_EXECUTION = td_constants.StandardSystemColumns.TD_VER_EXECUTION.value
    TD_VER_TRANSACTION = td_constants.StandardSystemColumns.TD_VER_TRANSACTION.value
    TD_VER_VERSION = td_constants.StandardSystemColumns.TD_VER_VERSION.value
    TD_VER_TIMESTAMP = td_constants.StandardSystemColumns.TD_VER_TIMESTAMP.value


# noinspection DuplicatedCode
_s_id_metadata = td_constants.StandardSystemColumnsMetadata.TD_IDENTIFIER.value
_s_ver_execution_metadata = (
    td_constants.StandardSystemColumnsMetadata.TD_VER_EXECUTION.value
)
_s_ver_transaction_metadata = (
    td_constants.StandardSystemColumnsMetadata.TD_VER_TRANSACTION.value
)
_s_ver_version_metadata = (
    td_constants.StandardSystemColumnsMetadata.TD_VER_VERSION.value
)
_s_ver_timestamp_metadata = (
    td_constants.StandardSystemColumnsMetadata.TD_VER_TIMESTAMP.value
)

SYSTEM_COLUMNS_METADATA = {
    SystemColumns.TD_IDENTIFIER.value: _s_id_metadata,
    SystemColumns.TD_VER_EXECUTION.value: _s_ver_execution_metadata,
    SystemColumns.TD_VER_TRANSACTION.value: _s_ver_transaction_metadata,
    SystemColumns.TD_VER_VERSION.value: _s_ver_version_metadata,
    SystemColumns.TD_VER_TIMESTAMP.value: _s_ver_timestamp_metadata,
}

# noinspection DuplicatedCode
_r_id_metadata = td_constants.StandardSystemColumnsMetadata.TD_IDENTIFIER.value
_r_ver_execution_metadata = (
    td_constants.StandardSystemColumnsMetadata.TD_VER_EXECUTION.value
)
_r_ver_transaction_metadata = (
    td_constants.StandardSystemColumnsMetadata.TD_VER_TRANSACTION.value
)
_r_ver_version_metadata = (
    td_constants.StandardSystemColumnsMetadata.TD_VER_VERSION.value
)
_r_ver_TIMESTAMP_metadata = (
    td_constants.StandardSystemColumnsMetadata.TD_VER_TIMESTAMP.value
)

REQUIRED_COLUMNS_METADATA = {
    RequiredColumns.TD_IDENTIFIER.value: _r_id_metadata,
    RequiredColumns.TD_VER_EXECUTION.value: _r_ver_execution_metadata,
    RequiredColumns.TD_VER_TRANSACTION.value: _r_ver_transaction_metadata,
    RequiredColumns.TD_VER_VERSION.value: _r_ver_version_metadata,
    RequiredColumns.TD_VER_TIMESTAMP.value: _r_ver_TIMESTAMP_metadata,
}


def system_columns() -> list[str]:
    return [member.value for member in SystemColumns]


class TableFrameExtension(Extension):
    name = "TableFrame Extension (Standard)"
    version = version()

    def __init__(self) -> None:
        FeaturesManager.instance().disable(Feature.ENTERPRISE)
        logger.debug(
            f"Single instance of {Extension.__name__}: {TableFrameExtension.name} -"
            f" {TableFrameExtension.version}"
        )

    @classmethod
    def instance(cls) -> "TableFrameExtension":
        return instance

    @property
    def summary(self) -> str:
        return "Open Source"

    @property
    def standard_system_columns(self) -> Type[Enum]:
        return td_constants.StandardSystemColumns

    @property
    def extended_system_columns(self) -> Type[Enum]:
        return ExtendedSystemColumns

    @property
    def system_columns(self) -> Type[Enum]:
        return SystemColumns

    @property
    def system_columns_metadata(self) -> dict[str, Any]:
        return SYSTEM_COLUMNS_METADATA

    @property
    def required_columns(self) -> Type[Enum]:
        return RequiredColumns

    @property
    def required_columns_metadata(self) -> dict[str, Any]:
        return REQUIRED_COLUMNS_METADATA

    def apply_system_column(
        self,
        lf: pl.LazyFrame,
        column: str,
        dtype: pl.DataType,
        default: Any,
        function: str,
        properties: TableFrameProperties = None,
    ) -> pl.LazyFrame:
        return apply_constant_system_column(
            lf,
            column,
            dtype,
            default,
            function,
            properties,
        )

    # From a given LazyFrame, expectedly coming from an internal of a TableFrame, it
    # selects:
    #   - All non system columns
    #   - All system columns
    # Therefore, all columns whose prefix is a system column prefix, but not being
    # recognized as a system column, are removed.
    # This way, when joins and similar operations are performed, which might provide
    # system columns from more than one source (and that will be attached automatically
    # discriminator suffix by polars), the one preserving the original name is
    # preserved.
    # As a general rule, system columns are totally system-managed. Therefore, dropping
    # (internally) these extra columns is safe, and should produce no data loss or
    # inconsistency.
    def assemble_system_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        target_cols = [
            c
            for c in lf.collect_schema().names()
            if c in system_columns() or not c.startswith(td_constants.TD_COLUMN_PREFIX)
        ]
        return lf.select(target_cols)


instance = TableFrameExtension()
