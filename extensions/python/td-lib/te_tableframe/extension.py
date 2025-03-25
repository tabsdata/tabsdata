#
# Copyright 2024 Tabs Data Inc.
#

import logging
from abc import ABC
from enum import Enum
from typing import Any, Type

import polars as pl

# noinspection PyProtectedMember
import tabsdata.utils.tableframe._constants as td_constants
from ta_tableframe.api.api import Extension
from td_features.features import Feature, FeaturesManager
from te_tableframe.version import version

logger = logging.getLogger(__name__)


class ExtendedSystemColumns(Enum):
    pass


class ExtendedSystemColumnsMetadata(Enum):
    pass


class SystemColumns(Enum):
    TD_IDENTIFIER = td_constants.StandardSystemColumns.TD_IDENTIFIER.value


class RequiredColumns(Enum):
    TD_IDENTIFIER = td_constants.StandardSystemColumns.TD_IDENTIFIER.value


_s_id_metadata = td_constants.StandardSystemColumnsMetadata.TD_IDENTIFIER.value

SYSTEM_COLUMNS_METADATA = {
    SystemColumns.TD_IDENTIFIER.value: _s_id_metadata,
}

_r_id_metadata = td_constants.StandardSystemColumnsMetadata.TD_IDENTIFIER.value

REQUIRED_COLUMNS_METADATA = {
    RequiredColumns.TD_IDENTIFIER.value: _r_id_metadata,
}


class TableFrameExtension(Extension, ABC):
    name = "TableFrame Extension (Standard)"
    version = version()

    def __init__(self) -> None:
        FeaturesManager.instance().disable(Feature.ENTERPRISE)
        logger.info(
            f"Single instance of {Extension.__name__}: {TableFrameExtension.name} -"
            f" {TableFrameExtension.version}"
        )

    @classmethod
    def instance(cls) -> "TableFrameExtension":
        return instance

    @property
    def summary(self) -> str:
        return "Standard"

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

    # ToDo: This function might be incorrect. Review when revisiting provenance.
    def assemble_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        target_cols = [
            td_constants.StandardSystemColumns.TD_IDENTIFIER.value,
        ] + [
            c
            for c in lf.collect_schema().names()
            if not c.startswith(td_constants.TD_COLUMN_PREFIX)
        ]
        lf = lf.select(target_cols)
        return lf


instance = TableFrameExtension()
