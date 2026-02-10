#
# Copyright 2025 Tabs Data Inc.
#

from abc import ABC, abstractmethod
from typing import Any

import polars as pl

from tabsdata.tableframe.lazyframe.properties import TableFrameProperties


class Extension(ABC):
    IDENTIFIER = "tableframe"

    @classmethod
    @abstractmethod
    def instance(cls) -> "Extension":
        """
        Returns the single instance of the extension engine.
        """
        pass

    @abstractmethod
    def summary(self) -> str:
        """
        Returns a self-descriptive message of the extension implementation.
        """
        pass

    @abstractmethod
    def standard_system_columns(self) -> list[str]:
        """
        Returns a list of standard system TableFrame columns.
        """
        pass

    @abstractmethod
    def extended_system_columns(self) -> list[str]:
        """
        Returns a list of extended system TableFrame columns.
        """
        pass

    @abstractmethod
    def system_columns(self) -> list[str]:
        """
        Returns a list of system TableFrame columns.
        """
        pass

    @abstractmethod
    def system_columns_metadata(self) -> dict[str, Any]:
        """
        Returns a list of required system TableFrame columns with their metadata.
        """
        pass

    @abstractmethod
    def required_columns(self) -> list[str]:
        """
        Returns a list of required system TableFrame columns.
        """
        pass

    @abstractmethod
    def required_columns_metadata(self) -> dict[str, Any]:
        """
        Returns a list of required system TableFrame columns with their metadata.
        """
        pass

    @abstractmethod
    def apply_system_column(
        self,
        lf: pl.LazyFrame,
        column: str,
        dtype: pl.DataType,
        default: Any,
        function: str,
        properties: TableFrameProperties = None,
    ) -> pl.LazyFrame:
        """
        Given a LazyFrame, creates a new column based on the provided abstract
        function, and returns a new LazyFrame with the new column.
        """
        pass

    @abstractmethod
    def assemble_system_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        For a LazyFrame coming from operating on the internal LazyFrame
        of one or more TableFrame's, normalizes and merges system columns
        as defined by Tabsdata internal algorithms.
        """
        pass
