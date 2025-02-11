#
# Copyright 2025 Tabs Data Inc.
#

from abc import ABC, abstractmethod
from typing import Any

import polars as pl


class InterceptorPlugin(ABC):
    IDENTIFIER = "ta_interceptor"

    @classmethod
    @abstractmethod
    def instance(cls) -> "InterceptorPlugin":
        """
        Returns the single instance of the interceptor engine.
        """
        pass

    @abstractmethod
    def summary(self) -> str:
        """
        Returns a self-descriptive message of the plugin implementation.
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
    def assemble_columns(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """
        For a LazyFrame coming from operating on the internal LazyFrame
        of one or more TableFrame's, normalizes and merges system columns
        as defined by Tabsdata internal algorithms.
        """
        pass
