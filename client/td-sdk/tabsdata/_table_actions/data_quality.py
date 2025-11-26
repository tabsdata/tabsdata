#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

from abc import ABC

from tabsdata._table_actions.actions import OnTablesAction
from tabsdata._table_actions.classifiers import Classifier


class DataQuality(OnTablesAction):
    """
    Data quality actions to perform on tables.
    """

    def __init__(
        self,
        table: str,
        classify: Classifier | list[Classifier],
        # actions: DataQualityOperation | list[DataQualityOperation] = Summary,
    ):
        pass


class DataQualityOperation(ABC):
    """
    Data quality actions to perform on tables.
    """

    def __init__(self, *, tags: str | list[str] | None = None):
        """
        Initialize the DataQuality object.
        Args:
        tags (str | list[str] | None, optional): Tag(s) for the classification
                results. Tags allow performing multiple actions, with each taking
                into account only classifiers that have certain tag(s).
                Defaults to None.
        """
        self.tags = tags
