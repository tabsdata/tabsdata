#
# Copyright 2025 Tabs Data Inc.
#

from enum import Enum


class TableKind(Enum):
    COMMON = (
        "common",
        None,
        "Common data table",
    )
    DQ_FILTER = (
        "dq_filter",
        "_dq_filter",
        "From data Quality Filter operation",
    )
    DQ_SELECT = (
        "dq_select",
        "_dq_select",
        "From Data Quality Select operation",
    )
    DQ_SUMMARY = (
        "dq_summary",
        "_dq_summary",
        "From Data Quality Summary operation",
    )

    def __init__(self, value: str, suffix: str, description: str):
        self._value_ = value
        self.suffix = suffix
        self.description = description


class TableInfo:
    """
    Class to represent a table specification to be used in messaging between internal
        architecture components.

    Attributes:
        table (str): The table name.
        kind (TableType): The table kind.
        collection (str): The table collection.
    """

    def __init__(
        self,
        table: str,
        kind: TableKind,
        collection: str | None = None,
    ):
        """
        Initialize a TableInfo object.

        Args:
            table (str): The table name.
            kind (TableType): The table kind.
            collection (str): The table collection.
        """
        self.table = table
        self.kind = kind
        self.collection = collection

    @property
    def table(self) -> str:
        """
        str: The table name.
        """
        return self._table

    @table.setter
    def table(self, table: str):
        """
        Set the table name.

        Args:
            table (str): The table name.
        """
        if table is None:
            raise ValueError("Table name cannot be None")
        elif isinstance(table, str):
            table = table.strip()
            if not table:
                raise ValueError("Table name cannot be empty")
            self._table = table
        else:
            raise ValueError("Table name must be an string")

    @property
    def kind(self) -> TableKind:
        """
        TableType: The table kind.
        """
        return self._kind

    @kind.setter
    def kind(self, kind: TableKind):
        """
        Set the table kind.

        Args:
            kind (TableKind): The table kind.
        """
        if kind is None:
            raise ValueError("Table kind cannot be None")
        elif isinstance(kind, TableKind):
            self._kind = kind
        else:
            raise ValueError("Table kind must be an TableKind")

    @property
    def collection(self) -> str:
        """
        str: The table collection.
        """
        return self._collection

    @collection.setter
    def collection(self, collection: str):
        """
        Set the table collection.

        Args:
            collection (str | None): The table collection.
        """
        if collection is None:
            raise ValueError("Table collection cannot be None")
        elif isinstance(collection, str):
            collection = collection.strip()
            if not collection:
                raise ValueError("Table collection cannot be empty")
            self._collection = collection
        else:
            raise ValueError("Table collection must be an string")
