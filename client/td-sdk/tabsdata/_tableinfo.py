#
# Copyright 2025 Tabs Data Inc.
#

from enum import Enum


class TableKind(Enum):
    PRIMITIVE = (
        "primitive",
        None,
        "Primitive data table",
    )
    DQ_FILTER = (
        "dq_filter",
        "_dq_filter",
        "From Data Quality Filter operation",
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
        from_table: str,
        from_collection: str,
        kind: TableKind,
        table: str,
        collection: str,
    ):
        """
        Initialize a TableInfo object.

        Args:
            from_table (str): The table name from which the table is derived.
            from_collection (str): The collection name from which the table is derived.
            kind (TableType): The table kind.
            table (str): The table name.
            collection (str): The table collection.
        """
        self.from_table = from_table
        self.from_collection = from_collection
        self.kind = kind
        self.table = table
        self.collection = collection

    @property
    def from_table(self) -> str:
        """
        str: The table name from which the table is derived.
        """
        return self._from_table

    @from_table.setter
    def from_table(self, from_table: str):
        """
        Set the table name from which the table is derived.

        Args:
            from_table (str): The table name from which the table is derived.
        """
        if isinstance(from_table, str):
            from_table = from_table.strip()
            if not from_table:
                raise ValueError("Table name (from) cannot be empty")
            self._from_table = from_table
        else:
            raise ValueError("Table name (from) must be an string")

    @property
    def from_collection(self) -> str:
        """
        str: The collection name from which the table is derived.
        """
        return self._from_collection

    @from_collection.setter
    def from_collection(self, from_collection: str):
        """
        Set the collection name from which the table is derived.

        Args:
            from_collection (str): The collection name from which the table is derived.
        """
        if isinstance(from_collection, str):
            from_collection = from_collection.strip()
            if not from_collection:
                raise ValueError("Collection name (from) cannot be empty")
            self._from_collection = from_collection
        else:
            raise ValueError("Collection name (from) must be an string")

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
        if isinstance(table, str):
            table = table.strip()
            if not table:
                raise ValueError("Table name cannot be empty")
            self._table = table
        else:
            raise ValueError("Table name must be an string")

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

    def __repr__(self) -> str:
        """
        Return a string representation of the TableInfo object.

        Returns:
            str: A string representation showing all key attributes.
        """
        return (
            "TableInfo("
            f"from_table={self.from_table!r}, "
            f"from_collection={self.from_collection!r}, "
            f"kind={self.kind!r}, "
            f"table={self.table!r}, "
            f"collection={self.collection!r}"
            ")"
        )
