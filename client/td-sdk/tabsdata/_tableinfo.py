#
# Copyright 2025 Tabs Data Inc.
#

from enum import Enum

from tabsdata.expansions.foundation.symbols import TableType


class TableKind(Enum):
    REGULAR = (
        TableType.Regular,
        None,
        "Primitive data table",
    )
    DQ_FILTER = (
        TableType.DQData,
        "_dq_filter",
        "From Data Quality Filter operation",
    )
    DQ_SELECT = (
        TableType.DQData,
        "_dq_select",
        "From Data Quality Select operation",
    )
    DQ_SUMMARY = (
        TableType.DQSummary,
        "_dq_summary",
        "From Data Quality Summary operation",
    )

    def __init__(self, value: str, suffix: str, description: str):
        self._value_ = value
        self._suffix = suffix
        self._description = description

    @property
    def code(self) -> TableType:
        return self._value_.value

    @property
    def type(self) -> TableType:
        return self._value_

    @property
    def suffix(self) -> str:
        return self._suffix

    @property
    def description(self) -> str:
        return self._description


class TableInfo:
    """
    Class to represent a table specification to be used in messaging between internal
        architecture components.

    Attributes:
        table (str): The table name.
        kind (TableKind): The table kind.
        collection (str): The table collection.
    """

    def __init__(
        self,
        from_table: str,
        from_collection: str,
        kind: TableKind,
        table: str,
        collection: str,
        position: int,
    ):
        """
        Initialize a TableInfo object.

        Args:
            from_table (str): The table name from which the table is derived.
            from_collection (str): The collection name from which the table is derived.
            kind (TableKind): The table kind.
            table (str): The table name.
            collection (str): The table collection.
            position (int): The table position in the decorator.
        """
        self.from_table = from_table
        self.from_collection = from_collection
        self.kind = kind
        self.table = table
        self.collection = collection
        self.position = position

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
        TableKind: The table kind.
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

    @property
    def position(self) -> int:
        """
        int: The table position.
        """
        return self._position

    @position.setter
    def position(self, position: int):
        """
        Set the table position.

        Args:
            position (int): The table position.
        """
        if isinstance(position, int):
            if position < 0:
                raise ValueError("Table position name cannot be negative")
            self._position = position
        else:
            raise ValueError("Table position must be an integer")

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
            f"collection={self.collection!r}, "
            f"position={self.position!r}, "
            ")"
        )


class TypedTable:
    """
    Class to represent a table name with its corresponding type.

    Attributes:
        _table_name (str): The table name.
        _table_type (TableKind): The table type.
    """

    def __init__(
        self,
        table_name: str,
        table_type: TableType,
    ):
        """
        Initialize a TypedTable object.

        Args:
            table_name (str): The table name.
            table_type (str): The table type.
        """
        self._table_name = table_name
        self._table_type = table_type

    @property
    def table_name(self) -> str:
        """
        str: The table name.
        """
        return self._table_name

    @property
    def table_type(self) -> TableType:
        """
        TableType: The table type.
        """
        return self._table_type

    def __repr__(self) -> str:
        """
        Return a string representation of the TableType object.

        Returns:
            str: A string representation showing all key attributes.
        """
        return f"TableType(name={self.table_name!r}, type={self.table_type!r})"
