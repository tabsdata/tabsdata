#
# Copyright 2025 Tabs Data Inc.
#

from enum import Enum


class IfTableExistsStrategy(Enum):
    """
    Enum for the strategies to follow when the table already exists.
    """

    APPEND = "append"
    REPLACE = "replace"


class SchemaStrategy(Enum):
    """
    Enum for the strategies to follow when the table already exists.
    """

    UPDATE = "update"
    STRICT = "strict"
