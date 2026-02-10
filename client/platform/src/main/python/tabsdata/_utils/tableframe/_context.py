#
# Copyright 2025 Tabs Data Inc.
#

from os import PathLike

from tabsdata._tabsserver.function.yaml_parsing import Table


class TableFrameContext:
    """
    Class to manage the information to handle TableFrame metadata.

    """

    def __init__(
        self,
        path: str | PathLike,
        table: Table | None = None,
    ):
        self.path = path
        self.table = table

    def __repr__(self):
        return f"TableFrameContext(path={self.path}, table={self.table})"
