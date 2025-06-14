#
# Copyright 2025 Tabs Data Inc.
#

from os import PathLike

from tabsdata.tabsserver.function.yaml_parsing import Table


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
