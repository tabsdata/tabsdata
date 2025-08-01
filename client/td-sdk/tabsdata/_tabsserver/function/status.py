#
# Copyright 2025 Tabs Data Inc.
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from tabsdata._tabsserver.function.logging_utils import pad_string
from tabsdata._tabsserver.function.offset_utils import Offset

if TYPE_CHECKING:
    from tabsdata._tabsserver.function.execution_context import ExecutionContext
    from tabsdata._tabsserver.function.yaml_parsing import InputYaml

logger = logging.getLogger(__name__)


class Status:
    """
    A class to represent the status (currently offset plus metadata) of the function.
    """

    def __init__(self):
        self.offset = Offset()
        self.meta = {}
        self.modified_tables = []

    def __str__(self):
        return f"#Status#< Offset: {str(self.offset)} ; meta: {str(self.meta)} >"

    def load(self, request: InputYaml, execution_context: ExecutionContext):
        """
        Load the current status from the execution context.

        Args:
            request: The request information.
        """
        logger.debug("Loading current status")
        self.offset.load_current_offset(request, execution_context)
        logger.debug(f"Current status: {self}")

    def store(self, request: InputYaml, execution_context: ExecutionContext):
        """
        Store the status.

        Args:
            request: The execution context.

        """
        logger.info(pad_string("[Storing execution information]"))
        logger.debug(f"Storing status: {self}")
        self.offset.store(request, execution_context)

    @property
    def modified_tables(self) -> list[dict]:
        return self._modified_tables

    @modified_tables.setter
    def modified_tables(self, tables):
        self._modified_tables = tables
        logger.debug(f"Modified tables set to '{self._modified_tables}'")
