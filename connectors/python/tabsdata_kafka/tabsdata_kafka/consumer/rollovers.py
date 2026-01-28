#
# Copyright 2026 Tabs Data Inc.
#

from enum import Enum

from pydantic import PositiveInt

from tabsdata.dataquality._utils import _td_validate_call
from tabsdata.exceptions import ErrorCode


class SizeRollover:
    """
    Represents a rollover condition based on the size of buffered messages in megabytes.
    """

    @_td_validate_call(ErrorCode.PVRLVR1)
    def __init__(self, size_mb: PositiveInt):
        self._size_mb = size_mb

    def size_mb(self):
        """
        Returns the rollover size in megabytes.
        """
        return self._size_mb

    def __eq__(self, other):
        if not isinstance(other, SizeRollover):
            return False
        return self._size_mb == other._size_mb


class MessagesRollover:
    """
    Represents a rollover condition based on the number of buffered messages.
    """

    @_td_validate_call(ErrorCode.PVRLVR2)
    def __init__(self, message_count: PositiveInt):
        self._message_count = message_count

    def message_count(self):
        """
        Returns the rollover message count.
        """
        return self._message_count

    def __eq__(self, other):
        if not isinstance(other, MessagesRollover):
            return False
        return self._message_count == other._message_count


class RolloverReason(Enum):
    """
    Enumerates the reasons for a buffer rollover.
    """

    SchemaChange = 1, "A message with an incompatible schema was received"
    Size = 2, "The buffer size limit was reached"
    Messages = 3, "The message count limit was reached"
    Time = 4, "The consume timeout was reached"
