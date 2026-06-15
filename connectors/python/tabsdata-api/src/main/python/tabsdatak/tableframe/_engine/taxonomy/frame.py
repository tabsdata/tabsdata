#
# Copyright 2026 Tabs Data Inc.
#

from __future__ import annotations

from enum import Enum, auto


class TableFrameOrigin(Enum):
    # Created from external data, or from a TableFrame already marked IMPORT.
    # External data is data not loaded from a function: pure external data.
    IMPORT = auto()
    # Created from internal data (loaded from a function or an existing table):
    # pure internal data.
    BUILD = auto()
    # Created via the init function (the standard way to declare data as internal
    # even if external). Unlike IMPORT, this one carries provenance support.
    INIT = auto()
    # Created from a TableFrame through an API transformation, unless a rule above
    # marks it differently.
    TRANSFORM = auto()


class CreationMode(Enum):
    # Create all the system columns, even if already present.
    # (Normally used to create a new TableFrame from scratch. Also used in source
    #  functions to statically stamp their values before processing.)
    RAW = "raw"
    # Apply the inception policy declared per column.
    # (Normally used to store a TableFrame after function execution: columns to keep
    #  retain their current value, and columns to regenerate are dropped and recreated.
    #  A future load then uses these lastly generated values.)
    SYS = "sys"
    # Create only the system columns that are missing.
    # (Normally used to load a TableFrame from a non-source function, as the stored
    #  columns are assumed to hold correct values to use in future function executions
    #  consuming them.)
    TAB = "tab"
    # Regenerate the pristine system columns ($td.id, $td.row, $td.src), resetting them
    # to a fresh per-function view, and keep everything else (user columns and version
    # columns) as-is. Pristine columns are the system columns that must be regenerated
    # for a stored TableFrame to serve as the input of a transformer/subscriber.
    # (Used to load a TableFrame as the input of a transformer/subscriber: every input
    #  row is re-stamped with a sequential $td.row and a single-entry $td.src pointing
    #  to itself, where the $td.src 'tab' is this input's position in the function
    #  signature -- the 'idx' argument, taken from the request 'input_idx'. This
    #  replaces whatever the producing function stored.)
    MUT = "mut"
