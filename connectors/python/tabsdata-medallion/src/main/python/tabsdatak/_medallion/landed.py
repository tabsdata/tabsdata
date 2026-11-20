#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.tableframe import TableFrame
from tabsdatak.tableframe.functions import concat


def landed(
    commits: list[TableFrame | None] | None, head: TableFrame | None = None
) -> TableFrame | None:
    """The rows of the commits read at ``@NEW``. A commit without data (None)
    or without columns (an empty version nothing could give a schema to)
    holds no rows, so it is left out. When none brought any, no rows with the
    columns of ``head`` (the same table read at ``@HEAD``), and None while
    there is no ``head``: a table that holds one batch per version gets an
    empty version, since a None one would hand its ``@NEW`` readers the
    previous batch again."""
    frames = [
        frame for frame in commits or [] if frame is not None and frame._user_columns()
    ]
    if frames:
        return concat(frames, how="diagonal_relaxed")
    return None if head is None else head.clear()
