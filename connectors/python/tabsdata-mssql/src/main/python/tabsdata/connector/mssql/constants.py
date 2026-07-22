#
# Copyright 2026 Tabsdata Inc.
#

from typing import Literal

MSSQLSrcLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
MSSQLDestWriterSpec = Literal["polars_sqlalchemy"]
