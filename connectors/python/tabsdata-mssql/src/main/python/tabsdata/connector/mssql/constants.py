#
# Copyright 2026 Tabs Data Inc.
#

from typing import Literal

MSSQLSrcLoaderSpec = Literal["polars_sqlalchemy", "pandas"]
MSSQLDestWriterSpec = Literal["polars_sqlalchemy"]
