#
# Copyright 2026 Tabsdata Inc.
#

from enum import Enum


class Format(Enum):
    AVRO = "avro"
    CSV = "csv"
    EXCEL = "excel"
    LOG = "log"
    NDJSON = "ndjson"
    PARQUET = "parquet"
