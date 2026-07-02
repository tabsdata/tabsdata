from __future__ import annotations

from enum import Enum


class SrcFileFormat(Enum):
    AUTO = "AUTO"
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"
    LOG = "LOG"


class DestFileFormat(Enum):
    AUTO = "AUTO"
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    PARQUET = "PARQUET"
