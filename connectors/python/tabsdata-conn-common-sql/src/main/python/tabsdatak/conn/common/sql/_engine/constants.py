#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatak.conn.common import _config

SQL_KEY_PREFIX = _config.TABSDATA_CFG_PREFIX + "sql."

# --- Read --------------------------------------------------------

READ_ENGINE_KEY = SQL_KEY_PREFIX + "sql.readEngine"
POLARS_SQL_ALCHEMY_READ_ENGINE = "polarsSqlAlchemyRead"
PANDAS_SQL_ALCHEMY_READ_ENGINE = "pandasSqlAlchemyRead"

AVAIL_READ_ENGINES_KEY = SQL_KEY_PREFIX + "sql.engine.availReadEngines"

READ_CHUNK_SIZE_KEY = SQL_KEY_PREFIX + "chunkSize"
READ_CHUNK_SIZE_DEFAULT = 100_000

POLARS_SQL_ALCHEMY_READ_CHUNK_SIZE_DEFAULT = READ_CHUNK_SIZE_DEFAULT
PANDAS_SQL_ALCHEMY_READ_CHUNK_SIZE_DEFAULT = READ_CHUNK_SIZE_DEFAULT


# --- Write -------------------------------------------------------

WRITE_ENGINE_KEY = SQL_KEY_PREFIX + "sql.writeEngine"
POLARS_SQL_ALCHEMY_WRITE_ENGINE = "polarsSqlAlchemyWrite"

AVAIL_WRITE_ENGINES_KEY = SQL_KEY_PREFIX + "sql.engine.availWriteEngines"

WRITE_CHUNK_SIZE_KEY = SQL_KEY_PREFIX + "chunkSize"
WRITE_CHUNK_SIZE_DEFAULT = 100_000

POLARS_SQL_ALCHEMY_WRITE_CHUNK_SIZE_DEFAULT = WRITE_CHUNK_SIZE_DEFAULT
