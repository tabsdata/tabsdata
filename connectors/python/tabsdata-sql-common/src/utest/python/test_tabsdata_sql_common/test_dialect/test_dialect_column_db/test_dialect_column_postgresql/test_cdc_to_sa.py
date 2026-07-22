#
# Copyright 2026 Tabsdata Inc.
#

import pytest
import sqlalchemy.types as sa_types

from tabsdata.connector.sql.common._database.postgres.dialect import PostgresDialect

dialect = PostgresDialect()


class TestWal2JsonCdcToSa:
    def test_integer(self):
        assert isinstance(dialect.cdc_to_sa("wal2json", "integer"), sa_types.Integer)

    def test_bigint(self):
        assert isinstance(dialect.cdc_to_sa("wal2json", "bigint"), sa_types.BigInteger)

    def test_numeric_with_params(self):
        result = dialect.cdc_to_sa("wal2json", "numeric(10,2)")
        assert isinstance(result, sa_types.Numeric)
        assert result.precision == 10
        assert result.scale == 2

    def test_character_varying(self):
        result = dialect.cdc_to_sa("wal2json", "character varying(255)")
        assert isinstance(result, sa_types.String)
        assert result.length == 255

    def test_timestamp_with_tz(self):
        result = dialect.cdc_to_sa("wal2json", "timestamp with time zone")
        assert isinstance(result, sa_types.DateTime)
        assert result.timezone is True

    def test_text(self):
        assert isinstance(dialect.cdc_to_sa("wal2json", "text"), sa_types.Text)

    def test_boolean(self):
        assert isinstance(dialect.cdc_to_sa("wal2json", "boolean"), sa_types.Boolean)

    def test_bytea(self):
        assert isinstance(dialect.cdc_to_sa("wal2json", "bytea"), sa_types.LargeBinary)

    def test_json(self):
        assert isinstance(dialect.cdc_to_sa("wal2json", "json"), sa_types.JSON)

    def test_jsonb(self):
        assert isinstance(dialect.cdc_to_sa("wal2json", "jsonb"), sa_types.JSON)

    def test_uuid(self):
        assert isinstance(dialect.cdc_to_sa("wal2json", "uuid"), sa_types.Uuid)

    def test_date(self):
        assert isinstance(dialect.cdc_to_sa("wal2json", "date"), sa_types.Date)

    def test_time_with_tz(self):
        result = dialect.cdc_to_sa("wal2json", "time with time zone")
        assert isinstance(result, sa_types.Time)
        assert result.timezone is True


class TestUnsupportedIngester:
    def test_binlog_raises(self):
        with pytest.raises(NotImplementedError, match="binlog"):
            dialect.cdc_to_sa("binlog", "integer")

    def test_sqlreplica_raises(self):
        with pytest.raises(NotImplementedError, match="sqlreplica"):
            dialect.cdc_to_sa("sqlreplica", "INTEGER")
