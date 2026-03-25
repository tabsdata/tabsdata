#
# Copyright 2026 Tabs Data Inc.
#

import pytest
import sqlalchemy.types as sa_types

from tabsdata.connector.sql.common._database.postgres.dialect import PostgresDialect

dialect = PostgresDialect()


class TestIntegerTypes:
    def test_integer(self):
        assert isinstance(dialect.ddl_to_sa("integer"), sa_types.Integer)

    def test_bigint(self):
        assert isinstance(dialect.ddl_to_sa("bigint"), sa_types.BigInteger)

    def test_smallint(self):
        assert isinstance(dialect.ddl_to_sa("smallint"), sa_types.SmallInteger)

    def test_serial(self):
        assert isinstance(dialect.ddl_to_sa("serial"), sa_types.Integer)

    def test_bigserial(self):
        assert isinstance(dialect.ddl_to_sa("bigserial"), sa_types.BigInteger)

    def test_smallserial(self):
        assert isinstance(dialect.ddl_to_sa("smallserial"), sa_types.SmallInteger)


class TestNumericTypes:
    def test_numeric_with_precision_and_scale(self):
        result = dialect.ddl_to_sa("numeric(10,2)")
        assert isinstance(result, sa_types.Numeric)
        assert result.precision == 10
        assert result.scale == 2

    def test_numeric_without_params(self):
        result = dialect.ddl_to_sa("numeric")
        assert isinstance(result, sa_types.Numeric)

    def test_real(self):
        assert isinstance(dialect.ddl_to_sa("real"), sa_types.Float)

    def test_double_precision(self):
        assert isinstance(dialect.ddl_to_sa("double precision"), sa_types.Float)


class TestStringTypes:
    def test_character_varying_with_length(self):
        result = dialect.ddl_to_sa("character varying(255)")
        assert isinstance(result, sa_types.String)
        assert result.length == 255

    def test_character_varying_without_length(self):
        result = dialect.ddl_to_sa("character varying")
        assert isinstance(result, sa_types.String)

    def test_character_with_length(self):
        result = dialect.ddl_to_sa("character(10)")
        assert isinstance(result, sa_types.String)
        assert result.length == 10

    def test_text(self):
        assert isinstance(dialect.ddl_to_sa("text"), sa_types.Text)


class TestBooleanTypes:
    def test_boolean(self):
        assert isinstance(dialect.ddl_to_sa("boolean"), sa_types.Boolean)


class TestTemporalTypes:
    def test_date(self):
        assert isinstance(dialect.ddl_to_sa("date"), sa_types.Date)

    def test_timestamp_with_time_zone(self):
        result = dialect.ddl_to_sa("timestamp with time zone")
        assert isinstance(result, sa_types.DateTime)
        assert result.timezone is True

    def test_timestamp_without_time_zone(self):
        result = dialect.ddl_to_sa("timestamp without time zone")
        assert isinstance(result, sa_types.DateTime)
        assert result.timezone is not True

    def test_time_with_time_zone(self):
        result = dialect.ddl_to_sa("time with time zone")
        assert isinstance(result, sa_types.Time)
        assert result.timezone is True

    def test_time_without_time_zone(self):
        result = dialect.ddl_to_sa("time without time zone")
        assert isinstance(result, sa_types.Time)
        assert result.timezone is not True


class TestBinaryTypes:
    def test_bytea(self):
        assert isinstance(dialect.ddl_to_sa("bytea"), sa_types.LargeBinary)


class TestJsonTypes:
    def test_json(self):
        assert isinstance(dialect.ddl_to_sa("json"), sa_types.JSON)

    def test_jsonb(self):
        assert isinstance(dialect.ddl_to_sa("jsonb"), sa_types.JSON)


class TestOtherTypes:
    def test_uuid(self):
        assert isinstance(dialect.ddl_to_sa("uuid"), sa_types.Uuid)
