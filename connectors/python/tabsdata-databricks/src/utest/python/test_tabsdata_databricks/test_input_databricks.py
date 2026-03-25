#
# Copyright 2026 Tabs Data Inc.
#

import logging
import time
import uuid
from datetime import date

import numpy as np
import polars as pl
import pytest
from mimesis import Field, Fieldset
from mimesis.locales import Locale

import tabsdata as td
from tabsdata._io.outputs.shared_enums import IfTableExistStrategySpec
from tabsdata._secret import DirectSecret, EnvironmentSecret

from tabsdata.connector.databricks._connector import _table_fqn_4sql
from test_tabsdata.fntest import tdfnrun

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


DATABRICKS_BUDGET_SAFETY_TIMEOUT = 600

DEFAULT_Q_RECORDS = 2048


BULK_POOL_SIZE = 10_000


def generate_bulk_data(q_records: int, seed: int = 1921) -> pl.DataFrame:
    random = np.random.default_rng(seed)
    fieldset = Fieldset(locale=Locale.EN, seed=seed, i=BULK_POOL_SIZE)

    pool_names = fieldset("person.full_name")
    pool_emails = fieldset("person.email")
    pool_sentences = fieldset("text.sentence")
    pool_cities = fieldset("address.city")
    pool_departments = fieldset("finance.company")

    def sample_str(pool):
        indices = random.integers(0, len(pool), size=q_records)
        return [pool[i] for i in indices]

    epoch_start = date(1960, 1, 1).toordinal()
    epoch_end = date(2025, 1, 1).toordinal()
    join_start = date(2015, 1, 1).toordinal()
    join_end = date(2025, 6, 1).toordinal()

    return pl.DataFrame({
        "id": np.arange(q_records),
        "name": sample_str(pool_names),
        "email": sample_str(pool_emails),
        "active": random.choice([True, False], size=q_records).tolist(),
        "verified": random.choice([True, False], size=q_records).tolist(),
        "birthdate": [
            date.fromordinal(int(ordinal))
            for ordinal in random.integers(epoch_start, epoch_end, size=q_records)
        ],
        "join_date": [
            date.fromordinal(int(ordinal))
            for ordinal in random.integers(join_start, join_end, size=q_records)
        ],
        "salary": np.round(random.uniform(20000.0, 150000.0, size=q_records), 2),
        "rating": np.round(random.uniform(1.0, 5.0, size=q_records), 2),
        "projects": random.integers(-500, 500, size=q_records),
        "experience_years": random.integers(0, 40, size=q_records),
        "city": sample_str(pool_cities),
        "department": sample_str(pool_departments),
        "bio": sample_str(pool_sentences),
    })


def generate_persons_data(q_records: int = DEFAULT_Q_RECORDS) -> pl.DataFrame:
    field = Field(locale=Locale.EN, seed=271)
    person_id = list(range(0, q_records))
    name = [
        field("person.full_name")
        for _ in range(q_records)
    ]
    email = [
        field("person.email")
        for _ in range(q_records)
    ]
    active = [
        field("development.boolean")
        for _ in range(q_records)
    ]
    birthdate = [
        field("datetime.date", start=1960, end=2025)
        for _ in range(q_records)
    ]
    title = [
        field("text.sentence")
        for _ in range(q_records)
    ]
    salary = [
        field("finance.price", minimum=0.01, maximum=99999.99)
        for _ in range(q_records)
    ]
    projects = [
        field("numeric.integer_number", start=-1000, end=1000)
        for _ in range(q_records)
    ]

    return pl.DataFrame({
        "id": person_id,
        "name": name,
        "email": email,
        "active": active,
        "birthdate": birthdate,
        "title": title,
        "salary": salary,
        "projects": projects,
    })


def generate_orders_data(q_records: int = DEFAULT_Q_RECORDS) -> pl.DataFrame:
    field = Field(locale=Locale.EN, seed=314)
    order_id = list(range(0, q_records))
    amount = [
        field("finance.price", minimum=1.0, maximum=50000.0)
        for _ in range(q_records)
    ]
    quantity = [
        field("numeric.integer_number", start=1, end=500)
        for _ in range(q_records)
    ]
    shipped = [
        field("development.boolean")
        for _ in range(q_records)
    ]
    city = [
        field("address.city")
        for _ in range(q_records)
    ]
    country = [
        field("address.country")
        for _ in range(q_records)
    ]

    return pl.DataFrame({
        "id": order_id,
        "amount": amount,
        "quantity": quantity,
        "shipped": shipped,
        "city": city,
        "country": country,
    })


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_source_class_initialization_single_query(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    query = "select * from catalog.schema.t1"
    source = td.DatabricksSource(
        host,
        token,
        queries=query,
        warehouse=warehouse_name,
    )
    assert source.host_url == host
    assert source.token == DirectSecret(token)
    assert source.queries == [query]
    assert source.warehouse == warehouse_name
    assert source.warehouse_id is None
    assert source.initial_values == {}


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_source_class_initialization_query_list(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    queries = ["select * from catalog.schema.t1", "select * from catalog.schema.t2"]
    source = td.DatabricksSource(
        host,
        token,
        queries=queries,
        warehouse=warehouse_name,
    )
    assert source.queries == queries


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_source_class_initialization_with_initial_values(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    source = td.DatabricksSource(
        host,
        token,
        queries="select * from t where id > :last_id",
        initial_values={"last_id": 0},
        warehouse=warehouse_name,
    )
    assert source.initial_values == {"last_id": 0}


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_source_class_initialization_all_options(databricks_config):
    host = databricks_config["HOST"]
    source = td.DatabricksSource(
        host,
        EnvironmentSecret("token"),
        queries=["select 1", "select 2"],
        initial_values={"key": "val"},
        warehouse_id="w0",
    )
    assert source.host_url == host
    assert source.token == EnvironmentSecret("token")
    assert source.queries == ["select 1", "select 2"]
    assert source.initial_values == {"key": "val"}
    assert source.warehouse is None
    assert source.warehouse_id == "w0"


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_source_no_warehouse(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    with pytest.raises(ValueError):
        td.DatabricksSource(
            host,
            token,
            queries="select 1",
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_source_both_warehouse(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    with pytest.raises(ValueError):
        td.DatabricksSource(
            host,
            token,
            queries="select 1",
            warehouse=warehouse_name,
            warehouse_id="w0",
        )


@pytest.mark.databricks
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
@pytest.mark.unit
def test_source_repr(databricks_config):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    source = td.DatabricksSource(
        host,
        token,
        queries="select 1",
        warehouse=warehouse_name,
    )
    assert "DatabricksSource" in repr(source)
    assert host in repr(source)
    assert "queries=" in repr(source)


def write_databricks_table(
    databricks_config,
    tmp_path,
    table_name,
    df,
    if_table_exists: IfTableExistStrategySpec = "replace",
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    volume = databricks_config["VOLUME"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    destination = td.DatabricksDestination(
        host,
        token,
        table_name,
        volume,
        warehouse=warehouse_name,
        if_table_exists=if_table_exists,
    )
    destination.stream(str(tmp_path), df.lazy())


def append_databricks_table(databricks_config, tmp_path, table_name, df):
    write_databricks_table(
        databricks_config,
        tmp_path,
        table_name,
        df,
        if_table_exists="append",)


def delete_databricks_table(databricks_client, table_name):
    # noinspection PyBroadException
    try:
        databricks_client.tables.delete(full_name=table_name)
    except Exception:
        pass


def clean_databricks_table(df: pl.DataFrame) -> pl.DataFrame:
    if "_rescued_data" in df.columns:
        df = df.drop("_rescued_data")
    return df.sort(df.columns[0])


def check_databricks_table(actual_df: pl.DataFrame, expected_df: pl.DataFrame):
    def first_difference(expected: pl.DataFrame, actual: pl.DataFrame) -> str:
        for i in range(min(expected.height, actual.height)):
            if not expected.row(i) == actual.row(i):
                return f"  row {i}: expected={expected.row(i)}; actual={actual.row(i)}"
        if expected.height != actual.height:
            return f"  heights differ: expected={expected.height}; actual={actual.height}"
        return "  (no diff found)"

    actual_df = clean_databricks_table(actual_df)
    expected_df = expected_df.sort(expected_df.columns[0])
    for column in expected_df.columns:
        if column in actual_df.columns:
            if expected_df[column].dtype == pl.Date and actual_df[column].dtype != pl.Date:
                actual_df = actual_df.with_columns(pl.col(column).cast(pl.Date))
    actual_df = actual_df.select(expected_df.columns)
    assert actual_df.equals(expected_df), (
        f"DataFrames differ.\n"
        f"Expected shape: {expected_df.shape}, got: {actual_df.shape}\n"
        f"First diff row:\n{first_difference(expected_df, actual_df)}"
    )


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_full_table_single(
    tmp_path,
    databricks_client,
    databricks_config
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df = generate_persons_data()
    table_name = f"{catalog}.{schema}.test_source_full_table_single_{uuid.uuid4().hex[:12]}"
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, df)
        source = td.DatabricksSource(
            host,
            token,
            queries=f"select * from {table_sql}",
            warehouse=warehouse_name,
        )
        result_files = source.chunk(str(tmp_path))
        assert len(result_files) == 1
        result_df = pl.read_parquet(result_files[0])
        check_databricks_table(result_df, df)
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.skip(reason="Use only for benchmarking.")
@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.parametrize("q_records", [100_000, 1_000_000, 10_000_000, 100_000_000])
def test_source_bulk_roundtrip(
    tmp_path,
    databricks_client,
    databricks_config,
    q_records,
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]

    t0 = time.time()
    df = generate_bulk_data(q_records)
    t1 = time.time()
    logger.info(f">>>>> generate_bulk_data({q_records}) took {t1 - t0:.3f}s")

    table_name = (
        f"{catalog}.{schema}.test_bulk_roundtrip_{q_records}_{uuid.uuid4().hex[:8]}"
    )
    table_sql = _table_fqn_4sql(table_name)
    try:
        t2 = time.time()
        write_databricks_table(databricks_config, tmp_path, table_name, df)
        t3 = time.time()
        logger.info(f">>>>> write_databricks_table took {t3 - t2:.3f}s")

        source = td.DatabricksSource(
            host,
            token,
            queries=f"select * from {table_sql}",
            warehouse=warehouse_name,
        )
        t4 = time.time()
        result_files = source.chunk(str(tmp_path))
        t5 = time.time()
        logger.info(f">>>>> source.chunk (reading) took {t5 - t4:.3f}s")

        assert len(result_files) == 1
        result_df = pl.read_parquet(result_files[0])
        check_databricks_table(result_df, df)
        logger.info(f">>>>> total roundtrip for {q_records} rows: {t5 - t0:.3f}s")
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.parametrize("q_records", [100_000])
def test_links_orchestration(
    tmp_path,
    databricks_client,
    databricks_config,
    q_records,
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]

    t0 = time.time()
    df = generate_bulk_data(q_records)
    t1 = time.time()
    logger.info(f">>>>> generate_bulk_data({q_records}) took {t1 - t0:.3f}s")

    table_name = (
        f"{catalog}.{schema}.test_links_orchestration_{q_records}_{uuid.uuid4().hex[:8]}"
    )
    table_sql = _table_fqn_4sql(table_name)
    try:
        t2 = time.time()
        write_databricks_table(databricks_config, tmp_path, table_name, df)
        t3 = time.time()
        logger.info(f">>>>> write_databricks_table took {t3 - t2:.3f}s")

        source = td.DatabricksSource(
            host,
            token,
            queries=f"select * from {table_sql}",
            warehouse=warehouse_name,
        )
        t4 = time.time()
        result_files = source.chunk(str(tmp_path))
        t5 = time.time()
        logger.info(f">>>>> source.chunk (reading) took {t5 - t4:.3f}s")

        assert len(result_files) == 1
        result_df = pl.read_parquet(result_files[0])
        check_databricks_table(result_df, df)
        logger.info(f">>>>> total roundtrip for {q_records} rows: {t5 - t0:.3f}s")
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_full_table_multiple(
    tmp_path,
    databricks_client,
    databricks_config
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df1 = generate_persons_data()
    df2 = generate_orders_data()
    uid = uuid.uuid4().hex[:12]
    table_name_1 = f"{catalog}.{schema}.test_source_full_table_multi_1_{uid}"
    table_name_2 = f"{catalog}.{schema}.test_source_full_table_multi_2_{uid}"
    table_sql_1 = _table_fqn_4sql(table_name_1)
    table_sql_2 = _table_fqn_4sql(table_name_2)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name_1, df1)
        write_databricks_table(databricks_config, tmp_path, table_name_2, df2)
        source = td.DatabricksSource(
            host,
            token,
            queries=[
                f"select * from {table_sql_1}",
                f"select * from {table_sql_2}",
            ],
            warehouse=warehouse_name,
        )
        result_files = source.chunk(str(tmp_path))
        assert len(result_files) == 2
        check_databricks_table(pl.read_parquet(result_files[0]), df1)
        check_databricks_table(pl.read_parquet(result_files[1]), df2)
    finally:
        delete_databricks_table(databricks_client, table_name_1)
        delete_databricks_table(databricks_client, table_name_2)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_partial_table_single(
    tmp_path,
    databricks_client,
    databricks_config
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df = generate_persons_data()
    table_name = f"{catalog}.{schema}.test_source_partial_table_single_{uuid.uuid4().hex[:12]}"
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, df)
        source = td.DatabricksSource(
            host,
            token,
            queries=f"select * from {table_sql} where id < 1024",
            warehouse=warehouse_name,
        )
        result_files = source.chunk(str(tmp_path))
        assert len(result_files) == 1
        result_df = pl.read_parquet(result_files[0])
        expected = df.filter(pl.col("id") < 1024)
        check_databricks_table(result_df, expected)
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_partial_table_multiple(
    tmp_path,
    databricks_client,
    databricks_config
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df1 = generate_persons_data()
    df2 = generate_orders_data()
    uid = uuid.uuid4().hex[:12]
    table_name_1 = f"{catalog}.{schema}.test_source_partial_multi_1_{uid}"
    table_name_2 = f"{catalog}.{schema}.test_source_partial_multi_2_{uid}"
    table_sql_1 = _table_fqn_4sql(table_name_1)
    table_sql_2 = _table_fqn_4sql(table_name_2)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name_1, df1)
        write_databricks_table(databricks_config, tmp_path, table_name_2, df2)
        source = td.DatabricksSource(
            host,
            token,
            queries=[
                f"select * from {table_sql_1} where id < 1024",
                f"select * from {table_sql_2} where id >= 1024",
            ],
            warehouse=warehouse_name,
        )
        result_files = source.chunk(str(tmp_path))
        assert len(result_files) == 2
        check_databricks_table(
            pl.read_parquet(result_files[0]),
            df1.filter(pl.col("id") < 1024),
        )
        check_databricks_table(
            pl.read_parquet(result_files[1]),
            df2.filter(pl.col("id") >= 1024),
        )
    finally:
        delete_databricks_table(databricks_client, table_name_1)
        delete_databricks_table(databricks_client, table_name_2)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_full_table_single_tdfnrun(
    tmp_path,
    databricks_client,
    databricks_config
):
    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df = generate_persons_data()
    table_name = f"{catalog}.{schema}.test_source_full_table_single_tdfnrun_{uuid.uuid4().hex[:12]}"
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, df)

        @td.publisher(
            name="test_source_full_table_single_tdfnrun",
            source=td.DatabricksSource(
                host,
                td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                queries=f"select * from {table_sql}",
                warehouse=warehouse_name,
            ),
            tables="output",
        )
        def input_databricks(data: td.TableFrame) -> td.TableFrame:
            return data

        fnrun = tdfnrun(function=input_databricks)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 1

        result_df = output[0].as_pl_df(with_sys_columns=False)
        check_databricks_table(result_df, df)
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_full_table_multiple_tdfnrun(
    tmp_path,
    databricks_client,
    databricks_config
):
    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df1 = generate_persons_data()
    df2 = generate_orders_data()
    uid = uuid.uuid4().hex[:12]
    table_name_1 = f"{catalog}.{schema}.test_source_full_multi_tdfnrun_1_{uid}"
    table_name_2 = f"{catalog}.{schema}.test_source_full_multi_tdfnrun_2_{uid}"
    table_sql_1 = _table_fqn_4sql(table_name_1)
    table_sql_2 = _table_fqn_4sql(table_name_2)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name_1, df1)
        write_databricks_table(databricks_config, tmp_path, table_name_2, df2)

        @td.publisher(
            name="test_source_full_table_multiple_tdfnrun",
            source=td.DatabricksSource(
                host,
                td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                queries=[
                    f"select * from {table_sql_1}",
                    f"select * from {table_sql_2}",
                ],
                warehouse=warehouse_name,
            ),
            tables=["output1", "output2"],
        )
        def input_databricks_multi(
            data1: td.TableFrame, data2: td.TableFrame
        ) -> tuple[td.TableFrame, td.TableFrame]:
            return data1, data2

        fnrun = tdfnrun(function=input_databricks_multi)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 2

        check_databricks_table(output[0].as_pl_df(with_sys_columns=False), df1)
        check_databricks_table(output[1].as_pl_df(with_sys_columns=False), df2)
    finally:
        delete_databricks_table(databricks_client, table_name_1)
        delete_databricks_table(databricks_client, table_name_2)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_partial_table_single_tdfnrun(
    tmp_path,
    databricks_client,
    databricks_config
):
    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df = generate_persons_data()
    table_name = f"{catalog}.{schema}.test_source_partial_table_single_tdfnrun_{uuid.uuid4().hex[:12]}"
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, df)

        @td.publisher(
            name="test_source_partial_table_single_tdfnrun",
            source=td.DatabricksSource(
                host,
                td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                queries=f"select * from {table_sql} where id < 1024",
                warehouse=warehouse_name,
            ),
            tables="output",
        )
        def input_databricks_partial(data: td.TableFrame) -> td.TableFrame:
            return data

        fnrun = tdfnrun(function=input_databricks_partial)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 1

        result_df = output[0].as_pl_df(with_sys_columns=False)
        expected = df.filter(pl.col("id") < 1024)
        check_databricks_table(result_df, expected)
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_partial_table_multiple_tdfnrun(
    tmp_path,
    databricks_client,
    databricks_config
):
    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df1 = generate_persons_data()
    df2 = generate_orders_data()
    uid = uuid.uuid4().hex[:12]
    table_name_1 = f"{catalog}.{schema}.test_source_partial_table_multiple_tdfnrun_1_{uid}"
    table_name_2 = f"{catalog}.{schema}.test_source_partial_table_multiple_tdfnrun_2_{uid}"
    table_sql_1 = _table_fqn_4sql(table_name_1)
    table_sql_2 = _table_fqn_4sql(table_name_2)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name_1, df1)
        write_databricks_table(databricks_config, tmp_path, table_name_2, df2)

        @td.publisher(
            name="test_source_partial_table_multiple_tdfnrun",
            source=td.DatabricksSource(
                host,
                td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                queries=[
                    f"select * from {table_sql_1} where id < 1024",
                    f"select * from {table_sql_2} where id >= 1024",
                ],
                warehouse=warehouse_name,
            ),
            tables=["output1", "output2"],
        )
        def input_databricks_partial_multi(
            data1: td.TableFrame, data2: td.TableFrame
        ) -> tuple[td.TableFrame, td.TableFrame]:
            return data1, data2

        fnrun = tdfnrun(function=input_databricks_partial_multi)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 2

        check_databricks_table(
            output[0].as_pl_df(with_sys_columns=False),
            df1.filter(pl.col("id") < 1024),
        )
        check_databricks_table(
            output[1].as_pl_df(with_sys_columns=False),
            df2.filter(pl.col("id") >= 1024),
        )
    finally:
        delete_databricks_table(databricks_client, table_name_1)
        delete_databricks_table(databricks_client, table_name_2)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_initial_values(
    tmp_path,
    databricks_client,
    databricks_config
):
    host = databricks_config["HOST"]
    token = databricks_config["TOKEN"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df = generate_persons_data()
    table_name = f"{catalog}.{schema}.test_source_initial_values_{uuid.uuid4().hex[:12]}"
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, df)
        source = td.DatabricksSource(
            host,
            token,
            queries=f"select * from {table_sql} where id > :min_id",
            initial_values={"min_id": 1024},
            warehouse=warehouse_name,
        )
        result_files = source.chunk(str(tmp_path))
        assert len(result_files) == 1
        result_df = pl.read_parquet(result_files[0])
        expected = df.filter(pl.col("id") > 1024)
        check_databricks_table(result_df, expected)
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_initial_values_tdfnrun(
    tmp_path,
    databricks_client,
    databricks_config
):
    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    df = generate_persons_data()
    table_name = f"{catalog}.{schema}.test_source_initial_values_tdfnrun_{uuid.uuid4().hex[:12]}"
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, df)

        @td.publisher(
            name="test_source_initial_values_tdfnrun",
            source=td.DatabricksSource(
                host,
                td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                queries=f"select * from {table_sql} where id >= :min_id",
                initial_values={"min_id": 1024},
                warehouse=warehouse_name,
            ),
            tables="output",
        )
        def input_databricks_iv(data: td.TableFrame) -> tuple[td.TableFrame, dict]:
            return data, {"min_id": 1024}

        fnrun = tdfnrun(function=input_databricks_iv)
        assert fnrun.status == 0

        output = fnrun.output
        assert len(output) == 1

        result_df = output[0].as_pl_df(with_sys_columns=False)
        expected = df.filter(pl.col("id") >= 1024)
        check_databricks_table(result_df, expected)
    finally:
        delete_databricks_table(databricks_client, table_name)


@pytest.mark.databricks
@pytest.mark.requires_internet
@pytest.mark.slow
@pytest.mark.tdfnrun
@pytest.mark.timeout(DATABRICKS_BUDGET_SAFETY_TIMEOUT)
def test_source_incremental_ingestion(
    tmp_path,
    databricks_client,
    databricks_config
):
    batch_size = 512
    rounds = 4

    host = databricks_config["HOST"]
    warehouse_name = databricks_config["WAREHOUSE_NAME"]
    catalog = databricks_config["CATALOG"]
    schema = databricks_config["SCHEMA"]
    persons_data = generate_persons_data(batch_size * rounds)
    batches = [
        persons_data.filter(
            (pl.col("id") >= i * batch_size)
            & (pl.col("id") < (i + 1) * batch_size)
        )
        for i in range(rounds)
    ]
    uid = uuid.uuid4().hex[:12]
    table_name = f"{catalog}.{schema}.test_source_incremental_{uid}"
    table_sql = _table_fqn_4sql(table_name)
    try:
        write_databricks_table(databricks_config, tmp_path, table_name, batches[0])
        current_offset = -1
        for round_idx in range(rounds):
            if round_idx > 0:
                append_databricks_table(
                    databricks_config, tmp_path, table_name, batches[round_idx]
                )
            expected_batch = batches[round_idx]
            min_id_for_query = current_offset

            @td.publisher(
                name=f"test_source_incremental_ingestion_{round_idx}",
                source=td.DatabricksSource(
                    host,
                    td.EnvironmentSecret(databricks_config["TOKEN_ENV"]),
                    queries=f"select * from {table_sql} where id > :min_id",
                    initial_values={"min_id": min_id_for_query},
                    warehouse=warehouse_name,
                ),
                tables="output",
            )
            def incremental_read(
                data: td.TableFrame,
            ) -> tuple[td.TableFrame, dict]:
                return data, {"min_id": min_id_for_query}

            fnrun = tdfnrun(function=incremental_read)
            assert fnrun.status == 0, (
                f"Round {round_idx} failed with status {fnrun.status}"
            )

            output = fnrun.output
            assert len(output) == 1

            result_df = output[0].as_pl_df(with_sys_columns=False)
            check_databricks_table(result_df, expected_batch)

            current_offset = expected_batch["id"].max()

    finally:
        delete_databricks_table(databricks_client, table_name)
