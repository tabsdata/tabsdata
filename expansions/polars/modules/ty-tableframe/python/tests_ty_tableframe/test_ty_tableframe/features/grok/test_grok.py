#
# Copyright 2025 Tabs Data Inc.
#

import logging
import tempfile
from timeit import default_timer as timer

import polars as pl
import pytest

import tabsdata as td
import tabsdata.tableframe.functions.col as td_col
from tabsdata._utils.temps import tabsdata_temp_folder

logger = logging.getLogger(__name__)


def setup_polars_config():
    pl.Config.set_tbl_cols(1024)
    pl.Config.set_tbl_rows(1024)
    pl.Config.set_fmt_table_cell_list_len(1024)
    pl.Config.set_tbl_width_chars(4096)
    pl.Config.set_fmt_str_lengths(4096)


class TestGrokCommonPatterns:

    @staticmethod
    def setup_method():
        setup_polars_config()

    def test_apache_access_log(self):
        pattern = (
            r"%{IPV4:client_ip} "
            r"%{USER:ident} %{USER:auth} "
            r"\[%{HTTPDATE:timestamp}\] "
            r'"%{WORD:method} '
            r"%{URIPATHPARAM:request} "
            r'HTTP/%{NUMBER:http_version}" '
            r"%{INT:response_code} %{INT:bytes}"
        )
        schema = {
            "client_ip": td_col.Column("ip_address", td.String),
            "ident": td_col.Column("identity", td.String),
            "auth": td_col.Column("auth_user", td.String),
            "timestamp": td_col.Column("request_time", td.String),
            "method": td_col.Column("http_method", td.String),
            "request": td_col.Column("uri_path", td.String),
            "http_version": td_col.Column("http_ver", td.Float64),
            "response_code": td_col.Column("status_code", td.Int32),
            "bytes": td_col.Column("response_bytes", td.Int64),
        }

        log_data = [
            (
                '192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif'
                ' HTTP/1.0" 200 2326'
            ),
            (
                '192.168.1.2 - alice [10/Oct/2000:13:55:37 -0700] "POST /login'
                ' HTTP/1.1" 302 1024'
            ),
            (
                '10.0.0.1 - bob [10/Oct/2000:13:55:38 -0700] "GET /index.html HTTP/1.1"'
                " 200 5678"
            ),
        ]

        tf = td.TableFrame(df={"log_entry": log_data}, origin=None)
        result = tf.grok("log_entry", pattern, schema)

        assert "ip_address" in result.schema.names()
        assert "status_code" in result.schema.names()
        assert "response_bytes" in result.schema.names()

        collected = result._lf.collect()

        assert collected["ip_address"].dtype == pl.String
        assert collected["status_code"].dtype == pl.Int32
        assert collected["response_bytes"].dtype == pl.Int64
        assert collected["http_ver"].dtype == pl.Float64

        assert collected["ip_address"][0] == "192.168.1.1"
        assert collected["status_code"][0] == 200
        assert collected["response_bytes"][0] == 2326

    def test_syslog_pattern(self):
        pattern = (
            r"%{SYSLOGTIMESTAMP:timestamp} "
            r"%{HOSTNAME:host} "
            r"%{WORD:process}(?:\[%{INT:pid}\])?: %{GREEDYDATA:message}"
        )
        schema = {
            "timestamp": td_col.Column("log_time", td.String),
            "host": td_col.Column("hostname", td.String),
            "process": td_col.Column("process_name", td.String),
            "pid": td_col.Column("process_id", td.Int32),
            "message": td_col.Column("log_message", td.String),
        }

        log_data = [
            (
                "Oct 10 13:55:36 server1 sshd[1234]: Accepted publickey for user from"
                " 192.168.1.1"
            ),
            "Oct 10 13:55:37 server2 kernel: Out of memory: Kill process 5678",
            "Oct 10 13:55:38 server1 httpd[9999]: Connection established",
        ]

        tf = td.TableFrame(df={"syslog": log_data}, origin=None)
        result = tf.grok("syslog", pattern, schema)

        assert "hostname" in result.schema.names()
        assert "process_name" in result.schema.names()
        assert "process_id" in result.schema.names()
        assert "log_message" in result.schema.names()

        collected = result._lf.collect()

        assert collected["hostname"][0] == "server1"
        assert collected["process_name"][0] == "sshd"
        assert collected["process_id"][0] == 1234

    def test_multiple_captures_same_pattern(self):
        pattern = (
            r"%{WORD:action}-"
            r"%{INT:year}-"
            r"%{WORD:user}-"
            r"%{INT:id}-"
            r"%{WORD:status}"
        )
        schema = {
            "action": td_col.Column("operation", td.String),
            "year": td_col.Column("event_year", td.Int64),
            "user": td_col.Column("username", td.String),
            "id": td_col.Column("user_id", td.Int64),
            "status": td_col.Column("result", td.String),
        }

        data = [
            "login-2023-alice-1001-success",
            "logout-2024-bob-1002-failed",
            "update-2023-charlie-1003-success",
        ]

        tf = td.TableFrame(df={"events": data}, origin=None)
        result = tf.grok("events", pattern, schema)

        expected_columns = ["operation", "event_year", "username", "user_id", "result"]
        for column in expected_columns:
            assert column in result.schema.names()

        collected = result._lf.collect()

        assert collected["operation"].dtype == pl.String
        assert collected["event_year"].dtype == pl.Int64
        assert collected["user_id"].dtype == pl.Int64

        assert collected["operation"][0] == "login"
        assert collected["event_year"][0] == 2023
        assert collected["username"][0] == "alice"
        assert collected["user_id"][0] == 1001


class TestGrokDataTypes:

    @staticmethod
    def setup_method():
        setup_polars_config()

    def test_numeric_data_types(self):
        pattern = (
            r"%{INT:small_int} "
            r"%{INT:regular_int} "
            r"%{INT:big_int} "
            r"%{NUMBER:decimal} "
            r"%{NUMBER:float_val} "
            r"%{WORD:bool_str}"
        )
        schema = {
            "small_int": td_col.Column("tiny", td.Int8),
            "regular_int": td_col.Column("normal", td.Int32),
            "big_int": td_col.Column("large", td.Int64),
            "decimal": td_col.Column("precise", td.Float64),
            "float_val": td_col.Column("approx", td.Float32),
            "bool_str": td_col.Column("flag", td.String),
        }

        data = [
            "42 1000 9223372036854775807 123.456 78.9 true",
            "-128 -50000 -1234567890123456 -987.654 -12.3 false",
            "0 0 0 0.0 0.0 null",
        ]

        tf = td.TableFrame(df={"numbers": data}, origin=None)
        result = tf.grok("numbers", pattern, schema)

        collected = result._lf.collect()

        assert collected["tiny"].dtype == pl.Int8
        assert collected["normal"].dtype == pl.Int32
        assert collected["large"].dtype == pl.Int64
        assert collected["precise"].dtype == pl.Float64
        assert collected["approx"].dtype == pl.Float32
        assert collected["flag"].dtype == pl.String

        assert collected["tiny"][0] == 42
        assert collected["normal"][0] == 1000
        assert collected["large"][0] == 9223372036854775807
        assert abs(collected["precise"][0] - 123.456) < 0.001
        assert abs(collected["approx"][0] - 78.9) < 0.1

    def test_string_and_date_types(self):
        pattern = (
            r"%{TIMESTAMP_ISO8601:iso_date} "
            r"%{WORD:category} "
            r'"%{DATA:description}" '
            r"%{UUID:record_id}"
        )
        schema = {
            "iso_date": td_col.Column("event_timestamp", td.String),
            "category": td_col.Column("event_type", td.String),
            "description": td_col.Column("details", td.String),
            "record_id": td_col.Column("uuid", td.String),
        }

        data = [
            (
                '2023-10-15T14:30:45.123Z ERROR "Database connection failed"'
                " 550e8400-e29b-41d4-a716-446655440000"
            ),
            (
                '2024-01-20T09:15:30.000Z INFO "User login successful"'
                " 6ba7b810-9dad-11d1-80b4-00c04fd430c8"
            ),
        ]

        tf = td.TableFrame(df={"log_entries": data}, origin=None)
        result = tf.grok("log_entries", pattern, schema)

        collected = result._lf.collect()

        assert collected["event_timestamp"][0] == "2023-10-15T14:30:45.123Z"
        assert collected["event_type"][0] == "ERROR"
        assert collected["details"][0] == "Database connection failed"
        assert collected["uuid"][0] == "550e8400-e29b-41d4-a716-446655440000"

    def test_boolean_casting(self):
        pattern = r"%{WORD:status} " r"%{WORD:active} " r"%{INT:enabled}"

        schema = {
            "status": td_col.Column("is_ok", td.String),
            "active": td_col.Column("is_active", td.String),
            "enabled": td_col.Column("is_enabled", td.Int8),
        }

        data = [
            "success true 1",
            "failed false 0",
            "pending unknown -1",
        ]

        tf = td.TableFrame(df={"flags": data}, origin=None)
        result = tf.grok("flags", pattern, schema)

        collected = result._lf.collect()

        assert collected["is_ok"][0] == "success"
        assert collected["is_active"][0] == "true"
        assert collected["is_enabled"][0] == 1


class TestGrokOperations:

    @staticmethod
    def setup_method():
        setup_polars_config()

    def test_filtering_after_grok(self):
        pattern = r"%{WORD:user} " r"%{INT:score} " r"%{WORD:level}"
        schema = {
            "user": td_col.Column("player", td.String),
            "score": td_col.Column("points", td.Int64),
            "level": td_col.Column("difficulty", td.String),
        }

        data = [
            "alice 1500 easy",
            "bob 2800 hard",
            "charlie 950 medium",
            "diana 3200 hard",
        ]

        tf = td.TableFrame(df={"game_data": data}, origin=None)
        result = tf.grok("game_data", pattern, schema)

        high_scores = result.filter(td.col("points") > 2000)

        collected = high_scores._lf.collect()

        assert len(collected) == 2
        assert "bob" in collected["player"].to_list()
        assert "diana" in collected["player"].to_list()

        hard_level = result.filter(td.col("difficulty") == "hard")

        collected_hard = hard_level._lf.collect()

        assert len(collected_hard) == 2
        assert all(level == "hard" for level in collected_hard["difficulty"])

    def test_group_by_after_grok(self):
        pattern = r"%{WORD:department} " r"%{INT:salary} " r"%{WORD:position}"
        schema = {
            "department": td_col.Column("dept", td.String),
            "salary": td_col.Column("pay", td.Int64),
            "position": td_col.Column("role", td.String),
        }

        data = [
            "engineering 75000 developer",
            "engineering 85000 senior",
            "marketing 60000 analyst",
            "marketing 70000 manager",
            "engineering 95000 lead",
        ]

        tf = td.TableFrame(df={"employee_data": data}, origin=None)
        result = tf.grok("employee_data", pattern, schema)

        grouped = result.group_by("dept").agg(td.col("pay").mean().alias("avg_salary"))

        collected = grouped._lf.collect().sort("dept")

        eng_avg = collected.filter(pl.col("dept") == "engineering")["avg_salary"][0]
        assert abs(eng_avg - 85000) < 0.1

        mkt_avg = collected.filter(pl.col("dept") == "marketing")["avg_salary"][0]
        assert abs(mkt_avg - 65000) < 0.1

    def test_joins_after_grok(self):
        pattern = r"%{INT:id} " r"%{WORD:status}"
        schema = {
            "id": td_col.Column("order_id", td.Int64),
            "status": td_col.Column("order_status", td.String),
        }

        order_data = [
            "1001 pending",
            "1002 shipped",
            "1003 delivered",
        ]

        tf_orders = td.TableFrame(df={"orders": order_data}, origin=None)
        grok_result = tf_orders.grok("orders", pattern, schema)

        lookup_data = {
            "order_id": [1001, 1002, 1003],
            "customer": ["Alice", "Bob", "Charlie"],
        }
        tf_customers = td.TableFrame(df=lookup_data, origin=None)

        joined = grok_result.join(tf_customers, on="order_id", how="inner")

        collected = joined._lf.collect().sort("order_id")

        assert len(collected) == 3
        assert collected["customer"][0] == "Alice"
        assert collected["order_status"][0] == "pending"

    def test_window_functions_after_grok(self):
        pattern = r"%{WORD:product} %{INT:quarter} %{INT:sales}"
        schema = {
            "product": td_col.Column("item", td.String),
            "quarter": td_col.Column("q", td.Int32),
            "sales": td_col.Column("revenue", td.Int64),
        }

        data = [
            "laptop 1 100000",
            "laptop 2 120000",
            "tablet 1 80000",
            "tablet 2 75000",
            "phone 1 150000",
            "phone 2 160000",
        ]

        tf = td.TableFrame(df={"sales_data": data}, origin=None)
        result = tf.grok("sales_data", pattern, schema)

        windowed = result.with_columns(td.col("revenue").rank().alias("rank"))

        collected = windowed._lf.collect().sort(["item", "q"])

        assert "rank" in collected.columns
        assert all(rank > 0 for rank in collected["rank"])

        min_revenue_row = collected.filter(
            pl.col("revenue") == collected["revenue"].min()
        )
        max_revenue_row = collected.filter(
            pl.col("revenue") == collected["revenue"].max()
        )

        assert min_revenue_row["rank"][0] == 1
        assert max_revenue_row["rank"][0] == len(collected)


class TestGrokSchemaOrdering:

    @staticmethod
    def setup_method():
        setup_polars_config()

    def test_field_ordering_by_schema(self):
        pattern = r"%{WORD:c} " r"%{WORD:a} " r"%{WORD:b}"
        schema = {
            "a": td_col.Column("alpha", td.String),
            "b": td_col.Column("beta", td.String),
            "c": td_col.Column("gamma", td.String),
        }

        data = ["third first second"]

        tf = td.TableFrame(df={"data": data}, origin=None)
        result = tf.grok("data", pattern, schema)

        schema_names = [schema[k].name or k for k in schema.keys()]
        result_names = result.schema.names()

        grok_fields = [name for name in result_names if name in schema_names]

        expected_order = ["alpha", "beta", "gamma"]
        assert grok_fields == expected_order

        collected = result._lf.collect()

        assert collected["alpha"][0] == "first"
        assert collected["beta"][0] == "second"
        assert collected["gamma"][0] == "third"

    def test_field_renaming(self):
        pattern = r"%{IPV4:ip} " r"%{INT:port} " r"%{WORD:proto}"
        schema = {
            "ip": td_col.Column("source_address", td.String),
            "port": td_col.Column("source_port", td.Int32),
            "proto": td_col.Column("protocol", td.String),
        }

        data = ["192.168.1.100 8080 tcp"]

        tf = td.TableFrame(df={"network": data}, origin=None)
        result = tf.grok("network", pattern, schema)

        result_names = result.schema.names()
        assert "ip" not in result_names
        assert "port" not in result_names
        assert "proto" not in result_names

        assert "source_address" in result_names
        assert "source_port" in result_names
        assert "protocol" in result_names

        collected = result._lf.collect()

        assert collected["source_address"][0] == "192.168.1.100"
        assert collected["source_port"][0] == 8080
        assert collected["protocol"][0] == "tcp"


class TestGrokPerformance:

    @staticmethod
    def setup_method():
        setup_polars_config()

    @pytest.mark.slow
    def test_large_dataset_performance(self):
        pattern = r"%{IPV4:ip} " r"%{WORD:method} " r"%{INT:response} " r"%{INT:bytes}"
        schema = {
            "ip": td_col.Column("client_ip", td.String),
            "method": td_col.Column("http_method", td.String),
            "response": td_col.Column("status_code", td.Int32),
            "bytes": td_col.Column("response_size", td.Int64),
        }

        def generate_log_entry(i: int) -> str:
            ip = f"192.168.{(i % 255) + 1}.{(i % 100) + 1}"
            method = ["GET", "POST", "PUT", "DELETE"][i % 4]
            response = [200, 404, 500, 302][i % 4]
            bytes_val = (i % 10000) + 1000
            return f"{ip} {method} {response} {bytes_val}"

        num_rows = 1_000_000

        print(f"\nGenerating {num_rows:,} rows of test data...")
        start = timer()
        log_data = [generate_log_entry(i) for i in range(num_rows)]
        end = timer()
        time_taken = end - start
        print(f"Data generation took {time_taken:.2f} seconds")

        tf = td.TableFrame(df={"logs": log_data}, origin=None)

        print("Applying grok pattern...")
        start = timer()
        result = tf.grok("logs", pattern, schema)
        end = timer()
        time_taken = end - start
        print(f"Grok operation took {time_taken:.2f} seconds")

        print("Verifying result structure...")
        schema_names = result.schema.names()
        assert "client_ip" in schema_names
        assert "http_method" in schema_names
        assert "status_code" in schema_names
        assert "response_size" in schema_names

        print("Collecting data...")
        start = timer()
        sample = result._lf.collect()
        end = timer()
        time_taken = end - start
        print(f"Data collection took {time_taken:.2f} seconds")
        rows_per_second = num_rows / time_taken
        print(f"Performance: {rows_per_second:,.0f} rows collected per second")
        assert len(sample) == num_rows
        assert sample["client_ip"].dtype == pl.String
        assert sample["status_code"].dtype == pl.Int32
        assert sample["response_size"].dtype == pl.Int64

        print("Sinking raw data to parquet...")
        start = timer()
        with tempfile.NamedTemporaryFile(
            suffix=".parquet", dir=tabsdata_temp_folder(), delete=False
        ) as temp_file:
            temp_path = temp_file.name
        tf._lf.sink_parquet(temp_path)
        end = timer()
        time_taken = end - start
        print(
            f"Raw data sinking to parquet collection took {time_taken:.2f} seconds"
            f" ({temp_path})"
        )
        rows_per_second = num_rows / time_taken
        print(f"Performance: {rows_per_second:,.0f} rows collected per second")

        print("Sinking data to parquet...")
        start = timer()
        with tempfile.NamedTemporaryFile(
            suffix=".parquet", dir=tabsdata_temp_folder(), delete=False
        ) as temp_file:
            temp_path = temp_file.name
        result._lf.sink_parquet(temp_path)
        end = timer()
        time_taken = end - start
        print(f"Data sinking to parquet took {time_taken:.2f} seconds ({temp_path})")
        rows_per_second = num_rows / time_taken
        print(f"Performance: {rows_per_second:,.0f} rows sinked to parquet per second")

        assert (
            rows_per_second > 10_000
        ), f"Performance too slow: {rows_per_second:,.0f} rows/sec"

        print("Testing operations on large result...")
        start = timer()
        method_counts = result.group_by("http_method").agg(
            td.col("client_ip").count().alias("count")
        )
        method_result = method_counts._lf.collect()
        high_traffic = (
            result.group_by("client_ip")
            .agg(td.col("status_code").count().alias("requests"))
            .filter(td.col("requests") > 1000)
        )
        high_traffic_count = high_traffic._lf.collect().shape[0]
        end = timer()
        time_taken = end - start
        print(f"Operations took {time_taken:.2f} seconds")
        print(f"Found {len(method_result)} HTTP methods")
        print(f"Found {high_traffic_count} high-traffic IPs")

        assert len(method_result) == 4

    def test_complex_pattern_performance(self):
        pattern = (
            r"%{TIMESTAMP_ISO8601:timestamp} \[%{WORD:level}\] "
            r"%{WORD:service}\.%{WORD:component} - "
            r"%{WORD:event}:%{INT:count} user=%{WORD:user} "
            r"ip=%{IPV4:ip} duration=%{NUMBER:duration}ms "
            r"status=%{INT:status} size=%{INT:size}"
        )
        schema = {
            "timestamp": td_col.Column("event_time", td.String),
            "level": td_col.Column("log_level", td.String),
            "service": td_col.Column("service_name", td.String),
            "component": td_col.Column("component_name", td.String),
            "event": td_col.Column("event_type", td.String),
            "count": td_col.Column("event_count", td.Int32),
            "user": td_col.Column("username", td.String),
            "ip": td_col.Column("client_ip", td.String),
            "duration": td_col.Column("response_time", td.Float64),
            "status": td_col.Column("http_status", td.Int32),
            "size": td_col.Column("response_bytes", td.Int64),
        }

        def generate_complex_log(i: int) -> str:
            timestamp = (
                f"2024-01-{(i % 30) + 1:02d}T{(i % 24):02d}:{(i % 60):02d}:{(i % 60):02d}.123Z"
            )
            level = ["INFO", "WARN", "ERROR", "DEBUG"][i % 4]
            service = ["api", "web", "auth", "db"][i % 4]
            component = ["controller", "handler", "validator", "client"][i % 4]
            event = ["request", "response", "error", "timeout"][i % 4]
            count = (i % 100) + 1
            user = f"user{i % 1000}"
            ip = f"10.{(i % 255)}.{(i % 255)}.{(i % 255)}"
            duration = ((i % 5000) + 10) / 10.0  # 1.0 to 500.0 ms
            status = [200, 400, 404, 500][i % 4]
            size = (i % 100000) + 1000

            return (
                f"{timestamp} [{level}] {service}.{component} - "
                f"{event}:{count} user={user} ip={ip} duration={duration}ms "
                f"status={status} size={size}"
            )

        num_rows = 1_000_000

        print(f"\nGenerating {num_rows:,} complex log entries...")
        start = timer()
        log_data = [generate_complex_log(i) for i in range(num_rows)]
        end = timer()
        time_taken = end - start
        print(f"Generation took {time_taken:.2f} seconds")

        tf = td.TableFrame(df={"complex_logs": log_data}, origin=None)

        print("Applying complex grok pattern...")
        start = timer()
        result = tf.grok("complex_logs", pattern, schema)
        end = timer()
        time_taken = end - start
        print(f"Complex grok took {time_taken:.2f} seconds")

        expected_fields = [
            ("event_time", pl.String),
            ("log_level", pl.String),
            ("service_name", pl.String),
            ("component_name", pl.String),
            ("event_type", pl.String),
            ("event_count", pl.Int32),
            ("username", pl.String),
            ("client_ip", pl.String),
            ("response_time", pl.Float64),
            ("http_status", pl.Int32),
            ("response_bytes", pl.Int64),
        ]

        print("Collecting data...")
        start = timer()
        sample = result._lf.collect()
        end = timer()
        time_taken = end - start
        print(f"Data collection took {time_taken:.2f} seconds")
        rows_per_second = num_rows / time_taken
        print(f"Performance: {rows_per_second:,.0f} rows collected per second")
        assert len(sample) == num_rows

        print("Sinking raw data to parquet...")
        start = timer()
        with tempfile.NamedTemporaryFile(
            suffix=".parquet", dir=tabsdata_temp_folder(), delete=False
        ) as temp_file:
            temp_path = temp_file.name
        tf._lf.sink_parquet(temp_path)
        end = timer()
        time_taken = end - start
        print(
            f"Raw data sinking to parquet collection took {time_taken:.2f} seconds"
            f" ({temp_path})"
        )
        rows_per_second = num_rows / time_taken
        print(f"Performance: {rows_per_second:,.0f} rows collected per second")

        print("Sinking data to parquet...")
        start = timer()
        with tempfile.NamedTemporaryFile(
            suffix=".parquet", dir=tabsdata_temp_folder(), delete=False
        ) as temp_file:
            temp_path = temp_file.name
        result._lf.sink_parquet(temp_path)
        end = timer()
        time_taken = end - start
        print(f"Data sinking to parquet took {time_taken:.2f} seconds ({temp_path})")
        rows_per_second = num_rows / time_taken
        print(f"Performance: {rows_per_second:,.0f} rows sinked to parquet per second")

        for field_name, expected_dtype in expected_fields:
            assert field_name in sample.columns
            assert sample[field_name].dtype == expected_dtype

        assert (
            rows_per_second > 5_000
        ), f"Complex pattern too slow: {rows_per_second:,.0f} rows/sec"


class TestGrokErrorHandling:

    @staticmethod
    def setup_method():
        setup_polars_config()

    def test_non_matching_rows(self):
        pattern = r"%{WORD:action} %{INT:value}"
        schema = {
            "action": td_col.Column("operation", td.String),
            "value": td_col.Column("amount", td.Int32),
        }

        data = [
            "save 100",
            "invalid data",
            "load 200",
            "",
            "delete 50",
        ]

        tf = td.TableFrame(df={"mixed_data": data}, origin=None)
        result = tf.grok("mixed_data", pattern, schema)

        collected = result._lf.collect()

        assert collected["operation"][0] == "save"
        assert collected["amount"][0] == 100

        assert collected["operation"][1] is None
        assert collected["amount"][1] is None

        assert collected["operation"][2] == "load"
        assert collected["amount"][2] == 200

    def test_empty_captures(self):
        pattern = r"%{WORD:first}(?:%{SPACE}%{WORD:second})?"

        schema = {
            "first": td_col.Column("word1", td.String),
            "second": td_col.Column("word2", td.String),
        }

        data = [
            "hello world",  # both captured
            "hello",
        ]

        tf = td.TableFrame(df={"optional_data": data}, origin=None)
        result = tf.grok("optional_data", pattern, schema)

        collected = result._lf.collect()

        assert collected["word1"][0] == "hello"
        assert collected["word2"][0] == "world"

        assert collected["word1"][1] == "hello"
        assert collected["word2"][1] is None or collected["word2"][1] == ""
