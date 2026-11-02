#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

import json
import textwrap
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli._errors import ExitCode
from tabsdatak.cli._fn_check import check_function
from tabsdatak.cli._scaffold import ScaffoldError, build_scaffold, connectors
from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _source(text: str) -> str:
    return textwrap.dedent(text).lstrip()


def test_every_connector_scaffolds_a_function_that_checks(tmp_path):
    found = [c for c in connectors() if c.cls.__module__.startswith("tabsdatak.conn.")]
    assert {c.kind for c in found} == {"publisher", "stream_publisher", "subscriber"}
    assert not [c for c in found if c.type.startswith(".")]
    invalid = {}
    for bound in found:
        made = build_scaffold(None, bound.type)
        path = tmp_path / f"{made.name}.py"
        path.write_text(made.source)
        result = check_function(f"{path}::{made.name}")
        if not result["valid"]:
            invalid[bound.type] = result["findings"]
        else:
            assert result["function"]["kind"] == bound.kind
    assert invalid == {}


def test_a_publisher_takes_one_slot_per_output():
    made = build_scaffold("publisher", "mysql-sql-in", outputs=["customers", "orders"])
    assert made.name == "pub_mysql_sql_in"
    assert made.connector.system == "MySQL"
    assert made.source == _source(
        """
        from tabsdatak.api import TableFrameSpec, publisher
        from tabsdatak.conn.mysql import MySQLSrc


        @publisher(
            source=MySQLSrc(
                queries=["<value>", "<value>"],
            ),
            output_tables=["customers", "orders"],
        )
        def pub_mysql_sql_in(
            customers: TableFrameSpec, orders: TableFrameSpec
        ) -> tuple[TableFrameSpec, TableFrameSpec]:
            return customers, orders
        """
    )


def test_a_file_publisher_receives_every_file_of_a_slot():
    made = build_scaffold("publisher", "s3-file-in", name="load_events")
    assert made.source == _source(
        """
        from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
        from tabsdatak.conn.s3 import S3Src
        from tabsdatak.tableframe.functions import concat


        @publisher(
            source=S3Src(
                paths=["<value>"],
            ),
            output_tables=["output"],
        )
        def load_events(output: TableFramesSpec) -> TableFrameSpec:
            return concat(output or [])
        """
    )


def test_a_stream_publisher_batches_its_frames():
    made = build_scaffold(None, "kafka-topic-in", outputs=["clicks"])
    assert made.kind == "stream_publisher"
    assert "batch_max_secs=300," in made.source
    assert "def stream_kafka_topic_in(clicks: TableFramesSpec)" in made.source
    assert "return concat(clicks or [])" in made.source


def test_a_transformer_binds_no_connector():
    made = build_scaffold(
        "transformer", inputs=["sales/orders@HEAD", "items"], outputs=["joined"]
    )
    assert made.connector is None
    assert made.source == _source(
        """
        from tabsdatak.api import TableFrameSpec, transformer


        @transformer(
            input_tables=["sales/orders@HEAD", "items"],
            output_tables=["joined"],
        )
        def transform(orders: TableFrameSpec, items: TableFrameSpec) -> TableFrameSpec:
            return orders
        """
    )


def test_a_subscriber_takes_one_slot_per_input():
    made = build_scaffold(
        "subscriber", "postgres-sql-out", inputs=["crm/customers", "crm/orders"]
    )
    assert 'tables=["<value>", "<value>"],' in made.source
    assert 'input_tables=["crm/customers", "crm/orders"],' in made.source
    assert "return customers, orders" in made.source
    assert "conn" not in made.source.replace("tabsdatak.conn.", "")


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"kind": None}, "Pass --kind"),
        ({"kind": "transformer", "type_": "mysql-sql-in"}, "binds no connector"),
        ({"kind": "publisher"}, "needs --connector"),
        ({"kind": "subscriber", "type_": "mysql-sql-in"}, "binds a publisher"),
        ({"kind": "publisher", "type_": "nope"}, "Unknown connector 'nope'"),
        ({"kind": "transformer", "name": "1bad"}, "not a valid Python"),
        (
            {"kind": "publisher", "type_": "mysql-sql-in", "inputs": ["t"]},
            "takes no --input",
        ),
        (
            {"kind": "subscriber", "type_": "dev-trash-out", "outputs": ["t"]},
            "takes no --output",
        ),
        (
            {"kind": "subscriber", "type_": "dev-trash-out", "inputs": ["a", "b"]},
            "takes a single table",
        ),
    ],
)
def test_scaffold_refuses_what_cannot_be_made(kwargs, message):
    with pytest.raises(ScaffoldError, match=message):
        build_scaffold(**kwargs)


def _invoke(argv, tmp_path, env=None):
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()

    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with (
        patch("tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject),
        patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)),
    ):
        result = CliRunner().invoke(cli, argv, env=env)
    assert client.method_calls == []
    return result


def _document(result) -> dict:
    lines = result.stdout.splitlines()
    assert len(lines) == 1, result.output
    return json.loads(lines[0])


def test_scaffold_prints_the_module(tmp_path):
    result = _invoke(["examples", "scaffold", "--kind", "transformer"], tmp_path)
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith("from tabsdatak.api import")
    assert "def transform(input: TableFrameSpec)" in result.stdout


def test_scaffold_writes_the_file_and_reports_it(tmp_path):
    target = tmp_path / "pub.py"
    argv = ["examples", "scaffold", "--connector", "postgres-sql-in", "-f", str(target)]
    document = _document(_invoke([*argv, "--machine"], tmp_path))
    assert document["path"] == str(target)
    assert document["source"] == target.read_text()
    assert document["connector"] == {"type": "postgres-sql-in", "system": "PostgreSQL"}
    assert "pub-postgres-full" in document["examples"]


def test_scaffold_never_overwrites_a_file(tmp_path):
    target = tmp_path / "pub.py"
    target.write_text("mine")
    result = _invoke(
        [
            "examples",
            "scaffold",
            "--kind",
            "transformer",
            "-f",
            str(target),
            "--machine",
        ],
        tmp_path,
    )
    assert result.exit_code != ExitCode.OK
    assert target.read_text() == "mine"


def test_scaffold_option_errors_are_usage_errors(tmp_path):
    result = _invoke(
        ["examples", "scaffold", "--kind", "publisher", "--machine"], tmp_path
    )
    assert result.exit_code == ExitCode.USAGE, result.output
    assert "needs --connector" in _document(result)["error"]["message"]


def test_scaffold_lists_connectors(tmp_path):
    document = _document(
        _invoke(["examples", "scaffold", "--list-connectors", "--machine"], tmp_path)
    )
    types = {row["type"]: row for row in document["items"]}
    assert types["mysql-sql-in"] == {
        "type": "mysql-sql-in",
        "system": "MySQL",
        "kind": "publisher",
        "class": "MySQLSrc",
        "module": "tabsdatak.conn.mysql",
    }
    assert document["truncated"] is False


def test_scaffold_refuses_keywords_and_imported_names():
    with pytest.raises(ScaffoldError, match="not a valid Python function name"):
        build_scaffold("transformer", name="class")
    with pytest.raises(ScaffoldError, match="shadow the imported 'transformer'"):
        build_scaffold("transformer", name="transformer")


def test_scaffold_parameters_never_shadow_imports_or_the_function():
    stream = next(c.type for c in connectors() if c.kind == "stream_publisher")
    made = build_scaffold(None, stream, name="pub", outputs=["concat", "pub"])
    compile(made.source, "scaffold.py", "exec")
    assert "concat(concat_2 or [])" in made.source
    assert "pub_2: TableFramesSpec" in made.source
    made = build_scaffold("transformer", inputs=["class", "TableFrameSpec"])
    assert "class_2: TableFrameSpec, tableframespec" in made.source.replace("\n", " ")
    compile(made.source, "scaffold.py", "exec")


def test_scaffold_file_conflicts_and_missing_directories(tmp_path):
    target = tmp_path / "pub.py"
    target.write_text("mine")
    argv = ["examples", "scaffold", "--kind", "transformer", "--machine", "-f"]
    assert _invoke([*argv, str(target)], tmp_path).exit_code == ExitCode.CONFLICT
    missing = _invoke([*argv, str(tmp_path / "nowhere" / "t.py")], tmp_path)
    assert missing.exit_code == ExitCode.USAGE, missing.output
    elsewhere = tmp_path / "elsewhere.py"
    link = tmp_path / "dangling.py"
    link.symlink_to(elsewhere)
    assert _invoke([*argv, str(link)], tmp_path).exit_code == ExitCode.CONFLICT
    assert not elsewhere.exists()
