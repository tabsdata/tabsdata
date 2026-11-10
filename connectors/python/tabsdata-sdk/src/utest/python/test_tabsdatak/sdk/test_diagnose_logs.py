#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tabsdatak.common._archive.bundle import BUNDLE_USER_CODE_DIRNAME
from tabsdatak.sdk import _diagnose as d, tabsdata_server as ts
from tabsdatak.sdk._apiserver import APIServerError
from test_tabsdatak.sdk.bootest import TESTING_RESOURCES_PATH

LOGS = Path(TESTING_RESOURCES_PATH) / "worker_logs"


def _log(name: str) -> str:
    return (LOGS / f"{name}.txt").read_text()


@pytest.mark.parametrize(
    "name, code, category, frame",
    [
        (
            "polars_column_not_found",
            "column_not_found_error",
            "schema_mismatch",
            ("clean.py", 42, "clean_orders"),
        ),
        (
            "pandas_key_error",
            "key_error",
            "schema_mismatch",
            ("pipeline/transform.py", 17, "enrich"),
        ),
        (
            "module_not_found",
            "module_not_found_error",
            "missing_dependency",
            ("loader.py", 3, "<module>"),
        ),
        (
            "import_error",
            "import_error",
            "missing_dependency",
            ("app.py", 1, "<module>"),
        ),
        ("connection_timeout", "operational_error", "connectivity", None),
        ("connection_refused", "interface_error", "connectivity", None),
        ("auth_denied", "client_error", "auth", None),
        ("out_of_memory", "memory_error", "resource", ("agg.py", 9, "rollup")),
        (
            "nested_during_handling",
            "runtime_error",
            "user_code",
            ("parse.py", 14, "parse"),
        ),
        ("direct_cause", "storage_error", "platform", None),
        ("platform_error", "runtime_error", "platform", None),
        (
            "schema_error",
            "schema_error",
            "schema_mismatch",
            ("join.py", 21, "join_orders"),
        ),
        ("no_traceback_error_line", "error_log_line", "resource", None),
        ("prefixed_traceback", "key_error", "schema_mismatch", ("fx.py", 5, "convert")),
    ],
)
def test_signature_over_the_log_corpus(name, code, category, frame):
    document = d.signature_document(_log(name))
    assert document["code"] == code
    assert document["category"] == category
    expected = (
        {"module": frame[0], "line": frame[1], "function": frame[2]} if frame else None
    )
    assert document["user_frame"] == expected
    assert len(document["message"]) <= d.MESSAGE_CHARS


def test_signature_names_the_short_exception_class():
    signature = d.extract_error_signature(_log("polars_column_not_found"))
    assert signature.message.startswith(
        'ColumnNotFoundError: unable to find column "amount"'
    )
    assert signature.traceback.startswith("Traceback (most recent call last):")


def test_log_records_interleaved_into_a_traceback_are_not_its_exception():
    log = "\n".join([
        "Traceback (most recent call last):",
        '  File "/w/runner.py", line 9, in invoke',
        "2026-11-03T22:22:48.830Z INFO tabsdatak.server.run: Reporting Failed",
        "    results = self.call()",
        '  File "/w/runner.py", line 12, in call',
        "2026-11-03T22:22:48.830Z INFO tabsdatak.server.run: Posting responder",
        "    raise RunFunctionErrorCode.RF_12.exception(",
        'x.UserFunctionFailedError: RF_12: unable to find column "margin_pct"',
    ])
    signature = d.extract_error_signature(log)
    assert signature.message == (
        'UserFunctionFailedError: RF_12: unable to find column "margin_pct"'
    )


def test_the_last_traceback_wins():
    signature = d.extract_error_signature(_log("nested_during_handling"))
    assert signature.message == "RuntimeError: could not parse the amount column"


_USER = f"/w/{BUNDLE_USER_CODE_DIRNAME}/fn.py"


@pytest.mark.parametrize(
    "frame, source, raised",
    [
        (
            "line 403, in transform",
            'total = df["amount"].sum() / n',
            "ZeroDivisionError: division by zero",
        ),
        (
            "line 12, in transform",
            "r = requests.get(url, timeout=5).json()",
            "ValueError: bad json",
        ),
        (
            "line 401, in unauthorized_rows",
            "x = 1",
            "ZeroDivisionError: division by zero",
        ),
    ],
)
def test_frames_and_source_lines_never_pick_the_category(frame, source, raised):
    log = (
        "INFO start\nTraceback (most recent call last):\n"
        f'  File "{_USER}", {frame}\n    {source}\n{raised}\n'
    )
    assert d.signature_document(log)["category"] == "user_code"


def test_chained_exceptions_are_categorized_together():
    log = (
        "Traceback (most recent call last):\n"
        '  File "/opt/c/pg.py", line 8, in connect\n'
        "    conn = psycopg.connect(dsn, connect_timeout=10)\n"
        "psycopg.OperationalError: connection failed: timeout expired\n"
        "\nThe above exception was the direct cause of the following exception:\n\n"
        "Traceback (most recent call last):\n"
        f'  File "{_USER}", line 3, in load\n'
        "    raise RuntimeError('load failed')\n"
        "RuntimeError: load failed\n"
    )
    signature = d.extract_error_signature(log)
    assert signature.raised == (
        "psycopg.OperationalError: connection failed: timeout expired",
        "RuntimeError: load failed",
    )
    assert signature.message == "RuntimeError: load failed"
    assert d.categorize(signature) is d.Category.CONNECTIVITY


def test_an_unrelated_earlier_traceback_is_not_chained():
    log = (
        "Traceback (most recent call last):\n"
        '  File "/opt/c/pg.py", line 8, in connect\n'
        "psycopg.OperationalError: timeout expired\n"
        "INFO retried and connected\n"
        "Traceback (most recent call last):\n"
        f'  File "{_USER}", line 3, in load\n'
        "RuntimeError: load failed\n"
    )
    signature = d.extract_error_signature(log)
    assert signature.raised == ("RuntimeError: load failed",)
    assert d.categorize(signature) is d.Category.USER_CODE


def test_a_clean_log_has_no_signature():
    assert d.extract_error_signature(_log("clean_success")) is None
    assert d.signature_document(_log("clean_success")) is None
    assert d.categorize(None) is d.Category.UNKNOWN


def test_messages_are_capped():
    text = "Traceback (most recent call last):\nValueError: " + "x" * 1000
    assert len(d.extract_error_signature(text).message) == d.MESSAGE_CHARS


def test_excerpt_keeps_an_earlier_traceback():
    text = _log("polars_column_not_found") + "\n".join(f"noise {i}" for i in range(50))
    body, truncated = d.excerpt(text, 5)
    assert truncated
    head = text.splitlines()
    start = next(i for i, row in enumerate(head) if row.startswith("Traceback"))
    assert body.startswith(head[max(start - d.EXCERPT_CONTEXT, 0)])
    assert "Traceback (most recent call last):" in body
    assert body.endswith("noise 49")


def test_excerpt_drops_a_long_preamble_before_the_traceback_context():
    preamble = "\n".join(f"pod env {i} " + "x" * 80 for i in range(80))
    context = "\n".join(f"context {i}" for i in range(d.EXCERPT_CONTEXT))
    text = f"{preamble}\n{context}\n" + _log("pandas_key_error")
    body, truncated = d.excerpt(text, 400)
    assert truncated and len(body) <= d.EXCERPT_CHARS
    assert body.startswith("[... ") and "lines cut]\ncontext " in body
    assert f"context {d.EXCERPT_CONTEXT - 1}" in body
    assert "pod env" not in body
    assert "Traceback (most recent call last):" in body
    assert "KeyError: 'country'" in body


def test_excerpt_of_a_short_log_is_the_log():
    text = _log("pandas_key_error")
    assert d.excerpt(text, 200) == (text, False)
    assert d.excerpt(text, 7) == (text, False)


@pytest.mark.parametrize("lines", [200, 20])
def test_excerpt_keeps_a_repeated_plan_dump_once_and_bounded(lines):
    body, truncated = d.excerpt(_log("polars_plan_repeated"), lines)
    assert truncated
    assert len(body) <= d.EXCERPT_CHARS
    assert body.count("polars.exceptions.ColumnNotFoundError") == 1
    assert body.count("Resolved plan until failure:") == 1
    assert "UserFunctionFailedError: RF_12" in body
    assert 'File "/work/bundle/user_code/salesops.py", line 85' in body


def test_excerpt_collapses_repeated_runs():
    block = ["Resolved plan until failure:", "", "SELECT [a]", "  SCAN [t]"]
    text = "\n".join(["start", *block, "middle", *block, "end"])
    body, truncated = d.excerpt(text, 200)
    assert truncated
    assert body.splitlines() == [
        "start",
        *block,
        "middle",
        "[... 4 lines repeated from above]",
        "end",
    ]


def test_excerpt_of_an_oversized_log_is_bounded():
    text = "\n".join(f"line {i} " + "x" * 80 for i in range(300))
    body, truncated = d.excerpt(text, 300)
    assert truncated
    assert len(body) <= d.EXCERPT_CHARS
    assert body.startswith("line 0 ")
    assert body.endswith("line 299 " + "x" * 80)


def _worker(id: str, run: str, status: str) -> SimpleNamespace:
    return SimpleNamespace(
        id=id,
        kwargs={
            "id": id,
            "logical_function_run_id": run,
            "collection": "sales",
            "function": "clean_orders",
            "status": status,
        },
    )


def _server(workers, logs: dict) -> ts.TabsdataServer:
    server = ts.TabsdataServer.__new__(ts.TabsdataServer)
    server.connection = MagicMock()
    server.list_workers = MagicMock(return_value=list(workers))

    def get_worker_log(worker_id):
        log = logs[worker_id]
        if isinstance(log, Exception):
            raise log
        return log

    server.get_worker_log = get_worker_log
    return server


def test_log_entry_of_a_failed_worker():
    server = _server([], {"W1": _log("pandas_key_error")})
    entry = d.log_entry(server, _worker("W1", "R1", "F"), tail=2, attempt=3)
    assert entry["worker_id"] == "W1"
    assert entry["function_run_id"] == "R1"
    assert entry["status_name"] == "Failed"
    assert entry["health"] == "error"
    assert entry["attempt"] == 3
    assert entry["lines_total"] == 7
    assert entry["log_truncated"] is True
    assert entry["log"].splitlines() == [
        "    indexer = self.columns.get_loc(key)",
        "KeyError: 'country'",
    ]
    assert entry["signature"]["category"] == "schema_mismatch"


def test_log_entry_of_a_done_worker_has_no_signature():
    server = _server([], {"W1": _log("no_traceback_error_line")})
    entry = d.log_entry(server, _worker("W1", "R1", "D"))
    assert entry["health"] == "ok"
    assert entry["signature"] is None
    assert entry["log_truncated"] is False


def test_forbidden_log_is_reported_not_raised():
    server = _server([], {"W1": APIServerError({"error": "forbidden"}, status=403)})
    entry = d.log_entry(server, _worker("W1", "R1", "F"))
    assert entry["log_unavailable"] == d.FORBIDDEN_LOG
    assert entry["log"] is None


def test_other_log_errors_raise():
    server = _server([], {"W1": APIServerError({"error": "boom"}, status=500)})
    with pytest.raises(APIServerError):
        d.log_entry(server, _worker("W1", "R1", "F"))


_WORKERS = [
    _worker("01a", "R1", "E"),
    _worker("01c", "R1", "F"),
    _worker("01b", "R2", "D"),
]
_LOGS = {w.id: f"log of {w.id}" for w in _WORKERS}


def test_scope_logs_lists_newest_first_with_attempts():
    server = _server(_WORKERS, _LOGS)
    entries, truncated = server.scope_logs(plan="P1", tail=10)
    assert [(e["worker_id"], e["attempt"]) for e in entries] == [
        ("01c", 2),
        ("01b", 1),
        ("01a", 1),
    ]
    assert truncated is False
    assert server.list_workers.call_args.kwargs["filter"] == ["execution_id:eq:P1"]


def test_scope_logs_keeps_the_failed_and_caps():
    server = _server(_WORKERS, _LOGS)
    entries, truncated = server.scope_logs(trx="T1", failed_only=True, max_workers=1)
    assert [e["worker_id"] for e in entries] == ["01c"]
    assert truncated is True
    assert server.list_workers.call_args.kwargs["filter"] == ["transaction_id:eq:T1"]


def test_scope_logs_without_a_cap():
    server = _server(_WORKERS, _LOGS)
    entries, truncated = server.scope_logs(fnrun="R1", max_workers=0)
    assert len(entries) == 3 and truncated is False
    assert server.list_workers.call_args.kwargs["filter"] == [
        "logical_function_run_id:eq:R1"
    ]


def test_worker_log_entry_reads_the_worker():
    server = _server([], {"W1": "ok"})
    server.get_worker = MagicMock(return_value=_worker("W1", "R1", "D"))
    entry = server.worker_log_entry("W1", tail=1)
    server.get_worker.assert_called_once_with("W1")
    assert entry["log"] == "ok"


def test_scope_logs_needs_a_scope():
    with pytest.raises(ValueError):
        _server(_WORKERS, _LOGS).scope_logs()


_LINES = "start\npod env\nINFO load\nTraceback (most recent call last):\n  x\nKeyError: 'a'\nINFO done"


def test_select_lines_numbers_the_lines_it_keeps():
    assert d.select_lines(_LINES, tail=2) == [(6, "KeyError: 'a'"), (7, "INFO done")]
    assert d.select_lines(_LINES, grep="INFO") == [(3, "INFO load"), (7, "INFO done")]
    assert d.select_lines(_LINES, from_error=True)[0] == (
        4,
        "Traceback (most recent call last):",
    )
    assert d.select_lines(_LINES, tail=1, grep="INFO", from_error=True) == [
        (7, "INFO done")
    ]
    assert len(d.select_lines("a\nb", from_error=True)) == 2


def test_cut_ranges_are_the_lines_left_out():
    assert d.cut_ranges([3, 7], 8) == [[1, 2], [4, 6], [8, 8]]
    assert d.cut_ranges(range(1, 4), 3) == []
