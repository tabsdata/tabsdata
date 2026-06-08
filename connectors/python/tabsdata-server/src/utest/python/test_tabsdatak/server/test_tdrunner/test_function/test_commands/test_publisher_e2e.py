#
# Copyright 2026 Tabs Data Inc.
#

"""End-to-end test of the publisher command chain.

Walks every command in the publisher chain except `prepare-py-env`,
asserting that each command's output composes correctly into the next
command's input. Uses the real `td_transporter` binary for the three
transporter steps and the real `tabsdata-conn-localfile` source plugin
for the read; the only mock is a local HTTP server standing in for the
apiserver endpoint that `report-completion` POSTs to.

The chain executed (matches `donotmerge.py`):

  1.  localize-fn-bundle   — pulls + extracts the bundle.
  2.  prep-localize-state  — emits an empty transporter request
                             (no input / system_input).
  3.  run-transporter      — no-op.
  4.  run-source-plugin    — produces raw parquets + inputs manifest.
  5.  stamp-inputs         — adds $td.* system columns; rewrites manifest.
  6.  run-function         — invokes the user function; writes one parquet
                             (other slot returned None → file: null in
                             the outputs manifest).
  7.  prep-upload-tables   — builds the upload transporter request,
                             skipping the file: null slot.
  8.  run-transporter      — uploads the modified table to its
                             apiserver-allocated file:// location.
  9.  prep-upload-state    — empty pairs (no system_output).
  10. run-transporter      — no-op.
  11. report-completion    — POSTs the response.yaml to a local HTTP
                             server; the response distinguishes the
                             modified table (Data) from the unmodified
                             one (NoData, i.e. no `info` key).

Mirrors `donotmerge.py`. Requires `td_transporter` on PATH and
`tabsdata-conn-localfile` installed.
"""

from __future__ import annotations

import http.server
import importlib
import socket
import sys
import threading
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from tabsdatak.common._archive.bundle import build_bundle

pytestmark = [
    pytest.mark.unit,
    pytest.mark.tabsdatak,
    pytest.mark.skip(reason="Pending revision of all commands"),
]


# --- user code ------------------------------------------------------------


_USER_PUBLISHER_SRC = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrcConn, LocalFileSrc

_SRC = LocalFileSrc(
    paths=["*.parquet"],
)


@publisher(
    conn=LocalFileSrcConn(base_path={base_path!r}),
    source=_SRC, 
    output_tables=["my_table", "my_table2"]
)
def my_publisher(in_1: TableFramesSpec) -> tuple[TableFrameSpec, TableFrameSpec]:
    # ARRAY-mode input → first source file appended with a marker col.
    # Second slot is None (== "not modified this run").
    import polars as pl

    if in_1 and in_1[0] is not None:
        first = in_1[0].with_columns(pl.lit("from_user_fn").alias("marker"))
    else:
        first = None
    return (first, None)
"""


def _write_sample_parquet(path: Path) -> None:
    table = pa.table({
        "id": [1, 2, 3],
        "name": ["alice", "bob", "carol"],
        "score": [0.5, 1.5, 2.5],
    })
    pq.write_table(table, path)


# --- fixtures -------------------------------------------------------------


@pytest.fixture
def user_project(tmp_path, monkeypatch) -> Path:
    """Materialise a user project on disk and make it importable.

    The publisher reads from a sibling `srcdata/` dir, also created here.
    Module-cache cleanup runs on teardown so each test starts fresh.
    """
    srcdata = tmp_path / "srcdata"
    srcdata.mkdir()
    _write_sample_parquet(srcdata / "rows.parquet")

    project = tmp_path / "project"
    pkg = project / "my_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "publisher.py").write_text(
        _USER_PUBLISHER_SRC.format(base_path=str(srcdata))
    )
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    yield project
    for name in list(sys.modules):
        if name == "my_pkg" or name.startswith("my_pkg."):
            del sys.modules[name]


@pytest.fixture
def bundle_path(tmp_path, user_project) -> Path:
    """Build the bundle tarball. Not extracted — `localize-fn-bundle`
    is what extracts it during the test."""
    from my_pkg.publisher import my_publisher  # noqa: E402  (lazy)

    path = tmp_path / "bundle.tar.zst"
    build_bundle(my_publisher, user_project, path)
    return path


# --- multi-input publisher (cardinality 2 in / cardinality 3 out) --------

# A second publisher used by the multi-input e2e test. Source has TWO
# distinct paths (cardinality 2 → 2 input slots → 2 function params)
# and the function declares THREE outputs (cardinality 3 ≠ 2 ≠ 1
# matters: input ≠ output, neither side is the default 1).
#
# Output mix:
#   * `concat_all` — vertical concat of all rows from both slots
#   * `first_only` — just the first row from slot A
#   * `skipped`    — None (exercises file: null in the outputs manifest)
_USER_PUBLISHER_MULTI_SRC = """\
from __future__ import annotations

from tabsdatak.api import TableFrameSpec, TableFramesSpec, publisher
from tabsdatak.conn.localfile import LocalFileSrcConn, LocalFileSrc

_SRC = LocalFileSrc(
    paths=["a.parquet", "b.parquet"],
)


@publisher(
    conn=LocalFileSrcConn(base_path={base_path!r}),
    source=_SRC,
    output_tables=["concat_all", "first_only", "skipped"],
)
def multi_publisher(
    in_a: TableFramesSpec, in_b: TableFramesSpec
) -> tuple[TableFrameSpec, TableFrameSpec, TableFrameSpec]:
    import polars as pl

    a_frames = [f for f in (in_a or []) if f is not None]
    b_frames = [f for f in (in_b or []) if f is not None]
    all_frames = a_frames + b_frames

    concat = (
        pl.concat(all_frames, how="vertical_relaxed") if all_frames else None
    )
    first = a_frames[0] if a_frames else None
    return (concat, first, None)
"""


@pytest.fixture
def multi_user_project(tmp_path, monkeypatch) -> Path:
    """User project with a 2-path publisher reading from two distinct
    parquet files in the source dir."""
    srcdata = tmp_path / "multi_srcdata"
    srcdata.mkdir()
    pl.DataFrame({"id": [1, 2], "name": ["a1", "a2"]}).write_parquet(
        srcdata / "a.parquet"
    )
    pl.DataFrame({"id": [10, 11, 12], "name": ["b1", "b2", "b3"]}).write_parquet(
        srcdata / "b.parquet"
    )

    project = tmp_path / "multi_project"
    pkg = project / "multi_pkg"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "publisher.py").write_text(
        _USER_PUBLISHER_MULTI_SRC.format(base_path=str(srcdata))
    )
    monkeypatch.syspath_prepend(str(project))
    importlib.invalidate_caches()
    yield project
    for name in list(sys.modules):
        if name == "multi_pkg" or name.startswith("multi_pkg."):
            del sys.modules[name]


@pytest.fixture
def multi_bundle_path(tmp_path, multi_user_project) -> Path:
    from multi_pkg.publisher import multi_publisher  # noqa: E402

    path = tmp_path / "multi_bundle.tar.zst"
    build_bundle(multi_publisher, multi_user_project, path)
    return path


@pytest.fixture
def mock_apiserver():
    """Tiny HTTP server in a thread. Yields (base_url, requests_log)."""
    requests_log: list[dict] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a, **kw):  # silence stderr in tests
            pass

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            requests_log.append({
                "path": self.path,
                "headers": dict(self.headers),
                "body": body,
            })
            self.send_response(200)
            self.end_headers()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", requests_log
    finally:
        server.shutdown()
        server.server_close()


# --- request.yaml ---------------------------------------------------------


def _write_request(path: Path, *, bundle_uri: str, uploaded_dir: Path) -> Path:
    """Write a request.yaml that declares both outputs with file://
    destinations under `uploaded_dir`."""
    body = {
        "info": {
            "collection": "smoke",
            "collection_id": 1,
            "execution_id": 1,
            "transaction_id": 1,
            "function_run_id": 1,
            "function_bundle": {"uri": bundle_uri},
        },
        "input": [],
        "output": [
            {
                "name": "my_table",
                "location": {
                    "uri": (uploaded_dir / "my_table.parquet").as_uri(),
                },
            },
            {
                "name": "my_table2",
                "location": {
                    "uri": (uploaded_dir / "my_table2.parquet").as_uri(),
                },
            },
        ],
        "system_input": [],
        "system_output": [],
    }
    path.write_text(yaml.safe_dump(body))
    return path


def _write_multi_request(path: Path, *, bundle_uri: str, uploaded_dir: Path) -> Path:
    """request.yaml for the multi-input publisher — 3 output tables."""
    body = {
        "info": {
            "collection": "smoke",
            "collection_id": 1,
            "execution_id": 1,
            "transaction_id": 1,
            "function_run_id": 1,
            "function_bundle": {"uri": bundle_uri},
        },
        "input": [],
        "output": [
            {
                "name": name,
                "location": {
                    "uri": (uploaded_dir / f"{name}.parquet").as_uri(),
                },
            }
            for name in ("concat_all", "first_only", "skipped")
        ],
        "system_input": [],
        "system_output": [],
    }
    path.write_text(yaml.safe_dump(body))
    return path


# --- command invocation helper -------------------------------------------


def _invoke(monkeypatch, main_fn, argv: list[str]) -> int:
    """Set sys.argv and call a command's main(). Restore argv on exit."""
    monkeypatch.setattr(sys, "argv", argv)
    return main_fn()


# --- end-to-end test ------------------------------------------------------


def test_publisher_full_chain(
    tmp_path, bundle_path, mock_apiserver, monkeypatch, http_ok_url
):
    """Walk every step of the publisher chain (except prepare-py-env)
    and verify the artifacts each command produces."""
    base_url, requests_log = mock_apiserver

    # Workspace layout.
    extracted = tmp_path / "extracted"
    in_dir = tmp_path / "in"
    state_dir = tmp_path / "state"
    out_dir = tmp_path / "out"
    enriched_dir = tmp_path / "enriched"
    fn_out_dir = tmp_path / "fn_out"
    uploaded_dir = tmp_path / "uploaded"
    in_dir.mkdir()
    state_dir.mkdir()
    out_dir.mkdir()
    uploaded_dir.mkdir()

    inputs_manifest = tmp_path / "inputs_manifest.yaml"
    enriched_inputs_manifest = tmp_path / "enriched_inputs_manifest.yaml"
    outputs_manifest = tmp_path / "outputs_manifest.yaml"
    localize_state_req = tmp_path / "localize_state_req.yaml"
    upload_tables_req = tmp_path / "upload_tables_req.yaml"
    upload_state_req = tmp_path / "upload_state_req.yaml"
    response_yaml = tmp_path / "response.yaml"

    request_yaml = _write_request(
        tmp_path / "request.yaml",
        bundle_uri=bundle_path.as_uri(),
        uploaded_dir=uploaded_dir,
    )

    # --- 1. localize-fn-bundle ------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.localize_fn_bundle import (
        main as localize_main,
    )

    rc = _invoke(
        monkeypatch,
        localize_main,
        [
            "localize-fn-bundle",
            "--request",
            str(request_yaml),
            "--bundle-dir",
            str(extracted),
            "--scratch-dir",
            str(tmp_path / "localize-scratch"),
            "--liveness",
            str(tmp_path / "liveness-localize"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0
    assert (extracted / "function_def.json").is_file()
    assert (extracted / "user_code" / "my_pkg" / "publisher.py").is_file()

    # --- 2. prep-localize-state ----------------------------------------
    from tabsdatak.server._tdrunner.function.commands.prep_localize_state import (
        main as prep_loc_main,
    )

    prep_loc_inputs_manifest = tmp_path / "prep_loc_inputs.yaml"
    prep_loc_state_manifest = tmp_path / "prep_loc_state.yaml"
    rc = _invoke(
        monkeypatch,
        prep_loc_main,
        [
            "prep-localize-state",
            "--request",
            str(request_yaml),
            "--in-dir",
            str(in_dir),
            "--state-dir",
            str(state_dir),
            "--out",
            str(localize_state_req),
            "--inputs-manifest",
            str(prep_loc_inputs_manifest),
            "--state-manifest",
            str(prep_loc_state_manifest),
            "--liveness",
            str(tmp_path / "liveness-prep-loc-state"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0
    # Empty pairs — no inputs and no system_input declared.
    assert "source_target_pairs: []" in localize_state_req.read_text()
    # Both manifests are empty (publishers have no request.input or
    # request.system_input).
    assert yaml.safe_load(prep_loc_inputs_manifest.read_text()) == {"inputs": []}
    assert yaml.safe_load(prep_loc_state_manifest.read_text()) == {"state": []}

    # --- 3. run-transporter (localize-state, empty) --------------------
    from tabsdatak.server._tdrunner.function.commands.run_transporter import (
        main as run_transporter_main,
    )

    rc = _invoke(
        monkeypatch,
        run_transporter_main,
        [
            "run-transporter",
            "--request",
            str(localize_state_req),
            "--report",
            str(tmp_path / "transporter-report-loc.yaml"),
            "--liveness",
            str(tmp_path / "liveness-rt-loc"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # --- 4. run-source-plugin ------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.run_source_plugin import (
        main as rsp_main,
    )

    rc = _invoke(
        monkeypatch,
        rsp_main,
        [
            "run-source-plugin",
            "--request",
            str(request_yaml),
            "--bundle-dir",
            str(extracted),
            "--out-dir",
            str(out_dir),
            "--inputs-manifest",
            str(inputs_manifest),
            "--state-dir",
            str(state_dir),
            "--liveness",
            str(tmp_path / "liveness-rsp"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0
    raw_parquets = list(out_dir.rglob("*.parquet"))
    assert len(raw_parquets) == 1, raw_parquets
    inputs = yaml.safe_load(inputs_manifest.read_text())
    assert [e["name"] for e in inputs["inputs"]] == ["in_1"]
    assert len(inputs["inputs"][0]["files"]) == 1

    # --- 5. stamp-inputs -----------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.stamp_inputs import (
        main as stamp_main,
    )

    rc = _invoke(
        monkeypatch,
        stamp_main,
        [
            "stamp-inputs",
            "--request",
            str(request_yaml),
            "--inputs-manifest",
            str(inputs_manifest),
            "--enriched-dir",
            str(enriched_dir),
            "--enriched-inputs-manifest",
            str(enriched_inputs_manifest),
            "--liveness",
            str(tmp_path / "liveness-stamp"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0
    enriched_parquets = list(enriched_dir.rglob("*.parquet"))
    assert len(enriched_parquets) == 1
    # System columns landed.
    enriched_schema = pq.read_schema(enriched_parquets[0])
    enriched_cols = enriched_schema.names
    for sys_col in (
        "$td.id",
        "$td.ver.execution",
        "$td.ver.transaction",
        "$td.ver.version",
        "$td.ver.timestamp",
    ):
        assert sys_col in enriched_cols, (sys_col, enriched_cols)

    # --- 6. run-function -----------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.run_function import (
        main as rf_main,
    )

    rc = _invoke(
        monkeypatch,
        rf_main,
        [
            "run-function",
            "--request",
            str(request_yaml),
            "--bundle-dir",
            str(extracted),
            "--inputs-manifest",
            str(enriched_inputs_manifest),
            "--out-dir",
            str(fn_out_dir),
            "--outputs-manifest",
            str(outputs_manifest),
            "--state-dir",
            str(state_dir),
            "--liveness",
            str(tmp_path / "liveness-fn"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # The user function returned (df, None); only the first slot has a
    # parquet on disk, and the outputs manifest reflects that.
    fn_parquets = list(fn_out_dir.rglob("*.parquet"))
    assert len(fn_parquets) == 1
    assert fn_parquets[0].name == "my_table.parquet"

    outputs = yaml.safe_load(outputs_manifest.read_text())
    by_name = {e["name"]: e for e in outputs["outputs"]}
    assert set(by_name) == {"my_table", "my_table2"}
    assert by_name["my_table"]["file"] is not None
    assert Path(by_name["my_table"]["file"]).samefile(fn_parquets[0])
    assert by_name["my_table2"]["file"] is None

    # User-added column round-tripped.
    df = pq.read_table(fn_parquets[0]).to_pydict()
    assert "marker" in df
    assert all(v == "from_user_fn" for v in df["marker"])

    # --- 7. prep-upload-tables -----------------------------------------
    from tabsdatak.server._tdrunner.function.commands.prep_upload_tables import (
        main as prep_up_tables_main,
    )

    rc = _invoke(
        monkeypatch,
        prep_up_tables_main,
        [
            "prep-upload-tables",
            "--request",
            str(request_yaml),
            "--outputs-manifest",
            str(outputs_manifest),
            "--out",
            str(upload_tables_req),
            "--liveness",
            str(tmp_path / "liveness-prep-up-tbl"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0
    upload_text = upload_tables_req.read_text()
    # Exactly one CopyPair — my_table — because my_table2's file is null.
    assert upload_text.count("- - !LocalFile") == 1
    assert "my_table.parquet" in upload_text
    # my_table2 must NOT have a pair (file: null in manifest).
    assert "my_table2.parquet" not in upload_text

    # --- 8. run-transporter (upload-tables) ----------------------------
    rc = _invoke(
        monkeypatch,
        run_transporter_main,
        [
            "run-transporter",
            "--request",
            str(upload_tables_req),
            "--report",
            str(tmp_path / "transporter-report-up-tbl.yaml"),
            "--liveness",
            str(tmp_path / "liveness-rt-up-tbl"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0
    uploaded = uploaded_dir / "my_table.parquet"
    assert uploaded.is_file()
    # my_table2 must NOT have been uploaded.
    assert not (uploaded_dir / "my_table2.parquet").exists()
    # The uploaded parquet is byte-identical to what run-function emitted.
    assert uploaded.read_bytes() == fn_parquets[0].read_bytes()

    # --- 9. prep-upload-state ------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.prep_upload_state import (
        main as prep_up_state_main,
    )

    rc = _invoke(
        monkeypatch,
        prep_up_state_main,
        [
            "prep-upload-state",
            "--request",
            str(request_yaml),
            "--state-dir",
            str(state_dir),
            "--out",
            str(upload_state_req),
            "--liveness",
            str(tmp_path / "liveness-prep-up-state"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0
    assert "source_target_pairs: []" in upload_state_req.read_text()

    # --- 10. run-transporter (upload-state, empty) ---------------------
    rc = _invoke(
        monkeypatch,
        run_transporter_main,
        [
            "run-transporter",
            "--request",
            str(upload_state_req),
            "--report",
            str(tmp_path / "transporter-report-up-state.yaml"),
            "--liveness",
            str(tmp_path / "liveness-rt-up-state"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # --- 11. report-completion -----------------------------------------
    from tabsdatak.server._tdrunner.function.commands.report_completion import (
        main as report_main,
    )

    rc = _invoke(
        monkeypatch,
        report_main,
        [
            "report-completion",
            "--url",
            f"val:{base_url}/api/v1/done",
            "--token",
            "val:mock-bearer-token",
            "--request",
            str(request_yaml),
            "--outputs-manifest",
            str(outputs_manifest),
            "--state-dir",
            str(state_dir),
            "--out",
            str(response_yaml),
            "--liveness",
            str(tmp_path / "liveness-rc"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # response.yaml encodes Data vs. NoData via presence/absence of `info`.
    response = yaml.safe_load(response_yaml.read_text())
    entries = {e["table"]: e for e in response["output"]}
    assert set(entries) == {"my_table", "my_table2"}
    assert "info" in entries["my_table"]
    assert entries["my_table"]["info"]["row_count"] == 3
    assert entries["my_table"]["info"]["schema_hash"].startswith("sha256:")
    # NoData entry: just the `table` key, no `info`.
    assert "info" not in entries["my_table2"]
    assert set(entries["my_table2"]) == {"table"}

    # The apiserver received exactly one POST with the local response body.
    assert len(requests_log) == 1
    posted = requests_log[0]
    assert posted["path"] == "/api/v1/done"
    assert posted["headers"]["Authorization"] == "Bearer mock-bearer-token"
    assert posted["headers"]["Content-Type"] == "application/x-yaml"
    assert posted["body"] == response_yaml.read_bytes()


def test_publisher_full_chain_multi_input_path(
    tmp_path, multi_bundle_path, mock_apiserver, monkeypatch, http_ok_url
):
    """End-to-end with a publisher whose source has 2 paths
    (cardinality 2 → 2 input slots / 2 function parameters) and
    3 output tables (cardinality 3 ≠ 2). Exercises:

      * `_input_count` must come from the function signature
        (regression coverage for the publisher-hardcoded-1 bug).
      * cardinality on input ≠ cardinality on output.
      * mixed output shapes (full concat, partial, None) across the
        upload + report-completion path.
    """
    base_url, requests_log = mock_apiserver

    extracted = tmp_path / "extracted"
    in_dir = tmp_path / "in"
    state_dir = tmp_path / "state"
    out_dir = tmp_path / "out"
    enriched_dir = tmp_path / "enriched"
    fn_out_dir = tmp_path / "fn_out"
    uploaded_dir = tmp_path / "uploaded"
    in_dir.mkdir()
    state_dir.mkdir()
    out_dir.mkdir()
    uploaded_dir.mkdir()

    inputs_manifest = tmp_path / "inputs_manifest.yaml"
    enriched_inputs_manifest = tmp_path / "enriched_inputs_manifest.yaml"
    outputs_manifest = tmp_path / "outputs_manifest.yaml"
    localize_state_req = tmp_path / "localize_state_req.yaml"
    upload_tables_req = tmp_path / "upload_tables_req.yaml"
    upload_state_req = tmp_path / "upload_state_req.yaml"
    response_yaml = tmp_path / "response.yaml"

    request_yaml = _write_multi_request(
        tmp_path / "request.yaml",
        bundle_uri=multi_bundle_path.as_uri(),
        uploaded_dir=uploaded_dir,
    )

    # --- 1. localize-fn-bundle ------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.localize_fn_bundle import (
        main as localize_main,
    )

    rc = _invoke(
        monkeypatch,
        localize_main,
        [
            "localize-fn-bundle",
            "--request",
            str(request_yaml),
            "--bundle-dir",
            str(extracted),
            "--scratch-dir",
            str(tmp_path / "localize-scratch"),
            "--liveness",
            str(tmp_path / "liveness-localize"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # --- 2. prep-localize-state ----------------------------------------
    from tabsdatak.server._tdrunner.function.commands.prep_localize_state import (
        main as prep_loc_main,
    )

    rc = _invoke(
        monkeypatch,
        prep_loc_main,
        [
            "prep-localize-state",
            "--request",
            str(request_yaml),
            "--in-dir",
            str(in_dir),
            "--state-dir",
            str(state_dir),
            "--out",
            str(localize_state_req),
            "--inputs-manifest",
            str(tmp_path / "prep_loc_inputs.yaml"),
            "--state-manifest",
            str(tmp_path / "prep_loc_state.yaml"),
            "--liveness",
            str(tmp_path / "liveness-prep-loc-state"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # --- 3. run-transporter (localize, empty) --------------------------
    from tabsdatak.server._tdrunner.function.commands.run_transporter import (
        main as run_transporter_main,
    )

    rc = _invoke(
        monkeypatch,
        run_transporter_main,
        [
            "run-transporter",
            "--request",
            str(localize_state_req),
            "--report",
            str(tmp_path / "transporter-report-loc.yaml"),
            "--liveness",
            str(tmp_path / "liveness-rt-loc"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # --- 4. run-source-plugin ------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.run_source_plugin import (
        main as rsp_main,
    )

    rc = _invoke(
        monkeypatch,
        rsp_main,
        [
            "run-source-plugin",
            "--request",
            str(request_yaml),
            "--bundle-dir",
            str(extracted),
            "--out-dir",
            str(out_dir),
            "--inputs-manifest",
            str(inputs_manifest),
            "--state-dir",
            str(state_dir),
            "--liveness",
            str(tmp_path / "liveness-rsp"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0
    # Two slots in the inputs manifest (one per source path).
    inputs = yaml.safe_load(inputs_manifest.read_text())
    assert len(inputs["inputs"]) == 2

    # --- 5. stamp-inputs -----------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.stamp_inputs import (
        main as stamp_main,
    )

    rc = _invoke(
        monkeypatch,
        stamp_main,
        [
            "stamp-inputs",
            "--request",
            str(request_yaml),
            "--inputs-manifest",
            str(inputs_manifest),
            "--enriched-dir",
            str(enriched_dir),
            "--enriched-inputs-manifest",
            str(enriched_inputs_manifest),
            "--liveness",
            str(tmp_path / "liveness-stamp"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # --- 6. run-function -----------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.run_function import (
        main as rf_main,
    )

    rc = _invoke(
        monkeypatch,
        rf_main,
        [
            "run-function",
            "--request",
            str(request_yaml),
            "--bundle-dir",
            str(extracted),
            "--inputs-manifest",
            str(enriched_inputs_manifest),
            "--out-dir",
            str(fn_out_dir),
            "--outputs-manifest",
            str(outputs_manifest),
            "--state-dir",
            str(state_dir),
            "--liveness",
            str(tmp_path / "liveness-fn"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # Two of three output slots produced parquet, one returned None.
    fn_parquets = sorted(fn_out_dir.rglob("*.parquet"))
    assert [p.name for p in fn_parquets] == [
        "concat_all.parquet",
        "first_only.parquet",
    ]
    outputs = yaml.safe_load(outputs_manifest.read_text())
    by_name = {e["name"]: e for e in outputs["outputs"]}
    assert set(by_name) == {"concat_all", "first_only", "skipped"}
    assert by_name["concat_all"]["file"] is not None
    assert by_name["first_only"]["file"] is not None
    assert by_name["skipped"]["file"] is None

    # Content checks. concat_all should have 2 + 3 = 5 rows (slot a +
    # slot b); first_only should have only slot a's first row.
    concat_df = pl.read_parquet(by_name["concat_all"]["file"])
    assert concat_df.height == 5
    assert sorted(concat_df["id"].to_list()) == [1, 2, 10, 11, 12]

    first_df = pl.read_parquet(by_name["first_only"]["file"])
    assert first_df.height == 2  # slot a is a single file with 2 rows
    assert first_df["id"].to_list() == [1, 2]

    # --- 7. prep-upload-tables -----------------------------------------
    from tabsdatak.server._tdrunner.function.commands.prep_upload_tables import (
        main as prep_up_tables_main,
    )

    rc = _invoke(
        monkeypatch,
        prep_up_tables_main,
        [
            "prep-upload-tables",
            "--request",
            str(request_yaml),
            "--outputs-manifest",
            str(outputs_manifest),
            "--out",
            str(upload_tables_req),
            "--liveness",
            str(tmp_path / "liveness-prep-up-tbl"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0
    upload_text = upload_tables_req.read_text()
    # Two CopyPairs (concat_all + first_only). `skipped` is file:null
    # in the manifest → omitted.
    assert upload_text.count("- - !LocalFile") == 2
    assert "concat_all.parquet" in upload_text
    assert "first_only.parquet" in upload_text
    assert "skipped.parquet" not in upload_text

    # --- 8. run-transporter (upload-tables) ----------------------------
    rc = _invoke(
        monkeypatch,
        run_transporter_main,
        [
            "run-transporter",
            "--request",
            str(upload_tables_req),
            "--report",
            str(tmp_path / "transporter-report-up-tbl.yaml"),
            "--liveness",
            str(tmp_path / "liveness-rt-up-tbl"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0
    assert (uploaded_dir / "concat_all.parquet").is_file()
    assert (uploaded_dir / "first_only.parquet").is_file()
    assert not (uploaded_dir / "skipped.parquet").exists()

    # --- 9. prep-upload-state ------------------------------------------
    from tabsdatak.server._tdrunner.function.commands.prep_upload_state import (
        main as prep_up_state_main,
    )

    rc = _invoke(
        monkeypatch,
        prep_up_state_main,
        [
            "prep-upload-state",
            "--request",
            str(request_yaml),
            "--state-dir",
            str(state_dir),
            "--out",
            str(upload_state_req),
            "--liveness",
            str(tmp_path / "liveness-prep-up-state"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # --- 10. run-transporter (upload-state, empty) ---------------------
    rc = _invoke(
        monkeypatch,
        run_transporter_main,
        [
            "run-transporter",
            "--request",
            str(upload_state_req),
            "--report",
            str(tmp_path / "transporter-report-up-state.yaml"),
            "--liveness",
            str(tmp_path / "liveness-rt-up-state"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    # --- 11. report-completion -----------------------------------------
    from tabsdatak.server._tdrunner.function.commands.report_completion import (
        main as report_main,
    )

    rc = _invoke(
        monkeypatch,
        report_main,
        [
            "report-completion",
            "--url",
            f"val:{base_url}/api/v1/done",
            "--token",
            "val:mock-bearer-token",
            "--request",
            str(request_yaml),
            "--outputs-manifest",
            str(outputs_manifest),
            "--state-dir",
            str(state_dir),
            "--out",
            str(response_yaml),
            "--liveness",
            str(tmp_path / "liveness-rc"),
            "--responder-url",
            http_ok_url,
            "--responder-method",
            "POST",
            "--responder-authorization",
            "Bearer tok",
        ],
    )
    assert rc == 0

    response = yaml.safe_load(response_yaml.read_text())
    entries = {e["table"]: e for e in response["output"]}
    assert set(entries) == {"concat_all", "first_only", "skipped"}
    assert entries["concat_all"]["info"]["row_count"] == 5
    assert entries["first_only"]["info"]["row_count"] == 2
    # NoData entry: just the `table` key.
    assert "info" not in entries["skipped"]
    assert set(entries["skipped"]) == {"table"}

    assert len(requests_log) == 1
