#
# Copyright 2026 Tabs Data Inc.
#

"""report-completion command.

Step 8 (final) of the publisher init-container chain. Builds the
response.yaml from the outputs manifest produced by `run-function`
(for `request.output`) and the `--state-dir` parquets (for
`request.system_output`), writes a copy to `--out`, and POSTs the
body to `--url` with `Authorization: Bearer <token>`.

For each declared `request.output` table the response includes a
`!Data` entry with column_count / row_count / schema_hash if the
matching outputs-manifest entry carries a non-null `file:`, or a
`!NoData` entry (table name only) when the entry's `file:` is `null`
(the user function returned `None` for that slot).

For each declared `request.system_output` table the response includes
a `!Data` entry if `<state_dir>/<name>.parquet` exists, or `!NoData`
otherwise.

A `request.output` table that has no entry in the outputs manifest is
treated as a config mismatch (the function definition declared fewer
outputs than the request expects) and surfaces `ManifestMismatchError`.
"""

from __future__ import annotations

import argparse
import hashlib
import urllib.error
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq

from tabsdatak.server._tdrunner.function._lib.cli import (
    build_parser,
    command,
    resolve_value,
)
from tabsdatak.server._tdrunner.function._lib.outputs_manifest import (
    MANIFEST_FILE_KEY,
    MANIFEST_NAME_KEY,
    load_outputs_manifest_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.request import (
    Request,
    load_request_or_raise,
)
from tabsdatak.server._tdrunner.function._lib.response import (
    Response,
    TableEntry,
    TableInfo,
    write_response,
)
from tabsdatak.server._tdrunner.function.commands.report_completion.error import (
    ReportCompletionErrorCode,
)

VERSION = "0.1.0"
INFO = "report-completion — submit the function-run response to the API server"
MAN = (
    "report-completion --url <URL> --token <TOKEN> --request <PATH> "
    "--outputs-manifest <PATH> --state-dir <PATH> --out <PATH> "
    "--liveness <PATH>\n\n"
    "Builds response.yaml from the outputs manifest (for request.output) "
    "and the --state-dir parquets (for request.system_output), writes a "
    "copy to --out, and POSTs the body to --url."
)


def _build_parser():
    parser = build_parser(
        prog="report-completion",
        description="Build and submit the function-run response.",
        version=VERSION,
        info=INFO,
        man=MAN,
    )
    parser.add_argument(
        "--url", required=True, help="API server URL or env:VAR / val:VALUE"
    )
    parser.add_argument(
        "--token", required=True, help="Bearer token or env:VAR / val:VALUE"
    )
    parser.add_argument("--request", required=True, help="Path to request.yaml")
    parser.add_argument(
        "--outputs-manifest",
        required=True,
        dest="outputs_manifest",
        help="Path to the outputs manifest YAML produced by run-function",
    )
    parser.add_argument(
        "--state-dir",
        required=True,
        dest="state_dir",
        help="Dir of produced state tables (parquet)",
    )
    parser.add_argument("--out", required=True, help="Path to write response.yaml")
    return parser


def _parquet_info(path: Path) -> TableInfo:
    md = pq.read_metadata(path)
    schema = pq.read_schema(path)
    schema_bytes = schema.serialize().to_pybytes()
    return TableInfo(
        column_count=len(schema),
        row_count=int(md.num_rows),
        schema_hash="sha256:" + hashlib.sha256(schema_bytes).hexdigest(),
    )


def _entry_from_state_dir(name: str, state_dir: Path) -> TableEntry:
    parquet = state_dir / f"{name}.parquet"
    if not parquet.exists():
        return TableEntry(table=name, info=None)
    return TableEntry(table=name, info=_parquet_info(parquet))


def _entry_from_manifest(name: str, file: str | None) -> TableEntry:
    if file is None:
        return TableEntry(table=name, info=None)
    return TableEntry(table=name, info=_parquet_info(Path(file)))


def _build_response_payload(
    request: Request,
    manifest_entries: list[dict],
    state_dir: Path,
) -> Response:
    """Build the response payload.

    `request.output` tables are resolved against the outputs manifest
    (manifest is authoritative for absolute file paths and for the
    "user returned None" sentinel via `file: null`). `request.system_output`
    tables are resolved against `state_dir/<name>.parquet`.

    Raises:
        ManifestMismatchError: a `request.output` table has no
            matching entry in the manifest.
    """
    files_by_name: dict[str, str | None] = {
        e[MANIFEST_NAME_KEY]: e.get(MANIFEST_FILE_KEY) for e in manifest_entries
    }
    entries: list[TableEntry] = []
    for table in request.output:
        if table.name not in files_by_name:
            raise ReportCompletionErrorCode.RC_3.exception(table=table.name)
        entries.append(_entry_from_manifest(table.name, files_by_name[table.name]))
    for table in request.system_output:
        entries.append(_entry_from_state_dir(table.name, state_dir))
    return Response(output=tuple(entries))


def _post(url: str, token: str, body: bytes) -> None:
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/x-yaml",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        raise ReportCompletionErrorCode.RC_5.exception(
            cause=exc, url=url, status=exc.code, detail=exc.reason
        )
    except urllib.error.URLError as exc:
        raise ReportCompletionErrorCode.RC_6.exception(
            cause=exc, url=url, detail=str(exc.reason)
        )


@command(name="td_report_completion", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    url = resolve_value(args.url)
    token = resolve_value(args.token)
    request_path = Path(args.request)
    outputs_manifest_path = Path(args.outputs_manifest)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)

    request = load_request_or_raise(request_path, ReportCompletionErrorCode.RC_1)
    manifest_entries = load_outputs_manifest_or_raise(
        outputs_manifest_path, ReportCompletionErrorCode.RC_2
    )

    response = _build_response_payload(request, manifest_entries, state_dir)

    # Write the response to --out FIRST so it's persisted even when
    # the subsequent POST fails.
    try:
        write_response(response, out_path)
    except OSError as exc:
        raise ReportCompletionErrorCode.RC_4.exception(
            cause=exc, path=out_path, detail=str(exc)
        )

    body = out_path.read_bytes()
    _post(url, token, body)
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
