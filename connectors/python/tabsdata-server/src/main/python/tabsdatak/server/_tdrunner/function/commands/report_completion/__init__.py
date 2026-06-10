#
# Copyright 2026 Tabs Data Inc.
#

"""report-completion command.

Step 8 (final) of the publisher init-container chain. Builds the
response.yaml from the outputs manifest produced by `run-function`
(for `request.output`) and the `--state-dir` parquets (for
`request.system_output`), writes a copy to `--out`, and POSTs the
body to `--url` with `--authorization` as the ``Authorization``
header value (the value is used verbatim, so callers must include any
scheme prefix such as ``Bearer ``). The POST sends the response
content as ``application/json`` — the on-disk artifact stays YAML
for human readability and audit.

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
import logging
import urllib.error
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq

from tabsdatak.server._tdrunner.function._lib.command import (
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

logger = logging.getLogger(__name__)

VERSION = "0.1.0"
INFO = "report-completion — submit the function-run response to the API server"
MAN = (
    "report-completion --url <URL> --authorization <HEADER> --request <PATH> "
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
        "--authorization",
        required=True,
        help=(
            "Authorization header value (used verbatim, including any "
            "scheme prefix). Accepts env:VAR / val:VALUE."
        ),
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
        # On-disk size of the parquet file; matches the apiserver-side
        # `TableInfo.data_bytes` field which aggregates these per-run.
        data_bytes=path.stat().st_size,
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


def _post(url: str, authorization: str, body: bytes) -> None:
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": authorization,
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        # Surface the apiserver's response body in the detail so serde
        # / validation errors diagnose themselves instead of bottoming
        # out at a bare "Bad Request".
        body = exc.read().decode("utf-8", errors="replace")
        detail = f"{exc.reason}: {body}" if body else exc.reason
        raise ReportCompletionErrorCode.RC_5.exception(
            cause=exc, url=url, status=exc.code, detail=detail
        )
    except urllib.error.URLError as exc:
        raise ReportCompletionErrorCode.RC_6.exception(
            cause=exc, url=url, detail=str(exc.reason)
        )


@command(name="td_report_completion", parser=_build_parser)
def main(args: argparse.Namespace) -> int:
    url = resolve_value(args.url)
    authorization = resolve_value(args.authorization)
    request_path = Path(args.request)
    outputs_manifest_path = Path(args.outputs_manifest)
    state_dir = Path(args.state_dir)
    out_path = Path(args.out)

    logger.info("Building and submitting function-run response")
    request = load_request_or_raise(request_path, ReportCompletionErrorCode.RC_1)
    manifest_entries = load_outputs_manifest_or_raise(
        outputs_manifest_path, ReportCompletionErrorCode.RC_2
    )
    logger.debug(
        "Outputs manifest has %d entr%s: %s; request declares %d output "
        "table(s): %s + %d system_output table(s): %s",
        len(manifest_entries),
        "y" if len(manifest_entries) == 1 else "ies",
        manifest_entries,
        len(request.output),
        request.output,
        len(request.system_output),
        request.system_output,
    )

    response = _build_response_payload(request, manifest_entries, state_dir)
    data_count = sum(1 for e in response.output if e.info is not None)
    nodata_count = len(response.output) - data_count
    logger.info(
        "Built response: %d table(s) with data, %d NoData",
        data_count,
        nodata_count,
    )

    # Write the response to --out FIRST so it's persisted even when
    # the subsequent POST fails.
    try:
        write_response(response, out_path)
    except OSError as exc:
        raise ReportCompletionErrorCode.RC_4.exception(
            cause=exc, path=out_path, detail=str(exc)
        )
    logger.debug("Wrote response.yaml to %s", out_path)

    # TODO follow-up: the apiserver expects a `CallbackRequest`
    # (`ResponseMessagePayload<WorkerOutput>`) envelope with
    # externally-tagged `Data`/`NoData` entries; the current Python
    # `Response` model serialises to a flat `{output:[{table,info?}]}`
    # shape. Switching content-type to JSON unblocks the wire-format
    # bind but the body still produces an apiserver 400. The body
    # wrapping/transformation lives in the next ticket.
    body = response.model_dump_json().encode("utf-8")
    logger.info("POSTing response to apiserver at %s", url)
    _post(url, authorization, body)
    logger.info("Reported completion to apiserver successfully")
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    sys.exit(main())
