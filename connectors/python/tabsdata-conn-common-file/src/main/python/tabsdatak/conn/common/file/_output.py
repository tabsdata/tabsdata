from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from tabsdatak.common._transporter import _transporter1

# --- Local-source format transcoding -------------------------------
#
# The transporter binary's copy operation is a raw byte-copy (the
# report flags ``copy_type: RawParallel``). When the user asks for a
# non-parquet dest format, we transcode the source parquet locally
# via polars first, then hand the binary a temp file as the source.
# The binary uploads / copies that temp file unchanged.


def _file_uri_to_path(url: str) -> Path:
    parsed = urlparse(url)
    if parsed.scheme != "file":
        raise ValueError(f"expected file:// URL, got {url!r}")
    return Path(unquote(parsed.path))


_CSV_SINK_KEYS: frozenset[str] = frozenset({
    "include_header",
    "separator",
    "line_terminator",
    "quote_char",
    "batch_size",
    "datetime_format",
    "date_format",
    "time_format",
    "float_scientific",
    "float_precision",
    "null_value",
    "quote_style",
})


def _csv_sink_kwargs(options: dict[str, Any]) -> dict[str, Any]:
    """Pick the subset of ``options`` that polars'
    :meth:`LazyFrame.sink_csv` accepts. ``eol_char`` is renamed to
    polars' ``line_terminator``; unknown keys are dropped silently
    so callers can carry the binary-spec defaults without polars
    complaining.
    """
    out: dict[str, Any] = {}
    for k, v in options.items():
        if k == "eol_char":
            out["line_terminator"] = v
        elif k in _CSV_SINK_KEYS:
            out[k] = v
    return out


def _transcode_to_local(
    source: _transporter1.TransporterLocalFile,
    work_dir: Path,
    format: _transporter1.TransporterFormat,
) -> _transporter1.TransporterLocalFile:
    """Read the parquet at ``source`` and write it in ``format`` to a
    temp file under ``work_dir / _transcoded``. Return a new local
    location pointing at the temp file."""
    import polars as pl

    source_path = _file_uri_to_path(source.url)
    out_dir = work_dir / "_transcoded"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{uuid.uuid4().hex}"

    if isinstance(format, _transporter1.TransporterCSVFormat):
        pl.scan_parquet(source_path).sink_csv(
            out_path,
            **_csv_sink_kwargs(format.options),
        )
    elif isinstance(format, _transporter1.TransporterJsonFormat):
        # ND-JSON: matches the src-side JSON read shape.
        pl.scan_parquet(source_path).sink_ndjson(out_path)
    elif isinstance(format, _transporter1.TransporterAvroFormat):
        # polars has ``write_avro`` (eager) but no ``sink_avro``.
        pl.read_parquet(source_path).write_avro(out_path)
    elif isinstance(format, _transporter1.TransporterParquetFormat):
        # Caller should not invoke us for parquet; bail loudly.
        raise AssertionError("parquet format does not need transcoding")
    else:
        raise ValueError(
            f"unsupported transcode format: {type(format).__name__}",
        )

    return _transporter1.TransporterLocalFile(
        _transporter1.path_to_file_uri(out_path),
    )


def copy_one(
    *,
    source_target_pairs: list[
        tuple[_transporter1.TransporterLocation, _transporter1.TransporterLocation]
    ],
    work_dir: Path,
    format: _transporter1.TransporterFormat,
    env: dict[str, str] | None = None,
    binary_path: str | None = None,
) -> _transporter1.TransporterReport:
    # Non-parquet formats: transcode each (local) parquet source via
    # polars before invoking the binary. The binary then byte-copies
    # the transcoded temp file to the user's target.
    if not isinstance(format, _transporter1.TransporterParquetFormat):
        transcoded: list[tuple] = []
        for src, tgt in source_target_pairs:
            if not isinstance(src, _transporter1.TransporterLocalFile):
                raise ValueError(
                    "source for transcoding must be a TransporterLocalFile; "
                    f"got {type(src).__name__}",
                )
            transcoded.append((_transcode_to_local(src, work_dir, format), tgt))
        source_target_pairs = transcoded

    request_path, report_path = _transporter1.new_request_paths(work_dir)
    pairs_yaml = [[s, t] for s, t in source_target_pairs]
    envelope = _transporter1.V1CopyFormat(pairs_yaml, format=format)
    _transporter1.dump_request_yaml(
        envelope, request_path, dumper_cls=_transporter1.OUTPUT_DUMPER
    )
    _transporter1.run_binary(
        request_path,
        report_path,
        env=env,
        binary_path=binary_path,
    )
    return _transporter1.load_report_yaml(report_path)
