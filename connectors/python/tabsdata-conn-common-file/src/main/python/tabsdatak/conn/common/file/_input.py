from __future__ import annotations

from pathlib import Path

from tabsdatak.conn.common.file import _transporter


def import_one(
    *,
    source: _transporter.TransporterLocation,
    target_dir: Path,
    format: _transporter.TransporterFormat,
    initial_lastmod: str | None = None,
    lastmod_info: str | None = None,
    file_info: str | None = None,
    env: dict[str, str] | None = None,
    binary_path: str | None = None,
) -> _transporter.TransporterReport:
    request_path, report_path = _transporter.new_request_paths(target_dir)
    target = _transporter.TransporterLocalFile(
        _transporter.path_to_file_uri(target_dir)
    )
    envelope = _transporter.V1ImportFormat(
        source=source,
        format=format,
        target=target,
        initial_lastmod=initial_lastmod,
        lastmod_info=lastmod_info,
        file_info=file_info,
    )
    _transporter.dump_request_yaml(
        envelope, request_path, dumper_cls=_transporter.INPUT_DUMPER
    )
    _transporter.run_binary(
        request_path,
        report_path,
        env=env,
        binary_path=binary_path,
    )
    return _transporter.load_report_yaml(report_path)
