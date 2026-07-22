#
# Copyright 2026 Tabsdata Inc.
#

"""``tdk auth add-cert`` copies the PEM into the managed certificate folder; the
source is a path, or ``-`` to read the certificate from stdin."""

from __future__ import annotations

from unittest.mock import patch

from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli

_PEM = "-----BEGIN CERTIFICATE-----\nMIIBfakecert==\n-----END CERTIFICATE-----\n"


def _patched_store(tmp_path, cert):
    # add_cert imports these from tabsdatak.sdk._apiserver lazily (inside the
    # command body), so patch them at that source module.
    return (
        patch(
            "tabsdatak.sdk._apiserver._obtain_certificate_file_path",
            return_value=str(cert),
        ),
        patch(
            "tabsdatak.sdk._apiserver.DEFAULT_TABSDATA_CERTIFICATE_FOLDER",
            str(tmp_path),
        ),
    )


def test_add_cert_pem_dash_reads_from_stdin(tmp_path):
    # ``--pem -`` reads the certificate from stdin instead of copying a file
    # literally named "-".
    cert = tmp_path / "server.pem"
    p1, p2 = _patched_store(tmp_path, cert)
    with p1, p2:
        result = CliRunner().invoke(
            cli,
            ["auth", "add-cert", "-s", "https://host:2457", "--pem", "-"],
            input=_PEM,
        )
    assert result.exit_code == 0, result.output
    assert cert.read_text() == _PEM
    assert "Certificate added successfully" in result.output


def test_add_cert_pem_path_copies_file(tmp_path):
    # A real path still copies the file's contents into the store.
    src = tmp_path / "src.pem"
    src.write_text(_PEM)
    cert = tmp_path / "server.pem"
    p1, p2 = _patched_store(tmp_path, cert)
    with p1, p2:
        result = CliRunner().invoke(
            cli, ["auth", "add-cert", "-s", "https://host:2457", "--pem", str(src)]
        )
    assert result.exit_code == 0, result.output
    assert cert.read_text() == _PEM
