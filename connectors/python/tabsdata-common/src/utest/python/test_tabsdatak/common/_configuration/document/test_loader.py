#
# Copyright 2026 Tabsdata Inc.
#

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tabsdatak.common._configuration.document.error import DocumentException
from tabsdatak.common._configuration.document.loader import DocumentModel, load

pytestmark = [pytest.mark.unit, pytest.mark.tabsdatak]


class Server(DocumentModel):
    host: str
    port: int = 0


class Settings(DocumentModel):
    server: Server
    servers: list[Server] = []


def written(folder: Path, document: dict) -> Path:
    path = folder / "settings.yaml"
    path.write_text(yaml.safe_dump(document))
    return path


def test_load_validates_the_document(tmp_path: Path):
    path = written(tmp_path, {"server": {"host": "localhost", "port": 2457}})
    assert load(path, Settings).server == Server(host="localhost", port=2457)


def test_lenient_load_keeps_undeclared_fields(tmp_path: Path):
    path = written(tmp_path, {"server": {"host": "localhost", "colour": "blue"}})
    assert load(path, Settings).server.model_extra == {"colour": "blue"}


def test_strict_load_names_every_undeclared_field(tmp_path: Path):
    path = written(
        tmp_path,
        {
            "server": {"host": "localhost", "colour": "blue"},
            "servers": [{"host": "replica", "zone": "west"}],
            "owner": "team",
        },
    )
    with pytest.raises(DocumentException) as raised:
        load(path, Settings, strict=True)
    message = str(raised.value)
    assert "owner" in message
    assert "server.colour" in message
    assert "servers.0.zone" in message


def test_load_resolves_secrets(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SECRETS__SERVER__HOST", "secret.host")
    path = written(tmp_path, {"server": {"host": "${{secret}}"}})
    assert load(path, Settings).server.host == "secret.host"


def test_load_with_an_unset_secret_fails(tmp_path: Path):
    path = written(tmp_path, {"server": {"host": "${{secret}}"}})
    with pytest.raises(DocumentException, match="SECRETS__SERVER__HOST"):
        load(path, Settings)


def test_load_can_leave_secrets_unresolved(tmp_path: Path):
    path = written(tmp_path, {"server": {"host": "${{secret}}"}})
    assert load(path, Settings, resolve_secrets=False).server.host == "${{secret}}"


def test_load_of_an_invalid_document_fails(tmp_path: Path):
    path = written(tmp_path, {"server": {"port": "not a number"}})
    with pytest.raises(DocumentException, match="does not match its model"):
        load(path, Settings)


def test_load_of_a_missing_document_fails(tmp_path: Path):
    with pytest.raises(DocumentException, match="Cannot read"):
        load(tmp_path / "missing.yaml", Settings)


def test_failed_validation_never_shows_resolved_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("SECRETS__SERVER__PORT", "very-secret-value")
    path = written(tmp_path, {"server": {"host": "localhost", "port": "${{secret}}"}})
    with pytest.raises(DocumentException) as raised:
        load(path, Settings)
    assert "very-secret-value" not in str(raised.value)
