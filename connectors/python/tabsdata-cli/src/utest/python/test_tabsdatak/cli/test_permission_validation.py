#
#  Copyright 2026 Tabs Data Inc.
#

"""Client-side validation for ``tdk permission grant`` / ``revoke``.

The happy paths (SDK call + success message) are pinned by the mutating harness.
This module pins the *rejection* rules enforced entirely client-side by
``permission_group._validate_permission`` -- rules that never reach the server:

* an unknown permission is rejected,
* a project-scoped permission requires ``--proj``,
* a system-scoped permission forbids ``--proj``, and
* the org-wide view/develop wildcards are RBAC-Mode-View-only and rejected here.

On every rejection the command must exit non-zero AND never call the SDK. Both
``grant`` and ``revoke`` share the validator, so each case runs against both.
Same mock-client seam as the render / mutating harnesses.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from tabsdatak.cli.td.cli import cli
from tabsdatak.sdk.tabsdata_server import TabsdataServer


def _client() -> MagicMock:
    client = MagicMock(spec=TabsdataServer)
    client.connection = MagicMock()
    return client


def _invoke(argv: list[str], *, client: MagicMock, tmp_path):
    def _inject(ctx):
        ctx.obj["tabsdataserver"] = client

    with patch(
        "tabsdatak.cli.td.cli.initialise_tabsdata_server_connection", _inject
    ), patch("tabsdatak.cli.td.cli.DEFAULT_TABSDATA_DIRECTORY", str(tmp_path)):
        return CliRunner().invoke(cli, argv)


# (verb, sdk method) -- the validator is shared, so every rule is checked on both.
_VERBS = [("grant", "grant_permission"), ("revoke", "revoke_permission")]
_VERB_IDS = [v for v, _ in _VERBS]


# --- rejections: exit non-zero AND the server is never touched ---------------


@pytest.mark.parametrize("verb,method", _VERBS, ids=_VERB_IDS)
def test_unknown_permission_is_rejected(verb, method, tmp_path):
    client = _client()
    result = _invoke(
        ["permission", verb, "-u", "alice", "--permission", "not_a_real_perm"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code != 0
    assert "invalid permission" in result.output
    assert getattr(client, method).call_count == 0


@pytest.mark.parametrize("verb,method", _VERBS, ids=_VERB_IDS)
def test_project_scoped_permission_requires_proj(verb, method, tmp_path):
    client = _client()
    result = _invoke(
        ["permission", verb, "-u", "alice", "--permission", "develop_project"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code != 0
    assert "project-scoped and requires --proj" in result.output
    assert getattr(client, method).call_count == 0


@pytest.mark.parametrize("verb,method", _VERBS, ids=_VERB_IDS)
def test_system_scoped_permission_forbids_proj(verb, method, tmp_path):
    client = _client()
    result = _invoke(
        [
            "permission", verb, "-u", "alice",
            "--permission", "manage_system", "--proj", "p1",
        ],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code != 0
    assert "system-scoped" in result.output
    assert getattr(client, method).call_count == 0


@pytest.mark.parametrize("verb,method", _VERBS, ids=_VERB_IDS)
def test_rbac_only_wildcard_is_rejected(verb, method, tmp_path):
    client = _client()
    result = _invoke(
        ["permission", verb, "-u", "alice", "--permission", "view_all_projects"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code != 0
    # RBAC_ONLY_WILDCARD_MESSAGE explains these are RBAC-Mode-View only.
    assert "RBAC" in result.output
    assert getattr(client, method).call_count == 0


# --- accepted: the validated permission reaches the SDK ----------------------


@pytest.mark.parametrize("verb,method", _VERBS, ids=_VERB_IDS)
def test_valid_system_permission_passes_without_proj(verb, method, tmp_path):
    client = _client()
    result = _invoke(
        ["permission", verb, "-u", "alice", "--permission", "manage_system"],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    getattr(client, method).assert_called_once_with("alice", "manage_system", None)


@pytest.mark.parametrize("verb,method", _VERBS, ids=_VERB_IDS)
def test_valid_project_permission_passes_with_proj(verb, method, tmp_path):
    client = _client()
    result = _invoke(
        [
            "permission", verb, "-u", "alice",
            "--permission", "develop_project", "--proj", "proj-a",
        ],
        client=client,
        tmp_path=tmp_path,
    )
    assert result.exit_code == 0, result.output
    getattr(client, method).assert_called_once_with(
        "alice", "develop_project", "proj-a"
    )
