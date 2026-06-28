#
# Copyright 2026 Tabs Data Inc.
#

"""Tests for the connection-template engine :mod:`tabsdatak._template`.

The engine introspects whatever connectors are installed (via the SPI
registry); ``localfile`` ships with the api package's reference install and is
always present, so the universal assertions key off it. Connector-shape
spot-checks (secret refs, nested credentials) are guarded on the richer
connectors being installed, mirroring ``test_registry.py``.
"""

from __future__ import annotations

import typing

import pytest

# noinspection PyProtectedMember
from tabsdatak import _template as template

# noinspection PyProtectedMember
from tabsdatak._api import resolve_class
from tabsdatak.api import Secret, Var


def _installed(type_: str, direction: str) -> bool:
    try:
        template.resolve_conn(type_, direction)
        return True
    except template.TemplateError:
        return False


def _scripted(values: dict, record: list | None = None):
    def prompt(path, *, secret, optional, default, pattern, choices):
        if record is not None:
            record.append((path, secret, optional, choices))
        if path in values:
            return values[path]
        if choices:
            return choices[0]
        if optional:
            return None
        # secret leaves: blank -> runtime-resolved ref; plain: a scripted value
        return None if secret else f"val_{path}"

    return prompt


# --- discovery --------------------------------------------------------------


def test_localfile_present_both_directions():
    rows = template.list_connectors()
    pairs = {(r.type, r.direction) for r in rows}
    assert ("localfile", "src") in pairs
    assert ("localfile", "dest") in pairs


def test_every_template_is_structurally_well_formed():
    for r in template.list_connectors():
        for all_fields in (False, True):
            doc = template.build_template(r.conn_cls, all_fields=all_fields)
            assert doc.keys() >= {"kind", "apiVersion", "type", "spec"}
            assert doc["kind"] == r.conn_cls.__kind__
            assert resolve_class(doc["type"]) is r.conn_cls
            assert isinstance(doc["spec"], dict)


def test_localfile_template_round_trips():
    # localfile has only optional fields, so the required-only template is a
    # complete, valid connection on its own.
    info = template.resolve_conn("localfile", "src")
    doc = template.build_template(info.conn_cls)
    assert doc["spec"] == {}
    assert template.validate(doc) == "localFileSrcConn"
    # --all-fields surfaces the optional fields.
    assert set(template.build_template(info.conn_cls, all_fields=True)["spec"]) == {
        "base_path",
        "conn_cfg",
    }


# --- resolve_conn -----------------------------------------------------------


def test_resolve_infers_unique_direction():
    # localfile is both src and dest -> ambiguous without -d.
    with pytest.raises(template.TemplateError, match="ambiguous"):
        template.resolve_conn("localfile", None)


def test_resolve_unknown_type():
    with pytest.raises(template.TemplateError, match="unknown connector type"):
        template.resolve_conn("does-not-exist", "src")


def test_resolve_bad_direction():
    with pytest.raises(template.TemplateError, match="unknown direction"):
        template.resolve_conn("localfile", "sideways")


def test_resolve_direction_not_offered():
    with pytest.raises(template.TemplateError, match="no .* direction"):
        template.resolve_conn("localfile", "stream-src")


# --- validate ---------------------------------------------------------------


def test_validate_rejects_non_dict():
    with pytest.raises(template.TemplateError, match="JSON object"):
        template.validate("nope")


def test_validate_rejects_missing_envelope_keys():
    with pytest.raises(template.TemplateError, match="missing envelope key"):
        template.validate({"kind": "localFileSrcConn"})


def test_validate_rejects_unresolvable_type():
    with pytest.raises(template.TemplateError):
        template.validate(
            {"kind": "x", "apiVersion": "1.0", "type": "no.such:Class", "spec": {}}
        )


def test_validate_rejects_non_connection_type():
    with pytest.raises(template.TemplateError, match="connection class"):
        template.validate(
            {
                "kind": "x",
                "apiVersion": "1.0",
                "type": "tabsdatak.api:Secret",
                "spec": {},
            }
        )


# --- annotation-analysis helpers (no connector deps) ------------------------


def test_members_unwraps_union_and_annotated():
    assert template._members(typing.Union[str, Secret, Var]) == [str, Secret, Var]
    assert template._members(typing.Optional[int]) == [int]


def test_is_secret_ref_distinguishes_identifier_from_secret():
    # str|Var|Secret (uri-style) is an identifier, not a secret leaf.
    assert not template._is_secret_ref(
        template._members(typing.Union[str, Var, Secret])
    )
    # Var|Secret (VarOrSecretSpec) is a secret leaf.
    assert template._is_secret_ref(template._members(typing.Union[Var, Secret]))


def test_scalar_placeholder_by_type():
    assert template._scalar_placeholder([str], None, "p") == "<FILL: p>"
    assert template._scalar_placeholder([int], None, "p") == 0
    assert template._scalar_placeholder([bool], None, "p") is False
    assert template._scalar_placeholder([list], None, "p") == []
    assert template._scalar_placeholder([str], "given", "p") == "given"
    assert template._scalar_placeholder([typing.Literal["a", "b"]], None, "p") == "a"


# --- wizard -----------------------------------------------------------------


def test_localfile_wizard_validates():
    info = template.resolve_conn("localfile", "src")
    doc = template.build_template_interactive(info.conn_cls, _scripted({}))
    assert template.validate(doc) == "localFileSrcConn"


@pytest.mark.skipif(
    not _installed("mysql", "src"), reason="mysql connector not installed"
)
def test_mysql_secrets_render_as_refs_and_wizard_validates():
    info = template.resolve_conn("mysql", "src")
    # Static template: credentials are secret refs, uri is a fill-in string.
    doc = template.build_template(info.conn_cls)
    creds = doc["spec"]["credentials"]["spec"]
    assert creds["user"]["kind"] == "secretRef"
    assert creds["password"]["kind"] == "secretRef"
    assert doc["spec"]["uri"] == "<FILL: uri>"
    # Wizard: secret leaves are prompted; a valid uri makes the result validate.
    record: list = []
    out = template.build_template_interactive(
        info.conn_cls, _scripted({"uri": "mysql://h:3306/db"}, record)
    )
    secret_paths = {p for p, s, o, c in record if s}
    assert {"credentials.user", "credentials.password"} <= secret_paths
    assert template.validate(out) == "mysqlSrcConn"
