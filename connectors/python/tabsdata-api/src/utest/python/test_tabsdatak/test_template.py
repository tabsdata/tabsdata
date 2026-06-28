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


def test_ref_class_picks_first_union_member():
    # First member Secret/Var -> that ref class drives the sample.
    assert template._ref_class(template._members(typing.Union[Secret, Var, str])[0]) is Secret
    assert template._ref_class(template._members(typing.Union[Var, str])[0]) is Var
    # First member a plain type -> not a ref (rendered as a scalar/placeholder).
    assert template._ref_class(template._members(typing.Union[str, Secret, Var])[0]) is None


def test_scalar_placeholder_by_type():
    assert template._scalar_placeholder([str], None, "p") == "<FILL: p>"
    assert template._scalar_placeholder([int], None, "p") == 0
    assert template._scalar_placeholder([bool], None, "p") is False
    assert template._scalar_placeholder([list], None, "p") == []
    assert template._scalar_placeholder([str], "given", "p") == "given"
    assert template._scalar_placeholder([typing.Literal["a", "b"]], None, "p") == "a"


def test_type_comment_lists_accepted_forms():
    # Forms are listed in the union's declaration order.
    assert (
        template._type_comment(template._members(typing.Union[str, Var, Secret]))
        == "accepts: <value> | var:<value> | secret:<value>"
    )
    assert (
        template._type_comment(template._members(typing.Union[Var, Secret]))
        == "accepts: var:<value> | secret:<value>"
    )
    # A single accepted form -- a plain scalar, or a lone Secret credential --
    # gets no comment.
    assert template._type_comment([str]) is None
    assert template._type_comment([int]) is None
    assert template._type_comment([Secret]) is None


# --- YAML serialization -----------------------------------------------------


def test_yaml_round_trips_through_from_yaml():
    info = template.resolve_conn("localfile", "src")
    doc = template.build_template(info.conn_cls, all_fields=True)
    text = template.to_yaml(doc)
    assert "kind: localFileSrcConn" in text
    # YAML parses back to an equivalent structure that still validates.
    parsed = template.from_yaml(text)
    assert parsed["kind"] == "localFileSrcConn"
    assert template.validate(parsed) == "localFileSrcConn"


def test_yaml_template_has_header():
    info = template.resolve_conn("localfile", "src")
    text = template.to_yaml(template.build_template(info.conn_cls))
    lines = text.splitlines()
    # Names the connector/direction the template targets.
    assert lines[0] == "# Connection template for: localfile (src)"
    # Reminds that secret/var values carry the 'secret:' / 'var:' prefix.
    assert "secret:" in lines[1] and "var:" in lines[1]
    # The header is a comment, so the document still parses + validates.
    assert template.validate(template.from_yaml(text)) == "localFileSrcConn"


@pytest.mark.skipif(
    not _installed("mysql", "src"), reason="mysql connector not installed"
)
def test_yaml_template_carries_accepts_comments():
    info = template.resolve_conn("mysql", "src")
    text = template.to_yaml(template.build_template(info.conn_cls))
    # uri (str|Var|Secret) lists its forms in the union's declaration order.
    assert "# accepts: <value> | var:<value> | secret:<value>" in text  # uri
    # Single-form fields (the Secret-only credentials) carry no comment, so uri
    # is the only "# accepts:" note.
    assert text.count("# accepts:") == 1
    # The YAML parses back; comments are dropped from the parsed structure (the
    # raw template carries a "<FILL: uri>" placeholder, so it is not yet a valid
    # connection -- the user fills it in).
    parsed = template.from_yaml(text)
    assert parsed["kind"] == "mysqlSrcConn"
    assert parsed["spec"]["uri"] == "<FILL: uri>"


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
    # Each field renders from its union's FIRST member. credentials.user/password
    # are Union[Secret, Var] -> the bare unhydrated ref string "secret:". uri is
    # Union[str, Secret, Var] (str-first, so plain strings parse as strings) ->
    # a fill-in placeholder; it still accepts a "secret:"/"var:" ref if edited.
    doc = template.build_template(info.conn_cls)
    creds = doc["spec"]["credentials"]["spec"]
    assert creds["user"] == "secret:"
    assert creds["password"] == "secret:"
    assert doc["spec"]["uri"] == "<FILL: uri>"
    # Wizard: secret/var-first leaves are prompted as refs; the str-first uri is
    # a plain prompt. A valid uri makes the result validate.
    record: list = []
    out = template.build_template_interactive(
        info.conn_cls, _scripted({"uri": "mysql://h:3306/db"}, record)
    )
    secret_paths = {p for p, s, o, c in record if s}
    assert {"credentials.user", "credentials.password"} <= secret_paths
    assert "uri" not in secret_paths
    assert template.validate(out) == "mysqlSrcConn"
