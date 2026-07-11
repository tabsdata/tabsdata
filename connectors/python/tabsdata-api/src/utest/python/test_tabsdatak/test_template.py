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
from tabsdatak.api import Secret


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
    assert ("local-file-in", "src") in pairs
    assert ("local-file-out", "dest") in pairs


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
    info = template.resolve_conn("local-file-in", "src")
    doc = template.build_template(info.conn_cls)
    assert doc["spec"] == {}
    assert template.validate(doc) == "connectionDef"
    # all_fields surfaces the optional fields: base_path (default '') as a real
    # key, conn_cfg (default None) commented out so the document stays valid.
    full = template.build_template(info.conn_cls, all_fields=True)
    assert set(full["spec"]) == {"base_path"}
    text = template.to_yaml(full)
    assert "base_path: ''" in text
    assert "# conn_cfg" in text
    # Still a valid connection (the commented conn_cfg is not parsed).
    assert template.validate(template.from_yaml(text)) == "connectionDef"


def test_all_fields_comments_out_none_defaults():
    # localfile: base_path (default '') is a real key with an 'optional' note;
    # conn_cfg (default None) is commented out with a short 'what's expected'.
    info = template.resolve_conn("local-file-in", "src")
    text = template.to_yaml(template.build_template(info.conn_cls, all_fields=True))
    assert "# optional (default: '')" in text
    assert "base_path: ''" in text
    assert "# conn_cfg  (optional, default: none) -- a dict (str-keyed)" in text
    assert "# conn_cfg: {}" in text


def test_expected_note():
    assert template._expected_note([dict]) == "a dict (str-keyed)"
    assert template._expected_note([list]) == "a list"
    assert (
        template._expected_note(template._members(typing.Union[str, Secret]))
        == "accepts: <value> | secret:<value>"
    )
    assert template._expected_note([typing.Literal["a", "b"]]) == "one of: a | b"
    assert template._expected_note([str]) is None


# --- resolve_conn -----------------------------------------------------------


def test_resolve_infers_unique_direction():
    # Each connector type is globally unique, so resolve infers the single
    # direction it offers even when none is given.
    info = template.resolve_conn("local-file-in", None)
    assert (info.type, info.direction) == ("local-file-in", "src")


def test_resolve_unknown_type():
    with pytest.raises(template.TemplateError, match="unknown connector type"):
        template.resolve_conn("does-not-exist", "src")


def test_resolve_bad_direction():
    with pytest.raises(template.TemplateError, match="unknown direction"):
        template.resolve_conn("local-file-in", "sideways")


def test_resolve_direction_not_offered():
    with pytest.raises(template.TemplateError, match="no .* direction"):
        template.resolve_conn("local-file-in", "stream-src")


# --- validate ---------------------------------------------------------------


def test_validate_rejects_non_dict():
    with pytest.raises(template.TemplateError, match="JSON object"):
        template.validate("nope")


def test_validate_rejects_missing_envelope_keys():
    with pytest.raises(template.TemplateError, match="missing envelope key"):
        template.validate({"kind": "connectionDef"})


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
    assert template._members(typing.Union[str, Secret]) == [str, Secret]
    assert template._members(typing.Optional[int]) == [int]


def test_ref_class_picks_first_union_member():
    # First member Secret -> that ref class drives the sample.
    assert template._ref_class(template._members(typing.Union[Secret, str])[0]) is Secret
    # First member a plain type -> not a ref (rendered as a scalar/placeholder).
    assert template._ref_class(template._members(typing.Union[str, Secret])[0]) is None


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
        template._type_comment(template._members(typing.Union[str, Secret]))
        == "accepts: <value> | secret:<value>"
    )
    # A single accepted form -- a plain scalar, or a lone Secret credential --
    # gets no comment.
    assert template._type_comment([str]) is None
    assert template._type_comment([int]) is None
    assert template._type_comment([Secret]) is None


# --- YAML serialization -----------------------------------------------------


def test_yaml_round_trips_through_from_yaml():
    info = template.resolve_conn("local-file-in", "src")
    doc = template.build_template(info.conn_cls, all_fields=True)
    text = template.to_yaml(doc)
    assert "kind: connectionDef" in text
    # YAML parses back to an equivalent structure that still validates.
    parsed = template.from_yaml(text)
    assert parsed["kind"] == "connectionDef"
    assert template.validate(parsed) == "connectionDef"


def test_yaml_template_has_header():
    info = template.resolve_conn("local-file-in", "src")
    text = template.to_yaml(template.build_template(info.conn_cls))
    lines = text.splitlines()
    # Names the connector/direction the template targets.
    assert lines[0] == "# Connection template for: local-file-in (src)"
    # Reminds that secret values carry the 'secret:' prefix, or a
    # '$secret:<ENV_VAR>' environment reference.
    assert "secret:" in lines[1]
    assert "$secret:" in lines[1]
    # Notes that a '$secret:' value is resolved client-side at create/update.
    assert "client" in lines[2]
    # The header is a comment, so the document still parses + validates.
    assert template.validate(template.from_yaml(text)) == "connectionDef"


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_yaml_template_carries_accepts_comments():
    info = template.resolve_conn("mysql-sql-in", "src")
    text = template.to_yaml(template.build_template(info.conn_cls))
    # uri (str|Secret) lists its forms in the union's declaration order.
    assert "# accepts: <value> | secret:<value>" in text  # uri
    # Single-form fields (the Secret-only credentials) carry no comment, so uri
    # is the only "# accepts:" note.
    assert text.count("# accepts:") == 1
    # The YAML parses back; comments are dropped from the parsed structure (the
    # raw template carries a "<value>" fill-in placeholder, so it is not yet a
    # valid connection -- the user replaces it).
    parsed = template.from_yaml(text)
    assert parsed["kind"] == "connectionDef"
    assert parsed["spec"]["uri"] == "<value> or secret:<value>"
    # Each fill-in field carries a "replace <value> ..." line in its block.
    assert "# replace <value> with the actual uri" in text


# --- wizard -----------------------------------------------------------------


def test_localfile_wizard_validates():
    info = template.resolve_conn("local-file-in", "src")
    doc = template.build_template_interactive(info.conn_cls, _scripted({}))
    assert template.validate(doc) == "connectionDef"


@pytest.mark.skipif(
    not _installed("mysql-sql-in", "src"), reason="mysql connector not installed"
)
def test_mysql_secrets_render_as_refs_and_wizard_validates():
    info = template.resolve_conn("mysql-sql-in", "src")
    # Each field renders a fill-in placeholder showing its accepted forms.
    # credentials.user/password are Secret-only -> 'secret:<value>'. uri is
    # Union[str, Secret] -> '<value> or secret:<value>'.
    doc = template.build_template(info.conn_cls)
    creds = doc["spec"]["credentials"]["spec"]
    assert creds["user"] == "secret:<value>"
    assert creds["password"] == "secret:<value>"
    assert doc["spec"]["uri"] == "<value> or secret:<value>"
    # Wizard: secret-first leaves are prompted as refs; the str-first uri is
    # a plain prompt. A valid uri makes the result validate.
    record: list = []
    out = template.build_template_interactive(
        info.conn_cls, _scripted({"uri": "mysql://h:3306/db"}, record)
    )
    secret_paths = {p for p, s, o, c in record if s}
    assert {"credentials.user", "credentials.password"} <= secret_paths
    assert "uri" not in secret_paths
    assert template.validate(out) == "connectionDef"
