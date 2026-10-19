#
# Copyright 2026 Tabsdata Inc.
#

import textwrap
from datetime import date

import pytest

from tabsdatax.cmake.kupgrader.common import rules
from tabsdatax.cmake.kupgrader.common.emission import HEADER, Emission
from tabsdatax.cmake.kupgrader.common.pipeline import Generation
from tabsdatax.cmake.kupgrader.common.rules import (
    DEFAULT_RULES,
    ensure_default_rules,
    rules_file,
    rules_problems,
)
from tabsdatax.cmake.kupgrader.common.versions import is_micro
from tabsdatax.cmake.kupgrader.common.workflow import Context


def context_in(root) -> Context:
    return Context(mode="build", root_folder=root)


def generation_emitting(tmp_path, spec: str, payloads=()) -> Generation:
    spec_file = tmp_path / "spec.yaml"
    spec_file.write_text(spec)
    return Generation(
        preparation=None,
        bundles=[],
        plans=[],
        changes=[],
        resolved=[],
        emission=Emission(
            spec_file=spec_file,
            content_folder=tmp_path / "content",
            payloads=list(payloads),
        ),
    )


@pytest.mark.parametrize(
    ("version", "micro"),
    [("2.3.0", False), ("2.3.1", True), ("3.0.0", False), ("2.2.12", True)],
)
def test_micro_versions_are_the_ones_with_a_micro_number(version, micro):
    assert is_micro(version) is micro


def test_a_spec_with_only_the_schema_is_a_noop(tmp_path):
    spec = textwrap.dedent(
        """\
        #
        # header
        #

        schema: 1
        """
    )
    assert generation_emitting(tmp_path, spec).noop


def test_a_spec_with_changes_is_not_a_noop(tmp_path):
    spec = textwrap.dedent(
        """\
        schema: 1
        pods:
          function.yaml:
            function_docker.yaml:
              action: replace
        """
    )
    assert not generation_emitting(tmp_path, spec).noop


def test_a_spec_with_payloads_is_not_a_noop(tmp_path):
    spec = textwrap.dedent(
        """\
        schema: 1
        """
    )
    assert not generation_emitting(
        tmp_path, spec, payloads=[tmp_path / "payload.yaml"]
    ).noop


def test_the_default_rules_are_written_when_missing(tmp_path):
    context = context_in(tmp_path)
    written = ensure_default_rules(context, "2.4.0")
    assert written == rules_file(context, "2.4.0")
    text = written.read_text()
    assert text.startswith(HEADER.format(year=date.today().year))
    assert text.endswith(DEFAULT_RULES)


def test_existing_rules_are_never_overwritten(tmp_path):
    context = context_in(tmp_path)
    path = rules_file(context, "2.3.0")
    path.parent.mkdir(parents=True)
    path.write_text(
        textwrap.dedent(
            """\
            schema: 1
            rules:
              - ensure-project-secret
            """
        )
    )
    assert ensure_default_rules(context, "2.3.0") is None
    assert "ensure-project-secret" in path.read_text()


def test_the_rules_problems_come_from_the_upgrader_binary(tmp_path, monkeypatch):
    calls = []

    def run_upgrader(binary, arguments):
        calls.append((binary, arguments))
        return {"problems": ["Version '2.3.1' is a micro version"]}

    monkeypatch.setattr(rules, "run_upgrader", run_upgrader)
    binary = tmp_path / "tdk_kupgrader"
    assert rules_problems(binary, tmp_path / "kupgrade") == [
        "Version '2.3.1' is a micro version"
    ]
    assert calls == [(binary, ["rules", "--kupgrade", str(tmp_path / "kupgrade")])]
