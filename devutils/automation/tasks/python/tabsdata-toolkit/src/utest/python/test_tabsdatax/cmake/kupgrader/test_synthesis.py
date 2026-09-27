#
# Copyright 2026 Tabsdata Inc.
#

from tabsdatax.cmake.kupgrader.common.actions import Action, Form, Plan
from tabsdatax.cmake.kupgrader.common.changes import (
    Change,
    settle_forms,
    synthesize_resolved,
    synthesize_yaml,
)
from tabsdatax.cmake.kupgrader.common.emission import resolved_of
from tabsdatax.cmake.kupgrader.common.inventory import Bundle, State, Variant
from tabsdatax.cmake.kupgrader.common.operations import Synthesis, synthesize
from tabsdatax.cmake.kupgrader.common.renames import read_renames

RENAMES = {"system:old_port": "system:new_port"}


def rendered(synthesis):
    return [operation.rendered() for operation in synthesis.operations]


def test_an_appended_ipv6_twin_is_derived_from_its_ipv4_item():
    synthesis = synthesize(
        {"addresses": ["0.0.0.0:${{system:port}}"]},
        {"addresses": ["0.0.0.0:${{system:port}}", "[::]:${{system:port}}"]},
    )
    assert synthesis.expressible
    assert rendered(synthesis) == [
        {
            "op": "derive-item-if-missing",
            "leaf": "addresses",
            "pattern": r"^0\.0\.0\.0:(?<port>.+)$",
            "item": "[::]:${port}",
        }
    ]


def test_an_appended_item_with_another_port_stays_a_gap():
    synthesis = synthesize(
        {"addresses": ["0.0.0.0:2457"]},
        {"addresses": ["0.0.0.0:2457", "[::]:9999"]},
    )
    assert not synthesis.expressible


def test_a_removed_key_is_removed():
    synthesis = synthesize({"port": 1, "keep": True}, {"keep": True})
    assert rendered(synthesis) == [{"op": "remove-leaf-if", "leaf": "port"}]


def test_a_moved_value_is_derived_before_its_key_is_removed():
    synthesis = synthesize(
        {"port": "${{system:port}}", "allow_writes": True},
        {
            "internal_addresses": ["0.0.0.0:${{system:port}}"],
            "allow_writes": True,
        },
    )
    assert rendered(synthesis) == [
        {
            "op": "derive-sibling-if-existing",
            "leaf": "port",
            "pattern": "^(?<value>.+)$",
            "sibling": "internal_addresses",
            "placement": "before",
            "value": ["0.0.0.0:${value}"],
        },
        {"op": "remove-leaf-if", "leaf": "port"},
    ]


def test_a_renamed_placeholder_is_rewritten_before_the_derived_changes():
    synthesis = synthesize_yaml(
        "agent_addresses:\n- host:${{system:old_port}}\n",
        "agent_addresses:\n- host:${{system:new_port}}\n",
        RENAMES,
    )
    assert rendered(synthesis) == [
        {
            "op": "modify-text-if",
            "pattern": r"\$\{\{system:old_port\}\}",
            "replacement": "${{system:new_port}}",
        }
    ]


def test_renames_are_read_for_their_version(tmp_path):
    renames = tmp_path / "renames.yaml"
    renames.write_text("2.2.0:\n  placeholders:\n    system:a: system:b\n")
    assert read_renames(renames, "2.2.0") == {"system:a": "system:b"}
    assert read_renames(renames, "2.1.0") == {}
    assert read_renames(tmp_path / "missing.yaml", "2.2.0") == {}


def variant(folder, name, state, previous_text=None, current_text=None):
    previous = current = None
    if previous_text is not None:
        previous = folder / "previous" / name
        previous.parent.mkdir(parents=True, exist_ok=True)
        previous.write_text(previous_text)
    if current_text is not None:
        current = folder / "current" / name
        current.parent.mkdir(parents=True, exist_ok=True)
        current.write_text(current_text)
    return Variant(
        family="configs",
        bundle="aiagent.yaml",
        name=name,
        state=state,
        parsable=True,
        previous=previous,
        current=current,
    )


def test_a_bundle_whose_winner_moves_edits_the_held_file(tmp_path):
    bundle = Bundle(
        family="configs",
        name="aiagent.yaml",
        variants=[
            variant(tmp_path, "aiagent.yaml", State.DELETED, "port: 1\nkeys: mine\n"),
            variant(
                tmp_path,
                "aiagent_eks.yaml",
                State.NEW,
                current_text="keys: mine\n",
            ),
        ],
    )
    changes = synthesize_resolved([bundle], {})
    assert [(change.family, change.name) for change in changes] == [
        ("resolved/configs", "aiagent_eks.yaml")
    ]
    assert rendered(changes[0].synthesis) == [{"op": "remove-leaf-if", "leaf": "port"}]


def test_a_bundle_whose_winner_stays_needs_no_resolved_change(tmp_path):
    bundle = Bundle(
        family="configs",
        name="aiagent.yaml",
        variants=[
            variant(tmp_path, "aiagent_eks.yaml", State.CHANGED, "a: 1\n", "a: 2\n"),
        ],
    )
    assert synthesize_resolved([bundle], {}) == []


def change(name, operations):
    synthesis = Synthesis(operations=operations)
    return Change(
        family="configs", bundle="apiserver.service.yaml", name=name, synthesis=synthesis
    )


def test_variants_needing_different_changes_are_emitted_one_by_one():
    plan = Plan(bundle=Bundle("configs", "apiserver.service.yaml", []), form=Form.GENERIC, reason="")
    eks = synthesize({"a": ["0.0.0.0:1"]}, {"a": ["0.0.0.0:1", "[::]:1"]})
    kminus = synthesize({"a": ["127.0.0.1:1"]}, {"a": ["127.0.0.1:1", "[::1]:1"]})
    settle_forms(
        [plan],
        [
            change("apiserver.service_eks.yaml", eks.operations),
            change("apiserver.service_kminus.yaml", kminus.operations),
        ],
    )
    assert plan.form is Form.VERBOSE


def test_variants_needing_the_same_changes_stay_generic():
    plan = Plan(bundle=Bundle("configs", "apiserver.service.yaml", []), form=Form.GENERIC, reason="")
    same = synthesize({"a": ["0.0.0.0:1"]}, {"a": ["0.0.0.0:1", "[::]:1"]}).operations
    settle_forms(
        [plan],
        [change("apiserver.service_eks.yaml", same), change("apiserver.service_aks.yaml", same)],
    )
    assert plan.form is Form.GENERIC


def test_resolved_changes_are_emitted_under_their_bundle():
    removal = synthesize({"port": 1}, {}).operations
    section = resolved_of(
        [
            Change("resolved/configs", "aiagent.yaml", "aiagent_eks.yaml", Synthesis(removal)),
            Change("resolved/configs", "aiagent.yaml", "aiagent_aks.yaml", Synthesis()),
        ]
    )
    assert section == {
        "configs": {
            "aiagent.yaml": {
                "aiagent_aks.yaml": {"action": Action.RETAIN.value},
                "aiagent_eks.yaml": {
                    "action": Action.MODIFY.value,
                    "changes": [{"op": "remove-leaf-if", "leaf": "port"}],
                },
            }
        }
    }
