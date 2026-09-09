#
# Copyright 2026 Tabsdata Inc.
#

from rich.console import Console
from rich.table import Table

from tabsdatax.cmake.kupgrader.common.actions import Action, Form, Plan
from tabsdatax.cmake.kupgrader.common.inventory import Bundle, State
from tabsdatax.cmake.kupgrader.common.naming import (
    FAMILIES,
    GENERIC_COLOR,
    PROVIDER_COLORS,
    colored,
)
from tabsdatax.cmake.kupgrader.common.workflow import RENDER_WIDTH

ACTION_STYLES = {
    Action.RETAIN: "dim",
    Action.CREATE: "green",
    Action.REPLACE: "yellow",
    Action.DELETE: "red",
    Action.MODIFY: "cyan",
}

FORM_STYLES = {
    Form.GENERIC: "green",
    Form.VERBOSE: "yellow",
}

STATE_STYLES = {
    State.UNCHANGED: "dim",
    State.CHANGED: "yellow",
    State.NEW: "green",
    State.DELETED: "red",
}


def display_inventory(bundles: list[Bundle]) -> None:
    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Inventory Summary",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Family", style="cyan", no_wrap=True)
    table.add_column("Bundles", justify="right")
    table.add_column("Altered", style="yellow", justify="right")
    table.add_column("Files", justify="right")
    for state in State:
        table.add_column(state.value, style=STATE_STYLES[state], justify="right")

    for family in FAMILIES:
        family_bundles = [bundle for bundle in bundles if bundle.family == family]
        variants = [variant for bundle in family_bundles for variant in bundle.variants]
        table.add_row(
            family,
            str(len(family_bundles)),
            str(sum(1 for bundle in family_bundles if bundle.altered)),
            str(len(variants)),
            *(
                str(sum(1 for variant in variants if variant.state is state))
                for state in State
            ),
        )

    console.print()
    console.print(table)
    console.print()


def display_gate(bundles: list[Bundle]) -> None:
    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Comparison Gate",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Family", style="cyan", no_wrap=True)
    table.add_column("Parsed as YAML", style="green", justify="right")
    table.add_column("Compared as text", style="yellow", justify="right")
    table.add_column("Text only reason", style="dim")

    for family in FAMILIES:
        variants = [
            variant
            for bundle in bundles
            if bundle.family == family
            for variant in bundle.variants
        ]
        textual = [variant for variant in variants if not variant.parsable]
        if family == "assets":
            reason = "Assets are always handled as whole files"
        elif textual:
            reason = "Not a YAML file"
        else:
            reason = ""
        table.add_row(
            family,
            str(sum(1 for variant in variants if variant.parsable)),
            str(len(textual)),
            reason,
        )

    console.print()
    console.print(table)
    console.print()


def display_providers() -> None:
    console = Console(width=RENDER_WIDTH)
    legend = "   ".join(
        f"[{color}]{provider}[/]" for provider, color in PROVIDER_COLORS.items()
    )
    console.print(f"Providers:   {legend}   [{GENERIC_COLOR}]generic[/]")


def display_bundles(bundles: list[Bundle]) -> None:
    altered = [bundle for bundle in bundles if bundle.altered]
    if not altered:
        return

    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Altered Bundles",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Family", style="cyan", no_wrap=True)
    table.add_column("Bundle", style="cyan")
    table.add_column("Variant", justify="right")
    table.add_column("State")

    for position, bundle in enumerate(altered):
        if position:
            table.add_section()
        for offset, variant in enumerate(bundle.variants):
            table.add_row(
                bundle.family if offset == 0 else "",
                bundle.name if offset == 0 else "",
                colored(variant.name),
                f"[{STATE_STYLES[variant.state]}]{variant.state.value}[/]",
            )

    console.print()
    console.print(table)
    console.print()


def display_plans(plans: list[Plan]) -> None:
    if not plans:
        return

    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Planned Actions",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Family", style="cyan", no_wrap=True)
    table.add_column("Bundle", style="cyan")
    table.add_column("Form", justify="center")
    table.add_column("Variant", justify="right")
    table.add_column("Action")
    table.add_column("Reason", style="dim")

    for position, plan in enumerate(plans):
        if position:
            table.add_section()
        for offset, decision in enumerate(plan.decisions):
            table.add_row(
                plan.family if offset == 0 else "",
                plan.name if offset == 0 else "",
                f"[{FORM_STYLES[plan.form]}]{plan.form.value}[/]"
                if offset == 0
                else "",
                colored(decision.name),
                f"[{ACTION_STYLES[decision.action]}]{decision.action.value}[/]",
                plan.reason if offset == 0 else "",
            )

    console.print()
    console.print(table)
    console.print()


def display_actions(plans: list[Plan], bundles: list[Bundle]) -> None:
    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Action Summary",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Family", style="cyan", no_wrap=True)
    for action in Action:
        table.add_column(
            action.value.capitalize(), style=ACTION_STYLES[action], justify="right"
        )
    table.add_column("Covered", style="bold blue", justify="right")
    table.add_column("Untouched", style="dim", justify="right")
    table.add_column("Total", style="bold", justify="right")

    totals = {action: 0 for action in Action}
    untouched_total = 0

    for family in FAMILIES:
        decisions = [
            decision
            for plan in plans
            if plan.family == family
            for decision in plan.decisions
        ]
        counts = {
            action: sum(1 for decision in decisions if decision.action is action)
            for action in Action
        }
        planned = {decision.name for decision in decisions}
        untouched = sum(
            1
            for bundle in bundles
            if bundle.family == family
            for variant in bundle.variants
            if variant.name not in planned
        )
        for action in Action:
            totals[action] += counts[action]
        untouched_total += untouched

        table.add_row(
            family,
            *(str(counts[action]) for action in Action),
            str(len(decisions)),
            str(untouched),
            str(len(decisions) + untouched),
        )

    table.add_section()
    covered_total = sum(totals.values())
    table.add_row(
        "Total",
        *(str(totals[action]) for action in Action),
        str(covered_total),
        str(untouched_total),
        str(covered_total + untouched_total),
    )

    console.print()
    console.print(table)
    console.print(
        "[dim]Untouched files are unaltered variants of unaltered bundles; they need "
        "no upgrader entry.[/]"
    )
    console.print()


def display_synthesis(entries: list) -> None:
    if not entries:
        return

    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Synthesized Changes",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Family", style="cyan", no_wrap=True)
    table.add_column("Variant", justify="right")
    table.add_column("Ops", justify="right")
    table.add_column("Operation", style="cyan")
    table.add_column("Leaf", style="dim")

    for position, entry in enumerate(entries):
        if position:
            table.add_section()
        operations = entry.synthesis.operations
        if not operations:
            table.add_row(entry.family, colored(entry.name), "0", "", "")
            continue
        for offset, operation in enumerate(operations):
            table.add_row(
                entry.family if offset == 0 else "",
                colored(entry.name) if offset == 0 else "",
                str(len(operations)) if offset == 0 else "",
                operation.op,
                str(operation.fields.get("leaf", "")),
            )

    console.print()
    console.print(table)
    console.print()


def display_gaps(entries: list) -> None:
    failing = [entry for entry in entries if not entry.synthesis.expressible]
    if not failing:
        return

    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Unexpressible Changes",
        show_header=True,
        header_style="bold red",
        border_style="red",
    )
    table.add_column("Family", style="cyan", no_wrap=True)
    table.add_column("Variant", justify="right")
    table.add_column("Leaf", style="yellow")
    table.add_column("Shape", style="bold red")
    table.add_column("Required operation", style="dim")

    for position, entry in enumerate(failing):
        if position:
            table.add_section()
        for offset, gap in enumerate(entry.synthesis.gaps):
            table.add_row(
                entry.family if offset == 0 else "",
                colored(entry.name) if offset == 0 else "",
                gap.leaf,
                gap.shape,
                gap.detail,
            )

    console.print()
    console.print(table)
    console.print()


def display_emission(emission, version: str) -> None:
    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Generated Upgrader",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Item", style="cyan", no_wrap=True)
    table.add_column("Value", style="yellow")

    table.add_row("Version", version)
    table.add_row("Specification", str(emission.spec_file))
    table.add_row("Payload folder", str(emission.content_folder))
    table.add_row("Payload files", str(len(emission.payloads)))

    console.print()
    console.print(table)
    console.print()


def display_verification(verification) -> None:
    console = Console(width=RENDER_WIDTH)

    table = Table(
        title="Upgrade Verification",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Item", style="cyan", no_wrap=True)
    table.add_column("Value", style="yellow")

    table.add_row("Upgrader binary", str(verification.binary))
    table.add_row("Applied steps", str(verification.steps))
    table.add_row("Changed resources", str(verification.changed))
    table.add_row("Files matching the current kprofile", str(verification.identical))
    table.add_row("Mismatches", str(len(verification.mismatches)))

    console.print()
    console.print(table)
    console.print()

    if verification.matched:
        return

    failures = Table(
        title="Verification Mismatches",
        show_header=True,
        header_style="bold red",
        border_style="red",
    )
    failures.add_column("Family", style="cyan", no_wrap=True)
    failures.add_column("File", justify="right")
    failures.add_column("Reason", style="yellow")

    for mismatch in verification.mismatches:
        failures.add_row(mismatch.family, colored(mismatch.name), mismatch.reason)

    console.print(failures)
    console.print()


def display_comparison(comparison) -> None:
    console = Console(width=RENDER_WIDTH)

    if comparison.matched:
        console.print()
        console.print(
            "[green]The shipped upgrader matches the generated one.[/]",
        )
        console.print()
        return

    table = Table(
        title="Specification Differences",
        show_header=True,
        header_style="bold red",
        border_style="red",
    )
    table.add_column("Path", style="cyan")
    table.add_column("Reason", style="yellow")

    for difference in comparison.differences:
        table.add_row(difference.path, difference.reason)

    console.print()
    console.print(table)
    console.print()

    for difference in comparison.differences:
        if not difference.diff:
            continue
        console.print(f"[bold]Differences in {difference.path}:[/]")
        for line in difference.diff:
            console.print(f"  {difference_style(line)}")
        console.print()


def difference_style(line: str) -> str:
    if line.startswith("+") and not line.startswith("+++"):
        return f"[green]{line}[/]"
    if line.startswith("-") and not line.startswith("---"):
        return f"[red]{line}[/]"
    if line.startswith("@@"):
        return f"[cyan]{line}[/]"
    return f"[dim]{line}[/]"
