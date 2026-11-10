#
# Copyright 2026 Tabsdata Inc.
#

"""Bounded waits for plans, transactions, function runs and connection probes.

A wait runs for ``--timeout`` seconds, else the machine run's
``wait_seconds``, or without limit for humans; the machine run's ``max_wait``
and the expiry of a ``TDK_BEARER`` session cap it. Running out of time exits 7
(``wait_timeout``) with the argv that resumes the wait, which never triggers
anything again; a wait that ends with runs (or a probe) that did not succeed
exits 8 (``run_failed``)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

import rich_click as click
from rich.table import Table

from tabsdatak.cli._credentials import EXPIRY_MARGIN_SECONDS
from tabsdatak.cli._errors import TdkException
from tabsdatak.cli._machine import machine
from tabsdatak.cli.cli_utils import (
    active_profile,
    beautify_time,
    collection_label,
    default_profile,
    output_format,
    progress,
    progress_console,
    render,
)
from tabsdatak.cli.td.execution_utils import span_projects
from tabsdatak.sdk.status_utils.function_run import PhysicalFunctionRunStatus
from tabsdatak.sdk.status_utils.groups import (
    Entity,
    Health,
    annotate,
    health,
    is_final,
)

if TYPE_CHECKING:
    from tabsdatak.sdk.tabsdata_server import (
        Execution,
        FunctionRun,
        TabsdataServer,
        Transaction,
    )

FINAL_READ_SECONDS = 5.0


def wait_option(subject: str, failed: str = "a function run", default: bool = False):
    """``--wait``, or ``--no-wait`` when a command waits by ``default``."""
    exits = f"Exits 7 when '--timeout' runs out and 8 when {failed} did not succeed."
    if default:
        return click.option(
            "--no-wait",
            "wait",
            flag_value=False,
            default=True,
            help=f"Return at once instead of waiting for {subject} to end. {exits}",
        )
    return click.option(
        "--wait",
        is_flag=True,
        default=False,
        help=f"Wait until {subject} reaches a final status. {exits}",
    )


SUMMARY = "wait_summary"


def _apply_summary(ctx: click.Context, param, value):
    ctx.ensure_object(dict)
    ctx.obj[SUMMARY] = value
    return value


def summary_option(subject: str):
    """``--summary``: a wait returns only the summary of ``subject``."""
    return click.option(
        "--summary",
        is_flag=True,
        default=False,
        expose_value=False,
        callback=_apply_summary,
        help=(
            f"With the wait, return only {subject}'s status, its function runs "
            "by status and the failed ones, instead of every run."
        ),
    )


def _by_status(runs: list[dict]) -> dict:
    counts: dict[str, int] = {}
    for run in runs:
        name = run.get("status_name") or run.get("status")
        counts[name] = counts.get(name, 0) + 1
    return counts


def wait_summary(entity: Entity, document: dict) -> dict:
    """What a waited plan, transaction or function run ended as: its id, its
    status and health, its function runs by status and those that did not
    succeed."""
    if entity is Entity.FUNCTION_RUN:
        failed = _failed(document.get("status"))
        return {
            "function_run": document.get("id"),
            "plan": document.get("execution_id"),
            "status_name": document.get("status_name"),
            "health": document.get("health"),
            "failed_runs": [document.get("id")] if failed else [],
        }
    runs = document.get("runs") or []
    key = "plan" if entity is Entity.EXECUTION else "transaction"
    return {
        key: document.get("id"),
        "status_name": document.get("status_name"),
        "health": document.get("health"),
        "function_runs_by_status": document.get("function_runs_by_status")
        or _by_status(runs),
        "failed_runs": [run["id"] for run in runs if _failed(run.get("status"))],
    }


def _summarized(ctx: click.Context, entity: Entity, document: dict) -> dict:
    if not (ctx.obj or {}).get(SUMMARY):
        return document
    summary = wait_summary(entity, document)
    if "waited_ms" in document:
        summary["waited_ms"] = document["waited_ms"]
    return summary


class TimeoutOption(click.Option):
    """``--timeout``: its help names the default a person gets; a program
    running tdk sees its own, ``Machine.wait_seconds``."""

    def __init__(self, *args, head: str, shown: str, **kwargs):
        super().__init__(*args, help=f"{head} Default: {shown}.", **kwargs)
        self.head = head

    def machine_help(self, run) -> str:
        seconds = run.wait_seconds
        if run.max_wait is not None and (seconds is None or seconds > run.max_wait[0]):
            seconds = run.max_wait[0]
        if seconds is None:
            return self.help
        return f"{self.head} Default: {seconds:g}."


def timeout_option(default: float | None = None, most: float | None = None):
    """``--timeout``; ``default`` is what the command applies when it is not
    given, and only describes it here."""
    at_most = "" if most is None else f", at most {most:g}"
    shown = "no limit" if default is None else f"{default:g}"
    return click.option(
        "--timeout",
        cls=TimeoutOption,
        type=click.FloatRange(min=0, max=most),
        default=None,
        metavar="S",
        head=f"Stop waiting after S seconds{at_most}.",
        shown=shown,
    )


def require_wait(wait: bool, timeout: float | None, attempt: str | None = None) -> None:
    if wait:
        return
    if timeout is not None:
        raise click.UsageError("'--timeout' applies only while waiting.")
    if attempt is not None:
        raise click.UsageError("'--attempt' applies only while waiting.")


@dataclass(frozen=True)
class Budget:
    seconds: float | None
    limit: str | None = None


def budget(ctx: click.Context, timeout: float | None) -> Budget:
    seconds, limit = timeout, None
    run = machine()
    if seconds is None and run is not None:
        seconds = run.wait_seconds
    if run is not None and run.max_wait is not None:
        cap, source = run.max_wait
        if seconds is None or seconds > cap:
            seconds, limit = cap, source
    credential = (ctx.obj or {}).get("env_credential")
    if credential is not None and credential.expires_at is not None:
        left = max(credential.expires_at - time.time() - EXPIRY_MARGIN_SECONDS, 0.0)
        if seconds is None or seconds > left:
            seconds, limit = left, f"the expiry of the session in {credential.env}"
    if limit is not None:
        progress(ctx, f"Waiting at most {seconds:.0f} s, limited by {limit}.")
    return Budget(seconds, limit)


def _timeout(wait: Budget, subject: str, status, resume, state) -> TdkException:
    hint = "Resume waiting with '" + " ".join(resume) + "'." if resume else None
    if wait.limit is not None:
        hint = f"The wait was limited by {wait.limit}. {hint or ''}".strip()
    return TdkException(
        "wait_timeout",
        f"{subject} is still {status} after {wait.seconds:.0f} s.",
        hint=hint,
        resume=resume,
        state=state,
    )


def _runs_table(runs, by_plan: bool) -> Table:
    table = Table(title="Function runs")
    table.add_column("Function run ID", style="cyan", no_wrap=True)
    table.add_column("Collection")
    table.add_column("Function")
    table.add_column("Transaction ID" if by_plan else "Plan ID", no_wrap=True)
    table.add_column("Started on", no_wrap=True)
    table.add_column("Ended on", no_wrap=True)
    table.add_column("Status")
    for run in runs:
        table.add_row(
            run.id,
            collection_label(run),
            run.function.name,
            run.transaction.id if by_plan else run.execution.id,
            beautify_time(run.started_on_str),
            beautify_time(run.ended_on_str),
            run.status,
        )
    return table


def _resume(ctx: click.Context, argv: list[str]) -> list[str]:
    profile = active_profile(ctx)
    if profile:
        return [*argv, "--profile", profile]
    if default_profile(ctx):
        return [*argv, "--profile"]
    return argv


def _observed(connection, read: Callable[[], dict], fallback: dict) -> dict:
    """The last state for a timeout: read again, for at most
    ``FINAL_READ_SECONDS``, or ``fallback``, the last state seen, when the
    server does not answer in time."""
    base = connection.timeout
    connection.timeout = tuple(min(part, FINAL_READ_SECONDS) for part in base)
    try:
        return read()
    except Exception:
        return fallback
    finally:
        connection.timeout = base


def _outage_reporter(ctx: click.Context) -> Callable[[Exception | None], None]:
    def report(error: Exception | None) -> None:
        from tabsdatak.sdk.tabsdata_server import WAIT_OUTAGE_GRACE

        if error is None:
            progress(ctx, "Connection to the server restored.")
        else:
            progress(
                ctx,
                f"Lost the connection to the server ({error}); retrying for up "
                f"to {WAIT_OUTAGE_GRACE:g}s.",
            )

    return report


def _elapsed_ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


def _wait_with_runs(
    ctx: click.Context,
    target: Execution | Transaction,
    entity: Entity,
    timeout: float | None,
    live: bool,
) -> dict:
    from rich.live import Live

    from tabsdatak.sdk.tabsdata_server import WaitTimeout

    by_plan = entity is Entity.EXECUTION
    keyword, command, flag = (
        ("plan", "plan", "--plan") if by_plan else ("transaction", "trx", "--trx")
    )
    subject = f"The {keyword} '{target.id}'"
    resume = _resume(ctx, ["tdk", command, "get", flag, target.id, "--wait"])
    wait = budget(ctx, timeout)
    live = live and output_format(ctx) == "table"
    runs: list = []
    seen = [{"id": target.id}]

    def snapshot() -> dict:
        return {
            **annotate(entity, target._data),
            "runs": [annotate(Entity.FUNCTION_RUN, run._data) for run in runs],
        }

    def record(current) -> None:
        seen[0] = snapshot()

    progress(ctx, f"Waiting for the {keyword} '{target.id}' to finish...")
    started = time.monotonic()
    try:
        if live:
            with Live(
                _runs_table(runs, by_plan),
                refresh_per_second=1,
                console=progress_console(ctx),
            ) as display:

                def show(current) -> None:
                    runs[:] = current.function_runs
                    display.update(_runs_table(runs, by_plan))
                    record(current)

                target.wait(
                    timeout=wait.seconds,
                    on_update=show,
                    on_outage=_outage_reporter(ctx),
                )
        else:
            target.wait(
                timeout=wait.seconds,
                on_update=record,
                on_outage=_outage_reporter(ctx),
            )
            runs[:] = target.function_runs
    except WaitTimeout:

        def observe() -> dict:
            if not live:
                runs[:] = target.function_runs
            return snapshot()

        state = _observed(target.connection, observe, seen[0])
        raise _timeout(
            wait, subject, state.get("status_name", "running"), resume, state
        )
    progress(ctx, f"{keyword.capitalize()} finished.")
    failed = [run.id for run in runs if _failed(run.status)]
    if failed:
        raise TdkException(
            "run_failed",
            f"{subject} finished, but {len(failed)} function run(s) did not "
            f"succeed: {', '.join(failed)}.",
            hint=f"Diagnose it with 'tdk fnrun diagnose --fnrun {failed[0]}'.",
            state=wait_summary(entity, snapshot()),
        )
    progress(ctx, "All function runs were successful.")
    return _summarized(ctx, entity, {**snapshot(), "waited_ms": _elapsed_ms(started)})


def wait_execution(
    ctx: click.Context, execution: Execution, timeout: float | None, live=True
) -> dict:
    """Wait for a plan; returns it annotated, with its runs and ``waited_ms``."""
    return _wait_with_runs(ctx, execution, Entity.EXECUTION, timeout, live)


def wait_transaction(
    ctx: click.Context, transaction: Transaction, timeout: float | None, live=True
) -> dict:
    """Wait for a transaction; returns it annotated, with its runs and
    ``waited_ms``."""
    return _wait_with_runs(ctx, transaction, Entity.TRANSACTION, timeout, live)


def render_waited_execution(
    ctx: click.Context, server: TabsdataServer, plan: str, timeout: float | None
) -> None:
    span_projects(server)
    execution = server.get_execution(plan)
    render(ctx, data=wait_execution(ctx, execution, timeout))


def render_waited_transaction(
    ctx: click.Context, server: TabsdataServer, trx: str, timeout: float | None
) -> None:
    span_projects(server)
    transaction = server.get_transaction(trx)
    render(ctx, data=wait_transaction(ctx, transaction, timeout))


def _failed(status: str | None) -> bool:
    return health(Entity.FUNCTION_RUN, status) not in (Health.OK, None)


_ATTEMPT_FAILED = frozenset({
    PhysicalFunctionRunStatus.ERROR.value,
    PhysicalFunctionRunStatus.FAILED.value,
})


def wait_function_run(
    ctx: click.Context,
    function_run: FunctionRun,
    timeout: float | None,
    attempt: str | None = None,
) -> dict:
    """Wait for a function run, or for one attempt of it; returns it annotated,
    with ``waited_ms``."""
    from tabsdatak.sdk.tabsdata_server import AttemptSuperseded, WaitTimeout

    subject = f"The function run '{function_run.id}'"
    resume = ["tdk", "fnrun", "get", "--fnrun", function_run.id, "--wait"]
    if attempt is not None:
        subject = f"Attempt '{attempt}' of the function run '{function_run.id}'"
        resume += ["--attempt", attempt]
    resume = _resume(ctx, resume)
    wait = budget(ctx, timeout)
    progress(ctx, f"Waiting for {subject[0].lower()}{subject[1:]} to finish...")
    started = time.monotonic()
    seen = [{"id": function_run.id}]

    def record(current) -> None:
        seen[0] = annotate(Entity.FUNCTION_RUN, current._data)

    try:
        function_run.wait(
            timeout=wait.seconds,
            attempt=attempt,
            on_update=record,
            on_outage=_outage_reporter(ctx),
        )
    except AttemptSuperseded as superseded:
        raise TdkException(
            "conflict",
            f"{subject} was superseded by the attempt '{superseded.current}'.",
            hint=(
                f"Wait for the current attempt with 'tdk fnrun get --fnrun "
                f"{function_run.id} --wait --attempt {superseded.current}'."
            ),
            state=seen[0],
        ) from None
    except WaitTimeout:
        state = _observed(
            function_run.connection,
            lambda: annotate(Entity.FUNCTION_RUN, function_run._data),
            seen[0],
        )
        status = (
            state.get("status_name")
            if attempt is None
            else state.get("physical_status")
        )
        raise _timeout(wait, subject, status or "running", resume, state)
    document = annotate(Entity.FUNCTION_RUN, function_run._data)
    if attempt is None:
        failed = _failed(function_run.status)
    else:
        failed = function_run._data.get("physical_status") in _ATTEMPT_FAILED
    if failed:
        raise TdkException(
            "run_failed",
            f"{subject} did not succeed.",
            hint=f"Diagnose it with 'tdk fnrun diagnose --fnrun {function_run.id}'.",
            state={
                **wait_summary(Entity.FUNCTION_RUN, document),
                "failed_runs": [function_run.id],
                **(
                    {}
                    if attempt is None
                    else {
                        "attempt": attempt,
                        "physical_status": document.get("physical_status"),
                    }
                ),
            },
        )
    return _summarized(
        ctx, Entity.FUNCTION_RUN, {**document, "waited_ms": _elapsed_ms(started)}
    )


def wait_listing(
    ctx: click.Context,
    server: TabsdataServer,
    items: list,
    fetch: Callable[[], list],
    entity: Entity,
    build_table: Callable[[list], Table],
    keyword: str,
    timeout: float | None,
) -> list:
    """Refetch a listing until every item in it is final; returns the last
    listing."""
    from rich.live import Live

    from tabsdatak.sdk.tabsdata_server import WaitTimeout, poll_until

    wait = budget(ctx, timeout)
    latest = [items]

    def done(current: list) -> bool:
        return all(is_final(entity, item.status) for item in current)

    def refresh() -> list:
        latest[0] = fetch()
        return latest[0]

    if done(items):
        if output_format(ctx) == "table":
            progress_console(ctx).print(build_table(items))
        return items
    progress(ctx, f"Waiting for the {keyword} to finish...")
    try:
        if output_format(ctx) == "table":
            with Live(
                build_table(items),
                refresh_per_second=1,
                console=progress_console(ctx),
            ) as display:
                items = poll_until(
                    refresh,
                    done,
                    connection=server.connection,
                    timeout=wait.seconds,
                    on_update=lambda current: display.update(build_table(current)),
                    on_outage=_outage_reporter(ctx),
                )
        else:
            items = poll_until(
                refresh,
                done,
                connection=server.connection,
                timeout=wait.seconds,
                on_outage=_outage_reporter(ctx),
            )
    except WaitTimeout:
        run = machine()
        raise _timeout(
            wait,
            f"Some of the {keyword}",
            "running",
            run.command if run is not None else None,
            _observed(
                server.connection,
                lambda: {"items": [annotate(entity, item._data) for item in latest[0]]},
                {"items": []},
            ),
        )
    progress(ctx, f"{keyword.capitalize()} finished.")
    return items


def wait_connection_probe(
    ctx: click.Context, server: TabsdataServer, probe: dict, timeout: float | None
) -> dict:
    """Wait for a connection probe; returns it, with ``waited_ms``, once it
    succeeded."""
    from tabsdatak.sdk.tabsdata_server import PROBE_FINAL_STATUSES, WaitTimeout

    probe_id = probe["probe_id"]
    named = f"{probe.get('kind', 'connection')} probe '{probe_id}'"
    subject = f"The {named}"
    argv = ["tdk", "connection", "probe", "--probe", probe_id, "--wait"]
    if server.connection.project:
        argv += ["--proj", server.connection.project]
    resume = _resume(ctx, argv)
    started = time.monotonic()
    if probe.get("status") not in PROBE_FINAL_STATUSES:
        wait = budget(ctx, timeout)
        seen = [probe]
        progress(ctx, f"Waiting for the {named} to finish...")
        try:
            probe = server.wait_connection_probe(
                probe_id,
                timeout=wait.seconds,
                on_update=lambda current: seen.__setitem__(0, current),
            )
        except WaitTimeout:
            raise _timeout(
                wait, subject, seen[0].get("status", "running"), resume, seen[0]
            )
    status = probe.get("status")
    if status != "succeeded":
        error = probe.get("error") or {}
        raise TdkException(
            "run_failed",
            f"{subject} ended {status}"
            + (f": {error['message']}" if error.get("message") else "."),
            state=probe,
        )
    return {**probe, "waited_ms": _elapsed_ms(started)}
