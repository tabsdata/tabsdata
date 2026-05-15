# T2 Function Runner Migration Plan

**Source (read-only — never modified):**
`client/platform/src/main/python/tabsdata/_tabsserver/`

**Target:**
`client/platform/src/main/python/tabsdatak/_tdrunner/function/`

**Tests:**
`client/platform/src/utest/python/test_tabsdatak/test_tdrunner/`

**Coverage requirement:** 100% line + branch on every new file under
`tabsdatak/_tdrunner/`. Existing `test_tabsdata/test_tabsserver` tests must
keep passing.

**Standalone rule:** `tabsdatak/` must never import from `tabsdata/`. When
existing logic is needed, copy the minimal symbols into `tabsdatak/` (Q1
answer: minimal cherry-picks). The same applies to test code: no imports
from `test_tabsdata/` into `test_tabsdatak/`. No code in `tabsdata/` or
`test_tabsdata/` is touched during this migration — including no deletions.

**Cutover model (Q2):** hard cutover at the end, handled by a separate
team. No bridges or compatibility shims.

**Phasing (Q3, Q10):** publisher chain end-to-end first under the local k-
harness, then transformer / subscriber / stream / cron variants.

**HTTP client (Q6):** stdlib `urllib.request` — zero new deps, sufficient
for `fetch-request` GET and `report-completion` POST with bearer token.

**Transporter binary (Q8):** `td_transporter` is in `PATH` and is a black
box for this migration.

**Local k- harness (Q9):** exists, built elsewhere. Out of scope here; we
just need to compose well with it.

The plan is split into six phases. Tasks within a phase are mostly
independent; tasks across phases follow strict dependency order. Each task
is sized to be a single PR.

---

## Progress

* **Phase 0** — ✅ done (2026-05-07). Scope was narrowed mid-flight: no
  `[project.scripts]` entries and no `fail_under = 100` coverage gate were
  added; both deferred. `test_tabsdatak/conftest.py` was refactored to
  drop all `tabsdata` / `test_tabsdata` imports.
* **Phase 1** — in progress.
  * ✅ 1.4 `errors.py` (2026-05-07) — 100% line + branch coverage.
  * ✅ 1.1 `cli.py` (2026-05-07) — 100% line + branch coverage.
  * ✅ 1.2 `liveness.py` (2026-05-07) — 100% line + branch coverage.
  * ✅ 1.3 `logging.py` (2026-05-07) — 100% line + branch coverage.
  * ✅ 1.5 `request.py` (2026-05-07) — 100% line + branch coverage.
  * ✅ 1.6 `response.py` (2026-05-07) — 100% line + branch coverage.
  * ✅ 1.7 `bundle.py` (2026-05-07) — 100% line + branch coverage.
  * ⏳ 1.8 (handed off — someone else is taking it).
  * ⏳ 1.9, 1.10, 1.11, 1.12 pending.
* **Phases 2–6** — not started.

---

## Phase 0 — Project scaffolding ✅

### 0.1 — Pyproject / packaging entries ✅

**Status:** Done (2026-05-07) — narrowed scope: `[project.scripts]`
entries and `fail_under = 100` deferred per user direction; existing
`test_tabsdatak/.coveragerc` and `test_tabsdatak/pytest.ini` accepted as-is.

**Scope:** make every command a console-script entry point and configure
dev tooling for tabsdatak.

**Code:** edits to the pyproject that ships tabsdatak:

* `[project.scripts]` entries (`fetch-request`, `prepare-py-env`,
  `localize-fn-bundle`, `prep-localize-state`, `prep-upload-state`,
  `run-transporter`, `run-source-plugin`, `run-function`,
  `run-destination-plugin`, `report-completion`, `run-trigger-plugin`).
* `[tool.coverage.run]` `branch = true`,
  `source = ["tabsdatak._tdrunner"]`.
* `[tool.coverage.report]` `fail_under = 100`, `show_missing = true`.
* `[tool.pytest.ini_options]` test paths under
  `utest/python/test_tabsdatak`.

**Tests:** smoke test that `python -m tabsdatak._tdrunner.function.commands.fetch_request`
and `fetch-request --help` both run after editable install. Lives in
`test_tabsdatak/test_tdrunner/test_packaging.py`.

**Depends on:** nothing.

**Open Q to resolve here:** confirm whether tabsdatak has its own
pyproject already, or shares the one with tabsdata.

### 0.2 — Test scaffolding ✅

**Status:** Done (2026-05-07). `test_tabsdatak/conftest.py` was refactored
under option (c) to drop all `tabsdata` and `test_tabsdata` imports
(778 → 45 lines); `test_tabsdatak/test_tdrunner/` tree created with
`tmp_mount` and `make_request_yaml` fixtures. Existing
`test_example/test_dummy.py` continues to pass.

**Scope:** mirror the `test_tabsdata` layout for the new runner.

**Code:** create `__init__.py` files at
`test_tabsdatak/test_tdrunner/`,
`test_tdrunner/test_function/`,
`test_tdrunner/test_function/test_lib/`,
`test_tdrunner/test_function/test_commands/`,
plus a top-level `conftest.py` with shared fixtures (`tmp_mount` →
`Path`, `make_request_yaml` → minimal valid request, etc.).

**Tests:** trivial — fixtures import without error. Coverage gate doesn't
apply to test code itself.

**Depends on:** 0.1.

---

## Phase 1 — Shared `_lib/`

All modules below live under `tabsdatak/_tdrunner/function/_lib/` with
mirrored tests under `test_tabsdatak/test_tdrunner/test_function/test_lib/`.
Copies from `_tabsserver` are minimal cherry-picks per Q1.

### 1.1 — `cli.py` — `--<opt> <V> | env:<VAR> | val:<V>` parser ✅

**Status:** Done (2026-05-07). 100% line + branch coverage (55 statements,
10 branches). 47 tests passing. Implementation choices:

* `_Parser` subclasses `argparse.ArgumentParser` overriding `error()` to
  raise `InvalidOptionsError` (so argparse's default `sys.exit(2)`
  becomes our exit 1).
* `--man/-m`, `--version/-v`, `--info/-i` use a custom print-and-exit
  argparse Action factory; `--help/-h` is left to argparse's default;
  `--liveness/-l PATH` is a required argument.
* `version`, `info`, `man` text passed as keyword args to `build_parser`.
* `run_command` lets `SystemExit` (from `--help` etc.) propagate while
  catching `CommandError` to return the exit code.

**Scope:** the spec heart of `01-CommandBasics.md`. Wraps argparse and
resolves arguments uniformly.

**Code:** `tabsdatak/_tdrunner/function/_lib/cli.py`

* `resolve_value(arg: str) -> str` — handles `<V>`, `env:<VAR>` (raises
  `UndefinedEnvError` mapped to exit 2), `val:<V>`.
* `resolve_flag(arg: str) -> bool` — accepts `true`/`false`
  case-insensitive.
* `build_parser(prog, description) -> argparse.ArgumentParser` —
  pre-installs `--help/-h`, `--man/-m`, `--version/-v`, `--info/-i`,
  `--liveness/-l`.
* `EXIT_INVALID_OPTIONS = 1`, `EXIT_UNDEFINED_ENV = 2`.
* `run_command(main_fn) -> int` — wraps a command's main, mapping
  `CommandError` subclasses → exit codes.

**Tests:** every branch — literal value, `env:` set/unset (exit 2),
`val:env:literal` returns `env:literal`, flag true/false case combinations
+ invalid (exit 1), common options short-circuit, missing `--liveness`
fails parse, `run_command` exit-code mapping.

**Depends on:** 0.1, 0.2, 1.4. **Every Phase-1 and Phase-2 task depends on
this.**

### 1.2 — `liveness.py` — periodic file touch ✅

**Status:** Done (2026-05-07). 100% line + branch coverage (32 statements,
6 branches). 13 tests passing. Both `start()` and `stop()` are idempotent;
the daemon thread uses `Event.wait(interval)` so `stop()` exits the loop
promptly without sleeping the full interval.

**Scope:** context manager / background-thread helper that updates the
liveness file's mtime at least once per minute while a step runs.

**Code:** `tabsdatak/_tdrunner/function/_lib/liveness.py`

* `class Liveness(path, interval=30.0)` with `start()` / `stop()` and
  context-manager protocol; daemon thread + `Event`.
* `touch(path)` for one-shot use.

**Tests:** single touch creates file and updates mtime; context manager
touches at least twice with small interval; stop cancels cleanly; missing
parent dir raises; exception inside `with` still calls `stop`.

**Depends on:** 0.2.

### 1.3 — `logging.py` — stdout/stderr split ✅

**Status:** Done (2026-05-07). 100% line + branch coverage (22 statements,
2 branches). 11 tests passing. `TRACE = 5` is registered at module import
time via `logging.addLevelName` (idempotent). `setup_logging` clears
existing handlers on the same logger name so it's safe to call twice;
`propagate = False` to keep the root logger from duplicating output.

**Scope:** per `01-CommandBasics.md`: INFO/DEBUG/TRACE → stdout;
WARN/ERROR → stderr.

**Code:** `tabsdatak/_tdrunner/function/_lib/logging.py`

* `setup_logging(name, level=logging.INFO) -> logging.Logger` — installs
  two `StreamHandler`s with level filters.
* Defines `TRACE = 5` if absent.

**Tests:** with `capsys` — INFO on stdout only, WARN on stderr only, TRACE
when explicitly enabled.

**Depends on:** 0.2.

### 1.4 — `errors.py` — exit-code-bearing exceptions ✅

**Status:** Done (2026-05-07). 100% line + branch coverage (24 statements,
4 branches). 16 tests passing. Constants live here (`EXIT_INVALID_OPTIONS`,
`EXIT_UNDEFINED_ENV`, `COMMAND_SPECIFIC_MIN_EXIT_CODE`); cli.py (1.1) will
import them. `CommandSpecificError` implemented as a class taking
`exit_code` in `__init__` (rather than a class-factory).

**Scope:** small exception hierarchy whose subclasses map to documented
exit codes (1, 2, 10+).

**Code:** `tabsdatak/_tdrunner/function/_lib/errors.py`

* `class CommandError(Exception)` with `exit_code: int`.
* `InvalidOptionsError` (1), `UndefinedEnvError` (2),
  `CommandSpecificError` factory for codes ≥ 10.

**Tests:** exit codes correct, base behaviour, custom `__str__`, factory
range check (raises for code < 10).

**Depends on:** 0.2.

### 1.5 — `request.py` — request.yaml model ✅

**Status:** Done (2026-05-07). 100% line + branch coverage (66 statements,
18 branches). 32 tests passing. Implementation choices:

* **Plain YAML, no custom tags.** The legacy tabsdata format uses
  `!FunctionV2` / `!Table` / `!Location` tags constructed via custom
  loaders. We own the new schema (resolved by the API Server), so we use
  vanilla `yaml.safe_load` — forward-compatible with the JSON-ish
  resolved output and simpler to test.
* **Lean Request.** Only fields publisher commands directly read:
  `collection`, `collection_id`, `execution_id`, `transaction_id`,
  `function_run_id`, `function_bundle`, plus `input`/`output`/
  `system_input`/`system_output`. Telemetry-only fields
  (`function_defined_on`, `execution_name`, `triggered_on`) and
  unconsumed optionals (`function_data`, `function_stage`,
  `function_version_id`) are deliberately not exposed; they can be added
  later if a command needs them.
* **No plugin descriptors.** Source/destination plugin descriptors live
  in the bundle, not the request — confirmed by reading
  `_tabsserver/function/execution_context.py`. The migration plan's
  earlier mention of "plugin descriptors" in `Request` was speculative;
  implementation reflects what's actually needed.
* **Frozen dataclasses + tuples** for value-like immutability of
  `Request`, `Location`, `Table`.
* **Errors:** `InvalidRequestError(ValueError)` for malformed YAML or
  schema violations; `FileNotFoundError` propagates as-is. Neither is a
  `CommandError` subclass — each consuming command picks its own
  command-specific exit code (≥10) when wrapping.
* **Eager validation** with dotted-path error messages (e.g.
  `"info.function_bundle.uri"`, `"output[0].location"`).
* **Test fixture** lives at
  `test_lib/resources/minimal_publisher_request.yaml`; error-case YAMLs
  are inline in the test file.

**Scope:** cherry-pick the publisher-shaped subset of
`_tabsserver/function/yaml_parsing.py::parse_request_yaml` and
`FunctionYaml`. Refactor as plain dataclasses exposing only the fields
publisher commands actually read.

**Code:** `tabsdatak/_tdrunner/function/_lib/request.py`

* `load_request(path) -> Request`.
* `Request` dataclass — bundle URI, requirements info, source/destination
  plugin descriptors, output table specs, state in/out specs.

**Tests:** load minimal sample fixture; missing file, malformed YAML,
missing required fields each raise expected errors. Fixture YAMLs live
under `test_tabsdatak/test_tdrunner/test_function/test_lib/resources/`.

**Depends on:** 1.4.

### 1.6 — `response.py` — response.yaml writer ✅

**Status:** Done (2026-05-07). 100% line + branch coverage (68 statements,
20 branches). 26 tests passing. Implementation choices:

* **Plain YAML, no custom tags** — same precedent as 1.5. The legacy
  format uses `!FunctionV2` / `!Data` / `!NoData` tags; we drop them.
* **Single `TableEntry` dataclass with `info: TableInfo | None`** rather
  than separate `Data` / `NoData` types. Presence/absence of `info`
  discriminates.
* **`load_response` added** alongside `write_response` even though the
  plan only listed the writer — the loader is needed for clean roundtrip
  tests and for symmetry with `request.py`.
* **`InvalidResponseError(ValueError)`** mirrors `InvalidRequestError`.
* **Frozen dataclasses + tuples** for immutability.

**Scope:** cherry-pick from `_tabsserver/function/response_utils.py`.

**Code:** `tabsdatak/_tdrunner/function/_lib/response.py`

* `Response` dataclass.
* `write_response(resp, path)`.

**Tests:** roundtrip, schema validation, write to non-writable path
raises.

**Depends on:** 1.4.

### 1.7 — `bundle.py` — tar extraction ✅

**Status:** Done (2026-05-07). 100% line+branch coverage (7 statements, 0
branches). 8 tests passing. Implementation choices:

* Single `extract_bundle(archive, dest_dir)` function — much simpler than
  the legacy `extract_bundle_folder` because paths arrive as args (per
  the runner's "paths are parameters, never deduced" rule). No temp-dir
  orchestration.
* Uses `tarfile.open(archive, "r:gz")` with `filter="data"` (Python 3.12+
  strict filter). Drops the legacy `TD_SYMLINK_POLARS_LIBS_PYTEST`
  fallback to the looser `tar` filter — that was a tabsdata-test-internal
  knob.
* `dest_dir.mkdir(parents=True, exist_ok=True)` so callers don't need to
  pre-create.

**Scope:** cherry-pick `extract_bundle_folder` from
`_tabsserver/utils.py`. Pure stdlib.

**Code:** `tabsdatak/_tdrunner/function/_lib/bundle.py`

* `extract_bundle(archive, dest_dir)` using `tarfile.open(...)` with the
  `data` filter.

**Tests:** extract a known good tar; reject a tar with traversal payload;
corrupt file raises.

**Depends on:** 1.4.

### 1.8 — `pyenv.py` — virtualenv creation

**Scope:** cherry-pick `create_virtual_environment` and
`get_path_to_environment_bin` from `_tabsserver/pyenv_creation.py`. Drop
tabsdata-specific paths.

**Code:** `tabsdatak/_tdrunner/function/_lib/pyenv.py`

* `create_virtual_environment(requirements, dest_dir) -> VirtualEnv`
  returning a dataclass with `python_bin`, `venv_dir`,
  `requirements_digest`.

**Tests:** integration with `tmp_path` and a tiny requirements file;
verify `python_bin` exists and runs; mock pip/uv invocation for unit-level
branches (success, install failure, bad requirements).

**Depends on:** 1.4. **Heaviest task in Phase 1 — establishes subprocess
mocking patterns reused later.**

### 1.9 — `run_env.py` — run-env.yaml model

**Scope:** define the schema for the run-env artifact (Q7).

**Proposed schema:**

```yaml
python_bin: /path/to/venv/bin/python
venv_dir: /path/to/venv
requirements_digest: sha256:abcd...
extra_env:
  PYTHONPATH: ""
```

**Code:** `tabsdatak/_tdrunner/function/_lib/run_env.py` — `RunEnv`
dataclass + `load_run_env(path)` / `write_run_env(env, path)`.

**Tests:** roundtrip; missing required fields error; `extra_env` defaults
to `{}` when absent.

**Depends on:** 1.4.

**Stop-and-ask before starting:** confirm the schema with the user — easy
to extend later but consumers (1.10, 3.x) want a stable shape.

### 1.10 — `transporter_request.py` — transporter request YAML builder

**Scope:** cherry-pick the V1CopyFormat / V1ImportFormat /
V1WildCardCopyFormat dataclasses + builder logic from
`_tabsserver/function/native_tables_utils.py`. Two builder functions for
publisher use.

**Code:** `tabsdatak/_tdrunner/function/_lib/transporter_request.py`

* `build_localize_state_request(req, in_dir, state_dir) -> dict`
* `build_upload_state_request(req, state_dir) -> dict`

**Tests:** golden YAML compare against fixtures; missing optional fields
produce minimal output; wildcard handling.

**Depends on:** 1.5.

### 1.11 — `transporter_proc.py` — subprocess wrapper for `td_transporter`

**Scope:** per Q8 the binary is in PATH and a black box. This wrapper just
exec's it.

**Code:** `tabsdatak/_tdrunner/function/_lib/transporter_proc.py`

* `run_transporter(request_yaml, liveness) -> int` →
  `subprocess.run(["td_transporter", "--request", str(request_yaml)])`
  with stdout/stderr forwarded.

**Tests:** monkeypatch `subprocess.run`; verify command shape; non-zero
exit propagated; missing binary raises a typed error.

**Depends on:** 1.2.

### 1.12 — `plugin_loader.py` — load source/destination plugins

**Scope:** cherry-pick the cloudpickle-load logic from
`_tabsserver/function/execution_context.py` (the `source` and
`destination` properties). Refactor as plain functions. The plugin types
themselves are tabsdata-defined — copy the **minimal** Protocol/ABC for
`_run` so we don't import tabsdata.

**Code:** `tabsdatak/_tdrunner/function/_lib/plugin_loader.py` plus
`tabsdatak/_tdrunner/function/_lib/plugin_protocol.py`.

**Tests:** load a fake cloudpickled plugin from `tmp_path`; missing
plugin → typed error; protocol mismatch → typed error.

**Depends on:** 1.5, 1.7.

**Note:** highest risk of accidental tabsdata imports — explicit care
needed.

---

## Phase 2 — Pure-disk leaf commands

Each command is a thin `main()` in
`tabsdatak/_tdrunner/function/commands/<name>/__init__.py`. Tests live at
`test_tabsdatak/test_tdrunner/test_function/test_commands/test_<name>.py`.
Every command:

* builds its parser via `_lib.cli.build_parser`,
* starts a `_lib.liveness.Liveness`,
* maps `CommandError` subclasses to exit codes via
  `_lib.cli.run_command`,
* logs via `_lib.logging`.

### 2.1 — `fetch-request`

**Scope:** HTTP GET `--url` with `Authorization: Bearer <token>`, write
body to `--out`.

**Code:** `tabsdatak/_tdrunner/function/commands/fetch_request/__init__.py`

* Args: `--url`, `--token`, `--out`, `--liveness`.
* Uses `urllib.request` with bearer header.

**Tests:** spin up a tiny `http.server` fixture; verify GET headers and
file contents; non-2xx → typed error → command-specific exit; network
error → typed error.

**Depends on:** 1.1, 1.2, 1.3, 1.4.

### 2.2 — `prepare-py-env`

**Scope:** read `--request`, locate requirements, call
`_lib.pyenv.create_virtual_environment`, write `--out` as run-env.yaml.

**Code:** `tabsdatak/_tdrunner/function/commands/prepare_py_env/__init__.py`

* Args: `--request`, `--out`, `--liveness`.

**Tests:** integration with a small requirements fixture (one tiny
pure-python dep, or empty); failure path when pyenv creation fails.

**Depends on:** 1.1–1.5, 1.8, 1.9. **Schema confirmation (1.9) must
happen first.**

### 2.3 — `localize-fn-bundle`

**Scope:** read bundle URI from `--request`, download via storage
credentials in env, extract to `--dir`.

**Code:** `tabsdatak/_tdrunner/function/commands/localize_fn_bundle/__init__.py`

* Args: `--request`, `--run-env`, `--dir`, `--liveness`.

**Stop-and-ask before starting:** how does the bundle get fetched? Three
options:

* (a) shell out to `td_transporter` (consistent with state/output
  transport);
* (b) implement a small stdlib S3/GCS/Azure client in tabsdatak;
* (c) the bundle URI is always a presigned HTTPS URL — just stdlib HTTP.

**Tests:** mock the fetcher; verify call shape; extract a known tar;
verify dest contents.

**Depends on:** 1.1–1.4, 1.7, 1.9 plus the open Q above.

### 2.4 — `prep-localize-state`

**Scope:** build the transporter request YAML for state-in localisation.

**Code:** `tabsdatak/_tdrunner/function/commands/prep_localize_state/__init__.py`

* Args: `--request`, `--in-dir`, `--state-dir`, `--out`, `--liveness`.

**Tests:** sample requests with and without initial-value state; output
YAML matches golden file.

**Depends on:** 1.1–1.5, 1.10.

### 2.5 — `prep-upload-state`

**Scope:** build the transporter request YAML for state-out upload.

**Code:** `tabsdatak/_tdrunner/function/commands/prep_upload_state/__init__.py`

* Args: `--request`, `--state-dir`, `--out`, `--liveness`.

**Tests:** empty `state_dir` yields no-op; with state files, builds
correct copy entries.

**Depends on:** 1.1–1.5, 1.10.

### 2.6 — `run-transporter`

**Scope:** pure wrapper — exec `td_transporter --request <PATH>` and
propagate exit code, while running a Liveness.

**Code:** `tabsdatak/_tdrunner/function/commands/run_transporter/__init__.py`

* Args: `--request`, `--liveness`.

**Tests:** monkeypatch `subprocess.run`; verify args; non-zero exit
propagated; binary missing → mapped exit code.

**Depends on:** 1.1–1.4, 1.11.

### 2.7 — `report-completion`

**Scope:** read `--request`, scan `--out-dir` and `--state-dir`, build
response, POST to `--url` with bearer token, write a copy to `--out`.

**Code:** `tabsdatak/_tdrunner/function/commands/report_completion/__init__.py`

* Args: `--url`, `--token`, `--request`, `--out-dir`, `--state-dir`,
  `--out`, `--liveness`.

**Tests:** local `http.server` fixture; verify POST body matches expected
response shape; non-2xx response error path; verifies `--out` written even
on POST failure.

**Depends on:** 1.1–1.6.

---

## Phase 3 — User-code-loading commands

These three actually invoke the user's bundled code, crossing from "Python
runtime that came with the container" into "Python runtime built by
prepare-py-env".

### 3.1 — `run-source-plugin`

**Scope:** activate the run-env (locate the venv's Python from
`--run-env`), spawn a subprocess of that Python that loads the source
plugin from the bundle and calls `_run(...)`. Output parquet lands in
`--out-dir`.

**Code:**
`tabsdatak/_tdrunner/function/commands/run_source_plugin/__init__.py`

* Args: `--request`, `--run-env`, `--bundle`, `--out-dir`, `--liveness`.
* Helper: `tabsdatak/_tdrunner/function/_lib/subproc_pyenv.py` for
  invoking the venv Python (promote to Phase 1 if more than one Phase-3
  task uses it; otherwise inline).

**Tests:** end-to-end with a fake cloudpickled source plugin that writes
known parquet; verify out-dir contents; failure inside plugin propagates
as command-specific exit code.

**Depends on:** 1.1–1.5, 1.7, 1.9, 1.12.

### 3.2 — `run-function`

**Scope:** load the user's decorated function from the bundle, invoke it
with input parquet from `--in-dir` and state from `--state-in`, write
outputs to `--out-dir` and state to `--state-out`. Heaviest single
command — most of `_tabsserver/function/execute_function_from_bundle_path.py`
and `execution_utils.py` is the source material.

**Code:** `tabsdatak/_tdrunner/function/commands/run_function/__init__.py`

* Args: `--request`, `--run-env`, `--bundle`, `--in-dir`, `--state-in`,
  `--out-dir`, `--state-out`, `--liveness`.

**Tests:** integration with a tiny fixture-built bundle containing an
identity transformer that echoes inputs to outputs and writes a state
file; assert all artefacts; user-function exception path; missing input
dir.

This task is large enough to want sub-tasks. Plan to split into 3.2a / b /
c (parameter binding, function invocation, results write-back) when
reaching it rather than land it as one PR.

**Depends on:** 1.1–1.9, 1.12.

### 3.3 — `run-destination-plugin`

**Scope:** mirror of 3.1 but for the destination plugin — reads
`--out-dir`, calls `dest_plugin._run(results)`.

**Code:**
`tabsdatak/_tdrunner/function/commands/run_destination_plugin/__init__.py`

* Args: `--request`, `--run-env`, `--bundle`, `--out-dir`, `--liveness`.

**Tests:** end-to-end with a fake destination plugin that records calls;
verify call signature; plugin exception path.

**Depends on:** 1.1–1.5, 1.7, 1.9, 1.12.

---

## Phase 4 — End-to-end publisher chain

### 4.1 — Publisher chain integration test under k-

**Scope:** wire up an end-to-end test that runs the 11 publisher
containers in order against a known fixture bundle and asserts the full
produced response.yaml + uploaded state + plugin-emitted output. k- harness
exists already (Q9) — we just need to compose well with it.

**Code:** test only — no production code (unless k- needs an adapter).

**Tests:** `test_tabsdatak/test_tdrunner/test_e2e/test_publisher_chain.py`.
A single `@pytest.mark.slow` test that:

1. seeds a tmp `mount/td/` skeleton + `request.yaml` + a tiny prebuilt
   bundle;
2. invokes each command's `main()` in sequence (or invokes k-);
3. asserts the final response.yaml shape, output parquet contents, and
   state-out contents.

**Depends on:** all Phase-2 + Phase-3 tasks.

### 4.2 — Per-command CLI smoke matrix

**Scope:** parametrised pytest matrix that runs `<command> --help`,
`--man`, `--version`, `--info` for every command and asserts exit 0 +
non-empty stdout. Catches regressions in common-options handling.

**Tests:**
`test_tabsdatak/test_tdrunner/test_function/test_commands/test_common_options.py`.

**Depends on:** all Phase-2 + Phase-3 tasks.

---

## Phase 5 — Other flow types

Each flow type reuses the publisher commands; only the chain composition
differs. New commands appear only where the flow needs work the publisher
doesn't.

### 5.1 — Transformer chain

**Scope:** transformer chain is publisher minus `run-source-plugin` and
minus `run-destination-plugin` — but the current `00-FunctionRunner.md`
transformer step 6 still says "Upload the function output tables and
state to storage" (pre-T2 wording). Aligning that wording is the
sub-task. May also need a `prep-upload-output` (output tables + state)
variant if transformers don't use destination plugins.

**Code:** possibly a new `prep_upload_output/` command.

**Tests:** e2e for transformer in the same shape as 4.1.

**Stop-and-ask before starting:** does T2 transformer also gain a
destination plugin, or does it still upload via transporter only?

### 5.2 — Subscriber chain

**Scope:** per the doc edit done earlier, subscriber step 6 should be
"destination plugin", making subscriber's chain identical to publisher's
minus `run-source-plugin`. May need a fix to `00-FunctionRunner.md` when
the user signs off on it.

**Tests:** e2e for subscriber.

**Depends on:** Phase-2 + Phase-3 done; 5.1 resolved.

### 5.3 — Stream chain

**Scope:** five-step chain. Adds one new command:
`run-trigger-plugin` (already a stub). Otherwise reuses fetch-request,
prepare-py-env, localize-fn-bundle, report-completion.

**Code:** flesh out
`tabsdatak/_tdrunner/function/commands/run_trigger_plugin/__init__.py`.
Lift from `_tabsserver`'s stage-trigger path (`StageTriggerYaml` branch in
`execute_function_from_bundle_path.py`).

**Tests:** e2e for stream + unit for `run-trigger-plugin`.

**Depends on:** Phase-2 done; 1.12 extended for trigger plugins.

### 5.4 — Cron chain

**Scope:** a single command that POSTs to the API server to invoke a
function trigger. Probably fits under a new `trigger_function/` command.

**Stop-and-ask before starting:** confirm naming + whether it lives under
`commands/` at all (it isn't really a runner step).

---

## Phase 6 — Hand-off

### 6.1 — `tabsdatak` standalone gate

**Scope:** verify nothing under `tabsdatak/_tdrunner/` imports from
`tabsdata`. CI-enforced.

**Code:**
`test_tabsdatak/test_tdrunner/test_no_tabsdata_imports.py` — walks
`_tdrunner/` and asserts every `import` and `from` line resolves outside
`tabsdata`.

**Depends on:** all earlier phases done.

### 6.2 — Hand-off doc

**Scope:** a short MIGRATION_NOTES under
`client/platform/src/main/docs/` summarising what `_tabsserver` work
`_tdrunner` now subsumes, so the team doing the production cutover (out
of scope here) has a clear map. We do not delete `_tabsserver` ourselves.

**Depends on:** Phase 5.

---

## Cross-cutting test discipline

For every task above:

1. **Branch coverage gate is enforced**, not just line. CI runs
   `pytest --cov --cov-branch --cov-fail-under=100` against
   `tabsdatak._tdrunner`.
2. **Fixture bundles** built in
   `test_tabsdatak/test_tdrunner/conftest.py` (a `bundle_archive` factory
   similar to the one in `test_tabsdata`, but freshly written — no
   imports across).
3. **No imports from `tabsdata` or `test_tabsdata`** anywhere — verified
   by 6.1.
4. **HTTP fixtures** use stdlib `http.server` in a thread (no extra
   deps).
5. **Subprocess fixtures** monkeypatch `subprocess.run` for unit tests;
   full subprocess invocation only in Phase 4 e2e.

---

## Estimated sequencing

```
0.1 → 0.2 → 1.4 → 1.1 ─┬─ 1.2, 1.3                 (parallel)
                       ├─ 1.5 → 1.6, 1.10          (parallel)
                       ├─ 1.7 → 1.12
                       ├─ 1.8 → 1.9
                       └─ 1.11

[Phase 1 done]

Phase 2: 2.1, 2.4, 2.5, 2.6, 2.7 in parallel.
         2.2 needs 1.9 schema confirmed.
         2.3 needs bundle-fetch Q resolved.

[Phase 2 done]

Phase 3: 3.1 and 3.3 in parallel; 3.2 split into sub-PRs.

[Phase 3 done]

4.1, 4.2 in parallel.

5.1, 5.2, 5.3, 5.4 — each with its own Q resolution.

6.1, 6.2.
```

---

## Stop-and-ask checkpoints

These are surfaced here so they're not forgotten when reaching them:

* **Before 1.9** — confirm the run-env.yaml schema sketch.
* **Before 2.3** — confirm how `localize-fn-bundle` actually fetches the
  bundle (transporter? presigned URL? storage SDK?).
* **Before 5.1** — confirm transformer's upload model (transporter only
  vs. destination plugin).
* **Before 5.4** — confirm cron's `trigger-function` placement and name.
