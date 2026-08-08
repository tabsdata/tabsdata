<!--
Copyright 2026 Tabsdata Inc.
-->

# T2 Function Runner Migration Plan

**Source (read-only — never modified):**
`client/platform/src/main/python/tabsdata/_tabsserver/`

**Target:**
`connectors/python/tabsdata-server/src/main/python/tabsdatak/server/_tdrunner/function/`

**Tests:**
`connectors/python/tabsdata-server/src/utest/python/test_tabsdatak/server/test_tdrunner/`

**Coverage requirement:** 100% line + branch on every new file under
`tabsdatak/server/_tdrunner/`. Existing `test_tabsdata/test_tabsserver` tests must
keep passing.

**Standalone rule (revised 2026-05-08):** `tabsdatak/` must never import
from `tabsdata/` *or* `tabsdataai/` — both are legacy and slated for
deletion. `tabsdatak` is the *destination* of the migration, not just
one piece of it: by the end, `tabsdatak` is the only code that survives
and will be renamed to `tabsdata`. When logic from either legacy package
is needed, **copy the source code into `tabsdatak`** and adapt — never
import, never invoke as a subprocess, never call across the boundary.
The same applies to test code: no imports from `test_tabsdata/` into
`test_tabsdatak/`. No code in `tabsdata/`, `tabsdataai/`, or
`test_tabsdata/` is touched during this migration — including no
deletions.

**Cutover model (Q2):** hard cutover at the end, handled by a separate
team. No bridges or compatibility shims.

**Pod-runner / venv split (2026-05-08):** `prepare-py-env` (§2 in
`02-PublisherInitContainers.md`) is the *only* command that produces or
reads `run-env.yaml`. The pod-runner consumes `run-env.yaml` after §2
and, for every subsequent container, invokes the command in a process
where the venv is **already active**. The venv always has `tabsdatak`
installed plus the function's resolved deps. Therefore commands from §3
onwards do not take `--run-env`, do not parse run-env.yaml, and do not
spawn a subprocess of the venv themselves — they just run in whatever
Python the pod-runner gave them.

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
  * ✅ 1.9 `run_env.py` (2026-05-07) — 100% line + branch coverage.
  * ✅ 1.10 `transporter_request.py` (2026-05-07) — 100% line + branch coverage.
  * ✅ 1.11 `transporter_proc.py` (2026-05-07) — 100% line + branch coverage.
  * ⚠️ 1.12 `plugin_loader.py` + `plugin_protocol.py` (2026-05-07) — 100%
    coverage, but **superseded** by the new framework copy in Phase 1B
    (cloudpickle plugin discovery is replaced by JSON-based definition
    deserialisation + class registry; `_run` becomes `read_in`/`write_out`).
    Files still present, will be replaced when 1B.6 lands.
* **Phase 1B** — new phase added 2026-05-08 (SDK framework copy from
  `tabsdataai`/`tabsdata` into `tabsdatak`). Not started.
* **Phase 2** — ✅ done (six of seven; 2.2 deferred until 1.8 lands).
  * ✅ 2.1 `fetch-request` (2026-05-07) — TD-1766.
  * ⏳ 2.2 `prepare-py-env` — depends on 1.8.
  * ✅ 2.3 `localize-fn-bundle` (2026-05-07) — TD-1767. **Amended
    2026-05-08 (TD-1777)** to drop `--run-env` per the pod-runner split.
  * ✅ 2.4 `prep-localize-state` (2026-05-07) — TD-1768.
  * ✅ 2.5 `prep-upload-state` (2026-05-07) — TD-1769.
  * ✅ 2.6 `run-transporter` (2026-05-07) — TD-1770.
  * ✅ 2.7 `report-completion` (2026-05-07) — TD-1771.
* **Doc & infrastructure follow-ups (2026-05-08):**
  * ✅ TD-1772 — platform-aware `td_transporter` binary name.
  * ✅ TD-1776 — drop `--run-env` from `02-PublisherInitContainers.md` for
    every step except §2.
  * ✅ TD-1777 — drop `--run-env` from `localize-fn-bundle` (2.3
    amendment).
* **Phase 3** — redesigned 2026-05-08 (in-process, no subprocess, no
  `--run-env`). Gated by Phase 1B + 1.8.
* **Phases 4–6** — not started.

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
  `source = ["tabsdatak.server._tdrunner"]`.
* `[tool.coverage.report]` `fail_under = 100`, `show_missing = true`.
* `[tool.pytest.ini_options]` test paths under
  `utest/python/test_tabsdatak`.

**Tests:** smoke test that `python -m tabsdatak.server._tdrunner.function.commands.fetch_request`
and `fetch-request --help` both run after editable install. Lives in
`test_tabsdatak/server/test_tdrunner/test_packaging.py`.

**Depends on:** nothing.

**Open Q to resolve here:** confirm whether tabsdatak has its own
pyproject already, or shares the one with tabsdata.

### 0.2 — Test scaffolding ✅

**Status:** Done (2026-05-07). `test_tabsdatak/conftest.py` was refactored
under option (c) to drop all `tabsdata` and `test_tabsdata` imports
(778 → 45 lines); `test_tabsdatak/server/test_tdrunner/` tree created with
`tmp_mount` and `make_request_yaml` fixtures. Existing
`test_example/test_dummy.py` continues to pass.

**Scope:** mirror the `test_tabsdata` layout for the new runner.

**Code:** create `__init__.py` files at
`test_tabsdatak/server/test_tdrunner/`,
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

All modules below live under `tabsdatak/server/_tdrunner/function/_lib/` with
mirrored tests under `test_tabsdatak/server/test_tdrunner/test_function/test_lib/`.
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

**Code:** `tabsdatak/server/_tdrunner/function/_lib/command.py`

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

**Code:** `tabsdatak/server/_tdrunner/function/_lib/liveness.py`

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

**Code:** `tabsdatak/server/_tdrunner/function/_lib/logging.py`

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

**Code:** `tabsdatak/server/_tdrunner/function/_lib/errors.py`

* `class CommandError(Exception)` with `exit_code: int`.
* `InvalidOptionsError` (1), `UndefinedEnvError` (2),
  `CommandSpecificError` factory for codes ≥ 10.

**Tests:** exit codes correct, base behaviour, custom `__str__`, factory
range check (raises for code < 10).

**Depends on:** 0.2.

### 1.5 — `request.py` — request.yaml model ✅ ⚠ amendment pending

**Pending amendment (2026-05-08):** the `Request` schema needs to grow a
`function_def` field carrying the JSON envelope of the
`PublisherInfo` / `TransformerInfo` / `SubscriberInfo` produced by the
new tabsdataai-style serialisation (replacing cloudpickle). Schedule
this after Phase 1B.4 (info classes) so the field can be typed against
the copied-into-tabsdatak versions of those classes. New ticket; the
existing implementation note below remains valid up to that point.



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

**Code:** `tabsdatak/server/_tdrunner/function/_lib/request.py`

* `load_request(path) -> Request`.
* `Request` dataclass — bundle URI, requirements info, source/destination
  plugin descriptors, output table specs, state in/out specs.

**Tests:** load minimal sample fixture; missing file, malformed YAML,
missing required fields each raise expected errors. Fixture YAMLs live
under `test_tabsdatak/server/test_tdrunner/test_function/test_lib/resources/`.

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

**Code:** `tabsdatak/server/_tdrunner/function/_lib/response.py`

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

**Code:** `tabsdatak/server/_tdrunner/function/_lib/bundle.py`

* `extract_bundle(archive, dest_dir)` using `tarfile.open(...)` with the
  `data` filter.

**Tests:** extract a known good tar; reject a tar with traversal payload;
corrupt file raises.

**Depends on:** 1.4.

### 1.8 — `pyenv.py` — virtualenv creation

**Scope:** cherry-pick `create_virtual_environment` and
`get_path_to_environment_bin` from `_tabsserver/pyenv_creation.py`. Drop
tabsdata-specific paths.

**Code:** `tabsdatak/server/_tdrunner/function/_lib/pyenv.py`

* `create_virtual_environment(requirements, dest_dir) -> VirtualEnv`
  returning a dataclass with `python_bin`, `venv_dir`,
  `requirements_digest`.

**Tests:** integration with `tmp_path` and a tiny requirements file;
verify `python_bin` exists and runs; mock pip/uv invocation for unit-level
branches (success, install failure, bad requirements).

**Depends on:** 1.4. **Heaviest task in Phase 1 — establishes subprocess
mocking patterns reused later.**

### 1.9 — `run_env.py` — run-env.yaml model ✅

**Status:** Done (2026-05-07). 100% line+branch coverage (51 statements,
18 branches). 22 tests passing. Schema confirmed by user:
`python_bin`, `venv_dir`, `requirements_digest` (required, must start
with `"sha256:"`), `extra_env` (optional `dict[str, str]`, defaults to
empty when omitted or null). Plain YAML, no custom tags. Matching
`InvalidRunEnvError(ValueError)` and validation patterns from
`request.py` / `response.py`.

**Scope:** define the schema for the run-env artifact (Q7).

**Proposed schema:**

```yaml
python_bin: /path/to/venv/bin/python
venv_dir: /path/to/venv
requirements_digest: sha256:abcd...
extra_env:
  PYTHONPATH: ""
```

**Code:** `tabsdatak/server/_tdrunner/function/_lib/run_env.py` — `RunEnv`
dataclass + `load_run_env(path)` / `write_run_env(env, path)`.

**Tests:** roundtrip; missing required fields error; `extra_env` defaults
to `{}` when absent.

**Depends on:** 1.4.

**Stop-and-ask before starting:** confirm the schema with the user — easy
to extend later but consumers (1.10, 3.x) want a stable shape.

### 1.10 — `transporter_request.py` — transporter request YAML builder ✅

**Status:** Done (2026-05-07). 100% line+branch coverage (110 statements,
22 branches). 26 tests passing. Implementation choices:

* **Custom YAML tags preserved.** Unlike request.py / response.py /
  run_env.py (where we own the schema), the transporter binary's request
  format is *its* contract — it expects `!CopyV1`, `!S3`, `!Azure`,
  `!GCS`, `!LocalFile`, `!Env`, `!Literal`. Mirrored exactly via
  PyYAML `add_representer`.
* **Endpoint dataclasses:** `S3Endpoint`, `AzureEndpoint`, `GCSEndpoint`,
  `LocalFileEndpoint` — frozen, fields match the legacy
  `TransporterS3` / etc. shape (`url` + `configs.{access_key, secret_key,
  region, extra_configs}` etc).
* **Credentials always env-driven** — `EnvRef` for the publisher path
  per Q8 (storage creds come from k8s secret env injection). `LiteralRef`
  is supported in the dumper for forward use but not produced by the
  builders today.
* **Env-prefix convention:** when `Location.env_prefix` is set on a
  request location, the env var name for each credential is
  `<env_prefix>_<CRED>` (uppercase underscore separator); otherwise the
  canonical name (`AWS_ACCESS_KEY_ID`, `AZURE_ACCOUNT_NAME`,
  `GCP_SERVICE_ACCOUNT_KEY`, etc.).
* **Two builders** (per plan): `build_localize_state_request(req, in_dir,
  state_dir)` walks `request.input` then `request.system_input` and pairs
  each with a `LocalFileEndpoint` under the appropriate dir;
  `build_upload_state_request(req, state_dir)` walks `request.system_output`
  and pairs each local source with the cloud target.
* **Skipped from scope** (not needed by the publisher path; can be added
  later for transformer/subscriber): `V1WildCardCopyFormat`,
  `V1ImportFormat`, the format classes (`TransporterCSVFormat`, etc.).
* **`UnsupportedSchemeError`** for URI schemes outside `s3://`, `az://`,
  `abfs://`, `abfss://`, `gs://`, `file://`.

**Scope:** cherry-pick the V1CopyFormat / V1ImportFormat /
V1WildCardCopyFormat dataclasses + builder logic from
`_tabsserver/function/native_tables_utils.py`. Two builder functions for
publisher use.

**Code:** `tabsdatak/server/_tdrunner/function/_lib/transporter_request.py`

* `build_localize_state_request(req, in_dir, state_dir) -> dict`
* `build_upload_state_request(req, state_dir) -> dict`

**Tests:** golden YAML compare against fixtures; missing optional fields
produce minimal output; wildcard handling.

**Depends on:** 1.5.

### 1.11 — `transporter_proc.py` — subprocess wrapper for `td_transporter` ✅

**Status:** Done (2026-05-07). 100% line+branch coverage (23 statements,
4 branches). 15 tests passing. `run_transporter(request_yaml, liveness=None)`
exec's `td_transporter --request <PATH>` with stdout/stderr inherited and
returns the exit code (caller decides what to do with non-zero). When a
liveness context manager is provided, it wraps the subprocess so the
background touch is started/stopped — including on exception.
`TransporterNotFoundError(FileNotFoundError)` for the missing-binary case.

**Platform-aware binary name (2026-05-07 follow-up):** the constant
`TRANSPORTER_BINARY` is now derived from a `_binary_name()` helper that
returns `"td_transporter.exe"` on Windows (`sys.platform == "win32"`)
and `"td_transporter"` elsewhere. Wrapping in a function (instead of an
inline conditional at module level) is what lets us reach 100% branch
coverage on a single platform via `monkeypatch.setattr(sys, "platform",
...)`.

**Scope:** per Q8 the binary is in PATH and a black box. This wrapper just
exec's it.

**Code:** `tabsdatak/server/_tdrunner/function/_lib/transporter_proc.py`

* `run_transporter(request_yaml, liveness) -> int` →
  `subprocess.run(["td_transporter", "--request", str(request_yaml)])`
  with stdout/stderr forwarded.

**Tests:** monkeypatch `subprocess.run`; verify command shape; non-zero
exit propagated; missing binary raises a typed error.

**Depends on:** 1.2.

### 1.12 — `plugin_loader.py` — load source/destination plugins ✅ ⚠ superseded

**Superseded (2026-05-08):** The cloudpickle `<bundle>/plugins/source.pickle`
model is gone in tabsdataai. Plugins are now class references discovered
via a registry (Phase 1B.6), and the protocol shape is
`read_in` / `write_out` (Phase 1B.3), not `_run`. The shipped
`plugin_loader.py` and `plugin_protocol.py` modules will be replaced by
new modules from Phase 1B that wrap the new mechanism. Files remain in
place until Phase 1B.6 ships its replacement (e.g. `plugin_resolver.py`).



**Status:** Done (2026-05-07). 100% line+branch coverage (27 statements,
4 branches, across `plugin_loader.py` + `plugin_protocol.py`). 11 tests
passing. Implementation choices:

* **Fixed-name convention** for plugin file paths — `<bundle>/plugins/source.pickle`
  and `<bundle>/plugins/destination.pickle`. The legacy
  `_tabsserver/function/execution_context.py` reads the filename from a
  `function_config.input/output[<IDENTIFIER>]` mapping; our `Request`
  doesn't carry plugin metadata and we don't import tabsdata config, so
  fixed names are the simplest contract. Bundle producer is responsible
  for placing files at these paths.
* **`runtime_checkable Protocol`** for `SourcePlugin` and
  `DestinationPlugin` — both require a callable `_run`. Lets
  `isinstance(loaded, SourcePlugin)` enforce the contract without
  importing tabsdata's actual plugin classes.
* **Two error types:** `PluginNotFoundError(FileNotFoundError)` for
  missing pickle files, `PluginProtocolError(TypeError)` for
  cloudpickle-loaded objects that don't have `_run`. Both inherit from
  the matching stdlib exceptions so callers can catch broadly if they
  prefer.
* **`cloudpickle`** is the only third-party dep — already on PATH from
  the venv.

**Scope:** cherry-pick the cloudpickle-load logic from
`_tabsserver/function/execution_context.py` (the `source` and
`destination` properties). Refactor as plain functions. The plugin types
themselves are tabsdata-defined — copy the **minimal** Protocol/ABC for
`_run` so we don't import tabsdata.

**Code:** `tabsdatak/server/_tdrunner/function/_lib/plugin_loader.py` plus
`tabsdatak/server/_tdrunner/function/_lib/plugin_protocol.py`.

**Tests:** load a fake cloudpickled plugin from `tmp_path`; missing
plugin → typed error; protocol mismatch → typed error.

**Depends on:** 1.5, 1.7.

**Note:** highest risk of accidental tabsdata imports — explicit care
needed.

---

## Phase 1B — SDK framework copy (added 2026-05-08)

`tabsdataai` defines the new framework shape (decorators, plugin
protocols, JSON envelope, registry). Per the standalone rule, we can't
import any of it; we copy the minimum subset needed for an end-to-end
publisher prototype, then iterate.

**Cherry-pick scope for this iteration:** the publisher path. Transformer,
subscriber, auto-publisher, and per-vendor connectors get later
iterations.

**TableFrame decision (2026-05-08):** Phase 3.2 uses `polars.LazyFrame`
as the stub for TableFrame. Migrating to a real TableFrame copy comes
in a later iteration of Phase 1B (call it **1B.8**) once the publisher
prototype works end-to-end with the stub.

**Bundle layout decision (2026-05-08):** the bundle is a **package tree
plus JSON manifest**:

```
<bundle>/
├── manifest.json            # the JSON envelope (PublisherInfo /
│                            # TransformerInfo / SubscriberInfo)
└── <user package tree>/     # importable Python source the user wrote
    ├── __init__.py
    └── ...
```

The `manifest.json` is what `info.func` (a `module:qualname` string)
resolves against — the package tree must be importable from the venv's
Python (the pod-runner adds `<bundle>/` to `sys.path`, or installs the
tree as part of `prepare-py-env`'s venv setup).

Each item below is one PR.

### 1B.1 — `_lib/serde.py` — JSON envelope + migration chain

**Scope:** copy the `{kind, apiVersion, type, spec}` envelope shape and
the schema-migration chain machinery from
`tabsdataai/_api.py:547-790`. Generic `to_json` / `from_json` that walks
`__migrations__` on version mismatch. No other 1B deps.

**Tests:** roundtrip of a synthetic dataclass, `__migrations__` walk
across two versions, missing/extra fields error paths.

### 1B.2 — `_lib/conn.py` — Conn / NamedConn types

**Scope:** copy `Conn` abstract base and `NamedConn` reference type from
`tabsdataai/_types.py:234-344`. Just enough surface for `Src` / `Dest`
to type their `conn` field.

**Tests:** `NamedConn(name=str)` validation, abstract `Conn` cannot be
instantiated, JSON roundtrip via 1B.1.

### 1B.3 — `_lib/spi.py` — plugin protocols + contexts + Src/Dest

**Scope:** copy `SrcPlugin[C, S]`, `DestPlugin[C, D]`, `SrcContext`,
`DestContext`, `Src`, `Dest`, `SrcDef`, `DestDef` from
`tabsdataai/spi.py`. `read_in` / `write_out` signatures preserved
verbatim.

**Tests:** abstract bases reject direct instantiation; concrete `Src`
subclass with `conn` field is acceptable; `read_in` / `write_out` exist
as abstract methods.

### 1B.4 — `_lib/info.py` — PublisherInfo

**Scope:** copy `PublisherInfo` dataclass from
`tabsdataai/_info.py:86-241` (fields: `func`, `source`, `output_tables`,
`trigger_by`, `on_tables`). Includes `func` ↔ `"module:qualname"`
serialisation (rejects closures at dump time). `TransformerInfo` /
`SubscriberInfo` / `AutoPublisherInfo` deferred.

**Tests:** roundtrip a publisher info via 1B.1, function-as-string
serialisation, closure rejection.

### 1B.5 — `_lib/decorators.py` — @publisher

**Scope:** copy `@publisher` decorator from `tabsdataai/api.py:237-419`.
Returns a `PublisherInfo`. `@transformer` / `@subscriber` /
`@auto_publisher` deferred.

**Tests:** decoration produces `PublisherInfo`, slot count / return-type
validation per `ApiErrorCode.API_4`.

### 1B.6 — `_lib/registry.py` + replacement for 1.12

**Scope:** copy the connector registry mechanism from
`tabsdataai/connectors.py`. Initially populated explicitly by
`_lib/builtins/__init__.py`; entry-point auto-discovery deferred. Add a
`plugin_resolver.py` that takes a deserialised `PublisherInfo` and
returns an instantiated `SrcPlugin` looked up by `type(info.source)`.
**This task replaces 1.12.**

**Tests:** explicit registration → lookup; missing type → typed error;
plugin instantiation works.

### 1B.7 — `_lib/builtins/local_file.py` — reference connector

**Scope:** one concrete connector — `LocalFileSrc` + `LocalFileSrcPlugin`
— registered into 1B.6 (chosen 2026-05-08 over a fake stub: real
CSV-to-parquet path exercises the I/O contract end-to-end and stays
useful as production code, not just test scaffolding). Copy/adapt from
`tabsdataai-conn-localfile/_plugin.py`. No cloud creds, no external
dependencies beyond `polars` for the parquet write.

**Tests:** plugin's `read_in` produces a parquet file from a known CSV
fixture; integration with 1B.6 registry lookup.

### 1B-amend.1.5 — `request.py` schema extension

**Scope:** extend `Request` to include a `function_def: dict` (or, after
1B.4, a typed union of the `*Info` classes) carrying the JSON envelope
the API server will produce. Pure schema growth — no breakages of
existing tests beyond updating fixtures. Schedule **after 1B.4**.

**Tests:** loader accepts a request.yaml with `function_def`; existing
fields unchanged.

---

## Phase 2 — Pure-disk leaf commands

Each command is a thin `main()` in
`tabsdatak/server/_tdrunner/function/commands/<name>/__init__.py`. Tests live at
`test_tabsdatak/server/test_tdrunner/test_function/test_commands/test_<name>.py`.
Every command:

* builds its parser via `_lib.command.build_parser`,
* starts a `_lib.liveness.Liveness`,
* maps `CommandError` subclasses to exit codes via
  `_lib.command.run_command`,
* logs via `_lib.logging`.

### 2.1 — `fetch-request` ✅

**Status:** Done (2026-05-07). 100% line+branch coverage. 10 tests
passing. Stdlib `urllib.request` for the HTTP GET; bearer auth via
`Authorization` header. Command-specific exit codes: 10 = HTTP/network
error, 11 = output write error. Fixed the broken stub import (was
`from tabsdatak._commands import tabsdata_command` → now imports from
`tabsdatak.server._tdrunner.function.commands` where the decorator actually
lives). Decorator metadata (`tabsdata_command_name = "td_fetch_request"`)
preserved.

**Scope:** HTTP GET `--url` with `Authorization: Bearer <token>`, write
body to `--out`.

**Code:** `tabsdatak/server/_tdrunner/function/commands/fetch_request/__init__.py`

* Args: `--url`, `--token`, `--out`, `--liveness`.
* Uses `urllib.request` with bearer header.

**Tests:** spin up a tiny `http.server` fixture; verify GET headers and
file contents; non-2xx → typed error → command-specific exit; network
error → typed error.

**Depends on:** 1.1, 1.2, 1.3, 1.4.

### 2.2 — `prepare-py-env`

**Scope:** read `--request`, locate requirements, call
`_lib.pyenv.create_virtual_environment`, write `--out` as run-env.yaml.

**Code:** `tabsdatak/server/_tdrunner/function/commands/prepare_py_env/__init__.py`

* Args: `--request`, `--out`, `--liveness`.

**Tests:** integration with a small requirements fixture (one tiny
pure-python dep, or empty); failure path when pyenv creation fails.

**Depends on:** 1.1–1.5, 1.8, 1.9. **Schema confirmation (1.9) must
happen first.**

### 2.3 — `localize-fn-bundle` ✅

**Status:** Done (2026-05-07; amended 2026-05-08).

* **Original (TD-1767):** 100% line+branch coverage, 11 tests. Bundle
  fetch via `td_transporter` (option a from the stop-and-ask). Added a
  `build_bundle_fetch_request` builder to `transporter_request.py`
  mirroring the existing localize/upload builders.
* **Amendment (TD-1777, 2026-05-08):** dropped `--run-env`, the
  `_load_run_env` helper, the `EXIT_BAD_RUN_ENV` exit code, and the
  matching tests. Per the pod-runner / venv split, the venv is already
  active by the time this command runs. Command-specific exit codes
  are now: 10 = bad request.yaml, 12 = transporter failed, 13 = extract
  failed. 9 tests, still 100% coverage.

**Scope:** read bundle URI from `--request`, download via storage
credentials in env, extract to `--dir`.

**Code:** `tabsdatak/server/_tdrunner/function/commands/localize_fn_bundle/__init__.py`

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

### 2.4 — `prep-localize-state` ✅

**Status:** Done (2026-05-07). 100% line+branch coverage. 8 tests
passing. Composes `load_request` + `build_localize_state_request` +
`write_transporter_request`. Command-specific exit codes: 10 = bad
request.yaml, 11 = unsupported URI scheme, 12 = write failed.

**Scope:** build the transporter request YAML for state-in localisation.

**Code:** `tabsdatak/server/_tdrunner/function/commands/prep_localize_state/__init__.py`

* Args: `--request`, `--in-dir`, `--state-dir`, `--out`, `--liveness`.

**Tests:** sample requests with and without initial-value state; output
YAML matches golden file.

**Depends on:** 1.1–1.5, 1.10.

### 2.5 — `prep-upload-state` ✅

**Status:** Done (2026-05-07). 100% line+branch coverage. 8 tests
passing. Mirror of 2.4 for the upload direction. Same exit-code scheme.

**Scope:** build the transporter request YAML for state-out upload.

**Code:** `tabsdatak/server/_tdrunner/function/commands/prep_upload_state/__init__.py`

* Args: `--request`, `--state-dir`, `--out`, `--liveness`.

**Tests:** empty `state_dir` yields no-op; with state files, builds
correct copy entries.

**Depends on:** 1.1–1.5, 1.10.

### 2.6 — `run-transporter` ✅

**Status:** Done (2026-05-07). 100% line+branch coverage. 8 tests
passing. Thin wrapper around `_lib.transporter_proc.run_transporter`,
forwards the exit code (so non-zero from the transporter binary becomes
the command's exit code). Command-specific exit code 10 = binary not
found.

**Scope:** pure wrapper — exec `td_transporter --request <PATH>` and
propagate exit code, while running a Liveness.

**Code:** `tabsdatak/server/_tdrunner/function/commands/run_transporter/__init__.py`

* Args: `--request`, `--liveness`.

**Tests:** monkeypatch `subprocess.run`; verify args; non-zero exit
propagated; binary missing → mapped exit code.

**Depends on:** 1.1–1.4, 1.11.

### 2.7 — `report-completion` ✅

**Status:** Done (2026-05-07). 100% line+branch coverage. 9 tests
passing. Per-table parquet metadata (`column_count`, `row_count`,
`schema_hash`) extracted via `pyarrow.parquet`. Tables present in the
request but absent on disk get an info-less entry (`!NoData` semantics).
`--out` is written **before** the POST so it survives network failures.
Command-specific exit codes: 10 = bad request.yaml, 11 = HTTP/network
error, 12 = write failed. Tests use a real stdlib `http.server` in a
thread.

**Scope:** read `--request`, scan `--out-dir` and `--state-dir`, build
response, POST to `--url` with bearer token, write a copy to `--out`.

**Code:** `tabsdatak/server/_tdrunner/function/commands/report_completion/__init__.py`

* Args: `--url`, `--token`, `--request`, `--out-dir`, `--state-dir`,
  `--out`, `--liveness`.

**Tests:** local `http.server` fixture; verify POST body matches expected
response shape; non-2xx response error path; verifies `--out` written even
on POST failure.

**Depends on:** 1.1–1.6.

---

## Phase 3 — User-code-loading commands (redesigned 2026-05-08)

These three commands invoke the user's bundled code. **In-process, no
subprocess.** The pod-runner has already activated the venv before these
commands run, so the command's interpreter already has `tabsdatak` plus
the function's resolved dependencies. The command just *does the work*
in one process.

The plugin contract is the new `tabsdataai`-style one (copied into
`tabsdatak` in Phase 1B): `SrcPlugin.read_in(ctx, src) -> list[list[TableFileSpec]]`,
`DestPlugin.write_out(ctx, dest, tables) -> None`. Plugins are looked up
in the registry (1B.6) by `type(info.source)` / `type(info.destination)`
— **not** loaded from cloudpickle files. Functions are resolved by
`module:qualname` import, **not** unpickled.

**Common shape for all three commands:**

1. Parse args (cli, liveness).
2. Load `request.yaml` (1.5; eventually with `function_def`).
3. Deserialise the JSON envelope into `PublisherInfo` /
   `SubscriberInfo` / etc. (1B.4 + 1B.1).
4. Look up the plugin class via the registry (1B.6) and instantiate.
5. Build a concrete `SrcContext` / `DestContext` (work_dir = `--out-dir`
   / similar).
6. Resolve any `NamedConn` references (assumed pre-resolved by the API
   server per 00-FunctionRunner.md; otherwise local fallback).
7. Call the plugin (or invoke the user function for 3.2).
8. Persist outputs to disk.

**No** `--run-env`, **no** subprocess, **no** bootstrap script.

### 3.1 — `run-source-plugin`

**Scope:** invoke the source plugin in-process; outputs Parquet files in
`--out-dir`.

**Code:**
`tabsdatak/server/_tdrunner/function/commands/run_source_plugin/__init__.py`

* Args: `--request`, `--bundle`, `--out-dir`, `--liveness`.
* Flow: load Request → deserialise `PublisherInfo` from
  `request.function_def` → registry lookup `SrcDef` for
  `type(info.source)` → instantiate `SrcPlugin` → build `SrcContext`
  (`work_dir = --out-dir`) → call `plugin.read_in(ctx, info.source)`.

**Tests:** end-to-end with the reference `LocalFileSrc` (1B.7) and a
tiny CSV fixture; verify `--out-dir` contents; failure inside `read_in`
propagates as a command-specific exit code; `NamedConn` not resolved →
typed error.

**Depends on:** 1.1–1.5, 1.7, **Phase 1B (1B.1 → 1B.7)**.

### 3.2 — `run-function`

**Scope:** load the user's decorated function via `module:qualname`,
materialise inputs as TableFrames, invoke, persist outputs and state.
Heaviest single command. The `module:qualname` import works because the
pod-runner has placed the bundle's code on `sys.path` (or installed it as
a wheel) and we're already in the venv where `tabsdatak` and the user's
deps both exist.

**Code:** `tabsdatak/server/_tdrunner/function/commands/run_function/__init__.py`

* Args: `--request`, `--bundle`, `--in-dir`, `--state-in`, `--out-dir`,
  `--state-out`, `--liveness`.
* Flow: load Request → deserialise `PublisherInfo` /
  `TransformerInfo` / `SubscriberInfo` → import `info.func` via
  `module:qualname` → materialise inputs as TableFrames from `--in-dir`
  parquets → invoke function → persist returned TableFrames to
  `--out-dir` → persist state to `--state-out`.
* TableFrame is stubbed via `polars.LazyFrame` for this iteration
  (decision 2026-05-08). Promoting a real TableFrame copy from
  `tabsdataai` / `tabsdata` to Phase 1B (as 1B.8) is the planned
  follow-up once the publisher prototype works end-to-end.

**Tests:** integration with a tiny fixture bundle containing an
identity transformer; assert artefacts; user-function exception path.

This task is large enough to want sub-tasks; split into 3.2a / b / c
(parameter binding, function invocation, results write-back) when
reaching it.

**Depends on:** 1.1–1.7, **Phase 1B** (using the `polars.LazyFrame`
TableFrame stub).

### 3.3 — `run-destination-plugin`

**Scope:** mirror of 3.1 but for the destination plugin —
`DestPlugin.write_out(ctx, dest, tables)` reads the parquets in
`--out-dir` and writes them to the destination described by `info.destination`.

**Code:**
`tabsdatak/server/_tdrunner/function/commands/run_destination_plugin/__init__.py`

* Args: `--request`, `--bundle`, `--out-dir`, `--liveness`.
* Flow: load Request → deserialise `SubscriberInfo` → registry lookup
  `DestDef` for `type(info.destination)` → instantiate `DestPlugin` →
  build `DestContext` → enumerate parquets in `--out-dir` → call
  `plugin.write_out(ctx, info.destination, tables)`.

**Tests:** end-to-end with a fake `DestPlugin` that records calls;
verify call signature; plugin exception path.

**Depends on:** 1.1–1.7, **Phase 1B**.

---

## Phase 4 — End-to-end publisher chain

### 4.1 — Publisher chain integration test under k-

**Scope:** wire up an end-to-end test that runs the 11 publisher
containers in order against a known fixture bundle and asserts the full
produced response.yaml + uploaded state + plugin-emitted output. k- harness
exists already (Q9) — we just need to compose well with it.

**Code:** test only — no production code (unless k- needs an adapter).

**Tests:** `test_tabsdatak/server/test_tdrunner/test_e2e/test_publisher_chain.py`.
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
`test_tabsdatak/server/test_tdrunner/test_function/test_commands/test_common_options.py`.

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
`tabsdatak/server/_tdrunner/function/commands/run_trigger_plugin/__init__.py`.
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

**Scope:** verify nothing under `tabsdatak/server/_tdrunner/` imports from
`tabsdata` *or* `tabsdataai`. CI-enforced.

**Code:**
`test_tabsdatak/server/test_tdrunner/test_no_legacy_imports.py` — walks
`_tdrunner/` and asserts every `import` and `from` line resolves outside
both `tabsdata` and `tabsdataai`.

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
   `tabsdatak.server._tdrunner`.
2. **Fixture bundles** built in
   `test_tabsdatak/server/test_tdrunner/conftest.py` (a `bundle_archive` factory
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

## Architectural decisions logged 2026-05-08

* **`tabsdataai` is also legacy and slated for deletion.** The standalone
  rule covers it; copy needed types into `tabsdatak`.
* **Pod-runner activates the venv between steps.** `prepare-py-env` is
  the only command that touches `run-env.yaml`; downstream commands run
  in-process in the already-active venv, with no `--run-env` arg, no
  subprocess, no bootstrap script.
* **Plugin contract is `read_in` / `write_out`** (not legacy `_run`).
* **Functions and definitions serialise as JSON envelopes** (not
  cloudpickle). `func` fields encode as `module:qualname` strings.
* **Plugins are class references** discovered via a registry, not
  cloudpickled blobs at known paths in the bundle.
* **`NamedConn` resolution** — assumed pre-resolved by the API server
  before producing `request.yaml`. Local fallback in `tabsdatak` is a
  later possibility if needed.
* **Bundle layout** — package tree under `<bundle>/` plus a sibling
  `<bundle>/manifest.json` carrying the JSON envelope. Pod-runner
  arranges for the package tree to be importable from the venv (added
  to `sys.path` by `prepare-py-env`'s setup, or installed as part of
  the venv build).
* **TableFrame** — Phase 3.2 stubs TableFrame via `polars.LazyFrame`.
  A real TableFrame copy (Phase 1B.8, deferred) replaces the stub once
  the publisher prototype is end-to-end green.
* **Reference connector for tests** — `LocalFileSrc` (real
  CSV → parquet path), copied from `tabsdataai-conn-localfile`. Chosen
  over a deliberately-fake stub because it exercises the I/O contract
  and stays useful as production code, not just test scaffolding.

## Stop-and-ask checkpoints

These are surfaced here so they're not forgotten when reaching them:

* **Before 1.9** — confirm the run-env.yaml schema sketch.
* **Before 2.3** — confirm how `localize-fn-bundle` actually fetches the
  bundle (transporter? presigned URL? storage SDK?).
* **Before 5.1** — confirm transformer's upload model (transporter only
  vs. destination plugin).
* **Before 5.4** — confirm cron's `trigger-function` placement and name.
