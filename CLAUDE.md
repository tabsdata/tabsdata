<!--
Copyright 2026 Tabsdata Inc.
-->

# tabsdata-os

Open-source core of Tabsdata: a publish/subscribe system for tables. Users write
Python functions decorated as publishers, transformers, and subscribers, composed
into flows. Rust server + Python SDK.

Shared multi-repo rules (symlinks, commits, comments, refactors, cross-platform), hosted in
the ee sibling checkout (import is skipped when ee is absent):
@../tabsdata-ee/variant/devutils/agents/GUIDELINES.md

## This repo is consumed by tabsdata-ee

`../tabsdata-ee` overlays this repo via committed symlinks and shares the
workspace `Cargo.toml`: workspace members under `extensions/rust/te-*` must
exist in both repos, and after dependency changes here ee needs re-locking
(its `Cargo.lock` is separate). Composition details: `../tabsdata-ee/CLAUDE.md`.

## Layout

- `server/libraries/` — Rust workspace crates. Prefixes are semantic:
  `td-*` core, `ta-*` abstract/trait layer, `te-*` extension implementations
  (in `extensions/rust/`), `tm-*` proc macros (in `macros/`, a separate
  workspace).
- `server/binaries/td-server/` — all server binaries as `[[bin]]` targets
  (`tdserver`, `td_apiserver`, `td_supervisor`, ...). Binary names must stay in
  sync with `setup.toml` `[[binary_groups]]` and the bundling utils in the
  tabsdata package (grep for `binary_groups`).
- `client/platform/src/main/python/` — the `tabsdata` distribution: a thin
  `tabsdata` package and the anchor of the `tabsdatak` namespace package.
  **`tabsdatak` must not import `tabsdata`** (ruff banned-api enforces this).
- `connectors/python/` — the `tabsdatak` distributions: `tabsdata-api`,
  `tabsdata-cli` (`tdk`), `tabsdata-sdk`, the runtime (`tabsdata-server`,
  `tabsdata-backend`, ...), and one `tabsdata-conn-<name>` per connector in the
  `tabsdatak.conn.<name>` namespace. Scaffold new connectors from
  `templates/connector/`.
- `extensions/` — extension points with open-source default implementations.
- `variant/` — edition-specific resources (locks, manifest assets, configs).
- Docs for the SDK surface: `LLM_GUIDE.md`, `STRUCTURE.md`, `tdk-reference.md`,
  and the fuller `books/guides/LLM.md`. The official documentation is
  https://docs.tabsdata.com/guide/.

## Conventions

- The public Python API is `tabsdatak.api`, `tabsdatak.tableframe`,
  `tabsdatak.dataquality` and the `tabsdatak.conn.<name>` packages; never
  import from `_`-prefixed paths in examples, docs, or tests of public behavior.
- Rust errors: thiserror-style domain enums mapped to `td-error`'s `ApiError`
  via the `td_error!` / `api_error!` macros — follow existing crates.
- Tests named `test_logging*` run isolated under a dedicated feature —
  don't rename them casually.
- Other editions add `tdk` commands and wrap its invocations through the
  neutral `tabsdatak/cli/_registry.py` hook (`cli_command`, `cli_extension`);
  os commands read only `tabsdatak/cli/_machine.py` for how a program-driven
  run differs. Sibling repos that extend tdk pin its leaf commands, so a new
  or renamed leaf needs their tests run too.
- `tdk-reference.md` documents only what os ships; `test_reference_drift.py`
  checks every command and option it quotes against the click tree.

## Build / test

Scope Rust tests with plain `cargo test -p <crate>` while iterating; run the
cargo-make tasks (see the shared guidelines) before finishing.
