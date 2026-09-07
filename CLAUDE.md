<!--
Copyright 2026 Tabsdata Inc.
-->

# tabsdata-os

Open-source core of Tabsdata: a publish/subscribe system for tables. Users write
Python functions decorated as publishers, transformers, and subscribers, composed
into flows. Rust server + Python SDK.

Shared multi-repo rules (symlinks, commits, comments, refactors), hosted in
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
- `client/platform/src/main/python/` — the `tabsdata` SDK and the `tabsdatak`
  CLI package. **`tabsdatak` must not import `tabsdata`** (ruff banned-api
  enforces this).
- `connectors/python/` — connector packages: `tabsdata-conn-*` building blocks,
  `tabsdata-<name>` installable packages in the `tabsdata.connector.<name>`
  namespace. Scaffold new ones from `templates/connector/`.
- `extensions/` — extension points with open-source default implementations.
- `variant/` — edition-specific resources (locks, manifest assets, configs).
- Docs for the SDK surface: `LLM_GUIDE.md`, `STRUCTURE.md`, `tdk-reference.md`.

## Conventions

- The public Python API is only what `import tabsdata as td` re-exports; never
  import from `_`-prefixed paths in examples, docs, or tests of public behavior.
- Rust errors: thiserror-style domain enums mapped to `td-error`'s `ApiError`
  via the `td_error!` / `api_error!` macros — follow existing crates.
- Tests named `test_logging*` run isolated under a dedicated feature —
  don't rename them casually.

## Build / test

Scope Rust tests with plain `cargo test -p <crate>` while iterating; run the
cargo-make tasks (see the shared guidelines) before finishing.
