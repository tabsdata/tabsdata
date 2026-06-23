//
// Copyright 2026 Tabs Data Inc.
//

//! Configuration resolver — design specification.
//!
//! kprofile template files (configs, pods, images) carry two sorts of values:
//!
//! * common  — a plain literal value, left untouched.
//! * special — a placeholder token resolved by this module.
//!
//! ## Placeholder syntax
//!
//! Basic:    `${kind}`
//! Extended: `${kind:category}`
//!
//! `category` is a plain label naming the value (e.g. `db-root-password`,
//! `secure-port`) — a machine token, not a human-facing sentence. A
//! placeholder's IDENTITY is the `(kind, category)` pair: `${secret:foo}` and
//! `${literal:foo}` are distinct. Every occurrence of the same `(kind, category)`
//! across one or many files must resolve to the SAME value.
//!
//! ## Placeholder kinds
//!
//! | kind          | stored as            | user input | when user omits | phase    | support |
//! |---------------|----------------------|------------|-----------------|----------|---------|
//! | `literal`     | inline in the file   | required   | error           | create   | yes     |
//! | `literalAuto` | inline in the file   | optional   | auto-generated  | create   | yes     |
//! | `config`      | kubernetes ConfigMap | required   | error           | runtime  | NOT YET |
//! | `configAuto`  | kubernetes ConfigMap | optional   | auto-generated  | runtime  | NOT YET |
//! | `secret`      | kubernetes Secret    | required   | error           | runtime  | yes     |
//! | `secretAuto`  | kubernetes Secret    | optional   | auto-generated  | runtime  | yes     |
//! | `system`      | inline in the file   | forbidden  | resolved by us  | create   | yes     |
//! | `systemAuto`  | inline in the file   | forbidden  | resolved by us  | create   | yes     |
//! | `runtime`     | inline in the file   | forbidden  | resolved by us  | runtime  | NOT YET |
//! | `runtimeAuto` | inline in the file   | forbidden  | resolved by us  | runtime  | NOT YET |
//! | `env`         | inline in the file   | forbidden  | (see `?`)       | create   | NOT YET |
//! | `var`         | inline in the file   | forbidden  | (see `?`)       | runtime  | NOT YET |
//!
//! `Auto` = if the user did not supply a value one is generated/resolved (the
//! user may leave it empty); non-`Auto` = mandatory user input. The `system`,
//! `runtime`, `env` and `var` kinds (and their Auto forms) are SYSTEM-CONTROLLED:
//! the user can never input or modify them.
//!
//! Determinism: a `system`/`runtime` value is decided DETERMINISTICALLY — the
//! same instance always resolves it to the same value. The `*Auto` forms are
//! GENERATED, so they may differ across consecutive runs, even for the same
//! instance created twice.
//!
//! Not-yet-supported kinds are recognized (so resolution errors clearly) but
//! resolving one raises an unsupported error:
//! * `config` / `configAuto` — value stored in a kubernetes ConfigMap.
//! * `runtime` / `runtimeAuto` — like `system`/`systemAuto`, but resolved at
//!   RUNTIME (left as-is at create, resolved when the pod runs).
//! * `env` — an environment variable in the CLIENT (creator) system, read and
//!   replaced with its value at CREATE time. Supports the `?` suffix (optional,
//!   empty when absent) and the other features the former env resolver had.
//! * `var` — an environment variable in the SERVER (runtime) system, left AS-IS
//!   at create and resolved at RUNTIME from the running process's environment.
//!   (`env` is the create-time/client twin of the runtime/server-side `var`.)
//!
//! ## Two functionalities: resolve (create) vs materialize (runtime)
//!
//! * resolve — create/stage time. Substitutes the create-phase kinds inline
//!   (`literal*`, `system*`, `env`). `${secret:...}` is LEFT AS-IS in the staged
//!   and cluster-stored config (its value is collected for a kubernetes Secret).
//! * materialize — runtime, by the worker launcher. Resolves ONLY `${secret:...}`
//!   (and, when supported, the other runtime kinds) by looking up the encoded
//!   env variable per our secret-key encoding. It does NOT touch the create-phase
//!   kinds — those were already substituted at create time.
//!
//! ## Deterministic positions
//!
//! Resolution parses each file as YAML, walks the structure and resolves at the
//! value level (so staged files lose comments and may lose key order — both
//! irrelevant). A placeholder may appear ONLY where its position is
//! deterministically addressable between an authored template and an edited
//! file:
//!
//! * as the scalar value of a plain nested key (addressed by key path), or
//! * as the value of a key inside a map that is a list item, WHEN each list
//!   entry's first key/value is a unique identity within the list (addressed by
//!   `list[firstKey=firstValue].key`).
//!
//! Forbidden (rejected against the authored template): a placeholder as a bare
//! scalar list element; a list of maps whose entries' first key differs; a list
//! of maps with a duplicated first key/value.

mod secret;
mod sys;

use td_error::TdError;

use crate::base::error::gateway::ResolverError;
pub use sys::{SystemKey, SystemValues};

pub struct ConfigurationResolver {
    system: SystemValues,
}

impl ConfigurationResolver {
    pub fn new(system: SystemValues) -> Self {
        Self { system }
    }

    pub async fn resolve_sys(&self, content: &str) -> Result<String, TdError> {
        sys::resolve(content, &self.system)
    }

    pub async fn resolve(&self, content: &str) -> Result<String, TdError> {
        let _ = content;
        Err(ResolverError::ResolveNotYetImplemented.into())
    }

    pub async fn materialize(&self, content: &str) -> Result<String, TdError> {
        self.materialize_secrets(content).await
    }

    pub async fn materialize_secrets(&self, content: &str) -> Result<String, TdError> {
        secret::resolve(content)
    }
}

#[cfg(test)]
mod tests {
    mod resolver;
}
