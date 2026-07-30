//
// Copyright 2026 Tabsdata Inc.
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
//! placeholder's IDENTITY is the `(kind, category)` pair: `${{secret:foo}}` and
//! `${{literal:foo}}` are distinct. Every occurrence of the same `(kind, category)`
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
//! | `env`         | inline in the file   | forbidden  | (see `?`)       | runtime  | yes     |
//! | `var`         | inline in the file   | forbidden  | (see `?`)       | create   | NOT YET |
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
//! `env` — an environment variable in the REMOTE (server / runtime) system. It
//! is left AS-IS at create time and resolved at RUNTIME from the running
//! process's environment. Supports the `?` suffix (optional, empty when absent)
//! and the other features of the env resolver. SUPPORTED.
//!
//! Not-yet-supported kinds are recognized (so resolution errors clearly) but
//! resolving one raises an unsupported error:
//! * `config` / `configAuto` — value stored in a kubernetes ConfigMap.
//! * `runtime` / `runtimeAuto` — like `system`/`systemAuto`, but resolved at
//!   RUNTIME (left as-is at create, resolved when the pod runs).
//! * `var` — an environment variable in the LOCAL (client / creator) system,
//!   read and replaced with its value at CREATE time. (`var` is the create-time
//!   local twin of the runtime/remote-side `env`.)
//!
//! ## Two functionalities: resolve (create) vs materialize (runtime)
//!
//! * resolve — create/stage time. Substitutes the create-phase kinds inline
//!   (`literal*`, `system*`, and `var` when supported). `${{secret:...}}` and
//!   `${{env:...}}` are LEFT AS-IS in the staged and cluster-stored config (the
//!   secret value is collected for a kubernetes Secret).
//! * materialize — runtime, by the worker launcher. Resolves the runtime kinds
//!   (`${{env:...}}`, `${{secret:...}}`) by reading the running process's
//!   environment (secrets via the encoded secret-key). It does NOT touch the
//!   create-phase kinds — those were already substituted at create time.
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

pub mod fusion;
mod kind;
mod placeholder;
pub(crate) mod reveal;
pub(crate) mod schema;
mod walker;

use std::cell::RefCell;
use std::collections::HashMap;

use td_error::TdError;

use crate::base::error::gateway::ResolverError;

pub use placeholder::{SystemKey, SystemValues, WorkerSettings};

pub struct ConfigurationResolver {
    system: SystemValues,
}

impl ConfigurationResolver {
    pub fn new(system: SystemValues) -> Self {
        Self { system }
    }

    pub async fn resolve(&self, content: &str) -> Result<String, TdError> {
        placeholder::system::resolve(content, &self.system)
    }

    pub async fn materialize(&self, content: &str) -> Result<String, TdError> {
        let mut value: serde_yaml::Value =
            serde_yaml::from_str(content).map_err(|cause| ResolverError::ParseFailed {
                path: CONFIG_LABEL.to_owned(),
                cause: cause.to_string(),
            })?;
        let mut visitor = MaterializeVisitor {
            system: &self.system,
            vault: RefCell::new(HashMap::new()),
            literals: RefCell::new(HashMap::new()),
        };
        walker::walk(&mut value, &mut visitor)?;
        let rendered =
            serde_yaml::to_string(&value).map_err(|cause| ResolverError::ParseFailed {
                path: CONFIG_LABEL.to_owned(),
                cause: cause.to_string(),
            })?;
        Ok(rendered)
    }
}

const CONFIG_LABEL: &str = "<config>";

struct MaterializeVisitor<'a> {
    system: &'a SystemValues,
    vault: RefCell<HashMap<String, String>>,
    literals: RefCell<HashMap<String, walker::LiteralOrigin>>,
}

impl walker::PlaceholderVisitor for MaterializeVisitor<'_> {
    fn visit_placeholder(
        &mut self,
        path: &[String],
        occurrence: usize,
        placeholder: &walker::Placeholder,
    ) -> Result<String, ResolverError> {
        let displayed_path = path.join(walker::PATH_SEPARATOR);
        let resolver = placeholder::resolver_for(&placeholder.kind, &displayed_path)?;
        let context = walker::ResolveContext {
            path,
            occurrence,
            placeholder,
            init_value: None,
            auto_value: None,
            vault: &self.vault,
            literals: &self.literals,
            system: self.system,
        };
        Ok(resolver.materialize(&context)?.rendered)
    }
}

#[cfg(test)]
mod tests;
