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
//! ## Placeholder kinds (4 manual / auto pairs)
//!
//! | kind          | stored as            | user input        | when user omits              |
//! |---------------|----------------------|-------------------|------------------------------|
//! | `secret`      | kubernetes Secret    | required          | error                        |
//! | `secretAuto`  | kubernetes Secret    | optional          | auto-generated               |
//! | `config`      | kubernetes ConfigMap | required          | error  (UNSUPPORTED)         |
//! | `configAuto`  | kubernetes ConfigMap | optional          | auto-generated (UNSUPPORTED) |
//! | `literal`     | inline in the file   | required          | error                        |
//! | `literalAuto` | inline in the file   | optional          | auto-generated               |
//! | `system`      | inline in the file   | forbidden         | always resolved by us        |
//! | `systemAuto`  | inline in the file   | forbidden         | always resolved by us        |
//!
//! `Auto` = if the user did not supply a value, one is generated/resolved (the
//! user may leave it empty). Non-`Auto` = mandatory user input, EXCEPT `system`
//! which the user can never supply. `config`/`configAuto` are recognized but
//! NOT YET SUPPORTED — resolving one raises an error.
//!
//! `system` vs `systemAuto`: a `system` value is decided DETERMINISTICALLY — the
//! same instance always resolves it to the same value. A `systemAuto` value is
//! GENERATED, so it may differ across consecutive runs, even for the same
//! instance created twice.
//!
//! ## Resolution into the staged file
//!
//! * `literal` / `literalAuto`, `system` / `systemAuto` — replaced INLINE with
//!   the resolved value.
//! * `secret` / `secretAuto` — the value is STRIPPED (even when the user
//!   supplied it) and replaced INLINE with the ENV variable name the runtime
//!   reads it from; the real value is kept in an in-memory structure for later
//!   dump into a kubernetes Secret.
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

    pub async fn resolve_secret(&self, content: &str) -> Result<String, TdError> {
        secret::resolve(content)
    }
}

#[cfg(test)]
mod tests {
    mod resolver;
}
