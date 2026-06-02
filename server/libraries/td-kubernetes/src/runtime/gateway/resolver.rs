//
// Copyright 2026 Tabs Data Inc.
//

//! Configuration resolver — substitutes typed placeholder tokens
//! (`${env:...}` and `${sys:...}`) inside arbitrary text with their
//! resolved values.
//!
//! Each placeholder family lives in its own submodule and is reached
//! through a dedicated method on [`ConfigurationResolver`]:
//!
//! * [`ConfigurationResolver::resolve_sys`] — substitutes `${sys:...}`
//!   tokens against the [`SystemValues`] passed at construction. Used
//!   by this crate at staging time to bake system-derived values into
//!   pod yamls.
//! * [`ConfigurationResolver::resolve_env`] — substitutes `${env:...}`
//!   tokens against the current process environment. Reserved for
//!   downstream consumers (workers loading their own config) and is
//!   intentionally NOT chained from `resolve_sys`.

mod env;
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

    pub async fn resolve_env(&self, content: &str) -> Result<String, TdError> {
        env::resolve(content)
    }

    pub async fn resolve_secret(&self, content: &str) -> Result<String, TdError> {
        secret::resolve(content)
    }

    pub async fn resolve(&self, content: &str) -> Result<String, TdError> {
        let partial = env::resolve(content)?;
        secret::resolve(&partial)
    }
}

#[cfg(test)]
mod tests {
    mod resolver;
}
