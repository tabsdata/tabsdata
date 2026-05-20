//
// Copyright 2026 Tabs Data Inc.
//

//! Configuration resolver — substitutes typed placeholder tokens
//! (`${env:...}`, `${hashicorp:...}`, `${sys:...}`) inside arbitrary text
//! with their resolved values.
//!
//! Each placeholder family ("source") lives in its own submodule. The
//! [`ConfigurationResolver`] dispatches to a [`Source`] variant and applies
//! the resulting transformation to the entire text in a single pass per
//! source. Sources are applied in the order requested by the caller via
//! [`ConfigurationResolver::resolve`].

mod env;
mod hashicorp;
mod sys;

use strum::{AsRefStr, EnumIter, IntoEnumIterator};
use td_error::TdError;

pub use hashicorp::HashicorpConfig;
pub use sys::{SystemKey, SystemValues};

use crate::error::gateway::ResolverError;

#[derive(Debug, Clone, Copy, Eq, PartialEq, AsRefStr, EnumIter)]
pub enum Source {
    #[strum(serialize = "env")]
    Environment,
    #[strum(serialize = "hashicorp")]
    Hashicorp,
    #[strum(serialize = "sys")]
    System,
}

pub struct ConfigurationResolver {
    hashicorp: HashicorpConfig,
    system: SystemValues,
}

impl ConfigurationResolver {
    pub fn new(hashicorp: HashicorpConfig, system: SystemValues) -> Self {
        Self { hashicorp, system }
    }

    pub async fn resolve(&self, contents: &str) -> Result<String, TdError> {
        let mut current = contents.to_string();
        for source in Source::iter() {
            current = self.apply(&current, source).await?;
        }
        Ok(current)
    }

    async fn apply(&self, text: &str, source: Source) -> Result<String, TdError> {
        match source {
            Source::Environment => env::resolve(text),
            Source::Hashicorp => hashicorp::resolve(text, &self.hashicorp).await,
            Source::System => sys::resolve(text, &self.system),
            #[allow(unreachable_patterns)]
            other => Err(ResolverError::UnimplementedSource {
                name: other.as_ref().to_string(),
            }
            .into()),
        }
    }
}
