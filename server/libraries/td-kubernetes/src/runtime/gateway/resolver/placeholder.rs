//
// Copyright 2026 Tabs Data Inc.
//

pub(super) mod env;
pub(super) mod literal;
pub(super) mod secret;
pub(super) mod system;

pub use system::{SystemKey, SystemValues, WorkerSettings};

use std::str::FromStr;

use crate::base::error::gateway::ResolverError;
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::placeholder::env::EnvResolver;
use crate::runtime::gateway::resolver::placeholder::literal::LiteralResolver;
use crate::runtime::gateway::resolver::placeholder::secret::SecretResolver;
use crate::runtime::gateway::resolver::placeholder::system::SystemResolver;
use crate::runtime::gateway::resolver::walker::ConcreteResolver;

pub(super) fn resolver_for(
    kind: &str,
    path: &str,
) -> Result<Box<dyn ConcreteResolver>, ResolverError> {
    let kind =
        PlaceholderKind::from_str(kind).map_err(|_| ResolverError::UnknownPlaceholderKind {
            path: path.to_owned(),
            kind: kind.to_owned(),
            supported: PlaceholderKind::supported(),
        })?;
    if !kind.is_supported() {
        return Err(ResolverError::UnsupportedPlaceholderKind {
            path: path.to_owned(),
            kind: kind.as_str().to_owned(),
        });
    }
    let resolver: Box<dyn ConcreteResolver> = match kind {
        PlaceholderKind::Literal => Box::new(LiteralResolver::new(false)),
        PlaceholderKind::LiteralAuto => Box::new(LiteralResolver::new(true)),
        PlaceholderKind::Secret => Box::new(SecretResolver::new(false)),
        PlaceholderKind::SecretAuto => Box::new(SecretResolver::new(true)),
        PlaceholderKind::System => Box::new(SystemResolver::new(false)),
        PlaceholderKind::SystemAuto => Box::new(SystemResolver::new(true)),
        PlaceholderKind::Env => Box::new(EnvResolver::new()),
        _ => {
            return Err(ResolverError::UnsupportedPlaceholderKind {
                path: path.to_owned(),
                kind: kind.as_str().to_owned(),
            });
        }
    };
    Ok(resolver)
}

#[cfg(test)]
mod tests;
