//
// Copyright 2026 Tabs Data Inc.
//

use std::env;

use crate::base::error::gateway::ResolverError;
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::{
    ConcreteResolver, PATH_SEPARATOR, Resolution, ResolveContext, Rule, SecretEntry,
};
use crate::runtime::secrets::walker::{Generator, encode_secret_key};

const CONSUMER_LABEL: &str = "<consumer config>";

pub(crate) struct SecretResolver {
    auto: bool,
}

impl SecretResolver {
    pub(crate) fn new(auto: bool) -> Self {
        Self { auto }
    }

    fn secret_key(&self, context: &ResolveContext) -> Result<String, ResolverError> {
        encode_secret_key(CONSUMER_LABEL, context.path).map_err(|cause| {
            ResolverError::SecretKeyEncodingFailed {
                path: context.path.join(PATH_SEPARATOR),
                cause: cause.to_string(),
            }
        })
    }

    fn generate(
        &self,
        context: &ResolveContext,
        key: String,
    ) -> Result<Option<SecretEntry>, ResolverError> {
        let category = context.placeholder.category.as_deref().ok_or_else(|| {
            ResolverError::MissingPlaceholderCategory {
                path: context.path.join(PATH_SEPARATOR),
                kind: self.kind().as_str().to_owned(),
            }
        })?;
        let generator =
            Generator::from_name(category).ok_or_else(|| ResolverError::UnknownGenerator {
                path: context.path.join(PATH_SEPARATOR),
                generator: category.to_owned(),
                supported: Generator::supported().join(", "),
            })?;
        Ok(Some(SecretEntry {
            key,
            value: generator.generate(),
            category: Some(category.to_owned()),
        }))
    }
}

impl ConcreteResolver for SecretResolver {
    fn kind(&self) -> PlaceholderKind {
        if self.auto {
            PlaceholderKind::SecretAuto
        } else {
            PlaceholderKind::Secret
        }
    }

    fn resolve(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        let key = self.secret_key(context)?;
        let token = context.placeholder.token();
        let provided = context
            .init_value
            .filter(|value| !value.is_empty() && *value != token);
        let category = context.placeholder.category.clone();
        let secret = match provided {
            Some(value) => Some(SecretEntry {
                key,
                value: value.to_owned(),
                category,
            }),
            None if self.auto => self.generate(context, key)?,
            None if context.placeholder.rule == Rule::Optional => Some(SecretEntry {
                key,
                value: String::new(),
                category,
            }),
            None => {
                return Err(ResolverError::MissingRequiredValue {
                    path: context.path.join(PATH_SEPARATOR),
                    kind: self.kind().as_str().to_owned(),
                    category: context.placeholder.category.clone().unwrap_or_default(),
                });
            }
        };
        Ok(Resolution {
            rendered: context.placeholder.token(),
            secret,
        })
    }

    fn materialize(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        let variable = self.secret_key(context)?;
        let optional = context.placeholder.rule == Rule::Optional;
        let value = match env::var(&variable) {
            Ok(value) => value,
            Err(_) if optional => String::new(),
            Err(_) => return Err(ResolverError::EnvironmentVariableNotFound { variable }),
        };
        Ok(Resolution {
            rendered: value,
            secret: None,
        })
    }
}
