//
// Copyright 2026 Tabs Data Inc.
//

use std::env;

use td_error::TdError;

use crate::base::error::gateway::ResolverError;
use crate::base::error::secret::SecretError;
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::{
    ConcreteResolver, PATH_SEPARATOR, Resolution, ResolveContext, Rule, SecretEntry,
};
use crate::runtime::secrets::walker::{Placeholder, SecretVisitor, encode_secret_key, walk};

const CONSUMER_LABEL: &str = "<consumer config>";

#[allow(dead_code)]
pub(crate) fn resolve(yaml: &str) -> Result<String, TdError> {
    let mut value: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|error| ResolverError::ParseFailed {
            path: CONSUMER_LABEL.to_owned(),
            cause: error.to_string(),
        })?;
    let mut visitor = EnvSubstitutor;
    walk(CONSUMER_LABEL, &mut value, &mut visitor)?;
    serde_yaml::to_string(&value).map_err(|error| {
        ResolverError::ParseFailed {
            path: CONSUMER_LABEL.to_owned(),
            cause: error.to_string(),
        }
        .into()
    })
}

pub(crate) fn materialize(yaml: &str) -> Result<String, TdError> {
    let mut value: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|error| ResolverError::ParseFailed {
            path: CONSUMER_LABEL.to_owned(),
            cause: error.to_string(),
        })?;
    let mut visitor = EnvSubstitutor;
    walk(CONSUMER_LABEL, &mut value, &mut visitor)?;
    serde_yaml::to_string(&value).map_err(|error| {
        ResolverError::ParseFailed {
            path: CONSUMER_LABEL.to_owned(),
            cause: error.to_string(),
        }
        .into()
    })
}

struct EnvSubstitutor;

enum SubstituteError {
    Walker(SecretError),
    Variable(ResolverError),
}

impl From<SecretError> for SubstituteError {
    fn from(error: SecretError) -> Self {
        SubstituteError::Walker(error)
    }
}

impl From<SubstituteError> for TdError {
    fn from(error: SubstituteError) -> Self {
        match error {
            SubstituteError::Walker(inner) => inner.into(),
            SubstituteError::Variable(inner) => inner.into(),
        }
    }
}

impl SecretVisitor for EnvSubstitutor {
    type Error = SubstituteError;

    fn visit_placeholder(
        &mut self,
        config: &str,
        path: &[String],
        _placeholder: Placeholder,
        scalar: &mut String,
    ) -> Result<(), Self::Error> {
        let variable = encode_secret_key(config, path)?;
        let value = env::var(&variable).map_err(|_| {
            SubstituteError::Variable(ResolverError::EnvironmentVariableNotFound { variable })
        })?;
        *scalar = value;
        Ok(())
    }
}

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
        let provided = context.init_value.filter(|value| !value.is_empty());
        let secret = match provided {
            Some(value) => Some(SecretEntry {
                key,
                value: value.to_owned(),
            }),
            None if self.auto || context.placeholder.rule == Rule::Optional => None,
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
        let value = env::var(&variable)
            .map_err(|_| ResolverError::EnvironmentVariableNotFound { variable })?;
        Ok(Resolution {
            rendered: value,
            secret: None,
        })
    }
}
