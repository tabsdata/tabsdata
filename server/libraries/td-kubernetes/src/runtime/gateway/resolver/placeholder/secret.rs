//
// Copyright 2026 Tabs Data Inc.
//

use std::env;

use td_error::TdError;

use crate::base::error::gateway::ResolverError;
use crate::base::error::secret::SecretError;
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
