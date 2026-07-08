//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use async_trait::async_trait;
use td_common::jwt::{
    JwtConfig, SERVICE_TOKEN_EXP_SECS, TokenClaims, encode_token, service_token_jti,
};

use crate::base::constants::{
    SECRET_GENERATOR_APISERVER_SERVICE_TOKEN, SECRET_GENERATOR_JWT_SIGNING_KEY,
    SECRET_GENERATOR_POSTGRES_PASSWORD, SECRET_GENERATOR_POSTGRES_SYSTEM_USER,
};
use crate::base::error::kubernetes::KubernetesError;
use crate::base::error::secret::SecretError;
use crate::kube::context::KubeContext;
use crate::runtime::secrets::phase::SecretPhase;

const EMBEDDED_POSTGRES_DEFAULT_USER: &str = "postgres";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Generator {
    JwtSigningKey,
    PostgresSystemUser,
    PostgresPassword,
    APIServerServiceToken,
}

impl Generator {
    pub fn from_name(name: &str) -> Option<Generator> {
        match name {
            SECRET_GENERATOR_JWT_SIGNING_KEY => Some(Generator::JwtSigningKey),
            SECRET_GENERATOR_POSTGRES_SYSTEM_USER => Some(Generator::PostgresSystemUser),
            SECRET_GENERATOR_POSTGRES_PASSWORD => Some(Generator::PostgresPassword),
            SECRET_GENERATOR_APISERVER_SERVICE_TOKEN => Some(Generator::APIServerServiceToken),
            _ => None,
        }
    }

    pub fn supported() -> &'static [&'static str] {
        &[
            SECRET_GENERATOR_JWT_SIGNING_KEY,
            SECRET_GENERATOR_POSTGRES_SYSTEM_USER,
            SECRET_GENERATOR_POSTGRES_PASSWORD,
            SECRET_GENERATOR_APISERVER_SERVICE_TOKEN,
        ]
    }

    pub fn name(self) -> &'static str {
        match self {
            Generator::JwtSigningKey => SECRET_GENERATOR_JWT_SIGNING_KEY,
            Generator::PostgresSystemUser => SECRET_GENERATOR_POSTGRES_SYSTEM_USER,
            Generator::PostgresPassword => SECRET_GENERATOR_POSTGRES_PASSWORD,
            Generator::APIServerServiceToken => SECRET_GENERATOR_APISERVER_SERVICE_TOKEN,
        }
    }

    pub fn dependencies(self) -> &'static [Generator] {
        match self {
            Generator::APIServerServiceToken => &[Generator::JwtSigningKey],
            _ => &[],
        }
    }

    pub fn has_dependencies(self) -> bool {
        !self.dependencies().is_empty()
    }

    pub fn generate(self, resolved: &BTreeMap<Generator, String>) -> Result<String, SecretError> {
        match self {
            Generator::JwtSigningKey => Ok(td_common::id::id().to_string()),
            Generator::PostgresSystemUser => Ok(EMBEDDED_POSTGRES_DEFAULT_USER.to_owned()),
            Generator::PostgresPassword => Ok(td_common::random::password()),
            Generator::APIServerServiceToken => sign_service_token(resolved),
        }
    }
}

fn sign_service_token(resolved: &BTreeMap<Generator, String>) -> Result<String, SecretError> {
    let signing_key = resolved.get(&Generator::JwtSigningKey).ok_or_else(|| {
        SecretError::SecretDependencyMissing {
            generator: SECRET_GENERATOR_APISERVER_SERVICE_TOKEN.to_owned(),
            dependency: SECRET_GENERATOR_JWT_SIGNING_KEY.to_owned(),
        }
    })?;
    let jwt = JwtConfig::new(signing_key.to_owned(), SERVICE_TOKEN_EXP_SECS);
    let claims = TokenClaims::new(service_token_jti(), SERVICE_TOKEN_EXP_SECS);
    encode_token(&jwt, &claims).map_err(|cause| SecretError::TokenSigningFailed {
        generator: SECRET_GENERATOR_APISERVER_SERVICE_TOKEN.to_owned(),
        cause: cause.to_string(),
    })
}

#[async_trait]
pub trait SecretGenerator: Send + Sync {
    fn name(&self) -> &str;

    fn phases(&self) -> &[SecretPhase];

    async fn generate(
        &self,
        context: &KubeContext,
    ) -> Result<BTreeMap<String, String>, KubernetesError>;
}
