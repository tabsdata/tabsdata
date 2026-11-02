//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use async_trait::async_trait;
use td_security::token;

#[cfg(feature = "enterprise")]
use crate::base::codec;
#[cfg(feature = "enterprise")]
use crate::base::constants::{
    SECRET_GENERATOR_AIAGENT_CONFIRM_KEY, SECRET_GENERATOR_CLASSIFIER_SERVICE_TOKEN,
    SECRET_GENERATOR_EMBEDDER_SERVICE_TOKEN, SECRET_GENERATOR_POSTGRES_AIAGENT_PASSWORD,
};
use crate::base::constants::{
    SECRET_GENERATOR_JWT_SIGNING_KEY, SECRET_GENERATOR_POSTGRES_APISERVER_PASSWORD,
    SECRET_GENERATOR_POSTGRES_SYSTEM_PASSWORD, SECRET_GENERATOR_POSTGRES_SYSTEM_USER,
    SECRET_GENERATOR_SERVICE_TOKEN,
};
use crate::base::error::kubernetes::KubernetesError;
use crate::kube::context::KubeContext;
use crate::runtime::secrets::phase::SecretPhase;

const EMBEDDED_POSTGRES_DEFAULT_USER: &str = "postgres";
/// The agent's confirmation HMAC key: every replica verifies what another
/// minted, so it is generated once and kept.
#[cfg(feature = "enterprise")]
const AIAGENT_CONFIRM_KEY_BYTES: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Generator {
    JwtSigningKey,
    PostgresSystemUser,
    PostgresSystemPassword,
    PostgresApiserverPassword,
    #[cfg(feature = "enterprise")]
    PostgresAiagentPassword,
    ServiceToken,
    #[cfg(feature = "enterprise")]
    ClassifierServiceToken,
    #[cfg(feature = "enterprise")]
    EmbedderServiceToken,
    #[cfg(feature = "enterprise")]
    AiagentConfirmKey,
}

impl Generator {
    pub fn from_name(name: &str) -> Option<Generator> {
        match name {
            SECRET_GENERATOR_JWT_SIGNING_KEY => Some(Generator::JwtSigningKey),
            SECRET_GENERATOR_POSTGRES_SYSTEM_USER => Some(Generator::PostgresSystemUser),
            SECRET_GENERATOR_POSTGRES_SYSTEM_PASSWORD => Some(Generator::PostgresSystemPassword),
            SECRET_GENERATOR_POSTGRES_APISERVER_PASSWORD => {
                Some(Generator::PostgresApiserverPassword)
            }
            #[cfg(feature = "enterprise")]
            SECRET_GENERATOR_POSTGRES_AIAGENT_PASSWORD => Some(Generator::PostgresAiagentPassword),
            SECRET_GENERATOR_SERVICE_TOKEN => Some(Generator::ServiceToken),
            #[cfg(feature = "enterprise")]
            SECRET_GENERATOR_CLASSIFIER_SERVICE_TOKEN => Some(Generator::ClassifierServiceToken),
            #[cfg(feature = "enterprise")]
            SECRET_GENERATOR_EMBEDDER_SERVICE_TOKEN => Some(Generator::EmbedderServiceToken),
            #[cfg(feature = "enterprise")]
            SECRET_GENERATOR_AIAGENT_CONFIRM_KEY => Some(Generator::AiagentConfirmKey),
            _ => None,
        }
    }

    pub fn supported() -> &'static [&'static str] {
        &[
            SECRET_GENERATOR_JWT_SIGNING_KEY,
            SECRET_GENERATOR_POSTGRES_SYSTEM_USER,
            SECRET_GENERATOR_POSTGRES_SYSTEM_PASSWORD,
            SECRET_GENERATOR_POSTGRES_APISERVER_PASSWORD,
            #[cfg(feature = "enterprise")]
            SECRET_GENERATOR_POSTGRES_AIAGENT_PASSWORD,
            SECRET_GENERATOR_SERVICE_TOKEN,
            #[cfg(feature = "enterprise")]
            SECRET_GENERATOR_CLASSIFIER_SERVICE_TOKEN,
            #[cfg(feature = "enterprise")]
            SECRET_GENERATOR_EMBEDDER_SERVICE_TOKEN,
            #[cfg(feature = "enterprise")]
            SECRET_GENERATOR_AIAGENT_CONFIRM_KEY,
        ]
    }

    pub fn name(self) -> &'static str {
        match self {
            Generator::JwtSigningKey => SECRET_GENERATOR_JWT_SIGNING_KEY,
            Generator::PostgresSystemUser => SECRET_GENERATOR_POSTGRES_SYSTEM_USER,
            Generator::PostgresSystemPassword => SECRET_GENERATOR_POSTGRES_SYSTEM_PASSWORD,
            Generator::PostgresApiserverPassword => SECRET_GENERATOR_POSTGRES_APISERVER_PASSWORD,
            #[cfg(feature = "enterprise")]
            Generator::PostgresAiagentPassword => SECRET_GENERATOR_POSTGRES_AIAGENT_PASSWORD,
            Generator::ServiceToken => SECRET_GENERATOR_SERVICE_TOKEN,
            #[cfg(feature = "enterprise")]
            Generator::ClassifierServiceToken => SECRET_GENERATOR_CLASSIFIER_SERVICE_TOKEN,
            #[cfg(feature = "enterprise")]
            Generator::EmbedderServiceToken => SECRET_GENERATOR_EMBEDDER_SERVICE_TOKEN,
            #[cfg(feature = "enterprise")]
            Generator::AiagentConfirmKey => SECRET_GENERATOR_AIAGENT_CONFIRM_KEY,
        }
    }

    pub fn is_storable(self) -> bool {
        match self {
            Generator::JwtSigningKey | Generator::ServiceToken => false,
            #[cfg(feature = "enterprise")]
            Generator::ClassifierServiceToken | Generator::EmbedderServiceToken => false,
            _ => true,
        }
    }

    /// A held value this generator would not produce, such as a token of a
    /// retired format, is regenerated instead of kept.
    pub fn accepts(self, value: &str) -> bool {
        match self {
            Generator::ServiceToken => token::parse(value).is_some(),
            #[cfg(feature = "enterprise")]
            Generator::ClassifierServiceToken | Generator::EmbedderServiceToken => {
                token::parse(value).is_some()
            }
            #[cfg(feature = "enterprise")]
            Generator::AiagentConfirmKey => {
                codec::decode(value).is_ok_and(|key| key.len() == AIAGENT_CONFIRM_KEY_BYTES)
            }
            _ => true,
        }
    }

    pub fn is_storable_category(category: &str) -> bool {
        match Generator::from_name(category) {
            Some(generator) => generator.is_storable(),
            None => !category.is_empty(),
        }
    }

    pub fn generate(self) -> String {
        match self {
            Generator::JwtSigningKey => td_common::id::id().to_string(),
            Generator::PostgresSystemUser => EMBEDDED_POSTGRES_DEFAULT_USER.to_owned(),
            Generator::PostgresSystemPassword | Generator::PostgresApiserverPassword => {
                td_common::random::password()
            }
            #[cfg(feature = "enterprise")]
            Generator::PostgresAiagentPassword => td_common::random::password(),
            Generator::ServiceToken => token::generate(),
            #[cfg(feature = "enterprise")]
            Generator::ClassifierServiceToken | Generator::EmbedderServiceToken => {
                token::generate()
            }
            #[cfg(feature = "enterprise")]
            Generator::AiagentConfirmKey => {
                codec::encode(td_common::random::bytes(AIAGENT_CONFIRM_KEY_BYTES))
            }
        }
    }
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

#[cfg(test)]
mod tests;
