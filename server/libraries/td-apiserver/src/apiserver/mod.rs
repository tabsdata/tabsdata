//
// Copyright 2025 Tabs Data Inc.
//

use crate::apiserver::server::ServerError;
use crate::apiserver::v1::ApiServerV1;
use crate::config::Config;
use async_trait::async_trait;
use std::error::Error;
use std::sync::Arc;
use ta_services::extension::ContextExt;
use ta_services::factory::ServiceFactory;
use td_authz::AuthzContext;
use td_authz::session_provider::Sessions;
use td_common::server_runner::ServerRunnable;
use td_database::pool::TdPool;
use td_services::execution::services::runtime_info::RuntimeContext;
use td_services::{Context, Services};
use td_storage::Storage;
use te_services::{ExtendedContext, ExtendedServices};
use tokio::sync::watch;

pub mod internal;
mod server;
pub mod v1;

#[allow(dead_code)]
pub struct ApiServerCtx {
    pub config: Config,
    pub context: Context,
    pub services: Services,
    pub extended_context: ExtendedContext,
    pub extended_services: ExtendedServices,
}

impl ApiServerCtx {
    pub fn new(
        config: Config,
        db: TdPool,
        storage: Arc<Storage>,
        runtime_context: Arc<RuntimeContext>,
    ) -> Self {
        let context = Context {
            db: db.clone(),
            server_addresses: Arc::new(config.addresses.clone()),
            jwt_config: Arc::new(config.jwt.clone()),
            auth_context: Arc::new(AuthzContext::default()),
            sessions: Arc::new(Sessions::default()),
            password_settings: {
                // to verify up front configuration is OK.
                let password_hash_config = config.password.clone();
                let _ = password_hash_config.password_hasher();
                Arc::new(password_hash_config)
            },
            ssl_folder: Arc::new(config.ssl_folder.clone()),
            storage: storage.clone(),
            runtime_context: runtime_context.clone(),
            transaction_by: Arc::new(config.transaction_by.clone()),
        };
        let services = Services::build(&context);
        let extended_context = ExtendedContext::build(&context, &config.extended_config);
        let extended_services = ExtendedServices::build(&extended_context);

        Self {
            config,
            context,
            services,
            extended_context,
            extended_services,
        }
    }
}

pub enum ApiServer {
    V1(ApiServerV1),
}

impl ApiServer {
    pub async fn new(context: &ApiServerCtx) -> Result<Self, ServerError> {
        Ok(ApiServer::V1(ApiServerV1::new(context).await?))
    }
}

#[async_trait]
impl ServerRunnable for ApiServer {
    async fn run(
        self: Box<Self>,
        shutdown: watch::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        match *self {
            ApiServer::V1(server) => server.inner.run(shutdown).await,
        }
    }
}
