//
// Copyright 2025 Tabs Data Inc.
//

use crate::auth::services::JwtConfig;
use crate::auth::session::Sessions;
use crate::execution::RuntimeContext;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use td_authz::AuthzContext;
use td_common::server::FileWorkerMessageQueue;
use td_objects::sql::DaoQueries;
use td_security::config::PasswordHashingConfig;
use td_storage::{MountDef, Storage};
use td_test::file::mount_uri;
use te_execution::transaction::TransactionBy;
use testdir::testdir;

/// Service default used in testing.
#[async_trait]
pub trait ServiceDefault: Sized {
    async fn service_default() -> Arc<Self>;
}

#[async_trait]
impl ServiceDefault for DaoQueries {
    async fn service_default() -> Arc<Self> {
        Arc::new(DaoQueries::default())
    }
}

#[async_trait]
impl ServiceDefault for PathBuf {
    async fn service_default() -> Arc<Self> {
        Arc::new(PathBuf::default())
    }
}

#[async_trait]
impl ServiceDefault for JwtConfig {
    async fn service_default() -> Arc<Self> {
        Arc::new(JwtConfig::default())
    }
}

#[async_trait]
impl ServiceDefault for PasswordHashingConfig {
    async fn service_default() -> Arc<Self> {
        Arc::new(PasswordHashingConfig::default())
    }
}

#[async_trait]
impl ServiceDefault for Sessions {
    async fn service_default() -> Arc<Self> {
        Arc::new(Sessions::default())
    }
}

#[async_trait]
impl ServiceDefault for AuthzContext {
    async fn service_default() -> Arc<Self> {
        Arc::new(AuthzContext::default())
    }
}

#[async_trait]
impl ServiceDefault for TransactionBy {
    async fn service_default() -> Arc<Self> {
        Arc::new(TransactionBy::default())
    }
}

#[async_trait]
impl ServiceDefault for RuntimeContext {
    async fn service_default() -> Arc<Self> {
        Arc::new(RuntimeContext::new().await.unwrap())
    }
}

#[async_trait]
impl ServiceDefault for SocketAddr {
    async fn service_default() -> Arc<Self> {
        Arc::new(SocketAddr::from(([127, 0, 0, 1], 2457)))
    }
}

#[async_trait]
impl ServiceDefault for Storage {
    async fn service_default() -> Arc<Self> {
        let test_dir = testdir!();
        let mount_def = MountDef::builder()
            .id("id")
            .path("/")
            .uri(mount_uri(&test_dir))
            .build()
            .unwrap();
        Arc::new(Storage::from(vec![mount_def]).await.unwrap())
    }
}

#[async_trait]
impl ServiceDefault for FileWorkerMessageQueue {
    async fn service_default() -> Arc<Self> {
        let test_dir = testdir!();
        Arc::new(FileWorkerMessageQueue::with_location(&test_dir).unwrap())
    }
}
