//
// Copyright 2025 Tabs Data Inc.
//

use crate::service_provider::TdBoxService;
use async_trait::async_trait;

#[async_trait]
pub trait TdService {
    type Request;
    type Response;
    type Error;

    async fn service(&self) -> TdBoxService<Self::Request, Self::Response, Self::Error>;

    #[cfg(feature = "test_tower_metadata")]
    async fn metadata(&self) -> crate::metadata::Metadata;
}
