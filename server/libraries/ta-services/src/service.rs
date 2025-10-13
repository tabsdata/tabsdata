//
// Copyright 2025 Tabs Data Inc.
//

use async_trait::async_trait;
use td_tower::service_provider::TdBoxService;

#[async_trait]
pub trait TdService {
    type Request;
    type Response;
    type Error;

    async fn service(&self) -> TdBoxService<Self::Request, Self::Response, Self::Error>;

    #[cfg(feature = "test_tower_metadata")]
    async fn metadata(&self) -> td_tower::metadata::Metadata;
}
