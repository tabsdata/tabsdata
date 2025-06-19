//
// Copyright 2025. Tabs Data Inc.
//

use crate::execution::layers::runtime_info::runtime_info;
use crate::execution::RuntimeContext;
use td_error::TdError;
use td_objects::crudl::ReadRequest;
use td_objects::types::runtime_info::RuntimeInfo;
use td_tower::from_fn::from_fn;
use td_tower::service_provider::IntoServiceProvider;
use td_tower::{layers, provider};

#[provider(
    name = RuntimeInfoService,
    context = RuntimeContext,
    request = ReadRequest<()>,
    response = RuntimeInfo,
)]
fn provider() {
    layers!(from_fn(runtime_info))
}

#[cfg(test)]
mod tests {

    //TODO check with Joaquin why this fails
    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_runtime_info() {
        use super::*;
        use std::sync::Arc;
        use td_objects::crudl::ReadRequest;
        use td_objects::types::runtime_info::RuntimeInfo;
        use td_tower::ctx_service::RawOneshot;

        use td_tower::metadata::{type_of_val, Metadata};

        let runtime_context = Arc::new(RuntimeContext::new().await.unwrap());

        let provider = RuntimeInfoService::provider(runtime_context);
        let service = provider.make().await;

        let response: Metadata = service.raw_oneshot(()).await.unwrap();
        let metadata = response.get();

        metadata.assert_service::<ReadRequest<()>, RuntimeInfo>(&[
            // Extract from request.
            type_of_val(&runtime_info),
        ]);
    }
}
