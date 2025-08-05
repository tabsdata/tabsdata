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
    request = ReadRequest<()>,
    response = RuntimeInfo,
    context = RuntimeContext,
)]
fn provider() {
    layers!(from_fn(runtime_info))
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "test_tower_metadata")]
    #[tokio::test]
    async fn test_tower_metadata_runtime_info() {
        use super::*;
        use td_objects::crudl::ReadRequest;
        use td_objects::types::runtime_info::RuntimeInfo;
        use td_tower::metadata::type_of_val;

        RuntimeInfoService::with_defaults()
            .await
            .metadata()
            .await
            .assert_service::<ReadRequest<()>, RuntimeInfo>(&[
                // Extract from request.
                type_of_val(&runtime_info),
            ]);
    }
}
