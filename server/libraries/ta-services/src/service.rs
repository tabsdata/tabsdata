//
// Copyright 2025 Tabs Data Inc.
//

use td_tower::service_provider::TdBoxService;

pub trait TdService {
    type Request;
    type Response;
    type Error;

    fn service(&self) -> TdBoxService<Self::Request, Self::Response, Self::Error>;

    #[cfg(feature = "test_tower_metadata")]
    fn metadata(&self) -> impl Future<Output = td_tower::metadata::Metadata>;
}
