//
// Copyright 2025 Tabs Data Inc.
//

use axum::extract::FromRef;
use td_tower::ServiceFactory;

#[derive(Clone, ServiceFactory, FromRef)]
pub struct ExtendedServices {}
