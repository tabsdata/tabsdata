//
// Copyright 2025 Tabs Data Inc.
//

use td_type::Dto;

#[Dto]
pub struct ApiStatus {
    status: HealthStatus,
    latency_as_nanos: u128,
}

#[td_type::typed_enum]
pub enum HealthStatus {
    OK,
    DatabaseError(String),
}
