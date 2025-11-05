//
// Copyright 2025 Tabs Data Inc.
//

#[td_type::Dto]
pub struct ApiStatus {
    pub status: HealthStatus,
    pub latency_as_nanos: u128,
}

#[td_type::typed_enum]
pub enum HealthStatus {
    OK,
    DatabaseError(String),
}
