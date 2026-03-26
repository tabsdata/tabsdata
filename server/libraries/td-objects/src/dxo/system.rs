//
// Copyright 2025 Tabs Data Inc.
//

#[ta_objects::Dto]
pub struct ApiStatus {
    pub status: HealthStatus,
    pub latency_as_nanos: u128,
}

#[ta_objects::typed_enum]
pub enum HealthStatus {
    OK,
    DatabaseError(String),
}
