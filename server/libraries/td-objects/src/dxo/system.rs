//
// Copyright 2025 Tabsdata Inc.
//

use crate::types::basic::InstanceId;

#[ta_objects::Dto]
pub struct Pong {}

#[ta_objects::Dto]
pub struct Instance {
    pub instance_id: InstanceId,
}

#[ta_objects::Dto]
pub struct ApiStatus {
    pub status: HealthStatus,
    pub latency_as_nanos: u128,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub enum HealthStatus {
    OK,
    DatabaseError(String),
}
