//
// Copyright 2025 Tabsdata Inc.
//

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
