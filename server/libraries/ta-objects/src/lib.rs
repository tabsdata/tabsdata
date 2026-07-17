//
// Copyright 2026 Tabs Data Inc.
//

#[cfg(test)]
mod tests;

mod composed_string;
pub mod dao_registry;
mod dxo;
pub mod errors;
pub mod list_condition;
mod list_filter;
mod sea_value;
pub mod serde_utils;
pub mod sql_condition;
mod tagged;

pub use {
    composed_string::ComposedString,
    dao_registry::{DaoProjection, RegisteredDao, dao_projection, registered_daos},
    dxo::*,
    list_condition::{ListFilter, ListFilterExtract, ListRef, NoFilter},
    list_filter::ListFilterStr,
    sea_value::{NullableSeaValue, SeaValue},
    tagged::{Tagged, TaggedExt},
    // re-exported so the `Dao` macro's emitted `type Db` default resolves
    td_database::domain::Apiserver,
};

// Re-exported so the `#[Dao]` macro can emit `ta_objects::inventory::submit!`
// without consumer crates needing a direct `inventory` dependency.
pub use inventory;

// external macros
pub use tm_objects::*;
