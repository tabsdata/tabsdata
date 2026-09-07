//
// Copyright 2026 Tabsdata Inc.
//

#[cfg(test)]
mod tests;

mod composed_string;
pub mod dao_registry;
mod dxo;
pub mod errors;
mod list_cursor;
mod path_params;
mod sea_value;
pub mod serde_utils;
pub mod sql_condition;
mod tagged;
pub mod typed;

pub use {
    composed_string::ComposedString,
    dao_registry::{
        DaoProjection, RegisteredDao, RegisteredListDto, dao_projection, registered_daos,
        registered_list_dtos,
    },
    dxo::*,
    list_cursor::ListFilterStr,
    path_params::{PathParams, Route},
    sea_value::{NullableSeaValue, SeaValue},
    tagged::{Tagged, TaggedExt},
    // re-exported so the `Dao` macro's emitted `type Db` default resolves
    td_database::domain::Apiserver,
    typed::{Typed, TypedError, TypedInner},
};

// Re-exported so the `#[Dao]` macro can emit `ta_objects::inventory::submit!`
// without consumer crates needing a direct `inventory` dependency.
pub use inventory;

pub use tm_objects::*;
