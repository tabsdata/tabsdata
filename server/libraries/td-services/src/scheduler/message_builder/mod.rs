//
// Copyright 2026 Tabsdata Inc.
//

//! Builder for the function-runner worker message (`Function`).
//!
//! The version-resolution + message-assembly logic shared by the
//! supervisor request layer (batch, at = dispatch tick) and the
//! kubernetes resolve endpoint (single FR, at = now()).

mod cron;
mod function_v2;
mod info;
mod inputs;
mod locations;
mod outputs;
mod streamer;

pub(crate) use cron::build_cron_input;
pub(crate) use function_v2::build_function_v2;
pub(crate) use inputs::prefetch_dispatch;
pub(crate) use locations::build_external_location;
pub(crate) use streamer::build_streamer_input;

use std::collections::HashMap;

/// Partition a position-keyed map into (system, user) vectors: negative
/// positions are system tables, the rest user tables; each side ordered
/// by position. `transform` shapes each value into the output table type.
pub(crate) fn partition_and_sort<T, U>(
    map: HashMap<i32, T>,
    transform: impl Fn(T) -> U,
) -> (Vec<U>, Vec<U>) {
    let mut system_tables = Vec::new();
    let mut user_tables = Vec::new();

    let mut entries: Vec<_> = map.into_iter().collect();
    entries.sort_by_key(|(i, _)| *i);

    for (i, value) in entries {
        let transformed = transform(value);
        if i < 0 {
            system_tables.push(transformed);
        } else {
            user_tables.push(transformed);
        }
    }

    (system_tables, user_tables)
}
