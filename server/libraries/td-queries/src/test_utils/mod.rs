//
// Copyright 2025 Tabsdata Inc.
//

// All seed methods exposed through the `Seed<E>` handle, reached via
// `SeedEntry::seed` on any `QueryExecutor`.
pub use exclusive::begin_exclusive;
pub use metrics_facts::derive_metrics_facts;
pub use seed::{Seed, SeedEntry};
pub use seed_run::{SeededRun, seed_project_viewer, seed_run};
pub use status_rollup::derive_status_rollup;

mod exclusive;
mod metrics_facts;
mod seed;
mod seed_collection;
mod seed_execution;
mod seed_function;
mod seed_group;
mod seed_logical_function_run;
mod seed_logical_input;
mod seed_permission;
mod seed_physical_function_run;
mod seed_physical_table_data_version;
mod seed_project;
mod seed_role;
mod seed_run;
mod seed_system_tables;
mod seed_transaction;
mod seed_user;
mod seed_user_role;
mod seed_worker;
mod status_rollup;
