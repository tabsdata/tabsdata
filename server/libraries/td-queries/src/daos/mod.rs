//
// Copyright 2026 Tabsdata Inc.
//

//! DAO-specific query handles, one module per entity cluster, each opened by
//! its own [`Queries`](crate::Queries) method.

pub mod data_version;
pub mod dependency;
pub mod execution;
pub mod function_runs;
pub mod metrics;
pub mod session;
pub mod table_data_versions;
