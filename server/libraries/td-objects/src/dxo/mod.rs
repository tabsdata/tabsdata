//
// Copyright 2025 Tabs Data Inc.
//

//! # DXO
//! DXO refers to the following diagram, composed on DAO, DLO and DTO.
//!
//! ```text
//!     +---------------------------+
//!     |       API / DTO Layer     |
//!     +---------------------------+
//!                |
//!     +---------------------------+
//!     |       Logic Layer (DLO)   |
//!     +---------------------------+
//!                |
//!     +---------------------------+
//!     |   DataAccessObject (DAO)  |
//!     +---------------------------+
//!                |
//!     +---------------------------+
//!     |       Database / SQLx     |
//!     +---------------------------+
//! ```

pub mod auth;
pub mod bundle;
pub mod collection;
pub mod crudl;
pub mod dependency;
pub mod execution;
pub mod file;
pub mod function;
pub mod function_upload;
pub mod global_status;
pub mod graph;
pub mod group;
pub mod inter_collection_access;
pub mod inter_collection_permission;
pub mod logical;
pub mod permission;
pub mod physical_function_run;
pub mod physical_table_data_version;
pub mod query_filters;
pub mod role;
pub mod run_metrics;
pub mod runtime_info;
pub mod synchrotron;
pub mod system;
pub mod table;
pub mod transaction;
pub mod trigger;
pub mod user;
pub mod user_role;
pub mod worker;
pub mod yaml;
