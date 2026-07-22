//
// Copyright 2026 Tabsdata Inc.
//

//! `transporter2` — the streaming convert pipeline.
//!
//! A `td_files`-style file mover (clean request/response YAML interface, same
//! `Location { uri, configs }` model) that ALSO transcodes between data formats
//! the way the legacy polars [`crate::transporter`] did — parquet / csv / ndjson
//! / avro / text, with per-codec (de)compression — but built as an arrow-rs
//! streaming pipeline instead of polars `LazyFrame`s.
//!
//! The architecture is split into a **pure** planner ([`plan`]) that decides the
//! [`Strategy`](plan::Strategy) for a fully-resolved [`CopySpec`](spec::CopySpec),
//! and the engines that execute it (`ByteCopy`, the streaming `Convert`, and the
//! `Staged` decorator). Keeping `plan` IO-free makes the whole routing truth
//! table unit-testable without mocks.

pub mod byte_copy;
pub mod cli;
pub mod codec;
pub mod commands;
pub mod convert;
pub mod csvprep;
pub mod error;
pub mod factory;
pub mod find;
pub mod format;
pub mod io;
pub mod lastmod;
pub mod plan;
pub mod schedule;
pub mod spec;
pub mod split;
pub mod staged;
pub mod task;
